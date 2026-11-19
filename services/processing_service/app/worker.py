import json
import logging
import os
import random
import signal
import socket
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import redis
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(name)s] - [%(levelname)s] - %(message)s"
)
logger = logging.getLogger("processing-service")

# Service Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
JOB_SERVICE_URL = os.getenv("JOB_SERVICE_URL", "http://job-service:8000")
WORKER_ID = os.getenv("WORKER_ID", f"worker-{socket.gethostname()}-{os.getpid()}")

# Queues Configuration: comma-separated list or individual queue
if os.getenv("QUEUES"):
    QUEUES = [q.strip() for q in os.getenv("QUEUES", "").split(",") if q.strip()]
else:
    # Default worker handles queue:default
    QUEUES = [os.getenv("QUEUE_NAME", "queue:default")]

QUEUE_DLQ = os.getenv("QUEUE_DLQ", "queue:dlq")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "1.5"))

# Global flag for graceful shutdown handling
shutdown_requested = False


def signal_handler(signum, frame):
    """Graceful shutdown handler for SIGINT and SIGTERM."""
    global shutdown_requested
    sig_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
    logger.info(f"Received {sig_name}. Initiating graceful shutdown (waiting for current job to complete)...")
    shutdown_requested = True


# Register shutdown signals
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_redis_client() -> redis.Redis:
    """Instantiate Redis client connection."""
    return redis.from_url(REDIS_URL, decode_responses=True)


def notify_job_service(job_id: str, status: str, result: Optional[str] = None) -> bool:
    """
    HTTP state callback to Job Service.
    Notifies the ingestion layer of task state transitions (PROCESSING, RETRYING, SUCCESS, FAILED).
    """
    url = f"{JOB_SERVICE_URL}/internal/jobs/{job_id}/status"
    payload = {"status": status, "result": result}
    try:
        response = requests.put(url, json=payload, timeout=5)
        response.raise_for_status()
        logger.debug(f"Successfully notified Job Service: job={job_id}, status={status}")
        return True
    except Exception as exc:
        logger.error(f"Failed to notify Job Service for job {job_id} (status={status}): {exc}")
        return False


def execute_task(payload: str, delay_seconds: int) -> str:
    """
    Simulates the core business logic or heavy compute processing.
    Raises an exception if payload triggers an intentional failure for testing resilience.
    """
    if "simulate_fail" in payload.lower():
        raise RuntimeError(f"Simulated processing failure triggered by payload keyword: '{payload}'")

    time.sleep(delay_seconds)
    return f"Processed: {payload}"


def handle_retry_or_dlq(redis_client: redis.Redis, data: Dict[str, Any], source_queue: str, exc: Exception):
    """
    ========================================================================================
    PRODUCTION RELIABILITY PATTERN: EXPONENTIAL BACKOFF + JITTER + DEAD-LETTER QUEUE (DLQ)
    ========================================================================================
    1. Retries: Allows transient failures (network blips, lock contention) to recover.
    2. Exponential Backoff: Each retry waits exponentially longer: base ^ attempt (e.g., 1.5^1, 1.5^2, 1.5^3).
    3. Jitter: Adds random delay (+ random.uniform(0.1, 0.5)) to prevent the 'thundering herd'
       problem where multiple failed workers hammer the system at the exact same moment.
    4. Dead-Letter Queue (DLQ): If all retries are exhausted, the poison-pill message is routed
       to queue:dlq rather than crashing the worker or looping forever.
    ========================================================================================
    """
    job_id = data.get("job_id", "unknown")
    attempt = data.get("attempt", 1)

    if attempt < MAX_RETRIES:
        next_attempt = attempt + 1
        data["attempt"] = next_attempt

        # === EXPONENTIAL BACKOFF WITH JITTER IMPLEMENTATION ===
        exponential_delay = BACKOFF_BASE ** attempt
        jitter = random.uniform(0.1, 0.5)
        backoff = exponential_delay + jitter
        # =======================================================

        logger.warning(
            f"Job {job_id} failed on attempt {attempt}/{MAX_RETRIES}: {exc}. "
            f"Retrying in {backoff:.2f}s (Attempt {next_attempt})..."
        )

        notify_job_service(
            job_id,
            status="RETRYING",
            result=f"Attempt {attempt} failed: {exc}. Retrying in {backoff:.1f}s (Attempt {next_attempt}/{MAX_RETRIES})"
        )

        # Wait backoff period before re-enqueuing
        time.sleep(backoff)
        try:
            redis_client.lpush(source_queue, json.dumps(data))
            logger.info(f"Re-enqueued job {job_id} to {source_queue} for attempt {next_attempt}")
        except Exception as redis_err:
            logger.error(f"Failed to re-enqueue job {job_id} to Redis: {redis_err}")
    else:
        # Max retries exhausted -> Route to Dead Letter Queue (DLQ)
        logger.critical(
            f"Job {job_id} exhausted all {MAX_RETRIES} attempts. Routing to Dead Letter Queue ({QUEUE_DLQ})."
        )
        dlq_event = {
            **data,
            "error": str(exc),
            "exhausted_at": datetime.now(timezone.utc).isoformat(),
            "original_queue": source_queue,
            "worker_id": WORKER_ID
        }
        try:
            redis_client.lpush(QUEUE_DLQ, json.dumps(dlq_event))
            logger.info(f"Job {job_id} safely stored in DLQ: {QUEUE_DLQ}")
        except Exception as redis_err:
            logger.error(f"Failed to push job {job_id} to DLQ: {redis_err}")

        notify_job_service(
            job_id,
            status="FAILED",
            result=f"Max retries ({MAX_RETRIES}) exhausted. Routed to DLQ: {exc}"
        )


def process_job(redis_client: redis.Redis, data: Dict[str, Any], source_queue: str):
    """
    Orchestrates the lifecycle of processing a single job:
    1. Transition to PROCESSING
    2. Execute task logic
    3. Transition to SUCCESS
    4. On failure, invoke retry or DLQ
    """
    job_id = data.get("job_id")
    payload = data.get("payload", "")
    delay_seconds = data.get("delay_seconds", 5)
    attempt = data.get("attempt", 1)

    logger.info(
        f"Started processing job {job_id} [Worker: {WORKER_ID}, Queue: {source_queue}, Attempt: {attempt}/{MAX_RETRIES}]"
    )

    # Inform Job Service that processing has commenced
    notify_job_service(job_id, status="PROCESSING")

    try:
        result = execute_task(payload, delay_seconds)
        logger.info(f"Successfully finished job {job_id} | Result: {result}")
        notify_job_service(job_id, status="SUCCESS", result=result)
    except Exception as exc:
        logger.error(f"Execution error on job {job_id}: {exc}")
        handle_retry_or_dlq(redis_client, data, source_queue, exc)


def main():
    """Main worker event loop listening across configured Redis queues."""
    logger.info(f"Initializing Processing Worker [{WORKER_ID}]...")
    logger.info(f"Target Redis: {REDIS_URL.split('@')[-1]}")
    logger.info(f"Assigned Queues: {QUEUES}")
    logger.info(f"Dead Letter Queue: {QUEUE_DLQ}")
    logger.info(f"Reliability Config: Max Retries={MAX_RETRIES}, Backoff Base={BACKOFF_BASE}")

    redis_client = get_redis_client()

    while not shutdown_requested:
        try:
            # BLPOP blocks until an item is available in one of the assigned queues.
            # A timeout of 2 seconds allows the loop to check shutdown_requested regularly.
            item = redis_client.blpop(QUEUES, timeout=2)
            if item:
                source_queue, raw_message = item
                try:
                    data = json.loads(raw_message)
                except json.JSONDecodeError as json_err:
                    logger.error(f"Corrupt message on queue {source_queue}: {raw_message} | Error: {json_err}")
                    continue

                process_job(redis_client, data, source_queue)

        except redis.exceptions.ConnectionError as conn_err:
            if not shutdown_requested:
                logger.error(f"Redis connection error: {conn_err}. Reconnecting in 3s...")
                time.sleep(3)
        except Exception as unhandled_err:
            logger.error(f"Unexpected error in worker loop: {unhandled_err}")
            time.sleep(1)

    logger.info(f"Processing Worker [{WORKER_ID}] shut down cleanly.")


if __name__ == "__main__":
    main()
