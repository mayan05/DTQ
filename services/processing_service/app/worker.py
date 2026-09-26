import os
import json
import time
import logging
import redis
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("processing-service")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
JOB_SERVICE_URL = os.getenv("JOB_SERVICE_URL", "http://job-service:8000")

# Order matters for priorities in blpop
QUEUES = [
    os.getenv("QUEUE_HIGH", "queue:high"),
    os.getenv("QUEUE_DEFAULT", "queue:default"),
    os.getenv("QUEUE_LOW", "queue:low")
]

redis_client = redis.from_url(REDIS_URL)

def process_job(job_id, payload, delay_seconds):
    logger.info(f"Task {job_id} started | payload={payload}")
    try:
        # Simulate heavy processing
        time.sleep(delay_seconds)
        result = f"Processed: {payload}"
        logger.info(f"Task {job_id} completed | result={result}")
        
        # Notify Job Service (HTTP Callback)
        resp = requests.put(
            f"{JOB_SERVICE_URL}/internal/jobs/{job_id}/status", 
            json={"status": "SUCCESS", "result": result}
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.error(f"Task {job_id} failed | error={exc}")
        try:
            requests.put(
                f"{JOB_SERVICE_URL}/internal/jobs/{job_id}/status", 
                json={"status": "FAILURE", "result": str(exc)}
            )
        except Exception as api_exc:
            logger.error(f"Failed to notify Job Service: {api_exc}")

def main():
    logger.info(f"Processing Service started. Listening on queues: {QUEUES}")
    while True:
        try:
            # BLPOP blocks until a message is available in one of the queues
            # Prioritizes queues in the order they are provided!
            item = redis_client.blpop(QUEUES, timeout=0)
            if item:
                queue, message = item
                data = json.loads(message)
                logger.info(f"Received job from {queue.decode('utf-8')}")
                process_job(data["job_id"], data["payload"], data.get("delay_seconds", 5))
        except Exception as e:
            logger.error(f"Error reading from Redis: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
