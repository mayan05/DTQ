import time
import logging

from app.worker import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=5,
    acks_late=True,
)
def process_job(self, payload: str, delay_seconds: int = 5):
    job_id = self.request.id
    logger.info(f"Task {job_id} started | payload={payload}")

    # Idempotency check: if task already completed, skip
    from celery.result import AsyncResult

    existing = AsyncResult(job_id, app=celery_app)
    if existing.state == "SUCCESS":
        logger.info(f"Task {job_id} already completed, skipping (idempotent)")
        return existing.result

    try:
        # Simulate processing
        time.sleep(delay_seconds)
        result = f"Processed: {payload}"
        logger.info(f"Task {job_id} completed | result={result}")
        return result
    except Exception as exc:
        logger.error(f"Task {job_id} failed | error={exc}")
        raise self.retry(exc=exc, countdown=2**self.request.retries)


@celery_app.task
def periodic_health_check():
    logger.info("Periodic health check: workers alive")
    return "healthy"