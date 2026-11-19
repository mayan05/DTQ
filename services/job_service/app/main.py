import json
import logging
import os
import sqlite3
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Optional

import redis
from fastapi import FastAPI, HTTPException, Query, Response, status
from fastapi.responses import JSONResponse

from .models import JobRequest, JobResponse, JobStatus, JobStatusResponse, JobUpdate, Priority

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("job-service")

DATABASE_PATH = os.getenv("DATABASE_PATH", "jobs.db")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# Lazy / reconnecting redis client
redis_client = redis.from_url(REDIS_URL, decode_responses=True)


def get_db_connection() -> sqlite3.Connection:
    """Creates a thread-safe connection to the SQLite database with WAL mode."""
    # Ensure parent directory exists if a path with directories is provided
    db_dir = os.path.dirname(DATABASE_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)

    conn = sqlite3.connect(DATABASE_PATH, timeout=20.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def init_db():
    """Initializes the database schema with necessary columns and indexes."""
    conn = get_db_connection()
    try:
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    priority TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    result TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            # Check for missing columns in case of legacy database
            cursor = conn.execute("PRAGMA table_info(jobs)")
            existing_columns = {row["name"] for row in cursor.fetchall()}
            if "created_at" not in existing_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN created_at TEXT DEFAULT ''")
            if "updated_at" not in existing_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN updated_at TEXT DEFAULT ''")
            # Index on status and priority for fast querying
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_priority ON jobs(priority)")
    finally:
        conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown lifespan management."""
    logger.info("Initializing Job Service database...")
    init_db()
    logger.info(f"Connected to Redis broker at {REDIS_URL.split('@')[-1]}")
    yield
    logger.info("Shutting down Job Service...")


app = FastAPI(
    title="Distributed Task Queue - Job Service",
    description="Microservice responsible for job ingestion, client state querying, and priority dispatching via Redis broker.",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/", tags=["General"])
def root():
    """Service metadata and navigation entry point."""
    return {
        "service": "Distributed Task Queue - Job Ingestion Service",
        "architecture": "Decoupled Microservice (Producer / State Manager)",
        "version": "1.0.0",
        "status": "online",
        "documentation": "/docs",
        "endpoints": {
            "submit_job": "POST /jobs",
            "get_job": "GET /jobs/{job_id}",
            "list_jobs": "GET /jobs",
            "health_check": "GET /health"
        }
    }


@app.get("/health", tags=["Observability"])
def health_check():
    """
    Readiness and liveness probe for Kubernetes and Docker Compose.
    Verifies connectivity to SQLite persistence and Redis message broker.
    """
    health_status = {
        "status": "healthy",
        "service": "job-service",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "dependencies": {
            "database": "unknown",
            "redis": "unknown"
        }
    }

    # Verify SQLite database connectivity
    try:
        conn = get_db_connection()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        health_status["dependencies"]["database"] = "healthy"
    except Exception as exc:
        logger.error(f"Database health check failed: {exc}")
        health_status["dependencies"]["database"] = f"unhealthy: {str(exc)}"
        health_status["status"] = "degraded"

    # Verify Redis broker connectivity
    try:
        redis_client.ping()
        health_status["dependencies"]["redis"] = "healthy"
    except Exception as exc:
        logger.error(f"Redis health check failed: {exc}")
        health_status["dependencies"]["redis"] = f"unhealthy: {str(exc)}"
        health_status["status"] = "degraded"

    if health_status["status"] != "healthy":
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content=health_status)

    return health_status


@app.post("/jobs", status_code=status.HTTP_202_ACCEPTED, response_model=JobResponse, tags=["Jobs"])
def submit_job(request: JobRequest, response: Response):
    """
    Asynchronously accept a compute task.
    Returns 202 Accepted with the job ID and Location header for status polling.
    Pushes an event to the priority-specific Redis list queue.
    """
    job_id = str(uuid.uuid4())
    now_iso = datetime.now(timezone.utc).isoformat()
    priority_val = request.priority.value if hasattr(request.priority, "value") else str(request.priority)

    # 1. Persist initial PENDING state in Database
    conn = get_db_connection()
    try:
        with conn:
            conn.execute(
                """
                INSERT INTO jobs (id, status, priority, payload, result, created_at, updated_at) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (job_id, JobStatus.PENDING.value, priority_val, request.payload, None, now_iso, now_iso)
            )
    finally:
        conn.close()

    # 2. Publish Task Event to Redis Queue (List) based on Priority
    event = {
        "job_id": job_id,
        "payload": request.payload,
        "delay_seconds": request.delay_seconds,
        "priority": priority_val,
        "attempt": 1,
        "enqueued_at": now_iso
    }
    queue_name = f"queue:{priority_val}"
    try:
        redis_client.lpush(queue_name, json.dumps(event))
    except Exception as exc:
        logger.error(f"Failed to enqueue task {job_id} to Redis: {exc}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to enqueue task to message broker"
        )

    # Set RFC 7231 Location header for asynchronous REST polling
    response.headers["Location"] = f"/jobs/{job_id}"

    logger.info(f"Enqueued job {job_id} on {queue_name} with priority {priority_val}")
    return JobResponse(
        job_id=job_id,
        status=JobStatus.PENDING,
        priority=Priority(priority_val),
        created_at=now_iso,
        message="Job accepted and queued for processing"
    )


@app.get("/jobs/{job_id}", response_model=JobStatusResponse, tags=["Jobs"])
def get_job_status(job_id: str):
    """Retrieve current processing status and result of a specific job."""
    conn = get_db_connection()
    try:
        cur = conn.execute(
            "SELECT id, status, priority, payload, result, created_at, updated_at FROM jobs WHERE id = ?",
            (job_id,)
        )
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID '{job_id}' not found"
        )

    return JobStatusResponse(
        job_id=row["id"],
        status=JobStatus(row["status"]),
        priority=Priority(row["priority"]),
        payload=row["payload"],
        result=row["result"],
        created_at=row["created_at"],
        updated_at=row["updated_at"]
    )


@app.get("/jobs", response_model=List[JobStatusResponse], tags=["Jobs"])
def list_jobs(
    limit: int = Query(default=50, ge=1, le=500, description="Max jobs to return"),
    offset: int = Query(default=0, ge=0, description="Pagination offset"),
    status: Optional[JobStatus] = Query(default=None, description="Filter by job status"),
    priority: Optional[Priority] = Query(default=None, description="Filter by priority tier")
):
    """List jobs with pagination and optional status or priority filtering."""
    query = "SELECT id, status, priority, payload, result, created_at, updated_at FROM jobs WHERE 1=1"
    params = []

    if status:
        query += " AND status = ?"
        params.append(status.value)
    if priority:
        query += " AND priority = ?"
        params.append(priority.value)

    query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    conn = get_db_connection()
    try:
        cur = conn.execute(query, tuple(params))
        rows = cur.fetchall()
    finally:
        conn.close()

    return [
        JobStatusResponse(
            job_id=row["id"],
            status=JobStatus(row["status"]),
            priority=Priority(row["priority"]),
            payload=row["payload"],
            result=row["result"],
            created_at=row["created_at"],
            updated_at=row["updated_at"]
        )
        for row in rows
    ]


@app.put("/internal/jobs/{job_id}/status", tags=["Internal"])
def update_job_status(job_id: str, update: JobUpdate):
    """
    Internal endpoint called by the processing worker microservice to update job status and result.
    In an event-driven architecture, this acts as the status synchronizer.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    status_val = update.status.value if hasattr(update.status, "value") else str(update.status)

    conn = get_db_connection()
    try:
        with conn:
            cur = conn.execute(
                """
                UPDATE jobs 
                SET status = ?, result = ?, updated_at = ? 
                WHERE id = ?
                """,
                (status_val, update.result, now_iso, job_id)
            )
            if cur.rowcount == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Job with ID '{job_id}' not found to update"
                )
    finally:
        conn.close()

    logger.info(f"Updated job {job_id} status to {status_val}")
    return {"message": "updated", "job_id": job_id, "status": status_val}
