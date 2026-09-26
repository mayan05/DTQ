from fastapi import FastAPI, HTTPException
from .models import *
import redis
import json
import uuid
import os
import sqlite3

app = FastAPI(title="Job Service", version="1.0.0")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
redis_client = redis.from_url(REDIS_URL)

def get_db():
    conn = sqlite3.connect('jobs.db')
    conn.execute('''CREATE TABLE IF NOT EXISTS jobs 
                    (id TEXT PRIMARY KEY, status TEXT, priority TEXT, payload TEXT, result TEXT)''')
    return conn

@app.post("/jobs", status_code=202, response_model=JobResponse)
def submit_job(request: JobRequest):
    if request.priority not in ("high", "default", "low"):
        raise HTTPException(
            status_code=400,
            detail="priority must be one of: high, default, low",
        )
    
    job_id = str(uuid.uuid4())
    conn = get_db()
    conn.execute("INSERT INTO jobs (id, status, priority, payload, result) VALUES (?, ?, ?, ?, ?)", 
                 (job_id, "PENDING", request.priority, request.payload, None))
    conn.commit()
    conn.close()

    # Publish to Redis Queue (List) based on priority
    # This acts as our Event Bus/Message Broker
    event = {
        "job_id": job_id,
        "payload": request.payload,
        "delay_seconds": request.delay_seconds
    }
    queue_name = f"queue:{request.priority}"
    redis_client.lpush(queue_name, json.dumps(event))

    return JobResponse(job_id=job_id, status="PENDING", priority=request.priority)

@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str):
    conn = get_db()
    cur = conn.execute("SELECT status, result FROM jobs WHERE id = ?", (job_id,))
    row = cur.fetchone()
    conn.close()
    
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return JobStatusResponse(job_id=job_id, status=row[0], result=row[1])

@app.get("/jobs", response_model=list[JobStatusResponse])
def list_jobs():
    conn = get_db()
    cur = conn.execute("SELECT id, status, result FROM jobs")
    rows = cur.fetchall()
    conn.close()
    return [JobStatusResponse(job_id=row[0], status=row[1], result=row[2]) for row in rows]

@app.put("/internal/jobs/{job_id}/status")
def update_job_status(job_id: str, update: JobUpdate):
    """
    Internal endpoint called by the processing service to update job status.
    In a fully event-driven architecture, this could also be a reverse message queue.
    """
    conn = get_db()
    conn.execute("UPDATE jobs SET status = ?, result = ? WHERE id = ?", 
                 (update.status, update.result, job_id))
    conn.commit()
    conn.close()
    return {"message": "updated"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}
