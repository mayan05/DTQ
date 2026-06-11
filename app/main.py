from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from celery.result import AsyncResult

from app.worker import celery_app
from app.tasks import process_job

app = FastAPI(title="Distributed Task Queue", version="1.0.0")


class JobRequest(BaseModel):
    payload: str
    delay_seconds: int = 5
    priority: str = "default"


class JobResponse(BaseModel):
    job_id: str
    status: str
    priority: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    result: str | None = None


@app.post("/jobs", status_code=202, response_model=JobResponse)
def submit_job(request: JobRequest):
    if request.priority not in ("high", "default", "low"):
        raise HTTPException(
            status_code=400,
            detail="priority must be one of: high, default, low",
        )

    task = process_job.apply_async(
        args=[request.payload, request.delay_seconds],
        queue=request.priority,
    )
    return JobResponse(job_id=task.id, status="PENDING", priority=request.priority)


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
def get_job_status(job_id: str):
    result = AsyncResult(job_id, app=celery_app)
    if result.state == "PENDING":
        return JobStatusResponse(job_id=job_id, status="PENDING")
    elif result.state == "FAILURE":
        return JobStatusResponse(
            job_id=job_id, status="FAILURE", result=str(result.info)
        )
    elif result.state == "SUCCESS":
        return JobStatusResponse(
            job_id=job_id, status="SUCCESS", result=str(result.result)
        )
    else:
        return JobStatusResponse(job_id=job_id, status=result.state)


@app.get("/jobs", response_model=list[JobStatusResponse])
def list_jobs():
    return []


@app.get("/health")
def health_check():
    return {"status": "healthy"}