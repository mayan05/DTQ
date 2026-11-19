from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


class Priority(str, Enum):
    HIGH = "high"
    DEFAULT = "default"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    RETRYING = "RETRYING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class JobRequest(BaseModel):
    payload: str = Field(..., description="Task payload or instruction data", json_schema_extra={"example": "Batch image processing task #42"})
    delay_seconds: int = Field(default=5, ge=0, le=300, description="Simulated task duration in seconds")
    priority: Priority = Field(default=Priority.DEFAULT, description="Priority queue tier: 'high' or 'default'")


class JobResponse(BaseModel):
    job_id: str
    status: JobStatus
    priority: Priority
    created_at: str
    message: str = "Job accepted and queued for processing"


class JobStatusResponse(BaseModel):
    job_id: str
    status: JobStatus
    priority: Priority
    payload: str
    result: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class JobUpdate(BaseModel):
    status: JobStatus
    result: Optional[str] = None
