from pydantic import BaseModel

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

class JobUpdate(BaseModel):
    status: str
    result: str | None = None