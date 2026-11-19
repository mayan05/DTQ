import json
import os
import tempfile
from unittest.mock import MagicMock
import pytest
from fastapi import HTTPException, Response

import services.job_service.app.main as main_module
from services.job_service.app.main import (
    get_job_status,
    health_check,
    init_db,
    list_jobs,
    root,
    submit_job,
    update_job_status,
)
from services.job_service.app.models import (
    JobRequest,
    JobStatus,
    JobUpdate,
    Priority,
)


@pytest.fixture(autouse=True)
def setup_test_db(monkeypatch):
    """Create an isolated temporary SQLite database for each test run."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        db_path = tmp.name

    monkeypatch.setattr(main_module, "DATABASE_PATH", db_path)
    init_db()

    yield db_path

    if os.path.exists(db_path):
        try:
            os.remove(db_path)
        except OSError:
            pass


def test_root_endpoint():
    data = root()
    assert data["service"] == "Distributed Task Queue - Job Ingestion Service"
    assert data["status"] == "online"
    assert "endpoints" in data


def test_health_check_healthy(monkeypatch):
    mock_redis = MagicMock()
    mock_redis.ping.return_value = True
    monkeypatch.setattr(main_module, "redis_client", mock_redis)

    result = health_check()
    assert result["status"] == "healthy"
    assert result["dependencies"]["database"] == "healthy"
    assert result["dependencies"]["redis"] == "healthy"


def test_health_check_degraded_when_redis_fails(monkeypatch):
    mock_redis = MagicMock()
    mock_redis.ping.side_effect = ConnectionError("Redis unreachable")
    monkeypatch.setattr(main_module, "redis_client", mock_redis)

    resp = health_check()
    assert resp.status_code == 503
    body = json.loads(resp.body)
    assert body["status"] == "degraded"
    assert "unhealthy" in body["dependencies"]["redis"]


def test_submit_job_success(monkeypatch):
    mock_redis = MagicMock()
    monkeypatch.setattr(main_module, "redis_client", mock_redis)

    req = JobRequest(
        payload="Resize profile avatar",
        delay_seconds=2,
        priority=Priority.HIGH
    )
    http_resp = Response()
    result = submit_job(req, http_resp)

    assert result.job_id is not None
    assert result.status == JobStatus.PENDING
    assert result.priority == Priority.HIGH
    assert http_resp.headers["Location"] == f"/jobs/{result.job_id}"

    # Verify event pushed to Redis high priority queue
    mock_redis.lpush.assert_called_once()
    queue_name, raw_event = mock_redis.lpush.call_args[0]
    assert queue_name == "queue:high"
    enqueued = json.loads(raw_event)
    assert enqueued["job_id"] == result.job_id
    assert enqueued["payload"] == "Resize profile avatar"


def test_submit_job_default_priority(monkeypatch):
    mock_redis = MagicMock()
    monkeypatch.setattr(main_module, "redis_client", mock_redis)

    req = JobRequest(payload="Send transactional email")
    http_resp = Response()
    result = submit_job(req, http_resp)

    assert result.priority == Priority.DEFAULT
    queue_name, _ = mock_redis.lpush.call_args[0]
    assert queue_name == "queue:default"


def test_get_job_status_found(monkeypatch):
    mock_redis = MagicMock()
    monkeypatch.setattr(main_module, "redis_client", mock_redis)

    http_resp = Response()
    created = submit_job(JobRequest(payload="Compute risk metrics", priority=Priority.DEFAULT), http_resp)

    status_resp = get_job_status(created.job_id)
    assert status_resp.job_id == created.job_id
    assert status_resp.status == JobStatus.PENDING
    assert status_resp.priority == Priority.DEFAULT
    assert status_resp.payload == "Compute risk metrics"
    assert status_resp.result is None


def test_get_job_status_not_found():
    with pytest.raises(HTTPException) as exc_info:
        get_job_status("non-existent-uuid-1234")
    assert exc_info.value.status_code == 404
    assert "not found" in exc_info.value.detail.lower()


def test_list_jobs_and_filter(monkeypatch):
    mock_redis = MagicMock()
    monkeypatch.setattr(main_module, "redis_client", mock_redis)

    submit_job(JobRequest(payload="High priority job", priority=Priority.HIGH), Response())
    submit_job(JobRequest(payload="Default priority job", priority=Priority.DEFAULT), Response())

    all_jobs = list_jobs(limit=10, offset=0, status=None, priority=None)
    assert len(all_jobs) == 2

    high_jobs = list_jobs(limit=10, offset=0, status=None, priority=Priority.HIGH)
    assert len(high_jobs) == 1
    assert high_jobs[0].priority == Priority.HIGH


def test_internal_update_job_status(monkeypatch):
    mock_redis = MagicMock()
    monkeypatch.setattr(main_module, "redis_client", mock_redis)

    created = submit_job(JobRequest(payload="Process video encoding"), Response())

    update_req = JobUpdate(status=JobStatus.SUCCESS, result="Render complete: 1080p")
    update_res = update_job_status(created.job_id, update_req)
    assert update_res["status"] == "SUCCESS"

    fetched = get_job_status(created.job_id)
    assert fetched.status == JobStatus.SUCCESS
    assert fetched.result == "Render complete: 1080p"


def test_internal_update_non_existent_job():
    with pytest.raises(HTTPException) as exc_info:
        update_job_status("unknown-id", JobUpdate(status=JobStatus.FAILED, result="Error"))
    assert exc_info.value.status_code == 404
