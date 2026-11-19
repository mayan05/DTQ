import json
from unittest.mock import MagicMock, patch
import pytest

from services.processing_service.app.worker import (
    execute_task,
    handle_retry_or_dlq,
    notify_job_service,
    process_job,
    QUEUE_DLQ,
    MAX_RETRIES,
)


def test_execute_task_success():
    result = execute_task("Process user data", delay_seconds=0)
    assert result == "Processed: Process user data"


def test_execute_task_failure_keyword():
    with pytest.raises(RuntimeError) as exc_info:
        execute_task("simulate_fail critical pipeline", delay_seconds=0)
    assert "Simulated processing failure" in str(exc_info.value)


@patch("services.processing_service.app.worker.requests.put")
def test_notify_job_service_success(mock_put):
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_put.return_value = mock_response

    ok = notify_job_service("job-123", "SUCCESS", "Done")
    assert ok is True
    mock_put.assert_called_once()
    assert "job-123" in mock_put.call_args[0][0]


@patch("services.processing_service.app.worker.requests.put")
def test_notify_job_service_failure(mock_put):
    mock_put.side_effect = Exception("Network down")
    ok = notify_job_service("job-123", "FAILED", "Error")
    assert ok is False


@patch("services.processing_service.app.worker.notify_job_service")
@patch("services.processing_service.app.worker.execute_task")
def test_process_job_success_flow(mock_execute, mock_notify):
    mock_execute.return_value = "Processed: Sample Task"
    mock_redis = MagicMock()

    data = {
        "job_id": "job-abc-456",
        "payload": "Sample Task",
        "delay_seconds": 0,
        "attempt": 1
    }
    process_job(mock_redis, data, "queue:high")

    # Verify state transitions: 1. PROCESSING, 2. SUCCESS
    assert mock_notify.call_count == 2
    mock_notify.assert_any_call("job-abc-456", status="PROCESSING")
    mock_notify.assert_any_call("job-abc-456", status="SUCCESS", result="Processed: Sample Task")


@patch("services.processing_service.app.worker.time.sleep")
@patch("services.processing_service.app.worker.notify_job_service")
def test_handle_retry_logic(mock_notify, mock_sleep):
    mock_redis = MagicMock()
    data = {
        "job_id": "job-retry-1",
        "payload": "Test payload",
        "attempt": 1
    }
    exc = RuntimeError("Transient DB timeout")

    handle_retry_or_dlq(mock_redis, data, "queue:default", exc)

    # Attempt should be incremented to 2
    assert data["attempt"] == 2
    # Should re-enqueue to source queue
    mock_redis.lpush.assert_called_once()
    assert mock_redis.lpush.call_args[0][0] == "queue:default"
    # Status should be notified as RETRYING
    mock_notify.assert_called_once()
    assert mock_notify.call_args.kwargs["status"] == "RETRYING"


@patch("services.processing_service.app.worker.time.sleep")
@patch("services.processing_service.app.worker.notify_job_service")
def test_handle_dlq_on_max_retries_exhausted(mock_notify, mock_sleep):
    mock_redis = MagicMock()
    data = {
        "job_id": "job-dlq-1",
        "payload": "Poison pill task",
        "attempt": MAX_RETRIES  # Reached max retries!
    }
    exc = ValueError("Invalid data format")

    handle_retry_or_dlq(mock_redis, data, "queue:high", exc)

    # Should push to DLQ
    mock_redis.lpush.assert_called_once()
    assert mock_redis.lpush.call_args[0][0] == QUEUE_DLQ
    dlq_payload = json.loads(mock_redis.lpush.call_args[0][1])
    assert dlq_payload["job_id"] == "job-dlq-1"
    assert dlq_payload["error"] == "Invalid data format"
    assert dlq_payload["original_queue"] == "queue:high"

    # Status should be notified as FAILED
    mock_notify.assert_called_once()
    assert mock_notify.call_args.kwargs["status"] == "FAILED"
