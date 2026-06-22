from src.workflow.search._retry import DETECT_RETRY


def test_detect_retry_marks_memory_errors_non_retryable():
    assert "MemoryError" in (DETECT_RETRY.non_retryable_error_types or [])
    # still allows a couple of transient retries
    assert DETECT_RETRY.maximum_attempts and DETECT_RETRY.maximum_attempts <= 3
