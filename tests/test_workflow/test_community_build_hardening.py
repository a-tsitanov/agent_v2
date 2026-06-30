from src.workflow.search._retry import (
    DETECT_HEARTBEAT_TIMEOUT,
    DETECT_RETRY,
    DETECT_SCHEDULE_TO_CLOSE,
    DETECT_START_TO_CLOSE,
)


def test_detect_retry_marks_memory_errors_non_retryable():
    assert "MemoryError" in (DETECT_RETRY.non_retryable_error_types or [])
    # still allows a couple of transient retries
    assert DETECT_RETRY.maximum_attempts and DETECT_RETRY.maximum_attempts <= 3


def test_detect_heartbeat_window_exceeds_gil_held_compute():
    # leidenalg find_partition holds the GIL for the whole compute, so the
    # heartbeat_every pulse cannot fire while clustering runs.  The window
    # must therefore be far larger than the old tight 2-minute value (which
    # false-killed a healthy detect with timeout_type_heartbeat), and must
    # stay below the start-to-close ceiling so a truly stuck/OOM run is
    # still bounded.
    assert DETECT_HEARTBEAT_TIMEOUT.total_seconds() > 120  # > old 2-min window
    assert DETECT_HEARTBEAT_TIMEOUT <= DETECT_START_TO_CLOSE
    assert DETECT_START_TO_CLOSE <= DETECT_SCHEDULE_TO_CLOSE
