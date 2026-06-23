"""Deadlock/transient retry wrapper for the Neo4j write path (Track A3).

The graph-write activities MERGE into shared canonical hub nodes from
concurrent transactions; under contention Neo4j throws a RETRYABLE
``Neo.TransientError.Transaction.DeadlockDetected`` (or lock-timeout).
``write_with_retry`` re-runs the whole write a bounded number of times
with backoff so a transient lock loss doesn't fail the document.
"""
from __future__ import annotations

import pytest

from src.graph.write_retry import _is_transient, write_with_retry


class _Transient(Exception):
    code = "Neo.TransientError.Transaction.DeadlockDetected"


class _Permanent(Exception):
    code = "Neo.ClientError.Statement.SyntaxError"


def test_returns_result_without_retry_on_success() -> None:
    calls = {"n": 0}

    def _fn(x):
        calls["n"] += 1
        return x * 2

    assert write_with_retry(_fn, 21, base_delay_s=0) == 42
    assert calls["n"] == 1


def test_retries_transient_then_succeeds() -> None:
    calls = {"n": 0}

    def _fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise _Transient()
        return "ok"

    assert write_with_retry(_fn, max_attempts=5, base_delay_s=0) == "ok"
    assert calls["n"] == 3  # two transient failures, third succeeds


def test_permanent_error_not_retried() -> None:
    calls = {"n": 0}

    def _fn():
        calls["n"] += 1
        raise _Permanent()

    with pytest.raises(_Permanent):
        write_with_retry(_fn, max_attempts=5, base_delay_s=0)
    assert calls["n"] == 1  # no retry on a non-transient error


def test_transient_exhausts_attempts_and_reraises() -> None:
    calls = {"n": 0}

    def _fn():
        calls["n"] += 1
        raise _Transient()

    with pytest.raises(_Transient):
        write_with_retry(_fn, max_attempts=4, base_delay_s=0)
    assert calls["n"] == 4  # tried exactly max_attempts times


def test_is_transient_detection() -> None:
    assert _is_transient(_Transient())
    assert not _is_transient(_Permanent())
    assert not _is_transient(ValueError("no code"))
