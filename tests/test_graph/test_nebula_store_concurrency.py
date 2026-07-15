"""NebulaGraphStore serializes access to its single (non-thread-safe) session.

Regression for the live ".31" failure: concurrent `execute()` on one shared
nebula session desynced the fbthrift protocol → 'Unknown client type'."""
from __future__ import annotations

import threading
import time

from src.graph.nebula_store import NebulaGraphStore


class _FakeResp:
    def is_succeeded(self):
        return True

    def error_msg(self):
        return ""


class _ConcurrencyDetectingSession:
    """execute() flags an in-flight window; if another thread enters while one
    is in-flight, that's a concurrency violation (the exact bug)."""

    def __init__(self):
        self._in = False
        self.violations = 0
        self.calls = 0

    def execute(self, stmt):
        if self._in:
            self.violations += 1
        self._in = True
        time.sleep(0.003)  # widen the race window
        self._in = False
        self.calls += 1
        return _FakeResp()

    def release(self):
        pass


def test_exec_is_serialized_across_threads():
    sess = _ConcurrencyDetectingSession()
    store = NebulaGraphStore(sess)
    threads = [threading.Thread(target=lambda: store._exec("INSERT ...")) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sess.calls == 12
    assert sess.violations == 0  # the store's lock prevents concurrent execute()
