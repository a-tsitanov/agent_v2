"""NebulaGraphStore self-heals an expired session.

Regression: graphd kills idle sessions (session_idle_timeout_secs); the
process-global store cached one session forever, so once it expired every
query failed with 'Session not existed' — notably on the rarely-used
global-search map-reduce. _run now reconnects once and replays."""
from __future__ import annotations

from src.graph.nebula_store import NebulaGraphStore


class _Resp:
    def __init__(self, ok: bool, msg: str = ""):
        self._ok = ok
        self._msg = msg

    def is_succeeded(self) -> bool:
        return self._ok

    def error_msg(self) -> str:
        return self._msg


class _DeadSession:
    """execute() always reports an expired (server-killed) session."""

    def __init__(self):
        self.calls = 0

    def execute(self, stmt):
        self.calls += 1
        return _Resp(False, "Get sessionId[1] failed: Session `1' not found: Session not existed!")


class _LiveSession:
    def __init__(self):
        self.calls = 0

    def execute(self, stmt):
        self.calls += 1
        return _Resp(True)


def test_run_reconnects_on_expired_session():
    dead, live = _DeadSession(), _LiveSession()
    store = NebulaGraphStore(dead, reconnect=lambda: live)
    resp = store._run("MATCH (c:Community) RETURN count(c)")
    assert resp.is_succeeded()      # healed
    assert dead.calls == 1          # stale session tried once
    assert live.calls == 1          # statement replayed on the fresh session
    assert store._session is live   # swapped in for subsequent calls


def test_run_reconnects_on_execute_exception():
    class _RaisingSession:
        def execute(self, stmt):
            raise RuntimeError("broken pipe")

    live = _LiveSession()
    store = NebulaGraphStore(_RaisingSession(), reconnect=lambda: live)
    resp = store._run("Q")
    assert resp.is_succeeded()
    assert live.calls == 1


def test_run_without_reconnect_returns_dead_resp():
    dead = _DeadSession()
    store = NebulaGraphStore(dead)  # no reconnect → prior behaviour, no retry
    resp = store._run("Q")
    assert not resp.is_succeeded()
    assert dead.calls == 1


def test_run_does_not_reconnect_on_ordinary_failure():
    # A normal nGQL error (syntax etc.) must NOT trigger a reconnect.
    class _SyntaxErrSession:
        def __init__(self):
            self.calls = 0

        def execute(self, stmt):
            self.calls += 1
            return _Resp(False, "SyntaxError: near `FOO'")

    sess = _SyntaxErrSession()
    reconnects = {"n": 0}

    def _rc():
        reconnects["n"] += 1
        return _LiveSession()

    store = NebulaGraphStore(sess, reconnect=_rc)
    resp = store._run("Q")
    assert not resp.is_succeeded()
    assert sess.calls == 1
    assert reconnects["n"] == 0
