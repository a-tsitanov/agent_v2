# tests/eval/migration/test_nebula_smoke.py
"""Connect to a live NebulaGraph and run `SHOW HOSTS`. Skipped in CI."""
from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.getenv("NEBULA_HOST"), reason="live NebulaGraph not configured"
)


def test_can_connect_and_show_hosts():
    from nebula3.Config import Config
    from nebula3.gclient.net import ConnectionPool

    pool = ConnectionPool()
    assert pool.init([(os.environ["NEBULA_HOST"], int(os.getenv("NEBULA_PORT", "9669")))], Config())
    sess = pool.get_session("root", os.getenv("NEBULA_PASSWORD", "nebula"))
    try:
        resp = sess.execute("SHOW HOSTS;")
        assert resp.is_succeeded(), resp.error_msg()
    finally:
        sess.release()
        pool.close()
