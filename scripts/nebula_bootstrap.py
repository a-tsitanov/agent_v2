"""One-time NebulaGraph bootstrap: register the storaged host.

Run ONCE after the cluster first boots (idempotent — re-running is safe).
    python scripts/nebula_bootstrap.py
"""
from __future__ import annotations

import os
import time

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool


def _connect(host: str, port: int, *, attempts: int = 30, delay_s: float = 3.0) -> ConnectionPool:
    """Init a pool, retrying while graphd is still coming up.

    graphd's compose healthcheck (its /status endpoint) can flip healthy a beat
    before the 9669 client port accepts connections, so `init` may see status
    BAD / ConnectionRefused right after a (re)start. Retry rather than crash the
    whole `init` service (which would cascade to api/worker not starting)."""
    last = None
    for i in range(attempts):
        pool = ConnectionPool()
        try:
            if pool.init([(host, port)], Config()):
                return pool
        except Exception as exc:  # graphd not ready yet, retry
            last = exc
            pool.close()
        print(f"nebula_bootstrap: graphd {host}:{port} not ready (attempt {i + 1}/{attempts})")
        time.sleep(delay_s)
    raise RuntimeError(f"nebula_bootstrap: graphd {host}:{port} never became reachable: {last}")


def main() -> None:
    host = os.getenv("NEBULA_HOST", "127.0.0.1")
    port = int(os.getenv("NEBULA_PORT", "9669"))
    user = os.getenv("NEBULA_USER", "root")
    pwd = os.getenv("NEBULA_PASSWORD", "nebula")

    pool = _connect(host, port)
    sess = pool.get_session(user, pwd)
    try:
        r = sess.execute("ADD HOSTS \"nebula-storaged\":9779;")
        print("ADD HOSTS:", "ok" if r.is_succeeded() else r.error_msg())
        print(sess.execute("SHOW HOSTS;"))
    finally:
        sess.release()
        pool.close()


if __name__ == "__main__":
    main()
