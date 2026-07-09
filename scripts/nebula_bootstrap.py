"""One-time NebulaGraph bootstrap: register the storaged host.

Run ONCE after the cluster first boots (idempotent — re-running is safe).
    python scripts/nebula_bootstrap.py
"""
from __future__ import annotations

import os

from nebula3.Config import Config
from nebula3.gclient.net import ConnectionPool


def main() -> None:
    host = os.getenv("NEBULA_HOST", "127.0.0.1")
    port = int(os.getenv("NEBULA_PORT", "9669"))
    user = os.getenv("NEBULA_USER", "root")
    pwd = os.getenv("NEBULA_PASSWORD", "nebula")

    pool = ConnectionPool()
    assert pool.init([(host, port)], Config())
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
