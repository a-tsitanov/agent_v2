"""Guard: stdlib `logging` must be configured, and the worker must configure it.

`temporalio`'s `activity.logger` is a stdlib logger, not loguru. The project never
touched stdlib logging: the root logger sat at its default (level WARNING, no
handler), so every `activity.logger.info(...)` was dropped and every
`activity.logger.warning(...)` fell through to `logging.lastResort`.

Whether activity logs appeared was therefore accidental and pool-dependent — the
worker forks one process per pool (`mp.set_start_method("spawn")`), so a
third-party import that happens to call `logging.basicConfig` at runtime
configured only the pool it ran in. In production the `kb-ingest` pool emitted
18654 activity INFO lines while `kb-graph-build` emitted zero, which made a
4.5-hour community build completely invisible to log-based monitoring.

`src/workflow/worker.py` never called `configure_logging` at all — only
`api/main.py` and `ingestion/run.py` did.
"""

from __future__ import annotations

import logging

from loguru import logger

from src.utils.logging import configure_logging


def test_configure_logging_routes_stdlib_records_into_loguru():
    """An `activity.logger`-style stdlib record must reach the loguru sink."""
    configure_logging(level="info")

    seen: list[str] = []
    sink_id = logger.add(lambda m: seen.append(m.record["message"]), level="INFO")
    try:
        logging.getLogger("temporalio.activity").info("summarize_community cid=7")
    finally:
        logger.remove(sink_id)

    assert any("summarize_community cid=7" in m for m in seen)


def test_configure_logging_lowers_the_stdlib_root_level():
    """Root defaults to WARNING, which silently drops every activity INFO line."""
    configure_logging(level="info")
    assert logging.root.level <= logging.INFO
    assert logging.root.handlers, "root logger must have a handler"


def test_worker_child_configures_logging_for_every_pool(monkeypatch):
    """Each spawned pool is a fresh process — it must configure logging itself,
    otherwise only the pools that trip a third-party basicConfig get logs."""
    import src.workflow.worker as w

    calls: list[str] = []
    monkeypatch.setattr(
        w, "configure_logging", lambda **kw: calls.append("configured"),
        raising=False,
    )
    monkeypatch.setattr(w.asyncio, "run", lambda _coro: None)
    # _run_one is never awaited (asyncio.run is stubbed); close the coroutine so
    # the stub doesn't leak a "never awaited" warning.
    monkeypatch.setattr(w, "_run_one", lambda group: None)

    w._child_main("graph_build")

    assert calls == ["configured"]


def test_intercepted_records_report_the_real_call_site():
    """The depth walk must land on the caller, not on `logging`'s own frames.

    A bridged record that reports `logging:callHandlers:1762` as its origin is
    worse than no location at all — it points every activity log line at the
    stdlib instead of the code that emitted it.
    """
    configure_logging(level="info")

    seen: list[tuple[str, str]] = []
    sink_id = logger.add(
        lambda m: seen.append((m.record["name"], m.record["function"])),
        level="INFO",
    )
    try:
        logging.getLogger("temporalio.activity").info("merge_and_resolve start")
    finally:
        logger.remove(sink_id)

    assert seen, "record did not reach the sink"
    name, func = seen[-1]
    assert name != "logging", f"call site collapsed to the logging module: {name}:{func}"
    assert func != "callHandlers"
