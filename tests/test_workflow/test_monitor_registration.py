"""Tests that the monitor group is registered in the worker module.

Module-level inspection only — no Temporal server connection, no network.
"""

from __future__ import annotations

from src.workflow import worker as w
from src.workflow.monitor.activities import MONITOR_ACTIVITIES, detect_alerts
from src.workflow.monitor.workflow import MonitorSweepWorkflow


def test_monitor_in_worker_groups():
    assert "monitor" in w.WORKER_GROUPS


def test_detect_alerts_in_monitor_activities():
    assert detect_alerts in MONITOR_ACTIVITIES


def test_monitor_sweep_workflow_importable():
    assert MonitorSweepWorkflow is not None
