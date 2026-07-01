"""Tests that the worker module exposes the correct workflow/activity registrations.

These assertions inspect module-level lists only — no Temporal server connection,
no network, no async setup required.
"""

from src.workflow import worker as w
from src.workflow.analytics.activities import analytical_plan, execute_step, synthesize_analytical
from src.workflow.analytics.workflow import AnalyticalQueryWorkflow


def test_search_group_registers_analytics_workflow():
    assert AnalyticalQueryWorkflow in w.SEARCH_WORKFLOWS


def test_search_group_registers_analytics_activities():
    all_search = w.SEARCH_ACTIVITIES + w.SEARCH_V2_ACTIVITIES + w.ANALYTICS_ACTIVITIES
    assert analytical_plan in all_search
    assert execute_step in w.ANALYTICS_ACTIVITIES


def test_large_group_registers_synthesize_analytical():
    assert synthesize_analytical in w.ANALYTICS_LARGE_ACTIVITIES
