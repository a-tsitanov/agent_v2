"""Channel-groups Task 3: `group` carried on IngestParams/Ctx.

Mirrors the existing `doc_date_epoch` propagation path — this file only
covers the pure contracts-level default/round-trip behaviour. Stamping
of `doc_group` onto chunk metadata inside `parse_and_chunk` is covered
by the "doc_group" assertions layered onto the existing
`test_parse_and_chunk.py` suite (see that file for the pipeline mocks);
here we keep to what Step 1 of the task brief asks for.
"""

from __future__ import annotations

from src.workflow.contracts import Ctx, IngestParams


def test_contracts_carry_group_default_empty():
    assert IngestParams(doc_id="d", path="s3://x").group == ""
    assert Ctx(doc_id="d", local_path="/p", cleanup_dir=None,
               workflow_run_id="r").group == ""


def test_contracts_accept_group():
    p = IngestParams(doc_id="d", path="s3://x", group="official")
    assert p.group == "official"
