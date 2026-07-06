"""Contracts cross the Temporal boundary, so they must JSON-roundtrip
losslessly with the default DataConverter (Pydantic v2 -> JSON)."""

from __future__ import annotations

import uuid

from src.workflow.contracts import (
    Ctx,
    FinalizeIn,
    Indexed,
    IngestParams,
    IngestResult,
    MarkFailedIn,
    Parsed,
)


def test_ingest_params_roundtrip() -> None:
    p = IngestParams(doc_id=str(uuid.uuid4()), path="s3://kb-uploads/x/y.pdf")
    assert IngestParams.model_validate_json(p.model_dump_json()) == p


def test_ingest_params_carries_wiki_enabled() -> None:
    # The wiki feature flag is snapshotted at submit time and crosses the
    # Temporal boundary so the workflow never reads settings.wiki (which
    # would hit .env from inside the sandbox — a determinism violation).
    p = IngestParams(doc_id="d", path="/tmp/x", wiki_enabled=True)
    assert p.wiki_enabled is True
    assert IngestParams.model_validate_json(p.model_dump_json()) == p
    # Default keeps older callers safe (feature off unless snapshotted on).
    assert IngestParams(doc_id="d", path="/tmp/x").wiki_enabled is False


def test_ctx_roundtrip() -> None:
    c = Ctx(
        doc_id="11111111-1111-1111-1111-111111111111",
        local_path="/tmp/x/y.pdf",
        cleanup_dir="/tmp/x",
        workflow_run_id="run-abc",
    )
    assert Ctx.model_validate_json(c.model_dump_json()) == c


def test_parsed_roundtrip() -> None:
    ctx = Ctx(doc_id="d", local_path="/tmp/f", cleanup_dir=None, workflow_run_id="r")
    p = Parsed(ctx=ctx, nodes_uri="s3://kb-staging/r/parsed.pkl", chunk_count=42)
    assert Parsed.model_validate_json(p.model_dump_json()) == p


def test_finalize_in_carries_graph_status() -> None:
    ctx = Ctx(doc_id="d", local_path="/tmp/f", cleanup_dir=None, workflow_run_id="r")
    idx = Indexed(node_ids=["a", "b"], count=2)
    fin = FinalizeIn(ctx=ctx, indexed=idx, graph_status="vector_only")
    assert FinalizeIn.model_validate_json(fin.model_dump_json()) == fin


def test_ingest_result_shape() -> None:
    r = IngestResult(doc_id="d", chunk_count=2, graph_status="completed")
    assert IngestResult.model_validate_json(r.model_dump_json()) == r


def test_mark_failed_in_optional_ctx() -> None:
    # mark_failed runs even before ctx exists (fetch_source crashed).
    m = MarkFailedIn(
        ctx=None,
        params=IngestParams(doc_id="d", path="/tmp/x"),
        error="boom",
    )
    assert MarkFailedIn.model_validate_json(m.model_dump_json()) == m
