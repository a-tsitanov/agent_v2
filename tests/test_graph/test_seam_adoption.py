# tests/test_graph/test_seam_adoption.py
"""No app code calls build_neo4j_graph_store() directly — only the seam."""
from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
ALLOW = {ROOT / "src" / "graph" / "store.py"}  # the dispatcher itself


def _is_exempt(py: pathlib.Path) -> bool:
    if py in ALLOW:
        return True
    # Neo4j->Milvus vector backfill scripts read the Neo4j SOURCE store
    # directly by design (one-way migration of er_vec/report_vec OUT of the
    # graph into Milvus); they legitimately call build_neo4j_graph_store().
    return py.name.startswith("backfill_") and py.name.endswith("_milvus.py")


def test_no_direct_neo4j_store_calls_outside_store_py():
    pat = re.compile(r"\bbuild_neo4j_graph_store\s*\(")
    offenders = []
    for base in (ROOT / "src", ROOT / "scripts"):
        for py in base.rglob("*.py"):
            if _is_exempt(py):
                continue
            if pat.search(py.read_text(encoding="utf-8")):
                offenders.append(str(py.relative_to(ROOT)))
    assert offenders == [], f"call build_graph_store() instead: {offenders}"
