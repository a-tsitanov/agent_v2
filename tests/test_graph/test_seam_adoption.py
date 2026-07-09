# tests/test_graph/test_seam_adoption.py
"""No app code calls build_neo4j_graph_store() directly — only the seam."""
from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
ALLOW = {ROOT / "src" / "graph" / "store.py"}  # the dispatcher itself


def test_no_direct_neo4j_store_calls_outside_store_py():
    pat = re.compile(r"\bbuild_neo4j_graph_store\s*\(")
    offenders = []
    for py in (ROOT / "src").rglob("*.py"):
        if py in ALLOW:
            continue
        if pat.search(py.read_text(encoding="utf-8")):
            offenders.append(str(py.relative_to(ROOT)))
    assert offenders == [], f"call build_graph_store() instead: {offenders}"
