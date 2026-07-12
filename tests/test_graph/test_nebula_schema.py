# tests/test_graph/test_nebula_schema.py
from __future__ import annotations

from src.graph.nebula_schema import SCHEMA_DDL, SPACE_DDL, SPACE_NAME, ensure_schema


def test_space_and_core_schema_present():
    assert f"CREATE SPACE IF NOT EXISTS `{SPACE_NAME}`" in SPACE_DDL

    joined = "\n".join(SCHEMA_DDL)
    assert "CREATE TAG IF NOT EXISTS `Entity`" in joined
    for edge in ("RELATED", "MENTIONS", "IN_COMMUNITY", "PARENT_OF"):
        assert f"CREATE EDGE IF NOT EXISTS `{edge}`" in joined
    assert "rel_type string" in "\n".join(SCHEMA_DDL)  # RELATED carries original type


def test_entity_ddl_has_er_canonical_name_column():
    # Entity-resolution canonical stamp (ER writes it via upsert_nodes).
    entity_ddl = next(
        s for s in SCHEMA_DDL if s.startswith("CREATE TAG IF NOT EXISTS `Entity`")
    )
    assert "er_canonical_name string" in entity_ddl


def test_schema_has_er_verdict_tag_and_index():
    tag = next((s for s in SCHEMA_DDL if "CREATE TAG IF NOT EXISTS `ERVerdict`" in s), None)
    assert tag is not None, "ERVerdict TAG missing from SCHEMA_DDL"
    for col in ("er_key string", "same bool", "updated int"):
        assert col in tag, f"missing column: {col}"
    assert any(
        "CREATE TAG INDEX IF NOT EXISTS `er_verdict_key_idx` ON `ERVerdict`(er_key(256))" in s
        for s in SCHEMA_DDL
    ), "er_verdict_key_idx missing (needed for verdict cache LOOKUP by key)"


def test_entity_ddl_has_first_doc_id_column():
    # Entity first-seen provenance (stamp_first_seen writes it via
    # upsert_nodes/UPDATE VERTEX; see nebula-first-seen design doc).
    entity_ddl = next(
        s for s in SCHEMA_DDL if s.startswith("CREATE TAG IF NOT EXISTS `Entity`")
    )
    assert "first_doc_id string" in entity_ddl


def test_related_edge_ddl_has_weight_column():
    # Weighted Leiden parity with neo4j (merge.py writes properties["weight"]):
    # fresh spaces must get a `weight` column on `RELATED`.
    related_ddl = next(
        s for s in SCHEMA_DDL if s.startswith("CREATE EDGE IF NOT EXISTS `RELATED`")
    )
    assert "weight double" in related_ddl


def test_ddl_is_idempotent():
    # every statement must be IF NOT EXISTS so ensure_schema can re-run.
    # There is no longer a non-idempotent `USE` in either list.
    assert "IF NOT EXISTS" in SPACE_DDL
    for stmt in SCHEMA_DDL:
        assert "IF NOT EXISTS" in stmt, stmt


class _FakeResp:
    """Always-succeeds response — no retry/sleep should ever fire."""

    def is_succeeded(self) -> bool:
        return True

    def error_msg(self) -> str:
        return ""


class _FakeSession:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, stmt: str) -> _FakeResp:
        self.statements.append(stmt)
        return _FakeResp()


def test_ensure_schema_selects_space_before_creates(monkeypatch):
    # Regression test for the Critical bug: ensure_schema must USE the
    # space before issuing any tag/edge/index DDL. Fail the test loudly
    # (rather than just slowly) if a retry/sleep path is ever exercised.
    def _no_sleep(*_args, **_kwargs):
        raise AssertionError("ensure_schema should not sleep on an always-succeeding session")

    monkeypatch.setattr("src.graph.nebula_schema.time.sleep", _no_sleep)

    fake = _FakeSession()
    ensure_schema(fake)

    assert fake.statements, "ensure_schema executed no statements"

    create_space_idx = next(
        i for i, s in enumerate(fake.statements) if s.startswith("CREATE SPACE")
    )
    use_idx = next(
        i for i, s in enumerate(fake.statements) if s == f"USE `{SPACE_NAME}`;"
    )
    schema_idxs = [
        i
        for i, s in enumerate(fake.statements)
        if s.startswith("CREATE TAG") or s.startswith("CREATE EDGE")
    ]

    assert schema_idxs, "no tag/edge DDL was executed"
    assert create_space_idx < use_idx
    assert all(use_idx < i for i in schema_idxs), (
        "USE must be issued before any CREATE TAG/EDGE/INDEX statement"
    )


def test_ensure_schema_probes_both_entity_and_community():
    # Both write-target tags share the storaged propagation lag (CREATE/DESCRIBE
    # succeed ~1 heartbeat before INSERT works). ensure_schema must probe-write
    # a sentinel for BOTH `Entity` (ingest) and `Community` (community BUILD) so
    # the first post-DDL write to EITHER doesn't race "Schema not exist".
    fake = _FakeSession()
    ensure_schema(fake)
    joined = "\n".join(fake.statements)
    assert "INSERT VERTEX `Entity` " in joined, "Entity write-readiness probe missing"
    assert "INSERT VERTEX `Community` " in joined, "Community write-readiness probe missing"
    # each successful probe removes its sentinel vertex
    assert fake.statements.count('DELETE VERTEX "__kb_schema_probe__";') == 3


def test_schema_has_community_tag_with_report_columns():
    tag = next((s for s in SCHEMA_DDL if "CREATE TAG IF NOT EXISTS `Community`" in s), None)
    assert tag is not None, "Community TAG missing from SCHEMA_DDL"
    # Structural columns written by the BUILD stage + report columns declared
    # now so the SUMMARIZE slice adds only writes, not a schema migration.
    for col in ("id string", "level int", "member_count int", "members_hash string",
                "updated int", "report string", "title string", "summary string",
                "summarized_at int"):
        assert col in tag, f"missing column: {col}"
    # report_vec lives in Milvus (Phase 3) — never on the vertex.
    assert "report_vec" not in tag


def test_schema_has_community_level_index():
    assert any(
        "CREATE TAG INDEX IF NOT EXISTS `community_level_idx` ON `Community`(level)" in s
        for s in SCHEMA_DDL
    ), "community_level_idx missing (needed for prune/lookup by level)"


def test_ensure_schema_alters_related_edge_to_add_weight():
    # Best-effort schema-evolution step for EXISTING spaces (created before
    # RELATED had a weight column). Fail-open: if the column already exists,
    # the ALTER errors harmlessly (see _FakeResp always-succeeds here — the
    # real fail-open behaviour is covered by _execute_with_retry's own
    # tests/usage elsewhere).
    fake = _FakeSession()
    ensure_schema(fake)
    assert "ALTER EDGE `RELATED` ADD (weight double DEFAULT 1.0);" in fake.statements


def test_ensure_schema_alters_entity_to_add_er_canonical_name():
    # Best-effort schema-evolution step for EXISTING spaces (created before
    # Entity had er_canonical_name). Fail-open, same pattern as the RELATED
    # weight ALTER.
    fake = _FakeSession()
    ensure_schema(fake)
    assert "ALTER TAG `Entity` ADD (er_canonical_name string DEFAULT '');" in fake.statements


def test_ensure_schema_entity_probe_includes_er_canonical_name():
    # The Entity write-readiness probe must cover the new column too, so
    # ensure_schema waits for the ALTER to propagate before the first
    # ingest write touches er_canonical_name (same propagation-lag class
    # already handled for RELATED.weight).
    fake = _FakeSession()
    ensure_schema(fake)
    joined = "\n".join(fake.statements)
    assert (
        "INSERT VERTEX `Entity` (name, description, mention_count, created_at, label, "
        "er_canonical_name, first_doc_id, " in joined
    )


def test_ensure_schema_alters_entity_to_add_first_doc_id():
    # Best-effort schema-evolution step for EXISTING spaces (created before
    # Entity had first_doc_id). Fail-open, same pattern as the RELATED
    # weight / er_canonical_name ALTERs.
    fake = _FakeSession()
    ensure_schema(fake)
    assert "ALTER TAG `Entity` ADD (first_doc_id string DEFAULT '');" in fake.statements


def test_ensure_schema_entity_probe_includes_first_doc_id():
    # The Entity write-readiness probe must cover first_doc_id too, so
    # ensure_schema waits for the ALTER to propagate before the first
    # ingest write touches it (same propagation-lag class as
    # er_canonical_name/RELATED.weight).
    fake = _FakeSession()
    ensure_schema(fake)
    joined = "\n".join(fake.statements)
    assert (
        "INSERT VERTEX `Entity` (name, description, mention_count, created_at, label, "
        "er_canonical_name, first_doc_id, " in joined
    )


def test_ensure_schema_probes_related_weighted_edge_write():
    # The RELATED weight column (fresh CREATE EDGE or the ALTER on an existing
    # space) propagates to storaged with a lag; ensure_schema must probe a
    # weighted edge-write until it lands so the first fail-open ingest batch
    # doesn't silently drop edges ("Unknown column weight").
    fake = _FakeSession()
    ensure_schema(fake)
    joined = "\n".join(fake.statements)
    assert ("INSERT EDGE `RELATED` (rel_type, polarity, valid_from, valid_to, weight, "
            "created_at, first_doc_id)") in joined
    assert "__kb_schema_probe_b__" in joined
    # both sentinel vertices are cleaned up WITH EDGE
    assert 'DELETE VERTEX "__kb_schema_probe_b__" WITH EDGE;' in fake.statements


def test_entity_ddl_has_wiki_columns():
    # Wiki-editor graph ops (nebula-wiki-ops design, Design.1): dirty-flag
    # bookkeeping + article metadata, mirrors er_canonical_name/first_doc_id.
    entity_ddl = next(
        s for s in SCHEMA_DDL if s.startswith("CREATE TAG IF NOT EXISTS `Entity`")
    )
    for col in (
        "wiki_dirty bool",
        "wiki_dirty_at int",
        "wiki_hash string",
        "wiki_synced_at int",
        "wiki_page_title string",
        "wikibase_qid string",
    ):
        assert col in entity_ddl, f"missing column: {col}"


def test_ensure_schema_alters_entity_to_add_wiki_columns():
    # Best-effort schema-evolution step for EXISTING spaces (created before
    # Entity had the wiki columns). Fail-open, same pattern as the
    # RELATED.weight / er_canonical_name / first_doc_id ALTERs.
    fake = _FakeSession()
    ensure_schema(fake)
    assert any(
        s.startswith("ALTER TAG `Entity` ADD (wiki_dirty") for s in fake.statements
    ), "ALTER TAG `Entity` ADD (wiki_dirty... missing from ensure_schema"


def test_ensure_schema_creates_wiki_dirty_index_after_entity_probe():
    # entity_wiki_dirty_idx is on the ALTER-added `wiki_dirty` column, so it is
    # NOT in SCHEMA_DDL (that would run before the ALTER on an existing space
    # and fail); ensure_schema creates it AFTER the Entity write-readiness
    # probe. Assert it's issued, and issued AFTER an Entity probe INSERT.
    fake = _FakeSession()
    ensure_schema(fake)
    idx_stmt = "CREATE TAG INDEX IF NOT EXISTS `entity_wiki_dirty_idx` ON `Entity`(wiki_dirty);"
    assert idx_stmt in fake.statements, "entity_wiki_dirty_idx not created by ensure_schema"
    idx_i = fake.statements.index(idx_stmt)
    entity_probe_i = next(
        i for i, s in enumerate(fake.statements) if s.startswith("INSERT VERTEX `Entity`")
    )
    assert entity_probe_i < idx_i, "wiki_dirty index must be created AFTER the Entity probe"


def test_ensure_schema_entity_probe_includes_wiki_columns():
    # The Entity write-readiness probe must grow to cover the 6 new wiki
    # columns too, so ensure_schema waits for the ALTER(s) to propagate
    # before the first wiki-sweep write touches them.
    fake = _FakeSession()
    ensure_schema(fake)
    joined = "\n".join(fake.statements)
    assert (
        "INSERT VERTEX `Entity` (name, description, mention_count, created_at, label, "
        "er_canonical_name, first_doc_id, wiki_dirty, wiki_dirty_at, wiki_hash, "
        "wiki_synced_at, wiki_page_title, wikibase_qid, "
        "event_type, event_ts_raw, event_start_epoch, event_end_epoch, event_ts_precision, "
        "pagerank, betweenness, eigenvector, risk_score, risk_band, risk_components, "
        "completeness_score, watched, risk_score_prev)"
        in joined
    )
