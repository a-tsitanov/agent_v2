"""upsert_nodes/upsert_relations emit the expected nGQL (no live DB)."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.config import settings
from src.graph.nebula_store import NebulaGraphStore, entity_vid


class _FakeSession:
    def __init__(self):
        self.executed = []

    def execute(self, stmt, *a, **k):
        self.executed.append(stmt)
        return SimpleNamespace(
            is_succeeded=lambda: True,
            error_msg=lambda: "",
            keys=lambda: [],
            row_size=lambda: 0,
        )


def _store_with_session(sess):
    s = NebulaGraphStore.__new__(NebulaGraphStore)
    s._session = sess
    return s


def test_entity_vid_is_stable_fixed_string():
    v = entity_vid("Иванов")
    assert isinstance(v, str)
    assert v == entity_vid("Иванов")
    assert len(v) == 32  # 128-bit blake2b hex digest -> FIXED_STRING(32)
    assert all(c in "0123456789abcdef" for c in v)


def test_upsert_nodes_inserts_entity_vertex():
    sess = _FakeSession()
    store = _store_with_session(sess)
    node = SimpleNamespace(name="Иванов", label="PERSON",
                           properties={"description": "d", "mention_count": 3})
    store.upsert_nodes([node])
    blob = "\n".join(sess.executed)
    assert "INSERT VERTEX `Entity`" in blob
    assert f'"{entity_vid("Иванов")}"' in blob  # quoted FIXED_STRING VID
    assert "Иванов" in blob


def test_upsert_relations_inserts_related_with_rel_type():
    sess = _FakeSession()
    store = _store_with_session(sess)
    rel = SimpleNamespace(source_id="Иванов", target_id="Москва",
                          label="WORKS_AT", properties={"polarity": "pos"})
    store.upsert_relations([rel])
    blob = "\n".join(sess.executed)
    assert "INSERT EDGE `RELATED`" in blob
    assert f'"{entity_vid("Иванов")}" -> "{entity_vid("Москва")}"' in blob
    assert '"WORKS_AT"' in blob            # original type preserved as rel_type value


def test_upsert_relations_rel_type_is_a_value_not_identifier():
    sess = _FakeSession()
    store = _store_with_session(sess)
    rel = SimpleNamespace(source_id="a", target_id="b",
                          label='X`; DROP SPACE', properties={})
    store.upsert_relations([rel])
    blob = "\n".join(sess.executed)
    assert "INSERT EDGE `RELATED`" in blob     # always RELATED edge type
    assert "INSERT EDGE `X" not in blob        # label never spliced as an edge-type identifier
    assert len(sess.executed) == 1             # a single statement — no injected 2nd statement


def test_q_escapes_newlines():
    sess = _FakeSession()
    store = _store_with_session(sess)
    node = SimpleNamespace(name="Иванов", label="PERSON",
                           properties={"description": "line1\nline2", "mention_count": 3})
    store.upsert_nodes([node])
    blob = "\n".join(sess.executed)
    assert "INSERT VERTEX" in blob
    assert "line1\\nline2" in blob
    assert "line1\nline2" not in blob


def test_structured_query_rejects_params():
    sess = _FakeSession()
    store = _store_with_session(sess)

    with pytest.raises(NotImplementedError):
        store.structured_query("MATCH ...", {"name": "X"})

    # None and empty dict are both "no params" and must pass through.
    store.structured_query("YIELD 1", {})
    store.structured_query("YIELD 1")
    assert sess.executed == ["YIELD 1", "YIELD 1"]


# --- write batching (multi-VALUES INSERT) -------------------------------


def _node(i: int) -> SimpleNamespace:
    return SimpleNamespace(
        name=f"Entity{i}",
        label="PERSON",
        properties={"description": f"desc{i}", "mention_count": i, "created_at": 1000 + i},
    )


def _rel(i: int) -> SimpleNamespace:
    return SimpleNamespace(
        source_id=f"Entity{i}",
        target_id=f"Entity{i + 1}",
        label="WORKS_WITH",
        properties={"polarity": "pos", "valid_from": 100 + i, "valid_to": 200 + i},
    )


def test_upsert_nodes_batches_into_multi_values_statements(monkeypatch):
    monkeypatch.setattr(settings.nebula, "write_batch_size", 2)
    sess = _FakeSession()
    store = _store_with_session(sess)
    nodes = [_node(i) for i in range(5)]

    store.upsert_nodes(nodes)

    assert len(sess.executed) == 3  # 2 + 2 + 1
    for stmt in sess.executed:
        assert stmt.startswith(
            "INSERT VERTEX `Entity` (name, description, mention_count, created_at, label) VALUES "
        )
    # first two statements have 2 comma-joined rows, last has 1
    assert sess.executed[0].count(":(") == 2
    assert sess.executed[1].count(":(") == 2
    assert sess.executed[2].count(":(") == 1
    # VID present and correctly quoted
    vid0 = entity_vid("Entity0")
    assert f'{_q_expect(vid0)}:(' in sess.executed[0]
    assert '"desc0"' in sess.executed[0]
    assert '"PERSON"' in sess.executed[0]


def _q_expect(value: str) -> str:
    return f'"{value}"'


def test_upsert_relations_batches_into_multi_values_statements(monkeypatch):
    monkeypatch.setattr(settings.nebula, "write_batch_size", 2)
    sess = _FakeSession()
    store = _store_with_session(sess)
    rels = [_rel(i) for i in range(5)]

    store.upsert_relations(rels)

    assert len(sess.executed) == 3  # 2 + 2 + 1
    for stmt in sess.executed:
        assert stmt.startswith(
            "INSERT EDGE `RELATED` (rel_type, polarity, valid_from, valid_to) VALUES "
        )
    assert sess.executed[0].count(" -> ") == 2
    assert sess.executed[1].count(" -> ") == 2
    assert sess.executed[2].count(" -> ") == 1
    src0, tgt0 = entity_vid("Entity0"), entity_vid("Entity1")
    assert f'"{src0}" -> "{tgt0}"' in sess.executed[0]
    assert '"WORKS_WITH"' in sess.executed[0]


def test_upsert_nodes_empty_list_emits_no_statements():
    sess = _FakeSession()
    store = _store_with_session(sess)
    store.upsert_nodes([])
    assert sess.executed == []


def test_upsert_relations_empty_list_emits_no_statements():
    sess = _FakeSession()
    store = _store_with_session(sess)
    store.upsert_relations([])
    assert sess.executed == []


def test_upsert_nodes_batch_size_ge_len_emits_one_statement(monkeypatch):
    monkeypatch.setattr(settings.nebula, "write_batch_size", 100)
    sess = _FakeSession()
    store = _store_with_session(sess)
    nodes = [_node(i) for i in range(5)]
    store.upsert_nodes(nodes)
    assert len(sess.executed) == 1
    assert sess.executed[0].count(":(") == 5


def test_upsert_relations_batch_size_ge_len_emits_one_statement(monkeypatch):
    monkeypatch.setattr(settings.nebula, "write_batch_size", 100)
    sess = _FakeSession()
    store = _store_with_session(sess)
    rels = [_rel(i) for i in range(5)]
    store.upsert_relations(rels)
    assert len(sess.executed) == 1
    assert sess.executed[0].count(" -> ") == 5
