"""upsert_nodes/upsert_relations emit the expected nGQL (no live DB)."""
from __future__ import annotations

from types import SimpleNamespace

from src.graph.nebula_store import NebulaGraphStore, entity_vid


class _FakeSession:
    def __init__(self):
        self.executed = []

    def execute(self, stmt, *a, **k):
        self.executed.append(stmt)
        return SimpleNamespace(is_succeeded=lambda: True, error_msg=lambda: "")


def _store_with_session(sess):
    s = NebulaGraphStore.__new__(NebulaGraphStore)
    s._session = sess
    return s


def test_entity_vid_is_stable_int64():
    v = entity_vid("Иванов")
    assert isinstance(v, int)
    assert v == entity_vid("Иванов")
    assert -(2**63) <= v < 2**63


def test_upsert_nodes_inserts_entity_vertex():
    sess = _FakeSession()
    store = _store_with_session(sess)
    node = SimpleNamespace(name="Иванов", label="PERSON",
                           properties={"description": "d", "mention_count": 3})
    store.upsert_nodes([node])
    blob = "\n".join(sess.executed)
    assert "INSERT VERTEX `Entity`" in blob
    assert str(entity_vid("Иванов")) in blob
    assert "Иванов" in blob


def test_upsert_relations_inserts_edge():
    sess = _FakeSession()
    store = _store_with_session(sess)
    rel = SimpleNamespace(source_id="Иванов", target_id="Москва",
                          label="RELATED", properties={"polarity": "pos"})
    store.upsert_relations([rel])
    blob = "\n".join(sess.executed)
    assert "INSERT EDGE `RELATED`" in blob
    assert f"{entity_vid('Иванов')} -> {entity_vid('Москва')}" in blob


def test_upsert_relations_rejects_unsafe_label():
    sess = _FakeSession()
    store = _store_with_session(sess)
    unsafe_label = 'RELATED`; DROP SPACE'
    rel = SimpleNamespace(source_id="Иванов", target_id="Москва",
                          label=unsafe_label, properties={"polarity": "pos"})
    store.upsert_relations([rel])
    blob = "\n".join(sess.executed)
    assert "INSERT EDGE `RELATED`" in blob
    assert unsafe_label not in blob
    assert "DROP SPACE" not in blob


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
