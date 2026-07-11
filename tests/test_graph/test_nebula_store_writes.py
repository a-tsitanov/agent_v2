"""upsert_nodes/upsert_relations emit the expected nGQL (no live DB)."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.config import settings
from src.graph.nebula_store import NebulaGraphStore, entity_vid


class _Cast:
    """Wraps a plain python value with a nebula-ValueWrapper-like .cast()."""

    def __init__(self, value):
        self._value = value

    def cast(self):
        return self._value


class _FakeSession:
    """Records executed statements. Also answers `FETCH PROP ON `Entity``
    read-backs (used by upsert_nodes' created_at/first_doc_id preserve
    logic) with canned rows, or a failure response when fetch_fails=True."""

    def __init__(self, fetch_rows: list[dict] | None = None, fetch_fails: bool = False):
        self.executed = []
        self._fetch_rows = fetch_rows if fetch_rows is not None else []
        self._fetch_fails = fetch_fails

    def execute(self, stmt, *a, **k):
        self.executed.append(stmt)
        if "FETCH PROP ON `Entity`" in stmt:
            if self._fetch_fails:
                return SimpleNamespace(
                    is_succeeded=lambda: False,
                    error_msg=lambda: "fetch boom",
                    keys=lambda: [],
                    row_size=lambda: 0,
                )
            cols = ["vid", "ca", "fdi"]
            rows = self._fetch_rows
            return SimpleNamespace(
                is_succeeded=lambda: True,
                error_msg=lambda: "",
                keys=lambda: cols,
                row_size=lambda: len(rows),
                row_values=lambda i, _rows=rows, _cols=cols: [
                    _Cast(_rows[i][c]) for c in _cols
                ],
            )
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


def test_upsert_nodes_writes_er_canonical_name_from_properties():
    sess = _FakeSession()
    store = _store_with_session(sess)
    node = SimpleNamespace(name="Иванов", label="PERSON",
                           properties={"description": "d", "mention_count": 3,
                                       "er_canonical_name": "Canon"})
    store.upsert_nodes([node])
    blob = "\n".join(sess.executed)
    assert (
        "INSERT VERTEX `Entity` (name, description, mention_count, created_at, label, "
        "er_canonical_name, first_doc_id) VALUES" in blob
    )
    assert '"Canon"' in blob


def test_upsert_nodes_defaults_er_canonical_name_to_empty_when_absent():
    sess = _FakeSession()
    store = _store_with_session(sess)
    node = SimpleNamespace(name="Иванов", label="PERSON",
                           properties={"description": "d", "mention_count": 3})
    store.upsert_nodes([node])
    blob = "\n".join(sess.executed)
    assert blob.rstrip(";").endswith('"")')


# --- created_at/first_doc_id read-back preserve (first-seen) -----------


def test_upsert_nodes_preserves_created_at_and_first_doc_id_for_existing_entity():
    vid_a = entity_vid("A")
    sess = _FakeSession(fetch_rows=[{"vid": vid_a, "ca": 111, "fdi": "d0"}])
    store = _store_with_session(sess)
    node = SimpleNamespace(
        name="A", label="PERSON",
        properties={"created_at": 999, "first_doc_id": "dNEW", "description": "newdesc"},
    )

    store.upsert_nodes([node])

    inserts = [s for s in sess.executed if s.startswith("INSERT VERTEX")]
    assert len(inserts) == 1
    # created_at/first_doc_id come from the read-back (111/"d0"), NOT props
    # (999/"dNEW"); description still overwrites from props ("newdesc").
    expected_row = f'{_q_expect(vid_a)}:("A", "newdesc", 0, 111, "PERSON", "", "d0")'
    assert expected_row in inserts[0]
    assert "999" not in inserts[0]
    assert "dNEW" not in inserts[0]


def test_upsert_nodes_uses_props_for_new_entity_when_not_in_readback():
    vid_b = entity_vid("B")
    sess = _FakeSession(fetch_rows=[])  # empty read-back -> B is new
    store = _store_with_session(sess)
    node = SimpleNamespace(
        name="B", label="ORG",
        properties={"created_at": 7, "first_doc_id": "dB", "description": "d"},
    )

    store.upsert_nodes([node])

    inserts = [s for s in sess.executed if s.startswith("INSERT VERTEX")]
    assert len(inserts) == 1
    expected_row = f'{_q_expect(vid_b)}:("B", "d", 0, 7, "ORG", "", "dB")'
    assert expected_row in inserts[0]


def test_upsert_nodes_readback_failure_treats_all_as_new_and_still_inserts():
    vid_c = entity_vid("C")
    sess = _FakeSession(fetch_fails=True)
    store = _store_with_session(sess)
    node = SimpleNamespace(
        name="C", label="PERSON",
        properties={"created_at": 42, "first_doc_id": "dC", "description": "d"},
    )

    store.upsert_nodes([node])  # must not raise despite the FETCH failure

    fetch_attempts = [s for s in sess.executed if "FETCH PROP ON `Entity`" in s]
    assert len(fetch_attempts) == 1  # read-back was attempted, then failed open

    inserts = [s for s in sess.executed if s.startswith("INSERT VERTEX")]
    assert len(inserts) == 1  # INSERT still issued (fail-open, no crash)
    expected_row = f'{_q_expect(vid_c)}:("C", "d", 0, 42, "PERSON", "", "dC")'
    assert expected_row in inserts[0]


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


def test_upsert_relations_writes_weight_from_properties():
    sess = _FakeSession()
    store = _store_with_session(sess)
    rel = SimpleNamespace(source_id="a", target_id="b", label="WORKS_AT",
                          properties={"weight": 7.0})
    store.upsert_relations([rel])
    blob = "\n".join(sess.executed)
    assert "INSERT EDGE `RELATED` (rel_type, polarity, valid_from, valid_to, weight) VALUES" in blob
    assert blob.rstrip(";").endswith("7.0)")


def test_upsert_relations_defaults_weight_to_one_when_absent():
    sess = _FakeSession()
    store = _store_with_session(sess)
    rel = SimpleNamespace(source_id="a", target_id="b", label="WORKS_AT", properties={})
    store.upsert_relations([rel])
    blob = "\n".join(sess.executed)
    assert blob.rstrip(";").endswith("1.0)")


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
        properties={
            "polarity": "pos", "valid_from": 100 + i, "valid_to": 200 + i,
            "weight": float(i) + 1.0,
        },
    )


def test_upsert_nodes_batches_into_multi_values_statements(monkeypatch):
    monkeypatch.setattr(settings.nebula, "write_batch_size", 2)
    sess = _FakeSession()
    store = _store_with_session(sess)
    nodes = [_node(i) for i in range(5)]

    store.upsert_nodes(nodes)

    inserts = [s for s in sess.executed if s.startswith("INSERT VERTEX")]
    fetches = [s for s in sess.executed if "FETCH PROP ON `Entity`" in s]
    assert len(inserts) == 3  # 2 + 2 + 1
    assert len(fetches) == 3  # one read-back per chunk
    for stmt in inserts:
        assert stmt.startswith(
            "INSERT VERTEX `Entity` (name, description, mention_count, created_at, label, "
            "er_canonical_name, first_doc_id) VALUES "
        )
    # first two statements have 2 comma-joined rows, last has 1
    assert inserts[0].count(":(") == 2
    assert inserts[1].count(":(") == 2
    assert inserts[2].count(":(") == 1
    # VID present and correctly quoted
    vid0 = entity_vid("Entity0")
    assert f'{_q_expect(vid0)}:(' in inserts[0]
    assert '"desc0"' in inserts[0]
    assert '"PERSON"' in inserts[0]


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
            "INSERT EDGE `RELATED` (rel_type, polarity, valid_from, valid_to, weight) VALUES "
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
    inserts = [s for s in sess.executed if s.startswith("INSERT VERTEX")]
    assert len(inserts) == 1
    assert inserts[0].count(":(") == 5


def test_upsert_relations_batch_size_ge_len_emits_one_statement(monkeypatch):
    monkeypatch.setattr(settings.nebula, "write_batch_size", 100)
    sess = _FakeSession()
    store = _store_with_session(sess)
    rels = [_rel(i) for i in range(5)]
    store.upsert_relations(rels)
    assert len(sess.executed) == 1
    assert sess.executed[0].count(" -> ") == 5
