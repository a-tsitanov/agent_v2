"""NebulaGraphStore.subgraph maps GET SUBGRAPH results into _map_walk_rows shape."""
from __future__ import annotations

from src.graph.nebula_store import NebulaGraphStore


class _VW:  # ValueWrapper stub
    def __init__(self, v): self._v = v
    def cast(self): return self._v

class _Node:
    def __init__(self, vid, props): self._vid, self._props = vid, props
    def get_id(self): return _VW(self._vid)
    def tags(self): return ["Entity"]
    def properties(self, tag): return {k: _VW(v) for k, v in self._props.items()}

class _Rel:
    def __init__(self, s, t, props): self._s, self._t, self._props = s, t, props
    def start_vertex_id(self): return _VW(self._s)
    def end_vertex_id(self): return _VW(self._t)
    def edge_name(self): return "RELATED"
    def properties(self): return {k: _VW(v) for k, v in self._props.items()}

class _Cell:  # a VERTICES/EDGES column value -> .as_list() of element wrappers
    def __init__(self, items, kind): self._items, self._kind = items, kind
    def as_list(self): return [_Elem(x, self._kind) for x in self._items]

class _Elem:
    def __init__(self, obj, kind): self._obj, self._kind = obj, kind
    def as_node(self): return self._obj
    def as_relationship(self): return self._obj

class _ResultSet:
    def __init__(self, rows): self._rows = rows  # rows: list[(nodes_cell, rels_cell)]
    def is_succeeded(self): return True
    def error_msg(self): return ""
    def keys(self): return ["nodes", "rels"]
    def row_size(self): return len(self._rows)
    def row_values(self, i): return list(self._rows[i])

class _Session:
    def __init__(self, rs): self._rs = rs
    def execute(self, q):
        self.last = q
        return self._rs


def test_subgraph_maps_to_walk_rows_shape():
    ivan = _Node("v_ivan", {"name": "Иванов", "label": "PERSON", "description": "инженер"})
    mosk = _Node("v_mosk", {"name": "Москва", "label": "CITY", "description": "город"})
    edge = _Rel("v_ivan", "v_mosk",
                {"rel_type": "WORKS_AT", "polarity": "pos", "valid_from": 0, "valid_to": 0})
    rs = _ResultSet([(_Cell([ivan], "n"), _Cell([edge], "e")),
                     (_Cell([mosk], "n"), _Cell([], "e"))])
    store = NebulaGraphStore(_Session(rs))
    rows = store.subgraph("v_ivan", 2)
    assert len(rows) == 1
    ents = {e["name"] for e in rows[0]["entities"]}
    assert ents == {"Иванов", "Москва"}
    rels = rows[0]["relations"]
    assert rels == [{"src": "Иванов", "tgt": "Москва", "label": "WORKS_AT",
                     "polarity": "pos", "valid_from": 0, "valid_to": 0}]
    assert 'GET SUBGRAPH WITH PROP 2 STEPS FROM "v_ivan" BOTH `RELATED`' in store._session.last
