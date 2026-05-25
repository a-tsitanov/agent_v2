from unittest.mock import MagicMock
from llama_index.core.schema import TextNode
from llama_index.core.graph_stores.types import KG_NODES_KEY
from src.graph.gliner_extract import GLiNERExtractor


def _fake_model(spans):
    m = MagicMock()
    m.predict_entities.return_value = spans
    return m


def test_populates_kg_nodes_from_spans():
    model = _fake_model([
        {"text": "Иванов И.П.", "label": "Person", "score": 0.97},
        {"text": "ООО Ромашка", "label": "Organization", "score": 0.91},
    ])
    ext = GLiNERExtractor(model=model, entity_types=["Person", "Organization"])
    node = TextNode(text="Иванов И.П. из ООО Ромашка")
    out = ext([node])
    ents = out[0].metadata[KG_NODES_KEY]
    names = {(e.name, e.label) for e in ents}
    assert ("Иванов И.П.", "Person") in names
    assert ("ООО Ромашка", "Organization") in names


def test_score_threshold_drops_low_confidence():
    model = _fake_model([
        {"text": "Maybe", "label": "Person", "score": 0.30},
    ])
    ext = GLiNERExtractor(model=model, entity_types=["Person"], threshold=0.5)
    out = ext([TextNode(text="Maybe something")])
    assert out[0].metadata[KG_NODES_KEY] == []
