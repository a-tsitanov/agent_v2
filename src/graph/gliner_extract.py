"""Optional GLiNER span-based entity extractor as a LlamaIndex
``TransformComponent``.

GLiNER is an encoder NER model that detects entity SPANS for an
arbitrary set of type labels supplied at inference time — no relations,
no descriptions.  This extractor is OPT-IN: it is registered as the
``gliner`` / ``gliner+llm`` modes in
``src/graph/index.py:build_kg_extractor`` but is NOT a default.

It populates ``KG_NODES_KEY`` on each input node with ``EntityNode``
objects (mirroring ``LightRAGExtractor``'s contract), so a
``NoOpKGExtractor`` downstream can persist/embed them identically.
Entities carry an empty ``description`` (GLiNER yields none) plus the
raw ``gliner_score`` for debugging / downstream filtering.

The ``gliner`` package is heavy and is only imported lazily inside the
constructor when building from a ``model_name`` — module import stays
free of the dependency so unit tests (which inject a mock ``model``)
never need it installed.
"""

from __future__ import annotations

from typing import Any

from llama_index.core.graph_stores.types import KG_NODES_KEY, EntityNode
from llama_index.core.schema import BaseNode, TransformComponent
from pydantic import ConfigDict, Field

from src.graph.lightrag_parse import _normalize_entity_name


def _default_entity_types() -> list[str]:
    """Pull entity-type strings out of ``src.graph.schema.EntityType``."""
    from typing import get_args

    from src.graph.schema import EntityType

    return list(get_args(EntityType))


class GLiNERExtractor(TransformComponent):
    """Per-chunk span NER extractor.

    Output: ``node.metadata[KG_NODES_KEY]`` — list[EntityNode], one per
    deduplicated detected span at/above ``threshold``.  No relations are
    produced (GLiNER does not emit them).

    Stub-test friendly: ``model`` is held as an arbitrary object and
    only needs a ``predict_entities(text, labels, threshold=...)`` method
    returning a list of ``{"text", "label", "score"}`` dicts.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    model: Any = None
    entity_types: list[str] = Field(default_factory=_default_entity_types)
    threshold: float = 0.5

    def __init__(
        self,
        model: Any = None,
        entity_types: list[str] | None = None,
        threshold: float = 0.5,
        model_name: str | None = None,
    ) -> None:
        types = entity_types or _default_entity_types()
        if model is None and model_name is not None:  # pragma: no cover
            from src.graph.gliner_extract import _load_gliner

            model = _load_gliner(model_name)
        super().__init__(model=model, entity_types=types, threshold=threshold)

    # ── TransformComponent contract ─────────────────────────────────

    def __call__(
        self, nodes: list[BaseNode], **kwargs: Any,
    ) -> list[BaseNode]:
        for node in nodes:
            spans = self.model.predict_entities(
                node.get_content(),
                self.entity_types,
                threshold=self.threshold,
            )
            seen: set[str] = set()
            ents: list[EntityNode] = []
            for sp in spans:
                score = sp.get("score", 1.0)
                if score < self.threshold:
                    continue
                name = _normalize_entity_name(sp["text"])
                key = name.casefold()
                if not name or key in seen:
                    continue
                seen.add(key)
                ents.append(
                    EntityNode(
                        name=name,
                        label=sp["label"],
                        properties={"description": "", "gliner_score": score},
                    )
                )
            node.metadata[KG_NODES_KEY] = ents
        return nodes

    async def acall(
        self, nodes: list[BaseNode], **kwargs: Any,
    ) -> list[BaseNode]:
        # GLiNER inference is synchronous CPU/GPU work — no async path.
        return self.__call__(nodes, **kwargs)


def _load_gliner(model_name: str):  # pragma: no cover
    """Load a real GLiNER model, honouring the offline HF cache.

    ``configure_hf()`` runs first (sets HF cache dir / offline env vars
    BEFORE gliner imports transformers + loads weights).  When
    ``settings.hf.offline`` we also pass ``local_files_only=True`` to
    ``from_pretrained`` as belt-and-suspenders; if a given gliner build
    rejects that kwarg we fall back to a plain call (the env var still
    forces offline).
    """
    from src.config import settings
    from src.retrieval.hf_offline import configure_hf

    configure_hf()
    from gliner import GLiNER

    if settings.hf.offline:
        try:
            return GLiNER.from_pretrained(model_name, local_files_only=True)
        except TypeError:
            # Older gliner builds without the kwarg — env var still
            # forces offline.
            return GLiNER.from_pretrained(model_name)
    return GLiNER.from_pretrained(model_name)


def gliner_ner_callable(model_name: str | None = None):  # pragma: no cover
    """Build a plain ``(text, types) -> list[(name, label)]`` callable
    backed by a real GLiNER model.

    Used where a lightweight NER function (not a TransformComponent) is
    wanted — e.g. canonical-linking probes.  Lazy-imports gliner.
    """
    from src.config import settings

    name = model_name or settings.ingestion.gliner_model
    model = _load_gliner(name)

    def _run(text: str, types: list[str]):
        spans = model.predict_entities(text, types, threshold=0.5)
        return [(s["text"], s["label"]) for s in spans]

    return _run


__all__ = ["GLiNERExtractor", "_load_gliner", "gliner_ner_callable"]
