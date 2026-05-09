"""Stage-2 tests for the ingestion pipeline.

Goals:
  * ``read_documents`` loads a fixture file and returns at least one
    Document with non-empty text + metadata.
  * ``build_ingestion_pipeline`` (sentence-splitter mode) chunks a
    Document into one or more Nodes, each carrying ``file_path`` and
    ``file_type`` metadata propagated from the source Document.
  * Cache is created on disk when ``cache_dir`` is supplied.

Semantic-splitter mode is exercised by a separate test that uses
LlamaIndex's MockEmbedding so we don't depend on a running LiteLLM /
Ollama for unit tests.
"""

from __future__ import annotations

from pathlib import Path

from llama_index.core import MockEmbedding

from src.ingestion.pipeline import (
    build_ingestion_pipeline,
    read_documents,
)

FIXTURES = Path(__file__).parent / "fixtures"


def test_read_documents_loads_text_fixture() -> None:
    docs = read_documents(FIXTURES, required_exts=[".txt"])
    assert len(docs) >= 1
    sample = next(d for d in docs if "sample" in d.metadata.get("file_name", ""))
    assert sample.text.strip()
    assert sample.metadata["file_path"].endswith("sample.txt")


def test_sentence_splitter_pipeline_emits_chunks() -> None:
    docs = read_documents(FIXTURES, required_exts=[".txt"])
    pipeline = build_ingestion_pipeline()  # default = SentenceSplitter
    nodes = pipeline.run(documents=docs)

    assert len(nodes) >= 1
    for node in nodes:
        assert node.text.strip()
        assert node.metadata.get("file_path", "").endswith(".txt")
        # file_type is added by SimpleDirectoryReader
        assert node.metadata.get("file_type")


def test_semantic_splitter_pipeline_runs_with_mock_embedding() -> None:
    """Smoke: SemanticSplitterNodeParser doesn't crash with a stub
    embedding.  All-zero embeddings produce no semantic break, so the
    splitter typically returns one node — that's still a valid pass."""
    docs = read_documents(FIXTURES, required_exts=[".txt"])
    pipeline = build_ingestion_pipeline(
        embed_model=MockEmbedding(embed_dim=8),
        semantic=True,
    )
    nodes = pipeline.run(documents=docs)
    assert len(nodes) >= 1


def test_pipeline_cache_persists_to_disk(tmp_path: Path) -> None:
    docs = read_documents(FIXTURES, required_exts=[".txt"])
    cache_dir = tmp_path / "cache"
    pipeline = build_ingestion_pipeline(cache_dir=cache_dir)

    pipeline.run(documents=docs)
    pipeline.persist(persist_dir=str(cache_dir))

    assert cache_dir.exists()
    contents = list(cache_dir.iterdir())
    assert contents, f"cache dir empty after run: {contents}"


def test_extra_transformations_are_applied() -> None:
    """Confirm the hook used by Stage 7 (canonical-identifier
    transform) plugs in correctly."""
    from llama_index.core.schema import TransformComponent

    class _TagTransform(TransformComponent):
        def __call__(self, nodes, **kwargs):  # type: ignore[override]
            for n in nodes:
                n.metadata["stage7_tag"] = "applied"
            return nodes

    docs = read_documents(FIXTURES, required_exts=[".txt"])
    pipeline = build_ingestion_pipeline(
        extra_transformations=[_TagTransform()],
    )
    nodes = pipeline.run(documents=docs)
    assert all(n.metadata.get("stage7_tag") == "applied" for n in nodes)
