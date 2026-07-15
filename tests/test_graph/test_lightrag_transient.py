"""Transient LLM-backend errors must NOT be swallowed as empty extraction.

A litellm→ollama (behind nginx) timeout/504 is retryable: the doc DID have
content, the request just failed. Committing empty triplets silently loses data.
So the extractor re-raises transient errors (→ Temporal retries the activity);
only genuine/non-transient failures fail-open to empty."""
from __future__ import annotations

import pytest
from llama_index.core.graph_stores.types import KG_NODES_KEY, KG_RELATIONS_KEY
from llama_index.core.schema import TextNode

from src.graph.lightrag_extract import LightRAGExtractor, _is_transient_llm_error


class _TransientError(Exception):
    def __init__(self, msg, status_code=None):
        super().__init__(msg)
        self.status_code = status_code


@pytest.mark.parametrize("exc, expected", [
    (TimeoutError("Read timed out"), True),
    (_TransientError("litellm.APIConnectionError: 504 Gateway Time-out"), True),
    (_TransientError("Internal Server Error", status_code=500), True),
    (_TransientError("Service Unavailable", status_code=503), True),
    (RuntimeError("boom"), False),
    (ValueError("bad json"), False),
    (_TransientError("400 Bad Request", status_code=400), False),
])
def test_is_transient_llm_error(exc, expected):
    assert _is_transient_llm_error(exc) is expected


@pytest.mark.asyncio
async def test_transient_error_raises_for_retry_not_empty():
    class _TimeoutLLM:
        async def achat(self, *a, **kw):
            raise TimeoutError("Read timed out")

    extractor = LightRAGExtractor(llm=_TimeoutLLM(), num_workers=1)
    with pytest.raises(TimeoutError):  # re-raised → activity fails → Temporal retries
        await extractor.acall([TextNode(id_="cx", text="t")])


@pytest.mark.asyncio
async def test_non_transient_error_still_fails_open_to_empty():
    class _BugLLM:
        async def achat(self, *a, **kw):
            raise RuntimeError("boom")  # not transient — a bug, not a backend blip

    extractor = LightRAGExtractor(llm=_BugLLM(), num_workers=1)
    out = await extractor.acall([TextNode(id_="cy", text="t")])
    assert out[0].metadata[KG_NODES_KEY] == []
    assert out[0].metadata[KG_RELATIONS_KEY] == []
