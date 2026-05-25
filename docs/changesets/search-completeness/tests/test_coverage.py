"""Real test of coverage_check against litellm → ollama gemma4:e4b.

Verifies the pre-submit completeness gate: a multi-part question with
only partial evidence is flagged incomplete with a named gap; the same
question with full evidence passes; empty evidence is incomplete.
"""

import asyncio
import os

os.environ["LITELLM_BASE_URL"] = "http://localhost:4000"
os.environ["LITELLM_LLM_MODEL"] = "gemma4:e4b"
os.environ["LITELLM_API_KEY"] = "sk-litellm-stub"
os.environ["LITELLM_FUNCTION_CALLING"] = "true"

from temporalio.testing import ActivityEnvironment  # noqa: E402

from src.workflow.activities.coverage_check import coverage_check  # noqa: E402
from src.workflow.contracts import CoverageParams  # noqa: E402

QUERY = (
    "Кто директор ООО Ромашка и какие договоры связывают её с ООО Лютик?"
)


async def run(evidence):
    env = ActivityEnvironment()
    return await env.run(
        coverage_check, CoverageParams(query=QUERY, evidence=evidence),
    )


async def main():
    print("=== partial evidence (director only, no contracts) ===")
    r = await run("[graph_search] * Иванов И.И. — директор ООО Ромашка")
    print(f"complete={r.complete}  missing={r.missing!r}")

    print("\n=== full evidence (director + contracts) ===")
    r2 = await run(
        "[graph_search] * Иванов И.И. — директор ООО Ромашка\n"
        "[vector_search] * Договор поставки №42 от 2024 между ООО Ромашка "
        "и ООО Лютик\n* Договор аренды №7 между ООО Ромашка и ООО Лютик"
    )
    print(f"complete={r2.complete}  missing={r2.missing!r}")

    print("\n=== empty evidence ===")
    r3 = await run("")
    print(f"complete={r3.complete}  missing={r3.missing!r}")


if __name__ == "__main__":
    asyncio.run(main())
