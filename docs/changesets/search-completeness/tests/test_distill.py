"""Real test of distill_observation against litellm → ollama gemma4:e4b.

Verifies: (1) a large observation is compacted, (2) relevance verdict
is returned, (3) an off-topic observation is graded irrelevant.
Runs the actual Temporal activity via ActivityEnvironment.
"""

import asyncio
import json
import os

os.environ["LITELLM_BASE_URL"] = "http://localhost:4000"
os.environ["LITELLM_LLM_MODEL"] = "gemma4:e4b"
os.environ["LITELLM_API_KEY"] = "sk-litellm-stub"
os.environ["LITELLM_FUNCTION_CALLING"] = "true"

from temporalio.testing import ActivityEnvironment  # noqa: E402

from src.workflow.activities.distill_observation import (  # noqa: E402
    distill_observation,
)
from src.workflow.contracts import DistillParams  # noqa: E402

# A big graph_search-style observation: a couple of relevant facts
# buried in lots of noise, padded to exceed distill_min_chars.
NOISE = [
    {"name": f"Сущность №{i}", "entity_type": "misc",
     "description": "нерелевантная запись для объёма " * 4}
    for i in range(40)
]
BIG_OBS = json.dumps({
    "entities": [
        {"name": "ООО Ромашка", "entity_type": "organization",
         "description": "поставщик логистических услуг"},
        {"name": "Иванов Иван Иванович", "entity_type": "person",
         "description": "генеральный директор ООО Ромашка"},
        *NOISE,
    ],
    "relations": [
        {"source": "Иванов Иван Иванович", "target": "ООО Ромашка",
         "label": "директор"},
    ],
}, ensure_ascii=False)


async def run(query, obs):
    env = ActivityEnvironment()
    return await env.run(
        distill_observation,
        DistillParams(query=query, tool_name="graph_search", observation=obs),
    )


async def main():
    print(f"input observation: {len(BIG_OBS)} chars")

    print("\n=== relevant query ===")
    r = await run("Кто директор ООО Ромашка?", BIG_OBS)
    print(f"relevance={r.relevance}  distilled {len(r.distilled)} chars")
    print("distilled:\n", r.distilled[:600])

    print("\n=== off-topic query (expect irrelevant/partial) ===")
    r2 = await run("Какая погода в Москве завтра?", BIG_OBS)
    print(f"relevance={r2.relevance}  distilled {len(r2.distilled)} chars")
    print("distilled:\n", r2.distilled[:300])


if __name__ == "__main__":
    asyncio.run(main())
