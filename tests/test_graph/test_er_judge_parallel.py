import asyncio

from src.graph.entity_resolution import ERConfig, _llm_judge_pairs


class _Item:
    def __init__(self, name, label="Person", desc="d"):
        self.name, self.label, self.description = name, label, desc


class _SpyLLM:
    """Records max in-flight calls; returns all-SAME verdicts."""

    def __init__(self):
        self.inflight = 0
        self.max_inflight = 0

    async def achat(self, messages):
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        await asyncio.sleep(0.01)
        self.inflight -= 1
        body = messages[1].content
        n = body.count("Pair ")
        verdicts = ", ".join(
            f'{{"pair": {i + 1}, "verdict": "SAME"}}' for i in range(n)
        )

        class _Resp:
            class message:
                content = f"[{verdicts}]"

        return _Resp()


def test_batches_run_concurrently_and_preserve_order():
    pairs = [(_Item(f"A{i}"), _Item(f"B{i}")) for i in range(25)]
    cfg = ERConfig()  # judge_batch defaults to 10 -> 3 batches
    llm = _SpyLLM()
    verdicts = asyncio.run(_llm_judge_pairs(pairs, llm, cfg))
    assert len(verdicts) == 25
    assert all(verdicts)
    assert llm.max_inflight >= 2
