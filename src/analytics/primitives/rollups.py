"""Arc 1 — numeric rollups over Amount identifier entities (mini-OLAP). Parsing is pure."""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.analytics.catalog import Primitive, PrimitiveResult, register
from src.analytics.ids import clamp_top_n
from src.analytics.store_query import run_rows

_NUM = re.compile(r"-?\d[\d  .,]*")


def parse_amount(s: str) -> float | None:
    if not s:
        return None
    m = _NUM.search(s)
    if not m:
        return None
    tok = m.group(0).replace(" ", "").replace(" ", "")
    # if both separators present, treat ',' as thousands; else ',' as decimal
    if "," in tok and "." in tok:
        tok = tok.replace(",", "")
    elif "," in tok:
        tok = tok.replace(",", ".")
    try:
        return float(tok)
    except ValueError:
        return None


class _Params(BaseModel):
    model_config = ConfigDict(extra="ignore")


class NumericRollupParams(_Params):
    counterparty: str | None = None
    top_n: int = 20


async def numeric_rollup(
    store: Any | None, *, counterparty: str | None = None, top_n: int = 20
) -> PrimitiveResult:
    top_n = clamp_top_n(top_n)
    cypher = (
        "MATCH (e:__Entity__)-[]-(a:__Entity__:Amount) "
        "WHERE ($cp IS NULL OR e.name=$cp) "
        "RETURN e.name AS counterparty, a.name AS amount"
    )
    params = {"cp": counterparty}
    raw = await run_rows(store, cypher, params)
    agg: dict[str, dict] = defaultdict(lambda: {"total": 0.0, "count": 0})
    for r in raw:
        v = parse_amount(str(r.get("amount", "")))
        if v is None:
            continue
        cp = r.get("counterparty")
        agg[cp]["total"] += v
        agg[cp]["count"] += 1
    rows = [
        {"counterparty": k, "total": round(v["total"], 2), "count": v["count"]}
        for k, v in agg.items()
    ]
    rows.sort(key=lambda x: x["total"], reverse=True)
    return PrimitiveResult(cypher=cypher, params=params, rows=rows[:top_n])


register(
    Primitive(
        "numeric_rollup",
        numeric_rollup,
        NumericRollupParams,
        "Sum/count of Amount values per counterparty (mini-OLAP over identifier amounts).",
    )
)
