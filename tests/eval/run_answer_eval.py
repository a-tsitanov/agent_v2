"""R9 CLI — run the 15 golden Q&A through the live search endpoints.

R7b cutover: the legacy ReAct endpoints were removed; this harness now
probes ``/api/v1/search/{local,global,drift,auto}``.

Assumes:
  * API server is up at `--api-url` (default http://localhost:8000),
  * corpus is already ingested (the same docs the golden cases
    refer to), and
  * `API_KEYS` env var includes the value passed as `--api-key`.

Iterates over every case × every endpoint, hits the API, scores
the response via `answer_quality.score_case`, then prints a
per-endpoint × per-doc-type table and (optionally) writes the
raw scores to JSON.

Usage::

    python -m tests.eval.run_answer_eval                         # informative
    python -m tests.eval.run_answer_eval --strict                # CI mode
    python -m tests.eval.run_answer_eval --json-out out.json
    python -m tests.eval.run_answer_eval --endpoints local,auto

Out-of-scope: ingestion of the corpus, model loading, prom-pulling.
The runner just hits the API.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import httpx  # noqa: E402

from tests.eval.answer_quality import (  # noqa: E402
    GOLDEN_DIR_DEFAULT,
    THRESHOLDS,
    CaseScore,
    aggregate_by,
    check_thresholds,
    load_golden_cases,
    score_case,
)
from tests.eval.medical_fixture import (  # noqa: E402
    KNOWN_QUESTION_TYPES,
    load_medical_golden_cases,
)


# R7b cutover: the legacy ReAct endpoints (/search, /agent, /selfrag)
# and the judge-based /legacy/agent baseline were removed.  The eval now
# probes the plan-execute / GraphRAG surface.
ENDPOINT_MAP = {
    "local": "/api/v1/search/local",
    "global": "/api/v1/search/global",
    "drift": "/api/v1/search/drift",
    "auto": "/api/v1/search/auto",
}


async def _hit_endpoint(
    client: httpx.AsyncClient, endpoint: str, query: str, api_key: str,
    timeout_s: float = 120.0,
) -> dict | None:
    """Returns parsed JSON or None on failure (logged)."""
    path = ENDPOINT_MAP[endpoint]
    try:
        resp = await client.post(
            path,
            json={"query": query},
            headers={"X-API-Key": api_key},
            timeout=timeout_s,
        )
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as exc:
        print(
            f"  [WARN] {endpoint} returned {exc.response.status_code}: "
            f"{exc.response.text[:200]}", file=sys.stderr,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"  [WARN] {endpoint} call failed: {exc}", file=sys.stderr)
    return None


def _score_response(case, *, endpoint: str, response: dict | None) -> CaseScore:
    if response is None:
        # Treat an endpoint failure as the worst score so violations
        # surface — never silently skip.
        return CaseScore(
            case_id=case.id,
            doc_type=case.doc_type,
            category=case.category,
            endpoint=endpoint,
            fact_recall=0.0,
            entity_recall=0.0,
            citation_precision=0.0,
            hallucination_rate=1.0,
            uncertainty_honesty=False,
        )
    return score_case(
        case,
        endpoint=endpoint,
        answer_text=response.get("answer", ""),
        sources=response.get("sources") or [],
        citations=(response.get("answer_detail") or {}).get("citations"),
    )


async def run(args: argparse.Namespace) -> int:
    cases = [] if args.no_golden else list(load_golden_cases(args.golden))
    if args.medical_sample:
        qt_filter = (
            {t.strip() for t in args.medical_types.split(",") if t.strip()}
            if args.medical_types else None
        )
        if qt_filter and not qt_filter.issubset(KNOWN_QUESTION_TYPES):
            unknown = qt_filter - KNOWN_QUESTION_TYPES
            raise ValueError(
                f"unknown medical question_type(s): {unknown}.  "
                f"valid: {sorted(KNOWN_QUESTION_TYPES)}"
            )
        medical = load_medical_golden_cases(
            limit=args.medical_sample,
            question_types=qt_filter,
            sample_seed=args.medical_seed,
        )
        print(f"  + {len(medical)} medical cases "
              f"(seed={args.medical_seed}, types={qt_filter or 'all'})")
        cases = list(cases) + medical
    endpoints = [e.strip() for e in args.endpoints.split(",") if e.strip()]

    print(f"Running {len(cases)} cases × {len(endpoints)} endpoints "
          f"→ {len(cases) * len(endpoints)} calls\n",
          flush=True)

    scores: list[CaseScore] = []
    raw_responses: list[dict] = []
    async with httpx.AsyncClient(base_url=args.api_url) as client:
        for i, case in enumerate(cases, 1):
            print(f"  [{i}/{len(cases)}] {case.id} ({case.doc_type}/{case.category})"
                  f": {case.query[:80]}", flush=True)
            for ep in endpoints:
                t_ep = time.monotonic()
                # `local` is the leanest path (plan → parallel retrieve →
                # one synth).  global/drift/auto chain a map-reduce and/or
                # an extra orchestration pass, so they get the larger
                # agentic budget on CPU-bound stacks.
                per_ep_timeout = (
                    args.search_timeout if ep == "local"
                    else args.agentic_timeout
                )
                resp = await _hit_endpoint(
                    client, ep, case.query, args.api_key,
                    timeout_s=per_ep_timeout,
                )
                dt = time.monotonic() - t_ep
                ok = "ok" if resp is not None else "fail"
                print(f"        {ep:8s} {ok:4s}  {dt:6.1f}s", flush=True)
                scores.append(_score_response(case, endpoint=ep, response=resp))
                # Persist the raw answer + sources so we can diff
                # endpoints on the same query and inspect failures
                # post-hoc without re-running.
                raw_responses.append({
                    "case_id": case.id,
                    "endpoint": ep,
                    "elapsed_s": round(dt, 2),
                    "answer": (resp or {}).get("answer", "")[:2000],
                    # Keep source content (truncated) so the
                    # hallucination heuristic survives offline
                    # rescoring without re-hitting the API.
                    "sources": [
                        {"chunk_id": s.get("chunk_id"),
                         "content": (s.get("content") or "")[:1500]}
                        for s in ((resp or {}).get("sources") or [])
                    ],
                    "agentic_steps": [
                        {"step": s.get("step"), "tool": s.get("tool_name"),
                         "args": s.get("tool_args")}
                        for s in ((resp or {}).get("agentic_step_stats") or [])
                    ],
                    "answer_detail": (resp or {}).get("answer_detail"),
                })

    # ── aggregations ────────────────────────────────────────────────
    by_endpoint = aggregate_by(scores, "endpoint")
    print("\n=== by endpoint ===")
    print(_render_table(by_endpoint))

    by_doc_type = aggregate_by(scores, "doc_type")
    print("\n=== by doc_type ===")
    print(_render_table(by_doc_type))

    by_category = aggregate_by(scores, "category")
    print("\n=== by category ===")
    print(_render_table(by_category))

    by_endpoint_and_doc: dict[str, dict[str, dict[str, float]]] = {}
    for ep in endpoints:
        ep_scores = [s for s in scores if s.endpoint == ep]
        by_endpoint_and_doc[ep] = aggregate_by(ep_scores, "doc_type")

    print("\n=== thresholds ===")
    violations = check_thresholds(by_endpoint_and_doc)
    if violations:
        print(f"\nTHRESHOLD VIOLATIONS ({len(violations)}):")
        for v in violations:
            print(f"  - {v}")
    else:
        print(f"\nAll thresholds satisfied  (entity_recall>="
              f"{THRESHOLDS['entity_recall']:.0%}, "
              f"fact_recall>={THRESHOLDS['fact_recall']:.0%}, "
              f"citation_precision>={THRESHOLDS['citation_precision']:.0%}, "
              f"hallucination_rate<={THRESHOLDS['hallucination_rate_max']:.0%}).")

    if args.json_out:
        payload = {
            "thresholds": THRESHOLDS,
            "by_endpoint": by_endpoint,
            "by_doc_type": by_doc_type,
            "by_category": by_category,
            "by_endpoint_and_doc": by_endpoint_and_doc,
            "raw_scores": [s.__dict__ for s in scores],
            "raw_responses": raw_responses,
            "violations": violations,
        }
        Path(args.json_out).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2)
        )
        print(f"\nJSON written to {args.json_out}")

    if args.strict and violations:
        return 1
    return 0


def _render_table(rows: dict[str, dict[str, float]]) -> str:
    if not rows:
        return "(empty)"
    cols = ["n_cases", "fact_recall", "entity_recall",
            "citation_precision", "hallucination_rate",
            "uncertainty_honesty_pct"]
    label_w = max(8, max(len(k) for k in rows))
    header = f"{'bucket':<{label_w}s}  " + "  ".join(f"{c:>20s}" for c in cols)
    out = [header, "-" * len(header)]
    for bucket, m in sorted(rows.items()):
        cells = [str(m.get("n_cases", 0))] + [
            f"{m.get(c, 0):.2%}" if c != "n_cases" else str(m.get(c, 0))
            for c in cols[1:]
        ]
        out.append(
            f"{bucket:<{label_w}s}  "
            + "  ".join(f"{v:>20s}" for v in cells)
        )
    return "\n".join(out)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--api-url", default="http://localhost:8000")
    p.add_argument("--api-key", default="dev-local-key")
    p.add_argument("--endpoints", default="local,global,drift,auto")
    p.add_argument("--golden", type=Path, default=GOLDEN_DIR_DEFAULT)
    p.add_argument(
        "--no-golden", action="store_true",
        help="skip the hand-crafted golden_qa cases; useful when "
             "the loaded corpus only covers one of the doc_types "
             "(e.g. medical-only).",
    )
    p.add_argument("--json-out", type=Path, default=None)
    p.add_argument("--strict", action="store_true")
    # Medical benchmark — pulled from tests/eval/corpora/medical/
    # (2 062 Q&A from the Medical KG-RAG benchmark).  Opt-in,
    # since the full set is too large for default CI runs.
    p.add_argument(
        "--medical-sample", type=int, default=0,
        help="add N medical Q&A cases (0 = skip; up to 2062).",
    )
    p.add_argument(
        "--medical-types", default="",
        help=(
            "comma-separated subset of medical question_types "
            "(Fact Retrieval, Complex Reasoning, Contextual Summarize, "
            "Creative Generation).  Default: all."
        ),
    )
    p.add_argument(
        "--medical-seed", type=int, default=42,
        help="deterministic shuffle seed for medical sampling.",
    )
    p.add_argument(
        "--search-timeout", type=float, default=180.0,
        help="httpx timeout (s) for /search/local calls.",
    )
    p.add_argument(
        "--agentic-timeout", type=float, default=900.0,
        help=(
            "httpx timeout (s) for /search/{global,drift,auto} calls. "
            "Long-form Creative Generation answers on qwen3:8b CPU "
            "can take 5-15 minutes through the map-reduce / multi-pass "
            "stack — default is generous on purpose."
        ),
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
