"""Interactive `.env` builder from `.env.example` (comment-preserving).

Run: uv run python -m scripts.make_env   (see --help for flags)
"""
from __future__ import annotations

import argparse
import getpass
import re
import secrets
import sys
from dataclasses import dataclass, field
from pathlib import Path

# Upstream credentials the user must supply — never auto-minted (an
# invented value would mask the validation that requires a real one).
_UPSTREAM_CREDENTIALS = {"OPENAI_API_KEY"}


@dataclass
class Comment:
    text: str


@dataclass
class Blank:
    pass


@dataclass
class Section:
    title: str
    raw: str


@dataclass
class KV:
    key: str
    example_val: str
    comment_lines: list[str] = field(default_factory=list)
    section: str = ""


Line = Comment | Blank | Section | KV

# A section header looks like:  # ── Title text ───────────
_SECTION_RE = re.compile(r"^#\s*─+\s*(.*?)\s*─+\s*$")
# An active KEY=VALUE line (uppercase env key, no leading '#').
_KV_RE = re.compile(r"^([A-Z][A-Z0-9_]*)=(.*)$")


def parse_example(text: str) -> list[Line]:
    """Parse `.env.example` text into an ordered list of Line records.

    Every source line becomes exactly one Line (so render can reproduce
    the file verbatim).  Each KV also captures the contiguous comment
    block directly above it (for prompting) and its current section.
    """
    lines: list[Line] = []
    section = ""
    recent: list[str] = []  # contiguous comments since last blank/kv/section
    parts = text.split("\n")
    if text.endswith("\n"):
        parts = parts[:-1]  # drop the empty artifact from the trailing newline
    for raw in parts:
        stripped = raw.strip()
        m_sec = _SECTION_RE.match(raw)
        if m_sec:
            section = m_sec.group(1)
            lines.append(Section(title=section, raw=raw))
            recent = []
        elif stripped == "":
            lines.append(Blank())
            recent = []
        elif raw.lstrip().startswith("#"):
            lines.append(Comment(text=raw))
            recent.append(raw)
        else:
            m_kv = _KV_RE.match(raw)
            if m_kv:
                lines.append(KV(
                    key=m_kv.group(1), example_val=m_kv.group(2),
                    comment_lines=list(recent), section=section,
                ))
                recent = []
            else:
                lines.append(Comment(text=raw))
                recent = []
    return lines


def render(lines: list[Line], values: dict[str, str]) -> str:
    """Re-emit the parsed file; KV lines take values[key] (fallback to the
    example default).  Comments / blanks / sections are verbatim."""
    out: list[str] = []
    for ln in lines:
        if isinstance(ln, Comment):
            out.append(ln.text)
        elif isinstance(ln, Blank):
            out.append("")
        elif isinstance(ln, Section):
            out.append(ln.raw)
        elif isinstance(ln, KV):
            out.append(f"{ln.key}={values.get(ln.key, ln.example_val)}")
    return "\n".join(out) + "\n"


def parse_env(text: str) -> dict[str, str]:
    """Simple KEY=VALUE reader for an existing .env (no interpolation).
    Skips blanks and comment lines; splits on the first '='."""
    out: dict[str, str] = {}
    for raw in text.split("\n"):
        s = raw.strip()
        if not s or s.startswith("#") or "=" not in raw:
            continue
        key, _, val = raw.partition("=")
        key = key.strip()
        if _KV_RE.match(f"{key}="):
            out[key] = val
    return out


_SECRET_MARKERS = ("PASSWORD", "PASS", "SECRET", "API_KEY", "API_KEYS",
                   "ACCESS_KEY", "_KEY")


def is_secret(key: str) -> bool:
    """Name heuristic: does this var hold a secret/credential?"""
    k = key.upper()
    return any(m in k for m in _SECRET_MARKERS)


def gen_secret(key: str) -> str:
    """Generate a sensible secret for `key` (opt-in per field)."""
    k = key.upper()
    if k == "WIKIBASE_SECRET_KEY":
        return secrets.token_hex(16)          # exactly 32 hex chars
    if "API_KEY" in k or k == "API_KEYS":
        return "sk-" + secrets.token_urlsafe(32)
    # passwords + everything else: long urlsafe token (>= 12 chars)
    return secrets.token_urlsafe(24)


@dataclass
class Issue:
    level: str  # "ERROR" | "WARN"
    msg: str


def _int(values: dict[str, str], key: str, default: int = 0) -> int:
    try:
        return int(values[key])
    except (KeyError, ValueError):
        return default


def validate(values: dict[str, str]) -> list[Issue]:
    """Cross-field checks; returns ERROR/WARN issues (empty == clean)."""
    issues: list[Issue] = []

    # K + N pool model: N = LLM_POOL_N (global semaphore), K = INGEST_ADMISSION_MAX_INFLIGHT
    pool_n = _int(values, "LLM_POOL_N", 8)
    llm_cap = _int(values, "TEMPORAL_LLM_ACTIVITY_CONCURRENCY", 0)
    if llm_cap and llm_cap < pool_n:
        issues.append(Issue("WARN",
            f"TEMPORAL_LLM_ACTIVITY_CONCURRENCY ({llm_cap}) < LLM_POOL_N "
            f"({pool_n}); Temporal will throttle before the pool"))
    merge_cap = _int(values, "TEMPORAL_MERGE_ACTIVITY_CONCURRENCY", 0)
    if merge_cap and merge_cap < pool_n:
        issues.append(Issue("WARN",
            f"TEMPORAL_MERGE_ACTIVITY_CONCURRENCY ({merge_cap}) < LLM_POOL_N "
            f"({pool_n}); Temporal will throttle before the pool"))

    models = (values.get("LITELLM_MODEL_SMALL", ""),
              values.get("LITELLM_MODEL_LARGE", ""))
    if any(m.startswith("gpt-") for m in models) and not values.get("OPENAI_API_KEY"):
        issues.append(Issue("ERROR",
            "OPENAI_API_KEY is empty but a model tier points at OpenAI (gpt-*)"))

    bp = values.get("WIKIBASE_BOT_PASSWORD", "")
    if bp and len(bp) < 8:
        issues.append(Issue("ERROR",
            f"WIKIBASE_BOT_PASSWORD must be >= 8 chars (MediaWiki minimum), got {len(bp)}"))
    ap = values.get("WIKIBASE_ADMIN_PASS", "")
    if ap and len(ap) < 10:
        issues.append(Issue("ERROR",
            f"WIKIBASE_ADMIN_PASS must be >= 10 chars (MediaWiki minimum), got {len(ap)}"))

    return issues


def _sections_in_order(lines: list[Line]) -> list[tuple[str, list[KV]]]:
    """Group KV lines by their section, preserving file order."""
    groups: list[tuple[str, list[KV]]] = []
    index: dict[str, int] = {}
    for ln in lines:
        if isinstance(ln, KV):
            if ln.section not in index:
                index[ln.section] = len(groups)
                groups.append((ln.section, []))
            groups[index[ln.section]][1].append(ln)
    return groups


def run_interactive(
    lines: list[Line],
    values: dict[str, str],
    *,
    input_fn=input,
    getpass_fn=None,
) -> dict[str, str]:
    """Section-by-section prompt loop. Mutates and returns `values`.

    Per section: Enter=keep, 'e'=configure (walk its vars), 'q'=stop.
    Per var: Enter keeps the current default; text overrides; for secrets
    'g' generates.  I/O is injected for testing.
    """
    if getpass_fn is None:
        getpass_fn = getpass.getpass

    for title, kvs in _sections_in_order(lines):
        print(f"\n=== {title or '(no section)'} ===")
        for kv in kvs:
            print(f"  {kv.key}={values.get(kv.key, kv.example_val)}")
        choice = input_fn("[Enter] keep  [e] configure  [q] quit: ").strip().lower()
        if choice == "q":
            break
        if choice != "e":
            continue
        for kv in kvs:
            for c in kv.comment_lines:
                print(c)
            cur = values.get(kv.key, kv.example_val)
            if is_secret(kv.key):
                ans = getpass_fn(f"{kv.key} [default kept; 'g'=generate]: ")
                if ans == "g":
                    values[kv.key] = gen_secret(kv.key)
                elif ans != "":
                    values[kv.key] = ans
            else:
                ans = input_fn(f"{kv.key} [{cur}]: ")
                if ans != "":
                    values[kv.key] = ans
    return values


def build_values(lines: list[Line], existing: dict[str, str]) -> dict[str, str]:
    """Initial values: example defaults overlaid with any existing .env values."""
    values = {ln.key: ln.example_val for ln in lines if isinstance(ln, KV)}
    for k, v in existing.items():
        if k in values:
            values[k] = v
    return values


def write_env(out: Path, content: str) -> None:
    """Write `content` to `out`, backing up an existing file to `<out>.bak`."""
    if out.exists():
        bak = out.with_name(out.name + ".bak")
        bak.write_text(out.read_text())
    out.write_text(content)


def _report_issues(issues: list[Issue]) -> bool:
    """Print issues; return True if any ERROR present."""
    has_error = False
    for i in issues:
        print(f"  [{i.level}] {i.msg}")
        has_error = has_error or i.level == "ERROR"
    return has_error


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build .env from .env.example.")
    p.add_argument("--example", default=".env.example")
    p.add_argument("--out", default=".env")
    p.add_argument("--non-interactive", action="store_true",
                   help="copy defaults + generate empty secrets, no prompts")
    p.add_argument("--force", action="store_true",
                   help="write despite ERROR-level validation")
    p.add_argument("--no-merge", action="store_true",
                   help="ignore an existing .env")
    args = p.parse_args(argv)

    example_path = Path(args.example)
    out_path = Path(args.out)
    lines = parse_example(example_path.read_text())

    existing: dict[str, str] = {}
    if not args.no_merge and out_path.exists():
        existing = parse_env(out_path.read_text())
        print(f"merging values from existing {out_path}")
    values = build_values(lines, existing)

    if args.non_interactive:
        for ln in lines:
            if (isinstance(ln, KV) and is_secret(ln.key)
                    and not values[ln.key]
                    and ln.key not in _UPSTREAM_CREDENTIALS):
                values[ln.key] = gen_secret(ln.key)
    else:
        if not sys.stdin.isatty():
            print("error: not a TTY; use --non-interactive", file=sys.stderr)
            return 2
        values = run_interactive(lines, values)

    print("\nvalidating…")
    issues = validate(values)
    if _report_issues(issues) and not args.force:
        print("ERRORs found; fix them or re-run with --force.", file=sys.stderr)
        return 1
    if not issues:
        print("  ok")

    write_env(out_path, render(lines, values))
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
