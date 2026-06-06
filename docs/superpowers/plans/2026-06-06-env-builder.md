# Interactive `.env` Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `scripts/make_env.py` — an interactive, comment-preserving `.env` generator from `.env.example` with secret generation, cross-field validation, and safe merge of an existing `.env`.

**Architecture:** A stdlib-only module of small pure units (`parse_example`, `parse_env`, `render`, `is_secret`, `gen_secret`, `validate`) wrapped by an injectable interactive shell (`run_interactive`) and a thin `main` CLI. The `.env` output re-emits `.env.example` byte-for-byte except for values on `KEY=` lines.

**Tech Stack:** Python 3.12 stdlib only — `dataclasses`, `re`, `json`, `secrets`, `getpass`, `argparse`. pytest for tests. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-06-06-env-builder-design.md`

---

## File Structure

- **Create** `scripts/make_env.py` — all units + `main`.
- **Create** `tests/test_scripts/__init__.py` (empty) if `tests/test_scripts/` does not exist.
- **Create** `tests/test_scripts/test_make_env.py` — unit tests.

Run tests with `uv run pytest`. The repo runs scripts as modules (e.g. `uv run python -m scripts.setup_db`); this script runs as `uv run python -m scripts.make_env`. Confirm `scripts/__init__.py` exists (it does — `setup_db.py` is imported as `scripts.setup_db`); if missing, create an empty one.

---

## Task 1: Line model + `parse_example`

**Files:**
- Create: `scripts/make_env.py`
- Create: `tests/test_scripts/test_make_env.py` (+ `tests/test_scripts/__init__.py` if absent)

- [ ] **Step 1: Write the failing test**

Create `tests/test_scripts/test_make_env.py`:

```python
from scripts.make_env import parse_example, Comment, Blank, Section, KV

EX = """# ── Sec One ──────────
# comment for A
A=1
B=2

# ── Sec Two ──
C=hello
"""


def test_parse_example_classifies_lines():
    lines = parse_example(EX)
    kinds = [type(l).__name__ for l in lines]
    assert kinds == [
        "Section", "Comment", "KV", "KV", "Blank", "Section", "KV",
    ]


def test_kv_captures_key_value_comment_and_section():
    lines = parse_example(EX)
    kvs = [l for l in lines if isinstance(l, KV)]
    a = kvs[0]
    assert a.key == "A" and a.example_val == "1"
    assert a.comment_lines == ["# comment for A"]
    assert a.section == "Sec One"
    b = kvs[1]
    assert b.key == "B" and b.comment_lines == []  # no contiguous comment above
    c = kvs[2]
    assert c.section == "Sec Two" and c.example_val == "hello"


def test_commented_out_var_is_comment_not_kv():
    lines = parse_example("# ── S ──\n# X_OPT=foo\nY=1\n")
    assert [type(l).__name__ for l in lines] == ["Section", "Comment", "KV"]
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_scripts/test_make_env.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts.make_env'`.

- [ ] **Step 3: Implement the model + parser**

Create `scripts/make_env.py`:

```python
"""Interactive `.env` builder from `.env.example` (comment-preserving).

Run: uv run python -m scripts.make_env   (see --help for flags)
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field


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
```

Note: the trailing-empty guard keeps `render(parse_example(EX)) == EX` for newline-terminated files (Task 2 verifies this).

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_scripts/test_make_env.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/make_env.py tests/test_scripts/
git commit -m "feat(scripts): make_env parse_example — comment-preserving line model"
```

---

## Task 2: `render` + round-trip invariant

**Files:**
- Modify: `scripts/make_env.py`
- Test: `tests/test_scripts/test_make_env.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_scripts/test_make_env.py`:

```python
from pathlib import Path
from scripts.make_env import render


def test_render_roundtrips_example_verbatim():
    lines = parse_example(EX)
    assert render(lines, {}) == EX  # values empty -> example defaults


def test_render_substitutes_values_only():
    lines = parse_example(EX)
    out = render(lines, {"A": "99", "C": "world"})
    assert "A=99" in out and "C=world" in out
    assert "# comment for A" in out          # comments untouched
    assert "# ── Sec One ──────────" in out  # section header untouched
    assert "B=2" in out                       # untouched var keeps default


def test_render_roundtrips_real_env_example():
    ex = Path(".env.example").read_text()
    assert render(parse_example(ex), {}) == ex
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_scripts/test_make_env.py -k render -v`
Expected: FAIL — `cannot import name 'render'`.

- [ ] **Step 3: Implement `render`**

Append to `scripts/make_env.py`:

```python
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
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_scripts/test_make_env.py -v`
Expected: PASS (all, incl. the real `.env.example` round-trip).

If `test_render_roundtrips_real_env_example` fails on a trailing-newline or a non-`KEY=VALUE` line, inspect the diff: `python -c "from scripts.make_env import *; ex=open('.env.example').read(); print(repr(render(parse_example(ex),{})[-50:]), repr(ex[-50:]))"` and fix the parser's trailing-empty handling (Task 1) so the byte-for-byte invariant holds.

- [ ] **Step 5: Commit**

```bash
git add scripts/make_env.py tests/test_scripts/test_make_env.py
git commit -m "feat(scripts): make_env render — verbatim round-trip + value substitution"
```

---

## Task 3: `parse_env` (merge default-source)

**Files:**
- Modify: `scripts/make_env.py`
- Test: `tests/test_scripts/test_make_env.py`

- [ ] **Step 1: Write the failing test**

Append:

```python
from scripts.make_env import parse_env


def test_parse_env_reads_keyvalues_ignores_comments_blanks():
    txt = "# c\nA=1\n\nB=hello world\n# D=skip\nC=\n"
    assert parse_env(txt) == {"A": "1", "B": "hello world", "C": ""}
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_scripts/test_make_env.py -k parse_env -v`
Expected: FAIL — `cannot import name 'parse_env'`.

- [ ] **Step 3: Implement**

Append to `scripts/make_env.py`:

```python
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
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_scripts/test_make_env.py -k parse_env -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/make_env.py tests/test_scripts/test_make_env.py
git commit -m "feat(scripts): make_env parse_env for merge defaults"
```

---

## Task 4: `is_secret` + `gen_secret`

**Files:**
- Modify: `scripts/make_env.py`
- Test: `tests/test_scripts/test_make_env.py`

- [ ] **Step 1: Write the failing test**

Append:

```python
import re as _re
from scripts.make_env import is_secret, gen_secret


def test_is_secret_matches_secret_keys():
    for k in ["NEO4J_PASSWORD", "WIKIBASE_SECRET_KEY", "API_KEYS",
              "MINIO_ACCESS_KEY", "LITELLM_API_KEY", "WIKIBASE_ADMIN_PASS"]:
        assert is_secret(k), k
    for k in ["API_HOST", "MILVUS_PORT", "LLM_POOL_TIER_SMALL_TOTAL"]:
        assert not is_secret(k), k


def test_gen_secret_wikibase_key_is_32_hex():
    v = gen_secret("WIKIBASE_SECRET_KEY")
    assert _re.fullmatch(r"[0-9a-f]{32}", v)


def test_gen_secret_password_meets_min_lengths():
    assert len(gen_secret("WIKIBASE_ADMIN_PASS")) >= 12
    assert len(gen_secret("NEO4J_PASSWORD")) >= 12


def test_gen_secret_api_key_has_sk_prefix():
    assert gen_secret("LITELLM_API_KEY").startswith("sk-")
    assert gen_secret("API_KEYS").startswith("sk-")
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_scripts/test_make_env.py -k "secret" -v`
Expected: FAIL — `cannot import name 'is_secret'`.

- [ ] **Step 3: Implement**

Append to `scripts/make_env.py` (add `import secrets` to the imports at top):

```python
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
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_scripts/test_make_env.py -k "secret" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/make_env.py tests/test_scripts/test_make_env.py
git commit -m "feat(scripts): make_env is_secret + gen_secret"
```

---

## Task 5: `validate` (cross-field)

**Files:**
- Modify: `scripts/make_env.py`
- Test: `tests/test_scripts/test_make_env.py`

- [ ] **Step 1: Write the failing test**

Append:

```python
from scripts.make_env import validate, Issue

BASE = {
    "MILVUS_DIM": "1536", "LITELLM_EMBEDDING_DIM": "1536",
    "LLM_POOL_TIER_SMALL_TOTAL": "25", "LLM_POOL_JUDGE_FLOOR": "7",
    "TEMPORAL_LLM_ACTIVITY_CONCURRENCY": "18",
    "TEMPORAL_MERGE_ACTIVITY_CONCURRENCY": "14",
    "OPENAI_API_KEY": "sk-x", "LITELLM_MODEL_SMALL": "gemma4:e4b",
    "LITELLM_MODEL_LARGE": "gpt-4o-mini",
}


def _levels(issues, needle):
    return [i.level for i in issues if needle in i.msg]


def test_validate_clean_base_has_no_errors():
    assert [i for i in validate(BASE) if i.level == "ERROR"] == []


def test_validate_dim_mismatch_errors():
    v = {**BASE, "LITELLM_EMBEDDING_DIM": "3072"}
    assert "ERROR" in _levels(validate(v), "DIM")


def test_validate_pool_rule_errors_when_extraction_too_high():
    # default lane_caps extraction=18; small-floor = 20-7=13 -> 18>13 -> ERROR
    v = {**BASE, "LLM_POOL_TIER_SMALL_TOTAL": "20"}
    assert "ERROR" in _levels(validate(v), "extraction")


def test_validate_temporal_caps_warn_when_below_ceiling():
    v = {**BASE, "TEMPORAL_LLM_ACTIVITY_CONCURRENCY": "2"}
    assert "WARN" in _levels(validate(v), "TEMPORAL_LLM_ACTIVITY_CONCURRENCY")


def test_validate_openai_key_required_for_gpt_model():
    v = {**BASE, "OPENAI_API_KEY": ""}
    assert "ERROR" in _levels(validate(v), "OPENAI_API_KEY")
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_scripts/test_make_env.py -k validate -v`
Expected: FAIL — `cannot import name 'validate'`.

- [ ] **Step 3: Implement**

Append to `scripts/make_env.py` (add `import json` to imports):

```python
@dataclass
class Issue:
    level: str  # "ERROR" | "WARN"
    msg: str


_DEFAULT_LANE_CAPS = {
    "extraction": 18, "judge": 14, "search": 14,
    "plan": 4, "route": 2, "retrieve": 4, "synthesis": 8,
}


def _int(values: dict[str, str], key: str, default: int = 0) -> int:
    try:
        return int(values[key])
    except (KeyError, ValueError):
        return default


def _lane_caps(values: dict[str, str]) -> dict[str, int]:
    raw = values.get("LLM_POOL_LANE_CAPS", "").strip()
    if raw:
        try:
            return {**_DEFAULT_LANE_CAPS, **json.loads(raw)}
        except json.JSONDecodeError:
            pass
    return dict(_DEFAULT_LANE_CAPS)


def validate(values: dict[str, str]) -> list[Issue]:
    """Cross-field checks; returns ERROR/WARN issues (empty == clean)."""
    issues: list[Issue] = []

    if "MILVUS_DIM" in values and "LITELLM_EMBEDDING_DIM" in values:
        if _int(values, "MILVUS_DIM") != _int(values, "LITELLM_EMBEDDING_DIM"):
            issues.append(Issue("ERROR",
                "MILVUS_DIM must equal LITELLM_EMBEDDING_DIM "
                f"({values['MILVUS_DIM']} != {values['LITELLM_EMBEDDING_DIM']})"))

    caps = _lane_caps(values)
    small = _int(values, "LLM_POOL_TIER_SMALL_TOTAL", 25)
    floor = _int(values, "LLM_POOL_JUDGE_FLOOR", 7)
    if caps["extraction"] > small - floor:
        issues.append(Issue("ERROR",
            f"LLM_POOL extraction ceiling ({caps['extraction']}) must be <= "
            f"tier_small_total - judge_floor ({small} - {floor} = {small - floor})"))

    llm_cap = _int(values, "TEMPORAL_LLM_ACTIVITY_CONCURRENCY", 0)
    if llm_cap and llm_cap < caps["extraction"]:
        issues.append(Issue("WARN",
            f"TEMPORAL_LLM_ACTIVITY_CONCURRENCY ({llm_cap}) < extraction lane "
            f"ceiling ({caps['extraction']}); Temporal will throttle before the pool"))
    merge_cap = _int(values, "TEMPORAL_MERGE_ACTIVITY_CONCURRENCY", 0)
    if merge_cap and merge_cap < caps["judge"]:
        issues.append(Issue("WARN",
            f"TEMPORAL_MERGE_ACTIVITY_CONCURRENCY ({merge_cap}) < judge lane "
            f"ceiling ({caps['judge']}); Temporal will throttle before the pool"))

    models = (values.get("LITELLM_MODEL_SMALL", ""),
              values.get("LITELLM_MODEL_LARGE", ""))
    if any(m.startswith("gpt-") for m in models) and not values.get("OPENAI_API_KEY"):
        issues.append(Issue("ERROR",
            "OPENAI_API_KEY is empty but a model tier points at OpenAI (gpt-*)"))

    return issues
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_scripts/test_make_env.py -k validate -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/make_env.py tests/test_scripts/test_make_env.py
git commit -m "feat(scripts): make_env cross-field validate"
```

---

## Task 6: `run_interactive` (section-skip loop, injected I/O)

**Files:**
- Modify: `scripts/make_env.py`
- Test: `tests/test_scripts/test_make_env.py`

- [ ] **Step 1: Write the failing test**

Append:

```python
from scripts.make_env import run_interactive


def _scripted(answers):
    it = iter(answers)
    return lambda prompt="": next(it)


def test_interactive_skip_keeps_defaults():
    lines = parse_example(EX)
    values = {l.key: l.example_val for l in lines if isinstance(l, KV)}
    # Two sections; press Enter (keep) for both.
    out = run_interactive(lines, dict(values),
                          input_fn=_scripted(["", ""]),
                          getpass_fn=_scripted([]))
    assert out == values


def test_interactive_configure_section_sets_value():
    lines = parse_example(EX)
    values = {l.key: l.example_val for l in lines if isinstance(l, KV)}
    # Sec One: 'e' to configure -> A="7", B="" (keep). Sec Two: "" skip.
    out = run_interactive(lines, dict(values),
                          input_fn=_scripted(["e", "7", "", ""]),
                          getpass_fn=_scripted([]))
    assert out["A"] == "7" and out["B"] == "2" and out["C"] == "hello"


def test_interactive_generate_secret():
    txt = "# ── S ──\nNEO4J_PASSWORD=changeme\n"
    lines = parse_example(txt)
    values = {"NEO4J_PASSWORD": "changeme"}
    # configure section -> secret prompt answered with 'g' (generate)
    out = run_interactive(lines, dict(values),
                          input_fn=_scripted(["e"]),
                          getpass_fn=_scripted(["g"]))
    assert out["NEO4J_PASSWORD"] != "changeme"
    assert len(out["NEO4J_PASSWORD"]) >= 12
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_scripts/test_make_env.py -k interactive -v`
Expected: FAIL — `cannot import name 'run_interactive'`.

- [ ] **Step 3: Implement**

Append to `scripts/make_env.py`:

```python
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
    import getpass as _gp
    if getpass_fn is None:
        getpass_fn = _gp.getpass

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
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_scripts/test_make_env.py -k interactive -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/make_env.py tests/test_scripts/test_make_env.py
git commit -m "feat(scripts): make_env run_interactive section-skip loop"
```

---

## Task 7: `main` CLI — merge, validate gate, backup, write

**Files:**
- Modify: `scripts/make_env.py`
- Test: `tests/test_scripts/test_make_env.py`

- [ ] **Step 1: Write the failing test**

Append:

```python
from scripts.make_env import build_values, write_env


def test_build_values_merges_existing_over_example():
    lines = parse_example(EX)
    existing = {"A": "merged", "C": "kept"}
    vals = build_values(lines, existing)
    assert vals["A"] == "merged"   # existing wins
    assert vals["B"] == "2"        # falls back to example default
    assert vals["C"] == "kept"


def test_write_env_backs_up_existing(tmp_path):
    out = tmp_path / ".env"
    out.write_text("OLD=1\n")
    write_env(out, "NEW=2\n")
    assert out.read_text() == "NEW=2\n"
    assert (tmp_path / ".env.bak").read_text() == "OLD=1\n"


def test_write_env_no_backup_when_absent(tmp_path):
    out = tmp_path / ".env"
    write_env(out, "NEW=2\n")
    assert out.read_text() == "NEW=2\n"
    assert not (tmp_path / ".env.bak").exists()
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/test_scripts/test_make_env.py -k "build_values or write_env" -v`
Expected: FAIL — `cannot import name 'build_values'`.

- [ ] **Step 3: Implement helpers + `main`**

Append to `scripts/make_env.py` (add `import argparse`, `import sys`, `from pathlib import Path` to imports):

```python
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
            if isinstance(ln, KV) and is_secret(ln.key) and not values[ln.key]:
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
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/test_scripts/test_make_env.py -v`
Expected: PASS (entire file).

- [ ] **Step 5: Smoke-test the CLI non-interactively**

Run: `uv run python -m scripts.make_env --non-interactive --out /tmp/.env.smoke && head -20 /tmp/.env.smoke && rm /tmp/.env.smoke`
Expected: prints "validating… / ok / wrote /tmp/.env.smoke" and the head shows the section headers + comments preserved with values. (Note: `OPENAI_API_KEY` empty + `LITELLM_MODEL_LARGE=gpt-4o-mini` default will raise the OpenAI ERROR — expected; re-run with `--force` for the smoke test, or set `OPENAI_API_KEY=sk-x` in the env first. Document this in the run output.)

Adjusted smoke command: `OPENAI_API_KEY= uv run python -m scripts.make_env --non-interactive --force --out /tmp/.env.smoke && grep -c "── " /tmp/.env.smoke && rm -f /tmp/.env.smoke /tmp/.env.smoke.bak`
Expected: section-header count > 0, exit 0.

- [ ] **Step 6: Commit**

```bash
git add scripts/make_env.py tests/test_scripts/test_make_env.py
git commit -m "feat(scripts): make_env CLI — merge, validate gate, backup, write"
```

---

## Self-Review notes (for the implementer)

- **Spec coverage:** §3 units → Tasks 1-7; §4 interaction → Task 6; §5 secrets → Task 4; §6 validators → Task 5; §7 CLI/backup/non-TTY → Task 7; §8 tests → spread across all tasks; §9 out-of-scope respected (no profiles, no preflight, commented vars stay comments).
- **Type consistency:** `Line`, `Comment/Blank/Section/KV`, `Issue(level,msg)`, `parse_example/parse_env/render/is_secret/gen_secret/validate/run_interactive/build_values/write_env/main` — names are used identically across tasks.
- If the real-`.env.example` round-trip (Task 2) reveals an unhandled line shape, fix `parse_example` rather than weakening the test — the byte-for-byte invariant is the core guarantee.
```
