import pytest
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


from scripts.make_env import parse_env


def test_parse_env_reads_keyvalues_ignores_comments_blanks():
    txt = "# c\nA=1\n\nB=hello world\n# D=skip\nC=\n"
    assert parse_env(txt) == {"A": "1", "B": "hello world", "C": ""}


import re as _re
from scripts.make_env import is_secret, gen_secret


def test_is_secret_matches_secret_keys():
    for k in ["NEO4J_PASSWORD", "WIKIBASE_SECRET_KEY", "API_KEYS",
              "MINIO_ACCESS_KEY", "LITELLM_API_KEY", "WIKIBASE_ADMIN_PASS"]:
        assert is_secret(k), k
    for k in ["API_HOST", "MILVUS_PORT", "LLM_POOL_N"]:
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


from scripts.make_env import validate, Issue

BASE = {
    "MILVUS_DIM": "1536",
    "LLM_POOL_N": "8",
    "INGEST_ADMISSION_MAX_INFLIGHT": "1",
    "TEMPORAL_LLM_ACTIVITY_CONCURRENCY": "18",
    "TEMPORAL_MERGE_ACTIVITY_CONCURRENCY": "14",
    "OPENAI_API_KEY": "sk-x", "LITELLM_MODEL_SMALL": "gemma4:e4b",
    "LITELLM_MODEL_LARGE": "gpt-4o-mini",
}


def _levels(issues, needle):
    return [i.level for i in issues if needle in i.msg]


def test_validate_clean_base_has_no_errors():
    assert [i for i in validate(BASE) if i.level == "ERROR"] == []


def test_validate_pool_n_temporal_warns_when_below_pool_n():
    # LLM_POOL_N=8; TEMPORAL cap of 4 < 8 → WARN
    v = {**BASE, "LLM_POOL_N": "8", "TEMPORAL_LLM_ACTIVITY_CONCURRENCY": "4"}
    assert "WARN" in _levels(validate(v), "TEMPORAL_LLM_ACTIVITY_CONCURRENCY")


def test_validate_temporal_caps_no_warn_when_above_pool_n():
    # TEMPORAL caps >= LLM_POOL_N → no WARN
    v = {**BASE, "LLM_POOL_N": "8",
         "TEMPORAL_LLM_ACTIVITY_CONCURRENCY": "18",
         "TEMPORAL_MERGE_ACTIVITY_CONCURRENCY": "14"}
    warns = [i for i in validate(v) if i.level == "WARN" and "TEMPORAL" in i.msg]
    assert warns == []


def test_validate_temporal_caps_warn_when_below_ceiling():
    v = {**BASE, "TEMPORAL_LLM_ACTIVITY_CONCURRENCY": "2"}
    assert "WARN" in _levels(validate(v), "TEMPORAL_LLM_ACTIVITY_CONCURRENCY")


def test_validate_openai_key_required_for_gpt_model():
    v = {**BASE, "OPENAI_API_KEY": ""}
    assert "ERROR" in _levels(validate(v), "OPENAI_API_KEY")


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


from scripts.make_env import main


_EXAMPLE_MIN = """# ── Models ──
OPENAI_API_KEY=
LITELLM_MODEL_SMALL=gemma4:e4b
LITELLM_MODEL_LARGE=gpt-4o-mini
# ── Vector ──
MILVUS_DIM=1536
NEO4J_PASSWORD=
"""


def test_noninteractive_does_not_mint_openai_key_and_errors(tmp_path, capsys):
    ex = tmp_path / ".env.example"
    ex.write_text(_EXAMPLE_MIN)
    out = tmp_path / ".env.out"
    rc = main(["--non-interactive", "--no-merge",
               "--example", str(ex), "--out", str(out)])
    assert rc == 1                      # OpenAI ERROR surfaced, no --force
    assert not out.exists()            # not written on ERROR
    out_text = capsys.readouterr().out
    assert "OPENAI_API_KEY" in out_text  # the error mentions it


def test_noninteractive_generates_project_secret_but_not_openai(tmp_path):
    ex = tmp_path / ".env.example"
    ex.write_text(_EXAMPLE_MIN)
    out = tmp_path / ".env.out"
    rc = main(["--non-interactive", "--no-merge", "--force",
               "--example", str(ex), "--out", str(out)])
    assert rc == 0                      # --force writes despite ERROR
    written = out.read_text()
    assert "OPENAI_API_KEY=\n" in written        # left EMPTY, not minted
    # project secret WAS auto-generated (non-empty, not the blank example default)
    assert "NEO4J_PASSWORD=\n" not in written


# --- MediaWiki password length validators ---

@pytest.mark.parametrize("bot_pass,expect_error", [
    ("short7", True),   # 6 chars — too short, ERROR
    ("exactly8", False),  # 8 chars — ok
    ("longenoughpass", False),  # well over 8 — ok
    ("", False),  # empty means not set → no check
])
def test_validate_bot_password_length(bot_pass, expect_error):
    v = {**BASE, "WIKIBASE_BOT_PASSWORD": bot_pass}
    errors = [i for i in validate(v) if i.level == "ERROR" and "WIKIBASE_BOT_PASSWORD" in i.msg]
    if expect_error:
        assert errors, f"Expected ERROR for password {bot_pass!r}, got none"
    else:
        assert not errors, f"Unexpected ERROR for password {bot_pass!r}: {errors}"
