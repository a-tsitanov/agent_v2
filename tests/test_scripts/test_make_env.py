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
