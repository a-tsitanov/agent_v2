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
