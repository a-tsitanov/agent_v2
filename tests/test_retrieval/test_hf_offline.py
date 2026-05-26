"""Tests for offline HuggingFace model loading.

Covers:
  * ``HFSettings`` env binding (reads HF_OFFLINE / HF_CACHE_DIR /
    HF_RERANK_MODEL, and DOES NOT clobber the real HF_HOME /
    HF_HUB_OFFLINE that HuggingFace libs themselves consume).
  * ``configure_hf()`` env-mutation behaviour + idempotency +
    operator-set env precedence.
  * ``build_reranker()`` resolving its model name from config.
  * The ``scripts/download_models.py`` CLI shape + online-forcing +
    that it drives the loaders with the configured model names.

NOTHING here downloads a model — the heavy loaders are always mocked.
"""

from __future__ import annotations

import importlib
import types

import pytest

# ── HFSettings env binding ───────────────────────────────────────────


def test_hfsettings_reads_explicit_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.config import HFSettings

    monkeypatch.setenv("HF_OFFLINE", "true")
    monkeypatch.setenv("HF_CACHE_DIR", "/data/hf")
    monkeypatch.setenv("HF_RERANK_MODEL", "some/other-reranker")
    s = HFSettings()
    assert s.offline is True
    assert s.cache_dir == "/data/hf"
    assert s.rerank_model == "some/other-reranker"


def test_hfsettings_does_not_bind_hf_home(monkeypatch: pytest.MonkeyPatch) -> None:
    """HuggingFace's own HF_HOME / HF_HUB_OFFLINE must NOT leak into
    our settings fields — those belong to the HF libs."""
    from src.config import HFSettings

    monkeypatch.delenv("HF_OFFLINE", raising=False)
    monkeypatch.delenv("HF_CACHE_DIR", raising=False)
    monkeypatch.setenv("HF_HOME", "/some/hf/home")
    monkeypatch.setenv("HF_HUB_OFFLINE", "1")
    s = HFSettings()
    # cache_dir must stay None (it must NOT pick up HF_HOME)
    assert s.cache_dir is None
    # offline must stay False (it must NOT pick up HF_HUB_OFFLINE)
    assert s.offline is False


def test_hfsettings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.config import HFSettings

    monkeypatch.delenv("HF_OFFLINE", raising=False)
    monkeypatch.delenv("HF_CACHE_DIR", raising=False)
    monkeypatch.delenv("HF_RERANK_MODEL", raising=False)
    s = HFSettings()
    assert s.offline is False
    assert s.cache_dir is None
    assert s.rerank_model == "BAAI/bge-reranker-v2-m3"


def test_settings_mounts_hf() -> None:
    from src.config import HFSettings, settings

    assert isinstance(settings.hf, HFSettings)


# ── configure_hf() ───────────────────────────────────────────────────


def _make_hf_stub(*, offline: bool, cache_dir: str | None, rerank_model: str = "m"):
    return types.SimpleNamespace(
        offline=offline, cache_dir=cache_dir, rerank_model=rerank_model,
    )


def test_configure_hf_offline_sets_offline_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.retrieval import hf_offline

    monkeypatch.delenv("HF_HUB_OFFLINE", raising=False)
    monkeypatch.delenv("TRANSFORMERS_OFFLINE", raising=False)
    monkeypatch.setattr(
        hf_offline.settings, "hf",
        _make_hf_stub(offline=True, cache_dir=None), raising=False,
    )
    hf_offline.configure_hf()
    assert hf_offline.os.environ["HF_HUB_OFFLINE"] == "1"
    assert hf_offline.os.environ["TRANSFORMERS_OFFLINE"] == "1"


def test_configure_hf_cache_dir_sets_cache_vars(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.retrieval import hf_offline

    for v in ("HF_HOME", "SENTENCE_TRANSFORMERS_HOME", "TRANSFORMERS_CACHE"):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setattr(
        hf_offline.settings, "hf",
        _make_hf_stub(offline=False, cache_dir="/data/hf"), raising=False,
    )
    hf_offline.configure_hf()
    assert hf_offline.os.environ["HF_HOME"] == "/data/hf"
    assert hf_offline.os.environ["SENTENCE_TRANSFORMERS_HOME"] == "/data/hf"
    assert hf_offline.os.environ["TRANSFORMERS_CACHE"] == "/data/hf"


def test_configure_hf_defaults_set_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.retrieval import hf_offline

    for v in (
        "HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HF_HOME",
        "SENTENCE_TRANSFORMERS_HOME", "TRANSFORMERS_CACHE",
    ):
        monkeypatch.delenv(v, raising=False)
    monkeypatch.setattr(
        hf_offline.settings, "hf",
        _make_hf_stub(offline=False, cache_dir=None), raising=False,
    )
    hf_offline.configure_hf()
    for v in (
        "HF_HUB_OFFLINE", "TRANSFORMERS_OFFLINE", "HF_HOME",
        "SENTENCE_TRANSFORMERS_HOME", "TRANSFORMERS_CACHE",
    ):
        assert v not in hf_offline.os.environ


def test_configure_hf_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.retrieval import hf_offline

    monkeypatch.setattr(
        hf_offline.settings, "hf",
        _make_hf_stub(offline=True, cache_dir="/data/hf"), raising=False,
    )
    hf_offline.configure_hf()
    # Second call must not raise.
    hf_offline.configure_hf()
    assert hf_offline.os.environ["HF_HUB_OFFLINE"] == "1"


def test_configure_hf_operator_env_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    """An explicit operator-set HF_HOME must NOT be overwritten."""
    from src.retrieval import hf_offline

    monkeypatch.setenv("HF_HOME", "/operator/explicit")
    monkeypatch.setattr(
        hf_offline.settings, "hf",
        _make_hf_stub(offline=False, cache_dir="/data/hf"), raising=False,
    )
    hf_offline.configure_hf()
    assert hf_offline.os.environ["HF_HOME"] == "/operator/explicit"


# ── build_reranker config-driven model ───────────────────────────────


def test_build_reranker_resolves_model_from_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.retrieval import reranker as rr

    captured: dict[str, object] = {}

    class _FakeRerank:
        def __init__(self, *, model, top_n, **kw):
            captured["model"] = model
            captured["top_n"] = top_n
            captured["kw"] = kw

    monkeypatch.setattr(rr, "SentenceTransformerRerank", _FakeRerank)
    monkeypatch.setattr(rr, "configure_hf", lambda: None)
    monkeypatch.setattr(
        rr.settings, "hf",
        _make_hf_stub(offline=False, cache_dir=None, rerank_model="cfg/model"),
        raising=False,
    )

    rr.build_reranker(top_n=3)
    assert captured["model"] == "cfg/model"
    assert captured["top_n"] == 3


def test_build_reranker_explicit_model_overrides_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.retrieval import reranker as rr

    captured: dict[str, object] = {}

    class _FakeRerank:
        def __init__(self, *, model, top_n, **kw):
            captured["model"] = model

    monkeypatch.setattr(rr, "SentenceTransformerRerank", _FakeRerank)
    monkeypatch.setattr(rr, "configure_hf", lambda: None)
    monkeypatch.setattr(
        rr.settings, "hf",
        _make_hf_stub(offline=False, cache_dir=None, rerank_model="cfg/model"),
        raising=False,
    )

    rr.build_reranker(model_name="explicit/model")
    assert captured["model"] == "explicit/model"


# ── download script shape ────────────────────────────────────────────


def test_download_script_has_main_and_parser() -> None:
    mod = importlib.import_module("scripts.download_models")
    assert hasattr(mod, "main")
    parser = mod.build_arg_parser()
    ns = parser.parse_args(["--models", "gliner"])
    assert ns.models == "gliner"


def test_download_script_forces_online_and_calls_loaders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = importlib.import_module("scripts.download_models")

    # Force offline in the ambient env — the script must override it.
    monkeypatch.setenv("HF_HUB_OFFLINE", "1")
    monkeypatch.setenv("TRANSFORMERS_OFFLINE", "1")

    calls: dict[str, str] = {}

    def _fake_gliner(name: str) -> None:
        calls["gliner"] = name

    def _fake_reranker(name: str) -> None:
        calls["reranker"] = name

    monkeypatch.setattr(mod, "_download_gliner", _fake_gliner)
    monkeypatch.setattr(mod, "_download_reranker", _fake_reranker)

    rc = mod.main(["--models", "all", "--cache-dir", "/tmp/hfcache"])
    assert rc == 0
    # Online forced for the download process.
    assert mod.os.environ["HF_HUB_OFFLINE"] == "0"
    assert mod.os.environ["TRANSFORMERS_OFFLINE"] == "0"
    # Cache dir wired.
    assert mod.os.environ["HF_HOME"] == "/tmp/hfcache"
    # Loaders driven with configured names.
    from src.config import settings

    assert calls["gliner"] == settings.ingestion.gliner_model
    assert calls["reranker"] == settings.hf.rerank_model
