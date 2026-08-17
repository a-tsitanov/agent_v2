from src.config import TemporalSettings


def test_community_backend_defaults_to_gds(monkeypatch):
    """The CODE's default, not whatever the developer's `.env` says.

    This used to read the process singleton (`settings.temporal....`) and
    assert "gds". It passed or failed depending on import order:
    `TemporalSettings` had no `env_file`, so it saw `.env` only once
    something had dumped the file into `os.environ` — which
    `pymilvus/settings.py` does with `load_dotenv()` at import time. Run
    alone, the test saw the class default; run after anything touching
    pymilvus, it saw `.env`'s `leidenalg`.

    Giving `TemporalSettings` the `env_file` its siblings all have made
    that consistent, and consistently red — because the assertion was
    never really about the default. Construct the class with both sources
    switched off and it is.
    """
    monkeypatch.delenv("TEMPORAL_COMMUNITY_BACKEND", raising=False)
    assert TemporalSettings(_env_file=None).community_backend == "gds"


def test_community_backend_reads_the_environment(monkeypatch):
    """And the operator's value wins over that default."""
    monkeypatch.setenv("TEMPORAL_COMMUNITY_BACKEND", "leidenalg")
    assert TemporalSettings().community_backend == "leidenalg"


def test_community_backend_is_constrained():
    import typing

    hints = typing.get_type_hints(TemporalSettings)
    # Literal["gds","leidenalg"]
    assert "gds" in typing.get_args(hints["community_backend"])
    assert "leidenalg" in typing.get_args(hints["community_backend"])
