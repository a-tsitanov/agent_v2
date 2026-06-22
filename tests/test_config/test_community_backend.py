from src.config import settings


def test_community_backend_defaults_to_gds():
    assert settings.temporal.community_backend == "gds"


def test_community_backend_is_constrained():
    import typing

    from src.config import TemporalSettings
    hints = typing.get_type_hints(TemporalSettings)
    # Literal["gds","leidenalg"]
    assert "gds" in typing.get_args(hints["community_backend"])
    assert "leidenalg" in typing.get_args(hints["community_backend"])
