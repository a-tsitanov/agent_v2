"""CLI wiring: `channels` calls status_counts_by('source_channel', ...) and
its rows render into the table. No DB — AsyncPostgres is patched."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

from scripts.message_stats import format_status_rows, main


def test_format_status_rows_aligns() -> None:
    rows = [{"key": "alpha", "total": 3, "pending": 0, "processing": 0,
             "completed": 2, "vector_only": 0, "failed": 1, "skipped": 0}]
    out = format_status_rows(rows)
    assert "alpha" in out
    assert "completed" in out  # header present
    assert "3" in out


def test_channels_subcommand_uses_source_channel() -> None:
    fake = [{"key": "alpha", "total": 1, "pending": 0, "processing": 0,
             "completed": 1, "vector_only": 0, "failed": 0, "skipped": 0}]
    with patch("scripts.message_stats.AsyncPostgres") as cls:
        cls.return_value.status_counts_by = AsyncMock(return_value=fake)
        main(["channels"])
    assert cls.return_value.status_counts_by.call_args.args[0] == "source_channel"
