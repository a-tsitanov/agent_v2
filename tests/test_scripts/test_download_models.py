"""Tests for scripts/download_models.py — flat local_dir mode."""

from __future__ import annotations

import scripts.download_models as dm


def test_snapshot_writes_flat_local_dir(monkeypatch, tmp_path):
    calls = {}

    def fake_snapshot_download(*, repo_id, local_dir, **kw):
        calls["repo_id"] = repo_id
        calls["local_dir"] = local_dir
        return local_dir

    monkeypatch.setattr(dm, "snapshot_download", fake_snapshot_download)

    dm._snapshot("BAAI/bge-reranker-v2-m3", str(tmp_path))
    assert calls["repo_id"] == "BAAI/bge-reranker-v2-m3"
    # flat dest = <local_dir>/<repo-leaf>, no blobs/snapshots involved
    assert calls["local_dir"].endswith("/bge-reranker-v2-m3")
