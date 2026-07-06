"""Staging helper persists pickled objects to MinIO `kb-staging` and
returns an s3:// URI.  Tests use a MagicMock MinIO client to keep
this layer unit-testable; live behaviour is exercised in
tests/test_workflow/test_workflow_local.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.workflow.staging import StagingStore


def test_write_returns_s3_uri() -> None:
    client = MagicMock()
    store = StagingStore(client=client, bucket="kb-staging")
    uri = store.write_pickle("run-1", "parsed", {"hello": "world"})
    assert uri == "s3://kb-staging/run-1/parsed.pkl"
    client.put_object.assert_called_once()
    args, kwargs = client.put_object.call_args
    # bucket, key, stream, length, content_type=...
    assert args[0] == "kb-staging"
    assert args[1] == "run-1/parsed.pkl"


def test_read_pickle_roundtrips_object() -> None:
    import pickle

    payload = {"answer": 42, "nodes": [1, 2, 3]}
    blob = pickle.dumps(payload)

    client = MagicMock()
    response = MagicMock()
    response.read.return_value = blob
    response.close = MagicMock()
    response.release_conn = MagicMock()
    client.get_object.return_value = response

    store = StagingStore(client=client, bucket="kb-staging")
    out = store.read_pickle("s3://kb-staging/run-1/parsed.pkl")
    assert out == payload
    client.get_object.assert_called_once_with("kb-staging", "run-1/parsed.pkl")


def test_delete_prefix_lists_then_removes() -> None:
    client = MagicMock()
    obj1 = MagicMock(object_name="run-1/parsed.pkl")
    obj2 = MagicMock(object_name="run-1/kg.pkl")
    client.list_objects.return_value = [obj1, obj2]

    store = StagingStore(client=client, bucket="kb-staging")
    store.delete_prefix("run-1")

    client.list_objects.assert_called_once_with(
        "kb-staging", prefix="run-1/", recursive=True,
    )
    assert client.remove_object.call_count == 2


def test_read_pickle_rejects_wrong_bucket() -> None:
    client = MagicMock()
    store = StagingStore(client=client, bucket="kb-staging")
    with pytest.raises(ValueError, match="wrong bucket"):
        store.read_pickle("s3://kb-uploads/run-1/parsed.pkl")


def test_list_orphan_runs_groups_by_prefix_and_filters_by_age() -> None:
    from datetime import UTC, datetime, timedelta

    now = datetime.now(UTC)
    fresh = now - timedelta(hours=1)
    stale = now - timedelta(hours=25)
    very_stale = now - timedelta(hours=72)

    client = MagicMock()
    client.list_objects.return_value = [
        MagicMock(object_name="run-fresh/parsed.pkl", last_modified=fresh),
        MagicMock(object_name="run-fresh/kg.pkl", last_modified=fresh),
        MagicMock(object_name="run-stale/parsed.pkl", last_modified=stale),
        MagicMock(object_name="run-mixed/parsed.pkl", last_modified=very_stale),
        # run-mixed has one fresh blob — must NOT be classified as orphan.
        MagicMock(object_name="run-mixed/kg.pkl", last_modified=fresh),
        MagicMock(object_name="run-ancient/x.pkl", last_modified=very_stale),
    ]

    store = StagingStore(client=client, bucket="kb-staging")
    orphans = store.list_orphan_runs(older_than_hours=24)

    assert sorted(orphans) == ["run-ancient", "run-stale"]


def test_cleanup_orphans_deletes_each_prefix() -> None:
    from datetime import UTC, datetime, timedelta

    stale = datetime.now(UTC) - timedelta(hours=48)
    client = MagicMock()
    # First call: list_orphan_runs walks everything.
    # Subsequent calls: delete_prefix walks per-prefix.
    list_calls = []

    def _list(bucket, **kw):
        list_calls.append(kw)
        if "prefix" not in kw:
            return [
                MagicMock(object_name="run-a/parsed.pkl", last_modified=stale),
                MagicMock(object_name="run-b/parsed.pkl", last_modified=stale),
            ]
        # delete_prefix listing
        prefix = kw["prefix"]
        return [MagicMock(object_name=f"{prefix}parsed.pkl")]

    client.list_objects.side_effect = _list

    store = StagingStore(client=client, bucket="kb-staging")
    deleted = store.cleanup_orphans(older_than_hours=24)

    assert sorted(deleted) == ["run-a", "run-b"]
    # Two delete_prefix calls → two remove_object calls.
    assert client.remove_object.call_count == 2
