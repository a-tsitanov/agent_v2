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
    import io
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
