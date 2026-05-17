"""Claim-check store for workflow stage outputs.

Activities pickle their large in-memory state (parsed LlamaIndex
nodes, KG entity/relation lists) to MinIO under
``s3://{bucket}/{workflow_run_id}/{stage}.pkl`` and pass only the URI
to the next activity.  ``finalize`` (and the failure path) removes
the whole ``{workflow_run_id}/`` prefix so we don't accrete blobs.

Pickle is fine here because:
  * the producer and consumer share the same Python image,
  * blobs are short-lived (lifetime of one workflow run),
  * the on-disk format is never read by anything outside this
    package.
"""

from __future__ import annotations

import io
import pickle
from datetime import UTC, datetime, timedelta
from typing import Any

from loguru import logger
from minio import Minio

from src.config import settings


class StagingStore:
    """Thin wrapper around the MinIO client for stage blobs."""

    def __init__(self, client: Minio, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    def write_pickle(self, run_id: str, stage: str, obj: Any) -> str:
        """Pickle `obj` and upload to ``{run_id}/{stage}.pkl``.

        Returns the full ``s3://`` URI suitable for handing to the
        next activity.
        """
        blob = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        key = f"{run_id}/{stage}.pkl"
        self._client.put_object(
            self._bucket, key, io.BytesIO(blob), len(blob),
            content_type="application/octet-stream",
        )
        uri = f"s3://{self._bucket}/{key}"
        logger.info(
            "staging write  run={r}  stage={s}  uri={u}  bytes={n}",
            r=run_id, s=stage, u=uri, n=len(blob),
        )
        return uri

    def read_pickle(self, uri: str) -> Any:
        """Reverse of `write_pickle`."""
        bucket, key = _parse_uri(uri)
        if bucket != self._bucket:
            raise ValueError(
                f"wrong bucket for staging read: {bucket!r} vs {self._bucket!r}",
            )
        response = self._client.get_object(bucket, key)
        try:
            blob = response.read()
        finally:
            response.close()
            response.release_conn()
        return pickle.loads(blob)

    def delete_prefix(self, run_id: str) -> None:
        """Best-effort cleanup of every blob under ``{run_id}/``."""
        prefix = f"{run_id}/"
        for obj in self._client.list_objects(
            self._bucket, prefix=prefix, recursive=True,
        ):
            self._client.remove_object(self._bucket, obj.object_name)

    def list_orphan_runs(self, older_than_hours: int) -> list[str]:
        """Return ``run_id`` prefixes whose newest blob is older than
        ``older_than_hours``.

        ``finalize`` and ``mark_failed`` activities call
        ``delete_prefix`` on their own ``workflow_run_id`` at end of
        workflow, so anything left behind under a stale prefix points
        at a workflow that died before either of those activities ran
        (worker OOM during ``index_vector``, network blip cancelling
        the run, etc).  The threshold keeps still-in-flight workflows
        safe.
        """
        threshold = datetime.now(UTC) - timedelta(hours=older_than_hours)
        newest_by_run: dict[str, datetime] = {}
        for obj in self._client.list_objects(
            self._bucket, recursive=True,
        ):
            head, _, _ = obj.object_name.partition("/")
            if not head:
                continue
            lm = obj.last_modified
            if lm is None:
                continue
            if lm.tzinfo is None:
                lm = lm.replace(tzinfo=UTC)
            if lm > newest_by_run.get(head, datetime.min.replace(tzinfo=UTC)):
                newest_by_run[head] = lm
        return [
            run_id for run_id, lm in newest_by_run.items()
            if lm < threshold
        ]

    def cleanup_orphans(self, older_than_hours: int = 24) -> list[str]:
        """Find and delete orphaned ``{run_id}/`` prefixes.  Returns
        the list of run_ids that were removed."""
        orphans = self.list_orphan_runs(older_than_hours=older_than_hours)
        for run_id in orphans:
            logger.info(
                "staging cleanup  run_id={r}  reason=orphan_>{h}h",
                r=run_id, h=older_than_hours,
            )
            self.delete_prefix(run_id)
        return orphans


def _parse_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3 uri: {uri!r}")
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise ValueError(f"malformed s3 uri: {uri!r}")
    return bucket, key


def build_staging_store() -> StagingStore:
    """Construct a StagingStore from the project settings.

    Reuses the same MinIO endpoint as ``build_minio_storage`` — the
    bucket is the only difference.  Ensures the bucket exists.
    """
    cfg = settings.minio
    client = Minio(
        cfg.endpoint,
        access_key=cfg.access_key.get_secret_value(),
        secret_key=cfg.secret_key.get_secret_value(),
        secure=cfg.secure,
        region=cfg.region,
    )
    bucket = settings.temporal.staging_bucket
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("staging  bucket created  name={b}", b=bucket)
    return StagingStore(client=client, bucket=bucket)
