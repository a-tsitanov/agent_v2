"""Processed-message statistics over the `documents` table.

Two read-only endpoints, both API-key gated:
  * GET /stats/messages  — per-channel or per-group × status breakdown
  * GET /stats/timeline  — daily message counts (created_at or doc_date)

All aggregation SQL lives in AsyncPostgres.status_counts_by /
timeline_counts so the CLI (scripts/message_stats.py) shares it verbatim.
"""

from __future__ import annotations

from datetime import date

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from src.api.auth import require_api_key
from src.storage.postgres import AsyncPostgres

router = APIRouter(
    prefix="/stats",
    tags=["stats"],
    dependencies=[Depends(require_api_key)],
)

_GROUP_BY_DIM = {"channel": "source_channel", "group": "source_group"}
_DATE_FIELDS = {"created_at", "doc_date"}


class StatRow(BaseModel):
    key: str
    total: int
    pending: int
    processing: int
    completed: int
    vector_only: int
    failed: int
    skipped: int


class MessagesStatsResponse(BaseModel):
    group_by: str
    rows: list[StatRow]


class TimelineBucket(BaseModel):
    day: date
    key: str | None = None
    count: int


class TimelineResponse(BaseModel):
    date_field: str
    buckets: list[TimelineBucket]


@router.get("/messages", response_model=MessagesStatsResponse,
            summary="Per-channel/group message counts by status")
@inject
async def messages_stats(
    pg: FromDishka[AsyncPostgres],
    group_by: str = "channel",
    since: date | None = None,
    until: date | None = None,
) -> MessagesStatsResponse:
    dimension = _GROUP_BY_DIM.get(group_by)
    if dimension is None:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"group_by must be one of {sorted(_GROUP_BY_DIM)}",
        )
    rows = await pg.status_counts_by(dimension, since=since, until=until)
    return MessagesStatsResponse(group_by=group_by, rows=rows)


@router.get("/timeline", response_model=TimelineResponse,
            summary="Daily message counts")
@inject
async def timeline_stats(
    pg: FromDishka[AsyncPostgres],
    date_field: str = "created_at",
    group_by: str | None = None,
    channel: str | None = None,
    group: str | None = None,
    since: date | None = None,
    until: date | None = None,
) -> TimelineResponse:
    if date_field not in _DATE_FIELDS:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"date_field must be one of {sorted(_DATE_FIELDS)}",
        )
    if group_by is not None and group_by not in _GROUP_BY_DIM:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            f"group_by must be one of {sorted(_GROUP_BY_DIM)}",
        )
    buckets = await pg.timeline_counts(
        date_field=date_field, group_by=group_by,
        channel=channel, group=group, since=since, until=until,
    )
    return TimelineResponse(date_field=date_field, buckets=buckets)
