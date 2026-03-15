from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class SnapshotSummaryResponse(BaseModel):
    snapshot: dict[str, Any]


class TopologyActionResponse(BaseModel):
    action: str
    status: str
    result: dict[str, Any]


class ReconciliationDetailsResponse(BaseModel):
    count: int = Field(ge=0)
    rows: list[dict[str, Any]]
