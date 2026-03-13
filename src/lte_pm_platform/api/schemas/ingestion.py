from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class IngestionStatusResponse(BaseModel):
    status_counts: list[dict[str, Any]]
    summary: dict[str, Any]
    latest_scan_at: Any | None = None
    recent_failures: list[dict[str, Any]]


class FailureDetailResponse(BaseModel):
    row: dict[str, Any] | None
