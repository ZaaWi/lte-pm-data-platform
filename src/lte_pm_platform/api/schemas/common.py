from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    service: str = "lte_pm_platform_api"
    status: str


class ReadyResponse(BaseModel):
    service: str = "lte_pm_platform_api"
    status: str
    database: str


class RowsResponse(BaseModel):
    count: int = Field(ge=0)
    rows: list[dict[str, Any]]
