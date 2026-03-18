from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class FtpRunCycleRequest(BaseModel):
    limit: int = Field(default=20, ge=1, le=1000)
    interval_start: datetime | None = None
    start: date | datetime | None = None
    end: date | datetime | None = None
    revision_policy: Literal["additive", "base-only", "revisions-only", "latest-only"] = "additive"
    families: list[str] | None = None
    dry_run: bool = False
    retry_failed: bool = False

    @model_validator(mode="after")
    def validate_interval_start(self) -> "FtpRunCycleRequest":
        if self.interval_start is None:
            return self
        if self.interval_start.second != 0 or self.interval_start.microsecond != 0:
            raise ValueError("interval_start must align to a 15-minute boundary")
        if self.interval_start.minute not in {0, 15, 30, 45}:
            raise ValueError("interval_start must align to a 15-minute boundary")
        return self


class RetryIdsRequest(BaseModel):
    ids: list[int]

    @model_validator(mode="after")
    def validate_ids(self) -> "RetryIdsRequest":
        if not self.ids:
            raise ValueError("ids must not be empty")
        return self


class EmptyOperationRequest(BaseModel):
    pass


class OperationResponse(BaseModel):
    operation: str
    status: str
    result: dict[str, Any]


class FtpRunEnqueueResponse(BaseModel):
    operation: str
    status: str
    run_id: int
    run: dict[str, Any]


class FtpRunResponse(BaseModel):
    run: dict[str, Any] | None


class FtpRunEventsResponse(BaseModel):
    count: int = Field(ge=0)
    rows: list[dict[str, Any]]
