from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class FtpRunCycleRequest(BaseModel):
    limit: int = Field(default=20, ge=1, le=1000)
    start: date | datetime | None = None
    end: date | datetime | None = None
    revision_policy: Literal["additive", "base-only", "revisions-only", "latest-only"] = "additive"
    families: list[str] | None = None
    dry_run: bool = False
    retry_failed: bool = False


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
