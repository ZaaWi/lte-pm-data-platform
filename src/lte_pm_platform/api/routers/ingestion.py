from fastapi import APIRouter, Depends
from psycopg import Connection

from lte_pm_platform.api.dependencies import get_db_connection
from lte_pm_platform.api.schemas.common import RowsResponse
from lte_pm_platform.api.schemas.ingestion import FailureDetailResponse, IngestionStatusResponse
from lte_pm_platform.services.ingestion_service import IngestionService

router = APIRouter()


@router.get("/status", response_model=IngestionStatusResponse)
def ingestion_status(
    limit_recent_failures: int = 10,
    connection: Connection = Depends(get_db_connection),
) -> IngestionStatusResponse:
    payload = IngestionService(connection).get_status(limit_recent_failures=limit_recent_failures)
    return IngestionStatusResponse(**payload)


@router.get("/failures", response_model=RowsResponse)
def ingestion_failures(
    limit: int = 100,
    connection: Connection = Depends(get_db_connection),
) -> RowsResponse:
    rows = IngestionService(connection).list_failures(limit=limit)
    return RowsResponse(count=len(rows), rows=rows)


@router.get("/failures/{remote_file_id}", response_model=FailureDetailResponse)
def ingestion_failure_detail(
    remote_file_id: int,
    connection: Connection = Depends(get_db_connection),
) -> FailureDetailResponse:
    row = IngestionService(connection).get_failure(remote_file_id=remote_file_id)
    return FailureDetailResponse(row=row)


@router.get("/reconciliation-preview", response_model=RowsResponse)
def ingestion_reconciliation_preview(
    limit: int = 100,
    connection: Connection = Depends(get_db_connection),
) -> RowsResponse:
    rows = IngestionService(connection).get_reconciliation_preview(limit=limit)
    return RowsResponse(count=len(rows), rows=rows)
