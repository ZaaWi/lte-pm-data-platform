from fastapi import APIRouter, Depends, HTTPException
from psycopg import Connection

from lte_pm_platform.api.dependencies import get_api_settings, get_db_connection
from lte_pm_platform.api.schemas.operations import (
    EmptyOperationRequest,
    FtpRunEnqueueResponse,
    FtpRunEventsResponse,
    FtpRunResponse,
    FtpRunCycleRequest,
    OperationResponse,
    RetryIdsRequest,
)
from lte_pm_platform.config import Settings
from lte_pm_platform.services.operation_service import OperationService, OperationValidationError

router = APIRouter()


def _operation_service(connection: Connection, settings: Settings) -> OperationService:
    return OperationService(connection=connection, settings=settings)


@router.post("/ftp-run-cycle", response_model=FtpRunEnqueueResponse)
def ftp_run_cycle(
    payload: FtpRunCycleRequest,
    connection: Connection = Depends(get_db_connection),
    settings: Settings = Depends(get_api_settings),
) -> FtpRunEnqueueResponse:
    service = _operation_service(connection, settings)
    try:
        run = service.enqueue_ftp_cycle(
            limit=payload.limit,
            start=payload.start,
            end=payload.end,
            revision_policy=payload.revision_policy,
            families=payload.families,
            dry_run=payload.dry_run,
            retry_failed=payload.retry_failed,
            trigger_source="api",
        )
    except OperationValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive API boundary
        raise HTTPException(status_code=500, detail=f"ftp_run_cycle failed: {exc}") from exc
    return FtpRunEnqueueResponse(operation="ftp_run_cycle", status="QUEUED", run_id=run["id"], run=run)


@router.get("/ftp-runs")
def list_ftp_runs(
    limit: int = 20,
    status: str | None = None,
    connection: Connection = Depends(get_db_connection),
    settings: Settings = Depends(get_api_settings),
):  # noqa: ANN201
    service = _operation_service(connection, settings)
    statuses = [status] if status else None
    rows = service.list_ftp_cycle_runs(limit=limit, statuses=statuses)
    return {"count": len(rows), "rows": rows}


@router.get("/ftp-runs/{run_id}", response_model=FtpRunResponse)
def get_ftp_run(
    run_id: int,
    connection: Connection = Depends(get_db_connection),
    settings: Settings = Depends(get_api_settings),
) -> FtpRunResponse:
    service = _operation_service(connection, settings)
    return FtpRunResponse(run=service.get_ftp_cycle_run(run_id=run_id))


@router.get("/ftp-runs/{run_id}/events", response_model=FtpRunEventsResponse)
def get_ftp_run_events(
    run_id: int,
    limit: int = 200,
    connection: Connection = Depends(get_db_connection),
    settings: Settings = Depends(get_api_settings),
) -> FtpRunEventsResponse:
    service = _operation_service(connection, settings)
    rows = service.list_ftp_cycle_run_events(run_id=run_id, limit=limit)
    return FtpRunEventsResponse(count=len(rows), rows=rows)


@router.post("/ftp-retry-download", response_model=OperationResponse)
def ftp_retry_download(
    payload: RetryIdsRequest,
    connection: Connection = Depends(get_db_connection),
    settings: Settings = Depends(get_api_settings),
) -> OperationResponse:
    service = _operation_service(connection, settings)
    try:
        result = service.retry_download(ids=payload.ids)
    except OperationValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive API boundary
        raise HTTPException(status_code=500, detail=f"ftp_retry_download failed: {exc}") from exc
    return OperationResponse(operation="ftp_retry_download", status="SUCCESS", result=result)


@router.post("/ftp-retry-ingest", response_model=OperationResponse)
def ftp_retry_ingest(
    payload: RetryIdsRequest,
    connection: Connection = Depends(get_db_connection),
    settings: Settings = Depends(get_api_settings),
) -> OperationResponse:
    service = _operation_service(connection, settings)
    try:
        result = service.retry_ingest(ids=payload.ids)
    except OperationValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive API boundary
        raise HTTPException(status_code=500, detail=f"ftp_retry_ingest failed: {exc}") from exc
    return OperationResponse(operation="ftp_retry_ingest", status="SUCCESS", result=result)


@router.post("/sync-entities", response_model=OperationResponse)
def sync_entities(
    payload: EmptyOperationRequest,
    connection: Connection = Depends(get_db_connection),
    settings: Settings = Depends(get_api_settings),
) -> OperationResponse:
    del payload
    service = _operation_service(connection, settings)
    try:
        result = service.sync_entities()
    except Exception as exc:  # pragma: no cover - defensive API boundary
        raise HTTPException(status_code=500, detail=f"sync_entities failed: {exc}") from exc
    return OperationResponse(operation="sync_entities", status="SUCCESS", result=result)


@router.post("/sync-topology", response_model=OperationResponse)
def sync_topology(
    payload: EmptyOperationRequest,
    connection: Connection = Depends(get_db_connection),
    settings: Settings = Depends(get_api_settings),
) -> OperationResponse:
    del payload
    service = _operation_service(connection, settings)
    try:
        result = service.sync_topology()
    except Exception as exc:  # pragma: no cover - defensive API boundary
        raise HTTPException(status_code=500, detail=f"sync_topology failed: {exc}") from exc
    return OperationResponse(operation="sync_topology", status="SUCCESS", result=result)
