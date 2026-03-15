from __future__ import annotations

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from psycopg import Connection

from lte_pm_platform.api.dependencies import get_db_connection
from lte_pm_platform.api.schemas.common import RowsResponse
from lte_pm_platform.api.schemas.topology import (
    ReconciliationDetailsResponse,
    SnapshotSummaryResponse,
    TopologyActionResponse,
)
from lte_pm_platform.services.topology_management_service import TopologyManagementService
from lte_pm_platform.services.topology_service import TopologyService

router = APIRouter()


@router.get("/unmapped-entities", response_model=RowsResponse)
def unmapped_entities(
    limit: int = 100,
    connection: Connection = Depends(get_db_connection),
) -> RowsResponse:
    rows = TopologyService(connection).list_unmapped_entities(limit=limit)
    return RowsResponse(count=len(rows), rows=rows)


@router.get("/site-coverage", response_model=RowsResponse)
def site_coverage(
    limit: int = 100,
    connection: Connection = Depends(get_db_connection),
) -> RowsResponse:
    rows = TopologyService(connection).summarize_site_coverage(limit=limit)
    return RowsResponse(count=len(rows), rows=rows)


@router.get("/region-coverage", response_model=RowsResponse)
def region_coverage(
    limit: int = 100,
    connection: Connection = Depends(get_db_connection),
) -> RowsResponse:
    rows = TopologyService(connection).summarize_region_coverage(limit=limit)
    return RowsResponse(count=len(rows), rows=rows)


@router.post("/workbook-preview", response_model=SnapshotSummaryResponse)
async def workbook_preview(
    file: UploadFile = File(...),
    connection: Connection = Depends(get_db_connection),
) -> SnapshotSummaryResponse:
    service = TopologyManagementService(connection)
    try:
        snapshot = service.create_preview_snapshot(source_file_name=file.filename or "topology.xlsx", upload_stream=file.file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - API boundary
        raise HTTPException(status_code=500, detail=f"workbook_preview failed: {exc}") from exc
    return SnapshotSummaryResponse(snapshot=snapshot)


@router.get("/snapshots", response_model=RowsResponse)
def list_snapshots(connection: Connection = Depends(get_db_connection)) -> RowsResponse:
    rows = TopologyManagementService(connection).list_snapshots()
    return RowsResponse(count=len(rows), rows=rows)


@router.get("/snapshots/{snapshot_id}", response_model=SnapshotSummaryResponse)
def get_snapshot_summary(
    snapshot_id: int,
    connection: Connection = Depends(get_db_connection),
) -> SnapshotSummaryResponse:
    snapshot = TopologyManagementService(connection).get_snapshot_summary(snapshot_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"snapshot not found: {snapshot_id}")
    return SnapshotSummaryResponse(snapshot=snapshot)


@router.get("/active-snapshot", response_model=SnapshotSummaryResponse)
def get_active_snapshot(connection: Connection = Depends(get_db_connection)) -> SnapshotSummaryResponse:
    snapshot = TopologyManagementService(connection).get_active_snapshot()
    return SnapshotSummaryResponse(snapshot=snapshot or {})


@router.post("/snapshots/{snapshot_id}/reconcile", response_model=SnapshotSummaryResponse)
def reconcile_snapshot(
    snapshot_id: int,
    connection: Connection = Depends(get_db_connection),
) -> SnapshotSummaryResponse:
    try:
        snapshot = TopologyManagementService(connection).reconcile_snapshot(snapshot_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return SnapshotSummaryResponse(snapshot=snapshot)


@router.get("/reconciliations/{reconciliation_id}/details", response_model=ReconciliationDetailsResponse)
def reconciliation_details(
    reconciliation_id: int,
    issue_type: str | None = Query(default=None),
    limit: int = 100,
    connection: Connection = Depends(get_db_connection),
) -> ReconciliationDetailsResponse:
    rows = TopologyManagementService(connection).get_reconciliation_details(
        reconciliation_id=reconciliation_id,
        issue_type=issue_type,
        limit=limit,
    )
    return ReconciliationDetailsResponse(count=len(rows), rows=rows)


@router.get("/snapshots/{snapshot_id}/drift", response_model=SnapshotSummaryResponse)
def compare_snapshot_to_active(
    snapshot_id: int,
    connection: Connection = Depends(get_db_connection),
) -> SnapshotSummaryResponse:
    snapshot = TopologyManagementService(connection).get_snapshot_summary(snapshot_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"snapshot not found: {snapshot_id}")
    return SnapshotSummaryResponse(snapshot=snapshot)


@router.post("/snapshots/{snapshot_id}/apply", response_model=TopologyActionResponse)
def apply_snapshot(
    snapshot_id: int,
    connection: Connection = Depends(get_db_connection),
) -> TopologyActionResponse:
    service = TopologyManagementService(connection)
    try:
        result = service.apply_snapshot(snapshot_id=snapshot_id, activated_by="api")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - API boundary
        raise HTTPException(status_code=500, detail=f"apply_snapshot failed: {exc}") from exc
    return TopologyActionResponse(action="apply_snapshot", status="SUCCESS", result=result)


@router.post("/sync", response_model=TopologyActionResponse)
def sync_topology(connection: Connection = Depends(get_db_connection)) -> TopologyActionResponse:
    try:
        result = TopologyManagementService(connection).run_sync_topology()
    except Exception as exc:  # pragma: no cover - API boundary
        raise HTTPException(status_code=500, detail=f"sync_topology failed: {exc}") from exc
    return TopologyActionResponse(action="sync_topology", status="SUCCESS", result=result)
