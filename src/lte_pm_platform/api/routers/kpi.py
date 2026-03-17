from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg import Connection

from lte_pm_platform.api.dependencies import get_db_connection
from lte_pm_platform.api.schemas.common import RowsResponse
from lte_pm_platform.api.schemas.kpi import KpiFamily
from lte_pm_platform.services.kpi_service import KpiService

router = APIRouter()


@router.get("/kpi-results/entity-time", response_model=RowsResponse)
def kpi_results_entity_time(
    family: KpiFamily,
    limit: int = 100,
    offset: int = 0,
    dataset_family: str | None = None,
    collect_time_from: datetime | None = Query(default=None),
    collect_time_to: datetime | None = Query(default=None),
    connection: Connection = Depends(get_db_connection),
) -> RowsResponse:
    try:
        rows = KpiService(connection).list_results(
            family=family,
            grain="entity-time",
            limit=limit,
            offset=offset,
            dataset_family=dataset_family,
            site_code=None,
            region_code=None,
            collect_time_from=collect_time_from,
            collect_time_to=collect_time_to,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RowsResponse(count=len(rows), rows=rows)


@router.get("/kpi-results/site-time", response_model=RowsResponse)
def kpi_results_site_time(
    family: KpiFamily,
    limit: int = 100,
    offset: int = 0,
    dataset_family: str | None = None,
    site_code: str | None = None,
    collect_time_from: datetime | None = Query(default=None),
    collect_time_to: datetime | None = Query(default=None),
    connection: Connection = Depends(get_db_connection),
) -> RowsResponse:
    try:
        rows = KpiService(connection).list_results(
            family=family,
            grain="site-time",
            limit=limit,
            offset=offset,
            dataset_family=dataset_family,
            site_code=site_code,
            region_code=None,
            collect_time_from=collect_time_from,
            collect_time_to=collect_time_to,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RowsResponse(count=len(rows), rows=rows)


@router.get("/kpi-results/region-time", response_model=RowsResponse)
def kpi_results_region_time(
    family: KpiFamily,
    limit: int = 100,
    offset: int = 0,
    dataset_family: str | None = None,
    region_code: str | None = None,
    collect_time_from: datetime | None = Query(default=None),
    collect_time_to: datetime | None = Query(default=None),
    connection: Connection = Depends(get_db_connection),
) -> RowsResponse:
    try:
        rows = KpiService(connection).list_results(
            family=family,
            grain="region-time",
            limit=limit,
            offset=offset,
            dataset_family=dataset_family,
            site_code=None,
            region_code=region_code,
            collect_time_from=collect_time_from,
            collect_time_to=collect_time_to,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RowsResponse(count=len(rows), rows=rows)


@router.get("/kpi-validation/entity-time", response_model=RowsResponse)
def kpi_validation_entity_time(
    family: KpiFamily,
    dataset_family: str | None = None,
    connection: Connection = Depends(get_db_connection),
) -> RowsResponse:
    try:
        rows = KpiService(connection).list_validation_filtered(
            family=family,
            grain="entity-time",
            dataset_family=dataset_family,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RowsResponse(count=len(rows), rows=rows)


@router.get("/kpi-validation/site-time", response_model=RowsResponse)
def kpi_validation_site_time(
    family: KpiFamily,
    dataset_family: str | None = None,
    connection: Connection = Depends(get_db_connection),
) -> RowsResponse:
    try:
        rows = KpiService(connection).list_validation_filtered(
            family=family,
            grain="site-time",
            dataset_family=dataset_family,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RowsResponse(count=len(rows), rows=rows)


@router.get("/kpi-validation/region-time", response_model=RowsResponse)
def kpi_validation_region_time(
    family: KpiFamily,
    dataset_family: str | None = None,
    connection: Connection = Depends(get_db_connection),
) -> RowsResponse:
    try:
        rows = KpiService(connection).list_validation_filtered(
            family=family,
            grain="region-time",
            dataset_family=dataset_family,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return RowsResponse(count=len(rows), rows=rows)
