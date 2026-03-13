from fastapi import APIRouter, Depends
from psycopg import Connection

from lte_pm_platform.api.dependencies import get_db_connection
from lte_pm_platform.api.schemas.common import RowsResponse
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
