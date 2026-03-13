from fastapi import APIRouter, Depends, HTTPException
from psycopg import Connection

from lte_pm_platform.api.dependencies import get_db_connection
from lte_pm_platform.api.schemas.common import HealthResponse, ReadyResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@router.get("/ready", response_model=ReadyResponse)
def ready(connection: Connection = Depends(get_db_connection)) -> ReadyResponse:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            cursor.fetchone()
    except Exception as exc:  # pragma: no cover - defensive API boundary
        raise HTTPException(status_code=503, detail=f"database not ready: {exc}") from exc
    return ReadyResponse(status="ok", database="ok")
