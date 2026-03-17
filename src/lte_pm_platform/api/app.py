from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from lte_pm_platform.api.routers.ingestion import router as ingestion_router
from lte_pm_platform.api.routers.kpi import router as kpi_router
from lte_pm_platform.api.routers.operations import router as operations_router
from lte_pm_platform.api.routers.system import router as system_router
from lte_pm_platform.api.routers.topology import router as topology_router
from lte_pm_platform.config import get_settings
from lte_pm_platform.db.connection import get_connection
from lte_pm_platform.db.repositories.ftp_cycle_run_repository import FtpCycleRunRepository
from lte_pm_platform.services.ftp_cycle_worker import FtpCycleRunWorker


LOCAL_DEV_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:4173",
    "http://127.0.0.1:4173",
]


STALE_RUN_ERROR_MESSAGE = "Run interrupted by process restart before completion"


def recover_stale_ftp_cycle_runs() -> int:
    settings = get_settings()
    with get_connection(settings) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    to_regclass('public.ftp_cycle_run'),
                    to_regclass('public.ftp_cycle_run_event')
                """
            )
            tables = cursor.fetchone()
            if tables is None or tables[0] is None or tables[1] is None:
                return 0
        repository = FtpCycleRunRepository(connection)
        recovered = repository.recover_stale_running_runs(error_message=STALE_RUN_ERROR_MESSAGE)
    return len(recovered)


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ANN201
    recovered_count = recover_stale_ftp_cycle_runs()
    worker = FtpCycleRunWorker(settings=get_settings())
    worker.start()
    app.state.ftp_cycle_run_worker = worker
    app.state.recovered_ftp_cycle_runs = recovered_count
    try:
        yield
    finally:
        worker.stop()


def create_app() -> FastAPI:
    app = FastAPI(
        title="LTE PM Platform API",
        version="0.1.0",
        description="Operator-facing API for the LTE PM Data Platform.",
        lifespan=lifespan,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=LOCAL_DEV_CORS_ORIGINS,
        allow_credentials=False,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )
    app.include_router(system_router, prefix="/api/v1", tags=["system"])
    app.include_router(ingestion_router, prefix="/api/v1/ingestion", tags=["ingestion"])
    app.include_router(topology_router, prefix="/api/v1/topology", tags=["topology"])
    app.include_router(kpi_router, prefix="/api/v1", tags=["kpi"])
    app.include_router(operations_router, prefix="/api/v1/operations", tags=["operations"])
    return app


app = create_app()
