from fastapi import FastAPI

from lte_pm_platform.api.routers.ingestion import router as ingestion_router
from lte_pm_platform.api.routers.kpi import router as kpi_router
from lte_pm_platform.api.routers.system import router as system_router
from lte_pm_platform.api.routers.topology import router as topology_router



def create_app() -> FastAPI:
    app = FastAPI(
        title="LTE PM Platform API",
        version="0.1.0",
        description="Operator-facing API for the LTE PM Data Platform.",
    )
    app.include_router(system_router, prefix="/api/v1", tags=["system"])
    app.include_router(ingestion_router, prefix="/api/v1/ingestion", tags=["ingestion"])
    app.include_router(topology_router, prefix="/api/v1/topology", tags=["topology"])
    app.include_router(kpi_router, prefix="/api/v1", tags=["kpi"])
    return app


app = create_app()
