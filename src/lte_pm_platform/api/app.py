from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from lte_pm_platform.api.routers.ingestion import router as ingestion_router
from lte_pm_platform.api.routers.kpi import router as kpi_router
from lte_pm_platform.api.routers.operations import router as operations_router
from lte_pm_platform.api.routers.system import router as system_router
from lte_pm_platform.api.routers.topology import router as topology_router


LOCAL_DEV_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:4173",
    "http://127.0.0.1:4173",
]



def create_app() -> FastAPI:
    app = FastAPI(
        title="LTE PM Platform API",
        version="0.1.0",
        description="Operator-facing API for the LTE PM Data Platform.",
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
