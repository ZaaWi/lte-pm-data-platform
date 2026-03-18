from __future__ import annotations

import importlib
from datetime import datetime

import pytest
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from lte_pm_platform.api.app import LOCAL_DEV_CORS_ORIGINS, STALE_RUN_ERROR_MESSAGE, create_app, recover_stale_ftp_cycle_runs
from lte_pm_platform.api.routers.ingestion import (
    ingestion_failure_detail,
    ingestion_failures,
    ingestion_reconciliation_preview,
    ingestion_source_intervals,
    ingestion_status,
)
from lte_pm_platform.api.routers.kpi import (
    kpi_results_entity_time,
    kpi_results_region_time,
    kpi_results_site_time,
    kpi_validation_entity_time,
    kpi_validation_region_time,
    kpi_validation_site_time,
)
from lte_pm_platform.api.routers.operations import (
    get_ftp_run,
    get_ftp_run_events,
    ftp_retry_download,
    ftp_retry_ingest,
    ftp_run_cycle,
    list_ftp_runs,
    sync_entities,
    sync_topology,
)
from lte_pm_platform.api.routers.system import health, ready
from lte_pm_platform.api.routers.topology import (
    get_active_snapshot,
    get_snapshot_summary,
    list_snapshots,
    reconcile_snapshot,
    region_coverage,
    site_coverage,
    topology_summary,
    unmapped_entities,
)
from lte_pm_platform.api.schemas.operations import EmptyOperationRequest, FtpRunCycleRequest, RetryIdsRequest
from lte_pm_platform.config import Settings
from lte_pm_platform.services.ingestion_service import IngestionService
from lte_pm_platform.services.kpi_service import KpiService
from lte_pm_platform.services.operation_service import OperationService, OperationValidationError
from lte_pm_platform.services.topology_management_service import TopologyManagementService
from lte_pm_platform.services.topology_service import TopologyService


class FakeCursor:
    def execute(self, query: str) -> None:
        self.query = query

    def fetchone(self):  # noqa: ANN201
        return (1,)

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class FakeConnection:
    def cursor(self):  # noqa: ANN201
        return FakeCursor()


def make_settings() -> Settings:
    return Settings(
        postgres_db="d",
        postgres_user="u",
        postgres_password="p",
        postgres_host="h",
        postgres_port=5432,
        ftp_host="ftp",
        ftp_port=21,
        ftp_username="user",
        ftp_password="pass",
        ftp_remote_directory="/",
        ftp_remote_directories=("/",),
        ftp_passive_mode=True,
    )


def test_create_app_registers_expected_routes() -> None:
    app = create_app()
    paths = {route.path for route in app.routes}

    assert "/api/v1/health" in paths
    assert "/api/v1/ready" in paths
    assert "/api/v1/ingestion/status" in paths
    assert "/api/v1/ingestion/source-intervals" in paths
    assert "/api/v1/topology/site-coverage" in paths
    assert "/api/v1/topology/workbook-preview" in paths
    assert "/api/v1/topology/snapshots" in paths
    assert "/api/v1/kpi-results/entity-time" in paths
    assert "/api/v1/kpi-validation/region-time" in paths
    assert "/api/v1/operations/ftp-run-cycle" in paths
    assert "/api/v1/operations/ftp-runs" in paths
    assert "/api/v1/operations/ftp-runs/{run_id}" in paths
    assert "/api/v1/operations/sync-topology" in paths


def test_create_app_registers_local_dev_cors_middleware() -> None:
    app = create_app()
    cors_middleware = next((middleware for middleware in app.user_middleware if middleware.cls is CORSMiddleware), None)

    assert cors_middleware is not None
    assert cors_middleware.kwargs["allow_origins"] == LOCAL_DEV_CORS_ORIGINS
    assert cors_middleware.kwargs["allow_methods"] == ["GET", "POST", "OPTIONS"]
    assert cors_middleware.kwargs["allow_headers"] == ["*"]


def test_recover_stale_ftp_cycle_runs(monkeypatch) -> None:  # noqa: ANN001
    class RecoveryCursor:
        def execute(self, query: str) -> None:
            self.query = query

        def fetchone(self):  # noqa: ANN201
            return ("ftp_cycle_run", "ftp_cycle_run_event")

        def __enter__(self):  # noqa: ANN201
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
            return None

    class FakeRepo:
        def __init__(self, connection) -> None:  # noqa: ANN001
            self.connection = connection

        def recover_stale_running_runs(self, *, error_message: str):  # noqa: ANN202
            assert error_message == STALE_RUN_ERROR_MESSAGE
            return [{"id": 1}, {"id": 2}]

    class FakeContext:
        def __enter__(self):  # noqa: ANN201
            return type("Connection", (), {"cursor": lambda self: RecoveryCursor()})()

        def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
            return None

    app_module = importlib.import_module("lte_pm_platform.api.app")
    monkeypatch.setattr(app_module, "get_settings", make_settings)
    monkeypatch.setattr(app_module, "get_connection", lambda settings: FakeContext())
    monkeypatch.setattr(app_module, "FtpCycleRunRepository", FakeRepo)

    assert recover_stale_ftp_cycle_runs() == 2


def test_recover_stale_ftp_cycle_runs_skips_when_tables_missing(monkeypatch) -> None:  # noqa: ANN001
    class MissingCursor:
        def execute(self, query: str) -> None:
            self.query = query

        def fetchone(self):  # noqa: ANN201
            return (None, None)

        def __enter__(self):  # noqa: ANN201
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
            return None

    class MissingConnection:
        def cursor(self):  # noqa: ANN201
            return MissingCursor()

    class FakeContext:
        def __enter__(self):  # noqa: ANN201
            return MissingConnection()

        def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
            return None

    app_module = importlib.import_module("lte_pm_platform.api.app")
    monkeypatch.setattr(app_module, "get_settings", make_settings)
    monkeypatch.setattr(app_module, "get_connection", lambda settings: FakeContext())

    assert recover_stale_ftp_cycle_runs() == 0


def test_health() -> None:
    response = health()
    assert response.status == "ok"


def test_ready() -> None:
    response = ready(connection=FakeConnection())
    assert response.database == "ok"


def test_ingestion_status(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        IngestionService,
        "get_status",
        lambda self, limit_recent_failures: {
            "status_counts": [{"status": "DISCOVERED", "file_count": 1}],
            "summary": {"pending_downloads": 1},
            "latest_scan_at": None,
            "recent_failures": [],
        },
    )

    response = ingestion_status(limit_recent_failures=10, connection=FakeConnection())

    assert response.summary["pending_downloads"] == 1


def test_ingestion_failures(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        IngestionService,
        "list_failures",
        lambda self, limit: [{"id": 1, "status": "FAILED_INGEST"}],
    )

    response = ingestion_failures(limit=5, connection=FakeConnection())

    assert response.count == 1


def test_ingestion_failure_detail(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        IngestionService,
        "get_failure",
        lambda self, remote_file_id: {"id": remote_file_id, "classification": "retryable_ingest"},
    )

    response = ingestion_failure_detail(remote_file_id=42, connection=FakeConnection())

    assert response.row["id"] == 42


def test_reconciliation_preview(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        IngestionService,
        "get_reconciliation_preview",
        lambda self, limit: [{"id": 7, "classification": "reconciliation_needed"}],
    )

    response = ingestion_reconciliation_preview(limit=10, connection=FakeConnection())

    assert response.rows[0]["classification"] == "reconciliation_needed"


def test_ingestion_source_intervals(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        IngestionService,
        "list_source_intervals",
        lambda self, limit: [
            {
                "interval_start": "2026-03-05T00:15:00",
                "total_files": 3,
                "families_present": ["PM/itbbu/ltefdd", "PM/sdr/ltefdd"],
                "family_count": 2,
                "statuses_present": ["DISCOVERED", "INGESTED"],
                "max_revision": 1,
                "last_seen_at": "2026-03-18T10:12:30",
                "last_scan_at": "2026-03-18T10:12:30",
            }
        ],
    )

    response = ingestion_source_intervals(limit=25, connection=FakeConnection())

    assert response.count == 1
    assert response.rows[0]["interval_start"] == "2026-03-05T00:15:00"
    assert response.rows[0]["family_count"] == 2


def test_unmapped_entities(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        TopologyService,
        "list_unmapped_entities",
        lambda self, limit: [{"logical_entity_key": "k1"}],
    )

    response = unmapped_entities(limit=10, connection=FakeConnection())

    assert response.rows[0]["logical_entity_key"] == "k1"


def test_site_coverage(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        TopologyService,
        "summarize_site_coverage",
        lambda self, limit: [{"site_code": "S1"}],
    )

    response = site_coverage(limit=10, connection=FakeConnection())

    assert response.rows[0]["site_code"] == "S1"


def test_region_coverage(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        TopologyService,
        "summarize_region_coverage",
        lambda self, limit: [{"region_code": "R1"}],
    )

    response = region_coverage(limit=10, connection=FakeConnection())

    assert response.rows[0]["region_code"] == "R1"


def test_topology_summary(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        TopologyService,
        "summarize_topology_overview",
        lambda self: {"total_entities": 10, "distinct_sites": 5},
    )

    response = topology_summary(connection=FakeConnection())

    assert response["summary"]["total_entities"] == 10


def test_list_topology_snapshots(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        TopologyManagementService,
        "list_snapshots",
        lambda self: [{"snapshot_id": 1, "status": "previewed"}],
    )

    response = list_snapshots(connection=FakeConnection())

    assert response.rows[0]["snapshot_id"] == 1


def test_get_topology_snapshot_summary(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        TopologyManagementService,
        "get_snapshot_summary",
        lambda self, snapshot_id: {"snapshot_id": snapshot_id, "status": "previewed"},
    )

    response = get_snapshot_summary(snapshot_id=7, connection=FakeConnection())

    assert response.snapshot["snapshot_id"] == 7


def test_get_active_topology_snapshot(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        TopologyManagementService,
        "get_active_snapshot",
        lambda self: {"snapshot_id": 3, "is_active_snapshot": True},
    )

    response = get_active_snapshot(connection=FakeConnection())

    assert response.snapshot["snapshot_id"] == 3


def test_reconcile_topology_snapshot(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        TopologyManagementService,
        "reconcile_snapshot",
        lambda self, snapshot_id: {"snapshot_id": snapshot_id, "status": "reconciled"},
    )

    response = reconcile_snapshot(snapshot_id=9, connection=FakeConnection())

    assert response.snapshot["status"] == "reconciled"


def test_kpi_results_entity_time(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        KpiService,
        "list_results",
        lambda self, **kwargs: [{"dataset_family": "PM/sdr/ltefdd", "kpi_code": "dl_prb_utilization"}],
    )

    response = kpi_results_entity_time(
        family="prb",
        limit=5,
        dataset_family="PM/sdr/ltefdd",
        collect_time_from=None,
        collect_time_to=None,
        connection=FakeConnection(),
    )

    assert response.rows[0]["kpi_code"] == "dl_prb_utilization"


def test_kpi_results_entity_time_missing_dataset_family_returns_400(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        KpiService,
        "list_results",
        lambda self, **kwargs: (_ for _ in ()).throw(ValueError("dataset_family is required for entity-time KPI results")),
    )

    with pytest.raises(Exception) as exc_info:
        kpi_results_entity_time(
            family="prb",
            limit=5,
            dataset_family=None,
            collect_time_from=None,
            collect_time_to=None,
            connection=FakeConnection(),
        )

    assert exc_info.value.status_code == 400


def test_kpi_results_site_time(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        KpiService,
        "list_results",
        lambda self, **kwargs: [{"dataset_family": "PM/sdr/ltefdd", "site_code": "S1"}],
    )

    response = kpi_results_site_time(family="bler", limit=5, dataset_family=None, site_code="S1", collect_time_from=None, collect_time_to=None, connection=FakeConnection())

    assert response.rows[0]["site_code"] == "S1"


def test_kpi_results_site_time_requires_dataset_family_for_prb_bler(monkeypatch) -> None:  # noqa: ANN001
    def raise_error(self, **kwargs):  # noqa: ANN001, ANN202
        raise ValueError("dataset_family is required for site-time and region-time KPI results")

    monkeypatch.setattr(KpiService, "list_results", raise_error)

    with pytest.raises(Exception) as exc_info:
        kpi_results_site_time(
            family="prb",
            limit=5,
            dataset_family=None,
            site_code="S1",
            collect_time_from=None,
            collect_time_to=None,
            connection=FakeConnection(),
        )

    assert exc_info.value.status_code == 400


def test_kpi_results_region_time(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        KpiService,
        "list_results",
        lambda self, **kwargs: [{"dataset_family": "PM/sdr/ltefdd", "region_code": "R1"}],
    )

    response = kpi_results_region_time(family="rrc", limit=5, dataset_family=None, region_code="R1", collect_time_from=None, collect_time_to=None, connection=FakeConnection())

    assert response.rows[0]["region_code"] == "R1"


def test_kpi_validation_entity_time(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        KpiService,
        "list_validation_filtered",
        lambda self, **kwargs: [{"dataset_family": "PM/sdr/ltefdd", "entity_time_rows": 10}],
    )

    response = kpi_validation_entity_time(family="rrc", dataset_family=None, connection=FakeConnection())

    assert response.rows[0]["entity_time_rows"] == 10


def test_kpi_validation_site_time(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        KpiService,
        "list_validation_filtered",
        lambda self, **kwargs: [{"dataset_family": "PM/sdr/ltefdd", "site_time_rows": 8}],
    )

    response = kpi_validation_site_time(family="prb", dataset_family="PM/sdr/ltefdd", connection=FakeConnection())

    assert response.rows[0]["site_time_rows"] == 8


def test_kpi_validation_region_time(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        KpiService,
        "list_validation_filtered",
        lambda self, **kwargs: [{"dataset_family": "PM/sdr/ltefdd", "region_time_rows": 4}],
    )

    response = kpi_validation_region_time(family="bler", dataset_family="PM/sdr/ltefdd", connection=FakeConnection())

    assert response.rows[0]["region_time_rows"] == 4


def test_kpi_validation_site_time_requires_dataset_family(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        KpiService,
        "list_validation_filtered",
        lambda self, **kwargs: (_ for _ in ()).throw(ValueError("dataset_family is required for site-time and region-time KPI validation")),
    )

    with pytest.raises(Exception) as exc_info:
        kpi_validation_site_time(family="prb", dataset_family=None, connection=FakeConnection())

    assert exc_info.value.status_code == 400


def test_ftp_run_cycle_route(monkeypatch) -> None:  # noqa: ANN001
    captured: dict[str, object] = {}

    def fake_enqueue(self, **kwargs):  # noqa: ANN001, ANN202
        captured.update(kwargs)
        return {"id": 12, "requested_at": "2026-03-16T12:00:00", "status": "queued"}

    monkeypatch.setattr(
        OperationService,
        "enqueue_ftp_cycle",
        fake_enqueue,
    )

    response = ftp_run_cycle(
        payload=FtpRunCycleRequest(interval_start=datetime(2026, 3, 5, 0, 15)),
        connection=FakeConnection(),
        settings=make_settings(),
    )

    assert response.operation == "ftp_run_cycle"
    assert response.run_id == 12
    assert response.status == "QUEUED"
    assert captured["interval_start"] == datetime(2026, 3, 5, 0, 15)


def test_ftp_run_cycle_route_validation_error(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        OperationService,
        "enqueue_ftp_cycle",
        lambda self, **kwargs: (_ for _ in ()).throw(OperationValidationError("bad request")),
    )

    with pytest.raises(Exception) as exc_info:
            ftp_run_cycle(
                payload=FtpRunCycleRequest(),
                connection=FakeConnection(),
                settings=make_settings(),
            )

    assert exc_info.value.status_code == 400


def test_ftp_run_cycle_request_rejects_invalid_interval_start() -> None:
    with pytest.raises(ValidationError, match="interval_start must align to a 15-minute boundary"):
        FtpRunCycleRequest(interval_start=datetime(2026, 3, 5, 0, 7))


def test_list_ftp_runs_route(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        OperationService,
        "list_ftp_cycle_runs",
        lambda self, **kwargs: [{"id": 1, "status": "running"}],
    )

    response = list_ftp_runs(limit=10, status="running", connection=FakeConnection(), settings=make_settings())

    assert response["count"] == 1
    assert response["rows"][0]["id"] == 1


def test_get_ftp_run_route(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        OperationService,
        "get_ftp_cycle_run",
        lambda self, **kwargs: {"id": 7, "status": "succeeded"},
    )

    response = get_ftp_run(run_id=7, connection=FakeConnection(), settings=make_settings())

    assert response.run["status"] == "succeeded"


def test_get_ftp_run_events_route(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        OperationService,
        "list_ftp_cycle_run_events",
        lambda self, **kwargs: [{"id": 1, "stage": "discover"}],
    )

    response = get_ftp_run_events(run_id=7, limit=10, connection=FakeConnection(), settings=make_settings())

    assert response.count == 1
    assert response.rows[0]["stage"] == "discover"


def test_ftp_retry_download_route(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        OperationService,
        "retry_download",
        lambda self, **kwargs: {"results": [{"remote_file_id": 1}]},
    )

    response = ftp_retry_download(
        payload=RetryIdsRequest(ids=[1]),
        connection=FakeConnection(),
        settings=make_settings(),
    )

    assert response.operation == "ftp_retry_download"
    assert response.result["results"][0]["remote_file_id"] == 1


def test_ftp_retry_ingest_route(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        OperationService,
        "retry_ingest",
        lambda self, **kwargs: {"results": [{"remote_file_id": 2}]},
    )

    response = ftp_retry_ingest(
        payload=RetryIdsRequest(ids=[2]),
        connection=FakeConnection(),
        settings=make_settings(),
    )

    assert response.operation == "ftp_retry_ingest"
    assert response.result["results"][0]["remote_file_id"] == 2


def test_sync_entities_route(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        OperationService,
        "sync_entities",
        lambda self: {"rows_synced": 12, "audits_updated": 3},
    )

    response = sync_entities(
        payload=EmptyOperationRequest(),
        connection=FakeConnection(),
        settings=make_settings(),
    )

    assert response.operation == "sync_entities"
    assert response.result["rows_synced"] == 12


def test_sync_topology_route(monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(
        OperationService,
        "sync_topology",
        lambda self: {"rows_synced": 9, "unmapped_entities": 2},
    )

    response = sync_topology(
        payload=EmptyOperationRequest(),
        connection=FakeConnection(),
        settings=make_settings(),
    )

    assert response.operation == "sync_topology"
    assert response.result["unmapped_entities"] == 2
