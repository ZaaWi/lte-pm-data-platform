from __future__ import annotations

from datetime import date, datetime

import pytest

from lte_pm_platform.pipeline.orchestration.run_lock import PipelineCycleLockError
from lte_pm_platform.services.operation_service import OperationService, OperationValidationError


class FakeConnection:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class FakeSettings:
    ftp_host = "ftp.example.com"
    ftp_port = 21
    ftp_username = "user"
    ftp_password = "pass"
    ftp_remote_directory = "/remote"
    ftp_remote_directories = ("/remote",)
    ftp_passive_mode = True


def make_service() -> OperationService:
    return OperationService(connection=FakeConnection(), settings=FakeSettings())


def make_settings(**overrides):  # noqa: ANN001
    values = {
        "ftp_host": "ftp.example.com",
        "ftp_port": 21,
        "ftp_username": "user",
        "ftp_password": "pass",
        "ftp_remote_directory": "/remote",
        "ftp_remote_directories": ("/remote",),
        "ftp_passive_mode": True,
    }
    values.update(overrides)
    return type("Settings", (), values)()


def test_build_ftp_client_rejects_missing_username() -> None:
    service = OperationService(connection=FakeConnection(), settings=make_settings(ftp_username=""))

    with pytest.raises(OperationValidationError, match="FTP_USERNAME is not configured"):
        service._build_ftp_client()


def test_build_ftp_client_rejects_missing_password() -> None:
    service = OperationService(connection=FakeConnection(), settings=make_settings(ftp_password=""))

    with pytest.raises(OperationValidationError, match="FTP_PASSWORD is not configured"):
        service._build_ftp_client()


def test_run_ftp_cycle_returns_summary(monkeypatch) -> None:  # noqa: ANN001
    service = make_service()
    captured: dict[str, object] = {}
    monkeypatch.setattr(service, "_build_pipeline", lambda: object())
    monkeypatch.setattr(service, "_build_ftp_client", lambda: object())
    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.run_locked_ftp_cycle",
        lambda **kwargs: captured.update(kwargs) or {"summary": {"scanned": 10, "downloaded": 2}},
    )

    result = service.run_ftp_cycle(
        limit=20,
        interval_start=None,
        start=date(2026, 3, 5),
        end=date(2026, 3, 5),
        revision_policy="additive",
        families=["PM/itbbu/ltefdd"],
        dry_run=False,
        retry_failed=False,
    )

    assert result == {"scanned": 10, "downloaded": 2}
    assert captured["start"] == datetime(2026, 3, 5, 0, 0)
    assert captured["end"] == datetime(2026, 3, 5, 23, 59, 59, 999999)


def test_enqueue_ftp_cycle_returns_run(monkeypatch) -> None:  # noqa: ANN001
    service = make_service()
    fake_repo = type(
        "Repo",
        (),
        {
            "create_run": lambda self, **kwargs: {"id": 9, "status": "queued", "parameters_json": kwargs["parameters"]},
        },
    )()
    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.FtpCycleRunRepository",
        lambda connection: fake_repo,
    )

    result = service.enqueue_ftp_cycle(
        limit=5,
        interval_start=None,
        start=date(2026, 3, 5),
        end=date(2026, 3, 5),
        revision_policy="additive",
        families=["PM/sdr/ltefdd"],
        dry_run=True,
        retry_failed=False,
        trigger_source="ui",
    )

    assert result["id"] == 9
    assert result["parameters_json"]["limit"] == 5


def test_enqueue_ftp_cycle_normalizes_interval_start_to_15_minute_window(monkeypatch) -> None:  # noqa: ANN001
    service = make_service()
    captured: dict[str, object] = {}

    class FakeRepo:
        def create_run(self, **kwargs):  # noqa: ANN001, ANN202
            captured.update(kwargs)
            return {"id": 10, "status": "queued", "parameters_json": kwargs["parameters"]}

    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.FtpCycleRunRepository",
        lambda connection: FakeRepo(),
    )

    result = service.enqueue_ftp_cycle(
        limit=3,
        interval_start=datetime(2026, 3, 5, 0, 15),
        start=None,
        end=None,
        revision_policy="latest-only",
        families=None,
        dry_run=False,
        retry_failed=False,
        trigger_source="ui",
    )

    assert result["parameters_json"]["interval_start"] == "2026-03-05T00:15:00"
    assert result["parameters_json"]["start"] == "2026-03-05T00:15:00"
    assert result["parameters_json"]["end"] == "2026-03-05T00:30:00"


def test_execute_ftp_cycle_run_marks_success(monkeypatch) -> None:  # noqa: ANN001
    service = make_service()
    events: list[tuple[str, str, str, object]] = []

    class FakeRunRepo:
        def get_run(self, *, run_id: int):  # noqa: ANN202
            return {
                "id": run_id,
                "parameters_json": {
                    "limit": 1,
                    "start": None,
                    "end": None,
                    "revision_policy": "additive",
                    "families": None,
                    "dry_run": True,
                    "retry_failed": False,
                },
                "summary_json": {},
            }

        def append_event(self, **kwargs) -> None:  # noqa: ANN001
            events.append((kwargs["stage"], kwargs["level"], kwargs["message"], kwargs["metrics"]))

        def update_summary(self, **kwargs) -> None:  # noqa: ANN001
            return None

        def mark_succeeded(self, **kwargs):  # noqa: ANN001, ANN202
            return {"id": kwargs["run_id"], "status": "succeeded"}

        def mark_failed(self, **kwargs):  # noqa: ANN001, ANN202
            raise AssertionError("mark_failed should not be called")

    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.FtpCycleRunRepository",
        lambda connection: FakeRunRepo(),
    )
    monkeypatch.setattr(
        service,
        "_run_ftp_cycle_with_callbacks",
        lambda **kwargs: {"scanned": 5, "downloaded": 0, "ingested": 0},
    )

    result = service.execute_ftp_cycle_run(run_id=4)

    assert result["scanned"] == 5
    assert events[0][0] == "discover"


def test_run_ftp_cycle_rejects_invalid_family() -> None:
    service = make_service()

    with pytest.raises(OperationValidationError):
        service.run_ftp_cycle(
            limit=20,
            interval_start=None,
            start=None,
            end=None,
            revision_policy="additive",
            families=["bad-family"],
            dry_run=False,
            retry_failed=False,
        )


def test_run_ftp_cycle_rejects_invalid_revision_policy() -> None:
    service = make_service()

    with pytest.raises(OperationValidationError):
        service.run_ftp_cycle(
            limit=20,
            interval_start=None,
            start=None,
            end=None,
            revision_policy="bad-policy",
            families=None,
            dry_run=False,
            retry_failed=False,
        )


def test_run_ftp_cycle_propagates_lock_error(monkeypatch) -> None:  # noqa: ANN001
    service = make_service()
    monkeypatch.setattr(service, "_build_pipeline", lambda: object())
    monkeypatch.setattr(service, "_build_ftp_client", lambda: object())
    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.run_locked_ftp_cycle",
        lambda **kwargs: (_ for _ in ()).throw(PipelineCycleLockError(kwargs["lock_path"])),
    )

    with pytest.raises(PipelineCycleLockError):
        service.run_ftp_cycle(
            limit=20,
            interval_start=None,
            start=None,
            end=None,
            revision_policy="additive",
            families=None,
            dry_run=False,
            retry_failed=False,
        )


def test_retry_download_rejects_empty_ids() -> None:
    service = make_service()

    with pytest.raises(OperationValidationError):
        service.retry_download(ids=[])


def test_retry_download_returns_results(monkeypatch) -> None:  # noqa: ANN001
    service = make_service()
    monkeypatch.setattr(service, "_build_ftp_client", lambda: object())
    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.retry_download_registry_files",
        lambda **kwargs: {"results": [{"remote_file_id": 1, "status": "DOWNLOADED"}]},
    )

    result = service.retry_download(ids=[1])

    assert result["results"][0]["status"] == "DOWNLOADED"
    assert service.connection.commits == 1


def test_retry_ingest_returns_results(monkeypatch) -> None:  # noqa: ANN001
    service = make_service()
    monkeypatch.setattr(service, "_build_pipeline", lambda: object())
    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.retry_ingest_registry_files",
        lambda **kwargs: {"results": [{"remote_file_id": 2, "status": "INGESTED"}]},
    )

    result = service.retry_ingest(ids=[2])

    assert result["results"][0]["status"] == "INGESTED"
    assert service.connection.commits == 1


def test_sync_entities_returns_counts(monkeypatch) -> None:  # noqa: ANN001
    service = make_service()
    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.EntityReferenceRepository",
        lambda connection: type("Repo", (), {"refresh_from_raw_entities": lambda self: 12})(),
    )
    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.FileAuditRepository",
        lambda connection: type(
            "AuditRepo",
            (),
            {
                "mark_success_normalization_completed": lambda self: 3,
                "mark_success_normalization_failed": lambda self, error_message: 0,
            },
        )(),
    )

    result = service.sync_entities()

    assert result == {"rows_synced": 12, "audits_updated": 3}
    assert service.connection.commits == 1


def test_sync_topology_returns_counts(monkeypatch) -> None:  # noqa: ANN001
    service = make_service()
    fake_repo = type("Repo", (), {"list_unmapped_entities": lambda self, limit: [{"k": 1}, {"k": 2}]})()
    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.TopologyReferenceRepository",
        lambda connection: fake_repo,
    )
    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.sync_topology_enrichment",
        lambda repository: {"rows_synced": 9},
    )

    result = service.sync_topology()

    assert result == {"rows_synced": 9, "unmapped_entities": 2}
    assert service.connection.commits == 1
