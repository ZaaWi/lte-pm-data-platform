from __future__ import annotations

from datetime import date

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
    ftp_passive_mode = True


def make_service() -> OperationService:
    return OperationService(connection=FakeConnection(), settings=FakeSettings())


def test_run_ftp_cycle_returns_summary(monkeypatch) -> None:  # noqa: ANN001
    service = make_service()
    monkeypatch.setattr(service, "_build_pipeline", lambda: object())
    monkeypatch.setattr(service, "_build_ftp_client", lambda: object())
    monkeypatch.setattr(
        "lte_pm_platform.services.operation_service.run_locked_ftp_cycle",
        lambda **kwargs: {"summary": {"scanned": 10, "downloaded": 2}},
    )

    result = service.run_ftp_cycle(
        limit=20,
        start=date(2026, 3, 5),
        end=date(2026, 3, 5),
        revision_policy="additive",
        families=["PM/itbbu/ltefdd"],
        dry_run=False,
        retry_failed=False,
    )

    assert result == {"scanned": 10, "downloaded": 2}


def test_run_ftp_cycle_rejects_invalid_family() -> None:
    service = make_service()

    with pytest.raises(OperationValidationError):
        service.run_ftp_cycle(
            limit=20,
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
