from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

from lte_pm_platform.domain.models import IngestSummary
from lte_pm_platform.pipeline.ingest.file_discovery import ParsedArchiveFile
from lte_pm_platform.pipeline.orchestration.ftp_staged_flow import (
    annotate_registry_row,
    build_operational_status,
    fetch_via_staged_flow,
    run_ftp_cycle,
    run_locked_ftp_cycle,
    scan_remote_files,
    retry_download_registry_files,
    retry_ingest_registry_files,
)
from lte_pm_platform.pipeline.orchestration.run_lock import PipelineCycleLockError, pipeline_cycle_lock


class FakeRepository:
    def __init__(self, pending_ingests: list[dict]) -> None:
        self.pending_ingests = pending_ingests
        self.events: list[str] = []
        self.upsert_calls: list[dict] = []
        self.download_filters: list[dict] = []
        self.ingest_filters: list[dict] = []
        self.download_success: list[dict] = []
        self.download_failure: list[dict] = []
        self.ingest_success: list[dict] = []
        self.ingest_failure: list[dict] = []
        self.retry_download_rows: list[dict] = []
        self.retry_ingest_rows: list[dict] = []

    def upsert_discovered_files(self, **kwargs) -> dict[str, int]:  # noqa: ANN003
        self.events.append("scan")
        self.upsert_calls.append(kwargs)
        return {"discovered": len(kwargs["files"]), "updated": 0}

    def fetch_pending_downloads(self, *, source_name: str, limit: int, remote_paths=None) -> list[dict]:  # noqa: ANN001
        self.events.append("fetch_pending_downloads")
        self.download_filters.append({"source_name": source_name, "limit": limit, "remote_paths": remote_paths})
        rows = [
            {
                "id": 1,
                "remote_filename": "file1.tar.gz",
                "remote_path": "/pm/file1.tar.gz",
                "status": "DISCOVERED",
            }
        ]
        if remote_paths is not None:
            rows = [row for row in rows if row["remote_path"] in set(remote_paths)]
        return rows

    def mark_download_succeeded(self, *, remote_file_id: int, local_staged_path: str) -> None:
        self.events.append(f"download_success:{remote_file_id}")
        self.download_success.append(
            {"remote_file_id": remote_file_id, "local_staged_path": local_staged_path}
        )

    def mark_download_failed(self, *, remote_file_id: int, error_message: str) -> None:  # noqa: ARG002
        self.events.append(f"download_failed:{remote_file_id}")
        self.download_failure.append(
            {"remote_file_id": remote_file_id, "error_message": error_message}
        )

    def fetch_pending_ingests(self, *, source_name: str, limit: int, remote_file_ids=None) -> list[dict]:  # noqa: ANN001
        self.events.append("fetch_pending_ingests")
        self.ingest_filters.append({"source_name": source_name, "limit": limit, "remote_file_ids": remote_file_ids})
        if remote_file_ids == [1]:
            return self.pending_ingests
        return [{"id": 99, "remote_path": "/pm/old.tar.gz", "local_staged_path": "/tmp/old.tar.gz"}]

    def mark_ingest_succeeded(self, *, remote_file_id: int, file_hash: str | None, ingest_run_id: str, final_file_path: str | None) -> None:
        self.events.append(f"ingest_success:{remote_file_id}")
        self.ingest_success.append(
            {
                "remote_file_id": remote_file_id,
                "file_hash": file_hash,
                "ingest_run_id": ingest_run_id,
                "final_file_path": final_file_path,
            }
        )

    def mark_ingest_skipped_duplicate(self, **kwargs) -> None:  # noqa: ANN003
        raise AssertionError("unexpected duplicate")

    def mark_ingest_failed(self, **kwargs) -> None:  # noqa: ANN003
        self.events.append(f"ingest_failed:{kwargs['remote_file_id']}")
        self.ingest_failure.append(kwargs)

    def fetch_retry_download_rows(self, *, source_name: str, remote_file_ids: list[int]) -> list[dict]:
        assert source_name == "default"
        return [row for row in self.retry_download_rows if row["id"] in set(remote_file_ids)]

    def fetch_retry_ingest_rows(self, *, source_name: str, remote_file_ids: list[int]) -> list[dict]:
        assert source_name == "default"
        return [row for row in self.retry_ingest_rows if row["id"] in set(remote_file_ids)]

    def fetch_failure_rows(self, *, limit: int = 100) -> list[dict]:
        return self.fetch_registry_rows(statuses=["FAILED_DOWNLOAD", "FAILED_INGEST"], limit=limit)

    def fetch_remote_file_by_id(self, *, remote_file_id: int) -> dict | None:
        rows = self.fetch_registry_rows(remote_file_ids=[remote_file_id], limit=1)
        return rows[0] if rows else None

    def summarize_status_counts(self) -> list[dict]:
        return [
            {"status": "DISCOVERED", "file_count": 1},
            {"status": "DOWNLOADED", "file_count": 1},
            {"status": "FAILED_DOWNLOAD", "file_count": 1},
            {"status": "FAILED_INGEST", "file_count": 2},
        ]

    def fetch_registry_rows(self, *, statuses=None, remote_file_ids=None, limit=100) -> list[dict]:  # noqa: ANN001
        now = datetime(2026, 3, 13, 10, 30)
        rows = [
            {
                "id": 10,
                "status": "FAILED_DOWNLOAD",
                "remote_path": "/pm/fd.tar.gz",
                "local_staged_path": None,
                "last_seen_at": now,
                "last_scan_at": now,
                "status_updated_at": now,
                "download_attempt_count": 3,
                "ingest_attempt_count": 0,
            },
            {
                "id": 11,
                "status": "FAILED_INGEST",
                "remote_path": "/pm/fi-retry.tar.gz",
                "local_staged_path": str(Path("/tmp/exists.tar.gz")),
                "last_seen_at": now,
                "last_scan_at": now,
                "status_updated_at": now,
                "download_attempt_count": 1,
                "ingest_attempt_count": 1,
            },
            {
                "id": 12,
                "status": "FAILED_INGEST",
                "remote_path": "/pm/fi-missing.tar.gz",
                "local_staged_path": str(Path("/tmp/missing.tar.gz")),
                "last_seen_at": now,
                "last_scan_at": now,
                "status_updated_at": now,
                "download_attempt_count": 1,
                "ingest_attempt_count": 3,
            },
            {
                "id": 13,
                "status": "DOWNLOADED",
                "remote_path": "/pm/down-missing.tar.gz",
                "local_staged_path": None,
                "last_seen_at": now,
                "last_scan_at": now,
                "status_updated_at": now,
                "download_attempt_count": 1,
                "ingest_attempt_count": 0,
            },
        ]
        if statuses is not None:
            rows = [row for row in rows if row["status"] in set(statuses)]
        if remote_file_ids is not None:
            rows = [row for row in rows if row["id"] in set(remote_file_ids)]
        return rows[:limit]

    def fetch_latest_scan_at(self):
        return datetime(2026, 3, 13, 10, 30)


class FakeClient:
    def __init__(self, candidate: ParsedArchiveFile) -> None:
        self.candidate = candidate
        self.calls: list[dict] = []

    def list_candidate_details(self, **kwargs) -> list[ParsedArchiveFile]:  # noqa: ANN003
        self.calls.append(kwargs)
        return [self.candidate]

    def download_file(self, remote_path: str, download_dir: Path) -> Path:
        if remote_path.endswith("bad.tar.gz"):
            raise RuntimeError("download failed")
        return download_dir / Path(remote_path).name


class FakePipeline:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def load_zip(self, zip_path: Path, *, trigger_type: str, source_type: str) -> IngestSummary:
        self.calls.append(
            {"zip_path": str(zip_path), "trigger_type": trigger_type, "source_type": source_type}
        )
        return IngestSummary(
            source_file=zip_path.name,
            run_id=uuid4(),
            trigger_type=trigger_type,
            source_type=source_type,
            file_hash="hash1",
            status="SUCCESS",
            final_file_path=f"/archive/{zip_path.name}",
        )


def test_fetch_via_staged_flow_ingests_only_rows_downloaded_in_same_call(tmp_path) -> None:  # noqa: ANN001
    staged = tmp_path / "file1.tar.gz"
    staged.write_text("ok")
    candidate = ParsedArchiveFile(
        dataset_family="PM/itbbu/ltefdd",
        filename="file1.tar.gz",
        interval_start=datetime(2026, 3, 13, 10, 15),
        revision=0,
        extension="tar.gz",
        path="/pm/file1.tar.gz",
    )
    repository = FakeRepository(
        pending_ingests=[
            {
                "id": 1,
                "remote_path": "/pm/file1.tar.gz",
                "local_staged_path": str(staged),
                "status": "DOWNLOADED",
            }
        ]
    )
    client = FakeClient(candidate)
    pipeline = FakePipeline()

    payload = fetch_via_staged_flow(
        repository=repository,
        client=client,
        pipeline=pipeline,
        source_name="default",
        remote_directory="/pm",
        download_dir=tmp_path,
        start=None,
        end=None,
        revision_policy="additive",
        limit=1,
        trigger_type="ftp_fetch",
        source_type="ftp",
    )

    assert payload["count"] == 1
    assert payload["results"][0]["remote_file"] == "file1.tar.gz"
    assert payload["results"][0]["downloaded_to"] == str(tmp_path / "file1.tar.gz")
    assert payload["results"][0]["ingest_summary"]["trigger_type"] == "ftp_fetch"
    assert repository.download_filters == [
        {"source_name": "default", "limit": 1, "remote_paths": ["/pm/file1.tar.gz"]}
    ]
    assert repository.ingest_filters == [
        {"source_name": "default", "limit": 1, "remote_file_ids": [1]}
    ]
    assert pipeline.calls == [
        {"zip_path": str(staged), "trigger_type": "ftp_fetch", "source_type": "ftp"}
    ]


def test_scan_remote_files_scans_each_configured_directory() -> None:
    candidate = ParsedArchiveFile(
        dataset_family="PM/sdr/ltefdd",
        filename="file1.tar.gz",
        interval_start=datetime(2026, 3, 13, 10, 15),
        revision=0,
        extension="tar.gz",
        path="/unused/file1.tar.gz",
    )
    repository = FakeRepository(pending_ingests=[])
    client = FakeClient(candidate)

    result = scan_remote_files(
        repository=repository,
        client=client,
        source_name="default",
        remote_directory=None,
        remote_directories=["/pm/a", "/pm/b"],
        start=None,
        end=None,
        revision_policy="additive",
        persist=True,
    )

    assert client.calls == [
        {"remote_directory": "/pm/a", "start": None, "end": None, "revision_policy": "additive"},
        {"remote_directory": "/pm/b", "start": None, "end": None, "revision_policy": "additive"},
    ]
    assert [call["remote_directory"] for call in repository.upsert_calls] == ["/pm/a", "/pm/b"]
    assert result["summary"] == {"discovered": 2, "updated": 0}


def test_retry_download_registry_files_scopes_to_requested_failed_rows(tmp_path) -> None:  # noqa: ANN001
    repository = FakeRepository(pending_ingests=[])
    repository.retry_download_rows = [
        {"id": 7, "remote_path": "/pm/good.tar.gz", "remote_filename": "good.tar.gz"},
    ]
    client = FakeClient(
        ParsedArchiveFile(
            dataset_family="PM/itbbu/ltefdd",
            filename="good.tar.gz",
            interval_start=datetime(2026, 3, 13, 10, 15),
            revision=0,
            extension="tar.gz",
            path="/pm/good.tar.gz",
        )
    )

    payload = retry_download_registry_files(
        repository=repository,
        client=client,
        source_name="default",
        download_dir=tmp_path,
        remote_file_ids=[7, 8],
    )

    assert payload["processed_ids"] == [7]
    assert payload["not_retryable_ids"] == [8]
    assert payload["results"][0]["status"] == "DOWNLOADED"


def test_retry_ingest_registry_files_marks_missing_staged_file_as_reconciliation_needed(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    existing = tmp_path / "exists.tar.gz"
    existing.write_text("ok")
    repository = FakeRepository(pending_ingests=[])
    repository.retry_ingest_rows = [
        {"id": 5, "remote_path": "/pm/missing.tar.gz", "local_staged_path": str(tmp_path / "missing.tar.gz")},
        {"id": 6, "remote_path": "/pm/exists.tar.gz", "local_staged_path": str(existing)},
    ]
    pipeline = FakePipeline()

    payload = retry_ingest_registry_files(
        repository=repository,
        pipeline=pipeline,
        source_name="default",
        trigger_type="ftp_stage_ingest",
        source_type="ftp",
        remote_file_ids=[5, 6],
    )

    assert payload["processed_ids"] == [5, 6]
    assert payload["results"][0]["classification"] == "reconciliation_needed"
    assert payload["results"][1]["status"] == "INGESTED"


def test_annotate_registry_row_classifies_failed_ingest_missing_stage_as_reconciliation_needed(tmp_path) -> None:  # noqa: ANN001
    missing = tmp_path / "missing.tar.gz"
    annotated = annotate_registry_row(
        {
            "id": 4,
            "status": "FAILED_INGEST",
            "remote_path": "/pm/missing.tar.gz",
            "local_staged_path": str(missing),
            "ingest_attempt_count": 1,
            "download_attempt_count": 1,
            "last_seen_at": datetime(2026, 3, 13, 10, 30),
            "status_updated_at": datetime(2026, 3, 13, 10, 30),
        }
    )

    assert annotated["classification"] == "reconciliation_needed"
    assert "staged file not found" in annotated["classification_reason"]


def test_build_operational_status_counts_retryable_and_reconciliation_rows(tmp_path, monkeypatch) -> None:  # noqa: ANN001
    existing = tmp_path / "exists.tar.gz"
    existing.write_text("ok")
    repository = FakeRepository(pending_ingests=[])

    def fake_fetch_registry_rows(*, statuses=None, remote_file_ids=None, limit=100):  # noqa: ANN001
        now = datetime(2026, 3, 13, 10, 30)
        rows = [
            {"id": 10, "status": "FAILED_DOWNLOAD", "remote_path": "/pm/fd.tar.gz", "local_staged_path": None, "download_attempt_count": 3, "ingest_attempt_count": 0, "last_seen_at": now, "status_updated_at": now},
            {"id": 11, "status": "FAILED_INGEST", "remote_path": "/pm/fi-retry.tar.gz", "local_staged_path": str(existing), "download_attempt_count": 1, "ingest_attempt_count": 1, "last_seen_at": now, "status_updated_at": now},
            {"id": 12, "status": "FAILED_INGEST", "remote_path": "/pm/fi-missing.tar.gz", "local_staged_path": str(tmp_path / "missing.tar.gz"), "download_attempt_count": 1, "ingest_attempt_count": 3, "last_seen_at": now - timedelta(days=1), "status_updated_at": now - timedelta(days=2)},
            {"id": 13, "status": "DOWNLOADED", "remote_path": "/pm/down-missing.tar.gz", "local_staged_path": None, "download_attempt_count": 1, "ingest_attempt_count": 0, "last_seen_at": now, "status_updated_at": now - timedelta(days=2)},
        ]
        if statuses is not None:
            rows = [row for row in rows if row["status"] in set(statuses)]
        if remote_file_ids is not None:
            rows = [row for row in rows if row["id"] in set(remote_file_ids)]
        return rows[:limit]

    repository.fetch_registry_rows = fake_fetch_registry_rows  # type: ignore[method-assign]
    repository.fetch_latest_scan_at = lambda: datetime(2026, 3, 13, 10, 30)  # type: ignore[method-assign]

    payload = build_operational_status(repository=repository, limit=10)

    assert payload["summary"]["retryable_downloads"] == 1
    assert payload["summary"]["retryable_ingests"] == 1
    assert payload["summary"]["reconciliation_needed"] == 2


def test_run_ftp_cycle_orchestrates_scan_download_ingest_in_order(tmp_path) -> None:  # noqa: ANN001
    staged = tmp_path / "file1.tar.gz"
    staged.write_text("ok")
    candidate = ParsedArchiveFile(
        dataset_family="PM/itbbu/ltefdd",
        filename="file1.tar.gz",
        interval_start=datetime(2026, 3, 13, 10, 15),
        revision=0,
        extension="tar.gz",
        path="/pm/file1.tar.gz",
    )
    repository = FakeRepository(
        pending_ingests=[
            {"id": 1, "remote_path": "/pm/file1.tar.gz", "local_staged_path": str(staged), "status": "DOWNLOADED"}
        ]
    )
    client = FakeClient(candidate)
    pipeline = FakePipeline()

    payload = run_ftp_cycle(
        repository=repository,
        client=client,
        pipeline=pipeline,
        source_name="default",
        remote_directory="/pm",
        download_dir=tmp_path,
        start=None,
        end=None,
        revision_policy="additive",
        limit=1,
        trigger_type="ftp_run_cycle",
        source_type="ftp",
    )

    assert payload["summary"]["scanned"] == 1
    assert payload["summary"]["downloaded"] == 1
    assert payload["summary"]["ingested"] == 1
    assert repository.events[:5] == [
        "scan",
        "fetch_pending_downloads",
        "download_success:1",
        "fetch_pending_ingests",
        "ingest_success:1",
    ]


def test_run_ftp_cycle_supports_family_scoping(tmp_path) -> None:  # noqa: ANN001
    repository = FakeRepository(pending_ingests=[])
    client = FakeClient(
        ParsedArchiveFile(
            dataset_family="PM/sdr/ltefdd",
            filename="file1.tar.gz",
            interval_start=datetime(2026, 3, 13, 10, 15),
            revision=0,
            extension="tar.gz",
            path="/pm/file1.tar.gz",
        )
    )
    pipeline = FakePipeline()

    payload = run_ftp_cycle(
        repository=repository,
        client=client,
        pipeline=pipeline,
        source_name="default",
        remote_directory="/pm",
        download_dir=tmp_path,
        start=None,
        end=None,
        revision_policy="additive",
        limit=1,
        families=["PM/itbbu/ltefdd"],
        dry_run=True,
        trigger_type="ftp_run_cycle",
        source_type="ftp",
    )

    assert payload["summary"]["scanned"] == 0
    assert payload["stages"]["planned_downloads"] == 0


def test_run_ftp_cycle_propagates_scan_failure(tmp_path) -> None:  # noqa: ANN001
    class FailingClient(FakeClient):
        def list_candidate_details(self, **kwargs):  # noqa: ANN003
            raise RuntimeError("ftp listing failed")

    repository = FakeRepository(pending_ingests=[])
    client = FailingClient(
        ParsedArchiveFile(
            dataset_family="PM/itbbu/ltefdd",
            filename="unused.tar.gz",
            interval_start=datetime(2026, 3, 13, 10, 15),
            revision=0,
            extension="tar.gz",
            path="/pm/unused.tar.gz",
        )
    )
    pipeline = FakePipeline()

    try:
        run_ftp_cycle(
            repository=repository,
            client=client,
            pipeline=pipeline,
            source_name="default",
            remote_directory="/pm",
            download_dir=tmp_path,
            start=None,
            end=None,
            revision_policy="additive",
            limit=1,
            trigger_type="ftp_run_cycle",
            source_type="ftp",
        )
    except RuntimeError as exc:
        assert str(exc) == "ftp listing failed"
    else:
        raise AssertionError("Expected cycle failure to propagate")


def test_run_locked_ftp_cycle_rejects_overlap(tmp_path) -> None:  # noqa: ANN001
    lock_path = tmp_path / "ftp.lock"
    repository = FakeRepository(pending_ingests=[])
    client = FakeClient(
        ParsedArchiveFile(
            dataset_family="PM/itbbu/ltefdd",
            filename="unused.tar.gz",
            interval_start=datetime(2026, 3, 13, 10, 15),
            revision=0,
            extension="tar.gz",
            path="/pm/unused.tar.gz",
        )
    )
    pipeline = FakePipeline()

    with pipeline_cycle_lock(lock_path):
        try:
            run_locked_ftp_cycle(
                lock_path=lock_path,
                repository=repository,
                client=client,
                pipeline=pipeline,
                source_name="default",
                remote_directory="/pm",
                download_dir=tmp_path,
                start=None,
                end=None,
                revision_policy="additive",
                limit=1,
                trigger_type="ftp_run_cycle",
                source_type="ftp",
            )
        except PipelineCycleLockError:
            return
    raise AssertionError("Expected overlap lock failure")
