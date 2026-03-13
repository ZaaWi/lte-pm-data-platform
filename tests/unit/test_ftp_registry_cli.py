import json
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

from typer.testing import CliRunner

from lte_pm_platform import cli
from lte_pm_platform.domain.models import IngestSummary
from lte_pm_platform.pipeline.ingest.file_discovery import ParsedArchiveFile

runner = CliRunner()


class FakeFtpClient:
    def __init__(self, candidates: list[ParsedArchiveFile]) -> None:
        self.candidates = candidates
        self.downloaded: list[str] = []

    def list_candidate_details(self, **kwargs) -> list[ParsedArchiveFile]:  # noqa: ANN003
        return self.candidates

    def download_file(self, remote_path: str, local_dir):  # noqa: ANN001, ANN201
        self.downloaded.append(remote_path)
        if remote_path.endswith("bad.tar.gz"):
            raise RuntimeError("download failed")
        return local_dir / remote_path.rsplit("/", 1)[-1]


class FakeFtpRemoteFileRepository:
    def __init__(self, connection) -> None:  # noqa: ANN001
        self.connection = connection

    def upsert_discovered_files(self, **kwargs) -> dict[str, int]:  # noqa: ANN003
        self.connection.upsert_kwargs = kwargs
        return {"discovered": 2, "updated": 1}

    def summarize_status_counts(self) -> list[dict]:
        return [
            {"status": "DISCOVERED", "file_count": 3},
            {"status": "FAILED_DOWNLOAD", "file_count": 1},
            {"status": "FAILED_INGEST", "file_count": 1},
        ]

    def fetch_registry_rows(self, *, statuses=None, remote_file_ids=None, limit=100):  # noqa: ANN001, ANN201
        now = datetime(2026, 3, 13, 10, 30)
        rows = [
            {
                "id": 7,
                "source_name": "default",
                "remote_filename": "failed-download.tar.gz",
                "remote_path": "/pm/failed-download.tar.gz",
                "status": "FAILED_DOWNLOAD",
                "local_staged_path": None,
                "last_error": "download failed",
                "remote_size_bytes": 100,
                "remote_modified_at": now - timedelta(minutes=10),
                "first_seen_at": now - timedelta(days=2),
                "last_seen_at": now,
                "last_scan_at": now,
                "download_attempt_count": 1,
                "ingest_attempt_count": 0,
                "last_download_attempt_at": now - timedelta(minutes=5),
                "last_ingest_attempt_at": None,
                "status_updated_at": now - timedelta(minutes=5),
                "updated_at": now - timedelta(minutes=5),
            },
            {
                "id": 8,
                "source_name": "default",
                "remote_filename": "failed-ingest.tar.gz",
                "remote_path": "/pm/failed-ingest.tar.gz",
                "status": "FAILED_INGEST",
                "local_staged_path": self.connection.reconcile_existing_path,
                "last_error": "parse failed",
                "remote_size_bytes": 200,
                "remote_modified_at": now - timedelta(minutes=20),
                "first_seen_at": now - timedelta(days=3),
                "last_seen_at": now,
                "last_scan_at": now,
                "download_attempt_count": 1,
                "ingest_attempt_count": 1,
                "last_download_attempt_at": now - timedelta(minutes=15),
                "last_ingest_attempt_at": now - timedelta(minutes=4),
                "status_updated_at": now - timedelta(minutes=4),
                "updated_at": now - timedelta(minutes=4),
            },
            {
                "id": 9,
                "source_name": "default",
                "remote_filename": "reconcile.tar.gz",
                "remote_path": "/pm/reconcile.tar.gz",
                "status": "FAILED_INGEST",
                "local_staged_path": self.connection.reconcile_missing_path,
                "last_error": "staged file missing",
                "remote_size_bytes": 300,
                "remote_modified_at": now - timedelta(minutes=30),
                "first_seen_at": now - timedelta(days=4),
                "last_seen_at": now - timedelta(days=1),
                "last_scan_at": now,
                "download_attempt_count": 1,
                "ingest_attempt_count": 2,
                "last_download_attempt_at": now - timedelta(days=1, minutes=20),
                "last_ingest_attempt_at": now - timedelta(days=1, minutes=10),
                "status_updated_at": now - timedelta(days=1),
                "updated_at": now - timedelta(days=1),
            },
        ]
        if statuses is not None:
            rows = [row for row in rows if row["status"] in set(statuses)]
        if remote_file_ids is not None:
            rows = [row for row in rows if row["id"] in set(remote_file_ids)]
        return rows[:limit]

    def fetch_failure_rows(self, *, limit: int = 100) -> list[dict]:
        return self.fetch_registry_rows(statuses=["FAILED_DOWNLOAD", "FAILED_INGEST"], limit=limit)

    def fetch_remote_file_by_id(self, *, remote_file_id: int) -> dict | None:
        rows = self.fetch_registry_rows(remote_file_ids=[remote_file_id], limit=1)
        return rows[0] if rows else None

    def fetch_latest_scan_at(self):
        return datetime(2026, 3, 13, 10, 30)

    def fetch_pending_downloads(self, *, source_name: str, limit: int, remote_paths=None) -> list[dict]:  # noqa: ANN001
        self.connection.pending_kwargs = {
            "source_name": source_name,
            "limit": limit,
            "remote_paths": remote_paths,
        }
        rows = [
            {"id": 1, "remote_filename": "good.tar.gz", "remote_path": "/pm/good.tar.gz", "status": "DISCOVERED"},
            {"id": 2, "remote_filename": "bad.tar.gz", "remote_path": "/pm/bad.tar.gz", "status": "FAILED_DOWNLOAD"},
        ]
        if remote_paths is not None:
            rows = [row for row in rows if row["remote_path"] in set(remote_paths)]
        return rows[:limit]

    def fetch_pending_ingests(self, *, source_name: str, limit: int, remote_file_ids=None) -> list[dict]:  # noqa: ANN001
        self.connection.pending_ingest_kwargs = {
            "source_name": source_name,
            "limit": limit,
            "remote_file_ids": remote_file_ids,
        }
        rows = self.connection.pending_ingests
        if remote_file_ids is not None:
            rows = [row for row in rows if row["id"] in set(remote_file_ids)]
        return rows[:limit]

    def mark_download_succeeded(self, *, remote_file_id: int, local_staged_path: str) -> None:
        self.connection.success_updates.append(
            {"remote_file_id": remote_file_id, "local_staged_path": local_staged_path}
        )

    def mark_download_failed(self, *, remote_file_id: int, error_message: str) -> None:
        self.connection.failure_updates.append(
            {"remote_file_id": remote_file_id, "error_message": error_message}
        )

    def mark_ingest_succeeded(
        self,
        *,
        remote_file_id: int,
        file_hash: str | None,
        ingest_run_id: str,
        final_file_path: str | None,
    ) -> None:
        self.connection.ingest_success_updates.append(
            {
                "remote_file_id": remote_file_id,
                "file_hash": file_hash,
                "ingest_run_id": ingest_run_id,
                "final_file_path": final_file_path,
            }
        )

    def mark_ingest_skipped_duplicate(
        self,
        *,
        remote_file_id: int,
        file_hash: str | None,
        ingest_run_id: str,
        final_file_path: str | None,
    ) -> None:
        self.connection.ingest_duplicate_updates.append(
            {
                "remote_file_id": remote_file_id,
                "file_hash": file_hash,
                "ingest_run_id": ingest_run_id,
                "final_file_path": final_file_path,
            }
        )

    def mark_ingest_failed(self, *, remote_file_id: int, error_message: str) -> None:
        self.connection.ingest_failure_updates.append(
            {"remote_file_id": remote_file_id, "error_message": error_message}
        )

    def fetch_recent_failures(self, limit: int = 10) -> list[dict]:
        return [{"remote_path": "/pm/failed.tar.gz", "status": "FAILED_DOWNLOAD", "limit": limit}]

    def fetch_retry_download_rows(self, *, source_name: str, remote_file_ids: list[int]) -> list[dict]:
        self.connection.retry_download_kwargs = {
            "source_name": source_name,
            "remote_file_ids": remote_file_ids,
        }
        return [
            {"id": 7, "remote_filename": "failed-download.tar.gz", "remote_path": "/pm/failed-download.tar.gz", "status": "FAILED_DOWNLOAD"}
        ]

    def fetch_retry_ingest_rows(self, *, source_name: str, remote_file_ids: list[int]) -> list[dict]:
        self.connection.retry_ingest_kwargs = {
            "source_name": source_name,
            "remote_file_ids": remote_file_ids,
        }
        rows = [
            {"id": 8, "remote_path": "/pm/failed-ingest.tar.gz", "local_staged_path": self.connection.reconcile_existing_path, "status": "FAILED_INGEST"},
            {"id": 9, "remote_path": "/pm/reconcile.tar.gz", "local_staged_path": self.connection.reconcile_missing_path, "status": "FAILED_INGEST"},
        ]
        return [row for row in rows if row["id"] in set(remote_file_ids)]


class FakeConnection:
    def __init__(self) -> None:
        self.commits = 0
        self.upsert_kwargs: dict | None = None
        self.pending_kwargs: dict | None = None
        self.pending_ingest_kwargs: dict | None = None
        self.pending_ingests: list[dict] = []
        self.success_updates: list[dict] = []
        self.failure_updates: list[dict] = []
        self.ingest_success_updates: list[dict] = []
        self.ingest_duplicate_updates: list[dict] = []
        self.ingest_failure_updates: list[dict] = []
        self.retry_download_kwargs: dict | None = None
        self.retry_ingest_kwargs: dict | None = None
        self.reconcile_existing_path: str = ""
        self.reconcile_missing_path: str = ""

    def commit(self) -> None:
        self.commits += 1


class FakeConnectionContext:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def __enter__(self) -> FakeConnection:
        return self.connection

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


def build_candidate(filename: str) -> ParsedArchiveFile:
    return ParsedArchiveFile(
        dataset_family="PM/itbbu/ltefdd",
        filename=filename,
        interval_start=datetime(2026, 3, 13, 10, 15),
        revision=0,
        extension="tar.gz",
        path=f"/pm/{filename}",
    )


def test_ftp_scan_registers_remote_files(monkeypatch) -> None:  # noqa: ANN001
    connection = FakeConnection()
    monkeypatch.setattr(cli, "get_settings", lambda: type("S", (), {"ftp_remote_directory": "/pm"})())
    monkeypatch.setattr(
        cli,
        "get_ftp_client",
        lambda: FakeFtpClient([build_candidate("a.tar.gz"), build_candidate("b.tar.gz")]),
    )
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)

    result = runner.invoke(cli.app, ["ftp-scan"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 2
    assert payload["summary"] == {"discovered": 2, "updated": 1}
    assert connection.commits == 1
    assert connection.upsert_kwargs is not None
    assert connection.upsert_kwargs["source_name"] == "default"


def test_ftp_status_returns_registry_summary(monkeypatch) -> None:  # noqa: ANN001
    connection = FakeConnection()
    existing = Path("/tmp")
    connection.reconcile_existing_path = str(existing)
    connection.reconcile_missing_path = "/tmp/missing-reconcile.tar.gz"
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)

    result = runner.invoke(cli.app, ["ftp-status", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["status_counts"][0]["status"] == "DISCOVERED"
    assert payload["summary"]["failed_downloads"] == 1
    assert payload["summary"]["failed_ingests"] == 1
    assert payload["latest_scan_at"] == "2026-03-13 10:30:00"
    assert "recent_failures" in payload


def test_ftp_download_updates_registry_backed_download_results(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    connection = FakeConnection()
    client = FakeFtpClient([])
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_ftp_client", lambda: client)
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "ftp_download_dir", lambda: tmp_path)
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)

    result = runner.invoke(cli.app, ["ftp-download", "--limit", "2"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 2
    assert payload["results"] == [
        {
            "remote_path": "/pm/good.tar.gz",
            "status": "DOWNLOADED",
            "local_staged_path": str(tmp_path / "good.tar.gz"),
        },
        {
            "remote_path": "/pm/bad.tar.gz",
            "status": "FAILED_DOWNLOAD",
            "error": "download failed",
        },
    ]
    assert connection.pending_kwargs == {"source_name": "default", "limit": 2, "remote_paths": None}
    assert connection.success_updates == [
        {"remote_file_id": 1, "local_staged_path": str(tmp_path / "good.tar.gz")}
    ]
    assert connection.failure_updates == [{"remote_file_id": 2, "error_message": "download failed"}]
    assert connection.commits == 1


class FakePipeline:
    def __init__(self, *, loader, audit_repository) -> None:  # noqa: ANN001
        self.loader = loader
        self.audit_repository = audit_repository
        self.calls: list[dict] = []

    def load_zip(self, zip_path: Path, *, trigger_type: str, source_type: str) -> IngestSummary:
        self.calls.append(
            {"zip_path": str(zip_path), "trigger_type": trigger_type, "source_type": source_type}
        )
        name = zip_path.name
        if name == "good.tar.gz":
            return IngestSummary(
                source_file=name,
                run_id=uuid4(),
                trigger_type=trigger_type,
                source_type=source_type,
                file_hash="hash-good",
                status="SUCCESS",
                final_file_path=f"/archive/{name}",
            )
        if name == "dup.tar.gz":
            return IngestSummary(
                source_file=name,
                run_id=uuid4(),
                trigger_type=trigger_type,
                source_type=source_type,
                file_hash="hash-dup",
                status="SKIPPED_DUPLICATE",
                final_file_path=f"/archive/{name}",
            )
        raise RuntimeError("ingest failed")


class FakePipelineFactory:
    def __init__(self) -> None:
        self.instances: list[FakePipeline] = []

    def __call__(self, *, loader, audit_repository) -> FakePipeline:  # noqa: ANN001
        pipeline = FakePipeline(loader=loader, audit_repository=audit_repository)
        self.instances.append(pipeline)
        return pipeline


def test_ftp_ingest_updates_registry_backed_ingest_results(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    connection = FakeConnection()
    good = tmp_path / "good.tar.gz"
    dup = tmp_path / "dup.tar.gz"
    bad = tmp_path / "bad.tar.gz"
    good.write_text("ok")
    dup.write_text("dup")
    bad.write_text("bad")
    connection.pending_ingests = [
        {"id": 1, "remote_path": "/pm/good.tar.gz", "local_staged_path": str(good), "status": "DOWNLOADED"},
        {"id": 2, "remote_path": "/pm/dup.tar.gz", "local_staged_path": str(dup), "status": "FAILED_INGEST"},
        {"id": 3, "remote_path": "/pm/bad.tar.gz", "local_staged_path": str(bad), "status": "DOWNLOADED"},
    ]
    pipeline_factory = FakePipelineFactory()
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)
    monkeypatch.setattr(cli, "SamplePipeline", pipeline_factory)
    monkeypatch.setattr(cli, "PostgresLoader", lambda connection: object())
    monkeypatch.setattr(cli, "FileAuditRepository", lambda connection: object())

    result = runner.invoke(cli.app, ["ftp-ingest", "--limit", "3"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 3
    assert payload["results"][0]["status"] == "INGESTED"
    assert payload["results"][0]["file_hash"] == "hash-good"
    assert payload["results"][1]["status"] == "SKIPPED_DUPLICATE"
    assert payload["results"][1]["file_hash"] == "hash-dup"
    assert payload["results"][2] == {
        "remote_path": "/pm/bad.tar.gz",
        "status": "FAILED_INGEST",
        "error": "ingest failed",
    }
    assert connection.pending_ingest_kwargs == {
        "source_name": "default",
        "limit": 3,
        "remote_file_ids": None,
    }
    assert len(pipeline_factory.instances) == 1
    assert pipeline_factory.instances[0].calls == [
        {"zip_path": str(good), "trigger_type": "ftp_stage_ingest", "source_type": "ftp"},
        {"zip_path": str(dup), "trigger_type": "ftp_stage_ingest", "source_type": "ftp"},
        {"zip_path": str(bad), "trigger_type": "ftp_stage_ingest", "source_type": "ftp"},
    ]
    assert len(connection.ingest_success_updates) == 1
    assert connection.ingest_success_updates[0]["remote_file_id"] == 1
    assert len(connection.ingest_duplicate_updates) == 1
    assert connection.ingest_duplicate_updates[0]["remote_file_id"] == 2
    assert connection.ingest_failure_updates == [
        {"remote_file_id": 3, "error_message": "ingest failed"}
    ]
    assert connection.commits == 1


def test_ftp_ingest_fails_safely_when_staged_path_missing(monkeypatch) -> None:  # noqa: ANN001
    connection = FakeConnection()
    connection.pending_ingests = [
        {"id": 4, "remote_path": "/pm/missing.tar.gz", "local_staged_path": None, "status": "DOWNLOADED"}
    ]
    pipeline_factory = FakePipelineFactory()
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)
    monkeypatch.setattr(cli, "SamplePipeline", pipeline_factory)
    monkeypatch.setattr(cli, "PostgresLoader", lambda connection: object())
    monkeypatch.setattr(cli, "FileAuditRepository", lambda connection: object())

    result = runner.invoke(cli.app, ["ftp-ingest"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["results"] == [
        {
            "remote_path": "/pm/missing.tar.gz",
            "status": "FAILED_INGEST",
            "error": "local_staged_path is missing for staged ingest.",
        }
    ]
    assert pipeline_factory.instances[0].calls == [] if pipeline_factory.instances else True
    assert connection.ingest_failure_updates == [
        {
            "remote_file_id": 4,
            "error_message": "local_staged_path is missing for staged ingest.",
        }
    ]


def test_ftp_ingest_fails_safely_when_staged_file_not_found(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    connection = FakeConnection()
    missing_path = tmp_path / "not-there.tar.gz"
    connection.pending_ingests = [
        {
            "id": 5,
            "remote_path": "/pm/not-there.tar.gz",
            "local_staged_path": str(missing_path),
            "status": "DOWNLOADED",
        }
    ]
    pipeline_factory = FakePipelineFactory()
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)
    monkeypatch.setattr(cli, "SamplePipeline", pipeline_factory)
    monkeypatch.setattr(cli, "PostgresLoader", lambda connection: object())
    monkeypatch.setattr(cli, "FileAuditRepository", lambda connection: object())

    result = runner.invoke(cli.app, ["ftp-ingest"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["results"] == [
        {
            "remote_path": "/pm/not-there.tar.gz",
            "status": "FAILED_INGEST",
            "error": f"staged file not found: {missing_path}",
        }
    ]
    assert pipeline_factory.instances[0].calls == [] if pipeline_factory.instances else True
    assert connection.ingest_failure_updates == [
        {
            "remote_file_id": 5,
            "error_message": f"staged file not found: {missing_path}",
        }
    ]


def test_ftp_fetch_uses_staged_flow_and_preserves_result_shape(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    connection = FakeConnection()
    good = tmp_path / "good.tar.gz"
    good.write_text("ok")
    connection.pending_ingests = [
        {"id": 1, "remote_path": "/pm/good.tar.gz", "local_staged_path": str(good), "status": "DOWNLOADED"},
        {"id": 99, "remote_path": "/pm/old.tar.gz", "local_staged_path": str(good), "status": "DOWNLOADED"},
    ]
    pipeline_factory = FakePipelineFactory()
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: type("S", (), {"ftp_remote_directory": "/pm"})(),
    )
    monkeypatch.setattr(cli, "get_ftp_client", lambda: FakeFtpClient([build_candidate("good.tar.gz")]))
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "ftp_download_dir", lambda: tmp_path)
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)
    monkeypatch.setattr(cli, "SamplePipeline", pipeline_factory)
    monkeypatch.setattr(cli, "PostgresLoader", lambda connection: object())
    monkeypatch.setattr(cli, "FileAuditRepository", lambda connection: object())

    result = runner.invoke(cli.app, ["ftp-fetch", "--limit", "1"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["results"] == [
        {
            "remote_file": "good.tar.gz",
            "dataset_family": "PM/itbbu/ltefdd",
            "interval_start": "2026-03-13 10:15:00",
            "revision": 0,
            "downloaded_to": str(tmp_path / "good.tar.gz"),
            "ingest_summary": {
                "source_file": "good.tar.gz",
                "run_id": payload["results"][0]["ingest_summary"]["run_id"],
                "trigger_type": "ftp_fetch",
                "source_type": "ftp",
                "file_hash": "hash-good",
                "csv_files_found": 0,
                "input_rows_read": 0,
                "normalized_rows_emitted": 0,
                "rows_inserted": 0,
                "unknown_columns": [],
                "null_counter_values": 0,
                "status": "SUCCESS",
                "error_message": None,
                "lifecycle_status": "PENDING",
                "lifecycle_action": None,
                "normalization_status": "PENDING",
                "final_file_path": "/archive/good.tar.gz",
            },
            "status": "INGESTED",
            "error": None,
        }
    ]
    assert connection.pending_kwargs == {
        "source_name": "default",
        "limit": 1,
        "remote_paths": ["/pm/good.tar.gz"],
    }
    assert connection.pending_ingest_kwargs == {
        "source_name": "default",
        "limit": 1,
        "remote_file_ids": [1],
    }
    assert pipeline_factory.instances[0].calls == [
        {"zip_path": str(good), "trigger_type": "ftp_fetch", "source_type": "ftp"}
    ]


def test_ftp_failures_lists_failed_rows_with_classification(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    connection = FakeConnection()
    existing = tmp_path / "exists.tar.gz"
    existing.write_text("ok")
    connection.reconcile_existing_path = str(existing)
    connection.reconcile_missing_path = str(tmp_path / "missing.tar.gz")
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)

    result = runner.invoke(cli.app, ["ftp-failures", "--limit", "5"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 3
    assert payload["rows"][0]["classification"] in {
        "retryable_download",
        "retryable_ingest",
        "reconciliation_needed",
        "not_seen_in_latest_scan",
    }
    assert "download_attempt_count" in payload["rows"][0]


def test_ftp_failure_show_returns_detailed_failure(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    connection = FakeConnection()
    existing = tmp_path / "exists.tar.gz"
    existing.write_text("ok")
    connection.reconcile_existing_path = str(existing)
    connection.reconcile_missing_path = str(tmp_path / "missing.tar.gz")
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)

    result = runner.invoke(cli.app, ["ftp-failure-show", "--id", "8"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["id"] == 8
    assert payload["classification"] == "retryable_ingest"
    assert payload["remote_modified_at"] == "2026-03-13 10:10:00"


def test_ftp_retry_download_scopes_to_requested_ids(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    connection = FakeConnection()
    client = FakeFtpClient([])
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_ftp_client", lambda: client)
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "ftp_download_dir", lambda: tmp_path)
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)

    result = runner.invoke(cli.app, ["ftp-retry-download", "--id", "7", "--id", "99"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["processed_ids"] == [7]
    assert payload["not_retryable_ids"] == [99]
    assert connection.retry_download_kwargs == {"source_name": "default", "remote_file_ids": [7, 99]}


def test_ftp_retry_ingest_marks_missing_stage_as_reconciliation_needed(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    connection = FakeConnection()
    existing = tmp_path / "exists.tar.gz"
    existing.write_text("ok")
    connection.reconcile_existing_path = str(existing)
    connection.reconcile_missing_path = str(tmp_path / "missing.tar.gz")
    pipeline_factory = FakePipelineFactory()
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)
    monkeypatch.setattr(cli, "SamplePipeline", pipeline_factory)
    monkeypatch.setattr(cli, "PostgresLoader", lambda connection: object())
    monkeypatch.setattr(cli, "FileAuditRepository", lambda connection: object())

    result = runner.invoke(cli.app, ["ftp-retry-ingest", "--id", "8", "--id", "9"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["processed_ids"] == [8, 9]
    assert payload["results"][0]["status"] == "FAILED_INGEST"
    assert payload["results"][0]["classification"] == "retryable_ingest"
    assert payload["results"][1]["classification"] == "reconciliation_needed"


def test_ftp_reconcile_lists_only_reconciliation_needed_rows(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    connection = FakeConnection()
    existing = tmp_path / "exists.tar.gz"
    existing.write_text("ok")
    connection.reconcile_existing_path = str(existing)
    connection.reconcile_missing_path = str(tmp_path / "missing.tar.gz")
    monkeypatch.setattr(cli, "get_settings", lambda: object())
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)

    result = runner.invoke(cli.app, ["ftp-reconcile", "--limit", "10"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["count"] == 1
    assert payload["rows"][0]["classification"] == "reconciliation_needed"


def test_ftp_run_cycle_returns_summary_payload(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    connection = FakeConnection()
    good = tmp_path / "good.tar.gz"
    good.write_text("ok")
    connection.pending_ingests = [
        {"id": 1, "remote_path": "/pm/good.tar.gz", "local_staged_path": str(good), "status": "DOWNLOADED"}
    ]
    connection.reconcile_existing_path = str(good)
    connection.reconcile_missing_path = str(tmp_path / "missing.tar.gz")
    pipeline_factory = FakePipelineFactory()
    lock_path = tmp_path / "cycle.lock"
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: type("S", (), {"ftp_remote_directory": "/pm"})(),
    )
    monkeypatch.setattr(cli, "get_ftp_client", lambda: FakeFtpClient([build_candidate("good.tar.gz")]))
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "ftp_download_dir", lambda: tmp_path)
    monkeypatch.setattr(cli, "ftp_cycle_lock_path", lambda: lock_path)
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)
    monkeypatch.setattr(cli, "SamplePipeline", pipeline_factory)
    monkeypatch.setattr(cli, "PostgresLoader", lambda connection: object())
    monkeypatch.setattr(cli, "FileAuditRepository", lambda connection: object())

    result = runner.invoke(cli.app, ["ftp-run-cycle", "--limit", "1", "--family", "PM/itbbu/ltefdd"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["summary"]["scanned"] == 1
    assert payload["summary"]["downloaded"] == 1
    assert payload["summary"]["ingested"] == 1
    assert payload["families"] == ["PM/itbbu/ltefdd"]


def test_ftp_run_cycle_fails_cleanly_when_locked(monkeypatch, tmp_path) -> None:  # noqa: ANN001
    connection = FakeConnection()
    lock_path = tmp_path / "cycle.lock"
    lock_path.write_text('{"pid":999}')
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: type("S", (), {"ftp_remote_directory": "/pm"})(),
    )
    monkeypatch.setattr(cli, "get_ftp_client", lambda: FakeFtpClient([build_candidate("good.tar.gz")]))
    monkeypatch.setattr(cli, "get_connection", lambda settings: FakeConnectionContext(connection))
    monkeypatch.setattr(cli, "ftp_download_dir", lambda: tmp_path)
    monkeypatch.setattr(cli, "ftp_cycle_lock_path", lambda: lock_path)
    monkeypatch.setattr(cli, "FtpRemoteFileRepository", FakeFtpRemoteFileRepository)
    monkeypatch.setattr(cli, "SamplePipeline", FakePipelineFactory())
    monkeypatch.setattr(cli, "PostgresLoader", lambda connection: object())
    monkeypatch.setattr(cli, "FileAuditRepository", lambda connection: object())

    result = runner.invoke(cli.app, ["ftp-run-cycle", "--limit", "1"])

    assert result.exit_code == 1
    assert "already active" in result.stderr
