from datetime import datetime

from lte_pm_platform.db.repositories.ftp_remote_file_repository import FtpRemoteFileRepository
from lte_pm_platform.pipeline.ingest.file_discovery import ParsedArchiveFile


class FakeCursor:
    def __init__(self, fetchall_results: list[object] | None = None, fetchone_result=None) -> None:  # noqa: ANN001
        self.fetchall_results = list(fetchall_results or [])
        self.fetchone_result = fetchone_result
        self.executed: list[tuple[str, tuple | None]] = []

    def execute(self, query: str, params: tuple | None = None) -> None:
        self.executed.append((query, params))

    def fetchall(self):  # noqa: ANN201
        if self.fetchall_results:
            return self.fetchall_results.pop(0)
        return []

    def fetchone(self):  # noqa: ANN201
        return self.fetchone_result

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class FakeConnection:
    def __init__(self, fetchall_results: list[object] | None = None, fetchone_result=None) -> None:  # noqa: ANN001
        self.cursor_obj = FakeCursor(fetchall_results, fetchone_result)

    def cursor(self, **kwargs):  # noqa: ANN003, ANN201
        return self.cursor_obj


def build_parsed_file(path: str) -> ParsedArchiveFile:
    return ParsedArchiveFile(
        dataset_family="PM/itbbu/ltefdd",
        filename=path.rsplit("/", 1)[-1],
        interval_start=datetime(2026, 3, 13, 10, 15),
        revision=0,
        extension="tar.gz",
        path=path,
        remote_size_bytes=12345,
        remote_modified_at=datetime(2026, 3, 13, 10, 20),
    )


def test_upsert_discovered_files_inserts_new_remote_files() -> None:
    connection = FakeConnection(fetchall_results=[[]])
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    summary = repository.upsert_discovered_files(
        source_name="default",
        remote_directory="/pm",
        files=[build_parsed_file("/pm/sample.tar.gz")],
    )

    assert summary == {"discovered": 1, "updated": 0}
    assert "SELECT remote_path" in connection.cursor_obj.executed[0][0]
    insert_query, insert_params = connection.cursor_obj.executed[1]
    assert "INSERT INTO ftp_remote_file" in insert_query
    assert insert_params == (
        "default",
        "/pm",
        "sample.tar.gz",
        "/pm/sample.tar.gz",
        "PM/itbbu/ltefdd",
        datetime(2026, 3, 13, 10, 15),
        0,
        "tar.gz",
        12345,
        datetime(2026, 3, 13, 10, 20),
    )


def test_upsert_discovered_files_updates_existing_rows() -> None:
    connection = FakeConnection(fetchall_results=[[("/pm/sample.tar.gz",)]])
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    summary = repository.upsert_discovered_files(
        source_name="default",
        remote_directory="/pm",
        files=[build_parsed_file("/pm/sample.tar.gz")],
    )

    assert summary == {"discovered": 0, "updated": 1}
    update_query, update_params = connection.cursor_obj.executed[1]
    assert "UPDATE ftp_remote_file" in update_query
    assert update_params == (
        "/pm",
        "sample.tar.gz",
        "PM/itbbu/ltefdd",
        datetime(2026, 3, 13, 10, 15),
        0,
        "tar.gz",
        12345,
        datetime(2026, 3, 13, 10, 20),
        "default",
        "/pm/sample.tar.gz",
    )


def test_summarize_status_counts_reads_grouped_rows() -> None:
    rows = [{"status": "DISCOVERED", "file_count": 3}]
    connection = FakeConnection(fetchall_results=[rows])
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    result = repository.summarize_status_counts()

    assert result == rows
    assert "GROUP BY status" in connection.cursor_obj.executed[0][0]


def test_fetch_remote_file_by_id_reads_single_registry_row() -> None:
    rows = [{"id": 7, "remote_path": "/pm/sample.tar.gz"}]
    connection = FakeConnection(fetchall_results=[rows])
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    result = repository.fetch_remote_file_by_id(remote_file_id=7)

    assert result == rows[0]
    query, params = connection.cursor_obj.executed[0]
    assert "AND id = ANY(%s)" in query
    assert params == ([7], 1)


def test_fetch_failure_rows_reads_failed_statuses() -> None:
    rows = [{"id": 1, "status": "FAILED_DOWNLOAD"}]
    connection = FakeConnection(fetchall_results=[rows])
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    result = repository.fetch_failure_rows(limit=5)

    assert result == rows
    query, params = connection.cursor_obj.executed[0]
    assert "status = ANY(%s)" in query
    assert params == (["FAILED_DOWNLOAD", "FAILED_INGEST"], 5)


def test_fetch_latest_scan_at_reads_max_scan_timestamp() -> None:
    connection = FakeConnection(fetchone_result=(datetime(2026, 3, 13, 10, 30),))
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    result = repository.fetch_latest_scan_at()

    assert result == datetime(2026, 3, 13, 10, 30)


def test_fetch_pending_downloads_selects_registry_backed_download_rows() -> None:
    rows = [{"id": 1, "remote_path": "/pm/sample.tar.gz", "status": "DISCOVERED"}]
    connection = FakeConnection(fetchall_results=[rows])
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    result = repository.fetch_pending_downloads(source_name="default", limit=3)

    assert result == rows
    query, params = connection.cursor_obj.executed[0]
    assert "status IN ('DISCOVERED', 'FAILED_DOWNLOAD')" in query
    assert params == ("default", 3)


def test_mark_download_succeeded_updates_status_and_path() -> None:
    connection = FakeConnection()
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    repository.mark_download_succeeded(remote_file_id=12, local_staged_path="/tmp/sample.tar.gz")

    query, params = connection.cursor_obj.executed[0]
    assert "status = 'DOWNLOADED'" in query
    assert "download_attempts = download_attempts + 1" in query
    assert params == ("/tmp/sample.tar.gz", 12)


def test_mark_download_failed_updates_status_and_error() -> None:
    connection = FakeConnection()
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    repository.mark_download_failed(remote_file_id=7, error_message="boom")

    query, params = connection.cursor_obj.executed[0]
    assert "status = 'FAILED_DOWNLOAD'" in query
    assert "download_attempts = download_attempts + 1" in query
    assert params == ("boom", 7)


def test_fetch_retry_download_rows_scopes_to_failed_download_ids() -> None:
    rows = [{"id": 2, "status": "FAILED_DOWNLOAD"}]
    connection = FakeConnection(fetchall_results=[rows])
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    result = repository.fetch_retry_download_rows(source_name="default", remote_file_ids=[2, 3])

    assert result == rows
    query, params = connection.cursor_obj.executed[0]
    assert "status = 'FAILED_DOWNLOAD'" in query
    assert params == ("default", [2, 3])


def test_fetch_pending_ingests_selects_registry_backed_ingest_rows() -> None:
    rows = [{"id": 2, "local_staged_path": "/tmp/sample.tar.gz", "status": "DOWNLOADED"}]
    connection = FakeConnection(fetchall_results=[rows])
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    result = repository.fetch_pending_ingests(source_name="default", limit=4)

    assert result == rows
    query, params = connection.cursor_obj.executed[0]
    assert "status IN ('DOWNLOADED', 'FAILED_INGEST')" in query
    assert params == ("default", 4)


def test_fetch_retry_ingest_rows_scopes_to_failed_ingest_ids() -> None:
    rows = [{"id": 4, "status": "FAILED_INGEST"}]
    connection = FakeConnection(fetchall_results=[rows])
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    result = repository.fetch_retry_ingest_rows(source_name="default", remote_file_ids=[4, 5])

    assert result == rows
    query, params = connection.cursor_obj.executed[0]
    assert "status = 'FAILED_INGEST'" in query
    assert params == ("default", [4, 5])


def test_mark_ingest_succeeded_updates_status_and_linkage() -> None:
    connection = FakeConnection()
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    repository.mark_ingest_succeeded(
        remote_file_id=12,
        file_hash="abc123",
        ingest_run_id="run-1",
        final_file_path="/tmp/archive/sample.tar.gz",
    )

    query, params = connection.cursor_obj.executed[0]
    assert "status = 'INGESTED'" in query
    assert params == ("abc123", "run-1", "/tmp/archive/sample.tar.gz", 12)


def test_mark_ingest_skipped_duplicate_updates_status_and_linkage() -> None:
    connection = FakeConnection()
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    repository.mark_ingest_skipped_duplicate(
        remote_file_id=9,
        file_hash="dup123",
        ingest_run_id="run-2",
        final_file_path="/tmp/archive/sample_1.tar.gz",
    )

    query, params = connection.cursor_obj.executed[0]
    assert "status = 'SKIPPED_DUPLICATE'" in query
    assert params == ("dup123", "run-2", "/tmp/archive/sample_1.tar.gz", 9)


def test_mark_ingest_failed_updates_status_and_error() -> None:
    connection = FakeConnection()
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    repository.mark_ingest_failed(remote_file_id=4, error_message="parse failed")

    query, params = connection.cursor_obj.executed[0]
    assert "status = 'FAILED_INGEST'" in query
    assert params == ("parse failed", 4)


def test_fetch_recent_failures_reads_failed_rows() -> None:
    rows = [{"remote_path": "/pm/sample.tar.gz", "status": "FAILED_DOWNLOAD"}]
    connection = FakeConnection(fetchall_results=[rows])
    repository = FtpRemoteFileRepository(connection)  # type: ignore[arg-type]

    result = repository.fetch_recent_failures(limit=5)

    assert result == rows
    query, params = connection.cursor_obj.executed[0]
    assert "FAILED_DOWNLOAD" in query
    assert params == (5,)
