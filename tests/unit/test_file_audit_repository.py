from uuid import uuid4

from lte_pm_platform.db.repositories.file_audit_repository import FileAuditRepository
from lte_pm_platform.domain.models import IngestSummary


class FakeCursor:
    def __init__(self, fetchone_result) -> None:  # noqa: ANN001
        self.fetchone_result = fetchone_result
        self.executed: list[tuple[str, tuple]] = []
        self.rowcount = 1

    def execute(self, query: str, params: tuple) -> None:
        self.executed.append((query, params))

    def fetchone(self):  # noqa: ANN201
        return self.fetchone_result

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
        return None


class FakeConnection:
    def __init__(self, fetchone_result) -> None:  # noqa: ANN001
        self.cursor_obj = FakeCursor(fetchone_result)
        self.commits = 0

    def cursor(self, **kwargs):  # noqa: ANN003, ANN201
        return self.cursor_obj

    def commit(self) -> None:
        self.commits += 1


def test_has_successful_hash_returns_true_when_success_exists() -> None:
    repository = FileAuditRepository(FakeConnection((True,)))  # type: ignore[arg-type]

    assert repository.has_successful_hash("abc123") is True
    assert repository.connection.cursor_obj.executed[0][1] == ("abc123",)


def test_has_successful_hash_returns_false_when_success_missing() -> None:
    repository = FileAuditRepository(FakeConnection((False,)))  # type: ignore[arg-type]

    assert repository.has_successful_hash("abc123") is False


def test_log_result_writes_run_metadata() -> None:
    connection = FakeConnection((False,))
    repository = FileAuditRepository(connection)  # type: ignore[arg-type]
    summary = IngestSummary(
        source_file="sample.zip",
        run_id=uuid4(),
        trigger_type="ftp_fetch",
        source_type="ftp",
        file_hash="abc123",
        status="SUCCESS",
        lifecycle_action="archived",
        final_file_path="/tmp/archive/sample.zip",
    )

    repository.log_result(summary=summary, file_hash=summary.file_hash, error_message=None)

    params = connection.cursor_obj.executed[0][1]
    assert params[0] == "sample.zip"
    assert params[1] == "abc123"
    assert params[2] == summary.run_id
    assert params[3] == "ftp_fetch"
    assert params[4] == "ftp"
    assert params[12] == "PENDING"
    assert params[13] == "archived"
    assert params[14] == "PENDING"
    assert params[15] == "/tmp/archive/sample.zip"
    assert connection.commits == 0


def test_update_lifecycle_writes_expected_fields() -> None:
    connection = FakeConnection((False,))
    repository = FileAuditRepository(connection)  # type: ignore[arg-type]

    repository.update_lifecycle(
        run_id="run-123",
        lifecycle_status="COMPLETED",
        lifecycle_action="archived",
        final_file_path="/tmp/archive/sample.zip",
        error_message=None,
    )

    query, params = connection.cursor_obj.executed[0]
    assert "UPDATE file_audit" in query
    assert params == ("COMPLETED", "archived", "/tmp/archive/sample.zip", None, "run-123")


def test_mark_success_normalization_completed_updates_pending_success_rows() -> None:
    connection = FakeConnection((False,))
    repository = FileAuditRepository(connection)  # type: ignore[arg-type]

    updated = repository.mark_success_normalization_completed()

    assert updated == 1
    assert "normalization_status = 'COMPLETED'" in connection.cursor_obj.executed[0][0]


def test_mark_success_normalization_failed_updates_pending_success_rows() -> None:
    connection = FakeConnection((False,))
    repository = FileAuditRepository(connection)  # type: ignore[arg-type]

    updated = repository.mark_success_normalization_failed("sync failed")

    assert updated == 1
    assert connection.cursor_obj.executed[0][1] == ("sync failed",)
