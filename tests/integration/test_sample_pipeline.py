import zipfile
from pathlib import Path
from uuid import UUID

from psycopg.errors import UniqueViolation

from lte_pm_platform.domain.models import NormalizedPmRecord
from lte_pm_platform.pipeline.orchestration import file_lifecycle
from lte_pm_platform.pipeline.orchestration.sample_pipeline import SamplePipeline


class FakeLoader:
    def __init__(self, *, fail: Exception | None = None) -> None:
        self.loaded: list[NormalizedPmRecord] = []
        self.load_calls = 0
        self.commits = 0
        self.rollbacks = 0
        self.fail = fail

    def load(self, records):  # noqa: ANN001
        self.load_calls += 1
        if self.fail is not None:
            raise self.fail
        chunk = list(records)
        self.loaded.extend(chunk)
        return len(chunk)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class FakeAuditRepository:
    def __init__(
        self,
        *,
        existing_hashes: set[str] | None = None,
        fail_first_log: Exception | None = None,
    ) -> None:
        self.existing_hashes = existing_hashes or set()
        self.logged_statuses: list[str] = []
        self.logged_hashes: list[str | None] = []
        self.logged_summaries = []
        self.updated_lifecycle: list[tuple[str, str | None, str | None, str | None]] = []
        self.commits = 0
        self.rollbacks = 0
        self.fail_first_log = fail_first_log
        self.log_calls = 0

    def has_successful_hash(self, file_hash: str) -> bool:
        return file_hash in self.existing_hashes

    def log_result(self, *, summary, file_hash, error_message=None) -> None:  # noqa: ANN001
        self.log_calls += 1
        if self.fail_first_log is not None and self.log_calls == 1:
            raise self.fail_first_log
        self.logged_statuses.append(summary.status)
        self.logged_hashes.append(file_hash)
        self.logged_summaries.append(summary)

    def update_lifecycle(  # noqa: ANN001
        self,
        *,
        run_id,
        lifecycle_status,
        lifecycle_action,
        final_file_path,
        error_message,
    ) -> None:
        self.updated_lifecycle.append(
            (run_id, lifecycle_status, lifecycle_action, final_file_path, error_message)
        )

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


def test_sample_pipeline_smoke(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    zip_path = tmp_path / "sample.zip"
    archive_dir = tmp_path / "archive"
    rejected_dir = tmp_path / "rejected"
    monkeypatch.setattr(file_lifecycle, "archive_dir", lambda: archive_dir)
    monkeypatch.setattr(file_lifecycle, "rejected_dir", lambda: rejected_dir)
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "pm.csv",
            (
                "COLLECTTIME,TRNCMEID,ANI,SBNID,eNBID,CellID,MEID,SYSTEMMODE,MIDFLAG,NETYPE,C380340003,C380340004,EXTRA_COL\n"
                "20260304143000,TR1,CELL_1,41,1010,1,ME1,FDD,MID_A,ENODEB,1,2,foo\n"
            ),
        )

    loader = FakeLoader()
    pipeline = SamplePipeline(loader=loader)

    summary = pipeline.load_zip(zip_path, trigger_type="local_cli", source_type="local")

    assert summary.source_file == "sample.zip"
    UUID(str(summary.run_id))
    assert summary.trigger_type == "local_cli"
    assert summary.source_type == "local"
    assert summary.csv_files_found == 1
    assert summary.input_rows_read == 1
    assert summary.normalized_rows_emitted == 2
    assert summary.rows_inserted == 2
    assert summary.unknown_columns == {"EXTRA_COL"}
    assert summary.null_counter_values == 0
    assert summary.status == "SUCCESS"
    assert summary.lifecycle_action == "archived"
    assert summary.lifecycle_status == "COMPLETED"
    assert Path(summary.final_file_path or "").parent == archive_dir
    assert not zip_path.exists()
    assert loader.load_calls == 1
    assert loader.commits == 1
    assert loader.rollbacks == 0
    assert [record.counter_id for record in loader.loaded] == ["C380340003", "C380340004"]
    assert loader.loaded[0].sbnid == "41"
    assert loader.loaded[0].enbid == "1010"
    assert loader.loaded[0].cellid == "1"
    assert loader.loaded[0].meid == "ME1"


def test_sample_pipeline_skips_duplicate_by_file_hash(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    zip_path = tmp_path / "sample.zip"
    archive_dir = tmp_path / "archive"
    rejected_dir = tmp_path / "rejected"
    monkeypatch.setattr(file_lifecycle, "archive_dir", lambda: archive_dir)
    monkeypatch.setattr(file_lifecycle, "rejected_dir", lambda: rejected_dir)
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "pm.csv",
            (
                "COLLECTTIME,TRNCMEID,ANI,SYSTEMMODE,MIDFLAG,NETYPE,C380340003\n"
                "20260304143000,ME1,CELL_1,FDD,MID_A,ENODEB,1\n"
            ),
        )

    from lte_pm_platform.utils.hash import file_sha256

    file_hash = file_sha256(zip_path)
    loader = FakeLoader()
    audit_repository = FakeAuditRepository(existing_hashes={file_hash})
    pipeline = SamplePipeline(loader=loader, audit_repository=audit_repository)

    summary = pipeline.load_zip(zip_path, trigger_type="local_cli", source_type="local")

    assert summary.status == "SKIPPED_DUPLICATE"
    assert summary.csv_files_found == 0
    assert summary.input_rows_read == 0
    assert summary.normalized_rows_emitted == 0
    assert summary.rows_inserted == 0
    assert summary.lifecycle_action == "archived"
    assert summary.lifecycle_status == "COMPLETED"
    assert Path(summary.final_file_path or "").parent == archive_dir
    assert loader.load_calls == 0
    assert audit_repository.logged_statuses == ["SKIPPED_DUPLICATE"]
    assert audit_repository.commits == 1


def test_sample_pipeline_moves_failed_file_to_rejected(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    zip_path = tmp_path / "bad_sample.zip"
    archive_dir = tmp_path / "archive"
    rejected_dir = tmp_path / "rejected"
    monkeypatch.setattr(file_lifecycle, "archive_dir", lambda: archive_dir)
    monkeypatch.setattr(file_lifecycle, "rejected_dir", lambda: rejected_dir)
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "pm.csv",
            (
                "COLLECTTIME,TRNCMEID,ANI,SYSTEMMODE,MIDFLAG,NETYPE,C380340003\n"
                "badtime,ME1,CELL_1,FDD,MID_A,ENODEB,1\n"
            ),
        )

    audit_repository = FakeAuditRepository()
    pipeline = SamplePipeline(loader=FakeLoader(), audit_repository=audit_repository)

    try:
        pipeline.load_zip(zip_path, trigger_type="local_cli", source_type="local")
    except ValueError:
        pass
    else:
        raise AssertionError("Expected parser failure")

    assert not zip_path.exists()
    rejected_files = list(rejected_dir.glob("bad_sample*.zip"))
    assert len(rejected_files) == 1
    assert audit_repository.logged_statuses == ["FAILED"]
    assert audit_repository.logged_summaries[0].lifecycle_action == "rejected"
    assert audit_repository.logged_summaries[0].lifecycle_status == "COMPLETED"


def test_sample_pipeline_rolls_back_when_loader_fails(tmp_path: Path, monkeypatch) -> None:  # noqa: ANN001
    zip_path = tmp_path / "load_fail.zip"
    archive_dir = tmp_path / "archive"
    rejected_dir = tmp_path / "rejected"
    monkeypatch.setattr(file_lifecycle, "archive_dir", lambda: archive_dir)
    monkeypatch.setattr(file_lifecycle, "rejected_dir", lambda: rejected_dir)
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "pm.csv",
            (
                "COLLECTTIME,TRNCMEID,ANI,SYSTEMMODE,MIDFLAG,NETYPE,C380340003\n"
                "20260304143000,ME1,CELL_1,FDD,MID_A,ENODEB,1\n"
            ),
        )

    loader = FakeLoader(fail=RuntimeError("insert failed"))
    audit_repository = FakeAuditRepository()
    pipeline = SamplePipeline(loader=loader, audit_repository=audit_repository)

    try:
        pipeline.load_zip(zip_path, trigger_type="local_cli", source_type="local")
    except RuntimeError as exc:
        assert str(exc) == "insert failed"
    else:
        raise AssertionError("Expected loader failure")

    assert loader.commits == 0
    assert loader.rollbacks == 1
    assert audit_repository.logged_statuses == ["FAILED"]
    assert audit_repository.logged_summaries[0].rows_inserted == 0
    assert audit_repository.commits == 1


def test_sample_pipeline_rolls_back_when_audit_write_fails_before_commit(  # noqa: ANN001
    tmp_path: Path,
    monkeypatch,
) -> None:
    zip_path = tmp_path / "audit_fail.zip"
    archive_dir = tmp_path / "archive"
    rejected_dir = tmp_path / "rejected"
    monkeypatch.setattr(file_lifecycle, "archive_dir", lambda: archive_dir)
    monkeypatch.setattr(file_lifecycle, "rejected_dir", lambda: rejected_dir)
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "pm.csv",
            (
                "COLLECTTIME,TRNCMEID,ANI,SYSTEMMODE,MIDFLAG,NETYPE,C380340003\n"
                "20260304143000,ME1,CELL_1,FDD,MID_A,ENODEB,1\n"
            ),
        )

    loader = FakeLoader()
    audit_repository = FakeAuditRepository(fail_first_log=RuntimeError("audit insert failed"))
    pipeline = SamplePipeline(loader=loader, audit_repository=audit_repository)

    try:
        pipeline.load_zip(zip_path, trigger_type="local_cli", source_type="local")
    except RuntimeError as exc:
        assert str(exc) == "audit insert failed"
    else:
        raise AssertionError("Expected audit failure")

    assert loader.commits == 0
    assert loader.rollbacks == 1
    assert audit_repository.logged_statuses == ["FAILED"]
    assert audit_repository.logged_summaries[0].rows_inserted == 0
    assert audit_repository.commits == 1


def test_sample_pipeline_converts_success_hash_conflict_to_duplicate_skip(  # noqa: ANN001
    tmp_path: Path,
    monkeypatch,
) -> None:
    zip_path = tmp_path / "duplicate_race.zip"
    archive_dir = tmp_path / "archive"
    rejected_dir = tmp_path / "rejected"
    monkeypatch.setattr(file_lifecycle, "archive_dir", lambda: archive_dir)
    monkeypatch.setattr(file_lifecycle, "rejected_dir", lambda: rejected_dir)
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "pm.csv",
            (
                "COLLECTTIME,TRNCMEID,ANI,SYSTEMMODE,MIDFLAG,NETYPE,C380340003\n"
                "20260304143000,ME1,CELL_1,FDD,MID_A,ENODEB,1\n"
            ),
        )

    loader = FakeLoader()
    audit_repository = FakeAuditRepository(fail_first_log=UniqueViolation("duplicate success hash"))
    pipeline = SamplePipeline(loader=loader, audit_repository=audit_repository)

    summary = pipeline.load_zip(zip_path, trigger_type="local_cli", source_type="local")

    assert summary.status == "SKIPPED_DUPLICATE"
    assert summary.rows_inserted == 0
    assert summary.lifecycle_status == "COMPLETED"
    assert loader.commits == 0
    assert loader.rollbacks == 1
    assert audit_repository.logged_statuses == ["SKIPPED_DUPLICATE"]
    assert audit_repository.commits == 1
