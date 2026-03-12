from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from psycopg.errors import UniqueViolation

from lte_pm_platform.db.repositories.file_audit_repository import FileAuditRepository
from lte_pm_platform.domain.models import IngestSummary
from lte_pm_platform.pipeline.ingest.zip_reader import iter_csv_members
from lte_pm_platform.pipeline.loaders.postgres_loader import PostgresLoader
from lte_pm_platform.pipeline.orchestration.file_lifecycle import move_by_status
from lte_pm_platform.pipeline.parsers.zte_lte_pm import ZteLtePmParser
from lte_pm_platform.utils.hash import file_sha256


class SamplePipeline:
    def __init__(
        self,
        *,
        loader: PostgresLoader,
        parser: ZteLtePmParser | None = None,
        audit_repository: FileAuditRepository | None = None,
    ) -> None:
        self.loader = loader
        self.parser = parser or ZteLtePmParser()
        self.audit_repository = audit_repository

    def load_zip(
        self,
        zip_path: Path,
        *,
        trigger_type: str,
        source_type: str,
    ) -> IngestSummary:
        source_file = zip_path.name
        file_hash = file_sha256(zip_path)
        summary = IngestSummary(
            source_file=source_file,
            run_id=uuid4(),
            trigger_type=trigger_type,
            source_type=source_type,
            file_hash=file_hash,
            status="RUNNING",
            final_file_path=str(zip_path),
        )

        if (
            self.audit_repository is not None
            and self.audit_repository.has_successful_hash(file_hash)
        ):
            summary.status = "SKIPPED_DUPLICATE"
            self._finalize_and_persist_lifecycle(zip_path, summary)
            self._log_result(summary, commit=True)
            return summary

        try:
            for csv_name, text_stream in iter_csv_members(zip_path):
                summary.csv_files_found += 1
                records = self.parser.parse_csv(
                    text_stream=text_stream,
                    source_file=source_file,
                    csv_name=csv_name,
                    summary=summary,
                )
                summary.rows_inserted += self.loader.load(records)
            summary.status = "SUCCESS"
            self._log_result(summary, commit=False)
            self.loader.commit()
        except UniqueViolation:
            self.loader.rollback()
            summary.status = "SKIPPED_DUPLICATE"
            summary.rows_inserted = 0
            self._finalize_and_persist_lifecycle(zip_path, summary)
            self._log_result(summary, commit=True)
            return summary
        except Exception as exc:
            self.loader.rollback()
            summary.status = "FAILED"
            summary.rows_inserted = 0
            summary.error_message = str(exc)
            self._finalize_and_persist_lifecycle(zip_path, summary)
            self._log_result(summary, commit=True)
            raise

        self._finalize_and_persist_lifecycle(zip_path, summary)

        return summary

    def _finalize_file(self, zip_path: Path, summary: IngestSummary) -> None:
        if not zip_path.exists():
            return
        action, final_path = move_by_status(zip_path, summary.status)
        summary.lifecycle_action = action
        summary.final_file_path = str(final_path)
        summary.lifecycle_status = "COMPLETED"

    def _log_result(self, summary: IngestSummary, *, commit: bool) -> None:
        if self.audit_repository is None:
            return
        self.audit_repository.log_result(
            summary=summary,
            file_hash=summary.file_hash,
            error_message=summary.error_message,
        )
        if commit:
            self.audit_repository.commit()

    def _update_audit_lifecycle(self, summary: IngestSummary) -> None:
        if self.audit_repository is None:
            return
        updated_rows = self.audit_repository.update_lifecycle(
            run_id=str(summary.run_id),
            lifecycle_status=summary.lifecycle_status,
            lifecycle_action=summary.lifecycle_action,
            final_file_path=summary.final_file_path,
            error_message=summary.error_message,
        )
        if updated_rows:
            self.audit_repository.commit()

    def _finalize_and_persist_lifecycle(self, zip_path: Path, summary: IngestSummary) -> None:
        try:
            self._finalize_file(zip_path, summary)
        except Exception as exc:
            summary.lifecycle_status = "FAILED"
            summary.lifecycle_action = "left_in_place"
            summary.final_file_path = str(zip_path)
            if summary.error_message is None:
                summary.error_message = str(exc)
        self._update_audit_lifecycle(summary)
