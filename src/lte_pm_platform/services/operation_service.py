from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time

from psycopg import Connection

from lte_pm_platform.config import Settings
from lte_pm_platform.db.repositories.entity_reference_repository import EntityReferenceRepository
from lte_pm_platform.db.repositories.file_audit_repository import FileAuditRepository
from lte_pm_platform.db.repositories.ftp_remote_file_repository import FtpRemoteFileRepository
from lte_pm_platform.db.repositories.topology_reference_repository import TopologyReferenceRepository
from lte_pm_platform.pipeline.ingest.file_discovery import RevisionPolicy
from lte_pm_platform.pipeline.ingest.ftp_client import FtpClient
from lte_pm_platform.pipeline.loaders.postgres_loader import PostgresLoader
from lte_pm_platform.pipeline.orchestration.ftp_staged_flow import (
    retry_download_registry_files,
    retry_ingest_registry_files,
    run_locked_ftp_cycle,
)
from lte_pm_platform.pipeline.orchestration.run_lock import PipelineCycleLockError
from lte_pm_platform.pipeline.orchestration.sample_pipeline import SamplePipeline
from lte_pm_platform.pipeline.orchestration.topology_enrichment import sync_topology_enrichment
from lte_pm_platform.utils.paths import ftp_cycle_lock_path, ftp_download_dir

_KNOWN_DATASET_FAMILIES = {
    "PM/itbbu/ltefdd",
    "PM/sdr/ltefdd",
    "PM/itbbu/itbbuplat",
}


class OperationValidationError(ValueError):
    pass


@dataclass(frozen=True)
class ParsedTimeWindow:
    start: datetime | None
    end: datetime | None


class OperationService:
    def __init__(self, connection: Connection, settings: Settings) -> None:
        self.connection = connection
        self.settings = settings

    def run_ftp_cycle(
        self,
        *,
        limit: int,
        start: date | datetime | None,
        end: date | datetime | None,
        revision_policy: str,
        families: list[str] | None,
        dry_run: bool,
        retry_failed: bool,
    ) -> dict[str, object]:
        self._validate_families(families)
        window = self._normalize_time_window(start=start, end=end)
        repository = FtpRemoteFileRepository(self.connection)
        pipeline = self._build_pipeline()
        client = self._build_ftp_client()
        try:
            payload = run_locked_ftp_cycle(
                lock_path=ftp_cycle_lock_path(),
                repository=repository,
                client=client,
                pipeline=pipeline,
                source_name="default",
                remote_directory=self.settings.ftp_remote_directory,
                download_dir=ftp_download_dir(),
                start=window.start,
                end=window.end,
                revision_policy=self._parse_revision_policy(revision_policy),
                limit=limit,
                families=families,
                retry_failed=retry_failed,
                dry_run=dry_run,
                trigger_type="api_manual_cycle",
                source_type="ftp",
            )
        except PipelineCycleLockError:
            raise
        except Exception:
            self.connection.rollback()
            raise
        return payload["summary"]

    def retry_download(self, *, ids: list[int]) -> dict[str, object]:
        self._validate_ids(ids)
        repository = FtpRemoteFileRepository(self.connection)
        client = self._build_ftp_client()
        try:
            payload = retry_download_registry_files(
                repository=repository,
                client=client,
                source_name="default",
                download_dir=ftp_download_dir(),
                remote_file_ids=ids,
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return {"results": payload["results"]}

    def retry_ingest(self, *, ids: list[int]) -> dict[str, object]:
        self._validate_ids(ids)
        repository = FtpRemoteFileRepository(self.connection)
        pipeline = self._build_pipeline()
        try:
            payload = retry_ingest_registry_files(
                repository=repository,
                pipeline=pipeline,
                source_name="default",
                trigger_type="api_retry_ingest",
                source_type="ftp",
                remote_file_ids=ids,
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return {"results": payload["results"]}

    def sync_entities(self) -> dict[str, object]:
        entity_repository = EntityReferenceRepository(self.connection)
        audit_repository = FileAuditRepository(self.connection)
        try:
            count = entity_repository.refresh_from_raw_entities()
            audits_updated = audit_repository.mark_success_normalization_completed()
            self.connection.commit()
        except Exception as exc:
            self.connection.rollback()
            audits_updated = audit_repository.mark_success_normalization_failed(str(exc))
            self.connection.commit()
            raise
        return {"rows_synced": count, "audits_updated": audits_updated}

    def sync_topology(self) -> dict[str, object]:
        repository = TopologyReferenceRepository(self.connection)
        try:
            payload = sync_topology_enrichment(repository=repository)
            unmapped_entities = len(repository.list_unmapped_entities(limit=100000))
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        return {
            "rows_synced": payload["rows_synced"],
            "unmapped_entities": unmapped_entities,
        }

    def _build_pipeline(self) -> SamplePipeline:
        return SamplePipeline(
            loader=PostgresLoader(self.connection),
            audit_repository=FileAuditRepository(self.connection),
        )

    def _build_ftp_client(self) -> FtpClient:
        if not self.settings.ftp_host:
            raise OperationValidationError("FTP_HOST is not configured.")
        return FtpClient(
            host=self.settings.ftp_host,
            port=self.settings.ftp_port,
            username=self.settings.ftp_username,
            password=self.settings.ftp_password,
            remote_directory=self.settings.ftp_remote_directory,
            passive_mode=self.settings.ftp_passive_mode,
        )

    @staticmethod
    def _validate_ids(ids: list[int]) -> None:
        if not ids:
            raise OperationValidationError("ids must not be empty")

    @staticmethod
    def _validate_families(families: list[str] | None) -> None:
        if families is None:
            return
        invalid = sorted(family for family in families if family not in _KNOWN_DATASET_FAMILIES)
        if invalid:
            raise OperationValidationError(f"unsupported dataset families: {', '.join(invalid)}")

    @staticmethod
    def _parse_revision_policy(value: str) -> RevisionPolicy:
        allowed: set[str] = {"additive", "base-only", "revisions-only", "latest-only"}
        if value not in allowed:
            raise OperationValidationError(f"unsupported revision policy: {value}")
        return value  # type: ignore[return-value]

    @staticmethod
    def _normalize_time_window(
        *,
        start: date | datetime | None,
        end: date | datetime | None,
    ) -> ParsedTimeWindow:
        normalized_start = OperationService._normalize_boundary(start, end_of_day=False)
        normalized_end = OperationService._normalize_boundary(end, end_of_day=True)
        return ParsedTimeWindow(start=normalized_start, end=normalized_end)

    @staticmethod
    def _normalize_boundary(value: date | datetime | None, *, end_of_day: bool) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        boundary_time = time.max if end_of_day else time.min
        normalized = datetime.combine(value, boundary_time)
        return normalized.replace(tzinfo=None)
