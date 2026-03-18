from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any, Callable

from psycopg import Connection

from lte_pm_platform.config import Settings
from lte_pm_platform.db.repositories.entity_reference_repository import EntityReferenceRepository
from lte_pm_platform.db.repositories.file_audit_repository import FileAuditRepository
from lte_pm_platform.db.repositories.ftp_cycle_run_repository import FtpCycleRunRepository
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
    interval_start: datetime | None
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
        interval_start: datetime | None,
        start: date | datetime | None,
        end: date | datetime | None,
        revision_policy: str,
        families: list[str] | None,
        dry_run: bool,
        retry_failed: bool,
    ) -> dict[str, object]:
        self._validate_families(families)
        window = self._normalize_time_window(interval_start=interval_start, start=start, end=end)
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
                remote_directories=self.settings.ftp_remote_directories,
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

    def enqueue_ftp_cycle(
        self,
        *,
        limit: int,
        interval_start: datetime | None,
        start: date | datetime | None,
        end: date | datetime | None,
        revision_policy: str,
        families: list[str] | None,
        dry_run: bool,
        retry_failed: bool,
        trigger_source: str,
    ) -> dict[str, Any]:
        self._validate_families(families)
        self._parse_revision_policy(revision_policy)
        window = self._normalize_time_window(interval_start=interval_start, start=start, end=end)
        repository = FtpCycleRunRepository(self.connection)
        parameters = {
            "limit": limit,
            "interval_start": window.interval_start.isoformat() if window.interval_start is not None else None,
            "start": window.start.isoformat() if window.start is not None else None,
            "end": window.end.isoformat() if window.end is not None else None,
            "revision_policy": revision_policy,
            "families": list(families) if families is not None else None,
            "dry_run": dry_run,
            "retry_failed": retry_failed,
        }
        return repository.create_run(
            trigger_source=trigger_source,
            parameters=parameters,
            summary={"scanned": 0, "downloaded": 0, "ingested": 0, "failed_downloads": 0, "failed_ingests": 0},
        )

    def claim_next_ftp_cycle_run(self) -> dict[str, Any] | None:
        return FtpCycleRunRepository(self.connection).claim_next_queued_run()

    def list_ftp_cycle_runs(self, *, limit: int = 20, statuses: list[str] | None = None) -> list[dict[str, Any]]:
        return FtpCycleRunRepository(self.connection).list_runs(limit=limit, statuses=statuses)

    def get_ftp_cycle_run(self, *, run_id: int) -> dict[str, Any] | None:
        return FtpCycleRunRepository(self.connection).get_run(run_id=run_id)

    def list_ftp_cycle_run_events(self, *, run_id: int, limit: int = 200) -> list[dict[str, Any]]:
        return FtpCycleRunRepository(self.connection).list_events(run_id=run_id, limit=limit)

    def execute_ftp_cycle_run(self, *, run_id: int) -> dict[str, Any]:
        run_repository = FtpCycleRunRepository(self.connection)
        run = run_repository.get_run(run_id=run_id)
        if run is None:
            raise OperationValidationError(f"ftp cycle run not found: {run_id}")

        parameters = dict(run.get("parameters_json") or {})
        limit = int(parameters.get("limit") or 20)
        interval_start = self._parse_iso_datetime(parameters.get("interval_start"))
        start = self._parse_iso_datetime(parameters.get("start"))
        end = self._parse_iso_datetime(parameters.get("end"))
        revision_policy = str(parameters.get("revision_policy") or "additive")
        families = parameters.get("families")
        dry_run = bool(parameters.get("dry_run"))
        retry_failed = bool(parameters.get("retry_failed"))

        event_logger = self._build_run_event_logger(run_id=run_id)
        summary_updater = self._build_run_summary_updater(run_id=run_id)

        event_logger(
            "discover",
            "info",
            "ftp cycle run started",
            {
                "limit": limit,
                "interval_start": interval_start.isoformat() if interval_start is not None else None,
                "dry_run": dry_run,
                "retry_failed": retry_failed,
                "families": families,
            },
        )
        try:
            summary = self._run_ftp_cycle_with_callbacks(
                limit=limit,
                start=start,
                end=end,
                revision_policy=revision_policy,
                families=families,
                dry_run=dry_run,
                retry_failed=retry_failed,
                event_logger=event_logger,
                summary_updater=summary_updater,
                trigger_type="api_manual_cycle",
                source_type="ftp",
            )
            run_repository.mark_succeeded(run_id=run_id, summary=summary)
            event_logger("finalize", "info", "ftp cycle run completed", summary)
            return summary
        except Exception as exc:
            current = run_repository.get_run(run_id=run_id)
            run_repository.mark_failed(
                run_id=run_id,
                error_message=str(exc),
                summary=dict(current.get("summary_json") or {}) if current is not None else None,
            )
            event_logger("finalize", "error", "ftp cycle run failed", {"error": str(exc)})
            raise

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
        if not self.settings.ftp_username:
            raise OperationValidationError("FTP_USERNAME is not configured.")
        if not self.settings.ftp_password:
            raise OperationValidationError("FTP_PASSWORD is not configured.")
        return FtpClient(
            host=self.settings.ftp_host,
            port=self.settings.ftp_port,
            username=self.settings.ftp_username,
            password=self.settings.ftp_password,
            remote_directory=self.settings.ftp_remote_directory,
            passive_mode=self.settings.ftp_passive_mode,
        )

    def _run_ftp_cycle_with_callbacks(
        self,
        *,
        limit: int,
        start: datetime | None,
        end: datetime | None,
        revision_policy: str,
        families: list[str] | None,
        dry_run: bool,
        retry_failed: bool,
        event_logger: Callable[[str, str, str, dict[str, Any] | None], None] | None,
        summary_updater: Callable[[dict[str, Any]], None] | None,
        trigger_type: str,
        source_type: str,
    ) -> dict[str, Any]:
        self._validate_families(families)
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
                remote_directories=self.settings.ftp_remote_directories,
                download_dir=ftp_download_dir(),
                start=start,
                end=end,
                revision_policy=self._parse_revision_policy(revision_policy),
                limit=limit,
                families=families,
                retry_failed=retry_failed,
                dry_run=dry_run,
                trigger_type=trigger_type,
                source_type=source_type,
                event_callback=event_logger,
                summary_callback=summary_updater,
            )
        except Exception:
            self.connection.rollback()
            raise
        return payload["summary"]

    def _build_run_event_logger(self, *, run_id: int) -> Callable[[str, str, str, dict[str, Any] | None], None]:
        repository = FtpCycleRunRepository(self.connection)

        def _log(stage: str, level: str, message: str, metrics: dict[str, Any] | None = None) -> None:
            repository.append_event(
                run_id=run_id,
                stage=stage,
                level=level,
                message=message,
                metrics=metrics,
            )

        return _log

    def _build_run_summary_updater(self, *, run_id: int) -> Callable[[dict[str, Any]], None]:
        repository = FtpCycleRunRepository(self.connection)

        def _update(summary: dict[str, Any]) -> None:
            repository.update_summary(run_id=run_id, summary=summary)

        return _update

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
        interval_start: datetime | None,
        start: date | datetime | None,
        end: date | datetime | None,
    ) -> ParsedTimeWindow:
        if interval_start is not None:
            normalized_interval_start = OperationService._normalize_boundary(interval_start, end_of_day=False)
            if normalized_interval_start is None:
                raise OperationValidationError("interval_start must not be empty")
            return ParsedTimeWindow(
                interval_start=normalized_interval_start,
                start=normalized_interval_start,
                end=normalized_interval_start + timedelta(minutes=15),
            )
        normalized_start = OperationService._normalize_boundary(start, end_of_day=False)
        normalized_end = OperationService._normalize_boundary(end, end_of_day=True)
        return ParsedTimeWindow(interval_start=None, start=normalized_start, end=normalized_end)

    @staticmethod
    def _normalize_boundary(value: date | datetime | None, *, end_of_day: bool) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        boundary_time = time.max if end_of_day else time.min
        normalized = datetime.combine(value, boundary_time)
        return normalized.replace(tzinfo=None)

    @staticmethod
    def _parse_iso_datetime(value: Any) -> datetime | None:
        if value in {None, ""}:
            return None
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(str(value))
