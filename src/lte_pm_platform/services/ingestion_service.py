from __future__ import annotations

from psycopg import Connection

from lte_pm_platform.db.repositories.ftp_remote_file_repository import FtpRemoteFileRepository
from lte_pm_platform.db.repositories.pm_sample_repository import PmSampleRepository
from lte_pm_platform.pipeline.orchestration.ftp_staged_flow import (
    build_operational_status,
    inspect_failure_row,
    inspect_failure_rows,
    reconcile_registry_rows,
)

_REQUIRED_INTERVAL_FAMILIES = (
    "PM/itbbu/ltefdd",
    "PM/sdr/ltefdd",
)


class IngestionService:
    def __init__(self, connection: Connection) -> None:
        self.repository = FtpRemoteFileRepository(connection)
        self.pm_repository = PmSampleRepository(connection)

    def get_status(self, *, limit_recent_failures: int) -> dict[str, object]:
        return build_operational_status(repository=self.repository, limit=limit_recent_failures)

    def list_failures(self, *, limit: int) -> list[dict[str, object]]:
        return inspect_failure_rows(repository=self.repository, limit=limit)

    def get_failure(self, *, remote_file_id: int) -> dict[str, object] | None:
        return inspect_failure_row(repository=self.repository, remote_file_id=remote_file_id)

    def get_reconciliation_preview(self, *, limit: int) -> list[dict[str, object]]:
        return reconcile_registry_rows(repository=self.repository, limit=limit)

    def list_source_intervals(self, *, limit: int) -> list[dict[str, object]]:
        rows = self.repository.summarize_source_intervals(limit=limit)
        topology_by_interval = self._fetch_interval_topology_coverage(rows)
        return [self._build_interval_quality_row(row, topology_by_interval=topology_by_interval) for row in rows]

    @staticmethod
    def _build_interval_quality_row(
        row: dict[str, object],
        *,
        topology_by_interval: dict[object, dict[str, object]],
    ) -> dict[str, object]:
        families_present = sorted(str(value) for value in (row.get("families_present") or []))
        statuses_present = sorted(str(value) for value in (row.get("statuses_present") or []))
        missing_families = [family for family in _REQUIRED_INTERVAL_FAMILIES if family not in families_present]
        partial_interval = bool(families_present) and bool(missing_families) and any(
            family in families_present for family in _REQUIRED_INTERVAL_FAMILIES
        )
        quality_status = "complete" if not missing_families else "partial"
        quality_notes = (
            "Required LTE PM families discovered for this interval."
            if not missing_families
            else f"Missing expected families: {', '.join(missing_families)}"
        )
        topology = topology_by_interval.get(row.get("interval_start"), {})
        topology_mapped_count = int(topology.get("topology_mapped_count") or 0)
        topology_unmapped_count = int(topology.get("topology_unmapped_count") or 0)
        topology_total = topology_mapped_count + topology_unmapped_count
        topology_coverage_pct = (
            round((topology_mapped_count / topology_total) * 100, 2)
            if topology_total > 0
            else None
        )
        return {
            **row,
            "families_present": families_present,
            "family_count": len(families_present),
            "statuses_present": statuses_present,
            "missing_families": missing_families,
            "partial_interval": partial_interval,
            "quality_status": quality_status,
            "quality_notes": quality_notes,
            "topology_mapped_count": topology_mapped_count,
            "topology_unmapped_count": topology_unmapped_count,
            "topology_coverage_pct": topology_coverage_pct,
        }

    def _fetch_interval_topology_coverage(self, rows: list[dict[str, object]]) -> dict[object, dict[str, object]]:
        interval_starts = [row["interval_start"] for row in rows if row.get("interval_start") is not None]
        ingested_source_rows = self.repository.list_ingested_interval_source_files(
            interval_starts=interval_starts,
            dataset_families=_REQUIRED_INTERVAL_FAMILIES,
        )
        source_files = list(
            dict.fromkeys(
                str(row["remote_filename"])
                for row in ingested_source_rows
                if row.get("remote_filename")
            )
        )
        coverage_rows = self.pm_repository.summarize_interval_topology_coverage(source_files=source_files)
        return {row["interval_start"]: row for row in coverage_rows}
