from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from psycopg import Connection

from lte_pm_platform.db.repositories.topology_reference_repository import TopologyReferenceRepository
from lte_pm_platform.pipeline.ingest.topology_workbook import (
    extract_release_date_from_filename,
    parse_topology_workbook,
)
from lte_pm_platform.pipeline.orchestration.topology_enrichment import sync_topology_enrichment
from lte_pm_platform.utils.paths import runtime_dir


class TopologyManagementService:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.repository = TopologyReferenceRepository(connection)

    def create_preview_snapshot(self, *, source_file_name: str, upload_stream) -> dict:
        stored_path = self._store_uploaded_workbook(source_file_name=source_file_name, upload_stream=upload_stream)
        parsed = parse_topology_workbook(stored_path)
        snapshot_id = self.repository.create_snapshot(
            source_file_name=source_file_name,
            stored_file_path=str(stored_path),
            source_sha256=parsed.source_sha256,
            topology_release_date=extract_release_date_from_filename(source_file_name),
            parser_error_count=len(parsed.parser_errors),
            parser_warning_count=len(parsed.parser_warnings),
            workbook_row_count=parsed.workbook_row_count,
            normalized_row_count=len(parsed.normalized_rows),
            parser_messages={
                "warnings": parsed.parser_warnings,
                "errors": parsed.parser_errors,
            },
        )
        self.repository.insert_snapshot_entity_rows(snapshot_id=snapshot_id, rows=parsed.normalized_rows)
        return self.repository.get_snapshot_summary(snapshot_id) or {}

    def list_snapshots(self) -> list[dict]:
        return self.repository.list_snapshots()

    def get_snapshot_summary(self, snapshot_id: int) -> dict | None:
        return self.repository.get_snapshot_summary(snapshot_id)

    def get_active_snapshot(self) -> dict | None:
        return self.repository.get_active_snapshot()

    def reconcile_snapshot(self, snapshot_id: int) -> dict:
        return self.repository.run_snapshot_reconciliation(snapshot_id)

    def get_reconciliation_details(
        self,
        *,
        reconciliation_id: int,
        issue_type: str | None,
        limit: int,
    ) -> list[dict]:
        return self.repository.list_reconciliation_details(
            reconciliation_id=reconciliation_id,
            issue_type=issue_type,
            limit=limit,
        )

    def apply_snapshot(self, *, snapshot_id: int, activated_by: str | None = None) -> dict:
        return self.repository.apply_snapshot(snapshot_id=snapshot_id, activated_by=activated_by)

    def run_sync_topology(self) -> dict[str, object]:
        return sync_topology_enrichment(repository=self.repository)

    def _store_uploaded_workbook(self, *, source_file_name: str, upload_stream) -> Path:
        upload_dir = runtime_dir() / "topology_snapshots"
        upload_dir.mkdir(parents=True, exist_ok=True)
        safe_name = Path(source_file_name).name
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        destination = upload_dir / f"{timestamp}_{safe_name}"
        with destination.open("wb") as handle:
            upload_stream.seek(0)
            shutil.copyfileobj(upload_stream, handle)
        return destination
