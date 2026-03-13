from __future__ import annotations

from psycopg import Connection

from lte_pm_platform.db.repositories.ftp_remote_file_repository import FtpRemoteFileRepository
from lte_pm_platform.pipeline.orchestration.ftp_staged_flow import (
    build_operational_status,
    inspect_failure_row,
    inspect_failure_rows,
    reconcile_registry_rows,
)


class IngestionService:
    def __init__(self, connection: Connection) -> None:
        self.repository = FtpRemoteFileRepository(connection)

    def get_status(self, *, limit_recent_failures: int) -> dict[str, object]:
        return build_operational_status(repository=self.repository, limit=limit_recent_failures)

    def list_failures(self, *, limit: int) -> list[dict[str, object]]:
        return inspect_failure_rows(repository=self.repository, limit=limit)

    def get_failure(self, *, remote_file_id: int) -> dict[str, object] | None:
        return inspect_failure_row(repository=self.repository, remote_file_id=remote_file_id)

    def get_reconciliation_preview(self, *, limit: int) -> list[dict[str, object]]:
        return reconcile_registry_rows(repository=self.repository, limit=limit)
