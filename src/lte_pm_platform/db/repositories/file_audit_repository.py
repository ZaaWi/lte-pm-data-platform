from __future__ import annotations

from psycopg import Connection
from psycopg.rows import dict_row

from lte_pm_platform.domain.models import IngestSummary


class FileAuditRepository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def has_successful_hash(self, file_hash: str) -> bool:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM file_audit
                    WHERE file_hash = %s
                      AND status = 'SUCCESS'
                )
                """,
                (file_hash,),
            )
            row = cursor.fetchone()
        return bool(row[0]) if row is not None else False

    def log_result(
        self,
        *,
        summary: IngestSummary,
        file_hash: str | None,
        error_message: str | None = None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO file_audit (
                    source_file,
                    file_hash,
                    run_id,
                    trigger_type,
                    source_type,
                    processed_at,
                    status,
                    csv_files_found,
                    input_rows_read,
                    normalized_rows_emitted,
                    rows_inserted,
                    unknown_columns,
                    null_counter_values,
                    lifecycle_status,
                    lifecycle_action,
                    normalization_status,
                    final_file_path,
                    error_message
                )
                VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    summary.source_file,
                    file_hash,
                    summary.run_id,
                    summary.trigger_type,
                    summary.source_type,
                    summary.status,
                    summary.csv_files_found,
                    summary.input_rows_read,
                    summary.normalized_rows_emitted,
                    summary.rows_inserted,
                    sorted(summary.unknown_columns),
                    summary.null_counter_values,
                    summary.lifecycle_status,
                    summary.lifecycle_action,
                    summary.normalization_status,
                    summary.final_file_path,
                    error_message,
                ),
            )

    def update_lifecycle(
        self,
        *,
        run_id: str,
        lifecycle_status: str,
        lifecycle_action: str | None,
        final_file_path: str | None,
        error_message: str | None,
    ) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE file_audit
                SET
                    lifecycle_status = %s,
                    lifecycle_action = %s,
                    final_file_path = %s,
                    error_message = %s
                WHERE run_id = %s
                """,
                (lifecycle_status, lifecycle_action, final_file_path, error_message, run_id),
            )
            return cursor.rowcount

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def fetch_recent(self, limit: int = 10) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    source_file,
                    file_hash,
                    run_id,
                    trigger_type,
                    source_type,
                    processed_at,
                    status,
                    csv_files_found,
                    input_rows_read,
                    normalized_rows_emitted,
                    rows_inserted,
                    unknown_columns,
                    null_counter_values,
                    lifecycle_status,
                    lifecycle_action,
                    normalization_status,
                    normalized_at,
                    normalization_error,
                    final_file_path,
                    error_message
                FROM file_audit
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def fetch_recent_for_reconciliation(self, limit: int = 100) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    source_file,
                    run_id,
                    status,
                    rows_inserted,
                    lifecycle_status,
                    lifecycle_action,
                    normalization_status,
                    normalized_at,
                    normalization_error,
                    final_file_path,
                    error_message
                FROM file_audit
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def fetch_pending_lifecycle(self, limit: int = 100) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    source_file,
                    run_id,
                    status,
                    lifecycle_status,
                    lifecycle_action,
                    final_file_path,
                    error_message
                FROM file_audit
                WHERE lifecycle_status = 'PENDING'
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def mark_success_normalization_completed(self) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE file_audit
                SET
                    normalization_status = 'COMPLETED',
                    normalized_at = NOW(),
                    normalization_error = NULL
                WHERE status = 'SUCCESS'
                  AND normalization_status = 'PENDING'
                """,
                (),
            )
            return cursor.rowcount

    def mark_success_normalization_failed(self, error_message: str) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE file_audit
                SET
                    normalization_status = 'FAILED',
                    normalization_error = %s
                WHERE status = 'SUCCESS'
                  AND normalization_status = 'PENDING'
                """,
                (error_message,),
            )
            return cursor.rowcount
