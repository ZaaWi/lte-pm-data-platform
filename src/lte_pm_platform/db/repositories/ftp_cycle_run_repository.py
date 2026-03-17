from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

from psycopg import Connection
from psycopg.rows import dict_row


class FtpCycleRunRepository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def create_run(
        self,
        *,
        trigger_source: str,
        parameters: dict[str, Any],
        summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                INSERT INTO ftp_cycle_run (
                    trigger_source,
                    parameters_json,
                    summary_json,
                    status
                )
                VALUES (%s, %s::jsonb, %s::jsonb, 'queued')
                RETURNING
                    id,
                    requested_at,
                    started_at,
                    finished_at,
                    status,
                    trigger_source,
                    parameters_json,
                    summary_json,
                    error_message
                """,
                (
                    trigger_source,
                    json.dumps(parameters),
                    json.dumps(summary or {}),
                ),
            )
            row = cursor.fetchone()
        self.connection.commit()
        return row

    def claim_next_queued_run(self) -> dict[str, Any] | None:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                WITH next_run AS (
                    SELECT id
                    FROM ftp_cycle_run
                    WHERE status = 'queued'
                    ORDER BY requested_at ASC, id ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE ftp_cycle_run AS run
                SET
                    status = 'running',
                    started_at = NOW(),
                    updated_at = NOW()
                FROM next_run
                WHERE run.id = next_run.id
                RETURNING
                    run.id,
                    run.requested_at,
                    run.started_at,
                    run.finished_at,
                    run.status,
                    run.trigger_source,
                    run.parameters_json,
                    run.summary_json,
                    run.error_message
                """
            )
            row = cursor.fetchone()
        self.connection.commit()
        return row

    def recover_stale_running_runs(self, *, error_message: str) -> list[dict[str, Any]]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                UPDATE ftp_cycle_run
                SET
                    status = 'failed',
                    finished_at = NOW(),
                    error_message = %s,
                    updated_at = NOW()
                WHERE status = 'running'
                RETURNING
                    id,
                    requested_at,
                    started_at,
                    finished_at,
                    status,
                    trigger_source,
                    parameters_json,
                    summary_json,
                    error_message
                """,
                (error_message,),
            )
            rows = list(cursor.fetchall())
            if rows:
                cursor.executemany(
                    """
                    INSERT INTO ftp_cycle_run_event (
                        run_id,
                        stage,
                        level,
                        message,
                        metrics_json
                    )
                    VALUES (%s, 'finalize', 'error', %s, '{}'::jsonb)
                    """,
                    [(row["id"], error_message) for row in rows],
                )
        self.connection.commit()
        return rows

    def update_summary(self, *, run_id: int, summary: dict[str, Any]) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE ftp_cycle_run
                SET
                    summary_json = %s::jsonb,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (json.dumps(summary), run_id),
            )
        self.connection.commit()

    def append_event(
        self,
        *,
        run_id: int,
        stage: str,
        level: str,
        message: str,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ftp_cycle_run_event (
                    run_id,
                    stage,
                    level,
                    message,
                    metrics_json
                )
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                (
                    run_id,
                    stage,
                    level,
                    message,
                    json.dumps(metrics or {}),
                ),
            )
        self.connection.commit()

    def mark_succeeded(self, *, run_id: int, summary: dict[str, Any]) -> dict[str, Any] | None:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                UPDATE ftp_cycle_run
                SET
                    status = 'succeeded',
                    finished_at = NOW(),
                    summary_json = %s::jsonb,
                    error_message = NULL,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING
                    id,
                    requested_at,
                    started_at,
                    finished_at,
                    status,
                    trigger_source,
                    parameters_json,
                    summary_json,
                    error_message
                """,
                (json.dumps(summary), run_id),
            )
            row = cursor.fetchone()
        self.connection.commit()
        return row

    def mark_failed(
        self,
        *,
        run_id: int,
        error_message: str,
        summary: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                UPDATE ftp_cycle_run
                SET
                    status = 'failed',
                    finished_at = NOW(),
                    summary_json = COALESCE(%s::jsonb, summary_json),
                    error_message = %s,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING
                    id,
                    requested_at,
                    started_at,
                    finished_at,
                    status,
                    trigger_source,
                    parameters_json,
                    summary_json,
                    error_message
                """,
                (json.dumps(summary) if summary is not None else None, error_message, run_id),
            )
            row = cursor.fetchone()
        self.connection.commit()
        return row

    def list_runs(
        self,
        *,
        limit: int = 20,
        statuses: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT
                id,
                requested_at,
                started_at,
                finished_at,
                status,
                trigger_source,
                parameters_json,
                summary_json,
                error_message
            FROM ftp_cycle_run
            WHERE TRUE
        """
        params: list[object] = []
        if statuses:
            query += " AND status = ANY(%s)"
            params.append(list(statuses))
        query += " ORDER BY requested_at DESC, id DESC LIMIT %s"
        params.append(limit)
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())

    def get_run(self, *, run_id: int) -> dict[str, Any] | None:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    requested_at,
                    started_at,
                    finished_at,
                    status,
                    trigger_source,
                    parameters_json,
                    summary_json,
                    error_message
                FROM ftp_cycle_run
                WHERE id = %s
                """,
                (run_id,),
            )
            return cursor.fetchone()

    def list_events(self, *, run_id: int, limit: int = 200) -> list[dict[str, Any]]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    run_id,
                    event_time,
                    stage,
                    level,
                    message,
                    metrics_json
                FROM ftp_cycle_run_event
                WHERE run_id = %s
                ORDER BY event_time DESC, id DESC
                LIMIT %s
                """,
                (run_id, limit),
            )
            return list(cursor.fetchall())
