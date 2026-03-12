from __future__ import annotations

from collections.abc import Sequence

from psycopg import Connection
from psycopg.rows import dict_row

from lte_pm_platform.pipeline.ingest.counter_reference_seed import CounterReferenceSeedRow


class CounterReferenceRepository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def fetch_all(self, limit: int = 100) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    counter_id,
                    vendor,
                    technology,
                    object_type,
                    description,
                    unit,
                    source_type,
                    source_reference,
                    verification_status,
                    verified_at,
                    notes
                FROM ref_pm_counter
                ORDER BY counter_id
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def fetch_by_id(self, counter_id: str) -> dict | None:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    counter_id,
                    vendor,
                    technology,
                    object_type,
                    description,
                    unit,
                    source_type,
                    source_reference,
                    verification_status,
                    verified_at,
                    notes
                FROM ref_pm_counter
                WHERE counter_id = %s
                """,
                (counter_id,),
            )
            return cursor.fetchone()

    def upsert_many(self, rows: Sequence[CounterReferenceSeedRow]) -> int:
        if not rows:
            return 0
        payload = [
            (
                row.counter_id,
                row.vendor,
                row.technology,
                row.object_type,
                row.description,
                row.unit,
                row.notes,
                row.source_type,
                row.source_reference,
                row.verification_status,
                row.verified_at,
            )
            for row in rows
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO ref_pm_counter (
                    counter_id,
                    vendor,
                    technology,
                    object_type,
                    description,
                    unit,
                    notes,
                    source_type,
                    source_reference,
                    verification_status,
                    verified_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (counter_id) DO UPDATE SET
                    vendor = EXCLUDED.vendor,
                    technology = EXCLUDED.technology,
                    object_type = EXCLUDED.object_type,
                    description = EXCLUDED.description,
                    unit = EXCLUDED.unit,
                    notes = EXCLUDED.notes,
                    source_type = EXCLUDED.source_type,
                    source_reference = EXCLUDED.source_reference,
                    verification_status = EXCLUDED.verification_status,
                    verified_at = EXCLUDED.verified_at,
                    updated_at = NOW()
                """,
                payload,
            )
        self.connection.commit()
        return len(payload)
