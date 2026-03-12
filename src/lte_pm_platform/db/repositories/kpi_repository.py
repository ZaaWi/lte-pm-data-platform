from __future__ import annotations

from psycopg import Connection
from psycopg.rows import dict_row


class KpiRepository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def list_definitions(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    kpi_name,
                    technology,
                    description,
                    formula_type,
                    scale_factor,
                    formula_notes,
                    unit,
                    aggregation_grain_notes,
                    status,
                    pending_reason,
                    numerator_counter_ids,
                    denominator_counter_ids,
                    unverified_counter_ids,
                    all_mapped_counters_verified
                FROM vw_kpi_definition_details
                ORDER BY kpi_name
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def get_definition(self, kpi_name: str) -> dict | None:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    kpi_name,
                    technology,
                    description,
                    formula_type,
                    scale_factor,
                    formula_notes,
                    unit,
                    aggregation_grain_notes,
                    status,
                    pending_reason,
                    numerator_counter_ids,
                    denominator_counter_ids,
                    unverified_counter_ids,
                    all_mapped_counters_verified
                FROM vw_kpi_definition_details
                WHERE kpi_name = %s
                """,
                (kpi_name,),
            )
            return cursor.fetchone()

    def summarize_kpi(self, kpi_name: str, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    kpi_name,
                    technology,
                    description,
                    unit,
                    aggregation_grain_notes,
                    formula_notes,
                    collect_time,
                    ani,
                    logical_entity_key,
                    site_code,
                    region_name,
                    numerator_value,
                    denominator_value,
                    kpi_value
                FROM vw_kpi_ratio_values_by_time_ani
                WHERE kpi_name = %s
                ORDER BY collect_time DESC, ani NULLS LAST
                LIMIT %s
                """,
                (kpi_name, limit),
            )
            return list(cursor.fetchall())
