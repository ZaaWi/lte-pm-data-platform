from __future__ import annotations

from psycopg import Connection
from psycopg.rows import dict_row


class EntityReferenceRepository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def fetch_all(self, limit: int = 100) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    logical_entity_key,
                    dataset_family,
                    sbnid,
                    enbid,
                    enodebid,
                    cellid,
                    meid,
                    ani,
                    entity_level,
                    notes
                FROM ref_lte_entity_identity
                ORDER BY dataset_family, logical_entity_key
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_entities(self, limit: int = 100) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    dataset_family,
                    entity_level,
                    distinct_logical_entity_keys,
                    distinct_sbnid,
                    distinct_enbid,
                    distinct_enodebid,
                    distinct_cellid,
                    distinct_meid
                FROM vw_pm_entity_summary_by_family
                ORDER BY distinct_logical_entity_keys DESC, dataset_family
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def show_entity_shape(self, limit: int = 100) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    logical_entity_key,
                    dataset_family,
                    entity_level,
                    sbnid,
                    enbid,
                    enodebid,
                    cellid,
                    meid,
                    ani
                FROM vw_pm_distinct_entities
                ORDER BY dataset_family, logical_entity_key
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def refresh_from_raw_entities(self) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ref_lte_entity_identity (
                    logical_entity_key,
                    dataset_family,
                    sbnid,
                    enbid,
                    enodebid,
                    cellid,
                    meid,
                    ani,
                    entity_level
                )
                SELECT
                    logical_entity_key,
                    dataset_family,
                    sbnid,
                    enbid,
                    enodebid,
                    cellid,
                    meid,
                    ani,
                    entity_level
                FROM vw_pm_distinct_entities
                ON CONFLICT (logical_entity_key) DO UPDATE
                SET
                    dataset_family = EXCLUDED.dataset_family,
                    sbnid = EXCLUDED.sbnid,
                    enbid = EXCLUDED.enbid,
                    enodebid = EXCLUDED.enodebid,
                    cellid = EXCLUDED.cellid,
                    meid = EXCLUDED.meid,
                    ani = EXCLUDED.ani,
                    entity_level = EXCLUDED.entity_level,
                    updated_at = NOW()
                """
            )
            rowcount = cursor.rowcount
        return rowcount
