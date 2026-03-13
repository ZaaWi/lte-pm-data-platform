from __future__ import annotations

from collections.abc import Sequence

from psycopg import Connection
from psycopg.rows import dict_row

from lte_pm_platform.pipeline.ingest.topology_reference_seed import (
    TopologyEntitySiteMapSeedRow,
    TopologyRegionSeedRow,
    TopologyReportingSeedRow,
    TopologySiteSeedRow,
)


class TopologyReferenceRepository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def upsert_regions(self, rows: Sequence[TopologyRegionSeedRow]) -> int:
        if not rows:
            return 0
        payload = [(row.region_code, row.region_name, row.notes) for row in rows]
        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO ref_topology_region (
                    region_code,
                    region_name,
                    notes
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (region_code) DO UPDATE
                SET
                    region_name = EXCLUDED.region_name,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
                """,
                payload,
            )
        self.connection.commit()
        return len(payload)

    def upsert_sites(self, rows: Sequence[TopologySiteSeedRow]) -> int:
        if not rows:
            return 0
        payload = [(row.site_code, row.site_name, row.region_code, row.notes) for row in rows]
        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO ref_topology_site (
                    site_code,
                    site_name,
                    region_code,
                    notes
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (site_code) DO UPDATE
                SET
                    site_name = EXCLUDED.site_name,
                    region_code = EXCLUDED.region_code,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
                """,
                payload,
            )
        self.connection.commit()
        return len(payload)

    def upsert_reporting_hierarchy(self, rows: Sequence[TopologyReportingSeedRow]) -> int:
        if not rows:
            return 0
        payload = [
            (
                row.reporting_key,
                row.reporting_name,
                row.reporting_level,
                row.parent_reporting_key,
                row.notes,
            )
            for row in rows
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO ref_topology_reporting_hierarchy (
                    reporting_key,
                    reporting_name,
                    reporting_level,
                    parent_reporting_key,
                    notes
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (reporting_key) DO UPDATE
                SET
                    reporting_name = EXCLUDED.reporting_name,
                    reporting_level = EXCLUDED.reporting_level,
                    parent_reporting_key = EXCLUDED.parent_reporting_key,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
                """,
                payload,
            )
        self.connection.commit()
        return len(payload)

    def upsert_entity_site_mappings(self, rows: Sequence[TopologyEntitySiteMapSeedRow]) -> int:
        if not rows:
            return 0
        payload = [
            (
                row.logical_entity_key,
                row.site_code,
                row.reporting_key,
                row.mapping_source,
                row.notes,
            )
            for row in rows
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO ref_topology_entity_site_map (
                    logical_entity_key,
                    site_code,
                    reporting_key,
                    mapping_source,
                    notes
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (logical_entity_key) DO UPDATE
                SET
                    site_code = EXCLUDED.site_code,
                    reporting_key = EXCLUDED.reporting_key,
                    mapping_source = EXCLUDED.mapping_source,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
                """,
                payload,
            )
        self.connection.commit()
        return len(payload)

    def refresh_topology_enrichment(self) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO ref_lte_entity_topology_enrichment (
                    logical_entity_key,
                    dataset_family,
                    entity_level,
                    site_code,
                    site_name,
                    region_code,
                    region_name,
                    reporting_key,
                    reporting_name,
                    reporting_level,
                    mapping_status
                )
                SELECT
                    e.logical_entity_key,
                    e.dataset_family,
                    e.entity_level,
                    m.site_code,
                    s.site_name,
                    s.region_code,
                    r.region_name,
                    m.reporting_key,
                    h.reporting_name,
                    h.reporting_level,
                    CASE
                        WHEN m.site_code IS NULL THEN 'UNMAPPED'
                        ELSE 'MAPPED'
                    END AS mapping_status
                FROM ref_lte_entity_identity AS e
                LEFT JOIN ref_topology_entity_site_map AS m
                    ON m.logical_entity_key = e.logical_entity_key
                LEFT JOIN ref_topology_site AS s
                    ON s.site_code = m.site_code
                LEFT JOIN ref_topology_region AS r
                    ON r.region_code = s.region_code
                LEFT JOIN ref_topology_reporting_hierarchy AS h
                    ON h.reporting_key = m.reporting_key
                ON CONFLICT (logical_entity_key) DO UPDATE
                SET
                    dataset_family = EXCLUDED.dataset_family,
                    entity_level = EXCLUDED.entity_level,
                    site_code = EXCLUDED.site_code,
                    site_name = EXCLUDED.site_name,
                    region_code = EXCLUDED.region_code,
                    region_name = EXCLUDED.region_name,
                    reporting_key = EXCLUDED.reporting_key,
                    reporting_name = EXCLUDED.reporting_name,
                    reporting_level = EXCLUDED.reporting_level,
                    mapping_status = EXCLUDED.mapping_status,
                    updated_at = NOW()
                """
            )
            rowcount = cursor.rowcount
        return rowcount

    def list_unmapped_entities(self, limit: int = 100) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    logical_entity_key,
                    dataset_family,
                    entity_level
                FROM ref_lte_entity_topology_enrichment
                WHERE mapping_status = 'UNMAPPED'
                ORDER BY dataset_family, logical_entity_key
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_site_coverage(self, limit: int = 100) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    site_code,
                    site_name,
                    region_code,
                    region_name,
                    dataset_family,
                    distinct_logical_entities,
                    distinct_collect_times,
                    row_count
                FROM vw_pm_site_coverage
                ORDER BY row_count DESC, site_code
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_region_coverage(self, limit: int = 100) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    region_code,
                    region_name,
                    dataset_family,
                    distinct_logical_entities,
                    distinct_collect_times,
                    row_count
                FROM vw_pm_region_coverage
                ORDER BY row_count DESC, region_code
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())
