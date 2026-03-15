from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from psycopg import Connection
from psycopg.rows import dict_row

from lte_pm_platform.pipeline.ingest.topology_reference_seed import (
    TopologyEntitySiteMapSeedRow,
    TopologyRegionSeedRow,
    TopologyReportingSeedRow,
    TopologySiteSeedRow,
)
from lte_pm_platform.pipeline.ingest.topology_workbook import ParsedTopologySnapshotRow


class TopologyReferenceRepository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def create_snapshot(
        self,
        *,
        source_file_name: str,
        stored_file_path: str,
        source_sha256: str,
        topology_release_date: date | None,
        parser_error_count: int,
        parser_warning_count: int,
        workbook_row_count: int,
        normalized_row_count: int,
        parser_messages: dict[str, object],
    ) -> int:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO topology_snapshot (
                    source_file_name,
                    stored_file_path,
                    source_sha256,
                    topology_release_date,
                    parser_error_count,
                    parser_warning_count,
                    workbook_row_count,
                    normalized_row_count,
                    parser_messages_json,
                    status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 'previewed')
                RETURNING snapshot_id
                """,
                (
                    source_file_name,
                    stored_file_path,
                    source_sha256,
                    topology_release_date,
                    parser_error_count,
                    parser_warning_count,
                    workbook_row_count,
                    normalized_row_count,
                    json.dumps(parser_messages),
                ),
            )
            snapshot_id = cursor.fetchone()[0]
        self.connection.commit()
        return snapshot_id

    def insert_snapshot_entity_rows(self, *, snapshot_id: int, rows: Sequence[ParsedTopologySnapshotRow]) -> int:
        if not rows:
            return 0
        payload = [
            (
                snapshot_id,
                row.source_row_number,
                row.logical_entity_key,
                row.dataset_family,
                row.site_code,
                row.site_name,
                row.region_code,
                row.region_name,
                row.area_name,
                row.cluster_id,
                row.team_code,
                row.reporting_key,
                row.reporting_name,
                row.reporting_level,
                row.workbook_subnet_id,
                row.workbook_enodeb_id,
                row.workbook_enodeb_name,
                row.workbook_cell_name,
                row.mapping_source,
                row.notes,
            )
            for row in rows
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO topology_snapshot_entity_map (
                    snapshot_id,
                    source_row_number,
                    logical_entity_key,
                    dataset_family,
                    site_code,
                    site_name,
                    region_code,
                    region_name,
                    area_name,
                    cluster_id,
                    team_code,
                    reporting_key,
                    reporting_name,
                    reporting_level,
                    workbook_subnet_id,
                    workbook_enodeb_id,
                    workbook_enodeb_name,
                    workbook_cell_name,
                    mapping_source,
                    notes
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                payload,
            )
        self.connection.commit()
        return len(payload)

    def list_snapshots(self) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    s.snapshot_id,
                    s.source_file_name,
                    s.topology_release_date,
                    s.uploaded_at,
                    s.status,
                    s.is_active_snapshot,
                    s.parser_error_count,
                    s.parser_warning_count,
                    s.workbook_row_count,
                    s.normalized_row_count,
                    COALESCE(r.blocking_error_count, 0) AS blocking_error_count,
                    COALESCE(r.warning_count, 0) AS warning_count,
                    r.reconciliation_id
                FROM topology_snapshot AS s
                LEFT JOIN LATERAL (
                    SELECT *
                    FROM topology_reconciliation_result
                    WHERE snapshot_id = s.snapshot_id
                    ORDER BY run_at DESC
                    LIMIT 1
                ) AS r ON TRUE
                ORDER BY s.uploaded_at DESC, s.snapshot_id DESC
                """
            )
            return list(cursor.fetchall())

    def get_snapshot_summary(self, snapshot_id: int) -> dict | None:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    s.snapshot_id,
                    s.source_file_name,
                    s.stored_file_path,
                    s.source_sha256,
                    s.topology_release_date,
                    s.uploaded_at,
                    s.status,
                    s.is_active_snapshot,
                    s.parser_error_count,
                    s.parser_warning_count,
                    s.workbook_row_count,
                    s.normalized_row_count,
                    s.parser_messages_json,
                    COALESCE(r.reconciliation_id, NULL) AS reconciliation_id,
                    COALESCE(r.blocking_error_count, 0) AS blocking_error_count,
                    COALESCE(r.warning_count, 0) AS warning_count,
                    COALESCE(r.pm_missing_from_workbook_count, 0) AS pm_missing_from_workbook_count,
                    COALESCE(r.workbook_missing_from_pm_count, 0) AS workbook_missing_from_pm_count,
                    COALESCE(r.workbook_sites_no_pm_count, 0) AS workbook_sites_no_pm_count,
                    COALESCE(r.duplicate_entity_mapping_count, 0) AS duplicate_entity_mapping_count,
                    COALESCE(r.conflicting_site_region_count, 0) AS conflicting_site_region_count,
                    COALESCE(r.entities_added_count, 0) AS entities_added_count,
                    COALESCE(r.entities_removed_count, 0) AS entities_removed_count,
                    COALESCE(r.entities_moved_site_count, 0) AS entities_moved_site_count,
                    COALESCE(r.sites_moved_region_count, 0) AS sites_moved_region_count
                FROM topology_snapshot AS s
                LEFT JOIN LATERAL (
                    SELECT *
                    FROM topology_reconciliation_result
                    WHERE snapshot_id = s.snapshot_id
                    ORDER BY run_at DESC
                    LIMIT 1
                ) AS r ON TRUE
                WHERE s.snapshot_id = %s
                """,
                (snapshot_id,),
            )
            return cursor.fetchone()

    def get_active_snapshot(self) -> dict | None:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    snapshot_id,
                    source_file_name,
                    topology_release_date,
                    uploaded_at,
                    status,
                    is_active_snapshot,
                    parser_error_count,
                    parser_warning_count,
                    workbook_row_count,
                    normalized_row_count
                FROM topology_snapshot
                WHERE is_active_snapshot = TRUE
                ORDER BY uploaded_at DESC
                LIMIT 1
                """
            )
            return cursor.fetchone()

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

    def run_snapshot_reconciliation(self, snapshot_id: int) -> dict:
        active_snapshot = self.get_active_snapshot()
        active_snapshot_id = active_snapshot["snapshot_id"] if active_snapshot else None

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO topology_reconciliation_result (
                    snapshot_id,
                    compared_active_snapshot_id,
                    parser_error_count,
                    parser_warning_count
                )
                SELECT
                    snapshot_id,
                    %s,
                    parser_error_count,
                    parser_warning_count
                FROM topology_snapshot
                WHERE snapshot_id = %s
                RETURNING reconciliation_id
                """,
                (active_snapshot_id, snapshot_id),
            )
            reconciliation_id = cursor.fetchone()[0]

            detail_queries = [
                (
                    "DUPLICATE_ENTITY_MULTIPLE_SITES",
                    "ERROR",
                    """
                    SELECT
                        logical_entity_key,
                        MIN(dataset_family) AS dataset_family,
                        jsonb_build_object(
                            'site_codes', ARRAY_AGG(DISTINCT site_code ORDER BY site_code)
                        ) AS detail_json
                    FROM topology_snapshot_entity_map
                    WHERE snapshot_id = %s
                    GROUP BY logical_entity_key
                    HAVING COUNT(DISTINCT site_code) > 1
                    """,
                ),
                (
                    "CONFLICTING_SITE_REGION",
                    "ERROR",
                    """
                    SELECT
                        NULL::text AS logical_entity_key,
                        NULL::text AS dataset_family,
                        jsonb_build_object(
                            'site_code', site_code,
                            'region_codes', ARRAY_AGG(DISTINCT region_code ORDER BY region_code)
                        ) AS detail_json
                    FROM topology_snapshot_entity_map
                    WHERE snapshot_id = %s
                    GROUP BY site_code
                    HAVING COUNT(DISTINCT region_code) > 1
                    """,
                ),
                (
                    "PM_MISSING_FROM_WORKBOOK",
                    "WARNING",
                    """
                    SELECT
                        e.logical_entity_key,
                        e.dataset_family,
                        jsonb_build_object('reason', 'pm_entity_missing_from_snapshot') AS detail_json
                    FROM ref_lte_entity_identity AS e
                    LEFT JOIN (
                        SELECT DISTINCT logical_entity_key
                        FROM topology_snapshot_entity_map
                        WHERE snapshot_id = %s
                    ) AS s
                        ON s.logical_entity_key = e.logical_entity_key
                    WHERE s.logical_entity_key IS NULL
                    """,
                ),
                (
                    "WORKBOOK_MISSING_FROM_PM",
                    "WARNING",
                    """
                    SELECT
                        s.logical_entity_key,
                        s.dataset_family,
                        jsonb_build_object('reason', 'snapshot_entity_missing_from_pm') AS detail_json
                    FROM (
                        SELECT DISTINCT logical_entity_key, dataset_family
                        FROM topology_snapshot_entity_map
                        WHERE snapshot_id = %s
                    ) AS s
                    LEFT JOIN ref_lte_entity_identity AS e
                        ON e.logical_entity_key = s.logical_entity_key
                    WHERE e.logical_entity_key IS NULL
                    """,
                ),
                (
                    "WORKBOOK_SITE_NO_PM_ACTIVITY",
                    "WARNING",
                    """
                    SELECT
                        NULL::text AS logical_entity_key,
                        NULL::text AS dataset_family,
                        jsonb_build_object('site_code', s.site_code) AS detail_json
                    FROM (
                        SELECT site_code
                        FROM topology_snapshot_entity_map
                        WHERE snapshot_id = %s
                        GROUP BY site_code
                    ) AS s
                    LEFT JOIN (
                        SELECT DISTINCT m.site_code
                        FROM topology_snapshot_entity_map AS m
                        JOIN ref_lte_entity_identity AS e
                            ON e.logical_entity_key = m.logical_entity_key
                        WHERE m.snapshot_id = %s
                    ) AS active_pm
                        ON active_pm.site_code = s.site_code
                    WHERE active_pm.site_code IS NULL
                    """,
                ),
            ]

            if active_snapshot_id is not None:
                detail_queries.extend(
                    [
                        (
                            "ENTITY_ADDED",
                            "WARNING",
                            """
                            SELECT
                                s.logical_entity_key,
                                s.dataset_family,
                                jsonb_build_object('candidate_site_code', s.site_code) AS detail_json
                            FROM (
                                SELECT DISTINCT logical_entity_key, dataset_family, site_code
                                FROM topology_snapshot_entity_map
                                WHERE snapshot_id = %s
                            ) AS s
                            LEFT JOIN (
                                SELECT DISTINCT logical_entity_key
                                FROM topology_snapshot_entity_map
                                WHERE snapshot_id = %s
                            ) AS a
                                ON a.logical_entity_key = s.logical_entity_key
                            WHERE a.logical_entity_key IS NULL
                            """,
                        ),
                        (
                            "ENTITY_REMOVED",
                            "WARNING",
                            """
                            SELECT
                                a.logical_entity_key,
                                a.dataset_family,
                                jsonb_build_object('active_site_code', a.site_code) AS detail_json
                            FROM (
                                SELECT DISTINCT logical_entity_key, dataset_family, site_code
                                FROM topology_snapshot_entity_map
                                WHERE snapshot_id = %s
                            ) AS a
                            LEFT JOIN (
                                SELECT DISTINCT logical_entity_key
                                FROM topology_snapshot_entity_map
                                WHERE snapshot_id = %s
                            ) AS s
                                ON s.logical_entity_key = a.logical_entity_key
                            WHERE s.logical_entity_key IS NULL
                            """,
                        ),
                        (
                            "ENTITY_MOVED_SITE",
                            "WARNING",
                            """
                            SELECT
                                s.logical_entity_key,
                                s.dataset_family,
                                jsonb_build_object(
                                    'active_site_code', a.site_code,
                                    'candidate_site_code', s.site_code
                                ) AS detail_json
                            FROM (
                                SELECT DISTINCT logical_entity_key, dataset_family, site_code
                                FROM topology_snapshot_entity_map
                                WHERE snapshot_id = %s
                            ) AS s
                            JOIN (
                                SELECT DISTINCT logical_entity_key, dataset_family, site_code
                                FROM topology_snapshot_entity_map
                                WHERE snapshot_id = %s
                            ) AS a
                                ON a.logical_entity_key = s.logical_entity_key
                            WHERE COALESCE(a.site_code, '') <> COALESCE(s.site_code, '')
                            """,
                        ),
                        (
                            "SITE_MOVED_REGION",
                            "WARNING",
                            """
                            SELECT
                                NULL::text AS logical_entity_key,
                                NULL::text AS dataset_family,
                                jsonb_build_object(
                                    'site_code', s.site_code,
                                    'active_region_code', a.region_code,
                                    'candidate_region_code', s.region_code
                                ) AS detail_json
                            FROM (
                                SELECT site_code, MIN(region_code) AS region_code
                                FROM topology_snapshot_entity_map
                                WHERE snapshot_id = %s
                                GROUP BY site_code
                            ) AS s
                            JOIN (
                                SELECT site_code, MIN(region_code) AS region_code
                                FROM topology_snapshot_entity_map
                                WHERE snapshot_id = %s
                                GROUP BY site_code
                            ) AS a
                                ON a.site_code = s.site_code
                            WHERE COALESCE(a.region_code, '') <> COALESCE(s.region_code, '')
                            """,
                        ),
                    ]
                )

            for issue_type, severity, detail_query in detail_queries:
                params = [snapshot_id]
                if issue_type == "WORKBOOK_SITE_NO_PM_ACTIVITY":
                    params = [snapshot_id, snapshot_id]
                if active_snapshot_id is not None and issue_type in {
                    "ENTITY_ADDED",
                    "ENTITY_REMOVED",
                    "ENTITY_MOVED_SITE",
                    "SITE_MOVED_REGION",
                }:
                    params = [snapshot_id, active_snapshot_id]
                cursor.execute(
                    f"""
                    INSERT INTO topology_reconciliation_detail (
                        reconciliation_id,
                        issue_type,
                        severity,
                        logical_entity_key,
                        dataset_family,
                        detail_json
                    )
                    SELECT
                        %s,
                        %s,
                        %s,
                        logical_entity_key,
                        dataset_family,
                        detail_json
                    FROM ({detail_query}) AS q
                    """,
                    (reconciliation_id, issue_type, severity, *params),
                )

            cursor.execute(
                """
                UPDATE topology_reconciliation_result AS r
                SET
                    blocking_error_count = summary.blocking_error_count,
                    warning_count = summary.warning_count,
                    pm_missing_from_workbook_count = summary.pm_missing_from_workbook_count,
                    workbook_missing_from_pm_count = summary.workbook_missing_from_pm_count,
                    workbook_sites_no_pm_count = summary.workbook_sites_no_pm_count,
                    duplicate_entity_mapping_count = summary.duplicate_entity_mapping_count,
                    conflicting_site_region_count = summary.conflicting_site_region_count,
                    entities_added_count = summary.entities_added_count,
                    entities_removed_count = summary.entities_removed_count,
                    entities_moved_site_count = summary.entities_moved_site_count,
                    sites_moved_region_count = summary.sites_moved_region_count,
                    updated_at = NOW()
                FROM (
                    SELECT
                        COUNT(*) FILTER (WHERE severity = 'ERROR') + r.parser_error_count AS blocking_error_count,
                        COUNT(*) FILTER (WHERE severity = 'WARNING') + r.parser_warning_count AS warning_count,
                        COUNT(*) FILTER (WHERE issue_type = 'PM_MISSING_FROM_WORKBOOK') AS pm_missing_from_workbook_count,
                        COUNT(*) FILTER (WHERE issue_type = 'WORKBOOK_MISSING_FROM_PM') AS workbook_missing_from_pm_count,
                        COUNT(*) FILTER (WHERE issue_type = 'WORKBOOK_SITE_NO_PM_ACTIVITY') AS workbook_sites_no_pm_count,
                        COUNT(*) FILTER (WHERE issue_type = 'DUPLICATE_ENTITY_MULTIPLE_SITES') AS duplicate_entity_mapping_count,
                        COUNT(*) FILTER (WHERE issue_type = 'CONFLICTING_SITE_REGION') AS conflicting_site_region_count,
                        COUNT(*) FILTER (WHERE issue_type = 'ENTITY_ADDED') AS entities_added_count,
                        COUNT(*) FILTER (WHERE issue_type = 'ENTITY_REMOVED') AS entities_removed_count,
                        COUNT(*) FILTER (WHERE issue_type = 'ENTITY_MOVED_SITE') AS entities_moved_site_count,
                        COUNT(*) FILTER (WHERE issue_type = 'SITE_MOVED_REGION') AS sites_moved_region_count
                    FROM topology_reconciliation_detail AS d
                    JOIN topology_reconciliation_result AS r
                        ON r.reconciliation_id = d.reconciliation_id
                    WHERE d.reconciliation_id = %s
                    GROUP BY r.parser_error_count, r.parser_warning_count
                ) AS summary
                WHERE r.reconciliation_id = %s
                """,
                (reconciliation_id, reconciliation_id),
            )

            cursor.execute(
                "UPDATE topology_snapshot SET status = 'reconciled', updated_at = NOW() WHERE snapshot_id = %s",
                (snapshot_id,),
            )
        self.connection.commit()
        return self.get_snapshot_summary(snapshot_id) or {}

    def list_reconciliation_details(
        self,
        *,
        reconciliation_id: int,
        issue_type: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        query = """
            SELECT
                reconciliation_detail_id,
                issue_type,
                severity,
                logical_entity_key,
                dataset_family,
                site_code,
                region_code,
                active_site_code,
                active_region_code,
                candidate_site_code,
                candidate_region_code,
                detail_json
            FROM topology_reconciliation_detail
            WHERE reconciliation_id = %s
        """
        params: list[object] = [reconciliation_id]
        if issue_type is not None:
            query += "\n  AND issue_type = %s"
            params.append(issue_type)
        query += "\nORDER BY issue_type, logical_entity_key NULLS LAST\nLIMIT %s"
        params.append(limit)
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())

    def apply_snapshot(self, *, snapshot_id: int, activated_by: str | None = None) -> dict[str, int | None]:
        summary = self.get_snapshot_summary(snapshot_id)
        if summary is None:
            raise ValueError(f"snapshot not found: {snapshot_id}")
        if summary["parser_error_count"] > 0:
            raise ValueError("cannot apply snapshot with parser errors")
        if summary["blocking_error_count"] > 0:
            raise ValueError("cannot apply snapshot with blocking reconciliation issues")

        active_snapshot = self.get_active_snapshot()
        prior_active_snapshot_id = active_snapshot["snapshot_id"] if active_snapshot else None

        with self.connection.cursor() as cursor:
            cursor.execute("DELETE FROM ref_topology_entity_site_map")
            cursor.execute("DELETE FROM ref_topology_reporting_hierarchy")
            cursor.execute("DELETE FROM ref_topology_site")
            cursor.execute("DELETE FROM ref_topology_region")

            cursor.execute(
                """
                INSERT INTO ref_topology_region (region_code, region_name, notes)
                SELECT DISTINCT region_code, COALESCE(region_name, region_code), source_file_name
                FROM topology_snapshot_entity_map AS m
                JOIN topology_snapshot AS s
                    ON s.snapshot_id = m.snapshot_id
                WHERE m.snapshot_id = %s
                  AND region_code IS NOT NULL
                """,
                (snapshot_id,),
            )
            regions_loaded = cursor.rowcount

            cursor.execute(
                """
                INSERT INTO ref_topology_site (site_code, site_name, region_code, notes)
                SELECT DISTINCT site_code, site_name, region_code, source_file_name
                FROM topology_snapshot_entity_map AS m
                JOIN topology_snapshot AS s
                    ON s.snapshot_id = m.snapshot_id
                WHERE m.snapshot_id = %s
                  AND site_code IS NOT NULL
                """,
                (snapshot_id,),
            )
            sites_loaded = cursor.rowcount

            cursor.execute(
                """
                INSERT INTO ref_topology_reporting_hierarchy (
                    reporting_key,
                    reporting_name,
                    reporting_level,
                    parent_reporting_key,
                    notes
                )
                SELECT DISTINCT
                    reporting_key,
                    COALESCE(reporting_name, reporting_key),
                    reporting_level,
                    NULL,
                    source_file_name
                FROM topology_snapshot_entity_map AS m
                JOIN topology_snapshot AS s
                    ON s.snapshot_id = m.snapshot_id
                WHERE m.snapshot_id = %s
                  AND reporting_key IS NOT NULL
                """,
                (snapshot_id,),
            )
            reporting_loaded = cursor.rowcount

            cursor.execute(
                """
                INSERT INTO ref_topology_entity_site_map (
                    logical_entity_key,
                    site_code,
                    reporting_key,
                    mapping_source,
                    notes
                )
                SELECT DISTINCT ON (logical_entity_key)
                    m.logical_entity_key,
                    m.site_code,
                    m.reporting_key,
                    m.mapping_source,
                    m.notes
                FROM topology_snapshot_entity_map AS m
                JOIN ref_lte_entity_identity AS e
                    ON e.logical_entity_key = m.logical_entity_key
                WHERE m.snapshot_id = %s
                ORDER BY m.logical_entity_key, m.source_row_number
                """,
                (snapshot_id,),
            )
            entity_map_loaded = cursor.rowcount

            cursor.execute(
                "UPDATE topology_snapshot SET is_active_snapshot = FALSE, updated_at = NOW() WHERE is_active_snapshot = TRUE"
            )
            cursor.execute(
                "UPDATE topology_snapshot SET is_active_snapshot = TRUE, status = 'applied', updated_at = NOW() WHERE snapshot_id = %s",
                (snapshot_id,),
            )
            cursor.execute(
                """
                INSERT INTO topology_activation_audit (
                    snapshot_id,
                    prior_active_snapshot_id,
                    rows_loaded_regions,
                    rows_loaded_sites,
                    rows_loaded_reporting,
                    rows_loaded_entity_map,
                    activated_by
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    snapshot_id,
                    prior_active_snapshot_id,
                    regions_loaded,
                    sites_loaded,
                    reporting_loaded,
                    entity_map_loaded,
                    activated_by,
                ),
            )
        self.connection.commit()
        return {
            "rows_loaded_regions": regions_loaded,
            "rows_loaded_sites": sites_loaded,
            "rows_loaded_reporting": reporting_loaded,
            "rows_loaded_entity_map": entity_map_loaded,
            "prior_active_snapshot_id": prior_active_snapshot_id,
        }
