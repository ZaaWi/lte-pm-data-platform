from __future__ import annotations

from collections.abc import Iterable, Sequence

from psycopg import Connection
from psycopg.rows import dict_row

from lte_pm_platform.domain.models import NormalizedPmRecord


class PmSampleRepository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def insert_batch(self, records: Sequence[NormalizedPmRecord]) -> int:
        if not records:
            return 0
        rows = [
            (
                record.source_file,
                record.dataset_family,
                record.interval_start,
                record.revision,
                record.csv_name,
                record.collect_time,
                record.trncmeid,
                record.ani,
                record.sbnid,
                record.enbid,
                record.enodebid,
                record.cellid,
                record.meid,
                record.systemmode,
                record.midflag,
                record.netype,
                record.counter_id,
                record.counter_value,
            )
            for record in records
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO pm_ltefdd_sample (
                    source_file,
                    dataset_family,
                    interval_start,
                    revision,
                    csv_name,
                    collect_time,
                    trncmeid,
                    ani,
                    sbnid,
                    enbid,
                    enodebid,
                    cellid,
                    meid,
                    systemmode,
                    midflag,
                    netype,
                    counter_id,
                    counter_value
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                rows,
            )
        return len(rows)

    def fetch_recent(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    source_file,
                    dataset_family,
                    revision,
                    csv_name,
                    collect_time,
                    ani,
                    sbnid,
                    enbid,
                    enodebid,
                    cellid,
                    meid,
                    counter_id,
                    counter_value
                FROM pm_ltefdd_sample
                ORDER BY id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_by_source_file(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    source_file,
                    dataset_family,
                    interval_start,
                    revision,
                    row_count,
                    non_null_counter_value_count
                FROM vw_pm_counts_by_source_file
                ORDER BY row_count DESC, source_file
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def list_seen_counters(self, limit: int) -> list[dict]:
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
                    row_count,
                    non_null_counter_value_count
                FROM vw_pm_counts_by_counter_id
                ORDER BY row_count DESC, counter_id
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def top_counters(self, limit: int) -> list[dict]:
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
                    row_count,
                    non_null_counter_value_count
                FROM vw_pm_counts_by_counter_id
                ORDER BY row_count DESC, counter_id
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_by_collect_time(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    collect_time,
                    dataset_family,
                    row_count,
                    non_null_counter_value_count
                FROM vw_pm_counts_by_collect_time
                ORDER BY collect_time DESC
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_by_ani(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    ani,
                    dataset_family,
                    sbnid,
                    enbid,
                    enodebid,
                    cellid,
                    meid,
                    logical_entity_key,
                    site_code,
                    region_name,
                    row_count,
                    non_null_counter_value_count
                FROM vw_pm_counts_by_ani
                ORDER BY row_count DESC, ani NULLS LAST
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_by_dataset_family(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    COALESCE(dataset_family, 'UNKNOWN') AS dataset_family,
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT source_file) AS distinct_source_files,
                    COUNT(DISTINCT counter_id) AS distinct_counter_ids,
                    COUNT(DISTINCT ani) AS distinct_ani,
                    MIN(collect_time) AS min_collect_time,
                    MAX(collect_time) AS max_collect_time
                FROM pm_ltefdd_sample
                GROUP BY dataset_family
                ORDER BY row_count DESC, dataset_family
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_entity_fields(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    COALESCE(dataset_family, 'UNKNOWN') AS dataset_family,
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT sbnid) AS distinct_sbnid,
                    COUNT(DISTINCT enbid) AS distinct_enbid,
                    COUNT(DISTINCT enodebid) AS distinct_enodebid,
                    COUNT(DISTINCT cellid) AS distinct_cellid,
                    COUNT(DISTINCT meid) AS distinct_meid
                FROM pm_ltefdd_sample
                GROUP BY COALESCE(dataset_family, 'UNKNOWN')
                ORDER BY row_count DESC, dataset_family
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def count_distinct_cells(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    COALESCE(dataset_family, 'UNKNOWN') AS dataset_family,
                    COUNT(DISTINCT (sbnid, enbid, enodebid, cellid, meid)) AS distinct_cell_keys
                FROM pm_ltefdd_sample
                GROUP BY COALESCE(dataset_family, 'UNKNOWN')
                ORDER BY distinct_cell_keys DESC, dataset_family
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_counter_aggregates(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    collect_time,
                    dataset_family,
                    counter_id,
                    vendor,
                    technology,
                    object_type,
                    description,
                    unit,
                    row_count,
                    non_null_counter_value_count,
                    sum_counter_value,
                    avg_counter_value,
                    min_counter_value,
                    max_counter_value
                FROM vw_pm_counter_agg_by_time_counter
                ORDER BY collect_time DESC, counter_id
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_ani_counter_aggregates(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    collect_time,
                    ani,
                    dataset_family,
                    sbnid,
                    enbid,
                    enodebid,
                    cellid,
                    meid,
                    logical_entity_key,
                    site_code,
                    region_name,
                    counter_id,
                    vendor,
                    technology,
                    object_type,
                    description,
                    unit,
                    row_count,
                    non_null_counter_value_count,
                    sum_counter_value,
                    avg_counter_value,
                    min_counter_value,
                    max_counter_value
                FROM vw_pm_counter_agg_by_time_ani_counter
                ORDER BY collect_time DESC, ani NULLS LAST, counter_id
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_entity_counters(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    dataset_family,
                    entity_level,
                    counter_id,
                    COUNT(DISTINCT logical_entity_key) AS distinct_entities,
                    COUNT(DISTINCT collect_time) AS distinct_collect_times,
                    SUM(row_count) AS row_count,
                    SUM(non_null_counter_value_count) AS non_null_counter_value_count
                FROM vw_pm_counter_agg_by_entity_time_counter
                GROUP BY dataset_family, entity_level, counter_id
                ORDER BY row_count DESC, dataset_family, counter_id
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_entity_intervals(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    dataset_family,
                    entity_level,
                    logical_entity_key,
                    distinct_counter_ids,
                    distinct_collect_times,
                    min_collect_time,
                    max_collect_time
                FROM vw_pm_entity_counter_coverage
                ORDER BY distinct_collect_times DESC, distinct_counter_ids DESC, logical_entity_key
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_interval_topology_coverage(
        self,
        *,
        source_files: Sequence[str],
    ) -> list[dict]:
        if not source_files:
            return []
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                WITH raw_keys AS (
                    SELECT
                        interval_start,
                        CONCAT(
                            'family=PM/itbbu/ltefdd',
                            '|sbnid=', COALESCE(sbnid, ''),
                            '|enbid=', COALESCE(enbid, ''),
                            '|cellid=', COALESCE(cellid, '')
                        ) AS logical_entity_key
                    FROM (
                        SELECT DISTINCT
                            interval_start,
                            sbnid,
                            enbid,
                            cellid
                        FROM pm_ltefdd_sample
                        WHERE source_file = ANY(%s)
                          AND dataset_family = 'PM/itbbu/ltefdd'
                    ) AS itbbu_entities
                    UNION ALL
                    SELECT
                        interval_start,
                        CONCAT(
                            'family=PM/sdr/ltefdd',
                            '|sbnid=', COALESCE(sbnid, ''),
                            '|enodebid=', COALESCE(enodebid, ''),
                            '|cellid=', COALESCE(cellid, '')
                        ) AS logical_entity_key
                    FROM (
                        SELECT DISTINCT
                            interval_start,
                            sbnid,
                            enodebid,
                            cellid
                        FROM pm_ltefdd_sample
                        WHERE source_file = ANY(%s)
                          AND dataset_family = 'PM/sdr/ltefdd'
                    ) AS sdr_entities
                )
                SELECT
                    r.interval_start,
                    COUNT(*) FILTER (
                        WHERE t.mapping_status IS NOT NULL
                          AND t.mapping_status <> 'UNMAPPED'
                    ) AS topology_mapped_count,
                    COUNT(*) FILTER (
                        WHERE t.mapping_status IS NULL
                           OR t.mapping_status = 'UNMAPPED'
                    ) AS topology_unmapped_count
                FROM raw_keys AS r
                LEFT JOIN ref_lte_entity_topology_enrichment AS t
                    ON t.logical_entity_key = r.logical_entity_key
                WHERE r.logical_entity_key IS NOT NULL
                GROUP BY r.interval_start
                ORDER BY r.interval_start DESC
                """,
                (list(source_files), list(source_files)),
            )
            return list(cursor.fetchall())

    def summarize_coverage(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    dataset_family,
                    interval_start,
                    min_collect_time,
                    max_collect_time,
                    distinct_source_files,
                    distinct_revisions,
                    distinct_logical_entity_keys,
                    distinct_cell_entities,
                    distinct_counter_ids,
                    row_count
                FROM vw_pm_family_interval_coverage
                ORDER BY interval_start DESC, dataset_family
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def summarize_logical_entity_counts(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    dataset_family,
                    entity_level,
                    collect_time,
                    interval_start,
                    distinct_logical_entity_keys
                FROM vw_pm_logical_entity_counts_by_time
                ORDER BY collect_time DESC, dataset_family, entity_level
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def compare_expected_cells(self, expected: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                WITH latest_family_counts AS (
                    SELECT DISTINCT ON (dataset_family)
                        dataset_family,
                        interval_start,
                        collect_time,
                        distinct_cell_entities
                    FROM vw_pm_cell_entity_counts_by_interval
                    ORDER BY dataset_family, interval_start DESC, collect_time DESC
                ),
                combined AS (
                    SELECT
                        SUM(distinct_cell_entities) AS combined_observed_cells
                    FROM latest_family_counts
                    WHERE dataset_family IN ('PM/itbbu/ltefdd', 'PM/sdr/ltefdd')
                )
                SELECT
                    l.dataset_family,
                    l.interval_start,
                    l.collect_time,
                    l.distinct_cell_entities AS observed_cells,
                    %s AS expected_cells,
                    (%s - l.distinct_cell_entities) AS cell_gap,
                    ROUND(
                        (l.distinct_cell_entities::numeric / NULLIF(%s, 0)) * 100,
                        2
                    ) AS coverage_pct,
                    c.combined_observed_cells,
                    (%s - c.combined_observed_cells) AS combined_cell_gap,
                    ROUND(
                        (c.combined_observed_cells::numeric / NULLIF(%s, 0)) * 100,
                        2
                    ) AS combined_coverage_pct
                FROM latest_family_counts AS l
                CROSS JOIN combined AS c
                ORDER BY l.dataset_family
                """,
                (expected, expected, expected, expected, expected),
            )
            return list(cursor.fetchall())

    def summarize_coverage_timeline(self, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    dataset_family,
                    collect_time,
                    interval_start,
                    distinct_cell_entities
                FROM vw_pm_cell_entity_counts_timeline
                ORDER BY collect_time DESC, dataset_family
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def compare_expected_cells_timeline(self, expected: int, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    collect_time,
                    interval_start,
                    combined_observed_cells AS observed_cells,
                    %s AS expected_cells,
                    (%s - combined_observed_cells) AS cell_gap,
                    ROUND(
                        (combined_observed_cells::numeric / NULLIF(%s, 0)) * 100,
                        2
                    ) AS coverage_pct
                FROM vw_pm_combined_ltefdd_cell_coverage_timeline
                ORDER BY collect_time DESC
                LIMIT %s
                """,
                (expected, expected, expected, limit),
            )
            return list(cursor.fetchall())

    def count_rows_by_source_files(self, source_files: Iterable[str]) -> dict[str, int]:
        names = list(source_files)
        if not names:
            return {}
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    source_file,
                    COUNT(*) AS row_count
                FROM pm_ltefdd_sample
                WHERE source_file = ANY(%s)
                GROUP BY source_file
                """,
                (names,),
            )
            rows = list(cursor.fetchall())
        return {row["source_file"]: row["row_count"] for row in rows}
