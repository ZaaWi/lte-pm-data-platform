DROP VIEW IF EXISTS vw_kpi_ratio_values_by_time_ani;
DROP VIEW IF EXISTS vw_kpi_ratio_values_by_time;
DROP VIEW IF EXISTS vw_pm_counter_agg_by_time_ani_counter;
DROP VIEW IF EXISTS vw_pm_counter_agg_by_time_counter;
DROP VIEW IF EXISTS vw_pm_counts_by_counter_id;
DROP VIEW IF EXISTS vw_pm_counts_by_ani;
DROP VIEW IF EXISTS vw_pm_counts_by_collect_time;
DROP VIEW IF EXISTS vw_pm_counts_by_source_file;
DROP VIEW IF EXISTS vw_kpi_scope_entity_counter_base;
DROP VIEW IF EXISTS vw_pm_daily_combined_ltefdd_coverage;
DROP VIEW IF EXISTS vw_pm_daily_family_coverage;
DROP VIEW IF EXISTS vw_file_lifecycle_exceptions;
DROP VIEW IF EXISTS vw_file_ingest_observability;
DROP VIEW IF EXISTS vw_pm_combined_ltefdd_cell_coverage_timeline;
DROP VIEW IF EXISTS vw_pm_cell_entity_counts_timeline;
DROP VIEW IF EXISTS vw_pm_family_interval_coverage;
DROP VIEW IF EXISTS vw_pm_cell_entity_counts_by_interval;
DROP VIEW IF EXISTS vw_pm_logical_entity_counts_by_time;
DROP VIEW IF EXISTS vw_pm_entity_counter_coverage;
DROP VIEW IF EXISTS vw_pm_counter_agg_by_entity_time_counter;
DROP VIEW IF EXISTS vw_pm_raw_with_entity;
DROP VIEW IF EXISTS vw_pm_entity_summary_by_family;
DROP VIEW IF EXISTS vw_pm_distinct_entities;

CREATE OR REPLACE VIEW vw_pm_counts_by_source_file AS
SELECT
    source_file,
    dataset_family,
    interval_start,
    revision,
    COUNT(*) AS row_count,
    COUNT(counter_value) AS non_null_counter_value_count
FROM pm_ltefdd_sample
GROUP BY source_file, dataset_family, interval_start, revision;

CREATE OR REPLACE VIEW vw_pm_counts_by_collect_time AS
SELECT
    collect_time,
    dataset_family,
    COUNT(*) AS row_count,
    COUNT(counter_value) AS non_null_counter_value_count
FROM pm_ltefdd_sample
GROUP BY collect_time, dataset_family;

CREATE OR REPLACE VIEW vw_pm_counts_by_ani AS
SELECT
    s.ani,
    s.dataset_family,
    s.sbnid,
    s.enbid,
    s.enodebid,
    s.cellid,
    s.meid,
    e.logical_entity_key,
    e.site_code,
    e.region_name,
    COUNT(*) AS row_count,
    COUNT(s.counter_value) AS non_null_counter_value_count
FROM pm_ltefdd_sample AS s
LEFT JOIN ref_lte_entity AS e
    ON e.ani = s.ani
GROUP BY
    s.ani,
    s.dataset_family,
    s.sbnid,
    s.enbid,
    s.enodebid,
    s.cellid,
    s.meid,
    e.logical_entity_key,
    e.site_code,
    e.region_name;

CREATE OR REPLACE VIEW vw_pm_counts_by_counter_id AS
SELECT
    s.counter_id,
    r.vendor,
    r.technology,
    r.object_type,
    r.description,
    r.unit,
    COUNT(*) AS row_count,
    COUNT(s.counter_value) AS non_null_counter_value_count
FROM pm_ltefdd_sample AS s
LEFT JOIN ref_pm_counter AS r
    ON r.counter_id = s.counter_id
GROUP BY
    s.counter_id,
    r.vendor,
    r.technology,
    r.object_type,
    r.description,
    r.unit;

CREATE OR REPLACE VIEW vw_pm_counter_agg_by_time_counter AS
SELECT
    s.collect_time,
    s.dataset_family,
    s.counter_id,
    r.vendor,
    r.technology,
    r.object_type,
    r.description,
    r.unit,
    COUNT(*) AS row_count,
    COUNT(s.counter_value) AS non_null_counter_value_count,
    SUM(s.counter_value) AS sum_counter_value,
    AVG(s.counter_value) AS avg_counter_value,
    MIN(s.counter_value) AS min_counter_value,
    MAX(s.counter_value) AS max_counter_value
FROM pm_ltefdd_sample AS s
LEFT JOIN ref_pm_counter AS r
    ON r.counter_id = s.counter_id
GROUP BY
    s.collect_time,
    s.dataset_family,
    s.counter_id,
    r.vendor,
    r.technology,
    r.object_type,
    r.description,
    r.unit;

CREATE OR REPLACE VIEW vw_pm_counter_agg_by_time_ani_counter AS
SELECT
    s.collect_time,
    s.ani,
    s.dataset_family,
    s.sbnid,
    s.enbid,
    s.enodebid,
    s.cellid,
    s.meid,
    e.logical_entity_key,
    e.site_code,
    e.region_name,
    s.counter_id,
    r.vendor,
    r.technology,
    r.object_type,
    r.description,
    r.unit,
    COUNT(*) AS row_count,
    COUNT(s.counter_value) AS non_null_counter_value_count,
    SUM(s.counter_value) AS sum_counter_value,
    AVG(s.counter_value) AS avg_counter_value,
    MIN(s.counter_value) AS min_counter_value,
    MAX(s.counter_value) AS max_counter_value
FROM pm_ltefdd_sample AS s
LEFT JOIN ref_pm_counter AS r
    ON r.counter_id = s.counter_id
LEFT JOIN ref_lte_entity AS e
    ON e.ani = s.ani
GROUP BY
    s.collect_time,
    s.ani,
    s.dataset_family,
    s.sbnid,
    s.enbid,
    s.enodebid,
    s.cellid,
    s.meid,
    e.logical_entity_key,
    e.site_code,
    e.region_name,
    s.counter_id,
    r.vendor,
    r.technology,
    r.object_type,
    r.description,
    r.unit;

CREATE OR REPLACE VIEW vw_pm_distinct_entities AS
SELECT DISTINCT
    dataset_family,
    sbnid,
    enbid,
    enodebid,
    cellid,
    meid,
    ani,
    CASE
        WHEN dataset_family = 'PM/sdr/ltefdd'
            THEN CONCAT(
                'family=', COALESCE(dataset_family, 'UNKNOWN'),
                '|sbnid=', COALESCE(sbnid, ''),
                '|enodebid=', COALESCE(enodebid, ''),
                '|cellid=', COALESCE(cellid, '')
            )
        WHEN dataset_family = 'PM/itbbu/ltefdd'
            THEN CONCAT(
                'family=', COALESCE(dataset_family, 'UNKNOWN'),
                '|sbnid=', COALESCE(sbnid, ''),
                '|enbid=', COALESCE(enbid, ''),
                '|cellid=', COALESCE(cellid, '')
            )
        WHEN dataset_family = 'PM/itbbu/itbbuplat'
            THEN CONCAT(
                'family=', COALESCE(dataset_family, 'UNKNOWN'),
                '|sbnid=', COALESCE(sbnid, ''),
                '|meid=', COALESCE(meid, '')
            )
        ELSE CONCAT(
            'family=', COALESCE(dataset_family, 'UNKNOWN'),
            '|sbnid=', COALESCE(sbnid, ''),
            '|enbid=', COALESCE(enbid, ''),
            '|enodebid=', COALESCE(enodebid, ''),
            '|cellid=', COALESCE(cellid, ''),
            '|meid=', COALESCE(meid, ''),
            '|ani=', COALESCE(ani, '')
        )
    END AS logical_entity_key,
    CASE
        WHEN dataset_family IN ('PM/sdr/ltefdd', 'PM/itbbu/ltefdd') THEN 'cell'
        WHEN dataset_family = 'PM/itbbu/itbbuplat' THEN 'meid'
        ELSE 'raw'
    END AS entity_level
FROM pm_ltefdd_sample;

CREATE OR REPLACE VIEW vw_pm_entity_summary_by_family AS
SELECT
    dataset_family,
    entity_level,
    COUNT(*) AS distinct_logical_entity_keys,
    COUNT(DISTINCT sbnid) AS distinct_sbnid,
    COUNT(DISTINCT enbid) AS distinct_enbid,
    COUNT(DISTINCT enodebid) AS distinct_enodebid,
    COUNT(DISTINCT cellid) AS distinct_cellid,
    COUNT(DISTINCT meid) AS distinct_meid
FROM vw_pm_distinct_entities
GROUP BY dataset_family, entity_level;

COMMENT ON VIEW vw_pm_distinct_entities IS
    'Distinct raw-entity shapes derived empirically from family-specific identity fields. This is a practical normalization layer, not vendor-confirmed telecom truth.';

COMMENT ON VIEW vw_pm_entity_summary_by_family IS
    'Entity-normalization summary by dataset family and empirical entity level.';

CREATE OR REPLACE VIEW vw_pm_raw_with_entity AS
WITH raw_keys AS (
    SELECT
        s.*,
        CASE
            WHEN s.dataset_family = 'PM/sdr/ltefdd'
                THEN CONCAT(
                    'family=', COALESCE(s.dataset_family, 'UNKNOWN'),
                    '|sbnid=', COALESCE(s.sbnid, ''),
                    '|enodebid=', COALESCE(s.enodebid, ''),
                    '|cellid=', COALESCE(s.cellid, '')
                )
            WHEN s.dataset_family = 'PM/itbbu/ltefdd'
                THEN CONCAT(
                    'family=', COALESCE(s.dataset_family, 'UNKNOWN'),
                    '|sbnid=', COALESCE(s.sbnid, ''),
                    '|enbid=', COALESCE(s.enbid, ''),
                    '|cellid=', COALESCE(s.cellid, '')
                )
            WHEN s.dataset_family = 'PM/itbbu/itbbuplat'
                THEN CONCAT(
                    'family=', COALESCE(s.dataset_family, 'UNKNOWN'),
                    '|sbnid=', COALESCE(s.sbnid, ''),
                    '|meid=', COALESCE(s.meid, '')
                )
            ELSE CONCAT(
                'family=', COALESCE(s.dataset_family, 'UNKNOWN'),
                '|sbnid=', COALESCE(s.sbnid, ''),
                '|enbid=', COALESCE(s.enbid, ''),
                '|enodebid=', COALESCE(s.enodebid, ''),
                '|cellid=', COALESCE(s.cellid, ''),
                '|meid=', COALESCE(s.meid, ''),
                '|ani=', COALESCE(s.ani, '')
            )
        END AS derived_logical_entity_key,
        CASE
            WHEN s.dataset_family IN ('PM/sdr/ltefdd', 'PM/itbbu/ltefdd') THEN 'cell'
            WHEN s.dataset_family = 'PM/itbbu/itbbuplat' THEN 'meid'
            ELSE 'raw'
        END AS derived_entity_level
    FROM pm_ltefdd_sample AS s
)
SELECT
    r.*,
    COALESCE(e.logical_entity_key, r.derived_logical_entity_key) AS logical_entity_key,
    COALESCE(e.entity_level, r.derived_entity_level) AS entity_level
FROM raw_keys AS r
LEFT JOIN ref_lte_entity_identity AS e
    ON e.logical_entity_key = r.derived_logical_entity_key;

COMMENT ON VIEW vw_pm_raw_with_entity IS
    'Raw fact rows with deterministic logical_entity_key and entity_level attached for entity-aware analytics and KPI scoping.';

CREATE OR REPLACE VIEW vw_pm_counter_agg_by_entity_time_counter AS
SELECT
    dataset_family,
    entity_level,
    logical_entity_key,
    collect_time,
    interval_start,
    counter_id,
    COUNT(*) AS row_count,
    COUNT(counter_value) AS non_null_counter_value_count,
    SUM(counter_value) AS sum_counter_value,
    AVG(counter_value) AS avg_counter_value,
    MIN(counter_value) AS min_counter_value,
    MAX(counter_value) AS max_counter_value
FROM vw_pm_raw_with_entity
GROUP BY
    dataset_family,
    entity_level,
    logical_entity_key,
    collect_time,
    interval_start,
    counter_id;

COMMENT ON VIEW vw_pm_counter_agg_by_entity_time_counter IS
    'Entity-aware aggregation grain for future KPI work: dataset_family + logical_entity_key + collect_time + interval_start + counter_id.';

CREATE OR REPLACE VIEW vw_pm_logical_entity_counts_by_time AS
SELECT
    dataset_family,
    entity_level,
    collect_time,
    interval_start,
    COUNT(DISTINCT logical_entity_key) AS distinct_logical_entity_keys
FROM vw_pm_raw_with_entity
GROUP BY
    dataset_family,
    entity_level,
    collect_time,
    interval_start;

COMMENT ON VIEW vw_pm_logical_entity_counts_by_time IS
    'Coverage view showing distinct logical_entity_key counts by dataset family and time.';

CREATE OR REPLACE VIEW vw_pm_cell_entity_counts_timeline AS
SELECT
    dataset_family,
    collect_time,
    interval_start,
    COUNT(DISTINCT logical_entity_key) AS distinct_cell_entities
FROM vw_pm_raw_with_entity
WHERE entity_level = 'cell'
GROUP BY
    dataset_family,
    collect_time,
    interval_start;

COMMENT ON VIEW vw_pm_cell_entity_counts_timeline IS
    'Cell-level distinct logical entity counts by family and collect_time for timeline coverage analysis.';

CREATE OR REPLACE VIEW vw_pm_cell_entity_counts_by_interval AS
SELECT
    dataset_family,
    interval_start,
    collect_time,
    COUNT(DISTINCT logical_entity_key) AS distinct_cell_entities
FROM vw_pm_raw_with_entity
WHERE entity_level = 'cell'
GROUP BY
    dataset_family,
    interval_start,
    collect_time;

COMMENT ON VIEW vw_pm_cell_entity_counts_by_interval IS
    'Coverage view showing cell-level logical entity counts by family and interval.';

CREATE OR REPLACE VIEW vw_pm_entity_counter_coverage AS
SELECT
    dataset_family,
    entity_level,
    logical_entity_key,
    COUNT(DISTINCT counter_id) AS distinct_counter_ids,
    COUNT(DISTINCT collect_time) AS distinct_collect_times,
    MIN(collect_time) AS min_collect_time,
    MAX(collect_time) AS max_collect_time
FROM vw_pm_raw_with_entity
GROUP BY
    dataset_family,
    entity_level,
    logical_entity_key;

COMMENT ON VIEW vw_pm_entity_counter_coverage IS
    'Per-entity counter and interval coverage summary derived from the entity-aware raw layer.';

CREATE OR REPLACE VIEW vw_pm_family_interval_coverage AS
SELECT
    dataset_family,
    interval_start,
    MIN(collect_time) AS min_collect_time,
    MAX(collect_time) AS max_collect_time,
    COUNT(DISTINCT source_file) AS distinct_source_files,
    COUNT(DISTINCT revision) AS distinct_revisions,
    COUNT(DISTINCT logical_entity_key) AS distinct_logical_entity_keys,
    COUNT(DISTINCT CASE WHEN entity_level = 'cell' THEN logical_entity_key END) AS distinct_cell_entities,
    COUNT(DISTINCT counter_id) AS distinct_counter_ids,
    COUNT(*) AS row_count
FROM vw_pm_raw_with_entity
GROUP BY
    dataset_family,
    interval_start;

COMMENT ON VIEW vw_pm_family_interval_coverage IS
    'Family and interval coverage summary for completeness analysis, including distinct entities, revisions, counters, and rows.';

CREATE OR REPLACE VIEW vw_file_ingest_observability AS
WITH raw_by_source_file AS (
    SELECT
        source_file,
        COUNT(*) AS raw_row_count,
        COUNT(DISTINCT collect_time) AS distinct_collect_times,
        MIN(collect_time) AS min_collect_time,
        MAX(collect_time) AS max_collect_time
    FROM pm_ltefdd_sample
    GROUP BY source_file
)
SELECT
    a.source_file,
    a.file_hash,
    a.run_id,
    a.trigger_type,
    a.source_type,
    a.processed_at,
    a.status,
    a.csv_files_found,
    a.input_rows_read,
    a.normalized_rows_emitted,
    a.rows_inserted,
    COALESCE(r.raw_row_count, 0) AS raw_row_count,
    a.lifecycle_status,
    a.lifecycle_action,
    a.normalization_status,
    a.normalized_at,
    a.final_file_path,
    a.error_message,
    a.normalization_error,
    r.distinct_collect_times,
    r.min_collect_time,
    r.max_collect_time
FROM file_audit AS a
LEFT JOIN raw_by_source_file AS r
    ON r.source_file = a.source_file;

COMMENT ON VIEW vw_file_ingest_observability IS
    'File-level ingest observability view combining audit metadata, raw row presence, lifecycle state, and normalization state.';

CREATE OR REPLACE VIEW vw_file_lifecycle_exceptions AS
SELECT
    *
FROM vw_file_ingest_observability
WHERE lifecycle_status <> 'COMPLETED'
   OR (status = 'SUCCESS' AND raw_row_count = 0)
   OR (status = 'FAILED' AND raw_row_count > 0)
   OR (status = 'SUCCESS' AND normalization_status = 'FAILED');

COMMENT ON VIEW vw_file_lifecycle_exceptions IS
    'Operational exception view for lifecycle mismatches, inconsistent raw-row presence, and failed normalization after successful ingest.';

CREATE OR REPLACE VIEW vw_pm_combined_ltefdd_cell_coverage_timeline AS
SELECT
    collect_time,
    interval_start,
    SUM(distinct_cell_entities) AS combined_observed_cells
FROM vw_pm_cell_entity_counts_timeline
WHERE dataset_family IN ('PM/itbbu/ltefdd', 'PM/sdr/ltefdd')
GROUP BY
    collect_time,
    interval_start;

COMMENT ON VIEW vw_pm_combined_ltefdd_cell_coverage_timeline IS
    'Combined LTEFDD cell-level observed entity counts by collect_time across the two empirically cell-level families.';

CREATE OR REPLACE VIEW vw_pm_daily_family_coverage AS
SELECT
    dataset_family,
    collect_time::date AS collect_date,
    COUNT(*) AS row_count,
    COUNT(DISTINCT source_file) AS distinct_source_files,
    COUNT(DISTINCT collect_time) AS distinct_collect_times,
    COUNT(DISTINCT logical_entity_key) AS distinct_logical_entities,
    COUNT(DISTINCT CASE WHEN entity_level = 'cell' THEN logical_entity_key END) AS distinct_cell_entities
FROM vw_pm_raw_with_entity
GROUP BY
    dataset_family,
    collect_time::date;

COMMENT ON VIEW vw_pm_daily_family_coverage IS
    'Daily family-level coverage summary with logical entity counts, cell-level counts, source files, and collect times.';

CREATE OR REPLACE VIEW vw_pm_daily_combined_ltefdd_coverage AS
SELECT
    collect_date,
    SUM(distinct_cell_entities) AS combined_observed_cells,
    SUM(row_count) AS row_count,
    SUM(distinct_source_files) AS distinct_source_files,
    SUM(distinct_collect_times) AS distinct_collect_times
FROM vw_pm_daily_family_coverage
WHERE dataset_family IN ('PM/itbbu/ltefdd', 'PM/sdr/ltefdd')
GROUP BY collect_date;

COMMENT ON VIEW vw_pm_daily_combined_ltefdd_coverage IS
    'Daily combined LTEFDD cell-level coverage summary across the two empirically cell-level families.';

CREATE OR REPLACE VIEW vw_kpi_scope_entity_counter_base AS
SELECT
    dataset_family,
    entity_level,
    logical_entity_key,
    collect_time,
    interval_start,
    counter_id,
    row_count,
    non_null_counter_value_count,
    sum_counter_value,
    avg_counter_value,
    min_counter_value,
    max_counter_value
FROM vw_pm_counter_agg_by_entity_time_counter;

COMMENT ON VIEW vw_kpi_scope_entity_counter_base IS
    'Semantic-safe KPI scoping base only. No KPI meaning is implied without verified counter mappings.';
