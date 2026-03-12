SELECT source_file, csv_name, collect_time, counter_id, counter_value
FROM pm_ltefdd_sample
ORDER BY id DESC
LIMIT 20;

SELECT
    source_file,
    dataset_family,
    interval_start,
    revision,
    row_count
FROM vw_pm_counts_by_source_file
ORDER BY row_count DESC, source_file;

SELECT
    counter_id,
    row_count,
    non_null_counter_value_count
FROM vw_pm_counts_by_counter_id
ORDER BY row_count DESC, counter_id;

SELECT
    dataset_family,
    collect_time,
    distinct_logical_entity_keys
FROM vw_pm_logical_entity_counts_by_time
ORDER BY collect_time DESC, dataset_family;

SELECT
    dataset_family,
    collect_time,
    distinct_cell_entities
FROM vw_pm_cell_entity_counts_timeline
ORDER BY collect_time DESC, dataset_family;

SELECT
    collect_date,
    dataset_family,
    distinct_cell_entities,
    distinct_collect_times,
    distinct_source_files
FROM vw_pm_daily_family_coverage
ORDER BY collect_date DESC, dataset_family;

SELECT
    collect_date,
    combined_observed_cells,
    row_count
FROM vw_pm_daily_combined_ltefdd_coverage
ORDER BY collect_date DESC;

SELECT
    source_file,
    status,
    raw_row_count,
    lifecycle_status,
    normalization_status,
    processed_at
FROM vw_file_ingest_observability
ORDER BY processed_at DESC;

SELECT
    source_file,
    status,
    lifecycle_status,
    final_file_path,
    error_message
FROM vw_file_lifecycle_exceptions
ORDER BY processed_at DESC;
