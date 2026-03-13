CREATE TABLE IF NOT EXISTS ref_kpi_counter_mapping (
    kpi_name TEXT NOT NULL REFERENCES ref_kpi_definition (kpi_name) ON DELETE CASCADE,
    counter_role TEXT NOT NULL CHECK (counter_role IN ('numerator', 'denominator')),
    counter_id TEXT NOT NULL REFERENCES ref_pm_counter (counter_id),
    weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kpi_name, counter_role, counter_id)
);

ALTER TABLE ref_pm_counter
    ADD COLUMN IF NOT EXISTS source_type TEXT,
    ADD COLUMN IF NOT EXISTS source_reference TEXT,
    ADD COLUMN IF NOT EXISTS verification_status TEXT NOT NULL DEFAULT 'UNKNOWN',
    ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP;

CREATE OR REPLACE VIEW vw_kpi_definition_details AS
SELECT
    d.kpi_name,
    d.technology,
    d.description,
    d.formula_type,
    d.scale_factor,
    d.formula_notes,
    d.unit,
    d.aggregation_grain_notes,
    d.status,
    d.pending_reason,
    COALESCE(
        ARRAY_AGG(m.counter_id ORDER BY m.counter_id)
            FILTER (WHERE m.counter_role = 'numerator'),
        '{}'::TEXT[]
    ) AS numerator_counter_ids,
    COALESCE(
        ARRAY_AGG(m.counter_id ORDER BY m.counter_id)
            FILTER (WHERE m.counter_role = 'denominator'),
        '{}'::TEXT[]
    ) AS denominator_counter_ids,
    COALESCE(
        ARRAY_AGG(m.counter_id ORDER BY m.counter_id)
            FILTER (WHERE c.verification_status IS DISTINCT FROM 'VERIFIED'),
        '{}'::TEXT[]
    ) AS unverified_counter_ids,
    COALESCE(
        BOOL_AND(c.verification_status = 'VERIFIED')
            FILTER (WHERE m.counter_id IS NOT NULL),
        FALSE
    ) AS all_mapped_counters_verified
FROM ref_kpi_definition AS d
LEFT JOIN ref_kpi_counter_mapping AS m
    ON m.kpi_name = d.kpi_name
LEFT JOIN ref_pm_counter AS c
    ON c.counter_id = m.counter_id
GROUP BY
    d.kpi_name,
    d.technology,
    d.description,
    d.formula_type,
    d.scale_factor,
    d.formula_notes,
    d.unit,
    d.aggregation_grain_notes,
    d.status,
    d.pending_reason;

CREATE OR REPLACE VIEW vw_kpi_ratio_values_by_time_ani AS
WITH numerator AS (
    SELECT
        d.kpi_name,
        a.collect_time,
        a.ani,
        a.logical_entity_key,
        a.site_code,
        a.region_name,
        SUM(a.sum_counter_value * m.weight) AS numerator_value
    FROM ref_kpi_definition AS d
    JOIN ref_kpi_counter_mapping AS m
        ON m.kpi_name = d.kpi_name
       AND m.counter_role = 'numerator'
    JOIN ref_pm_counter AS c
        ON c.counter_id = m.counter_id
       AND c.verification_status = 'VERIFIED'
    JOIN vw_pm_counter_agg_by_time_ani_counter AS a
        ON a.counter_id = m.counter_id
    WHERE d.status = 'ACTIVE'
      AND d.formula_type = 'ratio'
    GROUP BY
        d.kpi_name,
        a.collect_time,
        a.ani,
        a.logical_entity_key,
        a.site_code,
        a.region_name
),
denominator AS (
    SELECT
        d.kpi_name,
        a.collect_time,
        a.ani,
        a.logical_entity_key,
        a.site_code,
        a.region_name,
        SUM(a.sum_counter_value * m.weight) AS denominator_value
    FROM ref_kpi_definition AS d
    JOIN ref_kpi_counter_mapping AS m
        ON m.kpi_name = d.kpi_name
       AND m.counter_role = 'denominator'
    JOIN ref_pm_counter AS c
        ON c.counter_id = m.counter_id
       AND c.verification_status = 'VERIFIED'
    JOIN vw_pm_counter_agg_by_time_ani_counter AS a
        ON a.counter_id = m.counter_id
    WHERE d.status = 'ACTIVE'
      AND d.formula_type = 'ratio'
    GROUP BY
        d.kpi_name,
        a.collect_time,
        a.ani,
        a.logical_entity_key,
        a.site_code,
        a.region_name
)
SELECT
    d.kpi_name,
    d.technology,
    d.description,
    d.unit,
    d.aggregation_grain_notes,
    d.formula_notes,
    n.collect_time,
    n.ani,
    n.logical_entity_key,
    n.site_code,
    n.region_name,
    n.numerator_value,
    den.denominator_value,
    CASE
        WHEN n.numerator_value IS NULL THEN NULL
        WHEN den.denominator_value IS NULL THEN NULL
        WHEN den.denominator_value = 0 THEN NULL
        ELSE (n.numerator_value / den.denominator_value) * COALESCE(d.scale_factor, 1.0)
    END AS kpi_value
FROM ref_kpi_definition AS d
JOIN numerator AS n
    ON n.kpi_name = d.kpi_name
JOIN denominator AS den
    ON den.kpi_name = d.kpi_name
   AND den.collect_time = n.collect_time
   AND den.ani IS NOT DISTINCT FROM n.ani
WHERE d.status = 'ACTIVE'
  AND d.formula_type = 'ratio';

CREATE OR REPLACE VIEW vw_kpi_ratio_values_by_time AS
WITH numerator AS (
    SELECT
        d.kpi_name,
        a.collect_time,
        SUM(a.sum_counter_value * m.weight) AS numerator_value
    FROM ref_kpi_definition AS d
    JOIN ref_kpi_counter_mapping AS m
        ON m.kpi_name = d.kpi_name
       AND m.counter_role = 'numerator'
    JOIN ref_pm_counter AS c
        ON c.counter_id = m.counter_id
       AND c.verification_status = 'VERIFIED'
    JOIN vw_pm_counter_agg_by_time_counter AS a
        ON a.counter_id = m.counter_id
    WHERE d.status = 'ACTIVE'
      AND d.formula_type = 'ratio'
    GROUP BY
        d.kpi_name,
        a.collect_time
),
denominator AS (
    SELECT
        d.kpi_name,
        a.collect_time,
        SUM(a.sum_counter_value * m.weight) AS denominator_value
    FROM ref_kpi_definition AS d
    JOIN ref_kpi_counter_mapping AS m
        ON m.kpi_name = d.kpi_name
       AND m.counter_role = 'denominator'
    JOIN ref_pm_counter AS c
        ON c.counter_id = m.counter_id
       AND c.verification_status = 'VERIFIED'
    JOIN vw_pm_counter_agg_by_time_counter AS a
        ON a.counter_id = m.counter_id
    WHERE d.status = 'ACTIVE'
      AND d.formula_type = 'ratio'
    GROUP BY
        d.kpi_name,
        a.collect_time
)
SELECT
    d.kpi_name,
    d.technology,
    d.description,
    d.unit,
    d.aggregation_grain_notes,
    d.formula_notes,
    n.collect_time,
    n.numerator_value,
    den.denominator_value,
    CASE
        WHEN n.numerator_value IS NULL THEN NULL
        WHEN den.denominator_value IS NULL THEN NULL
        WHEN den.denominator_value = 0 THEN NULL
        ELSE (n.numerator_value / den.denominator_value) * COALESCE(d.scale_factor, 1.0)
    END AS kpi_value
FROM ref_kpi_definition AS d
JOIN numerator AS n
    ON n.kpi_name = d.kpi_name
JOIN denominator AS den
    ON den.kpi_name = d.kpi_name
   AND den.collect_time = n.collect_time
WHERE d.status = 'ACTIVE'
  AND d.formula_type = 'ratio';

CREATE OR REPLACE VIEW vw_pm_raw_with_counter_semantics AS
SELECT
    r.source_file,
    r.dataset_family,
    r.interval_start,
    r.collect_time,
    r.logical_entity_key,
    r.entity_level,
    r.site_code,
    r.site_name,
    r.region_code,
    r.region_name,
    r.reporting_key,
    r.reporting_name,
    r.reporting_level,
    r.counter_id,
    r.counter_value,
    d.counter_alias,
    d.counter_name,
    d.unit,
    d.aggregation_behavior,
    d.verification_status AS counter_verification_status,
    d.source_note
FROM vw_pm_raw_with_entity_topology AS r
LEFT JOIN ref_semantic_counter_dictionary AS d
    ON d.dataset_family = r.dataset_family
   AND d.counter_id = r.counter_id;

CREATE OR REPLACE VIEW vw_semantic_counter_mapping_gaps AS
SELECT
    s.dataset_family,
    s.counter_id,
    COUNT(*) AS row_count,
    COUNT(DISTINCT s.logical_entity_key) AS distinct_logical_entities,
    MIN(s.collect_time) AS min_collect_time,
    MAX(s.collect_time) AS max_collect_time
FROM vw_pm_raw_with_entity AS s
LEFT JOIN ref_semantic_counter_dictionary AS d
    ON d.dataset_family = s.dataset_family
   AND d.counter_id = s.counter_id
WHERE d.counter_id IS NULL
GROUP BY
    s.dataset_family,
    s.counter_id;

CREATE OR REPLACE VIEW vw_semantic_kpi_definition_details AS
SELECT
    d.kpi_code,
    d.kpi_name,
    d.formula_expression,
    d.grain,
    d.unit,
    d.verification_status,
    d.topology_rollup_allowed,
    d.notes,
    COUNT(i.input_alias) AS input_count,
    COALESCE(
        ARRAY_AGG(DISTINCT i.input_alias ORDER BY i.input_alias)
            FILTER (WHERE i.input_alias IS NOT NULL),
        '{}'::TEXT[]
    ) AS input_aliases,
    COALESCE(
        ARRAY_AGG(DISTINCT i.counter_alias ORDER BY i.counter_alias)
            FILTER (WHERE i.counter_alias IS NOT NULL),
        '{}'::TEXT[]
    ) AS counter_aliases,
    COALESCE(
        ARRAY_AGG(DISTINCT i.counter_alias ORDER BY i.counter_alias)
            FILTER (WHERE c.verification_status IS DISTINCT FROM 'VERIFIED'),
        '{}'::TEXT[]
    ) AS unverified_counter_aliases
FROM ref_semantic_kpi_definition AS d
LEFT JOIN ref_semantic_kpi_formula_input AS i
    ON i.kpi_code = d.kpi_code
LEFT JOIN ref_semantic_counter_dictionary AS c
    ON c.dataset_family = i.dataset_family
   AND c.counter_alias = i.counter_alias
GROUP BY
    d.kpi_code,
    d.kpi_name,
    d.formula_expression,
    d.grain,
    d.unit,
    d.verification_status,
    d.topology_rollup_allowed,
    d.notes;

CREATE OR REPLACE VIEW vw_semantic_provisional_kpis AS
SELECT
    *
FROM vw_semantic_kpi_definition_details
WHERE verification_status <> 'VERIFIED'
   OR array_length(unverified_counter_aliases, 1) > 0;

CREATE OR REPLACE VIEW vw_semantic_kpi_base_inputs AS
SELECT
    dataset_family,
    collect_time,
    interval_start,
    logical_entity_key,
    entity_level,
    site_code,
    site_name,
    region_code,
    region_name,
    reporting_key,
    reporting_name,
    reporting_level,
    counter_alias,
    aggregation_behavior,
    unit,
    SUM(counter_value) AS sum_counter_value,
    AVG(counter_value) AS avg_counter_value,
    MIN(counter_value) AS min_counter_value,
    MAX(counter_value) AS max_counter_value,
    COUNT(*) AS row_count
FROM vw_pm_raw_with_counter_semantics
WHERE counter_alias IS NOT NULL
GROUP BY
    dataset_family,
    collect_time,
    interval_start,
    logical_entity_key,
    entity_level,
    site_code,
    site_name,
    region_code,
    region_name,
    reporting_key,
    reporting_name,
    reporting_level,
    counter_alias,
    aggregation_behavior,
    unit;

CREATE OR REPLACE VIEW vw_semantic_kpi_input_coverage AS
SELECT
    i.kpi_code,
    d.kpi_name,
    i.input_alias,
    i.dataset_family,
    i.counter_alias,
    i.required,
    c.verification_status AS counter_verification_status,
    COUNT(DISTINCT b.logical_entity_key) AS distinct_logical_entities,
    COUNT(DISTINCT b.collect_time) AS distinct_collect_times,
    COALESCE(SUM(b.row_count), 0) AS row_count
FROM ref_semantic_kpi_formula_input AS i
JOIN ref_semantic_kpi_definition AS d
    ON d.kpi_code = i.kpi_code
LEFT JOIN ref_semantic_counter_dictionary AS c
    ON c.dataset_family = i.dataset_family
   AND c.counter_alias = i.counter_alias
LEFT JOIN vw_semantic_kpi_base_inputs AS b
    ON b.dataset_family = i.dataset_family
   AND b.counter_alias = i.counter_alias
GROUP BY
    i.kpi_code,
    d.kpi_name,
    i.input_alias,
    i.dataset_family,
    i.counter_alias,
    i.required,
    c.verification_status;

COMMENT ON VIEW vw_pm_raw_with_counter_semantics IS
    'Raw entity/topology-aware facts projected into semantic counter aliases where dictionary mappings exist.';

COMMENT ON VIEW vw_semantic_counter_mapping_gaps IS
    'Distinct raw counters observed in facts that do not yet have semantic dictionary mappings.';

COMMENT ON VIEW vw_semantic_kpi_definition_details IS
    'Semantic KPI definition details with input aliases and verification visibility.';

COMMENT ON VIEW vw_semantic_provisional_kpis IS
    'Semantic KPI definitions that remain provisional due to KPI or counter verification status.';

COMMENT ON VIEW vw_semantic_kpi_base_inputs IS
    'KPI-ready semantic base input aggregates by entity/time/topology grain and semantic counter alias.';

COMMENT ON VIEW vw_semantic_kpi_input_coverage IS
    'Coverage summary for KPI input aliases against semantic base inputs.';

CREATE OR REPLACE VIEW vw_verified_prb_kpi_entity_time AS
WITH prb_inputs AS (
    SELECT
        i.kpi_code,
        d.kpi_name,
        d.unit AS kpi_unit,
        d.topology_rollup_allowed,
        b.dataset_family,
        b.collect_time,
        b.interval_start,
        b.logical_entity_key,
        b.entity_level,
        b.site_code,
        b.site_name,
        b.region_code,
        b.region_name,
        b.reporting_key,
        b.reporting_name,
        b.reporting_level,
        i.input_alias,
        i.counter_alias,
        b.sum_counter_value
    FROM ref_semantic_kpi_definition AS d
    JOIN ref_semantic_kpi_formula_input AS i
        ON i.kpi_code = d.kpi_code
    JOIN ref_semantic_counter_dictionary AS c
        ON c.dataset_family = i.dataset_family
       AND c.counter_alias = i.counter_alias
       AND c.verification_status = 'VERIFIED'
    JOIN vw_semantic_kpi_base_inputs AS b
        ON b.dataset_family = i.dataset_family
       AND b.counter_alias = i.counter_alias
    WHERE d.kpi_code IN ('dl_prb_utilization', 'ul_prb_utilization')
      AND d.verification_status = 'VERIFIED'
      AND i.required = TRUE
      AND b.aggregation_behavior = 'SUM'
      AND b.unit = 'Number'
),
prb_pivot AS (
    SELECT
        kpi_code,
        kpi_name,
        kpi_unit,
        topology_rollup_allowed,
        dataset_family,
        collect_time,
        interval_start,
        logical_entity_key,
        entity_level,
        site_code,
        site_name,
        region_code,
        region_name,
        reporting_key,
        reporting_name,
        reporting_level,
        MAX(counter_alias) FILTER (WHERE input_alias = 'numerator') AS numerator_counter_alias,
        MAX(counter_alias) FILTER (WHERE input_alias = 'denominator') AS denominator_counter_alias,
        SUM(sum_counter_value) FILTER (WHERE input_alias = 'numerator') AS numerator_value,
        SUM(sum_counter_value) FILTER (WHERE input_alias = 'denominator') AS denominator_value,
        COUNT(DISTINCT input_alias) AS present_input_count
    FROM prb_inputs
    GROUP BY
        kpi_code,
        kpi_name,
        kpi_unit,
        topology_rollup_allowed,
        dataset_family,
        collect_time,
        interval_start,
        logical_entity_key,
        entity_level,
        site_code,
        site_name,
        region_code,
        region_name,
        reporting_key,
        reporting_name,
        reporting_level
)
SELECT
    kpi_code,
    kpi_name,
    dataset_family,
    collect_time,
    interval_start,
    logical_entity_key,
    entity_level,
    site_code,
    site_name,
    region_code,
    region_name,
    reporting_key,
    reporting_name,
    reporting_level,
    numerator_counter_alias,
    denominator_counter_alias,
    numerator_value,
    denominator_value,
    CASE
        WHEN present_input_count < 2 THEN NULL
        WHEN denominator_value IS NULL THEN NULL
        WHEN denominator_value = 0 THEN NULL
        ELSE 100.0 * numerator_value / denominator_value
    END AS kpi_value,
    kpi_unit AS unit,
    topology_rollup_allowed
FROM prb_pivot
WHERE present_input_count = 2;

CREATE OR REPLACE VIEW vw_verified_prb_kpi_execution_validation AS
WITH coverage AS (
    SELECT
        kpi_code,
        dataset_family,
        MIN(counter_verification_status) AS min_counter_verification_status,
        COUNT(DISTINCT input_alias) AS covered_input_aliases,
        MIN(distinct_collect_times) AS min_input_collect_times,
        MIN(distinct_logical_entities) AS min_input_entities
    FROM vw_semantic_kpi_input_coverage
    WHERE kpi_code IN ('dl_prb_utilization', 'ul_prb_utilization')
    GROUP BY
        kpi_code,
        dataset_family
),
executed AS (
    SELECT
        kpi_code,
        dataset_family,
        COUNT(*) AS executed_rows,
        COUNT(DISTINCT collect_time) AS executed_collect_times,
        COUNT(DISTINCT logical_entity_key) AS executed_entities
    FROM vw_verified_prb_kpi_entity_time
    GROUP BY
        kpi_code,
        dataset_family
)
SELECT
    c.kpi_code,
    c.dataset_family,
    c.covered_input_aliases,
    c.min_counter_verification_status,
    c.min_input_collect_times,
    c.min_input_entities,
    COALESCE(e.executed_rows, 0) AS executed_rows,
    COALESCE(e.executed_collect_times, 0) AS executed_collect_times,
    COALESCE(e.executed_entities, 0) AS executed_entities
FROM coverage AS c
LEFT JOIN executed AS e
    ON e.kpi_code = c.kpi_code
   AND e.dataset_family = c.dataset_family;

COMMENT ON VIEW vw_verified_prb_kpi_entity_time IS
    'Executed verified PRB KPI outputs at entity/time grain only, using verified semantic PRB inputs.';

COMMENT ON VIEW vw_verified_prb_kpi_execution_validation IS
    'Validation summary comparing verified PRB KPI execution rows against the semantic KPI input coverage layer.';

CREATE OR REPLACE VIEW vw_verified_bler_kpi_entity_time AS
WITH bler_inputs AS (
    SELECT
        i.kpi_code,
        d.kpi_name,
        d.unit AS kpi_unit,
        d.topology_rollup_allowed,
        b.dataset_family,
        b.collect_time,
        b.interval_start,
        b.logical_entity_key,
        b.entity_level,
        b.site_code,
        b.site_name,
        b.region_code,
        b.region_name,
        b.reporting_key,
        b.reporting_name,
        b.reporting_level,
        i.input_alias,
        i.counter_alias,
        b.sum_counter_value
    FROM ref_semantic_kpi_definition AS d
    JOIN ref_semantic_kpi_formula_input AS i
        ON i.kpi_code = d.kpi_code
    JOIN ref_semantic_counter_dictionary AS c
        ON c.dataset_family = i.dataset_family
       AND c.counter_alias = i.counter_alias
       AND c.verification_status = 'VERIFIED'
    JOIN vw_semantic_kpi_base_inputs AS b
        ON b.dataset_family = i.dataset_family
       AND b.counter_alias = i.counter_alias
    WHERE d.kpi_code IN ('dl_bler', 'ul_bler')
      AND d.verification_status = 'VERIFIED'
      AND i.required = TRUE
      AND b.aggregation_behavior = 'SUM'
      AND b.unit = 'Number'
),
bler_pivot AS (
    SELECT
        kpi_code,
        kpi_name,
        kpi_unit,
        topology_rollup_allowed,
        dataset_family,
        collect_time,
        interval_start,
        logical_entity_key,
        entity_level,
        site_code,
        site_name,
        region_code,
        region_name,
        reporting_key,
        reporting_name,
        reporting_level,
        MAX(counter_alias) FILTER (WHERE input_alias = 'numerator') AS numerator_counter_alias,
        MAX(counter_alias) FILTER (WHERE input_alias = 'denominator') AS denominator_counter_alias,
        SUM(sum_counter_value) FILTER (WHERE input_alias = 'numerator') AS numerator_value,
        SUM(sum_counter_value) FILTER (WHERE input_alias = 'denominator') AS denominator_value,
        COUNT(DISTINCT input_alias) AS present_input_count
    FROM bler_inputs
    GROUP BY
        kpi_code,
        kpi_name,
        kpi_unit,
        topology_rollup_allowed,
        dataset_family,
        collect_time,
        interval_start,
        logical_entity_key,
        entity_level,
        site_code,
        site_name,
        region_code,
        region_name,
        reporting_key,
        reporting_name,
        reporting_level
)
SELECT
    kpi_code,
    kpi_name,
    dataset_family,
    collect_time,
    interval_start,
    logical_entity_key,
    entity_level,
    site_code,
    site_name,
    region_code,
    region_name,
    reporting_key,
    reporting_name,
    reporting_level,
    numerator_counter_alias,
    denominator_counter_alias,
    numerator_value,
    denominator_value,
    CASE
        WHEN present_input_count < 2 THEN NULL
        WHEN denominator_value IS NULL THEN NULL
        WHEN denominator_value = 0 THEN NULL
        ELSE 100.0 * numerator_value / denominator_value
    END AS kpi_value,
    kpi_unit AS unit,
    topology_rollup_allowed
FROM bler_pivot
WHERE present_input_count = 2;

CREATE OR REPLACE VIEW vw_verified_bler_kpi_execution_validation AS
WITH coverage AS (
    SELECT
        kpi_code,
        dataset_family,
        MIN(counter_verification_status) AS min_counter_verification_status,
        COUNT(DISTINCT input_alias) AS covered_input_aliases,
        MIN(distinct_collect_times) AS min_input_collect_times,
        MIN(distinct_logical_entities) AS min_input_entities
    FROM vw_semantic_kpi_input_coverage
    WHERE kpi_code IN ('dl_bler', 'ul_bler')
    GROUP BY
        kpi_code,
        dataset_family
),
executed AS (
    SELECT
        kpi_code,
        dataset_family,
        COUNT(*) AS executed_rows,
        COUNT(DISTINCT collect_time) AS executed_collect_times,
        COUNT(DISTINCT logical_entity_key) AS executed_entities
    FROM vw_verified_bler_kpi_entity_time
    GROUP BY
        kpi_code,
        dataset_family
)
SELECT
    c.kpi_code,
    c.dataset_family,
    c.covered_input_aliases,
    c.min_counter_verification_status,
    c.min_input_collect_times,
    c.min_input_entities,
    COALESCE(e.executed_rows, 0) AS executed_rows,
    COALESCE(e.executed_collect_times, 0) AS executed_collect_times,
    COALESCE(e.executed_entities, 0) AS executed_entities
FROM coverage AS c
LEFT JOIN executed AS e
    ON e.kpi_code = c.kpi_code
   AND e.dataset_family = c.dataset_family;

COMMENT ON VIEW vw_verified_bler_kpi_entity_time IS
    'Executed verified BLER KPI outputs at entity/time grain only, using verified semantic BLER inputs.';

COMMENT ON VIEW vw_verified_bler_kpi_execution_validation IS
    'Validation summary comparing verified BLER KPI execution rows against the semantic KPI input coverage layer.';

CREATE OR REPLACE VIEW vw_vendor_indicator_dictionary_details AS
SELECT
    d.indicator_code,
    d.indicator_name,
    d.semantic_alias,
    d.aggregation_method,
    d.unit,
    d.verification_status,
    d.source,
    l.lineage_expression,
    l.lineage_type,
    l.raw_counter_dependencies
FROM ref_vendor_indicator_dictionary AS d
LEFT JOIN ref_vendor_indicator_lineage AS l
    ON l.indicator_code = d.indicator_code;

COMMENT ON VIEW vw_vendor_indicator_dictionary_details IS
    'Inspection view for curated vendor indicator semantics, lineage, and verification status.';
