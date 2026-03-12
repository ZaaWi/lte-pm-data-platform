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
