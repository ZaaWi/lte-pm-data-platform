CREATE TABLE IF NOT EXISTS ref_kpi_definition (
    kpi_name TEXT PRIMARY KEY,
    technology TEXT NOT NULL,
    description TEXT NOT NULL,
    formula_type TEXT NOT NULL,
    numerator_counter_id TEXT,
    denominator_counter_id TEXT,
    scale_factor DOUBLE PRECISION,
    formula_notes TEXT NOT NULL,
    unit TEXT,
    aggregation_grain_notes TEXT NOT NULL,
    status TEXT NOT NULL,
    pending_reason TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE ref_kpi_definition IS
    'KPI definition foundation. KPI rows remain pending until vendor counter mappings are verified.';

COMMENT ON COLUMN ref_kpi_definition.numerator_counter_id IS
    'Legacy single-counter field retained for backward compatibility. Use ref_kpi_counter_mapping for authoritative mappings.';

COMMENT ON COLUMN ref_kpi_definition.denominator_counter_id IS
    'Legacy single-counter field retained for backward compatibility. Use ref_kpi_counter_mapping for authoritative mappings.';

INSERT INTO ref_kpi_definition (
    kpi_name,
    technology,
    description,
    formula_type,
    numerator_counter_id,
    denominator_counter_id,
    scale_factor,
    formula_notes,
    unit,
    aggregation_grain_notes,
    status,
    pending_reason
)
VALUES
    (
        'lte_prb_utilization',
        'LTE',
        'Radio resource utilization ratio.',
        'ratio',
        NULL,
        NULL,
        NULL,
        'Expected formula when mappings are verified: 100 * SUM(PRB used) / NULLIF(SUM(PRB available), 0).',
        'percent',
        'collect_time + ani',
        'PENDING_COUNTER_MAPPING',
        'Verified ZTE numerator and denominator counters are not yet mapped in ref_pm_counter.'
    ),
    (
        'lte_dl_throughput',
        'LTE',
        'Downlink throughput rate.',
        'ratio',
        NULL,
        NULL,
        NULL,
        'Expected formula when mappings are verified: traffic volume / NULLIF(time, 0), with a unit conversion based on the real counter units.',
        'pending_unit',
        'collect_time + ani',
        'PENDING_COUNTER_MAPPING',
        'Verified ZTE traffic-volume and time counters are not yet mapped, so the unit conversion cannot be stated honestly.'
    ),
    (
        'lte_ul_throughput',
        'LTE',
        'Uplink throughput rate.',
        'ratio',
        NULL,
        NULL,
        NULL,
        'Expected formula when mappings are verified: traffic volume / NULLIF(time, 0), with a unit conversion based on the real counter units.',
        'pending_unit',
        'collect_time + ani',
        'PENDING_COUNTER_MAPPING',
        'Verified ZTE traffic-volume and time counters are not yet mapped, so the unit conversion cannot be stated honestly.'
    )
ON CONFLICT (kpi_name) DO NOTHING;
