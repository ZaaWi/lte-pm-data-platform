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

CREATE TABLE IF NOT EXISTS ref_semantic_counter_dictionary (
    dataset_family TEXT NOT NULL,
    counter_id TEXT NOT NULL,
    counter_alias TEXT NOT NULL,
    counter_name TEXT NOT NULL,
    unit TEXT,
    aggregation_behavior TEXT NOT NULL,
    verification_status TEXT NOT NULL,
    source_note TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset_family, counter_id),
    CONSTRAINT uq_semantic_counter_alias UNIQUE (dataset_family, counter_alias)
);

CREATE TABLE IF NOT EXISTS ref_semantic_counter_group (
    group_code TEXT PRIMARY KEY,
    group_name TEXT NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ref_semantic_counter_group_member (
    group_code TEXT NOT NULL REFERENCES ref_semantic_counter_group(group_code) ON DELETE CASCADE,
    dataset_family TEXT NOT NULL,
    counter_alias TEXT NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_code, dataset_family, counter_alias)
);

CREATE TABLE IF NOT EXISTS ref_semantic_kpi_definition (
    kpi_code TEXT PRIMARY KEY,
    kpi_name TEXT NOT NULL,
    formula_expression TEXT NOT NULL,
    grain TEXT NOT NULL,
    unit TEXT,
    verification_status TEXT NOT NULL,
    topology_rollup_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ref_semantic_kpi_formula_input (
    kpi_code TEXT NOT NULL REFERENCES ref_semantic_kpi_definition(kpi_code) ON DELETE CASCADE,
    input_alias TEXT NOT NULL,
    dataset_family TEXT NOT NULL,
    counter_alias TEXT NOT NULL,
    required BOOLEAN NOT NULL DEFAULT TRUE,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kpi_code, input_alias, dataset_family, counter_alias)
);

CREATE INDEX IF NOT EXISTS idx_semantic_counter_dictionary_alias
    ON ref_semantic_counter_dictionary (dataset_family, counter_alias);

CREATE INDEX IF NOT EXISTS idx_semantic_kpi_formula_input_alias
    ON ref_semantic_kpi_formula_input (dataset_family, counter_alias);

COMMENT ON TABLE ref_semantic_counter_dictionary IS
    'Curated semantic counter dictionary mapping raw dataset_family+counter_id values to stable semantic aliases.';

COMMENT ON TABLE ref_semantic_counter_group IS
    'Curated semantic counter group dictionary for reusable KPI inputs and reporting bundles.';

COMMENT ON TABLE ref_semantic_counter_group_member IS
    'Membership table linking semantic counter aliases into named counter groups.';

COMMENT ON TABLE ref_semantic_kpi_definition IS
    'Semantic KPI definitions expressed against semantic counter aliases rather than raw counter IDs.';

COMMENT ON TABLE ref_semantic_kpi_formula_input IS
    'Curated mapping of KPI input aliases to semantic counter aliases by dataset family.';

CREATE TABLE IF NOT EXISTS ref_vendor_indicator_dictionary (
    indicator_code TEXT PRIMARY KEY,
    indicator_name TEXT NOT NULL,
    semantic_alias TEXT NOT NULL,
    aggregation_method TEXT NOT NULL,
    unit TEXT,
    verification_status TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ref_vendor_indicator_lineage (
    indicator_code TEXT PRIMARY KEY REFERENCES ref_vendor_indicator_dictionary(indicator_code)
        ON DELETE CASCADE,
    lineage_expression TEXT NOT NULL,
    lineage_type TEXT NOT NULL,
    raw_counter_dependencies TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vendor_indicator_dictionary_alias
    ON ref_vendor_indicator_dictionary (semantic_alias);

COMMENT ON TABLE ref_vendor_indicator_dictionary IS
    'Curated vendor indicator dictionary mapping vendor indicator codes to semantic aliases.';

COMMENT ON TABLE ref_vendor_indicator_lineage IS
    'Curated lineage reference from vendor indicators to raw counter dependency expressions.';
