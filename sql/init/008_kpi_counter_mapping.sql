CREATE TABLE IF NOT EXISTS ref_kpi_counter_mapping (
    kpi_name TEXT NOT NULL REFERENCES ref_kpi_definition (kpi_name) ON DELETE CASCADE,
    counter_role TEXT NOT NULL CHECK (counter_role IN ('numerator', 'denominator')),
    counter_id TEXT NOT NULL REFERENCES ref_pm_counter (counter_id),
    weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (kpi_name, counter_role, counter_id)
);

COMMENT ON TABLE ref_kpi_counter_mapping IS
    'Authoritative KPI-to-counter mapping table. KPIs should only be marked ACTIVE when verified mappings exist here.';
