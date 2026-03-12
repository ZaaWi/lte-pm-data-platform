CREATE TABLE IF NOT EXISTS ref_pm_counter (
    counter_id TEXT PRIMARY KEY,
    vendor TEXT NOT NULL,
    technology TEXT NOT NULL,
    object_type TEXT,
    description TEXT,
    unit TEXT,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE ref_pm_counter IS
    'Reference foundation for PM counters. Meanings may remain null until vendor documentation is available.';

CREATE TABLE IF NOT EXISTS ref_lte_entity (
    ani TEXT PRIMARY KEY,
    logical_entity_key TEXT,
    site_code TEXT,
    region_name TEXT,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE ref_lte_entity IS
    'Lightweight ANI mapping foundation. Site and region values are placeholders until source mapping is available.';

CREATE TABLE IF NOT EXISTS ref_lte_entity_identity (
    logical_entity_key TEXT PRIMARY KEY,
    dataset_family TEXT NOT NULL,
    sbnid TEXT,
    enbid TEXT,
    enodebid TEXT,
    cellid TEXT,
    meid TEXT,
    ani TEXT,
    entity_level TEXT,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ref_lte_entity_identity_family
    ON ref_lte_entity_identity (dataset_family);

COMMENT ON TABLE ref_lte_entity_identity IS
    'Normalization foundation for real LTE raw entities, keyed by a deterministic platform logical_entity_key derived from raw dimensions.';
