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

CREATE TABLE IF NOT EXISTS ref_topology_region (
    region_code TEXT PRIMARY KEY,
    region_name TEXT NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ref_topology_site (
    site_code TEXT PRIMARY KEY,
    site_name TEXT,
    region_code TEXT REFERENCES ref_topology_region(region_code),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ref_topology_reporting_hierarchy (
    reporting_key TEXT PRIMARY KEY,
    reporting_name TEXT NOT NULL,
    reporting_level TEXT,
    parent_reporting_key TEXT REFERENCES ref_topology_reporting_hierarchy(reporting_key),
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ref_topology_entity_site_map (
    logical_entity_key TEXT PRIMARY KEY REFERENCES ref_lte_entity_identity(logical_entity_key),
    site_code TEXT REFERENCES ref_topology_site(site_code),
    reporting_key TEXT REFERENCES ref_topology_reporting_hierarchy(reporting_key),
    mapping_source TEXT,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ref_lte_entity_topology_enrichment (
    logical_entity_key TEXT PRIMARY KEY REFERENCES ref_lte_entity_identity(logical_entity_key),
    dataset_family TEXT NOT NULL,
    entity_level TEXT,
    site_code TEXT,
    site_name TEXT,
    region_code TEXT,
    region_name TEXT,
    reporting_key TEXT,
    reporting_name TEXT,
    reporting_level TEXT,
    mapping_status TEXT NOT NULL,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ref_topology_site_region
    ON ref_topology_site (region_code);

CREATE INDEX IF NOT EXISTS idx_ref_topology_entity_site_map_site
    ON ref_topology_entity_site_map (site_code);

CREATE INDEX IF NOT EXISTS idx_ref_topology_entity_site_map_reporting
    ON ref_topology_entity_site_map (reporting_key);

CREATE INDEX IF NOT EXISTS idx_ref_lte_entity_topology_enrichment_site
    ON ref_lte_entity_topology_enrichment (site_code);

CREATE INDEX IF NOT EXISTS idx_ref_lte_entity_topology_enrichment_region
    ON ref_lte_entity_topology_enrichment (region_code);

COMMENT ON TABLE ref_topology_region IS
    'Curated region dimension for topology enrichment.';

COMMENT ON TABLE ref_topology_site IS
    'Curated site dimension for topology enrichment.';

COMMENT ON TABLE ref_topology_reporting_hierarchy IS
    'Optional curated reporting hierarchy dimension for topology enrichment.';

COMMENT ON TABLE ref_topology_entity_site_map IS
    'Curated mapping from logical_entity_key to site and optional reporting hierarchy.';

COMMENT ON TABLE ref_lte_entity_topology_enrichment IS
    'Materialized topology enrichment projection layered on top of logical entity identity.';
