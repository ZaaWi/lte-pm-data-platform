CREATE TABLE IF NOT EXISTS topology_snapshot (
    snapshot_id BIGSERIAL PRIMARY KEY,
    source_file_name TEXT NOT NULL,
    stored_file_path TEXT,
    source_sha256 TEXT,
    topology_release_date DATE,
    uploaded_at TIMESTAMP NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'previewed',
    is_active_snapshot BOOLEAN NOT NULL DEFAULT FALSE,
    parser_error_count INTEGER NOT NULL DEFAULT 0,
    parser_warning_count INTEGER NOT NULL DEFAULT 0,
    workbook_row_count INTEGER NOT NULL DEFAULT 0,
    normalized_row_count INTEGER NOT NULL DEFAULT 0,
    parser_messages_json JSONB,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS topology_snapshot_entity_map (
    snapshot_entity_map_id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES topology_snapshot(snapshot_id) ON DELETE CASCADE,
    source_row_number INTEGER,
    logical_entity_key TEXT NOT NULL,
    dataset_family TEXT NOT NULL,
    site_code TEXT,
    site_name TEXT,
    region_code TEXT,
    region_name TEXT,
    area_name TEXT,
    cluster_id TEXT,
    team_code TEXT,
    reporting_key TEXT,
    reporting_name TEXT,
    reporting_level TEXT,
    workbook_subnet_id TEXT,
    workbook_enodeb_id TEXT,
    workbook_enodeb_name TEXT,
    workbook_cell_name TEXT,
    mapping_source TEXT,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS topology_reconciliation_result (
    reconciliation_id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES topology_snapshot(snapshot_id) ON DELETE CASCADE,
    compared_active_snapshot_id BIGINT REFERENCES topology_snapshot(snapshot_id),
    run_at TIMESTAMP NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL DEFAULT 'reconciled',
    blocking_error_count INTEGER NOT NULL DEFAULT 0,
    warning_count INTEGER NOT NULL DEFAULT 0,
    pm_missing_from_workbook_count INTEGER NOT NULL DEFAULT 0,
    workbook_missing_from_pm_count INTEGER NOT NULL DEFAULT 0,
    workbook_sites_no_pm_count INTEGER NOT NULL DEFAULT 0,
    duplicate_entity_mapping_count INTEGER NOT NULL DEFAULT 0,
    conflicting_site_region_count INTEGER NOT NULL DEFAULT 0,
    entities_added_count INTEGER NOT NULL DEFAULT 0,
    entities_removed_count INTEGER NOT NULL DEFAULT 0,
    entities_moved_site_count INTEGER NOT NULL DEFAULT 0,
    sites_moved_region_count INTEGER NOT NULL DEFAULT 0,
    parser_error_count INTEGER NOT NULL DEFAULT 0,
    parser_warning_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS topology_reconciliation_detail (
    reconciliation_detail_id BIGSERIAL PRIMARY KEY,
    reconciliation_id BIGINT NOT NULL REFERENCES topology_reconciliation_result(reconciliation_id) ON DELETE CASCADE,
    issue_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    logical_entity_key TEXT,
    dataset_family TEXT,
    site_code TEXT,
    region_code TEXT,
    active_site_code TEXT,
    active_region_code TEXT,
    candidate_site_code TEXT,
    candidate_region_code TEXT,
    detail_json JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS topology_activation_audit (
    activation_audit_id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES topology_snapshot(snapshot_id),
    prior_active_snapshot_id BIGINT REFERENCES topology_snapshot(snapshot_id),
    activated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    rows_loaded_regions INTEGER NOT NULL DEFAULT 0,
    rows_loaded_sites INTEGER NOT NULL DEFAULT 0,
    rows_loaded_reporting INTEGER NOT NULL DEFAULT 0,
    rows_loaded_entity_map INTEGER NOT NULL DEFAULT 0,
    rows_synced INTEGER,
    activated_by TEXT,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_topology_snapshot_uploaded_at
    ON topology_snapshot (uploaded_at DESC);

CREATE INDEX IF NOT EXISTS idx_topology_snapshot_active
    ON topology_snapshot (is_active_snapshot)
    WHERE is_active_snapshot = TRUE;

CREATE INDEX IF NOT EXISTS idx_topology_snapshot_entity_map_snapshot
    ON topology_snapshot_entity_map (snapshot_id);

CREATE INDEX IF NOT EXISTS idx_topology_snapshot_entity_map_key
    ON topology_snapshot_entity_map (snapshot_id, logical_entity_key);

CREATE INDEX IF NOT EXISTS idx_topology_snapshot_entity_map_site
    ON topology_snapshot_entity_map (snapshot_id, site_code);

CREATE INDEX IF NOT EXISTS idx_topology_reconciliation_result_snapshot
    ON topology_reconciliation_result (snapshot_id, run_at DESC);

CREATE INDEX IF NOT EXISTS idx_topology_reconciliation_detail_reconciliation
    ON topology_reconciliation_detail (reconciliation_id, issue_type);

COMMENT ON TABLE topology_snapshot IS
    'Versioned workbook-derived topology snapshot metadata. Each upload becomes a stored candidate snapshot.';

COMMENT ON TABLE topology_snapshot_entity_map IS
    'Normalized workbook-derived topology mappings stored per snapshot before activation.';

COMMENT ON TABLE topology_reconciliation_result IS
    'Summary reconciliation result for a candidate topology snapshot versus PM and the active snapshot.';

COMMENT ON TABLE topology_reconciliation_detail IS
    'Detailed reconciliation rows for one topology snapshot reconciliation run.';

COMMENT ON TABLE topology_activation_audit IS
    'Audit trail for applying a candidate topology snapshot into the live topology reference tables.';
