CREATE TABLE IF NOT EXISTS file_audit (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    file_hash TEXT,
    run_id UUID NOT NULL,
    trigger_type TEXT NOT NULL,
    source_type TEXT NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL,
    csv_files_found INTEGER NOT NULL DEFAULT 0,
    input_rows_read INTEGER NOT NULL DEFAULT 0,
    normalized_rows_emitted INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    unknown_columns TEXT[] NOT NULL DEFAULT '{}',
    null_counter_values INTEGER NOT NULL DEFAULT 0,
    lifecycle_status TEXT NOT NULL DEFAULT 'PENDING',
    lifecycle_action TEXT,
    normalization_status TEXT NOT NULL DEFAULT 'PENDING',
    normalized_at TIMESTAMP,
    normalization_error TEXT,
    final_file_path TEXT,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE file_audit
    ADD COLUMN IF NOT EXISTS lifecycle_status TEXT NOT NULL DEFAULT 'PENDING',
    ADD COLUMN IF NOT EXISTS normalization_status TEXT NOT NULL DEFAULT 'PENDING',
    ADD COLUMN IF NOT EXISTS normalized_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS normalization_error TEXT;

CREATE INDEX IF NOT EXISTS idx_file_audit_source_file
    ON file_audit (source_file);

CREATE INDEX IF NOT EXISTS idx_file_audit_hash_status
    ON file_audit (file_hash, status);

CREATE UNIQUE INDEX IF NOT EXISTS uq_file_audit_successful_hash
    ON file_audit (file_hash)
    WHERE status = 'SUCCESS' AND file_hash IS NOT NULL;
