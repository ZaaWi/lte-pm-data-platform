CREATE TABLE IF NOT EXISTS ftp_remote_file (
    id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL,
    remote_directory TEXT NOT NULL,
    remote_filename TEXT NOT NULL,
    remote_path TEXT NOT NULL,
    dataset_family TEXT,
    interval_start TIMESTAMP,
    revision INTEGER,
    extension TEXT,
    remote_size_bytes BIGINT,
    remote_modified_at TIMESTAMP,
    status TEXT NOT NULL,
    first_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_scan_at TIMESTAMP NOT NULL DEFAULT NOW(),
    status_updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    download_attempts INTEGER NOT NULL DEFAULT 0,
    download_attempt_count INTEGER NOT NULL DEFAULT 0,
    ingest_attempt_count INTEGER NOT NULL DEFAULT 0,
    last_download_attempt_at TIMESTAMP,
    last_ingest_attempt_at TIMESTAMP,
    local_staged_path TEXT,
    file_hash TEXT,
    ingest_run_id UUID,
    final_file_path TEXT,
    last_error TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_ftp_remote_file_source_path UNIQUE (source_name, remote_path)
);

ALTER TABLE ftp_remote_file
    ADD COLUMN IF NOT EXISTS remote_size_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS remote_modified_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_scan_at TIMESTAMP NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS download_attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS ingest_attempt_count INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_download_attempt_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_ingest_attempt_at TIMESTAMP;

CREATE INDEX IF NOT EXISTS idx_ftp_remote_file_status
    ON ftp_remote_file (status);

CREATE INDEX IF NOT EXISTS idx_ftp_remote_file_interval
    ON ftp_remote_file (dataset_family, interval_start);

CREATE INDEX IF NOT EXISTS idx_ftp_remote_file_last_seen
    ON ftp_remote_file (last_seen_at DESC);
