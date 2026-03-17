CREATE TABLE IF NOT EXISTS ftp_cycle_run (
    id BIGSERIAL PRIMARY KEY,
    requested_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'queued',
    trigger_source TEXT NOT NULL,
    parameters_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_message TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ftp_cycle_run_event (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES ftp_cycle_run(id) ON DELETE CASCADE,
    event_time TIMESTAMP NOT NULL DEFAULT NOW(),
    stage TEXT NOT NULL,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    metrics_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_ftp_cycle_run_status_requested
    ON ftp_cycle_run (status, requested_at);

CREATE INDEX IF NOT EXISTS idx_ftp_cycle_run_requested
    ON ftp_cycle_run (requested_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_ftp_cycle_run_event_run_time
    ON ftp_cycle_run_event (run_id, event_time DESC, id DESC);

COMMENT ON TABLE ftp_cycle_run IS
    'Persistent operator-facing execution record for FTP cycle runs.';

COMMENT ON TABLE ftp_cycle_run_event IS
    'Stage-level event log for one FTP cycle run.';
