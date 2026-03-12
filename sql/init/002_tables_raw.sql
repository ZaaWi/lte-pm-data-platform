CREATE TABLE IF NOT EXISTS pm_ltefdd_sample (
    id BIGSERIAL PRIMARY KEY,
    source_file TEXT NOT NULL,
    dataset_family TEXT,
    interval_start TIMESTAMP,
    revision INTEGER,
    csv_name TEXT NOT NULL,
    collect_time TIMESTAMP NOT NULL,
    trncmeid TEXT,
    ani TEXT,
    sbnid TEXT,
    enbid TEXT,
    enodebid TEXT,
    cellid TEXT,
    meid TEXT,
    systemmode TEXT,
    midflag TEXT,
    netype TEXT,
    counter_id TEXT NOT NULL,
    counter_value DOUBLE PRECISION,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE pm_ltefdd_sample IS
    'Current raw exploded fact table for ZTE LTE PM counters. One row per input counter cell.';

ALTER TABLE pm_ltefdd_sample
    ADD COLUMN IF NOT EXISTS dataset_family TEXT,
    ADD COLUMN IF NOT EXISTS interval_start TIMESTAMP,
    ADD COLUMN IF NOT EXISTS revision INTEGER,
    ADD COLUMN IF NOT EXISTS sbnid TEXT,
    ADD COLUMN IF NOT EXISTS enbid TEXT,
    ADD COLUMN IF NOT EXISTS enodebid TEXT,
    ADD COLUMN IF NOT EXISTS cellid TEXT,
    ADD COLUMN IF NOT EXISTS meid TEXT;

CREATE INDEX IF NOT EXISTS idx_pm_ltefdd_sample_collect_time
    ON pm_ltefdd_sample (collect_time);

CREATE INDEX IF NOT EXISTS idx_pm_ltefdd_sample_ani
    ON pm_ltefdd_sample (ani);

CREATE INDEX IF NOT EXISTS idx_pm_ltefdd_sample_counter_id
    ON pm_ltefdd_sample (counter_id);

CREATE INDEX IF NOT EXISTS idx_pm_ltefdd_sample_source_file
    ON pm_ltefdd_sample (source_file);

CREATE INDEX IF NOT EXISTS idx_pm_ltefdd_sample_dataset_family
    ON pm_ltefdd_sample (dataset_family);

CREATE INDEX IF NOT EXISTS idx_pm_ltefdd_sample_interval_start
    ON pm_ltefdd_sample (interval_start);

CREATE INDEX IF NOT EXISTS idx_pm_ltefdd_sample_cellid
    ON pm_ltefdd_sample (cellid);
