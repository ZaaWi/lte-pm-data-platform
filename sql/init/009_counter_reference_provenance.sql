ALTER TABLE ref_pm_counter
    ADD COLUMN IF NOT EXISTS source_type TEXT,
    ADD COLUMN IF NOT EXISTS source_reference TEXT,
    ADD COLUMN IF NOT EXISTS verification_status TEXT NOT NULL DEFAULT 'UNKNOWN',
    ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP NOT NULL DEFAULT NOW();

COMMENT ON COLUMN ref_pm_counter.source_type IS
    'Where the counter meaning came from, for example vendor_doc, user_provided, or repo_seed.';

COMMENT ON COLUMN ref_pm_counter.source_reference IS
    'Concrete document or file reference used to justify the counter meaning.';

COMMENT ON COLUMN ref_pm_counter.verification_status IS
    'Expected values are VERIFIED, UNVERIFIED, or UNKNOWN. Only VERIFIED counters should support ACTIVE KPI mappings.';

COMMENT ON COLUMN ref_pm_counter.verified_at IS
    'Timestamp when the counter meaning was verified from an authoritative source.';
