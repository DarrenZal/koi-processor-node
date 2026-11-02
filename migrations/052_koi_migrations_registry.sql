CREATE TABLE IF NOT EXISTS koi_migrations (
    migration_id TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ DEFAULT NOW(),
    checksum TEXT NOT NULL
);
