-- Rollback for migration 137. See that file for rationale.
-- Apply: psql -d personal_koi -v ON_ERROR_STOP=1 -1 -f migrations/137_federation_rid_blocks_down.sql
--
-- This is not a backout. The guard fails closed without the table, and deleting a block lets the next
-- update recreate that record on every peer that removed it. This file refuses while any record is
-- blocked, so it only removes an empty table.

DO $$
BEGIN
    IF to_regclass('federation_rid_blocks') IS NOT NULL
       AND EXISTS (SELECT 1 FROM federation_rid_blocks) THEN
        RAISE EXCEPTION 'federation_rid_blocks still blocks % record(s); refusing to drop it',
            (SELECT count(*) FROM federation_rid_blocks);
    END IF;
END $$;

DROP TABLE IF EXISTS federation_rid_blocks;

DO $$
BEGIN
    IF to_regclass('koi_migrations') IS NOT NULL THEN
        DELETE FROM koi_migrations WHERE migration_id = '137_federation_rid_blocks';
    END IF;
END $$;
