-- Migration 099 DOWN: revert validity_interval primitive
--
-- Pairs with: 099_validity_interval_tasks.sql.
-- Trigger conditions: migration 099 fails to apply,
-- post-migration regression on `due_date`-only entities, Dobby round-trip
-- regression, vault YAML reader regression.
--
-- Data safety:
--   * All ADD COLUMNs in the up-migration defaulted to NULL; no row rewrite.
--   * Down-migration drops only the COLUMNS and INDEXES created by 099.
--   * `commitments.validity_start` / `validity_end` are NOT dropped — those
--     pre-existed (migration 056) and carry production data.
--   * `intent_registry.expires_at` is NOT dropped — pre-existed (migration 074).
--   * `due_date` / `start_date` / `wait_until` on `task_registry` are NOT
--     touched.
--   * Rows that gained non-NULL `validity_start` / `validity_end` /
--     `valid_from` between up and down lose those values on rollback. This
--     is acceptable since rollback only fires when the feature is failing.
--
-- Pre-rollback safety dump (run manually before applying this file):
--   pg_dump personal_koi > /tmp/personal_koi_pre_099_down.sql

BEGIN;

-- task_registry: drop the partial index, then the columns.
DROP INDEX IF EXISTS idx_task_validity_end;

ALTER TABLE task_registry
  DROP COLUMN IF EXISTS validity_start,
  DROP COLUMN IF EXISTS validity_end;

-- commitments: drop ONLY the new partial index. Pre-existing columns
-- (validity_start, validity_end from migration 056) remain.
DROP INDEX IF EXISTS idx_commitments_validity_end;

-- intent_registry: drop the new partial index, then the column.
DROP INDEX IF EXISTS idx_intent_valid_from;

ALTER TABLE intent_registry
  DROP COLUMN IF EXISTS valid_from;

COMMIT;

-- Verification (run manually after rollback):
--   \d task_registry    -- expect NO validity_start, NO validity_end
--   \d commitments      -- expect validity_start, validity_end STILL present
--                          (from migration 056), idx_commitments_validity_end gone
--   \d intent_registry  -- expect NO valid_from; expires_at STILL present
--                          (from migration 074), idx_intent_expires STILL present
--
-- Loss-free check for due_date-only rows (AC1.5):
--   SELECT COUNT(*) FROM task_registry WHERE due_date IS NOT NULL;
--   -- expect: identical count to pre-up-migration baseline
