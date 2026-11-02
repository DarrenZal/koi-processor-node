-- Migration 099: validity_interval primitive on prospective-entity tables
--
-- Phase 1 of the EverMemOS-adoption roadmap (an internal planning document).
-- Execution plan: an internal planning document.
-- Inventory (AC1.0): an internal planning document.
--
-- Concept-level reuse from EverMemOS v1.1.0 (commit 77c416c9, Apache-2.0,
-- EverMind-AI/EverOS): the `[t_start, t_end]` Foresight `P` framing is realized
-- here as paired TIMESTAMPTZ columns. No EverMemOS prose / prompt / schema
-- language is reproduced; this is a column shape and a partial index. Cite the
-- bridge note `ic.connection.evermemos-memory-lifecycle` in any prose artifact;
-- no Apache-2.0 NOTICE propagation in source files (per Phase 0 attribution
-- discipline, ~50% threshold).
--
-- Semantic policy (governs retrieval; see plan §Approach):
--   * `due_date` (existing) keeps DEADLINE semantics — overdue tasks stay
--     visible. UNCHANGED by this migration.
--   * `validity_start` / `validity_end` are a NEW opt-in `[t_start, t_end]`
--     window — "true between bounds, expired beyond." Default retrieval
--     filters rows out only when callers pass `t_now` explicitly.
--   * Rows with NULL bounds are always visible regardless.
--
-- Backward compatibility:
--   * All ADD COLUMNs default NULL — no row rewrite, no table-rewriting ALTER.
--   * Existing callers that don't pass `t_now` see the IDENTICAL row set they
--     saw before this Phase. Filter is opt-in at query time, not at column time.
--   * `commitments` already has `validity_start` / `validity_end` (migration
--     056); only the new partial index is added here. Columns are NOT touched.
--
-- Column-naming asymmetry (intentional; see plan Constraints §"Column-naming
-- asymmetry"):
--   * task_registry      : validity_start  / validity_end   (TIMESTAMPTZ)
--   * commitments        : validity_start  / validity_end   (TIMESTAMPTZ, pre-existing)
--   * intent_registry    : valid_from      / expires_at     (DATE; expires_at pre-existing)
-- The Step-4 retrieval API uses one centralized per-router helper to apply the
-- WHERE-clause filter; the helper takes (start_col, end_col, value_cast) so
-- the three call sites share one implementation. Don't refactor the column
-- names to be uniform — `expires_at` is already on the intent wire surface.
--
-- Timestamp-shape note (intentional; see plan Constraints §"Timestamp shape on
-- task_registry validity columns"): task_registry's pre-existing timestamp
-- columns are `timestamp without time zone`. The new validity_* columns here
-- are TIMESTAMPTZ deliberately — matching commitments and the absolute-
-- moment-in-time semantic. Mixed timestamp shapes on task_registry is
-- accepted.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS, CREATE INDEX IF NOT EXISTS.
-- Reversible: paired down-migration at 099_validity_interval_tasks_down.sql.
-- No data loss for `due_date`-only rows on rollback (verified by AC1.5).

BEGIN;

-- task_registry: add paired interval columns + partial index on validity_end.
-- Partial index is sparse (most rows will have NULL validity_end); B-tree on
-- the non-null subset gives cheap freshness gating once retrieval honors it.
ALTER TABLE task_registry
  ADD COLUMN IF NOT EXISTS validity_start TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS validity_end   TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_task_validity_end
  ON task_registry(validity_end)
  WHERE validity_end IS NOT NULL;

-- commitments: columns already exist (migration 056). Add the missing partial
-- index on validity_end so the new opt-in retrieval filter doesn't seq-scan.
CREATE INDEX IF NOT EXISTS idx_commitments_validity_end
  ON commitments(validity_end)
  WHERE validity_end IS NOT NULL;

-- intent_registry: pair the existing `expires_at DATE` (migration 074) with a
-- new `valid_from DATE` so the full interval is representable. NULL valid_from
-- is treated as -∞ at retrieval time (symmetric with NULL expires_at = +∞).
ALTER TABLE intent_registry
  ADD COLUMN IF NOT EXISTS valid_from DATE;

-- Optional partial index on valid_from to mirror the expires_at index that
-- migration 074 already creates (`idx_intent_expires`). Keeps the symmetric
-- predicate cheap once retrieval honors both bounds.
CREATE INDEX IF NOT EXISTS idx_intent_valid_from
  ON intent_registry(valid_from)
  WHERE valid_from IS NOT NULL;

COMMIT;

-- Verification (run manually after apply):
--   \d task_registry    -- expect validity_start, validity_end columns
--   \d commitments      -- expect idx_commitments_validity_end (existing cols)
--   \d intent_registry  -- expect valid_from column + idx_intent_valid_from
--
-- Behavioral verification:
--   SELECT COUNT(*) FROM task_registry;  -- unchanged from pre-migration count
--   SELECT COUNT(*) FROM task_registry WHERE due_date IS NOT NULL;
--                                       -- unchanged: due_date untouched
