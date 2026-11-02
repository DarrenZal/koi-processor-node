-- Migration 095: dobby_tasks — live state for Dobby T1 dev capability.
--
-- Per an internal planning document §Operational details,
-- T1 (single-PR dev) needs a dedicated table for live task state
-- (acquire/heartbeat/release/abandoned). This is separate from
-- knowledge_episodes which receives only the TERMINAL WorkItem episode
-- per task.
--
-- Atomic single-active-task-per-repo is enforced via a partial unique
-- index, removing the need for advisory locks.

CREATE TABLE IF NOT EXISTS dobby_tasks (
  task_id        TEXT        PRIMARY KEY,
  repo           TEXT        NOT NULL,
  status         TEXT        NOT NULL CHECK (status IN
                   ('in_progress', 'success', 'failed', 'halted', 'abandoned')),
  started_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
  ended_at       TIMESTAMPTZ,
  metadata       JSONB       NOT NULL DEFAULT '{}'::jsonb
);

-- Atomic single-active-task-per-repo: two simultaneous INSERTs both
-- with status='in_progress' for the same repo: one wins, the other
-- gets UniqueViolationError. No advisory lock needed.
CREATE UNIQUE INDEX IF NOT EXISTS dobby_tasks_one_active_per_repo
  ON dobby_tasks(repo) WHERE status = 'in_progress';

-- Stale-detection support for the periodic + startup recovery scan
-- that marks tasks 'abandoned' when last_heartbeat < now() - 5min.
CREATE INDEX IF NOT EXISTS dobby_tasks_stale_idx
  ON dobby_tasks(last_heartbeat) WHERE status = 'in_progress';

COMMENT ON TABLE dobby_tasks IS
  'Live state for Dobby T1 dev tasks. WorkItem KOI episode emitted to '
  'knowledge_episodes only at terminal status. See '
  'an internal planning document.';
