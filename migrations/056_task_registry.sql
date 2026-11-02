-- Migration 056: Task Registry
-- Namespace: personal:056
--
-- Dedicated task storage table, replacing the fragmented /register-entity approach.
-- All task writers (meeting-notes, task-agent, /tasks add) converge here.
-- entity_relationships integration deferred to V2 (would require ghost entity_registry rows).

CREATE TABLE IF NOT EXISTS task_registry (
    id SERIAL PRIMARY KEY,
    task_key TEXT UNIQUE NOT NULL,
    uuid TEXT,
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'inbox'
        CHECK (status IN ('inbox', 'open', 'in-progress', 'waiting', 'done', 'cancelled')),
    priority TEXT DEFAULT 'medium',
    due_date DATE,
    start_date DATE,
    wait_until DATE,
    context TEXT,
    effort TEXT,
    owner_uri TEXT,                     -- resolved from entity_rid_mappings (nullable)
    project_uri TEXT,                   -- resolved from entity_rid_mappings (nullable)
    collaborator_uris TEXT[] DEFAULT '{}',
    blocked_by TEXT[] DEFAULT '{}',     -- task @id values (e.g. "tasks/2026-02-17-slug")
    source_note TEXT,
    source_type TEXT DEFAULT 'meeting',
    vault_path TEXT,
    tags TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    completed_at TIMESTAMP,
    started_at TIMESTAMP,
    triaged_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_task_status ON task_registry(status);
CREATE INDEX IF NOT EXISTS idx_task_due_date ON task_registry(due_date);
CREATE INDEX IF NOT EXISTS idx_task_owner ON task_registry(owner_uri);
CREATE INDEX IF NOT EXISTS idx_task_project ON task_registry(project_uri);
CREATE INDEX IF NOT EXISTS idx_task_source_note ON task_registry(source_note);
