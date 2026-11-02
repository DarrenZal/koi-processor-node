-- Migration 051: Bi-temporal assertion history for commons correctness
-- ADR-001: PostgreSQL-first graph database strategy
--
-- Tracks all assertions with two temporal dimensions:
--   Transaction time: when the DB recorded the assertion (system-managed, immutable)
--   Valid time: when the fact was/is true in the real world (user-managed)
--
-- Invariant: No destructive overwrite. Retraction sets tx_retracted_at;
-- correction creates new assertion with supersedes_assertion_id.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS assertion_history (
    assertion_id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    subject                  TEXT NOT NULL,        -- entity URI
    predicate                TEXT NOT NULL,        -- relationship type
    object_uri               TEXT,                 -- entity URI (NULL if literal)
    object_literal           TEXT,                 -- literal value (NULL if URI)
    object_datatype          TEXT,                 -- XSD datatype for literal
    object_lang              TEXT,                 -- language tag for literal
    asserted_by_node_rid     TEXT NOT NULL,        -- peer node RID

    -- Transaction time (system-managed, immutable once written)
    tx_recorded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tx_retracted_at          TIMESTAMPTZ,          -- NULL = active

    -- Valid time (user/domain-managed)
    valid_from               TIMESTAMPTZ,
    valid_to                 TIMESTAMPTZ,          -- NULL = still valid

    -- Provenance
    supersedes_assertion_id  UUID REFERENCES assertion_history(assertion_id),
    provenance_doc_rid       TEXT,                 -- source document RID

    -- Cross-peer replay idempotency
    source_event_id          UUID,                 -- originating KOI-net event
    source_node_rid          TEXT,                 -- node that originated the event

    -- Object type: exactly one of object_uri, object_literal must be NOT NULL
    CONSTRAINT chk_object_exactly_one CHECK (
        (object_uri IS NOT NULL AND object_literal IS NULL)
        OR (object_uri IS NULL AND object_literal IS NOT NULL)
    ),

    -- Literal metadata only valid when object_literal is set
    CONSTRAINT chk_datatype_requires_literal CHECK (
        object_datatype IS NULL OR object_literal IS NOT NULL
    ),
    CONSTRAINT chk_lang_requires_literal CHECK (
        object_lang IS NULL OR object_literal IS NOT NULL
    ),

    -- Temporal consistency
    CONSTRAINT chk_tx_retracted_after_recorded CHECK (
        tx_retracted_at IS NULL OR tx_retracted_at >= tx_recorded_at
    ),
    CONSTRAINT chk_valid_to_after_from CHECK (
        valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from
    )
);

-- Replay idempotency: same event from same node cannot create duplicate assertions
CREATE UNIQUE INDEX IF NOT EXISTS idx_assertion_replay_dedup
    ON assertion_history (source_node_rid, source_event_id)
    WHERE source_node_rid IS NOT NULL AND source_event_id IS NOT NULL;

-- Active assertion dedup: same triple + node cannot have duplicate active assertions
CREATE UNIQUE INDEX IF NOT EXISTS idx_assertion_active_dedup
    ON assertion_history (subject, predicate, COALESCE(object_uri, object_literal), asserted_by_node_rid)
    WHERE tx_retracted_at IS NULL;

-- Query indexes
CREATE INDEX IF NOT EXISTS idx_assertion_subject ON assertion_history (subject);
CREATE INDEX IF NOT EXISTS idx_assertion_predicate ON assertion_history (predicate);
CREATE INDEX IF NOT EXISTS idx_assertion_object_uri ON assertion_history (object_uri) WHERE object_uri IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_assertion_node_rid ON assertion_history (asserted_by_node_rid);
CREATE INDEX IF NOT EXISTS idx_assertion_active ON assertion_history (tx_retracted_at) WHERE tx_retracted_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_assertion_provenance ON assertion_history (provenance_doc_rid) WHERE provenance_doc_rid IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_assertion_supersedes ON assertion_history (supersedes_assertion_id) WHERE supersedes_assertion_id IS NOT NULL;

-- =============================================================================
-- Immutability triggers
-- =============================================================================

-- tx_recorded_at is immutable: reject any UPDATE that changes it
CREATE OR REPLACE FUNCTION assertion_immutable_tx_recorded()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.tx_recorded_at IS DISTINCT FROM OLD.tx_recorded_at THEN
        RAISE EXCEPTION 'tx_recorded_at is immutable after INSERT';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_assertion_immutable_tx_recorded ON assertion_history;
CREATE TRIGGER trg_assertion_immutable_tx_recorded
    BEFORE UPDATE ON assertion_history
    FOR EACH ROW
    EXECUTE FUNCTION assertion_immutable_tx_recorded();

-- tx_retracted_at is write-once: NULL -> timestamp OK, but cannot change once set
CREATE OR REPLACE FUNCTION assertion_write_once_tx_retracted()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.tx_retracted_at IS NOT NULL AND NEW.tx_retracted_at IS DISTINCT FROM OLD.tx_retracted_at THEN
        RAISE EXCEPTION 'tx_retracted_at is write-once: cannot change after being set';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_assertion_write_once_tx_retracted ON assertion_history;
CREATE TRIGGER trg_assertion_write_once_tx_retracted
    BEFORE UPDATE ON assertion_history
    FOR EACH ROW
    EXECUTE FUNCTION assertion_write_once_tx_retracted();
