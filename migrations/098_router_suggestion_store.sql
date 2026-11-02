BEGIN;

CREATE TABLE router_suggestion (
    suggestion_id uuid PRIMARY KEY,
    schema_version integer NOT NULL,
    candidate_source_rid text NOT NULL,
    source_triage_state text NOT NULL
        CONSTRAINT router_suggestion_source_triage_state_check
        CHECK (source_triage_state IN ('MAYBE', 'RELEVANT')),
    evidence_trail jsonb NOT NULL
        CONSTRAINT router_suggestion_evidence_trail_shape_check
        CHECK (
            jsonb_typeof(evidence_trail) = 'object'
            AND jsonb_typeof(evidence_trail->'sources') = 'array'
        ),
    boundary_profile jsonb NOT NULL
        CONSTRAINT router_suggestion_boundary_profile_shape_check
        CHECK (
            jsonb_typeof(boundary_profile) = 'object'
            AND boundary_profile ?& ARRAY[
                'source_layer',
                'target_layer',
                'source_workstream',
                'target_canon',
                'source_rights_class',
                'target_rights_class'
            ]
        ),
    gate_result jsonb NOT NULL
        CONSTRAINT router_suggestion_gate_result_shape_check
        CHECK (
            jsonb_typeof(gate_result) = 'object'
            AND gate_result ?& ARRAY[
                'technical_feasibility',
                'semantic_coherence',
                'rights_governance_continuity'
            ]
            AND gate_result->'technical_feasibility' ? 'verdict'
            AND gate_result->'semantic_coherence' ? 'verdict'
            AND gate_result->'rights_governance_continuity' ? 'verdict'
            AND gate_result->'technical_feasibility'->>'verdict' IN ('pass', 'fail', 'uncertain')
            AND gate_result->'semantic_coherence'->>'verdict' IN ('pass', 'fail', 'uncertain')
            AND gate_result->'rights_governance_continuity'->>'verdict' IN ('pass', 'fail', 'uncertain')
        ),
    proposed_route jsonb NOT NULL
        CONSTRAINT router_suggestion_proposed_route_shape_check
        CHECK (
            jsonb_typeof(proposed_route) = 'object'
            AND proposed_route ?& ARRAY['target_canon', 'target_workstream']
        ),
    requires_comparative_intake text
        CONSTRAINT router_suggestion_requires_comparative_intake_check
        CHECK (
            requires_comparative_intake IS NULL
            OR requires_comparative_intake IN ('yes', 'no', 'operator_decides')
        ),
    operator_state text NOT NULL DEFAULT 'pending'
        CONSTRAINT router_suggestion_operator_state_check
        CHECK (operator_state IN ('pending', 'accepted', 'declined', 'deferred', 'superseded')),
    idempotency_key text NOT NULL UNIQUE,
    superseded_by uuid NULL
        CONSTRAINT router_suggestion_superseded_by_fkey
        REFERENCES router_suggestion (suggestion_id),
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT router_suggestion_superseded_by_no_self_check
        CHECK (superseded_by IS NULL OR superseded_by <> suggestion_id)
);

CREATE INDEX router_suggestion_pending_idx
    ON router_suggestion (operator_state)
    WHERE operator_state = 'pending';

CREATE INDEX router_suggestion_candidate_source_rid_idx
    ON router_suggestion (candidate_source_rid);

CREATE INDEX router_suggestion_source_triage_state_idx
    ON router_suggestion (source_triage_state);

CREATE INDEX router_suggestion_superseded_by_idx
    ON router_suggestion (superseded_by);

CREATE OR REPLACE FUNCTION router_suggestion_set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER router_suggestion_set_updated_at_trigger
BEFORE UPDATE ON router_suggestion
FOR EACH ROW
EXECUTE FUNCTION router_suggestion_set_updated_at();

CREATE OR REPLACE FUNCTION router_suggestion_validate_operator_state_transition()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.operator_state = OLD.operator_state THEN
        RETURN NEW;
    END IF;

    IF OLD.operator_state = 'pending'
        AND NEW.operator_state IN ('accepted', 'declined', 'deferred', 'superseded') THEN
        RETURN NEW;
    END IF;

    IF OLD.operator_state = 'deferred'
        AND NEW.operator_state IN ('pending', 'accepted', 'declined', 'superseded') THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'invalid router_suggestion operator_state transition: % -> %',
        OLD.operator_state,
        NEW.operator_state
        USING ERRCODE = '23514';
END;
$$;

CREATE TRIGGER router_suggestion_operator_state_transition_trigger
BEFORE UPDATE OF operator_state ON router_suggestion
FOR EACH ROW
EXECUTE FUNCTION router_suggestion_validate_operator_state_transition();

CREATE OR REPLACE FUNCTION router_suggestion_prevent_superseded_by_cycle()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.superseded_by IS NULL THEN
        RETURN NEW;
    END IF;

    IF NEW.superseded_by = NEW.suggestion_id THEN
        RAISE EXCEPTION 'router_suggestion superseded_by cannot reference self: %',
            NEW.suggestion_id
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        WITH RECURSIVE chain(suggestion_id, superseded_by) AS (
            SELECT rs.suggestion_id, rs.superseded_by
            FROM router_suggestion rs
            WHERE rs.suggestion_id = NEW.superseded_by

            UNION ALL

            SELECT next_rs.suggestion_id, next_rs.superseded_by
            FROM router_suggestion next_rs
            JOIN chain ON next_rs.suggestion_id = chain.superseded_by
            WHERE chain.superseded_by IS NOT NULL
        )
        SELECT 1
        FROM chain
        WHERE superseded_by = NEW.suggestion_id
            OR suggestion_id = NEW.suggestion_id
    ) THEN
        RAISE EXCEPTION 'router_suggestion superseded_by cycle detected for %',
            NEW.suggestion_id
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER router_suggestion_superseded_by_cycle_trigger
AFTER INSERT OR UPDATE OF superseded_by ON router_suggestion
DEFERRABLE INITIALLY IMMEDIATE
FOR EACH ROW
EXECUTE FUNCTION router_suggestion_prevent_superseded_by_cycle();

COMMIT;
