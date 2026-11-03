BEGIN;

DROP TRIGGER IF EXISTS router_suggestion_superseded_by_cycle_trigger ON router_suggestion;
DROP TRIGGER IF EXISTS router_suggestion_operator_state_transition_trigger ON router_suggestion;
DROP TRIGGER IF EXISTS router_suggestion_set_updated_at_trigger ON router_suggestion;

DROP TABLE IF EXISTS router_suggestion;

DROP FUNCTION IF EXISTS router_suggestion_prevent_superseded_by_cycle();
DROP FUNCTION IF EXISTS router_suggestion_validate_operator_state_transition();
DROP FUNCTION IF EXISTS router_suggestion_set_updated_at();

COMMIT;
