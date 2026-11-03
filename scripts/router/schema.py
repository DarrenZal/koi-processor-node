"""Router suggestion schema constants and deterministic validation helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

TABLE_NAME = "router_suggestion"
SCHEMA_VERSION = 1

SOURCE_TRIAGE_STATES = frozenset({"MAYBE", "RELEVANT"})
REQUIRES_COMPARATIVE_INTAKE_VALUES = frozenset({"yes", "no", "operator_decides"})
OPERATOR_STATES = frozenset({"pending", "accepted", "declined", "deferred", "superseded"})
GATE_VERDICTS = frozenset({"pass", "fail", "uncertain"})

EVIDENCE_TRAIL_REQUIRED_KEYS = frozenset({"sources"})
BOUNDARY_PROFILE_REQUIRED_KEYS = frozenset(
    {
        "source_layer",
        "target_layer",
        "source_workstream",
        "target_canon",
        "source_rights_class",
        "target_rights_class",
    }
)
GATE_RESULT_REQUIRED_KEYS = frozenset(
    {
        "technical_feasibility",
        "semantic_coherence",
        "rights_governance_continuity",
    }
)
PROPOSED_ROUTE_REQUIRED_KEYS = frozenset({"target_canon", "target_workstream"})

VALID_OPERATOR_STATE_TRANSITIONS = {
    "pending": frozenset({"accepted", "declined", "deferred", "superseded"}),
    "deferred": frozenset({"pending", "accepted", "declined", "superseded"}),
    "accepted": frozenset(),
    "declined": frozenset(),
    "superseded": frozenset(),
}


class RouterSchemaError(ValueError):
    """Raised when a suggestion payload does not satisfy router v0 schema."""


def canonical_json(value: Mapping[str, Any]) -> str:
    """Return canonical JSON for deterministic hashes and idempotency keys."""

    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_idempotency_key(
    *,
    candidate_source_rid: str,
    boundary_profile: Mapping[str, Any],
    gate_result: Mapping[str, Any],
    proposed_route: Mapping[str, Any],
    schema_version: int = SCHEMA_VERSION,
) -> str:
    payload = {
        "candidate_source_rid": candidate_source_rid,
        "boundary_profile": boundary_profile,
        "gate_result": gate_result,
        "proposed_route": proposed_route,
        "schema_version": schema_version,
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def validate_json_shapes(
    *,
    evidence_trail: Mapping[str, Any],
    boundary_profile: Mapping[str, Any],
    gate_result: Mapping[str, Any],
    proposed_route: Mapping[str, Any],
) -> None:
    _require_keys("evidence_trail", evidence_trail, EVIDENCE_TRAIL_REQUIRED_KEYS)
    sources = evidence_trail.get("sources")
    if not isinstance(sources, list):
        raise RouterSchemaError("evidence_trail.sources must be an array")

    _require_keys("boundary_profile", boundary_profile, BOUNDARY_PROFILE_REQUIRED_KEYS)
    _require_keys("gate_result", gate_result, GATE_RESULT_REQUIRED_KEYS)
    for key in GATE_RESULT_REQUIRED_KEYS:
        result = gate_result.get(key)
        if not isinstance(result, Mapping):
            raise RouterSchemaError(f"gate_result.{key} must be an object")
        verdict = result.get("verdict")
        if verdict not in GATE_VERDICTS:
            raise RouterSchemaError(
                f"gate_result.{key}.verdict must be one of {sorted(GATE_VERDICTS)}"
            )

    _require_keys("proposed_route", proposed_route, PROPOSED_ROUTE_REQUIRED_KEYS)


def validate_operator_state_transition(old_state: str, new_state: str) -> None:
    if old_state not in OPERATOR_STATES:
        raise RouterSchemaError(f"unknown old operator_state: {old_state}")
    if new_state not in OPERATOR_STATES:
        raise RouterSchemaError(f"unknown new operator_state: {new_state}")
    if old_state == new_state:
        return
    if new_state not in VALID_OPERATOR_STATE_TRANSITIONS[old_state]:
        raise RouterSchemaError(f"invalid operator_state transition: {old_state} -> {new_state}")


def validate_suggestion_payload(payload: Mapping[str, Any]) -> None:
    if payload.get("source_triage_state") not in SOURCE_TRIAGE_STATES:
        raise RouterSchemaError("source_triage_state must be MAYBE or RELEVANT")

    requires_ci = payload.get("requires_comparative_intake")
    if requires_ci is not None and requires_ci not in REQUIRES_COMPARATIVE_INTAKE_VALUES:
        raise RouterSchemaError(
            "requires_comparative_intake must be yes, no, operator_decides, or null"
        )

    operator_state = payload.get("operator_state", "pending")
    if operator_state not in OPERATOR_STATES:
        raise RouterSchemaError("operator_state is not valid")

    validate_json_shapes(
        evidence_trail=payload["evidence_trail"],
        boundary_profile=payload["boundary_profile"],
        gate_result=payload["gate_result"],
        proposed_route=payload["proposed_route"],
    )


def _require_keys(name: str, value: Mapping[str, Any], required: frozenset[str]) -> None:
    missing = sorted(required.difference(value.keys()))
    if missing:
        raise RouterSchemaError(f"{name} missing required keys: {', '.join(missing)}")
