"""Router v0 suggestion-store API.

Functions accept an existing DB connection so tests can use fakes and runtime
callers can centralize DSN resolution through scripts.router.db_config.

Idempotency keys are computed from canonical JSON for boundary_profile,
gate_result, and proposed_route so repeated runs are stable across jsonb key
ordering differences.
"""

from __future__ import annotations

import os
from typing import Any, Mapping

from psycopg2.extras import Json

from scripts.router.schema import (
    SCHEMA_VERSION,
    TABLE_NAME,
    RouterSchemaError,
    compute_idempotency_key,
    validate_json_shapes,
    validate_operator_state_transition,
    validate_suggestion_payload,
)

JSONB_COLUMNS = ("evidence_trail", "boundary_profile", "gate_result", "proposed_route")


class RouterDisabled(RuntimeError):
    """Raised when ROUTER_DISABLED blocks a write path."""


def ensure_router_enabled() -> None:
    if os.environ.get("ROUTER_DISABLED") == "1":
        raise RouterDisabled("ROUTER_DISABLED=1; exiting per kill-switch")


def build_suggestion_record(
    *,
    suggestion_id: str,
    candidate_source_rid: str,
    source_triage_state: str,
    evidence_trail: Mapping[str, Any],
    boundary_profile: Mapping[str, Any],
    gate_result: Mapping[str, Any],
    proposed_route: Mapping[str, Any],
    requires_comparative_intake: str | None,
    superseded_by: str | None = None,
    operator_state: str = "pending",
    schema_version: int = SCHEMA_VERSION,
) -> dict[str, Any]:
    record = {
        "suggestion_id": suggestion_id,
        "schema_version": schema_version,
        "candidate_source_rid": candidate_source_rid,
        "source_triage_state": source_triage_state,
        "evidence_trail": dict(evidence_trail),
        "boundary_profile": dict(boundary_profile),
        "gate_result": dict(gate_result),
        "proposed_route": dict(proposed_route),
        "requires_comparative_intake": requires_comparative_intake,
        "operator_state": operator_state,
        "idempotency_key": compute_idempotency_key(
            candidate_source_rid=candidate_source_rid,
            boundary_profile=boundary_profile,
            gate_result=gate_result,
            proposed_route=proposed_route,
            schema_version=schema_version,
        ),
        "superseded_by": superseded_by,
    }
    validate_suggestion_payload(record)
    return record


def insert_suggestion(conn, record: Mapping[str, Any]) -> None:
    ensure_router_enabled()
    validate_suggestion_payload(record)
    _reject_known_cycle(conn, record["suggestion_id"], record.get("superseded_by"))

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {TABLE_NAME} (
                suggestion_id,
                schema_version,
                candidate_source_rid,
                source_triage_state,
                evidence_trail,
                boundary_profile,
                gate_result,
                proposed_route,
                requires_comparative_intake,
                operator_state,
                idempotency_key,
                superseded_by
            ) VALUES (
                %(suggestion_id)s,
                %(schema_version)s,
                %(candidate_source_rid)s,
                %(source_triage_state)s,
                %(evidence_trail)s,
                %(boundary_profile)s,
                %(gate_result)s,
                %(proposed_route)s,
                %(requires_comparative_intake)s,
                %(operator_state)s,
                %(idempotency_key)s,
                %(superseded_by)s
            )
            """,
            _adapt_record_for_insert(record),
        )


def update_operator_state(conn, suggestion_id: str, old_state: str, new_state: str) -> None:
    ensure_router_enabled()
    validate_operator_state_transition(old_state, new_state)

    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {TABLE_NAME}
            SET operator_state = %(new_state)s
            WHERE suggestion_id = %(suggestion_id)s
                AND operator_state = %(old_state)s
            """,
            {
                "suggestion_id": suggestion_id,
                "old_state": old_state,
                "new_state": new_state,
            },
        )


def read_pending_suggestions(conn) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                suggestion_id,
                schema_version,
                candidate_source_rid,
                source_triage_state,
                evidence_trail,
                boundary_profile,
                gate_result,
                proposed_route,
                requires_comparative_intake,
                operator_state,
                idempotency_key,
                superseded_by,
                created_at,
                updated_at
            FROM {TABLE_NAME}
            WHERE operator_state = 'pending'
            ORDER BY created_at ASC
            """
        )
        columns = [description[0] for description in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def _reject_known_cycle(conn, suggestion_id: str, superseded_by: str | None) -> None:
    if superseded_by is None:
        return
    if superseded_by == suggestion_id:
        raise RouterSchemaError("superseded_by cannot reference the suggestion itself")

    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH RECURSIVE chain(suggestion_id, superseded_by) AS (
                SELECT suggestion_id, superseded_by
                FROM {TABLE_NAME}
                WHERE suggestion_id = %(superseded_by)s

                UNION ALL

                SELECT rs.suggestion_id, rs.superseded_by
                FROM {TABLE_NAME} rs
                JOIN chain ON rs.suggestion_id = chain.superseded_by
                WHERE chain.superseded_by IS NOT NULL
            )
            SELECT 1
            FROM chain
            WHERE superseded_by = %(suggestion_id)s
                OR suggestion_id = %(suggestion_id)s
            LIMIT 1
            """,
            {"suggestion_id": suggestion_id, "superseded_by": superseded_by},
        )
        if cur.fetchone():
            raise RouterSchemaError("superseded_by cycle detected")


def validate_record_shapes(record: Mapping[str, Any]) -> None:
    validate_json_shapes(
        evidence_trail=record["evidence_trail"],
        boundary_profile=record["boundary_profile"],
        gate_result=record["gate_result"],
        proposed_route=record["proposed_route"],
    )


def _adapt_record_for_insert(record: Mapping[str, Any]) -> dict[str, Any]:
    adapted = dict(record)
    for column in JSONB_COLUMNS:
        adapted[column] = Json(record[column])
    return adapted
