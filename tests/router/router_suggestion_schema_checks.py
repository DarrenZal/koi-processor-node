from __future__ import annotations

import os
from pathlib import Path

import pytest
from psycopg2.extras import Json

from scripts.router.db_config import RouterConfigError, ensure_safe_dsn, resolve_dsn
from scripts.router.schema import (
    RouterSchemaError,
    compute_idempotency_key,
    validate_json_shapes,
    validate_operator_state_transition,
)
from scripts.router.suggestion_store import (
    RouterDisabled,
    build_suggestion_record,
    insert_suggestion,
    update_operator_state,
)

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "migrations" / "098_router_suggestion_store.sql"
ROLLBACK = ROOT / "migrations" / "098_router_suggestion_store.rollback.sql"


def valid_parts():
    return {
        "evidence_trail": {
            "source": "fixture",
            "sources": [{"candidate_source_rid": "rid:test:1"}],
        },
        "boundary_profile": {
            "source_layer": "triaged",
            "target_layer": "intake-candidate",
            "source_workstream": "spore",
            "target_canon": "spore",
            "source_rights_class": "private",
            "target_rights_class": "canon-review",
        },
        "gate_result": {
            "technical_feasibility": {"verdict": "pass"},
            "semantic_coherence": {"verdict": "uncertain"},
            "rights_governance_continuity": {"verdict": "pass"},
        },
        "proposed_route": {
            "target_canon": "spore",
            "target_workstream": "spore",
        },
    }


class FakeCursor:
    description = [("suggestion_id",)]

    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return self.rows


class FakeConn:
    def __init__(self, rows=None):
        self.cursor_obj = FakeCursor(rows)

    def cursor(self):
        return self.cursor_obj


def test_migration_contains_required_ddl_and_rollback_objects():
    ddl = MIGRATION.read_text(encoding="utf-8")
    rollback = ROLLBACK.read_text(encoding="utf-8")

    assert ddl.strip().startswith("BEGIN;")
    assert ddl.strip().endswith("COMMIT;")
    assert "CREATE TABLE router_suggestion" in ddl
    for column in [
        "suggestion_id uuid PRIMARY KEY",
        "schema_version integer NOT NULL",
        "candidate_source_rid text NOT NULL",
        "source_triage_state text NOT NULL",
        "operator_state text NOT NULL DEFAULT 'pending'",
        "idempotency_key text NOT NULL UNIQUE",
        "superseded_by uuid NULL",
        "updated_at timestamptz NOT NULL DEFAULT NOW()",
    ]:
        assert column in ddl
    assert "router_suggestion_set_updated_at_trigger" in ddl
    assert "router_suggestion_operator_state_transition_trigger" in ddl
    assert "router_suggestion_superseded_by_cycle_trigger" in ddl
    assert "WITH RECURSIVE chain" in ddl
    assert "router_suggestion_pending_idx" in ddl
    assert "DROP TABLE IF EXISTS router_suggestion" in rollback
    assert "DROP FUNCTION IF EXISTS router_suggestion_set_updated_at()" in rollback
    assert rollback.strip().startswith("BEGIN;")
    assert rollback.strip().endswith("COMMIT;")


def test_idempotency_key_is_deterministic_under_json_key_reordering():
    boundary_one = {
        "target_canon": "spore",
        "source_workstream": "spore",
        "target_layer": "intake-candidate",
        "source_rights_class": "private",
        "target_rights_class": "canon-review",
        "source_layer": "triaged",
    }
    boundary_two = dict(reversed(list(boundary_one.items())))
    parts = valid_parts()

    assert (
        compute_idempotency_key(
            candidate_source_rid="rid:test:1",
            boundary_profile=boundary_one,
            gate_result=parts["gate_result"],
            proposed_route=parts["proposed_route"],
        )
        == compute_idempotency_key(
            candidate_source_rid="rid:test:1",
            boundary_profile=boundary_two,
            gate_result=dict(reversed(list(parts["gate_result"].items()))),
            proposed_route=dict(reversed(list(parts["proposed_route"].items()))),
        )
    )


def test_idempotency_key_changes_when_gate_or_route_changes():
    parts = valid_parts()
    baseline = compute_idempotency_key(
        candidate_source_rid="rid:test:1",
        boundary_profile=parts["boundary_profile"],
        gate_result=parts["gate_result"],
        proposed_route=parts["proposed_route"],
    )

    changed_gate = dict(parts["gate_result"])
    changed_gate["semantic_coherence"] = {"verdict": "pass"}
    assert baseline != compute_idempotency_key(
        candidate_source_rid="rid:test:1",
        boundary_profile=parts["boundary_profile"],
        gate_result=changed_gate,
        proposed_route=parts["proposed_route"],
    )

    changed_route = dict(parts["proposed_route"])
    changed_route["target_canon"] = "intelligence-commons"
    assert baseline != compute_idempotency_key(
        candidate_source_rid="rid:test:1",
        boundary_profile=parts["boundary_profile"],
        gate_result=parts["gate_result"],
        proposed_route=changed_route,
    )


def test_json_shape_validation_rejects_missing_required_keys():
    parts = valid_parts()
    validate_json_shapes(**parts)

    bad_boundary = dict(parts["boundary_profile"])
    bad_boundary.pop("target_rights_class")
    with pytest.raises(RouterSchemaError, match="boundary_profile missing"):
        validate_json_shapes(
            evidence_trail=parts["evidence_trail"],
            boundary_profile=bad_boundary,
            gate_result=parts["gate_result"],
            proposed_route=parts["proposed_route"],
        )

    bad_gate = dict(parts["gate_result"])
    bad_gate["technical_feasibility"] = {"verdict": "maybe"}
    with pytest.raises(RouterSchemaError, match="technical_feasibility.verdict"):
        validate_json_shapes(
            evidence_trail=parts["evidence_trail"],
            boundary_profile=parts["boundary_profile"],
            gate_result=bad_gate,
            proposed_route=parts["proposed_route"],
        )


@pytest.mark.parametrize(
    ("old_state", "new_state"),
    [
        ("pending", "accepted"),
        ("pending", "declined"),
        ("pending", "deferred"),
        ("deferred", "pending"),
        ("deferred", "superseded"),
    ],
)
def test_valid_operator_state_transitions(old_state, new_state):
    validate_operator_state_transition(old_state, new_state)


@pytest.mark.parametrize(
    ("old_state", "new_state"),
    [
        ("declined", "accepted"),
        ("superseded", "pending"),
        ("accepted", "declined"),
    ],
)
def test_invalid_operator_state_transitions(old_state, new_state):
    with pytest.raises(RouterSchemaError, match="invalid operator_state transition"):
        validate_operator_state_transition(old_state, new_state)


def test_router_disabled_blocks_insert_and_update_before_sql(monkeypatch):
    monkeypatch.setenv("ROUTER_DISABLED", "1")
    conn = FakeConn()
    record = build_suggestion_record(
        suggestion_id="00000000-0000-0000-0000-000000000001",
        candidate_source_rid="rid:test:1",
        source_triage_state="MAYBE",
        requires_comparative_intake="operator_decides",
        **valid_parts(),
    )

    with pytest.raises(RouterDisabled, match="ROUTER_DISABLED=1"):
        insert_suggestion(conn, record)
    with pytest.raises(RouterDisabled, match="ROUTER_DISABLED=1"):
        update_operator_state(conn, record["suggestion_id"], "pending", "accepted")
    assert conn.cursor_obj.calls == []


def test_insert_rejects_self_supersession_without_sql(monkeypatch):
    monkeypatch.delenv("ROUTER_DISABLED", raising=False)
    conn = FakeConn()
    record = build_suggestion_record(
        suggestion_id="00000000-0000-0000-0000-000000000001",
        candidate_source_rid="rid:test:1",
        source_triage_state="MAYBE",
        requires_comparative_intake="operator_decides",
        superseded_by="00000000-0000-0000-0000-000000000001",
        **valid_parts(),
    )

    with pytest.raises(RouterSchemaError, match="superseded_by cannot reference"):
        insert_suggestion(conn, record)
    assert conn.cursor_obj.calls == []


def test_insert_rejects_detected_supersession_cycle(monkeypatch):
    monkeypatch.delenv("ROUTER_DISABLED", raising=False)
    conn = FakeConn(rows=[(1,)])
    record = build_suggestion_record(
        suggestion_id="00000000-0000-0000-0000-000000000003",
        candidate_source_rid="rid:test:3",
        source_triage_state="MAYBE",
        requires_comparative_intake="operator_decides",
        superseded_by="00000000-0000-0000-0000-000000000001",
        **valid_parts(),
    )

    with pytest.raises(RouterSchemaError, match="cycle detected"):
        insert_suggestion(conn, record)
    assert len(conn.cursor_obj.calls) == 1
    assert "WITH RECURSIVE chain" in conn.cursor_obj.calls[0][0]


def test_insert_wraps_jsonb_columns_for_psycopg2(monkeypatch):
    monkeypatch.delenv("ROUTER_DISABLED", raising=False)
    conn = FakeConn()
    record = build_suggestion_record(
        suggestion_id="00000000-0000-0000-0000-000000000004",
        candidate_source_rid="rid:test:4",
        source_triage_state="MAYBE",
        requires_comparative_intake="operator_decides",
        **valid_parts(),
    )

    insert_suggestion(conn, record)

    _, params = conn.cursor_obj.calls[-1]
    for column in ("evidence_trail", "boundary_profile", "gate_result", "proposed_route"):
        assert isinstance(params[column], Json)
        assert params[column].adapted == record[column]


def test_dsn_resolution_precedence_and_safety(tmp_path, monkeypatch):
    env_file = tmp_path / "personal.env"
    env_file.write_text(
        "\n".join(
            [
                "POSTGRES_HOST=localhost",
                "POSTGRES_PORT=5432",
                "POSTGRES_DB=personal_koi",
                "POSTGRES_USER=test_user",
            ]
        ),
        encoding="utf-8",
    )

    resolved = resolve_dsn(environ={}, env_path=env_file)
    assert resolved.source == str(env_file)
    ensure_safe_dsn(resolved.dsn)

    overridden = resolve_dsn(
        environ={"ROUTER_PG_DSN": "postgresql://localhost:5432/personal_koi"},
        env_path=env_file,
    )
    assert overridden.source == "ROUTER_PG_DSN"

    with pytest.raises(RouterConfigError, match="DSN safety check FAILED"):
        ensure_safe_dsn("postgresql://prod-server/regen_koi")

    with pytest.raises(RouterConfigError, match="DSN safety check FAILED"):
        ensure_safe_dsn("postgresql://localhost:5432/postgres")

    monkeypatch.delenv("ROUTER_PG_DSN", raising=False)


def test_build_record_requires_source_triage_state_and_valid_json():
    parts = valid_parts()
    record = build_suggestion_record(
        suggestion_id="00000000-0000-0000-0000-000000000001",
        candidate_source_rid="rid:test:1",
        source_triage_state="MAYBE",
        requires_comparative_intake="no",
        **parts,
    )

    assert record["operator_state"] == "pending"
    assert len(record["idempotency_key"]) == 64

    with pytest.raises(RouterSchemaError, match="source_triage_state"):
        build_suggestion_record(
            suggestion_id="00000000-0000-0000-0000-000000000001",
            candidate_source_rid="rid:test:1",
            source_triage_state="NOT_RELEVANT",
            requires_comparative_intake="no",
            **parts,
        )
