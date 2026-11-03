"""`_insert_with_drift_retry` must survive drift in columns named by ON CONFLICT.

THE BUG THIS PINS
-----------------
The helper drops an undefined column from the INSERT value list and retries. But
`conflict_clause` was a fixed string re-interpolated verbatim on every attempt,
so a column named in BOTH the INSERT and the `ON CONFLICT DO UPDATE SET` was
dropped from one and left in the other. The retry raised the identical
`UndefinedColumnError`, and because the column was no longer in `cv`, the
`col not in cv` guard re-raised it as `federation.drift.unparseable`.

Net effect: drift-retry silently protected only columns ABSENT from the conflict
clause — which, for both live call sites (`knowledge_episodes` at the episode
handler, `knowledge_facts` at the fact handler), is almost none. The protection
read as present and was mostly not there.

Found 2026-08-22 while assessing whether `_apply_entity` could be routed through
this helper to make migration-vs-code ordering safe. It could not: the column at
issue, `resolution_tier`, appears in both halves — so the proposed fix would have
failed in a new way rather than working.

The COALESCE case is not hypothetical. `knowledge_episodes` ships
`group_id = COALESCE(EXCLUDED.group_id, knowledge_episodes.group_id)`, whose
inner comma breaks any naive split on ",".
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from api.domain_event_handlers import (
    _insert_with_drift_retry,
    _strip_conflict_assignments,
)
from tests import koi_test_targets as targets


# ---------------------------------------------------------------------------
# Pure-function tests: no database needed.
# ---------------------------------------------------------------------------

def test_strip_removes_the_named_assignment():
    clause = "ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a, ghost = EXCLUDED.ghost"
    out = _strip_conflict_assignments(clause, "ghost")
    assert "ghost" not in out
    assert "a = EXCLUDED.a" in out


def test_strip_is_paren_aware():
    """A COALESCE's inner comma is not an assignment boundary."""
    clause = (
        "ON CONFLICT (id) DO UPDATE SET "
        "a = EXCLUDED.a, g = COALESCE(EXCLUDED.g, t.g), b = EXCLUDED.b"
    )
    out = _strip_conflict_assignments(clause, "g")
    assert "COALESCE" not in out
    assert "a = EXCLUDED.a" in out and "b = EXCLUDED.b" in out


def test_strip_does_not_match_substrings():
    """Dropping `id` must not damage `valid_id` or `id_hash`."""
    clause = "ON CONFLICT (x) DO UPDATE SET valid_id = EXCLUDED.valid_id, id = EXCLUDED.id"
    out = _strip_conflict_assignments(clause, "id")
    assert "valid_id = EXCLUDED.valid_id" in out
    assert "SET id = " not in out and ", id = " not in out


def test_strip_degrades_to_do_nothing_when_set_would_be_empty():
    """An empty SET list is a syntax error; DO NOTHING is the valid degradation."""
    clause = "ON CONFLICT (id) DO UPDATE SET ghost = EXCLUDED.ghost"
    assert _strip_conflict_assignments(clause, "ghost").strip() == (
        "ON CONFLICT (id) DO NOTHING"
    )


def test_strip_leaves_do_nothing_clauses_alone():
    clause = "ON CONFLICT (id) DO NOTHING"
    assert _strip_conflict_assignments(clause, "anything") == clause


# ---------------------------------------------------------------------------
# Behavioural tests against a scratch table.
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.get_event_loop_policy().new_event_loop().run_until_complete(coro)


async def _probe(clause: str, values: dict) -> str:
    conn = await asyncpg.connect(targets.database_url())
    try:
        await conn.execute("DROP TABLE IF EXISTS drift_probe")
        # Deliberately LACKS `ghost`, simulating a node whose migration lags.
        await conn.execute("CREATE TABLE drift_probe (id text primary key, a text)")
        tr = conn.transaction()
        await tr.start()
        try:
            await _insert_with_drift_retry(conn, "drift_probe", values, None, clause)
            return "RECOVERED"
        except Exception as exc:  # noqa: BLE001 — the assertion is on the outcome
            return f"FAILED:{type(exc).__name__}"
        finally:
            await tr.rollback()
            await conn.execute("DROP TABLE IF EXISTS drift_probe")
    finally:
        await conn.close()


@pytest.mark.parametrize(
    "label,clause",
    [
        ("insert_only", "ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a"),
        ("also_in_conflict",
         "ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a, ghost = EXCLUDED.ghost"),
        ("inside_coalesce",
         "ON CONFLICT (id) DO UPDATE SET a = EXCLUDED.a, "
         "ghost = COALESCE(EXCLUDED.ghost, drift_probe.ghost)"),
        ("only_assignment", "ON CONFLICT (id) DO UPDATE SET ghost = EXCLUDED.ghost"),
    ],
)
def test_drift_recovers_regardless_of_where_the_column_is_named(label, clause):
    result = _run(_probe(clause, {"id": label[:8], "a": "v", "ghost": "g"}))
    assert result == "RECOVERED", (
        f"{label}: {result}. Before 2026-08-22 every case except 'insert_only' "
        f"failed, because the conflict clause kept naming the dropped column."
    )


def test_negative_control_a_real_error_still_raises():
    """Drift-retry must not swallow errors that are not column drift.

    Without this, 'everything recovers' could mean the helper stopped raising.
    """
    result = _run(_probe("ON CONFLICT (nosuchcol) DO NOTHING", {"id": "n1", "a": "v"}))
    assert result.startswith("FAILED:"), (
        "A bad conflict target must still raise; the helper is not a catch-all."
    )
