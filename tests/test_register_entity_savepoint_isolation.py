"""A2: resolve_pending_relationships must isolate its own promotion behind a
SAVEPOINT, matching the pattern already used twice in sync_vault_relationships
(SAVEPOINT rel_insert / pending_insert) in the same file (api/vault_parser.py).

Without it, an exception during promotion (e.g. the
CHECK (subject_uri <> object_uri) constraint on entity_relationships, tripped
by a near-duplicate trigram match promoting a pending relationship keyed to
the entity's own URI) left the enclosing /register-entity transaction
aborted, and its closing COMMIT silently became a ROLLBACK — discarding the
entire write (entity row + RID mapping + relationships), not just this one
promotion, while the endpoint still returned HTTP 200 success=true.

No existing test file touched resolve_pending_relationships before this one.
"""

import pytest

from api import vault_parser


class _PendingPromoteConn:
    """Stub asyncpg connection: records every execute() call in order so the
    tests can assert SAVEPOINT/RELEASE/ROLLBACK ordering, and can be told to
    fail the relationship INSERT to simulate any DB-level promotion failure.
    """

    def __init__(self, pending_rows, fail_insert=False):
        self._pending_rows = pending_rows
        self._fail_insert = fail_insert
        self.calls = []  # ("fetch"|"execute", query_text)

    async def fetch(self, query, *args):
        self.calls.append(("fetch", query.strip()))
        return self._pending_rows

    async def execute(self, query, *args):
        text = query.strip()
        self.calls.append(("execute", text))
        if self._fail_insert and text.startswith("INSERT INTO entity_relationships"):
            raise Exception("simulated insert failure (e.g. CheckViolationError)")
        return "OK"


def _pending_row(sim=0.9):
    return {
        "id": 42,
        "subject_uri": "orn:personal-koi.entity:org-regen-network",
        "object_uri": None,
        "predicate": "represents",
        "raw_unknown_label": "Regen Network Inc.",
        "unknown_side": "object",
        "target_type_hint": "Organization",
        "source_rid": "Meetings/2026-08-01 Call.md",
        "source_field": "attendees",
        "sim": sim,
    }


def _executed_queries(conn):
    return [q for kind, q in conn.calls if kind == "execute"]


@pytest.mark.asyncio
async def test_successful_promotion_wraps_in_savepoint_and_releases():
    conn = _PendingPromoteConn([_pending_row()], fail_insert=False)
    promoted = await vault_parser.resolve_pending_relationships(
        conn,
        "orn:personal-koi.entity:org-regen-network-inc",
        "Regen Network Inc.",
        "Organization",
    )
    assert promoted == 1

    executed = _executed_queries(conn)
    assert "SAVEPOINT pending_promote" in executed
    assert "RELEASE SAVEPOINT pending_promote" in executed
    assert not any(q.startswith("ROLLBACK TO SAVEPOINT") for q in executed)
    assert executed.index("SAVEPOINT pending_promote") < executed.index(
        "RELEASE SAVEPOINT pending_promote"
    )
    # The pending row is consumed on success.
    assert any(q.startswith("DELETE FROM pending_relationships") for q in executed)


@pytest.mark.asyncio
async def test_failed_promotion_rolls_back_to_savepoint_and_does_not_raise():
    """The core regression: a promotion failure must not raise out of this
    function, and the transaction must be left usable via ROLLBACK TO
    SAVEPOINT — not merely caught, which (pre-fix) left it aborted."""
    conn = _PendingPromoteConn([_pending_row()], fail_insert=True)

    # No unhandled exception escapes — this is the assertion that would have
    # failed pre-fix only if the bare `except Exception` itself were missing;
    # what pre-fix silently broke was everything the CALLER did afterwards
    # inside the same DB transaction, which the ROLLBACK TO SAVEPOINT below
    # is what actually protects.
    promoted = await vault_parser.resolve_pending_relationships(
        conn,
        "orn:personal-koi.entity:org-regen-network-inc",
        "Regen Network Inc.",
        "Organization",
    )
    assert promoted == 0

    executed = _executed_queries(conn)
    assert "SAVEPOINT pending_promote" in executed
    assert "ROLLBACK TO SAVEPOINT pending_promote" in executed
    assert "RELEASE SAVEPOINT pending_promote" not in executed
    # The pending row must survive for a later retry, not be deleted.
    assert not any(q.startswith("DELETE FROM pending_relationships") for q in executed)


@pytest.mark.asyncio
async def test_self_loop_relationship_is_skipped_not_attempted():
    """Defense-in-depth companion fix: insert_relationship_with_symmetric's
    primary insert now guards subject_uri != object_uri (matching the guard
    the symmetric insert already had), so a pending row whose known side
    happens to equal the newly-registered entity's own URI never reaches the
    CHECK constraint at all."""
    conn = _PendingPromoteConn(
        [{**_pending_row(), "subject_uri": "orn:personal-koi.entity:org-regen-network-inc"}],
        fail_insert=True,  # would raise if the primary insert were attempted
    )
    promoted = await vault_parser.resolve_pending_relationships(
        conn,
        "orn:personal-koi.entity:org-regen-network-inc",  # same URI as subject_uri above
        "Regen Network Inc.",
        "Organization",
    )
    assert promoted == 1  # the DELETE + bookkeeping still complete
    executed = _executed_queries(conn)
    assert not any(q.startswith("INSERT INTO entity_relationships") for q in executed)
    assert not any(q.startswith("ROLLBACK TO SAVEPOINT") for q in executed)
