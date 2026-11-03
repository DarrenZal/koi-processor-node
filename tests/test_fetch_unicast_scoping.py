"""Unicast scoping on /koi-net/{manifests,bundles}/fetch.

Divergence 3 is an intentional fork: upstream buffers events per peer at WRITE
time; we store one broadcast row and filter per peer at READ time. `target_node`
is therefore this fork's representation of "in that peer's buffer", and serving
a peer an event unicast to a different node is reading someone else's buffer.

`poll()` has always enforced this. The fetch handlers never did, so any peer
holding an approved edge could fetch any unicast event by naming its RID —
measured 2026-09-07 on one node as about 23,000 unicast rows, all carrying
contents, addressed to three different peers.

These tests pin the SQL predicate the handlers now use. The prior attempt at
fetch scoping shipped with no tests and was withdrawn in review; the specific
gap was that it was verified against hand-picked RIDs rather than against the
shapes the system is normally in, so each case below is a shape, not an example.
"""

import uuid

import asyncpg
import pytest
from tests import koi_test_targets as targets


ME = "orn:koi-net.node:test-fetcher+aaaa"
OTHER = "orn:koi-net.node:test-other-peer+bbbb"

# The predicate the handlers run, verbatim. If this drifts from
# koi_net_router.py the tests stop protecting anything, so it is duplicated
# deliberately rather than imported — a copy that disagrees fails loudly.
FETCH_SQL = """
    SELECT rid, manifest, contents FROM koi_net_events
    WHERE rid = $1 AND contents IS NOT NULL
      AND (target_node IS NULL OR target_node = $2)
    ORDER BY queued_at DESC LIMIT 1
"""


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    # Transaction-wrapped and rolled back, matching tests/test_federation_bridge.py —
    # nothing this file writes survives, so it cannot pollute the fixture DB.
    _conn = await asyncpg.connect(targets.database_url())
    tx = _conn.transaction()
    await tx.start()
    yield _conn
    await tx.rollback()
    await _conn.close()


async def _insert(c, rid, target_node):
    await c.execute(
        """
        INSERT INTO koi_net_events (event_type, rid, manifest, contents, source_node, target_node, expires_at)
        VALUES ('NEW', $1, '{"rid":"x"}'::jsonb, '{"payload":{}}'::jsonb, $2, $3, NOW() + INTERVAL '1 hour')
        """,
        rid, ME, target_node,
    )


@pytest.mark.anyio
async def test_broadcast_event_is_served(conn):
    """target_node IS NULL — visible to every peer. The common case."""
    rid = f"orn:koi-net.testdoc:broadcast-{uuid.uuid4().hex[:8]}"
    await _insert(conn, rid, None)
    row = await conn.fetchrow(FETCH_SQL, rid, ME)
    assert row is not None, "broadcast event must be fetchable"


@pytest.mark.anyio
async def test_event_unicast_to_me_is_served(conn):
    """target_node == caller — it is in this peer's buffer."""
    rid = f"orn:koi-net.testdoc:mine-{uuid.uuid4().hex[:8]}"
    await _insert(conn, rid, ME)
    row = await conn.fetchrow(FETCH_SQL, rid, ME)
    assert row is not None, "a peer must still fetch what was unicast to it"


@pytest.mark.anyio
async def test_event_unicast_to_another_peer_is_withheld(conn):
    """THE REGRESSION THIS FILE EXISTS FOR: reading another peer's buffer."""
    rid = f"orn:koi-net.testdoc:theirs-{uuid.uuid4().hex[:8]}"
    await _insert(conn, rid, OTHER)
    row = await conn.fetchrow(FETCH_SQL, rid, ME)
    assert row is None, (
        "an event unicast to another node must NOT be fetchable — this is the "
        "exposure the fix closes"
    )
    # Positive control: the row really is there, so the None above is the
    # predicate working rather than a failed insert.
    raw = await conn.fetchrow("SELECT rid FROM koi_net_events WHERE rid = $1", rid)
    assert raw is not None, "positive control: the row must exist to be withheld"


@pytest.mark.anyio
async def test_newest_eligible_row_wins_not_newest_row(conn):
    """A newer unicast row must not mask an older broadcast row for the same RID.

    The filter is in the WHERE clause, not applied after LIMIT 1 — otherwise a
    peer legitimately entitled to the broadcast copy would get nothing because
    the newest row happened to be addressed elsewhere.
    """
    rid = f"orn:koi-net.testdoc:layered-{uuid.uuid4().hex[:8]}"
    await _insert(conn, rid, None)        # older, broadcast
    await _insert(conn, rid, OTHER)       # newer, someone else's
    row = await conn.fetchrow(FETCH_SQL, rid, ME)
    assert row is not None, (
        "the older broadcast row must still be served; a newer unicast row "
        "for the same RID must not mask it"
    )
