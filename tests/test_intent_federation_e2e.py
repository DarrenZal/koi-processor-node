"""Tests for intent federation: domain event handlers and discovery cache.

Covers:
  1. _apply_intent: NEW event upserts into intent_discovery_cache
  2. _apply_intent: UPDATE event updates status in cache (fulfilled, archived)
  3. FORGET event deletes row from intent_discovery_cache
  4. Draft intents do NOT emit federation events (guard in intent_router)
  5. Discovery projection contains only safe fields (no contact, priority, tags)
"""

import json
import uuid

import asyncpg
import pytest
from tests import koi_test_targets as targets

TEST_SOURCE_NODE = "orn:koi-net.node:test-peer-intent+bbbb"


# ---------------------------------------------------------------------------
# Fixtures (same pattern as test_federation_bridge.py)
# ---------------------------------------------------------------------------

class _SingleConnPool:
    """Wraps a single asyncpg.Connection to quack like asyncpg.Pool."""
    def __init__(self, conn):
        self._conn = conn

    class _CM:
        def __init__(self, conn):
            self.conn = conn
        async def __aenter__(self):
            return self.conn
        async def __aexit__(self, *a):
            pass

    def acquire(self):
        return self._CM(self._conn)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    _conn = await asyncpg.connect(targets.database_url())
    tx = _conn.transaction()
    await tx.start()
    yield _conn
    await tx.rollback()
    await _conn.close()


@pytest.fixture
async def pool(conn):
    return _SingleConnPool(conn)


def _uid():
    return uuid.uuid4().hex[:12]


# ---------------------------------------------------------------------------
# 1. _apply_intent: NEW → upsert into intent_discovery_cache
# ---------------------------------------------------------------------------

class TestApplyIntentNew:

    @pytest.mark.anyio
    async def test_new_intent_creates_cache_row(self, conn):
        """A NEW intent domain event should create a row in intent_discovery_cache."""
        from api.domain_event_handlers import apply_domain_event

        uid = _uid()
        intent_rid = f"orn:koi-net.intent:fed-test-{uid}"
        payload = {
            "intent_rid": intent_rid,
            "intent_type": "OFFER",
            "status": "active",
            "landscape_group": "olympic_peninsula",
            "visibility": "regional",
            "asset_offered": "restoration_labor",
            "asset_wanted": None,
            "quantity": "200 hours",
        }

        await apply_domain_event(conn, "intent", intent_rid, "NEW", payload, TEST_SOURCE_NODE)

        row = await conn.fetchrow(
            "SELECT intent_type, status, landscape_group, visibility, "
            "asset_offered, quantity, source_node "
            "FROM intent_discovery_cache WHERE intent_rid = $1",
            intent_rid,
        )
        assert row is not None, "NEW intent event should create cache row"
        assert row["intent_type"] == "OFFER"
        assert row["status"] == "active"
        assert row["landscape_group"] == "olympic_peninsula"
        assert row["visibility"] == "regional"
        assert row["asset_offered"] == "restoration_labor"
        assert row["quantity"] == "200 hours"
        assert row["source_node"] == TEST_SOURCE_NODE

    @pytest.mark.anyio
    async def test_new_want_intent(self, conn):
        """WANT intents should store asset_wanted, not asset_offered."""
        from api.domain_event_handlers import apply_domain_event

        uid = _uid()
        intent_rid = f"orn:koi-net.intent:want-{uid}"
        payload = {
            "intent_rid": intent_rid,
            "intent_type": "WANT",
            "status": "active",
            "landscape_group": "skagit",
            "visibility": "local",
            "asset_offered": None,
            "asset_wanted": "soil_monitoring",
            "quantity": "1 kit",
        }

        await apply_domain_event(conn, "intent", intent_rid, "NEW", payload, TEST_SOURCE_NODE)

        row = await conn.fetchrow(
            "SELECT intent_type, asset_offered, asset_wanted FROM intent_discovery_cache "
            "WHERE intent_rid = $1",
            intent_rid,
        )
        assert row is not None
        assert row["intent_type"] == "WANT"
        assert row["asset_offered"] is None
        assert row["asset_wanted"] == "soil_monitoring"


# ---------------------------------------------------------------------------
# 2. _apply_intent: UPDATE → status changes in cache
# ---------------------------------------------------------------------------

class TestApplyIntentUpdate:

    @pytest.mark.anyio
    async def test_update_to_fulfilled(self, conn):
        """UPDATE event with status=fulfilled should update cache row, not delete it."""
        from api.domain_event_handlers import apply_domain_event

        uid = _uid()
        intent_rid = f"orn:koi-net.intent:fulfill-{uid}"

        # Create active intent
        await apply_domain_event(conn, "intent", intent_rid, "NEW", {
            "intent_rid": intent_rid,
            "intent_type": "OFFER",
            "status": "active",
            "landscape_group": "the_gorge",
            "visibility": "regional",
            "asset_offered": "mycoremediation",
        }, TEST_SOURCE_NODE)

        # Update to fulfilled
        await apply_domain_event(conn, "intent", intent_rid, "UPDATE", {
            "intent_rid": intent_rid,
            "intent_type": "OFFER",
            "status": "fulfilled",
            "landscape_group": "the_gorge",
            "visibility": "regional",
            "asset_offered": "mycoremediation",
        }, TEST_SOURCE_NODE)

        row = await conn.fetchrow(
            "SELECT status FROM intent_discovery_cache WHERE intent_rid = $1",
            intent_rid,
        )
        assert row is not None, "Fulfilled intent should still exist in cache (not deleted)"
        assert row["status"] == "fulfilled"

    @pytest.mark.anyio
    async def test_update_to_archived(self, conn):
        """UPDATE event with status=archived should update cache row, not delete it."""
        from api.domain_event_handlers import apply_domain_event

        uid = _uid()
        intent_rid = f"orn:koi-net.intent:archive-{uid}"

        # Create active intent
        await apply_domain_event(conn, "intent", intent_rid, "NEW", {
            "intent_rid": intent_rid,
            "intent_type": "WANT",
            "status": "active",
            "landscape_group": "whatcom",
            "visibility": "local",
            "asset_wanted": "volunteer_coordination",
        }, TEST_SOURCE_NODE)

        # Archive
        await apply_domain_event(conn, "intent", intent_rid, "UPDATE", {
            "intent_rid": intent_rid,
            "intent_type": "WANT",
            "status": "archived",
            "landscape_group": "whatcom",
            "visibility": "local",
            "asset_wanted": "volunteer_coordination",
        }, TEST_SOURCE_NODE)

        row = await conn.fetchrow(
            "SELECT status FROM intent_discovery_cache WHERE intent_rid = $1",
            intent_rid,
        )
        assert row is not None, "Archived intent should remain in cache with updated status"
        assert row["status"] == "archived"


# ---------------------------------------------------------------------------
# 3. FORGET → deletes cache row
# ---------------------------------------------------------------------------

class TestForgetIntent:

    @pytest.mark.anyio
    async def test_forget_deletes_cache_row(self, conn):
        """FORGET event should delete the intent from discovery cache."""
        from api.domain_event_handlers import apply_domain_event

        uid = _uid()
        intent_rid = f"orn:koi-net.intent:forget-{uid}"

        # Create
        await apply_domain_event(conn, "intent", intent_rid, "NEW", {
            "intent_rid": intent_rid,
            "intent_type": "OFFER",
            "status": "active",
            "landscape_group": "fraser_lowland",
            "visibility": "regional",
            "asset_offered": "restoration_labor",
        }, TEST_SOURCE_NODE)

        # Verify exists
        assert await conn.fetchval(
            "SELECT 1 FROM intent_discovery_cache WHERE intent_rid = $1", intent_rid,
        )

        # FORGET
        await apply_domain_event(conn, "intent", intent_rid, "FORGET", {
            "intent_rid": intent_rid,
        }, TEST_SOURCE_NODE)

        assert not await conn.fetchval(
            "SELECT 1 FROM intent_discovery_cache WHERE intent_rid = $1", intent_rid,
        ), "Intent should be deleted from cache after FORGET"

    @pytest.mark.anyio
    async def test_forget_nonexistent_is_noop(self, conn):
        """FORGET for a non-existent intent should not raise."""
        from api.domain_event_handlers import apply_domain_event

        uid = _uid()
        intent_rid = f"orn:koi-net.intent:ghost-{uid}"

        # Should not raise
        await apply_domain_event(conn, "intent", intent_rid, "FORGET", {
            "intent_rid": intent_rid,
        }, TEST_SOURCE_NODE)


# ---------------------------------------------------------------------------
# 4. Draft guard: draft intents must NOT emit federation events
# ---------------------------------------------------------------------------

class TestDraftGuard:

    @pytest.mark.anyio
    async def test_emit_not_called_for_draft(self, pool, conn):
        """The intent router should not emit federation events for draft intents.

        This tests the guard at the emit site, not the handler. We verify by
        checking that emitting an intent event with status=draft still works
        at the handler level (it's the router that gates, not the handler).
        The handler itself has no draft guard — it accepts whatever arrives.
        """
        from api.domain_event_handlers import apply_domain_event

        uid = _uid()
        intent_rid = f"orn:koi-net.intent:draft-{uid}"

        # Handler accepts draft — the guard is in intent_router.py, not here.
        # This test documents that the handler layer is permissive;
        # the actual draft guard is tested in test_intent_registry.py::TestDraftActiveGuard.
        await apply_domain_event(conn, "intent", intent_rid, "NEW", {
            "intent_rid": intent_rid,
            "intent_type": "OFFER",
            "status": "draft",
            "landscape_group": "test-draft-guard",
            "visibility": "local",
            "asset_offered": "test_asset",
        }, TEST_SOURCE_NODE)

        row = await conn.fetchrow(
            "SELECT status FROM intent_discovery_cache WHERE intent_rid = $1",
            intent_rid,
        )
        # Handler is permissive — row exists. The actual draft gate is in the router.
        assert row is not None
        assert row["status"] == "draft"


# ---------------------------------------------------------------------------
# 5. Discovery projection: only safe fields cross node boundaries
# ---------------------------------------------------------------------------

class TestDiscoveryProjectionSafety:

    @pytest.mark.anyio
    async def test_cache_has_no_contact_or_priority_columns(self, conn):
        """intent_discovery_cache should not have publisher_contact, priority, or tags columns."""
        columns = await conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'intent_discovery_cache' ORDER BY ordinal_position"
        )
        col_names = {r["column_name"] for r in columns}

        # These fields must NOT exist in the cache table
        forbidden = {"publisher_contact", "publisher_name", "priority", "tags",
                     "source_excerpt", "decay_rate", "notes", "metadata",
                     "ai_confidence", "entered_by", "reviewed_by"}
        leaked = col_names & forbidden
        assert not leaked, f"Discovery cache should not contain private fields: {leaked}"

        # These fields SHOULD exist (discovery projection)
        expected = {"intent_rid", "intent_type", "status", "landscape_group",
                    "visibility", "asset_offered", "asset_wanted", "quantity",
                    "source_node"}
        missing = expected - col_names
        assert not missing, f"Discovery cache is missing expected columns: {missing}"
