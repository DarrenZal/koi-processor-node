"""
Tests for KOI_VAULT_MIRROR_PATHS — mirror-mode vault sync.

Mirror mode accepts incoming UPDATE/NEW/FORGET events for configured paths
unconditionally (no conflict-copy generation). Used on the MIRROR-side node
when a peer OWNS the path (via KOI_VAULT_READONLY_PATHS).
"""

import hashlib
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import asyncpg
from tests import koi_test_targets as targets

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


MY_NODE = "orn:koi-net.node:test-mirror+abc123"
PEER_NODE = "orn:koi-net.node:test-peer-mirror+def456"
PEER_NODE_B = "orn:koi-net.node:test-peer-mirror-b+ghi789"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def pool():
    p = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=5)
    yield p
    await p.close()


async def _has_060_schema(conn) -> bool:
    row = await conn.fetchrow(
        """SELECT 1 FROM information_schema.table_constraints
           WHERE table_name='vault_sync_peers' AND constraint_type='PRIMARY KEY'
           AND constraint_name='vault_sync_peers_pkey'"""
    )
    if not row:
        return False
    col = await conn.fetchval(
        """SELECT column_name FROM information_schema.key_column_usage
           WHERE table_name='vault_sync_peers' AND constraint_name='vault_sync_peers_pkey'"""
    )
    return col == "peer_node_rid"


@pytest.fixture
async def setup_peer(pool):
    """Insert peer + vault_sync_peers config for the 'Shared' folder."""
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, status)
               VALUES ($1, 'test-peer-mirror', 'FULL', 'http://10.0.0.2:58351', 'active')
               ON CONFLICT (node_rid) DO NOTHING""",
            PEER_NODE,
        )
        await conn.execute(
            "DELETE FROM vault_sync_peers WHERE peer_node_rid IN ($1, $2)",
            PEER_NODE, PEER_NODE_B,
        )
        await conn.execute(
            "DELETE FROM vault_sync_state WHERE relative_path LIKE 'Shared/%'"
        )
        if await _has_060_schema(conn):
            await conn.execute(
                """INSERT INTO vault_sync_peers (peer_node_rid, shared_folder, enabled)
                   VALUES ($1, 'Shared', TRUE)""",
                PEER_NODE,
            )
        else:
            await conn.execute(
                """INSERT INTO vault_sync_peers (id, peer_node_rid, shared_folder, enabled)
                   VALUES (1, $1, 'Shared', TRUE)
                   ON CONFLICT (id) DO UPDATE SET peer_node_rid=EXCLUDED.peer_node_rid, enabled=TRUE""",
                PEER_NODE,
            )
    yield
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM vault_sync_applied_events WHERE source_node IN ($1, $2)",
            PEER_NODE, PEER_NODE_B,
        )
        await conn.execute(
            "DELETE FROM vault_sync_state WHERE relative_path LIKE 'Shared/%'"
        )
        await conn.execute(
            "DELETE FROM vault_sync_peers WHERE peer_node_rid IN ($1, $2)",
            PEER_NODE, PEER_NODE_B,
        )
        await conn.execute(
            "DELETE FROM koi_net_nodes WHERE node_rid IN ($1, $2)",
            PEER_NODE, PEER_NODE_B,
        )
        await conn.execute("DELETE FROM vault_sync_metrics WHERE id=1")
        await conn.execute(
            "DELETE FROM koi_net_events WHERE rid LIKE 'orn:koi-net.vault-file:%'"
        )


@pytest.fixture
def tmp_vault(tmp_path):
    shared = tmp_path / "Shared"
    shared.mkdir()
    return tmp_path


@pytest.fixture
def mock_event_queue():
    eq = AsyncMock()
    eq.add = AsyncMock(return_value=str(uuid.uuid4()))
    eq.node_rid = MY_NODE
    return eq


@pytest.fixture
def mirror_shared(monkeypatch):
    """Configure 'Shared/' as a mirror path for the duration of the test."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_MIRROR_PATTERNS", ["Shared/"])
    yield
    # monkeypatch reverses on teardown


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _make_manager(pool, tmp_vault, event_queue):
    from api.vault_sync import VaultSyncManager
    mgr = VaultSyncManager(
        pool=pool,
        node_rid=MY_NODE,
        event_queue=event_queue,
        vault_path=str(tmp_vault),
    )
    _TEST_PEERS = {PEER_NODE, PEER_NODE_B}
    _orig_all = mgr._get_all_peers
    _orig_by_source = mgr._get_all_peers_by_source

    async def _all_peers():
        rows = await _orig_all()
        return [r for r in rows if r["peer_node_rid"] in _TEST_PEERS]

    async def _peers_by_source(source_node):
        if source_node not in _TEST_PEERS:
            return []
        return await _orig_by_source(source_node)

    mgr._get_all_peers = _all_peers
    mgr._get_all_peers_by_source = _peers_by_source
    return mgr


def _make_apply_kwargs(rel_path, event_type, content, base_hash=None,
                       origin_seq=1, deleted=False, origin_node=None):
    chash = _sha256(content) if content else ""
    eid = str(uuid.uuid4())
    contents = {"relative_path": rel_path, "_vault_sync": True}
    if content is not None:
        contents["markdown"] = content
    manifest = {
        "content_hash": chash,
        "relative_path": rel_path,
        "base_hash": base_hash,
        "bytes": len(content.encode("utf-8")) if content else 0,
        "origin_node": origin_node or PEER_NODE,
        "origin_seq": origin_seq,
        "deleted": deleted,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return dict(
        rid=f"orn:koi-net.vault-file:{rel_path}",
        event_type=event_type,
        contents=contents,
        manifest=manifest,
        source_node=PEER_NODE,
        event_id=eid,
    )


# =============================================================================
# Mirror-mode tests
# =============================================================================


@pytest.mark.anyio
async def test_mirror_new_overwrites_without_conflict(
    pool, setup_peer, tmp_vault, mock_event_queue, mirror_shared,
):
    """Mirror path + NEW event with on-disk hash mismatch → no conflict copy,
    file overwritten, state upserted, mirror_overwrite counter +1."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    # Local stub on disk (no state row)
    local_file = tmp_vault / "Shared" / "mirror-new.md"
    local_file.write_text("# Stub")

    peer_content = "# Rich Content From Peer"
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/mirror-new.md", "NEW", peer_content,
    ))

    assert local_file.read_text() == peer_content
    # No conflict files
    conflicts = list((tmp_vault / "Shared").glob("mirror-new (conflict*).md"))
    assert conflicts == []

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path='Shared/mirror-new.md'"
        )
    assert row is not None
    assert row["origin_node"] == PEER_NODE
    assert row["content_hash"] == _sha256(peer_content)

    assert mgr._metrics.mirror_overwrite == 1
    assert mgr._metrics.conflicts_created == 0


@pytest.mark.anyio
async def test_mirror_update_overwrites_without_conflict(
    pool, setup_peer, tmp_vault, mock_event_queue, mirror_shared,
):
    """Mirror path + UPDATE event with hash mismatch (no causal proof) →
    no conflict, overwrite + counter."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    local_content = "# Local stub before mirror update"
    local_file = tmp_vault / "Shared" / "mirror-update.md"
    local_file.write_text(local_content)
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, $4, NOW())""",
            "Shared/mirror-update.md", _sha256(local_content), MY_NODE,
            len(local_content.encode("utf-8")),
        )

    peer_content = "# Rich Update from Peer"
    # No base_hash — would normally trigger conflict copy
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/mirror-update.md", "UPDATE", peer_content, base_hash=None, origin_seq=2,
    ))

    assert local_file.read_text() == peer_content
    conflicts = list((tmp_vault / "Shared").glob("mirror-update (conflict*).md"))
    assert conflicts == []
    assert mgr._metrics.mirror_overwrite == 1
    assert mgr._metrics.conflicts_created == 0


@pytest.mark.anyio
async def test_mirror_forget_deletes(
    pool, setup_peer, tmp_vault, mock_event_queue, mirror_shared,
):
    """Mirror path + FORGET (owner-issued) → file deleted + state.is_deleted=True."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    content = "# To be deleted by mirror forget"
    chash = _sha256(content)
    local_file = tmp_vault / "Shared" / "mirror-forget.md"
    local_file.write_text(content)
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, $4, NOW())""",
            "Shared/mirror-forget.md", chash, PEER_NODE,
            len(content.encode("utf-8")),
        )

    kwargs = _make_apply_kwargs("Shared/mirror-forget.md", "FORGET", None,
                                base_hash=chash, origin_seq=2)
    kwargs["contents"] = {"relative_path": "Shared/mirror-forget.md", "_vault_sync": True}
    kwargs["manifest"]["content_hash"] = chash
    kwargs["manifest"]["deleted"] = True

    await mgr.apply_event(**kwargs)

    assert not local_file.exists()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path='Shared/mirror-forget.md'"
        )
    assert row["is_deleted"] is True
    assert mgr._metrics.mirror_forget == 1


@pytest.mark.anyio
async def test_mirror_stale_update_dropped(
    pool, setup_peer, tmp_vault, mock_event_queue, mirror_shared,
):
    """Mirror path + STALE UPDATE (origin_seq <= existing from same origin) →
    drop event (existing line-591 stale check), no overwrite, no counter increment."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    cur_content = "# Current content from peer (seq=5)"
    cur_hash = _sha256(cur_content)
    local_file = tmp_vault / "Shared" / "mirror-stale.md"
    local_file.write_text(cur_content)
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 5, $4, NOW())""",
            "Shared/mirror-stale.md", cur_hash, PEER_NODE,
            len(cur_content.encode("utf-8")),
        )

    # Stale event: origin_seq=3 <= existing 5
    stale_content = "# OLD content peer is replaying with stale seq=3"
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/mirror-stale.md", "UPDATE", stale_content,
        base_hash=cur_hash, origin_seq=3,
    ))

    # File unchanged
    assert local_file.read_text() == cur_content
    assert mgr._metrics.mirror_overwrite == 0
    assert mgr._metrics.rejected_stale_event == 1


@pytest.mark.anyio
async def test_mirror_stale_forget_dropped(
    pool, setup_peer, tmp_vault, mock_event_queue, mirror_shared,
):
    """Mirror path + STALE FORGET (same origin, older seq) → drop, file NOT deleted."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    content = "# Current content seq=5"
    chash = _sha256(content)
    local_file = tmp_vault / "Shared" / "mirror-stale-forget.md"
    local_file.write_text(content)
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 5, $4, NOW())""",
            "Shared/mirror-stale-forget.md", chash, PEER_NODE,
            len(content.encode("utf-8")),
        )

    # Stale FORGET: origin_seq=2 <= existing 5
    kwargs = _make_apply_kwargs("Shared/mirror-stale-forget.md", "FORGET", None,
                                base_hash=chash, origin_seq=2)
    kwargs["contents"] = {"relative_path": "Shared/mirror-stale-forget.md", "_vault_sync": True}
    kwargs["manifest"]["content_hash"] = chash
    kwargs["manifest"]["deleted"] = True

    await mgr.apply_event(**kwargs)

    # The line-591 stale check fires before _apply_forget — event is rejected outright
    # so file is unchanged and mirror_forget counter stays 0.
    assert local_file.exists()
    assert local_file.read_text() == content
    assert mgr._metrics.mirror_forget == 0


@pytest.mark.anyio
async def test_non_mirror_path_still_conflicts(
    pool, setup_peer, tmp_vault, mock_event_queue, mirror_shared,
):
    """Regression check: a path NOT in mirror set still produces conflict copies.

    mirror_shared makes 'Shared/' a mirror path; this test creates a peer for
    a 'Bridges' folder that's NOT mirrored, then induces a conflict on it.
    """
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    # Add a non-mirror peer subscription for "Bridges"
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_peers (peer_node_rid, shared_folder, enabled)
               VALUES ($1, 'Bridges', TRUE)
               ON CONFLICT DO NOTHING""",
            PEER_NODE,
        )
    # Make Bridges/ folder
    bridges = tmp_vault / "Bridges"
    bridges.mkdir(exist_ok=True)

    local_content = "# Local Bridges content"
    local_hash = _sha256(local_content)
    local_file = bridges / "non-mirror.md"
    local_file.write_text(local_content)
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, $4, NOW())""",
            "Bridges/non-mirror.md", local_hash, MY_NODE,
            len(local_content.encode("utf-8")),
        )

    peer_content = "# Peer wants different content"
    wrong_base = _sha256("# entirely different base")
    await mgr.apply_event(**_make_apply_kwargs(
        "Bridges/non-mirror.md", "UPDATE", peer_content,
        base_hash=wrong_base, origin_seq=2,
    ))

    # Local unchanged
    assert local_file.read_text() == local_content
    # Conflict copy created
    conflicts = list(bridges.glob("non-mirror (conflict*).md"))
    assert len(conflicts) == 1
    assert conflicts[0].read_text() == peer_content
    assert mgr._metrics.mirror_overwrite == 0
    assert mgr._metrics.conflicts_created == 1

    # Cleanup
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM vault_sync_peers WHERE peer_node_rid=$1 AND shared_folder='Bridges'",
            PEER_NODE,
        )
        await conn.execute(
            "DELETE FROM vault_sync_state WHERE relative_path LIKE 'Bridges/%'"
        )


@pytest.mark.anyio
async def test_readonly_wins_over_mirror(
    pool, setup_peer, tmp_vault, mock_event_queue, monkeypatch,
):
    """Path matching BOTH READONLY and MIRROR → READONLY wins (event rejected)."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_READONLY_INCOMING_PATHS", ["Shared/"])
    monkeypatch.setattr(vs, "_VAULT_MIRROR_PATTERNS", ["Shared/"])

    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    # Pre-existing local file + state row (so READONLY UPDATE rejection path triggers)
    local_content = "# Local owner content"
    local_file = tmp_vault / "Shared" / "readonly-wins.md"
    local_file.write_text(local_content)
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, $4, NOW())""",
            "Shared/readonly-wins.md", _sha256(local_content), MY_NODE,
            len(local_content.encode("utf-8")),
        )

    peer_content = "# Peer trying to overwrite"
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/readonly-wins.md", "UPDATE", peer_content,
        base_hash=_sha256(local_content), origin_seq=2,
    ))

    # READONLY rejects the event before mirror logic runs → file unchanged, no
    # mirror counter increment, rejected_path_authoritative incremented.
    assert local_file.read_text() == local_content
    assert mgr._metrics.mirror_overwrite == 0
    assert mgr._metrics.rejected_path_authoritative == 1


def test_readonly_mirror_overlap_warning_triggers():
    """_check_readonly_mirror_overlap surfaces overlapping configs."""
    from api import vault_sync as vs
    saved_ro = vs._VAULT_READONLY_INCOMING_PATHS
    saved_mp = vs._VAULT_MIRROR_PATTERNS
    try:
        vs._VAULT_READONLY_INCOMING_PATHS = ["Meetings/"]
        vs._VAULT_MIRROR_PATTERNS = ["Meetings/"]
        overlaps = vs._check_readonly_mirror_overlap()
        assert overlaps, "expected overlap detection when both lists contain 'Meetings/'"
        # And no overlap when disjoint
        vs._VAULT_MIRROR_PATTERNS = ["People/"]
        overlaps = vs._check_readonly_mirror_overlap()
        assert overlaps == [], "expected no overlap for disjoint sets"
    finally:
        vs._VAULT_READONLY_INCOMING_PATHS = saved_ro
        vs._VAULT_MIRROR_PATTERNS = saved_mp


# =============================================================================
# Per-file-origin ownership tests (KOI_VAULT_OWNER_UPDATE_ACCEPT)
# Added 2026-07-15 with the readonly owner-update fix. Reuse the fixtures above.
# =============================================================================


@pytest.fixture
def readonly_shared(monkeypatch):
    """Configure 'Shared/' as a READONLY (locally-authoritative) path."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_READONLY_INCOMING_PATHS", ["Shared/"])
    yield


async def _seed_new(mgr, rel_path, content, origin_node, origin_seq=1):
    """Establish initial state via a NEW event (readonly accepts NEW)."""
    await mgr.apply_event(**_make_apply_kwargs(
        rel_path, "NEW", content, origin_node=origin_node, origin_seq=origin_seq,
    ))


_BIG = "# Owner note\n" + ("substantive body line that makes the file comfortably large. " * 20)


@pytest.mark.anyio
async def test_owner_update_accepted_flag_on(
    pool, setup_peer, tmp_vault, mock_event_queue, readonly_shared, monkeypatch,
):
    """T1: flag ON + owner (origin==local origin) UPDATE on a readonly path → accepted."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_OWNER_UPDATE_ACCEPT", True)
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    f = tmp_vault / "Shared" / "t1.md"
    await _seed_new(mgr, "Shared/t1.md", _BIG, PEER_NODE, 1)
    assert f.read_text() == _BIG
    updated = _BIG + "\n# Enriched by the owning peer.\n" * 6
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/t1.md", "UPDATE", updated, base_hash=_sha256(_BIG),
        origin_node=PEER_NODE, origin_seq=2,
    ))
    assert f.read_text() == updated
    assert mgr._metrics.readonly_owner_update_accepted == 1
    assert mgr._metrics.rejected_path_authoritative == 0


@pytest.mark.anyio
async def test_non_owner_update_rejected(
    pool, setup_peer, tmp_vault, mock_event_queue, readonly_shared, monkeypatch,
):
    """T2: flag ON but file is locally authored (origin=MY_NODE); a PEER-origin UPDATE → rejected."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_OWNER_UPDATE_ACCEPT", True)
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    f = tmp_vault / "Shared" / "t2.md"
    await _seed_new(mgr, "Shared/t2.md", _BIG, MY_NODE, 1)  # Mac-authored
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/t2.md", "UPDATE", _BIG + "\nmutated by non-owner", base_hash=_sha256(_BIG),
        origin_node=PEER_NODE, origin_seq=2,
    ))
    assert f.read_text() == _BIG  # unchanged
    assert mgr._metrics.rejected_path_authoritative >= 1
    assert mgr._metrics.readonly_owner_update_accepted == 0


@pytest.mark.anyio
async def test_peer_claiming_this_node_as_origin_is_rejected(
    pool, setup_peer, tmp_vault, mock_event_queue, readonly_shared, monkeypatch,
):
    """Flag ON, file locally authored, a peer names THIS node as origin_node with a newer seq → rejected."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_OWNER_UPDATE_ACCEPT", True)
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    f = tmp_vault / "Shared" / "own-origin.md"
    await _seed_new(mgr, "Shared/own-origin.md", _BIG, MY_NODE, 1)
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/own-origin.md", "UPDATE", _BIG + "\nnot from this node", base_hash=_sha256(_BIG),
        origin_node=MY_NODE, origin_seq=2,
    ))
    assert f.read_text() == _BIG
    assert mgr._metrics.rejected_path_authoritative >= 1
    assert mgr._metrics.readonly_owner_update_accepted == 0


@pytest.mark.anyio
async def test_relayed_update_naming_the_authoring_node_is_still_accepted(
    pool, setup_peer, tmp_vault, mock_event_queue, readonly_shared, monkeypatch,
):
    """Mesh relay: source is one peer, origin is the third node that authored the file → accepted."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_OWNER_UPDATE_ACCEPT", True)
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    f = tmp_vault / "Shared" / "relayed.md"
    await _seed_new(mgr, "Shared/relayed.md", _BIG, PEER_NODE_B, 1)
    updated = _BIG + "\nrelayed update"
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/relayed.md", "UPDATE", updated, base_hash=_sha256(_BIG),
        origin_node=PEER_NODE_B, origin_seq=2,
    ))
    assert f.read_text() == updated
    assert mgr._metrics.readonly_owner_update_accepted == 1


@pytest.mark.anyio
async def test_no_local_row_rejected(
    pool, setup_peer, tmp_vault, mock_event_queue, readonly_shared, monkeypatch,
):
    """T3: flag ON, UPDATE with no local state row → rejected (safe default)."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_OWNER_UPDATE_ACCEPT", True)
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    f = tmp_vault / "Shared" / "t3.md"
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/t3.md", "UPDATE", _BIG, origin_node=PEER_NODE, origin_seq=2,
    ))
    assert not f.exists()
    assert mgr._metrics.rejected_path_authoritative >= 1
    assert mgr._metrics.readonly_owner_update_accepted == 0


@pytest.mark.anyio
async def test_flag_off_owner_update_rejected(
    pool, setup_peer, tmp_vault, mock_event_queue, readonly_shared, monkeypatch,
):
    """T4: flag OFF → owner UPDATE still rejected (identical to pre-fix behavior)."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_OWNER_UPDATE_ACCEPT", False)
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    f = tmp_vault / "Shared" / "t4.md"
    await _seed_new(mgr, "Shared/t4.md", _BIG, PEER_NODE, 1)
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/t4.md", "UPDATE", _BIG + "\nnope", base_hash=_sha256(_BIG),
        origin_node=PEER_NODE, origin_seq=2,
    ))
    assert f.read_text() == _BIG
    assert mgr._metrics.rejected_path_authoritative >= 1
    assert mgr._metrics.readonly_owner_update_accepted == 0


@pytest.mark.anyio
async def test_owner_update_shrink_guarded(
    pool, setup_peer, tmp_vault, mock_event_queue, readonly_shared, monkeypatch,
):
    """T5: flag ON, owner UPDATE that collapses a rich note to a stub → shrink-guarded reject."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_OWNER_UPDATE_ACCEPT", True)
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    f = tmp_vault / "Shared" / "t5.md"
    await _seed_new(mgr, "Shared/t5.md", _BIG, PEER_NODE, 1)  # big (>512B)
    tiny = "# stub"
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/t5.md", "UPDATE", tiny, base_hash=_sha256(_BIG),
        origin_node=PEER_NODE, origin_seq=2,
    ))
    assert f.read_text() == _BIG  # kept rich
    assert mgr._metrics.readonly_owner_update_shrink_guarded == 1
    assert mgr._metrics.readonly_owner_update_accepted == 0


@pytest.mark.anyio
async def test_owner_update_growth_accepted(
    pool, setup_peer, tmp_vault, mock_event_queue, readonly_shared, monkeypatch,
):
    """T5b: flag ON, stub→rich owner UPDATE (an enrichment agent's shape) → accepted."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_OWNER_UPDATE_ACCEPT", True)
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    f = tmp_vault / "Shared" / "t5b.md"
    stub = "# Awaiting enrichment"
    await _seed_new(mgr, "Shared/t5b.md", stub, PEER_NODE, 1)
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/t5b.md", "UPDATE", _BIG, base_hash=_sha256(stub),
        origin_node=PEER_NODE, origin_seq=2,
    ))
    assert f.read_text() == _BIG
    assert mgr._metrics.readonly_owner_update_accepted == 1


@pytest.mark.anyio
async def test_owner_forget_rejected(
    pool, setup_peer, tmp_vault, mock_event_queue, readonly_shared, monkeypatch,
):
    """T6: flag ON, FORGET from the owner on a readonly path → still rejected (UPDATE-only bypass)."""
    from api import vault_sync as vs
    monkeypatch.setattr(vs, "_VAULT_OWNER_UPDATE_ACCEPT", True)
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    f = tmp_vault / "Shared" / "t6.md"
    await _seed_new(mgr, "Shared/t6.md", _BIG, PEER_NODE, 1)
    await mgr.apply_event(**_make_apply_kwargs(
        "Shared/t6.md", "FORGET", _BIG, origin_node=PEER_NODE, origin_seq=2, deleted=True,
    ))
    assert f.exists() and f.read_text() == _BIG
    assert mgr._metrics.rejected_path_authoritative >= 1
