"""
Isolated fixture tests for vault sync.

Uses a real pool (manager acquires its own connections).
Cleanup happens via a fixture that removes test data after each test.
Requires KOI_TEST_DATABASE_URL naming a test database with the KOI schema and
migrations 049 and 050 applied; the tests skip when it is unset.
"""

import asyncio
import hashlib
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from api.vault_sync import _build_vault_manifest
from unittest.mock import AsyncMock, patch

import pytest
import asyncpg
from tests import koi_test_targets as targets

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


MY_NODE = "orn:koi-net.node:test-local+abc123"
PEER_NODE = "orn:koi-net.node:test-peer+def456"
PEER_NODE_B = "orn:koi-net.node:test-peer-b+ghi789"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def pool():
    """Create a connection pool."""
    p = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=5)
    yield p
    await p.close()


async def _has_060_schema(conn) -> bool:
    """Check if migration 060 has been applied (peer_node_rid is PK)."""
    row = await conn.fetchrow(
        """SELECT 1 FROM information_schema.table_constraints
           WHERE table_name='vault_sync_peers' AND constraint_type='PRIMARY KEY'
           AND constraint_name='vault_sync_peers_pkey'"""
    )
    if not row:
        return False
    # Check which column the PK is on
    col = await conn.fetchval(
        """SELECT column_name FROM information_schema.key_column_usage
           WHERE table_name='vault_sync_peers' AND constraint_name='vault_sync_peers_pkey'"""
    )
    return col == "peer_node_rid"


@pytest.fixture
async def setup_peer(pool):
    """Insert peer node and vault_sync_peers config (auto-committed). Cleanup after.

    Works with both pre-060 (id PK) and post-060 (peer_node_rid PK) schemas.
    """
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, status)
               VALUES ($1, 'test-peer', 'FULL', 'http://10.0.0.2:58351', 'active')
               ON CONFLICT (node_rid) DO NOTHING""",
            PEER_NODE,
        )
        # Clean all test peers first to avoid conflicts on either schema
        await conn.execute("DELETE FROM vault_sync_peers WHERE peer_node_rid IN ($1, $2)", PEER_NODE, PEER_NODE_B)

        # Clear any pre-existing vault_sync_state rows so scan tests
        # don't see stale rows that trigger spurious FORGET events.
        await conn.execute("DELETE FROM vault_sync_state WHERE relative_path LIKE 'Shared/%'")

        if await _has_060_schema(conn):
            await conn.execute(
                """INSERT INTO vault_sync_peers (peer_node_rid, shared_folder, enabled)
                   VALUES ($1, 'Shared', TRUE)""",
                PEER_NODE,
            )
        else:
            # Legacy schema: id is PK
            await conn.execute(
                """INSERT INTO vault_sync_peers (id, peer_node_rid, shared_folder, enabled)
                   VALUES (1, $1, 'Shared', TRUE)
                   ON CONFLICT (id) DO UPDATE SET peer_node_rid=EXCLUDED.peer_node_rid, enabled=TRUE""",
                PEER_NODE,
            )
    yield
    # Cleanup
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM vault_sync_applied_events WHERE source_node IN ($1, $2)", PEER_NODE, PEER_NODE_B)
        await conn.execute("DELETE FROM vault_sync_state WHERE relative_path LIKE 'Shared/%'")
        await conn.execute("DELETE FROM vault_sync_peers WHERE peer_node_rid IN ($1, $2)", PEER_NODE, PEER_NODE_B)
        await conn.execute("DELETE FROM koi_net_nodes WHERE node_rid IN ($1, $2)", PEER_NODE, PEER_NODE_B)
        await conn.execute("DELETE FROM vault_sync_metrics WHERE id=1")
        await conn.execute("DELETE FROM koi_net_events WHERE rid LIKE 'orn:koi-net.vault-file:%'")


@pytest.fixture
def tmp_vault(tmp_path):
    """Create a temp vault directory with Shared subfolder."""
    shared = tmp_path / "Shared"
    shared.mkdir()
    return tmp_path


@pytest.fixture
def mock_event_queue():
    """Mock EventQueue that records add() calls."""
    eq = AsyncMock()
    eq.add = AsyncMock(return_value=str(uuid.uuid4()))
    eq.node_rid = MY_NODE
    return eq


def _sha256(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _make_manager(pool, tmp_vault, event_queue):
    """Build a VaultSyncManager isolated from production peer rows.

    The local `personal_koi` DB may hold real vault_sync_peers rows for other
    peers and folders. Tests should only see the peers they
    registered via setup_peer. We override the instance methods so
    _scan_async / reconcile / etc. operate against just the test peers.
    """
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


# =============================================================================
# Scan tests
# =============================================================================


@pytest.mark.anyio
async def test_scan_new_file(pool, setup_peer, tmp_vault, mock_event_queue):
    """New .md file detected, sync state row created, NEW event queued."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    test_file = shared / "test-note.md"
    test_file.write_text("# Hello World\n\nThis is a test.")

    await mgr._scan_async()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path='Shared/test-note.md'"
        )
    assert row is not None
    assert row["origin_node"] == MY_NODE
    assert row["is_deleted"] is False

    mock_event_queue.add.assert_called()
    call_kw = mock_event_queue.add.call_args.kwargs
    assert call_kw["event_type"] == "NEW"


@pytest.mark.anyio
async def test_scan_modified_file(pool, setup_peer, tmp_vault, mock_event_queue):
    """Changed file detected via hash, UPDATE event queued with correct base_hash."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    test_file = shared / "modify-test.md"

    # Initial scan
    test_file.write_text("# Version 1")
    await mgr._scan_async()
    mock_event_queue.add.reset_mock()

    # Modify and clear stat cache
    time.sleep(0.01)
    test_file.write_text("# Version 2")
    mgr._stat_cache.clear()

    await mgr._scan_async()

    assert mock_event_queue.add.called
    call_kw = mock_event_queue.add.call_args.kwargs
    assert call_kw["event_type"] == "UPDATE"


@pytest.mark.anyio
async def test_scan_deleted_file(pool, setup_peer, tmp_vault, mock_event_queue):
    """Missing file → tombstone + FORGET event."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    test_file = shared / "delete-me.md"
    test_file.write_text("# Will be deleted")

    await mgr._scan_async()
    mock_event_queue.add.reset_mock()

    test_file.unlink()
    mgr._stat_cache.clear()

    await mgr._scan_async()

    assert mock_event_queue.add.called
    call_kw = mock_event_queue.add.call_args.kwargs
    assert call_kw["event_type"] == "FORGET"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path='Shared/delete-me.md'"
        )
    assert row["is_deleted"] is True


@pytest.mark.anyio
async def test_truncated_scan_emits_no_forget(pool, setup_peer, tmp_vault, mock_event_queue, monkeypatch):
    """A scan that hits the backpressure cap must delete nothing.

    Regression test for the 2026-08-25 FORGET storm. `seen_paths` is the only
    record of "this file exists on disk", so a glob that breaks early on
    MAX_FILES_PER_SCAN leaves present files out of it, and the deletion pass
    reads "not in seen_paths" as "deleted" and tombstones them. On the live
    node that produced 1,139 tombstones for files that were never deleted, and
    the resulting FORGET events removed real notes from a peer's vault.

    The positive control for this fix is test_scan_deleted_file above: a
    COMPLETE scan must still emit a FORGET for a genuinely absent file.
    """
    import api.vault_sync as vs

    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    for i in range(6):
        (shared / f"trunc-{i:02d}.md").write_text(f"# note {i}")

    # Register all six with a cap high enough that the scan completes.
    monkeypatch.setattr(vs, "MAX_FILES_PER_SCAN", 100)
    await mgr._scan_async()

    # Force truncation: a cold stat cache makes every file look changed, so a
    # tiny cap breaks the glob after two files. This is exactly the state a
    # service restart produces, which is what triggered the live incident.
    mock_event_queue.add.reset_mock()
    mgr._stat_cache.clear()
    monkeypatch.setattr(vs, "MAX_FILES_PER_SCAN", 2)
    await mgr._scan_async()

    forgets = [
        c for c in mock_event_queue.add.call_args_list
        if c.kwargs.get("event_type") == "FORGET"
    ]
    assert forgets == [], (
        f"truncated scan emitted {len(forgets)} FORGET event(s); a partial "
        f"enumeration cannot distinguish 'absent' from 'not reached'"
    )

    async with pool.acquire() as conn:
        tombstoned = await conn.fetchval(
            "SELECT count(*) FROM vault_sync_state "
            "WHERE relative_path LIKE 'Shared/trunc-%' AND is_deleted=TRUE"
        )
        # This test registers six extra rows; remove them so later tests that
        # assert on row counts are not perturbed by it.
        await conn.execute("DELETE FROM vault_sync_state WHERE relative_path LIKE 'Shared/trunc-%'")
    assert tombstoned == 0, f"{tombstoned} present file(s) tombstoned by a truncated scan"


@pytest.mark.anyio
async def test_cold_cache_does_not_truncate_and_real_delete_still_forgets(
    pool, setup_peer, tmp_vault, mock_event_queue, monkeypatch
):
    """After a restart, unchanged files must not consume the scan budget.

    This is the positive control for the A1 truncation guard. A1 alone makes a
    truncated scan safe by refusing to delete — but a cold _stat_cache makes
    EVERY file look changed, so every post-restart scan truncates and genuine
    deletions are never propagated. A2 compares the freshly computed hash
    against the DB, so unchanged files cost nothing, the scan completes, and a
    real deletion is still reported.

    Without A2 this test fails by emitting zero FORGETs: correct-but-useless.
    """
    import api.vault_sync as vs

    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    for i in range(6):
        (shared / f"cold-{i:02d}.md").write_text(f"# note {i}")
    doomed = shared / "cold-doomed.md"
    doomed.write_text("# genuinely deleted later")

    monkeypatch.setattr(vs, "MAX_FILES_PER_SCAN", 100)
    await mgr._scan_async()

    # Simulate a service restart: in-memory cache gone, tiny budget.
    mock_event_queue.add.reset_mock()
    mgr._stat_cache.clear()
    monkeypatch.setattr(vs, "MAX_FILES_PER_SCAN", 2)
    doomed.unlink()

    await mgr._scan_async()

    forgets = [
        c for c in mock_event_queue.add.call_args_list
        if c.kwargs.get("event_type") == "FORGET"
    ]
    rids = {c.kwargs["rid"] for c in forgets}
    async with pool.acquire() as conn:
        survivors = await conn.fetchval(
            "SELECT count(*) FROM vault_sync_state "
            "WHERE relative_path LIKE 'Shared/cold-%' "
            "AND relative_path <> 'Shared/cold-doomed.md' AND is_deleted=TRUE"
        )
        await conn.execute("DELETE FROM vault_sync_state WHERE relative_path LIKE 'Shared/cold-%'")

    assert survivors == 0, f"{survivors} present file(s) tombstoned on a cold-cache scan"
    assert rids == {"orn:koi-net.vault-file:Shared/cold-doomed.md"}, (
        f"expected exactly the genuinely deleted file to be forgotten, got {rids}"
    )


def test_vault_file_manifest_is_koi_net_conformant():
    """Our vault-file manifest must satisfy the upstream KOI-net Manifest contract.

    rid_lib.ext.Manifest requires exactly three fields — rid, timestamp,
    sha256_hash — and koi_net.protocol.Event.manifest is typed as that model.
    We were emitting `content_hash` (the same sha256 value under a different
    name) and no `rid` at all, so a stock KOI-net node rejected every vault-file
    event we sent with:

        2 validation errors for Event
          manifest.rid          Field required
          manifest.sha256_hash  Field required

    Verified against koi-net 2.1.2 + rid-lib 3.3.0 on 2026-08-26 using real
    payloads pulled from koi_net_events. Domain events were unaffected: they
    carry manifest=None, so there is nothing to validate.

    We already held all three values, so this is a naming mismatch, not a
    missing capability. The extension fields (relative_path, origin_seq,
    origin_node, base_hash, bytes, deleted, content_hash) are additive and
    survive, because pydantic ignores unknown keys by default.

    This test asserts the contract WITHOUT importing koi-net, so it does not add
    a dependency to run the suite.
    """
    UPSTREAM_MANIFEST_REQUIRED = {"rid", "timestamp", "sha256_hash"}

    relative_path = "Shared/conformance-probe.md"
    content_hash = "a" * 64
    manifest = _build_vault_manifest(
        relative_path=relative_path,
        content_hash=content_hash,
        base_hash=None,
        origin_node="orn:koi-net.node:test+deadbeef",
        origin_seq=7,
        file_size=123,
        event_type="UPDATE",
        now="2026-08-26T00:00:00+00:00",
    )

    missing = UPSTREAM_MANIFEST_REQUIRED - set(manifest)
    assert not missing, f"manifest is missing upstream-required field(s): {missing}"

    assert manifest["rid"] == f"orn:koi-net.vault-file:{relative_path}", (
        "manifest.rid must be the event's RID"
    )
    assert manifest["sha256_hash"] == content_hash, (
        "manifest.sha256_hash must carry the same value as content_hash"
    )
    # The extensions that make our vault sync work must not be dropped.
    for ext in ("relative_path", "origin_seq", "origin_node", "bytes", "deleted", "content_hash"):
        assert ext in manifest, f"extension field {ext} was lost"


@pytest.mark.anyio
async def test_unreadable_file_is_not_tombstoned(pool, setup_peer, tmp_vault, mock_event_queue, monkeypatch):
    """A file that exists but cannot be READ must not be reported as deleted.

    _reconcile() enumerates the folder with rglob and hashes each file. If
    read_bytes() raises OSError it does `continue`, so the path never enters
    disk_files — and the very next loop reads "in db_map, not in disk_files"
    as `missing_on_disk`, which the repair path tombstones and FORGETs to
    every peer.

    rglob already yielded the path, so the file demonstrably EXISTS. The only
    thing that happened is that this process could not read it: a permissions
    change, a transient I/O error, a cloud-sync placeholder not yet
    materialised, a file locked by another writer. Every one of those is
    temporary, and every one of them currently produces a permanent
    cross-peer delete.

    This is the same defect class as the truncated-scan storm (A1) but on the
    reconcile path, which has NO cap and therefore no scan_truncated guard:
    "I could not look at it" is being recorded as "it is not there".
    """
    import api.vault_sync as vs

    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    readable = shared / "unread-keeper.md"
    readable.write_text("# readable")
    unreadable = shared / "unread-target.md"
    unreadable.write_text("# exists but will fail to read")

    await mgr._scan_async()
    async with pool.acquire() as conn:
        registered = await conn.fetchval(
            "SELECT count(*) FROM vault_sync_state WHERE relative_path LIKE 'Shared/unread-%'"
        )
    assert registered == 2, f"setup: expected 2 registered rows, got {registered}"

    # Make exactly one file unreadable, leaving it present on disk.
    real_read_bytes = Path.read_bytes

    def _selective_read(self, *a, **kw):
        if self.name == "unread-target.md":
            raise OSError(13, "Permission denied")
        return real_read_bytes(self, *a, **kw)

    mock_event_queue.add.reset_mock()
    monkeypatch.setattr(Path, "read_bytes", _selective_read)
    result = await mgr.reconcile(mode="repair", confirm=True)
    monkeypatch.undo()

    assert unreadable.exists(), "precondition: the file must still be on disk"

    assert "Shared/unread-target.md" not in result.get("missing_on_disk", []), (
        "a file that exists but could not be read must not be reported missing_on_disk; "
        f"got {result.get('missing_on_disk')}"
    )

    forgets = [
        c for c in mock_event_queue.add.call_args_list
        if c.kwargs.get("event_type") == "FORGET"
    ]
    assert not forgets, f"no FORGET may be queued for an unreadable-but-present file, got {forgets}"

    async with pool.acquire() as conn:
        tombstoned = await conn.fetchval(
            "SELECT count(*) FROM vault_sync_state "
            "WHERE relative_path LIKE 'Shared/unread-%' AND is_deleted=TRUE"
        )
    assert tombstoned == 0, f"expected 0 tombstones, got {tombstoned}"


@pytest.mark.anyio
async def test_genuinely_absent_file_is_still_tombstoned(pool, setup_peer, tmp_vault, mock_event_queue):
    """Positive control for the above.

    Without this, making _reconcile refuse to tombstone anything at all would
    pass the unreadable-file test while silently disabling deletion entirely —
    which is exactly the "safe and wrong" outcome A2 was written to catch.
    """
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    doomed = shared / "gone-target.md"
    doomed.write_text("# will be really deleted")

    await mgr._scan_async()

    mock_event_queue.add.reset_mock()
    doomed.unlink()
    assert not doomed.exists()

    result = await mgr.reconcile(mode="repair", confirm=True)

    assert "Shared/gone-target.md" in result.get("missing_on_disk", []), (
        "a genuinely deleted file must still be reported missing_on_disk"
    )
    forgets = [
        c for c in mock_event_queue.add.call_args_list
        if c.kwargs.get("event_type") == "FORGET"
    ]
    assert any("gone-target.md" in str(c) for c in forgets), (
        f"a real deletion must still emit a FORGET, got {forgets}"
    )


@pytest.mark.anyio
async def test_excluded_path_is_not_tombstoned(pool, setup_peer, tmp_vault, mock_event_queue, monkeypatch):
    """An excluded path with a state row must not be read as deleted.

    The glob skips excluded paths BEFORE adding them to seen_paths, but the
    deletion query selected every tracked row regardless — so any excluded path
    that had a state row was in the DB set and absent from seen_paths by
    construction, and got tombstoned with a FORGET broadcast to every peer.

    Live consequence: KOI_VAULT_EXCLUDE_PATHS matches '*(conflict *).md' and
    _create_conflict_copy registered the copy it had just written, so every
    conflict copy self-destructed seconds after creation. On one peer 992 of 994
    false tombstones were conflict paths.
    """
    import api.vault_sync as vs

    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    keeper = shared / "excl-keeper.md"
    keeper.write_text("# not excluded")
    excluded = shared / "excl-target (conflict 2026-01-01 00-00-00).md"
    excluded.write_text("# a conflict copy")

    # Register both while nothing is excluded.
    monkeypatch.setattr(vs, "_VAULT_EXCLUDE_PATTERNS", [])
    await mgr._scan_async()

    async with pool.acquire() as conn:
        registered = await conn.fetchval(
            "SELECT count(*) FROM vault_sync_state WHERE relative_path LIKE 'Shared/excl-%'"
        )
    assert registered == 2, f"setup: expected 2 registered rows, got {registered}"

    # Now exclude the conflict copy, as the live config does.
    mock_event_queue.add.reset_mock()
    mgr._stat_cache.clear()
    monkeypatch.setattr(vs, "_VAULT_EXCLUDE_PATTERNS", ["*(conflict *).md"])
    await mgr._scan_async()

    forgets = [
        c for c in mock_event_queue.add.call_args_list
        if c.kwargs.get("event_type") == "FORGET"
    ]
    async with pool.acquire() as conn:
        tombstoned = await conn.fetchval(
            "SELECT count(*) FROM vault_sync_state "
            "WHERE relative_path LIKE 'Shared/excl-%' AND is_deleted=TRUE"
        )
        await conn.execute("DELETE FROM vault_sync_state WHERE relative_path LIKE 'Shared/excl-%'")

    assert tombstoned == 0, f"{tombstoned} excluded path(s) tombstoned while present on disk"
    assert forgets == [], f"excluded path produced {len(forgets)} FORGET event(s)"
    assert excluded.exists(), "the excluded file itself must be left alone"


@pytest.mark.anyio
async def test_scan_skips_unsupported_extensions(pool, setup_peer, tmp_vault, mock_event_queue):
    """Files with extensions not in VAULT_SYNC_PATTERNS (e.g. .pdf, .png) are ignored.

    History: this test was originally `test_scan_skips_non_md` on the
    assumption only Markdown was synced. Since commit 400e9c87 the
    supported set expanded to include .md/.jsonl/.json/.jsonld/.txt/
    .csv/.yaml/.yml/.toml, so using .txt as a negative case no longer
    holds. .pdf + .png remain safely outside the set.
    """
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    (shared / "doc.pdf").write_bytes(b"%PDF-1.4 fake")
    (shared / "img.png").write_bytes(b"\x89PNG\r\n\x1a\nfake")

    await mgr._scan_async()
    mock_event_queue.add.assert_not_called()


@pytest.mark.anyio
async def test_scan_skips_symlinks(pool, setup_peer, tmp_vault, mock_event_queue):
    """Symlinks are not followed."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    real_file = tmp_vault / "outside.md"
    real_file.write_text("# Outside")
    symlink = shared / "link.md"
    symlink.symlink_to(real_file)

    await mgr._scan_async()
    mock_event_queue.add.assert_not_called()


@pytest.mark.anyio
async def test_scan_rejects_oversize(pool, setup_peer, tmp_vault, mock_event_queue):
    """Files > 1MB are skipped."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    big_file = shared / "huge.md"
    big_file.write_text("x" * 1_048_577)

    await mgr._scan_async()
    mock_event_queue.add.assert_not_called()


# =============================================================================
# Apply tests
# =============================================================================


def _make_apply_kwargs(rel_path, event_type, content, base_hash=None, origin_seq=1, deleted=False):
    """Helper to build apply_event kwargs."""
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
        "origin_node": PEER_NODE,
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


@pytest.mark.anyio
async def test_apply_new_file(pool, setup_peer, tmp_vault, mock_event_queue):
    """Incoming NEW → file written to disk + sync state inserted."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    content = "# From Peer\n\nHello!"

    await mgr.apply_event(**_make_apply_kwargs("Shared/peer-note.md", "NEW", content))

    target = tmp_vault / "Shared" / "peer-note.md"
    assert target.exists()
    assert target.read_text() == content

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path='Shared/peer-note.md'"
        )
    assert row is not None
    assert row["origin_node"] == PEER_NODE


@pytest.mark.anyio
async def test_apply_idempotent(pool, setup_peer, tmp_vault, mock_event_queue):
    """Same event_id redelivered → no-op."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    content = "# Idempotent"
    kwargs = _make_apply_kwargs("Shared/idem.md", "NEW", content)

    await mgr.apply_event(**kwargs)
    await mgr.apply_event(**kwargs)  # Second delivery

    shared = tmp_vault / "Shared"
    md_files = list(shared.glob("idem*.md"))
    assert len(md_files) == 1


@pytest.mark.anyio
async def test_apply_conflict(pool, setup_peer, tmp_vault, mock_event_queue):
    """Incoming UPDATE where local hash != base_hash → conflict copy."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    local_content = "# Local Version"
    local_hash = _sha256(local_content)
    local_file = tmp_vault / "Shared" / "conflict-test.md"
    local_file.write_text(local_content)

    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, $4, NOW())""",
            "Shared/conflict-test.md", local_hash, MY_NODE,
            len(local_content.encode("utf-8")),
        )

    peer_content = "# Peer Version"
    wrong_base = _sha256("# Some Other Version")
    kwargs = _make_apply_kwargs("Shared/conflict-test.md", "UPDATE", peer_content,
                                base_hash=wrong_base, origin_seq=2)

    await mgr.apply_event(**kwargs)

    # Local file unchanged
    assert local_file.read_text() == local_content

    # Conflict copy created
    shared = tmp_vault / "Shared"
    conflict_files = [f for f in shared.glob("conflict-test (conflict*).md")]
    assert len(conflict_files) == 1
    assert conflict_files[0].read_text() == peer_content


@pytest.mark.anyio
async def test_apply_safe_update(pool, setup_peer, tmp_vault, mock_event_queue):
    """Incoming UPDATE where local hash == base_hash → overwrite."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    local_content = "# Base Version"
    local_hash = _sha256(local_content)
    local_file = tmp_vault / "Shared" / "safe-update.md"
    local_file.write_text(local_content)

    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, $4, NOW())""",
            "Shared/safe-update.md", local_hash, MY_NODE,
            len(local_content.encode("utf-8")),
        )

    peer_content = "# Updated Version"
    kwargs = _make_apply_kwargs("Shared/safe-update.md", "UPDATE", peer_content,
                                base_hash=local_hash, origin_seq=2)

    await mgr.apply_event(**kwargs)

    assert local_file.read_text() == peer_content

    # No conflict copies
    shared = tmp_vault / "Shared"
    conflict_files = [f for f in shared.glob("safe-update (conflict*).md")]
    assert len(conflict_files) == 0


@pytest.mark.anyio
async def test_apply_stale_delete(pool, setup_peer, tmp_vault, mock_event_queue):
    """Incoming FORGET where local hash != base_hash → delete ignored."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    local_content = "# Edited After Peer's Base"
    local_hash = _sha256(local_content)
    local_file = tmp_vault / "Shared" / "stale-delete.md"
    local_file.write_text(local_content)

    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 2, $4, NOW())""",
            "Shared/stale-delete.md", local_hash, MY_NODE,
            len(local_content.encode("utf-8")),
        )

    old_base = _sha256("# Original Version")
    kwargs = _make_apply_kwargs("Shared/stale-delete.md", "FORGET", None, base_hash=old_base)
    # Fix: FORGET has no markdown
    kwargs["contents"] = {"relative_path": "Shared/stale-delete.md", "_vault_sync": True}
    kwargs["manifest"]["content_hash"] = old_base
    kwargs["manifest"]["deleted"] = True

    await mgr.apply_event(**kwargs)

    assert local_file.exists()
    assert local_file.read_text() == local_content


@pytest.mark.anyio
async def test_apply_safe_delete(pool, setup_peer, tmp_vault, mock_event_queue):
    """Incoming FORGET where local hash == base_hash → file removed."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    content = "# To Be Deleted"
    chash = _sha256(content)
    local_file = tmp_vault / "Shared" / "safe-delete.md"
    local_file.write_text(content)

    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, $4, NOW())""",
            "Shared/safe-delete.md", chash, PEER_NODE,
            len(content.encode("utf-8")),
        )

    kwargs = _make_apply_kwargs("Shared/safe-delete.md", "FORGET", None, base_hash=chash, origin_seq=2)
    kwargs["contents"] = {"relative_path": "Shared/safe-delete.md", "_vault_sync": True}
    kwargs["manifest"]["content_hash"] = chash
    kwargs["manifest"]["deleted"] = True

    await mgr.apply_event(**kwargs)

    assert not local_file.exists()

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path='Shared/safe-delete.md'"
        )
    assert row["is_deleted"] is True


@pytest.mark.anyio
async def test_apply_delete_edit_conflict(pool, setup_peer, tmp_vault, mock_event_queue):
    """UPDATE arrives for deleted file → file recreated."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    # File was deleted locally (tombstone in DB)
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes,
                last_modified_at, is_deleted, deleted_at)
               VALUES ($1, $2, $3, 1, 0, NOW(), TRUE, NOW())""",
            "Shared/revived.md", _sha256("old"), MY_NODE,
        )

    new_content = "# Revived by peer"
    kwargs = _make_apply_kwargs("Shared/revived.md", "UPDATE", new_content,
                                base_hash=_sha256("old"), origin_seq=2)

    await mgr.apply_event(**kwargs)

    target = tmp_vault / "Shared" / "revived.md"
    assert target.exists()
    assert target.read_text() == new_content


@pytest.mark.anyio
async def test_path_traversal_rejected(pool, setup_peer, tmp_vault, mock_event_queue):
    """../etc/passwd in relative_path → rejected."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    await mgr.apply_event(
        rid="orn:koi-net.vault-file:Shared/../../../etc/passwd",
        event_type="NEW",
        contents={"markdown": "hacked", "relative_path": "Shared/../../../etc/passwd", "_vault_sync": True},
        manifest={"content_hash": _sha256("hacked"),
                  "relative_path": "Shared/../../../etc/passwd",
                  "bytes": 6, "origin_node": PEER_NODE, "origin_seq": 1,
                  "timestamp": datetime.now(timezone.utc).isoformat()},
        source_node=PEER_NODE,
        event_id=str(uuid.uuid4()),
    )

    total_rejected = sum(mgr._rejected_counts.values())
    assert total_rejected > 0


@pytest.mark.anyio
async def test_absolute_path_rejected(pool, setup_peer, tmp_vault, mock_event_queue):
    """/etc/passwd → rejected."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    await mgr.apply_event(
        rid="orn:koi-net.vault-file:/etc/passwd",
        event_type="NEW",
        contents={"markdown": "hacked", "relative_path": "/etc/passwd", "_vault_sync": True},
        manifest={"content_hash": _sha256("hacked"),
                  "relative_path": "/etc/passwd",
                  "bytes": 6, "origin_node": PEER_NODE, "origin_seq": 1,
                  "timestamp": datetime.now(timezone.utc).isoformat()},
        source_node=PEER_NODE,
        event_id=str(uuid.uuid4()),
    )

    total_rejected = sum(mgr._rejected_counts.values())
    assert total_rejected > 0


@pytest.mark.anyio
async def test_atomic_write(pool, tmp_vault, mock_event_queue):
    """File written via tmp+rename pattern."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    target = tmp_vault / "Shared" / "atomic-test.md"
    content = "# Atomic Write Test"

    await mgr._atomic_write(target, content)

    assert target.exists()
    assert target.read_text() == content
    tmp_files = list((tmp_vault / "Shared").glob("*.tmp.*"))
    assert len(tmp_files) == 0


@pytest.mark.anyio
async def test_conflict_copy_naming(pool, setup_peer, tmp_vault, mock_event_queue):
    """Conflict copy follows (conflict YYYY-MM-DD HH-MM-SS).md pattern."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    local_content = "# Local"
    local_hash = _sha256(local_content)
    local_file = tmp_vault / "Shared" / "naming-test.md"
    local_file.write_text(local_content)

    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, $4, NOW())""",
            "Shared/naming-test.md", local_hash, MY_NODE,
            len(local_content.encode("utf-8")),
        )

    peer_content = "# Peer"
    kwargs = _make_apply_kwargs("Shared/naming-test.md", "UPDATE", peer_content,
                                base_hash=_sha256("wrong base"), origin_seq=2)

    await mgr.apply_event(**kwargs)

    shared = tmp_vault / "Shared"
    conflict_files = list(shared.glob("naming-test (conflict*).md"))
    assert len(conflict_files) == 1

    name = conflict_files[0].name
    assert re.match(r"naming-test \(conflict \d{4}-\d{2}-\d{2} \d{2}-\d{2}-\d{2}\)\.md", name)


# =============================================================================
# WP1: Metrics tests
# =============================================================================


@pytest.mark.anyio
async def test_metrics_increment(pool, setup_peer, tmp_vault, mock_event_queue):
    """Scan and apply bump the correct counters."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"

    # Create a file and scan → events_queued should increase
    (shared / "metric1.md").write_text("# Metric Test")
    await mgr._scan_async()
    assert mgr._metrics.events_queued >= 1
    assert mgr._metrics.files_scanned >= 1

    # Apply an event → events_applied should increase
    content = "# Applied"
    await mgr.apply_event(**_make_apply_kwargs("Shared/applied.md", "NEW", content))
    assert mgr._metrics.events_applied >= 1

    # Apply duplicate → events_skipped_dedup should increase
    kwargs = _make_apply_kwargs("Shared/dedup-test.md", "NEW", "# Dedup")
    await mgr.apply_event(**kwargs)
    await mgr.apply_event(**kwargs)
    assert mgr._metrics.events_skipped_dedup >= 1


@pytest.mark.anyio
async def test_metrics_persist_load(pool, setup_peer, tmp_vault, mock_event_queue):
    """Persist metrics, create new manager, verify restored."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    (shared / "persist-test.md").write_text("# Persist")
    await mgr._scan_async()

    original_queued = mgr._metrics.events_queued
    assert original_queued >= 1

    # Persist
    await mgr.persist_metrics()

    # New manager, load metrics
    mgr2 = _make_manager(pool, tmp_vault, mock_event_queue)
    assert mgr2._metrics.events_queued == 0  # Fresh

    await mgr2.load_metrics()
    assert mgr2._metrics.events_queued == original_queued


@pytest.mark.anyio
async def test_status_contains_metrics(pool, setup_peer, tmp_vault, mock_event_queue):
    """get_status() includes metrics key."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    status = await mgr.get_status()

    assert "metrics" in status
    assert "schema_version" in status["metrics"]
    assert "events_queued" in status["metrics"]
    assert "scans_completed" in status["metrics"]
    # Backward compat
    assert "rejected_events" in status


@pytest.mark.anyio
async def test_scan_apply_concurrency(pool, setup_peer, tmp_vault, mock_event_queue):
    """Launch scan and apply_event concurrently — no false conflict, consistent state."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"

    # Create a file for scanning
    (shared / "concurrent-scan.md").write_text("# Scan Side")

    # Also prepare an apply event for a different file
    apply_content = "# Apply Side"
    apply_kwargs = _make_apply_kwargs("Shared/concurrent-apply.md", "NEW", apply_content)

    # Run scan and apply concurrently
    await asyncio.gather(
        mgr._scan_async(),
        mgr.apply_event(**apply_kwargs),
    )

    # Verify both files have consistent state
    async with pool.acquire() as conn:
        scan_row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path='Shared/concurrent-scan.md'"
        )
        apply_row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path='Shared/concurrent-apply.md'"
        )

    assert scan_row is not None
    assert apply_row is not None
    assert apply_row["origin_node"] == PEER_NODE

    # No conflict copies for the applied file
    conflict_files = list(shared.glob("concurrent-apply (conflict*).md"))
    assert len(conflict_files) == 0


@pytest.mark.anyio
async def test_large_scan_does_not_starve_apply(pool, setup_peer, tmp_vault, mock_event_queue):
    """Start a capped scan with 100+ files, fire apply_event, verify apply completes < 1s."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"

    # Create 120 small files
    for i in range(120):
        (shared / f"bulk-{i:04d}.md").write_text(f"# File {i}\n\nContent for bulk test.")

    apply_content = "# Urgent Apply"
    apply_kwargs = _make_apply_kwargs("Shared/urgent.md", "NEW", apply_content)

    # Start scan in background, fire apply
    scan_task = asyncio.create_task(mgr._scan_async())

    # Give scan a tiny head start
    await asyncio.sleep(0.05)

    start = time.monotonic()
    await mgr.apply_event(**apply_kwargs)
    elapsed = time.monotonic() - start

    # Wait for scan to finish
    await scan_task

    # SLO: apply_event returns in < 1s
    assert elapsed < 1.0, f"apply_event took {elapsed:.2f}s (SLO: < 1s)"

    # File was written
    assert (shared / "urgent.md").exists()


# =============================================================================
# WP3: Backpressure tests
# =============================================================================


@pytest.mark.anyio
async def test_backpressure_file_cap(pool, setup_peer, tmp_vault, mock_event_queue):
    """150 files, cap=50 → at most 50 events queued in one cycle."""
    import api.vault_sync as vs
    original = vs.MAX_FILES_PER_SCAN
    vs.MAX_FILES_PER_SCAN = 50
    try:
        mgr = _make_manager(pool, tmp_vault, mock_event_queue)
        shared = tmp_vault / "Shared"
        for i in range(150):
            (shared / f"cap-{i:04d}.md").write_text(f"# Cap test {i}")

        await mgr._scan_async()

        # Events queued should be <= 50
        assert mgr._metrics.events_queued <= 50
        assert mgr._metrics.scans_capped >= 1
    finally:
        vs.MAX_FILES_PER_SCAN = original


@pytest.mark.anyio
async def test_backpressure_byte_cap(pool, setup_peer, tmp_vault, mock_event_queue):
    """Large files with 5MB cap → scan stops mid-batch."""
    import api.vault_sync as vs
    original = vs.MAX_BYTES_PER_SCAN
    vs.MAX_BYTES_PER_SCAN = 5 * 1024 * 1024  # 5 MB
    try:
        mgr = _make_manager(pool, tmp_vault, mock_event_queue)
        shared = tmp_vault / "Shared"
        # Create 20 files of ~500KB each (10 MB total, should cap at ~10)
        for i in range(20):
            (shared / f"big-{i:04d}.md").write_text("x" * 500_000)

        await mgr._scan_async()

        assert mgr._metrics.scans_capped >= 1
        # Should have queued roughly 10 files (5MB / 500KB)
        assert mgr._metrics.events_queued <= 12
    finally:
        vs.MAX_BYTES_PER_SCAN = original


@pytest.mark.anyio
async def test_backpressure_event_cap_includes_deletes(pool, setup_peer, tmp_vault, mock_event_queue):
    """Total event cap=10 across creates and deletes."""
    import api.vault_sync as vs
    orig_max = vs.MAX_EVENTS_PER_SCAN
    orig_reserve = vs.DELETE_EVENT_RESERVE
    vs.MAX_EVENTS_PER_SCAN = 10
    vs.DELETE_EVENT_RESERVE = 3
    try:
        mgr = _make_manager(pool, tmp_vault, mock_event_queue)
        shared = tmp_vault / "Shared"

        # Create 5 files, scan them
        for i in range(5):
            (shared / f"evt-{i}.md").write_text(f"# Event {i}")
        await mgr._scan_async()
        initial_queued = mgr._metrics.events_queued
        mock_event_queue.add.reset_mock()

        # Now create 10 more files and delete the original 5
        for i in range(5):
            (shared / f"evt-{i}.md").unlink()
        for i in range(5, 15):
            (shared / f"evt-{i}.md").write_text(f"# Event {i}")
        mgr._stat_cache.clear()

        # Reset metrics for clarity
        mgr._metrics.events_queued = 0
        await mgr._scan_async()

        # Total events this cycle should be capped at 10
        assert mgr._metrics.events_queued <= 10
    finally:
        vs.MAX_EVENTS_PER_SCAN = orig_max
        vs.DELETE_EVENT_RESERVE = orig_reserve


# =============================================================================
# WP2a: Reconcile detect tests
# =============================================================================


@pytest.mark.anyio
async def test_reconcile_detect_missing_on_disk(pool, setup_peer, tmp_vault, mock_event_queue):
    """File in DB but missing on disk → reported in missing_on_disk."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    # Insert a row for a file that doesn't exist on disk
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, 10, NOW())""",
            "Shared/gone.md", _sha256("# Gone"), MY_NODE,
        )

    result = await mgr.reconcile(mode="detect", peer_rid=PEER_NODE)
    assert "Shared/gone.md" in result["missing_on_disk"]
    assert result["total_drift"] >= 1


@pytest.mark.anyio
async def test_reconcile_detect_hash_mismatch(pool, setup_peer, tmp_vault, mock_event_queue):
    """File on disk has different hash than DB → hash_mismatch."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"

    (shared / "stale.md").write_text("# Current Version")

    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, 10, NOW())""",
            "Shared/stale.md", _sha256("# Old Version"), MY_NODE,
        )

    result = await mgr.reconcile(mode="detect", peer_rid=PEER_NODE)
    assert "Shared/stale.md" in result["hash_mismatch"]
    assert result["total_drift"] >= 1


@pytest.mark.anyio
async def test_reconcile_detect_missing_in_db(pool, setup_peer, tmp_vault, mock_event_queue):
    """File on disk but not in DB → missing_in_db."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"

    (shared / "new-untracked.md").write_text("# Untracked")

    result = await mgr.reconcile(mode="detect", peer_rid=PEER_NODE)
    assert "Shared/new-untracked.md" in result["missing_in_db"]
    assert result["total_drift"] >= 1


@pytest.mark.anyio
async def test_reconcile_vault_unavailable_raises(pool, setup_peer, tmp_vault, mock_event_queue):
    """Missing shared folder raises VaultUnavailableError."""
    from api.vault_sync import VaultUnavailableError

    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    # Remove the shared folder
    import shutil
    shutil.rmtree(tmp_vault / "Shared")

    with pytest.raises(VaultUnavailableError):
        await mgr.reconcile(mode="detect", peer_rid=PEER_NODE)


# =============================================================================
# WP2b: Reconcile repair tests
# =============================================================================


@pytest.mark.anyio
async def test_reconcile_repair_disabled_by_default(pool, setup_peer, tmp_vault, mock_event_queue):
    """Repair mode returns error when VAULT_SYNC_REPAIR_ENABLED is not set."""
    # This tests the router-level gate, but we can also verify behavior directly
    # by checking that the endpoint would return 403 (tested via router integration).
    # Here we test the manager-level repair which does work — the gate is at the router.
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    # Repair without confirm returns drift report + repair_requires_confirm
    result = await mgr.reconcile(mode="repair", confirm=False, peer_rid=PEER_NODE)
    assert result.get("repair_requires_confirm") is True


@pytest.mark.anyio
async def test_reconcile_repair_requires_confirm(pool, setup_peer, tmp_vault, mock_event_queue):
    """mode=repair without confirm returns drift report with repair_requires_confirm=True."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    (shared / "untracked.md").write_text("# Untracked")

    result = await mgr.reconcile(mode="repair", confirm=False, peer_rid=PEER_NODE)
    assert result["repair_requires_confirm"] is True
    assert result["total_drift"] >= 1
    assert "actions_taken" not in result


@pytest.mark.anyio
async def test_reconcile_repair_queues_events(pool, setup_peer, tmp_vault, mock_event_queue):
    """With confirm=True, queues correct event types."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"

    # missing_in_db — file on disk, not in DB
    (shared / "repair-new.md").write_text("# Repair New")

    # hash_mismatch — DB has old hash
    (shared / "repair-stale.md").write_text("# Current")
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, 10, NOW())""",
            "Shared/repair-stale.md", _sha256("# Old"), MY_NODE,
        )

    # missing_on_disk — DB has row, file doesn't exist
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO vault_sync_state
               (relative_path, content_hash, origin_node, origin_seq, file_size_bytes, last_modified_at)
               VALUES ($1, $2, $3, 1, 10, NOW())""",
            "Shared/repair-gone.md", _sha256("# Gone"), MY_NODE,
        )

    result = await mgr.reconcile(mode="repair", confirm=True, peer_rid=PEER_NODE)
    assert result["actions_taken"] == 3
    assert mock_event_queue.add.call_count >= 3

    # Verify DB state is consistent after repair
    async with pool.acquire() as conn:
        new_row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path='Shared/repair-new.md'"
        )
        assert new_row is not None
        assert new_row["is_deleted"] is False

        gone_row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path='Shared/repair-gone.md'"
        )
        assert gone_row["is_deleted"] is True


@pytest.mark.anyio
async def test_reconcile_repair_respects_paths(pool, setup_peer, tmp_vault, mock_event_queue):
    """Only repairs listed paths."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"

    (shared / "include-me.md").write_text("# Include")
    (shared / "skip-me.md").write_text("# Skip")

    result = await mgr.reconcile(
        mode="repair", confirm=True,
        paths=["Shared/include-me.md"],
        peer_rid=PEER_NODE,
    )
    assert result["actions_taken"] == 1

    # skip-me.md should not be in DB
    async with pool.acquire() as conn:
        skip_row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path='Shared/skip-me.md'"
        )
    assert skip_row is None


@pytest.mark.anyio
async def test_reconcile_repair_max_actions(pool, setup_peer, tmp_vault, mock_event_queue):
    """Caps at limit, returns remaining count."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"

    for i in range(10):
        (shared / f"cap-{i}.md").write_text(f"# Cap {i}")

    result = await mgr.reconcile(mode="repair", confirm=True, max_actions=3, peer_rid=PEER_NODE)
    assert result["actions_taken"] == 3
    assert result["capped"] is True
    assert result["remaining"] == 7


@pytest.mark.anyio
async def test_reconcile_repair_idempotent(pool, setup_peer, tmp_vault, mock_event_queue):
    """Second run finds no drift."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"

    (shared / "idempotent.md").write_text("# Idempotent")

    result1 = await mgr.reconcile(mode="repair", confirm=True, peer_rid=PEER_NODE)
    assert result1["actions_taken"] >= 1

    result2 = await mgr.reconcile(mode="repair", confirm=True, peer_rid=PEER_NODE)
    assert result2["total_drift"] == 0
    assert result2.get("actions_taken", 0) == 0


@pytest.mark.anyio
async def test_reconcile_repair_vault_unavailable(pool, setup_peer, tmp_vault, mock_event_queue):
    """Refuses repair when shared folder missing (503 at endpoint level)."""
    from api.vault_sync import VaultUnavailableError

    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    import shutil
    shutil.rmtree(tmp_vault / "Shared")

    with pytest.raises(VaultUnavailableError):
        await mgr.reconcile(mode="repair", confirm=True, peer_rid=PEER_NODE)


# =============================================================================
# WP4: Watcher tests
# =============================================================================


@pytest.mark.anyio
async def test_watcher_triggers_early_scan(pool, setup_peer, tmp_vault, mock_event_queue):
    """Setting the change_event triggers scan before interval."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    (shared / "watcher-test.md").write_text("# Watcher")

    # Set scan interval very high so timer-based scan won't trigger
    mgr.scan_interval = 9999
    mgr._last_scan_at = datetime.now(timezone.utc)

    # But set the watcher event
    mgr._change_event.set()

    await mgr.run_cycle()

    assert mgr._metrics.watcher_triggered_scans >= 1
    assert mgr._metrics.scans_completed >= 1


@pytest.mark.anyio
async def test_watcher_fail_open(pool, setup_peer, tmp_vault, mock_event_queue):
    """When watchdog import fails, periodic scan still works."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    (shared / "failopen.md").write_text("# Fail Open")

    # Mock the import to fail
    with patch.dict("sys.modules", {"watchdog": None, "watchdog.observers": None, "watchdog.events": None}):
        mgr.start_watcher()

    assert mgr._metrics.watcher_enabled is False

    # Periodic scan should still work
    await mgr.run_cycle()
    assert mgr._metrics.scans_completed >= 1


@pytest.mark.anyio
async def test_scan_works_without_watcher(pool, setup_peer, tmp_vault, mock_event_queue):
    """VAULT_SYNC_WATCHER=false — periodic scan fires on interval."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    (shared / "no-watcher.md").write_text("# No Watcher")

    with patch.dict(os.environ, {"VAULT_SYNC_WATCHER": "false"}):
        mgr.start_watcher()

    assert mgr._metrics.watcher_enabled is False
    assert mgr._watcher is None

    # Timer-based scan should work
    await mgr.run_cycle()
    assert mgr._metrics.scans_completed >= 1


# =============================================================================
# Multi-peer tests
# =============================================================================


async def _setup_peer_b(pool):
    """Add a second peer for multi-peer tests."""
    async with pool.acquire() as conn:
        await conn.execute(
            """INSERT INTO koi_net_nodes (node_rid, node_name, node_type, base_url, status)
               VALUES ($1, 'test-peer-b', 'FULL', 'http://10.0.0.3:58351', 'active')
               ON CONFLICT (node_rid) DO NOTHING""",
            PEER_NODE_B,
        )
        await conn.execute("DELETE FROM vault_sync_peers WHERE peer_node_rid=$1", PEER_NODE_B)
        await conn.execute(
            """INSERT INTO vault_sync_peers (peer_node_rid, shared_folder, enabled)
               VALUES ($1, 'Shared', TRUE)""",
            PEER_NODE_B,
        )


@pytest.mark.anyio
async def test_multi_peer_scan_queues_for_both(pool, setup_peer, tmp_vault, mock_event_queue):
    """Configure 2 peers, verify scan queues events for both."""
    await _setup_peer_b(pool)
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    (shared / "multi-peer.md").write_text("# Multi-peer test")

    await mgr._scan_async()

    # Should have queued events targeting both peers
    calls = mock_event_queue.add.call_args_list
    target_nodes = {c.kwargs.get("target_node") for c in calls}
    assert PEER_NODE in target_nodes
    assert PEER_NODE_B in target_nodes


@pytest.mark.anyio
async def test_forwarding_to_other_peers(pool, setup_peer, tmp_vault, mock_event_queue):
    """Apply event from peer A → verify queued (forwarded) for peer B."""
    from api.event_queue import EventQueue
    # Use real event queue for forwarding verification
    eq = EventQueue(pool, MY_NODE)
    await _setup_peer_b(pool)

    from api.vault_sync import VaultSyncManager
    mgr = VaultSyncManager(
        pool=pool,
        node_rid=MY_NODE,
        event_queue=eq,
        vault_path=str(tmp_vault),
    )

    content = "# Forwarded content"
    kwargs = _make_apply_kwargs("Shared/forward-test.md", "NEW", content)
    await mgr.apply_event(**kwargs)

    # Check that an event was queued for PEER_NODE_B (forwarded)
    async with pool.acquire() as conn:
        forwarded = await conn.fetchval(
            """SELECT COUNT(*) FROM koi_net_events
               WHERE rid = 'orn:koi-net.vault-file:Shared/forward-test.md'
               AND target_node = $1""",
            PEER_NODE_B,
        )
    assert forwarded >= 1

    # Cleanup
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM koi_net_events WHERE rid LIKE 'orn:koi-net.vault-file:Shared/forward-test%'"
        )


@pytest.mark.anyio
async def test_loop_detection(pool, setup_peer, tmp_vault, mock_event_queue):
    """Apply forwarded event → verify no re-forward to source (loop prevented)."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    content = "# Loop test"
    kwargs = _make_apply_kwargs("Shared/loop-test.md", "NEW", content)

    # Apply once
    await mgr.apply_event(**kwargs)

    # Try to apply same event_id again (simulating a forwarded loop)
    await mgr.apply_event(**kwargs)

    # Second apply should be deduped
    assert mgr._metrics.events_skipped_dedup >= 1
    assert mgr._metrics.events_applied == 1


@pytest.mark.anyio
async def test_seq_monotonic(pool, setup_peer, tmp_vault, mock_event_queue):
    """Edit → foreign apply → edit → verify seq always increases."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"

    # Local edit
    (shared / "seq-test.md").write_text("# Version 1")
    await mgr._scan_async()

    async with pool.acquire() as conn:
        row1 = await conn.fetchrow(
            "SELECT origin_seq, local_edit_seq FROM vault_sync_state WHERE relative_path='Shared/seq-test.md'"
        )
    assert row1 is not None
    seq_after_local = row1["local_edit_seq"]

    # Foreign apply (simulates event from peer)
    foreign_content = "# Foreign Version"
    kwargs = _make_apply_kwargs("Shared/seq-test.md", "UPDATE", foreign_content,
                                base_hash=_sha256("# Version 1"), origin_seq=5)
    await mgr.apply_event(**kwargs)

    # Local edit again
    time.sleep(0.01)
    (shared / "seq-test.md").write_text("# Version 3")
    mgr._stat_cache.clear()
    mock_event_queue.add.reset_mock()
    await mgr._scan_async()

    async with pool.acquire() as conn:
        row2 = await conn.fetchrow(
            "SELECT origin_seq, local_edit_seq FROM vault_sync_state WHERE relative_path='Shared/seq-test.md'"
        )
    # local_edit_seq should always increase
    assert row2["local_edit_seq"] > seq_after_local


@pytest.mark.anyio
async def test_source_allowlist(pool, setup_peer, tmp_vault, mock_event_queue):
    """Event from unconfigured peer → rejected with unauthorized_source."""
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)

    unknown_peer = "orn:koi-net.node:unknown-peer+xyz999"
    content = "# Unauthorized"
    kwargs = _make_apply_kwargs("Shared/unauthorized.md", "NEW", content)
    kwargs["source_node"] = unknown_peer

    await mgr.apply_event(**kwargs)

    assert mgr._metrics.rejected_unauthorized_source >= 1
    # File should NOT exist
    assert not (tmp_vault / "Shared" / "unauthorized.md").exists()


# ---------------------------------------------------------------------------
# Divergence 1: vault-file RID namespace migration (staged)
# ---------------------------------------------------------------------------

def test_vault_file_rid_accepts_both_namespaces():
    """Inbound must parse BOTH the legacy and the owned namespace.

    This is the expand half of expand/migrate/contract. Until every peer accepts
    both, emission stays on the legacy namespace; a peer that only understands
    one must never see the other.
    """
    from api.vault_sync import (
        parse_vault_file_rid, VAULT_FILE_NS_LEGACY, VAULT_FILE_NS_OWNED,
    )

    for ns in (VAULT_FILE_NS_LEGACY, VAULT_FILE_NS_OWNED):
        assert parse_vault_file_rid(f"orn:{ns}:Shared/x.md") == "Shared/x.md"
        # Nested paths are the whole reason we are not adopting obsidian.note.
        assert parse_vault_file_rid(f"orn:{ns}:Shared/a/b/c.md") == "Shared/a/b/c.md"

    # "not a vault-file RID" must be distinguishable from "empty path".
    assert parse_vault_file_rid("orn:personal-koi.entity:abc") is None
    assert parse_vault_file_rid("orn:koi-net.node:n+h") is None
    assert parse_vault_file_rid(None) is None
    assert parse_vault_file_rid(12345) is None


def test_vault_file_emission_namespace_is_legacy_by_default():
    """Emission must NOT move until the operator flips it.

    A silent cutover would hand every peer RIDs it has never seen. A peer that
    has been unreachable for months cannot have been upgraded.
    """
    from api.vault_sync import vault_file_rid, VAULT_FILE_NS_LEGACY

    rid = vault_file_rid("Shared/x.md")
    assert rid == f"orn:{VAULT_FILE_NS_LEGACY}:Shared/x.md", (
        "default emission must stay on the legacy namespace"
    )


def test_vault_file_sql_clause_matches_both_namespaces():
    """The cleanup/metrics queries must not go blind when emission flips.

    A single LIKE on the legacy prefix keeps parsing fine and simply stops
    matching — so retention would stop reaping and the pending-event gauge would
    read zero, both silently.
    """
    from api.vault_sync import (
        vault_file_rid_sql_clause, VAULT_FILE_NS_LEGACY, VAULT_FILE_NS_OWNED,
    )

    clause = vault_file_rid_sql_clause()
    assert f"orn:{VAULT_FILE_NS_LEGACY}:%" in clause
    assert f"orn:{VAULT_FILE_NS_OWNED}:%" in clause
    assert clause.startswith("(") and clause.endswith(")"), (
        "must be parenthesised or it breaks the surrounding AND"
    )


def test_owned_namespace_does_not_squat_the_protocol():
    """The point of the exercise: stop using a namespace rid_lib reserves."""
    from rid_lib.types import KoiNetNode, KoiNetEdge
    from api.vault_sync import VAULT_FILE_NS_OWNED

    reserved_root = KoiNetNode.namespace.split(".")[0]  # "koi-net"
    assert KoiNetEdge.namespace.split(".")[0] == reserved_root
    assert not VAULT_FILE_NS_OWNED.startswith(reserved_root + "."), (
        f"{VAULT_FILE_NS_OWNED} still squats the reserved {reserved_root}.* namespace"
    )


# ---------------------------------------------------------------------------
# A4: confirmed absence — never tombstone a file that is on disk right now
# ---------------------------------------------------------------------------

@pytest.mark.anyio
async def test_scan_will_not_tombstone_a_file_that_exists(pool, setup_peer, tmp_vault, mock_event_queue, monkeypatch):
    """The last-mile guard: re-stat the path immediately before tombstoning it.

    A1 stops a TRUNCATED scan from deleting, and A5 stops an unreadable file
    from being called absent. Both reason about coverage. This one does not
    reason at all — it asks the filesystem, at the moment of the write, whether
    the file is there.

    That matters because it is independent of every coverage judgement above it.
    On 2026-08-25 the storm tombstoned 1,339 paths of which 1,139 were still on
    disk; this check alone would have blocked all 1,139 regardless of whether
    the truncation logic was right.

    Simulated here the way it actually happens: the file is tracked in
    vault_sync_state and present on disk, but the glob does not yield it on this
    pass (an exclusion change, a case-fold quirk, a transient FS hiccup), so it
    lands in the deletion-candidate set of a scan that was NOT truncated.
    """
    import api.vault_sync as vs

    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    survivor = shared / "a4-present.md"
    survivor.write_text("# still here")

    await mgr._scan_async()
    async with pool.acquire() as conn:
        assert await conn.fetchval(
            "SELECT count(*) FROM vault_sync_state WHERE relative_path=$1", "Shared/a4-present.md"
        ) == 1, "setup: file must be tracked"

    # Make the glob blind to it while leaving it on disk.
    mock_event_queue.add.reset_mock()
    mgr._stat_cache.clear()
    real_rglob = Path.rglob

    def _blind_rglob(self, pattern):
        for hit in real_rglob(self, pattern):
            if hit.name == "a4-present.md":
                continue
            yield hit

    monkeypatch.setattr(Path, "rglob", _blind_rglob)
    await mgr._scan_async()
    monkeypatch.undo()

    assert survivor.exists(), "precondition: the file is still on disk"

    forgets = [c for c in mock_event_queue.add.call_args_list
               if c.kwargs.get("event_type") == "FORGET"]
    assert not any("a4-present.md" in str(c) for c in forgets), (
        f"a file present on disk must never be FORGET'd, got {forgets}"
    )
    async with pool.acquire() as conn:
        tombstoned = await conn.fetchval(
            "SELECT is_deleted FROM vault_sync_state WHERE relative_path=$1", "Shared/a4-present.md"
        )
    assert tombstoned is False, "a file present on disk must not be tombstoned"


@pytest.mark.anyio
async def test_scan_still_tombstones_a_genuinely_deleted_file(pool, setup_peer, tmp_vault, mock_event_queue):
    """Positive control: A4 must not become a blanket refusal to delete.

    Without this, `return` at the top of the deletion loop would pass the test
    above — the "safe and wrong" outcome A2's commit message warned about.
    """
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    doomed = shared / "a4-really-gone.md"
    doomed.write_text("# will be deleted")

    await mgr._scan_async()

    mock_event_queue.add.reset_mock()
    doomed.unlink()
    mgr._stat_cache.clear()
    await mgr._scan_async()

    forgets = [c for c in mock_event_queue.add.call_args_list
               if c.kwargs.get("event_type") == "FORGET"]
    assert any("a4-really-gone.md" in str(c) for c in forgets), (
        f"a genuinely deleted file must still emit a FORGET, got {forgets}"
    )


@pytest.mark.anyio
async def test_reconcile_repair_will_not_tombstone_a_file_that_exists(pool, setup_peer, tmp_vault, mock_event_queue, monkeypatch):
    """Same guard on the reconcile repair path (TOCTOU).

    reconcile enumerates, then repairs. A file recreated between those two
    steps is in missing_on_disk but present on disk by the time the tombstone
    is written.
    """
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    flapper = shared / "a4-recreated.md"
    flapper.write_text("# v1")

    await mgr._scan_async()

    # Blind reconcile's ENUMERATION to the file while it stays on disk, so it
    # lands in missing_on_disk. Deleting it instead would not exercise the
    # guard: reconcile(mode="repair") re-enumerates in the same call, so the
    # file would simply be seen again.
    mock_event_queue.add.reset_mock()
    real_rglob = Path.rglob

    def _blind_rglob(self, pattern):
        for hit in real_rglob(self, pattern):
            if hit.name == "a4-recreated.md":
                continue
            yield hit

    monkeypatch.setattr(Path, "rglob", _blind_rglob)
    result = await mgr.reconcile(mode="repair", confirm=True)
    monkeypatch.undo()

    assert flapper.exists(), "precondition: the file never left the disk"
    assert "Shared/a4-recreated.md" in result.get("missing_on_disk", []), (
        "premise: enumeration must have reported it missing, or this proves nothing"
    )
    forgets = [c for c in mock_event_queue.add.call_args_list
               if c.kwargs.get("event_type") == "FORGET"]
    assert not any("a4-recreated.md" in str(c) for c in forgets), (
        f"a file recreated before the tombstone write must not be FORGET'd, got {forgets}"
    )


@pytest.mark.anyio
async def test_applying_a_peer_forget_still_deletes_an_existing_file(pool, setup_peer, tmp_vault, mock_event_queue):
    """A4 must NOT be applied to inbound FORGETs.

    Sites 3 and 4 apply a DECISION A PEER ALREADY MADE. The file existing
    locally is the normal case there — that is the whole point of replicating a
    delete. Guarding those on exists() would silently break every legitimate
    remote deletion, which is the mirror-image failure of the storm.
    """
    mgr = _make_manager(pool, tmp_vault, mock_event_queue)
    shared = tmp_vault / "Shared"
    target = shared / "a4-remote-delete.md"
    target.write_text("# peer will delete this")

    await mgr._scan_async()
    assert target.exists()

    async with pool.acquire() as conn:
        local_row = await conn.fetchrow(
            "SELECT * FROM vault_sync_state WHERE relative_path=$1",
            "Shared/a4-remote-delete.md",
        )
    assert local_row is not None, "setup: must be tracked"

    await mgr._apply_forget(
        target_path=target,
        relative_path="Shared/a4-remote-delete.md",
        local_row=local_row,
        base_hash=local_row["content_hash"],
        origin_node=PEER_NODE,
        origin_seq=local_row["origin_seq"] + 1,
        source_node=PEER_NODE,
        event_id=str(uuid.uuid4()),
        rid="orn:koi-net.vault-file:Shared/a4-remote-delete.md",
    )

    assert not target.exists(), (
        "an inbound FORGET must still delete the local file; A4 belongs only on "
        "the paths where WE decide a file is absent"
    )
