"""Vault sync, public issue 5: a configured peer must not update a note this node authored by naming this node as
origin_node. Rows of the issue's table, plus a NEW and a FORGET that claim this node as origin, on a readonly path,
against a throwaway Postgres (see conftest.py) and a temp vault.

The fix and the issue's cases are Daniel Ari Friedman's (koi-processor-node issue 5 and pull request 16).
Run: python -B -m pytest -q -p no:cacheprovider tests/peer_guards
"""
import asyncio
import hashlib
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from api import vault_sync as vs
from peer_guards_db import open_pool

MY_NODE = "orn:koi-net.node:test-self+" + "1" * 64
PEER = "orn:koi-net.node:test-peer+" + "2" * 64
THIRD = "orn:koi-net.node:test-third+" + "3" * 64
PATH = "Shared/note.md"
ORIGINAL = "# Note\n\n" + "authored here, long enough to stay above the shrink guard.\n" * 20
CHANGED = ORIGINAL + "\nchanged from outside\n"


def sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@pytest.fixture
def ro(monkeypatch):
    """Shared/ is a readonly path and owner updates are accepted, as in the issue."""
    monkeypatch.setattr(vs, "_VAULT_READONLY_INCOMING_PATHS", ["Shared/"])
    monkeypatch.setattr(vs, "_VAULT_OWNER_UPDATE_ACCEPT", True)
    return monkeypatch


def apply(pg, tmp_path, stored_origin, event_type, origin, seq, content=CHANGED):
    """Seed the note as authored by `stored_origin` at seq 1, apply one event from PEER, return (file text, mgr)."""
    vault = tmp_path / "vault"
    (vault / "Shared").mkdir(parents=True)
    note = vault / PATH
    note.write_text(ORIGINAL)

    async def go():
        pool = await open_pool(pg)
        try:
            async with pool.acquire() as conn:
                await conn.execute("INSERT INTO koi_net_nodes (node_rid, node_name, node_type, status) "
                                   "VALUES ($1, 'test-peer', 'FULL', 'active')", PEER)
                await conn.execute("INSERT INTO vault_sync_peers (peer_node_rid, shared_folder, enabled) "
                                   "VALUES ($1, 'Shared', TRUE)", PEER)
                await conn.execute(
                    "INSERT INTO vault_sync_state (relative_path, content_hash, origin_node, origin_seq, "
                    "file_size_bytes, last_modified_at) VALUES ($1, $2, $3, 1, $4, NOW())",
                    PATH, sha(ORIGINAL), stored_origin, len(ORIGINAL.encode()))
            queue = AsyncMock()
            queue.add = AsyncMock(return_value=str(uuid.uuid4()))
            queue.node_rid = MY_NODE
            mgr = vs.VaultSyncManager(pool=pool, node_rid=MY_NODE, event_queue=queue, vault_path=str(vault))
            deleted = event_type == "FORGET"
            text = None if deleted else content
            manifest = {
                "content_hash": sha(text) if text else "0" * 64,
                "relative_path": PATH,
                "base_hash": sha(ORIGINAL),
                "bytes": len(text.encode("utf-8")) if text else 0,
                "origin_node": origin,
                "origin_seq": seq,
                "deleted": deleted,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            contents = {"relative_path": PATH, "_vault_sync": True}
            if text is not None:
                contents["markdown"] = text
            await mgr.apply_event(rid=f"orn:koi-net.vault-file:{PATH}", event_type=event_type, contents=contents,
                                  manifest=manifest, source_node=PEER, event_id=str(uuid.uuid4()))
            return mgr
        finally:
            await pool.close()
    mgr = asyncio.run(go())
    return (note.read_text() if note.exists() else None), mgr


# The issue's table. Stored origin, then the UPDATE's origin_node and origin_seq.

def test_issue5_row1_peer_origin_update_refused(ro, pg, tmp_path):
    text, mgr = apply(pg, tmp_path, MY_NODE, "UPDATE", PEER, 2)
    assert text == ORIGINAL


def test_issue5_row2_honest_echo_stops_at_stale_guard(ro, pg, tmp_path):
    text, mgr = apply(pg, tmp_path, MY_NODE, "UPDATE", MY_NODE, 1)
    assert text == ORIGINAL
    assert mgr._metrics.rejected_stale_event == 1


def test_issue5_row3_claim_of_this_nodes_origin_refused(ro, pg, tmp_path):
    text, mgr = apply(pg, tmp_path, MY_NODE, "UPDATE", MY_NODE, 2)
    assert text == ORIGINAL
    assert mgr._metrics.readonly_owner_update_accepted == 0


def test_issue5_row4_relay_naming_the_authoring_third_node_accepted(ro, pg, tmp_path):
    """Control: the honest relay shape still updates the note."""
    text, mgr = apply(pg, tmp_path, THIRD, "UPDATE", THIRD, 2)
    assert text == CHANGED
    assert mgr._metrics.readonly_owner_update_accepted == 1


def test_issue5_row5_flag_off_refused(ro, pg, tmp_path):
    ro.setattr(vs, "_VAULT_OWNER_UPDATE_ACCEPT", False)
    text, mgr = apply(pg, tmp_path, MY_NODE, "UPDATE", MY_NODE, 2)
    assert text == ORIGINAL


# NEW and FORGET claiming this node as origin, on the same readonly path.

def test_issue5_new_claiming_this_node_leaves_note_unchanged(ro, pg, tmp_path):
    text, mgr = apply(pg, tmp_path, MY_NODE, "NEW", MY_NODE, 2)
    assert text == ORIGINAL


def test_issue5_forget_claiming_this_node_leaves_note_in_place(ro, pg, tmp_path):
    text, mgr = apply(pg, tmp_path, MY_NODE, "FORGET", MY_NODE, 2)
    assert text == ORIGINAL
