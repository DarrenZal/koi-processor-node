"""The discourse-move id derivation is a PERSISTENT CONTRACT, pinned here by value.

`scripts/extract_deep_documents.py` writes document discourse moves with
`uuid5(DISCOURSE_NAMESPACE, f"{document_rid}:{move_type}:{key(title)}:{chunk_start}")`
and upserts `ON CONFLICT (id)`. Every row already in `session_discourse_moves`
(13,767 rows across 1,507 documents when this was written) was derived with
`key = lower + strip + collapse whitespace` — nothing else. The M6 fix (`cdc445c`)
rebound the module's `_norm` to the entity normalizer (`normalize_entity_text`,
which also folds `-`/`_` to a space and strips a leading `@`) and the move-id hash
input silently changed with it: 5,578 stored rows on 1,169 documents would no
longer match their own id, so a replay would INSERT a duplicate beside each stale
row it was meant to repair (re-review of PR #66, the one remaining merge blocker).

These tests pin EXACT UUIDs computed with the historical derivation. They are
literals on purpose: a test that recomputes the expected value through the code
under test cannot notice the code changing. The hyphen-free title is the positive
control — it is stable under both normalizers, so it passes before and after the
fix and proves the failing cases fail for the reason claimed.
"""
from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path

import asyncpg
import pytest

from api.resolution_primitives import normalize_entity_text
from tests import koi_test_targets as targets

REPO_ROOT = Path(__file__).resolve().parent.parent


def _edd():
    spec = importlib.util.spec_from_file_location(
        "edd_move_ids", REPO_ROOT / "scripts/extract_deep_documents.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


RID = "substack-corpus:test:move-id-pin"

# (move_type, title, chunk_start, historical uuid5) — the uuid is what the extractor
# stored for this input from migration 103 until cdc445c. Do not recompute it.
HYPHENATED = ("claim", "Berry's tool-ethics — repairability, locality", 12,
              uuid.UUID("4224e58a-0ce8-5fed-8189-c03f055a3cac"))
UNDERSCORED = ("evidence", "The AI-based economy is evolving a new_regime", 3,
               uuid.UUID("b197aa7c-fa1b-5962-a75e-bec97895295c"))
WHITESPACE_RUN = ("premise", "  Multiple   spaces\tand tabs  ", 7,
                  uuid.UUID("f940e60b-4151-5d6b-b2fb-9c31b5af8346"))
# Positive control: no `-`, `_`, `@` or whitespace run — identical under both
# normalizers, so its id is the same before and after the fix.
CONTROL = ("thesis", "Scenius is collective genius", 0,
           uuid.UUID("c1cf183b-0e86-5db9-9232-0dbb6f8954aa"))

DRIFTING = [HYPHENATED, UNDERSCORED, WHITESPACE_RUN]


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    c = await asyncpg.connect(targets.database_url())
    db = await c.fetchval("SELECT current_database()")
    assert db != "personal_koi", (
        f"refusing to run: connected to {db!r}. This test writes discourse rows; the "
        f"live table is not an acceptable target.")
    tx = c.transaction()
    await tx.start()
    yield c
    await tx.rollback()
    await c.close()


class TestTheDerivationIsPinned:
    @pytest.mark.parametrize("case", DRIFTING, ids=["hyphen", "underscore", "whitespace-run"])
    def test_a_title_the_entity_normalizer_would_rewrite_keeps_its_historical_id(self, case):
        edd = _edd()
        mt, title, cs, expected = case
        assert edd.discourse_move_id(RID, mt, title, cs) == expected

    def test_the_move_id_key_is_whitespace_only_and_not_the_entity_key(self):
        """The id key is lower + strip + collapse whitespace. It is deliberately NOT
        `merge_key` / `normalize_entity_text`, which stays the entity merge and
        identity key (M6). Re-aliasing one to the other re-opens the drift."""
        edd = _edd()
        assert edd.discourse_move_id_key("  GPT-4   turbo_x  @H ") == "gpt-4 turbo_x @h"
        assert edd._norm("GPT-4") == normalize_entity_text("GPT-4") == "gpt 4"
        assert edd.discourse_move_id_key is not edd._norm
        # The fixtures above discriminate: the two normalizers disagree on every
        # DRIFTING title and agree on the CONTROL title only.
        for _mt, t, _cs, _u in DRIFTING:
            assert edd.discourse_move_id_key(t) != normalize_entity_text(t), t
        assert edd.discourse_move_id_key(CONTROL[1]) == normalize_entity_text(CONTROL[1])


class TestTheWritePathStoresTheHistoricalIds:
    @pytest.mark.anyio
    async def test_the_hyphen_free_control_keeps_its_id_under_either_normalizer(self, conn):
        """POSITIVE CONTROL, through the real writer and touching nothing the fix
        added: this title is identical under both normalizers, so its stored id is
        the historical one before the fix, after it, and with the fix reverted. If
        this fails, the namespace or the format string moved — a different, larger
        break than the normalizer. Its passing alongside the failing hyphen /
        underscore / whitespace cases is what proves those fail for the reason claimed."""
        edd = _edd()
        mt, title, cs, expected = CONTROL
        n = await edd.write_discourse_moves(conn, document_rid=RID, episode_id=None, moves=[
            {"move_type": mt, "title": title, "chunk_range": [cs, cs + 1], "status": "asserted"}])
        assert n == 1
        assert await conn.fetchval(
            "SELECT id FROM session_discourse_moves WHERE source_rid=$1", RID) == expected

    @pytest.mark.anyio
    async def test_write_discourse_moves_upserts_onto_the_ids_already_stored(self, conn):
        """Through the real writer: the row ids that land in session_discourse_moves
        are the historical ones, so a replay hits ON CONFLICT (id) DO UPDATE on the
        existing row instead of inserting a twin beside it."""
        edd = _edd()
        moves = [
            {"move_type": mt, "title": title, "chunk_range": [cs, cs + 1],
             "status": "asserted", "detail": "d"}
            for mt, title, cs, _u in DRIFTING + [CONTROL]
        ]
        # the hyphenated claim supports the control thesis — keeps the edge pass covered
        moves[0]["supports"] = CONTROL[1]

        n = await edd.write_discourse_moves(conn, document_rid=RID, episode_id=None, moves=moves)
        assert n == 4
        rows = await conn.fetch(
            "SELECT id, move_type, resolves_move_id FROM session_discourse_moves "
            "WHERE source_type='document' AND source_rid=$1", RID)
        stored = {r["move_type"]: r["id"] for r in rows}
        assert stored == {mt: u for mt, _t, _cs, u in DRIFTING + [CONTROL]}
        assert {r["resolves_move_id"] for r in rows if r["move_type"] == "claim"} == {CONTROL[3]}

        # Idempotent: a second write is an update, not a duplicate.
        await edd.write_discourse_moves(conn, document_rid=RID, episode_id=None, moves=moves)
        assert await conn.fetchval(
            "SELECT count(*) FROM session_discourse_moves WHERE source_rid=$1", RID) == 4
