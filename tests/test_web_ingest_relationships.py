"""Unit tests for web_router._store_relationships (P5 Fix 2).

These exercise the shared relationship-insert helper with a mocked asyncpg
connection — no DB required. They lock in three behaviors that the previous
inline blocks got wrong:

  1. subject/object are matched with normalize_entity_text() (hyphen- and
     underscore-aware), not a raw lower(trim()), so hyphenated names hit the
     stored entity_registry.normalized_text.
  2. the returned count reflects rows *actually* inserted — parsed from the
     asyncpg command tag ('INSERT 0 1' vs 'INSERT 0 0') — so ON CONFLICT
     DO NOTHING no longer inflates the total.
  3. self-loops (subject == object after normalization) are skipped rather than
     sent to a table whose CHECK (subject_uri != object_uri) would reject them.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.routers.web_router import (  # noqa: E402
    _count_inserted,
    _rel_attr,
    _store_relationships,
)


class Rel:
    """Stand-in for WebIngestRelationship (attribute access)."""

    def __init__(self, subject, predicate, obj):
        self.subject = subject
        self.predicate = predicate
        self.object = obj


class FakeConn:
    """Records execute() calls and returns queued command tags.

    Each queued tag is returned for the corresponding execute() call, mimicking
    asyncpg's 'INSERT 0 <n>' status strings.
    """

    def __init__(self, tags):
        self._tags = list(tags)
        self.calls = []  # list of (subj_norm, obj_norm, predicate, source)

    async def execute(self, _sql, *params):
        self.calls.append(tuple(params))
        return self._tags.pop(0) if self._tags else "INSERT 0 0"


# --------------------------------------------------------------------------
# _count_inserted — command-tag parsing
# --------------------------------------------------------------------------

def test_count_inserted_parses_command_tag():
    assert _count_inserted("INSERT 0 1") == 1
    assert _count_inserted("INSERT 0 0") == 0
    assert _count_inserted("INSERT 0 5") == 5


def test_count_inserted_is_defensive():
    assert _count_inserted(None) == 0
    assert _count_inserted("") == 0
    assert _count_inserted("weird") == 0


# --------------------------------------------------------------------------
# _rel_attr — dict and object access
# --------------------------------------------------------------------------

def test_rel_attr_reads_dict_and_object():
    d = {"subject": "A", "predicate": "part_of", "object": "B"}
    assert _rel_attr(d, "subject") == "A"
    assert _rel_attr(d, "object") == "B"
    o = Rel("A", "part_of", "B")
    assert _rel_attr(o, "subject") == "A"
    assert _rel_attr(o, "predicate") == "part_of"


# --------------------------------------------------------------------------
# _store_relationships — normalization
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_hyphenated_name_is_normalized_for_match():
    """A hyphenated/underscored surface form is normalized to spaces so it
    matches entity_registry.normalized_text (which stores it that way)."""
    conn = FakeConn(["INSERT 0 1"])
    n = await _store_relationships(
        conn,
        [Rel("Regen-Network", "part_of", "Cosmos_Ecosystem")],
        source="web_ingest",
    )
    assert n == 1
    subj_norm, obj_norm, predicate, source = conn.calls[0]
    assert subj_norm == "regen network"
    assert obj_norm == "cosmos ecosystem"
    assert predicate == "part_of"
    assert source == "web_ingest"


@pytest.mark.asyncio
async def test_dict_relationships_from_web_process_supported():
    conn = FakeConn(["INSERT 0 1"])
    n = await _store_relationships(
        conn,
        [{"subject": "Foo-Bar", "predicate": "relates_to", "object": "Baz"}],
        source="web_process",
    )
    assert n == 1
    assert conn.calls[0][0] == "foo bar"
    assert conn.calls[0][3] == "web_process"


# --------------------------------------------------------------------------
# _store_relationships — self-loop skip
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_self_loop_skipped_after_normalization():
    """subject == object post-normalization is skipped (never sent to DB)."""
    conn = FakeConn(["INSERT 0 1"])  # would count if it ran
    n = await _store_relationships(
        conn,
        [Rel("Regen-Network", "same_as", "regen_network")],
        source="web_ingest",
    )
    assert n == 0
    assert conn.calls == [], "self-loop must not reach conn.execute"


@pytest.mark.asyncio
async def test_incomplete_relationship_skipped():
    conn = FakeConn(["INSERT 0 1", "INSERT 0 1"])
    n = await _store_relationships(
        conn,
        [
            {"subject": "", "predicate": "part_of", "object": "B"},   # empty subj
            {"subject": "A", "predicate": None, "object": "B"},         # no predicate
        ],
        source="web_ingest",
    )
    assert n == 0
    assert conn.calls == []


# --------------------------------------------------------------------------
# _store_relationships — count reflects real inserts only
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_count_reflects_real_inserts_only():
    """ON CONFLICT DO NOTHING (INSERT 0 0) must not inflate the count."""
    conn = FakeConn(["INSERT 0 1", "INSERT 0 0", "INSERT 0 1"])
    rels = [
        Rel("A", "part_of", "B"),   # inserted
        Rel("C", "part_of", "D"),   # conflict → 0
        Rel("E", "part_of", "F"),   # inserted
    ]
    n = await _store_relationships(conn, rels, source="web_ingest")
    assert n == 2, "only actually-inserted rows should be counted"
    assert len(conn.calls) == 3


@pytest.mark.asyncio
async def test_empty_and_none_relationships():
    conn = FakeConn([])
    assert await _store_relationships(conn, [], source="web_ingest") == 0
    assert await _store_relationships(conn, None, source="web_ingest") == 0
    assert conn.calls == []


@pytest.mark.asyncio
async def test_db_exception_is_swallowed_and_not_counted():
    class BoomConn:
        def __init__(self):
            self.calls = 0

        async def execute(self, _sql, *params):
            self.calls += 1
            raise RuntimeError("fk violation")

    conn = BoomConn()
    n = await _store_relationships(
        conn, [Rel("A", "part_of", "B")], source="web_ingest",
    )
    assert n == 0
    assert conn.calls == 1
