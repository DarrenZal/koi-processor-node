"""Regression tests for the /koi-net/rids/fetch privacy filter.

The RID directory is peer-visible: any node with an approved poll edge can
enumerate it. Before this fix it listed every entity_registry row with a
koi_rid — including node-private entities and merge tombstones. These tests
lock in the three mandatory predicates and the optional group allowlist:

  1. ``NOT COALESCE(node_private, FALSE)`` — private entities never listed
  2. ``merged_into IS NULL`` — tombstoned identities never listed
  3. ``koi_rid IS NOT NULL`` — pre-existing behavior
  4. optional ``metadata->>'group_id'`` allowlist via KOI_FEDERATION_RID_GROUPS
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.koi_net_router import _build_rids_fetch_query, _federation_rid_groups
from tests import koi_test_targets as targets


# ---- Unit tests: query builder -------------------------------------------


def test_mandatory_privacy_predicates_always_present():
    for rid_types in (None, ["Person", "Organization"]):
        for groups in (None, ["biofi"]):
            sql, _ = _build_rids_fetch_query(rid_types, groups)
            assert "er.koi_rid IS NOT NULL" in sql
            assert "er.merged_into IS NULL" in sql
            assert "NOT COALESCE(er.node_private, FALSE)" in sql


def test_no_filters_yields_no_params():
    sql, params = _build_rids_fetch_query(None, None)
    assert params == []
    assert "$1" not in sql


def test_rid_types_param_position():
    sql, params = _build_rids_fetch_query(["Person"], None)
    assert params == [["Person"]]
    assert "er.entity_type = ANY($1)" in sql
    assert "group_id" not in sql


def test_group_allowlist_param_position():
    sql, params = _build_rids_fetch_query(None, ["biofi"])
    assert params == [["biofi"]]
    assert "er.metadata->>'group_id' = ANY($1)" in sql


def test_both_filters_param_ordering():
    sql, params = _build_rids_fetch_query(["Person"], ["biofi", "bioregional-coordination"])
    assert params == [["Person"], ["biofi", "bioregional-coordination"]]
    assert "er.entity_type = ANY($1)" in sql
    assert "er.metadata->>'group_id' = ANY($2)" in sql


def test_federation_rid_groups_env_parsing(monkeypatch):
    monkeypatch.delenv("KOI_FEDERATION_RID_GROUPS", raising=False)
    assert _federation_rid_groups() is None

    monkeypatch.setenv("KOI_FEDERATION_RID_GROUPS", "")
    assert _federation_rid_groups() is None

    monkeypatch.setenv("KOI_FEDERATION_RID_GROUPS", "  ,  ")
    assert _federation_rid_groups() is None

    monkeypatch.setenv("KOI_FEDERATION_RID_GROUPS", "biofi, bioregional-coordination ,")
    assert _federation_rid_groups() == ["biofi", "bioregional-coordination"]


# ---- Integration test: query semantics against a throwaway schema --------


asyncpg = pytest.importorskip("asyncpg")

_integration_skip = targets.requires_database


@_integration_skip
@pytest.mark.asyncio
async def test_rids_query_excludes_private_and_tombstoned():
    schema = f"rids_test_{uuid.uuid4().hex[:12]}"
    conn = await asyncpg.connect(targets.database_url())
    try:
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        await conn.execute(f'SET search_path TO "{schema}", public')
        await conn.execute(
            """
            CREATE TABLE entity_registry (
                id           SERIAL PRIMARY KEY,
                koi_rid      TEXT,
                entity_type  TEXT,
                node_private BOOLEAN,
                merged_into  TEXT,
                metadata     JSONB
            )
            """
        )
        await conn.executemany(
            "INSERT INTO entity_registry (koi_rid, entity_type, node_private, merged_into, metadata)"
            " VALUES ($1, $2, $3, $4, $5)",
            [
                ("rid:public-person", "Person", False, None, '{"group_id": "biofi"}'),
                ("rid:null-private", "Person", None, None, None),
                ("rid:private-person", "Person", True, None, None),
                ("rid:tombstoned", "Person", False, "entity:person-survivor", None),
                ("rid:no-rid-row-ignored", None, False, None, None),
                ("rid:other-group", "Organization", False, None, '{"group_id": "internal"}'),
            ],
        )
        # Simulate the koi_rid IS NOT NULL row properly: null out its rid.
        await conn.execute(
            "UPDATE entity_registry SET koi_rid = NULL WHERE koi_rid = 'rid:no-rid-row-ignored'"
        )

        sql, params = _build_rids_fetch_query(None, None)
        rows = await conn.fetch(sql, *params)
        rids = {r["koi_rid"] for r in rows}
        assert rids == {"rid:public-person", "rid:null-private", "rid:other-group"}

        sql, params = _build_rids_fetch_query(["Person"], None)
        rows = await conn.fetch(sql, *params)
        assert {r["koi_rid"] for r in rows} == {"rid:public-person", "rid:null-private"}

        sql, params = _build_rids_fetch_query(None, ["biofi"])
        rows = await conn.fetch(sql, *params)
        assert {r["koi_rid"] for r in rows} == {"rid:public-person"}
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
