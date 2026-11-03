"""Behavioural tests for the cross-source discourse-link prototype (issue #60).

Every database test runs against a throwaway koi_test_xsrc_* database created through
KOI_TEST_ADMIN_DSN by scripts/discourse_links_demo.create_scratch_db and dropped
afterwards; without that variable they skip. Most run inside a transaction that is
rolled back.

The embedder is a fixed concept-count vector. It makes "vessel" and "boat" land
on the same axis so the paraphrase case is deterministic. It tests mechanics
(visibility, freshness, binding, fusion, dedup, review, correction). It says
nothing about real semantic retrieval quality.

Regression cases carry a positive control: the same operation succeeding, or
the same item appearing, when the trigger is absent.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import json
import logging
import math
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from api import discourse_discovery as dd
from api import discourse_links as dl
from scripts import discourse_discovery_worker as worker
from scripts import discourse_links_canary as canary
from scripts import discourse_links_demo as demo
from scripts import extract_deep_documents as ex
from tests import koi_test_targets as targets

NS = uuid.UUID("0b8d7c1e-5f0a-4c55-9a0e-3f3cb8b1e2a4")
EXTRACTED = datetime(2026, 10, 1, tzinfo=timezone.utc)
CTX = dl.personal_context()


def mid(name: str) -> str:
    return str(uuid.uuid5(NS, name))


def sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def operator(actor: str) -> dl.AdmissionAuthority:
    return dl.mint_operator_authority(dl._mint_operator_principal(actor, "test"))


OPERATOR = operator("person:reviewer")
OTHER_OPERATOR = operator("person:second-reviewer")
DOC_SHA = sha("content-addressed source")

# Fictional sources. S3 summarises S1; S2 is an independent draft; S4 is superseded.
# S1-S3 get recorded extraction bindings when loaded, as an extraction run would write.
SOURCES = {
    "S1": {"rid": "fixture:tidal-ledgers", "title": "Field notes on tidal ledgers",
           "source_url": "https://example.org/tidal", "content_hash": sha("tidal v1"), "bind": True, "moves": [
               ("a1", "claim", "Tidal ledgers record every catch with the exact boat and hour it came from."),
               ("a2", "claim", "Shared tidal ledgers cut disputed catch reports from 30% to 12% in the harbour trial."),
               ("a3", "claim", "Each ledger entry names the boat and hour of the catch."),
           ]},
    "S2": {"rid": "fixture:harbour-protocol", "title": "Harbour protocol draft", "source_url": None,
           "content_hash": sha("protocol v1"), "bind": True, "moves": [
               ("b1", "claim", "Every exchanged record must name the vessel and the time of landing."),
               ("b2", "evidence", "In two of three other harbours disputed catch reports did not fall after ledgers were shared."),
           ]},
    "S3": {"rid": "fixture:tidal-summary", "title": "Blog summary of the field notes", "source_url": None,
           "content_hash": sha("summary v1"), "bind": True, "moves": [
               ("c1", "evidence", "The field notes say shared ledgers cut disputed reports from 30% to 12%."),
               ("c2", "evidence", "Harbour officers interviewed for the post say disputed catch reports fell after the trial."),
           ]},
    "S4": {"rid": "fixture:retired", "title": "Superseded copy", "source_url": None,
           "content_hash": sha("retired"), "superseded": True, "moves": [
               ("d1", "claim", "Tidal ledgers record every catch with the boat and the hour."),
           ]},
    # Substack-shaped: the log holds the feed:slug, and only koi_memories has a hash.
    "S5": {"rid": "substack-corpus:fixture:landing-post", "title": "A post on landing records", "source_url": None,
           "content_hash": "fixture:landing-post", "deep_extracted_at": EXTRACTED,
           "memory": {"hash": "0123456789abcdef", "updated_at": EXTRACTED - timedelta(days=1)}, "moves": [
               ("e1", "claim", "Every landing record should name the vessel and the hour."),
           ]},
    "S6": {"rid": "substack-corpus:fixture:slug-only", "title": "A post with no content hash", "source_url": None,
           "content_hash": "fixture:slug-only", "deep_extracted_at": EXTRACTED, "moves": [
               ("f1", "claim", "A landing record names the vessel and the hour of landing."),
           ]},
    "S7": {"rid": f"document:{DOC_SHA}", "title": "A content-addressed document", "source_url": None,
           "content_hash": DOC_SHA, "moves": [
               ("g1", "claim", "Landing records name the vessel and the hour."),
           ]},
    # Visibility: S8 is private, S9 is wiki material belonging to the held pilot.
    "S8": {"rid": "fixture:private-notes", "title": "Private notes", "source_url": None,
           "content_hash": sha("private v1"), "bind": True, "private": True, "moves": [
               ("h1", "claim", "A private note: every landing record names the vessel and the hour."),
           ]},
    "S9": {"rid": "mediawiki:p2p:Ledger", "title": "Wiki page", "source_url": None,
           "content_hash": sha("wiki v1"), "bind": True, "moves": [
               ("w1", "claim", "Ledgers record the vessel and the hour of landing."),
           ]},
}
BASE = ("S1", "S2", "S3", "S4")

CONCEPTS = {
    "provenance": {"boat", "vessel", "hour", "time", "landing", "record", "records", "entry", "names", "name"},
    "outcome": {"disputed", "reports", "fall", "fell", "cut", "30", "12", "trial"},
    "ledger": {"ledger", "ledgers", "tidal", "shared"},
    "catch": {"catch", "harbour", "harbours"},
}


class ConceptEmbedder:
    async def embed_batch(self, texts):
        out = []
        for t in texts:
            toks = dl._TOKEN_RE.findall(t.lower())
            v = [float(sum(tok in words for tok in toks)) for words in CONCEPTS.values()] + [0.01]
            n = math.sqrt(sum(x * x for x in v))
            out.append([x / n for x in v])
        return out


MODEL = "test:concept-count"


# Scratch databases are named koi_test_xsrc_<unique> and created through KOI_TEST_ADMIN_DSN.
# The demo script reads its own admin DSN at import and names scratch databases
# koi_xsrc_scratch_*, and api/discourse_links.py admits fixture-replay judgments only in
# databases carrying that prefix. Both are patched together for this module, so the code
# under test keeps its scratch-only behaviour under the test naming rule.
SCRATCH_PREFIX = targets.SCRATCH_PREFIX + "xsrc_"


@pytest.fixture(scope="module", autouse=True)
def scratch_patches():
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(demo, "ADMIN_DSN", targets.configured(targets.ADMIN_VAR) or targets.UNSET_DATABASE_URL)
        mp.setattr(demo, "SCRATCH_PREFIX", SCRATCH_PREFIX)
        mp.setattr(dl, "SCRATCH_DATABASE_PREFIX", SCRATCH_PREFIX)
        yield


def _new_scratch():
    targets.admin_dsn()                      # skips, naming KOI_TEST_ADMIN_DSN, when it is unset
    name = targets.scratch_name("xsrc")
    return name, asyncio.run(demo.create_scratch_db(name))


def _drop_scratch(name):
    asyncio.run(demo.drop_scratch_db(targets.droppable(name)))


# Both fixtures name scratch_patches so pytest tears them down before it: same-scope
# autouse fixtures are not otherwise guaranteed to outlive the fixtures that use them.
@pytest.fixture(scope="module")
def scratch_dsn(scratch_patches):
    name, dsn = _new_scratch()
    yield dsn
    _drop_scratch(name)


@pytest.fixture
def committed_dsn(scratch_patches):
    """A fresh scratch database with BASE committed, for code that opens its own connections."""
    name, dsn = _new_scratch()

    async def load():
        conn = await asyncpg.connect(dsn)
        try:
            await _load(conn, BASE)
        finally:
            await conn.close()
    asyncio.run(load())
    yield dsn
    _drop_scratch(name)


def run(dsn, scenario, sources=BASE):
    async def go():
        conn = await asyncpg.connect(dsn)
        tx = conn.transaction()
        await tx.start()
        try:
            assert (await conn.fetchval("SELECT current_database()")).startswith(demo.SCRATCH_PREFIX)
            await _load(conn, sources)
            return await scenario(conn)
        finally:
            await tx.rollback()
            await conn.close()
    return asyncio.run(go())


async def _load(conn, names):
    for key in names:
        s = SOURCES[key]
        await conn.execute(
            """INSERT INTO document_ingestion_log (document_rid, title, source_url, content_hash, deep_extracted_at)
               VALUES ($1,$2,$3,$4,$5)""",
            s["rid"], s["title"], s["source_url"], s["content_hash"], s.get("deep_extracted_at"))
        if s.get("memory") or s.get("private"):
            mem = s.get("memory") or {}
            await conn.execute(
                """INSERT INTO koi_memories (rid, event_type, source_sensor, content, metadata, updated_at, is_private)
                   VALUES ($1, 'NEW', 'fixture', '{}'::jsonb,
                           CASE WHEN $2::text IS NULL THEN '{}'::jsonb ELSE jsonb_build_object('content_hash', $2::text) END,
                           coalesce($3, now()), $4)""",
                s["rid"], mem.get("hash"), mem.get("updated_at"), bool(s.get("private")))
        for name, mtype, title in s["moves"]:
            await conn.execute(
                """INSERT INTO session_discourse_moves (id, source_type, source_rid, move_type, title, superseded_at)
                   VALUES ($1, 'document', $2, $3, $4, CASE WHEN $5 THEN now() END)""",
                mid(name), s["rid"], mtype, title, bool(s.get("superseded")))
        if s.get("bind"):
            await _bind(conn, key)
    ids = [mid(n) for key in names for n, _, _ in SOURCES[key]["moves"]]
    await dl.embed_moves(conn, ConceptEmbedder(), ids, model=MODEL)


def _chunks(key, run_id="extract-1"):
    async def read():
        return [(0, SOURCES[key]["rid"] + run_id)]
    return read


async def _bind(conn, key, run_id="extract-1"):
    s = SOURCES[key]
    _, captured = await dl.capture_extraction_input(conn, source_rid=s["rid"], read_chunks=_chunks(key, run_id))
    return await dl.record_extraction_bindings(
        conn, source_rid=s["rid"], move_ids=[mid(n) for n, _, _ in s["moves"]],
        extraction_input=captured, run_id=run_id, recorded_by="test-extractor")


async def _discover(conn, names, run_id="t1", context=CTX, **kw):
    kw.setdefault("embedding_model", MODEL)
    return await dl.discover_candidates(conn, [mid(n) for n in names], context=context,
                                        discovered_by="agent:test", run_id=run_id, **kw)


async def _cand(conn, a, b, status=None):
    return await conn.fetchrow(
        """SELECT * FROM discourse_link_candidates WHERE ((query_move_id=$1 AND target_move_id=$2)
              OR (query_move_id=$2 AND target_move_id=$1)) AND ($3::text IS NULL OR status = $3)
            ORDER BY discovered_at DESC, id LIMIT 1""", mid(a), mid(b), status)


async def _admit(conn, a, b, relation="aligns_with", semantics="design-alignment/v0", authority=OPERATOR):
    cand = await _cand(conn, a, b, status="candidate")
    return await dl.review_candidate(conn, str(cand["id"]), decision="admit", authority=authority,
                                     rationale="test review", relation=relation, semantics=semantics,
                                     reverse=str(cand["query_move_id"]) != mid(a))


async def _set_detail(conn, name, detail):
    await conn.execute("UPDATE session_discourse_moves SET detail = $2 WHERE id = $1", mid(name), detail)


async def _supports(conn, a, b, authority=OPERATOR):
    return await dl.record_authored_relation(conn, mid(a), mid(b), relation="supports",
                                             semantics="epistemic-relation/v0", authority=authority,
                                             rationale="r")


async def _fields(conn, assignments):
    for key, field_id in assignments:
        await conn.execute("INSERT INTO document_field_membership (document_rid, field_id) VALUES ($1, $2)",
                           SOURCES[key]["rid"], field_id)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def test_paraphrase_is_retrieved_across_sources_and_ineligible_moves_never_are(scratch_dsn):
    async def scenario(conn):
        await _discover(conn, ["b1"])
        rows = await conn.fetch("SELECT * FROM discourse_link_candidates ORDER BY fused_score DESC")
        targets = [str(r["target_move_id"]) for r in rows]
        assert targets[0] in (mid("a1"), mid("a3"))
        assert {mid("a1"), mid("a3")} <= set(targets)
        assert mid("d1") not in targets                      # superseded source
        assert all(r["target_source_rid"] != "fixture:harbour-protocol" for r in rows)  # own source
        top = rows[0]
        assert top["semantic_rank"] == 1 and top["lexical_rank"] is not None
        assert (top["query_binding_kind"], top["query_bound_version"]) == \
            ("recorded_extraction", "sha256:" + sha("protocol v1"))
    run(scratch_dsn, scenario)


def test_similarity_creates_no_relation_and_no_support(scratch_dsn):
    async def scenario(conn):
        await _discover(conn, ["b2", "c1"])
        assert (await _cand(conn, "b2", "a2"))["status"] == "candidate"
        assert (await _cand(conn, "c1", "a2"))["status"] == "candidate"
        assert await conn.fetchval("SELECT count(*) FROM discourse_relations") == 0
        report = await dl.corroboration_report(conn, mid("a2"), context=CTX)
        assert report["current_supporting_moves"] == 0
        assert report["independence"]["evaluated"] is False and report["independence"]["independent_sources"] is None
        assert report["open_candidates_not_counted"] >= 2 and report["naive_linked_count"] >= 2
    run(scratch_dsn, scenario)


def test_rerun_and_reverse_side_discovery_do_not_duplicate(scratch_dsn):
    async def scenario(conn):
        first = await _discover(conn, ["b1", "b2", "c1"], run_id="r1")
        n = await conn.fetchval("SELECT count(*) FROM discourse_link_candidates")
        assert first["inserted"] == n > 0
        declined = await _cand(conn, "b2", "a2")
        await dl.review_candidate(conn, str(declined["id"]), decision="decline", authority=OPERATOR,
                                  rationale="different cohort; not about the harbour trial")
        again = await _discover(conn, ["b1", "b2", "c1"], run_id="r2")
        assert again["inserted"] == 0
        assert again["rediscovered"] == first["inserted"] + first["rediscovered"]
        await _discover(conn, ["a1", "a2"], run_id="reverse")
        dupes = await conn.fetchval(
            """SELECT count(*) FROM (SELECT least(query_move_id::text, target_move_id::text),
                                            greatest(query_move_id::text, target_move_id::text)
                                       FROM discourse_link_candidates GROUP BY 1, 2 HAVING count(*) > 1) d""")
        assert dupes == 0
        after = await _cand(conn, "b2", "a2")
        assert after["status"] == "declined" and after["times_seen"] >= 3
    run(scratch_dsn, scenario)


def test_lexical_only_discovery_says_so(scratch_dsn):
    async def scenario(conn):
        summary = await _discover(conn, ["b2"], embedding_model=None)
        assert summary["method_params"]["embedding_model"] is None and summary["inserted"] > 0
        rows = await conn.fetch("SELECT semantic_rank, lexical_rank FROM discourse_link_candidates")
        assert all(r["semantic_rank"] is None and r["lexical_rank"] is not None for r in rows)
    run(scratch_dsn, scenario)


def test_semantic_leg_uses_only_vectors_of_the_current_representation(scratch_dsn):
    async def scenario(conn):
        async def semantic_ids(query):
            rows = await dl._semantic_leg(conn, CTX, mid(query), "fixture:harbour-protocol",
                                          dl.DEFAULT_MOVE_TYPES, MODEL, 20)
            return {r["id"] for r in rows}
        assert mid("a3") in await semantic_ids("b1")                      # control
        await _set_detail(conn, "a3", "Revised after embedding.")
        assert mid("a3") not in await semantic_ids("b1")
        await dl.embed_moves(conn, ConceptEmbedder(), [mid("a3")], model=MODEL)
        assert mid("a3") in await semantic_ids("b1")                      # control: re-embedded
        await _set_detail(conn, "b1", "Revised after embedding.")
        assert await semantic_ids("b1") == set()
        assert (await _discover(conn, ["b1"], run_id="stale-query"))["stale_query_embedding"] == 1
        await dl.embed_moves(conn, ConceptEmbedder(), [mid("b1")], model=MODEL)
        assert (await _discover(conn, ["b1"], run_id="fresh-query"))["stale_query_embedding"] == 0
    run(scratch_dsn, scenario)


def test_target_source_cap_is_a_budget_and_the_expanded_view_needs_authority(scratch_dsn):
    async def scenario(conn):
        with pytest.raises(dl.ReviewRefused, match="minted AdmissionAuthority"):
            await _discover(conn, ["b1"], run_id="uncapped", per_target_source_cap=None)
        with pytest.raises(dl.ReviewRefused, match="minted AdmissionAuthority"):
            await _discover(conn, ["b1"], run_id="uncapped", per_target_source_cap=0)
        await _discover(conn, ["b1"], run_id="uncapped", per_target_source_cap=None, expanded_by=OPERATOR)
        uncapped = await conn.fetchval(
            "SELECT count(*) FROM discourse_link_candidates WHERE target_source_rid='fixture:tidal-ledgers'")
        assert uncapped == 3                                              # control: all S1 matches kept
        await conn.execute("DELETE FROM discourse_link_candidates")
        summary = await _discover(conn, ["b1"], run_id="capped", per_target_source_cap=1)
        assert await conn.fetchval(
            "SELECT count(*) FROM discourse_link_candidates WHERE target_source_rid='fixture:tidal-ledgers'") == 1
        assert summary["capped_by_target_source"] >= 1
        assert summary["method_params"]["per_target_source_cap"] == 1
        assert await conn.fetchval("SELECT count(*) FROM session_discourse_moves") == 8    # nothing discarded
    run(scratch_dsn, scenario)


# ---------------------------------------------------------------------------
# A1: visibility before ranking
# ---------------------------------------------------------------------------

def test_hidden_material_neither_appears_nor_moves_visible_ranks(scratch_dsn):
    async def scenario(conn):
        await _fields(conn, [("S2", "harbours"), ("S3", "harbours"), ("S8", "harbours")])
        ctx = dl.field_context(["harbours"])

        async def legs(context):
            lex = await dl._lexical_leg(conn, context, SOURCES["S2"]["moves"][0][2], "fixture:harbour-protocol",
                                        dl.DEFAULT_MOVE_TYPES, 20)
            sem = await dl._semantic_leg(conn, context, mid("b1"), "fixture:harbour-protocol",
                                         dl.DEFAULT_MOVE_TYPES, MODEL, 20)
            return lex, sem

        with_hidden = await legs(ctx)
        seen = {r["id"] for leg in with_hidden for r in leg}
        assert seen <= {mid("c1"), mid("c2")}                 # S1 unassigned, S8 private, S9 wiki: hidden
        personal = await legs(CTX)
        assert {mid("a1"), mid("a3"), mid("h1")} <= {r["id"] for leg in personal for r in leg}  # control
        assert mid("w1") not in {r["id"] for leg in personal for r in leg}                    # pilot excluded
        for key in ("S1", "S8", "S9"):
            await conn.execute("DELETE FROM discourse_move_embeddings e USING session_discourse_moves m "
                               "WHERE e.move_id = m.id AND m.source_rid = $1", SOURCES[key]["rid"])
            await conn.execute("DELETE FROM discourse_move_extraction_bindings WHERE source_rid = $1",
                               SOURCES[key]["rid"])
            await conn.execute("DELETE FROM session_discourse_moves WHERE source_rid = $1", SOURCES[key]["rid"])
        assert await legs(ctx) == with_hidden                 # same ids, ranks and scores without them
    run(scratch_dsn, scenario, sources=BASE + ("S8", "S9"))


def test_field_membership_alone_grants_nothing(scratch_dsn):
    async def scenario(conn):
        await _fields(conn, [("S2", "harbours"), ("S8", "harbours")])
        everything = [mid(n) for key in BASE + ("S8", "S9") for n, _, _ in SOURCES[key]["moves"]]
        live = [m for m in everything if m != mid("d1")]

        async def seen(ctx):
            return await dl.visible_ids(conn, ctx, live)
        assert await seen(dl.field_context(["harbours"])) == {mid("b1"), mid("b2")}
        assert mid("h1") in await seen(dl.field_context(["harbours"], include_private=True))      # explicit grant
        assert mid("a1") in await seen(dl.field_context(["harbours"], include_unassigned=True))   # explicit grant
        assert await seen(dl.public_context()) == set()
        assert await seen(dl.public_context(sources=["fixture:tidal-ledgers"])) == {mid("a1"), mid("a2"), mid("a3")}
        assert await seen(dl.federated_context("peer:edge")) == set()
        personal = await seen(CTX)
        assert mid("h1") in personal and mid("a1") in personal and mid("w1") not in personal
        await _discover(conn, ["b1"], run_id="personal")
        with pytest.raises(dl.NotVisible):
            await dl.connected_material(conn, mid("a1"), context=dl.field_context(["harbours"]))
        view = await dl.connected_material(conn, mid("b1"), context=dl.field_context(["harbours"]),
                                           include_candidates=True)
        assert view["candidates"] == [] and view["open_candidate_count"] == 0
        assert (await dl.connected_material(conn, mid("b1"), context=CTX))["open_candidate_count"] > 0  # control
        cov = await dl.retrieval_coverage(conn, MODEL, context=dl.field_context(["harbours"]))
        assert cov["eligible"] == 2 and (await dl.retrieval_coverage(conn, MODEL, context=CTX))["eligible"] == 8
        assert await dl.same_work_signals(conn, context=dl.field_context(["harbours"]), min_cosine=0, min_pairs=1) == []
        assert await dl.same_work_signals(conn, context=CTX, min_cosine=0, min_pairs=1)                 # control
    run(scratch_dsn, scenario, sources=BASE + ("S8", "S9"))


# ---------------------------------------------------------------------------
# Authority
# ---------------------------------------------------------------------------

def test_admission_requires_minted_authority_and_a_label(scratch_dsn, monkeypatch):
    async def scenario(conn):
        await _discover(conn, ["b1"])
        cid = str((await _cand(conn, "b1", "a1"))["id"])

        async def admit(authority, **kw):
            kw.setdefault("relation", "aligns_with")
            kw.setdefault("semantics", "design-alignment/v0")
            return await dl.review_candidate(conn, cid, decision="admit", authority=authority, rationale="x", **kw)

        with pytest.raises(dl.ReviewRefused, match="minted"):
            dl.AdmissionAuthority(actor="person:reviewer", basis="operator")
        with pytest.raises(dl.ReviewRefused, match="minted only"):
            dl.OperatorPrincipal(actor="person:reviewer", authenticated_by="header")
        with pytest.raises(dl.ReviewRefused, match="minted AdmissionAuthority"):
            await admit("person:reviewer")
        with pytest.raises(dl.ReviewRefused, match="own candidate"):
            await admit(operator("agent:test"))
        await dl.propose_relation(conn, cid, relation="aligns_with", semantics="design-alignment/v0",
                                  proposed_by="agent:labeller", rationale="prescribes what a1 reports")
        assert (await _cand(conn, "b1", "a1"))["status"] == "candidate"
        with pytest.raises(dl.ReviewRefused, match="own candidate"):
            await admit(operator("agent:labeller"))
        with pytest.raises(dl.ReviewRefused, match="explicit relation"):
            await admit(OPERATOR, relation=None, semantics=None)
        with pytest.raises(dl.ReviewRefused, match="not in"):
            await admit(OPERATOR, relation="supports")
        # fixture-replay works only in a scratch database (judgments enabled, to isolate the check).
        monkeypatch.setenv(dl.JUDGMENTS_ENV, "1")
        monkeypatch.setattr(dl, "SCRATCH_DATABASE_PREFIX", "not_this_db_")
        with pytest.raises(dl.ReviewRefused, match="scratch database"):
            await admit(dl.mint_fixture_replay_authority("synthetic.ic"))
        monkeypatch.setattr(dl, "SCRATCH_DATABASE_PREFIX", demo.SCRATCH_PREFIX)
        monkeypatch.delenv(dl.JUDGMENTS_ENV)
        rel = await admit(OPERATOR)
        row = await conn.fetchrow("SELECT * FROM discourse_relations WHERE id=$1", rel)
        assert (row["origin"], str(row["from_move_id"]), str(row["to_move_id"]), row["authority_basis"]) == \
            ("reviewed_candidate", mid("b1"), mid("a1"), dl.LOCAL_CREDENTIAL)
        with pytest.raises(dl.ReviewRefused, match="already admitted"):
            await dl.review_candidate(conn, cid, decision="decline", authority=OTHER_OPERATOR, rationale="x")
    run(scratch_dsn, scenario)


# ---------------------------------------------------------------------------
# A2: binding and freshness
# ---------------------------------------------------------------------------

def test_admission_rechecks_claim_text_eligibility_and_binding(scratch_dsn):
    async def scenario(conn):
        await _discover(conn, ["b1", "c1"], per_target_source_cap=None, expanded_by=OPERATOR)
        await _set_detail(conn, "a1", "A clarification added after discovery.")
        with pytest.raises(dl.ReviewRefused, match="claim text changed"):
            await _admit(conn, "b1", "a1")
        view = await dl.connected_material(conn, mid("b1"), context=CTX, include_candidates=True)
        stale = [c for c in view["candidates"] if c["other"]["id"] == mid("a1")]
        assert stale and not stale[0]["current"] and "claim text changed" in stale[0]["not_current_reasons"]
        await conn.execute("UPDATE session_discourse_moves SET superseded_at = now() WHERE id = $1", mid("a3"))
        with pytest.raises(dl.ReviewRefused, match="move superseded"):
            await _admit(conn, "b1", "a3")
        # The source changed after its moves were extracted: refused until re-extraction.
        await conn.execute("UPDATE document_ingestion_log SET content_hash=$1 WHERE document_rid='fixture:tidal-summary'",
                           sha("summary v2"))
        with pytest.raises(dl.ReviewRefused, match="source changed since extraction"):
            await _admit(conn, "c1", "a2", relation="restates", semantics="lineage/v0")
        assert await _admit(conn, "b1", "a2", relation="related_to", semantics="topical/v0")   # control
    run(scratch_dsn, scenario)


def test_only_recorded_bindings_can_be_admitted_and_inferred_ones_stay_visible(scratch_dsn):
    async def scenario(conn):
        await _discover(conn, ["b1"], per_move_cap=10, per_target_source_cap=None, expanded_by=OPERATOR)
        moves = await dl.fetch_moves(conn, [mid("e1"), mid("f1"), mid("g1"), mid("a1")])
        kinds = {k: moves[mid(k)]["extraction"]["binding_kind"] for k in ("e1", "f1", "g1", "a1")}
        assert kinds == {"e1": "inferred_timestamp", "f1": "unknown", "g1": "content_addressed_source",
                         "a1": "recorded_extraction"}
        assert moves[mid("e1")]["extraction"]["binding_basis"] == "inferred: koi_memories row unchanged since deep extraction"
        assert moves[mid("e1")]["source"]["version"] == "sha256-prefix16:0123456789abcdef"
        assert moves[mid("f1")]["source"]["version"] is None             # feed:slug is never a version
        view = await dl.connected_material(conn, mid("b1"), context=CTX, include_candidates=True)
        by_other = {c["other"]["id"]: c for c in view["candidates"]}
        assert by_other[mid("e1")]["current"] and not by_other[mid("e1")]["admissible_now"]   # visible, not admissible
        with pytest.raises(dl.ReviewRefused, match="binding inferred from timestamps"):
            await _admit(conn, "b1", "e1")
        with pytest.raises(dl.ReviewRefused, match="no extraction binding"):
            await _admit(conn, "b1", "f1")
        with pytest.raises(dl.ReviewRefused, match="binding inferred"):
            await dl.record_authored_relation(conn, mid("e1"), mid("a1"), relation="related_to",
                                              semantics="topical/v0", authority=OPERATOR, rationale="r")
        # D1: a content address is source attribution, kept as its own class, not admissible.
        assert moves[mid("g1")]["source"]["version"] == "sha256:" + DOC_SHA
        with pytest.raises(dl.ReviewRefused, match="content-addressed source attribution"):
            await _admit(conn, "b1", "g1")
        assert await _admit(conn, "b1", "a1")                            # control: recorded extraction
        # A recorded binding whose text was edited afterwards no longer vouches for it.
        await _set_detail(conn, "a3", "Edited by hand after extraction.")
        assert "claim text differs from its recorded extraction" in \
            (await dl.fetch_moves(conn, [mid("a3")]))[mid("a3")]["admission_blockers"]
    run(scratch_dsn, scenario, sources=BASE + ("S5", "S6", "S7"))


def test_historical_judgments_keep_the_exact_text_they_judged(scratch_dsn):
    async def scenario(conn):
        await _discover(conn, ["b1"])
        rel = await _admit(conn, "b1", "a1")
        original = SOURCES["S1"]["moves"][0][2]
        await _set_detail(conn, "a1", "Edited after the judgment.")
        [hist] = (await dl.connected_material(conn, mid("a1"), context=CTX))["historical_relations"]
        assert hist["relation_id"] == rel and hist["judged"]["to"]["text_status"] == "retained"
        assert hist["judged"]["to"]["text"]["title"] == original and hist["judged"]["to"]["text"]["detail"] is None
        assert hist["other"]["id"] == mid("b1")
        # The retained text is checked against its hash on every read.
        await conn.execute("UPDATE discourse_claim_representations SET title = 'tampered' "
                           "WHERE representation_sha256 = $1", hist["judged"]["to"]["representation_sha256"])
        [hist] = (await dl.connected_material(conn, mid("a1"), context=CTX))["historical_relations"]
        assert hist["judged"]["to"]["text_status"] == "retained text fails its hash"
        assert dl._text_of({}, "abc") == {"representation_sha256": "abc", "text": None,
                                          "text_status": "hash only, text not retained"}
    run(scratch_dsn, scenario)


def test_a_changed_source_gets_a_fresh_judgment_after_re_extraction(scratch_dsn):
    async def scenario(conn):
        await _discover(conn, ["b1"])
        first = await _admit(conn, "b1", "a1")
        await conn.execute("UPDATE document_ingestion_log SET content_hash=$1 WHERE document_rid='fixture:tidal-ledgers'",
                           sha("tidal v2"))
        view = await dl.connected_material(conn, mid("a1"), context=CTX)
        assert view["relations"] == []
        assert view["historical_relations"][0]["not_current_reasons"] == ["source changed since extraction"]
        # Before re-extraction the candidate key is unchanged and admission is refused.
        assert (await _discover(conn, ["b1"], run_id="pre-extract"))["inserted"] == 0
        await _bind(conn, "S1", run_id="extract-2")                     # re-extraction records the new version
        assert (await _discover(conn, ["b1"], run_id="post-extract"))["inserted"] >= 1
        second = await _admit(conn, "b1", "a1")
        view = await dl.connected_material(conn, mid("a1"), context=CTX)
        assert [r["relation_id"] for r in view["relations"]] == [second]
        assert [r["relation_id"] for r in view["historical_relations"]] == [first]
        assert view["historical_relations"][0]["not_current_reasons"] == ["source version changed"]
        assert view["relations"][0]["judged"]["to"]["bound_version"] == "sha256:" + sha("tidal v2")
    run(scratch_dsn, scenario)


def test_extraction_hook_captures_the_input_version_only_when_enabled(scratch_dsn, monkeypatch, caplog):
    from scripts import extract_deep_documents as ex

    async def scenario(conn):
        rid = "fixture:hook-source"
        await conn.execute("INSERT INTO document_ingestion_log (document_rid, content_hash) VALUES ($1, $2)",
                           rid, sha("hook v1"))
        chunks = [(0, "chunk zero"), (1, "chunk one")]

        async def fake_fetch(_conn, _rid):
            return chunks
        monkeypatch.setattr(ex, "fetch_rag_chunks", fake_fetch)
        written = []
        n = await ex.write_discourse_moves(conn, document_rid=rid, episode_id=None, written_ids=written, moves=[
            {"move_type": "claim", "title": "Hook claim one", "chunk_range": [0, 0]},
            {"move_type": "evidence", "title": "Hook evidence", "chunk_range": [1, 1]}])
        assert n == 2 and len(written) == 2
        monkeypatch.delenv("KOI_DISCOURSE_BINDINGS", raising=False)
        got, captured = await ex.read_rag_input(conn, rid)
        assert got == chunks and captured is None                          # off: plain read, nothing captured
        assert await ex.record_discourse_bindings(conn, document_rid=rid, extraction_input=None,
                                                  move_ids=written, run_id="r1") == 0
        monkeypatch.setenv("KOI_DISCOURSE_BINDINGS", "1")
        got, captured = await ex.read_rag_input(conn, rid)
        assert got == chunks and captured.association == "captured_with_input"
        assert captured.source_version == "sha256:" + sha("hook v1")
        await conn.execute("ALTER TABLE discourse_move_extraction_bindings RENAME TO dmeb_hidden")
        with caplog.at_level(logging.ERROR):
            assert await ex.record_discourse_bindings(conn, document_rid=rid, extraction_input=captured,
                                                      move_ids=written, run_id="r1") == 0
        assert "left without a recorded binding" in caplog.text
        await conn.execute("ALTER TABLE dmeb_hidden RENAME TO discourse_move_extraction_bindings")
        assert await ex.record_discourse_bindings(conn, document_rid=rid, extraction_input=captured,
                                                  move_ids=written, run_id="r1") == 2
        row = await conn.fetchrow("SELECT * FROM discourse_move_extraction_bindings WHERE move_id = $1", written[0])
        assert (row["source_version_at_extraction"], row["association"]) == ("sha256:" + sha("hook v1"),
                                                                             "captured_with_input")
        assert row["extraction_input_sha256"] == dl.extraction_input_digest(chunks)
        assert (await dl.fetch_moves(conn, [written[0]]))[str(written[0])]["admissible"]
    run(scratch_dsn, scenario, sources=())


def test_binding_keeps_the_version_captured_with_the_input(scratch_dsn):
    """C8: the probe recorded an earlier input against a newer source hash and
    got an admissible binding. The version now comes from the read, not the record."""
    rid = SOURCES["S2"]["rid"]

    async def scenario(conn):
        async def binding():
            mv = (await dl.fetch_moves(conn, [mid("b1")]))[mid("b1")]
            return mv["extraction"], mv["admissible"], mv["admission_blockers"]

        ext, admissible, _ = await binding()                               # control: unchanged source
        assert ext["association"] == "captured_with_input" and admissible
        # The source changes after the input was read but before the binding is written.
        _, captured = await dl.capture_extraction_input(conn, source_rid=rid, read_chunks=_chunks("S2", "x2"))
        await conn.execute("UPDATE document_ingestion_log SET content_hash=$2 WHERE document_rid=$1",
                           rid, sha("protocol v2"))
        await dl.record_extraction_bindings(conn, source_rid=rid, move_ids=[mid("b1")], extraction_input=captured,
                                            run_id="x2", recorded_by="test-extractor")
        ext, admissible, blockers = await binding()
        assert ext["association"] == "source_changed_during_extraction"
        assert (ext["bound_version"], ext["version_at_record"]) == ("sha256:" + sha("protocol v1"),
                                                                     "sha256:" + sha("protocol v2"))
        assert not admissible and "source changed during extraction" in blockers
        # The source changes while the input is being read.
        async def read_while_changing():
            await conn.execute("UPDATE document_ingestion_log SET content_hash=$2 WHERE document_rid=$1",
                               rid, sha("protocol v3"))
            return [(0, "read during a change")]
        _, captured = await dl.capture_extraction_input(conn, source_rid=rid, read_chunks=read_while_changing)
        assert (captured.association, captured.source_version) == ("changed_while_reading", None)
        await dl.record_extraction_bindings(conn, source_rid=rid, move_ids=[mid("b1")], extraction_input=captured,
                                            run_id="x3", recorded_by="test-extractor")
        ext, admissible, blockers = await binding()
        assert ext["bound_version"] is None and not admissible
        assert "source changed while the extraction read its input" in blockers
        # The probe's call: an input hash with no captured version stays unknown.
        await dl.record_extraction_bindings(conn, source_rid=rid, move_ids=[mid("b1")],
                                            extraction_input_sha256=sha("earlier input"), run_id="x4",
                                            recorded_by="review-extractor")
        ext, admissible, blockers = await binding()
        assert (ext["association"], ext["bound_version"], admissible) == ("unestablished", None, False)
        # Control: a fresh capture at the current version is admissible again.
        await _bind(conn, "S2", run_id="x5")
        ext, admissible, _ = await binding()
        assert (ext["association"], ext["bound_version"], admissible) == \
            ("captured_with_input", "sha256:" + sha("protocol v3"), True)
    run(scratch_dsn, scenario)


def test_judgments_are_disabled_outside_scratch_databases(scratch_dsn, monkeypatch):
    """D2: the canary keeps every judgment off in a real database."""
    async def scenario(conn):
        await _discover(conn, ["b1"])
        cid = str((await _cand(conn, "b1", "a1"))["id"])
        monkeypatch.delenv(dl.JUDGMENTS_ENV, raising=False)
        monkeypatch.setattr(dl, "SCRATCH_DATABASE_PREFIX", "not_this_db_")   # behave as a real database
        await dl.propose_relation(conn, cid, relation="aligns_with", semantics="design-alignment/v0",
                                  proposed_by="agent:labeller", rationale="r")   # proposals stay open
        for attempt in (lambda: _admit(conn, "b1", "a1"),
                        lambda: dl.review_candidate(conn, cid, decision="decline", authority=OPERATOR, rationale="r"),
                        lambda: _supports(conn, "c2", "a2")):
            with pytest.raises(dl.ReviewRefused, match="judgments are disabled"):
                await attempt()
        monkeypatch.setenv(dl.JUDGMENTS_ENV, "1")
        assert await _admit(conn, "b1", "a1")                              # control: explicitly enabled
    run(scratch_dsn, scenario)


def test_seed_manifest_reveals_nothing_the_context_cannot_see(scratch_dsn):
    """C7: a public manifest returned a private claim's title with visible=false."""
    async def scenario(conn):
        await _fields(conn, [("S8", "harbours")])
        private = SOURCES["S8"]["rid"]
        for ctx in (dl.public_context(), dl.field_context(["harbours"]), dl.federated_context("peer:edge")):
            man = await dl.seed_manifest(conn, [private, "fixture:absent"], context=ctx, model=MODEL)
            text = json.dumps(man, default=str)
            assert man["entries"] == [] and man["moves"] == 0 and man["sources"] == []
            assert man["seed_eligible"] == 0 and man["admissible_for_live_judgment"] == 0
            assert man["unavailable_sources"] == sorted([private, "fixture:absent"])    # hidden == absent
            assert "private note" not in text and mid("h1") not in text
        personal = await dl.seed_manifest(conn, [private], context=CTX, model=MODEL)          # control
        assert [e["title"] for e in personal["entries"]] == [SOURCES["S8"]["moves"][0][2]]
        assert personal["unavailable_sources"] == []
    run(scratch_dsn, scenario, sources=BASE + ("S8",))


# ---------------------------------------------------------------------------
# Judgments over time and A3
# ---------------------------------------------------------------------------

def test_identical_judgments_are_refused_and_distinct_actors_are_not_merged(scratch_dsn):
    async def scenario(conn):
        await _supports(conn, "c2", "a2")
        with pytest.raises(dl.ReviewRefused, match="identical judgment"):
            await _supports(conn, "c2", "a2")
        await _supports(conn, "c2", "a2", authority=OTHER_OPERATOR)
        report = await dl.corroboration_report(conn, mid("a2"), context=CTX)
        assert report["current_supporting_moves"] == 1 and report["current_supporting_judgments"] == 2
        assert sorted(j["asserted_by"] for j in report["supporting"][0]["judgments"]) == \
            ["person:reviewer", "person:second-reviewer"]
    run(scratch_dsn, scenario)


def test_only_current_judgments_count_as_support(scratch_dsn):
    async def scenario(conn):
        await _supports(conn, "c2", "a2")
        await _supports(conn, "b1", "a1")
        assert (await dl.corroboration_report(conn, mid("a2"), context=CTX))["current_supporting_moves"] == 1
        assert (await dl.corroboration_report(conn, mid("a1"), context=CTX))["current_supporting_moves"] == 1
        await _set_detail(conn, "c2", "Edited after the judgment.")
        await conn.execute("UPDATE session_discourse_moves SET superseded_at = now() WHERE id = $1", mid("b1"))
        for target, reason in (("a2", "claim text changed"), ("a1", "move superseded")):
            report = await dl.corroboration_report(conn, mid(target), context=CTX)
            assert report["current_supporting_moves"] == 0
            [hist] = report["historical_supporting_judgments"]
            assert reason in hist["reasons"]
    run(scratch_dsn, scenario)


def test_dependence_is_scoped_to_the_restating_claim(scratch_dsn):
    async def scenario(conn):
        await _discover(conn, ["c1"])
        cid = str((await _cand(conn, "c1", "a2"))["id"])
        await dl.review_candidate(conn, cid, decision="admit", authority=OPERATOR,
                                  rationale="the post reports the field notes' result",
                                  relation="restates", semantics="lineage/v0")
        await _supports(conn, "c1", "a2")
        await _supports(conn, "c2", "a2")
        report = await dl.corroboration_report(conn, mid("a2"), context=CTX)
        dependence = {s["move_id"]: s["dependence"] for s in report["supporting"]}
        assert dependence == {mid("c1"): "restates_this_claim", mid("c2"): "not_assessed"}
        assert report["independence"]["evaluated"] is False and report["independence"]["independent_sources"] is None
    run(scratch_dsn, scenario)


def test_withdrawal_leaves_current_counts_and_keeps_history(scratch_dsn):
    async def scenario(conn):
        rel = await _supports(conn, "c2", "a2")
        assert (await dl.corroboration_report(conn, mid("a2"), context=CTX))["current_supporting_moves"] == 1
        with pytest.raises(dl.ReviewRefused, match="not allowed here"):
            await dl.withdraw_relation(conn, rel, authority=dl.mint_fixture_replay_authority("x"), rationale="r")
        await dl.withdraw_relation(conn, rel, authority=OPERATOR, rationale="the officers' account is second-hand")
        report = await dl.corroboration_report(conn, mid("a2"), context=CTX)
        assert report["current_supporting_moves"] == 0
        assert report["historical_supporting_judgments"][0]["reasons"] == ["withdrawn"]
        with pytest.raises(dl.ReviewRefused, match="already withdrawn"):
            await dl.withdraw_relation(conn, rel, authority=OPERATOR, rationale="again")
        with pytest.raises(dl.ReviewRefused, match="may be withdrawn"):
            await _supports(conn, "c2", "a2")
        assert await conn.fetchval("SELECT count(*) FROM discourse_relations") == 1      # nothing deleted
    run(scratch_dsn, scenario)


def test_correction_replaces_a_judgment_and_links_the_two(scratch_dsn):
    async def scenario(conn):
        rel = await _supports(conn, "b2", "a2")
        new = await dl.correct_relation(conn, rel, authority=OPERATOR, relation="challenges",
                                        semantics="epistemic-relation/v0",
                                        rationale="other harbours saw no fall; this argues against a2")
        view = await dl.connected_material(conn, mid("a2"), context=CTX)
        [cur] = view["relations"]
        [old] = view["historical_relations"]
        assert (cur["relation_id"], cur["relation"], cur["origin"]) == (new, "challenges", "correction")
        assert old["not_current_reasons"] == ["corrected"] and old["terminal_event"]["replaced_by"] == new
        with pytest.raises(dl.ReviewRefused, match="must change"):
            await dl.correct_relation(conn, new, authority=OPERATOR, relation="challenges",
                                      semantics="epistemic-relation/v0", rationale="same")
        await conn.execute("UPDATE document_ingestion_log SET content_hash=$1 WHERE document_rid='fixture:harbour-protocol'",
                           sha("protocol v2"))
        with pytest.raises(dl.ReviewRefused, match="source changed since extraction"):
            await dl.correct_relation(conn, new, authority=OPERATOR, relation="qualifies",
                                      semantics="epistemic-relation/v0", rationale="r")
    run(scratch_dsn, scenario)


def test_reopening_a_declined_candidate_keeps_the_earlier_decision(scratch_dsn):
    async def scenario(conn):
        await _discover(conn, ["b1"])
        cid = str((await _cand(conn, "b1", "a1"))["id"])
        with pytest.raises(dl.ReviewRefused, match="not declined"):
            await dl.reopen_candidate(conn, cid, authority=OPERATOR, rationale="r")
        await dl.review_candidate(conn, cid, decision="decline", authority=OPERATOR, rationale="looked unrelated")
        await dl.reopen_candidate(conn, cid, authority=OTHER_OPERATOR, rationale="on a second read it is related")
        assert (await _cand(conn, "b1", "a1"))["status"] == "candidate"
        prior = await conn.fetchval("SELECT prior_state FROM discourse_review_events WHERE candidate_id = $1", cid)
        assert '"looked unrelated"' in prior and '"declined"' in prior
        assert await _admit(conn, "b1", "a1")
    run(scratch_dsn, scenario)


def test_retrieval_separates_admitted_from_candidates_and_keeps_both_provenances(scratch_dsn):
    async def scenario(conn):
        await _discover(conn, ["b1", "c1"], per_target_source_cap=None, expanded_by=OPERATOR)
        await _admit(conn, "b1", "a1")
        plain = await dl.connected_material(conn, mid("a1"), context=CTX)
        assert plain["candidates"] == [] and plain["open_candidate_count"] >= 1
        [rel] = plain["relations"]
        assert rel["direction"] == "inbound" and rel["semantics"] == "design-alignment/v0" and rel["current"]
        assert plain["move"]["source"] == {
            "rid": "fixture:tidal-ledgers", "title": "Field notes on tidal ledgers",
            "source_url": "https://example.org/tidal", "version": "sha256:" + sha("tidal v1"),
            "version_status": "known", "version_basis": "document_ingestion_log.content_hash"}
        assert plain["move"]["extraction"]["binding_kind"] == "recorded_extraction"
        assert rel["other"]["source"]["rid"] == "fixture:harbour-protocol"
        full = await dl.connected_material(conn, mid("a1"), context=CTX, include_candidates=True)
        assert full["candidates"] and all(c["status"] == "candidate" for c in full["candidates"])
        assert all(c["other"]["source"]["rid"] != "fixture:tidal-ledgers" for c in full["candidates"])
    run(scratch_dsn, scenario)


def test_discovery_leaves_authored_relations_untouched(scratch_dsn):
    async def scenario(conn):
        rel = await dl.record_authored_relation(conn, mid("b2"), mid("a2"), relation="challenges",
                                                semantics="epistemic-relation/v0", authority=OPERATOR,
                                                rationale="other harbours saw no fall")
        before = dict(await conn.fetchrow("SELECT * FROM discourse_relations WHERE id=$1", rel))
        await _discover(conn, ["b1", "b2", "c1"])
        await _discover(conn, ["b1", "b2", "c1"], run_id="t2")
        assert dict(await conn.fetchrow("SELECT * FROM discourse_relations WHERE id=$1", rel)) == before
        assert await conn.fetchval("SELECT count(*) FROM discourse_relations") == 1
    run(scratch_dsn, scenario)


def test_coverage_reports_legs_and_binding_kinds_separately(scratch_dsn):
    async def scenario(conn):
        cov = await dl.retrieval_coverage(conn, MODEL, context=CTX)
        assert (cov["eligible"], cov["lexical"], cov["semantic_current"]) == (10, 10, 10)
        assert (cov["binding_recorded_extraction"], cov["binding_content_addressed_source"],
                cov["binding_inferred_timestamp"], cov["binding_unknown"]) == (7, 1, 1, 1)
        await _set_detail(conn, "a1", "Edited, not re-embedded.")
        await conn.execute("DELETE FROM discourse_move_embeddings WHERE move_id = $1", mid("b1"))
        cov = await dl.retrieval_coverage(conn, MODEL, context=CTX)
        assert (cov["lexical"], cov["semantic_current"], cov["semantic_stale"]) == (10, 8, 1)
        assert (await dl.retrieval_coverage(conn, None, context=CTX))["semantic_current"] == 0
    run(scratch_dsn, scenario, sources=BASE + ("S5", "S6", "S7"))


def test_seed_manifest_reports_eligibility_and_admissibility(scratch_dsn):
    async def scenario(conn):
        rids = [SOURCES[k]["rid"] for k in ("S1", "S4", "S5", "S6", "S7")] + ["fixture:absent"]
        man = await dl.seed_manifest(conn, rids, context=CTX, model=MODEL)
        assert man["unavailable_sources"] == ["fixture:absent"] and man["moves"] == 7
        by = {e["move_id"]: e for e in man["entries"]}
        assert not by[mid("d1")]["seed_eligible"] and by[mid("d1")]["ineligible_reasons"] == ["move superseded"]
        assert by[mid("e1")]["seed_eligible"] and not by[mid("e1")]["admissible_for_live_judgment"]
        assert by[mid("a1")]["admissible_for_live_judgment"] and by[mid("a1")]["embedding"] == "current"
        assert not by[mid("g1")]["admissible_for_live_judgment"]           # D1
        assert man["seed_eligible"] == 6 and man["admissible_for_live_judgment"] == 3
    run(scratch_dsn, scenario, sources=BASE + ("S5", "S6", "S7"))


# ---------------------------------------------------------------------------
# Router: authority is built server-side (Q2)
# ---------------------------------------------------------------------------

def _router_client(dsn, host="127.0.0.1"):
    import httpx
    from fastapi import FastAPI
    from api.routers.discourse_links_router import create_discourse_links_router

    async def make():
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        app = FastAPI()
        app.include_router(create_discourse_links_router(pool))
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app, client=(host, 5000)),
                                   base_url="http://test")
        return pool, client
    return make


def test_router_builds_authority_server_side(committed_dsn, monkeypatch):
    monkeypatch.delenv("KOI_STATE_DIR", raising=False)
    monkeypatch.setenv("KOI_DISCOURSE_OPERATOR_ACTOR", "person:operator")

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        await _discover(conn, ["b1"])
        cid = str((await _cand(conn, "b1", "a1"))["id"])
        await conn.close()
        pool, client = await _router_client(committed_dsn)()
        remote_pool, remote = await _router_client(committed_dsn, host="10.0.0.9")()
        review = {"decision": "admit", "rationale": "r", "relation": "aligns_with", "semantics": "design-alignment/v0"}
        try:
            monkeypatch.delenv("KOI_ADMIN_TOKEN", raising=False)
            assert (await client.post(f"/discourse-links/candidates/{cid}/review", json=review)).status_code == 503
            monkeypatch.setenv("KOI_ADMIN_TOKEN", "test-token")
            ok = {"Authorization": "Bearer test-token"}
            assert (await client.post(f"/discourse-links/candidates/{cid}/review", json=review)).status_code == 401
            assert (await remote.post(f"/discourse-links/candidates/{cid}/review", json=review,
                                      headers=ok)).status_code == 403
            forged = {**review, "actor": "person:someone-else", "basis": "fixture-replay"}
            assert (await client.post(f"/discourse-links/candidates/{cid}/review", json=forged,
                                      headers=ok)).status_code == 422
            # Agents propose from localhost without the token; the proposal is caller-declared.
            prop = await client.post(f"/discourse-links/candidates/{cid}/proposals", json={
                "relation": "aligns_with", "semantics": "design-alignment/v0", "rationale": "r",
                "proposed_by": "agent:labeller"})
            assert prop.status_code == 200
            assert (await remote.post(f"/discourse-links/candidates/{cid}/proposals", json={
                "relation": "aligns_with", "semantics": "design-alignment/v0", "rationale": "r",
                "proposed_by": "agent:x"})).status_code == 403
            done = await client.post(f"/discourse-links/candidates/{cid}/review", json=review, headers=ok)
            assert done.status_code == 200, done.text
            assert (done.json()["asserted_by"], done.json()["authority_basis"]) == ("person:operator",
                                                                                     dl.LOCAL_CREDENTIAL)
            view = (await client.get(f"/discourse-links/moves/{mid('a1')}/connected", headers=ok)).json()
            assert [r["asserted_by"] for r in view["relations"]] == ["person:operator"]
            assert (await client.get(f"/discourse-links/moves/{mid('a1')}/connected")).status_code == 401
            assert (await client.post(f"/discourse-links/candidates/{cid}/review", json=review,
                                      headers=ok)).status_code == 409
        finally:
            await client.aclose()
            await remote.aclose()
            await pool.close()
            await remote_pool.close()
    asyncio.run(scenario())


def test_router_cannot_reach_fixture_replay_or_discovery():
    from api.routers import discourse_links_router as router_module
    source = inspect.getsource(router_module)
    assert "mint_fixture_replay_authority" not in source
    assert "discover_candidates" not in source and "record_extraction_bindings" not in source
    for model in (router_module.ReviewBody, router_module.CorrectionBody, router_module.RationaleBody):
        assert not {"actor", "basis", "asserted_by", "authority_basis"} & set(model.model_fields)


# ---------------------------------------------------------------------------
# Canary script: approved seed (C6) and run-scoped rollback (C5)
# ---------------------------------------------------------------------------

SEED = (SOURCES["S2"]["rid"],)


async def _manifest(dsn, tmp_path, name="manifest.json", edit=None, sources=SEED, model=MODEL, **kw):
    conn = await asyncpg.connect(dsn)
    try:
        m = await canary.build_manifest(conn, sources=sources, model=model, **kw)
    finally:
        await conn.close()
    if edit:
        edit(m)
    path = tmp_path / name
    data = canary.manifest_bytes(m)
    path.write_bytes(data)
    return path, hashlib.sha256(data).hexdigest(), m


async def _state(dsn):
    """Everything a run could touch, for before/after comparison."""
    conn = await asyncpg.connect(dsn)
    try:
        return {
            "embeddings": [dict(r) for r in await conn.fetch(
                "SELECT move_id, model, representation_sha256, text_sha256, dimension, embedding::text AS e, "
                "created_at FROM discourse_move_embeddings ORDER BY move_id, model")],
            "candidates": [dict(r) for r in await conn.fetch(
                "SELECT id, candidate_key, status, times_seen, last_seen_at, proposed_by, run_id "
                "FROM discourse_link_candidates ORDER BY id")],
            "representations": [r["representation_sha256"] for r in await conn.fetch(
                "SELECT representation_sha256 FROM discourse_claim_representations ORDER BY 1")],
        }
    finally:
        await conn.close()


def _discover_with(dsn, path=None, digest=None, embedder=None, run_id="canary:t", **kw):
    return canary.run_discover(dsn, run_id, embedder=embedder or ConceptEmbedder(), manifest_path=path,
                               manifest_sha256=digest, **kw)


def test_canary_discover_requires_the_approved_manifest_and_refuses_drift(committed_dsn, tmp_path, monkeypatch):
    async def scenario():
        monkeypatch.delenv("KOI_DISCOURSE_DISCOVERY", raising=False)
        path, digest, m = await _manifest(committed_dsn, tmp_path, text_prefix="")
        assert m["approval_scope"]["seed_move_ids"] == sorted([mid("b1"), mid("b2")])
        assert m["approval_scope"]["limits"] == {"max_embedding_writes": 2, "max_candidate_inserts": 10}
        with pytest.raises(SystemExit, match="discovery is off"):
            await _discover_with(committed_dsn, path, digest)
        monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")
        with pytest.raises(SystemExit, match="approved manifest and its sha256"):
            await _discover_with(committed_dsn)                            # the probe's call
        with pytest.raises(SystemExit, match="sha256 is"):
            await _discover_with(committed_dsn, path, "0" * 64)
        with pytest.raises(SystemExit, match="differs from the approved"):
            await _discover_with(committed_dsn, path, digest, model="other-model")
        before = await _state(committed_dsn)
        conn = await asyncpg.connect(committed_dsn)
        try:
            # The probe's drift: a claim added to a seed source after review.
            await conn.execute("INSERT INTO session_discourse_moves (id, source_type, source_rid, move_type, title) "
                               "VALUES ($1, 'document', $2, 'claim', 'A newly added landing record names the vessel.')",
                               mid("new-after-review"), SEED[0])
            with pytest.raises(SystemExit, match="present now but not in the approved manifest"):
                await _discover_with(committed_dsn, path, digest, run_id="canary:added")
            await conn.execute("DELETE FROM session_discourse_moves WHERE id = $1", mid("new-after-review"))
            # A reviewed claim's text, or its source version, changes after review.
            await _set_detail(conn, "b1", "Edited after review.")
            with pytest.raises(SystemExit, match="representation_sha256"):
                await _discover_with(committed_dsn, path, digest, run_id="canary:edited")
            await _set_detail(conn, "b1", None)
            await conn.execute("UPDATE document_ingestion_log SET content_hash=$2 WHERE document_rid=$1",
                               SEED[0], sha("protocol v2"))
            with pytest.raises(SystemExit, match="source_version"):
                await _discover_with(committed_dsn, path, digest, run_id="canary:reversioned")
            await conn.execute("UPDATE document_ingestion_log SET content_hash=$2 WHERE document_rid=$1",
                               SEED[0], SOURCES["S2"]["content_hash"])
            assert await _state(committed_dsn) == before                  # every refusal wrote nothing
            failed = await conn.fetch("SELECT run_id FROM discourse_link_runs WHERE status = 'failed' ORDER BY 1")
            assert [r["run_id"] for r in failed] == ["canary:added", "canary:edited", "canary:reversioned"]
            # Control: the unchanged seed runs, within its approved limits.
            result = await _discover_with(committed_dsn, path, digest, run_id="canary:ok")
            assert result["seed_moves"] == 2 and 0 < result["discovery"]["inserted"] <= 10
            assert result["effects"].get("embedding_inserted", 0) == 0     # fixture vectors were current: reused
            with pytest.raises(SystemExit, match="already used"):
                await _discover_with(committed_dsn, path, digest, run_id="canary:ok")
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_canary_limits_and_failures_leave_nothing_behind(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    class BrokenEmbedder:
        async def embed_batch(self, texts):
            raise RuntimeError("embedding provider down")

    async def scenario():
        before = await _state(committed_dsn)
        # A different text prefix forces re-embedding, so the failing runs do write before failing.
        tight, tight_digest, _ = await _manifest(
            committed_dsn, tmp_path, "tight.json", text_prefix="search_document: ",
            edit=lambda m: m["approval_scope"]["limits"].update(max_candidate_inserts=1))
        with pytest.raises(SystemExit, match="failed and wrote nothing.*candidates > approved 1"):
            await _discover_with(committed_dsn, tight, tight_digest, run_id="canary:over-limit")
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="search_document: ")
        with pytest.raises(SystemExit, match="failed and wrote nothing.*provider down"):
            await _discover_with(committed_dsn, path, digest, embedder=BrokenEmbedder(), run_id="canary:broken")
        assert await _state(committed_dsn) == before
        conn = await asyncpg.connect(committed_dsn)
        try:
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_run_effects") == 0
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_canary_rollback_undoes_exactly_what_its_run_did(committed_dsn, tmp_path, monkeypatch):
    """C5: rolling back canary:never-created deleted two pre-existing seed embeddings."""
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await _discover(conn, ["c1"], run_id="other-run", per_target_source_cap=None, expanded_by=OPERATOR)
        finally:
            await conn.close()
        before = await _state(committed_dsn)
        assert len([e for e in before["embeddings"] if str(e["move_id"]) in (mid("b1"), mid("b2"))]) == 2
        with pytest.raises(SystemExit, match="no run canary:never-created"):
            await canary.run_rollback(committed_dsn, "canary:never-created", model=MODEL, sources=list(SEED))
        assert await _state(committed_dsn) == before                      # the probe's case: nothing deleted
        # The run replaces the two pre-existing vectors (new prefix) and meets other-run's candidates again.
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="search_document: ")
        result = await _discover_with(committed_dsn, path, digest, run_id="canary:r")
        assert result["effects"]["embedding_replaced"] == 2 and result["effects"]["candidate_seen"] >= 1
        assert await _state(committed_dsn) != before
        rolled = await canary.run_rollback(committed_dsn, "canary:r")
        assert rolled.get("embeddings_restored") == 2 and rolled.get("embeddings_deleted", 0) == 0
        assert rolled.get("candidates_deleted") == result["discovery"]["inserted"]
        assert rolled.get("candidates_restored", 0) >= 1
        assert await _state(committed_dsn) == before                      # vectors, other-run counts and texts back
        with pytest.raises(SystemExit, match="is rolled_back"):
            await canary.run_rollback(committed_dsn, "canary:r")
        with pytest.raises(SystemExit, match="is failed"):
            conn = await asyncpg.connect(committed_dsn)
            try:
                await conn.execute("INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, "
                                   "status) VALUES ('canary:f', 'canary', 'x', '{}', 'failed')")
            finally:
                await conn.close()
            await canary.run_rollback(committed_dsn, "canary:f")
    asyncio.run(scenario())


def test_canary_rollback_refuses_when_ownership_is_unclear(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    async def scenario():
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="")
        await _discover_with(committed_dsn, path, digest, run_id="canary:u")
        conn = await asyncpg.connect(committed_dsn)
        try:
            cid = await conn.fetchval("SELECT id FROM discourse_link_candidates WHERE run_id = 'canary:u' LIMIT 1")
            await dl.propose_relation(conn, str(cid), relation="related_to", semantics="topical/v0",
                                      proposed_by="agent:labeller", rationale="r")
            with pytest.raises(SystemExit, match="proposed on, reviewed or judged"):
                await canary.run_rollback(committed_dsn, "canary:u")
            await conn.execute("UPDATE discourse_link_candidates SET proposed_relation = NULL, proposed_semantics = NULL, "
                               "proposed_by = NULL, proposal_rationale = NULL WHERE id = $1", cid)
            await _discover(conn, ["b1", "b2"], run_id="later-run")          # meets the canary's pairs again
            with pytest.raises(SystemExit, match="seen again after the run"):
                await canary.run_rollback(committed_dsn, "canary:u")
            assert await conn.fetchval("SELECT status FROM discourse_link_runs WHERE run_id = 'canary:u'") == "completed"
            async with conn.transaction(readonly=True):
                report = await canary.build_report(conn, "canary:u")
            assert report["counts"]["judgments_total"] == 0 and report["run"]["status"] == "completed"
        finally:
            await conn.close()
    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Canary operational limits: deadline, lock timeout, one writer, write scope
# ---------------------------------------------------------------------------

class StallingEmbedder:
    """Answers the first batch, then never answers again."""

    def __init__(self):
        self.calls = 0

    async def embed_batch(self, texts):
        self.calls += 1
        if self.calls > 1:
            await asyncio.sleep(3600)
        return await ConceptEmbedder().embed_batch(texts)


async def _alone(conn, wait_s: float = 3.0) -> bool:
    """No other client session on this database, allowing a terminated one a moment to exit."""
    deadline = time.monotonic() + wait_s
    while await _others_connected(conn):
        if time.monotonic() > deadline:
            return False
        await asyncio.sleep(0.05)
    return True


async def _others_connected(conn) -> int:
    """Other client sessions on this database. Autovacuum workers on a fresh scratch database are not sessions."""
    return await conn.fetchval("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
                               "AND pid <> pg_backend_pid() AND backend_type = 'client backend'")


def test_canary_interruption_releases_locks_and_leaves_nothing(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    async def scenario():
        # A new prefix makes both seed vectors stale, so the first batch really writes before the stall.
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="search_document: ")
        before = await _state(committed_dsn)
        embedder = StallingEmbedder()
        started = time.monotonic()
        with pytest.raises(SystemExit, match="deadline of 1.5 s exceeded"):
            await canary.run_discover(committed_dsn, "canary:stall", embedder=embedder, manifest_path=path,
                                      manifest_sha256=digest, deadline_s=1.5, embed_batch_size=1)
        assert embedder.calls == 2                                        # one batch written, then the stall
        assert time.monotonic() - started < 6                             # the session is ended at once, not by the 10 s fallback
        assert await _state(committed_dsn) == before                      # no partial vectors, candidates or texts
        conn = await asyncpg.connect(committed_dsn)
        try:
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_run_effects") == 0
            run = await conn.fetchrow("SELECT status, error FROM discourse_link_runs WHERE run_id = 'canary:stall'")
            assert run["status"] == "failed" and "deadline" in run["error"]   # only the diagnostic row remains
            assert await _others_connected(conn) == 0                     # the canary's session is gone
            async with conn.transaction():                                # and it holds no lock on any table
                await conn.execute("SET LOCAL lock_timeout = '100ms'")
                for table in canary.DRAFT_TABLES:
                    await conn.execute(f"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE NOWAIT")
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_canary_lock_timeout_fails_fast_instead_of_waiting(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    async def scenario():
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="")
        before = await _state(committed_dsn)
        blocker = await asyncpg.connect(committed_dsn)
        tx = blocker.transaction()
        await tx.start()
        await blocker.execute("LOCK TABLE discourse_link_candidates IN ACCESS EXCLUSIVE MODE")
        try:
            started = time.monotonic()
            with pytest.raises(SystemExit, match="lock timeout"):
                await _discover_with(committed_dsn, path, digest, run_id="canary:blocked", deadline_s=60)
            assert time.monotonic() - started < 20                         # 2 s lock_timeout, not the 60 s deadline
        finally:
            await tx.rollback()
            await blocker.close()
        assert await _state(committed_dsn) == before
    asyncio.run(scenario())


def test_only_one_canary_writer_runs_at_a_time(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    async def scenario():
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="")
        before = await _state(committed_dsn)
        holder = await asyncpg.connect(committed_dsn)
        try:
            await holder.execute("SELECT pg_advisory_lock(hashtext($1))", canary.WRITER_LOCK)
            with pytest.raises(SystemExit, match="writer lock"):
                await _discover_with(committed_dsn, path, digest, run_id="canary:second")
            assert await _state(committed_dsn) == before
            await holder.execute("SELECT pg_advisory_unlock(hashtext($1))", canary.WRITER_LOCK)
        finally:
            await holder.close()
        result = await _discover_with(committed_dsn, path, digest, run_id="canary:first")   # control
        assert result["discovery"]["inserted"] > 0
    asyncio.run(scenario())


def test_a_commit_whose_response_was_lost_is_reported_as_committed(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")
    real = canary._discover_body

    async def committed_then_lost(*args, **kwargs):
        await real(*args, **kwargs)
        raise ConnectionError("connection lost after commit")
    monkeypatch.setattr(canary, "_discover_body", committed_then_lost)

    async def scenario():
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="")
        with pytest.raises(SystemExit, match="COMMITTED"):
            await _discover_with(committed_dsn, path, digest, run_id="canary:lost")
        conn = await asyncpg.connect(committed_dsn)
        try:
            assert await conn.fetchval("SELECT status FROM discourse_link_runs WHERE run_id = 'canary:lost'") == "completed"
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates WHERE run_id = 'canary:lost'") > 0
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_canary_refuses_to_commit_writes_outside_its_tables(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")
    real = dl.embed_moves

    async def leaky(conn, *args, **kwargs):
        await conn.execute("UPDATE document_ingestion_log SET title = title WHERE document_rid = $1", SEED[0])
        return await real(conn, *args, **kwargs)

    async def scenario():
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="")
        before = await _state(committed_dsn)
        monkeypatch.setattr(dl, "embed_moves", leaky)
        with pytest.raises(SystemExit, match="wrote outside its tables.*document_ingestion_log"):
            await _discover_with(committed_dsn, path, digest, run_id="canary:leaky")
        monkeypatch.setattr(dl, "embed_moves", real)
        assert await _state(committed_dsn) == before
        result = await _discover_with(committed_dsn, path, digest, run_id="canary:clean")       # control
        assert set(result["table_writes"]) <= {f"public.{t}" for t in canary.DRAFT_TABLES}
        assert "public.discourse_link_candidates" in result["table_writes"]
        assert set(result["timings"]) == {"embed_s", "discover_s", "transaction_s"}
        assert result["deadline_s"] == canary.CANARY_DEADLINE_S
    asyncio.run(scenario())


def test_deadline_override_is_bounded_and_forwarded(committed_dsn, tmp_path, monkeypatch):
    """--deadline-s: default 180, at most 600, and the effective value reaches the run and its ledger row."""
    for text, value in (("600", 600.0), ("0.5", 0.5), ("180", 180.0)):
        assert canary.deadline_seconds(text) == value
    for text in ("0", "-1", "600.5", "nan", "inf", "ten"):
        with pytest.raises(argparse.ArgumentTypeError):
            canary.deadline_seconds(text)
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")
    with pytest.raises(SystemExit, match="deadline of 601"):          # the programmatic path is bounded too
        asyncio.run(canary.run_discover(committed_dsn, "canary:long", embedder=None, deadline_s=601))

    data = canary.manifest_bytes({"approval_scope": {"kind": "canary", "model": MODEL}})
    path = tmp_path / "cli.json"
    path.write_bytes(data)
    argv = ["discourse_links_canary.py", "discover", "--manifest", str(path),
            "--manifest-sha256", hashlib.sha256(data).hexdigest(), "--dsn", committed_dsn,
            "--run-id", "canary:cli", "--apply"]
    seen = []

    async def recording_run_discover(dsn, run_id, **kw):
        seen.append(kw["deadline_s"])
        return {}
    monkeypatch.setattr(canary, "run_discover", recording_run_discover)
    monkeypatch.setattr(demo, "_Ollama", lambda model: object())
    for extra, value in (([], 180), (["--deadline-s", "600"], 600.0)):
        monkeypatch.setattr(sys, "argv", argv + extra)
        canary.main()
        assert seen[-1] == value
    monkeypatch.setattr(sys, "argv", argv + ["--deadline-s", "601"])
    with pytest.raises(SystemExit) as refused:
        canary.main()
    assert refused.value.code == 2 and len(seen) == 2                  # refused before any run
    monkeypatch.undo()

    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    async def scenario():                                               # forwarded into the run's ledger row
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="")
        await _discover_with(committed_dsn, path, digest, run_id="canary:deadline", deadline_s=45)
        conn = await asyncpg.connect(committed_dsn)
        try:
            summary = await conn.fetchval("SELECT summary FROM discourse_link_runs WHERE run_id = 'canary:deadline'")
            assert json.loads(summary)["deadline_s"] == 45
        finally:
            await conn.close()
    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Two-phase discovery (O2): plan outside the write transaction, apply briefly
# ---------------------------------------------------------------------------

async def _plan(dsn, tmp_path, run_id, path, digest, embedder=None, **kw):
    out = tmp_path / f"{run_id.replace(':', '_')}.plan.json"
    result = await canary.run_plan(dsn, run_id, embedder=embedder or ConceptEmbedder(), manifest_path=path,
                                   manifest_sha256=digest, out=out, **kw)
    return out, result["sha256"]


async def _apply(dsn, out, sha_, path, digest, **kw):
    return await canary.run_apply(dsn, plan_path=out, plan_sha256=sha_, manifest_path=path,
                                  manifest_sha256=digest, **kw)


async def _comparable(dsn):
    """What a run leaves behind, without ids and clock times."""
    conn = await asyncpg.connect(dsn)
    try:
        return {
            "candidates": [dict(r) for r in await conn.fetch(
                """SELECT candidate_key, query_move_id, target_move_id, query_source_rid, target_source_rid,
                          query_representation_sha256, target_representation_sha256, query_binding_kind,
                          target_binding_kind, query_bound_version, target_bound_version, method, method_params,
                          lexical_rank, lexical_score, semantic_rank, semantic_score, fused_score, status,
                          discovered_by, run_id, times_seen
                     FROM discourse_link_candidates ORDER BY candidate_key""")],
            "embeddings": [dict(r) for r in await conn.fetch(
                "SELECT move_id, model, representation_sha256, text_sha256, dimension, embedding::text AS e "
                "FROM discourse_move_embeddings ORDER BY move_id, model")],
            "effects": [dict(r) for r in await conn.fetch(
                """SELECT e.effect, e.move_id, e.model, c.candidate_key, e.representation_sha256,
                          e.after_state->'times_seen' AS times_seen, e.after_state->'occurrence' AS occurrence
                     FROM discourse_link_run_effects e LEFT JOIN discourse_link_candidates c ON c.id = e.candidate_id
                    ORDER BY e.id""")],
            "representations": [dict(r) for r in await conn.fetch(
                "SELECT representation_sha256, move_id, move_type, title, detail, status "
                "FROM discourse_claim_representations ORDER BY 1")],
        }
    finally:
        await conn.close()


EQ_SOURCES = BASE + ("S5", "S6", "S7", "S8", "S9")
EQ_SEED = tuple(SOURCES[k]["rid"] for k in ("S1", "S2", "S3"))


@pytest.mark.parametrize("model,prefix", [(MODEL, "search_document: "), ("test:concept-count-v2", "")],
                         ids=["vectors-replaced", "vectors-new"])
def test_two_phase_matches_single_transaction_discovery(tmp_path, monkeypatch, model, prefix):
    """On identical inputs, plan+apply writes what discover writes: the same
    pairs, ranks, scores, sightings, vectors, texts and effect sequence."""
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")
    dbs = [_new_scratch() for _ in range(2)]

    async def scenario():
        if True:
            for _, dsn in dbs:
                conn = await asyncpg.connect(dsn)
                try:
                    await _load(conn, EQ_SOURCES)
                finally:
                    await conn.close()
            (_, one), (_, two) = dbs
            p1, d1, _ = await _manifest(one, tmp_path, "one.json", sources=EQ_SEED, model=model, text_prefix=prefix)
            p2, d2, _ = await _manifest(two, tmp_path, "two.json", sources=EQ_SEED, model=model, text_prefix=prefix)
            single = await _discover_with(one, p1, d1, run_id="canary:eq")
            out, plan_sha = await _plan(two, tmp_path, "canary:eq", p2, d2)
            plan = json.loads(out.read_bytes())
            two_phase = await _apply(two, out, plan_sha, p2, d2)
            a, b = await _comparable(one), await _comparable(two)
            assert a == b
            assert all(e["occurrence"] for e in a["effects"] if e["effect"].startswith("candidate_"))   # both paths record picks
            assert single["effects"] == two_phase["effects"]
            assert {k: v for k, v in two_phase["effects"].items() if k != "representation_inserted"} \
                == plan["planned_effects"]
            assert ({k: v for k, v in single["discovery"].items()} == plan["summary"] == two_phase["discovery"])
            # The comparison covers the interesting cases.
            assert a["candidates"] and any(c["semantic_rank"] for c in a["candidates"])
            assert any(c["lexical_rank"] is None for c in a["candidates"])
            assert two_phase["effects"]["candidate_seen"] > 0               # pairs met from both sides
            assert two_phase["effects"]["embedding_inserted" if model != MODEL else "embedding_replaced"] == 7
            assert plan["summary"]["capped_by_target_source"] > 0
            assert not any(c["target_source_rid"].startswith("mediawiki:") for c in a["candidates"])
            assert two_phase["applied"]["endpoints_locked_for_share"] == len(plan["endpoints"])
    try:
        asyncio.run(scenario())
    finally:
        for name, _ in dbs:
            _drop_scratch(name)


def test_planning_writes_nothing_and_an_interrupted_plan_leaves_no_file(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    class BrokenEmbedder:
        async def embed_batch(self, texts):
            raise RuntimeError("embedding provider down")

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            with pytest.raises(dl.PlanRefused, match="READ ONLY"):           # the database must refuse any write
                await dl.plan_discovery(conn, ConceptEmbedder(), [mid("b1")], context=CTX,
                                        discovered_by="agent:test", run_id="canary:rw", embedding_model=MODEL)
        finally:
            await conn.close()
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="search_document: ")
        before = await _state(committed_dsn)
        started = time.monotonic()
        with pytest.raises(SystemExit, match="no plan was written: deadline of 1.5 s exceeded"):
            await _plan(committed_dsn, tmp_path, "canary:stall", path, digest, embedder=StallingEmbedder(),
                        embed_batch_size=1, deadline_s=1.5)
        assert time.monotonic() - started < 6
        with pytest.raises(SystemExit, match="no plan was written: RuntimeError: embedding provider down"):
            await _plan(committed_dsn, tmp_path, "canary:broken", path, digest, embedder=BrokenEmbedder())
        assert not list(tmp_path.glob("*.plan.json*"))                      # neither a plan nor a partial file
        assert await _state(committed_dsn) == before
        conn = await asyncpg.connect(committed_dsn)
        try:
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_runs") == 0   # planning records no run
            assert await _alone(conn)
        finally:
            await conn.close()
        # Control: the same inputs plan; the plan is written once and never overwritten.
        out, _ = await _plan(committed_dsn, tmp_path, "canary:ok", path, digest)
        plan = json.loads(out.read_bytes())
        assert plan["planned_effects"]["embedding_replaced"] == 2 and plan["planned_effects"]["candidate_inserted"] > 0
        assert await _state(committed_dsn) == before                        # still nothing written
        with pytest.raises(SystemExit, match="never overwritten"):
            await _plan(committed_dsn, tmp_path, "canary:ok", path, digest)
    asyncio.run(scenario())


def test_apply_refuses_any_change_to_what_the_plan_bound(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    async def scenario():
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="")
        conn = await asyncpg.connect(committed_dsn)
        try:
            async def refused(run_id, change, undo, match):
                out, plan_sha = await _plan(committed_dsn, tmp_path, run_id, path, digest)
                plan = json.loads(out.read_bytes())
                target = next(s for s in plan["sightings"] if s["semantic_rank"])["target_move_id"]
                await change(target)
                before = await _state(committed_dsn)
                with pytest.raises(SystemExit, match=f"failed and wrote nothing: PlanRefused: .*{match}"):
                    await _apply(committed_dsn, out, plan_sha, path, digest)
                assert await _state(committed_dsn) == before
                assert await conn.fetchval("SELECT status FROM discourse_link_runs WHERE run_id = $1", run_id) == "failed"
                await undo(target)

            async def detail(t, value="Edited after planning."):
                await conn.execute("UPDATE session_discourse_moves SET detail = $2 WHERE id = $1", t, value)

            async def superseded(t, value=True):
                await conn.execute("UPDATE session_discourse_moves SET superseded_at = CASE WHEN $2 THEN now() END "
                                   "WHERE id = $1", t, value)

            async def source_hash(t, value="protocol v2"):
                await conn.execute("UPDATE document_ingestion_log l SET content_hash = $2 FROM session_discourse_moves m "
                                   "WHERE m.id = $1 AND l.document_rid = m.source_rid", t,
                                   sha(value) if value else None)

            async def vector(t, value="changed"):
                await conn.execute("UPDATE discourse_move_embeddings SET text_sha256 = $2 WHERE move_id = $1",
                                   t, value)

            restore_hash = {}

            async def remember_hash(t):
                restore_hash[t] = await conn.fetchval(
                    "SELECT l.content_hash FROM document_ingestion_log l JOIN session_discourse_moves m "
                    "ON l.document_rid = m.source_rid WHERE m.id = $1", t)
                await source_hash(t)

            async def undo_hash(t):
                await conn.execute("UPDATE document_ingestion_log l SET content_hash = $2 FROM session_discourse_moves m "
                                   "WHERE m.id = $1 AND l.document_rid = m.source_rid", t, restore_hash[t])

            stamp = {}

            async def remember_vector(t):
                stamp[t] = await conn.fetchval("SELECT text_sha256 FROM discourse_move_embeddings WHERE move_id = $1", t)
                await vector(t)

            await refused("canary:edited", detail, lambda t: detail(t, None), "representation_sha256 changed")
            await refused("canary:retired", superseded, lambda t: superseded(t, False), "eligible changed")
            await refused("canary:reversioned", remember_hash, undo_hash, "source_version changed")
            await refused("canary:revectored", remember_vector, lambda t: vector(t, stamp[t]),
                          "the stored vector the plan ranked with changed")

            async def interloper(t):                                        # another discovery between plan and apply
                await _discover(conn, ["b1", "b2"], run_id="interloper")

            async def drop_interloper(t):
                await conn.execute("DELETE FROM discourse_link_candidates WHERE run_id = 'interloper'")
            await refused("canary:raced", interloper, drop_interloper, "stored state changed since planning")

            # Control: an unchanged plan applies.
            out, plan_sha = await _plan(committed_dsn, tmp_path, "canary:ok", path, digest)
            result = await _apply(committed_dsn, out, plan_sha, path, digest)
            assert result["effects"]["candidate_inserted"] > 0
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_apply_refuses_a_changed_stored_vector_whose_input_hashes_are_unchanged(committed_dsn, tmp_path, monkeypatch):
    """Review repro (2026-10-20): only discourse_move_embeddings.embedding changed,
    representation and text hashes kept; apply accepted the old plan and inserted
    eight candidates. The pin now covers the exact stored value and dimension."""
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            async def row(move_id):
                return dict(await conn.fetchrow(
                    "SELECT representation_sha256, text_sha256, dimension, embedding::text AS e "
                    "FROM discourse_move_embeddings WHERE move_id = $1 AND model = $2", move_id, MODEL))

            async def value_only(move_id):
                xs = [float(x) for x in (await row(move_id))["e"].strip("[]").split(",")]
                xs[0] += 0.001
                await conn.execute("UPDATE discourse_move_embeddings SET embedding = $3::vector "
                                   "WHERE move_id = $1 AND model = $2", move_id, MODEL, dl._vector_text(xs))

            async def dimension_only(move_id):
                await conn.execute("UPDATE discourse_move_embeddings SET dimension = 99 "
                                   "WHERE move_id = $1 AND model = $2", move_id, MODEL)

            async def case(run_id, prefix, pick, change, match):
                path, digest, _ = await _manifest(committed_dsn, tmp_path, f"{run_id[7:]}.json", text_prefix=prefix)
                out, plan_sha = await _plan(committed_dsn, tmp_path, run_id, path, digest)
                plan = json.loads(out.read_bytes())
                move_id = pick(plan)
                original = await row(move_id)
                await change(move_id)
                changed = await row(move_id)
                assert (changed["representation_sha256"], changed["text_sha256"]) \
                    == (original["representation_sha256"], original["text_sha256"])     # input hashes unchanged
                assert changed != original
                with pytest.raises(SystemExit, match=f"failed and wrote nothing: PlanRefused: .*{match}"):
                    await _apply(committed_dsn, out, plan_sha, path, digest)
                assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates WHERE run_id = $1",
                                           run_id) == 0
                await conn.execute("UPDATE discourse_move_embeddings SET embedding = $3::vector, dimension = $4 "
                                   "WHERE move_id = $1 AND model = $2", move_id, MODEL, original["e"],
                                   original["dimension"])
                assert await row(move_id) == original

            ranked_with = lambda plan: next(iter(plan["stored_embeddings_used"]))   # noqa: E731
            replaced = lambda plan: plan["embeddings"][0]["move_id"]                 # noqa: E731
            await case("canary:value", "", ranked_with, value_only,
                       r"the stored vector the plan ranked with changed \(vector_sha256\)")
            await case("canary:dimension", "", ranked_with, dimension_only,
                       r"the stored vector the plan ranked with changed \(dimension\)")
            await case("canary:prior", "search_document: ", replaced, value_only,
                       r"the stored vector it replaces changed since planning \(vector_sha256\)")
            # Control: the same plans, vectors unchanged, apply.
            for run_id, prefix in (("canary:same", ""), ("canary:same-prior", "search_document: ")):
                path, digest, _ = await _manifest(committed_dsn, tmp_path, f"{run_id[7:]}.json", text_prefix=prefix)
                out, plan_sha = await _plan(committed_dsn, tmp_path, run_id, path, digest)
                plan = json.loads(out.read_bytes())
                assert all(set(s) == {"representation_sha256", "text_sha256", "dimension", "vector_sha256"}
                           for s in plan["stored_embeddings_used"].values()) and plan["stored_embeddings_used"]
                result = await _apply(committed_dsn, out, plan_sha, path, digest)
                assert result["effects"].get("candidate_inserted", 0) + result["effects"].get("candidate_seen", 0) > 0
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_apply_holds_the_rows_it_checked_until_commit(committed_dsn):
    """Moves and the stored vectors the plan ranked with stay locked FOR SHARE until commit."""
    async def scenario():
        conn, other = await asyncpg.connect(committed_dsn), await asyncpg.connect(committed_dsn)
        try:
            async with conn.transaction(readonly=True):
                plan = await dl.plan_discovery(conn, ConceptEmbedder(), [mid("b1"), mid("b2")], context=CTX,
                                               discovered_by="agent:test", run_id="held", embedding_model=MODEL)
            target = next(iter(plan["stored_embeddings_used"]))
            writes = ("UPDATE session_discourse_moves SET detail = detail WHERE id = $1",
                      "UPDATE discourse_move_embeddings SET text_sha256 = text_sha256 WHERE move_id = $1")
            tx = conn.transaction()
            await tx.start()
            await conn.execute("INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, status) "
                               "VALUES ('held', 'canary', 'x', '{}', 'completed')")
            await dl.apply_plan(conn, plan, context=CTX, recorder=dl.RunRecorder(
                "held", max_embedding_writes=10, max_candidate_inserts=50))
            for sql in writes:
                with pytest.raises(asyncpg.LockNotAvailableError):
                    async with other.transaction():
                        await other.execute("SET LOCAL lock_timeout = '200ms'")
                        await other.execute(sql, target)
            await tx.rollback()
            for sql in writes:                                                # control: free after the transaction
                async with other.transaction():
                    await other.execute("SET LOCAL lock_timeout = '200ms'")
                    await other.execute(sql, target)
        finally:
            await conn.close()
            await other.close()
    asyncio.run(scenario())


def test_apply_rechecks_visibility_in_the_plans_own_context(committed_dsn):
    fctx = dl.field_context({"f1"})

    async def plan_in(conn, context, **kw):
        async with conn.transaction(readonly=True):
            return await dl.plan_discovery(conn, ConceptEmbedder(), [mid("b1"), mid("b2")], context=context,
                                           discovered_by="agent:test", run_id="perm", embedding_model=MODEL, **kw)

    async def apply_in(conn, plan, context, **kw):
        tx = conn.transaction()
        await tx.start()
        try:
            await conn.execute("INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, status) "
                               "VALUES ('perm', 'canary', 'x', '{}', 'completed')")
            recorder = dl.RunRecorder("perm", max_embedding_writes=10, max_candidate_inserts=50)
            return await dl.apply_plan(conn, plan, context=context, recorder=recorder, **kw)
        finally:
            await tx.rollback()

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await _fields(conn, [("S1", "f1"), ("S2", "f1"), ("S3", "f1")])
            plan = await plan_in(conn, fctx)
            s1 = SOURCES["S1"]["rid"]
            assert any(s["target_source_rid"] == s1 for s in plan["sightings"])
            await conn.execute("DELETE FROM document_field_membership WHERE document_rid = $1", s1)
            with pytest.raises(dl.PlanRefused, match="visible changed"):
                await apply_in(conn, plan, fctx)
            await _fields(conn, [("S1", "f1")])
            await conn.execute("INSERT INTO koi_memories (rid, event_type, source_sensor, content, is_private) "
                               "VALUES ($1, 'NEW', 'fixture', '{}'::jsonb, true)", s1)  # metadata defaults
            with pytest.raises(dl.PlanRefused, match="visible changed"):    # the source became private
                await apply_in(conn, plan, fctx)
            await conn.execute("DELETE FROM koi_memories WHERE rid = $1", s1)
            with pytest.raises(dl.PlanRefused, match="another caller context"):
                await apply_in(conn, plan, CTX)
            applied = await apply_in(conn, plan, fctx)                      # control
            assert applied["sightings_written"] == len(plan["sightings"]) > 0
            # The expanded view needs authority when the plan is applied, not only when it is made.
            wide = await plan_in(conn, fctx, per_target_source_cap=None, expanded_by=OPERATOR)
            with pytest.raises(dl.ReviewRefused, match="minted AdmissionAuthority"):
                await apply_in(conn, wide, fctx)
            assert (await apply_in(conn, wide, fctx, expanded_by=OPERATOR))["sightings_written"] > 0
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_a_failed_or_interrupted_apply_leaves_nothing_and_is_not_retried(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")
    real = dl._write_candidate

    def failing_after(n, how):
        calls = []

        async def write(conn, *args, **kwargs):
            calls.append(1)
            if len(calls) > n:
                if how == "stall":
                    await asyncio.sleep(3600)
                raise RuntimeError("write failed mid-apply")
            return await real(conn, *args, **kwargs)
        return write

    async def scenario():
        # A new prefix makes the plan replace both seed vectors, so the failing applies really write first.
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="search_document: ")
        before = await _state(committed_dsn)
        conn = await asyncpg.connect(committed_dsn)
        try:
            for run_id, how, match, kw in (("canary:boom", "raise", "RuntimeError: write failed mid-apply", {}),
                                           ("canary:stall", "stall", "deadline of 1.5 s exceeded", {"deadline_s": 1.5})):
                out, plan_sha = await _plan(committed_dsn, tmp_path, run_id, path, digest)
                monkeypatch.setattr(dl, "_write_candidate", failing_after(2, how))
                started = time.monotonic()
                with pytest.raises(SystemExit, match=f"failed and wrote nothing: {match}"):
                    await _apply(committed_dsn, out, plan_sha, path, digest, **kw)
                monkeypatch.setattr(dl, "_write_candidate", real)
                assert time.monotonic() - started < 6
                assert await _state(committed_dsn) == before                 # no vector, candidate or text
                assert await conn.fetchval("SELECT count(*) FROM discourse_link_run_effects") == 0
                assert await conn.fetchval("SELECT status FROM discourse_link_runs WHERE run_id = $1", run_id) == "failed"
                assert await _alone(conn)                                     # the apply's session is gone
                async with conn.transaction():                               # no row lock is left behind
                    await conn.execute("SET LOCAL lock_timeout = '100ms'")
                    await conn.fetch("SELECT id FROM session_discourse_moves FOR UPDATE NOWAIT")
                with pytest.raises(SystemExit, match="already used"):        # the run id is spent: no retry
                    await _apply(committed_dsn, out, plan_sha, path, digest)
        finally:
            await conn.close()
        out, plan_sha = await _plan(committed_dsn, tmp_path, "canary:after", path, digest)   # control: a new plan
        assert (await _apply(committed_dsn, out, plan_sha, path, digest))["effects"]["embedding_replaced"] == 2
    asyncio.run(scenario())


def test_two_phase_runs_are_idempotent_candidate_only_and_roll_back(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    async def scenario():
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="search_document: ")
        before = await _state(committed_dsn)
        out, plan_sha = await _plan(committed_dsn, tmp_path, "canary:one", path, digest)
        first = await _apply(committed_dsn, out, plan_sha, path, digest)
        after_first = await _state(committed_dsn)
        out, plan_sha = await _plan(committed_dsn, tmp_path, "canary:two", path, digest)
        second = await _apply(committed_dsn, out, plan_sha, path, digest)
        assert first["effects"]["candidate_inserted"] > 0
        assert "candidate_inserted" not in second["effects"] and not any(k.startswith("embedding") for k in second["effects"])
        assert second["effects"]["candidate_seen"] == first["effects"]["candidate_inserted"] + first["effects"].get("candidate_seen", 0)
        conn = await asyncpg.connect(committed_dsn)
        try:
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates") == first["effects"]["candidate_inserted"]
            assert await conn.fetchval("SELECT count(*) FROM discourse_relations") == 0
            assert await conn.fetchval("SELECT count(*) FROM discourse_review_events") == 0
            run = await conn.fetchrow("SELECT approval_scope, summary FROM discourse_link_runs WHERE run_id = 'canary:one'")
            assert json.loads(run["approval_scope"])["plan_sha256"]
        finally:
            await conn.close()
        for r in (first, second):
            assert set(r["table_writes"]) <= {f"public.{t}" for t in canary.CANDIDATE_TABLES}
            assert {"locktype": "advisory", "object": "advisory", "mode": "ExclusiveLock", "n": 1} in r["locks_at_commit"]
            assert r["applied"]["retrieval_rerun"] is False
        await canary.run_rollback(committed_dsn, "canary:two")
        assert await _state(committed_dsn) == after_first
        await canary.run_rollback(committed_dsn, "canary:one")
        assert await _state(committed_dsn) == before
    asyncio.run(scenario())


def test_completion_timestamps_use_clock_time(committed_dsn, tmp_path, monkeypatch):
    """canary:2026-10-19-seed6 recorded finished_at = started_at: now() is the transaction start."""
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    class SlowEmbedder:
        async def embed_batch(self, texts):
            await asyncio.sleep(1.1)
            return await ConceptEmbedder().embed_batch(texts)
    real = dl.apply_plan

    async def slow_apply(*args, **kwargs):
        await asyncio.sleep(1.1)
        return await real(*args, **kwargs)

    async def scenario():
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="search_document: ")
        await _discover_with(committed_dsn, path, digest, embedder=SlowEmbedder(), run_id="canary:slow-discover")
        out, plan_sha = await _plan(committed_dsn, tmp_path, "canary:slow-apply2", path, digest)
        monkeypatch.setattr(dl, "apply_plan", slow_apply)
        await _apply(committed_dsn, out, plan_sha, path, digest)
        conn = await asyncpg.connect(committed_dsn)
        try:
            await conn.execute("INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, status, "
                               "summary, finished_at) VALUES ('canary:legacy', 'canary', 'x', $1::jsonb, 'completed', "
                               "'{}', now())", json.dumps({"seed_move_ids": [], "model": MODEL}))
            for run_id in ("canary:slow-discover", "canary:slow-apply2"):
                row = await conn.fetchrow("SELECT started_at, finished_at, summary FROM discourse_link_runs "
                                          "WHERE run_id = $1", run_id)
                took = (row["finished_at"] - row["started_at"]).total_seconds()
                assert took >= 1.0 and abs(took - json.loads(row["summary"])["timings"]["transaction_s"]) < 0.5
            async with conn.transaction(readonly=True):
                assert (await canary.build_report(conn, "canary:slow-apply2"))["run"]["timestamp_note"] is None
                assert "now()" in (await canary.build_report(conn, "canary:legacy"))["run"]["timestamp_note"]
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_candidates_can_be_inspected_read_only_and_say_what_they_are(committed_dsn, tmp_path, monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    async def scenario():
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="")
        await _discover_with(committed_dsn, path, digest, run_id="canary:look")
        before = await _state(committed_dsn)
        view = await canary._readonly(committed_dsn, lambda c: canary.inspect_candidates(
            c, query="vessel landing", run_id="canary:look"))
        assert view["read_only"] and view["label"] == canary.CANDIDATE_LABEL
        assert [r["move"]["id"] for r in view["results"]] == [mid("b1")]
        cands = view["results"][0]["candidates"]
        assert cands and all(c["status"] == "candidate" and c["label"] == "candidate (unreviewed)" for c in cands)
        assert all(c["other"]["source"]["rid"] != SOURCES["S2"]["rid"] for c in cands)    # both provenances
        assert view["results"][0]["move"]["source"]["rid"] == SOURCES["S2"]["rid"]
        everything = await canary._readonly(committed_dsn, lambda c: canary.inspect_candidates(c, limit=50))
        assert {r["move"]["id"] for r in everything["results"]} >= {mid("b1"), mid("b2")}
        assert await _state(committed_dsn) == before
    asyncio.run(scenario())


def test_plan_and_apply_deadlines_are_bounded_and_forwarded(committed_dsn, tmp_path, monkeypatch):
    for parse, limit in ((canary.plan_deadline_seconds, 1800), (canary.apply_deadline_seconds, 180)):
        assert parse(str(limit)) == limit
        for text in ("0", str(limit + 0.5), "nan", "inf"):
            with pytest.raises(argparse.ArgumentTypeError):
                parse(text)
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")
    with pytest.raises(SystemExit, match="planning deadline of 1801"):
        asyncio.run(canary.run_plan(committed_dsn, "canary:x", embedder=None, manifest_path=None,
                                    manifest_sha256=None, out=tmp_path / "x", deadline_s=1801))
    with pytest.raises(SystemExit, match="apply deadline of 181"):
        asyncio.run(canary.run_apply(committed_dsn, plan_path=None, plan_sha256=None, manifest_path=None,
                                     manifest_sha256=None, deadline_s=181))
    data = canary.manifest_bytes({"approval_scope": {"kind": "canary", "model": MODEL}})
    path = tmp_path / "cli.json"
    path.write_bytes(data)
    m = ["--manifest", str(path), "--manifest-sha256", hashlib.sha256(data).hexdigest(), "--dsn", committed_dsn]
    seen = []

    async def recording(*args, **kw):
        seen.append(kw["deadline_s"])
        return {}
    monkeypatch.setattr(canary, "run_plan", recording)
    monkeypatch.setattr(canary, "run_apply", recording)
    monkeypatch.setattr(demo, "_Ollama", lambda model: object())
    monkeypatch.setattr(canary, "_ollama_digest", lambda model: None)
    plan_argv = ["x", "plan", *m, "--run-id", "canary:cli", "--out", str(tmp_path / "p.json")]
    apply_argv = ["x", "apply", *m, "--plan", "p.json", "--plan-sha256", "0" * 64, "--apply"]
    for argv, value in ((plan_argv, 600), (plan_argv + ["--plan-deadline-s", "900"], 900.0),
                        (apply_argv, 60), (apply_argv + ["--apply-deadline-s", "120"], 120.0)):
        monkeypatch.setattr(sys, "argv", argv)
        canary.main()
        assert seen[-1] == value
    for argv in (plan_argv + ["--plan-deadline-s", "1801"], apply_argv + ["--apply-deadline-s", "181"]):
        monkeypatch.setattr(sys, "argv", argv)
        with pytest.raises(SystemExit) as refused:
            canary.main()
        assert refused.value.code == 2
    assert len(seen) == 4                                                   # refused before any run
    monkeypatch.setattr(sys, "argv", apply_argv[:-1])
    with pytest.raises(SystemExit, match="rerun with --apply"):
        canary.main()


@pytest.mark.parametrize("name", ["personal_koi", "personal_koi_test", "octo_koi", "{prefix}x; DROP"])
def test_demo_refuses_anything_but_a_scratch_database(name):
    with pytest.raises(SystemExit):
        demo.check_scratch_name(name.format(prefix=demo.SCRATCH_PREFIX))


# ---------------------------------------------------------------------------
# Ingestion-time discovery: migration 134, api/discourse_discovery.py, worker
# ---------------------------------------------------------------------------

def _new_source(n=1, **kw):
    """A synthetic source as the extractor would see it: chunks, and the moves the model returned."""
    return {"rid": f"fixture:landing-guide-{n}", "title": f"A guide to landing records ({n})",
            "url": f"https://example.org/landing-{n}", "version": f"guide {n} v1",
            "chunks": ["Every landing record names the vessel and the hour.",
                       "Shared ledgers cut disputed catch reports in the harbour trial."],
            "moves": [{"move_type": "claim", "chunk_range": [0, 0],
                       "title": "Each landing record must name the vessel and the hour it landed."},
                      {"move_type": "evidence", "chunk_range": [1, 1],
                       "title": "Disputed catch reports fell after the harbour ledgers were shared."}], **kw}


async def _ingest(conn, src, run_id="extract-new-1"):
    """extract_deep_document's discourse path with the model's output given: read the
    input (capturing the source version), write the moves, record their bindings,
    mark the extraction complete, schedule discovery. Same functions, same order."""
    rid = src["rid"]
    await conn.execute(
        """INSERT INTO document_ingestion_log (document_rid, title, source_url, content_hash) VALUES ($1, $2, $3, $4)
           ON CONFLICT (document_rid) DO UPDATE SET content_hash = EXCLUDED.content_hash""",
        rid, src["title"], src["url"], sha(src["version"]))
    await conn.execute("INSERT INTO koi_memories (rid, event_type, source_sensor, content) "
                       "VALUES ($1, 'NEW', 'fixture', '{}'::jsonb) ON CONFLICT (rid) DO NOTHING", rid)
    for i, text in enumerate(src["chunks"]):
        await conn.execute(
            """INSERT INTO koi_memory_chunks (chunk_rid, document_rid, chunk_index, total_chunks, content)
               VALUES ($1, $2, $3, $4, $5::jsonb) ON CONFLICT (chunk_rid) DO NOTHING""",
            f"{rid}#{i}", rid, i, len(src["chunks"]), json.dumps({"text": text}))
    _, captured = await ex.read_rag_input(conn, rid)
    written = []
    await ex.write_discourse_moves(conn, document_rid=rid, episode_id=None, moves=src["moves"], written_ids=written)
    await ex.record_discourse_bindings(conn, document_rid=rid, extraction_input=captured, move_ids=written,
                                       run_id=run_id)
    await conn.execute("UPDATE document_ingestion_log SET deep_extracted_at = now() WHERE document_rid = $1", rid)
    return [str(w) for w in written], await ex.schedule_discourse_discovery(conn, document_rid=rid,
                                                                           move_ids=written, run_id=run_id)


def _work(dsn, embedder=None, **kw):
    return worker.run_once(dsn, embedder_for=lambda profile: embedder or ConceptEmbedder(), worker_id="w1", **kw)


@pytest.fixture
def ingestion(monkeypatch):
    """Flags on for this process only; the job profile uses the fixture vectors' model so
    stored target vectors take part, as they would with nomic in the corpus."""
    for flag in ("KOI_DISCOURSE_BINDINGS", "KOI_DISCOURSE_INGEST_DISCOVERY", "KOI_DISCOURSE_DISCOVERY"):
        monkeypatch.setenv(flag, "1")
    monkeypatch.setattr(dd, "PROFILE", dict(dd.PROFILE, model=MODEL, text_prefix=""))


async def _job(conn, rid):
    return await conn.fetchrow("SELECT * FROM discourse_discovery_jobs WHERE source_rid = $1 ORDER BY id DESC LIMIT 1",
                               rid)


def test_ingestion_discovery_runs_end_to_end_and_stays_candidate_only(committed_dsn, ingestion):
    src = _new_source()

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await _load(conn, ["S7"])                            # an existing claim with no recorded binding
            old_bindings = await conn.fetchval("SELECT count(*) FROM discourse_move_extraction_bindings")
            written, scheduled = await _ingest(conn, src)
            assert scheduled["status"] == "enqueued" and scheduled["moves"] == 2
            new = await dl.fetch_moves(conn, written)
            assert all(m["extraction"]["binding_kind"] == "recorded_extraction" and m["admissible"]
                       for m in new.values())                    # the new claims carry their binding
            assert (await _job(conn, src["rid"]))["status"] == "pending"
            out = await _work(committed_dsn)
            assert [j["status"] for j in out["jobs"]] == ["completed"]
            job = await _job(conn, src["rid"])
            run = await conn.fetchrow("SELECT * FROM discourse_link_runs WHERE run_id = $1", job["run_id"])
            assert (job["status"], run["kind"], run["status"]) == ("completed", "ingestion_discovery", "completed")
            assert run["finished_at"] > run["started_at"]
            cands = await conn.fetch("SELECT * FROM discourse_link_candidates WHERE run_id = $1", job["run_id"])
            assert cands and {c["query_source_rid"] for c in cands} == {src["rid"]}
            assert {c["target_source_rid"] for c in cands} >= {SOURCES["S1"]["rid"], SOURCES["S7"]["rid"]}
            assert all(c["status"] == "candidate" for c in cands)
            assert await conn.fetchval("SELECT count(*) FROM discourse_relations") == 0
            assert await conn.fetchval("SELECT count(*) FROM discourse_review_events") == 0
            assert await conn.fetchval("SELECT count(*) FROM discourse_move_extraction_bindings") == old_bindings + 2
            summary = json.loads(run["summary"])
            assert set(summary["table_writes"]) <= {f"public.{t}" for t in worker.ALLOWED_TABLES}
        finally:
            await conn.close()
        view = await canary._readonly(committed_dsn, lambda c: canary.inspect_candidates(c, move_id=written[0]))
        assert view["read_only"] and view["label"] == canary.CANDIDATE_LABEL
        assert view["results"][0]["move"]["source"] == {"rid": src["rid"], "title": src["title"],
                                                         "source_url": src["url"]}
        rows = view["results"][0]["candidates"]
        assert rows and all(r["label"] == "candidate (unreviewed)" for r in rows)
        to_s7 = [r for r in rows if r["other"]["source"]["rid"] == SOURCES["S7"]["rid"]]
        assert to_s7 and not to_s7[0]["admissible_now"]          # an old unbound claim gains nothing
        assert to_s7[0]["admission_blockers"] == [
            "content-addressed source attribution, not recorded extraction provenance"]
        status = await canary._readonly(committed_dsn, lambda c: dd.job_status(c, source_rid=src["rid"]))
        assert status["jobs"][0]["status"] == "completed" and status["jobs"][0]["candidates_inserted"] == len(cands)
        assert status["unscheduled"] == []
    asyncio.run(scenario())


def test_ingestion_discovery_recovers_and_retries_without_duplicates(committed_dsn, ingestion, monkeypatch):
    src = _new_source()

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            written, first = await _ingest(conn, src)
            again = await ex.schedule_discourse_discovery(conn, document_rid=src["rid"], move_ids=written,
                                                          run_id="extract-new-1")
            assert again == {"status": "exists", "job_id": first["job_id"], "job_key": first["job_key"], "per_move_cap": 5,
                             "note": "this extraction already has this job (same profile). Nothing was changed."}
            # A worker that claimed the job and died: its lease runs out and the job is reclaimed.
            await worker.claim_next(conn, "dead-worker")
            await conn.execute("UPDATE discourse_discovery_jobs SET claimed_at = now() - interval '1 hour'")
            out = await _work(committed_dsn)
            assert out["reclaimed"] == [{"id": first["job_id"], "status": "pending"}]
            assert [(j["status"], j["attempt"]) for j in out["jobs"]] == [("completed", 2)]
            n = await conn.fetchval("SELECT count(*) FROM discourse_link_candidates")
            assert n > 0 and (await _work(committed_dsn))["jobs"] == []   # nothing left to do
            # Re-extraction, same text: a new job that only meets the same pairs again.
            await _ingest(conn, src, run_id="extract-new-2")
            out = await _work(committed_dsn)
            effects = out["jobs"][0]["effects"]
            assert out["jobs"][0]["status"] == "completed" and "candidate_inserted" not in effects
            assert effects["candidate_seen"] > 0 and not any(k.startswith("embedding") for k in effects)
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates") == n
            # An apply interrupted past its deadline writes nothing; the next pass completes it.
            monkeypatch.setattr(dd, "LIMITS", dict(dd.LIMITS, apply_deadline_s=1.5))
            await _ingest(conn, src, run_id="extract-new-3")
            real = dl._write_candidate

            async def stall(*args, **kwargs):
                await asyncio.sleep(3600)
            monkeypatch.setattr(dl, "_write_candidate", stall)
            effects_before = await conn.fetchval("SELECT count(*) FROM discourse_link_run_effects")
            out = await _work(committed_dsn)
            job = await _job(conn, src["rid"])
            assert (out["jobs"][0]["status"], job["status"], job["attempts"]) == ("pending", "pending", 1)
            assert "deadline of 1.5 s exceeded" in job["last_error"]
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_run_effects") == effects_before
            assert await conn.fetchval("SELECT status FROM discourse_link_runs WHERE run_id = $1",
                                       f"ingest:{job['id']}:1") == "failed"
            monkeypatch.setattr(dl, "_write_candidate", real)
            await conn.execute("UPDATE discourse_discovery_jobs SET next_attempt_at = now() WHERE id = $1", job["id"])
            out = await _work(committed_dsn)
            assert [(j["status"], j["attempt"]) for j in out["jobs"]] == [("completed", 2)]
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates") == n
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_ingestion_discovery_refuses_stale_work_and_replans_at_most_once(committed_dsn, ingestion, monkeypatch):
    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            async def stale(n, change, reason):
                src = _new_source(n)
                written, scheduled = await _ingest(conn, src)
                await change(src, written)
                out = await _work(committed_dsn)
                job = await _job(conn, src["rid"])
                assert (out["jobs"][0]["status"], job["status"]) == ("stale", "stale") and reason in job["last_error"]
                assert job["run_id"] is None and job["finished_at"] is not None
                assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates WHERE query_source_rid = $1",
                                           src["rid"]) == 0

            async def new_revision(src, written):
                await conn.execute("UPDATE document_ingestion_log SET content_hash = $2 WHERE document_rid = $1",
                                   src["rid"], sha("a later revision"))

            async def edited(src, written):
                await conn.execute("UPDATE session_discourse_moves SET detail = 'edited' WHERE id = $1", written[0])

            async def wider_exclusion(src, written):
                monkeypatch.setattr(dl, "EXCLUDED_SOURCE_PATTERNS", dl.EXCLUDED_SOURCE_PATTERNS + ("fixture:held:%",))

            await stale(1, new_revision, "source revision changed since extraction")
            await stale(2, edited, "claim text changed since extraction")
            await stale(3, wider_exclusion, "caller context changed since the job was bound")
            monkeypatch.undo()
            for flag in ("KOI_DISCOURSE_BINDINGS", "KOI_DISCOURSE_INGEST_DISCOVERY", "KOI_DISCOURSE_DISCOVERY"):
                monkeypatch.setenv(flag, "1")
            monkeypatch.setattr(dd, "PROFILE", dict(dd.PROFILE, model=MODEL, text_prefix=""))
            # A target that changes between plan and apply: refused at apply, replanned once.
            real, edits = worker._plan_job, []

            def plan_then_edit(times):
                async def wrapped(dsn, job, context, embedder, run_id):
                    plan = await real(dsn, job, context, embedder, run_id)
                    if len(edits) < times:
                        edits.append(run_id)
                        await conn.execute("UPDATE session_discourse_moves SET detail = $2 WHERE id = $1",
                                           plan["sightings"][0]["target_move_id"], f"edited before apply {len(edits)}")
                    return plan
                return wrapped
            monkeypatch.setattr(worker, "_plan_job", plan_then_edit(1))
            await _ingest(conn, _new_source(4))
            out = (await _work(committed_dsn))["jobs"][0]
            assert out["status"] == "completed" and out["run_id"].endswith(":1.1") and len(out["replanned_after"]) == 1
            assert await conn.fetchval("SELECT status FROM discourse_link_runs WHERE run_id = $1",
                                       out["run_id"][:-2]) == "failed"
            edits.clear()
            monkeypatch.setattr(worker, "_plan_job", plan_then_edit(10))
            await _ingest(conn, _new_source(5))
            out = (await _work(committed_dsn))["jobs"][0]
            job = await _job(conn, _new_source(5)["rid"])
            assert out["status"] == "pending" and len(edits) == 2               # one plan and one replan, no more
            assert "PlanRefused" in job["last_error"] and job["run_id"] is None
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_a_discovery_failure_keeps_the_ingested_document(committed_dsn, ingestion, monkeypatch):
    class BrokenEmbedder:
        async def embed_batch(self, texts):
            raise RuntimeError("embedding provider down")
    monkeypatch.setattr(dd, "LIMITS", dict(dd.LIMITS, max_attempts=2))
    src = _new_source()

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            written, _ = await _ingest(conn, src)
            out = await _work(committed_dsn, embedder=BrokenEmbedder())
            job = await _job(conn, src["rid"])
            assert (out["jobs"][0]["status"], job["attempts"]) == ("pending", 1)
            assert job["last_error"] == "planning: RuntimeError: embedding provider down"
            assert job["next_attempt_at"] > await conn.fetchval("SELECT now()")      # backoff
            assert (await _work(committed_dsn, embedder=BrokenEmbedder()))["jobs"] == []   # not due yet
            await conn.execute("UPDATE discourse_discovery_jobs SET next_attempt_at = now()")
            out = await _work(committed_dsn, embedder=BrokenEmbedder())
            job = await _job(conn, src["rid"])
            assert (out["jobs"][0]["status"], job["status"], job["attempts"]) == ("failed", "failed", 2)
            # The document, its claims and their bindings are as ingestion left them.
            assert await conn.fetchval("SELECT deep_extracted_at IS NOT NULL FROM document_ingestion_log "
                                       "WHERE document_rid = $1", src["rid"])
            moves = await dl.fetch_moves(conn, written)
            assert len(moves) == 2 and all(m["admissible"] for m in moves.values())
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates") == 0
            status = await dd.job_status(conn, source_rid=src["rid"])
            assert status["jobs"][0]["status"] == "failed" and "provider down" in status["jobs"][0]["last_error"]
            # A source over the move limit is refused once, not retried and not truncated.
            monkeypatch.setattr(dd, "LIMITS", dict(dd.LIMITS, max_moves=1))
            big = _new_source(2)
            await _ingest(conn, big)
            out = await _work(committed_dsn)
            job = await _job(conn, big["rid"])
            assert (job["status"], job["attempts"], job["last_error"]) == ("failed", 1, "2 moves > max_moves 1")
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_ingestion_discovery_is_off_unless_its_flags_are_set(committed_dsn, monkeypatch):
    class NoDatabase:
        def __getattr__(self, name):
            raise AssertionError(f"the flag-off path touched the database ({name})")

    for flag in ("KOI_DISCOURSE_BINDINGS", "KOI_DISCOURSE_INGEST_DISCOVERY", "KOI_DISCOURSE_DISCOVERY"):
        monkeypatch.delenv(flag, raising=False)
    assert asyncio.run(ex.schedule_discourse_discovery(NoDatabase(), document_rid="fixture:x", move_ids=["m"],
                                                       run_id="r")) is None

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            written, scheduled = await _ingest(conn, _new_source(1))           # both flags off: the existing path
            assert scheduled is None and len(written) == 2
            assert await conn.fetchval("SELECT count(*) FROM discourse_move_extraction_bindings "
                                       "WHERE move_id = ANY($1::uuid[])", written) == 0
            monkeypatch.setenv("KOI_DISCOURSE_INGEST_DISCOVERY", "1")          # no bindings: nothing to bind a job to
            _, scheduled = await _ingest(conn, _new_source(2))
            assert scheduled["status"] == "skipped" and "captured with its input" in scheduled["reason"]
            assert await conn.fetchval("SELECT count(*) FROM discourse_discovery_jobs") == 0
        finally:
            await conn.close()
    asyncio.run(scenario())
    with pytest.raises(SystemExit, match="discovery is off"):
        asyncio.run(_work(committed_dsn))
    src = inspect.getsource(ex.extract_deep_document)             # scheduled only after a complete extraction
    completion = "SET deep_extracted_at = CASE WHEN $2 THEN NULL ELSE NOW() END"   # the completion update
    assert src.count(completion) == 1
    assert src.index("record_discourse_bindings(") < src.index(completion) < src.index("schedule_discourse_discovery(")
    assert "if want_discourse and not incomplete:" in src
    monkeypatch.setattr(sys, "argv", ["x", "run", "--dsn", committed_dsn])
    with pytest.raises(SystemExit, match="needs --once"):
        worker.main()


def test_migration_134_widens_the_run_kind_and_rolls_back(committed_dsn):
    up = (demo.REPO_ROOT / "migrations" / "134_discourse_discovery_jobs.sql").read_text()
    down = (demo.REPO_ROOT / "migrations" / "134_discourse_discovery_jobs_down.sql").read_text()

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await conn.execute(down)                                      # the state personal_koi is in: 133 only
            await conn.execute("INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, status) "
                               "VALUES ('canary:kept', 'canary', 'x', '{}', 'completed')")
            canary_row = dict(await conn.fetchrow("SELECT * FROM discourse_link_runs"))
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute("INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, "
                                   "status) VALUES ('ingest:x', 'ingestion_discovery', 'x', '{}', 'completed')")
            await conn.execute(up)
            assert dict(await conn.fetchrow("SELECT * FROM discourse_link_runs")) == canary_row
            await conn.execute("INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, status) "
                               "VALUES ('ingest:x', 'ingestion_discovery', 'x', '{}', 'completed')")
            with pytest.raises(asyncpg.CheckViolationError):
                await conn.execute("INSERT INTO discourse_link_runs (run_id, kind, manifest_sha256, approval_scope, "
                                   "status) VALUES ('other:x', 'publication', 'x', '{}', 'completed')")
            assert await conn.fetchval("SELECT checksum FROM koi_migrations "
                                       "WHERE migration_id = '134_discourse_discovery_jobs'") == "v1_ingestion_discovery"
            with pytest.raises(asyncpg.RaiseError, match="1 non-canary run row"):
                await conn.execute(down)
            await conn.execute("ROLLBACK")                                # psql -v ON_ERROR_STOP=1 would exit here
            assert await dl._has_table(conn, "discourse_discovery_jobs")  # the refused down changed nothing
            await conn.execute("DELETE FROM discourse_link_runs WHERE run_id = 'ingest:x'")
            await conn.execute(down)
            assert not await dl._has_table(conn, "discourse_discovery_jobs")
            assert dict(await conn.fetchrow("SELECT * FROM discourse_link_runs")) == canary_row
            assert await conn.fetchval("SELECT count(*) FROM koi_migrations "
                                       "WHERE migration_id = '134_discourse_discovery_jobs'") == 0
        finally:
            await conn.close()
    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Review repairs, 2026-10-20: extraction input, rollback ownership, backout
# ---------------------------------------------------------------------------

def test_a_changed_extraction_input_makes_the_job_stale(committed_dsn, ingestion, monkeypatch):
    """Review repro: only a binding's extraction_input_sha256 changed after enqueue;
    job_drift was empty and the worker completed."""
    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            async def rewrite_input(move_id):
                await conn.execute("UPDATE discourse_move_extraction_bindings SET extraction_input_sha256 = $2 "
                                   "WHERE move_id = $1", move_id, sha("different input, same text and version"))

            # Before planning.
            src = _new_source(1)
            written, _ = await _ingest(conn, src)
            await rewrite_input(written[0])
            out = (await _work(committed_dsn))["jobs"][0]
            job = await _job(conn, src["rid"])
            assert (out["status"], job["status"]) == ("stale", "stale")
            assert "binding changed since the job was bound (extraction_input_sha256)" in job["last_error"]
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates") == 0
            # Between planning and apply.
            src = _new_source(2)
            written, _ = await _ingest(conn, src)
            real = worker._plan_job

            async def plan_then_rewrite(*args, **kwargs):
                plan = await real(*args, **kwargs)
                await rewrite_input(written[1])
                return plan
            monkeypatch.setattr(worker, "_plan_job", plan_then_rewrite)
            out = (await _work(committed_dsn))["jobs"][0]
            job = await _job(conn, src["rid"])
            assert (out["status"], job["status"], job["run_id"]) == ("stale", "stale", None)
            assert "extraction_input_sha256" in job["last_error"]
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates") == 0
            assert await conn.fetchval("SELECT status FROM discourse_link_runs WHERE run_id = $1",
                                       f"ingest:{job['id']}:1") == "failed"
            # Control: unchanged input completes.
            monkeypatch.setattr(worker, "_plan_job", real)
            src = _new_source(3)
            await _ingest(conn, src)
            assert (await _work(committed_dsn))["jobs"][0]["status"] == "completed"
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates") > 0
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_rollback_proves_each_vector_is_still_the_runs(committed_dsn, tmp_path, ingestion, monkeypatch):
    """Review repro: a vector changed after the run, hashes kept, and rollback deleted
    it. Ownership now includes the exact value and dimension written, for inserted and
    replaced vectors alike, and any doubt refuses the whole rollback."""
    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            async def value(move_id, model, new=None):
                if new is None:
                    return await conn.fetchval("SELECT embedding::text FROM discourse_move_embeddings "
                                               "WHERE move_id = $1 AND model = $2", move_id, model)
                await conn.execute("UPDATE discourse_move_embeddings SET embedding = $3::vector "
                                   "WHERE move_id = $1 AND model = $2", move_id, model, new)

            def nudged(text):
                xs = [float(x) for x in text.strip("[]").split(",")]
                xs[0] += 0.001
                return dl._vector_text(xs)

            async def refused(run_id, change, undo, match):
                await change()
                before = await _state(committed_dsn)
                with pytest.raises(SystemExit, match=match):
                    await canary.run_rollback(committed_dsn, run_id)
                assert await _state(committed_dsn) == before                  # nothing undone at all
                assert await conn.fetchval("SELECT status FROM discourse_link_runs WHERE run_id = $1",
                                           run_id) == "completed"
                await undo()

            # Inserted vectors: an ingestion job for a new source.
            src = _new_source(1)
            written, _ = await _ingest(conn, src)
            await _work(committed_dsn)
            ingest_run = (await _job(conn, src["rid"]))["run_id"]
            mid0 = written[0]
            original = await value(mid0, MODEL)
            await refused(ingest_run, lambda: value(mid0, MODEL, nudged(original)), lambda: value(mid0, MODEL, original),
                          r"changed after the run \(vector_sha256\)")
            await refused(ingest_run,
                          lambda: conn.execute("UPDATE discourse_move_embeddings SET dimension = 99 WHERE move_id = $1",
                                               mid0),
                          lambda: conn.execute("UPDATE discourse_move_embeddings SET dimension = 5 WHERE move_id = $1",
                                               mid0),
                          r"changed after the run \(dimension\)")
            # Replaced vectors: a canary run that re-embeds the seed with a new prefix.
            path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="search_document: ")
            canary_state = await _state(committed_dsn)
            await _discover_with(committed_dsn, path, digest, run_id="canary:replace")
            b1 = mid("b1")
            replaced = await value(b1, MODEL)
            await refused("canary:replace", lambda: value(b1, MODEL, nudged(replaced)), lambda: value(b1, MODEL, replaced),
                          r"changed after the run \(vector_sha256\)")
            # Controls: unchanged vectors roll back, the replaced ones to their prior value.
            result = await canary.run_rollback(committed_dsn, "canary:replace")
            assert result["embeddings_restored"] == 2 and result["legacy_embedding_effects_accepted"] == 0
            assert await _state(committed_dsn) == canary_state
            result = await canary.run_rollback(committed_dsn, ingest_run)
            assert result["embeddings_deleted"] == 2
            assert await value(mid0, MODEL) is None
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_rollback_does_not_assume_ownership_of_legacy_vector_effects(committed_dsn, tmp_path, monkeypatch):
    """Effect rows written before vector values were recorded (the live canary's) are
    refused by default; the explicit acceptance uses input hashes plus created_at and is
    reported."""
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")

    async def scenario():
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="search_document: ", model="test:legacy")
        before = await _state(committed_dsn)
        await _discover_with(committed_dsn, path, digest, run_id="canary:legacy")
        conn = await asyncpg.connect(committed_dsn)
        try:
            await conn.execute("""UPDATE discourse_link_run_effects SET after_state = after_state - 'dimension'
                                  - 'vector_sha256' WHERE run_id = 'canary:legacy' AND effect LIKE 'embedding_%'""")
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_run_effects WHERE run_id = 'canary:legacy' "
                                       "AND effect = 'embedding_inserted' AND NOT after_state ? 'vector_sha256'") == 2
            legacy = await _state(committed_dsn)
            with pytest.raises(SystemExit, match="legacy effect row records no vector value"):
                await canary.run_rollback(committed_dsn, "canary:legacy")
            assert await _state(committed_dsn) == legacy
            # Accepted explicitly, but not if the row was rewritten since (created_at moved).
            await conn.execute("UPDATE discourse_move_embeddings SET created_at = created_at + interval '1 second' "
                               "WHERE move_id = $1 AND model = 'test:legacy'", mid("b1"))
            with pytest.raises(SystemExit, match=r"changed after the run \(legacy check\)"):
                await canary.run_rollback(committed_dsn, "canary:legacy", accept_legacy_embedding_ownership=True)
            await conn.execute("UPDATE discourse_move_embeddings SET created_at = created_at - interval '1 second' "
                               "WHERE move_id = $1 AND model = 'test:legacy'", mid("b1"))
            result = await canary.run_rollback(committed_dsn, "canary:legacy", accept_legacy_embedding_ownership=True)
            assert result["legacy_embedding_effects_accepted"] == 2 and result["embeddings_deleted"] == 2
            assert await _state(committed_dsn) == before
        finally:
            await conn.close()
    asyncio.run(scenario())
    seen = {}

    async def recording(dsn, run_id, **kw):
        seen.update(kw)
        return {}
    monkeypatch.setattr(canary, "run_rollback", recording)
    for extra, value in (([], False), (["--accept-legacy-embedding-ownership"], True)):
        monkeypatch.setattr(sys, "argv", ["x", "rollback", "--dsn", committed_dsn, "--run-id", "canary:x", "--apply", *extra])
        canary.main()
        assert seen["accept_legacy_embedding_ownership"] is value


def test_operational_rollback_is_the_supported_backout_for_an_ingestion_job(committed_dsn, ingestion):
    """Undo the run's owned outputs, keep the job, run and effect history, keep the
    document and its bindings. Schema removal is a separate, refused-by-default act."""
    down = (demo.REPO_ROOT / "migrations" / "134_discourse_discovery_jobs_down.sql").read_text()
    src = _new_source()

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            written, _ = await _ingest(conn, src)
            assert (await _work(committed_dsn))["jobs"][0]["status"] == "completed"
            job = await _job(conn, src["rid"])
            run_id = job["run_id"]
            effects = await conn.fetchval("SELECT count(*) FROM discourse_link_run_effects WHERE run_id = $1", run_id)
            holder = await asyncpg.connect(committed_dsn)                 # rollback is a writer like any other
            try:
                await holder.execute("SELECT pg_advisory_lock(hashtext($1))", canary.WRITER_LOCK)
                with pytest.raises(SystemExit, match="writer lock"):
                    await canary.run_rollback(committed_dsn, run_id)
            finally:
                await holder.close()
            bindings = [dict(r) for r in await conn.fetch(
                "SELECT * FROM discourse_move_extraction_bindings WHERE move_id = ANY($1::uuid[]) ORDER BY move_id",
                written)]
            assert effects > 0 and len(bindings) == 2
            result = await canary.run_rollback(committed_dsn, run_id)
            assert result["candidates_deleted"] > 0 and result["embeddings_deleted"] == 2 and result["history_kept"]
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates WHERE run_id = $1", run_id) == 0
            assert await conn.fetchval("SELECT count(*) FROM discourse_move_embeddings "
                                       "WHERE move_id = ANY($1::uuid[])", written) == 0
            # History kept.
            assert await conn.fetchval("SELECT status FROM discourse_link_runs WHERE run_id = $1", run_id) == "rolled_back"
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_run_effects WHERE run_id = $1", run_id) == effects
            kept = await _job(conn, src["rid"])
            assert (kept["status"], kept["run_id"]) == ("completed", run_id)
            status = (await dd.job_status(conn, source_rid=src["rid"]))["jobs"][0]
            assert (status["run_status"], status["outputs"], status["candidates_inserted"]) == ("rolled_back",
                                                                                               "rolled_back", 0)
            # The ingested document, its claims and their bindings are untouched.
            assert await conn.fetchval("SELECT deep_extracted_at IS NOT NULL FROM document_ingestion_log "
                                       "WHERE document_rid = $1", src["rid"])
            assert [dict(r) for r in await conn.fetch(
                "SELECT * FROM discourse_move_extraction_bindings WHERE move_id = ANY($1::uuid[]) ORDER BY move_id",
                written)] == bindings
            assert all(m["admissible"] for m in (await dl.fetch_moves(conn, written)).values())
            with pytest.raises(SystemExit, match="is rolled_back"):
                await canary.run_rollback(committed_dsn, run_id)
            # Destructive schema removal is separate and refuses while this history exists.
            with pytest.raises(asyncpg.RaiseError, match="would discard their audit history"):
                await conn.execute(down)
            await conn.execute("ROLLBACK")
            assert (await _job(conn, src["rid"]))["run_id"] == run_id
        finally:
            await conn.close()
    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Routine use: the ingest option, the scheduled pass, the query command
# ---------------------------------------------------------------------------

import subprocess  # noqa: E402
import urllib.request  # noqa: E402

from scripts import ingest_document as ingest_cli  # noqa: E402

REPO = demo.REPO_ROOT
DISCOURSE_FLAGS = ("KOI_DISCOURSE_BINDINGS", "KOI_DISCOURSE_INGEST_DISCOVERY")


def test_the_ingest_option_sets_both_flags_for_its_own_process_only(monkeypatch, tmp_path, capsys):
    seen = []

    async def fake_ingest_path(**kw):
        seen.append({**{f: os.environ.get(f) for f in DISCOURSE_FLAGS}, "tier": kw["tier"]})
        return {"document_rid": "document:x", "tier": kw["tier"], "rag": {},
                "extract": {"discourse_discovery": {"status": "enqueued", "job_id": 7}}}
    monkeypatch.setattr(ingest_cli, "ingest_path", fake_ingest_path)
    for flag in DISCOURSE_FLAGS:                       # recorded, so teardown restores the absence
        monkeypatch.setenv(flag, "0")
        monkeypatch.delenv(flag)
    src = tmp_path / "s.md"
    src.write_text("text")
    base = ["ingest_document.py", "--source-path", str(src)]
    monkeypatch.setattr(sys, "argv", base + ["--tier", "thorough"])
    ingest_cli.main()
    assert seen[-1] == {"KOI_DISCOURSE_BINDINGS": None, "KOI_DISCOURSE_INGEST_DISCOVERY": None, "tier": "thorough"}
    monkeypatch.setattr(sys, "argv", base + ["--tier", "thorough", "--discourse-discovery"])
    ingest_cli.main()
    assert seen[-1] == {"KOI_DISCOURSE_BINDINGS": "1", "KOI_DISCOURSE_INGEST_DISCOVERY": "1", "tier": "thorough"}
    assert "discovery job:  {'status': 'enqueued', 'job_id': 7}" in capsys.readouterr().out
    for extra in (["--tier", "standard", "--discourse-discovery"],
                  ["--tier", "thorough", "--discourse-discovery", "--dry-run"]):
        monkeypatch.setattr(sys, "argv", base + extra)
        with pytest.raises(SystemExit) as refused:
            ingest_cli.main()
        assert refused.value.code == 2
    assert len(seen) == 2                               # refused before anything ran


# The discovery worker embeds through OllamaEmbeddingProvider's fixed address. Reaching a
# developer's Ollama is a choice, so the test needs KOI_TEST_OLLAMA=1 before it even probes.
OLLAMA_OPT_IN = "KOI_TEST_OLLAMA"
OLLAMA_URL = "http://localhost:11434"


def _ollama_up() -> bool:
    try:
        urllib.request.urlopen(f"{OLLAMA_URL}/api/tags", timeout=2).read()
        return True
    except Exception:  # noqa: BLE001
        return False


def test_the_scheduled_pass_runs_one_job_and_reports_the_queue(committed_dsn, tmp_path, monkeypatch):
    """scripts/run_discourse_discovery.sh, as launchd runs it, against a throwaway database,
    with the default profile (local Ollama nomic-embed-text)."""
    if os.environ.get(OLLAMA_OPT_IN) != "1":
        pytest.skip(f"{OLLAMA_OPT_IN} not set: this test embeds through the Ollama at {OLLAMA_URL}; "
                    f"set {OLLAMA_OPT_IN}=1 to allow it")
    if not _ollama_up():
        pytest.skip(f"{OLLAMA_OPT_IN}=1 but no Ollama answers at {OLLAMA_URL}")
    for flag in DISCOURSE_FLAGS:                       # what --discourse-discovery sets in an ingestion
        monkeypatch.setenv(flag, "1")
    monkeypatch.delenv("KOI_DISCOURSE_DISCOVERY", raising=False)   # the wrapper sets it for itself
    state = tmp_path / "state.json"
    env = dict(os.environ, DISCOURSE_DISCOVERY_DSN=committed_dsn, DISCOURSE_DISCOVERY_STATE=str(state),
               DISCOURSE_DISCOVERY_PYTHON=sys.executable)

    def scheduled_pass():
        done = subprocess.run([str(REPO / "scripts/run_discourse_discovery.sh")], env=env, capture_output=True,
                              text=True, timeout=600)
        assert done.returncode == 0, done.stderr[-2000:]
        return json.loads(state.read_text())

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            ok, stale, big = _new_source(1), _new_source(2), _new_source(3)
            await _ingest(conn, ok)
            s = scheduled_pass()
            assert [j["status"] for j in s["pass"]["jobs"]] == ["completed"]
            assert s["queue"] == {"completed": 1} and s["needs_attention"] == []
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates") > 0
            await _ingest(conn, stale)
            await conn.execute("UPDATE document_ingestion_log SET content_hash = $2 WHERE document_rid = $1",
                               stale["rid"], sha("a later revision"))
            s = scheduled_pass()
            assert [j["status"] for j in s["pass"]["jobs"]] == ["stale"]
            monkeypatch.setattr(dd, "LIMITS", dict(dd.LIMITS, max_moves=1))
            await _ingest(conn, big)
            s = scheduled_pass()
            assert s["queue"] == {"completed": 1, "stale": 1, "failed": 1}
            attention = {j["source_rid"]: (j["status"], j["last_error"]) for j in s["needs_attention"]}
            assert attention[stale["rid"]][0] == "stale" and "source revision changed" in attention[stale["rid"]][1]
            assert attention[big["rid"]] == ("failed", "2 moves > max_moves 1")
            assert scheduled_pass()["pass"]["jobs"] == []          # nothing due: a pass that does nothing
            assert await conn.fetchval("SELECT count(*) FROM discourse_relations") == 0
        finally:
            await conn.close()
    asyncio.run(scenario())
    direct = subprocess.run([sys.executable, "-B", str(REPO / "scripts/discourse_discovery_worker.py"), "run",
                             "--dsn", committed_dsn, "--once"], env={k: v for k, v in env.items()
                                                                    if k != "KOI_DISCOURSE_DISCOVERY"},
                            capture_output=True, text=True, timeout=60)
    assert direct.returncode != 0 and "discovery is off" in direct.stderr   # the flag lives in the wrapper only


def test_the_query_command_is_read_only_and_keeps_candidates_apart_from_judgments(committed_dsn, tmp_path,
                                                                                    monkeypatch):
    monkeypatch.setenv("KOI_DISCOURSE_DISCOVERY", "1")
    env = dict(os.environ, KOI_DISCOURSE_DSN=committed_dsn, KOI_DISCOURSE_PYTHON=sys.executable)

    def koi_discourse(*args):
        return subprocess.run([str(REPO / "scripts/koi-discourse"), *args], env=env, capture_output=True,
                              text=True, timeout=60)

    async def scenario():
        path, digest, _ = await _manifest(committed_dsn, tmp_path, text_prefix="")
        await _discover_with(committed_dsn, path, digest, run_id="canary:look")
        conn = await asyncpg.connect(committed_dsn)
        try:                                            # one admitted judgment, authored in this scratch database
            await dl.record_authored_relation(conn, mid("b1"), mid("a1"), relation="related_to",
                                              semantics="topical/v0", authority=OPERATOR, rationale="test")
        finally:
            await conn.close()
        before = await _state(committed_dsn)
        out = koi_discourse("candidates", "--move-id", mid("b1"))
        assert out.returncode == 0, out.stderr
        view = json.loads(out.stdout)
        assert view["read_only"] and view["label"] == canary.CANDIDATE_LABEL
        r = view["results"][0]
        assert r["move"]["source"]["rid"] == SOURCES["S2"]["rid"]
        assert [(a["label"], a["relation"], a["other"]["id"]) for a in r["admitted_relations"]] == [
            ("admitted (reviewed judgment)", "related_to", mid("a1"))]
        assert r["candidates"] and all(c["label"] == "candidate (unreviewed)" and c["status"] == "candidate"
                                       and c["other"]["source"]["rid"] != SOURCES["S2"]["rid"] for c in r["candidates"])
        jobs = koi_discourse("jobs")
        assert jobs.returncode == 0 and json.loads(jobs.stdout)["jobs_table"] is True
        refused = koi_discourse("rollback", "--run-id", "canary:look")
        assert refused.returncode == 2 and "unknown command" in refused.stderr
        assert "READ ONLY" in koi_discourse("--help").stdout
        assert await _state(committed_dsn) == before                 # nothing written
    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Per-claim limit per job (I2): 5 unless an ingestion chooses 10
# ---------------------------------------------------------------------------

from collections import Counter  # noqa: E402

from api import discourse_review as dr  # noqa: E402

# Fictional targets, two claims per source, so one claim's search can fill ten picks at two per source.
WIDE = tuple((f"fixture:wide-{i}", f"Harbour {i} records (fictional)",
              (f"Landing records in harbour {i} name the vessel and the hour.",
               f"Harbour {i} ledgers record the vessel, the hour and the catch.")) for i in range(1, 6))
QUERY_CLAIM = _new_source()["moves"][0]["title"]


async def _add_sources(conn, sources=WIDE):
    """Target sources as the corpus holds them: a log row, document claims and their vectors. No bindings."""
    ids = []
    for rid, title, claims in sources:
        await conn.execute("INSERT INTO document_ingestion_log (document_rid, title, content_hash) VALUES ($1, $2, $3)",
                           rid, title, sha(rid))
        for n, text in enumerate(claims):
            ids.append(mid(f"{rid}#{n}"))
            await conn.execute("INSERT INTO session_discourse_moves (id, source_type, source_rid, move_type, title) "
                               "VALUES ($1, 'document', $2, 'claim', $3)", ids[-1], rid, text)
    await dl.embed_moves(conn, ConceptEmbedder(), ids, model=MODEL)


def _spec_key(job, profile):
    """The job_key enqueue_for_extraction derives, from a stored job and a profile."""
    return dd.job_key({"source_rid": job["source_rid"], "source_version": job["source_version"],
                       "extraction_run_id": job["extraction_run_id"],
                       "extraction_input_sha256": job["extraction_input_sha256"],
                       "moves": json.loads(job["moves"]), "context": json.loads(job["context"]),
                       "profile": profile, "limits": dd.LIMITS})


def _picks(plan):
    out = {}
    for s in plan["sightings"]:
        out.setdefault(s["query_move_id"], []).append((s["pick"], s["target_move_id"]))
    return out


async def _rows(dsn, run_ids=None, seen_in_runs=None):
    return {r["candidate_id"]: r for r in await canary._readonly(
        dsn, lambda c: dr.candidate_rows(c, run_ids=run_ids, seen_in_runs=seen_in_runs))}


def test_move_cap_default_is_unchanged_and_a_chosen_ten_is_bound_into_job_plan_and_run(committed_dsn, ingestion,
                                                                                       monkeypatch, tmp_path):
    monkeypatch.delenv(dd.CAP_ENV, raising=False)
    profile_before = json.loads(json.dumps(dd.PROFILE))

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await _add_sources(conn)
            default_src, chosen_src = _new_source(1), _new_source(2)
            _, default = await _ingest(conn, default_src)                     # no option
            await _work(committed_dsn, plan_dir=tmp_path)
            monkeypatch.setenv(dd.CAP_ENV, "10")                               # what --discourse-move-cap 10 sets
            _, chosen = await _ingest(conn, chosen_src)
            monkeypatch.delenv(dd.CAP_ENV)
            assert (default["status"], default["per_move_cap"], chosen["status"], chosen["per_move_cap"]) == (
                "enqueued", 5, "enqueued", 10)
            j5, j10 = await _job(conn, default_src["rid"]), await _job(conn, chosen_src["rid"])
            # No option: the profile, limits and key the code bound before this option existed.
            assert json.loads(j5["profile"]) == dd.PROFILE and json.loads(j5["limits"]) == dd.LIMITS
            assert j5["job_key"] == _spec_key(j5, dd.PROFILE)
            # Chosen: only the per-claim limit differs, the per-source limit stays 2, and the key covers it.
            p10 = json.loads(j10["profile"])
            assert p10 == {**dd.PROFILE, "per_move_cap": 10} and p10["per_target_source_cap"] == 2
            assert j10["job_key"] == _spec_key(j10, p10) != _spec_key(j10, dd.PROFILE)
            assert dd.PROFILE == profile_before                                # the module profile is never changed
            assert [j["status"] for j in (await _work(committed_dsn, plan_dir=tmp_path))["jobs"]] == ["completed"]
            for src, cap in ((default_src, 5), (chosen_src, 10)):
                job = await _job(conn, src["rid"])
                assert job["status"] == "completed"
                run = await conn.fetchrow("SELECT * FROM discourse_link_runs WHERE run_id = $1", job["run_id"])
                scope, summary = json.loads(run["approval_scope"]), json.loads(run["summary"])
                claims = len(json.loads(job["moves"]))
                assert (scope["per_move_cap"], scope["per_target_source_cap"]) == (cap, 2)
                assert scope["limits"] == {"max_embedding_writes": claims, "max_candidate_inserts": claims * cap}
                path = tmp_path / f"{job['run_id'].replace(':', '_')}.plan.json"
                assert hashlib.sha256(path.read_bytes()).hexdigest() == scope["plan_sha256"]
                plan = json.loads(path.read_bytes())
                assert plan["method_params"]["per_move_cap"] == summary["discovery"]["method_params"]["per_move_cap"] == cap
                picks = _picks(plan)
                assert max(len(p) for p in picks.values()) == cap               # the wide corpus fills the limit
                assert all([n for n, _ in p] == list(range(1, len(p) + 1)) for p in picks.values())
                cands = await conn.fetch("SELECT * FROM discourse_link_candidates WHERE run_id = $1", job["run_id"])
                assert {json.loads(c["method_params"])["per_move_cap"] for c in cands} == {cap}
                assert max(Counter((str(c["query_move_id"]), c["target_source_rid"]) for c in cands).values()) <= 2
                assert all(c["status"] == "candidate" for c in cands)
            assert await conn.fetchval("SELECT count(*) FROM discourse_relations") == 0
            assert await conn.fetchval("SELECT count(*) FROM discourse_review_events") == 0
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_move_cap_explicit_five_is_the_default_job_and_other_values_are_refused(committed_dsn, ingestion, monkeypatch):
    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            src = _new_source(1)
            monkeypatch.setenv(dd.CAP_ENV, "5")
            written, five = await _ingest(conn, src)
            monkeypatch.delenv(dd.CAP_ENV)
            again = await ex.schedule_discourse_discovery(conn, document_rid=src["rid"], move_ids=written,
                                                          run_id="extract-new-1")
            assert again == {"status": "exists", "job_id": five["job_id"], "job_key": five["job_key"], "per_move_cap": 5,
                             "note": "this extraction already has this job (same profile). Nothing was changed."}
            jobs = await conn.fetchval("SELECT count(*) FROM discourse_discovery_jobs")
            for bad in ("7", "20", "0", "ten", "", "10.0", "5 5"):
                monkeypatch.setenv(dd.CAP_ENV, bad)
                out = await ex.schedule_discourse_discovery(conn, document_rid=src["rid"], move_ids=written,
                                                            run_id="extract-new-1")
                assert out["status"] == "error" and "per-claim limit must be one of (5, 10)" in out["error"], bad
            monkeypatch.delenv(dd.CAP_ENV)
            for bad in (7, 20, 0, "10", 10.0, True):
                with pytest.raises(ValueError, match="per_move_cap must be one of"):
                    await dd.enqueue_for_extraction(conn, source_rid=src["rid"], extraction_run_id="extract-new-1",
                                                    move_ids=written, per_move_cap=bad)
            assert await conn.fetchval("SELECT count(*) FROM discourse_discovery_jobs") == jobs   # nothing queued
            assert all(m["admissible"] for m in (await dl.fetch_moves(conn, written)).values())   # ingestion kept
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_move_cap_never_changes_a_queued_job_or_the_module_profile(committed_dsn, ingestion, monkeypatch):
    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await _add_sources(conn)
            src = _new_source(1)
            written, queued = await _ingest(conn, src)                          # default, pending
            before = dict(await _job(conn, src["rid"]))
            # The same extraction asked for again with 10 is a separate job; the queued one is untouched.
            ten = await dd.enqueue_for_extraction(conn, source_rid=src["rid"], extraction_run_id="extract-new-1",
                                                  move_ids=written, per_move_cap=10)
            assert ten["status"] == "enqueued" and ten["job_key"] != queued["job_key"] and ten["per_move_cap"] == 10
            assert ten["other_jobs_for_this_extraction"] == [{"job_id": queued["job_id"], "per_move_cap": 5,
                                                              "status": "pending"}]
            assert "separate job" in ten["note"] and "were not changed" in ten["note"]
            assert dict(await conn.fetchrow("SELECT * FROM discourse_discovery_jobs WHERE id = $1",
                                            queued["job_id"])) == before
            # Repeating a request (same extraction, same profile) is that job again, and says so.
            again = await dd.enqueue_for_extraction(conn, source_rid=src["rid"], extraction_run_id="extract-new-1",
                                                    move_ids=written, per_move_cap=10)
            assert (again["status"], again["job_id"], again["per_move_cap"]) == ("exists", ten["job_id"], 10)
            assert "Nothing was changed" in again["note"]
            assert again["other_jobs_for_this_extraction"] == ten["other_jobs_for_this_extraction"]
            listed = {j["id"]: j["per_move_cap"] for j in (await dd.job_status(conn, source_rid=src["rid"]))["jobs"]}
            assert listed == {queued["job_id"]: 5, ten["job_id"]: 10}
            assert await conn.fetchval("SELECT count(*) FROM discourse_discovery_jobs") == 2
            # A copy is returned: changing it changes nothing else.
            p = dd.job_profile(10)
            p["per_target_source_cap"] = None
            assert dd.PROFILE["per_target_source_cap"] == 2 and dd.job_profile()["per_move_cap"] == 5
            # A later change to the module default does not reach a queued job: it runs with its own copy.
            monkeypatch.setitem(dd.PROFILE, "per_move_cap", 10)
            out = await _work(committed_dsn, max_jobs=1)
            assert [j["job_id"] for j in out["jobs"]] == [queued["job_id"]]
            run = await conn.fetchrow("SELECT approval_scope FROM discourse_link_runs WHERE run_id = $1",
                                      out["jobs"][0]["run_id"])
            assert json.loads(run["approval_scope"])["per_move_cap"] == 5
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_move_cap_write_budget_and_plan_shape_follow_the_jobs_own_limit(committed_dsn, ingestion, monkeypatch):
    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await _add_sources(conn)
            # A stored limit outside the allowed values (a hand-edited row) fails the job; nothing is written.
            src = _new_source(1)
            await _ingest(conn, src)
            await conn.execute("UPDATE discourse_discovery_jobs SET profile = jsonb_set(profile, '{per_move_cap}', '50')")
            out = await _work(committed_dsn)
            job = await _job(conn, src["rid"])
            assert (out["jobs"][0]["status"], job["status"], job["run_id"]) == ("failed", "failed", None)
            assert job["last_error"] == "per_move_cap 50 is not one of (5, 10)"
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_runs") == 0
            assert await conn.fetchval("SELECT count(*) FROM discourse_link_candidates") == 0
            # The approved budget is claims x the job's own limit.
            for n, cap in ((2, 5), (3, 10)):
                monkeypatch.setenv(dd.CAP_ENV, str(cap))
                await _ingest(conn, _new_source(n))
                job = dict(await _job(conn, _new_source(n)["rid"]))
                assert worker._scope(job, "x")["limits"]["max_candidate_inserts"] == 2 * cap
            monkeypatch.delenv(dd.CAP_ENV)
            # A plan must keep each claim's picks to 1..n within its limit, or apply refuses it before writing.
            plan = await worker._plan_job(committed_dsn, job, dd.configured_context(), ConceptEmbedder(), "ingest:x:1")
            dl._check_plan_shape(plan, "ingest:x:1")                            # control: as planned, accepted
            q = plan["sightings"][0]["query_move_id"]
            mine = [i for i, s in enumerate(plan["sightings"]) if s["query_move_id"] == q]
            assert len(mine) == 10

            def refused(change):
                bad = json.loads(json.dumps(plan, default=str))
                change(bad)
                with pytest.raises(dl.PlanRefused, match="not 1..n within the per-claim limit"):
                    dl._check_plan_shape(bad, "ingest:x:1")

            def eleventh(p):
                p["sightings"].insert(mine[-1] + 1, dict(p["sightings"][mine[-1]], pick=11, effect="candidate_seen"))
                p["planned_effects"]["candidate_seen"] = p["planned_effects"].get("candidate_seen", 0) + 1

            def gap(p):
                p["sightings"][mine[3]]["pick"] = 5

            def lowered(p):
                p["method_params"]["per_move_cap"] = 5

            for change in (eleventh, gap, lowered):
                refused(change)
            unrecorded = json.loads(json.dumps(plan, default=str))   # as a plan made before picks were recorded
            for s in unrecorded["sightings"]:
                s.pop("pick")
            dl._check_plan_shape(unrecorded, "ingest:x:1")
            unrecorded["method_params"]["per_move_cap"] = 5
            with pytest.raises(dl.PlanRefused, match="within the per-claim limit 5"):
                dl._check_plan_shape(unrecorded, "ingest:x:1")
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_move_cap_the_first_five_picks_at_ten_are_the_picks_at_five(committed_dsn, ingestion):
    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await _add_sources(conn)
            written, _ = await _ingest(conn, _new_source(1))
            plans = {}
            for cap in (5, 10):
                async with conn.transaction(isolation="repeatable_read", readonly=True):
                    plans[cap] = await dl.plan_discovery(conn, ConceptEmbedder(), written, context=CTX,
                                                         discovered_by="agent:test", run_id=f"plan:{cap}",
                                                         embedding_model=MODEL, per_move_cap=cap)
            five, ten = _picks(plans[5]), _picks(plans[10])
            assert set(five) == set(ten) == set(written)
            for q in written:
                assert len(five[q]) == 5 and len(ten[q]) == 10 and ten[q][:5] == five[q]
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_move_cap_rediscovery_keeps_the_first_sighting_and_each_run_has_its_own_tier(committed_dsn, ingestion,
                                                                                     monkeypatch):
    keep = ("run_id", "query_move_id", "target_move_id", "method_params", "lexical_rank", "lexical_score",
            "semantic_rank", "semantic_score", "fused_score", "discovered_at", "discovered_by")

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await _add_sources(conn)
            src = _new_source(1)
            await _ingest(conn, src)                                            # the default limit
            await _work(committed_dsn)
            r1 = (await _job(conn, src["rid"]))["run_id"]
            first = {str(r["id"]): dict(r) for r in await conn.fetch(
                "SELECT * FROM discourse_link_candidates WHERE run_id = $1", r1)}
            assert len(first) == 10                                             # two claims, five picks each
            # The same text extracted again, now with 10: each claim meets its five again and adds five.
            monkeypatch.setenv(dd.CAP_ENV, "10")
            await _ingest(conn, src, run_id="extract-new-2")
            out = await _work(committed_dsn)
            r2 = (await _job(conn, src["rid"]))["run_id"]
            assert (out["jobs"][0]["effects"]["candidate_seen"], out["jobs"][0]["effects"]["candidate_inserted"]) == (10, 10)
            after = {str(r["id"]): dict(r) for r in await conn.fetch(
                "SELECT * FROM discourse_link_candidates WHERE id = ANY($1::uuid[])", list(first))}
            for cid, row in first.items():
                assert {k: after[cid][k] for k in keep} == {k: row[k] for k in keep}     # first sighting kept
                assert after[cid]["times_seen"] == 2
            rows = await _rows(committed_dsn, seen_in_runs=[r2])
            assert set(first) < set(rows)                                       # seen again by r2, so listed under it
            assert set(await _rows(committed_dsn, run_ids=[r2])) == set(rows) - set(first)   # first discovered by r2
            assert set(await _rows(committed_dsn, run_ids=[r1])) == set(first)
            for cid in first:
                assert [(o["run_id"], o["sighting"], o["tier"], o["query_end"]) for o in rows[cid]["occurrences"]] == [
                    (r1, "first", "first_five", "a"), (r2, "again", "first_five", "a")]
                assert rows[cid]["occurrences"][0]["pick"] == rows[cid]["occurrences"][1]["pick"] <= 5
                assert rows[cid]["run_id"] == r1                                # the row still names its first run
            new = [r for cid, r in rows.items() if cid not in first]
            assert len(new) == 10 and all(
                [(o["run_id"], o["sighting"], o["tier"]) for o in r["occurrences"]] == [(r2, "first", "additional")]
                and 6 <= r["occurrences"][0]["pick"] <= 10 for r in new)
            items, unanchored = dr.review_items(list(rows.values()), seen_in_runs=[r2])
            assert dr.tier_counts(items) == {"first_five": 10, "additional": 10, "unknown": 0} and unanchored == 0
            assert all(it["anchor"]["stored_end"] == "a" for it in items)     # every r2 search was the stored query's
            # Two claims that rank above everything push earlier picks down: the same pair, first five in r1
            # and r2, is additional in r3. A tier belongs to the occurrence, not to the candidate.
            await _add_sources(conn, [("fixture:closer", "Closer source (fictional)",
                                       (QUERY_CLAIM, QUERY_CLAIM.replace("it landed", "that it landed")))])
            await _ingest(conn, src, run_id="extract-new-3")
            await _work(committed_dsn)
            r3 = (await _job(conn, src["rid"]))["run_id"]
            rows = await _rows(committed_dsn, seen_in_runs=[r3])
            items, _ = dr.review_items(list(rows.values()), seen_in_runs=[r3], tier="additional")
            moved = [it for it in items if it["candidate_id"] in first]
            assert moved
            for it in moved:
                assert {o["run_id"]: o["tier"] for o in it["occurrences"]} == {r1: "first_five", r2: "first_five",
                                                                               r3: "additional"}
                assert it["sighting"]["run_id"] == r3 and it["run_id"] == r1
                assert dr.review_items([rows[it["candidate_id"]]])[0][0]["tier"] == "first_five"   # its first sighting
            # Rolling r3 back removes its sightings from the queue and keeps the earlier ones.
            await canary.run_rollback(committed_dsn, r3)
            assert await _rows(committed_dsn, seen_in_runs=[r3]) == {} == await _rows(committed_dsn, run_ids=[r3])
            rows = await _rows(committed_dsn, run_ids=[r1])
            assert all({o["run_id"] for o in rows[cid]["occurrences"]} == {r1, r2} for cid in first)
            assert {cid: after[cid]["times_seen"] for cid in first} == {
                str(r["id"]): r["times_seen"] for r in await conn.fetch(
                    "SELECT id, times_seen FROM discourse_link_candidates WHERE id = ANY($1::uuid[])", list(first))}
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_move_cap_a_sighting_from_the_other_end_is_reviewed_from_its_own_query(committed_dsn, ingestion, monkeypatch,
                                                                               tmp_path):
    """Y's run finds Y->X first (stored A = Y's claim). X extracted again meets the pair from X's side.
    Reviewing that sighting must study X's claim, record that orientation and bind to that sighting,
    while the stored first sighting stays Y's. Control: a sighting from the stored query's side."""
    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await _add_sources(conn)
            x, y = _new_source(1), _new_source(2)
            xs, _ = await _ingest(conn, x)
            await _work(committed_dsn)
            rx1 = (await _job(conn, x["rid"]))["run_id"]
            monkeypatch.setenv(dd.CAP_ENV, "10")
            ys, _ = await _ingest(conn, y)
            await _work(committed_dsn)
            ry = (await _job(conn, y["rid"]))["run_id"]
            await _ingest(conn, x, run_id="extract-new-2")
            await _work(committed_dsn)
            rx2 = (await _job(conn, x["rid"]))["run_id"]
            rows = await _rows(committed_dsn, seen_in_runs=[rx2])
            items, unanchored = dr.review_items(list(rows.values()), seen_in_runs=[rx2])
            assert unanchored == 0
            reversed_ = [it for it in items if it["anchor"]["stored_end"] == "b"]
            control = next(it for it in items if it["anchor"]["stored_end"] == "a" and it["run_id"] == rx1)
            assert reversed_
            for it in reversed_:
                cid = it["candidate_id"]
                stored = rows[cid]
                assert stored["a"]["move_id"] in ys and stored["run_id"] == ry          # first discovered by Y's run
                assert it["a"]["move_id"] in xs and it["b"]["move_id"] == stored["a"]["move_id"]
                assert it["sighting"] == {"run_id": rx2, "query_move_id": it["a"]["move_id"]}
                occ = json.loads(await conn.fetchval(
                    "SELECT after_state->'occurrence' FROM discourse_link_run_effects WHERE run_id = $1 AND candidate_id = $2",
                    rx2, cid))
                assert occ["query_move_id"] == it["a"]["move_id"]                     # what the run actually searched
            assert control["a"]["move_id"] in xs and rows[control["candidate_id"]]["a"]["move_id"] == control["a"]["move_id"]
            batch = dr.make_batch([reversed_[0], control], database="scratch", created_at="t",
                                  selection={"run_ids": None, "seen_in_runs": [rx2], "tier": None})
            current = {r["candidate_id"]: r for r in await canary._readonly(committed_dsn, lambda c: dr.candidate_rows(
                c, candidate_ids=[i["candidate_id"] for i in batch["items"]]))}
            for item, source in zip(batch["items"], (reversed_[0], control)):
                rec = dr.annotation(batch, item["position"], reviewer="agent:test", answer="useful", created_at="t1")
                assert rec["shown"]["orientation"] == dr.SIGHTING_ORIENTATION
                assert rec["shown"]["a"]["move_id"] == source["a"]["move_id"] == rec["anchor"]["move_id"]
                assert rec["sighting"] == source["sighting"] and rec["anchor"] == source["anchor"]
                assert dr.binding_problems(current[item["candidate_id"]], item) == []
            # The same pair from both ends, in two runs: two items, two separate reviews.
            both, _ = dr.review_items(list((await _rows(committed_dsn, seen_in_runs=[ry, rx2])).values()),
                                      seen_in_runs=[ry, rx2])
            pair = [it for it in both if it["candidate_id"] == reversed_[0]["candidate_id"]]
            assert sorted((it["sighting"]["run_id"], it["anchor"]["stored_end"]) for it in pair) == sorted(
                [(ry, "a"), (rx2, "b")])
            two = dr.make_batch(pair, database="scratch", created_at="t")
            path = str(tmp_path / "ann.jsonl")
            recs = [dr.annotation(two, i, reviewer="agent:test", answer="useful", created_at="t1") for i in (1, 2)]
            assert recs[0]["annotation_id"] != recs[1]["annotation_id"]
            assert recs[0]["shown"]["a"]["move_id"] == recs[1]["shown"]["b"]["move_id"]
            for rec in recs:
                dr.append_annotation(path, rec)
            crossed = dr.annotation(two, 2, reviewer="agent:test", answer="marginal", created_at="t2",
                                    supersedes=recs[0]["annotation_id"])
            with pytest.raises(dr.ReviewError, match="studying the same claim A"):
                dr.append_annotation(path, crossed)
            # After the run is rolled back, its sightings no longer bind an annotation.
            await canary.run_rollback(committed_dsn, rx2)
            current = {r["candidate_id"]: r for r in await canary._readonly(committed_dsn, lambda c: dr.candidate_rows(
                c, candidate_ids=[i["candidate_id"] for i in batch["items"]]))}
            for item in batch["items"]:
                assert any("selected sighting is no longer current" in p
                           for p in dr.binding_problems(current.get(item["candidate_id"]), item))
        finally:
            await conn.close()
    asyncio.run(scenario())


def _keys(obj):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield k
            yield from _keys(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _keys(v)


def test_reviewer_file_cli_flow_reversed_sighting_binding_controls_and_rollback(committed_dsn, ingestion, monkeypatch,
                                                                                tmp_path):
    """S1 end to end through koi-discourse on a throwaway database. Y's run finds Y->X first; X extracted again
    at limit 10 meets that pair from X's side and others at picks 1-10, so its sightings span both tiers.
    `batch --seen-in-run` writes a reviewer file with no run, tier, pick, anchor or candidate id, and a binding
    file with them. annotate refuses without the binding file, with another batch's and with an edited one, and
    records with the right one, reviewed from X's side. Once X's run is rolled back the sighting no longer binds."""
    env = dict(os.environ, KOI_DISCOURSE_DSN=committed_dsn, KOI_DISCOURSE_PYTHON=sys.executable)
    env.pop("KOI_DISCOURSE_REVIEW_MAPPINGS", None)

    def koi(*args):
        return subprocess.run([str(REPO / "scripts/koi-discourse"), *map(str, args)], env=env, capture_output=True,
                              text=True, timeout=120)

    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await _add_sources(conn)
            x, y = _new_source(1), _new_source(2)
            xs, _ = await _ingest(conn, x)
            await _work(committed_dsn)
            rx1 = (await _job(conn, x["rid"]))["run_id"]
            monkeypatch.setenv(dd.CAP_ENV, "10")
            ys, _ = await _ingest(conn, y)
            await _work(committed_dsn)
            ry = (await _job(conn, y["rid"]))["run_id"]
            await _ingest(conn, x, run_id="extract-new-2")
            await _work(committed_dsn)
            rx2 = (await _job(conn, x["rid"]))["run_id"]
        finally:
            await conn.close()
        rows = await _rows(committed_dsn, seen_in_runs=[rx2])
        items, _ = dr.review_items(list(rows.values()), seen_in_runs=[rx2])
        rev = next(it for it in items if it["anchor"]["stored_end"] == "b")
        others = [it for it in items if it["candidate_id"] != rev["candidate_id"]]
        five = next(it for it in others if it["tier"] == "first_five")
        extra = next(it for it in others if it["tier"] == "additional")
        chosen = [rev["candidate_id"], five["candidate_id"], extra["candidate_id"]]
        (tmp_path / "reviewer").mkdir()
        (tmp_path / "operator").mkdir()
        rfile, bfile = tmp_path / "reviewer" / "batch.json", tmp_path / "operator" / "batch.binding.json"
        made = koi("batch", "--seen-in-run", rx2, *[a for c in chosen for a in ("--candidate-id", c)], "--out", rfile,
                   "--binding-out", bfile)
        assert made.returncode == 0, made.stderr
        raw, binding = rfile.read_bytes(), json.loads(bfile.read_text())
        text = raw.decode()
        assert not set(_keys(json.loads(raw))) & {"candidate_id", "candidate_key", "scores", "run_id", "pick", "tier",
                                                   "anchor", "sighting", "stored_end", "selection", "occurrences"}
        for s in (rx1, rx2, ry, "first_five", "additional", *rows):
            assert s not in text, s
        assert {b["tier"] for b in binding["items"]} == {"first_five", "additional"}      # a mixed-tier batch
        assert binding["selection"] == {"run_ids": None, "seen_in_runs": [rx2], "tier": None}
        bound = next(b for b in binding["items"] if b["candidate_id"] == rev["candidate_id"])
        shown = next(i for i in json.loads(raw)["items"] if i["position"] == bound["position"])
        assert bound["anchor"] == {"move_id": rev["a"]["move_id"], "stored_end": "b"}
        assert shown["a"]["move_id"] == rev["a"]["move_id"] and shown["a"]["move_id"] in xs   # reviewed from X's side
        assert shown["b"]["move_id"] == rows[rev["candidate_id"]]["a"]["move_id"] and shown["b"]["move_id"] in ys
        blind = koi("show", "--batch", rfile)
        assert blind.returncode == 0 and rx2 not in blind.stdout and "first_five" not in blind.stdout
        ann = tmp_path / "operator" / "annotations.jsonl"
        args = ("annotate", "--batch", rfile, "--item", bound["position"], "--item-id", shown["item_id"][:10],
                "--answer", "useful", "--reviewer", "agent:a", "--annotations", ann)
        missing = koi(*args)
        assert missing.returncode != 0 and "needs its binding file" in missing.stderr
        assert koi("batch", "--seen-in-run", ry, "--candidate-id", rev["candidate_id"], "--out",
                   tmp_path / "reviewer" / "other.json", "--binding-out", tmp_path / "operator" / "other.binding.json"
                   ).returncode == 0
        wrong = koi(*args, "--binding", tmp_path / "operator" / "other.binding.json")
        assert wrong.returncode != 0 and "belongs to another reviewer file" in wrong.stderr
        edited = json.loads(bfile.read_text())
        edited["items"][0]["tier"] = {"first_five": "additional", "additional": "first_five"}[edited["items"][0]["tier"]]
        (tmp_path / "operator" / "edited.binding.json").write_text(json.dumps(edited))
        tampered = koi(*args, "--binding", tmp_path / "operator" / "edited.binding.json")
        assert tampered.returncode != 0 and "binding file does not match its own id" in tampered.stderr
        assert not ann.exists()                                                       # nothing recorded so far
        ok = koi(*args, "--binding", bfile)
        assert ok.returncode == 0, ok.stderr
        (rec,) = dr.read_annotations(str(ann))
        assert rec["schema"] == dr.SPLIT_ANNOTATION_SCHEMA and rec["shown"]["orientation"] == dr.SIGHTING_ORIENTATION
        assert (rec["candidate"]["id"], rec["shown"]["a"]["move_id"], rec["anchor"], rec["sighting"]) == (
            rev["candidate_id"], rev["a"]["move_id"], bound["anchor"], {"run_id": rx2, "query_move_id": rev["a"]["move_id"]})
        assert (rec["binding_id"], rec["item_id"], rec["batch_id"]) == (
            binding["binding_id"], bound["item_id"], binding["reviewer_batch_id"])
        verified = koi("show", "--batch", rfile, "--binding", bfile, "--verify")
        assert verified.returncode == 0 and verified.stdout.count("unchanged since the batch was made") == 3
        await canary.run_rollback(committed_dsn, rx2)
        after = koi(*args[:-4], "--reviewer", "agent:b", "--annotations", ann, "--binding", bfile)
        assert after.returncode != 0 and "selected sighting is no longer current" in after.stderr
        assert len(dr.read_annotations(str(ann))) == 1                                # the earlier line is kept
    asyncio.run(scenario())


def test_move_cap_sightings_recorded_before_picks_are_first_five_only_when_their_run_kept_five(committed_dsn,
                                                                                              ingestion, monkeypatch):
    async def scenario():
        conn = await asyncpg.connect(committed_dsn)
        try:
            await _add_sources(conn)
            five, ten = _new_source(1), _new_source(2)
            await _ingest(conn, five)
            await _work(committed_dsn)
            monkeypatch.setenv(dd.CAP_ENV, "10")
            await _ingest(conn, ten)
            monkeypatch.delenv(dd.CAP_ENV)
            await _work(committed_dsn)
            r5, r10 = (await _job(conn, five["rid"]))["run_id"], (await _job(conn, ten["rid"]))["run_id"]
            # As effect rows written before picks were recorded (the canary's and ingest:1:1's).
            await conn.execute("UPDATE discourse_link_run_effects SET after_state = after_state - 'occurrence'")
            rows = await _rows(committed_dsn, run_ids=[r5, r10])
            got = {(o["run_id"], o["tier"], o["tier_basis"], o["pick"], o["query_end"])
                   for r in rows.values() for o in r["occurrences"]}
            items, unanchored = dr.review_items(list(rows.values()), seen_in_runs=[r5, r10])
            assert unanchored == 0 and all(it["anchor"]["stored_end"] == "a" for it in items)   # a first sighting's
            # query is the stored row's own
            assert got == {(r5, "first_five", "pick not recorded; the run kept at most 5 per claim", None, "a"),
                           (r10, "unknown", "pick not recorded", None, "a")}
        finally:
            await conn.close()
    asyncio.run(scenario())


def test_move_cap_ingest_option_is_explicit_and_needs_discovery(monkeypatch, tmp_path, capsys):
    seen = []
    queued = {"status": "enqueued", "job_id": 7, "per_move_cap": 10}
    result = {"discourse_discovery": queued}

    async def fake_ingest_path(**kw):
        seen.append(os.environ.get(dd.CAP_ENV))
        return {"document_rid": "document:x", "tier": kw["tier"], "rag": {}, "extract": dict(result)}
    monkeypatch.setattr(ingest_cli, "ingest_path", fake_ingest_path)
    for flag in DISCOURSE_FLAGS + (dd.CAP_ENV,):        # recorded, so teardown restores the absence
        monkeypatch.setenv(flag, "0")
        monkeypatch.delenv(flag)
    src = tmp_path / "s.md"
    src.write_text("text")
    base = ["ingest_document.py", "--source-path", str(src), "--tier", "thorough", "--discourse-discovery"]
    monkeypatch.setenv(dd.CAP_ENV, "10")                 # inherited, not chosen: no option means the default
    monkeypatch.setattr(sys, "argv", base)
    ingest_cli.main()
    assert seen == [None] and dd.CAP_ENV not in os.environ
    monkeypatch.setattr(sys, "argv", base + ["--discourse-move-cap", "10"])
    ingest_cli.main()
    assert seen[-1] == "10" and str(queued) in capsys.readouterr().out
    result["discourse_discovery"] = None                 # the extraction queued nothing
    ingest_cli.main()
    assert "discovery job:  none queued; the per-claim limit 10 was not applied" in capsys.readouterr().out
    for argv in (base + ["--discourse-move-cap", "7"], base + ["--discourse-move-cap", "20"],
                 base[:-1] + ["--discourse-move-cap", "10"]):
        monkeypatch.setattr(sys, "argv", argv)
        with pytest.raises(SystemExit) as refused:
            ingest_cli.main()
        assert refused.value.code == 2
    assert len(seen) == 3                                # refused before anything ran
