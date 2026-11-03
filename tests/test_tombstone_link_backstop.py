"""Phase 1b: no write path may link a document to a merged-away entity.

Found 2026-10-12: 81 document_entity_links rows pointed at tombstones. The main
writer was /ingest. generate_entity_uri is a pure hash of (type, normalized name),
so when that URI belongs to a merged-away row every tier misses (they match live
rows only), Tier 3 "creates", store_new_entity's ON CONFLICT DO NOTHING no-ops, and
/ingest linked the document to the tombstone and emitted a NEW event for it.

Every test drives the real function against KOI_TEST_DATABASE_URL inside a transaction
that is rolled back, so nothing persists. The trigger tests apply migration 132
inside that same transaction; the /ingest tests deliberately do NOT, so they prove
the code fix on its own and the trigger stays a backstop.
"""

from __future__ import annotations

import logging
import uuid
from pathlib import Path

import asyncpg
import pytest
from tests import koi_test_targets as targets

pytestmark = pytest.mark.anyio

REPO = Path(__file__).resolve().parents[1]
MIGRATION_132 = REPO / "migrations" / "132_document_entity_links_live_entity_uri.sql"



@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def conn():
    c = await asyncpg.connect(targets.database_url())
    tx = c.transaction()
    await tx.start()
    yield c
    await tx.rollback()   # nothing this file does ever persists
    await c.close()


def _tag() -> str:
    return uuid.uuid4().hex[:10]


async def _mk(conn, uri, text, etype, merged_into=None):
    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text, "
        "merged_into) VALUES ($1,$2,$3,$4,$5)",
        uri, text, etype, text.lower(), merged_into)


async def _tombstoned_name(conn):
    """A name whose own generated Concept URI is a tombstone merged into a live Project.

    The survivor has a different name and no alias, so tiers 1, 1.1 and 1.1b all
    miss and only the create-collision check can find it.
    """
    from api.personal_ingest_api import generate_entity_uri

    t = _tag()
    name = f"Phase1b Probe {t}"
    survivor = f"orn:personal-koi.entity:project-phase1b-survivor-{t}"
    tomb = generate_entity_uri(name, "Concept")
    await _mk(conn, survivor, f"Phase1b Survivor {t}", "Project")
    await _mk(conn, tomb, name, "Concept", merged_into=survivor)
    return name, tomb, survivor


class _OneConnPool:
    """db_pool stand-in that hands out the test's own connection.

    /ingest opens conn.transaction() on what it acquires; on a connection that is
    already inside the fixture's transaction that becomes a savepoint, so the whole
    request rolls back with the fixture.
    """

    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Ctx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *_exc):
                return False

        return _Ctx()


@pytest.fixture
def ingest_env(monkeypatch, conn):
    """Point /ingest at the fixture connection and capture its side channels."""
    from api import federation_events
    from api import personal_ingest_api as pia

    events = []
    outbox = []

    async def capture_event(domain, event_type, rid, payload, **_kw):
        events.append((domain, event_type, rid, payload))

    async def capture_outbox(_conn, kind, payload, **_kw):
        outbox.append((kind, payload))

    async def no_receipt(*_a, **_kw):
        raise RuntimeError("receipts are not under test")

    monkeypatch.setattr(pia, "db_pool", _OneConnPool(conn))
    monkeypatch.setattr(pia, "embedding_provider", None, raising=False)
    monkeypatch.setattr(pia, "ENABLE_SEMANTIC_MATCHING", False, raising=False)
    monkeypatch.setattr(pia, "_log_resolver_decision", lambda *a, **kw: None)
    monkeypatch.setattr(pia, "create_receipt", no_receipt)
    monkeypatch.setattr(pia, "enqueue_outbox", capture_outbox)
    monkeypatch.setattr(federation_events, "emit_domain_event", capture_event)
    return {"events": events, "outbox": outbox}


# ---------------------------------------------------------------------------
# 11a. resolve_entity: a create that collides with a tombstone returns the survivor
# ---------------------------------------------------------------------------

async def test_resolve_entity_follows_a_tombstoned_generated_uri(conn, monkeypatch):
    from api import personal_ingest_api as pia
    from api.personal_ingest_api import ExtractedEntity, resolve_entity

    monkeypatch.setattr(pia, "ENABLE_SEMANTIC_MATCHING", False, raising=False)
    name, tomb, survivor = await _tombstoned_name(conn)

    canonical, is_new = await resolve_entity(conn, ExtractedEntity(name=name, type="Concept"))
    assert (canonical.uri, is_new, canonical.is_new) == (survivor, False, False)
    assert canonical.type == "Project", "type must be the survivor's, not the tombstone's"
    assert canonical.merged_with == name

    # The exact-only path (register-entity exact_only) has its own create branch.
    canonical, is_new = await resolve_entity(
        conn, ExtractedEntity(name=name, type="Concept"), skip_fuzzy=True)
    assert (canonical.uri, is_new) == (survivor, False)


async def test_resolve_entity_follows_a_two_hop_chain(conn, monkeypatch):
    from api import personal_ingest_api as pia
    from api.personal_ingest_api import ExtractedEntity, generate_entity_uri, resolve_entity

    monkeypatch.setattr(pia, "ENABLE_SEMANTIC_MATCHING", False, raising=False)
    t = _tag()
    name = f"Phase1b Chain {t}"
    live = f"orn:personal-koi.entity:project-phase1b-live-{t}"
    mid = f"orn:personal-koi.entity:organization-phase1b-mid-{t}"
    await _mk(conn, live, f"Phase1b Live {t}", "Project")
    await _mk(conn, mid, f"Phase1b Mid {t}", "Organization", merged_into=live)
    await _mk(conn, generate_entity_uri(name, "Concept"), name, "Concept", merged_into=mid)

    canonical, is_new = await resolve_entity(conn, ExtractedEntity(name=name, type="Concept"))
    assert (canonical.uri, is_new) == (live, False)


async def test_resolve_entity_still_creates_when_the_uri_is_free(conn, monkeypatch):
    """Positive twin: the collision check must not turn a real create into a match."""
    from api import personal_ingest_api as pia
    from api.personal_ingest_api import ExtractedEntity, generate_entity_uri, resolve_entity

    monkeypatch.setattr(pia, "ENABLE_SEMANTIC_MATCHING", False, raising=False)
    name = f"Phase1b Fresh {_tag()}"
    canonical, is_new = await resolve_entity(conn, ExtractedEntity(name=name, type="Concept"))
    assert is_new and canonical.uri == generate_entity_uri(name, "Concept")


async def test_store_new_entity_reports_a_noop_insert(conn, monkeypatch):
    from api import personal_ingest_api as pia
    from api.personal_ingest_api import (
        CanonicalEntity, ExtractedEntity, generate_entity_uri, store_new_entity,
    )

    outbox = []

    async def capture_outbox(_conn, kind, payload, **_kw):
        outbox.append(payload["fuseki_uri"])

    monkeypatch.setattr(pia, "embedding_provider", None, raising=False)
    monkeypatch.setattr(pia, "enqueue_outbox", capture_outbox)

    name, tomb, _survivor = await _tombstoned_name(conn)
    ent = ExtractedEntity(name=name, type="Concept")
    taken = CanonicalEntity(name=name, uri=tomb, type="Concept", is_new=True)
    assert await store_new_entity(conn, ent, taken, "orn:test.doc:phase1b") is False
    assert outbox == [], "a no-op insert wrote a TerminusDB upsert for a row it did not create"

    fresh_name = f"Phase1b Fresh {_tag()}"
    fresh_uri = generate_entity_uri(fresh_name, "Concept")
    fresh = CanonicalEntity(name=fresh_name, uri=fresh_uri, type="Concept", is_new=True)
    assert await store_new_entity(
        conn, ExtractedEntity(name=fresh_name, type="Concept"), fresh, "orn:test.doc:phase1b") is True
    assert outbox == [fresh_uri]


def _resolve_or_create_fn():
    """The real nested _resolve_or_create POST /knowledge/episodes uses, via its closure."""
    from api.routers.knowledge_router import create_router

    for route in create_router(None).routes:
        if getattr(route, "path", "") == "/episodes" and "POST" in route.methods:
            fn = route.endpoint
            free = dict(zip(fn.__code__.co_freevars,
                            [c.cell_contents for c in (fn.__closure__ or ())]))
            assert "_resolve_or_create" in free, f"closure has {sorted(free)}"
            return free["_resolve_or_create"]
    raise AssertionError("POST /episodes route not found; the wiring changed")


async def test_knowledge_add_create_collision_follows_the_tombstone(conn):
    """F3: deep extraction / knowledge-add returned the tombstone URI on a create collision.

    Its name tiers match tombstones by normalized_text and follow them, so the create
    branch is reached only when the tombstone holding the generated URI no longer carries
    that name (renamed before it was merged). That is the case set up here.
    """
    from api.personal_ingest_api import generate_entity_uri

    t = _tag()
    name = f"Phase1b Renamed {t}"
    survivor = f"orn:personal-koi.entity:project-phase1b-kr-survivor-{t}"
    tomb = generate_entity_uri(name, "Concept")
    await _mk(conn, survivor, f"Phase1b Kr Survivor {t}", "Project")
    await _mk(conn, tomb, f"Zz Unrelated Label {t}", "Concept", merged_into=survivor)

    resolve_or_create = _resolve_or_create_fn()
    uri, is_new, rtype = await resolve_or_create(
        conn, name, True, None, {}, type_hint="Concept")
    assert (uri, is_new, rtype) == (survivor, False, "Project")


# ---------------------------------------------------------------------------
# 11a / 11e. /ingest end to end: link the survivor, report no NEW, emit no NEW
# ---------------------------------------------------------------------------

async def _links(conn, doc):
    return sorted(r["entity_uri"] for r in await conn.fetch(
        "SELECT entity_uri FROM document_entity_links WHERE document_rid = $1", doc))


async def test_ingest_links_the_survivor_not_the_tombstone(conn, ingest_env):
    """The 2026-10-12 incident shape: "KOI" as Concept while concept-koi was merged away."""
    from api import personal_ingest_api as pia

    name, tomb, survivor = await _tombstoned_name(conn)
    doc = f"orn:test.doc:phase1b-{_tag()}"
    rows_before = await conn.fetchval(
        "SELECT count(*) FROM entity_registry WHERE normalized_text = $1", name.lower())

    resp = await pia.ingest_extraction(pia.IngestRequest(
        document_rid=doc, entities=[pia.ExtractedEntity(name=name, type="Concept")]))

    assert resp.stats.new_entities == 0 and resp.stats.resolved_entities == 1
    assert resp.stats.failed_entities == 0
    [ce] = resp.canonical_entities
    assert (ce.uri, ce.is_new) == (survivor, False)
    assert await _links(conn, doc) == [survivor]
    assert await conn.fetchval(
        "SELECT count(*) FROM entity_registry WHERE normalized_text = $1", name.lower()
    ) == rows_before, "a duplicate row was minted"

    entity_events = [(t, rid) for d, t, rid, _ in ingest_env["events"] if d == "entity"]
    assert entity_events == [("UPDATE", survivor)], entity_events
    # A peer's apply overwrites entity_type and entity_text, so the UPDATE must carry
    # the survivor's own identity, not the extracted Concept (which would retype it).
    [payload] = [p for d, _, _, p in ingest_env["events"] if d == "entity"]
    stored = await conn.fetchrow(
        "SELECT entity_text, entity_type FROM entity_registry WHERE fuseki_uri = $1", survivor)
    assert (payload["entity_type"], payload["entity_text"]) == (
        stored["entity_type"], stored["entity_text"]) == ("Project", ce.name)
    doclinks = [p["entity_uri"] for d, _, _, p in ingest_env["events"]
                if d == "document_entity_link"]
    assert doclinks == [survivor]


async def test_ingest_handles_a_noop_insert_after_resolve(conn, ingest_env, monkeypatch):
    """The race arm: resolve said NEW, but the URI was taken (here, by a tombstone)
    before store_new_entity ran. /ingest must not count, link or announce it as NEW.
    """
    from api import personal_ingest_api as pia

    name, tomb, survivor = await _tombstoned_name(conn)

    async def stale_resolve(conn_, entity, context=None, **_kw):
        return pia.CanonicalEntity(
            name=entity.name, uri=tomb, type="Concept", is_new=True), True

    monkeypatch.setattr(pia, "resolve_entity", stale_resolve)
    doc = f"orn:test.doc:phase1b-{_tag()}"
    resp = await pia.ingest_extraction(pia.IngestRequest(
        document_rid=doc, entities=[pia.ExtractedEntity(name=name, type="Concept")]))

    assert resp.stats.new_entities == 0 and resp.stats.resolved_entities == 1
    [ce] = resp.canonical_entities
    assert (ce.uri, ce.is_new) == (survivor, False)
    assert await _links(conn, doc) == [survivor]
    assert not any(d == "entity" and t == "NEW" for d, t, _, _ in ingest_env["events"])
    assert ingest_env["outbox"] == []


# ---------------------------------------------------------------------------
# 11c. IngestRequest: the sensor's unimplemented fields are accepted and logged
# ---------------------------------------------------------------------------

def test_ingest_request_logs_the_sensor_fields_it_drops(caplog):
    from api.personal_ingest_api import IngestRequest

    payload = {
        "document_rid": "claude-session:phase1b",
        "entities": [],
        "source": "claude-sessions-sensor",
        "replace_existing": True,
        "link_existing_only": True,
    }
    with caplog.at_level(logging.WARNING, logger="api.personal_ingest_api"):
        req = IngestRequest(**payload)   # must not 422 the live sensor
    assert req.document_rid == "claude-session:phase1b"
    msgs = [r.getMessage() for r in caplog.records]
    assert any("link_existing_only" in m and "replace_existing" in m for m in msgs), msgs

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="api.personal_ingest_api"):
        IngestRequest(document_rid="d", entities=[])
    assert not caplog.records, "a request with only known fields must not warn"


def test_ingest_request_schema_is_unchanged():
    """Plan AC7: the /ingest OpenAPI surface does not change."""
    from api.personal_ingest_api import IngestRequest

    props = set(IngestRequest.model_json_schema()["properties"])
    assert props == {
        "document_rid", "content", "entities", "relationships", "source",
        "context", "parent_receipt_id", "request_id",
    }


# ---------------------------------------------------------------------------
# 11b. Tier 1.5 and the register-entity cross-type warning ignore tombstones
# ---------------------------------------------------------------------------

async def test_tier_1_5_candidates_exclude_tombstones(conn):
    from api.personal_ingest_api import get_contextual_candidates_internal

    t = _tag()
    person = f"orn:personal-koi.entity:person-phase1b-attendee-{t}"
    live = f"orn:personal-koi.entity:concept-phase1b-live-{t}"
    tomb = f"orn:personal-koi.entity:concept-phase1b-tomb-{t}"
    doc = f"orn:test.doc:phase1b-ctx-{t}"
    await _mk(conn, person, f"Phase1b Attendee {t}", "Person")
    await _mk(conn, live, f"Phase1b Live Concept {t}", "Concept")
    await _mk(conn, tomb, f"Phase1b Tomb Concept {t}", "Concept", merged_into=live)
    for uri in (person, live, tomb):   # no trigger here: the link to tomb is stored as-is
        await conn.execute(
            "INSERT INTO document_entity_links (document_rid, entity_uri) VALUES ($1,$2)",
            doc, uri)

    out = await get_contextual_candidates_internal(
        conn, attendee_uris=[person], entity_types=["Concept"])
    uris = {c["uri"] for c in out["candidates"]}
    assert live in uris, "positive twin: the live co-occurring entity must be a candidate"
    assert tomb not in uris


async def test_register_entity_cross_type_warning_ignores_tombstones(conn, ingest_env):
    from api import personal_ingest_api as pia

    t = _tag()
    name = f"Phase1b Shared {t}"
    other = f"orn:personal-koi.entity:project-phase1b-other-{t}"
    await _mk(conn, f"orn:personal-koi.entity:concept-phase1b-shared-{t}", name, "Concept")
    await _mk(conn, other, f"Phase1b Other {t}", "Project")
    await _mk(conn, f"orn:personal-koi.entity:organization-phase1b-shared-{t}", name,
              "Organization", merged_into=other)

    resp = await pia.register_vault_entity(pia.RegisterEntityRequest(
        name=name, entity_type="Person", force_type=True, exact_only=True,
        vault_rid=f"orn:obsidian.entity:Notes/People/phase1b-{t}",
        vault_path=f"People/Phase1b {t}.md"))

    assert resp.cross_type_warning and "Concept" in resp.cross_type_warning, \
        "positive twin: the live same-name Concept must still be warned about"
    assert "Organization" not in resp.cross_type_warning


async def test_register_entity_event_names_the_survivor(conn, ingest_env):
    """Without force_type a create collision links the survivor; the UPDATE event must
    carry the survivor's stored name and type, or every peer renames and retypes it."""
    from api import personal_ingest_api as pia

    name, _tomb, survivor = await _tombstoned_name(conn)
    t = _tag()
    resp = await pia.register_vault_entity(pia.RegisterEntityRequest(
        name=name, entity_type="Concept",
        vault_rid=f"orn:obsidian.entity:Notes/Concepts/phase1b-{t}",
        vault_path=f"Concepts/Phase1b {t}.md"))

    assert (resp.canonical_uri, resp.is_new) == (survivor, False)
    [(etype, rid, payload)] = [(et, r, p) for d, et, r, p in ingest_env["events"]
                               if d == "entity"]
    stored = await conn.fetchrow(
        "SELECT entity_text, entity_type FROM entity_registry WHERE fuseki_uri = $1", survivor)
    assert (etype, rid) == ("UPDATE", survivor)
    assert stored["entity_type"] == "Project"
    assert (payload["entity_type"], payload["entity_text"]) == (
        stored["entity_type"], stored["entity_text"])


@pytest.mark.parametrize("exact_only", [False, True])
async def test_register_entity_force_type_refuses_a_cross_type_holder(
        conn, ingest_env, exact_only):
    """force_type promises a same-named entity of THIS type. When the deterministic URI
    is a tombstone merged into another type, that create is impossible, so the caller
    gets a 409 rather than a silent link to a different-type survivor."""
    from fastapi import HTTPException

    from api import personal_ingest_api as pia

    name, tomb, survivor = await _tombstoned_name(conn)
    t = _tag()
    with pytest.raises(HTTPException) as exc:
        await pia.register_vault_entity(pia.RegisterEntityRequest(
            name=name, entity_type="Concept", force_type=True, exact_only=exact_only,
            vault_rid=f"orn:obsidian.entity:Notes/Concepts/phase1b-ft-{t}",
            vault_path=f"Concepts/Phase1b Ft {t}.md"))
    assert exc.value.status_code == 409
    assert tomb in exc.value.detail and survivor in exc.value.detail
    assert not any(d == "entity" for d, *_ in ingest_env["events"])


async def test_register_entity_force_type_keeps_a_same_type_holder(conn, ingest_env):
    """Positive twin: a same-type survivor does not break force_type, so it is returned."""
    from api import personal_ingest_api as pia
    from api.personal_ingest_api import generate_entity_uri

    t = _tag()
    name = f"Phase1b Same {t}"
    survivor = f"orn:personal-koi.entity:concept-phase1b-same-survivor-{t}"
    await _mk(conn, survivor, f"Phase1b Same Survivor {t}", "Concept")
    await _mk(conn, generate_entity_uri(name, "Concept"), name, "Concept", merged_into=survivor)

    resp = await pia.register_vault_entity(pia.RegisterEntityRequest(
        name=name, entity_type="Concept", force_type=True,
        vault_rid=f"orn:obsidian.entity:Notes/Concepts/phase1b-same-{t}",
        vault_path=f"Concepts/Phase1b Same {t}.md"))
    assert (resp.canonical_uri, resp.is_new) == (survivor, False)


# ---------------------------------------------------------------------------
# Resolver shadow telemetry: a collision is not recorded as a create
# ---------------------------------------------------------------------------

@pytest.fixture
def shadow_records(monkeypatch):
    """Force every resolver-shadow attempt to be sampled and capture its record."""
    from api import personal_ingest_api as pia
    from api import resolver_shadow

    records = []
    real_start = pia.start_attempt

    def sampled_start(**kw):
        kw["sampled_override"] = True
        return real_start(**kw)

    monkeypatch.setattr(pia, "start_attempt", sampled_start)
    monkeypatch.setattr(resolver_shadow, "emit_nonblocking", records.append)
    monkeypatch.setattr(pia, "ENABLE_SEMANTIC_MATCHING", False, raising=False)
    return records


async def test_shadow_records_a_collision_as_uri_held(conn, shadow_records):
    from api.personal_ingest_api import ExtractedEntity, resolve_entity

    name, tomb, survivor = await _tombstoned_name(conn)
    await resolve_entity(conn, ExtractedEntity(name=name, type="Concept"))

    [rec] = shadow_records
    assert (rec["active_uri"], rec["active_outcome"]) == (survivor, "uri_held")
    assert tomb not in (rec["legacy_uri"], rec["strict_uri"])


async def test_shadow_still_records_a_real_create(conn, shadow_records):
    """Positive twin: a free URI is still recorded as a create of that URI."""
    from api.personal_ingest_api import ExtractedEntity, generate_entity_uri, resolve_entity

    name = f"Phase1b Shadow Fresh {_tag()}"
    await resolve_entity(conn, ExtractedEntity(name=name, type="Concept"))

    [rec] = shadow_records
    assert (rec["active_uri"], rec["active_outcome"]) == (
        generate_entity_uri(name, "Concept"), "create")


# ---------------------------------------------------------------------------
# 11d. Migration 132: the trigger rewrites inserts and updates to the survivor
# ---------------------------------------------------------------------------

async def _apply_132(conn):
    await conn.execute(MIGRATION_132.read_text())


async def _chain(conn):
    t = _tag()
    live = f"orn:personal-koi.entity:concept-phase1b-trg-live-{t}"
    mid = f"orn:personal-koi.entity:concept-phase1b-trg-mid-{t}"
    tomb = f"orn:personal-koi.entity:concept-phase1b-trg-tomb-{t}"
    await _mk(conn, live, f"Trg Live {t}", "Concept")
    await _mk(conn, mid, f"Trg Mid {t}", "Concept", merged_into=live)
    await _mk(conn, tomb, f"Trg Tomb {t}", "Concept", merged_into=mid)
    return live, mid, tomb, f"orn:test.doc:phase1b-trg-{t}"


async def test_trigger_rewrites_inserts_through_the_chain(conn):
    live, mid, tomb, doc = await _chain(conn)
    await _apply_132(conn)

    await conn.execute(
        "INSERT INTO document_entity_links (document_rid, entity_uri) VALUES ($1,$2)", doc, tomb)
    assert await _links(conn, doc) == [live]

    # Live and unregistered URIs are left alone (entity_uri has no FK).
    other = f"{doc}-2"
    unknown = f"orn:personal-koi.entity:concept-phase1b-unregistered-{_tag()}"
    await conn.execute(
        "INSERT INTO document_entity_links (document_rid, entity_uri) VALUES ($1,$2),($1,$3)",
        other, live, unknown)
    assert await _links(conn, other) == sorted([live, unknown])


async def test_trigger_lets_on_conflict_fold_into_the_survivor_row(conn):
    """/ingest's upsert must find the survivor's existing row, not add a second one."""
    live, _mid, tomb, doc = await _chain(conn)
    await _apply_132(conn)
    upsert = """
        INSERT INTO document_entity_links (document_rid, entity_uri, context)
        VALUES ($1, $2, NULL)
        ON CONFLICT (document_rid, entity_uri)
        DO UPDATE SET mention_count = document_entity_links.mention_count + 1
    """
    await conn.execute(upsert, doc, live)
    await conn.execute(upsert, doc, tomb)
    rows = await conn.fetch(
        "SELECT entity_uri, mention_count FROM document_entity_links WHERE document_rid=$1", doc)
    assert [(r["entity_uri"], r["mention_count"]) for r in rows] == [(live, 2)]


async def test_trigger_rewrites_updates_of_entity_uri_only(conn):
    live, _mid, tomb, doc = await _chain(conn)
    # A tombstone link written BEFORE the trigger exists (the 81-row state).
    await conn.execute(
        "INSERT INTO document_entity_links (document_rid, entity_uri) VALUES ($1,$2)", doc, tomb)
    await _apply_132(conn)

    # UPDATE of another column does not fire the UPDATE OF entity_uri arm.
    await conn.execute(
        "UPDATE document_entity_links SET mention_count = 5 WHERE document_rid = $1", doc)
    assert await _links(conn, doc) == [tomb]

    await conn.execute(
        "UPDATE document_entity_links SET entity_uri = $2 WHERE document_rid = $1", doc, tomb)
    assert await _links(conn, doc) == [live]


async def test_trigger_leaves_a_cycle_unchanged(conn):
    t = _tag()
    a = f"orn:personal-koi.entity:concept-phase1b-cyc-a-{t}"
    b = f"orn:personal-koi.entity:concept-phase1b-cyc-b-{t}"
    await _mk(conn, a, f"Cyc A {t}", "Concept")
    await _mk(conn, b, f"Cyc B {t}", "Concept", merged_into=a)
    await conn.execute("UPDATE entity_registry SET merged_into = $2 WHERE fuseki_uri = $1", a, b)
    await _apply_132(conn)

    doc = f"orn:test.doc:phase1b-cyc-{t}"
    await conn.execute(
        "INSERT INTO document_entity_links (document_rid, entity_uri) VALUES ($1,$2)", doc, b)
    assert await _links(conn, doc) == [b]


async def test_unmerge_restores_links_with_the_trigger_installed(conn):
    """unmerge repoints links to the loser; with 132 installed that only sticks if the
    loser is un-tombstoned first, or the trigger sends every link back to the survivor.
    """
    import json

    from api.merge_reversal import capture_reversal, unmerge

    t = _tag()
    survivor = f"orn:personal-koi.entity:concept-phase1b-um-a-{t}"
    loser = f"orn:personal-koi.entity:concept-phase1b-um-b-{t}"
    doc = f"orn:test.doc:phase1b-um-{t}"
    await _mk(conn, survivor, f"Um Alpha {t}", "Concept")
    await _mk(conn, loser, f"Um Beta {t}", "Concept")
    await conn.execute(
        "INSERT INTO document_entity_links (document_rid, entity_uri) VALUES ($1,$2)", doc, loser)
    await _apply_132(conn)

    reversal = await capture_reversal(conn, loser=loser, survivor=survivor)
    await conn.execute(
        "UPDATE document_entity_links SET entity_uri = $1 WHERE entity_uri = $2", survivor, loser)
    await conn.execute(
        "UPDATE entity_registry SET merged_into=$2, merged_at=NOW() WHERE fuseki_uri=$1",
        loser, survivor)
    mid = await conn.fetchval(
        "INSERT INTO entity_merge_log (survivor_uri, loser_uri, rewired, merged_by, reversal) "
        "VALUES ($1,$2,'{}'::jsonb,'test',$3::jsonb) RETURNING id",
        survivor, loser, json.dumps(reversal))
    assert await _links(conn, doc) == [survivor]

    result = await unmerge(conn, mid)
    assert await _links(conn, doc) == [loser], "the trigger sent the restored link back"
    assert result["restored"]["document_entity_links.entity_uri"] == 1


# ---------------------------------------------------------------------------
# Multi-row writers under migration 132: deep extraction Flag 2 and the github sensor
# ---------------------------------------------------------------------------

async def _episode_naming_a_shared_survivor(conn):
    """An episode whose live facts name two tombstones and the survivor they share.

    The live shape: episode 3f2c38fe names project-pol.is and project-polis, both
    merged into softwareapplication-polis.
    """
    t = _tag()
    survivor = f"orn:personal-koi.entity:project-phase1b-f2-live-{t}"
    tomb_a = f"orn:personal-koi.entity:concept-phase1b-f2-a-{t}"
    tomb_b = f"orn:personal-koi.entity:concept-phase1b-f2-b-{t}"
    other = f"orn:personal-koi.entity:concept-phase1b-f2-other-{t}"
    await _mk(conn, survivor, f"F2 Live {t}", "Project")
    await _mk(conn, tomb_a, f"F2 A {t}", "Concept", merged_into=survivor)
    await _mk(conn, tomb_b, f"F2 B {t}", "Concept", merged_into=survivor)
    await _mk(conn, other, f"F2 Other {t}", "Concept")
    ep = await conn.fetchval(
        "INSERT INTO knowledge_episodes (name, source_document) VALUES ($1, $2) RETURNING id",
        f"phase1b-f2-{t}", f"https://example.test/phase1b-{t}")
    for s, o in ((tomb_a, other), (tomb_b, other), (survivor, other)):
        await conn.execute(
            "INSERT INTO knowledge_facts (episode_id, subject_uri, predicate, object_uri, "
            "fact_text) VALUES ($1, $2, 'relatedTo', $3, 'phase1b')", ep, s, o)
    return ep, survivor, other, f"orn:test.doc:phase1b-f2-{t}"


async def test_trigger_rejects_a_multi_row_upsert_that_folds_two_rows(conn):
    """The hazard the Flag 2 fix exists for, pinned so the migration header stays true."""
    ep, _survivor, _other, doc = await _episode_naming_a_shared_survivor(conn)
    await _apply_132(conn)
    with pytest.raises(asyncpg.exceptions.CardinalityViolationError):
        await conn.execute("""
            INSERT INTO document_entity_links (document_rid, entity_uri, mention_count)
            SELECT $2, subject_uri, count(*)::int FROM knowledge_facts
             WHERE episode_id = $1 GROUP BY subject_uri
            ON CONFLICT (document_rid, entity_uri) DO UPDATE SET mention_count = EXCLUDED.mention_count
        """, ep, doc)


@pytest.mark.parametrize("with_trigger", [True, False])
async def test_flag2_links_fold_onto_the_survivor(conn, with_trigger):
    """Deep extraction's Flag 2 statement resolves before grouping: it succeeds under
    the trigger, and without it (132 not applied) it still writes no tombstone link."""
    from scripts.extract_deep_documents import FLAG2_DOC_ENTITY_LINKS_SQL

    ep, survivor, other, doc = await _episode_naming_a_shared_survivor(conn)
    if with_trigger:
        await _apply_132(conn)

    for _ in range(2):   # a re-extraction reuses the episode and re-runs the upsert
        await conn.execute(FLAG2_DOC_ENTITY_LINKS_SQL, ep, doc, "phase1b")
    rows = await conn.fetch(
        "SELECT entity_uri, mention_count FROM document_entity_links "
        "WHERE document_rid = $1 ORDER BY entity_uri", doc)
    assert {r["entity_uri"]: r["mention_count"] for r in rows} == {survivor: 3, other: 3}


async def test_flag2_keeps_an_unregistered_uri(conn):
    """Same rule as the trigger: a URI with no registry row is written as-is (entity_uri
    has no FK). A broken merged_into chain cannot be built: entity_registry has an FK."""
    from scripts.extract_deep_documents import FLAG2_DOC_ENTITY_LINKS_SQL

    t = _tag()
    live = f"orn:personal-koi.entity:concept-phase1b-f2-reg-{t}"
    unregistered = f"orn:personal-koi.entity:concept-phase1b-f2-unreg-{t}"
    await _mk(conn, live, f"F2 Reg {t}", "Concept")
    ep = await conn.fetchval(
        "INSERT INTO knowledge_episodes (name) VALUES ($1) RETURNING id", f"phase1b-f2u-{t}")
    await conn.execute(
        "INSERT INTO knowledge_facts (episode_id, subject_uri, predicate, object_uri, fact_text) "
        "VALUES ($1, $2, 'relatedTo', $3, 'phase1b')", ep, live, unregistered)
    doc = f"orn:test.doc:phase1b-f2u-{t}"
    await conn.execute(FLAG2_DOC_ENTITY_LINKS_SQL, ep, doc, "phase1b")
    assert await _links(conn, doc) == sorted([live, unregistered])


async def test_github_sensor_links_and_emits_live_entities_only(conn, monkeypatch, tmp_path):
    """The sensor matched every registry row's text, tombstones included; under 132 the
    stored link moved to the survivor while the emit still named the tombstone."""
    monkeypatch.setenv("VAULT_PATH", str(tmp_path))   # required at import time
    try:
        from api.github_sensor import GitHubSensor
    except ImportError as e:   # the GitHub sensor (or its tree-sitter grammars) is optional
        pytest.skip(f"GitHub sensor not importable here: {e}")

    t = _tag()
    survivor = f"orn:personal-koi.entity:project-phase1b-gh-live-{t}"
    tomb = f"orn:personal-koi.entity:concept-phase1b-gh-tomb-{t}"
    await _mk(conn, survivor, f"Ghlive{t}", "Project")
    await _mk(conn, tomb, f"Ghtomb{t}", "Concept", merged_into=survivor)
    await _apply_132(conn)

    sensor = GitHubSensor.__new__(GitHubSensor)   # _link_entities uses no instance state
    content = f"Ghlive{t} and Ghtomb{t} appear in this file. " + "x" * 60
    emits = await sensor._link_entities(
        conn, {"content": content, "rel_path": f"phase1b-{t}.md"}, "phase1b-repo")

    doc = f"github:phase1b-repo:phase1b-{t}.md"
    stored = {r["entity_uri"]: r["mention_count"] for r in await conn.fetch(
        "SELECT entity_uri, mention_count FROM document_entity_links WHERE document_rid = $1",
        doc)}
    assert stored == {survivor: 1}, "the tombstone's own name must not add a mention"
    assert [u for _d, u in emits if u in (survivor, tomb)] == [survivor]
