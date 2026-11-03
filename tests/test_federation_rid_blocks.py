"""Blocked records are never sent to a peer again (migration 137).

Each test builds its own schema in the KOI_TEST_DATABASE_URL database: koi_net_events as it is on this
node, the record tables the fetch paths and the block loader read, and federation_rid_blocks from
migration 137. It drives the real EventQueue, emit_domain_event and emit_doclink_event, and the real
/koi-net fetch handlers. Three synthetic records are blocked: a task, a person and a doclink. A kept
task, a kept organization and an unrelated doclink are the controls.

Covered: queueing (local emits and inbound broadcasts), events that only refer to a blocked record,
events already queued before the block (poll withholds and marks them, so the peer's queue does not
stall, and they are never re-served), WEBHOOK peek across retry cycles, /bundles/fetch and
/manifests/fetch on stored and expired events, the /rids/fetch listing, failing closed when the table
is missing, an empty block list changing nothing, the down file's refusal and the hash-only loader.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import uuid
from pathlib import Path

import pytest
import pytest_asyncio
from tests import koi_test_targets as targets

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

asyncpg = pytest.importorskip("asyncpg")

pytestmark = targets.requires_database

from api import federation_events as fe  # noqa: E402
from api import koi_net_router as knr  # noqa: E402
from api.event_queue import EventQueue  # noqa: E402
from api.federation_blocks import FederationBlocksUnavailable, rid_sha256  # noqa: E402

MIGRATION_137 = (ROOT / "migrations" / "137_federation_rid_blocks.sql").read_text()
MIGRATION_137_DOWN = (ROOT / "migrations" / "137_federation_rid_blocks_down.sql").read_text()
DDL = """
-- Shadows public.koi_migrations, so the migration's own bookkeeping row lands in this schema.
CREATE TABLE koi_migrations (migration_id TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now(), checksum TEXT);
CREATE TABLE koi_net_events (
    id SERIAL PRIMARY KEY, event_id UUID DEFAULT gen_random_uuid(), event_type VARCHAR(10) NOT NULL,
    rid TEXT NOT NULL, manifest JSONB, contents JSONB, source_node TEXT, queued_at TIMESTAMPTZ DEFAULT now(),
    delivered_to TEXT[] DEFAULT '{}', confirmed_by TEXT[] DEFAULT '{}',
    expires_at TIMESTAMPTZ DEFAULT now() + interval '24 hours', target_node TEXT
);
CREATE UNIQUE INDEX ON koi_net_events (source_node, event_id, COALESCE(target_node, '')) WHERE event_id IS NOT NULL;
CREATE TABLE entity_registry (
    id SERIAL PRIMARY KEY, koi_rid TEXT, fuseki_uri TEXT UNIQUE, entity_type TEXT, node_private BOOLEAN,
    merged_into TEXT, metadata JSONB
);
CREATE TABLE task_registry (task_key TEXT PRIMARY KEY, title TEXT);
CREATE TABLE document_entity_links (
    document_rid TEXT, entity_uri TEXT, mention_count INT DEFAULT 1, PRIMARY KEY (document_rid, entity_uri)
);
CREATE TABLE koi_outbound_shares (
    document_rid TEXT, target_node TEXT, shared_at TIMESTAMPTZ DEFAULT now(), retracted_at TIMESTAMPTZ,
    PRIMARY KEY (document_rid, target_node)
);
"""
SELF, PEER = "orn:koi-net.node:self", "orn:koi-net.node:peer"
GONE_DOC, OTHER_DOC = "orn:personal-koi.claude-session:gone-1111", "orn:personal-koi.document:other-2222"
BLOCKED_TASK, KEPT_TASK = "incident::blocked-task-0001", "incident::kept-task-0002"
BLOCKED_PERSON = "orn:personal-koi.entity:person-blocked-name-0003"
KEPT_ORG = "orn:personal-koi.entity:organization-kept-0004"
BLOCKED_DOCLINK = fe.doclink_rid(GONE_DOC, KEPT_ORG)  # blocked by its own RID; refers only to a kept entity
KEPT_DOCLINK = fe.doclink_rid(OTHER_DOC, KEPT_ORG)
PERSON_DOCLINK = fe.doclink_rid(OTHER_DOC, BLOCKED_PERSON)  # a new link that refers to the blocked person
BLOCKED = (BLOCKED_TASK, BLOCKED_PERSON, BLOCKED_DOCLINK)


def _domain(domain, payload):
    return {"_koi_domain": domain, "payload": payload}


@pytest_asyncio.fixture
async def db():
    schema = f"fed_block_{uuid.uuid4().hex[:10]}"
    pool0 = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=1)
    async with pool0.acquire() as conn:
        await conn.execute(f'CREATE SCHEMA "{schema}"')
        await conn.execute(f'SET search_path TO "{schema}"')
        await conn.execute(DDL)
        await conn.execute(MIGRATION_137)
        await conn.executemany("INSERT INTO task_registry VALUES ($1, 'synthetic')", [(BLOCKED_TASK,), (KEPT_TASK,)])
        await conn.executemany(
            "INSERT INTO entity_registry (koi_rid, fuseki_uri, entity_type, node_private) VALUES ($1, $2, $3, false)",
            [("orn:koi-net.entity:person-blocked", BLOCKED_PERSON, "Person"), ("orn:koi-net.entity:org-kept", KEPT_ORG, "Organization")])
        await conn.executemany("INSERT INTO document_entity_links (document_rid, entity_uri) VALUES ($1, $2)",
                               [(GONE_DOC, KEPT_ORG), (OTHER_DOC, KEPT_ORG)])
    await pool0.close()
    # Only the test schema is on the path, so a public table of the same name can never stand in.
    pool = await asyncpg.create_pool(targets.database_url(), min_size=1, max_size=4, server_settings={"search_path": f'"{schema}"'})
    try:
        yield schema, pool
    finally:
        fe.set_event_queue(None)
        async with pool.acquire() as conn:
            await conn.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await pool.close()


async def _block(pool, *ids):
    async with pool.acquire() as conn:
        await conn.executemany("INSERT INTO federation_rid_blocks (rid_sha256, kind, reason) VALUES ($1, 'test', 'test')",
                               [(rid_sha256(i),) for i in ids])


async def _rows(pool):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT rid, delivered_to, confirmed_by FROM koi_net_events ORDER BY id")


async def _seed_queue(pool, expired=False):
    """Events queued before the block existed, blocked ones at the head of the queue."""
    exp = "now() - interval '1 hour'" if expired else "now() + interval '24 hours'"
    rows = [
        ("UPDATE", BLOCKED_TASK, _domain("task", {"task_key": BLOCKED_TASK, "title": "t"})),
        ("NEW", BLOCKED_DOCLINK, _domain("document_entity_link", {"document_rid": GONE_DOC, "entity_uri": KEPT_ORG})),
        ("NEW", PERSON_DOCLINK, _domain("document_entity_link", {"document_rid": OTHER_DOC, "entity_uri": BLOCKED_PERSON})),
        ("UPDATE", KEPT_TASK, _domain("task", {"task_key": KEPT_TASK, "title": "k"})),
        ("NEW", KEPT_DOCLINK, _domain("document_entity_link", {"document_rid": OTHER_DOC, "entity_uri": KEPT_ORG})),
    ]
    async with pool.acquire() as conn:
        for n, (et, rid, c) in enumerate(rows):
            await conn.execute(
                f"INSERT INTO koi_net_events (event_type, rid, manifest, contents, source_node, queued_at, expires_at) "
                f"VALUES ($1, $2, $3, $4, $5, now() - make_interval(mins => $6), {exp})",
                et, rid, json.dumps({"rid": rid, "timestamp": "2026-10-25T00:00:00Z"}), json.dumps(c), SELF, 60 - n)


@pytest.mark.asyncio
async def test_add_withholds_blocked_and_referring_events_and_queues_the_rest(db):
    _, pool = db
    await _block(pool, *BLOCKED)
    eq = EventQueue(pool, SELF)
    assert await eq.add("UPDATE", BLOCKED_TASK, contents=_domain("task", {"task_key": BLOCKED_TASK})) is None
    assert await eq.add("UPDATE", BLOCKED_PERSON, contents=_domain("entity", {"fuseki_uri": BLOCKED_PERSON})) is None
    assert await eq.add("NEW", BLOCKED_DOCLINK, contents=_domain("document_entity_link", {"document_rid": GONE_DOC, "entity_uri": KEPT_ORG})) is None
    # Refers to the blocked person without being it: a doclink, and a task listing it as a collaborator.
    assert await eq.add("NEW", PERSON_DOCLINK, contents=_domain("document_entity_link", {"document_rid": OTHER_DOC, "entity_uri": BLOCKED_PERSON})) is None
    assert await eq.add("UPDATE", "incident::other-0005", contents=_domain("task", {"collaborator_uris": [KEPT_ORG, BLOCKED_PERSON]})) is None
    assert await eq.add("NEW", "orn:x:1", manifest={"rid": BLOCKED_TASK}) is None
    # Inbound broadcast from a peer for a blocked record.
    assert await eq.add("UPDATE", BLOCKED_TASK, contents=_domain("task", {}), source_node=PEER, event_id=str(uuid.uuid4())) is None
    for rid, c in ((KEPT_TASK, _domain("task", {"task_key": KEPT_TASK})), (KEPT_ORG, _domain("entity", {"fuseki_uri": KEPT_ORG})),
                   (KEPT_DOCLINK, _domain("document_entity_link", {"document_rid": OTHER_DOC, "entity_uri": KEPT_ORG}))):
        assert await eq.add("UPDATE", rid, contents=c)
    assert [r["rid"] for r in await _rows(pool)] == [KEPT_TASK, KEPT_ORG, KEPT_DOCLINK]


@pytest.mark.asyncio
async def test_emitters_withhold_and_fail_closed_without_raising(db, monkeypatch):
    _, pool = db
    monkeypatch.setenv("KOI_FEDERATE_KNOWLEDGE", "true")
    await _block(pool, *BLOCKED)
    fe.set_event_queue(EventQueue(pool, SELF))
    await fe.emit_domain_event("task", "UPDATE", BLOCKED_TASK, {"task_key": BLOCKED_TASK})
    await fe.emit_domain_event("entity", "UPDATE", BLOCKED_PERSON, {"fuseki_uri": BLOCKED_PERSON})
    await fe.emit_doclink_event(GONE_DOC, KEPT_ORG, 1)
    await fe.emit_doclink_event(OTHER_DOC, BLOCKED_PERSON, 1)
    await fe.emit_domain_event("task", "UPDATE", KEPT_TASK, {"task_key": KEPT_TASK})
    await fe.emit_doclink_event(OTHER_DOC, KEPT_ORG, 1)
    assert [r["rid"] for r in await _rows(pool)] == [KEPT_TASK, KEPT_DOCLINK]
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE federation_rid_blocks")
    with pytest.raises(FederationBlocksUnavailable):
        await EventQueue(pool, SELF).add("UPDATE", KEPT_TASK, contents=_domain("task", {}))
    await fe.emit_domain_event("task", "UPDATE", KEPT_TASK, {"task_key": KEPT_TASK})  # logs, never raises
    assert len(await _rows(pool)) == 2


@pytest.mark.asyncio
async def test_poll_withholds_events_queued_before_the_block_without_stalling_or_retrying(db):
    _, pool = db
    await _seed_queue(pool)
    await _block(pool, *BLOCKED)
    eq = EventQueue(pool, SELF)
    # The 3 blocked events fill the first page. They are marked, not served, so the next page
    # reaches the kept events instead of re-reading the same head forever.
    assert await eq.poll(PEER, limit=3) == []
    assert [e["rid"] for e in await eq.poll(PEER, limit=3)] == [KEPT_TASK, KEPT_DOCLINK]
    assert await eq.poll(PEER, limit=50) == []  # withheld events are never re-served
    # The same holds for an edge filter that admits the domains.
    other = "orn:koi-net.node:other"
    assert [e["rid"] for e in await eq.poll(other, limit=50, rid_types=["task", "document_entity_link"])] == [KEPT_TASK, KEPT_DOCLINK]
    rows = {r["rid"]: r for r in await _rows(pool)}
    for rid in (BLOCKED_TASK, BLOCKED_DOCLINK, PERSON_DOCLINK):
        assert set(rows[rid]["delivered_to"]) == {PEER, other} and rows[rid]["confirmed_by"] == []


@pytest.mark.asyncio
async def test_peek_skips_blocked_events_on_every_push_retry(db):
    _, pool = db
    await _seed_queue(pool)
    await _block(pool, *BLOCKED)
    eq = EventQueue(pool, SELF)
    first = await eq.peek_undelivered(PEER)
    assert [e["rid"] for e in first] == [KEPT_TASK, KEPT_DOCLINK]
    # A failed push marks nothing; the next cycle peeks the same kept events and still no blocked one.
    assert [e["rid"] for e in await eq.peek_undelivered(PEER)] == [KEPT_TASK, KEPT_DOCLINK]
    assert await eq.mark_delivered([e["event_id"] for e in first], PEER) == 2
    assert await eq.peek_undelivered(PEER) == []


@pytest.mark.asyncio
async def test_poll_and_peek_serve_nothing_without_the_table(db):
    _, pool = db
    await _seed_queue(pool)
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE federation_rid_blocks")
    eq = EventQueue(pool, SELF)
    assert await eq.poll(PEER) == []
    assert await eq.peek_undelivered(PEER) == []
    assert all(r["delivered_to"] == [] for r in await _rows(pool))


@pytest.mark.asyncio
async def test_empty_block_list_changes_nothing(db):
    _, pool = db
    await _seed_queue(pool)
    eq = EventQueue(pool, SELF)
    everything = [BLOCKED_TASK, BLOCKED_DOCLINK, PERSON_DOCLINK, KEPT_TASK, KEPT_DOCLINK]
    assert [e["rid"] for e in await eq.peek_undelivered(PEER)] == everything
    assert [e["rid"] for e in await eq.poll(PEER)] == everything
    assert await eq.add("UPDATE", BLOCKED_TASK, contents=_domain("task", {}))


@pytest_asyncio.fixture
async def router(db, monkeypatch):
    _, pool = db
    monkeypatch.setattr(knr, "_db_pool", pool)
    monkeypatch.setattr(knr, "_security_policy", lambda: {"require_approved_edge_for_poll": False})
    monkeypatch.setattr(knr, "_federation_rid_groups", lambda: None)

    async def call(handler, payload):
        async def unwrap(_request, allow_unsigned=False):
            return payload, PEER, False
        monkeypatch.setattr(knr, "_unwrap_request", unwrap)
        return json.loads((await handler(None)).body)
    return pool, call


@pytest.mark.asyncio
@pytest.mark.parametrize("expired", [False, True])
async def test_bundle_and_manifest_fetch_never_serve_blocked_records(router, expired):
    pool, call = router
    await _seed_queue(pool, expired=expired)  # fetch reads stored events whether or not they expired
    await _block(pool, *BLOCKED)
    asked = [BLOCKED_TASK, BLOCKED_DOCLINK, PERSON_DOCLINK, KEPT_TASK, KEPT_DOCLINK, BLOCKED_PERSON]
    b = await call(knr.bundles_fetch, {"rids": asked})
    assert [x["manifest"]["rid"] for x in b["bundles"]] == [KEPT_TASK, KEPT_DOCLINK]
    assert set(b["not_found"]) == {BLOCKED_TASK, BLOCKED_DOCLINK, PERSON_DOCLINK, BLOCKED_PERSON}
    m = await call(knr.manifests_fetch, {"rids": asked})
    assert [x["rid"] for x in m["manifests"]] == [KEPT_TASK, KEPT_DOCLINK]
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE federation_rid_blocks")
    b = await call(knr.bundles_fetch, {"rids": asked})
    assert b["bundles"] == [] and set(b["not_found"]) == set(asked)
    assert (await call(knr.manifests_fetch, {"rids": asked})).get("manifests") == []


@pytest.mark.asyncio
async def test_rids_listing_hides_blocked_entities(router):
    pool, call = router
    assert set((await call(knr.rids_fetch, {}))["rids"]) == {"orn:koi-net.entity:person-blocked", "orn:koi-net.entity:org-kept"}
    await _block(pool, BLOCKED_PERSON)  # blocked by entity URI; the listing shows koi_rids
    assert (await call(knr.rids_fetch, {}))["rids"] == ["orn:koi-net.entity:org-kept"]
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE federation_rid_blocks")
    assert (await call(knr.rids_fetch, {}))["rids"] == []


@pytest.mark.asyncio
async def test_migration_records_itself_only_in_the_test_schema(db):
    schema, pool = db
    async with pool.acquire() as conn:
        public = await conn.fetchval("SELECT to_regclass('public.koi_migrations') IS NOT NULL")
        q = "SELECT count(*), max(applied_at) FROM public.koi_migrations WHERE migration_id = '137_federation_rid_blocks'"
        before = tuple(await conn.fetchrow(q)) if public else None
        await conn.execute("DELETE FROM koi_migrations")
        await conn.execute(MIGRATION_137)
        assert await conn.fetchval(f'SELECT count(*) FROM "{schema}".koi_migrations WHERE migration_id = $1', "137_federation_rid_blocks") == 1
        assert (tuple(await conn.fetchrow(q)) if public else None) == before


@pytest.mark.asyncio
async def test_down_migration_refuses_while_a_record_is_blocked(db):
    _, pool = db
    await _block(pool, BLOCKED_TASK)
    async with pool.acquire() as conn:
        with pytest.raises(asyncpg.RaiseError, match="still blocks 1 record"):
            await conn.execute(MIGRATION_137_DOWN)
        assert await conn.fetchval("SELECT count(*) FROM federation_rid_blocks") == 1
        await conn.execute("DELETE FROM federation_rid_blocks")
        await conn.execute(MIGRATION_137_DOWN)
        assert await conn.fetchval("SELECT to_regclass('federation_rid_blocks')") is None


def _loader():
    spec = importlib.util.spec_from_file_location("block_rids", ROOT / "scripts" / "federation" / "block_rids.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _manifest(tmp_path, records, expected=None):
    exp = expected or {}
    if expected is None:
        for _, k in records:
            exp[k] = exp.get(k, 0) + 1
    p = tmp_path / f"m-{uuid.uuid4().hex[:8]}.json"
    p.write_text(json.dumps({"reason": "test", "expected": exp, "records": [{"sha256": rid_sha256(i), "kind": k} for i, k in records]}))
    return str(p)


@pytest.mark.asyncio
async def test_loader_blocks_exact_records_from_hashes_and_refuses_otherwise(db, tmp_path, capsys):
    schema, pool = db
    br = _loader()
    br.DB_URL = f"{targets.database_url()}?search_path={schema}"
    good = [(BLOCKED_TASK, "task"), (BLOCKED_PERSON, "entity"), ("orn:koi-net.entity:person-blocked", "entity-koi-rid"),
            (BLOCKED_DOCLINK, "doclink")]
    m = _manifest(tmp_path, good)

    async def count():
        async with pool.acquire() as conn:
            return await conn.fetchval("SELECT count(*) FROM federation_rid_blocks")

    await br.run(m, apply=False)
    assert await count() == 0
    await br.run(m, apply=True)
    assert await count() == 4
    await br.run(m, apply=True)
    assert await count() == 4 and "blocked 0 new" in capsys.readouterr().out
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM federation_rid_blocks")
        # A doclink whose local row moved to a merge survivor is still known by its emitted event.
        await conn.execute("INSERT INTO koi_net_events (event_type, rid, source_node) VALUES ('NEW', $1, $2)", PERSON_DOCLINK, SELF)
    await br.run(_manifest(tmp_path, [(PERSON_DOCLINK, "doclink")]), apply=True)
    assert await count() == 1
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM federation_rid_blocks")
    for bad in (_manifest(tmp_path, good + [("incident::not-here", "task")]),  # not a local record
                _manifest(tmp_path, [(KEPT_TASK, "entity")]),  # right identifier, wrong kind
                _manifest(tmp_path, [(fe.doclink_rid(OTHER_DOC, "orn:x:none"), "doclink")]),  # no link and no event
                _manifest(tmp_path, good, expected={"task": 2, "entity": 1, "entity-koi-rid": 1, "doclink": 1})):
        with pytest.raises(br.Refused):
            await br.run(bad, apply=True)
        assert await count() == 0
    out = capsys.readouterr().out
    for ident in BLOCKED + (KEPT_TASK, "incident::not-here"):
        assert ident not in out
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE federation_rid_blocks")
    with pytest.raises(br.Refused, match="apply migration 137 first"):
        await br.run(m, apply=False)


async def _seed_manifest_refs(pool):
    """Unrelated RIDs whose manifest alone names a blocked record."""
    async with pool.acquire() as conn:
        for n, rid in enumerate(("orn:x:manifest-ref-1", "orn:x:manifest-ref-2")):
            await conn.execute(
                "INSERT INTO koi_net_events (event_type, rid, manifest, contents, source_node, queued_at) "
                "VALUES ('NEW', $1, $2, $3, $4, now() - make_interval(mins => $5))",
                rid, json.dumps({"rid": rid, "timestamp": "2026-10-25T00:00:00Z", "references": [BLOCKED_PERSON]}),
                json.dumps({"_koi_domain": "task", "payload": {"title": "clean"}}), SELF, 90 - n)


@pytest.mark.asyncio
async def test_a_reference_in_the_manifest_alone_is_withheld_on_every_read_path(router):
    pool, call = router
    await _seed_queue(pool)
    await _seed_manifest_refs(pool)
    await _block(pool, *BLOCKED)
    eq = EventQueue(pool, SELF)
    refs = ["orn:x:manifest-ref-1", "orn:x:manifest-ref-2"]
    assert [e["rid"] for e in await eq.peek_undelivered(PEER)] == [KEPT_TASK, KEPT_DOCLINK]
    assert [e["rid"] for e in await eq.poll(PEER)] == [KEPT_TASK, KEPT_DOCLINK]
    b = await call(knr.bundles_fetch, {"rids": refs + [KEPT_TASK]})
    assert [x["manifest"]["rid"] for x in b["bundles"]] == [KEPT_TASK] and set(b["not_found"]) == set(refs)
    m = await call(knr.manifests_fetch, {"rids": refs + [KEPT_TASK]})
    assert [x["rid"] for x in m["manifests"]] == [KEPT_TASK]


@pytest.mark.asyncio
async def test_rids_listing_hides_an_entity_blocked_by_koi_rid(router):
    pool, call = router
    await _block(pool, "orn:koi-net.entity:person-blocked")
    assert (await call(knr.rids_fetch, {}))["rids"] == ["orn:koi-net.entity:org-kept"]


@pytest.mark.asyncio
async def test_logs_never_name_a_blocked_record(db, router, caplog, monkeypatch):
    _, pool = db
    _, call = router
    monkeypatch.setenv("KOI_FEDERATE_KNOWLEDGE", "true")
    caplog.set_level("DEBUG")
    await _seed_queue(pool)
    await _block(pool, *BLOCKED)
    eq = EventQueue(pool, SELF)
    fe.set_event_queue(eq)
    monkeypatch.setattr(knr, "_event_queue", eq)
    await fe.emit_domain_event("entity", "UPDATE", BLOCKED_PERSON, {"fuseki_uri": BLOCKED_PERSON})
    await fe.emit_doclink_event(OTHER_DOC, BLOCKED_PERSON, 1)
    await eq.poll(PEER)
    await eq.peek_undelivered(PEER)
    await call(knr.bundles_fetch, {"rids": list(BLOCKED)})
    await call(knr.events_broadcast, {"events": [{"rid": BLOCKED_TASK, "event_type": "UPDATE", "contents": _domain("task", {})}]})
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE federation_rid_blocks")
    # Fail closed: nothing can be recognised as blocked, and still nothing is named.
    await fe.emit_domain_event("entity", "UPDATE", BLOCKED_PERSON, {"fuseki_uri": BLOCKED_PERSON})
    await call(knr.events_broadcast, {"events": [{"rid": BLOCKED_TASK, "event_type": "UPDATE", "contents": _domain("task", {})}]})
    await eq.poll(PEER)
    text = caplog.text
    assert "Withheld" in text and "federation_rid_blocks is missing" in text
    for ident in BLOCKED + (PERSON_DOCLINK,):
        assert ident not in text


@pytest.mark.asyncio
async def test_a_blocked_share_is_refused_and_not_recorded(db, router, monkeypatch):
    _, pool = db
    await _block(pool, *BLOCKED)
    monkeypatch.setattr(knr, "_event_queue", EventQueue(pool, SELF))
    monkeypatch.setattr(knr, "_node_profile", type("P", (), {"node_rid": SELF})())

    async def resolve(_conn, _recipient):
        return PEER
    monkeypatch.setattr(knr, "_resolve_recipient", resolve)
    blocked = await knr.share_document(knr.ShareDocumentRequest(
        document_rid=OTHER_DOC, recipient="peer", share_mode="root_only", contents={"about": BLOCKED_PERSON}))
    assert blocked.status_code == 409 and json.loads(blocked.body)["error_code"] == "RECORD_BLOCKED"
    ok = await knr.share_document(knr.ShareDocumentRequest(document_rid=OTHER_DOC, recipient="peer", share_mode="root_only"))
    assert ok.status == "queued"
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM koi_outbound_shares") == 1
        assert [r["rid"] for r in await conn.fetch("SELECT rid FROM koi_net_events")] == [OTHER_DOC]


@pytest.mark.asyncio
async def test_vault_scan_is_skipped_without_the_table_so_no_state_is_written(db, monkeypatch, tmp_path):
    _, pool = db
    from api.vault_sync import VaultSyncManager
    mgr = VaultSyncManager(pool, SELF, EventQueue(pool, SELF), str(tmp_path))
    scanned = []

    async def peers():
        return [{"shared_folder": "Shared", "peer_node_rid": PEER}]

    async def scan_folder(folder, peer_list):
        scanned.append(folder)
    monkeypatch.setattr(mgr, "_get_all_peers", peers)
    monkeypatch.setattr(mgr, "_scan_folder", scan_folder)
    await mgr._scan_async()
    assert scanned == ["Shared"]
    async with pool.acquire() as conn:
        await conn.execute("DROP TABLE federation_rid_blocks")
    await mgr._scan_async()
    assert scanned == ["Shared"]
