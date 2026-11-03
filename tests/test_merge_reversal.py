"""Round-trip tests for merge reversal.

The assertion that matters is NOT "unmerge ran without error" -- it is that the
graph is byte-for-byte back where it started. A reversal that reports success
while leaving references pointing at the survivor is the failure mode this
whole feature exists to prevent, and it looks identical from the return value.

Requires KOI_TEST_DATABASE_URL; skipped otherwise.
"""

import json

import asyncpg
import pytest

from api.merge_reversal import capture_reversal, unmerge, _ref_cols
from tests import koi_test_targets as targets

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"

A = "orn:personal-koi.entity:concept-unmerge-test-alpha-aaaaaaaaaaaa"
B = "orn:personal-koi.entity:concept-unmerge-test-beta-bbbbbbbbbbbb"


async def _mk(conn, uri, text, aliases):
    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, "
        "normalized_text, aliases) VALUES ($1,$2,'Concept',$3,$4) "
        "ON CONFLICT (fuseki_uri) DO NOTHING",
        uri, text, text.lower(), aliases,
    )


@pytest.fixture
async def conn():
    c = await asyncpg.connect(targets.database_url())
    tx = c.transaction()
    await tx.start()
    yield c
    await tx.rollback()   # nothing this test does ever persists
    await c.close()


async def test_capture_covers_every_column_the_merge_rewires(conn):
    """A capture narrower than the rewire yields an undo that silently misses rows."""
    from api.routers.admin_router import _PLAIN_REF_COLS

    covered = _ref_cols()
    missing = [c for c in _PLAIN_REF_COLS if c not in covered]
    assert not missing, f"merge rewires columns the capture does not record: {missing}"


async def test_unmerge_refuses_a_pre_118_merge(conn):
    """262 historical merges have reversal IS NULL and must be refused, not guessed."""
    mid = await conn.fetchval(
        "INSERT INTO entity_merge_log (survivor_uri, loser_uri, rewired, merged_by) "
        "VALUES ($1,$2,'{}'::jsonb,'test') RETURNING id", A, B)
    with pytest.raises(ValueError, match="NOT REVERSIBLE"):
        await unmerge(conn, mid)


async def test_unmerge_refuses_a_double_revert(conn):
    mid = await conn.fetchval(
        "INSERT INTO entity_merge_log (survivor_uri, loser_uri, rewired, merged_by, "
        "reversal, reverted_at) VALUES ($1,$2,'{}'::jsonb,'test',$3::jsonb, NOW()) "
        "RETURNING id", A, B, json.dumps({"schema": 1, "refs": {}}))
    with pytest.raises(ValueError, match="already reverted"):
        await unmerge(conn, mid)


async def test_round_trip_restores_facts_and_tombstone(conn):
    """The real test: merge, then unmerge, then assert the world is back."""
    await _mk(conn, A, "Unmerge Test Alpha", ["alpha alias"])
    await _mk(conn, B, "Unmerge Test Beta", ["beta alias"])

    ep = await conn.fetchval(
        "INSERT INTO knowledge_episodes (name) VALUES ('unmerge-test') RETURNING id")
    fid = await conn.fetchval(
        "INSERT INTO knowledge_facts (episode_id, subject_uri, predicate, fact_text) "
        "VALUES ($1,$2,'TESTS','beta owns this fact') RETURNING id", ep, B)

    reversal = await capture_reversal(conn, loser=B, survivor=A)
    # ids are captured as TEXT on purpose: knowledge_facts.id is uuid while the
    # other 12 tables are integer, and text is the only representation that
    # both json.dumps and the restore cast round-trip.
    assert str(fid) in reversal["refs"]["knowledge_facts.subject_uri"]
    assert "beta alias" in reversal["aliases_added"]

    # Perform the merge's essential effects (rewire + alias union + tombstone).
    await conn.execute("UPDATE knowledge_facts SET subject_uri=$1 WHERE subject_uri=$2", A, B)
    await conn.execute(
        "UPDATE entity_registry SET aliases = ARRAY(SELECT DISTINCT unnest("
        "array_cat(aliases, $2::text[]))) WHERE fuseki_uri=$1",
        A, reversal["aliases_added"])
    await conn.execute(
        "UPDATE entity_registry SET merged_into=$2, merged_at=NOW() WHERE fuseki_uri=$1", B, A)

    mid = await conn.fetchval(
        "INSERT INTO entity_merge_log (survivor_uri, loser_uri, rewired, merged_by, reversal) "
        "VALUES ($1,$2,'{}'::jsonb,'test',$3::jsonb) RETURNING id",
        A, B, json.dumps(reversal))

    # Confirm the merge actually took effect, or the round trip proves nothing.
    assert await conn.fetchval("SELECT subject_uri FROM knowledge_facts WHERE id=$1", fid) == A
    assert await conn.fetchval(
        "SELECT merged_into FROM entity_registry WHERE fuseki_uri=$1", B) == A

    result = await unmerge(conn, mid)

    # --- the assertions that matter ---
    assert await conn.fetchval(
        "SELECT subject_uri FROM knowledge_facts WHERE id=$1", fid) == B, \
        "the fact did not go back to the loser"
    assert await conn.fetchval(
        "SELECT merged_into FROM entity_registry WHERE fuseki_uri=$1", B) is None, \
        "the loser is still tombstoned"
    surv_aliases = await conn.fetchval(
        "SELECT aliases FROM entity_registry WHERE fuseki_uri=$1", A)
    assert "beta alias" not in surv_aliases, "the merge's added alias was not removed"
    assert "alpha alias" in surv_aliases, \
        "unmerge stripped an alias the survivor owned BEFORE the merge"
    assert await conn.fetchval(
        "SELECT reverted_at FROM entity_merge_log WHERE id=$1", mid) is not None
    assert result["restored"]["knowledge_facts.subject_uri"] == 1


async def test_unmerge_leaves_rows_moved_after_the_merge_alone(conn):
    """A row someone repointed elsewhere post-merge must not be yanked back."""
    await _mk(conn, A, "Unmerge Test Alpha", [])
    await _mk(conn, B, "Unmerge Test Beta", [])
    C = "orn:personal-koi.entity:concept-unmerge-test-gamma-cccccccccccc"
    await _mk(conn, C, "Unmerge Test Gamma", [])

    ep = await conn.fetchval(
        "INSERT INTO knowledge_episodes (name) VALUES ('unmerge-test-2') RETURNING id")
    fid = await conn.fetchval(
        "INSERT INTO knowledge_facts (episode_id, subject_uri, predicate, fact_text) "
        "VALUES ($1,$2,'TESTS','moved later') RETURNING id", ep, B)

    reversal = await capture_reversal(conn, loser=B, survivor=A)
    await conn.execute("UPDATE knowledge_facts SET subject_uri=$1 WHERE id=$2", A, fid)
    await conn.execute(
        "UPDATE entity_registry SET merged_into=$2 WHERE fuseki_uri=$1", B, A)
    mid = await conn.fetchval(
        "INSERT INTO entity_merge_log (survivor_uri, loser_uri, rewired, merged_by, reversal) "
        "VALUES ($1,$2,'{}'::jsonb,'test',$3::jsonb) RETURNING id",
        A, B, json.dumps(reversal))

    # Somebody moves it on to a third entity after the merge.
    await conn.execute("UPDATE knowledge_facts SET subject_uri=$1 WHERE id=$2", C, fid)

    result = await unmerge(conn, mid)
    assert await conn.fetchval(
        "SELECT subject_uri FROM knowledge_facts WHERE id=$1", fid) == C, \
        "unmerge clobbered a row that had been deliberately moved elsewhere"
    assert result["restored"]["knowledge_facts.subject_uri"] == 0


# --- adversarial URIs --------------------------------------------------------
# Live data contains 93 URIs with '&', 10 with '?', 4 with '%', and one with
# double quotes:
#   orn:personal-koi.entity:person-anita-&-sam-from-"whats-happening?"-newsletter-…
# Anything that concatenates a URI into SQL rather than parameterising it is
# fragile and injection-shaped. These assert the round trip survives the worst
# real example plus the characters that break naive quoting.

HOSTILE = [
    'orn:personal-koi.entity:person-anita-&-sam-from-"whats-happening?"-newsletter-4f416ae9d5c2',
    "orn:personal-koi.entity:concept-o'brien-and-sons-111111111111",   # apostrophe
    "orn:personal-koi.entity:concept-100%-coverage-222222222222",      # LIKE wildcard
    "orn:personal-koi.entity:concept-back\\slash-333333333333",        # backslash
    "orn:personal-koi.entity:concept-semi;colon--drop-444444444444",   # statement break
]


@pytest.mark.parametrize("hostile_uri", HOSTILE)
async def test_capture_and_unmerge_survive_hostile_uris(conn, hostile_uri):
    """Capture + restore must round-trip a URI full of SQL metacharacters."""
    survivor = "orn:personal-koi.entity:concept-hostile-survivor-555555555555"
    await _mk(conn, survivor, "Hostile Survivor", [])
    await _mk(conn, hostile_uri, "Hostile Loser", [])

    ep = await conn.fetchval(
        "INSERT INTO knowledge_episodes (name) VALUES ('hostile-uri-test') RETURNING id")
    fid = await conn.fetchval(
        "INSERT INTO knowledge_facts (episode_id, subject_uri, predicate, fact_text) "
        "VALUES ($1,$2,'TESTS','hostile') RETURNING id", ep, hostile_uri)

    reversal = await capture_reversal(conn, loser=hostile_uri, survivor=survivor)
    assert str(fid) in reversal["refs"]["knowledge_facts.subject_uri"], \
        "capture missed a row because the URI contains SQL metacharacters"

    await conn.execute(
        "UPDATE knowledge_facts SET subject_uri=$1 WHERE subject_uri=$2", survivor, hostile_uri)
    await conn.execute(
        "UPDATE entity_registry SET merged_into=$2 WHERE fuseki_uri=$1", hostile_uri, survivor)
    mid = await conn.fetchval(
        "INSERT INTO entity_merge_log (survivor_uri, loser_uri, rewired, merged_by, reversal) "
        "VALUES ($1,$2,'{}'::jsonb,'test',$3::jsonb) RETURNING id",
        survivor, hostile_uri, json.dumps(reversal))

    await unmerge(conn, mid)

    assert await conn.fetchval(
        "SELECT subject_uri FROM knowledge_facts WHERE id=$1", fid) == hostile_uri, \
        "unmerge failed to restore a row whose URI contains SQL metacharacters"
    assert await conn.fetchval(
        "SELECT merged_into FROM entity_registry WHERE fuseki_uri=$1", hostile_uri) is None


async def test_no_sql_injection_via_uri(conn):
    """A URI shaped like an injection payload must be inert data, not SQL."""
    payload = "orn:personal-koi.entity:concept-x'; DROP TABLE knowledge_facts; --666666666666"
    survivor = "orn:personal-koi.entity:concept-inject-survivor-777777777777"
    await _mk(conn, survivor, "Inject Survivor", [])
    await _mk(conn, payload, "Inject Loser", [])

    rev = await capture_reversal(conn, loser=payload, survivor=survivor)
    assert rev["loser"] == payload
    # The table it tried to drop is still there.
    assert await conn.fetchval("SELECT to_regclass('public.knowledge_facts') IS NOT NULL")


# --- persona guard -----------------------------------------------------------

async def test_persona_with_no_independent_principal_is_refused(conn):
    """A '(via X)' row whose principal has no other live row IS that principal.

    Operator finding after Batch A: "Corin Ashdown (via Hylo)" is the only
    Ashdown in the graph, so a cleanup folding every persona into its apparent
    principal would have merged a real person into a different Corin.
    """
    from api.merge_reversal import persona_merge_hazard

    uri = "orn:personal-koi.entity:person-lone-persona-via-hylo-999999999999"
    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text) "
        "VALUES ($1,'Lone Persona (via Hylo)','Person','lone persona (via hylo)') "
        "ON CONFLICT (fuseki_uri) DO NOTHING", uri)
    hazard = await persona_merge_hazard(conn, uri)
    assert hazard, "a persona with no independent principal must be refused"
    assert "only record" in hazard


async def test_persona_with_an_existing_principal_is_allowed(conn):
    from api.merge_reversal import persona_merge_hazard

    persona = "orn:personal-koi.entity:person-paired-persona-via-hylo-888888888888"
    principal = "orn:personal-koi.entity:person-paired-persona-777777777777"
    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text) "
        "VALUES ($1,'Paired Persona (via Hylo)','Person','paired persona (via hylo)') "
        "ON CONFLICT (fuseki_uri) DO NOTHING", persona)
    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text) "
        "VALUES ($1,'Paired Persona','Person','paired persona') "
        "ON CONFLICT (fuseki_uri) DO NOTHING", principal)
    assert await persona_merge_hazard(conn, persona) is None


async def test_non_persona_is_unaffected(conn):
    from api.merge_reversal import persona_merge_hazard

    uri = "orn:personal-koi.entity:organization-plain-org-666666666666"
    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, normalized_text) "
        "VALUES ($1,'Plain Org','Organization','plain org') "
        "ON CONFLICT (fuseki_uri) DO NOTHING", uri)
    assert await persona_merge_hazard(conn, uri) is None


# ---------------------------------------------------------------------------
# Retype round trips (2026-09-03)
#
# /entities/retype performed 142 merges with reversal IS NULL before this: it
# calls _do_merge, but capture_reversal lived only in the /merge ROUTE, so the
# sibling entry point to the same helper captured nothing.
#
# The assertion below is deliberately NOT "unmerge ran" or "the row does not
# bind". Both pass on the mint branch while an orphan is created: _do_retype
# INSERTs a new-typed row copying entity_text, normalized_text and both
# embeddings, and unmerge() restores the loser without deleting it -- leaving
# TWO live rows sharing a normalized_text, both embedded, both competing in
# exact, fuzzy and ANN resolution. So the terminal state is what is asserted:
# the live rows for that normalized_text must be EXACTLY what they were before.
# ---------------------------------------------------------------------------

def _do_retype_fn():
    """The real nested _do_retype the /retype route calls, via its closure."""
    from api.routers.admin_router import create_router

    for route in create_router(None).routes:
        if getattr(route, "path", "") == "/retype":
            fn = route.endpoint
            free = dict(zip(fn.__code__.co_freevars,
                            [c.cell_contents for c in (fn.__closure__ or ())]))
            assert "_do_retype" in free, f"closure has {sorted(free)}"
            return free["_do_retype"]
    raise AssertionError("/retype route not found — the wiring changed")


async def _live(conn, normalized):
    rows = await conn.fetch(
        "SELECT fuseki_uri, entity_type FROM entity_registry "
        "WHERE normalized_text = $1 AND merged_into IS NULL", normalized)
    return sorted((r["fuseki_uri"], r["entity_type"]) for r in rows)


async def _log(conn, new_uri, old_uri, rewired, reversal):
    return await conn.fetchval(
        "INSERT INTO entity_merge_log (survivor_uri, loser_uri, rewired, merged_by, "
        "reversal) VALUES ($1,$2,$3::jsonb,'test',$4::jsonb) RETURNING id",
        new_uri, old_uri, json.dumps(rewired), json.dumps(reversal, default=str))


async def _mk_typed(conn, uri, text, etype):
    await conn.execute(
        "INSERT INTO entity_registry (fuseki_uri, entity_text, entity_type, "
        "normalized_text) VALUES ($1,$2,$3,$4) ON CONFLICT (fuseki_uri) DO NOTHING",
        uri, text, etype, text.lower())


async def test_retype_mint_round_trip_leaves_no_orphan(conn):
    """The branch that motivated this: the survivor is CREATED, so undo must delete it."""
    from api.personal_ingest_api import generate_entity_uri

    do_retype = _do_retype_fn()
    text = "Zz Retype Mint Probe"
    norm = text.lower()
    old_uri = generate_entity_uri(text, "Concept")
    await _mk_typed(conn, old_uri, text, "Concept")

    before = await _live(conn, norm)
    assert len(before) == 1

    new_uri, _, rewired, reversal = await do_retype(
        conn, old_uri, "Concept", "Person", "test")
    assert new_uri != old_uri, "expected the mint branch, got an in-place retype"
    assert reversal["retype"]["survivor_minted"] is True
    assert len(await _live(conn, norm)) == 1, "retype itself should leave one live row"

    await unmerge(conn, await _log(conn, new_uri, old_uri, rewired, reversal))

    after = await _live(conn, norm)
    assert after == before, (
        f"retype/unmerge did not restore the graph: before={before} after={after}. "
        f"Two live rows here means the minted survivor was left behind."
    )
    assert not await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM entity_registry WHERE fuseki_uri=$1)", new_uri)


async def test_retype_live_twin_round_trip(conn):
    """Survivor pre-existed: undo must restore BOTH rows, and delete neither."""
    from api.personal_ingest_api import generate_entity_uri

    do_retype = _do_retype_fn()
    text = "Zz Retype Twin Probe"
    norm = text.lower()
    old_uri = generate_entity_uri(text, "Concept")
    twin_uri = generate_entity_uri(text, "Person")
    await _mk_typed(conn, old_uri, text, "Concept")
    await _mk_typed(conn, twin_uri, text, "Person")

    before = await _live(conn, norm)
    assert len(before) == 2

    new_uri, merged_into_existing, rewired, reversal = await do_retype(
        conn, old_uri, "Concept", "Person", "test")
    assert merged_into_existing and new_uri == twin_uri
    assert reversal["retype"]["survivor_minted"] is False
    assert len(await _live(conn, norm)) == 1

    await unmerge(conn, await _log(conn, new_uri, old_uri, rewired, reversal))

    after = await _live(conn, norm)
    assert after == before, (
        f"before={before} after={after}. The pre-existing twin must survive the undo — "
        f"deleting it would destroy a row the retype never created."
    )


async def test_retype_in_place_round_trip(conn):
    """No merge happens, so the merge-undo path would fail its tombstone assertion."""
    from api.personal_ingest_api import generate_entity_uri

    do_retype = _do_retype_fn()
    text = "Zz Retype Inplace Probe"
    norm = text.lower()
    # A row minted at the canonical Person URI but carrying a drifted type label
    # — the shape of the 156 namespace-prefixed rows in the registry.
    uri = generate_entity_uri(text, "Person")
    await _mk_typed(conn, uri, text, "schema:Person")

    before = await _live(conn, norm)

    new_uri, _, rewired, reversal = await do_retype(
        conn, uri, "schema:Person", "Person", "test")
    assert new_uri == uri, "expected the in-place branch"
    assert reversal["retype"]["branch"] == "in_place"
    assert (await _live(conn, norm))[0][1] == "Person"

    await unmerge(conn, await _log(conn, new_uri, uri, rewired, reversal))

    after = await _live(conn, norm)
    assert after == before, f"before={before} after={after}"


async def test_schema_1_reversals_are_still_accepted(conn):
    """Bumping REVERSAL_SCHEMA must not orphan the 57 captures written 2026-09-02."""
    from api.merge_reversal import REVERSAL_SCHEMA, _SUPPORTED_SCHEMAS

    assert REVERSAL_SCHEMA == 2
    assert 1 in _SUPPORTED_SCHEMAS, (
        "schema 1 dropped: every merge captured on 2026-09-02 would become "
        "un-undoable, which is the opposite of what widening the capture is for"
    )
