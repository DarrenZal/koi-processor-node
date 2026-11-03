"""Payload-endpoint identity for document ingestion — issues #62, #61, #53.

ISOLATION. Every DB test here runs on ONE asyncpg connection wrapped in a
transaction that is rolled back at teardown, and every HTTP test drives the router
IN PROCESS via ASGITransport. Nothing in this file talks to localhost:8351.

That is not incidental style. tests/conftest.py documents the failure it prevents:
a test that posts over HTTP to the live backend writes through a separate uvicorn
process holding its own pool against the LIVE database, and no environment
variable can redirect that — the ingests landed in personal_koi while the teardown
DELETE went to personal_koi_test, and 225 orphaned rows accumulated over 30 hours.
An in-process app shares this connection, so the rollback is the guarantee rather
than a cleanup that might not have run.

Requires KOI_TEST_DATABASE_URL to carry migrations 124 and 125:
    psql "$KOI_TEST_DATABASE_URL" -f migrations/124_entity_normalization_compat.sql
    psql "$KOI_TEST_DATABASE_URL" -f migrations/125_extraction_provenance_and_quality.sql
"""

from __future__ import annotations

import uuid

import asyncpg
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from api import ingest_identity as ident
from api.extraction_quality import assess_extraction
from api.resolution_primitives import (
    normalize_alias,
    normalize_entity_text,
)
from tests import koi_test_targets as targets

EMBED_DIM = 3072
SVC_TOKEN = "test-token-g"
SVC_AUTH = {"Authorization": f"Bearer {SVC_TOKEN}"}

# The four collapses issue #62 reports, with the Jaro-Winkler scores it recorded.
REPORTED_COLLAPSES = [
    ("Anthropic Claude 3 Opus", "Anthropic Claude 3 Sonnet", 0.9411),
    ("Buehler 2024 fitted exponential degree model",
     "Buehler 2024 fitted power-law degree model", 0.9168),
    ("Buehler 2024 adversarial-X-LoRA generated graph",
     "Buehler 2024 adversarial-X-LoRA augmented graph", 0.9608),
    ("Buehler 2024 global-graph modularity score",
     "Buehler 2024 global-graph community structure", 0.9393),
]


@pytest.fixture
def anyio_backend():
    return "asyncio"


# ═══════════════════════════════════════════════════════════════════════════
# 0. The defect is live. This is the positive control for the whole change.
# ═══════════════════════════════════════════════════════════════════════════

class TestTheDefectIsReal:
    """If these stop failing-by-collapsing, the premise of #62 has changed.

    Without this, every test below could pass against a system that never had the
    bug, and nothing would say so.
    """

    def test_the_four_reported_collapses_still_reproduce(self):
        """Each pair still clears every guard on the live fuzzy path.

        They survive because `passes_distinctive_token_check` rejects DISJOINT
        distinctive-token sets, and these pairs are not disjoint: they share most
        tokens and differ in exactly the one that distinguishes them
        (opus/sonnet, power-law/exponential). That is why raising a threshold is
        not the fix and pinning is.
        """
        from api.entity_schema import get_schema_for_type
        from api.resolution_primitives import (
            jaro_winkler_similarity,
            passes_distinctive_token_check,
            passes_token_overlap_strict,
        )

        threshold = get_schema_for_type("Concept").similarity_threshold
        for requested, sibling, reported_jw in REPORTED_COLLAPSES:
            a, b = normalize_entity_text(requested), normalize_entity_text(sibling)
            jw = jaro_winkler_similarity(a, b)
            assert round(jw, 4) == reported_jw, (
                f"{requested!r} vs {sibling!r}: JW is now {jw:.4f}, issue #62 recorded "
                f"{reported_jw}. The scoring changed — re-derive the issue's table.")
            assert jw >= threshold, "no longer reaches the fuzzy tier"
            assert passes_token_overlap_strict(a, b, "Concept"), "strict guard now rejects it"
            assert passes_distinctive_token_check(a, b), "distinctive-token guard now rejects it"

    def test_normalizer_drift_still_hides_a_legacy_row(self):
        """#61: the current normalizer cannot find a label an older one stored.

        `GPT-4` is the repaired instance. The assertion is about the FUNCTION, so
        it keeps holding after that row is merged — the class, not the instance.
        """
        assert normalize_entity_text("GPT-4") == "gpt 4"
        assert normalize_entity_text("GPT-4") != "gpt-4", (
            "the legacy stored form and the current form have converged; #61's "
            "premise no longer holds")


# ═══════════════════════════════════════════════════════════════════════════
# 1. Endpoint collection — pure, no DB
# ═══════════════════════════════════════════════════════════════════════════

def _payload(facts, entities=None, type_map=None):
    return {"entities": entities or [], "facts": facts, "type_map": type_map or {}}


class TestCollectEndpoints:

    def test_collects_fact_endpoints_including_those_absent_from_entities(self):
        """A fact endpoint absent from entities[] must still be collected.

        Those are exactly the endpoints that get minted with a DEFAULTED type
        (the reason EpisodeCreateResponse.entities_typed_by_default exists), so
        collecting from facts alone would leave the weakest endpoints unpinned.
        """
        eps = ident.collect_endpoints(
            _payload(
                facts=[{"subject": "Alpha", "object": "Ghost", "predicate": "P",
                        "fact_text": "t"}],
                entities=[{"name": "Alpha", "type": "Concept"}],
            ),
            normalize=normalize_entity_text,
        )
        by_name = {e.name: e for e in eps}
        assert set(by_name) == {"Alpha", "Ghost"}
        assert by_name["Alpha"].type_defaulted is False
        assert by_name["Ghost"].type_defaulted is True
        assert by_name["Ghost"].roles == frozenset({"object"})

    def test_reversed_fact_order_yields_an_identical_endpoint_set(self):
        """#62 acceptance: reversing fact order produces the same endpoint map."""
        facts = [
            {"subject": f"S{i}", "object": f"O{i}", "predicate": "P", "fact_text": f"f{i}"}
            for i in range(25)
        ]
        fwd = ident.collect_endpoints(_payload(facts), normalize=normalize_entity_text)
        rev = ident.collect_endpoints(_payload(list(reversed(facts))),
                                      normalize=normalize_entity_text)
        assert [e.key for e in fwd] == [e.key for e in rev]

    def test_rejects_one_label_claimed_under_two_types(self):
        """Two entities[] entries whose labels normalize the same but disagree on type.

        Guessing which type is meant would be inventing an identity decision, and
        the URI hashes the type, so the guess would be permanent.
        """
        with pytest.raises(ident.IdentityError) as exc:
            ident.collect_endpoints(
                _payload(
                    facts=[{"subject": "Janus", "predicate": "P", "object": None,
                            "object_literal": "x", "fact_text": "t"}],
                    entities=[{"name": "Janus", "type": "Person"},
                              {"name": "janus", "type": "Organization"}],
                ),
                normalize=normalize_entity_text,
            )
        assert "multiple entity types" in str(exc.value)

    def test_normalization_collapses_spelling_variants_into_one_endpoint(self):
        eps = ident.collect_endpoints(
            _payload(facts=[
                {"subject": "Vault-Sync", "predicate": "P", "object": "vault_sync",
                 "fact_text": "t"},
            ]),
            normalize=normalize_entity_text,
        )
        assert len(eps) == 1, f"expected one endpoint, got {[e.key for e in eps]}"
        assert eps[0].normalized == "vault sync"


# ═══════════════════════════════════════════════════════════════════════════
# 2. Preflight against a real registry — scratch DB, rolled back
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
async def conn():
    c = await asyncpg.connect(targets.database_url())
    db = await c.fetchval("SELECT current_database()")
    assert db != "personal_koi", (
        f"refusing to run: connected to {db!r}. These tests write entity rows; the "
        f"live graph is not an acceptable target.")
    tx = c.transaction()
    await tx.start()
    yield c
    await tx.rollback()
    await c.close()


async def _seed_entity(conn, *, uri, text, etype, normalized=None, aliases=None,
                       merged_into=None):
    """Insert a registry row, optionally with a LEGACY normalized_text.

    `normalized` defaults to the CURRENT normalization; passing a different value
    is how #61's drift is reproduced.
    """
    await conn.execute(
        """INSERT INTO entity_registry
             (fuseki_uri, entity_text, entity_type, normalized_text, aliases,
              merged_into, source)
           VALUES ($1,$2,$3,$4,$5,$6,'pytest')""",
        uri, text, etype,
        normalized if normalized is not None else normalize_entity_text(text),
        aliases, merged_into,
    )


def _tag():
    return uuid.uuid4().hex[:10]


async def _preflight(conn, names_types, **kw):
    eps = [
        ident.Endpoint(name=n, entity_type=t, normalized=normalize_entity_text(n))
        for n, t in names_types
    ]
    report = await ident.preflight_endpoints(
        conn, eps, normalize=normalize_entity_text,
        normalize_alias_fn=normalize_alias, **kw)
    return eps, report


@pytest.mark.anyio
class TestPreflight:

    async def test_finds_a_row_whose_stored_normalization_is_legacy(self, conn):
        """#61 AC1. The row is reachable ONLY by recomputing the stored label.

        The paired control below shows the stock exact lookup missing the same
        row, which is what makes this assertion mean something.
        """
        t = _tag()
        uri = f"orn:personal-koi.entity:concept-gpt4-{t}"
        await _seed_entity(conn, uri=uri, text=f"GPT-4x{t}", etype="Concept",
                           normalized=f"gpt-4x{t}")          # LEGACY spelling

        _eps, report = await _preflight(conn, [(f"GPT-4x{t}", "Concept")])
        f = report.findings[0]
        assert f.state == ident.STATE_LIVE_EXACT, (
            f"expected the legacy row to be found, got {f.state} — this is #61")
        assert f.matched_via == "current_norm"
        assert f.live_uris == [uri]

    async def test_control_the_stock_exact_lookup_misses_that_row(self, conn):
        """The defect, stated as a query. Without this the test above is unmoored."""
        t = _tag()
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:concept-gpt4-{t}",
                           text=f"GPT-4x{t}", etype="Concept", normalized=f"gpt-4x{t}")
        hit = await conn.fetchval(
            "SELECT fuseki_uri FROM entity_registry "
            "WHERE normalized_text=$1 AND entity_type='Concept' AND merged_into IS NULL",
            normalize_entity_text(f"GPT-4x{t}"))
        assert hit is None, (
            "the stock exact lookup now finds the legacy row; #61 may be fixed "
            "elsewhere and this suite's premise needs re-deriving")

    async def test_missing_endpoint_is_missing_not_ambiguous(self, conn):
        _eps, report = await _preflight(conn, [(f"Nothing Like This {_tag()}", "Concept")])
        assert report.findings[0].state == ident.STATE_MISSING

    async def test_two_live_rows_with_one_current_label_block(self, conn):
        """#61 AC5 / #62 requirement 3: a graph-global duplicate set blocks."""
        t = _tag()
        name = f"Duplicated Thing {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:concept-a-{t}",
                           text=name, etype="Concept")
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:concept-b-{t}",
                           text=name.replace(" ", "-"), etype="Concept",
                           normalized=name.replace(" ", "-").lower())  # legacy form
        _eps, report = await _preflight(conn, [(name, "Concept")])
        f = report.findings[0]
        assert f.state == ident.STATE_GLOBAL_DUPLICATE, f.as_evidence()
        with pytest.raises(ident.IdentityError):
            ident.require_no_blockers(report)

    async def test_a_tombstone_with_no_live_row_blocks(self, conn):
        """The label matches ONLY a merged-away row: identity would depend on
        following a merge chain that can change under the payload."""
        t = _tag()
        name = f"Merged Away {t}"
        survivor = f"orn:personal-koi.entity:concept-live-{t}"
        await _seed_entity(conn, uri=survivor, text=f"Survivor {t}", etype="Concept")
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:concept-dead-{t}",
                           text=name, etype="Concept", merged_into=survivor)
        _eps, report = await _preflight(conn, [(name, "Concept")])
        assert report.findings[0].state == ident.STATE_TOMBSTONE_RISK

    async def test_a_tombstone_ALONGSIDE_a_live_row_does_not_block(self, conn):
        """REGRESSION, found by running the real payload against the real graph.

        The single blocker over Buehler's 115 endpoints was `GPT-4` — issue #61's
        own repaired entity, whose legacy row is tombstoned *because the repair
        worked*. Blocking there makes a correctly-merged graph permanently
        un-ingestable, i.e. punishes the repair.

        The tombstone is unreachable on the pinned path anyway: the binding names
        the live URI, and `_bind_pinned_uri` refuses a merged-away one outright.
        """
        t = _tag()
        name = f"Repaired Label {t}"
        live = f"orn:personal-koi.entity:concept-live-{t}"
        await _seed_entity(conn, uri=live, text=name, etype="Concept")
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:concept-legacy-{t}",
                           text=name, etype="Concept",
                           normalized=name.replace(" ", "-").lower(),
                           merged_into=live)
        _eps, report = await _preflight(conn, [(name, "Concept")])
        f = report.findings[0]
        assert f.state == ident.STATE_LIVE_EXACT, f.as_evidence()
        assert f.live_uris == [live]
        assert f.tombstoned_uris, "the tombstone must still be recorded as evidence"
        ident.require_no_blockers(report)  # must not raise

    async def test_alias_only_blocks_but_an_audited_decision_clears_it(self, conn):
        """#62 requirement 6 — the ONLY sanctioned override, and it is recorded."""
        t = _tag()
        uri = f"orn:personal-koi.entity:concept-canon-{t}"
        alias = f"Alias Spelling {t}"
        await _seed_entity(conn, uri=uri, text=f"Canonical Spelling {t}",
                           etype="Concept", aliases=[normalize_alias(alias)])

        _eps, blocked = await _preflight(conn, [(alias, "Concept")])
        assert blocked.findings[0].state == ident.STATE_ALIAS_ONLY
        with pytest.raises(ident.IdentityError):
            ident.require_no_blockers(blocked)

        _eps, cleared = await _preflight(conn, [(alias, "Concept")],
                                         alias_decisions={alias: uri})
        assert cleared.findings[0].state == ident.STATE_LIVE_EXACT
        assert cleared.findings[0].matched_via == "audited_alias_decision"
        ident.require_no_blockers(cleared)

    async def test_a_wrong_alias_decision_does_not_clear_the_block(self, conn):
        """A decision naming a URI that is not actually a candidate must not win."""
        t = _tag()
        uri = f"orn:personal-koi.entity:concept-canon-{t}"
        alias = f"Alias Spelling {t}"
        await _seed_entity(conn, uri=uri, text=f"Canonical Spelling {t}",
                           etype="Concept", aliases=[normalize_alias(alias)])
        _eps, report = await _preflight(
            conn, [(alias, "Concept")],
            alias_decisions={alias: f"orn:personal-koi.entity:concept-unrelated-{t}"})
        assert report.findings[0].state == ident.STATE_ALIAS_ONLY

    async def test_same_label_different_type_BLOCKS(self, conn):
        """SUPERSEDES an earlier test that asserted this was merely advisory.

        That assertion was wrong, and only real data showed it: an advisory
        cross-type match leaves the endpoint MISSING, and preregistration then
        mints the cross-type twin. See TestCrossTypeConflictBlocks for the
        DWeb Berlin case that forced the change.
        """
        t = _tag()
        name = f"Polysemous {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:org-{t}",
                           text=name, etype="Organization")
        _eps, report = await _preflight(conn, [(name, "Concept")])
        f = report.findings[0]
        assert f.state == ident.STATE_CROSS_TYPE_CONFLICT
        assert f.cross_type_uris, "the cross-type row must be surfaced as evidence"


# ═══════════════════════════════════════════════════════════════════════════
# 3. Freeze — the bijection assertion
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.anyio
class TestFreeze:

    async def test_distinct_endpoints_get_distinct_uris(self, conn):
        t = _tag()
        a, b = f"Claude 3 Opus {t}", f"Claude 3 Sonnet {t}"
        ua = f"orn:personal-koi.entity:concept-opus-{t}"
        ub = f"orn:personal-koi.entity:concept-sonnet-{t}"
        await _seed_entity(conn, uri=ua, text=a, etype="Concept")
        await _seed_entity(conn, uri=ub, text=b, etype="Concept")
        eps, _ = await _preflight(conn, [(a, "Concept"), (b, "Concept")])
        frozen = await ident.freeze_endpoint_map(conn, eps, normalize=normalize_entity_text)
        assert frozen.by_name == {a: ua, b: ub}
        assert frozen.distinct_uris == 2
        assert frozen.type_for(a) == "Concept"

    async def test_two_endpoints_reaching_one_row_via_drift_raise(self, conn):
        """The bijection assertion catches a collapse the DUAL READ itself enables.

        This is the case worth guarding, and it is not hypothetical: the #61
        compatibility read matches a row by EITHER its stored normalized_text or
        the current normalization of its canonical label. A drifted row therefore
        answers to two different keys at once. Here endpoint A matches it via the
        stored value and endpoint B via the recomputed one — two distinct typed
        payload endpoints, one identity.

        Making the exact lookup more forgiving is what creates this, so the
        bijection check has to be the thing that catches it. Without the check,
        widening the read would trade a false SPLIT (#61) for a false MERGE (#62)
        and call it a fix.
        """
        t = _tag()
        uri = f"orn:personal-koi.entity:concept-drifted-{t}"
        # entity_text normalizes to "current spelling <t>"; the STORED value is a
        # different string entirely — the shape 1,268 live rows are in today.
        await _seed_entity(conn, uri=uri, text=f"Current Spelling {t}", etype="Concept",
                           normalized=f"legacy spelling {t}")

        eps = [
            ident.Endpoint(name=f"Current Spelling {t}", entity_type="Concept",
                           normalized=f"current spelling {t}"),
            ident.Endpoint(name=f"Legacy Spelling {t}", entity_type="Concept",
                           normalized=f"legacy spelling {t}"),
        ]
        with pytest.raises(ident.IdentityError) as exc:
            await ident.freeze_endpoint_map(conn, eps, normalize=normalize_entity_text)
        assert "same identity" in str(exc.value)
        assert exc.value.blockers[0]["state"] == "endpoint_collapse"
        assert exc.value.blockers[0]["uri"] == uri

    async def test_an_audited_decision_may_point_two_labels_at_one_uri(self, conn):
        """#62 requirement 6's escape hatch, and the ONLY one.

        An operator who has audited the case may map variants onto one identity.
        The contrast with the test above is the whole point: the same outcome is a
        blocker when nobody decided it and permitted when somebody did.
        """
        t = _tag()
        shared = f"orn:personal-koi.entity:concept-shared-{t}"
        await _seed_entity(conn, uri=shared, text=f"Shared {t}", etype="Concept")
        eps = [
            ident.Endpoint(name=f"First {t}", entity_type="Concept",
                           normalized=normalize_entity_text(f"First {t}")),
            ident.Endpoint(name=f"Second {t}", entity_type="Concept",
                           normalized=normalize_entity_text(f"Second {t}")),
        ]
        frozen = await ident.freeze_endpoint_map(
            conn, eps, normalize=normalize_entity_text,
            alias_decisions={f"First {t}": shared, f"Second {t}": shared})
        assert frozen.by_name == {f"First {t}": shared, f"Second {t}": shared}
        assert all(e["bound_via"] == "audited_alias_decision" for e in frozen.evidence)

    async def test_a_registration_that_returned_a_different_uri_raises(self, conn):
        t = _tag()
        name = f"Drifted {t}"
        real = f"orn:personal-koi.entity:concept-real-{t}"
        await _seed_entity(conn, uri=real, text=name, etype="Concept")
        eps, _ = await _preflight(conn, [(name, "Concept")])
        with pytest.raises(ident.IdentityError) as exc:
            await ident.freeze_endpoint_map(
                conn, eps, normalize=normalize_entity_text,
                expected_uris={name: f"orn:personal-koi.entity:concept-other-{t}"})
        assert "registration_disagreement" in str(exc.value.blockers[0]["state"])


# ═══════════════════════════════════════════════════════════════════════════
# 4. The endpoint honours pins — in-process ASGI, no live server
# ═══════════════════════════════════════════════════════════════════════════

class _Pool:
    """One connection, quacking like asyncpg.Pool, inside the rolled-back txn."""

    def __init__(self, conn):
        self._conn = conn

    class _CM:
        def __init__(self, c):
            self._c = c

        async def __aenter__(self):
            return self._c

        async def __aexit__(self, *exc):
            return False

    def acquire(self):
        return self._CM(self._conn)


async def _fake_embed(text, **kwargs):
    seed = hash(text)
    return [float(((seed + i) % 97) + 1) / 97.0 for i in range(EMBED_DIM)]


@pytest.fixture
async def api(conn, monkeypatch):
    from api.routers.knowledge_router import create_router

    monkeypatch.setenv("KOI_CLAIMS_SERVICE_TOKEN", SVC_TOKEN)
    app = FastAPI()
    app.include_router(
        create_router(_Pool(conn), generate_document_embedding=_fake_embed),
        prefix="/knowledge")
    async with AsyncClient(transport=ASGITransport(app=app),
                           base_url="http://test", headers=SVC_AUTH) as client:
        yield client, conn


def _episode(facts, *, tag, create_entities=True):
    return {
        "name": f"Episode {tag}",
        "source_description": "pytest",
        "source_document": f"doc-{tag}",
        "group_id": f"test-{tag}",
        "facts": facts,
        "create_entities": create_entities,
    }


@pytest.mark.anyio
class TestPinnedWrites:

    async def test_unpinned_siblings_collapse_control(self, api):
        """CONTROL: without pinning, the two reported siblings become one entity.

        This is the behaviour #62 describes, exercised through the real endpoint.
        If it ever stops collapsing, the pinning tests below are proving nothing
        and this file should say so loudly rather than stay green.
        """
        client, conn = api
        t = _tag()
        a, b = f"Anthropic Claude 3 Opus {t}", f"Anthropic Claude 3 Sonnet {t}"
        r = await client.post("/knowledge/episodes", json=_episode([
            {"subject": a, "subject_type": "Concept", "predicate": "EVALUATED_ON",
             "object_literal": "bench", "fact_text": f"{a} was evaluated."},
            {"subject": b, "subject_type": "Concept", "predicate": "EVALUATED_ON",
             "object_literal": "bench", "fact_text": f"{b} was evaluated."},
        ], tag=t))
        assert r.status_code == 201, r.text
        uris = [row["subject_uri"] for row in await conn.fetch(
            "SELECT subject_uri FROM knowledge_facts WHERE episode_id=$1 ORDER BY created_at",
            uuid.UUID(r.json()["episode_id"]))]
        assert len(set(uris)) == 1, (
            "the siblings no longer collapse unpinned — #62's mechanism has changed "
            "and the pinning tests below need re-grounding")
        assert r.json()["endpoints_pinned"] == 0

    @pytest.mark.parametrize("pair_index", range(len(REPORTED_COLLAPSES)))
    async def test_pinned_siblings_stay_distinct(self, api, pair_index):
        """#62 acceptance: each reported pair keeps two identities when pinned."""
        client, conn = api
        t = _tag()
        left, right, _jw = REPORTED_COLLAPSES[pair_index]
        a, b = f"{left} {t}", f"{right} {t}"
        ua = f"orn:personal-koi.entity:concept-a-{t}"
        ub = f"orn:personal-koi.entity:concept-b-{t}"
        await _seed_entity(conn, uri=ua, text=a, etype="Concept")
        await _seed_entity(conn, uri=ub, text=b, etype="Concept")

        r = await client.post("/knowledge/episodes", json=_episode([
            {"subject": a, "subject_type": "Concept", "subject_uri": ua,
             "predicate": "EVALUATED_ON", "object_literal": "bench",
             "fact_text": f"{a} was evaluated."},
            {"subject": b, "subject_type": "Concept", "subject_uri": ub,
             "predicate": "EVALUATED_ON", "object_literal": "bench",
             "fact_text": f"{b} was evaluated."},
        ], tag=t, create_entities=False))
        assert r.status_code == 201, r.text
        assert r.json()["endpoints_pinned"] == 2
        uris = {row["subject_uri"] for row in await conn.fetch(
            "SELECT subject_uri FROM knowledge_facts WHERE episode_id=$1",
            uuid.UUID(r.json()["episode_id"]))}
        assert uris == {ua, ub}

    async def test_reversed_fact_order_produces_the_same_graph(self, api):
        """#62 acceptance: reversing fact order yields the same fact graph."""
        client, conn = api
        results = []
        for reverse in (False, True):
            t = _tag()
            uris = {}
            facts = []
            for i in range(6):
                name = f"Endpoint {i} {t}"
                uri = f"orn:personal-koi.entity:concept-{i}-{t}"
                await _seed_entity(conn, uri=uri, text=name, etype="Concept")
                uris[name] = uri
                facts.append({
                    "subject": name, "subject_type": "Concept", "subject_uri": uri,
                    "predicate": "HAS_INDEX", "object_literal": str(i),
                    "fact_text": f"{name} has index {i}.",
                })
            r = await client.post("/knowledge/episodes", json=_episode(
                list(reversed(facts)) if reverse else facts, tag=t,
                create_entities=False))
            assert r.status_code == 201, r.text
            rows = await conn.fetch(
                "SELECT subject_uri, predicate, object_literal FROM knowledge_facts "
                "WHERE episode_id=$1", uuid.UUID(r.json()["episode_id"]))
            # Compare the SHAPE, with the per-run tag stripped, so the two runs are
            # comparable without sharing rows.
            results.append(sorted(
                (row["subject_uri"].replace(t, "TAG"), row["predicate"],
                 row["object_literal"]) for row in rows))
        assert results[0] == results[1]

    async def test_more_than_one_batch_keeps_every_endpoint_distinct(self, api):
        """#62 acceptance: 'tests cover imports larger than one fact batch'.

        DOC_EPISODE_BATCH_SIZE defaults to 100, so the Buehler payload's 213 facts
        are written as three sequential same-episode requests. This exercises >100
        endpoints in one episode and asserts the cardinality survives.
        """
        client, conn = api
        t = _tag()
        n = 120
        facts = []
        for i in range(n):
            name = f"Batch Endpoint {i} {t}"
            uri = f"orn:personal-koi.entity:concept-b{i}-{t}"
            await _seed_entity(conn, uri=uri, text=name, etype="Concept")
            facts.append({
                "subject": name, "subject_type": "Concept", "subject_uri": uri,
                "predicate": "HAS_INDEX", "object_literal": str(i),
                "fact_text": f"{name} has index {i}.",
            })
        r = await client.post("/knowledge/episodes",
                              json=_episode(facts, tag=t, create_entities=False))
        assert r.status_code == 201, r.text
        assert r.json()["endpoints_pinned"] == n
        distinct = await conn.fetchval(
            "SELECT count(DISTINCT subject_uri) FROM knowledge_facts WHERE episode_id=$1",
            uuid.UUID(r.json()["episode_id"]))
        assert distinct == n

    async def test_rerun_creates_no_additional_entities_or_facts(self, api):
        """#62 acceptance: re-running the same import is a no-op."""
        client, conn = api
        t = _tag()
        name = f"Idempotent Endpoint {t}"
        uri = f"orn:personal-koi.entity:concept-idem-{t}"
        await _seed_entity(conn, uri=uri, text=name, etype="Concept")
        body = _episode([{
            "subject": name, "subject_type": "Concept", "subject_uri": uri,
            "predicate": "IS_STABLE", "object_literal": "yes",
            "fact_text": f"{name} is stable.",
        }], tag=t, create_entities=False)

        first = await client.post("/knowledge/episodes", json=body)
        assert first.status_code == 201, first.text
        ents_before = await conn.fetchval("SELECT count(*) FROM entity_registry")
        second = await client.post("/knowledge/episodes", json=body)
        assert second.status_code == 201, second.text

        assert second.json()["episode_id"] == first.json()["episode_id"], \
            "episode reuse by (source_document, group_id) did not hold"
        assert await conn.fetchval("SELECT count(*) FROM entity_registry") == ents_before
        assert await conn.fetchval(
            "SELECT count(*) FROM knowledge_facts WHERE episode_id=$1 AND valid_to IS NULL",
            uuid.UUID(first.json()["episode_id"])) == 1

    async def test_an_unknown_pinned_uri_is_422_and_writes_nothing(self, api):
        """A pin that cannot be honoured must NEVER fall back to name resolution."""
        client, conn = api
        t = _tag()
        before = await conn.fetchval("SELECT count(*) FROM knowledge_facts")
        r = await client.post("/knowledge/episodes", json=_episode([{
            "subject": f"Ghost {t}", "subject_type": "Concept",
            "subject_uri": f"orn:personal-koi.entity:concept-does-not-exist-{t}",
            "predicate": "P", "object_literal": "x", "fact_text": "t",
        }], tag=t, create_entities=False))
        assert r.status_code == 422, r.text
        assert r.json()["detail"]["error"] == "pinned_entity_not_found"
        assert await conn.fetchval("SELECT count(*) FROM knowledge_facts") == before, \
            "the failed request left facts behind — the transaction did not roll back"

    async def test_a_tombstoned_pin_is_422_and_is_not_silently_redirected(self, api):
        """The pin is an assertion of identity, not a hint to be followed."""
        client, conn = api
        t = _tag()
        survivor = f"orn:personal-koi.entity:concept-live-{t}"
        dead = f"orn:personal-koi.entity:concept-dead-{t}"
        await _seed_entity(conn, uri=survivor, text=f"Survivor {t}", etype="Concept")
        await _seed_entity(conn, uri=dead, text=f"Dead {t}", etype="Concept",
                           merged_into=survivor)
        r = await client.post("/knowledge/episodes", json=_episode([{
            "subject": f"Dead {t}", "subject_type": "Concept", "subject_uri": dead,
            "predicate": "P", "object_literal": "x", "fact_text": "t",
        }], tag=t, create_entities=False))
        assert r.status_code == 422, r.text
        assert r.json()["detail"]["error"] == "pinned_entity_merged_away"
        assert r.json()["detail"]["merged_into"] == survivor

    async def test_a_type_mismatched_pin_is_422(self, api):
        client, conn = api
        t = _tag()
        uri = f"orn:personal-koi.entity:org-{t}"
        await _seed_entity(conn, uri=uri, text=f"Thing {t}", etype="Organization")
        r = await client.post("/knowledge/episodes", json=_episode([{
            "subject": f"Thing {t}", "subject_type": "Concept", "subject_uri": uri,
            "predicate": "P", "object_literal": "x", "fact_text": "t",
        }], tag=t, create_entities=False))
        assert r.status_code == 422, r.text
        assert r.json()["detail"]["error"] == "pinned_entity_type_mismatch"

    async def test_object_endpoints_are_pinned_too(self, api):
        client, conn = api
        t = _tag()
        us = f"orn:personal-koi.entity:concept-s-{t}"
        uo = f"orn:personal-koi.entity:concept-o-{t}"
        await _seed_entity(conn, uri=us, text=f"Subj {t}", etype="Concept")
        await _seed_entity(conn, uri=uo, text=f"Obj {t}", etype="Concept")
        r = await client.post("/knowledge/episodes", json=_episode([{
            "subject": f"Subj {t}", "subject_type": "Concept", "subject_uri": us,
            "object": f"Obj {t}", "object_type": "Concept", "object_uri": uo,
            "predicate": "RELATES_TO", "fact_text": "t",
        }], tag=t, create_entities=False))
        assert r.status_code == 201, r.text
        assert r.json()["endpoints_pinned"] == 2
        row = await conn.fetchrow(
            "SELECT subject_uri, object_uri FROM knowledge_facts WHERE episode_id=$1",
            uuid.UUID(r.json()["episode_id"]))
        assert (row["subject_uri"], row["object_uri"]) == (us, uo)


# ═══════════════════════════════════════════════════════════════════════════
# 5. Post-write verification
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.anyio
class TestVerifyPersistedGraph:

    async def test_an_endpoint_outside_the_frozen_map_is_caught(self, api):
        client, conn = api
        t = _tag()
        pinned = f"orn:personal-koi.entity:concept-pinned-{t}"
        stray = f"orn:personal-koi.entity:concept-stray-{t}"
        await _seed_entity(conn, uri=pinned, text=f"Pinned {t}", etype="Concept")
        await _seed_entity(conn, uri=stray, text=f"Stray {t}", etype="Concept")
        r = await client.post("/knowledge/episodes", json=_episode([
            {"subject": f"Pinned {t}", "subject_type": "Concept", "subject_uri": pinned,
             "predicate": "P", "object_literal": "a", "fact_text": "one"},
            {"subject": f"Stray {t}", "subject_type": "Concept", "subject_uri": stray,
             "predicate": "P", "object_literal": "b", "fact_text": "two"},
        ], tag=t, create_entities=False))
        assert r.status_code == 201

        # A map that knows about only ONE of the two persisted endpoints.
        frozen = ident.FrozenMap(by_name={f"Pinned {t}": pinned}, by_key={},
                                 by_name_type={f"Pinned {t}": "Concept"}, evidence=[])
        report = await ident.verify_persisted_graph(
            conn, uuid.UUID(r.json()["episode_id"]), frozen)
        assert not report.ok
        assert report.offending[0]["persisted_uri"] == stray
        with pytest.raises(ident.IdentityError):
            ident.require_binding_verified(report)

    async def test_a_fully_pinned_episode_verifies_clean(self, api):
        client, conn = api
        t = _tag()
        uri = f"orn:personal-koi.entity:concept-ok-{t}"
        name = f"Ok {t}"
        await _seed_entity(conn, uri=uri, text=name, etype="Concept")
        r = await client.post("/knowledge/episodes", json=_episode([{
            "subject": name, "subject_type": "Concept", "subject_uri": uri,
            "predicate": "P", "object_literal": "a", "fact_text": "one",
        }], tag=t, create_entities=False))
        assert r.status_code == 201
        frozen = ident.FrozenMap(by_name={name: uri}, by_key={},
                                 by_name_type={name: "Concept"}, evidence=[])
        report = await ident.verify_persisted_graph(
            conn, uuid.UUID(r.json()["episode_id"]), frozen)
        assert report.ok
        assert report.checked_facts == 1
        ident.require_binding_verified(report)  # must not raise


# ═══════════════════════════════════════════════════════════════════════════
# 6. Semantic quality is a SEPARATE verdict — issue #64
# ═══════════════════════════════════════════════════════════════════════════

class _W:
    def __init__(self, index, chunk_indices):
        self.index = index
        self.chunk_indices = chunk_indices
        self.chunk_index_base = min(chunk_indices)


def _doc(n_windows=5):
    chunks = {i: f"chunk {i} alpha{i} beta{i} gamma{i} content words here" for i in range(n_windows)}
    windows = [_W(i, [i]) for i in range(n_windows)]
    return chunks, windows


class TestSemanticQuality:

    def test_a_thin_extraction_passes_structurally_but_fails_semantically(self):
        """#64 acceptance, exactly as worded.

        Structurally this run is complete: it produced facts, they have real
        endpoints, there are no duplicates. Every floor in the completion gate's
        catalog is a `>= 1`, and this clears them. It is still a bad extraction —
        it read one window of five.
        """
        chunks, windows = _doc(5)
        thin = {"facts": [{
            "subject": "alpha0", "object": "beta0", "predicate": "P",
            "fact_text": "alpha0 P beta0", "chunk_range": [0, 0],
        }]}

        # Structural floors the real gate applies, evaluated on this run.
        assert len(thin["facts"]) >= 1          # facts_available
        assert all(f.get("chunk_range") for f in thin["facts"])

        report = assess_extraction(merged=thin, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        assert report.status == "fail"
        assert "window_coverage" in report.as_dict()["failed"]

    def test_padding_with_invented_facts_makes_the_verdict_worse(self):
        """#64 requirement 5: thresholds must not reward hallucinated volume."""
        chunks, windows = _doc(3)
        real = [{"subject": f"alpha{i}", "object": f"beta{i}", "predicate": f"P{i}",
                 "fact_text": "t", "chunk_range": [i, i]} for i in range(3)]
        honest = assess_extraction(merged={"facts": real}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        padded_facts = real + [
            {"subject": f"invented{i}", "object": f"fabricated{i}", "predicate": "P0",
             "fact_text": "t", "chunk_range": [0, 0]} for i in range(20)]
        padded = assess_extraction(merged={"facts": padded_facts}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")

        def val(rep, name):
            return next(d.value for d in rep.dimensions if d.name == name)

        assert val(padded, "citation_support") < val(honest, "citation_support")
        # Concentration RISES when one predicate is repeated for volume (higher is
        # worse for this dimension), and chunk_coverage does not move at all,
        # because the invented facts all cite a chunk that was already covered.
        assert val(padded, "predicate_concentration") > val(honest, "predicate_concentration")
        assert val(padded, "chunk_coverage") <= val(honest, "chunk_coverage")
        assert len(padded_facts) > len(real), "the padded run has MORE facts"

    def test_a_well_covered_extraction_passes(self):
        """#64 acceptance: a well-covered fixture passes with span evidence."""
        chunks, windows = _doc(4)
        facts = [{"subject": f"alpha{i}", "object": f"beta{i}", "predicate": f"PRED_{i}",
                  "fact_text": f"alpha{i} relates to beta{i}", "chunk_range": [i, i]}
                 for i in range(4)]
        report = assess_extraction(
            merged={"facts": facts}, chunks_by_index=chunks, windows=windows,
            tier="standard",
            identity_evidence={"mode": "strict",
                               "verification": {"binding_verified": True,
                                                "checked_facts": 4, "offending_facts": 0}})
        assert report.status == "pass", report.as_dict()

    def test_an_unmeasurable_dimension_is_review_never_pass(self):
        """'could not check' must not read the same as 'checked and fine'."""
        chunks, windows = _doc(2)
        report = assess_extraction(
            merged={"facts": [{"subject": "alpha0", "object": "beta0", "predicate": "P",
                               "fact_text": "t", "chunk_range": [0, 0]},
                              {"subject": "alpha1", "object": "beta1", "predicate": "Q",
                               "fact_text": "t", "chunk_range": [1, 1]}]},
            chunks_by_index=chunks, windows=windows, tier="standard")
        integrity = next(d for d in report.dimensions if d.name == "endpoint_integrity")
        assert integrity.value is None
        assert integrity.status == "review"
        assert report.status == "review"

    def test_a_fabricated_citation_range_is_caught(self):
        chunks, windows = _doc(3)
        facts = [{"subject": f"alpha{i}", "object": f"beta{i}", "predicate": f"P{i}",
                  "fact_text": "t", "chunk_range": [i, i]} for i in range(3)]
        facts.append({"subject": "ghost", "object": "phantom", "predicate": "P9",
                      "fact_text": "t", "chunk_range": [900, 900]})
        report = assess_extraction(merged={"facts": facts}, chunks_by_index=chunks,
                                   windows=windows, tier="standard")
        in_range = next(d for d in report.dimensions if d.name == "citation_in_range")
        assert in_range.value == 0.75
        assert in_range.status == "fail"


# ═══════════════════════════════════════════════════════════════════════════
# 7. Producer provenance — issue #64
# ═══════════════════════════════════════════════════════════════════════════

def _edd():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "edd_under_test", "scripts/extract_deep_documents.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class TestProducerProvenance:

    def test_an_openai_run_never_reports_an_anthropic_model(self):
        """#64 acceptance, in the exact words of the issue.

        `_call_openai` ignores its `model` argument and sends OPENAI_MODEL, while
        the caller passed ANTHROPIC_MODEL. The receipt must name what did the work.
        """
        edd = _edd()
        receipt = edd._producer_for("openai", "claude-sonnet-5")
        assert receipt["model"] == edd.OPENAI_MODEL
        assert receipt["model"] != "claude-sonnet-5"
        assert receipt["provider"] == "openai_compatible"

    def test_each_transport_names_its_own_provider(self):
        edd = _edd()
        assert edd._producer_for("api", "m")["provider"] == "anthropic"
        assert edd._producer_for("claude_p", "m")["provider"] == "claude_subscription"

    def test_the_receipt_records_the_endpoint_host_and_never_the_key(self):
        edd = _edd()
        r = edd._producer_for("openai", "m")
        blob = repr(r)
        assert "endpoint_host" in r
        assert "Bearer" not in blob and "api_key" not in blob.lower()

    def test_a_provider_4xx_becomes_a_dead_letterable_extraction_error(self):
        """A non-retryable provider status must NOT abort the whole document.

        `raise_for_status()` raises httpx.HTTPStatusError, which is not an
        ExtractionError — so before this was handled it escaped the transport
        fallback, the repair loop AND the per-window dead-letter, and one 401 or
        413 cost every window already extracted. The reason must be
        `extract_http_error` specifically, because that is what makes it eligible
        for transport fallback.
        """
        import asyncio

        import httpx

        edd = _edd()

        class _Resp:
            status_code = 413
            text = "payload too large"

            def raise_for_status(self):
                raise httpx.HTTPStatusError(
                    "413", request=httpx.Request("POST", "http://x"), response=self)

            def json(self):  # pragma: no cover - never reached
                return {}

        class _Client:
            async def post(self, *a, **kw):
                return _Resp()

        with pytest.raises(edd.ExtractionError) as exc:
            asyncio.run(edd._call_openai("prompt", _Client()))
        assert exc.value.reason == "extract_http_error"
        assert exc.value.reason in edd.TRANSPORT_FALLBACK_REASONS

    def test_a_thorough_extraction_is_not_penalised_for_being_thorough(self):
        """REGRESSION. The first version of this layer scored the real fixtures
        BACKWARDS: the thin 25-fact Buehler run PASSED and the curated 213-fact
        re-run went to REVIEW.

        The cause was a "distinct predicates / facts" dimension. Any
        distinct-over-total ratio FALLS as an extraction legitimately adds facts,
        so it penalised exactly the behaviour it was meant to reward — an inert
        threshold pointing the wrong way, which is the failure #64 requirement 5
        names from the other direction.

        This asserts the shape that broke: same document, same coverage, one
        extraction simply more thorough than the other. The thorough one must not
        score worse on any dimension.
        """
        chunks, windows = _doc(6)
        sparse = [{"subject": f"alpha{i}", "object": f"beta{i}", "predicate": "P",
                   "fact_text": "t", "chunk_range": [i, i]} for i in range(6)]
        # Same 6 chunks, but each one yields several distinct, supported relations.
        thorough = []
        for i in range(6):
            for pred in ("P", "Q", "R", "S"):
                thorough.append({"subject": f"alpha{i}", "object": f"beta{i}",
                                 "predicate": pred, "fact_text": "t",
                                 "chunk_range": [i, i]})

        rs = assess_extraction(merged={"facts": sparse}, chunks_by_index=chunks,
                               windows=windows, tier="standard")
        rt = assess_extraction(merged={"facts": thorough}, chunks_by_index=chunks,
                               windows=windows, tier="standard")

        rank = {"pass": 3, "review": 2, "fail": 1, "not_evaluated": 0}
        by_name = {d.name: d for d in rs.dimensions}
        for d in rt.dimensions:
            assert rank[d.status] >= rank[by_name[d.name].status], (
                f"the thorough extraction scored WORSE on {d.name}: "
                f"{by_name[d.name].status} -> {d.status}")
        assert len(thorough) > len(sparse)


class TestDeclaredButUnusedEntities:

    def test_a_declared_entity_no_fact_references_is_not_an_endpoint(self):
        """REGRESSION, found by running the gate on three freshly-ingested posts.

        The extractor declares many more entities than its facts reference — 26
        declared vs 14 used on one real document. Treating the union as endpoints
        would pre-register a dozen rows per document that no fact points at:
        registry inflation introduced by the very thing meant to make identity
        safer, and something the unpinned path never did.

        A declared-but-unreferenced entity is a candidate the extractor mentioned,
        not an identity the payload asserts.
        """
        eps = ident.collect_endpoints(
            _payload(
                facts=[{"subject": "Used", "object": None, "object_literal": "x",
                        "predicate": "P", "fact_text": "t"}],
                entities=[{"name": "Used", "type": "Concept"},
                          {"name": "Declared But Unused", "type": "Person"},
                          {"name": "Also Unused", "type": "Organization"}],
            ),
            normalize=normalize_entity_text,
        )
        assert {e.name for e in eps} == {"Used"}

    def test_entities_still_supply_the_type_for_a_fact_endpoint(self):
        """entities[] keeps its one job: saying what type an endpoint is."""
        eps = ident.collect_endpoints(
            _payload(
                facts=[{"subject": "Ada Lovelace", "object": None,
                        "object_literal": "x", "predicate": "P", "fact_text": "t"}],
                entities=[{"name": "Ada Lovelace", "type": "Person"}],
            ),
            normalize=normalize_entity_text,
        )
        assert len(eps) == 1
        assert eps[0].entity_type == "Person"
        assert eps[0].type_defaulted is False


@pytest.mark.anyio
class TestCrossTypeConflictBlocks:
    """REGRESSION for the DWeb Berlin shape (operator decision, 2026-09-14).

    The extractor declared `DWeb Berlin` an Organization while the graph held it
    as a Project. The first version classified that MISSING with a cross-type
    ADVISORY, so `preregister_missing` would have minted a SECOND `DWeb Berlin` —
    an Organization beside the Project. The contract exists to stop one payload
    endpoint becoming two identities; creating the cross-type twin is that same
    failure wearing a different hat.
    """

    async def test_a_cross_type_label_blocks_instead_of_minting_a_twin(self, conn):
        t = _tag()
        name = f"Cross Typed {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:project-{t}",
                           text=name, etype="Project")
        _eps, report = await _preflight(conn, [(name, "Organization")])
        f = report.findings[0]
        assert f.state == ident.STATE_CROSS_TYPE_CONFLICT, f.as_evidence()
        assert f.cross_type_uris
        with pytest.raises(ident.IdentityError):
            ident.require_no_blockers(report)

    async def test_an_audited_type_decision_clears_it(self, conn):
        t = _tag()
        name = f"Cross Typed {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:project-{t}",
                           text=name, etype="Project")
        _eps, report = await _preflight(conn, [(name, "Organization")],
                                        type_decisions={name: "Organization"})
        assert report.findings[0].state == ident.STATE_MISSING
        ident.require_no_blockers(report)

    async def test_preregistration_refuses_the_twin_when_no_decision_covers_it(self, conn):
        """preregister_missing is callable directly, and minting is irreversible
        once the type is hashed into a URI. It refuses rather than trusting the
        caller ran the gate."""
        t = _tag()
        name = f"Cross Typed {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:project-{t}",
                           text=name, etype="Project")
        _eps, report = await _preflight(conn, [(name, "Organization")])
        # Forced past the gate the way a direct caller would: the state says
        # MISSING, but the finding still carries the cross-type evidence and NO
        # audited decision.
        report.findings[0].state = ident.STATE_MISSING
        assert report.findings[0].type_decision is None

        class _NeverCalled:
            async def post(self, *a, **k):
                raise AssertionError("preregistration must refuse BEFORE any HTTP call")

        # base_url supplied: without one the B1 refusal fires first, which is a
        # different (and earlier) protection than the one this test is about.
        with pytest.raises(ident.IdentityError) as exc:
            await ident.preregister_missing(_NeverCalled(), report, base_url="http://koi.test")
        assert exc.value.blockers[0]["state"] == ident.STATE_CROSS_TYPE_CONFLICT

    async def test_a_decision_naming_a_different_type_does_not_clear_anything(self, conn):
        """A decision that contradicts the payload is not a decision about it.

        Acting on it would bind the endpoint to a type nothing asserted, so the
        conflict stands at BOTH gates.
        """
        t = _tag()
        name = f"Cross Typed {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:project-{t}",
                           text=name, etype="Project")
        _eps, report = await _preflight(conn, [(name, "Organization")],
                                        type_decisions={name: "Event"})
        assert report.findings[0].state == ident.STATE_CROSS_TYPE_CONFLICT
        assert report.findings[0].type_decision is None
        with pytest.raises(ident.IdentityError):
            ident.require_no_blockers(report)

    async def test_an_audited_decision_reaches_preregistration(self, conn):
        """CORRECTED CONTRACT, 2026-09-14 (issue #68 requirement 7).

        This test previously asserted the opposite: that preregistration refuses a
        cross-type mint EVEN WHEN an audited type decision had cleared the gate.
        That made the escape hatch the blocker's own message advertises —
        "Supply an audited type decision" — unreachable end to end. The preflight
        cleared the finding to MISSING and the registration step then raised on it,
        so the operator's options were really just "merge/retype first", and
        supplying a decision turned a clean, explanatory block into a later and
        more confusing failure.

        The refusal is kept for everything it was actually protecting: a caller who
        skipped the gate (above), and a decision that names a different type
        (above). What changes is that a decision the preflight ACCEPTED now travels
        on the finding and is honoured here, and the receipt records that the mint
        rested on it.
        """
        t = _tag()
        name = f"Cross Typed {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:project-{t}",
                           text=name, etype="Project")
        _eps, report = await _preflight(conn, [(name, "Organization")],
                                        type_decisions={name: "Organization"})
        assert report.findings[0].state == ident.STATE_MISSING
        assert report.findings[0].type_decision == "Organization"

        posted = []
        minted = f"orn:personal-koi.entity:organization-{t}"

        class _Recording:
            async def post(self, path, json=None, timeout=None):
                posted.append(json)

                class _R:
                    status_code = 200

                    @staticmethod
                    def json():
                        return {"success": True, "canonical_uri": minted,
                                "is_new": True, "cross_type_warning": "same label is a Project"}
                return _R()

        out = await ident.preregister_missing(_Recording(), report, base_url="http://koi.test")
        assert out["uri_map"] == {name: minted}
        assert posted[0]["entity_type"] == "Organization"
        assert posted[0]["force_type"] is True and posted[0]["exact_only"] is True
        receipt = out["receipts"][0]
        assert receipt["audited_type_decision"] == "Organization", (
            "a mint that rested on an operator decision must say so in its receipt"
        )
        assert receipt["cross_type_warning"], (
            "the twin must not be minted quietly — the advisory is still recorded"
        )


# ═══════════════════════════════════════════════════════════════════════════
# 9. Independent-review regressions (2026-09-14) — B1–B3, M3–M6
#
# Every test here was written BEFORE its fix and observed to fail. Each encodes
# the payload SHAPE the earlier fixtures did not: a client with no base_url, a
# multi-type payload, two raw spellings of one label, an untyped fact endpoint,
# an alias decision naming a dead / wrong-type URI, a shared episode.
# ═══════════════════════════════════════════════════════════════════════════

import httpx as _httpx


def _missing_report(name, etype="Concept"):
    ep = ident.Endpoint(name=name, entity_type=etype, normalized=normalize_entity_text(name))
    return ident.PreflightReport(findings=[ident.EndpointFinding(
        endpoint=ep, state=ident.STATE_MISSING, live_uris=[], tombstoned_uris=[],
        alias_uris=[], cross_type_uris=[])])


def _ok_register(minted):
    def handler(request):
        return _httpx.Response(200, json={"success": True, "canonical_uri": minted,
                                          "is_new": True})
    return handler


@pytest.mark.anyio
class TestB1PreregistrationUrl:
    """B1. `preregister_missing` POSTed a RELATIVE path on a client with no base_url.

    The extractor's client is `provider_async_client()`, which sets none, so every
    strict run with >= 1 missing endpoint — the gate's own docstring calls that
    "every first ingest" — died in httpx before a socket opened, with a bare
    `UnsupportedProtocol` that is neither IdentityError nor ExtractionError. The
    shipped fakes (`_Recording`, `_NeverCalled`) accepted any path, so 107 green
    tests never sent one request through a real client.
    """

    async def test_a_real_client_without_base_url_gets_an_absolute_url(self):
        t = _tag()
        minted = f"orn:personal-koi.entity:concept-{t}"
        seen = []

        def handler(request):
            seen.append(str(request.url))
            return _ok_register(minted)(request)

        async with _httpx.AsyncClient(transport=_httpx.MockTransport(handler)) as http:
            out = await ident.preregister_missing(
                http, _missing_report(f"Brand New {t}"), base_url="http://koi.test")
        assert out["uri_map"] == {f"Brand New {t}": minted}
        assert seen == ["http://koi.test/register-entity"], seen

    async def test_no_base_url_anywhere_is_a_typed_refusal_before_any_request(self):
        """A client with no base_url and no base_url argument must fail as an
        IdentityError that names the problem, not as an httpx traceback — and
        must fail before a request is attempted."""
        t = _tag()
        seen = []

        def handler(request):  # pragma: no cover - must not be reached
            seen.append(str(request.url))
            return _ok_register("x")(request)

        async with _httpx.AsyncClient(transport=_httpx.MockTransport(handler)) as http:
            with pytest.raises(ident.IdentityError) as exc:
                await ident.preregister_missing(http, _missing_report(f"Brand New {t}"))
        assert "base_url" in str(exc.value) or "absolute" in str(exc.value)
        assert seen == []

    async def test_a_client_with_its_own_base_url_is_honoured(self):
        t = _tag()
        minted = f"orn:personal-koi.entity:concept-{t}"
        seen = []

        def handler(request):
            seen.append(str(request.url))
            return _ok_register(minted)(request)

        async with _httpx.AsyncClient(transport=_httpx.MockTransport(handler),
                                      base_url="http://client.test:1") as http:
            await ident.preregister_missing(http, _missing_report(f"Brand New {t}"))
        assert seen == ["http://client.test:1/register-entity"]


@pytest.mark.anyio
class TestB2CrossTypeIsEndpointSpecific:
    """B2. The cross-type read excluded the PAYLOAD-WIDE set of types, not the
    endpoint's own type. One live `Project` row declared `Organization` by the
    payload blocked correctly — until any OTHER endpoint in the same payload was a
    `Project`, at which point the same label quietly became `missing` and would
    have been minted as a cross-type twin with force_type. Every shipped
    cross-type test used a single-endpoint payload; real payloads declare 4+ types.
    """

    async def test_the_conflict_survives_another_endpoint_of_the_conflicting_type(self, conn):
        t = _tag()
        name = f"Cross Typed {t}"
        other = f"Some Other Thing {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:project-{t}",
                           text=name, etype="Project")
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:project-other-{t}",
                           text=other, etype="Project")
        # CONTROL — the shipped test's exact single-endpoint shape.
        _eps, alone = await _preflight(conn, [(name, "Organization")])
        assert alone.findings[0].state == ident.STATE_CROSS_TYPE_CONFLICT
        # PROBE — one extra endpoint typed Project in the same payload.
        _eps, together = await _preflight(conn, [(name, "Organization"), (other, "Project")])
        f = next(x for x in together.findings if x.endpoint.name == name)
        assert f.state == ident.STATE_CROSS_TYPE_CONFLICT, f.as_evidence()
        assert f.cross_type_uris == [f"orn:personal-koi.entity:project-{t}"]
        # And the unrelated Project resolves normally — the fix must not over-fire.
        g = next(x for x in together.findings if x.endpoint.name == other)
        assert g.state == ident.STATE_LIVE_EXACT
        assert g.cross_type_uris == []

    async def test_preregistration_still_refuses_in_the_multi_type_payload(self, conn):
        """The belt-and-braces refusal reads `finding.cross_type_uris`, which the
        payload-wide read left EMPTY for exactly this shape."""
        t = _tag()
        name = f"Cross Typed {t}"
        other = f"Some Other Thing {t}"
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:project-{t}",
                           text=name, etype="Project")
        await _seed_entity(conn, uri=f"orn:personal-koi.entity:project-other-{t}",
                           text=other, etype="Project")
        _eps, report = await _preflight(conn, [(name, "Organization"), (other, "Project")])
        for f in report.findings:
            f.state = ident.STATE_MISSING  # a caller who skipped the gate

        class _NeverCalled:
            async def post(self, *a, **k):
                raise AssertionError("must refuse BEFORE any HTTP call")

        with pytest.raises(ident.IdentityError) as exc:
            await ident.preregister_missing(_NeverCalled(), report, base_url="http://x")
        assert exc.value.blockers[0]["state"] == ident.STATE_CROSS_TYPE_CONFLICT

    async def test_an_alias_decision_cannot_bind_a_live_row_of_another_payload_type(self, conn):
        """Same root, other symptom: a decided URI was accepted if it was live under
        ANY type present in the payload. A pinned write with a type-mismatched
        URI 422s, so accepting it here made the gate print PASS for a write that
        cannot land."""
        t = _tag()
        label = f"Decided {t}"
        wrong = f"orn:personal-koi.entity:project-wrong-{t}"
        await _seed_entity(conn, uri=wrong, text=f"Unrelated Project {t}", etype="Project")
        _eps, report = await _preflight(
            conn, [(label, "Concept"), (f"Filler Project {t}", "Project")],
            alias_decisions={label: wrong})
        f = next(x for x in report.findings if x.endpoint.name == label)
        assert f.state != ident.STATE_LIVE_EXACT, f.as_evidence()
        assert f.live_uris == []


class TestB3FrozenMapUsesTheCanonicalKey:
    """B3. `collect_endpoints` keyed on (normalize(name), type) and kept the FIRST
    raw spelling; `FrozenMap.uri_for` was a bare dict.get on that spelling; the
    episode builder looked up each fact's OWN raw string. `GPT-4` and `gpt_4` are
    one endpoint and one frozen binding — and the second spelling raised
    `identity_map_incomplete` AFTER preregistration had already minted. 86 of
    1,755 cached documents carry such a pair. `by_key` existed and nothing read it.
    """

    def test_every_raw_spelling_of_a_bound_endpoint_resolves(self):
        payload = _payload(facts=[
            {"subject": "GPT-4", "predicate": "IS_A", "object": "model", "fact_text": "t"},
            {"subject": "gpt_4", "predicate": "HAS_SIZE", "object": "large", "fact_text": "t"},
        ], entities=[{"name": "GPT-4", "type": "Concept"}])
        eps = ident.collect_endpoints(payload, normalize=normalize_entity_text)
        assert len(eps) == 3  # gpt 4, model, large
        frozen = ident.FrozenMap(
            by_name={e.name: f"orn:personal-koi.entity:x-{i}" for i, e in enumerate(eps)},
            by_key={e.key: f"orn:personal-koi.entity:x-{i}" for i, e in enumerate(eps)},
            by_name_type={e.name: e.entity_type for e in eps}, evidence=[],
            normalize=normalize_entity_text)
        assert frozen.uri_for("GPT-4") == frozen.uri_for("gpt_4") is not None
        assert frozen.type_for("gpt_4") == "Concept"
        # Positive control: a label the payload never had is still unbound.
        assert frozen.uri_for("Nothing Like This") is None

    def test_the_episode_builder_pins_both_spellings_to_one_uri(self):
        """Through the REAL builder, not a stand-in."""
        edd = _edd()
        merged = edd.merge_extractions([{
            "entities": [{"name": "GPT-4", "type": "Concept", "first_seen_chunk": 0,
                          "mention_count": 1},
                         {"name": "model", "type": "Concept", "first_seen_chunk": 0,
                          "mention_count": 1}],
            "facts": [
                {"subject": "GPT-4", "predicate": "IS_A", "object": "model",
                 "fact_text": "a", "chunk_range": [0, 0], "confidence": "high"},
                {"subject": "gpt_4", "predicate": "RELATES_TO", "object": "model",
                 "fact_text": "b", "chunk_range": [1, 1], "confidence": "high"},
            ],
        }], [])
        eps = ident.collect_endpoints(merged, normalize=normalize_entity_text)
        uri = {e.normalized: f"orn:personal-koi.entity:{e.normalized.replace(' ', '-')}"
               for e in eps}
        frozen = ident.FrozenMap(
            by_name={e.name: uri[e.normalized] for e in eps},
            by_key={e.key: uri[e.normalized] for e in eps},
            by_name_type={e.name: e.entity_type for e in eps}, evidence=[],
            normalize=normalize_entity_text)
        payload = edd.facts_to_episode_payload(
            merged, name="n", summary="", source_document="d", group_id="g", frozen=frozen)
        subj = {f["subject_uri"] for f in payload["facts"]}
        assert subj == {uri["gpt 4"]}, payload["facts"]
        assert all(f["subject_type"] == "Concept" for f in payload["facts"])


class TestM6MergeKeysOnTheCanonicalNormalizer:
    """M6. The merge keyed entities on a private `_norm` (case + whitespace) while
    identity keys on `normalize_entity_text` (also `-`/`_`). `omni-mapping` and
    `omni mapping` therefore survived the merge as TWO typed records, and
    `collect_endpoints` then raised `payload_type_conflict` on a disagreement the
    merge was supposed to fold. 7 cached documents; uncaught in main()."""

    def test_hyphen_and_space_spellings_merge_into_one_entity(self):
        edd = _edd()
        merged = edd.merge_extractions([
            {"entities": [{"name": "omni-mapping", "type": "Concept",
                           "first_seen_chunk": 0, "mention_count": 1}], "facts": []},
            {"entities": [{"name": "omni mapping", "type": "Project",
                           "first_seen_chunk": 3, "mention_count": 2}], "facts": []},
        ], [])
        assert len(merged["entities"]) == 1, merged["entities"]
        assert merged["type_conflicts"], "the coercion must be reported, not hidden"
        assert set(merged["type_map"]) == {normalize_entity_text("omni-mapping")}

    def test_the_merge_key_is_the_identity_key(self):
        """One normalizer. If these diverge again, this is the test that says so."""
        edd = _edd()
        for s in ("GPT-4", "gpt_4", "Omni-Mapping", "  Spaced  Out ", "@handle", "A_b-C"):
            assert edd._norm(s) == normalize_entity_text(s), s


class TestM3UntypedEndpointsNeverDefaultToConcept:
    """M3. A fact endpoint absent from `entities[]` was typed `Concept` by default,
    preregistered as `Concept` with force_type, and the default was hashed into
    the URI. `type_defaulted` was counted and gated nothing. The prompt itself
    says every fact endpoint MUST appear in entities[], so an untyped endpoint is
    an extraction defect — it blocks, and only an audited type decision types it.
    """

    def test_an_untyped_endpoint_carries_no_type(self):
        eps = ident.collect_endpoints(
            _payload(facts=[{"subject": "Alpha", "object": "Ghost", "predicate": "P",
                             "fact_text": "t"}],
                     entities=[{"name": "Alpha", "type": "Concept"}]),
            normalize=normalize_entity_text)
        ghost = next(e for e in eps if e.name == "Ghost")
        assert ghost.entity_type is None
        assert ghost.type_defaulted is True

    def test_an_audited_type_decision_types_it(self):
        eps = ident.collect_endpoints(
            _payload(facts=[{"subject": "Alpha", "object": "Ada Lovelace", "predicate": "P",
                             "fact_text": "t"}],
                     entities=[{"name": "Alpha", "type": "Concept"}]),
            normalize=normalize_entity_text,
            type_decisions={"Ada Lovelace": "Person"})
        ada = next(e for e in eps if e.name == "Ada Lovelace")
        assert ada.entity_type == "Person"
        assert ada.type_defaulted is False
        assert ada.type_source == "audited_decision"

    @pytest.mark.anyio
    async def test_preflight_blocks_it_and_names_what_the_graph_holds(self, conn):
        t = _tag()
        label = f"Untyped Person {t}"
        person = f"orn:personal-koi.entity:person-{t}"
        await _seed_entity(conn, uri=person, text=label, etype="Person")
        eps = ident.collect_endpoints(
            _payload(facts=[{"subject": label, "object": None, "object_literal": "x",
                             "predicate": "P", "fact_text": "t"}]),
            normalize=normalize_entity_text)
        report = await ident.preflight_endpoints(
            conn, eps, normalize=normalize_entity_text, normalize_alias_fn=normalize_alias)
        f = report.findings[0]
        assert f.state == ident.STATE_TYPE_UNDECLARED
        assert f.state in ident.BLOCKING_STATES
        assert person in f.cross_type_uris, "evidence must show the operator what exists"
        with pytest.raises(ident.IdentityError):
            ident.require_no_blockers(report)
        assert not report.missing, "an untyped endpoint must never reach preregistration"

    @pytest.mark.anyio
    async def test_freeze_refuses_an_untyped_endpoint_even_if_the_gate_was_skipped(self, conn):
        t = _tag()
        eps = [ident.Endpoint(name=f"Ghost {t}", entity_type=None,
                              normalized=normalize_entity_text(f"Ghost {t}"),
                              type_defaulted=True)]
        with pytest.raises(ident.IdentityError) as exc:
            await ident.freeze_endpoint_map(conn, eps, normalize=normalize_entity_text)
        assert exc.value.blockers[0]["state"] == ident.STATE_TYPE_UNDECLARED


@pytest.mark.anyio
class TestM4FreezeValidatesAliasDecisions:
    """M4. The freeze's alias branch bound the decided URI with ZERO validation —
    no liveness, no type, and it skipped the `expected_uris` disagreement check.
    Preflight refused a dead-URI decision; freeze honoured it; the gate printed
    PASS for a write `_bind_pinned_uri` 422s on."""

    async def test_a_dead_uri_decision_is_refused(self, conn):
        t = _tag()
        dead = f"orn:personal-koi.entity:concept-dead-{t}"
        survivor = f"orn:personal-koi.entity:concept-survivor-{t}"
        await _seed_entity(conn, uri=survivor, text=f"Survivor {t}", etype="Concept")
        await _seed_entity(conn, uri=dead, text=f"Dead {t}", etype="Concept",
                           merged_into=survivor)
        ep = ident.Endpoint(name=f"Label {t}", entity_type="Concept",
                            normalized=normalize_entity_text(f"Label {t}"))
        with pytest.raises(ident.IdentityError) as exc:
            await ident.freeze_endpoint_map(conn, [ep], normalize=normalize_entity_text,
                                            alias_decisions={ep.name: dead})
        assert exc.value.blockers[0]["state"] == "alias_decision_not_live"

    async def test_an_unknown_uri_decision_is_refused(self, conn):
        t = _tag()
        ep = ident.Endpoint(name=f"Label {t}", entity_type="Concept",
                            normalized=normalize_entity_text(f"Label {t}"))
        with pytest.raises(ident.IdentityError) as exc:
            await ident.freeze_endpoint_map(
                conn, [ep], normalize=normalize_entity_text,
                alias_decisions={ep.name: f"orn:personal-koi.entity:concept-ghost-{t}"})
        assert exc.value.blockers[0]["state"] == "alias_decision_not_live"

    async def test_a_wrong_type_decision_is_refused(self, conn):
        t = _tag()
        org = f"orn:personal-koi.entity:org-{t}"
        await _seed_entity(conn, uri=org, text=f"An Org {t}", etype="Organization")
        ep = ident.Endpoint(name=f"Label {t}", entity_type="Concept",
                            normalized=normalize_entity_text(f"Label {t}"))
        with pytest.raises(ident.IdentityError) as exc:
            await ident.freeze_endpoint_map(conn, [ep], normalize=normalize_entity_text,
                                            alias_decisions={ep.name: org})
        assert exc.value.blockers[0]["state"] == "alias_decision_type_mismatch"

    async def test_a_decision_disagreeing_with_the_registration_is_refused(self, conn):
        t = _tag()
        a = f"orn:personal-koi.entity:concept-a-{t}"
        b = f"orn:personal-koi.entity:concept-b-{t}"
        await _seed_entity(conn, uri=a, text=f"A {t}", etype="Concept")
        await _seed_entity(conn, uri=b, text=f"B {t}", etype="Concept")
        ep = ident.Endpoint(name=f"Label {t}", entity_type="Concept",
                            normalized=normalize_entity_text(f"Label {t}"))
        with pytest.raises(ident.IdentityError) as exc:
            await ident.freeze_endpoint_map(conn, [ep], normalize=normalize_entity_text,
                                            alias_decisions={ep.name: a},
                                            expected_uris={ep.name: b})
        assert exc.value.blockers[0]["state"] == "registration_disagreement"

    async def test_a_live_same_type_decision_still_binds(self, conn):
        """Positive control for the four refusals above."""
        t = _tag()
        good = f"orn:personal-koi.entity:concept-good-{t}"
        await _seed_entity(conn, uri=good, text=f"Good {t}", etype="Concept")
        ep = ident.Endpoint(name=f"Label {t}", entity_type="Concept",
                            normalized=normalize_entity_text(f"Label {t}"))
        frozen = await ident.freeze_endpoint_map(conn, [ep], normalize=normalize_entity_text,
                                                 alias_decisions={ep.name: good})
        assert frozen.by_name == {ep.name: good}
        assert frozen.evidence[0]["bound_via"] == "audited_alias_decision"


@pytest.mark.anyio
class TestM5VerificationIsScopedToThisRunsFacts:
    """M5. `verify_persisted_graph` was EPISODE-scoped, and episodes are keyed on
    (source_document, group_id): 11 live episodes are shared by 24 documents. A
    strict run on a shared episode failed verification against a SIBLING
    document's facts — after this run's facts had committed. The server now
    returns the ids it wrote, and verification checks exactly those.
    """

    async def test_the_response_names_the_facts_it_wrote(self, api):
        client, conn = api
        t = _tag()
        uri = f"orn:personal-koi.entity:concept-ok-{t}"
        await _seed_entity(conn, uri=uri, text=f"Ok {t}", etype="Concept")
        r = await client.post("/knowledge/episodes", json=_episode([{
            "subject": f"Ok {t}", "subject_type": "Concept", "subject_uri": uri,
            "predicate": "P", "object_literal": "a", "fact_text": "one",
        }], tag=t, create_entities=False))
        assert r.status_code == 201
        body = r.json()
        assert body["facts_created"] == 1
        assert len(body["fact_ids"]) == 1
        persisted = await conn.fetchval(
            "SELECT count(*) FROM knowledge_facts WHERE id = $1::uuid", body["fact_ids"][0])
        assert persisted == 1

    async def test_a_sibling_documents_facts_do_not_fail_this_run(self, api):
        client, conn = api
        t = _tag()
        a = f"orn:personal-koi.entity:concept-a-{t}"
        b = f"orn:personal-koi.entity:concept-b-{t}"
        await _seed_entity(conn, uri=a, text=f"A {t}", etype="Concept")
        await _seed_entity(conn, uri=b, text=f"B {t}", etype="Concept")
        shared = _episode([{"subject": f"A {t}", "subject_type": "Concept", "subject_uri": a,
                            "predicate": "P", "object_literal": "x", "fact_text": "sib"}],
                          tag=t, create_entities=False)
        r1 = await client.post("/knowledge/episodes", json=shared)
        assert r1.status_code == 201
        # Second "document": same (source_document, group_id) → same episode.
        mine = dict(shared, facts=[{"subject": f"B {t}", "subject_type": "Concept",
                                    "subject_uri": b, "predicate": "Q",
                                    "object_literal": "y", "fact_text": "mine"}])
        r2 = await client.post("/knowledge/episodes", json=mine)
        assert r2.status_code == 201
        assert r2.json()["episode_id"] == r1.json()["episode_id"], "precondition: shared episode"

        frozen = ident.FrozenMap(by_name={f"B {t}": b}, by_key={}, by_name_type={f"B {t}": "Concept"},
                                 evidence=[])
        episode_id = uuid.UUID(r2.json()["episode_id"])
        # CONTROL — the old episode scope flags the sibling's URI.
        old = await ident.verify_persisted_graph(conn, episode_id, frozen)
        assert not old.ok and old.offending[0]["persisted_uri"] == a
        # FIX — scoped to the facts THIS run wrote.
        new = await ident.verify_persisted_graph(conn, episode_id, frozen,
                                                 fact_ids=r2.json()["fact_ids"])
        assert new.ok, new.as_evidence()
        assert new.checked_facts == 1


@pytest.mark.anyio
class TestTheExtractorSeamEndToEnd:
    """The production call path, not a stand-in: `establish_identity` with the
    extractor's own client shape (no base_url), a rolled-back registry, and a
    mock `/register-entity`. This is the test that would have caught B1, and it
    also proves type decisions reach `collect_endpoints` (M3) and that the
    freeze receives the classified endpoints."""

    async def test_a_first_ingest_registers_at_an_absolute_url_and_freezes(self, conn):
        edd = _edd()
        t = _tag()
        known = f"Known {t}"
        fresh = f"Fresh {t}"
        known_uri = f"orn:personal-koi.entity:concept-known-{t}"
        fresh_uri = f"orn:personal-koi.entity:concept-fresh-{t}"
        await _seed_entity(conn, uri=known_uri, text=known, etype="Concept")
        seen = []

        def handler(request):
            seen.append((str(request.url), request.read()))
            # Mimic the server: the exact-only registration creates the row, so the
            # freeze's re-read finds it.
            return _httpx.Response(200, json={"success": True, "canonical_uri": fresh_uri,
                                              "is_new": True})

        merged = edd.merge_extractions([{
            "entities": [{"name": known, "type": "Concept", "first_seen_chunk": 0, "mention_count": 1},
                         {"name": fresh, "type": "Concept", "first_seen_chunk": 0, "mention_count": 1}],
            "facts": [{"subject": known, "predicate": "RELATES_TO", "object": fresh,
                       "fact_text": "x", "chunk_range": [0, 0], "confidence": "high"}],
        }], [])
        evidence = {"mode": "strict"}
        async with _httpx.AsyncClient(transport=_httpx.MockTransport(handler)) as http:
            # Seed the row the mock "registered", inside the same rolled-back txn,
            # right before the freeze re-reads. Done via a wrapper so the order is
            # register -> row exists -> freeze, as in production.
            real_prereg = ident.preregister_missing

            async def prereg_then_seed(*a, **k):
                out = await real_prereg(*a, **k)
                await _seed_entity(conn, uri=fresh_uri, text=fresh, etype="Concept")
                return out
            edd.ident.preregister_missing = prereg_then_seed
            try:
                frozen = await edd.establish_identity(
                    conn, http, merged, evidence, document_rid=f"document:test-{t}",
                    mode="strict", alias_decisions_override={}, type_decisions_override={})
            finally:
                edd.ident.preregister_missing = real_prereg
        assert seen and seen[0][0] == f"{edd.KOI_BASE_URL}/register-entity", seen
        assert frozen is not None
        assert frozen.by_name == {known: known_uri, fresh: fresh_uri}
        assert evidence["preregistration"]["registered"] == 1
        assert evidence["frozen_map"]["distinct_uris"] == 2

    async def test_an_untyped_endpoint_blocks_the_run_and_a_decision_unblocks_it(self, conn):
        edd = _edd()
        t = _tag()
        person = f"Ada {t}"
        person_uri = f"orn:personal-koi.entity:person-{t}"
        await _seed_entity(conn, uri=person_uri, text=person, etype="Person")
        merged = edd.merge_extractions([{
            "entities": [],   # the extractor forgot to declare the endpoint
            "facts": [{"subject": person, "predicate": "AUTHORED_BY", "object": None,
                       "object_literal": "x", "fact_text": "x", "chunk_range": [0, 0],
                       "confidence": "high"}],
        }], [])

        class _NeverCalled:
            base_url = ""

            async def post(self, *a, **k):
                raise AssertionError("nothing may be registered for an untyped endpoint")

        evidence = {"mode": "strict"}
        with pytest.raises(ident.IdentityError) as exc:
            await edd.establish_identity(conn, _NeverCalled(), merged, evidence,
                                         document_rid=f"document:test-{t}", mode="strict",
                                         alias_decisions_override={}, type_decisions_override={})
        assert exc.value.blockers[0]["state"] == ident.STATE_TYPE_UNDECLARED
        assert exc.value.blockers[0]["type"] is None
        assert person_uri in exc.value.blockers[0]["cross_type_uris"]
        assert "error" in evidence

        # With the audited decision the endpoint is typed Person, resolves to the
        # live row, and nothing is registered.
        evidence2 = {"mode": "strict"}
        frozen = await edd.establish_identity(
            conn, _NeverCalled(), merged, evidence2, document_rid=f"document:test-{t}",
            mode="strict", alias_decisions_override={},
            type_decisions_override={person: "Person"})
        assert frozen.by_name == {person: person_uri}
        assert frozen.type_for(person) == "Person"
        assert evidence2["preregistration"]["registered"] == 0


@pytest.mark.anyio
async def test_preflight_applies_a_type_decision_to_an_untyped_endpoint_before_reading(conn):
    """A decision typing an endpoint the caller left untyped must be applied BEFORE
    the candidate read, or the decided type's rows are never fetched and the
    endpoint classifies MISSING beside a live row of exactly that type."""
    t = _tag()
    label = f"Untyped Person {t}"
    person = f"orn:personal-koi.entity:person-{t}"
    await _seed_entity(conn, uri=person, text=label, etype="Person")
    ep = ident.Endpoint(name=label, entity_type=None, normalized=normalize_entity_text(label),
                        type_defaulted=True)
    report = await ident.preflight_endpoints(
        conn, [ep], normalize=normalize_entity_text, normalize_alias_fn=normalize_alias,
        type_decisions={label: "Person"})
    f = report.findings[0]
    assert f.state == ident.STATE_LIVE_EXACT and f.live_uris == [person]
    assert f.endpoint.entity_type == "Person" and f.endpoint.type_source == "audited_decision"
    assert report.endpoints[0].entity_type == "Person", "the freeze must receive the typed endpoint"
