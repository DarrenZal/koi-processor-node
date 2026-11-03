"""A merged-away entity must never reach a reader, and must never be written to.

`/entities/merge` TOMBSTONES the loser — sets `merged_into`, keeps the row, keeps its
`entity_text`, `aliases` and `embedding_3072` so history stays resolvable. That is a good
design and it has one consequence nothing accounted for: **every merge ever performed
leaves a row that still matches by name and still competes in the ANN.** 202 exist; 167
share `normalized_text` with the live survivor they point at.

Commit 00a3049 fixed this in `api/retrieval_executors.py` and only there. The sweep that
followed found the same shape at ten more sites — including `/knowledge/unified-search`,
whose top hit for "Pol.is" was `schema:softwareapplication-pol.is-568dbb48`, a tombstone
merged into `softwareapplication-polis-b6aedccc`, which is *itself* a tombstone merged
into the live `concept-pol.is-053fa414`. Doubly dead, ranked first, on the tool the global
CLAUDE.md tells every session to call before anything else.

Two behaviours, and using the wrong one causes a different bug:

  RETRIEVAL — exclude. The survivor is in the same index, so a tombstone is a pure
  duplicate that costs a result slot and can be cited by an LLM under a dead URI.

  RESOLUTION — follow. Excluding here is worse than doing nothing: the lookup misses,
  falls through to create_new, and mints a THIRD row for a name that already has a
  canonical home. 53 `knowledge_facts` rows (21 subject, 32 object) are already bound to
  tombstoned URIs because the resolution path did neither.

These tests assert the two rules over the query text itself, which is the only way to
catch the *next* site rather than re-fixing these. A live-DB test would pass on any query
that happens not to have a tombstone in range today.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests import koi_test_targets as targets

REPO = Path(__file__).resolve().parents[1]

# Query sites that serve entity_registry rows to a user or an LLM. Each entry is
# (file, a fragment unique to the query, why it matters).
RETRIEVAL_SITES = [
    ("api/routers/knowledge_router.py", "er.embedding_3072::halfvec(3072) <=> $1::halfvec(3072)) AS score",
     "/knowledge/unified-search entity ANN — the first-call tool for every session"),
    ("api/routers/knowledge_router.py", "ORDER BY LENGTH(er.entity_text)",
     "/knowledge/unified-search keyword fallback — serves whenever embeddings are down"),
    ("api/personal_ingest_api.py", "ORDER BY embedding_3072::halfvec(3072) <=> $1::halfvec(3072)\n            LIMIT $2",
     "POST /chat graph-guided retrieval seed ANN"),
    ("api/personal_ingest_api.py", "ORDER BY ({match_score}) DESC, created_at DESC",
     "POST /chat graph-guided TEXT fallback — the path taken while embeddings are down"),
    ("api/personal_ingest_api.py", "WHERE entity_type = $1 AND NOT node_private",
     "GET /entities list endpoint"),
    ("api/retrieval_executors.py", "LIVE_FILTER",
     "POST /chat entity ANN (fixed in 00a3049)"),
    ("api/web_fetcher.py", "WHERE LENGTH(entity_text) >= 3",
     "scan_for_known_entities — web-submission entity matching, feeds "
     "web_submissions.matching_entities (a 7th site the original sweep missed)"),
]


def read(rel: str) -> str:
    return (REPO / rel).read_text()


def statement_around(src: str, fragment: str, before: int = 30, after: int = 12) -> str:
    """Line window. Only safe where the anchor is not adjacent to another SQL string."""
    idx = src.find(fragment)
    assert idx != -1, f"anchor fragment vanished: {fragment!r}"
    line = src[:idx].count("\n")
    lines = src.split("\n")
    return "\n".join(lines[max(0, line - before): line + after])


def sql_statement(src: str, fragment: str) -> str:
    """Exactly the triple-quoted SQL literal containing `fragment`, and nothing else.

    A line window is not good enough here and the first version of this file proved it:
    the graph-guided text fallback sits ~23 lines below the vector query in the same
    function, so a 30-line lookback saw the VECTOR query's `merged_into` and passed while
    the fallback had none. Removing the fallback's filter — the query that serves every
    request while embeddings are down — did not fail a single test.

    That is the exact defect this file is about, committed inside the test written to
    catch it. Bounding to the literal makes each query answer for itself.
    """
    idx = src.find(fragment)
    assert idx != -1, f"anchor fragment vanished: {fragment!r}"
    start = src.rfind('"""', 0, idx)
    assert start != -1, f"no opening triple-quote before {fragment!r}"
    end = src.find('"""', idx)
    assert end != -1, f"no closing triple-quote after {fragment!r}"
    return src[start + 3:end]


def enclosing_function(src: str, anchor: str) -> str:
    """The full body of the function containing `anchor`.

    Resolution happens in PYTHON, after the query returns, and often dozens of lines
    below the SQL — `batch_resolve_entities` runs three tiers and follows merges at the
    end. A fixed line window around the query would miss it, and a window around the fix
    would be circular. The honest claim is "this function does not return a URI without
    resolving it", so the function is the unit.
    """
    idx = src.find(anchor)
    assert idx != -1, f"anchor vanished: {anchor!r}"
    lines = src.split("\n")
    at = src[:idx].count("\n")
    start = next((i for i in range(at, -1, -1)
                  if re.match(r"\s*(async def|def)\s", lines[i])), None)
    assert start is not None, f"no enclosing def for {anchor!r}"
    indent = len(lines[start]) - len(lines[start].lstrip())
    end = len(lines)
    for i in range(start + 1, len(lines)):
        ln = lines[i]
        if ln.strip() and (len(ln) - len(ln.lstrip())) <= indent and \
                re.match(r"\s*(async def|def|class)\s", ln):
            end = i
            break
    return "\n".join(lines[start:end])


@pytest.mark.parametrize("rel,fragment,why", RETRIEVAL_SITES,
                         ids=[f"{s[0].split('/')[-1]}:{s[2][:34]}" for s in RETRIEVAL_SITES])
def test_retrieval_sites_exclude_tombstones(rel: str, fragment: str, why: str) -> None:
    """Every query that RETURNS entities to a reader filters merged_into.

    Anchored on a fragment of the query rather than a line number so it survives edits
    above it — a line-numbered assertion would go stale and start passing vacuously,
    which is the failure mode this whole file is about.
    """
    stmt = sql_statement(read(rel), fragment)
    assert "merged_into" in stmt, (
        f"{rel} — {why}\n"
        f"This query returns entity_registry rows with no merged_into filter, so "
        f"tombstoned duplicates compete with the live row they were merged into."
    )


def test_the_resolver_returns_live_uris_at_one_choke_point() -> None:
    """`resolve_entity_multi_tier` must follow merges for ALL tiers, not per-tier.

    Four tiers can each match a tombstone (merges keep normalized_text, aliases AND the
    embedding, so exact/alias/fuzzy/semantic are equally exposed). Patching them one at a
    time is how the next tier added silently reintroduces the bug — so the guarantee lives
    at the single exit, and this test pins that structure, not the four call sites.
    """
    src = read("api/resolution_primitives.py")
    assert "_resolve_entity_multi_tier_raw" in src, (
        "the raw/wrapped split is gone — resolve_entity_multi_tier can return a tombstone again"
    )
    wrapper = statement_around(src, "async def resolve_entity_multi_tier(", before=0, after=40)
    assert "resolve_to_live_uri" in wrapper, (
        "resolve_entity_multi_tier no longer resolves its result to the live row"
    )


def test_the_chain_walker_is_transitive_and_cycle_safe() -> None:
    """Merges chain: 14 rows in the live registry are two hops from a live entity.

    A single-hop follow lands on another tombstone and looks like it worked, which is
    strictly worse than not following at all — it launders a dead URI into one that
    passes an `IS NULL` check nowhere.
    """
    src = read("api/resolution_primitives.py")
    fn = statement_around(src, "async def resolve_to_live_uri", before=0, after=35)
    assert "for _ in range(MAX_MERGE_CHAIN)" in fn, "the follow is not transitive"
    assert "seen" in fn and "cycle" in fn.lower(), "no cycle guard: A->B->A would spin in-request"


# Name -> uri lookups whose result is PERSISTED. Excluding a tombstone here would drop the
# binding; following it attaches to the entity the caller meant. Each entry names the
# function that must call resolve_to_live_uri before returning.
RESOLUTION_SITES = [
    ("api/personal_ingest_api.py", "async def _resolve_entity_uri",
     "POST /chat structured graph query — runs on every request"),
    ("api/vault_parser.py", "async def batch_resolve_entities",
     "wikilink -> document_entity_links; its typed and alias tiers can both land on a "
     "tombstone (merges keep normalized_text and aliases deliberately)"),
    ("api/routers/claims_router.py", "WHERE normalized_text = $1 OR entity_text ILIKE $2",
     "claimant on an auto-created claim — can be attested, verified and anchored on chain"),
    ("api/routers/commitment_router.py", "candidate.pledger_organization or candidate.pledger_name",
     "pledger on an auto-created commitment"),
    ("api/routers/task_router.py", 'AND (entity_type = $2 OR entity_type IS NULL)',
     "task ownerWikilink resolution"),
    ("api/routers/intent_router.py", 'AND (entity_type = $2 OR entity_type IS NULL)',
     "intent subject resolution"),
]


@pytest.mark.parametrize("rel,anchor,why", RESOLUTION_SITES,
                         ids=[f"{s[0].split('/')[-1]}" for s in RESOLUTION_SITES])
def test_resolution_sites_follow_merges(rel: str, anchor: str, why: str) -> None:
    """Every name->uri lookup whose result is stored resolves through to the live row.

    Deliberately a window rather than a SQL literal: the follow happens in PYTHON after
    the query, so bounding to the SQL would look at the wrong thing entirely.
    """
    region = enclosing_function(read(rel), anchor)
    assert "resolve_to_live_uri" in region, (
        f"{rel} — {why}\n"
        f"This lookup can return a tombstoned row and its URI is persisted. Excluding "
        f"would lose the binding; it must FOLLOW merged_into instead."
    )


def test_write_paths_never_persist_a_tombstoned_uri() -> None:
    """knowledge-add resolves the URI that gets STORED on knowledge_facts.

    This one is not a degraded response, it is damage on disk — and there are already 53
    such rows. All four tiers must route through the accept helper that follows the merge.
    """
    src = read("api/routers/knowledge_router.py")
    assert "async def _accept(row)" in src, "the tombstone-following accept helper is gone"
    # Every tier must go through it: a bare `return row["fuseki_uri"], False, ...` is the
    # pre-fix shape and must not come back.
    bare = re.findall(r'return row\["fuseki_uri"\], False', src)
    assert not bare, (
        f"{len(bare)} resolution tier(s) return the matched row directly, bypassing _accept"
    )


def test_the_live_registry_still_has_the_shape_these_tests_assume() -> None:
    """Guard the guard.

    If merges ever start hard-deleting instead of tombstoning, every assertion above
    becomes vacuously true and this file would keep reporting green while testing nothing.
    Assert the hazard still exists, so the tests stay meaningful or fail loudly.

    Read-only, against KOI_TEST_DATABASE_URL. It needs a copy of a populated registry; on
    an empty one (a fresh migration build) it skips and says so, because there is nothing
    to measure. A populated registry without tombstones fails.
    """
    import psycopg2
    with psycopg2.connect(targets.database_url()) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM entity_registry")
        if cur.fetchone()[0] == 0:
            pytest.skip("entity_registry at KOI_TEST_DATABASE_URL is empty; this check "
                        "needs a copy of a populated registry")
        cur.execute("SELECT count(*) FROM entity_registry WHERE merged_into IS NOT NULL")
        tombstones = cur.fetchone()[0]
        cur.execute("""SELECT count(*) FROM entity_registry
                       WHERE merged_into IS NOT NULL AND embedding_3072 IS NOT NULL""")
        embedded = cur.fetchone()[0]
    assert tombstones > 0, (
        "no tombstones in entity_registry — either merges now hard-delete (in which case "
        "these tests are vacuous and should be deleted) or this is not the live database"
    )
    assert embedded > 0, (
        "tombstones exist but none carry an embedding, so none can reach an ANN. The "
        "retrieval half of this file is no longer testing a live hazard."
    )
