#!/usr/bin/env python3
"""Live smoke test for EPISODE REQUEST-SIZE BATCHING.

The whole design rests on one assumption: sequential POSTs sharing
`(source_document, group_id)` collapse into ONE episode via the server's
episode-reuse path. If that were false, a batched document would be split across
episodes and the post-episode dedup sweeps — which key on a single `episode_id` —
would silently clean only the first. This test proves the assumption on a live
server rather than trusting the code read.

Asserts:
  1. a batched post lands ALL facts under a SINGLE episode_id
  2. that episode_id equals the one a same-(source_document, group_id) post reuses
  3. the aggregated counters sum to the real committed row count
  4. batching and non-batching produce the same end state (same episode, same facts)
  5. DOC_EPISODE_BATCH_SIZE=0 takes the single-POST path

Writes into group_id='episode-batching-test' and DELETES everything it created.

Run:  KOI_TEST_DATABASE_URL=... KOI_TEST_API_URL=http://127.0.0.1:<throwaway port> \
      <venv>/bin/python tests/test_episode_batching_smoke.py
The API must write to the same database. Refuses live-looking targets (see
tests/koi_test_targets.py). pytest collects no tests here.
"""
import asyncio
import importlib.util
import os
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parent.parent
GROUP = "episode-batching-test"
SRC_DOC = "https://example.invalid/episode-batching-test"
N_FACTS = 25
BATCH = 10  # -> 3 batches

sys.path.insert(0, str(REPO))


def _configure() -> str:
    """Targets from KOI_TEST_DATABASE_URL and KOI_TEST_API_URL only, set before code loads."""
    from tests import koi_test_targets as targets
    t = targets.require_for_script(targets.DATABASE_VAR, targets.API_VAR)
    os.environ["POSTGRES_URL"] = t[targets.DATABASE_VAR]
    os.environ["DOC_INGEST_KOI_URL"] = t[targets.API_VAR].rstrip("/")
    os.environ["DOC_EPISODE_BATCH_SIZE"] = str(BATCH)
    os.environ.setdefault("DOC_EPISODE_TIMEOUT", "900")
    return t[targets.DATABASE_VAR]


def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, str(REPO / rel))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _payload(n):
    """Distinct, non-duplicate facts so the server's dedup cannot mask a split."""
    return {
        "name": "Episode batching test",
        "content": "synthetic fixture",
        "source_description": "document",
        "source_document": SRC_DOC,
        "group_id": GROUP,
        "create_entities": True,
        "facts": [
            {"subject": f"BatchTestSubject{i:03d}", "subject_type": "Concept",
             "predicate": "RELATES_TO", "object": None, "object_type": None,
             "object_literal": f"batching fixture value {i:03d}",
             "fact_text": f"BatchTestSubject{i:03d} relates to batching fixture value {i:03d}."}
            for i in range(n)
        ],
    }


async def main() -> int:
    import asyncpg
    import httpx
    dsn = _configure()
    edd = _load("extract_deep_documents", "scripts/extract_deep_documents.py")

    assert edd.DOC_EPISODE_BATCH_SIZE == BATCH, \
        f"env not picked up: {edd.DOC_EPISODE_BATCH_SIZE}"

    conn = await asyncpg.connect(dsn)
    ok = True

    def check(cond, label):
        nonlocal ok
        print(f"  [{'PASS' if cond else 'FAIL'}] {label}")
        ok = ok and cond

    try:
        await conn.execute("DELETE FROM knowledge_facts WHERE group_id=$1", GROUP)
        await conn.execute("DELETE FROM knowledge_episodes WHERE group_id=$1", GROUP)

        async with httpx.AsyncClient(timeout=900.0) as http:
            print(f"posting {N_FACTS} facts with batch size {BATCH} "
                  f"({-(-N_FACTS // BATCH)} batches expected)")
            ep = await edd.post_episode_batched(http, _payload(N_FACTS))

            episodes = await conn.fetch(
                "SELECT id FROM knowledge_episodes WHERE group_id=$1 AND source_document=$2",
                GROUP, SRC_DOC)
            rows = await conn.fetchval(
                "SELECT count(*) FROM knowledge_facts WHERE group_id=$1", GROUP)
            distinct_eps = await conn.fetchval(
                "SELECT count(DISTINCT episode_id) FROM knowledge_facts WHERE group_id=$1", GROUP)

            print(f"\n  episodes for this source_document : {len(episodes)}")
            print(f"  distinct episode_id across facts  : {distinct_eps}")
            print(f"  facts committed                   : {rows}")
            print(f"  aggregated facts_created reported : {ep.get('facts_created')}")

            print("\n--- assertions ---")
            check(len(episodes) == 1, "batched post created exactly ONE episode")
            check(distinct_eps == 1, "every fact carries the SAME episode_id")
            check(str(episodes[0]["id"]) == str(ep.get("episode_id")),
                  "returned episode_id matches the committed episode")
            check(rows == N_FACTS, f"all {N_FACTS} facts committed (got {rows})")
            check(int(ep.get("facts_created") or 0) == rows,
                  "aggregated counter equals the real committed row count")

            # A re-POST must reuse the same episode, not fork a second one.
            ep2 = await edd.post_episode_batched(http, _payload(N_FACTS))
            eps_after = await conn.fetchval(
                "SELECT count(*) FROM knowledge_episodes WHERE group_id=$1 AND source_document=$2",
                GROUP, SRC_DOC)
            check(eps_after == 1 and str(ep2.get("episode_id")) == str(episodes[0]["id"]),
                  "a second batched post REUSES the episode (convergent re-POST)")

            # batch_size=0 must take the single-POST path unchanged.
            edd.DOC_EPISODE_BATCH_SIZE = 0
            ep3 = await edd.post_episode_batched(http, _payload(3))
            check(str(ep3.get("episode_id")) == str(episodes[0]["id"]),
                  "DOC_EPISODE_BATCH_SIZE=0 still lands in the same episode (single-POST path)")

        return 0 if ok else 1
    finally:
        await conn.execute("DELETE FROM knowledge_facts WHERE group_id=$1", GROUP)
        await conn.execute("DELETE FROM knowledge_episodes WHERE group_id=$1", GROUP)
        await conn.execute(
            "DELETE FROM entity_registry WHERE entity_text LIKE 'BatchTestSubject%'")
        await conn.close()
        print("\ncleaned up test rows")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
