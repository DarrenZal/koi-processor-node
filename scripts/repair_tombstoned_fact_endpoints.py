#!/usr/bin/env python3
"""Rewrite knowledge_facts endpoints that point at tombstoned entities.

`/entities/merge` tombstones the loser and rewires the references it knows about. The
resolution path used by `POST /knowledge/add` did not follow merged_into, so facts
ingested AFTER a merge could be written against the dead URI. Those rows are invisible
when you query the surviving entity — the fact exists, is embedded, is retrievable by
text, and simply is not attached to the thing it is about.

Typical example: a fact such as "Neo4j is a graph database…" bound to a tombstoned
duplicate of `Neo4j` rather than to the surviving entity.

This repairs the rows. The code fix that stops new ones is in knowledge_router's
`_resolve_or_create` (all four tiers now route through `_accept`, which follows the
merge) and in `resolve_entity_multi_tier`, which now guarantees a live URI at a single
choke point. Repairing without that fix would just refill.

Resolution is TRANSITIVE — merges chain, so a registry row can be two or more hops from
a live entity, and a single-hop rewrite lands on another tombstone and looks like it
worked.

Dry-run by default. `--apply` writes, inside one transaction, after copying every
affected row to a timestamped backup table.

    python3 scripts/repair_tombstoned_fact_endpoints.py            # show the plan
    python3 scripts/repair_tombstoned_fact_endpoints.py --apply
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

import asyncpg

# Terminal live URI for every entity, following merged_into transitively.
# DISTINCT ON (orig) ... ORDER BY orig, d DESC takes the deepest hop reached, which is the
# terminal one; rows that are already live appear at d=0 and map to themselves.
LIVE_MAP = """
WITH RECURSIVE live AS (
    SELECT fuseki_uri AS orig, fuseki_uri AS cur, merged_into, 0 AS d
      FROM entity_registry
    UNION ALL
    SELECT l.orig, e.fuseki_uri, e.merged_into, l.d + 1
      FROM live l
      JOIN entity_registry e ON e.fuseki_uri = l.merged_into
     WHERE l.d < 10
)
SELECT DISTINCT ON (orig) orig, cur
  FROM live
 WHERE merged_into IS NULL
 ORDER BY orig, d DESC
"""

PLAN = f"""
WITH term AS ({LIVE_MAP})
SELECT f.id,
       f.subject_uri, ts.cur AS new_subject,
       f.object_uri,  tobj.cur AS new_object,
       left(f.fact_text, 70) AS fact
  FROM knowledge_facts f
  JOIN term ts   ON ts.orig  = f.subject_uri
  JOIN term tobj ON tobj.orig = f.object_uri
 WHERE ts.cur <> f.subject_uri OR tobj.cur <> f.object_uri
 ORDER BY f.id
"""


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write (default is dry-run)")
    args = ap.parse_args()

    dsn = os.environ.get("POSTGRES_URL")
    if not dsn:
        print("POSTGRES_URL not set; source config/personal.env first", file=sys.stderr)
        return 2

    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(PLAN)
        if not rows:
            print("no facts bound to tombstoned entities. Nothing to repair.")
            return 0

        print(f"{len(rows)} fact(s) point at a tombstoned entity:\n")
        for r in rows:
            print(f"  {str(r['id'])[:8]}  {r['fact']}")
            if r["subject_uri"] != r["new_subject"]:
                print(f"      subject {r['subject_uri']}\n           -> {r['new_subject']}")
            if r["object_uri"] != r["new_object"]:
                print(f"      object  {r['object_uri']}\n           -> {r['new_object']}")

        if not args.apply:
            print(f"\nDRY RUN. Re-run with --apply to rewrite these {len(rows)} row(s).")
            return 0

        # Backup BEFORE the update and inside the same transaction, so a failure leaves
        # neither a half-rewrite nor a backup that describes a state that never existed.
        backup = "knowledge_facts_backup_tombstone_repair"
        async with conn.transaction():
            await conn.execute(f"DROP TABLE IF EXISTS {backup}")
            await conn.execute(f"""
                CREATE TABLE {backup} AS
                SELECT f.* FROM knowledge_facts f WHERE f.id = ANY($1::uuid[])
            """, [r["id"] for r in rows])
            result = await conn.execute(f"""
                WITH term AS ({LIVE_MAP})
                UPDATE knowledge_facts f
                   SET subject_uri = ts.cur,
                       object_uri  = tobj.cur
                  FROM term ts, term tobj
                 WHERE ts.orig = f.subject_uri
                   AND tobj.orig = f.object_uri
                   AND (ts.cur <> f.subject_uri OR tobj.cur <> f.object_uri)
            """)
            print(f"\n{result}  (backup: {backup})")

        # Verify inside the same connection but after commit: assert the condition the
        # script exists to eliminate is actually gone, rather than trusting the row count.
        left = await conn.fetchval(f"WITH term AS ({LIVE_MAP}) "
                                   "SELECT count(*) FROM knowledge_facts f "
                                   "JOIN term ts ON ts.orig = f.subject_uri "
                                   "JOIN term tobj ON tobj.orig = f.object_uri "
                                   "WHERE ts.cur <> f.subject_uri OR tobj.cur <> f.object_uri")
        print(f"remaining facts on tombstoned endpoints: {left}")
        return 0 if left == 0 else 1
    finally:
        await conn.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
