#!/usr/bin/env python3
"""Block exact records from federation (migration 137), from a hash-only manifest.

    python scripts/federation/block_rids.py --manifest M.json            # report, writes nothing
    python scripts/federation/block_rids.py --manifest M.json --apply    # one transaction

The manifest lists records as {"sha256": <hex of the exact identifier>, "kind": <kind>} with a
"reason" and "expected" counts per kind. Kinds:
    task            task_registry.task_key
    entity          entity_registry.fuseki_uri
    entity-koi-rid  entity_registry.koi_rid
    doclink         orn:personal-koi.doclink:<sha256(document_rid || entity_uri)[:16]>, as
                    api.federation_events.doclink_rid builds it, from a live link or from an event
                    in koi_net_events (a link whose entity was merged away here has moved to the
                    survivor, but a peer still holds the old pair)
Each hash must match exactly one local record of its kind. A missing, duplicated or miscounted record
refuses and writes nothing. Output carries counts and hash prefixes only, because some identifiers
carry a name.
"""

import argparse
import asyncio
import json
import os
import re
import sys

import asyncpg

DB_URL = os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")

_H = "encode(sha256(convert_to({}, 'UTF8')), 'hex')"
MATCH_SQL = {
    "task": f"SELECT {_H.format('task_key')} AS h FROM task_registry WHERE {_H.format('task_key')} = ANY($1::text[])",
    "entity": f"SELECT {_H.format('fuseki_uri')} AS h FROM entity_registry WHERE {_H.format('fuseki_uri')} = ANY($1::text[])",
    "entity-koi-rid": f"SELECT {_H.format('koi_rid')} AS h FROM entity_registry WHERE {_H.format('koi_rid')} = ANY($1::text[])",
    "doclink": "SELECT DISTINCT h FROM (SELECT " + _H.format(
        "'orn:personal-koi.doclink:' || left(" + _H.format("document_rid || entity_uri") + ", 16)")
        + " AS h FROM document_entity_links UNION ALL SELECT " + _H.format("rid")
        + " FROM koi_net_events WHERE rid LIKE 'orn:personal-koi.doclink:%') d WHERE h = ANY($1::text[])",
}


class Refused(Exception):
    pass


def load_manifest(path):
    m = json.load(open(path))
    recs, expected, reason = m.get("records"), m.get("expected"), m.get("reason")
    if not isinstance(recs, list) or not isinstance(expected, dict) or not reason:
        raise Refused("manifest needs records, expected and reason")
    by_kind = {}
    for r in recs:
        h, k = r.get("sha256", ""), r.get("kind")
        if not re.fullmatch(r"[0-9a-f]{64}", h) or k not in MATCH_SQL:
            raise Refused(f"bad record {h[:12]!r} kind {k!r}")
        by_kind.setdefault(k, []).append(h)
    if len({r["sha256"] for r in recs}) != len(recs):
        raise Refused("duplicate hash in manifest")
    counts = {k: len(v) for k, v in by_kind.items()}
    if counts != {k: v for k, v in expected.items() if v}:
        raise Refused(f"manifest counts {counts} differ from expected {expected}")
    return by_kind, reason


async def run(manifest_path, apply):
    by_kind, reason = load_manifest(manifest_path)
    conn = await asyncpg.connect(DB_URL)
    try:
        if not await conn.fetchval("SELECT to_regclass('federation_rid_blocks') IS NOT NULL"):
            raise Refused("federation_rid_blocks is missing; apply migration 137 first")
        async with conn.transaction():
            for kind, hashes in by_kind.items():
                found = [r["h"] for r in await conn.fetch(MATCH_SQL[kind], hashes)]
                missing = sorted(set(hashes) - set(found))
                dup = sorted({h for h in found if found.count(h) > 1})
                if missing or dup:
                    raise Refused(f"{kind}: {len(missing)} not found {[h[:12] for h in missing]}, "
                                  f"{len(dup)} matched more than once {[h[:12] for h in dup]}")
                print(f"{kind}: {len(hashes)} of {len(hashes)} match exactly one local record")
            every = [h for v in by_kind.values() for h in v]
            already = await conn.fetchval("SELECT count(*) FROM federation_rid_blocks WHERE rid_sha256 = ANY($1::text[])", every)
            print(f"requested {len(every)}, already blocked {already}")
            if not apply:
                print("report only; rerun with --apply")
                return
            kind_of = {h: k for k, v in by_kind.items() for h in v}
            await conn.executemany(
                "INSERT INTO federation_rid_blocks (rid_sha256, kind, reason) VALUES ($1, $2, $3)"
                " ON CONFLICT (rid_sha256) DO NOTHING",
                [(h, kind_of[h], reason) for h in every],
            )
            now = await conn.fetchval("SELECT count(*) FROM federation_rid_blocks WHERE rid_sha256 = ANY($1::text[])", every)
            if now != len(every):
                raise Refused(f"after insert {now} of {len(every)} are blocked; rolled back")
            print(f"blocked {len(every) - already} new, {now} of {len(every)} now blocked")
    finally:
        await conn.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--manifest", required=True)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    try:
        asyncio.run(run(a.manifest, a.apply))
    except Refused as e:
        print(f"REFUSED, nothing written: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
