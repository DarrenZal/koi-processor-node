#!/usr/bin/env python3
"""Fold prefixed / non-canonical entity_type spellings to their canonical type.

Enumerates live entity_registry rows whose ``entity_type`` is not already the
canonical schema ``type_key`` (e.g. ``schema:SoftwareApplication`` -> ``Software
Application``, ``bkc:Concept`` -> ``Concept``, ``organization`` -> ``Organization``)
and, for each, calls ``POST /entities/retype`` with the canonical target. The
retype endpoint mints the correctly-typed URI, redirects the old URI, rewires
references, and updates entity_rid_mappings.entity_type — see api/routers/admin_router.py.

Report-first and idempotent:
  * Default: enumerate + print what WOULD fold (no writes). Add --json for a
    machine-readable report.
  * --apply --confirm: actually call the retype endpoint for each row.
  * Re-running after --apply is a no-op: folded rows now carry the canonical
    type and no longer match the enumeration filter.

Auth: the retype endpoint is service-token gated. Set KOI_CLAIMS_SERVICE_TOKEN
(the same token the service uses) so --apply can authenticate.

Usage:
    python scripts/fold_prefixed_types.py                 # report (human)
    python scripts/fold_prefixed_types.py --json          # report (JSON)
    python scripts/fold_prefixed_types.py --apply --confirm
    python scripts/fold_prefixed_types.py --apply --confirm --dry-run-endpoint

Options:
    --json                 Emit the report as JSON.
    --apply                Perform retypes (requires --confirm).
    --confirm              Safety interlock required with --apply.
    --dry-run-endpoint     With --apply: call the endpoint in dry_run mode
                           (server does the work then rolls back) — a live
                           preview that writes nothing.
    --base-url URL         Backend base URL (default $KOI_API_URL or :8351).
    --dsn DSN              Postgres DSN (default $POSTGRES_URL).
    --limit N              Only process the first N rows (0 = all).
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import asyncpg
import httpx

sys.path.insert(0, str(Path(__file__).parent.parent))

from api.entity_schema import canonicalize_entity_type, get_entity_schemas  # noqa: E402

DB_URL = os.getenv("POSTGRES_URL", "postgresql://koi:@localhost:5432/personal_koi")
BASE_URL = os.getenv("KOI_API_URL", os.getenv("PERSONAL_KOI_API_URL", "http://localhost:8351"))


async def enumerate_foldable(dsn: str, limit: int = 0):
    """Return (foldable, blocked) live entity_registry rows with non-canonical types.

    A row's stored ``entity_type`` differs from ``canonicalize_entity_type(...)``:
      * foldable — the canonical target is a known schema type_key, so the retype
        endpoint will accept it.
      * blocked  — canonicalization only strips a prefix but the result is NOT a
        registered schema type_key (e.g. ``schema:SoftwareApplication`` when no
        ``SoftwareApplication`` schema exists). The retype endpoint's 422 guard
        would refuse these; they need a schema definition first, so we surface
        them separately rather than attempt (and fail) a fold.
    """
    # Warm the schema registry so canonicalize_entity_type resolves aliases.
    schema_keys = set(get_entity_schemas().keys())
    conn = await asyncpg.connect(dsn)
    try:
        rows = await conn.fetch(
            "SELECT fuseki_uri, entity_text, entity_type "
            "FROM entity_registry "
            "WHERE merged_into IS NULL AND entity_type IS NOT NULL "
            "ORDER BY entity_type, entity_text")
    finally:
        await conn.close()

    foldable, blocked = [], []
    for r in rows:
        raw = r["entity_type"]
        canon = canonicalize_entity_type(raw)
        if canon == raw:
            continue
        rec = {
            "uri": r["fuseki_uri"],
            "name": r["entity_text"],
            "from_type": raw,
            "to_type": canon,
        }
        (foldable if canon in schema_keys else blocked).append(rec)
    if limit:
        foldable = foldable[:limit]
    return foldable, blocked


def summarize(foldable):
    by_pair = {}
    for f in foldable:
        key = f"{f['from_type']} -> {f['to_type']}"
        by_pair[key] = by_pair.get(key, 0) + 1
    return by_pair


async def apply_retypes(foldable, base_url: str, token: str, dry_run_endpoint: bool):
    headers = {"Authorization": f"Bearer {token}"}
    results = []
    async with httpx.AsyncClient(base_url=base_url, timeout=60.0, headers=headers) as client:
        for f in foldable:
            payload = {
                "uri": f["uri"],
                "new_type": f["to_type"],
                "retyped_by": "fold_prefixed_types.py",
                "dry_run": dry_run_endpoint,
            }
            try:
                resp = await client.post("/entities/retype", json=payload)
                ok = resp.status_code == 200
                body = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
                results.append({
                    **f,
                    "status_code": resp.status_code,
                    "ok": ok,
                    "new_uri": body.get("new_uri"),
                    "merged_into_existing": body.get("merged_into_existing"),
                    "already_typed": body.get("already_typed"),
                    "detail": None if ok else (body.get("detail") or resp.text[:200]),
                })
            except Exception as e:  # noqa: BLE001
                results.append({**f, "status_code": None, "ok": False, "detail": str(e)})
    return results


async def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--json", action="store_true", help="Emit report as JSON")
    ap.add_argument("--apply", action="store_true", help="Perform retypes (needs --confirm)")
    ap.add_argument("--confirm", action="store_true", help="Safety interlock for --apply")
    ap.add_argument("--dry-run-endpoint", action="store_true",
                    help="With --apply: call retype in dry_run mode (writes nothing)")
    ap.add_argument("--base-url", default=BASE_URL)
    ap.add_argument("--dsn", default=DB_URL)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    foldable, blocked = await enumerate_foldable(args.dsn, args.limit)
    summary = summarize(foldable)
    blocked_summary = summarize(blocked)

    if not args.apply:
        report = {
            "total_foldable": len(foldable),
            "by_pair": summary,
            "rows": foldable,
            "total_blocked": len(blocked),
            "blocked_by_pair": blocked_summary,
            "blocked_rows": blocked,
        }
        if args.json:
            print(json.dumps(report, indent=2))
        else:
            print(f"Foldable rows: {len(foldable)}")
            for pair, n in sorted(summary.items(), key=lambda kv: -kv[1]):
                print(f"  {n:4d}  {pair}")
            if blocked:
                print(f"\nBlocked rows (target not a registered schema type — "
                      f"endpoint would 422; needs a schema def first): {len(blocked)}")
                for pair, n in sorted(blocked_summary.items(), key=lambda kv: -kv[1]):
                    print(f"  {n:4d}  {pair}")
            print("\nRun with --apply --confirm to fold (or --dry-run-endpoint for a live preview).")
        return 0

    if not args.confirm:
        print("ERROR: --apply requires --confirm", file=sys.stderr)
        return 2

    token = os.getenv("KOI_CLAIMS_SERVICE_TOKEN", "")
    if not token:
        print("ERROR: KOI_CLAIMS_SERVICE_TOKEN not set (retype endpoint is service-token gated)",
              file=sys.stderr)
        return 2

    results = await apply_retypes(foldable, args.base_url, token, args.dry_run_endpoint)
    ok = sum(1 for r in results if r["ok"])
    failed = [r for r in results if not r["ok"]]
    out = {
        "attempted": len(results),
        "ok": ok,
        "failed": len(failed),
        "dry_run_endpoint": args.dry_run_endpoint,
        "results": results,
    }
    if args.json:
        print(json.dumps(out, indent=2))
    else:
        print(f"Retyped: {ok}/{len(results)} ok"
              + (" (endpoint dry_run — nothing committed)" if args.dry_run_endpoint else ""))
        for r in failed:
            print(f"  FAILED {r['from_type']} -> {r['to_type']} {r['uri']}: {r.get('detail')}")
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
