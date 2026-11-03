#!/usr/bin/env python3
"""One-time sweep: normalize every entity_registry.aliases array to
`normalize_alias()` form.

Read-only by default; `--apply` writes. Run as a MODULE from the repo root so
the `api.resolution_primitives` import resolves:

    cd <repo> && <venv>/bin/python -m scripts.normalize_existing_aliases \
        --confirm-target laptop --expect-hostname "$(hostname)"           # dry run
    cd <repo> && <venv>/bin/python -m scripts.normalize_existing_aliases \
        --confirm-target laptop --expect-hostname "$(hostname)" --apply    # write

Safety:
  - REQUIRES --confirm-target {laptop|nuc} AND --expect-hostname; the OS hostname
    (socket.gethostname()) must match --expect-hostname (the real disambiguator
    when several nodes each name their database personal_koi and connect over
    local sockets, so inet_server_addr() is NULL on each).
  - DB guard: current_database()='personal_koi'.
  - Backup: column-scoped, microsecond-timestamped table, never dropped; SKIPPED
    when there are 0 changes.
  - Writes use compare-and-swap (UPDATE ... WHERE fuseki_uri=$1 AND aliases=$2);
    if a concurrent writer changed the row, re-read + retry (bounded) — never
    blind-overwrite.
  - Sweeps ALL rows (including tombstones) so 100% of stored aliases are normalized.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import socket
import sys

import asyncpg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from api.resolution_primitives import normalize_alias_list  # noqa: E402

MAX_RETRIES = 5


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--confirm-target", required=True, choices=["laptop", "nuc"])
    ap.add_argument("--expect-hostname", required=True,
                    help="OS hostname this run must be executing on (capture via `hostname`)")
    ap.add_argument("--dsn", default=os.environ.get("DATABASE_URL") or "postgresql:///personal_koi")
    ap.add_argument("--apply", action="store_true", help="write changes (default: read-only)")
    args = ap.parse_args()

    actual_host = socket.gethostname()
    if actual_host != args.expect_hostname:
        sys.exit(f"HOSTNAME MISMATCH: running on '{actual_host}' but --expect-hostname "
                 f"'{args.expect_hostname}' (target={args.confirm_target}). Refusing to run.")

    conn = await asyncpg.connect(args.dsn)
    try:
        dbname = await conn.fetchval("SELECT current_database()")
        dbuser = await conn.fetchval("SELECT current_user")
        srv_addr = await conn.fetchval("SELECT inet_server_addr()")
        print(f"target={args.confirm_target} host={actual_host} db={dbname} user={dbuser} "
              f"server_addr={srv_addr} (NULL=local socket) | mode={'APPLY' if args.apply else 'READ-ONLY'}")
        if dbname != "personal_koi":
            sys.exit(f"DB GUARD: current_database()='{dbname}' != 'personal_koi'. Refusing.")

        rows = await conn.fetch(
            "SELECT fuseki_uri, aliases FROM entity_registry "
            "WHERE aliases IS NOT NULL AND array_length(aliases, 1) > 0"
        )
        changed = []
        for r in rows:
            cur = list(r["aliases"])
            new = normalize_alias_list(cur)
            if new != cur:
                changed.append((r["fuseki_uri"], cur, new))

        total = await conn.fetchval("SELECT count(*) FROM entity_registry")
        print(f"entity_registry rows: {total} | rows with aliases: {len(rows)} | "
              f"would-change: {len(changed)}")
        for uri, cur, new in changed[:10]:
            print(f"  {uri}\n     {cur}  ->  {new}")
        if len(changed) > 10:
            print(f"  … and {len(changed) - 10} more")

        if not args.apply:
            print("READ-ONLY: no writes. Re-run with --apply to write.")
            return 0

        if not changed:
            print("0 changes — skipping backup (idempotent no-op).")
            return 0

        # Microsecond-precision, column-scoped backup (never dropped).
        ts = (await conn.fetchval("SELECT to_char(clock_timestamp(), 'YYYYMMDD_HH24MISSUS')"))
        bak = f"entity_registry_aliasbak_{ts}"
        await conn.execute(
            f'CREATE TABLE "{bak}" AS SELECT fuseki_uri, aliases FROM entity_registry'
        )
        bak_count = await conn.fetchval(f'SELECT count(*) FROM "{bak}"')
        print(f"backup: {bak} ({bak_count} rows; must equal {total})")
        assert bak_count == total, "backup row count != entity_registry row count"

        applied = retried = failed = 0
        for uri, cur, new in changed:
            expected = cur
            for attempt in range(MAX_RETRIES):
                status = await conn.execute(
                    "UPDATE entity_registry SET aliases = $1 "
                    "WHERE fuseki_uri = $2 AND aliases = $3",
                    new, uri, expected,
                )
                if status.endswith(" 1"):
                    applied += 1
                    break
                # 0 rows: concurrent writer changed it → re-read, recompute, retry.
                fresh = await conn.fetchval(
                    "SELECT aliases FROM entity_registry WHERE fuseki_uri = $1", uri
                )
                if fresh is None:
                    failed += 1
                    break
                expected = list(fresh)
                new = normalize_alias_list(expected)
                if new == expected:
                    break  # already normalized by the concurrent write
                retried += 1
            else:
                failed += 1
        print(f"APPLIED: {applied} updated, {retried} CAS-retries, {failed} failed")
        if failed:
            print("WARNING: some rows failed after retries — re-run to converge.")
        return 1 if failed else 0
    finally:
        await conn.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
