#!/usr/bin/env python3
"""Re-embed NULL-embed facts in personal_koi.knowledge_facts.

A permanent home for what began as an ad-hoc null-embed cleanup script.

When to run
-----------
After any embedding-provider outage or transient failure that causes
fact rows to land with `fact_embedding_3072 IS NULL`. Surface signal:
the `/diagnostics/embedding-health` endpoint or `/health`
exposes `null_embed_fact_count_db` — when that number is non-zero and
the provider is healthy again, run this script to clear the debt.

Usage
-----
    # preview (no writes; the default)
    python scripts/tier-3/reembed-null-facts.py

    # bounded re-embed (--max-records defaults to 500)
    python scripts/tier-3/reembed-null-facts.py --apply --batch-size 50

    # full re-embed (unbounded)
    python scripts/tier-3/reembed-null-facts.py --apply --max-records 0

CLI flags
---------
    --apply          Embed the rows (paid calls with the OpenAI provider) and
                     UPDATE knowledge_facts. Without it the run is a dry run.
    --dry-run        Count rows + show first-3-row preview, then exit (the
                     default; wins over --apply).
    --batch-size N   Commit every N rows (default 100).
    --max-records N  Safety ceiling on rows attempted (default 500; 0 = no
                     limit, which bounds neither spend nor blast radius).
    --db-url URL     Override DB URL. Default: env POSTGRES_URL.

Pre-flight
----------
With --apply: GET http://localhost:8351/health — fail-fast if not healthy. The
KOI API must be up so the embedding provider has a current configuration. A dry
run does not contact the API and reads in a read-only session.

Idempotency
-----------
The UPDATE re-checks `fact_embedding_3072 IS NULL` in its WHERE clause,
so concurrent runs (or repeated runs after partial success) skip rows
that have already been embedded.

Exit codes
----------
    0  Success (any rows attempted, or dry-run, or 0 rows to process).
    1  Pre-flight failure, or 100% of attempted rows failed.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import re
import urllib.error
import urllib.request

# ─── Imports w/ helpful failure messages (checked in main, not at import) ────
try:
    import asyncpg  # type: ignore
except ImportError:
    asyncpg = None

try:
    from dotenv import load_dotenv  # type: ignore
except ImportError:
    load_dotenv = None


def require_dependencies() -> None:
    """Exit 1 with a hint when asyncpg or python-dotenv is missing."""
    for name, mod in (("asyncpg", asyncpg), ("python-dotenv", load_dotenv)):
        if mod is None:
            sys.stderr.write(
                f"ERROR: {name} not available. Run from koi-server venv:\n"
                "  ~/venvs/koi-server/bin/python3 scripts/tier-3/reembed-null-facts.py\n"
            )
            sys.exit(1)


# ─── Config / paths ──────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[2]  # koi-processor/
PERSONAL_ENV = REPO_ROOT / "config" / "personal.env"
HEALTH_URL = "http://localhost:8351/health"
DEFAULT_MAX_RECORDS = 500
READ_ONLY = {"default_transaction_read_only": "on"}

SELECT_NULL_EMBED_SQL = """
    SELECT id, fact_text
      FROM knowledge_facts
     WHERE fact_embedding_3072 IS NULL
       {validity_filter}
       AND fact_text IS NOT NULL
       AND length(fact_text) > 0
     ORDER BY id
"""

UPDATE_EMBED_SQL = """
    UPDATE knowledge_facts
       SET fact_embedding_3072 = $1::vector(3072)
     WHERE id = $2
       AND fact_embedding_3072 IS NULL
"""

COUNT_NULL_EMBED_SQL = """
    SELECT COUNT(*)
      FROM knowledge_facts
     WHERE fact_embedding_3072 IS NULL
       {validity_filter}
       AND fact_text IS NOT NULL
       AND length(fact_text) > 0
"""


# A superseded fact (valid_to IS NOT NULL) is still a ROW: knowledge_facts has no
# delete, the row still returns from any query that does not filter validity, and per
# the router's own warning it still BYPASSES cosine dedup until re-embedded. Excluding
# those rows has made this script (and /health) under-report the backlog several-fold.
# Default stays live-only so existing callers
# are unaffected; --include-superseded repairs the whole backlog.
VALIDITY_LIVE_ONLY = "AND valid_to IS NULL"
VALIDITY_ALL = "-- validity filter disabled (--include-superseded)"


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Re-embed NULL-embed facts in knowledge_facts.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--include-superseded", action="store_true",
        help="also re-embed facts whose valid_to is set. They are still rows and still "
             "bypass cosine dedup; excluding them under-reports the real backlog.")
    p.add_argument(
        "--apply",
        action="store_true",
        help="Embed the rows (paid calls with the OpenAI provider) and UPDATE "
             "knowledge_facts. Without it the run is a dry run.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Count rows + preview first 3, no writes (the default; wins over --apply).",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Commit every N rows (default 100).",
    )
    p.add_argument(
        "--max-records",
        type=int,
        default=DEFAULT_MAX_RECORDS,
        help=f"Safety ceiling on rows attempted (default {DEFAULT_MAX_RECORDS}; 0 = no limit).",
    )
    p.add_argument(
        "--db-url",
        type=str,
        default=None,
        help="Postgres URL (default: env POSTGRES_URL).",
    )
    return p.parse_args(argv)


def mask_url(url: str) -> str:
    """The URL with its password replaced by ***."""
    return re.sub(r"(://[^:/@]*:)[^@]*@", r"\1***@", url or "")


def preflight_health() -> bool:
    """GET /health; return True iff status is 'healthy'."""
    try:
        req = urllib.request.Request(HEALTH_URL)
        with urllib.request.urlopen(req, timeout=5) as resp:
            import json
            payload = json.loads(resp.read().decode("utf-8"))
            if payload.get("status") == "healthy":
                return True
            sys.stderr.write(
                f"ERROR: /health reports status={payload.get('status')!r}\n"
            )
            return False
    except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
        sys.stderr.write(f"ERROR: cannot reach {HEALTH_URL}: {e}\n")
        return False
    except Exception as e:
        sys.stderr.write(f"ERROR: pre-flight failed: {e}\n")
        return False


def resolve_db_url(args: argparse.Namespace) -> str:
    if args.db_url:
        return args.db_url
    # Load personal.env so POSTGRES_URL is available even outside `source`-d shell.
    if PERSONAL_ENV.exists():
        load_dotenv(PERSONAL_ENV)
    url = os.environ.get("POSTGRES_URL")
    if not url:
        sys.stderr.write(
            "ERROR: POSTGRES_URL not set. Use --db-url or "
            "`source config/personal.env` first.\n"
        )
        sys.exit(1)
    return url


async def get_provider():
    """Lazy import of api.embedding_provider so the CLI fails fast on env issues
    before paying the import cost."""
    # Make repo root importable
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from api.embedding_provider import create_embedding_provider  # type: ignore
    provider = create_embedding_provider()
    if provider is None:
        sys.stderr.write(
            "ERROR: create_embedding_provider() returned None. "
            "Check EMBEDDING_PROVIDER + OPENAI_API_KEY in personal.env.\n"
        )
        sys.exit(1)
    return provider


async def run(args: argparse.Namespace) -> int:
    dry_run = args.dry_run or not getattr(args, "apply", False)
    db_url = resolve_db_url(args)
    print(f"database: {mask_url(db_url)}")
    vf = VALIDITY_ALL if getattr(args, "include_superseded", False) else VALIDITY_LIVE_ONLY
    count_sql = COUNT_NULL_EMBED_SQL.format(validity_filter=vf)
    select_sql = SELECT_NULL_EMBED_SQL.format(validity_filter=vf)
    if getattr(args, "include_superseded", False):
        print("scope: ALL null-embed facts, INCLUDING superseded (valid_to IS NOT NULL)")
    else:
        print("scope: live facts only (valid_to IS NULL) — "
              "use --include-superseded to repair the full backlog")

    if dry_run:
        print(f"dry run: {HEALTH_URL} not checked; read-only session")
        conn = await asyncpg.connect(db_url, server_settings=READ_ONLY)
    else:
        if not preflight_health():
            return 1

        conn = await asyncpg.connect(db_url)
    try:
        total = await conn.fetchval(count_sql)
        print(f"NULL-embed fact rows found: {total}")

        if total == 0:
            print("DONE. ok=0 fail=0 of 0. NULL-embed remaining: 0")
            return 0

        if dry_run:
            preview = await conn.fetch(select_sql + " LIMIT 3")
            print("── DRY-RUN preview (first 3 rows) ──")
            for r in preview:
                snippet = (r["fact_text"] or "")[:100].replace("\n", " ")
                print(f"  id={r['id']}  fact_text={snippet!r}")
            cap = args.max_records if args.max_records is not None and args.max_records > 0 else None
            would = min(total, cap) if cap else total
            print(f"DONE (dry-run). would-attempt={would} of {total} "
                  f"(--max-records {args.max_records or 0}; 0 = no limit). Pass --apply to embed.")
            return 0

        provider = await get_provider()

        rows = await conn.fetch(select_sql)
        if args.max_records is not None and args.max_records > 0:
            rows = rows[: args.max_records]

        n = len(rows)
        ok = 0
        fail = 0
        batch_buf: list[tuple[list[float], int]] = []

        for i, row in enumerate(rows, start=1):
            fact_id = row["id"]
            fact_text = row["fact_text"]
            try:
                emb = await provider.embed_or_none(
                    fact_text, prompt_type="extraction"
                )
                if emb is None:
                    fail += 1
                else:
                    # pgvector text format: '[f1,f2,...]'
                    vec_text = "[" + ",".join(repr(float(x)) for x in emb) + "]"
                    batch_buf.append((vec_text, fact_id))
                    ok += 1
            except Exception as e:
                fail += 1
                sys.stderr.write(f"  fail id={fact_id}: {e}\n")

            if len(batch_buf) >= args.batch_size:
                async with conn.transaction():
                    for vec_text, fid in batch_buf:
                        await conn.execute(UPDATE_EMBED_SQL, vec_text, fid)
                batch_buf.clear()

            if i % 100 == 0 or i == n:
                print(f"[{i}/{n}] ok={ok} fail={fail}")

        if batch_buf:
            async with conn.transaction():
                for vec_text, fid in batch_buf:
                    await conn.execute(UPDATE_EMBED_SQL, vec_text, fid)
            batch_buf.clear()

        remaining = await conn.fetchval(count_sql)
        print(f"DONE. ok={ok} fail={fail} of {n}. NULL-embed remaining: {remaining}")

        # Exit 1 only if we attempted >0 and 100% failed.
        if n > 0 and ok == 0:
            return 1
        return 0
    finally:
        await conn.close()


def main(argv=None) -> int:
    args = parse_args(argv)
    require_dependencies()
    try:
        return asyncio.run(run(args))
    except KeyboardInterrupt:
        sys.stderr.write("\nINTERRUPTED\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
