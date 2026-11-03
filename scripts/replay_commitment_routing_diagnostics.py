#!/usr/bin/env python3
"""Replay recent commitments through routing diagnostics.

This script gives the routing-diagnostic PR a baseline denominator: for recent
commitments, which diagnostic codes would have fired and how often?

It replays persisted commitments as drafts because routing attempts are not
currently logged as first-class events.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from datetime import datetime
import json
import os
from pathlib import Path
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import asyncpg  # noqa: E402

from api.routers.commitment_router import (  # noqa: E402
    RoutingSuggestionRequest,
    _draft_level_routing_diagnostics,
    _score_pools,
)


def _loads_jsonb(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        return json.loads(value)
    if isinstance(value, dict):
        return value
    return {}


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _as_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


class _PreCapacityFixPool:
    """Pool wrapper that simulates the pre-fix capacity fallback bug.

    Before the fix, ``remaining_capacity_usd: 0`` was treated as falsey and
    fell back to ``capacity_usd``. This wrapper lets replay report before/after
    diagnostic differences without duplicating the whole scorer.
    """

    def __init__(self, pool):
        self.pool = pool

    def acquire(self):
        return _PreCapacityFixAcquire(self.pool.acquire())


class _PreCapacityFixAcquire:
    def __init__(self, acquire_cm):
        self.acquire_cm = acquire_cm
        self.conn = None

    async def __aenter__(self):
        self.conn = await self.acquire_cm.__aenter__()
        return _PreCapacityFixConn(self.conn)

    async def __aexit__(self, exc_type, exc, tb):
        return await self.acquire_cm.__aexit__(exc_type, exc, tb)


class _PreCapacityFixConn:
    def __init__(self, conn):
        self.conn = conn

    async def fetch(self, query, *args):
        rows = await self.conn.fetch(query, *args)
        if "FROM commitment_pools" not in query:
            return rows
        return [self._simulate_pool_row(row) for row in rows]

    async def fetchval(self, query, *args):
        return await self.conn.fetchval(query, *args)

    @staticmethod
    def _simulate_pool_row(row):
        data = dict(row)
        meta = _loads_jsonb(data.get("metadata"))
        capacity = _as_float(meta.get("capacity_usd"))
        raw_remaining = meta.get("remaining_capacity_usd")
        if raw_remaining not in (None, "") and _as_float(raw_remaining) == 0 and capacity > 0:
            meta["remaining_capacity_usd"] = capacity
            data["metadata"] = meta
        return data


async def _load_recent_commitments(
    conn,
    days: int,
    limit: int,
    bioregion_uri: str | None,
) -> list[asyncpg.Record]:
    table_check = await conn.fetchrow(
        "SELECT to_regclass('commitments') AS commitments, "
        "to_regclass('commitment_pools') AS commitment_pools"
    )
    missing = [
        name
        for name in ("commitments", "commitment_pools")
        if not table_check or table_check[name] is None
    ]
    if missing:
        raise RuntimeError(
            "database is missing required table(s): "
            + ", ".join(missing)
            + "; point --db-url at a KOI database with commitment migrations applied"
        )

    params: list[Any] = [days]
    conditions = ["c.created_at >= NOW() - ($1::int * INTERVAL '1 day')"]

    if bioregion_uri:
        params.append(bioregion_uri)
        idx = len(params)
        conditions.append(
            f"(c.metadata->>'bioregion_uri' = ${idx} OR cp.bioregion_uri = ${idx})"
        )

    params.append(limit)
    limit_idx = len(params)
    where = " AND ".join(conditions)
    return await conn.fetch(
        f"""
        SELECT
            c.commitment_rid,
            c.pledger_uri,
            c.title,
            c.offer_type,
            c.quantity,
            c.unit,
            c.validity_start,
            c.validity_end,
            c.metadata,
            c.created_at,
            cp.pool_rid AS current_pool_rid,
            cp.name AS current_pool_name
        FROM commitments c
        LEFT JOIN commitment_pools cp ON cp.id = c.pool_id
        WHERE {where}
        ORDER BY c.created_at DESC
        LIMIT ${limit_idx}
        """,
        *params,
    )


def _row_to_draft(row: asyncpg.Record) -> RoutingSuggestionRequest:
    return RoutingSuggestionRequest(
        pledger_uri=row["pledger_uri"],
        title=row["title"],
        offer_type=row["offer_type"],
        quantity=float(row["quantity"]) if row["quantity"] is not None else None,
        unit=row["unit"],
        validity_start=row["validity_start"],
        validity_end=row["validity_end"],
        metadata=_loads_jsonb(row["metadata"]),
    )


def _count_suggestions(
    suggestions,
    pool_counts: Counter[str],
    effect_counts: Counter[str],
) -> None:
    if not suggestions:
        effect_counts["no_pool_suggestions"] += 1
        return

    for suggestion in suggestions:
        effect_counts["suggestion_pairs"] += 1
        if suggestion.recommended:
            effect_counts["recommended_pairs"] += 1
        if suggestion.total_score == 0:
            effect_counts["zero_score_pairs"] += 1
        if suggestion.hard_excludes:
            effect_counts["hard_excluded_pairs"] += 1

        for diagnostic in suggestion.diagnostics:
            pool_counts[f"{diagnostic.kind}:{diagnostic.code}"] += 1
            if diagnostic.kind in {"score_degradation", "review_gap"}:
                effect_counts["review_or_score_degraded_pairs"] += 1


def _count_capacity_fix_delta(post_fix_suggestions, pre_fix_suggestions, delta_counts: Counter[str]) -> None:
    pre_by_pool = {suggestion.pool_rid: suggestion for suggestion in pre_fix_suggestions}
    pre_recommended = {s.pool_rid for s in pre_fix_suggestions if s.recommended}
    post_recommended = {s.pool_rid for s in post_fix_suggestions if s.recommended}

    if pre_recommended != post_recommended:
        delta_counts["recommended_set_changed"] += 1

    for post_fix in post_fix_suggestions:
        pre_fix = pre_by_pool.get(post_fix.pool_rid)
        pre_excludes = set(pre_fix.hard_excludes) if pre_fix else set()
        for code in post_fix.hard_excludes:
            if code not in pre_excludes:
                delta_counts[f"new_after_capacity_fix:{code}"] += 1


async def replay(args: argparse.Namespace) -> dict[str, Any]:
    pool = await asyncpg.create_pool(args.db_url, min_size=1, max_size=5)
    try:
        async with pool.acquire() as conn:
            rows = await _load_recent_commitments(
                conn,
                days=args.days,
                limit=args.limit,
                bioregion_uri=args.bioregion_uri,
            )

        draft_counts: Counter[str] = Counter()
        pool_counts: Counter[str] = Counter()
        effect_counts: Counter[str] = Counter()
        pre_fix_pool_counts: Counter[str] = Counter()
        pre_fix_effect_counts: Counter[str] = Counter()
        capacity_fix_delta_counts: Counter[str] = Counter()
        records: list[dict[str, Any]] = []

        for row in rows:
            draft = _row_to_draft(row)
            draft_diagnostics = _draft_level_routing_diagnostics(draft)
            for diagnostic in draft_diagnostics:
                draft_counts[f"{diagnostic.kind}:{diagnostic.code}"] += 1

            suggestions = await _score_pools(pool, draft)
            pre_fix_suggestions = await _score_pools(_PreCapacityFixPool(pool), draft)
            if args.pool_name_contains:
                needle = args.pool_name_contains.lower()
                suggestions = [s for s in suggestions if needle in s.pool_name.lower()]
                pre_fix_suggestions = [
                    s for s in pre_fix_suggestions if needle in s.pool_name.lower()
                ]

            _count_suggestions(suggestions, pool_counts, effect_counts)
            _count_suggestions(pre_fix_suggestions, pre_fix_pool_counts, pre_fix_effect_counts)
            _count_capacity_fix_delta(
                suggestions,
                pre_fix_suggestions,
                capacity_fix_delta_counts,
            )

            top_suggestion = suggestions[0] if suggestions else None
            pre_fix_top_suggestion = pre_fix_suggestions[0] if pre_fix_suggestions else None
            record = {
                "commitment_rid": row["commitment_rid"],
                "title": row["title"],
                "created_at": row["created_at"],
                "current_pool_rid": row["current_pool_rid"],
                "current_pool_name": row["current_pool_name"],
                "draft_diagnostics": [d.code for d in draft_diagnostics],
                "suggestion_count": len(suggestions),
                "top_pool_rid": top_suggestion.pool_rid if top_suggestion else None,
                "top_pool_name": top_suggestion.pool_name if top_suggestion else None,
                "top_score": top_suggestion.total_score if top_suggestion else None,
                "pre_fix_top_pool_rid": pre_fix_top_suggestion.pool_rid if pre_fix_top_suggestion else None,
                "pre_fix_top_score": pre_fix_top_suggestion.total_score if pre_fix_top_suggestion else None,
                "recommended_pool_rids": [s.pool_rid for s in suggestions if s.recommended],
                "pre_fix_recommended_pool_rids": [
                    s.pool_rid for s in pre_fix_suggestions if s.recommended
                ],
                "hard_excluded_pool_rids": [
                    s.pool_rid for s in suggestions if s.hard_excludes
                ],
            }
            if args.include_records:
                records.append(record)

        return {
            "scope": {
                "days": args.days,
                "limit": args.limit,
                "bioregion_uri": args.bioregion_uri,
                "pool_name_contains": args.pool_name_contains,
                "replay_source": "persisted_commitments",
                "note": "Routing attempts are not currently logged; persisted commitments were replayed as draft inputs.",
            },
            "commitments_replayed": len(rows),
            "draft_diagnostic_counts": dict(sorted(draft_counts.items())),
            "pool_diagnostic_counts": dict(sorted(pool_counts.items())),
            "effect_counts": dict(sorted(effect_counts.items())),
            "pre_capacity_fix_simulation": {
                "pool_diagnostic_counts": dict(sorted(pre_fix_pool_counts.items())),
                "effect_counts": dict(sorted(pre_fix_effect_counts.items())),
            },
            "capacity_fix_delta_counts": dict(sorted(capacity_fix_delta_counts.items())),
            "records": records,
        }
    finally:
        await pool.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay recent commitments through routing diagnostics."
    )
    parser.add_argument(
        "--db-url",
        default=os.getenv("DATABASE_URL") or os.getenv("POSTGRES_URL"),
        help="PostgreSQL DSN. Defaults to DATABASE_URL or POSTGRES_URL.",
    )
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument(
        "--bioregion-uri",
        help="Optional filter on commitment metadata bioregion_uri or current pool bioregion_uri.",
    )
    parser.add_argument(
        "--pool-name-contains",
        help="Optional post-score filter for pool suggestions, e.g. Victoria.",
    )
    parser.add_argument(
        "--include-records",
        action="store_true",
        help="Include per-commitment replay rows in the output.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON only.")
    args = parser.parse_args()
    if not args.db_url:
        parser.error("--db-url is required unless DATABASE_URL or POSTGRES_URL is set")
    return args


def print_text_report(report: dict[str, Any]) -> None:
    print("Commitment routing diagnostics replay")
    print("=====================================")
    print(f"days: {report['scope']['days']}")
    print(f"commitments_replayed: {report['commitments_replayed']}")
    if report["scope"].get("bioregion_uri"):
        print(f"bioregion_uri: {report['scope']['bioregion_uri']}")
    if report["scope"].get("pool_name_contains"):
        print(f"pool_name_contains: {report['scope']['pool_name_contains']}")
    print(f"note: {report['scope']['note']}")
    print("")

    for section in (
        "draft_diagnostic_counts",
        "pool_diagnostic_counts",
        "effect_counts",
        "capacity_fix_delta_counts",
    ):
        print(section)
        counts = report[section]
        if not counts:
            print("  none")
        else:
            for key, value in counts.items():
                print(f"  {key}: {value}")
        print("")

    print("pre_capacity_fix_simulation")
    for section in ("pool_diagnostic_counts", "effect_counts"):
        print(f"  {section}")
        counts = report["pre_capacity_fix_simulation"][section]
        if not counts:
            print("    none")
        else:
            for key, value in counts.items():
                print(f"    {key}: {value}")
    print("")

    if report["records"]:
        print("records")
        for record in report["records"]:
            print(
                "  "
                f"{record['commitment_rid']} | top={record['top_pool_name']} "
                f"score={record['top_score']} draft_diags={record['draft_diagnostics']}"
            )


def main() -> None:
    args = parse_args()
    try:
        report = asyncio.run(replay(args))
    except RuntimeError as exc:
        raise SystemExit(f"error: {exc}") from exc
    if args.json:
        print(json.dumps(report, indent=2, default=_json_default))
    else:
        print_text_report(report)


if __name__ == "__main__":
    main()
