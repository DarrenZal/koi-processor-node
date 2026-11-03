#!/usr/bin/env python3
"""Gate Migration 112 on a full soak and non-decorative tier evidence."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


DEFAULT_INSTRUMENTATION_START = "2026-08-22T10:05:47-07:00"
EXCLUDED_SOURCE = "doc-scanner"


class EvidenceError(RuntimeError):
    """The evidence gate could not be evaluated safely."""


def parse_start(value: str) -> datetime:
    try:
        start = datetime.fromisoformat(value)
    except ValueError as exc:
        raise EvidenceError(f"Invalid ISO instrumentation start: {value}") from exc
    if start.tzinfo is None:
        raise EvidenceError("Instrumentation start must include an explicit UTC offset")
    return start


def classify_evidence(
    *,
    database_now: datetime,
    deadline: datetime,
    observed_rows: int,
    null_rows: int,
) -> tuple[str, int]:
    if database_now < deadline:
        return "incomplete_soak", 2
    if observed_rows == 0:
        return "decorative_instrumentation", 3
    if null_rows:
        return "producer_coverage_failed", 4
    return "migration_112_evidence_ready", 0


def evaluate(
    conn,
    *,
    instrumentation_start: datetime,
    minimum_days: float,
) -> tuple[dict[str, Any], int]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT now() AS database_now")
        database_now = cur.fetchone()["database_now"]
        deadline = instrumentation_start + timedelta(days=minimum_days)
        cur.execute(
            """
            SELECT source, resolution_tier, count(*) AS rows
            FROM entity_registry
            WHERE created_at >= (
              %s::timestamptz AT TIME ZONE current_setting('TimeZone')
            )
              AND source IS DISTINCT FROM %s
            GROUP BY source, resolution_tier
            ORDER BY source NULLS FIRST, resolution_tier NULLS FIRST
            """,
            (instrumentation_start, EXCLUDED_SOURCE),
        )
        breakdown = [dict(row) for row in cur.fetchall()]
        cur.execute(
            """
            SELECT count(*) AS rows
            FROM entity_registry
            WHERE created_at >= (
              %s::timestamptz AT TIME ZONE current_setting('TimeZone')
            ) AND source=%s
            """,
            (instrumentation_start, EXCLUDED_SOURCE),
        )
        excluded_rows = int(cur.fetchone()["rows"])

    observed_rows = sum(int(row["rows"]) for row in breakdown)
    null_rows = sum(
        int(row["rows"]) for row in breakdown if row["resolution_tier"] is None
    )
    null_sources = {
        str(row["source"]): int(row["rows"])
        for row in breakdown
        if row["resolution_tier"] is None
    }
    elapsed = database_now >= deadline
    verdict, exit_code = classify_evidence(
        database_now=database_now,
        deadline=deadline,
        observed_rows=observed_rows,
        null_rows=null_rows,
    )
    report = {
        "verdict": verdict,
        "instrumentation_start": instrumentation_start.isoformat(),
        "deadline": deadline.isoformat(),
        "database_now": database_now.isoformat(),
        "minimum_days": minimum_days,
        "window_elapsed": elapsed,
        "observed_rows": observed_rows,
        "null_resolution_tier_rows": null_rows,
        "null_resolution_tier_sources": null_sources,
        "observed_producer_breakdown": breakdown,
        "excluded_source": EXCLUDED_SOURCE,
        "excluded_source_rows": excluded_rows,
        "organization_must_remain_core": True,
    }
    return report, exit_code


def connect(database: str):
    return psycopg2.connect(database) if "://" in database else psycopg2.connect(dbname=database)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="personal_koi")
    parser.add_argument("--instrumentation-start", default=DEFAULT_INSTRUMENTATION_START)
    parser.add_argument("--minimum-days", type=float, default=7.0)
    args = parser.parse_args()
    conn = connect(args.database)
    try:
        report, exit_code = evaluate(
            conn,
            instrumentation_start=parse_start(args.instrumentation_start),
            minimum_days=args.minimum_days,
        )
    finally:
        conn.close()
    print(json.dumps(report, indent=2, sort_keys=True, default=str))
    return exit_code


if __name__ == "__main__":
    try:
        sys.exit(main())
    except EvidenceError as exc:
        raise SystemExit(f"ERROR: {exc}")
