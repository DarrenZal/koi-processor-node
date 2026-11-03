#!/usr/bin/env python3
"""Analyze structured resolver shadow observations from application logs."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import sys

PREFIX = "RESOLVER_SHADOW "

# Every production call site that can reach the fuzzy/semantic policy check.
# A zero-traffic caller remains a gate blocker until a deterministic replay
# fixture is explicitly supplied with --fixture-caller. Writing those fixtures
# is part of this phase; absence never silently shrinks the measured surface.
DEFAULT_EXPECTED_CALLERS = {
    "bundle_handlers.cross_reference_resolver",
    "commons_ingest_worker.ingest",
    "knowledge_router.add_knowledge",
    "mediawiki_ingest.editorial_edge",
    "mediawiki_ingest.page",
    "mediawiki_ingest.structural_edge",
    "personal_ingest_api.entity_resolve_get",
    "personal_ingest_api.entity_resolve_post",
    "personal_ingest_api.ingest",
    "personal_ingest_api.register_vault_entity",
    "web_router.batch_ingest",
    "web_router.ingest",
    "web_router.process",
}


def load_records(paths: list[Path]) -> list[dict]:
    records: list[dict] = []
    for path in paths:
        with path.open(errors="replace") as handle:
            for line in handle:
                marker = line.find(PREFIX)
                if marker < 0:
                    continue
                try:
                    records.append(json.loads(line[marker + len(PREFIX):]))
                except json.JSONDecodeError:
                    continue
    return records


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, int(len(ordered) * fraction) - 1))
    return ordered[index]


def analyze(
    records: list[dict],
    *,
    minimum_attempts: int,
    minimum_days: float,
    expected_callers: set[str],
    fixture_callers: set[str],
    max_overhead_ratio: float,
) -> tuple[dict, int]:
    observed_callers = {r.get("caller", "") for r in records}
    missing_callers = sorted(expected_callers - observed_callers - fixture_callers)
    divergences = [r for r in records if r.get("outcome_diverged")]
    # Overhead is a LIVE-TRAFFIC question and only live records can answer it. In a
    # replay the shadow comparison is the entire workload, so the ratio approaches 1.0;
    # including replays would fail the gate on exit 4 "overhead_failed" for a reason
    # unrelated to the policy, and would do so precisely when a replay large enough to
    # satisfy the attempt bar was supplied. Divergence, by contrast, is a property of
    # the two policies and is equally true whoever produced the record — so replays DO
    # count toward attempts, callers and divergences below.
    live_records = [r for r in records if not r.get("replay")]
    replay_records = [r for r in records if r.get("replay")]
    ratios = [
        float(r.get("shadow_overhead_ms", 0))
        / float(r.get("resolver_elapsed_ms", 1))
        for r in live_records
        if float(r.get("resolver_elapsed_ms", 0)) > 0
    ]
    # Live records only: a replay runs in one burst, so its timestamps describe how long
    # the script took, not how long the policy was observed under real traffic. Mixing
    # them in would let a large replay masquerade as elapsed soak. A replay-based
    # evaluation therefore has to pass --minimum-days 0 explicitly, which is the point:
    # the operator states that the days requirement is being waived rather than having a
    # burst of records quietly satisfy it.
    timestamps = sorted(
        datetime.fromisoformat(r["observed_at"])
        for r in live_records
        if r.get("observed_at")
    )
    observed_days = (
        (timestamps[-1] - timestamps[0]).total_seconds() / 86400
        if len(timestamps) >= 2
        else 0.0
    )
    p95_ratio = percentile(ratios, 0.95)
    by_caller = Counter(r.get("caller", "unknown") for r in records)
    by_type = Counter(r.get("entity_type", "unknown") for r in records)

    incomplete = (
        len(records) < minimum_attempts
        or observed_days < minimum_days
        or bool(missing_callers)
    )
    # No live records means no overhead evidence. Reporting p95 0.0 as a pass would be
    # the "populated is not conformant" failure again: an empty measurement is not a
    # clean one. It does not fail the run, but it is stated, and observed_days below is
    # computed over live records for the same reason.
    overhead_failed = bool(ratios) and p95_ratio > max_overhead_ratio
    if overhead_failed:
        exit_code = 4
        verdict = "overhead_failed"
    elif divergences:
        exit_code = 3
        verdict = "explicit_policy_split"
    elif incomplete:
        exit_code = 2
        verdict = "incomplete"
    else:
        exit_code = 0
        verdict = "zero_divergence_consolidation_eligible"

    report = {
        "verdict": verdict,
        "attempts": len(records),
        "required_attempts": minimum_attempts,
        "observed_days": round(observed_days, 4),
        "required_days": minimum_days,
        "outcome_divergences": len(divergences),
        "candidate_divergences": sum(
            int(r.get("candidate_divergences", 0)) for r in records
        ),
        "p95_shadow_overhead_ratio": round(p95_ratio, 6),
        "max_shadow_overhead_ratio": max_overhead_ratio,
        "overhead_measured_on_live_records": len(ratios),
        "live_attempts": len(live_records),
        "replay_attempts": len(replay_records),
        "observed_callers": dict(sorted(by_caller.items())),
        "fixture_callers": sorted(fixture_callers),
        "missing_callers": missing_callers,
        "entity_types": dict(sorted(by_type.items())),
    }
    return report, exit_code


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("logs", nargs="+", type=Path)
    parser.add_argument("--minimum-attempts", type=int, default=1000)
    parser.add_argument("--minimum-days", type=float, default=7.0)
    parser.add_argument("--expected-caller", action="append", default=[])
    parser.add_argument("--fixture-caller", action="append", default=[])
    parser.add_argument(
        "--no-default-callers",
        action="store_true",
        help="Disable the conservative built-in production call-site list",
    )
    parser.add_argument("--max-overhead-ratio", type=float, default=0.05)
    args = parser.parse_args()
    report, exit_code = analyze(
        load_records(args.logs),
        minimum_attempts=args.minimum_attempts,
        minimum_days=args.minimum_days,
        expected_callers=(
            set(args.expected_caller)
            if args.no_default_callers
            else DEFAULT_EXPECTED_CALLERS | set(args.expected_caller)
        ),
        fixture_callers=set(args.fixture_caller),
        max_overhead_ratio=args.max_overhead_ratio,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
