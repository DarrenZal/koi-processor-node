#!/usr/bin/env python3
"""Claims Engine — Pipeline Evaluation Harness.

Runs the full claims lifecycle and produces structured metrics JSON
for testnet dogfooding and regression tracking.

Pipeline steps measured:
  1. POST /claims/           — create claim
  2. POST /claims/{rid}/attestations (reviewer 1)
  3. PATCH /claims/{rid}/verify → peer_reviewed
  4. POST /claims/{rid}/attestations (reviewer 2)
  5. PATCH /claims/{rid}/verify → verified
  6. POST /claims/{rid}/prepare-anchor
  7. POST /claims/{rid}/anchor
  8. GET  /claims/{rid}/proof-pack

Without --apply this is a dry run: it prints the plan and contacts nothing. --apply
creates test entities and claims through the API (steps 1-6). The anchor step (7) and
the proof-pack step (8) run only with --anchor as well; they anchor on whatever chain
the API is configured for.

Usage:
    python -m scripts.eval_claims_pipeline                      # dry run
    python -m scripts.eval_claims_pipeline --apply --runs 3     # no anchoring
    python -m scripts.eval_claims_pipeline --apply --anchor     # also anchors on chain
    python -m scripts.eval_claims_pipeline --apply --compare docs/eval/claims-baseline.json
"""

import argparse
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

BASE_URL = "http://localhost:8351"

# Claims write endpoints require auth (require_auth -> KOI_CLAIMS_SERVICE_TOKEN).
# Fetched once at import; main() fails fast with a clear message if it's unset.
_SERVICE_TOKEN = os.getenv("KOI_CLAIMS_SERVICE_TOKEN", "")


# ── HTTP helper (copied from test_claims_api.py — stdlib only) ────────

def _req(method: str, path: str, body=None):
    """Make HTTP request and return (status, data)."""
    url = f"{BASE_URL}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if _SERVICE_TOKEN:
        req.add_header("Authorization", f"Bearer {_SERVICE_TOKEN}")

    try:
        # Explicit timeout (< asyncpg command_timeout=60) so a slow/hung backend
        # can't make the harness hold connections open indefinitely (single-worker).
        with urllib.request.urlopen(req, timeout=50) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        try:
            resp_data = json.loads(e.read().decode())
        except Exception:
            resp_data = {"detail": str(e)}
        return e.code, resp_data


def _resolve_or_create_entity(name: str, entity_type: str):
    """Resolve an existing entity or create via /ingest. Returns URI or None."""
    status, data = _req("POST", "/entity/resolve", {
        "label": name, "type_hint": entity_type,
    })
    if status == 200 and data.get("candidates") and not data.get("is_new"):
        return data["candidates"][0]["uri"]

    ts = int(time.time())
    status, data = _req("POST", "/ingest", {
        "document_rid": f"eval:claims-pipeline-{entity_type.lower()}-{ts}",
        "source": "eval_claims_pipeline",
        "entities": [{"name": name, "type": entity_type}],
    })
    if status == 200 and data.get("canonical_entities"):
        uri = data["canonical_entities"][0].get("uri")
        if uri:
            return uri
    return None


# ── Pipeline step runner ──────────────────────────────────────────────

def run_step(name: str, method: str, path: str, body=None, expected_status=None):
    """Run a single pipeline step, returning a step result dict."""
    if expected_status is None:
        expected_status = {200, 201, 202}

    start = time.monotonic()
    try:
        status, data = _req(method, path, body)
        elapsed = time.monotonic() - start
        success = status in expected_status
        return {
            "step": name,
            "success": success,
            "status": status,
            "latency_s": round(elapsed, 4),
            "data": data,
            "error": None if success else f"HTTP {status}: {data.get('detail', '')}",
        }
    except Exception as e:
        elapsed = time.monotonic() - start
        return {
            "step": name,
            "success": False,
            "status": 0,
            "latency_s": round(elapsed, 4),
            "data": {},
            "error": str(e),
        }


def run_pipeline(run_id: int, skip_anchor: bool, claimant_uri: str, reviewer_uris: list):
    """Execute the full claims pipeline once. Returns list of step results."""
    steps = []
    ts = int(time.time())

    # Step 1: Create claim
    step = run_step(
        "create_claim", "POST", "/claims/",
        body={
            "claimant_uri": claimant_uri,
            "statement": f"Eval harness: restored 25 hectares of degraded wetland in test bioregion (run {run_id} t={ts})",
            "claim_type": "ecological",
            "metadata": {"quantity": 25, "unit": "hectares", "eval_run": run_id},
        },
        expected_status={201},
    )
    steps.append(step)
    if not step["success"]:
        return steps
    rid = step["data"].get("claim_rid")
    if not rid:
        step["error"] = "No claim_rid in response"
        step["success"] = False
        return steps

    # Step 2: Attestation (reviewer 1)
    step = run_step(
        "attestation_1", "POST", f"/claims/{rid}/attestations",
        body={
            "reviewer_uri": reviewer_uris[0],
            "verdict": "approved",
            "rationale": f"Eval run {run_id}: first reviewer attestation",
        },
        expected_status={200, 201},
    )
    steps.append(step)
    if not step["success"]:
        return steps

    # Step 3: Verify → peer_reviewed
    step = run_step(
        "verify_peer_reviewed", "PATCH", f"/claims/{rid}/verify",
        body={
            "new_level": "peer_reviewed",
            "actor": "eval_harness",
            "reason": f"Eval run {run_id}",
        },
    )
    steps.append(step)
    if not step["success"]:
        return steps

    # Step 4: Attestation (reviewer 2)
    step = run_step(
        "attestation_2", "POST", f"/claims/{rid}/attestations",
        body={
            "reviewer_uri": reviewer_uris[1],
            "verdict": "approved",
            "rationale": f"Eval run {run_id}: second reviewer attestation",
        },
        expected_status={200, 201},
    )
    steps.append(step)
    if not step["success"]:
        return steps

    # Step 5: Verify → verified
    step = run_step(
        "verify_verified", "PATCH", f"/claims/{rid}/verify",
        body={
            "new_level": "verified",
            "actor": "eval_harness",
            "reason": f"Eval run {run_id}",
        },
    )
    steps.append(step)
    if not step["success"]:
        return steps

    # Step 6: Prepare anchor
    step = run_step("prepare_anchor", "POST", f"/claims/{rid}/prepare-anchor")
    steps.append(step)
    content_hash = step["data"].get("content_hash") if step["success"] else None

    # Step 7: Anchor (optional)
    if skip_anchor:
        steps.append({
            "step": "anchor",
            "success": True,
            "status": 0,
            "latency_s": 0,
            "data": {"skipped": True},
            "error": None,
        })
    else:
        step = run_step(
            "anchor", "POST", f"/claims/{rid}/anchor",
            expected_status={200, 202},
        )
        steps.append(step)
        if not step["success"]:
            return steps

    # Step 8: Proof-pack (only if anchored)
    if skip_anchor:
        steps.append({
            "step": "proof_pack",
            "success": True,
            "status": 0,
            "latency_s": 0,
            "data": {"skipped": True, "reason": "anchor skipped"},
            "error": None,
        })
    else:
        step = run_step("proof_pack", "GET", f"/claims/{rid}/proof-pack")
        steps.append(step)
        if step["success"] and content_hash:
            step["data"]["hash_verified"] = (
                step["data"].get("claim_content_hash_verified", False)
            )

    return steps


# ── Aggregation ───────────────────────────────────────────────────────

def compute_aggregates(all_runs):
    """Compute aggregate metrics across all runs."""
    step_names = [
        "create_claim", "attestation_1", "verify_peer_reviewed",
        "attestation_2", "verify_verified", "prepare_anchor",
        "anchor", "proof_pack",
    ]

    by_step = {}
    total_success = 0
    total_steps = 0
    all_latencies = []
    anchor_latencies = []

    for run in all_runs:
        for step in run:
            name = step["step"]
            if name not in by_step:
                by_step[name] = {"successes": 0, "total": 0, "latencies": []}
            by_step[name]["total"] += 1
            total_steps += 1
            if step["success"]:
                by_step[name]["successes"] += 1
                total_success += 1
            if step["latency_s"] > 0:
                by_step[name]["latencies"].append(step["latency_s"])
                all_latencies.append(step["latency_s"])
                if name == "anchor":
                    anchor_latencies.append(step["latency_s"])

    step_agg = {}
    for name in step_names:
        if name in by_step:
            s = by_step[name]
            lats = s["latencies"]
            step_agg[name] = {
                "avg_latency_s": round(statistics.mean(lats), 4) if lats else 0,
                "success_rate": round(s["successes"] / s["total"] * 100, 1) if s["total"] else 0,
                "count": s["total"],
            }

    return {
        "total_steps": len(step_names),
        "success_rate": round(total_success / total_steps * 100, 1) if total_steps else 0,
        "total_latency_s": round(sum(all_latencies), 3),
        "anchor_latency_s": round(sum(anchor_latencies), 3) if anchor_latencies else 0,
        "by_step": step_agg,
    }


# ── Comparison ────────────────────────────────────────────────────────

def compare_baselines(current, baseline_path, latency_multiplier=None):
    """Compare current run against a saved baseline.

    Returns True if a regression was detected (latency exceeded threshold).
    """
    with open(baseline_path) as f:
        baseline = json.load(f)

    b_agg = baseline["aggregates"]
    c_agg = current["aggregates"]

    print("\n" + "=" * 60)
    print("  BASELINE COMPARISON")
    print("=" * 60)

    metrics = [
        ("success_rate", "%", True),
        ("total_latency_s", "s", False),
        ("anchor_latency_s", "s", False),
    ]

    for metric, unit, higher_better in metrics:
        b_val = b_agg.get(metric, 0)
        c_val = c_agg.get(metric, 0)
        if b_val == 0:
            delta = "N/A"
        else:
            pct = (c_val - b_val) / b_val * 100
            arrow = "+" if pct > 0 else ""
            good = (pct > 0) == higher_better
            indicator = "better" if good else "WORSE"
            delta = f"{arrow}{pct:.1f}% ({indicator})"
        print(f"  {metric:25s} | baseline={b_val:8.3f}{unit} | current={c_val:8.3f}{unit} | {delta}")

    # Per-step comparison
    print("\n  BY STEP:")
    for step_name, c_data in c_agg.get("by_step", {}).items():
        b_data = b_agg.get("by_step", {}).get(step_name, {})
        b_lat = b_data.get("avg_latency_s", 0)
        c_lat = c_data.get("avg_latency_s", 0)
        if b_lat > 0:
            pct = (c_lat - b_lat) / b_lat * 100
            arrow = "+" if pct > 0 else ""
            indicator = "SLOWER" if pct > 0 else "faster"
            delta = f"{arrow}{pct:.1f}% ({indicator})"
        else:
            delta = "N/A"
        print(f"    {step_name:25s} | {b_lat:6.4f}s → {c_lat:6.4f}s | {delta}")

    chain_match = baseline.get("chain_id") == current.get("chain_id")
    print(f"\n  Chain ID match: {'YES' if chain_match else 'NO (different environments)'}")

    # Latency regression check
    latency_regression = False
    if latency_multiplier is not None:
        b_latency = b_agg.get("total_latency_s", 0)
        c_latency = c_agg.get("total_latency_s", 0)
        threshold = b_latency * latency_multiplier
        if b_latency > 0 and c_latency > threshold:
            print(f"\n  LATENCY REGRESSION: {c_latency:.3f}s > {threshold:.3f}s "
                  f"(baseline {b_latency:.3f}s × {latency_multiplier})")
            latency_regression = True
        elif b_latency > 0:
            print(f"\n  Latency within threshold: {c_latency:.3f}s <= {threshold:.3f}s "
                  f"(baseline {b_latency:.3f}s × {latency_multiplier})")

    print("=" * 60)
    return latency_regression


# ── Main ──────────────────────────────────────────────────────────────

PIPELINE_STEPS = [
    ("create_claim", "POST /claims/"),
    ("attestation_1", "POST /claims/{rid}/attestations"),
    ("verify_peer_reviewed", "PATCH /claims/{rid}/verify"),
    ("attestation_2", "POST /claims/{rid}/attestations"),
    ("verify_verified", "PATCH /claims/{rid}/verify"),
    ("prepare_anchor", "POST /claims/{rid}/prepare-anchor"),
    ("anchor", "POST /claims/{rid}/anchor (on chain)"),
    ("proof_pack", "GET /claims/{rid}/proof-pack"),
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Claims Engine Pipeline Eval Harness. Without --apply it is a dry run "
                    "that prints the plan and contacts nothing.")
    parser.add_argument("--base-url", default="http://localhost:8351", help="API base URL")
    parser.add_argument("--runs", type=int, default=1, help="Number of pipeline runs (default 1)")
    parser.add_argument("--apply", action="store_true",
                        help="Create test entities and claims through the API at --base-url")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the plan and exit (the default; wins over --apply)")
    anchor = parser.add_mutually_exclusive_group()
    anchor.add_argument("--anchor", action="store_true",
                        help="Also run the anchor step, which anchors each claim on the chain "
                             "the API is configured for, and the proof-pack step (needs --apply)")
    anchor.add_argument("--skip-anchor", action="store_true",
                        help="Skip the anchor and proof-pack steps (the default)")
    parser.add_argument("--save", default="docs/eval/claims-baseline.json", help="Save results path")
    parser.add_argument("--compare", help="Compare against saved baseline")
    parser.add_argument("--fail-on-latency-regression", type=float, metavar="MULTIPLIER",
                        help="Exit 1 if total latency exceeds baseline × MULTIPLIER (requires --compare)")
    return parser


def print_plan(args) -> None:
    """The dry run: what --apply would do, with no network call and no file write."""
    skip_anchor = not args.anchor
    print("DRY RUN: nothing is sent and no file is written. Pass --apply to run.")
    print(f"  Target API: {args.base_url}")
    print(f"  Runs: {args.runs}")
    print("  Setup: resolve or create 3 test entities (POST /entity/resolve, POST /ingest)")
    for name, call in PIPELINE_STEPS:
        skipped = skip_anchor and name in ("anchor", "proof_pack")
        print(f"  {'skip' if skipped else 'run '} {name:22s} {call}")
    if skip_anchor:
        print("  Anchoring: off (pass --anchor to anchor on chain)")
    else:
        print("  Anchoring: ON, each run anchors one claim on the chain the API is configured for")
    print(f"  Would save results to: {args.save}")
    if args.compare:
        print(f"  Would compare with: {args.compare}")
    if not _SERVICE_TOKEN:
        print("  Note: KOI_CLAIMS_SERVICE_TOKEN is not set; --apply would stop before writing.")


def main(argv=None):
    global BASE_URL
    args = build_parser().parse_args(argv)
    if args.dry_run or not args.apply:
        print_plan(args)
        return
    BASE_URL = args.base_url
    skip_anchor = not args.anchor

    # Health check
    print(f"Claims Engine — Pipeline Eval Harness")
    print(f"Base URL: {BASE_URL}")
    print(f"Runs: {args.runs}, Skip anchor: {skip_anchor}")
    print("=" * 60)

    try:
        status, data = _req("GET", "/health")
        if status != 200:
            raise Exception(f"status={status}")
    except Exception as e:
        print(f"ERROR: Server not reachable — {e}")
        print("Start with: ~/.config/personal-koi/start.sh")
        sys.exit(1)

    # Fail fast if the auth token is missing — every write step would 401 otherwise.
    if not _SERVICE_TOKEN:
        print("ERROR: KOI_CLAIMS_SERVICE_TOKEN not set — claims write endpoints require auth.")
        print("  Set it from config/personal.env and re-run, e.g.:")
        print("    export KOI_CLAIMS_SERVICE_TOKEN=$(grep '^KOI_CLAIMS_SERVICE_TOKEN=' config/personal.env | cut -d= -f2-)")
        sys.exit(1)

    # Fetch chain info
    chain_id = "unknown"
    is_testnet = False
    try:
        status, ci = _req("GET", "/claims/chain-info")
        if status == 200:
            chain_id = ci.get("chain_id", "unknown")
            is_testnet = ci.get("is_testnet", False)
            print(f"Chain: {chain_id} ({'testnet' if is_testnet else 'mainnet'})")
    except Exception:
        print("Warning: Could not fetch chain info")

    # Setup entities
    print("\nSetup: resolving test entities...")
    claimant_uri = _resolve_or_create_entity("Eval Harness Test Org", "Organization")
    if not claimant_uri:
        print("ERROR: Could not create test claimant")
        sys.exit(1)
    print(f"  Claimant: {claimant_uri}")

    reviewer_uris = []
    for name in ["Eval Reviewer Alpha", "Eval Pipeline Verifier"]:
        uri = _resolve_or_create_entity(name, "Person")
        if uri and uri not in reviewer_uris:
            reviewer_uris.append(uri)
    if len(reviewer_uris) < 2:
        print(f"ERROR: Need 2 reviewers, got {len(reviewer_uris)}")
        sys.exit(1)
    print(f"  Reviewers: {reviewer_uris}")

    # Run pipeline
    all_runs = []
    for i in range(1, args.runs + 1):
        print(f"\n--- Run {i}/{args.runs} ---")
        steps = run_pipeline(i, skip_anchor, claimant_uri, reviewer_uris)
        all_runs.append(steps)

        for step in steps:
            status_str = "OK" if step["success"] else "FAIL"
            skip = " (skipped)" if step["data"].get("skipped") else ""
            print(f"  [{status_str}] {step['step']:25s} | {step['latency_s']:6.4f}s{skip}")
            if step["error"]:
                print(f"         {step['error'][:80]}")

    # Aggregate
    aggregates = compute_aggregates(all_runs)

    print(f"\n{'=' * 60}")
    print("  AGGREGATE METRICS")
    print(f"{'=' * 60}")
    print(f"  Success rate:     {aggregates['success_rate']:5.1f}%")
    print(f"  Total latency:    {aggregates['total_latency_s']:5.3f}s")
    print(f"  Anchor latency:   {aggregates['anchor_latency_s']:5.3f}s")
    print(f"\n  BY STEP:")
    for step_name, data in aggregates["by_step"].items():
        print(f"    {step_name:25s} | lat={data['avg_latency_s']:6.4f}s | success={data['success_rate']:5.1f}%")

    # Build result
    result = {
        "eval_version": "claims-v1",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "api_url": BASE_URL,
        "chain_id": chain_id,
        "is_testnet": is_testnet,
        "run_count": args.runs,
        "skip_anchor": skip_anchor,
        "aggregates": aggregates,
        "runs": [
            {
                "run_id": i + 1,
                "steps": [
                    {k: v for k, v in step.items() if k != "data"}
                    for step in run
                ],
            }
            for i, run in enumerate(all_runs)
        ],
    }

    # Save
    save_path = Path(args.save)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    with open(save_path, "w") as f:
        json.dump(result, f, indent=2)
    print(f"\nResults saved to {save_path}")

    # Compare
    latency_regression = False
    if args.compare:
        latency_regression = compare_baselines(
            result, args.compare,
            latency_multiplier=args.fail_on_latency_regression,
        )

    # Exit code
    if aggregates["success_rate"] < 100:
        print("\nWARNING: Not all steps passed")
        sys.exit(1)
    if latency_regression:
        print("\nFAIL: Latency regression detected")
        sys.exit(1)
    print("\nAll pipeline steps passed!")


if __name__ == "__main__":
    main()
