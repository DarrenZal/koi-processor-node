#!/usr/bin/env bash
# Claims pipeline regression gate
# Usage: ./scripts/check_claims_regression.sh [--apply] [--runs N | N]
#   Without --apply it prints what it would run and contacts nothing.
#   With --apply it needs koi-server running on :8351 and writes test claims through it
#   (no on-chain anchoring), then compares with docs/eval/claims-baseline.json.
# Exit 0 = pass, Exit 1 = regression detected
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: ./scripts/check_claims_regression.sh [--apply] [--runs N | N]

Runs scripts/eval_claims_pipeline.py N times (default 3) against the API on
http://localhost:8351 without anchoring, and compares the result with
docs/eval/claims-baseline.json (fails on a latency above 2.0x the baseline).

  --apply      run it: creates test entities and claims through the API
  --dry-run    print what --apply would run (the default)
  --runs N     number of pipeline runs (a bare number works too)
  -h, --help   this text
EOF
}

APPLY=0
DRY_RUN=0
RUNS=3
while [ $# -gt 0 ]; do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --apply) APPLY=1 ;;
        --dry-run) DRY_RUN=1 ;;
        --runs)
            [ $# -ge 2 ] || { echo "ERROR: --runs needs a number" >&2; exit 2; }
            RUNS="$2"; shift ;;
        --runs=*) RUNS="${1#--runs=}" ;;
        *)
            if [[ "$1" =~ ^[0-9]+$ ]]; then
                RUNS="$1"
            else
                echo "ERROR: unknown argument: $1" >&2; usage >&2; exit 2
            fi ;;
    esac
    shift
done
if ! [[ "$RUNS" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: --runs must be a positive integer (got '$RUNS')" >&2
    exit 2
fi

BASELINE="docs/eval/claims-baseline.json"
OUTPUT="/tmp/claims-eval-latest.json"
EVAL_ARGS=(--apply --skip-anchor --runs "$RUNS" --compare "$BASELINE"
           --fail-on-latency-regression 2.0 --save "$OUTPUT")

if [ "$APPLY" -ne 1 ] || [ "$DRY_RUN" -eq 1 ]; then
    echo "=== Claims Regression Gate (dry run: nothing is sent) ==="
    echo "Target API: http://localhost:8351"
    echo "Runs: $RUNS | Baseline: $BASELINE"
    if [ -f "$BASELINE" ]; then
        echo "Baseline: present"
    else
        echo "Baseline: missing (--apply would stop; generate one with"
        echo "  python -m scripts.eval_claims_pipeline --apply --runs 3 --save $BASELINE)"
    fi
    echo "Would run: python -m scripts.eval_claims_pipeline ${EVAL_ARGS[*]}"
    echo "Pass --apply to run it."
    exit 0
fi

echo "=== Claims Regression Gate ==="
echo "Runs: $RUNS | Baseline: $BASELINE"
echo ""

# Health check — fail fast if server not running
if ! curl -sf http://localhost:8351/health > /dev/null 2>&1; then
    echo "ERROR: koi-server not reachable on :8351"
    echo "Start with: ~/.config/personal-koi/start.sh"
    exit 1
fi

# Check baseline exists
if [ ! -f "$BASELINE" ]; then
    echo "ERROR: Baseline not found at $BASELINE"
    echo "Generate one with: python -m scripts.eval_claims_pipeline --apply --runs 3 --save $BASELINE"
    exit 1
fi

# Run eval harness with comparison and latency gate
exec python -m scripts.eval_claims_pipeline "${EVAL_ARGS[@]}"
