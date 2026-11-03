#!/usr/bin/env bash
set -euo pipefail

# Phase 0 TerminusDB evaluation harness.
#
# Without --apply it prints what the phase would do and runs nothing (no docker, no
# import). --apply runs it. Removing containers and volumes (the clean phase, and
# --fresh) also needs --confirm-destructive.

usage() {
    cat <<EOF
Usage: $0 [0a|0b|all|clean] [--fresh] [--apply] [--confirm-destructive] [--dry-run]

  0a (default)  start the TerminusDB container on :6363 (creating it and its volume if
                missing), import POSTGRES_URL's graph into it, run test_merge.py
  0b            start a second container on :6364 and run test_federation.py
  all           0a then 0b
  clean         remove both containers and their volumes
  --fresh       remove both containers and their volumes before the phase
  --apply       run it (without it this prints the plan and runs nothing)
  --confirm-destructive  needed with --apply for clean and --fresh
  --dry-run     print the plan (the default; wins over --apply)
EOF
}

PHASE=""
FRESH=""
APPLY=0
DRY_RUN=0
CONFIRM_DESTRUCTIVE=0
for arg in "$@"; do
    case "$arg" in
        -h|--help) usage; exit 0 ;;
        --fresh) FRESH="--fresh" ;;
        --apply) APPLY=1 ;;
        --dry-run) DRY_RUN=1 ;;
        --confirm-destructive) CONFIRM_DESTRUCTIVE=1 ;;
        0a|0b|all|clean) PHASE="$arg" ;;
        *) echo "Unknown argument: $arg"; usage; exit 1 ;;
    esac
done
PHASE="${PHASE:-0a}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ARTIFACTS_DIR="$SCRIPT_DIR/artifacts/$(date -u +%Y%m%dT%H%M%SZ)"

DESTRUCTIVE=0
if [[ "$FRESH" == "--fresh" || "$PHASE" == "clean" ]]; then
    DESTRUCTIVE=1
fi

if [[ "$APPLY" -ne 1 || "$DRY_RUN" -eq 1 ]]; then
    echo "=== Phase 0 TerminusDB Evaluation: dry run (nothing is run) ==="
    echo "Phase: $PHASE | Fresh: ${FRESH:-no}"
    if [[ "$DESTRUCTIVE" -eq 1 ]]; then
        echo "  Would remove both TerminusDB containers and their volumes (needs --confirm-destructive)"
    fi
    if [[ "$PHASE" == "0a" || "$PHASE" == "all" ]]; then
        echo "  Would start the terminusdb container on :6363 (docker run if it does not exist)"
        echo "  Would run: python3 -m scripts.terminusdb.import_from_postgres --apply"
        echo "             (reads POSTGRES_URL, writes the koi_knowledge_graph database on :6363)"
        echo "  Would run: python3 -m scripts.terminusdb.test_merge --import-first"
    fi
    if [[ "$PHASE" == "0b" || "$PHASE" == "all" ]]; then
        echo "  Would start a second TerminusDB container on :6364 and run scripts.terminusdb.test_federation"
    fi
    echo "  Would write logs under $SCRIPT_DIR/artifacts/"
    echo "Pass --apply to run."
    exit 0
fi

if [[ "$DESTRUCTIVE" -eq 1 && "$CONFIRM_DESTRUCTIVE" -ne 1 ]]; then
    echo "Refusing: '$PHASE${FRESH:+ $FRESH}' removes the TerminusDB containers and their data volumes."
    echo "Add --confirm-destructive to do that."
    exit 2
fi

if [[ "$FRESH" == "--fresh" ]]; then
    echo "=== Fresh mode: removing existing containers and volumes ==="
    docker rm -f terminusdb terminusdb-peer 2>/dev/null || true
    docker volume rm terminusdb_data terminusdb_peer_data 2>/dev/null || true
fi

mkdir -p "$ARTIFACTS_DIR"

echo "=== Phase 0 TerminusDB Evaluation ==="
echo "Phase: $PHASE | Fresh: ${FRESH:-no} | Artifacts: $ARTIFACTS_DIR"
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"

# Activate venv if it exists
if [[ -f "$PROJECT_ROOT/venv/bin/activate" ]]; then
    source "$PROJECT_ROOT/venv/bin/activate"
fi

if [[ "$PHASE" == "0a" || "$PHASE" == "all" ]]; then
    echo -e "\n--- Step 1: Start TerminusDB ---"
    if docker ps --format '{{.Names}}' | grep -q '^terminusdb$'; then
        echo "terminusdb already running"
    elif docker ps -a --format '{{.Names}}' | grep -q '^terminusdb$'; then
        echo "terminusdb exists but stopped, starting..."
        docker start terminusdb
        sleep 3
    else
        docker run -d --name terminusdb \
            -p 6363:6363 \
            -v terminusdb_data:/app/terminusdb/storage \
            terminusdb/terminusdb-server:v12
        echo "Waiting for TerminusDB to start..."
        sleep 5
    fi

    # Health check with retry
    for i in {1..10}; do
        if curl -sf http://localhost:6363/api/info > /dev/null 2>&1; then
            echo "TerminusDB is ready"
            curl -sf http://localhost:6363/api/info | python3 -m json.tool
            break
        fi
        echo "Waiting for TerminusDB... ($i/10)"
        sleep 2
    done

    echo -e "\n--- Step 2: Import ---"
    cd "$PROJECT_ROOT"
    python3 -m scripts.terminusdb.import_from_postgres --apply 2>&1 | tee "$ARTIFACTS_DIR/import.log"

    echo -e "\n--- Step 3: Merge Tests ---"
    python3 -m scripts.terminusdb.test_merge --import-first 2>&1 | tee "$ARTIFACTS_DIR/test_merge.log"

    echo -e "\n--- Step 4: Metrics ---"
    docker stats --no-stream terminusdb > "$ARTIFACTS_DIR/docker_stats.txt" 2>&1 || true
    cp "$SCRIPT_DIR/results.json" "$ARTIFACTS_DIR/" 2>/dev/null || true
fi

if [[ "$PHASE" == "0b" || "$PHASE" == "all" ]]; then
    # Precondition: 0a must have passed
    if [[ ! -f "$SCRIPT_DIR/results.json" ]]; then
        echo "ERROR: Phase 0a results not found. Run 0a first."
        exit 1
    fi
    GO=$(python3 -c "import json; r=json.load(open('$SCRIPT_DIR/results.json')); print(r[-1]['go_nogo'] if r[-1]['phase']=='0a' else 'missing_0a')")
    if [[ "$GO" != "go" ]]; then
        echo "ERROR: Phase 0a did not pass (go_nogo=$GO). Cannot run 0b."
        exit 1
    fi

    echo -e "\n--- Step 5: Second Instance ---"
    if docker ps --format '{{.Names}}' | grep -q '^terminusdb-peer$'; then
        echo "terminusdb-peer already running"
    elif docker ps -a --format '{{.Names}}' | grep -q '^terminusdb-peer$'; then
        echo "terminusdb-peer exists but stopped, starting..."
        docker start terminusdb-peer
        sleep 3
    else
        docker run -d --name terminusdb-peer \
            -p 6364:6363 \
            -v terminusdb_peer_data:/app/terminusdb/storage \
            terminusdb/terminusdb-server:v12
        echo "Waiting for terminusdb-peer to start..."
        sleep 5
    fi

    # Health check
    for i in {1..10}; do
        if curl -sf http://localhost:6364/api/info > /dev/null 2>&1; then
            echo "terminusdb-peer is ready"
            break
        fi
        echo "Waiting for terminusdb-peer... ($i/10)"
        sleep 2
    done

    echo -e "\n--- Step 6: Federation Tests ---"
    cd "$PROJECT_ROOT"
    python3 -m scripts.terminusdb.test_federation 2>&1 | tee "$ARTIFACTS_DIR/test_federation.log"

    echo -e "\n--- Step 7: Metrics ---"
    docker stats --no-stream terminusdb terminusdb-peer > "$ARTIFACTS_DIR/docker_stats_both.txt" 2>&1 || true
    cp "$SCRIPT_DIR/results.json" "$ARTIFACTS_DIR/" 2>/dev/null || true
fi

if [[ "$PHASE" == "clean" ]]; then
    docker rm -f terminusdb terminusdb-peer 2>/dev/null || true
    docker volume rm terminusdb_data terminusdb_peer_data 2>/dev/null || true
    echo "Cleaned up."
fi

echo -e "\n=== Done. Artifacts: $ARTIFACTS_DIR ==="
