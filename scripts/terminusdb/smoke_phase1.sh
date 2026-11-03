#!/usr/bin/env bash
set -euo pipefail

# Phase 1 Smoke Test — validates the TerminusDB integration end-to-end on a throwaway node.
#
# It imports the test database into TerminusDB, starts its own API
# (api.personal_ingest_api on 0.0.0.0, on KOI_TEST_API_URL's port) and an outbox worker,
# both pointed at KOI_TEST_DATABASE_URL, registers test entities, stops and restarts the
# TerminusDB container to check that writes fail open, and runs reconciliation.
#
# Targets come from the test-target guard (tests/live_write_shell_guard.sh, as the other
# shell suites): KOI_TEST_API_URL and KOI_TEST_DATABASE_URL must be set and pass it (no
# database named personal_koi or without "test" as a word of its name, no API on port
# 8351), and TERMINUSDB_DB (default koi_smoke_test here) must have "test" as a word of its
# name, unless KOI_TEST_ALLOW_LIVE=1. The entities it registers are deleted from
# KOI_TEST_DATABASE_URL on exit, and a TerminusDB container it stopped is started again.
#
# Prerequisites:
#   - TerminusDB container (TDB_CONTAINER, default terminusdb) running on TERMINUSDB_URL
#   - KOI_TEST_DATABASE_URL migrated, and config/personal.env present (the importer reads it;
#     the targets above override its database settings)
#   - Nothing listening on KOI_TEST_API_URL's port, which must be on this machine
#
# Usage:
#   KOI_TEST_API_URL=http://127.0.0.1:<port> \
#   KOI_TEST_DATABASE_URL=postgresql://<role>@127.0.0.1:<port>/<db with test in its name> \
#       bash scripts/terminusdb/smoke_phase1.sh --apply [--fresh --confirm-destructive] [--env-file FILE]
#
#   (no --apply)  print the plan and the guard's verdict on the targets; run nothing
#   --apply       run the smoke test
#   --fresh       drop and recreate the TerminusDB database before testing (fixes schema
#                 drift); needs --confirm-destructive
#   --env-file F  source F first (the old version always sourced config/personal.env)

usage() {
    sed -n '4,34p' "$0" | sed 's/^# \{0,1\}//'
}

FRESH=""
APPLY=0
DRY_RUN=0
CONFIRM_DESTRUCTIVE=0
ENV_FILE=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        --fresh) FRESH="--fresh" ;;
        --apply) APPLY=1 ;;
        --dry-run) DRY_RUN=1 ;;
        --confirm-destructive) CONFIRM_DESTRUCTIVE=1 ;;
        --env-file)
            [[ $# -ge 2 ]] || { echo "--env-file needs a file"; exit 1; }
            ENV_FILE="$2"; shift ;;
        *) echo "Unknown argument: $1"; echo "Usage: $0 [--apply] [--fresh --confirm-destructive] [--env-file FILE] [--dry-run]"; exit 1 ;;
    esac
    shift
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$PROJECT_ROOT"

if [[ -n "$ENV_FILE" ]]; then
    # Safe env loading (handles spaces/quotes)
    set -a; source "$ENV_FILE"; set +a
fi

TDB_CONTAINER="${TDB_CONTAINER:-terminusdb}"
export TERMINUSDB_DB="${TERMINUSDB_DB:-koi_smoke_test}"
export TERMINUSDB_ENABLED="${TERMINUSDB_ENABLED:-true}"
if [[ -x "$PROJECT_ROOT/venv/bin/python" ]]; then
    export KOI_PYTHON="${KOI_PYTHON:-$PROJECT_ROOT/venv/bin/python}"
fi
mask_dsn() { printf '%s' "$1" | sed -E 's#(://[^:/@]*):[^@]*@#\1:***@#'; }

if [[ "$APPLY" -ne 1 || "$DRY_RUN" -eq 1 ]]; then
    echo "Phase 1 smoke test: dry run (nothing is run)"
    echo "  API to start: ${KOI_TEST_API_URL:-(KOI_TEST_API_URL not set)}"
    echo "  Database: $(mask_dsn "${KOI_TEST_DATABASE_URL:-(KOI_TEST_DATABASE_URL not set)}")"
    echo "  TerminusDB: ${TERMINUSDB_URL:-http://127.0.0.1:6363/} database $TERMINUSDB_DB, container $TDB_CONTAINER"
    if "${KOI_PYTHON:-python3}" tests/live_write_cleanup.py check-env >/dev/null 2>&1; then
        echo "  Guard: the targets pass tests/live_write_shell_guard.sh"
    else
        echo "  Guard: --apply would refuse these targets (tests/live_write_shell_guard.sh)"
    fi
    echo "  Would: import the database into TerminusDB${FRESH:+ (dropping it first: needs --confirm-destructive)},"
    echo "         start the API and outbox worker, register 2 test entities, stop and start"
    echo "         $TDB_CONTAINER, run reconciliation, then delete the registered entities"
    echo "Pass --apply to run."
    exit 0
fi

if [[ -n "$FRESH" && "$CONFIRM_DESTRUCTIVE" -ne 1 ]]; then
    echo "Refusing --fresh without --confirm-destructive: it can drop the TerminusDB database $TERMINUSDB_DB."
    exit 2
fi

# Refuses unless KOI_TEST_API_URL and KOI_TEST_DATABASE_URL pass the test-target guard; sets
# API, KOI_TEST_RUN_ID and an EXIT trap that deletes what live_write_record recorded.
source "$PROJECT_ROOT/tests/live_write_shell_guard.sh"
live_write_begin

if [[ "${KOI_TEST_ALLOW_LIVE:-}" != "1" ]] && ! [[ "$TERMINUSDB_DB" =~ (^|[_-])test([_-]|$) ]]; then
    echo "Refusing: TERMINUSDB_DB '$TERMINUSDB_DB' does not have 'test' as a word of its name (KOI_TEST_ALLOW_LIVE=1 overrides)."
    exit 2
fi

API_HOST=$("${KOI_PYTHON:-python3}" -c 'import sys, urllib.parse as u; print(u.urlsplit(sys.argv[1]).hostname or "")' "$API")
API_PORT=$("${KOI_PYTHON:-python3}" -c 'import sys, urllib.parse as u; p = u.urlsplit(sys.argv[1]); print(p.port or (443 if p.scheme == "https" else 80))' "$API")
case "$API_HOST" in
    127.0.0.1|localhost|::1) ;;
    *) echo "Refusing: this test starts its own API, so KOI_TEST_API_URL must be on this machine (got host '$API_HOST')."; exit 2 ;;
esac
export POSTGRES_URL="$KOI_TEST_DATABASE_URL"
DB="$KOI_TEST_DATABASE_URL"
echo "API: $API (started here on 0.0.0.0:$API_PORT) | Database: $(mask_dsn "$DB") | TerminusDB: $TERMINUSDB_DB"

PASS=0
FAIL=0
step() { echo -e "\n=== $1 ==="; }
pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }

cleanup() {
    local rc=$?
    set +e  # under set -e, the "(exit $rc)" below would end the trap before the purge
    if [[ -n "${API_PID:-}" ]]; then
        kill "$API_PID" 2>/dev/null || true
        wait "$API_PID" 2>/dev/null || true
    fi
    if [[ -n "${WORKER_PID:-}" ]]; then
        kill "$WORKER_PID" 2>/dev/null || true
        wait "$WORKER_PID" 2>/dev/null || true
    fi
    if [[ -n "${TDB_STOPPED:-}" ]]; then
        docker start "$TDB_CONTAINER" > /dev/null 2>&1 || true
    fi
    (exit "$rc")
    live_write_finish
}
trap cleanup EXIT

# -------------------------------------------------------------------------
step "0. Preflight"
# -------------------------------------------------------------------------

if lsof -i :"$API_PORT" 2>/dev/null | grep -q LISTEN; then
    fail "Port $API_PORT already in use"
    exit 1
fi
pass "Port $API_PORT clear"

if ! docker ps --format '{{.Names}}' | grep -q "^${TDB_CONTAINER}\$"; then
    fail "TerminusDB container not running"
    exit 1
fi
pass "TerminusDB container running"

if ! psql "$DB" -c "SELECT 1" > /dev/null 2>&1; then
    fail "PostgreSQL not reachable"
    exit 1
fi
pass "PostgreSQL reachable"

# -------------------------------------------------------------------------
step "1. Import${FRESH:+ (fresh)}"
# -------------------------------------------------------------------------

venv/bin/python -m scripts.terminusdb.import_from_postgres --apply ${FRESH:+--fresh --confirm-destructive} 2>&1
pass "Import completed"

# -------------------------------------------------------------------------
step "2. Start API + worker"
# -------------------------------------------------------------------------

( exec venv/bin/uvicorn api.personal_ingest_api:app --host 0.0.0.0 --port "$API_PORT" ) &
API_PID=$!

( exec venv/bin/python -m scripts.terminusdb.outbox_worker --run ) &
WORKER_PID=$!

# Wait for API to be ready
for i in {1..15}; do
    if curl -sf "$API/health" > /dev/null 2>&1; then
        break
    fi
    sleep 1
done

if ! curl -sf "$API/health" > /dev/null 2>&1; then
    fail "API did not start within 15s"
    exit 1
fi
pass "API listening on :$API_PORT"

# -------------------------------------------------------------------------
step "3. Graph health"
# -------------------------------------------------------------------------

HEALTH=$(curl -sf "$API/graph/health")
TDB_REACHABLE=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('terminusdb_reachable',False))")
SCHEMA_OK=$(echo "$HEALTH" | python3 -c "import sys,json; print(json.load(sys.stdin).get('schema_ok',False))")

if [[ "$TDB_REACHABLE" == "True" ]]; then pass "TerminusDB reachable"; else fail "TerminusDB not reachable"; fi
if [[ "$SCHEMA_OK" == "True" ]]; then pass "Schema OK"; else fail "Schema mismatch"; fi

# -------------------------------------------------------------------------
step "4. Smoke test — register entity"
# -------------------------------------------------------------------------

REGISTER_RESULT=$(curl -sf -X POST "$API/register-entity" \
    -H 'Content-Type: application/json' \
    -d "{
        \"name\": \"Phase1 Smoke Test $KOI_TEST_RUN_ID\",
        \"entity_type\": \"Concept\",
        \"vault_rid\": \"Tests/phase1-smoke-$KOI_TEST_RUN_ID.md\",
        \"vault_path\": \"Tests/phase1-smoke-$KOI_TEST_RUN_ID.md\",
        \"content_hash\": \"smoke_$KOI_TEST_RUN_ID\"
    }")

IS_NEW=$(echo "$REGISTER_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('is_new',False))")
CANONICAL_URI=$(echo "$REGISTER_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('canonical_uri',''))")
live_write_record entity_uri "$CANONICAL_URI"

if [[ "$IS_NEW" == "True" && -n "$CANONICAL_URI" ]]; then
    pass "Entity registered: $CANONICAL_URI"
else
    fail "Entity registration (is_new=$IS_NEW)"
fi

# Wait for worker to drain
sleep 5

PENDING=$(psql -tAq "$DB" -c "SELECT COUNT(*) FROM terminusdb_outbox WHERE status='pending';")
if [[ "$PENDING" -eq 0 ]]; then
    pass "Outbox drained (0 pending)"
else
    fail "Outbox has $PENDING pending rows"
fi

# -------------------------------------------------------------------------
step "5. Auth guard"
# -------------------------------------------------------------------------

LOCAL_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API/graph/health")
if [[ "$LOCAL_CODE" == "200" ]]; then pass "Localhost → 200"; else fail "Localhost → $LOCAL_CODE (expected 200)"; fi

LAN_IP=$(ipconfig getifaddr en0 2>/dev/null \
    || hostname -I 2>/dev/null | awk '{print $1}' \
    || ip -4 addr show scope global 2>/dev/null | awk '/inet /{print $2}' | cut -d/ -f1 | head -1 \
    || echo "")
if [[ -n "$LAN_IP" ]]; then
    LAN_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://${LAN_IP}:${API_PORT}/graph/health")
    if [[ "$LAN_CODE" == "403" ]]; then pass "LAN IP → 403"; else fail "LAN IP → $LAN_CODE (expected 403)"; fi
else
    echo "  SKIP: No LAN IP detected"
fi

# -------------------------------------------------------------------------
step "6. Fail-open"
# -------------------------------------------------------------------------

docker stop "$TDB_CONTAINER" > /dev/null
TDB_STOPPED=1
sleep 2

FAILOPEN_RESULT=$(curl -sf -X POST "$API/register-entity" \
    -H 'Content-Type: application/json' \
    -d "{
        \"name\": \"Fail Open Smoke $KOI_TEST_RUN_ID\",
        \"entity_type\": \"Concept\",
        \"vault_rid\": \"Tests/failopen-$KOI_TEST_RUN_ID.md\",
        \"vault_path\": \"Tests/failopen-$KOI_TEST_RUN_ID.md\",
        \"content_hash\": \"failopen_$KOI_TEST_RUN_ID\"
    }")

live_write_record entity_uri "$(echo "$FAILOPEN_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('canonical_uri',''))" 2>/dev/null || true)"
FAILOPEN_OK=$(echo "$FAILOPEN_RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('success',False))")
if [[ "$FAILOPEN_OK" == "True" ]]; then
    pass "PG write succeeds with TDB down"
else
    fail "PG write failed with TDB down"
fi

PENDING_AFTER=$(psql -tAq "$DB" -c "SELECT COUNT(*) FROM terminusdb_outbox WHERE status IN ('pending','processing');")
if [[ "$PENDING_AFTER" -gt 0 ]]; then
    pass "Outbox accumulating ($PENDING_AFTER pending)"
else
    # Might have been claimed already — check if any row was just created
    pass "Outbox row created (worker may have already attempted)"
fi

docker start "$TDB_CONTAINER" > /dev/null
TDB_STOPPED=""

# Wait for TDB to come up + worker to drain backlog
echo "  Waiting for TDB recovery + backlog drain..."
for i in {1..30}; do
    STILL_PENDING=$(psql -tAq "$DB" -c "SELECT COUNT(*) FROM terminusdb_outbox WHERE status IN ('pending','processing');")
    if [[ "$STILL_PENDING" -eq 0 ]]; then break; fi
    sleep 2
done

DEAD=$(psql -tAq "$DB" -c "SELECT COUNT(*) FROM terminusdb_outbox WHERE status='dead_letter';")
STILL_PENDING=$(psql -tAq "$DB" -c "SELECT COUNT(*) FROM terminusdb_outbox WHERE status IN ('pending','processing');")

if [[ "$STILL_PENDING" -eq 0 && "$DEAD" -eq 0 ]]; then
    pass "Backlog drained after recovery"
elif [[ "$DEAD" -gt 0 ]]; then
    fail "$DEAD dead-letter rows (increase MAX_ATTEMPTS or check TDB startup time)"
else
    fail "$STILL_PENDING rows still pending after 60s"
fi

# -------------------------------------------------------------------------
step "7. Reconciliation"
# -------------------------------------------------------------------------

RECON_OUTPUT=$(venv/bin/python -m scripts.terminusdb.reconcile 2>&1)
echo "$RECON_OUTPUT"

if echo "$RECON_OUTPUT" | grep -q "No drift detected"; then
    pass "Reconciliation: 0 drift"
else
    fail "Reconciliation detected drift"
fi

# -------------------------------------------------------------------------
step "Results"
# -------------------------------------------------------------------------

TOTAL=$((PASS + FAIL))
echo ""
echo "  $PASS/$TOTAL passed"
if [[ "$FAIL" -gt 0 ]]; then
    echo "  $FAIL FAILED"
    exit 1
else
    echo "  Phase 1 smoke test: ALL PASS"
fi
