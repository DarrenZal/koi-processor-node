#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Vault Sync Soak Check — periodic status capture
# ============================================================
# Usage:
#   bash scripts/federation/soak-check.sh
#   PEER_SSH=<user>@<peer-host> bash scripts/federation/soak-check.sh
#
# Captures status + reconcile from both local and peer nodes,
# appends timestamped results to a log file for trend analysis.
# ============================================================

API_PORT="${API_PORT:-8351}"
PEER_SSH="${PEER_SSH:-<peer>@<peer-lan-ip>}"
PEER_NAME="${PEER_NAME:-<peer-node>}"
SOAK_LOG="${SOAK_LOG:-/tmp/vault-sync-soak.jsonl}"

# Durable copy in repo (survives /tmp cleanup)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DURABLE_LOG="${PROJECT_DIR}/docs/soak-results/vault-sync-soak.jsonl"
mkdir -p "$(dirname "$DURABLE_LOG")"

# Admin tokens
LOCAL_TOKEN="${KOI_ADMIN_TOKEN:-}"
PEER_TOKEN="${PEER_KOI_ADMIN_TOKEN:-}"

# Read tokens from state dirs if not set
if [[ -z "$LOCAL_TOKEN" ]]; then
    LOCAL_TOKEN_FILE="${KOI_STATE_DIR:-$HOME/.config/personal-koi/koi-state}/admin_token"
    [[ -f "$LOCAL_TOKEN_FILE" ]] && LOCAL_TOKEN=$(cat "$LOCAL_TOKEN_FILE")
fi
if [[ -z "$PEER_TOKEN" ]]; then
    PEER_TOKEN=$(ssh "$PEER_SSH" "cat ~/.config/personal-koi/koi-state/admin_token 2>/dev/null" 2>/dev/null || echo "")
fi

TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
LOCAL_URL="http://localhost:${API_PORT}"

local_curl() {
    if [[ -n "$LOCAL_TOKEN" ]]; then
        curl -sf -H "Authorization: Bearer ${LOCAL_TOKEN}" "$@"
    else
        curl -sf "$@"
    fi
}

peer_curl() {
    local endpoint="$1"
    local data="${2:-}"
    local cmd="curl -sf"
    [[ -n "$PEER_TOKEN" ]] && cmd="$cmd -H 'Authorization: Bearer ${PEER_TOKEN}'"
    if [[ -n "$data" ]]; then
        cmd="$cmd -X POST -H 'Content-Type: application/json' -d '${data}' http://localhost:${API_PORT}${endpoint}"
    else
        cmd="$cmd http://localhost:${API_PORT}${endpoint}"
    fi
    ssh "$PEER_SSH" "$cmd" 2>/dev/null
}

echo "=== Soak Check: ${TIMESTAMP} ==="
echo ""

# --- Local status ---
echo "--- Local (darren-personal) ---"
LOCAL_STATUS=$(local_curl "${LOCAL_URL}/koi-net/vault-sync/status" 2>/dev/null || echo '{"error":"unreachable"}')
LOCAL_RECONCILE=$(local_curl -X POST -H "Content-Type: application/json" -d '{"mode":"detect"}' "${LOCAL_URL}/koi-net/vault-sync/reconcile" 2>/dev/null || echo '{"error":"unreachable"}')

LOCAL_PENDING=$(echo "$LOCAL_STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pending_events','?'))" 2>/dev/null || echo "?")
LOCAL_SCANS=$(echo "$LOCAL_STATUS" | python3 -c "import sys,json; m=json.load(sys.stdin).get('metrics',{}); print(m.get('scans_completed','?'))" 2>/dev/null || echo "?")
LOCAL_WATCHER=$(echo "$LOCAL_STATUS" | python3 -c "import sys,json; m=json.load(sys.stdin).get('metrics',{}); print(m.get('watcher_enabled','?'))" 2>/dev/null || echo "?")
LOCAL_REJECTED=$(echo "$LOCAL_STATUS" | python3 -c "import sys,json; r=json.load(sys.stdin).get('rejected_events'); print(sum(r.values()) if isinstance(r, dict) else '?')" 2>/dev/null || echo "?")
LOCAL_DRIFT=$(echo "$LOCAL_RECONCILE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total_drift','?'))" 2>/dev/null || echo "?")

printf "  pending_events: %s\n" "$LOCAL_PENDING"
printf "  scans_completed: %s\n" "$LOCAL_SCANS"
printf "  watcher_enabled: %s\n" "$LOCAL_WATCHER"
printf "  rejected_total: %s\n" "$LOCAL_REJECTED"
printf "  reconcile_drift: %s\n" "$LOCAL_DRIFT"

# --- Peer status ---
echo ""
echo "--- Peer (${PEER_NAME}) ---"
PEER_STATUS=$(peer_curl "/koi-net/vault-sync/status" 2>/dev/null || echo '{"error":"unreachable"}')
PEER_RECONCILE=$(peer_curl "/koi-net/vault-sync/reconcile" '{"mode":"detect"}' 2>/dev/null || echo '{"error":"unreachable"}')

PEER_PENDING=$(echo "$PEER_STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin).get('pending_events','?'))" 2>/dev/null || echo "?")
PEER_SCANS=$(echo "$PEER_STATUS" | python3 -c "import sys,json; m=json.load(sys.stdin).get('metrics',{}); print(m.get('scans_completed','?'))" 2>/dev/null || echo "?")
PEER_WATCHER=$(echo "$PEER_STATUS" | python3 -c "import sys,json; m=json.load(sys.stdin).get('metrics',{}); print(m.get('watcher_enabled','?'))" 2>/dev/null || echo "?")
PEER_REJECTED=$(echo "$PEER_STATUS" | python3 -c "import sys,json; r=json.load(sys.stdin).get('rejected_events'); print(sum(r.values()) if isinstance(r, dict) else '?')" 2>/dev/null || echo "?")
PEER_DRIFT=$(echo "$PEER_RECONCILE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('total_drift','?'))" 2>/dev/null || echo "?")

printf "  pending_events: %s\n" "$PEER_PENDING"
printf "  scans_completed: %s\n" "$PEER_SCANS"
printf "  watcher_enabled: %s\n" "$PEER_WATCHER"
printf "  rejected_total: %s\n" "$PEER_REJECTED"
printf "  reconcile_drift: %s\n" "$PEER_DRIFT"

# --- Append JSONL record ---
RECORD=$(python3 -c "
import json, sys
print(json.dumps({
    'timestamp': '${TIMESTAMP}',
    'local': {
        'pending_events': '${LOCAL_PENDING}',
        'scans_completed': '${LOCAL_SCANS}',
        'watcher_enabled': '${LOCAL_WATCHER}',
        'rejected_total': '${LOCAL_REJECTED}',
        'reconcile_drift': '${LOCAL_DRIFT}',
    },
    'peer': {
        'name': '${PEER_NAME}',
        'pending_events': '${PEER_PENDING}',
        'scans_completed': '${PEER_SCANS}',
        'watcher_enabled': '${PEER_WATCHER}',
        'rejected_total': '${PEER_REJECTED}',
        'reconcile_drift': '${PEER_DRIFT}',
    }
}))
")
echo "$RECORD" >> "$SOAK_LOG"
echo "$RECORD" >> "$DURABLE_LOG"

echo ""
echo "--- Summary ---"
echo "  Local drift: ${LOCAL_DRIFT}  Peer drift: ${PEER_DRIFT}"
if [[ "$LOCAL_DRIFT" == "0" && "$PEER_DRIFT" == "0" ]]; then
    echo "  Status: OK (zero drift both peers)"
else
    echo "  Status: DRIFT DETECTED — investigate before proceeding"
fi
echo "  Log: ${SOAK_LOG}"
echo ""

# --- Trend (last 5 entries) ---
if [[ -f "$SOAK_LOG" ]]; then
    ENTRIES=$(wc -l < "$SOAK_LOG" | tr -d ' ')
    echo "--- Trend (${ENTRIES} entries) ---"
    tail -5 "$SOAK_LOG" | python3 -c "
import sys, json
print(f'  {\"Timestamp\":<22} {\"L.Pend\":>7} {\"L.Scan\":>7} {\"L.Rej\":>6} {\"L.Drift\":>7}  {\"P.Pend\":>7} {\"P.Scan\":>7} {\"P.Rej\":>6} {\"P.Drift\":>7}')
print(f'  {\"-\"*22} {\"-\"*7} {\"-\"*7} {\"-\"*6} {\"-\"*7}  {\"-\"*7} {\"-\"*7} {\"-\"*6} {\"-\"*7}')
for line in sys.stdin:
    try:
        d = json.loads(line)
        l, p = d['local'], d['peer']
        print(f'  {d[\"timestamp\"]:<22} {l[\"pending_events\"]:>7} {l[\"scans_completed\"]:>7} {l[\"rejected_total\"]:>6} {l[\"reconcile_drift\"]:>7}  {p[\"pending_events\"]:>7} {p[\"scans_completed\"]:>7} {p[\"rejected_total\"]:>6} {p[\"reconcile_drift\"]:>7}')
    except: pass
"
fi
