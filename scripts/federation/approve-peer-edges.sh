#!/usr/bin/env bash
# approve-peer-edges.sh — Admin approves all PROPOSED edges to/from a specific peer
#
# Usage: ./approve-peer-edges.sh <peer-name-or-rid>
# Example: ./approve-peer-edges.sh peer-b

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <peer-name-or-rid>"
    echo "Example: $0 peer-b"
    exit 1
fi

PEER_ID="$1"
KOI_PORT="${KOI_API_PORT:-8351}"
STATE_DIR="${KOI_STATE_DIR:-$HOME/.config/personal-koi/koi-state}"
ADMIN_TOKEN_FILE="$STATE_DIR/admin_token"

# ============================================
# RESOLVE PEER RID
# ============================================

if [[ "$PEER_ID" == orn:koi-net.node:* ]]; then
    PEER_RID="$PEER_ID"
else
    # Look up by alias then by node_name
    PEER_RID=$(python3 -c "
import sys, json, urllib.request

# Try peer_aliases table via psql
import subprocess
result = subprocess.run(
    ['psql', '-d', 'personal_koi', '-Atc',
     \"SELECT node_rid FROM koi_net_peer_aliases WHERE alias = '${PEER_ID}'\"],
    capture_output=True, text=True
)
rids = [r.strip() for r in result.stdout.strip().split('\n') if r.strip()]
if not rids:
    # Try node_name
    result = subprocess.run(
        ['psql', '-d', 'personal_koi', '-Atc',
         \"SELECT node_rid FROM koi_net_nodes WHERE lower(node_name) = lower('${PEER_ID}')\"],
        capture_output=True, text=True
    )
    rids = [r.strip() for r in result.stdout.strip().split('\n') if r.strip()]

if len(rids) == 0:
    print('NOT_FOUND', file=sys.stderr)
    sys.exit(1)
if len(rids) > 1:
    print('AMBIGUOUS', file=sys.stderr)
    sys.exit(2)
print(rids[0])
" 2>/tmp/approve-edges-err) || {
        ERR=$(cat /tmp/approve-edges-err)
        if [[ "$ERR" == "AMBIGUOUS" ]]; then
            log_fatal "Ambiguous peer name '$PEER_ID' — use exact node_rid instead"
        else
            log_fatal "Peer '$PEER_ID' not found in database"
        fi
    }
fi

log_info "Resolved peer: $PEER_RID"

# ============================================
# GET LOCAL NODE RID
# ============================================

HEALTH=$(curl -sf "http://127.0.0.1:${KOI_PORT}/koi-net/health") || \
    log_fatal "Cannot reach local KOI-net API"
LOCAL_RID=$(echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('node', d.get('profile', {}))['node_rid'])")

log_info "Local node: $LOCAL_RID"

# ============================================
# READ ADMIN TOKEN
# ============================================

if [[ -f "$ADMIN_TOKEN_FILE" ]]; then
    ADMIN_TOKEN=$(cat "$ADMIN_TOKEN_FILE")
else
    log_fatal "Admin token not found at $ADMIN_TOKEN_FILE"
fi

# ============================================
# FIND PROPOSED EDGES
# ============================================

EDGE_RIDS=$(psql -d personal_koi -Atc "
    SELECT edge_rid FROM koi_net_edges WHERE status='PROPOSED'
      AND ((source_node='$LOCAL_RID' AND target_node='$PEER_RID')
        OR (source_node='$PEER_RID' AND target_node='$LOCAL_RID'))
")

if [[ -z "$EDGE_RIDS" ]]; then
    log_info "No PROPOSED edges found between $LOCAL_RID and $PEER_RID"
    echo "All edges are already approved (or none exist)."
    exit 0
fi

# ============================================
# APPROVE EACH EDGE
# ============================================

APPROVED=0
FAILED=0

while IFS= read -r edge_rid; do
    [[ -z "$edge_rid" ]] && continue
    log_info "Approving edge: $edge_rid"

    RESPONSE=$(curl -sf -X POST "http://127.0.0.1:${KOI_PORT}/koi-net/edges/approve" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $ADMIN_TOKEN" \
        -d "{\"edge_rid\": \"$edge_rid\"}" 2>&1) || {
        log_error "Failed to approve edge: $edge_rid"
        FAILED=$((FAILED + 1))
        continue
    }
    APPROVED=$((APPROVED + 1))
done <<< "$EDGE_RIDS"

# ============================================
# SUMMARY
# ============================================

echo ""
echo "==================================="
echo "  Edge Approval Complete"
echo "==================================="
echo ""
echo "  Peer:     $PEER_ID ($PEER_RID)"
echo "  Approved: $APPROVED"
if [[ $FAILED -gt 0 ]]; then
    echo "  Failed:   $FAILED"
fi
echo "==================================="
