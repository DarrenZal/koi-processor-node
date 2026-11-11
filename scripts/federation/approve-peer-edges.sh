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

# Values reach SQL as psql variables (:'name', quoted by psql), never pasted into the query,
# and the admin token reaches curl through a file descriptor, never its argv (as in
# connect-peers.sh). The database is personal_koi unless KOI_PSQL_DB says otherwise.
PSQL_DB="${KOI_PSQL_DB:-personal_koi}"

# ============================================
# RESOLVE PEER RID
# ============================================

if [[ "$PEER_ID" == orn:koi-net.node:* ]]; then
    PEER_RID="$PEER_ID"
else
    # An alias first, then a node name.
    RIDS=$(psql -d "$PSQL_DB" -At -v id="$PEER_ID" -v ON_ERROR_STOP=1 <<'SQL'
SELECT node_rid FROM koi_net_peer_aliases WHERE alias = :'id'
UNION
SELECT node_rid FROM koi_net_nodes WHERE lower(node_name) = lower(:'id')
  AND NOT EXISTS (SELECT 1 FROM koi_net_peer_aliases WHERE alias = :'id');
SQL
) || log_fatal "Peer lookup failed (psql -d $PSQL_DB)"
    COUNT=$(printf '%s\n' "$RIDS" | grep -c . || true)
    if [[ "$COUNT" -eq 0 ]]; then
        log_fatal "Peer '$PEER_ID' not found in database"
    elif [[ "$COUNT" -gt 1 ]]; then
        log_fatal "Ambiguous peer name '$PEER_ID' — use exact node_rid instead"
    fi
    PEER_RID="$RIDS"
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

EDGE_RIDS=$(psql -d "$PSQL_DB" -At -v me="$LOCAL_RID" -v peer="$PEER_RID" -v ON_ERROR_STOP=1 <<'SQL'
SELECT edge_rid FROM koi_net_edges WHERE status = 'PROPOSED'
  AND ((source_node = :'me' AND target_node = :'peer') OR (source_node = :'peer' AND target_node = :'me'));
SQL
) || log_fatal "Edge lookup failed"

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

    BODY=$(AP_EDGE_RID="$edge_rid" python3 -c "import json,os; print(json.dumps({'edge_rid': os.environ['AP_EDGE_RID']}))")
    RESPONSE=$(curl -sf -X POST "http://127.0.0.1:${KOI_PORT}/koi-net/edges/approve" \
        -H "Content-Type: application/json" \
        -H @<(printf 'Authorization: Bearer %s\n' "$ADMIN_TOKEN") \
        -d "$BODY" 2>/dev/null) || {
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
