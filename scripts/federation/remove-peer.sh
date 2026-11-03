#!/usr/bin/env bash
# remove-peer.sh — Offboarding / revocation
#
# Usage: ./remove-peer.sh [--dry-run] <peer-name-or-rid> [relay-ssh]
# Example: ./remove-peer.sh peer-b <relay-user>@<relay-host>
#
# Critical: delivers retractions BEFORE revoking transport.

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# Prefer project virtualenv Python if available.
if [[ -f "$KOI_PROCESSOR_DIR/venv/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source "$KOI_PROCESSOR_DIR/venv/bin/activate"
fi

# ============================================
# PARSE ARGS
# ============================================

if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
    shift
fi

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 [--dry-run] <peer-name-or-rid> [relay-ssh]"
    echo "Example: $0 peer-b <relay-user>@<relay-host>"
    exit 1
fi

PEER_QUERY="$1"
RELAY_SSH="${2:-<relay-user>@203.0.113.13}"

log_info "Removing peer: $PEER_QUERY"
log_info "Dry run: $DRY_RUN"

# ============================================
# STEP 1: Resolve peer
# ============================================

log_info "Step 1: Resolving peer..."

# Source config for DB access
PERSONAL_ENV="$KOI_PROCESSOR_DIR/config/personal.env"
if [[ -f "$PERSONAL_ENV" ]]; then
    set -a
    source "$PERSONAL_ENV"
    set +a
fi

POSTGRES_URL="${POSTGRES_URL:-postgresql:///personal_koi}"

# DB operations below require psycopg2.
ensure_python_module "psycopg2" "psycopg2-binary"

# Try peer registry first
PEER_INFO=""
if [[ -f "$PEER_REGISTRY" ]]; then
    PEER_INFO=$(peer_registry_lookup "$PEER_QUERY" 2>/dev/null || echo "")
fi

if [[ -z "$PEER_INFO" ]]; then
    # Try database alias lookup
    PEER_INFO=$(python3 -c "
import json, psycopg2
conn = psycopg2.connect('$POSTGRES_URL')
cur = conn.cursor()
# Try alias
cur.execute('SELECT node_rid FROM koi_net_peer_aliases WHERE alias = %s', ('$PEER_QUERY',))
row = cur.fetchone()
if row:
    node_rid = row[0]
else:
    # Try direct node_rid or node_name
    cur.execute('SELECT node_rid, node_name, base_url, public_key FROM koi_net_nodes WHERE node_rid = %s OR node_name = %s', ('$PEER_QUERY', '$PEER_QUERY'))
    row = cur.fetchone()
    if row:
        node_rid = row[0]
    else:
        print('')
        exit(0)

# Get full node info
cur.execute('SELECT node_rid, node_name, base_url, public_key FROM koi_net_nodes WHERE node_rid = %s', (node_rid,))
n = cur.fetchone()
if n:
    print(json.dumps({'node_rid': n[0], 'node_name': n[1], 'base_url': n[2], 'public_key': n[3]}))
conn.close()
" 2>/dev/null || echo "")
fi

if [[ -z "$PEER_INFO" ]]; then
    log_fatal "Peer not found: $PEER_QUERY"
fi

PEER_NAME=$(echo "$PEER_INFO" | python3 -c "
import json, sys
d = json.load(sys.stdin)
print(d.get('peer_name', d.get('node_name', '')))
" 2>/dev/null || echo "$PEER_QUERY")
NODE_RID=$(echo "$PEER_INFO" | python3 -c "import json,sys; print(json.load(sys.stdin).get('node_rid',''))")

log_info "Resolved peer: $PEER_NAME ($NODE_RID)"

# Get WG pubkey from peer registry for relay removal
WG_PUBKEY=$(echo "$PEER_INFO" | python3 -c "import json,sys; print(json.load(sys.stdin).get('wg_pubkey',''))" 2>/dev/null || echo "")

# ============================================
# PHASE 1: Retract (while transport is live)
# ============================================

log_info "Phase 1: Retracting shared documents..."

RETRACTION_RESULT=$(python3 -c "
import json, sys, uuid, psycopg2
from datetime import datetime, timezone

pg_url = '$POSTGRES_URL'
node_rid = '$NODE_RID'
dry_run = $( $DRY_RUN && echo "True" || echo "False" )

conn = psycopg2.connect(pg_url)
conn.autocommit = True
cur = conn.cursor()

# Check if koi_outbound_shares table exists (requires migration 045)
cur.execute(\"\"\"
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'koi_outbound_shares'
    )
\"\"\")
ledger_exists = cur.fetchone()[0]

if not ledger_exists:
    print('koi_outbound_shares table does not exist (migration 045 not applied)', file=sys.stderr)
    print('Falling back to koi_net_events for retraction candidates...', file=sys.stderr)
    # Fallback: query events table for shares we sent to this peer
    cur.execute('''
        SELECT DISTINCT rid FROM koi_net_events
        WHERE source_node = %s AND target_node = %s AND event_type IN ('NEW', 'UPDATE')
    ''', ('$KOI_NODE_NAME', node_rid))
    shares = cur.fetchall()
else:
    # Query outbound share ledger for un-retracted shares
    cur.execute('''
        SELECT document_rid FROM koi_outbound_shares
        WHERE target_node = %s AND retracted_at IS NULL
    ''', (node_rid,))
    shares = cur.fetchall()

if not shares:
    print(json.dumps({'count': 0, 'documents': []}))
    sys.exit(0)

docs = [row[0] for row in shares]
print(f'Found {len(docs)} documents to retract', file=sys.stderr)

if dry_run:
    print(json.dumps({'count': len(docs), 'documents': docs, 'dry_run': True}))
    sys.exit(0)

# Emit FORGET events for each document
retracted = []
for doc_rid in docs:
    event_id = str(uuid.uuid4())
    try:
        cur.execute('''
            INSERT INTO koi_net_events (event_id, event_type, rid, source_node, target_node, manifest, contents)
            VALUES (%s, 'FORGET', %s, %s, %s, '{}'::jsonb, '{}'::jsonb)
        ''', (event_id, doc_rid, '$KOI_NODE_NAME', node_rid))
        retracted.append(doc_rid)
    except Exception as e:
        print(f'Failed to emit FORGET for {doc_rid}: {e}', file=sys.stderr)

# Mark as retracted in ledger (if table exists)
if ledger_exists:
    now = datetime.now(timezone.utc).isoformat()
    for doc_rid in retracted:
        cur.execute('''
            UPDATE koi_outbound_shares SET retracted_at = %s
            WHERE document_rid = %s AND target_node = %s
        ''', (now, doc_rid, node_rid))

conn.close()
print(json.dumps({'count': len(retracted), 'documents': retracted}))
" 2>&1)

RETRACT_COUNT=$(echo "$RETRACTION_RESULT" | tail -1 | python3 -c "import json,sys; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo "0")
log_info "Retracted $RETRACT_COUNT document(s)"

# Wait for delivery (best-effort)
if [[ "$RETRACT_COUNT" -gt 0 ]] && ! $DRY_RUN; then
    log_info "Waiting 30s for FORGET events to be delivered..."
    sleep 30
fi

# ============================================
# PHASE 2: Revoke KOI access
# ============================================

log_info "Phase 2: Revoking KOI access..."

if $DRY_RUN; then
    echo "[DRY-RUN] Would revoke edges, aliases, and node status for $NODE_RID"
else
    python3 -c "
import psycopg2

conn = psycopg2.connect('$POSTGRES_URL')
conn.autocommit = True
cur = conn.cursor()

# Revoke all edges involving this peer
cur.execute('''
    UPDATE koi_net_edges SET status = 'REVOKED', updated_at = NOW()
    WHERE source_node = %s OR target_node = %s
''', ('$NODE_RID', '$NODE_RID'))
edges_revoked = cur.rowcount

# Delete aliases
cur.execute('''
    DELETE FROM koi_net_peer_aliases WHERE node_rid = %s
''', ('$NODE_RID',))
aliases_deleted = cur.rowcount

# Revoke node
cur.execute('''
    UPDATE koi_net_nodes SET status = 'revoked' WHERE node_rid = %s
''', ('$NODE_RID',))

conn.close()
print(f'Edges revoked: {edges_revoked}')
print(f'Aliases deleted: {aliases_deleted}')
print(f'Node status: revoked')
"
fi

# ============================================
# PHASE 3: Revoke network access
# ============================================

log_info "Phase 3: Revoking WireGuard access..."

if [[ -n "$WG_PUBKEY" ]]; then
    relay_config_edit "$RELAY_SSH" remove-peer "$WG_PUBKEY"
else
    log_warn "No WireGuard public key found in peer registry — skipping relay removal"
    log_warn "Manually remove the peer from /etc/wireguard/wg-koi.conf on the relay"
fi

# ============================================
# PHASE 4: Update peer registry
# ============================================

log_info "Updating peer registry..."

REMOVED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
if ! $DRY_RUN; then
    peer_registry_update_status "$PEER_NAME" "removed" "removed_at" "$REMOVED_AT" 2>/dev/null || \
        log_warn "Could not update peer registry (peer may not be in registry)"
fi

# ============================================
# SUMMARY
# ============================================

echo ""
echo "==================================="
echo "  Peer Removed: $PEER_NAME"
echo "==================================="
echo ""
echo "  Node RID:        $NODE_RID"
echo "  Documents retracted: $RETRACT_COUNT"
echo "  KOI edges:       revoked"
echo "  KOI aliases:     deleted"
echo "  KOI node status: revoked"
echo "  WireGuard:       $(if [[ -n "$WG_PUBKEY" ]]; then echo 'removed from relay'; else echo 'MANUAL removal needed'; fi)"
echo "  Peer registry:   $(if $DRY_RUN; then echo 'would update to removed'; else echo 'updated to removed'; fi)"
echo ""
if $DRY_RUN; then
    echo "  (DRY RUN — no changes were made)"
fi
echo "==================================="
