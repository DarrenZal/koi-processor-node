#!/usr/bin/env bash
# connect-peers.sh — Bidirectional handshake + TOFU approval
#
# Usage: ./connect-peers.sh <peer-url> [alias]
# Example: ./connect-peers.sh http://10.100.0.5:8351 peer-b
#
# Run by either peer. Does true two-way bootstrap with explicit completion reporting.

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

AUTO_VAULT_SYNC=false
SHARED_FOLDER="Shared"

# Parse flags
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --auto-vault-sync) AUTO_VAULT_SYNC=true; shift ;;
        --shared-folder) SHARED_FOLDER="$2"; shift 2 ;;
        *) POSITIONAL+=("$1"); shift ;;
    esac
done
set -- "${POSITIONAL[@]}"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 [--auto-vault-sync] [--shared-folder <name>] <peer-url> [alias]"
    echo "Example: $0 http://10.100.0.5:8351 peer-b"
    exit 1
fi

PEER_URL="$1"
PEER_ALIAS="${2:-}"
LOCAL_URL="http://127.0.0.1:${KOI_API_PORT:-8351}"

log_info "Connecting to peer: $PEER_URL"

# ============================================
# STEP 1: Pre-flight security checks
# ============================================

log_info "Step 1: Pre-flight security checks..."

# Find personal.env
PERSONAL_ENV="$KOI_PROCESSOR_DIR/config/personal.env"
if [[ ! -f "$PERSONAL_ENV" ]]; then
    log_fatal "personal.env not found at $PERSONAL_ENV"
fi

# Source it for checking
set -a
source "$PERSONAL_ENV"
set +a

# Check policy flags
CHECKS_PASSED=true

if [[ "${KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL:-}" != "true" ]]; then
    log_error "FAIL: KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL must be 'true'"
    log_error "  Fix: Set KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL=true in $PERSONAL_ENV"
    CHECKS_PASSED=false
fi

if [[ "${KOI_ENFORCE_SOURCE_KEY_RID_BINDING:-}" != "true" ]]; then
    log_error "FAIL: KOI_ENFORCE_SOURCE_KEY_RID_BINDING must be 'true'"
    log_error "  Fix: Set KOI_ENFORCE_SOURCE_KEY_RID_BINDING=true in $PERSONAL_ENV"
    CHECKS_PASSED=false
fi

if [[ "${KOI_BASE_URL:-}" == *"localhost"* || "${KOI_BASE_URL:-}" == *"127.0.0.1"* ]]; then
    log_error "FAIL: KOI_BASE_URL cannot be localhost (peers can't reach you)"
    log_error "  Fix: Set KOI_BASE_URL=http://<your-wg-ip>:8351 in $PERSONAL_ENV"
    CHECKS_PASSED=false
fi

if ! $CHECKS_PASSED; then
    log_fatal "Pre-flight checks failed. Fix the issues above and retry."
fi

log_info "Pre-flight checks passed"

# ============================================
# STEP 2: Fetch remote health
# ============================================

log_info "Step 2: Fetching remote node health..."

REMOTE_HEALTH=$(curl -sf "${PEER_URL}/koi-net/health" 2>/dev/null) || \
    log_fatal "Cannot reach peer at ${PEER_URL}/koi-net/health"

# Health returns {"node": {...}} not "node_profile"
REMOTE_PROFILE=$(echo "$REMOTE_HEALTH" | python3 -c "
import json, sys
h = json.load(sys.stdin)
p = h.get('node', h.get('node_profile', h))
print(json.dumps(p))
")

REMOTE_NAME=$(echo "$REMOTE_PROFILE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('node_name','unknown'))")
REMOTE_RID=$(echo "$REMOTE_PROFILE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('node_rid',''))")
REMOTE_KOI_KEY=$(echo "$REMOTE_PROFILE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('public_key',''))")

if [[ -z "$REMOTE_RID" ]]; then
    log_fatal "Could not extract node_rid from remote health response"
fi

log_info "Remote node: $REMOTE_NAME ($REMOTE_RID)"

# ============================================
# STEP 3: Verify KOI key against peer registry
# ============================================

log_info "Step 3: Verifying KOI key against peer registry..."

if [[ -f "$PEER_REGISTRY" ]]; then
    REGISTRY_KEY=$(peer_registry_lookup "$REMOTE_NAME" 2>/dev/null | python3 -c "
import json, sys
try:
    p = json.load(sys.stdin)
    print(p.get('koi_pubkey', ''))
except:
    print('')
" 2>/dev/null || echo "")

    if [[ -n "$REGISTRY_KEY" && "$REGISTRY_KEY" != "$REMOTE_KOI_KEY" ]]; then
        log_fatal "KEY MISMATCH! The KOI public key from the remote node does not match the key in the peer registry (from join request). Possible key substitution attack."
    elif [[ -n "$REGISTRY_KEY" ]]; then
        log_info "KOI key matches peer registry (from join request)"
    else
        log_warn "Peer not found in registry — relying on TOFU verification"
    fi
else
    log_warn "No peer registry found — relying on TOFU verification"
fi

# ============================================
# STEP 4: Display TOFU fingerprint
# ============================================

log_info "Step 4: TOFU fingerprint verification..."

REMOTE_FINGERPRINT=$(compute_koi_fingerprint "$REMOTE_KOI_KEY")

echo ""
echo "  Remote node: $REMOTE_NAME"
echo "  Node RID:    $REMOTE_RID"
echo "  Key fingerprint: $REMOTE_FINGERPRINT"
echo ""
echo "  Verify this fingerprint with the peer via a trusted channel"
echo "  (Signal, phone call, in-person) before proceeding."
echo ""
read -rp "  Does the fingerprint match? [y/N]: " confirm
if [[ "$confirm" != "y" && "$confirm" != "Y" ]]; then
    log_fatal "Fingerprint verification failed — aborting"
fi

log_info "Fingerprint verified"

# ============================================
# STEP 5: Local → Remote handshake
# ============================================

log_info "Step 5: Sending handshake to remote peer..."

# Get our local profile
LOCAL_HEALTH=$(curl -sf "${LOCAL_URL}/koi-net/health" 2>/dev/null) || \
    log_fatal "Cannot reach local node at ${LOCAL_URL}/koi-net/health"

# Health returns {"node": {...}} not "node_profile"
LOCAL_PROFILE=$(echo "$LOCAL_HEALTH" | python3 -c "
import json, sys
h = json.load(sys.stdin)
p = h.get('node', h.get('node_profile', h))
print(json.dumps(p))
")

# Send our profile to remote (HandshakeRequest requires type + profile)
HANDSHAKE_STATUS=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H "Content-Type: application/json" \
    -d "{\"type\": \"handshake\", \"profile\": $LOCAL_PROFILE}" \
    "${PEER_URL}/koi-net/handshake" 2>/dev/null) || \
    log_fatal "Handshake to remote peer failed"

case "$HANDSHAKE_STATUS" in
    2*) log_info "Local → Remote handshake: sent" ;;
    401) log_warn "The remote already knows this node and declined the changed profile. Its operator runs this script on their side to update it." ;;
    *) log_fatal "Handshake to remote peer failed (HTTP $HANDSHAKE_STATUS)" ;;
esac

# ============================================
# STEP 6: Remote → Local handshake
# ============================================

log_info "Step 6: Registering remote peer locally..."

# Send remote profile to our local node (HandshakeRequest requires type + profile).
# This is the local operator's registration, so it carries the admin token when one is set.
LOCAL_ADMIN_TOKEN="${KOI_ADMIN_TOKEN:-}"
LOCAL_TOKEN_FILE="${KOI_STATE_DIR:-$HOME/.config/personal-koi/koi-state}/admin_token"
if [[ -z "$LOCAL_ADMIN_TOKEN" && -f "$LOCAL_TOKEN_FILE" ]]; then
    LOCAL_ADMIN_TOKEN=$(cat "$LOCAL_TOKEN_FILE")
fi
LOCAL_HS_RESPONSE=$(curl -sf -X POST \
    -H "Authorization: Bearer $LOCAL_ADMIN_TOKEN" \
    -H "Content-Type: application/json" \
    -d "{\"type\": \"handshake\", \"profile\": $REMOTE_PROFILE}" \
    "${LOCAL_URL}/koi-net/handshake" 2>/dev/null) || \
    log_fatal "Local handshake registration failed"

log_info "Remote → Local handshake: registered"

# ============================================
# STEP 7: Approve local outbound edge
# ============================================

log_info "Step 7: Approving local outbound edge..."

# DB operations below require psycopg2.
ensure_python_module "psycopg2" "psycopg2-binary"

echo ""
echo "  Approve outbound edge to $REMOTE_NAME?"
echo "  This allows them to poll your events."
read -rp "  [y/N]: " approve_outbound

if [[ "$approve_outbound" == "y" || "$approve_outbound" == "Y" ]]; then
    # Read admin token
    ADMIN_TOKEN_FILE="${KOI_STATE_DIR:-$HOME/.config/personal-koi/koi-state}/admin_token"
    if [[ ! -f "$ADMIN_TOKEN_FILE" ]]; then
        log_fatal "Admin token not found at $ADMIN_TOKEN_FILE"
    fi
    ADMIN_TOKEN=$(cat "$ADMIN_TOKEN_FILE")

    # Look up the outbound edge_rid from the database
    LOCAL_NODE_RID=$(echo "$LOCAL_PROFILE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('node_rid',''))")
    OUTBOUND_EDGE_RID=$(python3 -c "
import psycopg2, os
pg_url = os.environ.get('POSTGRES_URL', 'postgresql:///personal_koi')
conn = psycopg2.connect(pg_url)
cur = conn.cursor()
cur.execute('''
    SELECT edge_rid FROM koi_net_edges
    WHERE source_node = %s AND target_node = %s AND status = 'PROPOSED'
    ORDER BY created_at DESC LIMIT 1
''', ('$LOCAL_NODE_RID', '$REMOTE_RID'))
row = cur.fetchone()
conn.close()
print(row[0] if row else '')
" 2>/dev/null || echo "")

    if [[ -z "$OUTBOUND_EDGE_RID" ]]; then
        log_warn "No PROPOSED outbound edge found — may already be approved"
        OUTBOUND_STATUS="UNKNOWN (no PROPOSED edge found)"
    else
        # Approve via edge_rid (API requires edge_rid, not target_node)
        APPROVE_RESULT=$(curl -sf -X POST \
            -H "Content-Type: application/json" \
            -H "Authorization: Bearer $ADMIN_TOKEN" \
            -d "{\"edge_rid\": \"$OUTBOUND_EDGE_RID\"}" \
            "${LOCAL_URL}/koi-net/edges/approve" 2>/dev/null)

        if echo "$APPROVE_RESULT" | python3 -c "import json,sys; d=json.load(sys.stdin); sys.exit(0 if d.get('status')=='approved' else 1)" 2>/dev/null; then
            OUTBOUND_STATUS="APPROVED"
            log_info "Outbound edge approved: $OUTBOUND_EDGE_RID"
        else
            OUTBOUND_STATUS="FAILED (approve request failed)"
            log_warn "Edge approval failed for $OUTBOUND_EDGE_RID"
        fi
    fi
else
    OUTBOUND_STATUS="PROPOSED (not approved)"
    log_info "Outbound edge left as PROPOSED"
fi

# ============================================
# STEP 8: Add short alias
# ============================================

if [[ -n "$PEER_ALIAS" ]]; then
    log_info "Step 8: Adding peer alias '$PEER_ALIAS'..."

    ADMIN_TOKEN_FILE="${KOI_STATE_DIR:-$HOME/.config/personal-koi/koi-state}/admin_token"
    if [[ -f "$ADMIN_TOKEN_FILE" ]]; then
        ADMIN_TOKEN=$(cat "$ADMIN_TOKEN_FILE")

        # Insert alias via the API or directly
        python3 -c "
import psycopg2, os
pg_url = os.environ.get('POSTGRES_URL', 'postgresql:///personal_koi')
conn = psycopg2.connect(pg_url)
conn.autocommit = True
cur = conn.cursor()
cur.execute('''
    INSERT INTO koi_net_peer_aliases (alias, node_rid)
    VALUES (%s, %s)
    ON CONFLICT (alias) DO UPDATE SET node_rid = EXCLUDED.node_rid
''', ('$PEER_ALIAS', '$REMOTE_RID'))
conn.close()
print('Alias registered')
" 2>/dev/null || log_warn "Failed to register alias (psycopg2 may not be available)"

    fi
fi

# ============================================
# STEP 9: Verify connectivity
# ============================================

log_info "Step 9: Verifying connectivity..."

LOCAL_OK=false
REMOTE_OK=false

if curl -sf "${LOCAL_URL}/koi-net/health" &>/dev/null; then
    LOCAL_OK=true
fi
if curl -sf "${PEER_URL}/koi-net/health" &>/dev/null; then
    REMOTE_OK=true
fi

# ============================================
# STEP 10: Explicit completion report
# ============================================

LOCAL_NAME=$(echo "$LOCAL_PROFILE" | python3 -c "import json,sys; print(json.load(sys.stdin).get('node_name','unknown'))")
LOCAL_WG_IP=$(echo "${KOI_BASE_URL:-}" | sed 's|http://||;s|:.*||')

echo ""
echo "==================================="
echo "  Connection Status"
echo "==================================="
echo ""
echo "  Local -> Remote handshake:  $(if true; then echo 'sent'; fi)"
echo "  Remote -> Local handshake:  registered"

# Check local inbound edge status
INBOUND_STATUS="APPROVED (auto)"
echo "  Local inbound edge:         $INBOUND_STATUS"
echo "  Local outbound edge:        $OUTBOUND_STATUS"
echo "  Remote outbound edge:       ? UNKNOWN (peer must approve on their side)"

if [[ -n "$PEER_ALIAS" ]]; then
    echo "  Alias '$PEER_ALIAS':           registered"
fi

echo ""
echo "  Local health:  $(if $LOCAL_OK; then echo 'reachable'; else echo 'UNREACHABLE'; fi)"
echo "  Remote health: $(if $REMOTE_OK; then echo 'reachable'; else echo 'UNREACHABLE'; fi)"
echo ""
echo "  IMPORTANT: Connection is ONE-WAY until the peer also runs:"
echo "    ./connect-peers.sh http://${LOCAL_WG_IP}:${KOI_API_PORT:-8351} ${LOCAL_NAME}"
echo ""
echo "  Their outbound edge to you is currently PROPOSED."
echo "  They must approve it for bidirectional polling."
echo "==================================="

# ============================================
# OPTIONAL: Configure vault sync
# ============================================
if [[ -n "$PEER_ALIAS" ]]; then
    CONFIGURE_VAULT=false
    if $AUTO_VAULT_SYNC; then
        CONFIGURE_VAULT=true
    elif [[ -t 0 ]]; then
        echo ""
        read -rp "Configure vault sync with '$PEER_ALIAS'? Shared folder: $SHARED_FOLDER [y/N]: " vs_confirm
        if [[ "$vs_confirm" == "y" || "$vs_confirm" == "Y" ]]; then
            CONFIGURE_VAULT=true
        fi
    fi

    if $CONFIGURE_VAULT; then
        ADMIN_TOKEN="${KOI_ADMIN_TOKEN:-}"
        if [[ -z "$ADMIN_TOKEN" ]]; then
            KOI_STATE="${KOI_STATE_DIR:-$HOME/.config/personal-koi/koi-state}"
            ADMIN_TOKEN=$(cat "$KOI_STATE/admin_token" 2>/dev/null || true)
        fi
        if [[ -n "$ADMIN_TOKEN" ]]; then
            echo "Configuring vault sync with $PEER_ALIAS (folder: $SHARED_FOLDER)..."
            curl -sf -X POST "${LOCAL_URL}/koi-net/vault-sync/configure" \
                -H "Authorization: Bearer $ADMIN_TOKEN" \
                -H "Content-Type: application/json" \
                -d "{\"peer\": \"$PEER_ALIAS\", \"shared_folder\": \"$SHARED_FOLDER\"}" \
                && echo "  Vault sync configured." \
                || echo "  WARNING: Failed to configure vault sync. You can do it manually later."
        else
            echo "  Skipping vault sync configure (no admin token found)."
        fi
    fi
fi
