#!/usr/bin/env bash
# approve-peer.sh — Admin adds peer to relay mesh (no private keys)
#
# Usage: ./approve-peer.sh [--dry-run] <peer-name> <peer-number> <wg-public-key> [relay-ssh]
# Example: ./approve-peer.sh peer-b 3 "abc123..." <relay-user>@<relay-host>
#
# Can also read from join request file:
#   ./approve-peer.sh --from-file <join-request.txt> <peer-number> [relay-ssh]

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

# ============================================
# PARSE ARGS
# ============================================

if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN=true
    shift
fi

PUBKEY_ONLY=false
FROM_FILE=""

if [[ "${1:-}" == "--pubkey-only" ]]; then
    PUBKEY_ONLY=true
    WG_PUBKEY="$2"
    PEER_NUMBER="$3"
    RELAY_SSH="${4:-<relay-user>@203.0.113.13}"
    shift $((2 + ($# > 3 ? 2 : 1)))
    KOI_PUBKEY=""
    NODE_RID=""

    # Look up peer_name from registry by peer_number
    PEER_ENTRY=$(peer_registry_lookup_by_number "$PEER_NUMBER" 2>/dev/null) || \
        log_fatal "No invite found for peer_number=$PEER_NUMBER in registry"
    PEER_NAME=$(echo "$PEER_ENTRY" | python3 -c "import sys,json; print(json.load(sys.stdin)['peer_name'])")
    ENTRY_STATUS=$(echo "$PEER_ENTRY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',''))")
    ENTRY_CONSUMED=$(echo "$PEER_ENTRY" | python3 -c "import sys,json; print(json.load(sys.stdin).get('consumed', False))")

    if [[ "$ENTRY_STATUS" == "active" ]]; then
        log_fatal "Peer number $PEER_NUMBER already active (peer: $PEER_NAME)"
    fi

    if [[ "$ENTRY_STATUS" == "approving" ]]; then
        # Stale approving recovery: check relay state
        log_warn "Found stale 'approving' status for $PEER_NAME — checking relay state..."
        PEER_IP="10.100.0.${PEER_NUMBER}"
        RELAY_STATE=$(run_ssh "$RELAY_SSH" "sudo wg show wg-koi dump" 2>/dev/null) || \
            log_fatal "Cannot query relay WG state"
        # Check if IP is already on relay
        if echo "$RELAY_STATE" | grep -q "${PEER_IP}/32"; then
            # IP exists — check if pubkey matches
            RELAY_PEER_PUBKEY=$(echo "$RELAY_STATE" | grep "${PEER_IP}/32" | awk '{print $1}')
            if [[ "$RELAY_PEER_PUBKEY" == "$WG_PUBKEY" ]]; then
                log_info "Relay already has correct pubkey for $PEER_IP — skipping relay_config_edit"
                # Finalize registry
                peer_registry_update_status "$PEER_NAME" "active" "wg_pubkey" "$WG_PUBKEY"
                peer_registry_update_status "$PEER_NAME" "active" "consumed" "true"
                log_info "Registry finalized: $PEER_NAME → active"
                echo ""
                echo "==================================="
                echo "  Peer Approved (recovered): $PEER_NAME"
                echo "==================================="
                echo ""
                echo "  After peer connects, verify SAS code over Signal"
                echo "==================================="
                exit 0
            else
                log_fatal "IP $PEER_IP already bound to different pubkey on relay — manual cleanup required"
            fi
        fi
        # IP not on relay — re-run normally
        log_info "IP not found on relay, proceeding with add-peer"
    fi

    if [[ "$ENTRY_STATUS" != "invited" && "$ENTRY_STATUS" != "approving" ]]; then
        log_fatal "Unexpected registry status '$ENTRY_STATUS' for peer_number=$PEER_NUMBER (expected 'invited')"
    fi

elif [[ "${1:-}" == "--from-file" ]]; then
    FROM_FILE="$2"
    shift 2

    # Parse from join request file
    if [[ $# -lt 1 ]]; then
        echo "Usage: $0 [--dry-run] --from-file <join-request.txt> <peer-number> [relay-ssh]"
        exit 1
    fi
    PEER_NUMBER="$1"
    RELAY_SSH="${2:-<relay-user>@203.0.113.13}"

    # Parse join request
    FIELDS=$(parse_join_request "$FROM_FILE")
    if [[ $? -ne 0 ]]; then
        log_fatal "Failed to parse join request"
    fi

    PEER_NAME=$(echo "$FIELDS" | grep '^peer_name=' | cut -d= -f2-)
    WG_PUBKEY=$(echo "$FIELDS" | grep '^wg_public_key=' | cut -d= -f2-)
    KOI_PUBKEY=$(echo "$FIELDS" | grep '^koi_public_key=' | cut -d= -f2-)
    NODE_RID=$(echo "$FIELDS" | grep '^node_rid=' | cut -d= -f2-)
else
    if [[ $# -lt 3 ]]; then
        echo "Usage: $0 [--dry-run] <peer-name> <peer-number> <wg-public-key> [relay-ssh]"
        echo "   or: $0 [--dry-run] --from-file <join-request.txt> <peer-number> [relay-ssh]"
        echo "   or: $0 [--dry-run] --pubkey-only <wg-pubkey> <peer-number> [relay-ssh]"
        exit 1
    fi
    PEER_NAME="$1"
    PEER_NUMBER="$2"
    WG_PUBKEY="$3"
    RELAY_SSH="${4:-<relay-user>@203.0.113.13}"
    KOI_PUBKEY=""
    NODE_RID=""
fi

# ============================================
# VALIDATE INPUTS
# ============================================

validate_peer_name "$PEER_NAME"
validate_peer_number "$PEER_NUMBER"
validate_wg_pubkey "$WG_PUBKEY"
validate_ssh_target "$RELAY_SSH"

PEER_IP="10.100.0.${PEER_NUMBER}"

log_info "Approving peer: $PEER_NAME (IP: $PEER_IP)"
log_info "Relay: $RELAY_SSH"
log_info "Dry run: $DRY_RUN"

# ============================================
# STEP 1: Record in peer registry
# ============================================

log_info "Step 1: Recording in peer registry..."

ADDED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)

if $PUBKEY_ONLY; then
    # Invite flow: transition invited → approving
    if $DRY_RUN; then
        echo "[DRY-RUN] Would update registry: $PEER_NAME invited → approving"
    else
        peer_registry_update_status "$PEER_NAME" "approving"
        log_info "Registry: $PEER_NAME → approving"
    fi
else
    # Direct flow: create new active entry
    REGISTRY_ENTRY=$(python3 -c "
import json
entry = {
    'peer_name': '$PEER_NAME',
    'peer_number': $PEER_NUMBER,
    'wg_ip': '$PEER_IP',
    'wg_pubkey': '$WG_PUBKEY',
    'koi_pubkey': '${KOI_PUBKEY:-}',
    'node_rid': '${NODE_RID:-}',
    'added_at': '$ADDED_AT',
    'status': 'active'
}
# Remove empty optional fields
entry = {k: v for k, v in entry.items() if v}
if 'status' not in entry:
    entry['status'] = 'active'
print(json.dumps(entry))
")

    if $DRY_RUN; then
        echo "[DRY-RUN] Would add to peer registry:"
        echo "$REGISTRY_ENTRY" | python3 -m json.tool
    else
        peer_registry_add "$REGISTRY_ENTRY"
        log_info "Added to peer registry: $PEER_REGISTRY"
    fi
fi

# ============================================
# STEP 2: Transactional relay config edit
# ============================================

log_info "Step 2: Adding peer to relay WireGuard config..."

if ! relay_config_edit "$RELAY_SSH" add-peer "$WG_PUBKEY" "$PEER_IP"; then
    if $PUBKEY_ONLY; then
        # Rollback registry: approving → invited
        peer_registry_update_status "$PEER_NAME" "invited" 2>/dev/null || true
        log_error "Relay config edit failed. Registry rolled back to 'invited'."
    fi
    log_fatal "Failed to add peer to relay"
fi

# Finalize registry for pubkey-only mode
if $PUBKEY_ONLY && ! $DRY_RUN; then
    peer_registry_update_status "$PEER_NAME" "active" "wg_pubkey" "$WG_PUBKEY"
    # Mark consumed via Python (update_status only handles one extra field)
    python3 -c "
import json, os, fcntl

registry = '$PEER_REGISTRY'
lockfile = registry + '.lock'

with open(lockfile, 'w') as lf:
    fcntl.flock(lf.fileno(), fcntl.LOCK_EX)
    with open(registry) as rf:
        reg = json.load(rf)
    # Find the active entry (just set by peer_registry_update_status)
    # Iterate in reverse to pick the latest entry if duplicates exist
    for p in reversed(reg):
        if p.get('peer_name') == '$PEER_NAME' and p.get('status') == 'active':
            p['consumed'] = True
            break
    tmp = registry + '.tmp.' + str(os.getpid())
    with open(tmp, 'w') as wf:
        json.dump(reg, wf, indent=2)
    os.replace(tmp, registry)
"
    log_info "Registry finalized: $PEER_NAME → active, consumed=true"
fi

if $PUBKEY_ONLY; then
    # Pubkey-only mode: no config template needed (peer generated WG config from invite token)
    echo ""
    echo "==================================="
    echo "  Peer Approved: $PEER_NAME"
    echo "==================================="
    echo ""
    echo "  WireGuard IP:   $PEER_IP"
    echo "  WG public key added to relay"
    echo ""
    echo "  After peer connects, verify SAS code over Signal:"
    echo "    ./compute-sas.sh $PEER_NAME"
    echo "  Then approve edges:"
    echo "    ./approve-peer-edges.sh $PEER_NAME"
    echo "==================================="
    exit 0
fi

# ============================================
# STEP 3: Get relay public key (direct mode only)
# ============================================

log_info "Step 3: Fetching relay public key..."

RELAY_PUBKEY=""
RELAY_ENDPOINT="${RELAY_SSH#*@}"
if ! $DRY_RUN; then
    RELAY_PUBKEY=$(ssh -o ConnectTimeout=10 "$RELAY_SSH" "sudo cat /etc/wireguard/koi_private.key | wg pubkey")
else
    RELAY_PUBKEY="<relay-public-key>"
fi

# ============================================
# STEP 4: Generate peer's WireGuard config template
# ============================================

log_info "Step 4: Generating WireGuard config template for peer..."

INVITE_DIR="federation-invite-${PEER_NAME}"
mkdir -p "$INVITE_DIR"

cat > "$INVITE_DIR/wg-koi.conf" <<CONF
[Interface]
Address = ${PEER_IP}/24
PrivateKey = <PASTE_YOUR_PRIVATE_KEY_HERE>

[Peer]
PublicKey = ${RELAY_PUBKEY}
Endpoint = ${RELAY_ENDPOINT}:51820
AllowedIPs = 10.100.0.0/24
PersistentKeepalive = 25
CONF

# ============================================
# STEP 5: Print setup instructions
# ============================================

echo ""
echo "==================================="
echo "  Peer Approved: $PEER_NAME"
echo "==================================="
echo ""
echo "  WireGuard IP:   $PEER_IP"
echo "  Relay endpoint: ${RELAY_ENDPOINT}:51820"
echo ""
echo "  Config template: $INVITE_DIR/wg-koi.conf"
echo "    (peer must insert their private key)"
echo ""
echo "  Send to peer:"
echo "    1. The config template ($INVITE_DIR/wg-koi.conf)"
echo "    2. Instructions to:"
echo "       a. Replace <PASTE_YOUR_PRIVATE_KEY_HERE> with their WG private key"
echo "       b. Copy to /etc/wireguard/wg-koi.conf (Linux) or use WireGuard app (macOS)"
echo "       c. Activate: wg-quick up wg-koi (Linux) or toggle in WireGuard app"
echo "       d. Test: ping 10.100.0.1"
echo "       e. Run: ./setup-node.sh $PEER_NAME $PEER_IP"
echo "       f. Run: ./connect-peers.sh http://10.100.0.2:8351 darren"
echo ""
if [[ -n "${KOI_PUBKEY:-}" ]]; then
    FINGERPRINT=$(compute_koi_fingerprint "$KOI_PUBKEY")
    echo "  KOI key fingerprint: $FINGERPRINT"
    echo "    (verify with peer during connect-peers.sh)"
fi
echo "==================================="
