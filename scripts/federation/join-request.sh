#!/usr/bin/env bash
# join-request.sh — Peer generates their own WireGuard + KOI keys
#
# Usage: ./join-request.sh <peer-name> [state-dir]
# Example: ./join-request.sh peer-b
#
# Run on the peer's machine. Private keys NEVER leave this machine.

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

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <peer-name> [state-dir]"
    echo "Example: $0 peer-b"
    exit 1
fi

PEER_NAME="$1"
STATE_DIR="${2:-$HOME/.config/personal-koi}"

validate_peer_name "$PEER_NAME"

WG_DIR="$STATE_DIR/wireguard"
KOI_STATE="$STATE_DIR/koi-state"

log_info "Generating join request for peer: $PEER_NAME"

# ============================================
# STEP 1: Install wireguard-tools if missing
# ============================================

log_info "Step 1: Checking wireguard-tools..."

if ! check_wg_tools; then
    log_fatal "Please install wireguard-tools first"
fi

# ============================================
# STEP 2: Create state directories
# ============================================

log_info "Step 2: Creating state directories..."

mkdir -p "$WG_DIR"
mkdir -p "$KOI_STATE"
chmod 700 "$WG_DIR" "$KOI_STATE"

# ============================================
# STEP 3: Generate WireGuard keypair
# ============================================

log_info "Step 3: Generating WireGuard keypair..."

WG_PRIVKEY_FILE="$WG_DIR/private.key"
WG_PUBKEY_FILE="$WG_DIR/public.key"

if [[ -f "$WG_PRIVKEY_FILE" ]]; then
    log_info "Using existing WireGuard keypair"
    WG_PUBKEY=$(cat "$WG_PUBKEY_FILE")
else
    umask 077
    wg genkey > "$WG_PRIVKEY_FILE"
    wg pubkey < "$WG_PRIVKEY_FILE" > "$WG_PUBKEY_FILE"
    chmod 600 "$WG_PRIVKEY_FILE"
    WG_PUBKEY=$(cat "$WG_PUBKEY_FILE")
    log_info "Generated new WireGuard keypair"
fi

# ============================================
# STEP 4: Generate KOI node identity
# ============================================

log_info "Step 4: Generating KOI node identity..."

require_python_3_11

# Check for cryptography module (required for ECDSA key generation)
if ! python3 -c "import cryptography" 2>/dev/null; then
    log_warn "Python 'cryptography' package not found"
    # Try to install it
    if [[ -f "$KOI_PROCESSOR_DIR/venv/bin/activate" ]]; then
        log_info "Activating venv and installing..."
        source "$KOI_PROCESSOR_DIR/venv/bin/activate"
        pip install -q cryptography
    else
        log_info "Attempting pip install..."
        pip3 install -q cryptography 2>/dev/null || \
            log_fatal "Cannot install 'cryptography'. Run: pip install cryptography"
    fi
    # Verify
    python3 -c "import cryptography" 2>/dev/null || \
        log_fatal "cryptography package still not available after install attempt"
fi

KOI_KEY_FILE="$KOI_STATE/${PEER_NAME}_private_key.pem"

# Use the koi-processor's node_identity module
KOI_IDENTITY=$(KOI_STATE_DIR="$KOI_STATE" python3 -c "
import sys, os
sys.path.insert(0, '$KOI_PROCESSOR_DIR')
os.environ['KOI_STATE_DIR'] = '$KOI_STATE'

from api.node_identity import load_or_create_identity, get_public_key_der_b64

# load_or_create_identity may return (private_key, profile) or
# (private_key, profile, encryption_private_key) depending on version.
identity = load_or_create_identity('$PEER_NAME')
if not isinstance(identity, tuple) or len(identity) < 2:
    raise RuntimeError(f'Unexpected identity return shape: {type(identity)}')
private_key, profile = identity[0], identity[1]
koi_pubkey = get_public_key_der_b64(private_key)

print(f'node_rid={profile.node_rid}')
print(f'koi_public_key={koi_pubkey}')
")

NODE_RID=$(echo "$KOI_IDENTITY" | grep '^node_rid=' | cut -d= -f2-)
KOI_PUBKEY=$(echo "$KOI_IDENTITY" | grep '^koi_public_key=' | cut -d= -f2-)

if [[ -z "$NODE_RID" || -z "$KOI_PUBKEY" ]]; then
    log_fatal "Failed to generate KOI node identity"
fi

log_info "Node RID: $NODE_RID"

# ============================================
# STEP 5: Compute fingerprint
# ============================================

FINGERPRINT=$(compute_koi_fingerprint "$KOI_PUBKEY")

# ============================================
# STEP 6: Generate join request
# ============================================

log_info "Step 5: Writing join request..."

GENERATED=$(date -u +%Y-%m-%dT%H:%M:%SZ)
EXPIRES=$(date -u -v+24H +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -d '+24 hours' +%Y-%m-%dT%H:%M:%SZ)

JOIN_REQUEST="=== KOI-net Join Request ===
peer_name: $PEER_NAME
wg_public_key: $WG_PUBKEY
koi_public_key: $KOI_PUBKEY
node_rid: $NODE_RID
key_fingerprint: $FINGERPRINT
generated: $GENERATED
expires: $EXPIRES
==========================="

JOIN_REQUEST_FILE="$WG_DIR/join-request.txt"
echo "$JOIN_REQUEST" > "$JOIN_REQUEST_FILE"

echo ""
echo "==================================="
echo "  KOI-net Join Request Generated"
echo "==================================="
echo ""
echo "$JOIN_REQUEST"
echo ""
echo "  Saved to: $JOIN_REQUEST_FILE"
echo ""
echo "  Private keys stored (NEVER share these):"
echo "    WG:  $WG_PRIVKEY_FILE"
echo "    KOI: $KOI_KEY_FILE"
echo ""
echo "  Key fingerprint: $FINGERPRINT"
echo "    (verify this with admin after approval)"
echo ""
echo "  Next steps:"
echo "    1. Send the join request block above to admin via secure channel (Signal)"
echo "    2. Wait for admin to run approve-peer.sh"
echo "    3. Admin will send you a WireGuard config template"
echo "    4. Insert your private key and activate WireGuard"
echo "    5. Run setup-node.sh to configure KOI-net"
echo "==================================="
