#!/usr/bin/env bash
# setup-relay.sh — One-time VPS relay provisioning for KOI-net federation
#
# Usage: ./setup-relay.sh [--dry-run] <ssh-target> [listen-port]
# Example: ./setup-relay.sh <relay-user>@<relay-host> 51820
#
# Idempotent. Safe to re-run.

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

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 [--dry-run] <ssh-target> [listen-port]"
    echo "Example: $0 <relay-user>@<relay-host> 51820"
    exit 1
fi

SSH_TARGET="$1"
LISTEN_PORT="${2:-51820}"

validate_ssh_target "$SSH_TARGET"

log_info "Setting up KOI-net relay on $SSH_TARGET (port $LISTEN_PORT)"
log_info "Dry run: $DRY_RUN"

# ============================================
# STEP 1: Install wireguard-tools if missing
# ============================================

log_info "Step 1: Checking wireguard-tools..."

if ! $DRY_RUN; then
    if ! ssh_cmd_exists "$SSH_TARGET" wg; then
        log_info "Installing wireguard-tools..."
        ssh -o ConnectTimeout=10 "$SSH_TARGET" "sudo apt-get update -qq && sudo apt-get install -y -qq wireguard-tools"
    else
        log_info "wireguard-tools already installed"
    fi
else
    echo "[DRY-RUN] Would install wireguard-tools if missing"
fi

# ============================================
# STEP 2: Generate relay keypair if not exists
# ============================================

log_info "Step 2: Checking relay keypair..."

RELAY_PUBKEY=""
if ! $DRY_RUN; then
    RELAY_PUBKEY=$(ssh -o ConnectTimeout=10 "$SSH_TARGET" "
        if [ ! -f /etc/wireguard/koi_private.key ]; then
            umask 077
            wg genkey | sudo tee /etc/wireguard/koi_private.key > /dev/null
            sudo chmod 600 /etc/wireguard/koi_private.key
            echo 'GENERATED'
        else
            echo 'EXISTS'
        fi
        sudo cat /etc/wireguard/koi_private.key | wg pubkey
    ")
    local_status=$(echo "$RELAY_PUBKEY" | head -1)
    RELAY_PUBKEY=$(echo "$RELAY_PUBKEY" | tail -1)
    if [[ "$local_status" == "GENERATED" ]]; then
        log_info "Generated new relay keypair"
    else
        log_info "Using existing relay keypair"
    fi
else
    echo "[DRY-RUN] Would generate relay keypair at /etc/wireguard/koi_private.key if not exists"
fi

# ============================================
# STEP 3: Create wg-koi.conf (Interface only)
# ============================================

log_info "Step 3: Creating WireGuard config..."

if ! $DRY_RUN; then
    ssh -o ConnectTimeout=10 "$SSH_TARGET" "
        if [ ! -f /etc/wireguard/wg-koi.conf ]; then
            PRIVKEY=\$(sudo cat /etc/wireguard/koi_private.key)
            sudo tee /etc/wireguard/wg-koi.conf > /dev/null <<CONF
[Interface]
Address = 10.100.0.1/24
ListenPort = $LISTEN_PORT
PrivateKey = \$PRIVKEY
CONF
            sudo chmod 600 /etc/wireguard/wg-koi.conf
            echo 'CREATED'
        else
            echo 'EXISTS'
        fi
    "
else
    echo "[DRY-RUN] Would create /etc/wireguard/wg-koi.conf with Interface block (port $LISTEN_PORT)"
fi

# ============================================
# STEP 4: Enable IP forwarding
# ============================================

log_info "Step 4: Enabling IP forwarding..."

if ! $DRY_RUN; then
    ssh -o ConnectTimeout=10 "$SSH_TARGET" "
        if ! grep -q '^net.ipv4.ip_forward=1' /etc/sysctl.conf 2>/dev/null; then
            echo 'net.ipv4.ip_forward=1' | sudo tee -a /etc/sysctl.conf > /dev/null
        fi
        sudo sysctl -w net.ipv4.ip_forward=1 > /dev/null
    "
else
    echo "[DRY-RUN] Would enable net.ipv4.ip_forward=1 persistently"
fi

# ============================================
# STEP 5: Verify forwarding path
# ============================================

log_info "Step 5: Verifying forwarding path..."

if ! $DRY_RUN; then
    ssh -o ConnectTimeout=10 "$SSH_TARGET" "
        # Check IP forwarding
        FWD=\$(sysctl -n net.ipv4.ip_forward)
        if [ \"\$FWD\" != '1' ]; then
            echo 'WARNING: IP forwarding is not enabled'
        else
            echo 'IP forwarding: enabled'
        fi

        # Check for UFW
        if command -v ufw > /dev/null 2>&1; then
            STATUS=\$(sudo ufw status 2>/dev/null | head -1)
            echo \"UFW: \$STATUS\"
            if echo \"\$STATUS\" | grep -q 'active'; then
                if ! sudo ufw status | grep -q '$LISTEN_PORT'; then
                    echo 'WARNING: UFW is active but port $LISTEN_PORT may not be allowed'
                    echo '  Run: sudo ufw allow $LISTEN_PORT/udp'
                fi
            fi
        fi

        # Check iptables for DROP on the WG port
        if command -v iptables > /dev/null 2>&1; then
            if sudo iptables -L INPUT -n 2>/dev/null | grep -q 'DROP.*$LISTEN_PORT'; then
                echo 'WARNING: iptables has a DROP rule for port $LISTEN_PORT'
            fi
        fi
    "
else
    echo "[DRY-RUN] Would verify IP forwarding and check firewall rules"
fi

# ============================================
# STEP 6: Start WireGuard interface
# ============================================

log_info "Step 6: Starting WireGuard interface..."

if ! $DRY_RUN; then
    ssh -o ConnectTimeout=10 "$SSH_TARGET" "
        if ! sudo wg show wg-koi > /dev/null 2>&1; then
            sudo wg-quick up wg-koi
            echo 'Interface started'
        else
            echo 'Interface already running'
        fi
        sudo systemctl enable wg-quick@wg-koi 2>/dev/null || true
    "
else
    echo "[DRY-RUN] Would run: wg-quick up wg-koi && systemctl enable wg-quick@wg-koi"
fi

# ============================================
# STEP 7: Connectivity self-test
# ============================================

log_info "Step 7: Connectivity self-test..."

if ! $DRY_RUN; then
    ssh -o ConnectTimeout=10 "$SSH_TARGET" "
        echo '--- WireGuard Interface Status ---'
        sudo wg show wg-koi
        echo '---------------------------------'
    "
else
    echo "[DRY-RUN] Would run: wg show wg-koi"
fi

# ============================================
# STEP 8: Print relay public key
# ============================================

echo ""
echo "==================================="
echo "  KOI-net Relay Setup Complete"
echo "==================================="
echo ""
if [[ -n "$RELAY_PUBKEY" ]]; then
    echo "  Relay public key: $RELAY_PUBKEY"
else
    echo "  Relay public key: (run without --dry-run to see)"
fi
echo "  Relay endpoint:   ${SSH_TARGET#*@}:$LISTEN_PORT"
echo "  Relay WG IP:      10.100.0.1"
echo ""
echo "  Next step: Have peers run join-request.sh"
echo "==================================="
