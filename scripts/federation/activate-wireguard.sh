#!/usr/bin/env bash
# activate-wireguard.sh — Insert private key into WG config template and activate tunnel.
#
# Usage: ./activate-wireguard.sh <wg-config-template> [admin-wg-ip]
#
# Reads private key from ~/.config/personal-koi/wireguard/private.key,
# substitutes it into the template, installs to /etc/wireguard/wg-koi.conf,
# and activates the tunnel.
set -euo pipefail

CONFIG_TEMPLATE="${1:-}"
ADMIN_WG_IP="${2:-10.100.0.2}"
KEY_FILE="$HOME/.config/personal-koi/wireguard/private.key"

if [[ -z "$CONFIG_TEMPLATE" ]]; then
    echo "Usage: $0 <wg-config-template> [admin-wg-ip]"
    echo "  wg-config-template: path to wg-koi.conf with <PASTE_YOUR_PRIVATE_KEY_HERE>"
    echo "  admin-wg-ip:        WireGuard IP of admin node (default: 10.100.0.2)"
    exit 1
fi

if [[ ! -f "$CONFIG_TEMPLATE" ]]; then
    echo "ERROR: Config template not found: $CONFIG_TEMPLATE"
    exit 1
fi

if [[ ! -f "$KEY_FILE" ]]; then
    echo "ERROR: Private key not found at $KEY_FILE"
    echo "  Run join-request.sh first to generate keys."
    exit 1
fi

PRIVATE_KEY=$(cat "$KEY_FILE")
if [[ -z "$PRIVATE_KEY" ]]; then
    echo "ERROR: Private key file is empty: $KEY_FILE"
    exit 1
fi

# Detect OS
OS="$(uname -s)"

# Create filled config in temp file
FILLED_CONFIG=$(mktemp)
trap 'rm -f "$FILLED_CONFIG"' EXIT

sed "s|<PASTE_YOUR_PRIVATE_KEY_HERE>|${PRIVATE_KEY}|" "$CONFIG_TEMPLATE" > "$FILLED_CONFIG"

# Verify substitution worked
if grep -q "PASTE_YOUR_PRIVATE_KEY_HERE" "$FILLED_CONFIG"; then
    echo "ERROR: Private key placeholder not found in template. Check template format."
    exit 1
fi

echo "Private key inserted into WireGuard config."

if [[ "$OS" == "Darwin" ]]; then
    # macOS: WireGuard.app manages tunnels, can't use wg-quick directly
    DEST="$HOME/wg-koi.conf"
    cp "$FILLED_CONFIG" "$DEST"
    chmod 600 "$DEST"
    echo ""
    echo "macOS detected. Config written to: $DEST"
    echo "  1. Open WireGuard.app"
    echo "  2. Import tunnel from file: $DEST"
    echo "  3. Activate the 'wg-koi' tunnel"
    echo "  4. Delete the config file: rm $DEST"
    echo ""
    echo "After activation, verify:"
    echo "  ping -c 1 10.100.0.1   # relay"
    echo "  ping -c 1 $ADMIN_WG_IP   # admin"
    exit 0
fi

# Linux: install and activate via wg-quick
echo "Installing WireGuard config..."
sudo cp "$FILLED_CONFIG" /etc/wireguard/wg-koi.conf
sudo chmod 600 /etc/wireguard/wg-koi.conf

echo "Activating wg-koi tunnel..."
sudo wg-quick up wg-koi

echo ""
echo "WireGuard tunnel activated. Verifying connectivity..."

RELAY_OK=false
ADMIN_OK=false

if ping -c 1 -W 3 10.100.0.1 &>/dev/null; then
    echo "  Relay (10.100.0.1): reachable"
    RELAY_OK=true
else
    echo "  Relay (10.100.0.1): UNREACHABLE"
fi

if ping -c 1 -W 3 "$ADMIN_WG_IP" &>/dev/null; then
    echo "  Admin ($ADMIN_WG_IP): reachable"
    ADMIN_OK=true
else
    echo "  Admin ($ADMIN_WG_IP): UNREACHABLE"
fi

echo ""
if $RELAY_OK && $ADMIN_OK; then
    echo "WireGuard tunnel is UP and peers are reachable."
else
    echo "WARNING: Some peers unreachable. Check firewall and relay config."
fi
