#!/usr/bin/env bash
# compute-sas.sh — Admin computes SAS code for peer identity verification
#
# Usage: ./compute-sas.sh <peer-name-or-rid>
# Example: ./compute-sas.sh peer-b

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <peer-name-or-rid>"
    exit 1
fi

PEER_ID="$1"
KOI_PORT="${KOI_API_PORT:-8351}"

# Get local KOI public key
HEALTH=$(curl -sf "http://127.0.0.1:${KOI_PORT}/koi-net/health") || \
    log_fatal "Cannot reach local KOI-net API"
LOCAL_PUBKEY=$(echo "$HEALTH" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('node', d.get('profile', {}))['public_key'])")

# Get peer's KOI public key from DB
PEER_PUBKEY=$(psql -d personal_koi -Atc "
    SELECT public_key FROM koi_net_nodes
    WHERE node_rid IN (
        SELECT node_rid FROM koi_net_peer_aliases WHERE alias = lower('$PEER_ID')
        UNION
        SELECT node_rid FROM koi_net_nodes WHERE lower(node_name) = lower('$PEER_ID')
        UNION
        SELECT '$PEER_ID' WHERE '$PEER_ID' LIKE 'orn:koi-net.node:%'
    )
    LIMIT 1
") || log_fatal "Cannot find peer '$PEER_ID' in database"

if [[ -z "$PEER_PUBKEY" ]]; then
    log_fatal "Peer '$PEER_ID' has no public key in database (handshake not completed yet?)"
fi

# Compute SAS
SAS=$(compute_sas "$LOCAL_PUBKEY" "$PEER_PUBKEY")

echo ""
echo "==================================="
echo "  SAS Verification Code"
echo "==================================="
echo ""
echo "  Peer: $PEER_ID"
echo "  SAS:  $SAS"
echo ""
echo "  Compare this code with the peer over Signal."
echo "  If codes match → identity verified."
echo "  If codes differ → possible MITM, do NOT approve edges."
echo "==================================="
