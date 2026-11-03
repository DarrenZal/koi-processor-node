#!/usr/bin/env bash
# validate-node.sh — Post-bootstrap validation for KOI-net peer nodes
#
# Usage:
#   ./validate-node.sh [--koi-path <path>] [--expect-wg-ip <ip>] [--strict]
#
# Exit code:
#   0 on success
#   1 if any required check fails

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/lib.sh"

KOI_PATH="${KOI_PROCESSOR_DIR}"
EXPECT_WG_IP=""
STRICT=false

usage() {
    cat <<USAGE
Usage:
  $0 [--koi-path <path>] [--expect-wg-ip <ip>] [--strict]
USAGE
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --koi-path)
            KOI_PATH="${2:-}"
            shift 2
            ;;
        --expect-wg-ip)
            EXPECT_WG_IP="${2:-}"
            shift 2
            ;;
        --strict)
            STRICT=true
            shift
            ;;
        --help|-h)
            usage
            exit 0
            ;;
        *)
            log_fatal "Unknown option: $1"
            ;;
    esac
done

STATE_DIR="${STATE_DIR:-$HOME/.config/personal-koi}"
ENV_FILE="$KOI_PATH/config/personal.env"
KOI_PORT="8351"

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

pass() { echo "[PASS]  $*"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { echo "[FAIL]  $*" >&2; FAIL_COUNT=$((FAIL_COUNT + 1)); }
warn() { echo "[WARN]  $*" >&2; WARN_COUNT=$((WARN_COUNT + 1)); }

check_cmd() {
    local cmd="$1"
    if command -v "$cmd" >/dev/null 2>&1; then
        pass "command available: $cmd"
    else
        fail "missing command: $cmd"
    fi
}

check_file() {
    local f="$1"
    if [[ -f "$f" ]]; then
        pass "file exists: $f"
    else
        fail "missing file: $f"
    fi
}

check_cmd python3
check_cmd curl
check_cmd wg
check_cmd psql

check_file "$ENV_FILE"
check_file "$STATE_DIR/start.sh"
check_file "$STATE_DIR/koi-state/admin_token"

if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck source=/dev/null
    source "$ENV_FILE"
    set +a
    KOI_PORT="${KOI_API_PORT:-8351}"

    if [[ "${KOI_NET_ENABLED:-}" == "true" ]]; then
        pass "KOI_NET_ENABLED=true"
    else
        fail "KOI_NET_ENABLED must be true"
    fi

    if [[ "${KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL:-}" == "true" ]]; then
        pass "KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL=true"
    else
        fail "KOI_NET_REQUIRE_APPROVED_EDGE_FOR_POLL must be true"
    fi

    if [[ "${KOI_ENFORCE_SOURCE_KEY_RID_BINDING:-}" == "true" ]]; then
        pass "KOI_ENFORCE_SOURCE_KEY_RID_BINDING=true"
    else
        fail "KOI_ENFORCE_SOURCE_KEY_RID_BINDING must be true"
    fi

    if [[ "${KOI_BASE_URL:-}" == *"localhost"* || "${KOI_BASE_URL:-}" == *"127.0.0.1"* ]]; then
        fail "KOI_BASE_URL must not be localhost ($KOI_BASE_URL)"
    else
        pass "KOI_BASE_URL is non-localhost: ${KOI_BASE_URL:-<unset>}"
    fi

    if [[ -n "$EXPECT_WG_IP" ]]; then
        if [[ "${KOI_BASE_URL:-}" == "http://${EXPECT_WG_IP}:${KOI_PORT}" ]]; then
            pass "KOI_BASE_URL matches expected WG IP ($EXPECT_WG_IP)"
        else
            fail "KOI_BASE_URL mismatch (expected http://${EXPECT_WG_IP}:${KOI_PORT}, got ${KOI_BASE_URL:-<unset>})"
        fi
    fi
fi

if wg show wg-koi >/dev/null 2>&1; then
    pass "WireGuard interface wg-koi is up"
    if ping -c 1 -W 2 10.100.0.1 >/dev/null 2>&1; then
        pass "relay reachable: 10.100.0.1"
    else
        fail "cannot reach relay 10.100.0.1 (WireGuard route/tunnel issue)"
    fi
else
    warn "WireGuard interface wg-koi is not up"
fi

PG_URL="${POSTGRES_URL:-postgresql:///personal_koi}"
if psql "$PG_URL" -Atc "SELECT 1" >/dev/null 2>&1; then
    pass "PostgreSQL reachable: personal_koi"
else
    fail "PostgreSQL connection failed: $PG_URL"
fi

if curl -sf "http://127.0.0.1:${KOI_PORT}/health" >/dev/null 2>&1; then
    pass "local API health ok on 127.0.0.1:${KOI_PORT}"
else
    warn "local API is not healthy yet (start service then re-run)"
fi

if curl -sf "http://127.0.0.1:${KOI_PORT}/koi-net/health" >/dev/null 2>&1; then
    pass "koi-net health endpoint reachable"
else
    warn "koi-net health endpoint not reachable yet"
fi

echo ""
echo "==================================="
echo "  Validation Summary"
echo "==================================="
echo "  PASS: $PASS_COUNT"
echo "  WARN: $WARN_COUNT"
echo "  FAIL: $FAIL_COUNT"
echo "==================================="

if (( FAIL_COUNT > 0 )); then
    exit 1
fi

if $STRICT && (( WARN_COUNT > 0 )); then
    log_fatal "--strict enabled and warnings were present"
fi

exit 0
