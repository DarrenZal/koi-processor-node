#!/usr/bin/env bash
# Shared target check and fail-closed cleanup for shell suites that write through the API.
#
# A suite runs only when KOI_TEST_API_URL and KOI_TEST_DATABASE_URL are set and pass the
# test-target guard (tests/koi_test_targets.py: no database named personal_koi or without
# "test", no API on port 8351, unless KOI_TEST_ALLOW_LIVE=1). The API must write to that
# database. Everything the suite records is deleted from it on exit.

LIVE_WRITE_GUARD_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
LIVE_WRITE_PYTHON=${KOI_PYTHON:-python3}

live_write_begin() {
    if [ -z "${KOI_TEST_API_URL:-}" ] || [ -z "${KOI_TEST_DATABASE_URL:-}" ]; then
        echo "Refusing test writes: set KOI_TEST_API_URL and KOI_TEST_DATABASE_URL" >&2
        return 2
    fi
    "$LIVE_WRITE_PYTHON" "$LIVE_WRITE_GUARD_DIR/live_write_cleanup.py" check-env || return $?
    API=${KOI_TEST_API_URL%/}
    KOI_TEST_RUN_ID=${KOI_TEST_RUN_ID:-$(uuidgen | tr '[:upper:]' '[:lower:]')}
    if ! [[ "$KOI_TEST_RUN_ID" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
        echo "Refusing test writes: KOI_TEST_RUN_ID must be a UUID" >&2
        return 2
    fi
    KOI_CLEANUP_MANIFEST=$(mktemp)
    export API KOI_TEST_RUN_ID KOI_CLEANUP_MANIFEST
    trap live_write_finish EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
}

live_write_record() {
    local kind=$1 value=${2:-}
    [ -z "$value" ] && return 0
    "$LIVE_WRITE_PYTHON" "$LIVE_WRITE_GUARD_DIR/live_write_cleanup.py" record \
        --manifest "$KOI_CLEANUP_MANIFEST" --kind "$kind" --value "$value"
}

live_write_finish() {
    local status=$?
    trap - EXIT INT TERM
    if ! "$LIVE_WRITE_PYTHON" "$LIVE_WRITE_GUARD_DIR/live_write_cleanup.py" cleanup \
        --dsn "$KOI_TEST_DATABASE_URL" \
        --manifest "$KOI_CLEANUP_MANIFEST" \
        --run-id "$KOI_TEST_RUN_ID"; then
        status=1
    fi
    rm -f "$KOI_CLEANUP_MANIFEST"
    exit "$status"
}
