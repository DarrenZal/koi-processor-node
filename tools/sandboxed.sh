#!/usr/bin/env bash
# Offline test sandbox.
#
# Runs the given command with live-infrastructure environment neutralized so a
# test can never reach the production database or API. Any test that genuinely
# needs a database must stand up a throwaway local instance and read KOI_TEST_*
# — never the real DSN.
#
# Usage:
#   tools/sandboxed.sh -- pytest tests/
#   tools/sandboxed.sh -- bun test
set -euo pipefail

# Drop a leading "--" separator if present.
if [[ "${1:-}" == "--" ]]; then
  shift
fi
if [[ $# -eq 0 ]]; then
  echo "usage: tools/sandboxed.sh -- <command> [args...]" >&2
  exit 2
fi

# Point every DB/API setting at an unroutable local port so an accidental
# connection fails fast instead of reaching prod.
export POSTGRES_URL="postgresql://koi_test:koi_test@127.0.0.1:5999/koi_test_offline"
export DATABASE_URL="$POSTGRES_URL"
export DB_URL="$POSTGRES_URL"
export KOI_API_ENDPOINT="http://127.0.0.1:5999/__offline__"
export KOI_TEST_MODE=1

# Test DSN placeholders for suites that opt into a throwaway database.
export KOI_TEST_POSTGRES_URL="${KOI_TEST_POSTGRES_URL:-postgresql://koi_test:koi_test@127.0.0.1:5999/koi_test_offline}"

exec "$@"
