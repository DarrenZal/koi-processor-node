#!/usr/bin/env bash
# run-migration.sh
#
# Thin harness for any tier-3 SQL migration that follows the
# `migrate-session-entity-normalization.sql` convention:
#   - SQL file emits its own pre-state SELECTs at the top
#   - SQL file does the work in BEGIN; … COMMIT;
#   - SQL file emits post-state + final-verification SELECTs
#
# By default this is a dry run: it prints the target database (password masked), the
# file, and the transaction-control and write statements the file holds, and runs no
# SQL. (An outer BEGIN; … ROLLBACK; wrapper cannot make such a file safe: the file's
# own COMMIT ends the outer transaction and commits the work.) With --apply it runs
# `psql -f <file>` against the target and surfaces stdout.
#
# Usage:
#   bash run-migration.sh <path-to-migration.sql> [--dry-run]   # report only (default)
#   bash run-migration.sh <path-to-migration.sql> --apply       # run it
#   bash run-migration.sh ./migrate-session-entity-normalization.sql --apply
#
# Pre-flight:
#   - SQL file exists + readable
#   - psql available on PATH (--apply)
#   - DB url resolved from POSTGRES_URL env (or default `personal_koi`)
#
# Exit codes:
#   0  dry run printed, or psql ran without error
#   1  pre-flight failure or psql error
#   2  unknown argument

set -euo pipefail

usage() {
  echo "Usage: bash run-migration.sh <migration.sql> [--dry-run | --apply]"
  echo "  (default) dry run: print the target and the file's statements, run nothing"
  echo "  --apply   run the file with psql against POSTGRES_URL (default personal_koi)"
}

MIGRATION_FILE=""
APPLY=0
DRY_RUN=0
for arg in "$@"; do
  case "$arg" in
    -h|--help) usage; exit 0 ;;
    --apply) APPLY=1 ;;
    --dry-run) DRY_RUN=1 ;;
    -*) echo "ERROR: unknown argument: $arg" >&2; usage >&2; exit 2 ;;
    *)
      if [[ -n "$MIGRATION_FILE" ]]; then
        echo "ERROR: more than one migration file given" >&2; usage >&2; exit 2
      fi
      MIGRATION_FILE="$arg" ;;
  esac
done

if [[ -z "$MIGRATION_FILE" ]]; then
  echo "ERROR: migration SQL file required" >&2
  usage >&2
  exit 1
fi

if [[ ! -f "$MIGRATION_FILE" ]]; then
  echo "ERROR: migration file not found: $MIGRATION_FILE" >&2
  exit 1
fi

if [[ ! -r "$MIGRATION_FILE" ]]; then
  echo "ERROR: migration file not readable: $MIGRATION_FILE" >&2
  exit 1
fi

# Resolve DB. Prefer POSTGRES_URL; fall back to the personal_koi default.
DB_TARGET="${POSTGRES_URL:-personal_koi}"
DB_SHOWN=$(printf '%s' "$DB_TARGET" | sed -E 's#(://[^:/@]*):[^@]*@#\1:***@#')

if [[ "$APPLY" -ne 1 || "$DRY_RUN" -eq 1 ]]; then
  echo "── DRY-RUN: no SQL is run ──"
  echo "── File: $MIGRATION_FILE ──"
  echo "── Target: $DB_SHOWN ──"
  echo "Transaction control (line: statement):"
  grep -n -i -E '^[[:space:]]*(BEGIN|COMMIT|END|ROLLBACK|ABORT|START[[:space:]]+TRANSACTION|SAVEPOINT|RELEASE|PREPARE[[:space:]]+TRANSACTION)([[:space:];]|$)' \
    "$MIGRATION_FILE" | sed 's/^/  /' || echo "  (none)"
  echo "Statements that write (line: statement):"
  grep -n -i -E '^[[:space:]]*(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|TRUNCATE|MERGE|COPY|GRANT|REVOKE|VACUUM|REINDEX|REFRESH|CLUSTER|COMMENT)([[:space:]]|$)' \
    "$MIGRATION_FILE" | cut -c1-160 | sed 's/^/  /' || echo "  (none)"
  echo "psql meta-commands that read other files:"
  grep -n -E '^[[:space:]]*\\(i|ir|include|include_relative)[[:space:]]' "$MIGRATION_FILE" | sed 's/^/  /' || echo "  (none)"
  echo "── Pass --apply to run: psql <target> -f $MIGRATION_FILE ──"
  exit 0
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "ERROR: psql not found on PATH" >&2
  exit 1
fi

echo "── LIVE: $MIGRATION_FILE → $DB_SHOWN ──"
psql "$DB_TARGET" -f "$MIGRATION_FILE"
rc=$?

if [[ "$rc" -ne 0 ]]; then
  echo "ERROR: psql exited with code $rc" >&2
  exit 1
fi

echo "── DONE (rc=0) ──"
exit 0
