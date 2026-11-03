#!/bin/bash
# Partial indexes backing the /health null-embed drift gauges.
#
# Each predicate normally matches ZERO rows, so the resulting index is ~8KB and
# turns a full seq scan into an index-only scan. Building one still costs a single
# table pass, so run this on a QUIET machine: under load the session_chunks scan
# alone can take minutes, and a CONCURRENTLY build killed midway leaves an invalid
# index behind (drop those with DROP INDEX CONCURRENTLY).
#
# This script builds the two indexes on the large chunk tables (koi_memory_chunks,
# session_chunks). The gauges also count NULL embeddings in knowledge_facts (two
# predicates) and entity_registry; neither a migration nor this script creates
# partial indexes for those, so add them the same way if those scans are slow.
# PRE-STEP: if an earlier CONCURRENTLY build was interrupted, it left an INVALID index
# behind. It is inert -- indisvalid=f AND indisready=f, so it is neither read nor
# maintained on writes -- but drop it before rebuilding, or the CREATE below will
# skip it via IF NOT EXISTS and you will keep the dead one:
#     DROP INDEX CONCURRENTLY IF EXISTS idx_kmc_null_embed3072;
# That DROP blocks behind writers on koi_memory_chunks, so run it when quiet.
#
# Usage: scripts/ops/build_null_embed_indexes.sh [--apply] [--dry-run] [DB]
# Without --apply it prints the target and the statements and runs nothing.
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: scripts/ops/build_null_embed_indexes.sh [--apply] [--dry-run] [DB]

Builds the partial indexes behind the /health null-embed gauges on koi_memory_chunks
and session_chunks with CREATE INDEX CONCURRENTLY, then shows whether both are valid.
DB defaults to personal_koi; psql takes the host, port and user from PGHOST, PGPORT
and PGUSER (or its own defaults).

  --apply     run the statements (run on a quiet machine; see the header of this file)
  --dry-run   print the target and the statements only (the default; wins over --apply)
  -h, --help  show this help
USAGE
}

APPLY=0
DRY_RUN=0
DB=""
while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --apply) APPLY=1 ;;
    --dry-run) DRY_RUN=1 ;;
    --) shift; break ;;
    -*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    *)
      if [ -n "$DB" ]; then echo "more than one database given" >&2; exit 2; fi
      DB="$1" ;;
  esac
  shift
done
if [ $# -gt 0 ]; then
  if [ -n "$DB" ] || [ $# -gt 1 ]; then echo "more than one database given" >&2; exit 2; fi
  DB="$1"
fi
DB="${DB:-personal_koi}"

SQL_KMC="CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_kmc_null_embed3072 ON koi_memory_chunks (id) WHERE embedding_3072 IS NULL;"
SQL_SC="CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sc_null_embed3072  ON session_chunks  (id) WHERE embedding_3072 IS NULL;"
SQL_CHECK="select c.relname, i.indisvalid from pg_class c join pg_index i on i.indexrelid=c.oid where c.relname like 'idx_%null_embed3072%' order by 1;"

echo "Target: database $DB (PGHOST=${PGHOST:-<psql default>} PGPORT=${PGPORT:-<psql default>} PGUSER=${PGUSER:-<psql default>})"
if [ "$APPLY" != 1 ] || [ "$DRY_RUN" = 1 ]; then
  echo "DRY RUN: nothing was run. With --apply this would run, in order:"
  echo "  $SQL_KMC"
  echo "  $SQL_SC"
  echo "  $SQL_CHECK"
  exit 0
fi

run() { echo "--- $1"; time psql -d "$DB" -X -q -c "$2"; }
run "koi_memory_chunks" "$SQL_KMC"
run "session_chunks"    "$SQL_SC"
echo "--- validity check (indisvalid must be t for both)"
psql -d "$DB" -X -c "$SQL_CHECK"
