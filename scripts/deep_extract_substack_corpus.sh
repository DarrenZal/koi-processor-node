#!/bin/bash
# Daily deep-extraction of newly-ingested substack-corpus posts into the discourse graph.
#
# The substack sensor (substack_sensor.py, scheduled separately ~07:15) only INGESTS +
# embeds new posts — it does NOT deep-extract them. This job runs after it (~07:45), finds
# every substack-corpus:* post that has no completed deep-extraction yet, and runs the
# thorough-tier extractor (entities + facts + discourse moves) on each.
#
# Covers ALL substack-corpus feeds via the source_sensor
# filter. Idempotent + resumable: extract_deep_documents.py caches windows and takes a
# per-doc advisory lock, so overlap with a manual run or a re-fire is safe. Sequential —
# daily volume is 0-2 posts; no need for concurrency.
#
# Extraction transport defaults to the Claude Code SUBSCRIPTION via `claude -p`
# (DOC_EXTRACTOR_TRANSPORT=claude_p, $0 marginal cost) — set DOC_EXTRACTOR_TRANSPORT=api
# to use the pay-per-token ANTHROPIC_API_KEY instead (faster, but billed). Either way,
# posts that fail extract individually, are logged, and the next run retries them
# (resumable); the job exits cleanly (no crash-loop).
set -uo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SELF_DIR/.." && pwd)"
cd "$REPO_ROOT" || exit 9

# launchd runs this under macOS system /bin/bash (3.2) with a minimal PATH that
# lacks Homebrew bins — psql lives in /opt/homebrew/bin. Prepend it (and ~/.local/bin,
# where the `claude` CLI lives — needed for the subscription extraction transport) so
# both resolve the same way they do in an interactive shell.
export PATH="/opt/homebrew/bin:$HOME/.local/bin:$PATH"

VENV="${KOI_VENV:-/Users/example/venvs/koi-server}"
PY="$VENV/bin/python"
LOG_DIR="$REPO_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/substack-deep-extract.log"
SOURCE_SENSOR="substack-corpus-backfill"   # what substack_sensor.py stamps on every row

ts() { date '+%F %T'; }

set -a; source config/personal.env 2>/dev/null; set +a
export DOC_INGEST_KOI_URL="${DOC_INGEST_KOI_URL:-http://localhost:8351}"
PSQL_URL="${POSTGRES_URL:-postgresql://koi:@localhost:5432/personal_koi}"

if [[ ! -x "$PY" ]]; then echo "[$(ts)] ERROR: venv python not found at $PY" >> "$LOG"; exit 1; fi

# Un-extracted substack-corpus posts = ingested rows with no completed deep-extraction.
# NOTE: `mapfile` is a bash-4 builtin; macOS system /bin/bash (used by launchd) is
# 3.2 and lacks it — read into the array with a portable while-loop instead.
#
# The query's output goes to a temp file rather than a `< <(psql ...)` process
# substitution: bash cannot see a process substitution's exit status through the
# while-loop's `$?`, so a failed psql (bad DSN, network blip, syntax error after an
# edit) produced an empty RIDS array indistinguishable from the genuine "0 rows
# pending" case below — "nothing to extract" and "the query never ran" logged
# identically. Capturing psql's own exit code makes that distinction explicit.
RIDS=()
PSQL_OUT="$(mktemp)"
trap 'rm -f "$PSQL_OUT"' EXIT
psql "$PSQL_URL" -tAc "
  SELECT m.rid FROM koi_memories m
  WHERE m.source_sensor = '$SOURCE_SENSOR'
    AND m.rid LIKE 'substack-corpus:%'
    AND NOT EXISTS (
      SELECT 1 FROM document_ingestion_log l
      WHERE l.document_rid = m.rid AND l.deep_extracted_at IS NOT NULL)
  ORDER BY m.rid;" > "$PSQL_OUT" 2>&1
psql_status=$?
if [[ "$psql_status" -ne 0 ]]; then
  echo "[$(ts)] ERROR: psql query failed (exit $psql_status): $(cat "$PSQL_OUT")" >> "$LOG"
  exit 1
fi
while IFS= read -r _rid; do
  [[ -n "$_rid" ]] && RIDS+=("$_rid")
done < "$PSQL_OUT"

N=${#RIDS[@]}
if [[ "$N" -eq 0 ]]; then
  echo "[$(ts)] nothing to extract — all substack-corpus posts already in the discourse graph" >> "$LOG"
  exit 0
fi
echo "[$(ts)] deep-extracting $N new substack-corpus post(s)" >> "$LOG"

ok=0; fail=0
for rid in "${RIDS[@]}"; do
  out=$("$PY" scripts/extract_deep_documents.py \
          --document-rid "$rid" --tier thorough --source-sensor "$SOURCE_SENSOR" 2>&1)
  st=$(printf '%s\n' "$out" | grep -m1 -E "^  status:|ExtractionError|Traceback" | sed 's/^ *//')
  printf '[%s] %-62s | %s\n' "$(ts)" "$rid" "${st:-NO_STATUS}" >> "$LOG"
  if printf '%s' "$st" | grep -q "status: ok"; then ok=$((ok+1)); else fail=$((fail+1)); fi
done
echo "[$(ts)] DONE ok=$ok fail=$fail" >> "$LOG"
