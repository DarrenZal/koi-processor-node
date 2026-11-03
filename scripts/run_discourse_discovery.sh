#!/bin/bash
# Scheduled entry point for ingestion-time discourse discovery:
# one worker pass that claims at most one queued job, plans it (read-only) and
# applies it (one short transaction). Run it from a scheduler (a launchd agent,
# a cron entry) or by hand.
#
# Jobs exist only for sources ingested with
#   scripts/ingest_document.py --tier thorough --discourse-discovery
# so with an empty queue a pass connects, finds nothing and exits 0.
#
# Same lessons as run_embedding_repair.sh: REPO_DIR resolves from this script's
# own location, each missing piece has its own exit code, and a launchd agent for
# it should not set KeepAlive. KOI_DISCOURSE_DISCOVERY is set here, for this process only. No
# provider secrets are loaded: the worker embeds with local Ollama and refuses
# any other model.
#
# Overrides: DISCOURSE_DISCOVERY_REPO_DIR, DISCOURSE_DISCOVERY_PYTHON,
# DISCOURSE_DISCOVERY_DSN, DISCOURSE_DISCOVERY_STATE.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${DISCOURSE_DISCOVERY_REPO_DIR:-$(dirname "$SCRIPT_DIR")}"
PY="${DISCOURSE_DISCOVERY_PYTHON:-$REPO_DIR/venv/bin/python}"
TARGET="$REPO_DIR/scripts/discourse_discovery_worker.py"
DSN="${DISCOURSE_DISCOVERY_DSN:-postgresql://koi:@localhost:5432/personal_koi?application_name=discourse-discovery-worker}"
STATE="${DISCOURSE_DISCOVERY_STATE:-$REPO_DIR/logs/discourse-discovery-state.json}"

[ -x "$PY" ]     || { echo "ERROR: missing interpreter $PY" >&2; exit 3; }
[ -f "$TARGET" ] || { echo "ERROR: missing $TARGET — it must be on the DEPLOYED branch, not only a feature branch" >&2; exit 4; }

BRANCH="$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
case "$BRANCH" in
    regen-prod|stable) ;;
    *) echo "WARN: $REPO_DIR is on branch '$BRANCH', not a deployed branch." >&2 ;;
esac

mkdir -p "$(dirname "$STATE")"
cd "$REPO_DIR"
export KOI_DISCOURSE_DISCOVERY=1
exec "$PY" -B "$TARGET" run --dsn "$DSN" --once --max-jobs 1 --state-file "$STATE" "$@"
