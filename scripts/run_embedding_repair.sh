#!/bin/bash
# Wrapper for backfill_null_embeddings.py, for a scheduled job (a launchd agent
# with StartInterval, a cron entry) or a run by hand.
#
# Replaces an older chunk-only embedder job. That job repaired only ONE of four
# semantic-read surfaces, and its launchd agent carried
# KeepAlive{SuccessfulExit:false} with no ThrottleInterval, which turned a
# failing provider (an account with no credits) into thousands of back-to-back
# crashed runs, each making a failed API call, while the pending queue grew.
# Schedule this with an interval and no KeepAlive.
#
# REPO_DIR resolves from THIS script's own location, so a branch switch in some
# other checkout cannot orphan the job, and each missing piece gets its OWN exit
# code (a single generic exit code once made a multi-day outage opaque).
#
# Override the tree with EMBED_REPAIR_REPO_DIR.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${EMBED_REPAIR_REPO_DIR:-$(dirname "$SCRIPT_DIR")}"
SECRETS_FILE="$REPO_DIR/config/personal.env"
PY="$REPO_DIR/venv/bin/python"
TARGET="$REPO_DIR/scripts/backfill_null_embeddings.py"

[ -f "$SECRETS_FILE" ] || { echo "ERROR: missing $SECRETS_FILE (expected POSTGRES_URL + OPENAI_API_KEY)" >&2; exit 2; }
[ -x "$PY" ]           || { echo "ERROR: missing venv interpreter $PY" >&2; exit 3; }
[ -f "$TARGET" ]       || { echo "ERROR: missing $TARGET — this file and its wrapper must be on the DEPLOYED branch, not only a feature branch" >&2; exit 4; }

# WARN, not fail: with KeepAlive removed, a hard failure here is a silent
# 5-minute no-op, which is the failure mode this job exists to eliminate.
BRANCH="$(git -C "$REPO_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
case "$BRANCH" in
    regen-prod|stable) ;;
    *) echo "WARN: $REPO_DIR is on branch '$BRANCH', not a deployed branch. Repair code may be stale or absent." >&2 ;;
esac

# Pin the tiktoken BPE cache to a PERSISTENT path. Unset, it lands in launchd's
# per-boot $TMPDIR and cl100k_base.tiktoken is re-downloaded after every reboot;
# when that download fails the job crashes with nothing on stdout, so the
# crashes are invisible in the run log.
export TIKTOKEN_CACHE_DIR="${TIKTOKEN_CACHE_DIR:-$HOME/.cache/tiktoken}"
mkdir -p "$TIKTOKEN_CACHE_DIR"

set -a
# shellcheck disable=SC1090
source "$SECRETS_FILE"
set +a

cd "$REPO_DIR"
exec "$PY" "$TARGET" --limit 500 "$@"
