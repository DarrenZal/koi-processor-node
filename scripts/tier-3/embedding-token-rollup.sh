#!/bin/bash
# embedding-token-rollup
#
# Reads ~/.koi/logs/embedding-tokens.jsonl (populated by the token-tracking
# instrumentation in api/embedding_provider.py) and emits a tabular roll-up grouped by prompt_type.
#
# Usage:
#   embedding-token-rollup.sh            # last 7 days, all providers
#   embedding-token-rollup.sh --days 30  # last 30 days
#   embedding-token-rollup.sh --since 2026-04-29T00:00:00  # explicit since-ts
#   embedding-token-rollup.sh --provider openai
#
# Optional cron entry:
#   0 9 * * *   <repo>/scripts/tier-3/embedding-token-rollup.sh --days 1 >> ~/.koi/logs/embedding-rollup.log

set -euo pipefail

LOG=${EMBEDDING_TOKEN_LOG:-$HOME/.koi/logs/embedding-tokens.jsonl}
DAYS=7
SINCE=""
PROVIDER=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --days) DAYS="$2"; shift 2;;
    --since) SINCE="$2"; shift 2;;
    --provider) PROVIDER="$2"; shift 2;;
    --log) LOG="$2"; shift 2;;
    -h|--help)
      echo "Usage: $0 [--days N] [--since ISO8601] [--provider NAME] [--log PATH]"
      exit 0
      ;;
    *) echo "Unknown arg: $1" >&2; exit 2;;
  esac
done

if [[ ! -f "$LOG" ]]; then
  echo "embedding token log not found: $LOG" >&2
  exit 1
fi

# Use python for the heavy aggregation (jsonl + grouping + formatting).
PROVIDER="$PROVIDER" SINCE="$SINCE" DAYS="$DAYS" python3 - "$LOG" <<'PY'
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

log_path = sys.argv[1]
provider_filter = os.environ.get("PROVIDER") or None
since_str = os.environ.get("SINCE") or None
days = int(os.environ.get("DAYS") or "7")

if since_str:
    since = datetime.fromisoformat(since_str.replace("Z", "+00:00"))
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
else:
    since = datetime.now(timezone.utc) - timedelta(days=days)

# Aggregations: per-prompt-type sum.
by_type = defaultdict(lambda: {
    "calls": 0, "ok": 0, "fail": 0,
    "tokens_total": 0, "tokens_known": 0,
    "duration_ms_total": 0.0,
    "providers": set(), "models": set(),
})
total_calls = 0
total_skipped_provider = 0
total_skipped_marker = 0
window_start = None
window_end = None

with open(log_path) as f:
    for ln in f:
        ln = ln.strip()
        if not ln:
            continue
        # Skip non-JSON marker lines (e.g. "===MARKER ...===").
        if not ln.startswith("{"):
            total_skipped_marker += 1
            continue
        try:
            r = json.loads(ln)
        except json.JSONDecodeError:
            continue
        ts_raw = r.get("ts")
        if not ts_raw:
            continue
        try:
            ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if ts < since:
            continue
        if provider_filter and r.get("provider") != provider_filter:
            total_skipped_provider += 1
            continue
        total_calls += 1
        if window_start is None or ts < window_start:
            window_start = ts
        if window_end is None or ts > window_end:
            window_end = ts
        pt = r.get("prompt_type") or "unknown"
        bucket = by_type[pt]
        bucket["calls"] += 1
        if r.get("ok"):
            bucket["ok"] += 1
        else:
            bucket["fail"] += 1
        tokens = r.get("prompt_tokens")
        if isinstance(tokens, int):
            bucket["tokens_total"] += tokens
            bucket["tokens_known"] += 1
        dur = r.get("duration_ms") or 0
        try:
            bucket["duration_ms_total"] += float(dur)
        except (TypeError, ValueError):
            pass
        prov = r.get("provider")
        if prov:
            bucket["providers"].add(prov)
        model = r.get("model")
        if model:
            bucket["models"].add(model)

# Print roll-up table.
print(f"# embedding-token-rollup")
print(f"# log:           {log_path}")
print(f"# since:         {since.isoformat()}")
if window_start and window_end:
    print(f"# window:        {window_start.isoformat()} → {window_end.isoformat()}")
if provider_filter:
    print(f"# provider:      {provider_filter}")
print(f"# total calls:   {total_calls}")
if total_skipped_provider:
    print(f"# skipped (provider mismatch): {total_skipped_provider}")
if total_skipped_marker:
    print(f"# skipped (marker lines):      {total_skipped_marker}")
print()

if total_calls == 0:
    print("(no records in window)")
    sys.exit(0)

header = f"{'prompt_type':<12} {'calls':>6} {'ok':>4} {'fail':>4} {'tokens':>10} {'avg_tok':>8} {'tot_dur_s':>10} {'avg_ms':>8} {'providers':>16}"
print(header)
print("-" * len(header))
total_tokens = 0
total_known = 0
total_dur = 0.0
for pt in sorted(by_type.keys()):
    b = by_type[pt]
    avg_tok = (b["tokens_total"] / b["tokens_known"]) if b["tokens_known"] else 0.0
    avg_ms = (b["duration_ms_total"] / b["calls"]) if b["calls"] else 0.0
    provs = ",".join(sorted(b["providers"]))
    total_tokens += b["tokens_total"]
    total_known += b["tokens_known"]
    total_dur += b["duration_ms_total"]
    print(
        f"{pt:<12} {b['calls']:>6} {b['ok']:>4} {b['fail']:>4} "
        f"{b['tokens_total']:>10} {avg_tok:>8.1f} "
        f"{b['duration_ms_total']/1000:>10.1f} {avg_ms:>8.1f} {provs:>16}"
    )
print("-" * len(header))
overall_avg_tok = (total_tokens / total_known) if total_known else 0.0
overall_avg_ms = (total_dur / total_calls) if total_calls else 0.0
print(
    f"{'TOTAL':<12} {total_calls:>6} {sum(b['ok'] for b in by_type.values()):>4} "
    f"{sum(b['fail'] for b in by_type.values()):>4} "
    f"{total_tokens:>10} {overall_avg_tok:>8.1f} "
    f"{total_dur/1000:>10.1f} {overall_avg_ms:>8.1f}"
)
PY
