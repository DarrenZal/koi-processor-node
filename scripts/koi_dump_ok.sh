#!/usr/bin/env bash
# koi-dump-ok — is there a VALID pg_dump custom-format archive, recent enough to rely on?
#
# Written because `pg_restore --list` CANNOT distinguish a truncated dump from a complete
# one. Measured against a real 4.25GB partial (a dump killed mid-COPY by a restart) and
# the 12.46GB complete dump taken the day before:
#
#     pg_restore --list  partial -> exit 0, 1183 TOC entries   <- indistinguishable
#     pg_restore --list  good    -> exit 0, 1183 TOC entries
#     pg_restore -f /dev/null partial -> exit 1 in ~22s  "could not read from input file: end of file"
#     pg_restore -f /dev/null good    -> exit 0 in ~96s
#
# Only a FULL READ separates them, so that is what this does. It is slow on purpose.
#
# Usage:
#   koi-dump-ok <dir> [max_age_hours]     # newest dump in dir must be valid and fresh
#   koi-dump-ok --file <dump>             # verify one specific file, ignore age
#
# Exit: 0 valid (and fresh, if an age was given) · 1 invalid/truncated · 2 none found
#       · 3 too old · 4 usage
set -uo pipefail

die()  { echo "koi-dump-ok: $*" >&2; exit 4; }
note() { echo "koi-dump-ok: $*" >&2; }

command -v pg_restore >/dev/null || die "pg_restore not on PATH"

TARGET=""; MAX_AGE_H=""
case "${1:-}" in
  --file) TARGET="${2:-}"; [ -n "$TARGET" ] || die "--file needs a path" ;;
  "")     die "usage: koi-dump-ok <dir> [max_age_hours] | koi-dump-ok --file <dump>" ;;
  *)      DIR="$1"; MAX_AGE_H="${2:-}"
          [ -d "$DIR" ] || die "not a directory: $DIR"
          # Newest *.dump by mtime. NOTE the glob: a partial renamed out of it (e.g.
          # ….dump.PARTIAL-restart-kill) is deliberately invisible here, which is how the
          # backup script's own size guard is kept honest too.
          TARGET=$(ls -t "$DIR"/*.dump 2>/dev/null | head -1)
          [ -n "$TARGET" ] || { note "no *.dump found in $DIR"; exit 2; }
          ;;
esac
[ -f "$TARGET" ] || { note "not a file: $TARGET"; exit 2; }

BYTES=$(wc -c < "$TARGET" | tr -d '[:space:]')
note "checking $TARGET (${BYTES} bytes)"

# Age first — cheap, and a stale-but-valid dump is a different answer from a broken one.
if [ -n "$MAX_AGE_H" ]; then
  MTIME=$(/usr/bin/stat -f %m "$TARGET")   # /usr/bin/stat explicitly (BSD stat): a bare
                                           # `stat` can resolve to a different binary on PATH
  AGE_H=$(( ( $(date +%s) - MTIME ) / 3600 ))
  if [ "$AGE_H" -gt "$MAX_AGE_H" ]; then
    note "FAIL: ${AGE_H}h old, limit ${MAX_AGE_H}h"
    exit 3
  fi
  note "age ${AGE_H}h (limit ${MAX_AGE_H}h) OK"
fi

# The full read. Output to /dev/null: we are proving the archive is READABLE end to end,
# not restoring it. Nothing is written to any database.
note "full read in progress (minutes, not seconds — this is the point)…"
START=$(date +%s)
if pg_restore -f /dev/null "$TARGET" 2>/tmp/koi-dump-ok.err; then
  note "PASS: full read completed in $(( $(date +%s) - START ))s"
  exit 0
else
  note "FAIL: full read aborted after $(( $(date +%s) - START ))s"
  sed 's/^/  /' /tmp/koi-dump-ok.err >&2
  exit 1
fi
