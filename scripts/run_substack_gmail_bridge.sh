#!/bin/bash
# Wrapper for a scheduled Substack-from-Gmail bridge job (launchd or cron).
#
# Captures full-content PAID Substack posts (from the publications you subscribe to) that
# the API-based substack_sensor.py skips. Reads them from Gmail over IMAP and
# feeds them into the same substack-corpus pipeline. Schedule it after
# substack_sensor.py and before any deep-extraction job, so a new paid post is
# ingested AND graphed in the same cycle.
#
# Sources personal.env (vars are NOT exported there, so use `set -a`) for
# OPENAI_API_KEY + POSTGRES_URL. IMAP auth reads a Gmail app password from a
# file (see ingest_substack_from_gmail.py); no browser/OAuth, so it runs unattended.
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1   # -> koi-processor repo root
set -a
# shellcheck disable=SC1091
source config/personal.env
set +a
# Look back a few days each run so a missed run self-heals; ingest is idempotent
# (ingest_substack_corpus.py dedups by canonical slug).
exec venv/bin/python scripts/ingest_substack_from_gmail.py --apply --since-days "${1:-4}"
