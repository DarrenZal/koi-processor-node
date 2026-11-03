#!/bin/bash
# Wrapper for a scheduled research-author sensor job (launchd or cron).
# Sources personal.env for POSTGRES_URL and embedding credentials, then runs
# the author-paper sensor in apply mode.
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1
set -a
# shellcheck disable=SC1091
source config/personal.env
set +a
exec venv/bin/python scripts/research_author_sensor.py --apply --download-pdfs "$@"
