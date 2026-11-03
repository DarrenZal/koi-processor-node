#!/bin/bash
# Wrapper for a scheduled Substack sensor job (launchd or cron).
# Sources personal.env (vars are NOT exported there, so use `set -a`) for
# OPENAI_API_KEY + POSTGRES_URL, then runs the substack sensor with the koi venv.
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1   # -> koi-processor repo root
set -a
# shellcheck disable=SC1091
source config/personal.env
set +a
exec venv/bin/python scripts/substack_sensor.py "$@"
