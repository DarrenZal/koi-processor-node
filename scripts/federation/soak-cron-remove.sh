#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Remove soak-check cron job
# ============================================================

if crontab -l 2>/dev/null | grep -q "koi-soak"; then
    crontab -l | grep -v "koi-soak" | crontab -
    echo "Soak cron removed"
else
    echo "No soak cron found"
fi
