#!/bin/bash
# Batch / one-off deep extraction with an alternate transport.
#
# Runs a manual batch on a different model (e.g. an OpenAI-compatible endpoint) for a
# backfill, without changing your scheduled extraction or committing an endpoint or key.
#
# It sources config/personal.env, then — if present — an optional gitignored transport
# override at config/extract-batch.env (put your DOC_EXTRACTOR_* / DOC_EXTRACTOR_OPENAI_*
# there; see config/extract-batch.env.example), then execs the extraction command you
# pass. A .py first argument runs under $KOI_VENV/bin/python. Set KOI_VENV to your
# virtualenv directory (KOI_VENV=venv for the quickstart's): the default below is the
# maintainer's path and does not exist elsewhere.
#
#   KOI_VENV=venv ./scripts/run_batch_extract.sh scripts/extract_deep_documents.py \
#       --document-rid <rid> --tier thorough --source-sensor <s>
#   ./scripts/run_batch_extract.sh path/to/your-batch.sh
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1
# Homebrew (psql) + ~/.local/bin (claude, if a batch still uses claude_p) on PATH.
export PATH="/opt/homebrew/bin:$HOME/.local/bin:$PATH"
set -a
# shellcheck disable=SC1091
source config/personal.env
# shellcheck disable=SC1091
[ -f config/extract-batch.env ] && source config/extract-batch.env
set +a

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <script.(sh|py)> [args...]   (transport from config/extract-batch.env)" >&2
  exit 2
fi
echo "[run_batch_extract] transport=${DOC_EXTRACTOR_TRANSPORT:-claude_p} model=${DOC_EXTRACTOR_OPENAI_MODEL:-${DOC_EXTRACTOR_MODEL:-default}}" >&2

first="$1"; shift
case "$first" in
  *.py) exec "${KOI_VENV:-/Users/example/venvs/koi-server}/bin/python" "$first" "$@" ;;
  *)    exec /bin/bash "$first" "$@" ;;
esac
