#!/usr/bin/env bash
# Prepare the KOI-net interop gate: a test-only venv with koi-net, and the stock node
# sources from koi-net-node-template at a pinned commit.
#
# This is the only step of the gate that uses the network. Run it once; run_gate.py then
# runs offline on loopback.
#
# Usage:
#   tests/interop/setup_stock_nodes.sh                  # build .venv-koinet and fetch the template
#   tests/interop/setup_stock_nodes.sh --verify-only DIR  # offline: check DIR holds the pinned files
#
# Environment:
#   KOI_GATE_PYTHON   interpreter for the venv (default python3.13; koi-net needs >= 3.12)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEMPLATE_COMMIT="c45b188f1acdb357c907d2478b2804a0cf3b07d7"
TEMPLATE_URL="https://codeload.github.com/DynamicalSystemsGroup/koi-net-node-template/tar.gz/${TEMPLATE_COMMIT}"
PKG="koi_net_CHANGE_NAME_node"

# sha256 of each template source file at TEMPLATE_COMMIT. GitHub's archive bytes are not
# guaranteed stable, so the files are pinned, not the tarball.
PINNED_FILES=(
  "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855  src/${PKG}/__init__.py"
  "8b444f7b1943565b4f84a15bfbfd211a9b50125bb4e70089070c9d57873fa55a  src/${PKG}/__main__.py"
  "359f49c3f04068ac76cc3ed7ee31bbc5efc3fc434194fcaaa486bf8c5deb33fe  src/${PKG}/config.py"
  "a6176b0d818fc9ff7ece722c628d58992176971fdb8614a3365f1c18ed8858de  src/${PKG}/core.py"
  "be65601b6bba553d17ef302a9251e7ff1a42e14a245605620ac55f35573c5f77  src/${PKG}/custom_handler.py"
)

verify_template() {
  local dir="$1"
  if [[ ! -d "$dir/src/$PKG" ]]; then
    echo "template: $dir/src/$PKG is missing" >&2
    return 1
  fi
  local line expected rel actual
  for line in "${PINNED_FILES[@]}"; do
    expected="${line%%  *}"
    rel="${line#*  }"
    if [[ ! -f "$dir/$rel" ]]; then
      echo "template: $rel is missing" >&2
      return 1
    fi
    actual="$(shasum -a 256 "$dir/$rel" | awk '{print $1}')"
    if [[ "$actual" != "$expected" ]]; then
      echo "template: $rel has sha256 $actual, expected $expected" >&2
      return 1
    fi
  done
  # Exactly the pinned files, so nothing unreviewed is importable from the package.
  local count
  count="$(find "$dir/src/$PKG" -type f -name '*.py' | wc -l | tr -d ' ')"
  if [[ "$count" != "${#PINNED_FILES[@]}" ]]; then
    echo "template: $dir/src/$PKG holds $count .py files, expected ${#PINNED_FILES[@]}" >&2
    return 1
  fi
  return 0
}

if [[ "${1:-}" == "--verify-only" ]]; then
  [[ $# -eq 2 ]] || { echo "usage: $0 --verify-only DIR" >&2; exit 2; }
  verify_template "$2"
  echo "template: $2 matches koi-net-node-template@${TEMPLATE_COMMIT:0:7}"
  exit 0
fi
[[ $# -eq 0 ]] || { echo "usage: $0 [--verify-only DIR]" >&2; exit 2; }

PY="${KOI_GATE_PYTHON:-python3.13}"
VENV="$REPO_ROOT/.venv-koinet"
CACHE="$REPO_ROOT/.interop"
TEMPLATE_DIR="$CACHE/template-${TEMPLATE_COMMIT:0:7}"

"$PY" -c 'import sys; sys.exit(0 if sys.version_info >= (3, 12) else 1)' \
  || { echo "$PY is older than 3.12; koi-net 2.1.2 does not import there" >&2; exit 2; }

if [[ ! -x "$VENV/bin/python" ]]; then
  "$PY" -m venv "$VENV"
fi
"$VENV/bin/python" -m pip install -q --upgrade pip
"$VENV/bin/python" -m pip install -q -r "$REPO_ROOT/tests/interop/requirements.txt"
"$VENV/bin/python" -I -c 'import koi_net.protocol, rid_lib, psycopg, pytest; from importlib.metadata import version; print("venv:", "koi-net", version("koi-net"), "rid-lib", version("rid-lib"))'

if ! verify_template "$TEMPLATE_DIR" 2>/dev/null; then
  rm -rf "$TEMPLATE_DIR"
  mkdir -p "$CACHE"
  download="$(mktemp -d "$CACHE/download.XXXXXX")"
  curl -fsSL --max-time 60 -o "$download/template.tar.gz" "$TEMPLATE_URL"
  mkdir -p "$download/x"
  tar -xzf "$download/template.tar.gz" -C "$download/x"
  mkdir -p "$TEMPLATE_DIR/src"
  cp -R "$download/x/koi-net-node-template-${TEMPLATE_COMMIT}/src/$PKG" "$TEMPLATE_DIR/src/$PKG"
  rm -rf "$download"
  verify_template "$TEMPLATE_DIR"
fi
echo "template: $TEMPLATE_DIR (koi-net-node-template@${TEMPLATE_COMMIT:0:7})"
