#!/usr/bin/env bash
# runBenchmark.sh - thin wrapper around run.py.
#
# - Resolves to its own directory (so it works no matter where you call it from).
# - Creates a venv on first use and installs requirements.txt.
# - Forwards every argument straight to run.py.
#
# Usage:
#   ./runBenchmark.sh                          # full sweep with config.yaml
#   ./runBenchmark.sh --dry-run                # resolve config + selected tests, then exit
#   ./runBenchmark.sh --filter '*BALANCE*30M*' # filter override
#   ./runBenchmark.sh -c other.yaml --dry-run  # custom config
#
# Override the config file globally via $CONFIG, e.g.:
#   CONFIG=staging.yaml ./runBenchmark.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR="${VENV_DIR:-$SCRIPT_DIR/venv}"
CONFIG="${CONFIG:-config.yaml}"

# 1. Ensure venv + deps.
if [[ ! -x "$VENV_DIR/bin/python3" ]]; then
    echo "runBenchmark.sh: bootstrapping venv at $VENV_DIR"
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/pip" install --upgrade pip >/dev/null
    "$VENV_DIR/bin/pip" install -r "$SCRIPT_DIR/requirements.txt"
fi

# 2. Honor CONFIG only if -c/--config wasn't passed by the caller.
forward_config=true
for arg in "$@"; do
    case "$arg" in
        -c|--config|-c=*|--config=*) forward_config=false; break ;;
    esac
done

if $forward_config; then
    if [[ ! -f "$CONFIG" ]]; then
        echo "runBenchmark.sh: config file not found: $CONFIG" >&2
        echo "  cp config.example.yaml config.yaml   # then edit it" >&2
        exit 2
    fi
    set -- --config "$CONFIG" "$@"
fi

# 3. Run.
exec "$VENV_DIR/bin/python3" "$SCRIPT_DIR/run.py" "$@"
