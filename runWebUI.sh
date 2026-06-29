#!/usr/bin/env bash
# runWebUI.sh - launch the benchmark web console (webui/server.py).
#
# - Resolves to its own directory (works from anywhere).
# - Reuses / bootstraps the same venv as runBenchmark.sh.
# - Forwards extra args to the server (e.g. --port, --host, --config).
#
# Usage:
#   ./runWebUI.sh                       # serve on 127.0.0.1:8765 with config.yaml
#   ./runWebUI.sh --port 9000           # different port
#   ./runWebUI.sh --host 0.0.0.0        # expose on the network (careful!)
#   CONFIG=staging.yaml ./runWebUI.sh   # different config
#
# Recommended: keep it bound to 127.0.0.1 on the VM and port-forward from your
# laptop, e.g.:   ssh -N -L 8765:127.0.0.1:8765 <vm>
# then open http://127.0.0.1:8765 locally.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV_DIR="${VENV_DIR:-$SCRIPT_DIR/venv}"
CONFIG="${CONFIG:-config.yaml}"

# 1. Ensure venv + deps (shared with runBenchmark.sh).
if [[ ! -x "$VENV_DIR/bin/python3" ]]; then
    echo "runWebUI.sh: bootstrapping venv at $VENV_DIR"
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

if $forward_config && [[ -f "$CONFIG" ]]; then
    set -- --config "$CONFIG" "$@"
fi

# 3. Serve.
exec "$VENV_DIR/bin/python3" "$SCRIPT_DIR/webui/server.py" "$@"
