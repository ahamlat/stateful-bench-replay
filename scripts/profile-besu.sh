#!/usr/bin/env bash
# Profile the running Besu container with async-profiler for a fixed time.
#
# Usage:
#   scripts/profile-besu.sh                 # 60s, wall, 5ms, per-thread
#   scripts/profile-besu.sh 120
#   scripts/profile-besu.sh --duration 90 --event wall --interval 5ms
#
# The sweep does not need --profile. This script copies asprof into the
# container if the profiler mount is missing.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/profile-besu.sh [DURATION_S] [options]

  DURATION_S              sample time in seconds (default: 60)

  -d, --duration SEC      same as DURATION_S
  -c, --container NAME    Docker container (default: besu-bench)
  -e, --event EVENT       asprof event (default: wall)
  -i, --interval INT      sample interval (default: 5ms)
      --no-threads        do not pass -t (one combined flame graph)
      --lock [THRESH]     Java monitor / j.u.c contention (default thresh: 1ms).
                          Alone: -e lock HTML flame graph. With --jfr: add
                          --lock to a wall JFR (needed for mixed events).
  -o, --output FILE       host HTML/JFR path (default: ./besu-wall-<ts>.html)
      --jfr               write JFR instead of HTML
      --pid PID           JVM pid inside the container (default: detect)
      --asprof-host DIR   host async-profiler tree (default: ~/async-profiler)
  -h, --help              this help

Examples:
  scripts/profile-besu.sh 60
  scripts/profile-besu.sh 120 -o /tmp/besu-wall.html
  scripts/profile-besu.sh 60 --lock 1ms
  scripts/profile-besu.sh 60 --jfr --lock 1ms -o /tmp/besu-lock.jfr
EOF
}

DURATION=""
CONTAINER="${CONTAINER:-besu-bench}"
EVENT="${EVENT:-wall}"
INTERVAL="${INTERVAL:-5ms}"
THREADS=1
FORMAT="html"
LOCK=""
OUT=""
PID=""
ASPROF_HOST="${ASPROF_HOST:-$HOME/async-profiler}"
CONTAINER_ASPROF="/opt/async-profiler"
LOG_LEVEL="${LOG_LEVEL:-warn}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage; exit 0 ;;
        -d|--duration)
            DURATION="${2:?missing duration}"
            shift 2
            ;;
        -c|--container)
            CONTAINER="${2:?missing container}"
            shift 2
            ;;
        -e|--event)
            EVENT="${2:?missing event}"
            shift 2
            ;;
        -i|--interval)
            INTERVAL="${2:?missing interval}"
            shift 2
            ;;
        --no-threads) THREADS=0; shift ;;
        --lock)
            if [[ $# -ge 2 && "$2" != -* ]]; then
                LOCK="$2"
                shift 2
            else
                LOCK="1ms"
                shift
            fi
            ;;
        --jfr) FORMAT="jfr"; shift ;;
        -o|--output)
            OUT="${2:?missing output path}"
            shift 2
            ;;
        --pid)
            PID="${2:?missing pid}"
            shift 2
            ;;
        --asprof-host)
            ASPROF_HOST="${2:?missing directory}"
            shift 2
            ;;
        --duration=*|-d=*)
            DURATION="${1#*=}"
            shift
            ;;
        [0-9]*)
            if [[ -n "$DURATION" ]]; then
                echo "profile-besu.sh: extra duration argument: $1" >&2
                exit 2
            fi
            DURATION="$1"
            shift
            ;;
        *)
            echo "profile-besu.sh: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
done

DURATION="${DURATION:-60}"
if ! [[ "$DURATION" =~ ^[0-9]+$ ]] || [[ "$DURATION" -lt 1 ]]; then
    echo "profile-besu.sh: duration must be a positive integer, got $DURATION" >&2
    exit 2
fi

if [[ -n "$LOCK" && "$FORMAT" != "jfr" ]]; then
    EVENT="lock"
fi

dock() {
    if sudo -n docker version >/dev/null 2>&1; then
        sudo -n docker "$@"
    else
        sudo docker "$@"
    fi
}

if [[ ! -x "$ASPROF_HOST/bin/asprof" ]]; then
    echo "profile-besu.sh: missing $ASPROF_HOST/bin/asprof" >&2
    echo "  run: scripts/install-async-profiler.sh $ASPROF_HOST" >&2
    exit 1
fi

if ! dock ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    echo "profile-besu.sh: container not found: $CONTAINER" >&2
    dock ps -a
    exit 1
fi

state="$(dock inspect -f '{{.State.Running}}' "$CONTAINER")"
if [[ "$state" != "true" ]]; then
    echo "profile-besu.sh: container $CONTAINER is not running" >&2
    exit 1
fi

if ! dock exec "$CONTAINER" test -x "$CONTAINER_ASPROF/bin/asprof"; then
    echo "profile-besu.sh: copying $ASPROF_HOST -> $CONTAINER:$CONTAINER_ASPROF"
    dock cp "$ASPROF_HOST" "$CONTAINER:$CONTAINER_ASPROF"
fi

if [[ -z "$PID" ]]; then
    PID="$(dock exec "$CONTAINER" sh -c '
        if [ -r /proc/1/comm ] && grep -qi java /proc/1/comm; then
            echo 1
            exit 0
        fi
        for d in /proc/[0-9]*; do
            pid=${d#/proc/}
            if [ -r "$d/comm" ] && grep -qi java "$d/comm"; then
                echo "$pid"
                exit 0
            fi
        done
        exit 1
    ')" || {
        echo "profile-besu.sh: no Java process in $CONTAINER" >&2
        exit 1
    }
fi

stamp="$(date +%Y%m%d-%H%M%S)"
if [[ -z "$OUT" ]]; then
    OUT="besu-${EVENT}-${stamp}.${FORMAT}"
fi
OUT="$(cd "$(dirname "$OUT")" && pwd)/$(basename "$OUT")"
mkdir -p "$(dirname "$OUT")"

ctn_dir="/tmp/profile-output"
ctn_out="${ctn_dir}/$(basename "$OUT")"
dock exec "$CONTAINER" mkdir -p "$ctn_dir"

asprof_start=(
    "$CONTAINER_ASPROF/bin/asprof" start
    --log "$LOG_LEVEL"
    -e "$EVENT"
    -i "$INTERVAL"
)
if [[ -n "$LOCK" ]]; then
    if [[ "$FORMAT" == "jfr" ]]; then
        asprof_start+=(--lock "$LOCK")
    else
        INTERVAL="$LOCK"
    fi
fi
if [[ "$THREADS" -eq 1 ]]; then
    asprof_start+=(-t)
fi
if [[ "$FORMAT" == "jfr" ]]; then
    asprof_start+=(-f "$ctn_out")
fi
asprof_start+=("$PID")

STARTED=0
stop_profiler() {
    local rc=0
    if [[ "$STARTED" -ne 1 ]]; then
        return 0
    fi
    STARTED=0
    echo "profile-besu.sh: stopping asprof (pid $PID)"
    local stop=(
        "$CONTAINER_ASPROF/bin/asprof" stop
        --log "$LOG_LEVEL"
    )
    if [[ "$FORMAT" != "jfr" ]]; then
        stop+=(-f "$ctn_out")
    fi
    stop+=("$PID")
    if ! dock exec "$CONTAINER" "${stop[@]}"; then
        echo "profile-besu.sh: asprof stop failed" >&2
        rc=1
    fi
    if dock cp "$CONTAINER:$ctn_out" "$OUT"; then
        echo "profile-besu.sh: wrote $OUT"
    else
        echo "profile-besu.sh: failed to copy $ctn_out out of $CONTAINER" >&2
        rc=1
    fi
    return "$rc"
}

trap 'stop_profiler; exit 130' INT TERM

echo "profile-besu.sh: start event=$EVENT interval=$INTERVAL lock=${LOCK:-off} threads=$THREADS pid=$PID for ${DURATION}s"
dock exec "$CONTAINER" "${asprof_start[@]}"
STARTED=1

echo "profile-besu.sh: sampling ${DURATION}s (Ctrl-C stops and writes the file)"
sleep "$DURATION"

stop_profiler
trap - INT TERM
