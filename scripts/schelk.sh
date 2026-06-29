#!/usr/bin/env bash
# schelk.sh - drop-in alternative to overlay.sh that resets Besu's on-disk
# state with tempoxyz/schelk (https://github.com/tempoxyz/schelk) instead of
# OverlayFS.
#
# Where overlay.sh stacks a throw-away filesystem over a read-only snapshot
# DIRECTORY, schelk works one layer lower: it tracks the 4 KiB BLOCKS a
# benchmark writes (via the kernel's dm-era target) and, on reset, copies only
# those blocks back from a pristine "virgin" volume to the "scratch" volume the
# benchmark mounts. During the run there is NO overlay and NO copy-up: Besu
# writes straight to a plain ext4 on a real NVMe, so block-execution numbers
# are not distorted by the rollback machinery.
#
# This script exposes the SAME verbs as overlay.sh so run.py can drive either
# backend interchangeably:
#
#   schelk.sh init        --virgin DEV --scratch DEV --ramdisk DEV \
#                         --mount-point DIR --fstype FS [--granularity N] \
#                         [--ramdisk-size-kb N] [--no-copy]   (one-time, heavy)
#   schelk.sh mount-all   [flags]   -> schelk mount     (mount scratch + dm-era)
#   schelk.sh reset-all   [flags]   -> schelk restore   (roll back to virgin + mount)
#   schelk.sh reset-test  [flags]   -> schelk restore   (single baseline; same as reset-all)
#   schelk.sh umount-all  [flags]   -> schelk recover   (roll back to virgin, leave unmounted)
#   schelk.sh promote     [flags]   -> schelk promote   (make current scratch the new virgin baseline)
#   schelk.sh status      [flags]   -> schelk status
#   schelk.sh help|--help           -> this help (no root / devices needed)
#
# Verb mapping rationale
# ----------------------
# The sweep in run.py wipes state to the pristine baseline before EVERY test
# (it calls reset-all per test and replays the prelude fresh each time). schelk
# has a single baseline (the virgin volume), so both reset-all and reset-test
# map to `schelk restore` = recover-then-mount. `init` is the heavy one-time
# step (adopt the virgin volume and clone it to scratch); after that each
# reset-all is just an incremental block copy (seconds), not a directory wipe.
#
# Mount point
# -----------
# schelk records its mount point at init time and reuses it for mount/restore.
# Point --mount-point at <overlay_dir>/test/merged so Besu's existing bind
# mount (start_besu mounts <overlay_dir>/test/merged) keeps working unchanged,
# letting you A/B the two backends without touching the docker side.
#
# Parameters that are not relevant to a given verb are accepted and ignored, so
# run.py can pass the full flag set uniformly.
#
# Like overlay.sh this is meant to be the single sudo entrypoint, e.g.:
#   /etc/sudoers.d/besu-bench:
#     <user> ALL=(root) NOPASSWD: /home/<user>/stateful-bench-replay/scripts/schelk.sh
set -euo pipefail

die() { echo "schelk.sh: $*" >&2; exit 1; }

# ---- defaults / parsed flags --------------------------------------------------
SCHELK_BIN="schelk"
VIRGIN=""
SCRATCH=""
RAMDISK=""
MOUNT_POINT=""
FSTYPE="ext4"
GRANULARITY=""
RAMDISK_SIZE_KB=""
MOUNT_OPTIONS=""
NO_COPY=0

require() { [[ -n "$1" ]] || die "$2 is required for this action"; }

require_abs() {
    case "$1" in
        /*) ;;
        *) die "path must be absolute: $1" ;;
    esac
}

# Ensure the dm-era metadata ramdisk exists. brd stays loaded until reboot, so
# this is a no-op on every call after the first. Size only takes effect when brd
# is first inserted; if it is already loaded with a different size we leave it.
ensure_ramdisk() {
    [[ -n "$RAMDISK" ]] || return 0
    [[ -b "$RAMDISK" ]] && return 0
    if [[ -n "$RAMDISK_SIZE_KB" ]]; then
        echo "schelk.sh: $RAMDISK missing; loading brd (rd_size=${RAMDISK_SIZE_KB}KB)"
        modprobe brd "rd_size=$RAMDISK_SIZE_KB" "rd_nr=1" 2>/dev/null \
            || modprobe brd "rd_size=$RAMDISK_SIZE_KB" \
            || die "failed to modprobe brd for ramdisk $RAMDISK"
    else
        modprobe brd 2>/dev/null || true
    fi
    [[ -b "$RAMDISK" ]] || die "ramdisk $RAMDISK still not present after modprobe brd"
}

# NB: this function is deliberately NOT named `schelk`. The default SCHELK_BIN
# is the bare string "schelk", and a function of the same name would shadow the
# external binary, so `"$SCHELK_BIN" "$@"` would recurse into this function
# forever (stack overflow / segfault) instead of running the real program. We
# also invoke through the `command` builtin, which skips shell functions and
# resolves "schelk" via PATH, as a second line of defence.
run_schelk() {
    command -v "$SCHELK_BIN" >/dev/null 2>&1 || [[ -x "$SCHELK_BIN" ]] \
        || die "schelk binary not found: $SCHELK_BIN (set schelk.bin to an absolute path; sudo sanitises PATH so ~/.cargo/bin is not visible)"
    command "$SCHELK_BIN" "$@"
}

cmd_init() {
    require "$VIRGIN" "--virgin";   require_abs "$VIRGIN"
    require "$SCRATCH" "--scratch"; require_abs "$SCRATCH"
    require "$RAMDISK" "--ramdisk"; require_abs "$RAMDISK"
    require "$MOUNT_POINT" "--mount-point"; require_abs "$MOUNT_POINT"
    mkdir -p "$MOUNT_POINT"
    ensure_ramdisk
    local args=(init-from -y
        --virgin "$VIRGIN"
        --scratch "$SCRATCH"
        --ramdisk "$RAMDISK"
        --mount-point "$MOUNT_POINT"
        --fstype "$FSTYPE")
    [[ -n "$GRANULARITY" ]]    && args+=(--granularity "$GRANULARITY")
    [[ -n "$MOUNT_OPTIONS" ]]  && args+=(--mount-options "$MOUNT_OPTIONS")
    [[ "$NO_COPY" -eq 1 ]]     && args+=(--no-copy)
    echo "schelk.sh: ${SCHELK_BIN} ${args[*]}"
    run_schelk "${args[@]}"
}

cmd_mount_all() {
    ensure_ramdisk
    echo "schelk.sh: ${SCHELK_BIN} mount"
    run_schelk mount
}

# reset-all / reset-test: roll scratch back to the virgin baseline, then mount.
# `restore` = recover (no-op if not mounted) + mount, so it is safe whether or
# not the previous test left the volume mounted. -k kills anything still
# holding the mount (e.g. a lingering container mount) so a sweep never wedges.
cmd_reset() {
    ensure_ramdisk
    echo "schelk.sh: ${SCHELK_BIN} restore -k"
    run_schelk restore -k
}

cmd_umount_all() {
    echo "schelk.sh: ${SCHELK_BIN} recover -k"
    run_schelk recover -k
}

# promote: copy the blocks the benchmark wrote onto VIRGIN, so the current
# scratch state becomes the new pristine baseline. Used by --prepare-baseline
# to bake the gas-bump into the schelk baseline: every later `restore` then
# rolls back to the gas-bumped state instead of the pre-bump snapshot.
# This OVERWRITES virgin in place; the original pristine baseline is gone.
cmd_promote() {
    ensure_ramdisk
    echo "schelk.sh: ${SCHELK_BIN} promote"
    run_schelk promote
}

cmd_status() {
    run_schelk status
}

usage() { sed -n '2,49p' "$0"; }

main() {
    local action="${1:-}"; shift || true
    case "$action" in
        ""|-h|--help|help) usage; exit 0 ;;
    esac

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --bin)             SCHELK_BIN="$2"; shift 2 ;;
            --virgin)          VIRGIN="$2"; shift 2 ;;
            --scratch)         SCRATCH="$2"; shift 2 ;;
            --ramdisk)         RAMDISK="$2"; shift 2 ;;
            --mount-point)     MOUNT_POINT="$2"; shift 2 ;;
            --fstype)          FSTYPE="$2"; shift 2 ;;
            --granularity)     GRANULARITY="$2"; shift 2 ;;
            --ramdisk-size-kb) RAMDISK_SIZE_KB="$2"; shift 2 ;;
            --mount-options)   MOUNT_OPTIONS="$2"; shift 2 ;;
            --no-copy)         NO_COPY=1; shift ;;
            *) die "unknown flag: $1 (try --help)" ;;
        esac
    done

    case "$action" in
        init)                  cmd_init ;;
        mount-all|mount)       cmd_mount_all ;;
        reset-all|reset|reset-test) cmd_reset ;;
        umount-all|umount)     cmd_umount_all ;;
        promote)               cmd_promote ;;
        status)                cmd_status ;;
        *) die "unknown action: $action (try --help)" ;;
    esac
}

main "$@"
