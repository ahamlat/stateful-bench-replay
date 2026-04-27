#!/usr/bin/env bash
# overlay.sh - manage the OverlayFS layers used to throw away Besu writes.
#
# Two-layer layout under <overlay_root>:
#   lower (snapshot, read-only)    = <snapshot_dir>
#   prelude/{upper,work,merged}    = writes from funding + gas-bump (kept across tests)
#   test/{upper,work,merged}       = writes from the current test (resettable)
#
# Besu mounts <overlay_root>/test/merged. Underneath, the test layer's lower
# is the prelude layer's merged view, and the prelude layer's lower is the
# untouched snapshot. So:
#   - reset-all  -> wipe both layers (full reset; needed before re-running prelude)
#   - reset-test -> wipe only the test layer (keeps prelude state, used per-test)
#
# Usage:
#   overlay.sh init       <snapshot_dir> <overlay_root>
#   overlay.sh mount-all  <snapshot_dir> <overlay_root>
#   overlay.sh umount-all <overlay_root>
#   overlay.sh reset-all  <snapshot_dir> <overlay_root>
#   overlay.sh reset-test <snapshot_dir> <overlay_root>
#   overlay.sh status     <overlay_root>
#
# Backwards-compat aliases (single-layer naming from earlier version):
#   overlay.sh mount   <snapshot_dir> <overlay_root>   = mount-all
#   overlay.sh umount  <overlay_root>                  = umount-all
#   overlay.sh reset   <snapshot_dir> <overlay_root>   = reset-all
#
# Designed to be the single sudo entrypoint, e.g. via:
#   /etc/sudoers.d/besu-bench:
#     <user> ALL=(root) NOPASSWD: /usr/local/sbin/besu-overlay.sh
set -euo pipefail

die() { echo "overlay.sh: $*" >&2; exit 1; }

require_abs() {
    case "$1" in
        /*) ;;
        *) die "path must be absolute: $1" ;;
    esac
}

is_mounted() { mountpoint -q "$1"; }

cmd_init() {
    local snapshot="$1" root="$2"
    require_abs "$snapshot"; require_abs "$root"
    [[ -d "$snapshot" ]] || die "snapshot dir not found: $snapshot"
    mkdir -p "$root/prelude/upper" "$root/prelude/work" "$root/prelude/merged" \
             "$root/test/upper"    "$root/test/work"    "$root/test/merged"
    chmod 755 "$root" \
              "$root/prelude" "$root/prelude/upper" "$root/prelude/work" "$root/prelude/merged" \
              "$root/test"    "$root/test/upper"    "$root/test/work"    "$root/test/merged"
}

mount_prelude() {
    local snapshot="$1" root="$2"
    if is_mounted "$root/prelude/merged"; then
        echo "overlay.sh: prelude already mounted at $root/prelude/merged"
        return 0
    fi
    mount -t overlay overlay \
        -o "lowerdir=$snapshot,upperdir=$root/prelude/upper,workdir=$root/prelude/work" \
        "$root/prelude/merged"
    echo "overlay.sh: mounted prelude at $root/prelude/merged (lower=$snapshot)"
}

mount_test() {
    local root="$1"
    is_mounted "$root/prelude/merged" || die "prelude must be mounted before test layer"
    if is_mounted "$root/test/merged"; then
        echo "overlay.sh: test already mounted at $root/test/merged"
        return 0
    fi
    mount -t overlay overlay \
        -o "lowerdir=$root/prelude/merged,upperdir=$root/test/upper,workdir=$root/test/work" \
        "$root/test/merged"
    echo "overlay.sh: mounted test at $root/test/merged (lower=$root/prelude/merged)"
}

umount_test() {
    local root="$1"
    if is_mounted "$root/test/merged"; then
        umount "$root/test/merged"
        echo "overlay.sh: unmounted $root/test/merged"
    fi
}

umount_prelude() {
    local root="$1"
    if is_mounted "$root/prelude/merged"; then
        umount "$root/prelude/merged"
        echo "overlay.sh: unmounted $root/prelude/merged"
    fi
}

wipe_test() {
    local root="$1"
    is_mounted "$root/test/merged" && die "refusing to wipe while mounted: $root/test/merged"
    [[ -d "$root/test/upper" ]] && find "$root/test/upper" -mindepth 1 -delete
    [[ -d "$root/test/work"  ]] && find "$root/test/work"  -mindepth 1 -delete
    echo "overlay.sh: wiped $root/test/upper and $root/test/work"
}

wipe_prelude() {
    local root="$1"
    is_mounted "$root/prelude/merged" && die "refusing to wipe while mounted: $root/prelude/merged"
    [[ -d "$root/prelude/upper" ]] && find "$root/prelude/upper" -mindepth 1 -delete
    [[ -d "$root/prelude/work"  ]] && find "$root/prelude/work"  -mindepth 1 -delete
    echo "overlay.sh: wiped $root/prelude/upper and $root/prelude/work"
}

cmd_mount_all() {
    local snapshot="$1" root="$2"
    require_abs "$snapshot"; require_abs "$root"
    cmd_init "$snapshot" "$root"
    mount_prelude "$snapshot" "$root"
    mount_test "$root"
}

cmd_umount_all() {
    local root="$1"
    require_abs "$root"
    umount_test "$root"
    umount_prelude "$root"
}

cmd_reset_all() {
    local snapshot="$1" root="$2"
    require_abs "$snapshot"; require_abs "$root"
    cmd_init "$snapshot" "$root"
    umount_test "$root"
    umount_prelude "$root"
    wipe_test "$root"
    wipe_prelude "$root"
    mount_prelude "$snapshot" "$root"
    mount_test "$root"
}

cmd_reset_test() {
    local snapshot="$1" root="$2"
    require_abs "$snapshot"; require_abs "$root"
    cmd_init "$snapshot" "$root"
    # Make sure the prelude layer is still mounted; do NOT touch its writes.
    if ! is_mounted "$root/prelude/merged"; then
        mount_prelude "$snapshot" "$root"
    fi
    umount_test "$root"
    wipe_test "$root"
    mount_test "$root"
}

cmd_status() {
    local root="$1"
    require_abs "$root"
    for layer in prelude test; do
        if is_mounted "$root/$layer/merged"; then
            echo "$layer: mounted ($root/$layer/merged)"
        else
            echo "$layer: not mounted"
        fi
        if [[ -d "$root/$layer/upper" ]]; then
            echo -n "  upper size: "; du -sh "$root/$layer/upper" 2>/dev/null | awk '{print $1}'
        fi
    done
}

main() {
    local action="${1:-}"; shift || true
    case "$action" in
        init)        [[ $# -eq 2 ]] || die "usage: $0 init <snapshot> <root>";        cmd_init       "$1" "$2" ;;
        mount-all|mount)
                     [[ $# -eq 2 ]] || die "usage: $0 mount-all <snapshot> <root>";   cmd_mount_all  "$1" "$2" ;;
        umount-all|umount)
                     [[ $# -eq 1 ]] || die "usage: $0 umount-all <root>";             cmd_umount_all "$1" ;;
        reset-all|reset)
                     [[ $# -eq 2 ]] || die "usage: $0 reset-all <snapshot> <root>";   cmd_reset_all  "$1" "$2" ;;
        reset-test)  [[ $# -eq 2 ]] || die "usage: $0 reset-test <snapshot> <root>";  cmd_reset_test "$1" "$2" ;;
        status)      [[ $# -eq 1 ]] || die "usage: $0 status <root>";                 cmd_status     "$1" ;;
        ""|-h|--help|help) sed -n '2,28p' "$0" ;;
        *) die "unknown action: $action (try --help)" ;;
    esac
}

main "$@"
