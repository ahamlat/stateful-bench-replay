#!/usr/bin/env bash
# overlay.sh - manage the OverlayFS used to throw away Besu writes between sweeps.
#
# Layout under <overlay_root>:
#   lower   = <snapshot_dir>            (read-only Besu snapshot)
#   upper   = <overlay_root>/upper      (sweep writes land here)
#   work    = <overlay_root>/work       (overlay metadata)
#   merged  = <overlay_root>/merged     (the union, mounted to Besu container)
#
# Usage:
#   overlay.sh init    <snapshot_dir> <overlay_root>   # create dirs (idempotent)
#   overlay.sh mount   <snapshot_dir> <overlay_root>   # mount overlay if not mounted
#   overlay.sh umount  <overlay_root>                  # umount if mounted
#   overlay.sh wipe    <overlay_root>                  # rm -rf upper/* work/* (must be unmounted)
#   overlay.sh reset   <snapshot_dir> <overlay_root>   # umount + wipe + mount (the sweep entrypoint)
#   overlay.sh status  <overlay_root>                  # print mounted? + sizes
#
# Designed to be the single sudo entrypoint, e.g. via:
#   /etc/sudoers.d/besu-bench:
#     ubuntu ALL=(root) NOPASSWD: /opt/besu-bench/overlay.sh
set -euo pipefail

die() { echo "overlay.sh: $*" >&2; exit 1; }

require_abs() {
    case "$1" in
        /*) ;;
        *) die "path must be absolute: $1" ;;
    esac
}

is_mounted() {
    local merged="$1"
    mountpoint -q "$merged"
}

cmd_init() {
    local snapshot="$1" root="$2"
    require_abs "$snapshot"; require_abs "$root"
    [[ -d "$snapshot" ]] || die "snapshot dir not found: $snapshot"
    mkdir -p "$root/upper" "$root/work" "$root/merged"
    chmod 755 "$root" "$root/upper" "$root/work" "$root/merged"
}

cmd_mount() {
    local snapshot="$1" root="$2"
    require_abs "$snapshot"; require_abs "$root"
    [[ -d "$snapshot" ]] || die "snapshot dir not found: $snapshot"
    cmd_init "$snapshot" "$root"
    if is_mounted "$root/merged"; then
        echo "overlay.sh: already mounted at $root/merged"
        return 0
    fi
    mount -t overlay overlay \
        -o "lowerdir=$snapshot,upperdir=$root/upper,workdir=$root/work" \
        "$root/merged"
    echo "overlay.sh: mounted $root/merged (lower=$snapshot)"
}

cmd_umount() {
    local root="$1"
    require_abs "$root"
    if is_mounted "$root/merged"; then
        umount "$root/merged"
        echo "overlay.sh: unmounted $root/merged"
    else
        echo "overlay.sh: $root/merged not mounted"
    fi
}

cmd_wipe() {
    local root="$1"
    require_abs "$root"
    if is_mounted "$root/merged"; then
        die "refusing to wipe while mounted: $root/merged"
    fi
    # Belt-and-braces: only touch the two scratch subdirs.
    [[ -d "$root/upper" ]] && find "$root/upper" -mindepth 1 -delete
    [[ -d "$root/work"  ]] && find "$root/work"  -mindepth 1 -delete
    echo "overlay.sh: wiped $root/upper and $root/work"
}

cmd_reset() {
    local snapshot="$1" root="$2"
    require_abs "$snapshot"; require_abs "$root"
    cmd_umount "$root" || true
    cmd_wipe "$root"
    cmd_mount "$snapshot" "$root"
}

cmd_status() {
    local root="$1"
    require_abs "$root"
    if is_mounted "$root/merged"; then
        echo "mounted: yes ($root/merged)"
    else
        echo "mounted: no  ($root/merged)"
    fi
    if [[ -d "$root/upper" ]]; then
        echo -n "upper size: "; du -sh "$root/upper" 2>/dev/null | awk '{print $1}'
    fi
    if [[ -d "$root/work" ]]; then
        echo -n "work size:  "; du -sh "$root/work" 2>/dev/null | awk '{print $1}'
    fi
}

main() {
    local action="${1:-}"; shift || true
    case "$action" in
        init)    [[ $# -eq 2 ]] || die "usage: $0 init <snapshot_dir> <overlay_root>";   cmd_init   "$1" "$2" ;;
        mount)   [[ $# -eq 2 ]] || die "usage: $0 mount <snapshot_dir> <overlay_root>";  cmd_mount  "$1" "$2" ;;
        umount)  [[ $# -eq 1 ]] || die "usage: $0 umount <overlay_root>";                cmd_umount "$1" ;;
        wipe)    [[ $# -eq 1 ]] || die "usage: $0 wipe <overlay_root>";                  cmd_wipe   "$1" ;;
        reset)   [[ $# -eq 2 ]] || die "usage: $0 reset <snapshot_dir> <overlay_root>";  cmd_reset  "$1" "$2" ;;
        status)  [[ $# -eq 1 ]] || die "usage: $0 status <overlay_root>";                cmd_status "$1" ;;
        ""|-h|--help|help)
            sed -n '2,18p' "$0"
            ;;
        *) die "unknown action: $action (try --help)" ;;
    esac
}

main "$@"
