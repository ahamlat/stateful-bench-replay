#!/usr/bin/env bash
# Install async-profiler 4.4 (linux x64) into the given directory.
#
# Usage:
#   scripts/install-async-profiler.sh                 # installs into $HOME/async-profiler
#   scripts/install-async-profiler.sh /opt/asprof    # installs into /opt/asprof

set -euo pipefail

VERSION="4.4"
URL="https://github.com/async-profiler/async-profiler/releases/download/v${VERSION}/async-profiler-${VERSION}-linux-x64.tar.gz"
DEST="${1:-$HOME/async-profiler}"

mkdir -p "$DEST"

if [[ -x "$DEST/bin/asprof" ]]; then
    echo "async-profiler already installed at $DEST"
    "$DEST/bin/asprof" --version
    exit 0
fi

tmp="$(mktemp -t async-profiler.XXXXXX.tar.gz)"
trap 'rm -f "$tmp"' EXIT

echo "downloading async-profiler $VERSION from $URL"
curl -fsSL "$URL" -o "$tmp"

echo "extracting into $DEST"
tar -xzf "$tmp" --strip-components=1 -C "$DEST"

echo "installed:"
"$DEST/bin/asprof" --version
echo
echo "next: point profile.host_dir at $DEST in your config.yaml"
