#!/usr/bin/env bash

set -euo pipefail

dpkg -s xgc2-media-edge >/dev/null
test -x /usr/bin/xgc-media-edge
test -x /usr/lib/xgc2-media-edge/mediamtx

version="$(xgc-media-edge --version)"
case "${version}" in
  dev|"") 
    echo "installed binary has invalid version ${version}" >&2
    exit 1
    ;;
esac

help="$(xgc-media-edge --help 2>&1)"
grep -q -- '-control-address' <<<"${help}"
grep -q -- '127.0.0.1:18090' <<<"${help}"
grep -q -- '-rtp-listen-address' <<<"${help}"
grep -q -- '-source-control-socket' <<<"${help}"
grep -q -- '-mediamtx-executable' <<<"${help}"
test "$(/usr/lib/xgc2-media-edge/mediamtx --version)" = "v1.20.0"

file /usr/bin/xgc-media-edge | tee /tmp/xgc-media-edge-file.txt
grep -q 'statically linked' /tmp/xgc-media-edge-file.txt
file /usr/lib/xgc2-media-edge/mediamtx | tee /tmp/xgc-media-edge-mediamtx-file.txt

echo "xgc2-media-edge installed smoke test passed (version ${version})."
