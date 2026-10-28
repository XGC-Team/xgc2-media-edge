#!/usr/bin/env bash

set -euo pipefail

dpkg -s xgc2-media-edge >/dev/null
test -f /usr/share/xgc2/process-definitions/xgc-media-edge.json
test -x /usr/bin/xgc-media-edge
test -x /usr/lib/xgc2-media-edge/mediamtx
test -x /usr/lib/xgc2-media-edge/prepare-runtime
test -f /usr/share/xgc2-media-edge/pipelines/x264-fdsrc.json
test -f /usr/share/xgc2-media-edge/pipelines/x264-ros-image.json
test -f /usr/share/xgc2-media-edge/pipelines/x264-v4l2src-raw.json

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
grep -q -- '-sources-config' <<<"${help}"
grep -q -- '-sources-stdin' <<<"${help}"
grep -q -- '-rpc-socket' <<<"${help}"
grep -q -- '-mediamtx-executable' <<<"${help}"
grep -q -- '-pipeline-templates' <<<"${help}"
grep -q -- '-pipeline-runtime-dir' <<<"${help}"
grep -q -- '0.0.0.0:18189' <<<"${help}"
if grep -Eq -- '-source-id|-rtp-listen-address|-source-control-socket|-width|-height|-fps|-frame-id|-transport|legacy-pion|recording-ffmpeg' <<<"${help}"; then
  echo "installed binary still exposes a retired source or media interface" >&2
  exit 1
fi
test "$(/usr/lib/xgc2-media-edge/mediamtx --version)" = "v1.20.0"

if ! command -v readelf >/dev/null; then
  echo "readelf is required to check installed Media Edge ELF binaries" >&2
  exit 1
fi
readelf -h /usr/bin/xgc-media-edge | tee /tmp/xgc-media-edge-file.txt
program_headers="$(readelf -l /usr/bin/xgc-media-edge)"
dynamic_tags="$(readelf -d /usr/bin/xgc-media-edge)"
if grep -Eq '^[[:space:]]*INTERP[[:space:]]' <<<"${program_headers}" ||
   grep -Fq '(NEEDED)' <<<"${dynamic_tags}"; then
  echo "installed Media Edge binary is not statically linked" >&2
  exit 1
fi
readelf -h /usr/lib/xgc2-media-edge/mediamtx | tee /tmp/xgc-media-edge-mediamtx-file.txt

echo "xgc2-media-edge installed smoke test passed (version ${version})."
