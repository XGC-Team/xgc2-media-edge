#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

bash -n .xgc2/scripts/*.sh
export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/xgc2-media-edge-pycache}"
python3 -m py_compile \
  .xgc2/scripts/check_manifest_contract.py \
  .xgc2/scripts/xgc2_artifact_manifest.py
python3 .xgc2/scripts/check_manifest_contract.py

required_files=(
  .github/workflows/ci.yml
  .github/workflows/release.yml
  .xgc2/product.yml
  .xgc2/scripts/build.sh
  .xgc2/scripts/build_deb.sh
  .xgc2/scripts/fetch_mediamtx.sh
  .xgc2/scripts/check_manifest_contract.py
  .xgc2/scripts/check_package_compliance.sh
  .xgc2/scripts/smoke_test_installed.sh
  .xgc2/scripts/xgc2_artifact_manifest.py
  LICENSE
  README.md
  cmd/xgc-media-edge/main.go
  go.mod
  go.sum
  internal/mediaedge/config.go
  internal/mediaedge/http.go
  internal/mediaedge/mediamtx_server.go
  internal/mediaedge/mediamtx_recording.go
  internal/mediaedge/rtp_continuity.go
  internal/mediaedge/rtp_continuity_test.go
  internal/mediaedge/server.go
  internal/mediaedge/server_test.go
  internal/mediaedge/source_control.go
  internal/mediamtx/client.go
  internal/mediamtx/config.go
  internal/mediamtx/process.go
)
for file in "${required_files[@]}"; do
  if [[ ! -f "${file}" ]]; then
    echo "Missing required file: ${file}" >&2
    exit 1
  fi
done

unformatted="$(gofmt -l cmd internal)"
if [[ -n "${unformatted}" ]]; then
  echo "Go sources are not formatted:" >&2
  echo "${unformatted}" >&2
  exit 1
fi
go mod verify

if rg -n \
  'xgc2/core-xgc|(^|/)(ros|gazebo|catkin)(/|$)|github.com/gin-gonic' \
  --glob '*.go' --glob 'go.mod' .; then
  echo "media edge contains a forbidden XGC2 Core, ROS, or Gazebo dependency" >&2
  exit 1
fi

grep -q '^id: xgc2-media-edge$' .xgc2/product.yml
grep -q '^  distribution: focal,jammy,noble$' .xgc2/product.yml
grep -q '^  - /usr/bin/xgc-media-edge$' .xgc2/product.yml
grep -q '^  - /usr/lib/xgc2-media-edge/mediamtx$' .xgc2/product.yml
grep -q '^  - ffmpeg$' .xgc2/product.yml
grep -q '^Depends: ca-certificates, ffmpeg$' .xgc2/scripts/build_deb.sh
grep -q '^version="v1.20.0"$' .xgc2/scripts/fetch_mediamtx.sh
grep -q '^BSD 3-Clause License$' LICENSE

echo "xgc2-media-edge package compliance checks passed."
