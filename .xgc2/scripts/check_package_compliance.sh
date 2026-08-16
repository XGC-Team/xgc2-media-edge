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

if grep -ERn \
  'xgc2\.build-artifact\.v2|run_cpp_quality|run_source_tests|actions/(checkout|setup-node|setup-go|upload-artifact)@v[0-9]' \
  .github/workflows .xgc2/scripts/xgc2_artifact_manifest.py; then
  echo "release contract contains a legacy schema, optional quality gate, or floating action" >&2
  exit 1
fi
grep -q 'xgc2.build-artifact.v1' .xgc2/scripts/xgc2_artifact_manifest.py
if grep -ERn -- '--(prepare-action|dependency-set-digest|dependency-mode)' \
  .github/workflows .xgc2/scripts/xgc2_artifact_manifest.py; then
  echo "build manifest generation must not embed release dependency inputs" >&2
  exit 1
fi
grep -q '^      prepare_action:' .github/workflows/release.yml
grep -q '^      dependency_set_digest:' .github/workflows/release.yml
if [[ "$(grep -c 'verify-build' .github/workflows/ci.yml)" -ne 1 ||
      "$(grep -c 'verify-build' .github/workflows/release.yml)" -ne 1 ]]; then
  echo "CI and release must verify each build manifest before upload" >&2
  exit 1
fi

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
  .xgc2/scripts/verify_unprivileged_mediamtx.sh
  .xgc2/scripts/xgc2_artifact_manifest.py
  LICENSE
  README.md
  cmd/xgc-media-edge/main.go
  go.mod
  go.sum
  internal/mediaedge/config.go
  internal/mediaedge/contracts.go
  internal/mediaedge/http.go
  internal/mediaedge/mediamtx_server.go
  internal/mediaedge/mediamtx_recording.go
  internal/mediaedge/recording_contract.go
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

if find . -type f \( -name '*.go' -o -name go.mod \) -print0 |
  xargs -0 grep -En \
    'xgc2/core-xgc|(^|/)(ros|gazebo|catkin)(/|$)|github.com/gin-gonic|github.com/pion|legacy-pion'; then
  echo "media edge contains a forbidden XGC2 Core, ROS, or Gazebo dependency" >&2
  exit 1
fi

grep -q '^id: xgc2-media-edge$' .xgc2/product.yml
grep -q '^  distribution: focal,jammy,noble$' .xgc2/product.yml
grep -q '^  - /usr/bin/xgc-media-edge$' .xgc2/product.yml
grep -q '^  - /usr/lib/xgc2-media-edge/mediamtx$' .xgc2/product.yml
grep -q '^Depends: ca-certificates$' .xgc2/scripts/build_deb.sh
if find internal/mediaedge -maxdepth 1 -type f \
  \( -name 'server.go' -o -name 'recording.go' -o -name 'recording_muxer.go' \
     -o -name 'h264_access_unit.go' -o -name 'rtp_continuity.go' \) | grep -q .; then
  echo "legacy media fanout or muxer source is still present" >&2
  exit 1
fi
grep -q '^version="v1.20.0"$' .xgc2/scripts/fetch_mediamtx.sh
grep -q '^BSD 3-Clause License$' LICENSE

echo "xgc2-media-edge package compliance checks passed."
