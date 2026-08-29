#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -ne 3 ]]; then
  echo "usage: $0 DEB_PATH CONTAINER_IMAGE CONTAINER_PLATFORM" >&2
  exit 2
fi

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
deb_path="$1"
container_image="$2"
container_platform="$3"

if [[ ! -f "${deb_path}" ]]; then
  echo "Debian artifact does not exist: ${deb_path}" >&2
  exit 1
fi
case "${container_platform}" in
  linux/amd64) goarch="amd64" ;;
  linux/arm64) goarch="arm64" ;;
  *)
    echo "container platform must be linux/amd64 or linux/arm64" >&2
    exit 1
    ;;
esac
if [[ -z "${container_image}" || "${container_image}" =~ [[:space:]] ]]; then
  echo "container image must be one non-empty reference" >&2
  exit 1
fi

mkdir -p "${repo_root}/.ci"
work_dir="$(mktemp -d "${repo_root}/.ci/unprivileged-mediamtx.XXXXXX")"
trap 'rm -rf -- "${work_dir}"' EXIT
package_root="${work_dir}/package-root"
runtime_root="${work_dir}/runtime"
install -d -m 0755 "${package_root}" "${runtime_root}"

dpkg-deb --extract "${deb_path}" "${package_root}"
test -x "${package_root}/usr/lib/xgc2-media-edge/mediamtx"
install -m 0755 \
  "${package_root}/usr/lib/xgc2-media-edge/mediamtx" \
  "${runtime_root}/mediamtx"

(
  cd "${repo_root}"
  CGO_ENABLED=0 GOOS=linux GOARCH="${goarch}" \
    go test -c -o "${runtime_root}/mediamtx.test" ./internal/mediamtx
)

docker run --rm \
  --platform "${container_platform}" \
  --network none \
  --user 65532:65532 \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --read-only \
  --pids-limit 64 \
  --tmpfs /tmp:rw,nosuid,nodev,noexec,size=64m,mode=1777 \
  --env TMPDIR=/tmp \
  --env XGC2_MEDIAMTX_TEST_EXECUTABLE=/runtime/mediamtx \
  --volume "${runtime_root}:/runtime:ro" \
  "${container_image}" \
  /runtime/mediamtx.test \
    -test.v \
    -test.run '^TestRealMediaMTXStartsInUnprivilegedProbe$'

echo "MediaMTX readiness passed as uid 65532 with no capabilities and a read-only root filesystem."
