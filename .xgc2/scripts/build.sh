#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
output="${XGC2_MEDIA_EDGE_OUTPUT:-${repo_root}/.ci/bin/xgc-media-edge}"
goos="${GOOS:-linux}"
goarch="${GOARCH:-$(go env GOARCH)}"

if [[ "${output}" != /* ]]; then
  output="${repo_root}/${output}"
fi

case "${goarch}" in
  amd64|arm64) ;;
  *)
    echo "unsupported GOARCH ${goarch}; expected amd64 or arm64" >&2
    exit 1
    ;;
esac
if [[ "${goos}" != "linux" ]]; then
  echo "unsupported GOOS ${goos}; expected linux" >&2
  exit 1
fi

product_version="$(
  sed -n 's/^version:[[:space:]]*//p' \
    "${repo_root}/.xgc2/product.yml" | head -n 1
)"
if [[ -z "${product_version}" ]]; then
  echo "product version is missing" >&2
  exit 1
fi

mkdir -p "$(dirname "${output}")"
(
  cd "${repo_root}"
  CGO_ENABLED=0 GOOS="${goos}" GOARCH="${goarch}" \
    go build \
      -trimpath \
      -ldflags="-s -w -X main.version=${product_version}" \
      -o "${output}" \
      ./cmd/xgc-media-edge
)
echo "Built ${output} (${goos}/${goarch}, version ${product_version})"
