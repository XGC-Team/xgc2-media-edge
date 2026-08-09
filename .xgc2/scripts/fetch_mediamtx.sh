#!/usr/bin/env bash

set -euo pipefail

version="v1.20.0"
architecture="${1:-}"
output_dir="${2:-}"

case "${architecture}" in
  amd64)
    checksum="952d5f7d31d1b448ab4da4509550594c511d42636db9d7bb175d377f4ede81df"
    ;;
  arm64)
    checksum="6aa3c03da7b6477f1e110c8e18e819cf9ef121e8981b52b8f8219982dae35f2f"
    ;;
  *)
    echo "usage: $0 {amd64|arm64} OUTPUT_DIR" >&2
    exit 1
    ;;
esac
if [[ -z "${output_dir}" || "${output_dir}" != /* ]]; then
  echo "MediaMTX output directory must be absolute" >&2
  exit 1
fi

work_dir="$(mktemp -d)"
trap 'rm -rf -- "${work_dir}"' EXIT
archive="${work_dir}/mediamtx.tar.gz"
source_archive="${XGC2_MEDIAMTX_ARCHIVE:-}"
if [[ -n "${source_archive}" ]]; then
  if [[ ! -f "${source_archive}" ]]; then
    echo "XGC2_MEDIAMTX_ARCHIVE does not exist: ${source_archive}" >&2
    exit 1
  fi
  cp -- "${source_archive}" "${archive}"
else
  curl \
    --fail \
    --location \
    --retry 4 \
    --retry-connrefused \
    --silent \
    --show-error \
    --output "${archive}" \
    "https://github.com/bluenviron/mediamtx/releases/download/${version}/mediamtx_${version}_linux_${architecture}.tar.gz"
fi
printf '%s  %s\n' "${checksum}" "${archive}" | sha256sum --check --status

tar \
  --extract \
  --gzip \
  --file "${archive}" \
  --directory "${work_dir}" \
  --no-same-owner \
  --no-same-permissions \
  mediamtx LICENSE
test -x "${work_dir}/mediamtx"
test -f "${work_dir}/LICENSE"

install -d -m 0755 "${output_dir}"
install -m 0755 "${work_dir}/mediamtx" "${output_dir}/mediamtx"
install -m 0644 "${work_dir}/LICENSE" "${output_dir}/LICENSE.mediamtx"
echo "Fetched MediaMTX ${version} (${architecture}) with verified SHA-256."
