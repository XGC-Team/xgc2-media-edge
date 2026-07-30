#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
package_name="xgc2-media-edge"
package_distribution="${PACKAGE_DISTRIBUTION:-}"
output_dir="${XGC2_MEDIA_EDGE_DEB_OUTPUT_DIR:-${repo_root}/debs}"

if [[ -z "${package_distribution}" && -r /etc/os-release ]]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  package_distribution="${VERSION_CODENAME:-${UBUNTU_CODENAME:-}}"
fi
case "${package_distribution}" in
  focal|jammy|noble) ;;
  *)
    echo "PACKAGE_DISTRIBUTION must be focal, jammy, or noble" >&2
    exit 1
    ;;
esac

package_base_version="$(
  sed -n 's/^version:[[:space:]]*//p' \
    "${repo_root}/.xgc2/product.yml" | head -n 1
)"
if [[ -z "${package_base_version}" ]]; then
  echo "package version is missing" >&2
  exit 1
fi
version="${PACKAGE_VERSION:-${package_base_version}~${package_distribution}}"
case "${version}" in
  *"~${package_distribution}"*|*"+${package_distribution}"*) ;;
  *)
    echo "binary package version ${version} must identify ${package_distribution}" >&2
    exit 1
    ;;
esac

arch="$(dpkg --print-architecture)"
case "${arch}" in
  amd64|arm64) ;;
  *)
    echo "unsupported Debian architecture ${arch}" >&2
    exit 1
    ;;
esac

mkdir -p "${repo_root}/.ci" "${output_dir}"
work_dir="$(mktemp -d "${repo_root}/.ci/package.XXXXXX")"
trap 'rm -rf -- "${work_dir}"' EXIT
pkg_root="${work_dir}/pkg/${package_name}"
binary="${work_dir}/xgc-media-edge"

GOOS=linux GOARCH="${arch}" XGC2_MEDIA_EDGE_OUTPUT="${binary}" \
  "${repo_root}/.xgc2/scripts/build.sh"

install -d \
  "${pkg_root}/DEBIAN" \
  "${pkg_root}/usr/bin" \
  "${pkg_root}/usr/share/doc/${package_name}"
install -m 0755 "${binary}" "${pkg_root}/usr/bin/xgc-media-edge"
install -m 0644 "${repo_root}/LICENSE" \
  "${pkg_root}/usr/share/doc/${package_name}/copyright"

cat >"${pkg_root}/DEBIAN/control" <<EOF
Package: ${package_name}
Version: ${version}
Section: net
Priority: optional
Architecture: ${arch}
Maintainer: XGC2 <apt@example.com>
Depends: ca-certificates, ffmpeg
Description: Target-resident XGC2 H264/WebRTC media data plane
 Receives co-located H264/RTP and source-control traffic, fans encoded media
 out to WebRTC viewers, optionally records stream-copy Matroska segments, and
 serves immutable camera calibration snapshots.
EOF

test -x "${pkg_root}/usr/bin/xgc-media-edge"
test "$("${pkg_root}/usr/bin/xgc-media-edge" --version)" = "${package_base_version}"

artifact="${output_dir}/${package_name}_${version}_${arch}.deb"
rm -f -- "${artifact}"
fakeroot dpkg-deb --build "${pkg_root}" "${artifact}" >/dev/null
dpkg-deb -I "${artifact}"
echo "Debian artifact written to ${artifact}"
