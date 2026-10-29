#!/usr/bin/env bash

# Build one target binary. The React player is architecture-independent: it is
# produced and checked once by the maintained Noble Web builder and committed
# with its receipt. XGC2_MEDIA_EDGE_WEB selects how this invocation treats it:
#
#   build   (default) run the Web builder: verify the receipt, install locked
#           dependencies and rebuild when needed. Requires Node 22.12+.
#   verify  package the committed player only: run the portable receipt
#           verifier (Python, no Node) and fail closed on source/lock/artifact
#           drift. Target Debian builders use this mode.
#
# Both modes then build the Go binary and write its runtime receipt.

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/../.." && pwd)"
output="${XGC2_MEDIA_EDGE_OUTPUT:-${repo_root}/.ci/bin/xgc-media-edge}"
goos="${GOOS:-linux}"
goarch="${GOARCH:-$(go env GOARCH)}"
web_mode="${XGC2_MEDIA_EDGE_WEB:-build}"

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
case "${web_mode}" in
  build)
    if ! command -v node >/dev/null 2>&1 || ! command -v npm >/dev/null 2>&1 ||
      ! node -e 'const [major, minor] = process.versions.node.split(".").map(Number); process.exit(major > 22 || major === 22 && minor >= 12 ? 0 : 1)'; then
      echo "the Web builder requires Node 22.12+ and npm; target packaging verifies the committed player with XGC2_MEDIA_EDGE_WEB=verify" >&2
      exit 1
    fi
    (
      cd "${repo_root}/web"
      if ! npm run verify:build --silent; then
        if ! node scripts/build-receipt.mjs --check-install || ! npm ls --all --silent >/dev/null; then
          npm ci --include=dev --no-audit --no-fund
        fi
        npm run build
      fi
      npm run verify:build --silent
    )
    ;;
  verify)
    # Packaging consumes the committed player bytes; the portable verifier
    # fails closed on source, lock or artifact drift and never needs Node.
    python3 "${repo_root}/.xgc2/scripts/verify_web_receipt.py"
    ;;
  *)
    echo "XGC2_MEDIA_EDGE_WEB must be 'build' (Web builder) or 'verify' (package the committed player)" >&2
    exit 1
    ;;
esac
(
  cd "${repo_root}"
  CGO_ENABLED=0 GOOS="${goos}" GOARCH="${goarch}" \
    go build \
      -trimpath \
      -ldflags="-s -w -X main.version=${product_version}" \
      -o "${output}" \
      ./cmd/xgc-media-edge
)
node "${repo_root}/web/scripts/build-runtime-receipt.mjs" "${output}" "${goos}" "${goarch}" "${product_version}"
echo "Built ${output} (${goos}/${goarch}, version ${product_version})"
