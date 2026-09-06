#!/usr/bin/env bash
# Materialize the exact TailscaleKit binary consumed by Package.swift.
set -euo pipefail

REVISION="59d4bb82744915815178e0f0776d60026a397ee7"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE="${TAILSCALE_SOURCE_DIR:-$ROOT/.vendor/libtailscale-$REVISION}"
DESTINATION="$ROOT/Vendor/TailscaleKit.xcframework"
PATCH="$ROOT/patches/libtailscale-remote-address-fd.patch"
PEER_PATCH="$ROOT/patches/libtailscale-peer-notifications.patch"
GO_DEPS="$ROOT/Vendor/libtailscale"
PRIVACY_MANIFEST="$ROOT/Vendor/TailscaleKit-PrivacyInfo.xcprivacy"
DEVELOPER_DIR="${DEVELOPER_DIR:-/Applications/Xcode.app/Contents/Developer}"
export DEVELOPER_DIR
export PATH="$DEVELOPER_DIR/usr/bin:$PATH"

if [[ ! -d "$DEVELOPER_DIR" ]]; then
  echo "error: full Xcode is required; set DEVELOPER_DIR" >&2
  exit 1
fi
if [[ ! -f "$PRIVACY_MANIFEST" ]]; then
  echo "error: missing reviewed privacy manifest $PRIVACY_MANIFEST" >&2
  exit 1
fi

# libtailscale's upstream Go pin lags the sidecar. Build from the reviewed
# module graph we commit, using the same recorded toolchain as the sidecar.
GO_VERSION="$(awk '$1 == "go" { print $2 }' "$GO_DEPS/go.mod")"
TAILSCALE_VERSION="$(awk '$1 == "tailscale.com" { print $2 } $1 == "require" && $2 == "tailscale.com" { print $3 }' "$GO_DEPS/go.mod")"
SIDECAR_TAILSCALE_VERSION="$(awk '$1 == "require" && $2 == "tailscale.com" { print $3 }' "$ROOT/../packages/sidecar-slim/go.mod")"
if [[ -z "$TAILSCALE_VERSION" || "$TAILSCALE_VERSION" != "$SIDECAR_TAILSCALE_VERSION" ]]; then
  echo "error: Apple and sidecar Tailscale module versions must match" >&2
  exit 1
fi
export GOTOOLCHAIN=local
if [[ "$(go env GOVERSION)" != "go$GO_VERSION" ]]; then
  echo "error: TailscaleKit requires Go $GO_VERSION on PATH" >&2
  exit 1
fi
export GOFLAGS="${GOFLAGS:-} -mod=readonly -trimpath -buildvcs=false"

if [[ ! -d "$SOURCE/.git" ]]; then
  mkdir -p "$(dirname "$SOURCE")"
  git init "$SOURCE"
  git -C "$SOURCE" remote add origin https://github.com/tailscale/libtailscale.git
  git -C "$SOURCE" fetch --depth 1 origin "$REVISION"
  git -C "$SOURCE" checkout --detach FETCH_HEAD
fi

ACTUAL_REVISION="$(git -C "$SOURCE" rev-parse HEAD)"
if [[ "$ACTUAL_REVISION" != "$REVISION" ]]; then
  echo "error: libtailscale is $ACTUAL_REVISION; expected $REVISION" >&2
  exit 1
fi
if [[ -n "$(git -C "$SOURCE" status --porcelain)" ]]; then
  echo "error: libtailscale source has local modifications" >&2
  exit 1
fi

BUILD_SOURCE="$(mktemp -d "$ROOT/.vendor/libtailscale-build.XXXXXX")"
cleanup() {
  git -C "$SOURCE" worktree remove --force "$BUILD_SOURCE" >/dev/null 2>&1 || true
}
trap cleanup EXIT
git -C "$SOURCE" worktree add --detach "$BUILD_SOURCE" "$REVISION"
git -C "$BUILD_SOURCE" apply --check "$PATCH"
git -C "$BUILD_SOURCE" apply "$PATCH"
git -C "$BUILD_SOURCE" apply --check "$PEER_PATCH"
git -C "$BUILD_SOURCE" apply "$PEER_PATCH"
cp "$GO_DEPS/go.mod" "$BUILD_SOURCE/go.mod"
cp "$GO_DEPS/go.sum" "$BUILD_SOURCE/go.sum"
ACTUAL_TAILSCALE_VERSION="$(cd "$BUILD_SOURCE" && go list -m -f '{{.Version}}' tailscale.com)"
if [[ "$ACTUAL_TAILSCALE_VERSION" != "$TAILSCALE_VERSION" ]]; then
  echo "error: Tailscale module is $ACTUAL_TAILSCALE_VERSION; expected $TAILSCALE_VERSION" >&2
  exit 1
fi
if [[ "${TAILSCALE_RUN_TESTS:-0}" == 1 ]]; then
  (cd "$BUILD_SOURCE" && go test -race -count=1 -timeout=3m .)
fi

echo "Building patched TailscaleKit from $REVISION with Tailscale $TAILSCALE_VERSION and Go $GO_VERSION"
make -C "$BUILD_SOURCE/swift" ios-fat
ARTIFACT="$BUILD_SOURCE/swift/build/Build/Products/Release-iphonefat/TailscaleKit.xcframework"
if [[ ! -d "$ARTIFACT" ]]; then
  echo "error: build did not produce $ARTIFACT" >&2
  exit 1
fi
for framework in "$ARTIFACT"/*/TailscaleKit.framework; do
  BUILD_INFO="$(go version -m "$framework/TailscaleKit")"
  BINARY_GO_VERSION="$(awk 'NR == 1 { print $NF }' <<< "$BUILD_INFO")"
  BINARY_TAILSCALE_VERSION="$(awk '$1 == "dep" && $2 == "tailscale.com" { print $3 }' <<< "$BUILD_INFO")"
  if [[ "$BINARY_GO_VERSION" != "go$GO_VERSION" || "$BINARY_TAILSCALE_VERSION" != "$TAILSCALE_VERSION" ]]; then
    echo "error: framework contains unexpected Go/Tailscale versions: $framework" >&2
    exit 1
  fi
done

mkdir -p "$ROOT/Vendor"
ditto "$ARTIFACT" "$DESTINATION"
framework_count=0
for framework in "$DESTINATION"/*/TailscaleKit.framework; do
  [[ -d "$framework" ]] || continue
  cp "$PRIVACY_MANIFEST" "$framework/PrivacyInfo.xcprivacy"
  ((framework_count += 1))
done
if [[ "$framework_count" -ne 2 ]]; then
  echo "error: expected two TailscaleKit framework slices, found $framework_count" >&2
  exit 1
fi
cp "$SOURCE/LICENSE" "$ROOT/Vendor/TAILSCALE-LICENSE"

echo "Materialized $DESTINATION"
find "$DESTINATION" -type f -maxdepth 5 -print | sort | xargs shasum -a 256
