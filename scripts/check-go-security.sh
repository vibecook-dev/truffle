#!/usr/bin/env bash
# Scan reachable code using both locked production Go graphs, including patches.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SCANNER="${GOVULNCHECK:-govulncheck}"
export GOFLAGS="${GOFLAGS:-} -mod=readonly"
(cd "$ROOT/packages/sidecar-slim" && "$SCANNER" ./...)

REVISION="$(cat "$ROOT/apple/Vendor/libtailscale/REVISION")"
SOURCE="$(mktemp -d "${TMPDIR:-/tmp}/truffle-go-audit.XXXXXX")"
trap 'rm -rf "$SOURCE"' EXIT
git -C "$SOURCE" init -q
git -C "$SOURCE" fetch -q --depth 1 https://github.com/tailscale/libtailscale.git "$REVISION"
git -C "$SOURCE" checkout -q --detach FETCH_HEAD
test "$(git -C "$SOURCE" rev-parse HEAD)" = "$REVISION"
for patch in libtailscale-conn-cleanup libtailscale-peer-notifications libtailscale-ipn-stream-recovery; do
  git -C "$SOURCE" apply "$ROOT/apple/patches/$patch.patch"
done
cp "$ROOT/apple/Vendor/libtailscale/"go.{mod,sum} "$SOURCE/"
(cd "$SOURCE" && "$SCANNER" ./...)
