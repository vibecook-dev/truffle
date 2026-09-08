#!/usr/bin/env bash
# Run the IPN stream regression tests against the exact source being built.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SOURCE="${1:?usage: test-tailscalekit.sh PATCHED_LIBTAILSCALE_SOURCE}"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/truffle-tailscalekit-tests.XXXXXX")"
trap 'rm -rf "$TEST_ROOT"' EXIT
cp "$ROOT/Tests/TailscaleKitRegression/Package.swift" "$TEST_ROOT/Package.swift"
cp -R "$ROOT/Tests/TailscaleKitRegression/Tests" "$TEST_ROOT/Tests"
mkdir -p "$TEST_ROOT/Sources/TailscaleKit"
for file in MessageReader MessageProcessor Types GoTime; do
  cp "$SOURCE/swift/TailscaleKit/LocalAPI/$file.swift" "$TEST_ROOT/Sources/TailscaleKit/"
done
cp "$SOURCE/swift/TailscaleKit/LogSink.swift" "$TEST_ROOT/Sources/TailscaleKit/"
swift test --package-path "$TEST_ROOT"
