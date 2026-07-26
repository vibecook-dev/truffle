// swift-tools-version: 6.0
// Truffle Swift — Apple-native mesh runtime (RFC 024).
//
// This manifest lives at the repository root because SwiftPM resolves a URL
// dependency only against a root manifest — `.package(url:)` cannot point at a
// subdirectory. The sources stay under `apple/`; every target names its path
// explicitly so nothing else in this Rust/TypeScript monorepo is scanned.
//
// `apple/Package.swift` is deliberately kept alongside this one as a
// compatibility shim for consumers still using a relative-path dependency on
// `apple/`. SwiftPM only reads the root manifest for URL dependencies, so the
// two coexist. Delete the shim once those consumers have migrated.
//
// ## TailscaleKit is pinned to the vendored dependency, not to a release
//
// The XCFramework's bytes are a function of the libtailscale revision, the
// reviewed patch, and the build toolchain — never of Truffle's version. So it
// is published once under a dependency-keyed release tag and referenced by a
// stable URL, rather than rebuilt per release.
//
// This matters for correctness, not just tidiness: `.binaryTarget(url:)`
// requires the checksum to be committed at the tag SwiftPM resolves, but this
// repository's release assets are built *after* the release commit. A
// per-release artifact could therefore never carry a valid checksum at its own
// tag. Keying the artifact to the dependency removes the ordering problem
// entirely, and every Truffle tag gets an immutable, already-valid checksum.
//
// To change it: bump the revision in `apple/scripts/materialize-tailscalekit.sh`,
// re-materialize, publish a new `tailscalekit-<rev>` release, and update the URL
// and checksum below together. See `apple/Vendor/README.md` for the provenance
// record and the exact commands.

import PackageDescription

let package = Package(
    name: "Truffle",
    platforms: [
        .macOS(.v14),
        .iOS("18.1"),
    ],
    products: [
        .library(name: "Truffle", targets: ["Truffle"]),
        .library(name: "TruffleSwiftUI", targets: ["TruffleSwiftUI"]),
        .library(name: "TruffleTailscale", targets: ["TruffleTailscale"]),
    ],
    targets: [
        .binaryTarget(
            name: "TailscaleKit",
            url:
                "https://github.com/vibecook-dev/truffle/releases/download/tailscalekit-5e89501d/TailscaleKit.xcframework.zip",
            checksum: "25c84847b70f673835e9c0fd75a697fbe76943a0b20314bf56d2f5569c68f494"
        ),
        .target(
            name: "Truffle",
            path: "apple/Sources/Truffle"
        ),
        .target(
            name: "TruffleSwiftUI",
            dependencies: ["Truffle"],
            path: "apple/Sources/TruffleSwiftUI"
        ),
        .target(
            name: "TruffleTailscale",
            dependencies: [
                "Truffle",
                .target(name: "TailscaleKit", condition: .when(platforms: [.iOS])),
            ],
            path: "apple/Sources/TruffleTailscale"
        ),
        .testTarget(
            name: "TruffleTests",
            dependencies: ["Truffle"],
            path: "apple/Tests/TruffleTests",
            resources: [.copy("Fixtures")]
        ),
    ]
)
