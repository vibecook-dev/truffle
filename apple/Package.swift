// swift-tools-version: 6.0
// Truffle Swift — Apple-native mesh runtime (RFC 024).
//
// The `Truffle` target is the backend-agnostic product core (identity, wire
// codecs, session handshake, peer registry, MeshNode). It has no dependency
// on TailscaleKit and builds/tests on macOS with plain SwiftPM.
//
// The `TruffleTailscale` target hosts the L0–L1 runtime (libtailscale /
// TailscaleKit glue). Its pinned XCFramework must be materialized before the
// package is resolved so dependency wiring cannot vary with the caller's CWD.

import PackageDescription

let tailscaleArtifactPath = "Vendor/TailscaleKit.xcframework"

let tailscaleDependencies: [Target.Dependency] = [
    "Truffle",
    .target(name: "TailscaleKit", condition: .when(platforms: [.iOS])),
]
let packageTargets: [Target] = [
    .binaryTarget(name: "TailscaleKit", path: tailscaleArtifactPath),
    .target(
        name: "Truffle",
        path: "Sources/Truffle"
    ),
    .target(
        name: "TruffleSwiftUI",
        dependencies: ["Truffle"],
        path: "Sources/TruffleSwiftUI"
    ),
    .target(
        name: "TruffleTailscale",
        dependencies: tailscaleDependencies,
        path: "Sources/TruffleTailscale"
    ),
    .testTarget(
        name: "TruffleTailscaleTests",
        dependencies: ["TruffleTailscale"],
        path: "Tests/TruffleTailscaleTests"
    ),
    .testTarget(
        name: "TruffleTests",
        dependencies: ["Truffle"],
        path: "Tests/TruffleTests",
        resources: [.copy("Fixtures")]
    ),
]

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
    targets: packageTargets
)
