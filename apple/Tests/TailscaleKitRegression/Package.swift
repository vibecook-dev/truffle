// swift-tools-version: 6.0
import PackageDescription

// test-tailscalekit.sh copies the exact patched framework sources into this
// temporary package so these runtime tests cannot silently use an old binary.
let package = Package(
    name: "TailscaleKitRegression",
    platforms: [.macOS(.v14)],
    targets: [
        .target(name: "TailscaleKit", swiftSettings: [.swiftLanguageMode(.v5)]),
        .testTarget(name: "TailscaleKitTests", dependencies: ["TailscaleKit"]),
    ]
)
