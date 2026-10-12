import Foundation

/// Peer identity metadata advertised in the hello envelope (RFC 017 §8,
/// mirrors `truffle-core::session::hello::PeerIdentity`).
public struct PeerIdentity: Codable, Hashable, Sendable {
    /// The application namespace. Peers with different `app_id` close the
    /// connection immediately (close code 4001).
    public var appId: String
    /// Stable per-device ULID.
    public var deviceId: String
    /// Human-readable device name (original form, NOT the slug).
    public var deviceName: String
    /// Operating system identifier. Swift sends `"ios"`; macOS embed sends
    /// `"darwin"` to match desktop convention (RFC 024 §8.2).
    public var os: String
    /// Tailscale stable node ID — the session layer's routing key.
    /// Optional on the wire in Rust (`serde(default)`), so it defaults to "".
    public var tailscaleId: String

    public init(
        appId: String, deviceId: String, deviceName: String, os: String, tailscaleId: String
    ) {
        self.appId = appId
        self.deviceId = deviceId
        self.deviceName = deviceName
        self.os = os
        self.tailscaleId = tailscaleId
    }

    enum CodingKeys: String, CodingKey {
        case appId = "app_id"
        case deviceId = "device_id"
        case deviceName = "device_name"
        case os
        case tailscaleId = "tailscale_id"
    }

    public init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        appId = try c.decode(String.self, forKey: .appId)
        deviceId = try c.decode(String.self, forKey: .deviceId)
        deviceName = try c.decode(String.self, forKey: .deviceName)
        os = try c.decode(String.self, forKey: .os)
        // Matches Rust `#[serde(default)]` on tailscale_id.
        tailscaleId = try c.decodeIfPresent(String.self, forKey: .tailscaleId) ?? ""
    }
}

/// Identity block sent once in each direction when a session link comes up
/// (RFC 017 §8). Version 2 is the first with a structured `identity` field;
/// version 1 is pre-RFC-017 and rejected.
public struct HelloEnvelope: Codable, Sendable, Equatable {
    /// Wire tag discriminating this frame; must be `"hello"`.
    public var kind: String
    /// Version of the hello envelope schema. Must be >= 2.
    public var version: UInt32
    /// The sender's identity block.
    public var identity: PeerIdentity

    /// The hello envelope schema version this build emits.
    public static let currentVersion: UInt32 = 2
    /// The minimum hello envelope version this build accepts from a peer.
    public static let minSupportedVersion: UInt32 = 2

    public init(identity: PeerIdentity) {
        self.kind = "hello"
        self.version = Self.currentVersion
        self.identity = identity
    }

    public func encoded() throws -> Data {
        try JSONEncoder().encode(self)
    }

    public static func decoding(_ data: Data) throws -> HelloEnvelope {
        try JSONDecoder().decode(HelloEnvelope.self, from: data)
    }
}

// MARK: - Validation (port of websocket.rs::validate_hello)

/// Classified hello failures. Each maps to an RFC 017 close code.
///
/// Validation covers only what the hello itself can be wrong about: 4001 and
/// 4002. The refusals that depend on evidence OUTSIDE the hello are decided
/// afterwards by `Handshake.server`, which closes with
/// `SessionCloseCode.identityMismatch` (4003) for a contradicted or absent
/// WhoIs identity and `SessionCloseCode.loginRefused` (4004) for a login the
/// node's `loginAllow` does not admit (RFC 025 §3.4). The login is never
/// declared in the hello — WhoIs is its only authority (RFC 025 §3.7, D8).
public enum HelloValidationError: Error, Sendable, Equatable {
    /// Malformed / invalid hello → close code 4002.
    case malformed(String)
    /// `app_id` mismatch → close code 4001.
    case appMismatch(local: String, remote: String)

    public var closeCode: UInt16 {
        switch self {
        case .malformed: return SessionCloseCode.helloProtocol
        case .appMismatch: return SessionCloseCode.appMismatch
        }
    }
}

extension HelloEnvelope {
    /// Validate a remote hello against our `app_id`, mirroring desktop
    /// `validate_hello` exactly: kind, version, per-field byte bounds
    /// (checked BEFORE the app_id comparison so an oversized field cannot
    /// sneak through on a matched app_id), the RFC 022 I1 rule
    /// (`device_id` non-empty and distinct from `tailscale_id`), then
    /// `app_id` equality. Returns the validated identity.
    public func validate(localAppId: String) throws -> PeerIdentity {
        guard kind == "hello" else {
            throw HelloValidationError.malformed("unexpected hello kind: \(kind)")
        }
        guard version >= Self.minSupportedVersion else {
            throw HelloValidationError.malformed(
                "unsupported hello version \(version) (minimum supported: \(Self.minSupportedVersion))"
            )
        }
        if identity.appId.utf8.count > SessionLimits.maxAppIdBytes
            || identity.deviceName.utf8.count > SessionLimits.maxDeviceNameBytes
            || identity.deviceId.utf8.count > SessionLimits.maxDeviceIdBytes
            || identity.tailscaleId.utf8.count > SessionLimits.maxTailscaleIdBytes
            || identity.os.utf8.count > SessionLimits.maxOsBytes
        {
            throw HelloValidationError.malformed("identity field exceeds maximum allowed length")
        }
        if identity.deviceId.isEmpty || identity.deviceId == identity.tailscaleId {
            throw HelloValidationError.malformed(
                "identity device_id must be non-empty and distinct from tailscale_id")
        }
        guard identity.appId == localAppId else {
            throw HelloValidationError.appMismatch(local: localAppId, remote: identity.appId)
        }
        return identity
    }
}
