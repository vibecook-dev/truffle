import Foundation

/// Session protocol constants (RFC 024 §8.1).
///
/// These are the shipped desktop `truffle-core` defaults (`WsConfig::default`,
/// `session/hello.rs`, `transport/websocket.rs`) and the v0.1 Swift contract.
/// Any locally stricter value that can reject otherwise-valid peers must be
/// documented in release notes.
public enum SessionLimits {
    /// Session WebSocket listen port (`truffle-core` `ws_port` default).
    public static let sessionPort: UInt16 = 9417
    /// WebSocket client request path.
    public static let wsPath = "/ws"

    /// Timeout applied to the first hello envelope read (RFC 017 §8).
    public static let helloTimeout: Duration = .seconds(5)
    /// Bound on the complete incoming upgrade + hello exchange.
    public static let handshakeTimeout: Duration = .seconds(10)

    /// Maximum WebSocket frame AND message size (desktop sets both to 16 MiB).
    public static let maxMessageSize = 16 * 1024 * 1024
    /// Maximum encoded envelope size accepted before JSON decoding.
    /// Stricter than the 16 MiB transport bound (documented in RFC 024 §8.3).
    public static let maxEnvelopeBytes = 15 * 1024 * 1024
    /// Maximum raw input to `sendBytes` (base64 expansion headroom under the
    /// envelope limit) and maximum decoded size accepted from a `"bytes"`
    /// payload.
    public static let maxRawBytesPayload = 11 * 1024 * 1024

    /// Maximum concurrent in-flight incoming upgrade/hello handshakes.
    public static let maxPendingHandshakes = 256
    /// Control frames (Ping/Pong) tolerated before the hello frame.
    public static let maxControlFramesBeforeHello = 16

    /// Interval between keepalive Ping frames after the hello exchange.
    public static let pingInterval: Duration = .seconds(10)
    /// Maximum wait for a Pong before the connection is considered dead.
    public static let pongTimeout: Duration = .seconds(30)

    /// Local emission rule (RFC 024 §8.3): namespace / msg_type must be
    /// non-empty and at most this many UTF-8 bytes. Received envelopes are
    /// bounded primarily by `maxEnvelopeBytes` for desktop compatibility.
    public static let maxNamespaceBytes = 1024
    public static let maxMsgTypeBytes = 1024

    /// Hello identity field bounds — desktop maximums (`websocket.rs`).
    public static let maxAppIdBytes = 32
    public static let maxDeviceNameBytes = 512
    public static let maxDeviceIdBytes = 64
    public static let maxTailscaleIdBytes = 256
    public static let maxOsBytes = 32
}

/// WebSocket close codes for the session handshake (RFC 017 §8).
public enum SessionCloseCode {
    /// Remote peer's `app_id` did not match ours.
    public static let appMismatch: UInt16 = 4001
    /// Malformed, invalid, or missing hello envelope.
    public static let helloProtocol: UInt16 = 4002
    /// Claimed `tailscale_id` contradicts the authenticated identity, or no
    /// authenticated identity was available — under the fail-closed policy,
    /// or on a login-gated node under ANY policy (RFC 025 §3.4).
    public static let identityMismatch: UInt16 = 4003
    /// The caller's WhoIs login is absent from, or matches no glob in, this
    /// node's `loginAllow` list (RFC 025 §3.4/§4, D4). Sent before our own
    /// hello, so a refused caller never learns our identity block.
    public static let loginRefused: UInt16 = 4004
    /// RFC 6455 normal closure.
    public static let normal: UInt16 = 1000
}
