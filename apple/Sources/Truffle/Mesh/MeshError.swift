/// Errors thrown by the Truffle Swift runtime (RFC 024 §10).
public enum MeshError: Error, Sendable, Equatable {
    case invalidAppId(String)
    case needsLogin
    case needsMachineAuth
    case notRunning
    case peerNotFound(String)
    case peerAmbiguous(query: String, candidates: [String])
    /// A `Peer` snapshot's underlying node re-keyed or left the mesh between
    /// snapshot and use (desktop `PeerGone` equivalent — RFC 022 §7.7).
    case peerGone(String)
    case identityUnavailable(String)
    case identityMismatch(claimed: String, authenticated: String)
    /// The caller's WhoIs login is absent from, or matches no glob in, this
    /// node's `loginAllow` list (RFC 025 §3.4, D4). Close code 4004.
    case loginRefused(login: String?)
    case invalidPayload(String)
    case payloadTooLarge(actual: Int, limit: Int)
    case protocolViolation(String)
    case dialFailed(String)
    case listenFailed(String)
    case transport(String)
    case timeout(String)
    case stopped
}
