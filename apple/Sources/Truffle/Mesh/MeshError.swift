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
    /// node's `loginAllow` list (RFC 025 §3.4, D4). Close code 4004. This is
    /// the SERVER-role error — the side that ran the gate.
    case loginRefused(login: String?)
    /// The remote closed with an application code (4000–4999) before sending
    /// its hello: 4001 app mismatch, 4002 hello protocol, 4003 identity,
    /// 4004 login refused. This is the DIALING side's view of a refusal, and
    /// it carries the code so a caller can tell a gate from a broken pipe.
    case helloRefused(code: UInt16, reason: String)
    case invalidPayload(String)
    case payloadTooLarge(actual: Int, limit: Int)
    case protocolViolation(String)
    case dialFailed(String)
    case listenFailed(String)
    case transport(String)
    case timeout(String)
    case stopped
}
