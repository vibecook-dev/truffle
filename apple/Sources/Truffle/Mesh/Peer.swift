/// Opaque process-local peer reference token (RFC 024 §6.3).
///
/// Encodes `{tailscaleId}:{generation}` — treat the format as opaque and
/// unstable across versions. Constructed only by Truffle; never persist it
/// across launches (persist `deviceId` instead).
public struct PeerRef: Hashable, Sendable, CustomStringConvertible {
    public let description: String

    init(tailscaleId: String, generation: UInt64) {
        self.description = "\(tailscaleId):\(generation)"
    }

    /// Parse a candidate ref string back into its parts (used to classify
    /// stale refs as `peerGone` rather than not-found — RFC 022 I5).
    static func parse(_ raw: String) -> (tailscaleId: String, generation: UInt64)? {
        guard let sep = raw.lastIndex(of: ":") else { return nil }
        let idPart = String(raw[raw.startIndex..<sep])
        let genPart = String(raw[raw.index(after: sep)...])
        guard !idPart.isEmpty, let gen = UInt64(genPart) else { return nil }
        return (idPart, gen)
    }
}

/// An immutable snapshot of a mesh peer (RFC 024 §6.3).
///
/// `Peer` is a value snapshot, not a live handle. Every peer-taking call
/// re-resolves `ref` including its generation; if that generation left the
/// registry the call throws `MeshError.peerGone`, even if the same Tailscale
/// node has since rejoined.
public struct Peer: Identifiable, Hashable, Sendable {
    /// Stable for this registry generation. A leave + rejoin gets a new ref.
    /// Row identity NEVER flips when hello lands (RFC 022 identity-flip
    /// bug class).
    public let ref: PeerRef
    public var id: PeerRef { ref }

    /// Durable app-level identity (ULID) after hello — nil before hello.
    /// Use for persistence across restarts; never for row identity.
    public let deviceId: String?

    /// Tailscale stable node ID — the routing key; matches `tailscale_id`
    /// on the wire (RFC 017 §8). NOT the rotating WireGuard node key, and
    /// never pretend this is deviceId.
    public let tailscaleId: String
    public let generation: UInt64

    /// The login of the tailnet user who owns this node (RFC 025 §3.6, D7) —
    /// `nil` when Layer 3 reported none. On a gated node every listed peer
    /// matched the node's allow-list, so this is the login that passed it.
    public let loginName: String?

    public let displayName: String
    public let hostname: String
    public let tailnetIPs: [String]
    public let online: Bool
    public let appId: String?
    public let isLocal: Bool

    init(
        ref: PeerRef, deviceId: String?, tailscaleId: String, generation: UInt64,
        displayName: String, hostname: String, tailnetIPs: [String], online: Bool,
        appId: String?, isLocal: Bool, loginName: String? = nil
    ) {
        self.ref = ref
        self.deviceId = deviceId
        self.tailscaleId = tailscaleId
        self.generation = generation
        self.loginName = loginName
        self.displayName = displayName
        self.hostname = hostname
        self.tailnetIPs = tailnetIPs
        self.online = online
        self.appId = appId
        self.isLocal = isLocal
    }

    /// Row IDENTITY is `ref` (stable per generation — SwiftUI rows never
    /// churn when hello lands), but EQUALITY is full content: two snapshots
    /// of the same generation differ once `deviceId`/`online`/metadata
    /// change, so SwiftUI's Equatable-based diffing re-renders them.
    /// Hashing stays ref-only (legal: equal values always share a ref).
    public static func == (lhs: Peer, rhs: Peer) -> Bool {
        lhs.ref == rhs.ref
            && lhs.deviceId == rhs.deviceId
            && lhs.displayName == rhs.displayName
            && lhs.hostname == rhs.hostname
            && lhs.tailnetIPs == rhs.tailnetIPs
            && lhs.online == rhs.online
            && lhs.appId == rhs.appId
            && lhs.loginName == rhs.loginName
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(ref)
    }
}
