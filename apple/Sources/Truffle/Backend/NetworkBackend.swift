import Foundation

/// A full-duplex byte stream over the mesh (RFC 024 §6.6). Both dialed and
/// accepted sockets map to this abstraction. EOF returns nil (never an
/// empty-data sentinel); writes either consume all bytes or throw.
public protocol MeshConnection: Sendable {
    func read(_ max: Int) async throws -> Data?
    func write(_ data: Data) async throws
    func close() async
}

/// An accepted connection plus the numeric remote endpoint from the
/// authenticated tailnet connection.
public struct MeshAcceptedConnection: Sendable {
    public let connection: any MeshConnection
    /// Numeric remote `ip:port`.
    public let remoteEndpoint: String

    public init(connection: any MeshConnection, remoteEndpoint: String) {
        self.connection = connection
        self.remoteEndpoint = remoteEndpoint
    }
}

public protocol MeshListener: Sendable {
    func accept() async throws -> MeshAcceptedConnection
    func close() async
}

/// WhoIs result for an accepted connection (RFC 024 §13): at minimum the
/// stable Tailscale node ID and the normalized remote addresses used for the
/// comparison. An empty `tailscaleId` means WhoIs produced no concrete
/// identity — the production inbound policy fails closed on that.
///
/// `loginName` / `displayName` carry the caller's tailnet user profile
/// (RFC 025 §3.6, D7). Both are ABSENT, never fabricated: a WhoIs answer with
/// no user profile — or with an empty string in one — leaves the field `nil`.
/// A tagged node reports Tailscale's `tagged-devices` pseudo-login, which is
/// passed through unchanged rather than special-cased.
public struct AuthenticatedPeer: Sendable, Hashable {
    public let tailscaleId: String
    public let remoteAddresses: [String]
    /// The caller's tailnet login (`UserProfile.LoginName`), e.g.
    /// `alice@corp.com`. The login gate's only authority — never
    /// self-declared (RFC 025 §3.7, D8).
    public let loginName: String?
    /// The caller's human-readable profile name (`UserProfile.DisplayName`).
    public let displayName: String?

    public init(
        tailscaleId: String, remoteAddresses: [String], loginName: String? = nil,
        displayName: String? = nil
    ) {
        self.tailscaleId = tailscaleId
        self.remoteAddresses = remoteAddresses
        self.loginName = loginName
        self.displayName = displayName
    }
}

/// A peer row as reported by Layer 3 (netmap / backend status).
public struct BackendPeer: Sendable, Equatable {
    public var tailscaleId: String
    public var hostname: String
    public var dnsName: String?
    public var tailnetIPs: [String]
    public var online: Bool
    /// The login of the tailnet user who owns this node (RFC 025 §3.3) —
    /// a netmap fact, `nil` when the backend cannot report one. A gated node
    /// treats a row without a login as NOT a peer (fail closed).
    public var loginName: String?

    public init(
        tailscaleId: String, hostname: String, dnsName: String? = nil,
        tailnetIPs: [String] = [], online: Bool = true, loginName: String? = nil
    ) {
        self.tailscaleId = tailscaleId
        self.hostname = hostname
        self.dnsName = dnsName
        self.tailnetIPs = tailnetIPs
        self.online = online
        self.loginName = loginName
    }
}

/// Snapshot of the embedded node's backend state.
public struct BackendStatus: Sendable, Equatable {
    public var running: Bool
    public var needsLogin: Bool
    public var needsMachineAuth: Bool
    public var authURL: String?
    public var dnsName: String?
    public var tailnetIPs: [String]
    /// Our own stable Tailscale node ID (empty until known).
    public var tailscaleId: String
    /// The login this node is signed in as (RFC 025 §3.6, D7) — `nil` until
    /// known, never fabricated. A tagged node reports `tagged-devices`.
    public var loginName: String?
    public var peers: [BackendPeer]

    public init(
        running: Bool = false, needsLogin: Bool = false, needsMachineAuth: Bool = false,
        authURL: String? = nil, dnsName: String? = nil, tailnetIPs: [String] = [],
        tailscaleId: String = "", loginName: String? = nil, peers: [BackendPeer] = []
    ) {
        self.running = running
        self.needsLogin = needsLogin
        self.needsMachineAuth = needsMachineAuth
        self.authURL = authURL
        self.dnsName = dnsName
        self.tailnetIPs = tailnetIPs
        self.tailscaleId = tailscaleId
        self.loginName = loginName
        self.peers = peers
    }
}

/// Push events from Layer 0–1 (IPN bus / netmap).
public enum BackendEvent: Sendable {
    case status(BackendStatus)
    case peerUpsert(BackendPeer)
    case peerLeft(tailscaleId: String)
    case authRequired(URL)
    case health(String)
}

/// The seam between the Truffle product core and the Layer 0–1 runtime
/// (RFC 024 §13). `TailscaleKitBackend` (TruffleTailscale) is production;
/// `LoopbackBackend` is for tests and must make unverified identity an
/// explicit opt-in rather than silently returning a claimed ID.
public protocol NetworkBackend: Sendable {
    func start() async throws
    func stop() async
    func status() async throws -> BackendStatus
    /// Each access returns a fresh stream of push events.
    func events() async -> AsyncStream<BackendEvent>
    func dial(host: String, port: UInt16) async throws -> any MeshConnection
    func listen(port: UInt16) async throws -> any MeshListener
    func whoIs(remoteEndpoint: String) async throws -> AuthenticatedPeer
    /// Build a URLSession that routes through the mesh, based on the
    /// caller's configuration (RFC 024 §6.5): the implementation copies it
    /// before installing proxy settings, preserves cache/cookie/timeout
    /// options, and rejects background sessions or conflicting preconfigured
    /// proxies rather than returning a session that bypasses the mesh.
    func makeURLSession(configuration: URLSessionConfiguration) async throws -> URLSession
}
