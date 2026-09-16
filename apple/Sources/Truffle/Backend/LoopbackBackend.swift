import Foundation

/// An in-memory tailnet for tests (RFC 024 §13): nodes register with a
/// shared `LoopbackNetwork`, discover each other as Layer 3 peers, dial by
/// hostname or synthetic IP, and get authentic WhoIs answers derived from
/// the dialing node — with unverified identity as an explicit opt-in.
public actor LoopbackNetwork {
    public struct NodeHandle: Sendable {
        public let tailscaleId: String
        public let ip: String
    }

    private struct Node {
        var tailscaleId: String
        var hostname: String
        var ip: String
        var online: Bool
        /// The node owner's tailnet login (RFC 025 §3.3). `nil` models a
        /// backend that cannot report one — the gate's fail-closed row.
        var loginName: String?
        var displayName: String?
        /// Hidden nodes can dial and are WhoIs-resolvable but never appear
        /// in snapshots or join announcements — simulates an inbound hello
        /// racing ahead of the netmap (RFC 024 §7.2).
        var hidden: Bool = false
        var listeners: [UInt16: LoopbackListener] = [:]
        var backend: LoopbackBackend?
    }

    private var nodes: [String: Node] = [:]  // by tailscaleId
    private var nextIP = 1
    private var nextPort: UInt16 = 40_000
    /// When true, `whoIs` reports no concrete identity (exercises the
    /// fail-closed path).
    private var withholdWhoIs = false
    /// When true, `whoIs` still reports a concrete node ID but no login —
    /// the RFC 025 §3.4 row "`nodeId` ok, `loginName` absent".
    private var withholdWhoIsLogin = false

    public init() {}

    public func setWithholdWhoIs(_ value: Bool) {
        withholdWhoIs = value
    }

    public func setWithholdWhoIsLogin(_ value: Bool) {
        withholdWhoIsLogin = value
    }

    /// Change a registered node's login after `join` (a profile switch).
    public func setLogin(tailscaleId: String, loginName: String?) {
        nodes[tailscaleId]?.loginName = loginName
    }

    /// Register a node and return its backend. `hostname` should follow the
    /// `truffle-{appId}-{slug}` scheme for discovery (RFC 024 §7.2).
    /// `hidden` nodes stay out of snapshots/announcements (raced-netmap
    /// simulation) until `reveal(tailscaleId:)`.
    public func join(
        tailscaleId: String, hostname: String, hidden: Bool = false,
        loginName: String? = nil, displayName: String? = nil
    ) -> LoopbackBackend {
        let ip = "100.64.0.\(nextIP)"
        nextIP += 1
        let node = Node(
            tailscaleId: tailscaleId, hostname: hostname, ip: ip, online: true,
            loginName: loginName, displayName: displayName, hidden: hidden)
        nodes[tailscaleId] = node
        let backend = LoopbackBackend(network: self, tailscaleId: tailscaleId, ip: ip)
        nodes[tailscaleId]?.backend = backend
        if !hidden {
            announce(tailscaleId)
        }
        return backend
    }

    /// Make a hidden node visible: it enters snapshots and everyone gets
    /// the netmap upsert (the "later Layer 3 event" of RFC 024 §7.2).
    public func reveal(tailscaleId: String) {
        guard nodes[tailscaleId]?.hidden == true else { return }
        nodes[tailscaleId]?.hidden = false
        announce(tailscaleId)
    }

    private func announce(_ tailscaleId: String) {
        for (id, other) in nodes where id != tailscaleId {
            if let peer = backendPeer(for: tailscaleId) {
                other.backend?.push(.peerUpsert(peer))
            }
        }
    }

    /// Remove a node from the tailnet (peers observe `peerLeft`).
    public func leave(tailscaleId: String) async {
        guard let node = nodes.removeValue(forKey: tailscaleId) else { return }
        for listener in node.listeners.values {
            await listener.shutdown()
        }
        for (_, other) in nodes {
            other.backend?.push(.peerLeft(tailscaleId: tailscaleId))
        }
    }

    func backendPeer(for tailscaleId: String) -> BackendPeer? {
        guard let node = nodes[tailscaleId], !node.hidden else { return nil }
        return BackendPeer(
            tailscaleId: node.tailscaleId,
            hostname: node.hostname,
            dnsName: "\(node.hostname).loopback.ts.net",
            tailnetIPs: [node.ip],
            online: node.online,
            loginName: node.loginName)
    }

    func snapshot(for selfId: String) -> BackendStatus {
        guard let me = nodes[selfId] else {
            return BackendStatus(running: false)
        }
        let peers = nodes.keys
            .filter { $0 != selfId }
            .compactMap(backendPeer(for:))
        return BackendStatus(
            running: true,
            dnsName: "\(me.hostname).loopback.ts.net",
            tailnetIPs: [me.ip],
            tailscaleId: me.tailscaleId,
            loginName: me.loginName,
            peers: peers)
    }

    func registerListener(
        owner tailscaleId: String, port: UInt16, listener: LoopbackListener
    ) throws {
        guard nodes[tailscaleId] != nil else {
            throw MeshError.listenFailed("node not on network")
        }
        guard nodes[tailscaleId]?.listeners[port] == nil else {
            throw MeshError.listenFailed("port \(port) already in use")
        }
        nodes[tailscaleId]?.listeners[port] = listener
    }

    func unregisterListener(owner tailscaleId: String, port: UInt16) {
        nodes[tailscaleId]?.listeners[port] = nil
    }

    /// Dial `host` (hostname or synthetic IP) from `fromId`. Returns the
    /// caller's end of a byte pipe; the listener's end is delivered to
    /// `accept()` with a remote endpoint that WhoIs can resolve back to the
    /// dialing node.
    func dial(from fromId: String, host: String, port: UInt16) async throws -> any MeshConnection {
        guard let dialer = nodes[fromId] else {
            throw MeshError.dialFailed("dialing node not on network")
        }
        guard
            let target = nodes.values.first(where: {
                $0.hostname == host || $0.ip == host
                    || "\($0.hostname).loopback.ts.net" == host
            })
        else {
            throw MeshError.dialFailed("no such host: \(host)")
        }
        guard let listener = target.listeners[port] else {
            throw MeshError.dialFailed("connection refused: \(host):\(port)")
        }
        let (dialerEnd, acceptedEnd) = LoopbackConnection.makePair()
        let remotePort = nextPort
        nextPort += 1
        let remoteEndpoint = "\(dialer.ip):\(remotePort)"
        try await listener.deliver(
            MeshAcceptedConnection(connection: acceptedEnd, remoteEndpoint: remoteEndpoint))
        return dialerEnd
    }

    func whoIs(remoteEndpoint: String) -> AuthenticatedPeer {
        guard !withholdWhoIs else {
            return AuthenticatedPeer(tailscaleId: "", remoteAddresses: [remoteEndpoint])
        }
        let ip = remoteEndpoint.split(separator: ":").first.map(String.init) ?? ""
        let match = nodes.values.first { $0.ip == ip }
        return AuthenticatedPeer(
            tailscaleId: match?.tailscaleId ?? "",
            remoteAddresses: [remoteEndpoint],
            loginName: withholdWhoIsLogin ? nil : match?.loginName,
            displayName: withholdWhoIsLogin ? nil : match?.displayName)
    }
}

/// Listener half: a mailbox of accepted connections.
public actor LoopbackListener: MeshListener {
    private let accepted = Mailbox<MeshAcceptedConnection>()
    private let network: LoopbackNetwork
    private let owner: String
    private let port: UInt16

    init(network: LoopbackNetwork, owner: String, port: UInt16) {
        self.network = network
        self.owner = owner
        self.port = port
    }

    func deliver(_ connection: MeshAcceptedConnection) async throws {
        guard await accepted.put(connection) else {
            throw MeshError.dialFailed("listener closed")
        }
    }

    public func accept() async throws -> MeshAcceptedConnection {
        guard let next = await accepted.take() else {
            throw MeshError.listenFailed("listener closed")
        }
        return next
    }

    public func close() async {
        await accepted.finish()
        await network.unregisterListener(owner: owner, port: port)
    }

    func shutdown() async {
        await accepted.finish()
    }
}

/// Per-node backend over a shared `LoopbackNetwork`.
public actor LoopbackBackend: NetworkBackend {
    private let network: LoopbackNetwork
    private let tailscaleId: String
    private let ip: String
    private var started = false
    private var eventSubs: [UUID: AsyncStream<BackendEvent>.Continuation] = [:]

    init(network: LoopbackNetwork, tailscaleId: String, ip: String) {
        self.network = network
        self.tailscaleId = tailscaleId
        self.ip = ip
    }

    public func start() async throws {
        started = true
    }

    public func stop() async {
        started = false
        for (_, continuation) in eventSubs { continuation.finish() }
        eventSubs.removeAll()
        await network.leave(tailscaleId: tailscaleId)
    }

    public func status() async throws -> BackendStatus {
        guard started else { throw MeshError.notRunning }
        return await network.snapshot(for: tailscaleId)
    }

    public func events() async -> AsyncStream<BackendEvent> {
        let id = UUID()
        let (stream, continuation) = AsyncStream.makeStream(of: BackendEvent.self)
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removeEventSub(id) }
        }
        eventSubs[id] = continuation
        return stream
    }

    private func removeEventSub(_ id: UUID) {
        eventSubs[id] = nil
    }

    nonisolated func push(_ event: BackendEvent) {
        Task { await self.fanOut(event) }
    }

    private func fanOut(_ event: BackendEvent) {
        for (_, continuation) in eventSubs {
            continuation.yield(event)
        }
    }

    public func dial(host: String, port: UInt16) async throws -> any MeshConnection {
        guard started else { throw MeshError.notRunning }
        return try await network.dial(from: tailscaleId, host: host, port: port)
    }

    public func listen(port: UInt16) async throws -> any MeshListener {
        guard started else { throw MeshError.notRunning }
        let listener = LoopbackListener(network: network, owner: tailscaleId, port: port)
        try await network.registerListener(owner: tailscaleId, port: port, listener: listener)
        return listener
    }

    public func whoIs(remoteEndpoint: String) async throws -> AuthenticatedPeer {
        await network.whoIs(remoteEndpoint: remoteEndpoint)
    }

    public func makeURLSession(configuration: URLSessionConfiguration) async throws -> URLSession {
        throw MeshError.transport("LoopbackBackend does not provide URLSession")
    }
}
