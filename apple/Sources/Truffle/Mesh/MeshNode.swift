import Foundation

/// Session timing knobs — production values come from `SessionLimits`;
/// tests inject faster ones (heartbeat, handshake deadlines).
struct SessionTuning: Sendable {
    var handshakeTimeout: Duration = SessionLimits.handshakeTimeout
    var pingInterval: Duration = SessionLimits.pingInterval
    var pongTimeout: Duration = SessionLimits.pongTimeout
}

/// The Truffle mesh node (RFC 024 §6.2) — Apple runtime, product core.
///
/// Generic over a `NetworkBackend` (Layer 0–1) and a `FrameTransport`
/// (session framing). Production uses `TailscaleKitBackend` + the RFC 6455
/// adapter (Phase 0 device spike); tests use `LoopbackBackend` +
/// `LengthPrefixFrameTransport`.
public actor MeshNode {
    // MARK: configuration / collaborators

    private let config: MeshConfiguration
    private let backend: any NetworkBackend
    private let transport: any FrameTransport
    private let identityPolicy: Handshake.IdentityPolicy
    private let tuning: SessionTuning

    private let appId: AppId
    private let deviceName: DeviceName
    private let deviceId: DeviceId

    // MARK: state

    private struct RegistryEntry {
        var tailscaleId: String
        var generation: UInt64
        var hostname: String
        var tailnetIPs: [String]
        var online: Bool
        /// The owner's tailnet login as Layer 3 reported it (RFC 025 §3.3);
        /// `nil` for a provisional entry whose netmap row has not arrived.
        var loginName: String?
        /// Confirmed identity after a completed hello; nil for candidates.
        var identity: PeerIdentity?
        /// Created from an inbound hello that raced ahead of the netmap
        /// (RFC 024 §7.2). Preserved across full snapshots that lack the
        /// peer; Layer 3 metadata merges into the same generation.
        var provisional: Bool
    }

    private struct SessionState {
        /// Uniquely identifies THIS session record so a superseded
        /// session's cleanup can never remove its replacement
        /// (compare-and-remove).
        let token: UUID
        let frames: any SessionFrames
        let identity: PeerIdentity
        var pump: Task<Void, Never>?
        var heartbeat: Task<Void, Never>?
        var lastPong: ContinuousClock.Instant
    }

    /// Weak registry slot: the caller owns the subscription; dropping the
    /// handle deinits it, which is what makes auto-cancel work.
    private final class SubscriptionBox {
        weak var value: MessageSubscription?
        init(_ value: MessageSubscription) { self.value = value }
    }

    private struct EventSub {
        let continuation: AsyncStream<MeshEvent>.Continuation
        /// Set when a yield dropped; produces exactly one health notice
        /// once buffer space is available again (RFC 024 §6.2).
        var lagged = false
    }

    private var phaseValue: MeshPhase = .starting
    private var entries: [String: RegistryEntry] = [:]
    private var generationCounter: UInt64 = 0
    private var sessions: [String: SessionState] = [:]
    private var dialsInFlight: [String: Task<SessionState, Error>] = [:]
    private var subscriptions: [String: [(id: UUID, box: SubscriptionBox)]] = [:]
    private var eventSubs: [UUID: EventSub] = [:]
    private var supervisorTasks: [Task<Void, Never>] = []
    private var inboundTasks: [UUID: Task<Void, Never>] = [:]
    private var pendingHandshakes = 0
    private var listener: (any MeshListener)?
    private var lastAuthURL: URL?
    private var isStopped = false

    private var localTailscaleId = ""
    private var localDnsName: String?
    private var localIPs: [String] = []
    private var localLoginName: String?
    /// Set once a gated node has seen a Layer 3 snapshot with no self login,
    /// so the health notice is emitted once rather than per refresh.
    private var warnedAboutMissingLogins = false

    // MARK: - Lifecycle

    /// Production entry point (RFC 024 §6.2). Requires the TailscaleKit
    /// backend from the Phase 0 device spike; until it is wired up this
    /// throws. Tests and advanced callers use
    /// `start(_:backend:frameTransport:identityPolicy:)`.
    public static func start(_ config: MeshConfiguration) async throws -> MeshNode {
        throw MeshError.transport(
            "TailscaleKitBackend is not wired up yet (RFC 024 Phase 0); "
                + "use start(_:backend:frameTransport:) with an explicit backend")
    }

    /// Start a node over an explicit backend and frame transport.
    public static func start(
        _ config: MeshConfiguration,
        backend: any NetworkBackend,
        frameTransport: any FrameTransport,
        identityPolicy: Handshake.IdentityPolicy = .failClosed
    ) async throws -> MeshNode {
        try await start(
            config, backend: backend, frameTransport: frameTransport,
            identityPolicy: identityPolicy, tuning: SessionTuning())
    }

    /// Internal: as above with injectable session timing (tests).
    static func start(
        _ config: MeshConfiguration,
        backend: any NetworkBackend,
        frameTransport: any FrameTransport,
        identityPolicy: Handshake.IdentityPolicy,
        tuning: SessionTuning
    ) async throws -> MeshNode {
        let node = try MeshNode(
            config: config, backend: backend, transport: frameTransport,
            identityPolicy: identityPolicy, tuning: tuning)
        try await node.bootstrap()
        return node
    }

    private init(
        config: MeshConfiguration,
        backend: any NetworkBackend,
        transport: any FrameTransport,
        identityPolicy: Handshake.IdentityPolicy,
        tuning: SessionTuning
    ) throws {
        self.config = config
        self.backend = backend
        self.transport = transport
        self.identityPolicy = identityPolicy
        self.tuning = tuning
        self.appId = try AppId(parsing: config.appId)
        self.deviceName = DeviceName(config.deviceName)
        let stateDir =
            config.stateDirectory
            ?? FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
                .appendingPathComponent("truffle", isDirectory: true)
                .appendingPathComponent(config.appId, isDirectory: true)
        self.deviceId = try DeviceId.loadOrCreate(stateDirectory: stateDir)
    }

    /// Start contract steps (RFC 024 §6.2): backend up, initial status,
    /// event pump, listener supervision. Returns once watchers are live —
    /// does NOT wait for `.running` (use `waitUntilRunning`). If any step
    /// after `backend.start()` fails, the backend is stopped before the
    /// error propagates (no leaked embedded node).
    private func bootstrap() async throws {
        try await backend.start()
        do {
            let status = try await backend.status()
            await apply(status: status)

            if case .existingState = config.auth {
                // §6.2: fail fast instead of returning an unusable node.
                if status.needsLogin { throw MeshError.needsLogin }
                if status.needsMachineAuth { throw MeshError.needsMachineAuth }
            }

            let backendEvents = await backend.events()
            supervisorTasks.append(
                Task { [weak self] in
                    for await event in backendEvents {
                        guard let self else { return }
                        await self.handle(backendEvent: event)
                    }
                    // The backend event stream ended. During stop() that is
                    // expected; otherwise Layer 0–1 died under us.
                    await self?.backendEventsEnded()
                })

            supervisorTasks.append(
                Task { [weak self] in
                    guard let self else { return }
                    await self.superviseListener()
                })
        } catch {
            await backend.stop()
            throw error
        }
    }

    private func backendEventsEnded() {
        guard !isStopped else { return }
        setPhase(.failed)
        emit(.health("backend event stream ended unexpectedly"))
    }

    /// Idempotent shutdown (RFC 024 §6.2 step 9): stop accepting, cancel
    /// and AWAIT handshakes/supervisors/pumps, close sessions, then close
    /// the backend. Returns only after owned tasks have terminated.
    public func stop() async {
        guard !isStopped else { return }
        isStopped = true
        setPhase(.stopping)

        await listener?.close()
        listener = nil

        for task in supervisorTasks { task.cancel() }
        for (_, task) in inboundTasks { task.cancel() }
        for (_, session) in sessions {
            session.pump?.cancel()
            session.heartbeat?.cancel()
        }

        // Await termination (actor reentrancy lets the tasks' actor calls
        // interleave while we are suspended here).
        for task in supervisorTasks { await task.value }
        supervisorTasks.removeAll()
        for (_, task) in inboundTasks { await task.value }
        inboundTasks.removeAll()
        pendingHandshakes = 0

        for (_, session) in sessions {
            await session.frames.close(code: SessionCloseCode.normal, reason: "node stopping")
            await session.pump?.value
            await session.heartbeat?.value
        }
        sessions.removeAll()
        dialsInFlight.removeAll()

        for (_, subs) in subscriptions {
            for (_, box) in subs {
                if let sub = box.value { await sub.cancel() }
            }
        }
        subscriptions.removeAll()

        await backend.stop()
        setPhase(.stopped)
        for (_, sub) in eventSubs { sub.continuation.finish() }
        eventSubs.removeAll()
    }

    /// Re-poll backend status (e.g. after an interactive login sheet is
    /// dismissed). Clears auth-URL deduplication for a new attempt.
    public func refresh() async throws {
        guard !isStopped else { throw MeshError.stopped }
        lastAuthURL = nil
        let status = try await backend.status()
        await apply(status: status)
    }

    /// Block until the node reaches `.running` (RFC 024 §6.2 step 7).
    public func waitUntilRunning(timeout: Duration) async throws {
        try await withDeadline(timeout, label: "waitUntilRunning") { [weak self] in
            while true {
                guard let self else { throw MeshError.stopped }
                if await self.phase == .running { return }
                if await self.isStoppedNow { throw MeshError.stopped }
                try await Task.sleep(for: .milliseconds(25))
            }
        }
    }

    // MARK: - Introspection

    public var phase: MeshPhase { phaseValue }
    private var isStoppedNow: Bool { isStopped }

    public var dnsName: String? { localDnsName }
    public var tailnetIPs: [String] { localIPs }

    /// The tailnet login this node is signed in as (RFC 025 §3.6, D7), from
    /// the last Layer 3 status. `nil` until known — never fabricated.
    public var loginName: String? { localLoginName }

    /// The login allow-list this node was started with (RFC 025 §3.1).
    /// Empty means ungated. Fixed for the node's lifetime.
    public var loginAllow: [String] { config.loginAllow }

    public var localPeer: Peer {
        Peer(
            ref: PeerRef(tailscaleId: localTailscaleId, generation: 0),
            deviceId: deviceId.value,
            tailscaleId: localTailscaleId,
            generation: 0,
            displayName: deviceName.value,
            hostname: Hostname.tailscaleHostname(appId: appId, deviceName: deviceName),
            tailnetIPs: localIPs,
            online: phaseValue == .running,
            appId: appId.value,
            isLocal: true,
            loginName: localLoginName)
    }

    /// Each access mints a NEW independently-buffered stream (RFC 024 §6.2):
    /// bounded newest-value buffer of 256; the current phase is emitted
    /// immediately; no history replay.
    public var events: AsyncStream<MeshEvent> {
        let id = UUID()
        let (stream, continuation) = AsyncStream.makeStream(
            of: MeshEvent.self, bufferingPolicy: .bufferingNewest(256))
        continuation.onTermination = { [weak self] _ in
            Task { await self?.removeEventSub(id) }
        }
        eventSubs[id] = EventSub(continuation: continuation)
        continuation.yield(.phase(phaseValue))
        return stream
    }

    private func removeEventSub(_ id: UUID) {
        eventSubs[id] = nil
    }

    private func emit(_ event: MeshEvent) {
        for (id, sub) in eventSubs {
            // One lag notice per lag episode, sent once space is available
            // again (RFC 024 §6.2) — never a notice per dropped event.
            if sub.lagged {
                if case .enqueued = sub.continuation.yield(
                    .health("event stream lagged; resync"))
                {
                    eventSubs[id]?.lagged = false
                } else {
                    continue  // still full; the event below would drop too
                }
            }
            if case .dropped = sub.continuation.yield(event) {
                eventSubs[id]?.lagged = true
            }
        }
    }

    private func setPhase(_ phase: MeshPhase) {
        guard phaseValue != phase else { return }
        phaseValue = phase
        emit(.phase(phase))
    }

    // MARK: - Peers (RFC 024 §6.3)

    public func peers() -> [Peer] {
        entries.values.map(makePeer(from:))
    }

    /// Resolve a peer query (RFC 024 §6.3 resolution order). Not found or
    /// wait timeout → nil; ambiguity, stale refs, and shutdown throw.
    public func peer(_ query: String, waitMs: Int = 0) async throws -> Peer? {
        let deadline = ContinuousClock.now.advanced(by: .milliseconds(waitMs))
        while true {
            if isStopped { throw MeshError.stopped }
            if let found = try resolveQuery(query) { return found }
            if ContinuousClock.now >= deadline { break }
            try await Task.sleep(for: .milliseconds(50))
        }
        // Well-formed ref that resolved nothing → departed generation
        // (live refs exact-matched above). RFC 022 I5: fail loudly.
        if PeerRef.parse(query) != nil, entries[query] == nil {
            throw MeshError.peerGone(query)
        }
        return nil
    }

    private func resolveQuery(_ query: String) throws -> Peer? {
        // 1. Exact process-local PeerRef.
        if let hit = entries.values.first(where: {
            PeerRef(tailscaleId: $0.tailscaleId, generation: $0.generation).description == query
        }) {
            return makePeer(from: hit)
        }
        // 2. Exact deviceId (confirmed identities only).
        if let hit = entries.values.first(where: { $0.identity?.deviceId == query }) {
            return makePeer(from: hit)
        }
        // 3. Exact tailscaleId.
        if let hit = entries[query] {
            return makePeer(from: hit)
        }
        // 4. Exact hostname (case-sensitive) or device name
        //    (case-insensitive) — may be ambiguous.
        let nameHits = entries.values.filter { entry in
            entry.hostname == query
                || entry.identity?.deviceName.lowercased() == query.lowercased()
        }
        if nameHits.count > 1 {
            throw MeshError.peerAmbiguous(
                query: query,
                candidates: nameHits.map { $0.identity?.deviceId ?? $0.tailscaleId })
        }
        if let hit = nameHits.first {
            return makePeer(from: hit)
        }
        // 5. Unique deviceId prefix (≥4 chars).
        if query.count >= 4 {
            let prefixHits = entries.values.filter {
                $0.identity?.deviceId.hasPrefix(query) == true
            }
            if prefixHits.count > 1 {
                throw MeshError.peerAmbiguous(
                    query: query,
                    candidates: prefixHits.compactMap { $0.identity?.deviceId })
            }
            if let hit = prefixHits.first {
                return makePeer(from: hit)
            }
        }
        // 6. Tailnet IP.
        if let hit = entries.values.first(where: { $0.tailnetIPs.contains(query) }) {
            return makePeer(from: hit)
        }
        return nil
    }

    private func makePeer(from entry: RegistryEntry) -> Peer {
        Peer(
            ref: PeerRef(tailscaleId: entry.tailscaleId, generation: entry.generation),
            deviceId: entry.identity?.deviceId,
            tailscaleId: entry.tailscaleId,
            generation: entry.generation,
            displayName: entry.identity?.deviceName ?? entry.hostname,
            hostname: entry.hostname,
            tailnetIPs: entry.tailnetIPs,
            online: entry.online,
            appId: entry.identity != nil ? appId.value : nil,
            isLocal: false,
            loginName: entry.loginName)
    }

    /// Generation-checked live lookup for peer-taking calls (RFC 024 §6.3).
    private func resolveLive(_ peer: Peer) throws -> RegistryEntry {
        guard !isStopped else { throw MeshError.stopped }
        guard let entry = entries[peer.tailscaleId], entry.generation == peer.generation
        else {
            throw MeshError.peerGone(peer.ref.description)
        }
        return entry
    }

    // MARK: - Messaging (RFC 024 §6.4)

    /// Convenience alias for `sendBytes`; never content-sniffs the data.
    public func send(to peer: Peer, namespace: String, data: Data) async throws {
        try await sendBytes(to: peer, namespace: namespace, data: data)
    }

    public func sendBytes(to peer: Peer, namespace: String, data: Data) async throws {
        let envelope = try Envelope.bytes(namespace: namespace, data: data)
        try await sendEnvelope(envelope, to: peer)
    }

    public func sendJSON<T: Encodable>(
        to peer: Peer, namespace: String, msgType: String = "message", payload: T
    ) async throws {
        let envelope = Envelope(
            namespace: namespace, msgType: msgType,
            payload: try JSONValue.encoding(payload),
            timestamp: Envelope.nowMillis())
        try await sendEnvelope(envelope, to: peer)
    }

    private func sendEnvelope(_ envelope: Envelope, to peer: Peer) async throws {
        let entry = try resolveLive(peer)
        let data = try EnvelopeCodec.encode(envelope)
        let session = try await session(for: entry)
        // Envelopes travel as Binary frames (RFC 024 §8.1).
        try await session.frames.send(.binary(data))
    }

    public func onMessage(
        namespace: String,
        handler: @escaping @Sendable (MeshMessage) async -> Void
    ) -> MessageSubscription {
        let id = UUID()
        let subscription = MessageSubscription(handler: handler) { [weak self] in
            Task { await self?.removeSubscription(id: id, namespace: namespace) }
        }
        // Held weakly: dropping the caller's handle deinits the
        // subscription, which auto-cancels it (RFC 024 §6.4).
        subscriptions[namespace, default: []].append(
            (id: id, box: SubscriptionBox(subscription)))
        return subscription
    }

    private func removeSubscription(id: UUID, namespace: String) {
        subscriptions[namespace]?.removeAll { $0.id == id }
    }

    /// Completes the authenticated Truffle hello without sending an
    /// application message. The supplied peer snapshot is generation-checked
    /// before dialing, and the returned snapshot contains the durable device
    /// ID learned from the handshake.
    public func confirmIdentity(of peer: Peer) async throws -> Peer {
        let entry = try resolveLive(peer)
        _ = try await session(for: entry)
        guard let confirmed = entries[entry.tailscaleId],
            confirmed.generation == entry.generation,
            confirmed.identity != nil
        else {
            throw MeshError.protocolViolation("peer identity handshake completed without identity")
        }
        return makePeer(from: confirmed)
    }

    // MARK: - Raw transport (RFC 024 §6.6 — Phase 1b surface)

    public func dial(to peer: Peer, port: UInt16) async throws -> any MeshConnection {
        let entry = try resolveLive(peer)
        return try await backend.dial(host: dialHost(for: entry), port: port)
    }

    /// WhoIs for an address this node accepted on the RAW plane
    /// (RFC 025 §3.7, D9). `listen(port:)` is NOT gated by `loginAllow` — the
    /// node's list admits peers and session-plane hellos, and the app owns
    /// admission on its own port. This is how an app gates an accepted
    /// connection: resolve `MeshAcceptedConnection.remoteEndpoint`, then
    /// decide on `loginName` (with `LoginGlob.allowed` if it wants the same
    /// grammar). Throws when the lookup fails; an empty `tailscaleId` means
    /// WhoIs produced no concrete identity and must be treated as untrusted.
    public func whoIs(remoteEndpoint: String) async throws -> AuthenticatedPeer {
        guard !isStopped else { throw MeshError.stopped }
        return try await backend.whoIs(remoteEndpoint: remoteEndpoint)
    }

    public func listen(port: UInt16) async throws -> any MeshListener {
        guard !isStopped else { throw MeshError.stopped }
        guard port != SessionLimits.sessionPort else {
            throw MeshError.listenFailed(
                "port \(SessionLimits.sessionPort) is reserved (the session WebSocket port)")
        }
        return try await backend.listen(port: port)
    }

    // MARK: - HTTP (RFC 024 §6.5)

    public func urlSession(
        configuration: URLSessionConfiguration = .default
    ) async throws -> URLSession {
        guard !isStopped else { throw MeshError.stopped }
        return try await backend.makeURLSession(configuration: configuration)
    }

    public func httpGet(_ url: URL) async throws -> (Data, URLResponse) {
        let session = try await urlSession()
        return try await session.data(from: url)
    }

    // MARK: - Backend event handling

    private func handle(backendEvent event: BackendEvent) async {
        guard !isStopped else { return }
        switch event {
        case .status(let status):
            await apply(status: status)
        case .peerUpsert(let peer):
            if upsertFromLayer3(peer) {
                await removeEntry(tailscaleId: peer.tailscaleId)
            }
        case .peerLeft(let tailscaleId):
            await removeEntry(tailscaleId: tailscaleId)
        case .authRequired(let url):
            await handleAuthRequired(url)
        case .health(let message):
            emit(.health(message))
        }
    }

    private func apply(status: BackendStatus) async {
        localTailscaleId = status.tailscaleId
        localDnsName = status.dnsName
        localIPs = status.tailnetIPs
        localLoginName = status.loginName

        if status.running {
            setPhase(.running)
        } else if status.needsMachineAuth {
            setPhase(.needsMachineAuth)
        } else if status.needsLogin {
            setPhase(.needsLogin)
        } else if phaseValue == .running {
            // Backend lost its state (disconnected / restarting): a node
            // must not remain `.running` on a stale snapshot.
            setPhase(.starting)
        }

        // An auth URL present only in this snapshot (e.g. the initial
        // status read before any event fires) must still reach the host.
        if let raw = status.authURL, let url = URL(string: raw) {
            await handleAuthRequired(url)
        }

        var seen = Set<String>()
        for peer in status.peers {
            seen.insert(peer.tailscaleId)
            if upsertFromLayer3(peer) {
                await removeEntry(tailscaleId: peer.tailscaleId)
            }
        }
        // RFC 025 §3.3: a gated node cannot admit a peer whose login Layer 3
        // never reported. Say so — once — rather than presenting an
        // unexplained empty mesh (the Rust provider refuses to start; a Swift
        // node has no sidecar to interrogate, so it reports honestly).
        if !config.loginAllow.isEmpty, !warnedAboutMissingLogins,
            status.peers.contains(where: {
                $0.loginName == nil
                    && Hostname.isAppPeer(hostname: $0.hostname, appId: appId.value)
            })
        {
            warnedAboutMissingLogins = true
            emit(
                .health(
                    "login gate active but Layer 3 reported an app peer with no login; "
                        + "peers without a login are not admitted"))
        }
        // Entries absent from a full snapshot have left Layer 3 — EXCEPT
        // provisional entries, whose netmap event hasn't arrived yet
        // (RFC 024 §7.2: preserve, then merge).
        for (tailscaleId, entry) in entries
        where !seen.contains(tailscaleId) && !entry.provisional {
            await removeEntry(tailscaleId: tailscaleId)
        }
    }

    /// Candidate filtering (RFC 024 §7.2, RFC 025 §3.3): a hostname matching
    /// `truffle-{appId}-{slug}` AND — on a login-gated node — a login on the
    /// allow-list. A gated node treats a row WITHOUT a login as not a peer
    /// (fail closed).
    ///
    /// The gate runs on entry CREATION and on every later row for an entry
    /// that already exists. A stable node ID survives a device transfer, so
    /// the netmap reports a re-signed node as an UPDATE, not a new row: if the
    /// gate ran only at creation, a peer admitted as `bob@corp.com` would keep
    /// its place after re-signing as a foreign login. A row whose login the
    /// gate REFUSES is therefore a departure — the caller evicts the entry,
    /// which closes its session and emits `peerLeft`.
    ///
    /// An ABSENT login on an existing row is sticky instead: a transient
    /// overlay failure must not empty a gated mesh. Provisional entries from a
    /// raced inbound hello keep merging — that hello passed the gate in
    /// `Handshake.server` — but they are evicted on a refused login like any
    /// other row.
    ///
    /// - Returns: `true` when this row must be evicted. Eviction is async and
    ///   this is not, so the caller performs it.
    private func upsertFromLayer3(_ peer: BackendPeer) -> Bool {
        if var existing = entries[peer.tailscaleId] {
            if let login = peer.loginName,
                !LoginGlob.allowed(config.loginAllow, login: login)
            {
                return true
            }
            existing.hostname = peer.hostname
            existing.tailnetIPs = peer.tailnetIPs
            existing.online = peer.online
            existing.loginName = peer.loginName ?? existing.loginName
            existing.provisional = false
            entries[peer.tailscaleId] = existing
            emit(.peerUpsert(makePeer(from: existing)))
            return false
        }
        guard Hostname.isAppPeer(hostname: peer.hostname, appId: appId.value) else {
            return false
        }
        guard LoginGlob.allowed(config.loginAllow, login: peer.loginName) else {
            return false
        }
        generationCounter += 1
        let entry = RegistryEntry(
            tailscaleId: peer.tailscaleId,
            generation: generationCounter,
            hostname: peer.hostname,
            tailnetIPs: peer.tailnetIPs,
            online: peer.online,
            loginName: peer.loginName,
            identity: nil,
            provisional: false)
        entries[peer.tailscaleId] = entry
        emit(.peerUpsert(makePeer(from: entry)))
        return false
    }

    private func removeEntry(tailscaleId: String) async {
        guard let entry = entries.removeValue(forKey: tailscaleId) else { return }
        if let session = sessions.removeValue(forKey: tailscaleId) {
            session.pump?.cancel()
            session.heartbeat?.cancel()
            await session.frames.close(code: SessionCloseCode.normal, reason: "peer left")
        }
        emit(.peerLeft(makePeer(from: entry)))
    }

    private func handleAuthRequired(_ url: URL) async {
        setPhase(.needsLogin)
        // Deduplicate the same URL until it changes or refresh() begins a
        // new attempt (RFC 024 §6.2 step 5). Emit the event first, then
        // invoke the interactive callback on the main actor.
        guard url != lastAuthURL else { return }
        lastAuthURL = url
        emit(.authRequired(url))
        if case .interactive(let onAuthRequired) = config.auth {
            await onAuthRequired(url)
        }
    }

    // MARK: - Session management (dial side)

    private func dialHost(for entry: RegistryEntry) -> String {
        entry.tailnetIPs.first ?? entry.hostname
    }

    /// Get or create the session for a peer. Concurrent callers share one
    /// in-flight dial (no duplicate sessions across actor reentrancy).
    private func session(for entry: RegistryEntry) async throws -> SessionState {
        if let existing = sessions[entry.tailscaleId] { return existing }
        if let inFlight = dialsInFlight[entry.tailscaleId] {
            return try await inFlight.value
        }
        let tailscaleId = entry.tailscaleId
        let host = dialHost(for: entry)
        let dial = Task { [weak self] () throws -> SessionState in
            guard let self else { throw MeshError.stopped }
            return try await self.establishSession(tailscaleId: tailscaleId, host: host)
        }
        dialsInFlight[tailscaleId] = dial
        defer { dialsInFlight[tailscaleId] = nil }
        return try await dial.value
    }

    private func establishSession(
        tailscaleId: String, host: String
    ) async throws -> SessionState {
        let connection = try await backend.dial(host: host, port: SessionLimits.sessionPort)
        let frames = try await transport.clientFrames(over: connection)
        let identity: PeerIdentity
        do {
            identity = try await withDeadline(
                tuning.handshakeTimeout, label: "session handshake"
            ) { [localHello] in
                try await Handshake.client(
                    frames: frames,
                    localHello: localHello,
                    expectedTailscaleId: tailscaleId)
            }
        } catch {
            // A refusal (4001–4004) already closed the socket from the far
            // end; echoing a close would be noise. Anything else gets one.
            if !Handshake.isRefusal(error) {
                await frames.close(
                    code: SessionCloseCode.helloProtocol, reason: "handshake failed")
            }
            throw error
        }

        confirm(identity: identity, tailscaleId: tailscaleId)
        // An inbound session may have crossed our dial and registered
        // meanwhile: keep it and drop ours.
        if let existing = sessions[tailscaleId] {
            await frames.close(code: SessionCloseCode.normal, reason: "duplicate session")
            return existing
        }
        return register(frames: frames, identity: identity, tailscaleId: tailscaleId)
    }

    private var localHello: HelloEnvelope {
        HelloEnvelope(
            identity: PeerIdentity(
                appId: appId.value,
                deviceId: deviceId.value,
                deviceName: deviceName.value,
                os: Self.currentOS,
                tailscaleId: localTailscaleId))
    }

    static var currentOS: String {
        #if os(iOS)
            return "ios"
        #else
            return "darwin"
        #endif
    }

    /// Record a completed hello: confirm an existing candidate, or create a
    /// provisional entry when the hello raced ahead of the netmap
    /// (RFC 024 §7.2).
    private func confirm(identity: PeerIdentity, tailscaleId: String, loginName: String? = nil) {
        if var entry = entries[tailscaleId] {
            entry.identity = identity
            // Never overwrite a known login with nothing.
            if let loginName { entry.loginName = loginName }
            entries[tailscaleId] = entry
            emit(.peerUpsert(makePeer(from: entry)))
        } else {
            generationCounter += 1
            let entry = RegistryEntry(
                tailscaleId: tailscaleId,
                generation: generationCounter,
                hostname: "",
                tailnetIPs: [],
                online: true,
                // On a gated node this login is the one that PASSED the hello
                // gate, so the provisional row is honest about who it admitted
                // instead of waiting for the netmap to say.
                loginName: loginName,
                identity: identity,
                provisional: true)
            entries[tailscaleId] = entry
            emit(.peerUpsert(makePeer(from: entry)))
        }
    }

    private func register(
        frames: any SessionFrames, identity: PeerIdentity, tailscaleId: String
    ) -> SessionState {
        let token = UUID()
        var state = SessionState(
            token: token, frames: frames, identity: identity,
            pump: nil, heartbeat: nil, lastPong: ContinuousClock.now)
        state.pump = Task { [weak self] in
            guard let self else { return }
            await self.pumpSession(tailscaleId: tailscaleId, token: token, frames: frames)
        }
        state.heartbeat = Task { [weak self] in
            guard let self else { return }
            await self.heartbeatLoop(tailscaleId: tailscaleId, token: token, frames: frames)
        }
        sessions[tailscaleId] = state
        return state
    }

    /// Compare-and-remove: only the session that owns `token` is removed —
    /// a superseded session's cleanup can never evict its replacement.
    private func endSession(tailscaleId: String, token: UUID) {
        guard let current = sessions[tailscaleId], current.token == token else { return }
        current.pump?.cancel()
        current.heartbeat?.cancel()
        sessions[tailscaleId] = nil
    }

    private func notePong(tailscaleId: String, token: UUID) {
        guard sessions[tailscaleId]?.token == token else { return }
        sessions[tailscaleId]?.lastPong = ContinuousClock.now
    }

    // MARK: - Keepalive (RFC 024 §8.1 step 8)

    /// Send Ping every `pingInterval`; if no Pong within `pongTimeout`,
    /// close and end the session — the shipped desktop heartbeat contract.
    private func heartbeatLoop(
        tailscaleId: String, token: UUID, frames: any SessionFrames
    ) async {
        while !Task.isCancelled {
            try? await Task.sleep(for: tuning.pingInterval)
            if Task.isCancelled || isStopped { return }
            guard let session = sessions[tailscaleId], session.token == token else { return }
            if session.lastPong.duration(to: ContinuousClock.now) > tuning.pongTimeout {
                emit(.health("session heartbeat timeout (\(tailscaleId)); closing"))
                await frames.close(
                    code: SessionCloseCode.normal, reason: "heartbeat timeout")
                endSession(tailscaleId: tailscaleId, token: token)
                return
            }
            do {
                try await frames.send(.ping(Data()))
            } catch {
                endSession(tailscaleId: tailscaleId, token: token)
                return
            }
        }
    }

    // MARK: - Session management (accept side)

    private func superviseListener() async {
        var backoffMs = 100
        while !Task.isCancelled, !isStopped {
            do {
                let listener = try await backend.listen(port: SessionLimits.sessionPort)
                self.listener = listener
                backoffMs = 100
                await acceptLoop(listener)
                // `acceptLoop` returning means this listener can no longer
                // accept. Close it before asking the backend to bind the same
                // port again; embedded tsnet keeps the address registered
                // until the listener descriptor is closed.
                await listener.close()
                self.listener = nil
            } catch {
                emit(.health("listener error: \(error); retrying"))
            }
            guard !Task.isCancelled, !isStopped else { return }
            // RFC 024 §6.2 step 8: recreate with bounded backoff; never
            // silently end inbound messaging.
            try? await Task.sleep(for: .milliseconds(backoffMs))
            backoffMs = min(backoffMs * 2, 5_000)
        }
    }

    private func acceptLoop(_ listener: any MeshListener) async {
        while !Task.isCancelled, !isStopped {
            do {
                let accepted = try await listener.accept()
                // RFC 024 §8.1: at most `maxPendingHandshakes` concurrent
                // upgrade/hello exchanges; excess connections are dropped
                // before the upgrade.
                guard pendingHandshakes < SessionLimits.maxPendingHandshakes else {
                    emit(.health("handshake capacity reached; dropping incoming connection"))
                    await accepted.connection.close()
                    continue
                }
                pendingHandshakes += 1
                let taskId = UUID()
                let task = Task { [weak self] in
                    guard let self else { return }
                    await self.handleInbound(accepted)
                    await self.inboundFinished(taskId)
                }
                inboundTasks[taskId] = task
            } catch {
                if !isStopped {
                    emit(.health("accept error: \(error); recreating listener"))
                }
                return
            }
        }
    }

    private func inboundFinished(_ taskId: UUID) {
        pendingHandshakes -= 1
        inboundTasks[taskId] = nil
    }

    private func handleInbound(_ accepted: MeshAcceptedConnection) async {
        do {
            // The FULL exchange — upgrade, WhoIs, hello — is bounded by the
            // handshake deadline (RFC 024 §8.1 step 4), not just the hello.
            let identity = try await withDeadline(
                tuning.handshakeTimeout, label: "inbound handshake"
            ) { [transport, backend, localHello, identityPolicy, loginAllow = config.loginAllow] in
                let frames = try await transport.serverFrames(over: accepted.connection)
                let authenticated = try? await backend.whoIs(
                    remoteEndpoint: accepted.remoteEndpoint)
                let identity = try await Handshake.server(
                    frames: frames,
                    localHello: localHello,
                    authenticated: authenticated,
                    policy: identityPolicy,
                    loginAllow: loginAllow)
                return InboundHandshake(
                    frames: frames, identity: identity,
                    loginName: authenticated?.loginName)
            }
            await adoptInbound(identity)
        } catch {
            emit(.health("inbound handshake rejected: \(error)"))
            await accepted.connection.close()
        }
    }

    private struct InboundHandshake: Sendable {
        let frames: any SessionFrames
        let identity: PeerIdentity
        /// The WhoIs login this caller passed the gate with, if any.
        let loginName: String?
    }

    private func adoptInbound(_ handshake: InboundHandshake) async {
        confirm(
            identity: handshake.identity, tailscaleId: handshake.identity.tailscaleId,
            loginName: handshake.loginName)
        if let previous = sessions.removeValue(forKey: handshake.identity.tailscaleId) {
            previous.pump?.cancel()
            previous.heartbeat?.cancel()
            await previous.frames.close(code: SessionCloseCode.normal, reason: "superseded")
        }
        _ = register(
            frames: handshake.frames,
            identity: handshake.identity,
            tailscaleId: handshake.identity.tailscaleId)
    }

    // MARK: - Message pump

    private func pumpSession(
        tailscaleId: String, token: UUID, frames: any SessionFrames
    ) async {
        defer {
            Task { [weak self] in
                await self?.endSession(tailscaleId: tailscaleId, token: token)
            }
        }
        while !Task.isCancelled {
            let frame: SessionFrame?
            do {
                frame = try await frames.receive()
            } catch {
                emit(.health("session receive error (\(tailscaleId)): \(error)"))
                return
            }
            guard let frame else { return }
            switch frame {
            case .binary(let data):
                await deliver(data: data, from: tailscaleId)
            case .text(let text):
                await deliver(data: Data(text.utf8), from: tailscaleId)
            case .ping(let payload):
                try? await frames.send(.pong(payload))
            case .pong:
                notePong(tailscaleId: tailscaleId, token: token)
            case .close:
                return
            }
        }
    }

    private func deliver(data: Data, from tailscaleId: String) async {
        guard let entry = entries[tailscaleId] else { return }
        let envelope: Envelope
        do {
            envelope = try EnvelopeCodec.decode(data)
        } catch {
            emit(.health("dropped malformed envelope from \(tailscaleId): \(error)"))
            return
        }
        let sender = makePeer(from: entry)
        let message: MeshMessage
        do {
            // Attribution comes from the authenticated session, never from
            // the wire `from` field (RFC 024 §8.3).
            message = try MeshMessage(
                from: sender,
                namespace: envelope.namespace,
                msgType: envelope.msgType,
                payloadValue: envelope.payload,
                timestamp: envelope.timestamp.map {
                    Date(timeIntervalSince1970: Double($0) / 1000)
                })
        } catch {
            emit(.health("dropped undecodable payload from \(tailscaleId): \(error)"))
            return
        }

        if var subs = subscriptions[envelope.namespace] {
            var pruned = false
            for (index, item) in subs.enumerated().reversed() {
                guard let sub = item.box.value else {
                    subs.remove(at: index)  // handle released → auto-cancelled
                    pruned = true
                    continue
                }
                let accepted = await sub.enqueue(message)
                if !accepted {
                    emit(
                        .health(
                            "subscription on '\(envelope.namespace)' overflowed (\(MessageSubscription.maxQueue) queued); cancelled"
                        ))
                }
            }
            if pruned {
                subscriptions[envelope.namespace] = subs
            }
        }
        emit(.message(message))
    }
}
