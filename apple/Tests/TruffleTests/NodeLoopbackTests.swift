import Foundation
import Testing

@testable import Truffle

/// A frame transport whose endpoints silently drop outgoing Pong frames —
/// simulates a peer that stops answering keepalives (heartbeat tests).
struct PongDroppingTransport: FrameTransport {
    struct DroppingFrames: SessionFrames {
        let inner: any SessionFrames

        func send(_ frame: SessionFrame) async throws {
            if case .pong = frame { return }
            try await inner.send(frame)
        }

        func receive() async throws -> SessionFrame? {
            try await inner.receive()
        }

        func close(code: UInt16, reason: String) async {
            await inner.close(code: code, reason: reason)
        }
    }

    private let base = LengthPrefixFrameTransport()

    func clientFrames(over connection: any MeshConnection) async throws -> any SessionFrames {
        DroppingFrames(inner: try await base.clientFrames(over: connection))
    }

    func serverFrames(over connection: any MeshConnection) async throws -> any SessionFrames {
        DroppingFrames(inner: try await base.serverFrames(over: connection))
    }
}

/// End-to-end tests: two `MeshNode`s over a shared `LoopbackNetwork`
/// (RFC 024 §13 — mock-provider integration level).
@Suite struct NodeLoopbackTests {
    struct ChatPayload: Codable, Equatable {
        var text: String
    }

    private func tempDir() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("truffle-node-test-\(UUID().uuidString)")
    }

    /// Start a node named `deviceName` for `appId` on `network`, advertising
    /// the RFC 017 hostname derived from its own identity — unless
    /// `advertisedHostname` overrides it (lying-hostname tests).
    private func startNode(
        network: LoopbackNetwork,
        tailscaleId: String,
        appId: String,
        deviceName: String,
        advertisedHostname: String? = nil,
        hidden: Bool = false,
        identityPolicy: Handshake.IdentityPolicy = .failClosed,
        loginName: String? = nil,
        loginAllow: [String] = []
    ) async throws -> (MeshNode, URL) {
        let derived = Hostname.tailscaleHostname(
            appId: try AppId(parsing: appId), deviceName: DeviceName(deviceName))
        let hostname = advertisedHostname ?? derived
        let backend = await network.join(
            tailscaleId: tailscaleId, hostname: hostname, hidden: hidden,
            loginName: loginName)
        let dir = tempDir()
        let node = try await MeshNode.start(
            MeshConfiguration(
                appId: appId, deviceName: deviceName, stateDirectory: dir,
                auth: .existingState, loginAllow: loginAllow),
            backend: backend,
            frameTransport: LengthPrefixFrameTransport(),
            identityPolicy: identityPolicy)
        return (node, dir)
    }

    @Test func confirmsIdentityWithoutApplicationTraffic() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", appId: "demo", deviceName: "Alice")
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", appId: "demo", deviceName: "Bob")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        guard let candidate = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("alice never discovered bob")
            return
        }
        #expect(candidate.deviceId == nil)

        let confirmed = try await alice.confirmIdentity(of: candidate)
        #expect(confirmed.ref == candidate.ref)
        #expect(confirmed.generation == candidate.generation)
        #expect(confirmed.deviceId != nil)

        await alice.stop()
        await bob.stop()
    }

    @Test func exchangesJSONBothDirectionsWithAttribution() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", appId: "demo", deviceName: "Alice")
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", appId: "demo", deviceName: "Bob")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        try await alice.waitUntilRunning(timeout: .seconds(2))
        try await bob.waitUntilRunning(timeout: .seconds(2))

        let bobInbox = Mailbox<MeshMessage>()
        let aliceInbox = Mailbox<MeshMessage>()
        let subBob = await bob.onMessage(namespace: "chat") { message in
            await bobInbox.put(message)
        }
        let subAlice = await alice.onMessage(namespace: "chat") { message in
            await aliceInbox.put(message)
        }

        // Alice discovers Bob as a pre-hello candidate: deviceId nil.
        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("alice never discovered bob")
            return
        }
        #expect(bobPeer.deviceId == nil)

        // Alice → Bob (dial direction 1).
        try await alice.sendJSON(
            to: bobPeer, namespace: "chat", payload: ChatPayload(text: "hi bob"))
        guard let atBob = await bobInbox.take() else {
            Issue.record("bob received nothing")
            return
        }
        #expect(try atBob.decodePayload(ChatPayload.self) == ChatPayload(text: "hi bob"))
        #expect(atBob.namespace == "chat")
        #expect(atBob.msgType == "message")
        // Attribution from the authenticated session, incl. Alice's ULID.
        #expect(atBob.from.tailscaleId == "ts-a")
        #expect(atBob.from.deviceId != nil)
        #expect(atBob.timestamp != nil)

        // Bob → Alice (reuses the established session; direction 2).
        guard let alicePeer = try await bob.peer("ts-a", waitMs: 2_000) else {
            Issue.record("bob never discovered alice")
            return
        }
        try await bob.sendJSON(
            to: alicePeer, namespace: "chat", payload: ChatPayload(text: "hi alice"))
        guard let atAlice = await aliceInbox.take() else {
            Issue.record("alice received nothing")
            return
        }
        #expect(try atAlice.decodePayload(ChatPayload.self) == ChatPayload(text: "hi alice"))
        #expect(atAlice.from.tailscaleId == "ts-b")

        // After hello, Bob's snapshot for Alice carries her real ULID.
        let confirmed = try await alice.peer("ts-b")
        #expect(confirmed?.deviceId != nil)
        #expect(confirmed?.appId == "demo")

        await subBob.cancel()
        await subAlice.cancel()
        await alice.stop()
        await bob.stop()
    }

    @Test func exchangesOpaqueBytes() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", appId: "demo", deviceName: "Alice")
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", appId: "demo", deviceName: "Bob")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        let inbox = Mailbox<MeshMessage>()
        let sub = await bob.onMessage(namespace: "ft") { await inbox.put($0) }

        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("no peer")
            return
        }
        let blob = Data((0..<1024).map { UInt8($0 % 251) })
        try await alice.send(to: bobPeer, namespace: "ft", data: blob)

        guard let received = await inbox.take() else {
            Issue.record("no message")
            return
        }
        #expect(received.msgType == "bytes")
        #expect(try received.payloadBytes() == blob)

        await sub.cancel()
        await alice.stop()
        await bob.stop()
    }

    @Test func appIdMismatchNeverConfirms() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", appId: "app-one", deviceName: "Alice")
        // Mallory advertises app-one's hostname prefix but actually runs
        // app-two: a candidate that must never survive the hello.
        let (mallory, dirB) = try await startNode(
            network: network, tailscaleId: "ts-m", appId: "app-two", deviceName: "Mallory",
            advertisedHostname: "truffle-app-one-mallory")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        guard let candidate = try await alice.peer("ts-m", waitMs: 2_000) else {
            Issue.record("candidate not visible")
            return
        }
        #expect(candidate.deviceId == nil)

        // The hello closes with 4001; the send fails; Mallory never confirms.
        await #expect(throws: MeshError.self) {
            try await alice.sendJSON(
                to: candidate, namespace: "chat", payload: ChatPayload(text: "?"))
        }
        let after = try await alice.peer("ts-m")
        #expect(after?.deviceId == nil)

        await alice.stop()
        await mallory.stop()
    }

    @Test func staleRefThrowsPeerGoneEvenAfterRejoin() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", appId: "demo", deviceName: "Alice")
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", appId: "demo", deviceName: "Bob")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        guard let staleBob = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("no peer")
            return
        }

        // Bob leaves...
        await bob.stop()
        while try await alice.peer("ts-b") != nil {
            try await Task.sleep(for: .milliseconds(20))
        }

        // ...and rejoins with the SAME tailscale id (new generation).
        let backendB2 = await network.join(
            tailscaleId: "ts-b",
            hostname: Hostname.tailscaleHostname(
                appId: try AppId(parsing: "demo"), deviceName: DeviceName("Bob")))
        let bob2 = try await MeshNode.start(
            MeshConfiguration(
                appId: "demo", deviceName: "Bob", stateDirectory: dirB,
                auth: .existingState),
            backend: backendB2,
            frameTransport: LengthPrefixFrameTransport())
        guard let freshBob = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("bob did not rejoin")
            return
        }
        #expect(freshBob.generation != staleBob.generation)

        // The stale snapshot fails loudly — never routes to the new node.
        await #expect(throws: MeshError.peerGone(staleBob.ref.description)) {
            try await alice.sendJSON(
                to: staleBob, namespace: "chat", payload: ChatPayload(text: "stale"))
        }
        // A stale ref *query* also classifies as peerGone, not not-found.
        await #expect(throws: MeshError.peerGone(staleBob.ref.description)) {
            _ = try await alice.peer(staleBob.ref.description)
        }

        await alice.stop()
        await bob2.stop()
    }

    @Test func resolvesQueriesAndDetectsAmbiguity() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", appId: "demo", deviceName: "Alice")
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", appId: "demo", deviceName: "Laptop")
        let (carol, dirC) = try await startNode(
            network: network, tailscaleId: "ts-c", appId: "demo", deviceName: "Laptop")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
            try? FileManager.default.removeItem(at: dirC)
        }

        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("no peer")
            return
        }
        // Confirm both via hello so device names/ULIDs are known.
        try await alice.sendJSON(to: bobPeer, namespace: "x", payload: ChatPayload(text: "."))
        guard let carolPeer = try await alice.peer("ts-c", waitMs: 2_000) else {
            Issue.record("no peer")
            return
        }
        try await alice.sendJSON(to: carolPeer, namespace: "x", payload: ChatPayload(text: "."))

        // Exact tailscaleId / ref / IP / deviceId / prefix. Same identity
        // (ref) — content differs since bobPeer predates the hello.
        let byRef = try await alice.peer(bobPeer.ref.description)
        #expect(byRef?.ref == bobPeer.ref)
        guard let confirmedBob = try await alice.peer("ts-b"),
            let bobUlid = confirmedBob.deviceId
        else {
            Issue.record("bob not confirmed")
            return
        }
        let byUlid = try await alice.peer(bobUlid)
        #expect(byUlid?.tailscaleId == "ts-b")
        // ULIDs minted in the same millisecond share their 10-char timestamp
        // prefix, so use a prefix deep into the 80 random bits.
        let byPrefix = try await alice.peer(String(bobUlid.prefix(20)))
        #expect(byPrefix?.tailscaleId == "ts-b")
        let byIP = try await alice.peer(confirmedBob.tailnetIPs[0])
        #expect(byIP?.tailscaleId == "ts-b")

        // Shared display name "Laptop" → ambiguous (throws, never guesses).
        await #expect(throws: MeshError.self) {
            _ = try await alice.peer("laptop")
        }

        // Unknown → nil (never throws not-found from peer()).
        let missing = try await alice.peer("no-such-peer")
        #expect(missing == nil)

        await alice.stop()
        await bob.stop()
        await carol.stop()
    }

    @Test func failClosedIdentityRejectsWhenWhoIsUnavailable() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", appId: "demo", deviceName: "Alice")
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", appId: "demo", deviceName: "Bob",
            identityPolicy: .failClosed)
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("no peer")
            return
        }

        // WhoIs stops answering: Bob (server, fail-closed) must reject with
        // 4003 and Alice's send must fail. Nobody confirms.
        await network.setWithholdWhoIs(true)
        await #expect(throws: MeshError.self) {
            try await alice.sendJSON(
                to: bobPeer, namespace: "chat", payload: ChatPayload(text: "hi"))
        }
        let unconfirmed = try await alice.peer("ts-b")
        #expect(unconfirmed?.deviceId == nil)

        // WhoIs recovers → the same peers can now confirm.
        await network.setWithholdWhoIs(false)
        try await alice.sendJSON(
            to: bobPeer, namespace: "chat", payload: ChatPayload(text: "hi again"))
        let confirmed = try await alice.peer("ts-b")
        #expect(confirmed?.deviceId != nil)

        await alice.stop()
        await bob.stop()
    }

    @Test func droppedSubscriptionHandleAutoCancels() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", appId: "demo", deviceName: "Alice")
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", appId: "demo", deviceName: "Bob")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        let inbox = Mailbox<MeshMessage>()
        // Register a handler and immediately DROP the handle: the node holds
        // it weakly, so release deinits + auto-cancels it (RFC 024 §6.4).
        do {
            _ = await bob.onMessage(namespace: "chat") { await inbox.put($0) }
        }
        // Keep a second, retained subscription on another namespace to prove
        // delivery in general still works.
        let keptInbox = Mailbox<MeshMessage>()
        let kept = await bob.onMessage(namespace: "kept") { await keptInbox.put($0) }

        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("no peer")
            return
        }
        try await alice.sendJSON(
            to: bobPeer, namespace: "chat", payload: ChatPayload(text: "into the void"))
        try await alice.sendJSON(
            to: bobPeer, namespace: "kept", payload: ChatPayload(text: "delivered"))

        // The retained subscription received its message...
        let deliveredMessage = await keptInbox.take()
        #expect(deliveredMessage != nil)
        // ...while the dropped one never fired (its mailbox stays empty).
        await inbox.finish()
        let ghost = await inbox.take()
        #expect(ghost == nil)

        await kept.cancel()
        await alice.stop()
        await bob.stop()
    }

    @Test func provisionalEntrySurvivesSnapshotAndMergesLater() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", appId: "demo", deviceName: "Alice")
        defer { try? FileManager.default.removeItem(at: dirA) }

        // Bob joins HIDDEN: dialable + WhoIs-resolvable, but absent from
        // Alice's snapshots — his hello will race ahead of the netmap.
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", appId: "demo", deviceName: "Bob",
            hidden: true)
        defer { try? FileManager.default.removeItem(at: dirB) }

        // Bob dials Alice (Bob sees Alice via his own snapshot).
        guard let alicePeer = try await bob.peer("ts-a", waitMs: 2_000) else {
            Issue.record("bob cannot see alice")
            return
        }
        try await bob.sendJSON(to: alicePeer, namespace: "x", payload: ChatPayload(text: "."))

        // Alice now has a provisional entry for Bob (hello-before-netmap).
        guard let provisional = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("provisional entry missing")
            return
        }
        #expect(provisional.deviceId != nil)
        #expect(provisional.hostname.isEmpty)

        // A full snapshot WITHOUT Bob must NOT reap the provisional entry
        // (RFC 024 §7.2).
        try await alice.refresh()
        let afterSnapshot = try await alice.peer("ts-b")
        #expect(afterSnapshot != nil)
        #expect(afterSnapshot?.generation == provisional.generation)

        // The netmap event finally arrives → merges into the SAME
        // generation, now with Layer 3 metadata.
        await network.reveal(tailscaleId: "ts-b")
        var merged: Peer?
        for _ in 0..<40 {
            merged = try await alice.peer("ts-b")
            if merged?.hostname.isEmpty == false { break }
            try await Task.sleep(for: .milliseconds(50))
        }
        #expect(merged?.hostname.isEmpty == false)
        #expect(merged?.generation == provisional.generation)

        await alice.stop()
        await bob.stop()
    }

    @Test func heartbeatTimeoutClosesDeadSession() async throws {
        let network = LoopbackNetwork()
        let tuning = SessionTuning(
            handshakeTimeout: .seconds(2),
            pingInterval: .milliseconds(50),
            pongTimeout: .milliseconds(250))

        let hostA = Hostname.tailscaleHostname(
            appId: try AppId(parsing: "demo"), deviceName: DeviceName("Alice"))
        let backendA = await network.join(tailscaleId: "ts-a", hostname: hostA)
        let dirA = tempDir()
        defer { try? FileManager.default.removeItem(at: dirA) }
        let alice = try await MeshNode.start(
            MeshConfiguration(
                appId: "demo", deviceName: "Alice", stateDirectory: dirA,
                auth: .existingState),
            backend: backendA,
            frameTransport: LengthPrefixFrameTransport(),
            identityPolicy: .failClosed,
            tuning: tuning)

        // Bob's transport silently drops his outgoing Pongs: from Alice's
        // side he is a peer that stops answering keepalives.
        let hostB = Hostname.tailscaleHostname(
            appId: try AppId(parsing: "demo"), deviceName: DeviceName("Bob"))
        let backendB = await network.join(tailscaleId: "ts-b", hostname: hostB)
        let dirB = tempDir()
        defer { try? FileManager.default.removeItem(at: dirB) }
        let bob = try await MeshNode.start(
            MeshConfiguration(
                appId: "demo", deviceName: "Bob", stateDirectory: dirB,
                auth: .existingState),
            backend: backendB,
            frameTransport: PongDroppingTransport(),
            identityPolicy: .failClosed,
            tuning: tuning)

        let aliceEvents = await alice.events

        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("no peer")
            return
        }
        try await alice.sendJSON(to: bobPeer, namespace: "chat", payload: ChatPayload(text: "hi"))

        // Alice's heartbeat must detect the missing Pongs and close the
        // session (RFC 024 §8.1 step 8) within a few timeout periods.
        var sawTimeout = false
        let deadline = ContinuousClock.now.advanced(by: .seconds(5))
        for await event in aliceEvents {
            if case .health(let message) = event, message.contains("heartbeat timeout") {
                sawTimeout = true
                break
            }
            if ContinuousClock.now >= deadline { break }
        }
        #expect(sawTimeout)

        await alice.stop()
        await bob.stop()
    }

    @Test func eventsStreamEmitsPhaseImmediately() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", appId: "demo", deviceName: "Alice")
        defer { try? FileManager.default.removeItem(at: dirA) }

        var iterator = await alice.events.makeAsyncIterator()
        let first = await iterator.next()
        guard case .phase(let phase) = first else {
            Issue.record("expected immediate phase event, got \(String(describing: first))")
            return
        }
        #expect(phase == .running)

        #expect(await alice.localPeer.isLocal)
        #expect(await alice.localPeer.deviceId != nil)
        #expect(await alice.dnsName != nil)

        await alice.stop()
    }
}

// MARK: - The login gate, end to end (RFC 025 §3.3/§3.4, D1–D5)

/// A gated pair over the loopback tailnet: the Layer 3 filter, the hello
/// refusal, and the fail-closed row where Layer 3 reports no login at all.
@Suite struct NodeLoginGateTests {
    struct ChatPayload: Codable, Equatable {
        var text: String
    }

    private func tempDir() -> URL {
        FileManager.default.temporaryDirectory
            .appendingPathComponent("truffle-gate-test-\(UUID().uuidString)")
    }

    private func startNode(
        network: LoopbackNetwork,
        tailscaleId: String,
        deviceName: String,
        loginName: String? = nil,
        displayName: String? = nil,
        loginAllow: [String] = []
    ) async throws -> (MeshNode, URL) {
        let hostname = Hostname.tailscaleHostname(
            appId: try AppId(parsing: "demo"), deviceName: DeviceName(deviceName))
        let backend = await network.join(
            tailscaleId: tailscaleId, hostname: hostname, loginName: loginName,
            displayName: displayName)
        let dir = tempDir()
        let node = try await MeshNode.start(
            MeshConfiguration(
                appId: "demo", deviceName: deviceName, stateDirectory: dir,
                auth: .existingState, loginAllow: loginAllow),
            backend: backend,
            frameTransport: LengthPrefixFrameTransport(),
            identityPolicy: .failClosed)
        return (node, dir)
    }

    /// Await one value with a deadline, so a missing event fails the test
    /// instead of hanging it.
    private func firstOrNil<T: Sendable>(
        timeout: Duration, _ produce: @escaping @Sendable () async -> T?
    ) async -> T? {
        await withTaskGroup(of: T?.self) { group in
            group.addTask { await produce() }
            group.addTask {
                try? await Task.sleep(for: timeout)
                return nil
            }
            let first = await group.next() ?? nil
            group.cancelAll()
            return first
        }
    }

    /// (a) A matching glob: the pair converges and messages flow, exactly as
    /// an ungated pair does.
    @Test func gatedPairWithMatchingLoginConverges() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice",
            loginName: "alice@corp.com", loginAllow: ["*@corp.com"])
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", deviceName: "Bob",
            loginName: "bob@CORP.com", loginAllow: ["*@corp.com"])
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        let inbox = Mailbox<MeshMessage>()
        let subscription = await bob.onMessage(namespace: "chat") { message in
            await inbox.put(message)
        }

        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("gated alice never listed bob under a matching glob")
            return
        }
        // The login is a first-class field on the snapshot (D7), carried
        // through Layer 3 in the case the netmap reported it.
        #expect(bobPeer.loginName == "bob@CORP.com")
        #expect(await alice.loginName == "alice@corp.com")
        #expect(await alice.localPeer.loginName == "alice@corp.com")
        #expect(await alice.loginAllow == ["*@corp.com"])

        try await alice.sendJSON(
            to: bobPeer, namespace: "chat", payload: ChatPayload(text: "hi bob"))
        guard let received = await inbox.take() else {
            Issue.record("bob received nothing")
            return
        }
        #expect(try received.decodePayload(ChatPayload.self) == ChatPayload(text: "hi bob"))
        #expect(received.from.tailscaleId == "ts-a")

        await subscription.cancel()
        await alice.stop()
        await bob.stop()
    }

    /// (b) A foreign glob: the peer is never listed, AND the hello that peer
    /// dials with is refused — the two halves of the gate, separately.
    @Test func foreignLoginIsNeitherListedNorAdmitted() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice",
            loginName: "alice@corp.com", loginAllow: ["*@corp.com"])
        // Bob is ungated and on a different login: he still discovers and
        // dials Alice, which is exactly what the hello gate must stop.
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", deviceName: "Bob",
            loginName: "mallory@evil.com")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        try await alice.waitUntilRunning(timeout: .seconds(2))
        try await bob.waitUntilRunning(timeout: .seconds(2))

        // Layer 3: Alice never lists Bob, though the hostname prefix matches.
        #expect(try await alice.peer("ts-b", waitMs: 500) == nil)
        #expect(await alice.peers().isEmpty)

        // Layer 4/5: Bob DOES list Alice and dials her; the hello is refused.
        guard let alicePeer = try await bob.peer("ts-a", waitMs: 2_000) else {
            Issue.record("ungated bob should still discover alice")
            return
        }
        await #expect(throws: MeshError.self) {
            try await bob.sendJSON(
                to: alicePeer, namespace: "chat", payload: ChatPayload(text: "let me in"))
        }
        // The refusal left no provisional entry behind on the gated node.
        #expect(try await alice.peer("ts-b") == nil)
        #expect(await alice.peers().isEmpty)

        await alice.stop()
        await bob.stop()
    }

    /// (c) Fail closed: on a gated node a Layer 3 row with NO login is not a
    /// peer — and the node says so rather than showing an empty mesh.
    @Test func gatedNodeTreatsLoginlessRowAsNotAPeer() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice",
            loginName: "alice@corp.com", loginAllow: ["*@corp.com"])
        defer { try? FileManager.default.removeItem(at: dirA) }

        let notices = Mailbox<String>()
        let stream = await alice.events
        let drain = Task {
            for await event in stream {
                if case .health(let message) = event {
                    _ = await notices.put(message)
                }
            }
        }
        defer { drain.cancel() }

        // A well-named app peer whose netmap row carries no login at all.
        _ = await network.join(
            tailscaleId: "ts-nologin",
            hostname: Hostname.tailscaleHostname(
                appId: try AppId(parsing: "demo"), deviceName: DeviceName("Ghost")),
            loginName: nil)
        try await alice.refresh()

        #expect(try await alice.peer("ts-nologin") == nil)
        #expect(await alice.peers().isEmpty)

        let notice = await firstOrNil(timeout: .seconds(2)) { await notices.take() }
        #expect(notice?.contains("login gate active") == true)

        // The same row WITH a matching login is admitted — proving the row
        // was dropped for its login and not for its hostname.
        await network.setLogin(tailscaleId: "ts-nologin", loginName: "ghost@corp.com")
        try await alice.refresh()
        let admitted = try await alice.peer("ts-nologin", waitMs: 1_000)
        #expect(admitted?.loginName == "ghost@corp.com")

        await alice.stop()
    }

    /// HIGH-1 (found on review): the gate must run on every row, not only at
    /// entry creation. A stable node ID survives a device transfer, so a
    /// re-signed node arrives as an UPDATE to the existing row — and before
    /// this fix an admitted peer kept its place after re-signing as a foreign
    /// login, sessions and all.
    @Test func aPeerThatResignsAsAForeignLoginIsEvicted() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice",
            loginName: "alice@corp.com", loginAllow: ["*@corp.com"])
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", deviceName: "Bob",
            loginName: "bob@corp.com")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        let departures = Mailbox<String>()
        let stream = await alice.events
        let drain = Task {
            for await event in stream {
                if case .peerLeft(let peer) = event { _ = await departures.put(peer.tailscaleId) }
            }
        }
        defer { drain.cancel() }

        // Admitted, and a real session established.
        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("alice never admitted bob")
            return
        }
        try await alice.sendJSON(
            to: bobPeer, namespace: "chat", payload: ChatPayload(text: "hi"))
        let admittedGeneration = bobPeer.generation

        // The device is transferred: same stable node ID, foreign login.
        await network.setLogin(tailscaleId: "ts-b", loginName: "mallory@evil.com")
        try await alice.refresh()

        // A refused login is a DEPARTURE: the row goes, and removeEntry —
        // which is what emits this event — is also what closes the session.
        let departed = await firstOrNil(timeout: .seconds(2)) { await departures.take() }
        #expect(departed == "ts-b")
        #expect(try await alice.peer("ts-b") == nil)
        #expect(await alice.peers().isEmpty)
        // The snapshot taken while he was admitted no longer resolves.
        await #expect(throws: MeshError.peerGone(bobPeer.ref.description)) {
            try await alice.sendJSON(
                to: bobPeer, namespace: "chat", payload: ChatPayload(text: "still there?"))
        }

        // And the reverse: re-signing back onto the allow-list readmits him,
        // as a NEW generation — a rejoin is never the same row.
        await network.setLogin(tailscaleId: "ts-b", loginName: "bob@corp.com")
        try await alice.refresh()
        guard let readmitted = try await alice.peer("ts-b", waitMs: 1_000) else {
            Issue.record("alice never readmitted bob")
            return
        }
        #expect(readmitted.loginName == "bob@corp.com")
        #expect(readmitted.generation != admittedGeneration)

        await alice.stop()
        await bob.stop()
    }

    /// A row that names no owner is KEPT but reports no login (RFC 025 §3.3 as
    /// refined). Two halves, and both matter: the entry survives, so a
    /// transient failure to read the logins cannot empty a gated mesh; and the
    /// last-known login does NOT stick, because the row names no owner and so
    /// neither may we. A later resolvable row restores it, in the same
    /// generation — this is not a departure.
    @Test func anAbsentLoginIsKeptAsAPeerButReportedAbsent() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice",
            loginName: "alice@corp.com", loginAllow: ["*@corp.com"])
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", deviceName: "Bob",
            loginName: "bob@corp.com")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("alice never admitted bob")
            return
        }
        #expect(bobPeer.loginName == "bob@corp.com")

        await network.setLogin(tailscaleId: "ts-b", loginName: nil)
        try await alice.refresh()

        let kept = try await alice.peer("ts-b")
        #expect(kept != nil)
        #expect(kept?.generation == bobPeer.generation)
        #expect(kept?.loginName == nil)

        // Restored, same generation: a login that comes back is not a rejoin.
        await network.setLogin(tailscaleId: "ts-b", loginName: "bob@corp.com")
        try await alice.refresh()
        let restored = try await alice.peer("ts-b")
        #expect(restored?.loginName == "bob@corp.com")
        #expect(restored?.generation == bobPeer.generation)

        await alice.stop()
        await bob.stop()
    }

    /// The dial-side half: a gated node opens no NEW session to a peer whose
    /// row cannot name its owner, but never tears down one that is already
    /// open. Both halves are asserted here because a fix that closed the live
    /// session would also make the first assertion pass.
    @Test func aGatedNodeWillNotOpenASessionToAnUnattributablePeer() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice",
            loginName: "alice@corp.com", loginAllow: ["*@corp.com"])
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", deviceName: "Bob",
            loginName: "bob@corp.com", loginAllow: ["*@corp.com"])
        let (carol, dirC) = try await startNode(
            network: network, tailscaleId: "ts-c", deviceName: "Carol",
            loginName: "carol@corp.com", loginAllow: ["*@corp.com"])
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
            try? FileManager.default.removeItem(at: dirC)
        }

        let bobInbox = Mailbox<MeshMessage>()
        let subBob = await bob.onMessage(namespace: "chat") { await bobInbox.put($0) }

        // Bob: a session is OPEN before his login goes absent.
        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("alice never admitted bob")
            return
        }
        try await alice.sendJSON(
            to: bobPeer, namespace: "chat", payload: ChatPayload(text: "before"))
        #expect(await bobInbox.take() != nil)

        // Carol: admitted, but NO session opened yet.
        guard try await alice.peer("ts-c", waitMs: 2_000) != nil else {
            Issue.record("alice never admitted carol")
            return
        }

        await network.setLogin(tailscaleId: "ts-b", loginName: nil)
        await network.setLogin(tailscaleId: "ts-c", loginName: nil)
        try await alice.refresh()

        // The live session stands — no flap.
        guard let bobNow = try await alice.peer("ts-b") else {
            Issue.record("bob should still be listed")
            return
        }
        try await alice.sendJSON(
            to: bobNow, namespace: "chat", payload: ChatPayload(text: "after"))
        #expect(await bobInbox.take() != nil)

        // Carol has no session to stand on, so opening one is refused.
        guard let carolNow = try await alice.peer("ts-c") else {
            Issue.record("carol should still be listed")
            return
        }
        #expect(carolNow.loginName == nil)
        await #expect(throws: MeshError.loginUnknown(peer: carolNow.ref.description)) {
            try await alice.sendJSON(
                to: carolNow, namespace: "chat", payload: ChatPayload(text: "who are you?"))
        }
        // The raw plane's outbound dial is this node's act too.
        await #expect(throws: MeshError.loginUnknown(peer: carolNow.ref.description)) {
            _ = try await alice.dial(to: carolNow, port: 9500)
        }
        await #expect(throws: MeshError.loginUnknown(peer: carolNow.ref.description)) {
            _ = try await alice.confirmIdentity(of: carolNow)
        }

        // A login that comes back makes her dialable again.
        await network.setLogin(tailscaleId: "ts-c", loginName: "carol@corp.com")
        try await alice.refresh()
        guard let carolBack = try await alice.peer("ts-c") else {
            Issue.record("carol should still be listed")
            return
        }
        let confirmed = try await alice.confirmIdentity(of: carolBack)
        #expect(confirmed.deviceId != nil)

        await subBob.cancel()
        await alice.stop()
        await bob.stop()
        await carol.stop()
    }

    /// An UNGATED node ignores the field: an absent login never blocks a dial.
    @Test func anUngatedNodeDialsAPeerWithNoLogin() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice")
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", deviceName: "Bob")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("alice never discovered bob")
            return
        }
        #expect(bobPeer.loginName == nil)
        let confirmed = try await alice.confirmIdentity(of: bobPeer)
        #expect(confirmed.deviceId != nil)

        await alice.stop()
        await bob.stop()
    }

    /// MEDIUM-2: the dialer must be able to tell a gate from a broken pipe.
    /// The refusal reaches the dialing side as the close code the gate sent.
    @Test func aRefusedDialerSeesTheCloseCodeNotAProtocolViolation() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice",
            loginName: "alice@corp.com", loginAllow: ["*@corp.com"])
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", deviceName: "Bob",
            loginName: "mallory@evil.com")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        guard let alicePeer = try await bob.peer("ts-a", waitMs: 2_000) else {
            Issue.record("ungated bob should still discover alice")
            return
        }
        await #expect(
            throws: MeshError.helloRefused(
                code: SessionCloseCode.loginRefused, reason: "login refused")
        ) {
            try await bob.sendJSON(
                to: alicePeer, namespace: "chat", payload: ChatPayload(text: "let me in"))
        }

        await alice.stop()
        await bob.stop()
    }

    /// MEDIUM-1: the raw plane has an identity surface. `listen(port:)` is NOT
    /// gated by loginAllow, so an app that opens its own port gates itself
    /// with this — the login it returns is the one WhoIs authenticated.
    @Test func whoIsResolvesAnAcceptedAddressOnTheRawPlane() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice",
            loginName: "alice@corp.com", loginAllow: ["*@corp.com"])
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", deviceName: "Bob",
            loginName: "bob@corp.com", displayName: "Bob Example")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        guard let bobPeer = try await alice.peer("ts-b", waitMs: 2_000),
            let bobIP = bobPeer.tailnetIPs.first
        else {
            Issue.record("alice never discovered bob's address")
            return
        }
        let identity = try await alice.whoIs(remoteEndpoint: "\(bobIP):40001")
        #expect(identity.tailscaleId == "ts-b")
        #expect(identity.loginName == "bob@corp.com")
        #expect(identity.displayName == "Bob Example")
        // The grammar an app would gate with is the same one the node uses.
        #expect(LoginGlob.allowed(["*@corp.com"], login: identity.loginName))
        #expect(!LoginGlob.allowed(["*@other.com"], login: identity.loginName))

        await alice.stop()
        await bob.stop()
    }

    /// LOW: a provisional entry from a raced inbound hello carries the login
    /// that PASSED the gate, rather than waiting for the netmap to say. Bob is
    /// hidden — WhoIs-resolvable and able to dial, but absent from snapshots.
    @Test func aProvisionalEntryCarriesTheLoginThatPassedTheGate() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice",
            loginName: "alice@corp.com", loginAllow: ["*@corp.com"])
        let bobHostname = Hostname.tailscaleHostname(
            appId: try AppId(parsing: "demo"), deviceName: DeviceName("Bob"))
        let bobBackend = await network.join(
            tailscaleId: "ts-b", hostname: bobHostname, hidden: true,
            loginName: "bob@corp.com")
        let dirB = tempDir()
        let bob = try await MeshNode.start(
            MeshConfiguration(
                appId: "demo", deviceName: "Bob", stateDirectory: dirB, auth: .existingState),
            backend: bobBackend,
            frameTransport: LengthPrefixFrameTransport(),
            identityPolicy: .failClosed)
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        guard let alicePeer = try await bob.peer("ts-a", waitMs: 2_000) else {
            Issue.record("bob never discovered alice")
            return
        }
        try await bob.sendJSON(
            to: alicePeer, namespace: "chat", payload: ChatPayload(text: "hello"))

        // Alice knows him only from the hello — no netmap row exists yet.
        guard let provisional = try await alice.peer("ts-b", waitMs: 2_000) else {
            Issue.record("alice never created a provisional entry for bob")
            return
        }
        #expect(provisional.hostname.isEmpty)
        #expect(provisional.deviceId != nil)
        #expect(provisional.loginName == "bob@corp.com")

        await alice.stop()
        await bob.stop()
    }

    /// The witness the overlay-level row cannot be: a REAL `MeshNode` over a
    /// real `LoopbackNetwork`, applying its own predicate to every row shape
    /// the status overlay can produce — plus the `isAppPeer` half, which an
    /// inline re-statement of the login check silently drops.
    ///
    /// The overlay renders BOTH "a `UserID` with no profile" and "a row with
    /// no `UserID`" as an absent login, so those two arrive at the node
    /// identically; they are kept as separate rows here because they are
    /// separate nodes on the tailnet, not because the node can tell them
    /// apart.
    @Test func aGatedNodeAdmitsOnlyTheRowThatPassesBothHalves() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice",
            loginName: "alice@corp.com", loginAllow: ["*@corp.com"])
        defer { try? FileManager.default.removeItem(at: dirA) }

        func appHostname(_ name: String) throws -> String {
            Hostname.tailscaleHostname(
                appId: try AppId(parsing: "demo"), deviceName: DeviceName(name))
        }

        // A UserID the status's User{} does not describe.
        _ = await network.join(
            tailscaleId: "ts-orphan", hostname: try appHostname("Orphan"), loginName: nil)
        // A row carrying no UserID at all.
        _ = await network.join(
            tailscaleId: "ts-nouser", hostname: try appHostname("NoUser"), loginName: nil)
        // A resolvable owner, on the wrong domain.
        _ = await network.join(
            tailscaleId: "ts-mallory", hostname: try appHostname("Mallory"),
            loginName: "mallory@evil.com")
        // An allowed login that is NOT an app peer — the half a login-only
        // predicate would admit.
        _ = await network.join(
            tailscaleId: "ts-stranger", hostname: "workstation-corp",
            loginName: "bob@corp.com")
        // Both halves.
        _ = await network.join(
            tailscaleId: "ts-bob", hostname: try appHostname("Bob"), loginName: "bob@corp.com")

        try await alice.refresh()
        let admitted = await alice.peers().map(\.tailscaleId).sorted()
        #expect(admitted == ["ts-bob"])
        #expect(try await alice.peer("ts-bob")?.loginName == "bob@corp.com")

        await alice.stop()
    }

    /// An ungated node is unchanged: a login-less row is still a peer.
    @Test func ungatedNodeStillAdmitsLoginlessRows() async throws {
        let network = LoopbackNetwork()
        let (alice, dirA) = try await startNode(
            network: network, tailscaleId: "ts-a", deviceName: "Alice")
        let (bob, dirB) = try await startNode(
            network: network, tailscaleId: "ts-b", deviceName: "Bob")
        defer {
            try? FileManager.default.removeItem(at: dirA)
            try? FileManager.default.removeItem(at: dirB)
        }

        let bobPeer = try await alice.peer("ts-b", waitMs: 2_000)
        #expect(bobPeer != nil)
        #expect(bobPeer?.loginName == nil)
        #expect(await alice.loginName == nil)
        #expect(await alice.loginAllow.isEmpty)

        await alice.stop()
        await bob.stop()
    }
}
