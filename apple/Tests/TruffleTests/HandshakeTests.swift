import Foundation
import Testing

@testable import Truffle

private func makeHello(
    appId: String = "chat", deviceId: String = "01HZZZZZZZZZZZZZZZZZZZZZZ1",
    tailscaleId: String = "ts-a"
) -> HelloEnvelope {
    HelloEnvelope(
        identity: PeerIdentity(
            appId: appId, deviceId: deviceId, deviceName: "test", os: "darwin",
            tailscaleId: tailscaleId))
}

@Suite struct HandshakeTests {
    @Test func successfulExchangeBothRoles() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()
        let clientHello = makeHello(deviceId: "01HZZZZZZZZZZZZZZZZZZZZZZ1", tailscaleId: "ts-a")
        let serverHello = makeHello(deviceId: "01HZZZZZZZZZZZZZZZZZZZZZZ2", tailscaleId: "ts-b")

        async let serverSide = Handshake.server(
            frames: serverFrames,
            localHello: serverHello,
            authenticated: AuthenticatedPeer(tailscaleId: "ts-a", remoteAddresses: ["100.64.0.1:9417"]),
            policy: .failClosed)
        async let clientSide = Handshake.client(
            frames: clientFrames, localHello: clientHello, expectedTailscaleId: "ts-b")

        let (serverSeen, clientSeen) = try await (serverSide, clientSide)
        #expect(serverSeen.tailscaleId == "ts-a")
        #expect(serverSeen.deviceId == "01HZZZZZZZZZZZZZZZZZZZZZZ1")
        #expect(clientSeen.tailscaleId == "ts-b")
        #expect(clientSeen.deviceId == "01HZZZZZZZZZZZZZZZZZZZZZZ2")
    }

    @Test func appMismatchCloses4001AndNeverSendsServerHello() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()

        // Client with a DIFFERENT appId sends its hello; server must reject.
        try await clientFrames.send(
            .text(String(decoding: try makeHello(appId: "other").encoded(), as: UTF8.self)))

        await #expect(throws: MeshError.self) {
            try await Handshake.server(
                frames: serverFrames,
                localHello: makeHello(appId: "chat", tailscaleId: "ts-b"),
                authenticated: AuthenticatedPeer(tailscaleId: "ts-a", remoteAddresses: []),
                policy: .failClosed)
        }

        // The client observes a 4001 close and NO server hello before it.
        let frame = try await clientFrames.receive()
        guard case .close(let code, _) = frame else {
            Issue.record("expected close frame, got \(String(describing: frame))")
            return
        }
        #expect(code == SessionCloseCode.appMismatch)
    }

    @Test func malformedHelloCloses4002() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()
        try await clientFrames.send(.text("{not json"))

        await #expect(throws: MeshError.self) {
            try await Handshake.server(
                frames: serverFrames, localHello: makeHello(tailscaleId: "ts-b"),
                authenticated: nil, policy: .allowUnverified)
        }
        let frame = try await clientFrames.receive()
        guard case .close(let code, _) = frame else {
            Issue.record("expected close frame")
            return
        }
        #expect(code == SessionCloseCode.helloProtocol)
    }

    @Test func identityContradictionCloses4003() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()
        try await clientFrames.send(
            .text(String(decoding: try makeHello(tailscaleId: "ts-a").encoded(), as: UTF8.self)))

        await #expect(throws: MeshError.identityMismatch(claimed: "ts-a", authenticated: "ts-EVIL")) {
            try await Handshake.server(
                frames: serverFrames, localHello: makeHello(tailscaleId: "ts-b"),
                authenticated: AuthenticatedPeer(tailscaleId: "ts-EVIL", remoteAddresses: []),
                policy: .failClosed)
        }
        let frame = try await clientFrames.receive()
        guard case .close(let code, _) = frame else {
            Issue.record("expected close frame")
            return
        }
        #expect(code == SessionCloseCode.identityMismatch)
    }

    @Test func failClosedRejectsMissingIdentityWith4003() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()
        try await clientFrames.send(
            .text(String(decoding: try makeHello().encoded(), as: UTF8.self)))

        await #expect(throws: MeshError.self) {
            try await Handshake.server(
                frames: serverFrames, localHello: makeHello(tailscaleId: "ts-b"),
                authenticated: AuthenticatedPeer(tailscaleId: "", remoteAddresses: []),
                policy: .failClosed)
        }
        let frame = try await clientFrames.receive()
        guard case .close(let code, _) = frame else {
            Issue.record("expected close frame")
            return
        }
        #expect(code == SessionCloseCode.identityMismatch)
    }

    @Test func allowUnverifiedIsExplicitOptIn() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()
        try await clientFrames.send(
            .text(String(decoding: try makeHello(tailscaleId: "ts-a").encoded(), as: UTF8.self)))

        let identity = try await Handshake.server(
            frames: serverFrames, localHello: makeHello(tailscaleId: "ts-b"),
            authenticated: nil, policy: .allowUnverified)
        #expect(identity.tailscaleId == "ts-a")
    }

    @Test func clientRejectsWrongServerIdentity() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()

        async let serverSide: Void = {
            // Legitimate-looking server that identifies as ts-OTHER.
            _ = try await Handshake.server(
                frames: serverFrames, localHello: makeHello(tailscaleId: "ts-OTHER"),
                authenticated: nil, policy: .allowUnverified)
        }()

        await #expect(throws: MeshError.identityMismatch(claimed: "ts-OTHER", authenticated: "ts-b")) {
            try await Handshake.client(
                frames: clientFrames, localHello: makeHello(tailscaleId: "ts-a"),
                expectedTailscaleId: "ts-b")
        }
        try? await serverSide
    }

    @Test func toleratesUpTo16ControlFramesBeforeHello() async throws {
        let (sender, receiver) = FramePipe.makePair()
        for _ in 0..<16 {
            try await sender.send(.ping(Data()))
        }
        try await sender.send(
            .text(String(decoding: try makeHello().encoded(), as: UTF8.self)))

        let hello = try await Handshake.receiveHello(receiver)
        #expect(hello.identity.tailscaleId == "ts-a")
    }

    @Test func rejectsMoreThan16ControlFramesBeforeHello() async throws {
        let (sender, receiver) = FramePipe.makePair()
        for _ in 0..<17 {
            try await sender.send(.ping(Data()))
        }
        await #expect(throws: MeshError.protocolViolation("too many control frames before hello")) {
            try await Handshake.receiveHello(receiver)
        }
    }

    @Test func helloReadTimesOut() async throws {
        let (_, serverFrames) = FramePipe.makePair()
        // Nothing is ever sent; use a short deadline instead of the real 5s.
        await #expect(throws: MeshError.timeout("hello read")) {
            try await withDeadline(.milliseconds(50), label: "hello read") {
                try await Handshake.receiveHello(serverFrames)
            }
        }
    }
}

@Suite struct LengthPrefixTransportTests {
    @Test func roundTripsAllFrameKinds() async throws {
        let (connA, connB) = LoopbackConnection.makePair()
        let a = try await LengthPrefixFrameTransport().clientFrames(over: connA)
        let b = try await LengthPrefixFrameTransport().serverFrames(over: connB)

        try await a.send(.text("hello"))
        try await a.send(.binary(Data([1, 2, 3])))
        try await a.send(.ping(Data([9])))
        try await a.send(.pong(Data()))

        #expect(try await b.receive() == .text("hello"))
        #expect(try await b.receive() == .binary(Data([1, 2, 3])))
        #expect(try await b.receive() == .ping(Data([9])))
        #expect(try await b.receive() == .pong(Data()))

        try await a.send(.close(code: 4001, reason: "nope"))
        #expect(try await b.receive() == .close(code: 4001, reason: "nope"))
        #expect(try await b.receive() == nil)
    }
}

// MARK: - The login gate (RFC 025 §3.4, D4)

/// Every row of the §3.4 table for `Handshake.server`. Each refusal asserts
/// the close code the CLIENT's frames see, and asserts it is the FIRST frame
/// the client receives — which is what proves our hello was never revealed to
/// a caller the gate rejected.
@Suite struct HandshakeLoginGateTests {
    private static let gate = ["*@corp.com"]

    private func firstFrame(_ frames: any SessionFrames) async throws -> SessionFrame? {
        try await frames.receive()
    }

    private func sendClientHello(_ frames: any SessionFrames, tailscaleId: String = "ts-a")
        async throws
    {
        try await frames.send(
            .text(String(decoding: try makeHello(tailscaleId: tailscaleId).encoded(), as: UTF8.self))
        )
    }

    // Row 1, gated column: absent identity is refused 4003 under EITHER
    // policy — a gate is never bypassed by the test policy.
    @Test func gatedRefusesAbsentIdentityEvenUnderAllowUnverified() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()
        try await sendClientHello(clientFrames)

        await #expect(throws: MeshError.self) {
            try await Handshake.server(
                frames: serverFrames, localHello: makeHello(tailscaleId: "ts-b"),
                authenticated: nil, policy: .allowUnverified, loginAllow: Self.gate)
        }
        guard case .close(let code, _) = try await firstFrame(clientFrames) else {
            Issue.record("expected close frame")
            return
        }
        #expect(code == SessionCloseCode.identityMismatch)
    }

    // Same row via an AuthenticatedPeer whose stable ID is empty.
    @Test func gatedRefusesEmptyStableIdEvenUnderAllowUnverified() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()
        try await sendClientHello(clientFrames)

        await #expect(throws: MeshError.self) {
            try await Handshake.server(
                frames: serverFrames, localHello: makeHello(tailscaleId: "ts-b"),
                authenticated: AuthenticatedPeer(
                    tailscaleId: "", remoteAddresses: [], loginName: "alice@corp.com"),
                policy: .allowUnverified, loginAllow: Self.gate)
        }
        guard case .close(let code, _) = try await firstFrame(clientFrames) else {
            Issue.record("expected close frame")
            return
        }
        #expect(code == SessionCloseCode.identityMismatch)
    }

    // Row 2: a node id mismatch is still 4003, and is decided BEFORE the
    // login — even when the login would have passed the gate.
    @Test func gatedNodeIdMismatchStillCloses4003() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()
        try await sendClientHello(clientFrames)

        await #expect(throws: MeshError.identityMismatch(claimed: "ts-a", authenticated: "ts-EVIL"))
        {
            try await Handshake.server(
                frames: serverFrames, localHello: makeHello(tailscaleId: "ts-b"),
                authenticated: AuthenticatedPeer(
                    tailscaleId: "ts-EVIL", remoteAddresses: [], loginName: "alice@corp.com"),
                policy: .failClosed, loginAllow: Self.gate)
        }
        guard case .close(let code, _) = try await firstFrame(clientFrames) else {
            Issue.record("expected close frame")
            return
        }
        #expect(code == SessionCloseCode.identityMismatch)
    }

    // Row 3: the node id is good but WhoIs carried no login — 4004, closed.
    @Test func gatedRefusesAbsentLoginWith4004() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()
        try await sendClientHello(clientFrames)

        await #expect(throws: MeshError.loginRefused(login: nil)) {
            try await Handshake.server(
                frames: serverFrames, localHello: makeHello(tailscaleId: "ts-b"),
                authenticated: AuthenticatedPeer(tailscaleId: "ts-a", remoteAddresses: []),
                policy: .failClosed, loginAllow: Self.gate)
        }
        guard case .close(let code, _) = try await firstFrame(clientFrames) else {
            Issue.record("expected close frame")
            return
        }
        #expect(code == SessionCloseCode.loginRefused)
    }

    // Row 4: a real login that matches no glob — 4004.
    @Test func gatedRefusesUnmatchedLoginWith4004() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()
        try await sendClientHello(clientFrames)

        await #expect(throws: MeshError.loginRefused(login: "mallory@evil.com")) {
            try await Handshake.server(
                frames: serverFrames, localHello: makeHello(tailscaleId: "ts-b"),
                authenticated: AuthenticatedPeer(
                    tailscaleId: "ts-a", remoteAddresses: [], loginName: "mallory@evil.com"),
                policy: .failClosed, loginAllow: Self.gate)
        }
        guard case .close(let code, _) = try await firstFrame(clientFrames) else {
            Issue.record("expected close frame")
            return
        }
        #expect(code == SessionCloseCode.loginRefused)
    }

    // Row 5: a matching login exchanges hellos exactly as before.
    @Test func gatedAcceptsMatchingLoginAndExchangesHellos() async throws {
        let (clientFrames, serverFrames) = FramePipe.makePair()
        let clientHello = makeHello(deviceId: "01HZZZZZZZZZZZZZZZZZZZZZZ1", tailscaleId: "ts-a")
        let serverHello = makeHello(deviceId: "01HZZZZZZZZZZZZZZZZZZZZZZ2", tailscaleId: "ts-b")

        async let serverSide = Handshake.server(
            frames: serverFrames,
            localHello: serverHello,
            authenticated: AuthenticatedPeer(
                tailscaleId: "ts-a", remoteAddresses: ["100.64.0.1:9417"],
                loginName: "Alice@CORP.com", displayName: "Alice"),
            policy: .failClosed,
            loginAllow: Self.gate)
        async let clientSide = Handshake.client(
            frames: clientFrames, localHello: clientHello, expectedTailscaleId: "ts-b")

        let (serverSeen, clientSeen) = try await (serverSide, clientSide)
        #expect(serverSeen.tailscaleId == "ts-a")
        #expect(clientSeen.tailscaleId == "ts-b")
        #expect(clientSeen.deviceId == "01HZZZZZZZZZZZZZZZZZZZZZZ2")
    }

    // The ungated column: an empty list is today's behaviour exactly, so a
    // foreign login — and an absent one — are both still accepted.
    @Test func ungatedAcceptsAnyLogin() async throws {
        for login in ["mallory@evil.com", nil] {
            let (clientFrames, serverFrames) = FramePipe.makePair()
            try await sendClientHello(clientFrames)

            let identity = try await Handshake.server(
                frames: serverFrames, localHello: makeHello(tailscaleId: "ts-b"),
                authenticated: AuthenticatedPeer(
                    tailscaleId: "ts-a", remoteAddresses: [], loginName: login),
                policy: .failClosed)
            #expect(identity.tailscaleId == "ts-a")
        }
    }

    // A tagged node's pseudo-login is passed through, not special-cased: it
    // is refused by a personal glob and admitted by one that names it.
    @Test func taggedDevicesPseudoLoginIsNotSpecialCased() async throws {
        let (refusedClient, refusedServer) = FramePipe.makePair()
        try await sendClientHello(refusedClient)
        await #expect(throws: MeshError.loginRefused(login: "tagged-devices")) {
            try await Handshake.server(
                frames: refusedServer, localHello: makeHello(tailscaleId: "ts-b"),
                authenticated: AuthenticatedPeer(
                    tailscaleId: "ts-a", remoteAddresses: [], loginName: "tagged-devices"),
                policy: .failClosed, loginAllow: Self.gate)
        }

        let (acceptedClient, acceptedServer) = FramePipe.makePair()
        try await sendClientHello(acceptedClient)
        let identity = try await Handshake.server(
            frames: acceptedServer, localHello: makeHello(tailscaleId: "ts-b"),
            authenticated: AuthenticatedPeer(
                tailscaleId: "ts-a", remoteAddresses: [], loginName: "tagged-devices"),
            policy: .failClosed, loginAllow: ["tagged-devices"])
        #expect(identity.tailscaleId == "ts-a")
    }
}
