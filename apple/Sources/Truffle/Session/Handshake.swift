import Foundation

/// Run `operation` with a deadline; throws `MeshError.timeout(label)` if it
/// does not complete in time.
func withDeadline<T: Sendable>(
    _ limit: Duration,
    label: String,
    operation: @escaping @Sendable () async throws -> T
) async throws -> T {
    try await withThrowingTaskGroup(of: T.self) { group in
        group.addTask { try await operation() }
        group.addTask {
            try await Task.sleep(for: limit)
            throw MeshError.timeout(label)
        }
        guard let first = try await group.next() else {
            throw MeshError.timeout(label)
        }
        group.cancelAll()
        return first
    }
}

/// The hello exchange (RFC 017 §8 / RFC 024 §8.1), transport-agnostic over
/// `SessionFrames`. Role ordering mirrors desktop `websocket.rs`:
///
/// - client: send hello → read + validate server hello
/// - server: read + validate client hello → verify authenticated identity →
///   send own hello (never before verification — RFC 024 §14.4)
public enum Handshake {
    /// Inbound identity policy (RFC 024 §8.1.1).
    public enum IdentityPolicy: Sendable {
        /// Production: absence of a concrete stable node ID fails closed (4003).
        case failClosed
        /// Explicit opt-in for loopback/mock tests only.
        case allowUnverified
    }

    // MARK: receive

    /// Receive and parse the first hello frame, tolerating up to
    /// `SessionLimits.maxControlFramesBeforeHello` Ping/Pong frames (answering
    /// pings), accepting Text or Binary JSON. Does not apply the timeout —
    /// callers wrap with `withDeadline`.
    static func receiveHello(_ frames: any SessionFrames) async throws -> HelloEnvelope {
        var controlFrames = 0
        while true {
            guard let frame = try await frames.receive() else {
                throw MeshError.protocolViolation("peer closed connection before hello")
            }
            switch frame {
            case .text(let text):
                do {
                    return try HelloEnvelope.decoding(Data(text.utf8))
                } catch {
                    throw MeshError.protocolViolation("parse hello: \(error)")
                }
            case .binary(let data):
                do {
                    return try HelloEnvelope.decoding(data)
                } catch {
                    throw MeshError.protocolViolation("parse hello: \(error)")
                }
            case .ping(let payload):
                controlFrames += 1
                if controlFrames > SessionLimits.maxControlFramesBeforeHello {
                    throw MeshError.protocolViolation("too many control frames before hello")
                }
                try? await frames.send(.pong(payload))
            case .pong:
                controlFrames += 1
                if controlFrames > SessionLimits.maxControlFramesBeforeHello {
                    throw MeshError.protocolViolation("too many control frames before hello")
                }
            case .close(let code, let reason):
                throw MeshError.protocolViolation(
                    "peer closed connection before hello (code \(code): \(reason))")
            }
        }
    }

    // MARK: client role

    /// Dialing side: send our hello as a Text frame, then read and validate
    /// the server hello. `expectedTailscaleId` enforces the outbound identity
    /// policy (RFC 024 §8.1.1): the server hello must identify the exact
    /// Layer 3 peer that was dialed.
    public static func client(
        frames: any SessionFrames,
        localHello: HelloEnvelope,
        expectedTailscaleId: String?
    ) async throws -> PeerIdentity {
        let payload = String(decoding: try localHello.encoded(), as: UTF8.self)
        try await frames.send(.text(payload))

        let remote: HelloEnvelope
        do {
            remote = try await withDeadline(SessionLimits.helloTimeout, label: "hello read") {
                try await receiveHello(frames)
            }
        } catch {
            // Malformed / missing hello → 4002, mirroring desktop. Without
            // this close the remote side would wait out its own timeout.
            await frames.close(
                code: SessionCloseCode.helloProtocol, reason: "hello not received")
            throw error
        }

        let identity: PeerIdentity
        do {
            identity = try remote.validate(localAppId: localHello.identity.appId)
        } catch let error as HelloValidationError {
            await frames.close(code: error.closeCode, reason: "\(error)")
            throw map(error)
        }

        if let expected = expectedTailscaleId, identity.tailscaleId != expected {
            await frames.close(
                code: SessionCloseCode.identityMismatch,
                reason: "server hello tailscale_id does not match dialed peer")
            throw MeshError.identityMismatch(
                claimed: identity.tailscaleId, authenticated: expected)
        }
        return identity
    }

    // MARK: server role

    /// Accepting side: read and validate the client hello, verify the claimed
    /// `tailscale_id` against the WhoIs-authenticated identity, then — and
    /// only then — send our own hello.
    ///
    /// `authenticated` is the backend's WhoIs answer for the accepted
    /// connection (nil when the lookup itself failed). Under `.failClosed`,
    /// a missing or empty stable node ID rejects with 4003; under
    /// `.allowUnverified` (tests only) the claim is accepted unverified —
    /// mirroring, explicitly, what desktop currently does implicitly.
    ///
    /// `loginAllow` is the node's login gate (RFC 025 §3.4, D4). Empty is
    /// today's behaviour exactly. Non-empty applies the §3.4 table, in this
    /// order, all of it BEFORE our own hello is sent so an impostor never
    /// learns our identity block:
    ///
    /// | bridge identity                      | ungated            | gated      |
    /// |--------------------------------------|--------------------|------------|
    /// | absent / no `tailscaleId`            | policy decides     | **4003**   |
    /// | `tailscaleId` ≠ claimed              | 4003               | 4003       |
    /// | ok, `loginName` absent               | accept             | **4004**   |
    /// | ok, `loginName` matches no glob      | accept             | **4004**   |
    /// | ok, `loginName` matches              | accept             | accept     |
    ///
    /// A gate is never bypassed by `.allowUnverified`: on a gated node an
    /// absent identity is refused under EITHER policy, because the login the
    /// gate needs can only come from an authenticated WhoIs answer.
    public static func server(
        frames: any SessionFrames,
        localHello: HelloEnvelope,
        authenticated: AuthenticatedPeer?,
        policy: IdentityPolicy,
        loginAllow: [String] = []
    ) async throws -> PeerIdentity {
        let remote: HelloEnvelope
        do {
            remote = try await withDeadline(SessionLimits.helloTimeout, label: "hello read") {
                try await receiveHello(frames)
            }
        } catch {
            // Malformed / missing hello → 4002, mirroring desktop. Without
            // this close the remote side would wait out its own timeout.
            await frames.close(
                code: SessionCloseCode.helloProtocol, reason: "hello not received")
            throw error
        }

        let identity: PeerIdentity
        do {
            identity = try remote.validate(localAppId: localHello.identity.appId)
        } catch let error as HelloValidationError {
            await frames.close(code: error.closeCode, reason: "\(error)")
            throw map(error)
        }

        let gated = !loginAllow.isEmpty
        let authenticatedId = authenticated?.tailscaleId ?? ""
        if authenticatedId.isEmpty {
            let refuse: Bool
            switch policy {
            case .failClosed:
                refuse = true
            case .allowUnverified:
                // A gate is never bypassed by the test policy: without an
                // authenticated identity there is no login to gate on.
                refuse = gated
            }
            if refuse {
                await frames.close(
                    code: SessionCloseCode.identityMismatch, reason: "identity unavailable")
                throw MeshError.identityUnavailable(
                    "no authenticated identity for incoming connection")
            }
        } else if authenticatedId != identity.tailscaleId {
            await frames.close(
                code: SessionCloseCode.identityMismatch,
                reason: "claimed tailscale_id contradicts authenticated identity")
            throw MeshError.identityMismatch(
                claimed: identity.tailscaleId, authenticated: authenticatedId)
        }

        if gated {
            // WhoIs is the only authority for the login — the hello never
            // declares one (RFC 025 §3.7, D8).
            let login = authenticated?.loginName
            guard LoginGlob.allowed(loginAllow, login: login) else {
                await frames.close(
                    code: SessionCloseCode.loginRefused, reason: "login refused")
                throw MeshError.loginRefused(login: login)
            }
        }

        let payload = String(decoding: try localHello.encoded(), as: UTF8.self)
        try await frames.send(.text(payload))
        return identity
    }

    // MARK: helpers

    static func map(_ error: HelloValidationError) -> MeshError {
        switch error {
        case .malformed(let msg):
            return .protocolViolation("hello rejected: \(msg)")
        case .appMismatch(let local, let remote):
            return .protocolViolation("app_id mismatch: local '\(local)', remote '\(remote)'")
        }
    }
}
