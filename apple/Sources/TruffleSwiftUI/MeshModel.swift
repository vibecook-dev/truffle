import Foundation
import Observation
import Truffle

/// SwiftUI-friendly observation wrapper around a `MeshNode`
/// (RFC 024 §6.7). Host apps need ~20 lines to go
/// Connect → Safari → Connected.
///
/// Two ways to construct:
/// - `init(config:)` — the production path (TailscaleKit backend; throws at
///   `start()` until RFC 024 Phase 0 lands).
/// - `init(starter:)` — inject any node factory (custom backends, demos,
///   tests).
@MainActor
@Observable
public final class MeshModel {
    public private(set) var phase: MeshPhase = .stopped
    public private(set) var peers: [Peer] = []
    /// The tailnet login this node is signed in as (RFC 025 §3.6, D7), as of
    /// the last Layer 3 status. `nil` until known — never fabricated.
    public private(set) var loginName: String?
    public private(set) var authURL: URL?
    public private(set) var lastError: String?

    /// The underlying node once started — escape hatch for messaging APIs.
    public private(set) var node: MeshNode?

    private let starter: @Sendable () async throws -> MeshNode
    private var eventTask: Task<Void, Never>?

    public convenience init(config: MeshConfiguration) {
        self.init(starter: { try await MeshNode.start(config) })
    }

    public init(starter: @escaping @Sendable () async throws -> MeshNode) {
        self.starter = starter
    }

    public func start() async {
        guard node == nil else { return }
        lastError = nil
        phase = .starting
        do {
            let node = try await starter()
            self.node = node
            let events = await node.events
            eventTask = Task { [weak self] in
                for await event in events {
                    guard let self else { return }
                    await self.handle(event, node: node)
                }
            }
        } catch {
            lastError = "\(error)"
            phase = .failed
        }
    }

    public func stop() async {
        eventTask?.cancel()
        eventTask = nil
        if let node {
            await node.stop()
        }
        node = nil
        peers = []
        authURL = nil
        phase = .stopped
    }

    /// Recover after login (e.g. the Safari sheet was dismissed).
    public func refresh() async {
        guard let node else { return }
        do {
            try await node.refresh()
        } catch {
            lastError = "\(error)"
        }
    }

    private func handle(_ event: MeshEvent, node: MeshNode) async {
        switch event {
        case .phase(let phase):
            self.phase = phase
            if phase == .running {
                authURL = nil
            }
            peers = await node.peers()
            loginName = await node.loginName
        case .authRequired(let url):
            authURL = url
        case .peerUpsert, .peerLeft:
            peers = await node.peers()
            loginName = await node.loginName
        case .message:
            break  // chat-level concerns live in the host app
        case .health(let message):
            lastError = message
        }
    }
}
