import Foundation
import Testing
import Truffle

@testable import TruffleTailscale

/// The client-log switch on the Swift plane: what `MeshConfiguration` hands
/// TailscaleKit, and what the node's status reports. Pinned from literal JSON
/// and a loopback node, because `TailscaleKitBackend` compiles only on iOS.
private func decode<T: Decodable>(_ type: T.Type, _ json: String) throws -> T {
    try JSONDecoder().decode(type, from: Data(json.utf8))
}

/// A status read on a tailnet with network flow logs on: `Self` carries the
/// data-plane-audit-logs capability, with the `null` Go writes for a
/// capability that has no values.
private let flowLogsStatusJSON = """
    {
      "Self": {
        "ID": "nSELF", "UserID": 1, "HostName": "truffle-demo-phone",
        "CapMap": {
          "https://tailscale.com/cap/data-plane-audit-logs": null,
          "https://tailscale.com/cap/is-admin": [{"note": "any value"}]
        }
      },
      "User": {"1": {"LoginName": "alice@corp.com"}}
    }
    """

/// The same node on a tailnet without flow logs.
private let plainStatusJSON = """
    {
      "Self": {
        "ID": "nSELF", "UserID": 1, "HostName": "truffle-demo-phone",
        "CapMap": {"https://tailscale.com/cap/is-admin": null}
      },
      "User": {"1": {"LoginName": "alice@corp.com"}}
    }
    """

@Suite struct ClientLogUploadTests {
    /// The default keeps Tailscale's own, which is to upload; an opt-out
    /// reaches TailscaleKit's `Configuration` unchanged.
    @Test func uploadsByDefaultAndPassesAnOptOutThrough() {
        let standard = MeshConfiguration(appId: "demo", deviceName: "Phone")
        #expect(standard.clientLogUpload)
        #expect(ClientLogUpload.requested(by: standard))

        let optedOut = MeshConfiguration(appId: "demo", deviceName: "Phone", clientLogUpload: false)
        #expect(optedOut.clientLogUpload == false)
        #expect(ClientLogUpload.requested(by: optedOut) == false)
    }

    @Test func readsTheSelfCapabilitiesWhateverTheirValues() throws {
        let decoded = try decode(LocalAPIStatusLogins.self, flowLogsStatusJSON)
        #expect(
            decoded.SelfStatus?.CapMap?.names
                == Set([ClientLogUpload.dataPlaneAuditLogs, "https://tailscale.com/cap/is-admin"]))
        // The login overlay still reads the same answer.
        #expect(LoginOverlay(decoded).selfLogin == "alice@corp.com")

        let bare = try decode(LocalAPIStatusLogins.self, #"{"Self": {"ID": "nSELF"}}"#)
        #expect(bare.SelfStatus?.CapMap == nil, "a row without capabilities has none")
    }

    /// The status reports what the process does, and a refusal only when
    /// the node does not upload AND the tailnet requires client logs.
    @Test func statusReportsTheUploadAndARefusalForFlowLogs() throws {
        let flowLogs = try decode(LocalAPIStatusLogins.self, flowLogsStatusJSON)
        let plain = try decode(LocalAPIStatusLogins.self, plainStatusJSON)
        let base = BackendStatus(running: false, tailscaleId: "nSELF")

        let refused = ClientLogUpload.applied(to: base, uploading: false, rows: flowLogs)
        #expect(refused.clientLogUpload == false)
        #expect(refused.loggingRequired)

        let uploading = ClientLogUpload.applied(to: base, uploading: true, rows: flowLogs)
        #expect(uploading.clientLogUpload)
        #expect(uploading.loggingRequired == false, "a node that uploads is never refused for it")

        let noFlowLogs = ClientLogUpload.applied(to: base, uploading: false, rows: plain)
        #expect(noFlowLogs.clientLogUpload == false)
        #expect(noFlowLogs.loggingRequired == false, "no capability is no refusal")

        let failedRead = ClientLogUpload.applied(to: base, uploading: false, rows: nil)
        #expect(failedRead.loggingRequired == false, "a failed read reports no refusal")
    }

    /// A backend that knows nothing of the switch reports Tailscale's default.
    @Test func aStatusWithoutTheSwitchReportsUploads() {
        let status = BackendStatus()
        #expect(status.clientLogUpload)
        #expect(status.loggingRequired == false)
    }

    /// End to end on a loopback node: the node reports what its backend
    /// reports, a refused node fails its phase and says so once, and
    /// `waitUntilRunning` stops waiting for a start that cannot come.
    @Test func aRefusedNodeFailsAndSaysSoOnce() async throws {
        let network = LoopbackNetwork()
        let refusal = Refusal()
        let hostname = Hostname.tailscaleHostname(
            appId: try AppId(parsing: "demo"), deviceName: DeviceName("Phone"))
        let backend = OptedOutBackend(
            inner: await network.join(tailscaleId: "ts-phone", hostname: hostname),
            refusal: refusal)
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("truffle-client-log-test-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: dir) }

        let node = try await MeshNode.start(
            MeshConfiguration(
                appId: "demo", deviceName: "Phone", stateDirectory: dir,
                clientLogUpload: false),
            backend: backend,
            frameTransport: LengthPrefixFrameTransport())
        #expect(await node.phase == .running)
        #expect(await node.clientLogUpload == false)
        #expect(await node.loggingRequired == false)

        let events = await node.events
        let notices = Task {
            var messages: [String] = []
            for await event in events {
                if case .health(let message) = event { messages.append(message) }
            }
            return messages
        }

        await refusal.set()
        try await node.refresh()
        try await node.refresh()
        #expect(await node.phase == .failed)
        #expect(await node.loggingRequired)
        await #expect(throws: MeshError.loggingRequired) {
            try await node.waitUntilRunning(timeout: .seconds(2))
        }

        // stop() finishes the event stream, so the collected notices are final.
        await node.stop()
        let refusals = await notices.value.filter { $0.contains("requires client logging") }
        #expect(refusals.count == 1, "the refusal is reported once, not on every status")
    }
}

/// Whether the tailnet has refused the test node for its logs.
private actor Refusal {
    private(set) var isSet = false
    func set() { isSet = true }
}

/// A loopback backend that reports what `TailscaleKitBackend` reports for a
/// node that turned its client-log upload off. Once refused, it reports what
/// Tailscale does to such a node on a tailnet with flow logs on: it stops it.
private struct OptedOutBackend: NetworkBackend {
    let inner: LoopbackBackend
    let refusal: Refusal

    func start() async throws { try await inner.start() }
    func stop() async { await inner.stop() }
    func status() async throws -> BackendStatus {
        var status = try await inner.status()
        status.clientLogUpload = false
        if await refusal.isSet {
            status.running = false
            status.loggingRequired = true
        }
        return status
    }
    func events() async -> AsyncStream<BackendEvent> { await inner.events() }
    func dial(host: String, port: UInt16) async throws -> any MeshConnection {
        try await inner.dial(host: host, port: port)
    }
    func listen(port: UInt16) async throws -> any MeshListener {
        try await inner.listen(port: port)
    }
    func whoIs(remoteEndpoint: String) async throws -> AuthenticatedPeer {
        try await inner.whoIs(remoteEndpoint: remoteEndpoint)
    }
    func makeURLSession(configuration: URLSessionConfiguration) async throws -> URLSession {
        try await inner.makeURLSession(configuration: configuration)
    }
}
