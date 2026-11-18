import Foundation
import Truffle

/// Whether the embedded node uploads its Tailscale client logs, and whether
/// its tailnet stopped it for not uploading them.
///
/// `MeshConfiguration.clientLogUpload` reaches TailscaleKit as
/// `Configuration.clientLogUpload`, which turns the upload off before the node
/// starts, as the sidecar's `clientLogUpload: false` does: `logtail.Disable()`
/// and `envknob.SetNoLogsNoSupport()`. Those switches are process-wide and
/// one-way, so the status reports what the process does
/// (`TailscaleNode.clientLogUpload`), not what one node asked for.
///
/// A tailnet with network flow logs on grants its nodes the
/// data-plane-audit-logs capability, and Tailscale stops a node that does not
/// upload its logs there. The capability is read from the `Self` row of the
/// same `/localapi/v0/status` read the login overlay decodes, as the sidecar's
/// `loggingRequired` reads it; that row keeps the last network map after the
/// node stops.
///
/// Like `LocalAPIIdentity`, this lives outside `TailscaleKitBackend.swift`'s
/// `#if os(iOS) && canImport(TailscaleKit)` so the macOS test target pins it.
enum ClientLogUpload {
    /// `tailcfg.CapabilityDataPlaneAuditLogs`: network flow logs are on, and
    /// the tailnet requires client logs.
    static let dataPlaneAuditLogs = "https://tailscale.com/cap/data-plane-audit-logs"

    /// What TailscaleKit's `Configuration.clientLogUpload` gets.
    static func requested(by configuration: MeshConfiguration) -> Bool {
        configuration.clientLogUpload
    }

    /// Merge onto a mapped status. `uploading` is what TailscaleKit reports
    /// for the process; `rows` is the status read, `nil` when it failed. A
    /// failed read reports no refusal: absent, never fabricated.
    static func applied(
        to status: BackendStatus, uploading: Bool, rows: LocalAPIStatusLogins?
    ) -> BackendStatus {
        var merged = status
        merged.clientLogUpload = uploading
        let flowLogs = rows?.SelfStatus?.CapMap?.names.contains(dataPlaneAuditLogs) ?? false
        merged.loggingRequired = !uploading && flowLogs
        return merged
    }
}

/// The names in a capability map (`tailcfg.NodeCapMap`, a JSON object keyed
/// by capability). The values are not read: Go writes `null` for a capability
/// without values, and anything else for one with them.
struct CapabilityNames: Decodable, Equatable {
    let names: Set<String>

    init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: Name.self)
        names = Set(container.allKeys.map(\.stringValue))
    }

    private struct Name: CodingKey {
        let stringValue: String
        var intValue: Int? { nil }

        init?(stringValue: String) {
            self.stringValue = stringValue
        }

        init?(intValue: Int) {
            return nil
        }
    }
}
