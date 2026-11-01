import Foundation
import Truffle

/// T8: each peer's `Hostinfo.App`, read over the LocalAPI loopback.
///
/// TailscaleKit's `IpnState.PeerStatus` does not carry `Hostinfo.App`, and the
/// status JSON tsnet serves does not either. A peer's full `tailcfg.Node` does:
/// LocalAPI `GET /localapi/v0/peer-by-id?id=<NodeID>` answers it, and control
/// relays `App` to peers (measured on a real tailnet, 2026-11-01: a peer's row
/// keeps App, Hostname, OS and Services). The node ID comes from the same
/// `/localapi/v0/status` read the login overlay decodes.
///
/// Like `LocalAPIIdentity`, this lives outside `TailscaleKitBackend.swift`'s
/// `#if os(iOS) && canImport(TailscaleKit)` so the macOS test target pins it.

/// The part of a LocalAPI `peer-by-id` answer (a `tailcfg.Node`) this reads.
struct PeerNodeAppAnswer: Decodable, Equatable {
    struct HostinfoBody: Decodable, Equatable {
        let App: String?
    }

    let Hostinfo: HostinfoBody?

    /// `nil` for no Hostinfo, no `App`, or an empty one: absent, never "".
    var app: String? { LocalAPIIdentity.present(Hostinfo?.App) }
}

/// One `peer-by-id` read the cache wants.
struct PeerAppLookup: Equatable, Sendable {
    let stableNodeId: String
    let nodeID: Int64
    let hostname: String
}

/// Each peer's app tag, read once per (node ID, hostname).
///
/// A recorded answer, a tag or none, is kept until the peer's hostname
/// changes: an upgrade that renames a peer to an app-chosen prefix is exactly
/// when its tag appears, so a rename re-reads. A failed read records nothing,
/// so the next refresh asks again; meanwhile the row has no tag and the
/// hostname prefix decides (`Hostname.isAppRow`).
struct PeerAppCache: Equatable, Sendable {
    private struct Entry: Equatable, Sendable {
        var nodeID: Int64
        var hostname: String
        var app: String?
    }

    private var byStableNodeId: [String: Entry] = [:]

    init() {}

    /// The peers of `decoded` with no answer for their current node ID and
    /// hostname. A row without a stable ID or a node ID cannot be asked.
    func lookups(for decoded: LocalAPIStatusLogins) -> [PeerAppLookup] {
        (decoded.Peer?.values ?? [String: LocalAPIStatusLogins.NodeRow]().values)
            .compactMap { row in
                guard let stableID = LocalAPIIdentity.present(row.ID), let nodeID = row.NodeID
                else { return nil }
                let hostname = row.HostName ?? ""
                if let entry = byStableNodeId[stableID], entry.nodeID == nodeID,
                    entry.hostname == hostname
                {
                    return nil
                }
                return PeerAppLookup(stableNodeId: stableID, nodeID: nodeID, hostname: hostname)
            }
    }

    /// Keep one read's answer (`nil` = the peer announces no app).
    mutating func record(_ lookup: PeerAppLookup, app: String?) {
        byStableNodeId[lookup.stableNodeId] = Entry(
            nodeID: lookup.nodeID, hostname: lookup.hostname, app: app)
    }

    /// Forget the peers `decoded` no longer lists.
    mutating func prune(to decoded: LocalAPIStatusLogins) {
        let present = Set(
            (decoded.Peer?.values ?? [String: LocalAPIStatusLogins.NodeRow]().values)
                .compactMap { LocalAPIIdentity.present($0.ID) })
        byStableNodeId = byStableNodeId.filter { present.contains($0.key) }
    }

    /// Merge onto a mapped status. The cache is the authority for `app`: a
    /// peer it has no answer for under its CURRENT hostname gets `nil`, never
    /// a stale value (a renamed peer whose re-read failed has no tag).
    func applied(to status: BackendStatus) -> BackendStatus {
        var merged = status
        merged.peers = status.peers.map { peer in
            var row = peer
            let entry = byStableNodeId[peer.tailscaleId]
            row.app = entry?.hostname == peer.hostname ? entry?.app : nil
            return row
        }
        return merged
    }
}
