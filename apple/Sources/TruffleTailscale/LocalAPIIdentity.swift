import Foundation

/// Pure decoding of the two LocalAPI answers that carry tailnet **identity**
/// (RFC 025 §3.3/§3.6): the WhoIs response for an accepted connection, and
/// the login half of the node status.
///
/// These types live outside `TailscaleKitBackend.swift`'s
/// `#if os(iOS) && canImport(TailscaleKit)` on purpose — the same reason
/// `TailscaleEndpoint` does. The decoding is the part that can be wrong in a
/// way tests can catch, and the macOS test target is the only place that runs.
///
/// ## Why the status logins are read separately
///
/// TailscaleKit's `IpnState.PeerStatus` models neither `UserID` nor a login,
/// and `IpnState.Status.SelfStatus` is a `PeerStatus` too, so
/// `status.User[String(peer.UserID)]` — the mapping RFC 025 §3.3 specifies —
/// cannot be expressed against the vendored binding at all: the field the map
/// is keyed by is dropped at decode. The JSON tsnet serves does carry it, so
/// the logins are read from the same `/localapi/v0/status` endpoint with a
/// decoder that keeps `UserID`, and merged onto the mapped `BackendStatus`.
/// Absent, never fabricated: a failed or partial read leaves the fields `nil`,
/// and a gated node then admits nobody (fail closed, RFC 025 §3.2).

// MARK: - WhoIs

/// A LocalAPI `/localapi/v0/whois` answer (`tailscale.com/client/tailscale/apitype`).
///
/// `UserProfile` is optional: a tagged node or an unresolvable caller has
/// none, and the fields stay `nil` rather than becoming empty strings.
struct WhoIsResponse: Decodable, Equatable {
    struct NodeInfo: Decodable, Equatable {
        let StableID: String
        let Addresses: [String]?
    }

    struct UserProfileInfo: Decodable, Equatable {
        let LoginName: String?
        let DisplayName: String?
    }

    let Node: NodeInfo
    let UserProfile: UserProfileInfo?

    /// The caller's login, or `nil` when absent or empty on the wire.
    var loginName: String? { LocalAPIIdentity.present(UserProfile?.LoginName) }
    /// The caller's profile name, or `nil` when absent or empty on the wire.
    var displayName: String? { LocalAPIIdentity.present(UserProfile?.DisplayName) }
}

// MARK: - Status logins

/// The login-bearing subset of a LocalAPI `/localapi/v0/status` answer.
///
/// Only the fields RFC 025 §3.3 needs are modelled; everything else in the
/// status keeps coming from TailscaleKit's own decode, which stays the
/// authority for the rest of `BackendStatus`.
struct LocalAPIStatusLogins: Decodable, Equatable {
    struct NodeRow: Decodable, Equatable {
        let ID: String?
        let UserID: Int64?
    }

    struct Profile: Decodable, Equatable {
        let LoginName: String?
        let DisplayName: String?
    }

    let SelfStatus: NodeRow?
    let Peer: [String: NodeRow]?
    let User: [String: Profile]?

    enum CodingKeys: String, CodingKey {
        case Peer, User
        case SelfStatus = "Self"
    }
}

/// The logins a status answer yields, keyed the way `BackendStatus` is: the
/// node's own login, and each peer's by **stable node ID**.
struct LoginOverlay: Equatable, Sendable {
    var selfLogin: String?
    var byStableNodeId: [String: String]

    init(selfLogin: String? = nil, byStableNodeId: [String: String] = [:]) {
        self.selfLogin = selfLogin
        self.byStableNodeId = byStableNodeId
    }

    /// Resolve every node row's `UserID` through the status's user map.
    /// A row with no `UserID`, no `ID`, or no matching profile contributes
    /// nothing — its peer keeps a `nil` login.
    init(_ decoded: LocalAPIStatusLogins) {
        func login(for row: LocalAPIStatusLogins.NodeRow?) -> String? {
            guard let userID = row?.UserID else { return nil }
            return LocalAPIIdentity.present(decoded.User?[String(userID)]?.LoginName)
        }
        selfLogin = login(for: decoded.SelfStatus)
        var byStableNodeId: [String: String] = [:]
        for row in decoded.Peer?.values ?? [String: LocalAPIStatusLogins.NodeRow]().values {
            guard let stableID = LocalAPIIdentity.present(row.ID), let name = login(for: row)
            else { continue }
            byStableNodeId[stableID] = name
        }
        self.byStableNodeId = byStableNodeId
    }

    /// Merge onto a mapped status. This overlay is the authority for the
    /// login fields of the snapshot it was read with: a peer it has no login
    /// for gets `nil`, never a stale value from an earlier read.
    func applied(to status: BackendStatus) -> BackendStatus {
        var merged = status
        merged.loginName = selfLogin
        merged.peers = status.peers.map { peer in
            var row = peer
            row.loginName = byStableNodeId[peer.tailscaleId]
            return row
        }
        return merged
    }
}

// MARK: - Shared helpers

enum LocalAPIIdentity {
    /// `nil` for an absent OR empty string: an empty login is not a login
    /// (RFC 025 §3.2 fails closed on it), and RFC 022's honesty rule forbids
    /// surfacing `""` as if it were an identity.
    static func present(_ value: String?) -> String? {
        guard let value, !value.isEmpty else { return nil }
        return value
    }
}
