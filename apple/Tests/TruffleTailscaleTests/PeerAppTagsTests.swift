import Foundation
import Testing
import Truffle

@testable import TruffleTailscale

/// T8: the Swift node reads each peer's `Hostinfo.App` over the LocalAPI
/// loopback (`peer-by-id`), once per (node ID, hostname). Pinned from literal
/// JSON, like the login overlay: `TailscaleKitBackend` compiles only on iOS.
private func decode<T: Decodable>(_ type: T.Type, _ json: String) throws -> T {
    try JSONDecoder().decode(type, from: Data(json.utf8))
}

private let statusJSON = """
    {
      "Self": {"ID": "nSELF", "UserID": 1, "NodeID": 1, "HostName": "truffle-demo-phone"},
      "Peer": {
        "key-a": {"ID": "nA", "UserID": 1, "NodeID": 101, "HostName": "vibefield-studio"},
        "key-b": {"ID": "nB", "UserID": 1, "NodeID": 102, "HostName": "truffle-demo-laptop"},
        "key-c": {"ID": "nC", "UserID": 1, "HostName": "no-node-id"}
      },
      "User": {"1": {"LoginName": "alice@corp.com"}}
    }
    """

@Suite struct PeerAppTagsTests {
    @Test func statusRowsCarryTheNodeIdAndHostname() throws {
        let decoded = try decode(LocalAPIStatusLogins.self, statusJSON)
        let row = try #require(decoded.Peer?["key-a"])
        #expect(row.NodeID == 101)
        #expect(row.HostName == "vibefield-studio")
        // The login overlay still reads the same answer.
        #expect(LoginOverlay(decoded).byStableNodeId["nA"] == "alice@corp.com")
    }

    @Test func peerByIdYieldsTheApp() throws {
        let tagged = try decode(
            PeerNodeAppAnswer.self,
            #"{"ID": 101, "StableID": "nA", "Hostinfo": {"OS": "macOS", "App": "truffle:demo"}}"#)
        #expect(tagged.app == "truffle:demo")
        let none = try decode(PeerNodeAppAnswer.self, #"{"ID": 102, "Hostinfo": {"OS": "iOS"}}"#)
        #expect(none.app == nil)
        let empty = try decode(PeerNodeAppAnswer.self, #"{"ID": 103, "Hostinfo": {"App": ""}}"#)
        #expect(empty.app == nil, "an empty tag is no tag")
        let bare = try decode(PeerNodeAppAnswer.self, #"{"ID": 104}"#)
        #expect(bare.app == nil, "a node sent without Hostinfo has no app")
    }

    /// Every peer with a node ID is looked up once; a recorded answer (a tag
    /// or none) is not asked again until the peer's hostname changes.
    @Test func looksEachPeerUpOnceAndAgainWhenItIsRenamed() throws {
        let decoded = try decode(LocalAPIStatusLogins.self, statusJSON)
        var cache = PeerAppCache()
        let first = cache.lookups(for: decoded).sorted { $0.nodeID < $1.nodeID }
        #expect(
            first == [
                PeerAppLookup(stableNodeId: "nA", nodeID: 101, hostname: "vibefield-studio"),
                PeerAppLookup(stableNodeId: "nB", nodeID: 102, hostname: "truffle-demo-laptop"),
            ], "a row with no node ID cannot be asked")
        try #require(first.count == 2)
        cache.record(first[0], app: "truffle:demo")
        cache.record(first[1], app: nil)
        #expect(cache.lookups(for: decoded).isEmpty, "an answer, even none, is kept")

        // The laptop upgrades and renames itself: its row is asked again.
        let renamed = try decode(
            LocalAPIStatusLogins.self,
            statusJSON.replacingOccurrences(
                of: "truffle-demo-laptop", with: "vibefield-laptop"))
        #expect(
            cache.lookups(for: renamed)
                == [PeerAppLookup(stableNodeId: "nB", nodeID: 102, hostname: "vibefield-laptop")])
    }

    /// The cache is the authority for `app` on the snapshot it is applied
    /// to: a peer it has no answer for gets `nil`, and departed peers are
    /// forgotten.
    @Test func appliesTagsAndForgetsDepartedPeers() throws {
        let decoded = try decode(LocalAPIStatusLogins.self, statusJSON)
        var cache = PeerAppCache()
        for lookup in cache.lookups(for: decoded) {
            cache.record(lookup, app: lookup.nodeID == 101 ? "truffle:demo" : nil)
        }
        let status = BackendStatus(
            running: true,
            peers: [
                BackendPeer(tailscaleId: "nA", hostname: "vibefield-studio", app: "stale"),
                BackendPeer(tailscaleId: "nB", hostname: "truffle-demo-laptop", app: "stale"),
                BackendPeer(tailscaleId: "nC", hostname: "no-node-id", app: "stale"),
            ])
        let applied = cache.applied(to: status)
        #expect(applied.peers.map(\.app) == ["truffle:demo", nil, nil])

        // Renamed, with its re-read not yet answered: no tag, never the old one.
        var renamed = status
        renamed.peers[0].hostname = "vibefield-studio-2"
        #expect(cache.applied(to: renamed).peers[0].app == nil)

        let gone = try decode(
            LocalAPIStatusLogins.self,
            #"{"Peer": {"key-b": {"ID": "nB", "NodeID": 102, "HostName": "truffle-demo-laptop"}}}"#)
        cache.prune(to: gone)
        #expect(cache.lookups(for: decoded).map(\.stableNodeId) == ["nA"], "nA was forgotten")
    }
}
