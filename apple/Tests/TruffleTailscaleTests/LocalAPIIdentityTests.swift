import Foundation
import Testing

@testable import TruffleTailscale

/// The LocalAPI identity decoders (RFC 025 §3.3/§3.6), driven from literal
/// JSON so the mapping is pinned without a node, a tailnet, or a device.
///
/// These live in `TruffleTailscaleTests` and not beside `TailscaleKitBackend`
/// because that file is `#if os(iOS) && canImport(TailscaleKit)` and compiles
/// to nothing on the macOS host the test target runs on — the same reason
/// `TailscaleEndpoint` is its own file. The decoding is the part that can be
/// wrong in a way a test can catch.
private func decode<T: Decodable>(_ type: T.Type, _ json: String) throws -> T {
    try JSONDecoder().decode(type, from: Data(json.utf8))
}

@Suite struct WhoIsResponseTests {
    @Test func carriesLoginAndDisplayNameWhenTheProfileIsPresent() throws {
        let decoded = try decode(
            WhoIsResponse.self,
            """
            {
              "Node": {
                "ID": 4711,
                "StableID": "nABC123",
                "Name": "truffle-demo-alice.corp.ts.net.",
                "Addresses": ["100.64.0.2/32", "fd7a:115c:a1e0::2/128"]
              },
              "UserProfile": {
                "ID": 12345,
                "LoginName": "alice@corp.com",
                "DisplayName": "Alice Example",
                "ProfilePicURL": "https://example.com/a.png"
              }
            }
            """)
        #expect(decoded.Node.StableID == "nABC123")
        #expect(decoded.Node.Addresses == ["100.64.0.2/32", "fd7a:115c:a1e0::2/128"])
        #expect(decoded.loginName == "alice@corp.com")
        #expect(decoded.displayName == "Alice Example")
    }

    /// The shape #188 already handled: no profile at all. The node ID still
    /// decodes and the identity fields stay absent rather than becoming "".
    @Test func hasNoLoginWhenTheProfileIsAbsent() throws {
        let decoded = try decode(
            WhoIsResponse.self,
            """
            {"Node": {"ID": 1, "StableID": "nNOPROFILE", "Addresses": ["100.64.0.3/32"]}}
            """)
        #expect(decoded.Node.StableID == "nNOPROFILE")
        #expect(decoded.loginName == nil)
        #expect(decoded.displayName == nil)
    }

    /// An empty string on the wire is not an identity (RFC 022's honesty
    /// rule; RFC 025 §3.2 fails closed on an empty login).
    @Test func emptyProfileStringsBecomeNil() throws {
        let decoded = try decode(
            WhoIsResponse.self,
            """
            {
              "Node": {"ID": 2, "StableID": "nEMPTY", "Addresses": []},
              "UserProfile": {"ID": 0, "LoginName": "", "DisplayName": ""}
            }
            """)
        #expect(decoded.loginName == nil)
        #expect(decoded.displayName == nil)
    }

    /// A tagged node's pseudo-login is passed through, never special-cased —
    /// it only matches a glob that names it (RFC 025 §3.2).
    @Test func taggedDevicesLoginIsPassedThrough() throws {
        let decoded = try decode(
            WhoIsResponse.self,
            """
            {
              "Node": {"ID": 3, "StableID": "nTAGGED", "Addresses": ["100.64.0.4/32"]},
              "UserProfile": {"ID": 99, "LoginName": "tagged-devices", "DisplayName": "Tagged"}
            }
            """)
        #expect(decoded.loginName == "tagged-devices")
    }

    /// Missing `Addresses` stays `nil` rather than failing the decode — the
    /// tolerant-reader rule, and the shape #188's address check relies on.
    @Test func absentAddressesDecodeAsNil() throws {
        let decoded = try decode(
            WhoIsResponse.self, #"{"Node": {"ID": 5, "StableID": "nNOADDR"}}"#)
        #expect(decoded.Node.Addresses == nil)
    }
}

@Suite struct LoginOverlayTests {
    /// A realistic `/localapi/v0/status` answer: `Peer` is keyed by node key
    /// while each row's own `ID` is the stable node ID `BackendPeer` uses,
    /// and `User` is keyed by the STRINGIFIED numeric user id.
    private static let statusJSON = """
        {
          "Version": "1.102.3",
          "BackendState": "Running",
          "AuthURL": "",
          "TailscaleIPs": ["100.64.0.1"],
          "Self": {
            "ID": "nSELF", "UserID": 12345,
            "HostName": "truffle-demo-alice", "Online": true
          },
          "Peer": {
            "nodekey:aaaa": {
              "ID": "nBOB", "UserID": 12345,
              "HostName": "truffle-demo-bob", "Online": true
            },
            "nodekey:bbbb": {
              "ID": "nMALLORY", "UserID": 67890,
              "HostName": "truffle-demo-mallory", "Online": true
            },
            "nodekey:cccc": {
              "ID": "nORPHAN", "UserID": 55555,
              "HostName": "truffle-demo-orphan", "Online": true
            },
            "nodekey:dddd": {
              "ID": "nNOUSER",
              "HostName": "truffle-demo-nouser", "Online": true
            }
          },
          "User": {
            "12345": {"ID": 12345, "LoginName": "alice@corp.com", "DisplayName": "Alice"},
            "67890": {"ID": 67890, "LoginName": "mallory@evil.com", "DisplayName": "Mallory"}
          }
        }
        """

    private func overlay() throws -> LoginOverlay {
        LoginOverlay(try decode(LocalAPIStatusLogins.self, Self.statusJSON))
    }

    @Test func resolvesSelfAndPeerLoginsThroughTheUserMap() throws {
        let overlay = try overlay()
        #expect(overlay.selfLogin == "alice@corp.com")
        #expect(overlay.byStableNodeId["nBOB"] == "alice@corp.com")
        #expect(overlay.byStableNodeId["nMALLORY"] == "mallory@evil.com")
        // A UserID with no profile in the map, and a row with no UserID at
        // all, contribute nothing — absent, never fabricated.
        #expect(overlay.byStableNodeId["nORPHAN"] == nil)
        #expect(overlay.byStableNodeId["nNOUSER"] == nil)
        #expect(overlay.byStableNodeId.count == 2)
    }

    @Test func mergesOntoAMappedStatusByStableNodeId() throws {
        let mapped = BackendStatus(
            running: true,
            dnsName: "truffle-demo-alice.corp.ts.net",
            tailnetIPs: ["100.64.0.1"],
            tailscaleId: "nSELF",
            peers: [
                BackendPeer(tailscaleId: "nBOB", hostname: "truffle-demo-bob"),
                BackendPeer(tailscaleId: "nMALLORY", hostname: "truffle-demo-mallory"),
                BackendPeer(tailscaleId: "nNOUSER", hostname: "truffle-demo-nouser"),
            ])
        let merged = try overlay().applied(to: mapped)

        #expect(merged.loginName == "alice@corp.com")
        #expect(merged.peers.map(\.loginName) == ["alice@corp.com", "mallory@evil.com", nil])
        // Everything the overlay does not own is carried through untouched.
        #expect(merged.tailscaleId == "nSELF")
        #expect(merged.dnsName == "truffle-demo-alice.corp.ts.net")
        #expect(merged.tailnetIPs == ["100.64.0.1"])
        #expect(merged.peers.map(\.hostname) == mapped.peers.map(\.hostname))
        #expect(merged.peers.map(\.tailscaleId) == mapped.peers.map(\.tailscaleId))
        #expect(merged.running)
    }

    /// The overlay is the authority for the snapshot it was read with: a peer
    /// it has no login for is cleared, never left holding an earlier read's
    /// value. Otherwise a departed user's login could gate a new node in.
    @Test func clearsStaleLoginsItDoesNotOwn() throws {
        let stale = BackendStatus(
            tailscaleId: "nSELF",
            loginName: "someone-else@corp.com",
            peers: [
                BackendPeer(
                    tailscaleId: "nNOUSER", hostname: "truffle-demo-nouser",
                    loginName: "alice@corp.com")
            ])
        let merged = try overlay().applied(to: stale)
        #expect(merged.loginName == "alice@corp.com")
        #expect(merged.peers[0].loginName == nil)
    }

    /// A status with no `Self`, no `Peer` and no `User` decodes and yields
    /// nothing — the shape a not-yet-running backend returns.
    @Test func emptyStatusYieldsNoLogins() throws {
        let overlay = LoginOverlay(
            try decode(LocalAPIStatusLogins.self, #"{"BackendState": "NeedsLogin"}"#))
        #expect(overlay.selfLogin == nil)
        #expect(overlay.byStableNodeId.isEmpty)

        let merged = overlay.applied(
            to: BackendStatus(
                tailscaleId: "nSELF",
                peers: [BackendPeer(tailscaleId: "nBOB", hostname: "truffle-demo-bob")]))
        #expect(merged.loginName == nil)
        #expect(merged.peers[0].loginName == nil)
    }

    /// An empty `LoginName` in the user map is not a login.
    @Test func emptyLoginNameInTheUserMapIsAbsent() throws {
        let overlay = LoginOverlay(
            try decode(
                LocalAPIStatusLogins.self,
                """
                {
                  "Self": {"ID": "nSELF", "UserID": 7},
                  "Peer": {"k": {"ID": "nBOB", "UserID": 7}},
                  "User": {"7": {"ID": 7, "LoginName": "", "DisplayName": "Nameless"}}
                }
                """))
        #expect(overlay.selfLogin == nil)
        #expect(overlay.byStableNodeId["nBOB"] == nil)
    }
}
