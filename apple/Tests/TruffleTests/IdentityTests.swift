import Foundation
import Testing

@testable import Truffle

// MARK: - AppId (parity with truffle-core identity.rs)

@Suite struct AppIdTests {
    @Test func acceptsValidIds() throws {
        for ok in ["ab", "chat", "field-tools", "a1", "app-2x", "a" + String(repeating: "b", count: 31)] {
            _ = try AppId(parsing: ok)
        }
    }

    @Test func rejectsInvalidIds() {
        let bad = [
            "", "a",                       // too short
            "a" + String(repeating: "b", count: 32),  // too long (33)
            "1app", "-app", "Aapp",        // bad first char
            "app_x", "app x", "appé",      // bad charset
            "app-",                        // trailing hyphen (desktop rule)
        ]
        for s in bad {
            #expect(throws: MeshError.invalidAppId(s)) { try AppId(parsing: s) }
        }
    }
}

// MARK: - DeviceId (ULID)

@Suite struct DeviceIdTests {
    @Test func generateProducesValidUlid() throws {
        let id = DeviceId.generate()
        #expect(id.value.count == 26)
        let reparsed = try DeviceId(parsing: id.value)
        #expect(reparsed == id)
    }

    @Test func parsesKnownRustUlid() throws {
        // ULID from truffle-core hello.rs tests.
        let id = try DeviceId(parsing: "01J4K9M2Z8AB3RNYQPW6H5TC0X")
        #expect(id.value == "01J4K9M2Z8AB3RNYQPW6H5TC0X")
    }

    @Test func parseIsCaseInsensitiveCanonicalUppercase() throws {
        let id = try DeviceId(parsing: "01j4k9m2z8ab3rnyqpw6h5tc0x")
        #expect(id.value == "01J4K9M2Z8AB3RNYQPW6H5TC0X")
    }

    @Test func rejectsInvalidUlids() {
        for bad in ["", "0123", "01J4K9M2Z8AB3RNYQPW6H5TC0I", "81J4K9M2Z8AB3RNYQPW6H5TC0X",
                    String(repeating: "0", count: 25), String(repeating: "0", count: 27)] {
            #expect(throws: MeshError.self) { try DeviceId(parsing: bad) }
        }
    }

    @Test func timestampOrderingIsPreserved() {
        // ULIDs generated at strictly increasing millisecond timestamps sort
        // lexicographically (Crockford base32 preserves numeric order).
        let a = DeviceId.generate(now: Date(timeIntervalSince1970: 1_000))
        let b = DeviceId.generate(now: Date(timeIntervalSince1970: 2_000))
        #expect(a.value < b.value)
    }

    @Test func persistenceRoundTrips() throws {
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("truffle-test-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: dir) }

        let first = try DeviceId.loadOrCreate(stateDirectory: dir)
        let second = try DeviceId.loadOrCreate(stateDirectory: dir)
        #expect(first == second)

        let onDisk = try String(
            contentsOf: dir.appendingPathComponent(DeviceId.stateFileName), encoding: .utf8)
        #expect(onDisk.trimmingCharacters(in: .whitespacesAndNewlines) == first.value)
    }

    @Test func corruptPersistedIdThrowsInsteadOfRotating() throws {
        // Desktop parity (node.rs "device-id.txt contains an invalid
        // ULID"): durable identity must fail loudly, never silently mint a
        // new identity over a corrupt file.
        let dir = FileManager.default.temporaryDirectory
            .appendingPathComponent("truffle-test-\(UUID().uuidString)")
        defer { try? FileManager.default.removeItem(at: dir) }
        try FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        let file = dir.appendingPathComponent(DeviceId.stateFileName)
        try Data("not-a-ulid".utf8).write(to: file)

        #expect(throws: MeshError.self) {
            _ = try DeviceId.loadOrCreate(stateDirectory: dir)
        }
        // The corrupt file is untouched — no rotation happened.
        let onDisk = try String(contentsOf: file, encoding: .utf8)
        #expect(onDisk == "not-a-ulid")
    }
}

// MARK: - DeviceName

@Suite struct DeviceNameTests {
    @Test func passesShortNamesThrough() {
        #expect(DeviceName("Alice's MacBook Pro").value == "Alice's MacBook Pro")
    }

    @Test func truncatesAt256Scalars() {
        let long = String(repeating: "x", count: 300)
        #expect(DeviceName(long).value.count == 256)
    }
}

// MARK: - Slug / hostname (pipeline parity with identity.rs §5.3)

@Suite struct SlugTests {
    @Test func alicesMacbookPro() {
        // Same expectation as the Rust test slug_alice_s_macbook_pro.
        #expect(Hostname.slug("Alice's MacBook Pro", budget: 63) == "alice-s-macbook-pro")
    }

    @Test func collapsesAndTrimsHyphens() {
        #expect(Hostname.slug("--a  b--", budget: 63) == "a-b")
        #expect(Hostname.slug("a———b", budget: 63) == "a-b")
    }

    @Test func lowercasesAndStripsDiacritics() {
        #expect(Hostname.slug("Café Über", budget: 63) == "cafe-uber")
    }

    @Test func unmappableInputFallsBackToHash() {
        // Pure-emoji input produces no ASCII; the fallback hash kicks in.
        let s = Hostname.slug("🦄🦄🦄", budget: 63)
        #expect(!s.isEmpty)
        #expect(s.count >= 2)
        #expect(s.allSatisfy { ($0.isLowercase && $0.isASCII) || $0.isNumber || $0 == "-" })
    }

    @Test func respectsBudget() {
        let s = Hostname.slug(String(repeating: "a", count: 100), budget: 10)
        #expect(s.count == 10)
    }

    @Test func budgetZeroIsEmpty() {
        #expect(Hostname.slug("anything", budget: 0) == "")
    }

    @Test func composesHostnameWithinDnsLimit() throws {
        let appId = try AppId(parsing: "field-tools")
        let host = Hostname.tailscaleHostname(
            appId: appId, deviceName: DeviceName("Alice's iPhone 15 Pro Max With A Very Long Name Indeed"))
        #expect(host.hasPrefix("truffle-field-tools-"))
        #expect(host.count <= Hostname.dnsLabelLimit)
        #expect(Hostname.isAppPeer(hostname: host, appId: "field-tools"))
    }

    @Test func isAppPeerParity() {
        // Mirrors provider.rs::is_app_peer semantics.
        #expect(Hostname.isAppPeer(hostname: "truffle-demo-dev", appId: "demo"))
        #expect(!Hostname.isAppPeer(hostname: "truffle-demo-", appId: "demo"))   // empty slug
        #expect(!Hostname.isAppPeer(hostname: "truffle-demo", appId: "demo"))
        #expect(!Hostname.isAppPeer(hostname: "truffle-other-dev", appId: "demo"))
        #expect(!Hostname.isAppPeer(hostname: "laptop", appId: "demo"))
    }
}

// MARK: - LoginGlob (RFC 025 §3.2 — one grammar, three planes)

/// Both tables are reproduced verbatim from the Rust port
/// (`crates/truffle-core/src/network/login_allow.rs`), which in turn
/// reproduces the Go reference (`TestAllowedLogin` / `path.Match`'s `TestMatch`
/// in `sidecar-slim`). If a row here disagrees with a row there, one of the
/// three planes has drifted and the gate is no longer one grammar.
@Suite struct LoginGlobTests {
    @Test func allowedLoginMatchesTheGoTable() {
        let cases: [(name: String, globs: [String], login: String, want: Bool)] = [
            ("empty globs allow all", [], "anyone@example.com", true),
            ("empty globs allow even empty login", [], "", true),
            ("non-empty gate, empty login fails closed", ["*@corp.com"], "", false),
            ("exact match", ["alice@corp.com"], "alice@corp.com", true),
            ("exact non-match", ["alice@corp.com"], "bob@corp.com", false),
            ("domain glob matches", ["*@corp.com"], "alice@corp.com", true),
            ("domain glob rejects other domain", ["*@corp.com"], "alice@evil.com", false),
            ("case-insensitive glob vs login", ["*@CORP.com"], "Alice@corp.COM", true),
            ("case-insensitive exact", ["Alice@Corp.Com"], "alice@corp.com", true),
            ("second glob in list matches", ["*@other.com", "*@corp.com"], "bob@corp.com", true),
            ("no glob in list matches", ["*@other.com", "*@more.com"], "bob@corp.com", false),
            ("star does not cross slash", ["*@corp.com"], "a/b@corp.com", false),
            ("invalid glob does not match", ["[unterminated"], "alice@corp.com", false),
            ("invalid glob skipped, valid one still matches", ["[bad", "*@corp.com"],
                "alice@corp.com", true),
        ]
        for row in cases {
            #expect(
                LoginGlob.allowed(row.globs, login: row.login) == row.want,
                "\(row.name): allowed(\(row.globs), login: \"\(row.login)\")")
        }
        // `nil` is the absent login: fails closed under a gate, passes without one.
        #expect(!LoginGlob.allowed(["*@corp.com"], login: nil))
        #expect(LoginGlob.allowed([], login: nil))
        // A tagged node only passes a glob that names the pseudo-login.
        #expect(!LoginGlob.allowed(["*@corp.com"], login: "tagged-devices"))
        #expect(LoginGlob.allowed(["tagged-devices"], login: "tagged-devices"))
    }

    @Test func globMatchFollowsPathMatch() throws {
        func ok(_ p: String, _ n: String) throws -> Bool { try LoginGlob.match(p, n) }
        #expect(try ok("abc", "abc"))
        #expect(try ok("*", "abc"))
        #expect(try ok("*c", "abc"))
        #expect(try !ok("a*", "a/b"))
        #expect(try ok("a*", "ab"))
        #expect(try !ok("a*", "abc/d"))
        #expect(try ok("a*/b", "abc/b"))
        #expect(try !ok("a*/b", "a/c/b"))
        #expect(try ok("a*b*c*d*e*/f", "axbxcxdxe/f"))
        #expect(try ok("a*b*c*d*e*/f", "axbxcxdxexxx/f"))
        #expect(try !ok("a*b*c*d*e*/f", "axbxcxdxe/xxx/f"))
        #expect(try !ok("a*b*c*d*e*/f", "axbxcxdxexxx/fff"))
        #expect(try ok("a*b?c*x", "abxbbxdbxebxczzx"))
        #expect(try !ok("a*b?c*x", "abxbbxdbxebxczzy"))
        #expect(try ok("ab[c]", "abc"))
        #expect(try ok("ab[b-d]", "abc"))
        #expect(try !ok("ab[e-g]", "abc"))
        #expect(try !ok("ab[^c]", "abc"))
        #expect(try !ok("ab[^b-d]", "abc"))
        #expect(try ok("ab[^e-g]", "abc"))
        #expect(try ok("a\\*b", "a*b"))
        #expect(try !ok("a\\*b", "ab"))
        #expect(try ok("a?b", "a☺b"))
        #expect(try ok("a[^a]b", "a☺b"))
        #expect(try !ok("a???b", "a☺b"))
        #expect(try !ok("a[^a][^a][^a]b", "a☺b"))
        #expect(try ok("[a-ζ]*", "α"))
        #expect(try !ok("*[a-ζ]", "A"))
        #expect(try ok("a?b", "a/b") == false)
        #expect(try ok("a*b", "a/b") == false)
        #expect(try ok("[\\]a]", "]"))
        #expect(try ok("[\\-]", "-"))
        #expect(try ok("[x\\-]", "x"))
        #expect(try ok("[x\\-]", "-"))
        #expect(try !ok("[x\\-]", "z"))
        #expect(try ok("[\\-x]", "x"))
        #expect(try ok("[\\-x]", "-"))
        #expect(try !ok("[\\-x]", "a"))
        #expect(try ok("*x", "xxx"))
        #expect(try !ok("", "a"))
        #expect(try ok("", ""))
    }

    @Test func globMatchReportsBadPatternsLikeGo() {
        for bad in [
            "[]a]", "[-]", "[x-]", "[-x]", "\\", "[a-b-c]", "[", "[^", "[^bc", "a[",
            "[unterminated",
        ] {
            #expect(throws: LoginGlob.BadPattern.self, "\(bad) must be a bad pattern") {
                try LoginGlob.match(bad, "a")
            }
        }
        // A bad pattern is an error even when an earlier chunk already failed
        // to match — Go checks the remainder's syntax before answering false.
        #expect(throws: LoginGlob.BadPattern.self) {
            try LoginGlob.match("a*[", "b")
        }
    }
}
