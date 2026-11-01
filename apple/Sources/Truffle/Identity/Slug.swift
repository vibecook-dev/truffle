import CryptoKit
import Foundation

/// Hostname derivation (RFC 017 §5.3 / §4).
///
/// Implements the same 10-step sanitisation pipeline as
/// `truffle-core::identity::slug`. Two building blocks intentionally differ
/// from the Rust implementation — this is fine because a node only ever
/// derives *its own* hostname, and remote nodes validate just the
/// `truffle-{appId}-` prefix plus a non-empty slug (`is_app_peer`), never the
/// slug bytes themselves:
///
/// - Transliteration uses `CFStringTransform(toLatin)` instead of Rust's
///   `deunicode` (different ASCII approximations for some scripts).
/// - The fallback hash uses SHA-256 instead of BLAKE3 (both base36-encoded).
public enum Hostname {
    /// Prefix every truffle-managed Tailscale hostname starts with.
    public static let prefix = "truffle-"
    /// The DNS label length limit Tailscale enforces.
    public static let dnsLabelLimit = 63

    // MARK: slug (§5.3)

    /// Produce a Tailscale-safe slug from a raw device-name string, fitting
    /// within `budget` ASCII characters.
    public static func slug(_ raw: String, budget: Int) -> String {
        guard budget > 0 else { return "" }

        // Step 1: NFKC-normalise.
        let normalized = raw.precomposedStringWithCompatibilityMapping
        // Step 2: transliterate to ASCII where a mapping exists.
        let transliterated = transliterate(normalized)
        // Step 3: lowercase.
        let lowered = transliterated.lowercased()
        // Step 4: replace non-[a-z0-9] with '-'.
        let replaced = String(
            lowered.map { c -> Character in
                if let ascii = c.asciiValue,
                    (ascii >= UInt8(ascii: "a") && ascii <= UInt8(ascii: "z"))
                        || (ascii >= UInt8(ascii: "0") && ascii <= UInt8(ascii: "9"))
                {
                    return c
                }
                return "-"
            })
        // Step 5: collapse runs of '-'.
        let collapsed = replaced.split(separator: "-", omittingEmptySubsequences: true)
            .joined(separator: "-")
        // Step 6: trim leading/trailing '-' (already handled by the split above).
        let trimmed = collapsed

        // Step 7: if empty, use the hash fallback.
        var candidate = trimmed.isEmpty ? fallbackHash(raw, length: 8) : trimmed

        // Step 8: truncate to budget.
        if candidate.count > budget {
            candidate = String(candidate.prefix(budget))
        }
        // Step 9: trim trailing '-' post-truncation.
        while candidate.hasSuffix("-") {
            candidate.removeLast()
        }
        // Step 10: if the result would be <2 chars, append hash characters.
        if candidate.count < 2 {
            let hashLen = min(6, max(0, budget - candidate.count))
            candidate += fallbackHash(raw, length: hashLen)
            if candidate.count > budget {
                candidate = String(candidate.prefix(budget))
            }
            while candidate.hasSuffix("-") {
                candidate.removeLast()
            }
        }
        return candidate
    }

    // MARK: hostname composition (§5.3)

    /// Compose the final Tailscale hostname `truffle-{appId}-{slug}`.
    /// Budget = 63 − len("truffle-") − len(appId) − 1 (separating hyphen).
    public static func tailscaleHostname(appId: AppId, deviceName: DeviceName) -> String {
        let fixedPrefixLen = prefix.count + appId.value.count + 1
        let budget = max(0, dnsLabelLimit - fixedPrefixLen)
        let slugPart = slug(deviceName.value, budget: budget)
        return "\(prefix)\(appId.value)-\(slugPart)"
    }

    /// Check if a hostname belongs to a truffle node in the given app
    /// (RFC 017 §4). Mirrors `provider.rs::is_app_peer`: requires the
    /// `truffle-{appId}-` prefix AND at least one character of slug after it,
    /// so `truffle-playground-` (empty slug) cannot masquerade as a peer.
    public static func isAppPeer(hostname: String, appId: String) -> Bool {
        let expected = "\(prefix)\(appId)-"
        return hostname.count > expected.count && hostname.hasPrefix(expected)
    }

    // MARK: app membership (T8)

    /// The prefix of the tag a truffle node announces in `Hostinfo.App`.
    public static let appTagPrefix = "truffle:"

    /// The tag a node of `appId` announces in `Hostinfo.App`: `truffle:{appId}`.
    public static func appTag(appId: String) -> String {
        "\(appTagPrefix)\(appId)"
    }

    /// Whether one Layer 3 row belongs to this app (T8). Mirrors
    /// `provider.rs::is_app_row`.
    ///
    /// A row that carries a tag decides by it: `Hostinfo.App` must be exactly
    /// `truffle:{appId}`, whatever the hostname, so a peer may carry an
    /// app-chosen prefix and a node of another app under the same prefix
    /// stays out. A row with no tag (a peer before sidecar protocol 12, or a
    /// backend that could not read it) falls back to the `truffle-{appId}-`
    /// prefix (`isAppPeer`). An empty tag is the same fact as none.
    ///
    /// The tag is the peer's own claim, exactly as its hostname is: it widens
    /// nothing a same-tailnet node could not already claim by its name.
    public static func isAppRow(hostname: String, app: String?, appId: String) -> Bool {
        if let app, !app.isEmpty {
            return app == appTag(appId: appId)
        }
        return isAppPeer(hostname: hostname, appId: appId)
    }

    // MARK: helpers

    /// Transliterate to ASCII via CoreFoundation's Latin transform, then
    /// strip combining marks. Characters with no mapping fall through and are
    /// rewritten to hyphens in step 4.
    static func transliterate(_ s: String) -> String {
        let mutable = NSMutableString(string: s)
        CFStringTransform(mutable, nil, kCFStringTransformToLatin, false)
        CFStringTransform(mutable, nil, kCFStringTransformStripCombiningMarks, false)
        return mutable as String
    }

    /// Deterministic hash fallback: first `length` characters of the base36
    /// encoding of SHA-256(raw).
    static func fallbackHash(_ raw: String, length: Int) -> String {
        let digest = SHA256.hash(data: Data(raw.utf8))
        let encoded = base36Encode(Array(digest))
        return String(encoded.prefix(length))
    }

    /// Encode a byte slice as lowercase base36 (big-endian long division).
    /// Direct port of `truffle-core::identity::base36_encode`.
    static func base36Encode(_ bytes: [UInt8]) -> String {
        if bytes.allSatisfy({ $0 == 0 }) { return "0" }
        var n: [UInt16] = bytes.map { UInt16($0) }
        var out = ""
        while !n.isEmpty {
            var rem: UInt16 = 0
            var newN: [UInt16] = []
            newN.reserveCapacity(n.count)
            var started = false
            for d in n {
                let cur = rem * 256 + d
                let q = cur / 36
                rem = cur % 36
                if started || q != 0 {
                    newN.append(q)
                    started = true
                }
            }
            let digit = UInt8(rem)
            let c: Character =
                digit < 10
                ? Character(UnicodeScalar(UInt8(ascii: "0") + digit))
                : Character(UnicodeScalar(UInt8(ascii: "a") + digit - 10))
            out.append(c)
            n = newN
        }
        return String(out.reversed())
    }
}
