/// Login allow-lists (RFC 025 §3.2).
///
/// A node may declare which tailnet **logins** may join its session plane.
/// The list is a set of shell-style globs evaluated against a caller's WhoIs
/// `loginName` — the same gate the Go sidecar applies to served routes
/// (RFC 023 §9.7, `allowedLogin` in `sidecar-slim/main.go`) and the Rust core
/// applies to its peer filter and hello (`network/login_allow.rs`). One
/// grammar and one test table cover all three planes.
///
/// The grammar is Go's `path.Match`, applied after lowercasing both sides:
///
/// - `*` matches any run (including empty) of characters other than `/`;
/// - `?` matches exactly one character other than `/`;
/// - `[abc]`, `[a-z]`, `[^abc]` character classes (ranges, negation, `\`
///   escapes inside);
/// - `\x` matches `x` literally;
/// - a malformed pattern (an unterminated class, a trailing `\`, an empty or
///   reversed range) never matches and never traps.
///
/// An **empty list means no gate**. A non-empty list against an **absent or
/// empty login fails closed** — tagged nodes report Tailscale's
/// `tagged-devices` pseudo-login and only match a glob that names it.
///
/// Matching walks Unicode **scalars**, not grapheme clusters, so `?` and the
/// class ranges count and order exactly what Go's `rune` and Rust's `char`
/// count and order.
public enum LoginGlob {
    /// A pattern `path.Match` would reject with `ErrBadPattern`.
    public struct BadPattern: Error, Equatable, CustomStringConvertible, Sendable {
        public init() {}
        public var description: String { "syntax error in login glob" }
    }

    /// The gate: does `login` pass `globs`?
    ///
    /// `globs` empty → `true` (no gate). `login` `nil` or empty with a
    /// non-empty list → `false` (fail closed). Otherwise `true` iff at least
    /// one glob matches, case-insensitively; malformed globs are skipped.
    public static func allowed(_ globs: [String], login: String?) -> Bool {
        if globs.isEmpty { return true }
        guard let login, !login.isEmpty else { return false }
        let lowered = login.lowercased()
        return globs.contains { glob in
            ((try? match(glob.lowercased(), lowered)) ?? false)
        }
    }

    /// A faithful port of Go's `path.Match(pattern, name)`: case-sensitive,
    /// `*` and `?` never cross `/`. Callers wanting the gate's semantics use
    /// ``allowed(_:login:)``, which lowercases and treats a throw as
    /// "no match".
    public static func match(_ pattern: String, _ name: String) throws -> Bool {
        var pattern = ArraySlice(Array(pattern.unicodeScalars))
        var name = ArraySlice(Array(name.unicodeScalars))

        patternLoop: while !pattern.isEmpty {
            let (star, chunk, rest) = scanChunk(pattern)
            pattern = rest
            if star && chunk.isEmpty {
                // A trailing `*` matches the rest of the name unless it has a `/`.
                return !name.contains("/")
            }
            // Look for a match at the current position.
            let (t, ok, err) = matchChunk(chunk, name)
            // If this is the last chunk, the name must be exhausted here;
            // otherwise a later chunk could still match via the star.
            if ok && (t.isEmpty || !pattern.isEmpty) {
                name = t
                continue
            }
            if err { throw BadPattern() }
            if star {
                // Look for a match skipping i+1 characters. Cannot skip `/`.
                let scalars = Array(name)
                var i = 0
                while i < scalars.count && scalars[i] != "/" {
                    let (t, ok, err) = matchChunk(chunk, name.dropFirst(i + 1))
                    if ok {
                        // If this is the last chunk, the name must be exhausted.
                        if pattern.isEmpty && !t.isEmpty {
                            i += 1
                            continue
                        }
                        name = t
                        continue patternLoop
                    }
                    if err { throw BadPattern() }
                    i += 1
                }
            }
            // Before answering "no match", check the remainder of the pattern
            // is syntactically valid (Go reports ErrBadPattern first).
            while !pattern.isEmpty {
                let (_, chunk, rest) = scanChunk(pattern)
                pattern = rest
                let (_, _, err) = matchChunk(chunk, ArraySlice<Unicode.Scalar>())
                if err { throw BadPattern() }
            }
            return false
        }
        return name.isEmpty
    }

    // MARK: - Go `path.Match` internals

    /// Split `pattern` into a leading run of `*`s, the next literal chunk (up
    /// to but not including the next unescaped `*` outside a class), and the
    /// rest.
    private static func scanChunk(
        _ pattern: ArraySlice<Unicode.Scalar>
    ) -> (star: Bool, chunk: ArraySlice<Unicode.Scalar>, rest: ArraySlice<Unicode.Scalar>) {
        var star = false
        var p = pattern
        while p.first == "*" {
            p = p.dropFirst()
            star = true
        }
        let scalars = Array(p)
        var inRange = false
        var i = 0
        scan: while i < scalars.count {
            switch scalars[i] {
            case "\\":
                // An escaped character never ends the chunk.
                if i + 1 < scalars.count { i += 1 }
            case "[":
                inRange = true
            case "]":
                inRange = false
            case "*":
                if !inRange { break scan }
            default:
                break
            }
            i += 1
        }
        return (star, p.prefix(i), p.dropFirst(i))
    }

    /// Match `chunk` (which has no `*`) against the start of `s`. Returns the
    /// remainder of `s`, whether it matched, and whether the chunk was
    /// malformed. Like Go, syntax is checked to the end of the chunk even
    /// after the match has already failed.
    private static func matchChunk(
        _ chunk: ArraySlice<Unicode.Scalar>, _ s: ArraySlice<Unicode.Scalar>
    ) -> (rest: ArraySlice<Unicode.Scalar>, ok: Bool, err: Bool) {
        var chunk = chunk
        var s = s
        var failed = false
        while let head = chunk.first {
            if !failed && s.isEmpty { failed = true }
            switch head {
            case "[":
                // Character class.
                var r: Unicode.Scalar = "\0"
                if !failed {
                    r = s.first!
                    s = s.dropFirst()
                }
                chunk = chunk.dropFirst()
                // Possibly negated.
                var negated = false
                if chunk.first == "^" {
                    negated = true
                    chunk = chunk.dropFirst()
                }
                // Parse all ranges.
                var matched = false
                var nrange = 0
                while true {
                    if chunk.first == "]" && nrange > 0 {
                        chunk = chunk.dropFirst()
                        break
                    }
                    guard let (lo, afterLo) = getEsc(chunk) else {
                        return (ArraySlice<Unicode.Scalar>(), false, true)
                    }
                    chunk = afterLo
                    var hi = lo
                    if chunk.first == "-" {
                        guard let (h, afterHi) = getEsc(chunk.dropFirst()) else {
                            return (ArraySlice<Unicode.Scalar>(), false, true)
                        }
                        hi = h
                        chunk = afterHi
                    }
                    if lo <= r && r <= hi { matched = true }
                    nrange += 1
                }
                if matched == negated { failed = true }
            case "?":
                if !failed {
                    if s.first! == "/" { failed = true }
                    s = s.dropFirst()
                }
                chunk = chunk.dropFirst()
            case "\\":
                chunk = chunk.dropFirst()
                if chunk.isEmpty {
                    return (ArraySlice<Unicode.Scalar>(), false, true)
                }
                // Fall through to the literal comparison.
                fallthrough
            default:
                if !failed {
                    if chunk.first! != s.first! { failed = true }
                    s = s.dropFirst()
                }
                chunk = chunk.dropFirst()
            }
        }
        if failed {
            return (ArraySlice<Unicode.Scalar>(), false, false)
        }
        return (s, true, false)
    }

    /// Read one possibly-escaped character of a class body. `nil` is Go's
    /// `ErrBadPattern`: an empty body, a `-` or `]` where a character is
    /// required, a trailing `\`, or a class that ends right after the
    /// character.
    private static func getEsc(
        _ chunk: ArraySlice<Unicode.Scalar>
    ) -> (scalar: Unicode.Scalar, rest: ArraySlice<Unicode.Scalar>)? {
        guard let head = chunk.first, head != "-", head != "]" else { return nil }
        var c = chunk
        if c.first == "\\" {
            c = c.dropFirst()
            if c.isEmpty { return nil }
        }
        let r = c.first!
        let rest = c.dropFirst()
        if rest.isEmpty { return nil }
        return (r, rest)
    }
}
