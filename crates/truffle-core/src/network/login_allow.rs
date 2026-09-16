//! Login allow-lists (RFC 025 §3.2).
//!
//! A node may declare which tailnet **logins** may join its session plane.
//! The list is a set of shell-style globs evaluated against a caller's
//! WhoIs `loginName` — the same gate the Go sidecar applies to served
//! routes (RFC 023 §9.7, `allowedLogin` in `sidecar-slim/main.go`), so one
//! grammar and one test table cover both planes and the Swift core.
//!
//! The grammar is Go's `path.Match`, applied after lowercasing both sides:
//!
//! - `*` matches any run (including empty) of characters other than `/`;
//! - `?` matches exactly one character other than `/`;
//! - `[abc]`, `[a-z]`, `[^abc]` character classes (ranges, negation, `\`
//!   escapes inside);
//! - `\x` matches `x` literally;
//! - a malformed pattern (an unterminated class, a trailing `\`, an empty or
//!   reversed range) never matches and never panics.
//!
//! An **empty list means no gate**. A non-empty list against an **absent or
//! empty login fails closed** — tagged nodes report Tailscale's
//! `tagged-devices` pseudo-login and only match a glob that names it.

/// The gate: does `login` pass `globs`?
///
/// `globs` empty → `true` (no gate). `login` `None` or empty with a
/// non-empty list → `false` (fail closed). Otherwise `true` iff at least one
/// glob matches, case-insensitively; malformed globs are skipped.
pub fn login_allowed(globs: &[String], login: Option<&str>) -> bool {
    if globs.is_empty() {
        return true;
    }
    let login = match login {
        Some(l) if !l.is_empty() => l.to_lowercase(),
        _ => return false,
    };
    globs
        .iter()
        .any(|g| glob_match(&g.to_lowercase(), &login).unwrap_or(false))
}

/// A pattern `path.Match` would reject with `ErrBadPattern`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BadPattern;

impl std::fmt::Display for BadPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("syntax error in login glob")
    }
}

impl std::error::Error for BadPattern {}

/// A faithful port of Go's `path.Match(pattern, name)`: case-sensitive,
/// `*` and `?` never cross `/`. Callers wanting the gate's semantics use
/// [`login_allowed`], which lowercases and treats `Err` as "no match".
pub fn glob_match(pattern: &str, name: &str) -> Result<bool, BadPattern> {
    let mut pattern: &[char] = &pattern.chars().collect::<Vec<_>>();
    let mut name: &[char] = &name.chars().collect::<Vec<_>>();

    'pattern: while !pattern.is_empty() {
        let (star, chunk, rest) = scan_chunk(pattern);
        pattern = rest;
        if star && chunk.is_empty() {
            // A trailing `*` matches the rest of the name unless it has a `/`.
            return Ok(!name.contains(&'/'));
        }
        // Look for a match at the current position.
        let (t, ok, err) = match_chunk(chunk, name);
        // If this is the last chunk, the name must be exhausted here;
        // otherwise a later chunk could still match via the star.
        if ok && (t.is_empty() || !pattern.is_empty()) {
            name = t;
            continue;
        }
        if err {
            return Err(BadPattern);
        }
        if star {
            // Look for a match skipping i+1 characters. Cannot skip `/`.
            let mut i = 0;
            while i < name.len() && name[i] != '/' {
                let (t, ok, err) = match_chunk(chunk, &name[i + 1..]);
                if ok {
                    // If this is the last chunk, the name must be exhausted.
                    if pattern.is_empty() && !t.is_empty() {
                        i += 1;
                        continue;
                    }
                    name = t;
                    continue 'pattern;
                }
                if err {
                    return Err(BadPattern);
                }
                i += 1;
            }
        }
        // Before answering "no match", check the remainder of the pattern
        // is syntactically valid (Go reports ErrBadPattern first).
        while !pattern.is_empty() {
            let (_, chunk, rest) = scan_chunk(pattern);
            pattern = rest;
            let (_, _, err) = match_chunk(chunk, &[]);
            if err {
                return Err(BadPattern);
            }
        }
        return Ok(false);
    }
    Ok(name.is_empty())
}

/// Split `pattern` into a leading run of `*`s, the next literal chunk (up to
/// but not including the next unescaped `*` outside a class), and the rest.
fn scan_chunk(pattern: &[char]) -> (bool, &[char], &[char]) {
    let mut star = false;
    let mut p = pattern;
    while !p.is_empty() && p[0] == '*' {
        p = &p[1..];
        star = true;
    }
    let mut in_range = false;
    let mut i = 0;
    'scan: while i < p.len() {
        match p[i] {
            '\\' => {
                // An escaped character never ends the chunk.
                if i + 1 < p.len() {
                    i += 1;
                }
            }
            '[' => in_range = true,
            ']' => in_range = false,
            '*' => {
                if !in_range {
                    break 'scan;
                }
            }
            _ => {}
        }
        i += 1;
    }
    (star, &p[..i], &p[i..])
}

/// Match `chunk` (which has no `*`) against the start of `s`. Returns the
/// remainder of `s`, whether it matched, and whether the chunk was
/// malformed. Like Go, syntax is checked to the end of the chunk even after
/// the match has already failed.
fn match_chunk<'a>(mut chunk: &[char], mut s: &'a [char]) -> (&'a [char], bool, bool) {
    let mut failed = false;
    while !chunk.is_empty() {
        if !failed && s.is_empty() {
            failed = true;
        }
        match chunk[0] {
            '[' => {
                // Character class.
                let mut r = '\0';
                if !failed {
                    r = s[0];
                    s = &s[1..];
                }
                chunk = &chunk[1..];
                // Possibly negated.
                let mut negated = false;
                if !chunk.is_empty() && chunk[0] == '^' {
                    negated = true;
                    chunk = &chunk[1..];
                }
                // Parse all ranges.
                let mut matched = false;
                let mut nrange = 0;
                loop {
                    if !chunk.is_empty() && chunk[0] == ']' && nrange > 0 {
                        chunk = &chunk[1..];
                        break;
                    }
                    let (lo, rest) = match get_esc(chunk) {
                        Some(v) => v,
                        None => return (&[], false, true),
                    };
                    chunk = rest;
                    let mut hi = lo;
                    if chunk[0] == '-' {
                        let (h, rest) = match get_esc(&chunk[1..]) {
                            Some(v) => v,
                            None => return (&[], false, true),
                        };
                        hi = h;
                        chunk = rest;
                    }
                    if lo <= r && r <= hi {
                        matched = true;
                    }
                    nrange += 1;
                }
                if matched == negated {
                    failed = true;
                }
            }
            '?' => {
                if !failed {
                    if s[0] == '/' {
                        failed = true;
                    }
                    s = &s[1..];
                }
                chunk = &chunk[1..];
            }
            '\\' => {
                chunk = &chunk[1..];
                if chunk.is_empty() {
                    return (&[], false, true);
                }
                // Fall through to the literal comparison.
                if !failed {
                    if chunk[0] != s[0] {
                        failed = true;
                    }
                    s = &s[1..];
                }
                chunk = &chunk[1..];
            }
            _ => {
                if !failed {
                    if chunk[0] != s[0] {
                        failed = true;
                    }
                    s = &s[1..];
                }
                chunk = &chunk[1..];
            }
        }
    }
    if failed {
        return (&[], false, false);
    }
    (s, true, false)
}

/// Read one possibly-escaped character of a class body. `None` is Go's
/// `ErrBadPattern`: an empty body, a `-` or `]` where a character is
/// required, a trailing `\`, or a class that ends right after the character.
fn get_esc(chunk: &[char]) -> Option<(char, &[char])> {
    if chunk.is_empty() || chunk[0] == '-' || chunk[0] == ']' {
        return None;
    }
    let mut c = chunk;
    if c[0] == '\\' {
        c = &c[1..];
        if c.is_empty() {
            return None;
        }
    }
    let r = c[0];
    let rest = &c[1..];
    if rest.is_empty() {
        return None;
    }
    Some((r, rest))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn globs(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// The Go reference table (`TestAllowedLogin` in `sidecar-slim/main_test.go`),
    /// reproduced verbatim so the two planes cannot drift.
    #[test]
    fn allowed_login_matches_the_go_table() {
        let cases: &[(&str, &[&str], &str, bool)] = &[
            ("empty globs allow all", &[], "anyone@example.com", true),
            ("empty globs allow even empty login", &[], "", true),
            (
                "non-empty gate, empty login fails closed",
                &["*@corp.com"],
                "",
                false,
            ),
            ("exact match", &["alice@corp.com"], "alice@corp.com", true),
            (
                "exact non-match",
                &["alice@corp.com"],
                "bob@corp.com",
                false,
            ),
            (
                "domain glob matches",
                &["*@corp.com"],
                "alice@corp.com",
                true,
            ),
            (
                "domain glob rejects other domain",
                &["*@corp.com"],
                "alice@evil.com",
                false,
            ),
            (
                "case-insensitive glob vs login",
                &["*@CORP.com"],
                "Alice@corp.COM",
                true,
            ),
            (
                "case-insensitive exact",
                &["Alice@Corp.Com"],
                "alice@corp.com",
                true,
            ),
            (
                "second glob in list matches",
                &["*@other.com", "*@corp.com"],
                "bob@corp.com",
                true,
            ),
            (
                "no glob in list matches",
                &["*@other.com", "*@more.com"],
                "bob@corp.com",
                false,
            ),
            (
                "star does not cross slash",
                &["*@corp.com"],
                "a/b@corp.com",
                false,
            ),
            (
                "invalid glob does not match",
                &["[unterminated"],
                "alice@corp.com",
                false,
            ),
            (
                "invalid glob skipped, valid one still matches",
                &["[bad", "*@corp.com"],
                "alice@corp.com",
                true,
            ),
        ];
        for (name, list, login, want) in cases {
            assert_eq!(
                login_allowed(&globs(list), Some(login)),
                *want,
                "{name}: login_allowed({list:?}, {login:?})"
            );
        }
        // `None` is the absent login: fails closed under a gate, passes without one.
        assert!(!login_allowed(&globs(&["*@corp.com"]), None));
        assert!(login_allowed(&[], None));
        // A tagged node only passes a glob that names the pseudo-login.
        assert!(!login_allowed(
            &globs(&["*@corp.com"]),
            Some("tagged-devices")
        ));
        assert!(login_allowed(
            &globs(&["tagged-devices"]),
            Some("tagged-devices")
        ));
    }

    /// `path.Match` behaviours the gate relies on, checked against Go's own
    /// `TestMatch` rows where they apply.
    #[test]
    fn glob_match_follows_path_match() {
        let ok = |p: &str, n: &str| glob_match(p, n).unwrap();
        assert!(ok("abc", "abc"));
        assert!(ok("*", "abc"));
        assert!(ok("*c", "abc"));
        assert!(!ok("a*", "a/b"));
        assert!(ok("a*", "ab"));
        assert!(!ok("a*", "abc/d"));
        assert!(ok("a*/b", "abc/b"));
        assert!(!ok("a*/b", "a/c/b"));
        assert!(ok("a*b*c*d*e*/f", "axbxcxdxe/f"));
        assert!(ok("a*b*c*d*e*/f", "axbxcxdxexxx/f"));
        assert!(!ok("a*b*c*d*e*/f", "axbxcxdxe/xxx/f"));
        assert!(!ok("a*b*c*d*e*/f", "axbxcxdxexxx/fff"));
        assert!(ok("a*b?c*x", "abxbbxdbxebxczzx"));
        assert!(!ok("a*b?c*x", "abxbbxdbxebxczzy"));
        assert!(ok("ab[c]", "abc"));
        assert!(ok("ab[b-d]", "abc"));
        assert!(!ok("ab[e-g]", "abc"));
        assert!(!ok("ab[^c]", "abc"));
        assert!(!ok("ab[^b-d]", "abc"));
        assert!(ok("ab[^e-g]", "abc"));
        assert!(ok("a\\*b", "a*b"));
        assert!(!ok("a\\*b", "ab"));
        assert!(ok("a?b", "a☺b"));
        assert!(ok("a[^a]b", "a☺b"));
        assert!(!ok("a???b", "a☺b"));
        assert!(!ok("a[^a][^a][^a]b", "a☺b"));
        assert!(ok("[a-ζ]*", "α"));
        assert!(!ok("*[a-ζ]", "A"));
        assert!(ok("a?b", "a/b") == false);
        assert!(ok("a*b", "a/b") == false);
        assert!(ok("[\\]a]", "]"));
        assert!(ok("[\\-]", "-"));
        assert!(ok("[x\\-]", "x"));
        assert!(ok("[x\\-]", "-"));
        assert!(!ok("[x\\-]", "z"));
        assert!(ok("[\\-x]", "x"));
        assert!(ok("[\\-x]", "-"));
        assert!(!ok("[\\-x]", "a"));
        assert!(ok("*x", "xxx"));
        assert!(!ok("", "a"));
        assert!(ok("", ""));
    }

    #[test]
    fn glob_match_reports_bad_patterns_like_go() {
        for bad in [
            "[]a]",
            "[-]",
            "[x-]",
            "[-x]",
            "\\",
            "[a-b-c]",
            "[",
            "[^",
            "[^bc",
            "a[",
            "[unterminated",
        ] {
            assert_eq!(
                glob_match(bad, "a"),
                Err(BadPattern),
                "{bad:?} must be a bad pattern"
            );
        }
        // A bad pattern is an error even when an earlier chunk already failed
        // to match — Go checks the remainder's syntax before answering false.
        assert_eq!(glob_match("a*[", "b"), Err(BadPattern));
    }
}
