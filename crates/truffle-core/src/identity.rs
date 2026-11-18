//! Identity and namespacing primitives (RFC 017).
//!
//! This module defines the three identifier types that truffle exposes at the
//! application layer:
//!
//! - [`AppId`] — application-level namespace (`^[a-z][a-z0-9-]{1,31}$`).
//! - [`DeviceName`] — user-supplied, Unicode-friendly, up to 256 graphemes.
//! - [`DeviceId`] — 26-character Crockford base32 ULID, stable per-device.
//!
//! Plus helpers for deriving a Tailscale-safe hostname from a raw `DeviceName`:
//!
//! - [`slug`] — implements the 10-step sanitisation algorithm from §5.3.
//! - [`tailscale_hostname`] — composes the final `truffle-{app_id}-{slug}` name.
//!
//! # Why these types?
//!
//! Before RFC 017, truffle hand-waved identity: callers passed a raw `name`
//! string that the library handed verbatim to Tailscale. That broke for
//! non-ASCII names, for names with spaces/quotes, for names longer than
//! 63 characters, and it gave every app on the tailnet the same implicit
//! hostname namespace.
//!
//! These newtypes lock down the invariants at construction time so lower
//! layers can assume their inputs are already valid.

use std::fmt;

use unicode_normalization::UnicodeNormalization;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors produced when parsing identifier inputs.
#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    /// `AppId` failed the regex `^[a-z][a-z0-9-]{1,31}$`.
    #[error("invalid app_id '{0}': must match ^[a-z][a-z0-9-]{{1,31}}$")]
    InvalidAppId(String),
    /// `DeviceId` was not a 26-char Crockford base32 ULID.
    #[error("invalid device_id '{0}': not a valid ULID")]
    InvalidDeviceId(String),
}

// ---------------------------------------------------------------------------
// DeviceId — 26-char Crockford base32 ULID
// ---------------------------------------------------------------------------

/// A stable per-device identifier, formatted as a 26-character Crockford
/// base32 ULID.
///
/// Applications that care about "is this the same logical device as last
/// week" should persist and re-use a `DeviceId` across restarts. Truffle
/// takes care of persistence automatically under `{state_dir}/device-id.txt`
/// when no override is provided.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeviceId(String);

impl DeviceId {
    /// Generate a new random ULID.
    pub fn generate() -> Self {
        let ulid = ulid::Ulid::new();
        Self(ulid.to_string())
    }

    /// Parse a string as a ULID, returning an error if it is not a valid
    /// 26-character Crockford base32 ULID.
    pub fn parse(s: &str) -> Result<Self, IdentityError> {
        ulid::Ulid::from_string(s)
            .map(|u| Self(u.to_string()))
            .map_err(|_| IdentityError::InvalidDeviceId(s.to_string()))
    }

    /// Borrow the underlying ULID string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DeviceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// AppId — ^[a-z][a-z0-9-]{1,31}$
// ---------------------------------------------------------------------------

/// An application identifier that defines the namespace two nodes must share
/// in order to see each other as peers.
///
/// Valid `AppId`s match `^[a-z][a-z0-9-]{1,31}$`:
/// - first character: lowercase ASCII letter
/// - remaining: lowercase ASCII letters, digits, or `-`
/// - length: 2–32 characters
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppId(String);

impl AppId {
    /// Parse a string as an `AppId`, rejecting anything that fails the
    /// regex.
    pub fn parse(s: &str) -> Result<Self, IdentityError> {
        if !Self::is_valid(s) {
            return Err(IdentityError::InvalidAppId(s.to_string()));
        }
        Ok(Self(s.to_string()))
    }

    fn is_valid(s: &str) -> bool {
        // Length check: 2..=32 bytes (ASCII-only so bytes == chars).
        let len = s.len();
        if !(2..=32).contains(&len) {
            return false;
        }
        let bytes = s.as_bytes();
        // First character must be lowercase ASCII letter.
        if !bytes[0].is_ascii_lowercase() {
            return false;
        }
        // Remaining characters must be lowercase letter, digit, or '-'.
        for &b in &bytes[1..] {
            let ok = b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-';
            if !ok {
                return false;
            }
        }
        // Trailing hyphen is reserved (would produce ambiguous
        // `truffle-{app_id}-{slug}` hostnames with double separators).
        bytes[bytes.len() - 1] != b'-'
    }

    /// Borrow the underlying string representation.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AppId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// DeviceName — up to 256 graphemes of arbitrary Unicode
// ---------------------------------------------------------------------------

/// The soft cap on `DeviceName` length, measured in Unicode graphemes.
const DEVICE_NAME_MAX_GRAPHEMES: usize = 256;

/// A human-readable device name supplied by the user.
///
/// Accepts any Unicode input. Strings longer than 256 graphemes are
/// truncated with a warning logged via `tracing`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeviceName(String);

impl DeviceName {
    /// Parse a string as a `DeviceName`. Infallible — the only transformation
    /// is a soft-cap truncation at 256 graphemes.
    pub fn parse(s: impl Into<String>) -> Self {
        let raw: String = s.into();
        // Count graphemes without dragging in unicode-segmentation; walk
        // char_indices and cut on the boundary after the 256th char. This
        // is "close enough" — Rust `char` is a Unicode scalar value, which
        // matches what most DNS-adjacent tooling means by "character".
        //
        // The RFC says "keep the first 256 graphemes, not bytes" — we use
        // chars here to avoid an extra dependency; chars split on scalar
        // values which never land in the middle of a multi-byte UTF-8
        // sequence, so we never corrupt a codepoint.
        let char_count = raw.chars().count();
        if char_count <= DEVICE_NAME_MAX_GRAPHEMES {
            return Self(raw);
        }
        tracing::warn!(
            len = char_count,
            max = DEVICE_NAME_MAX_GRAPHEMES,
            "DeviceName exceeded {DEVICE_NAME_MAX_GRAPHEMES} graphemes; truncating"
        );
        let truncated: String = raw.chars().take(DEVICE_NAME_MAX_GRAPHEMES).collect();
        Self(truncated)
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DeviceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

// ---------------------------------------------------------------------------
// Internal helpers for slug derivation (§5.3)
// ---------------------------------------------------------------------------

/// Step 1 of §5.3: NFKC-normalise the input.
fn apply_unicode_normalization(s: &str) -> String {
    s.nfkc().collect()
}

/// Step 2 of §5.3: transliterate to ASCII where a reasonable mapping exists.
///
/// `deunicode` handles diacritics, ligatures, and common scripts. Characters
/// that `deunicode` cannot map get replaced with a `[?]` marker — we rewrite
/// those (plus anything else non-ASCII-alphanumeric) to single hyphens in
/// step 4, with contiguous runs collapsed in step 5.
fn transliterate(s: &str) -> String {
    deunicode::deunicode(s)
}

/// Step 5 of §5.3: collapse runs of multiple `-` into a single `-`.
fn collapse_hyphens(s: &str) -> String {
    s.split('-')
        .filter(|x| !x.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Deterministic hash fallback used when the sanitised slug would otherwise
/// be empty or too short.
///
/// Returns the first `len` characters of the base36 encoding of
/// `blake3::hash(raw)`.
fn slug_fallback_hash(raw: &str, len: usize) -> String {
    let hash = blake3::hash(raw.as_bytes());
    let bytes = hash.as_bytes();
    let encoded = base36_encode(bytes);
    encoded.chars().take(len).collect()
}

/// Encode a byte slice as lowercase base36.
///
/// Treats `bytes` as a big-endian unsigned integer and emits digits
/// `0-9a-z`. This is a minimal implementation — we do long division on
/// the byte slice and reverse the resulting digit vector.
fn base36_encode(bytes: &[u8]) -> String {
    if bytes.iter().all(|b| *b == 0) {
        return "0".to_string();
    }
    let mut n: Vec<u16> = bytes.iter().map(|b| *b as u16).collect();
    let mut out = String::new();
    while !n.is_empty() {
        let mut rem: u16 = 0;
        let mut new_n: Vec<u16> = Vec::with_capacity(n.len());
        let mut started = false;
        for d in &n {
            let cur = rem * 256 + d;
            let q = cur / 36;
            rem = cur % 36;
            if started || q != 0 {
                new_n.push(q);
                started = true;
            }
        }
        let digit = rem as u8;
        let c = if digit < 10 {
            (b'0' + digit) as char
        } else {
            (b'a' + (digit - 10)) as char
        };
        out.push(c);
        n = new_n;
    }
    out.chars().rev().collect()
}

// ---------------------------------------------------------------------------
// slug() — §5.3
// ---------------------------------------------------------------------------

/// Produce a Tailscale-safe slug from a raw device-name string, fitting
/// within `budget` ASCII characters.
///
/// Follows the 10-step algorithm defined in §5.3 of RFC 017 exactly:
///
/// 1. NFKC-normalise.
/// 2. Transliterate Unicode to ASCII.
/// 3. Lowercase.
/// 4. Replace non-`[a-z0-9]` characters with `-`.
/// 5. Collapse runs of `-`.
/// 6. Trim leading/trailing `-`.
/// 7. If empty, replace with first 8 chars of `base36(blake3(raw))`.
/// 8. Truncate to `budget`.
/// 9. Trim trailing `-` post-truncation.
/// 10. If the result is <2 chars, append first 6 chars of
///     `base36(blake3(raw))`.
pub fn slug(raw: &str, budget: usize) -> String {
    if budget == 0 {
        return String::new();
    }

    // Step 1: NFKC.
    let normalized = apply_unicode_normalization(raw);
    // Step 2: transliterate.
    let transliterated = transliterate(&normalized);
    // Step 3: lowercase.
    let lowered = transliterated.to_lowercase();
    // Step 4: replace non-[a-z0-9] with '-'.
    let replaced: String = lowered
        .chars()
        .map(|c| {
            if c.is_ascii_lowercase() || c.is_ascii_digit() {
                c
            } else {
                '-'
            }
        })
        .collect();
    // Step 5: collapse runs of '-'.
    let collapsed = collapse_hyphens(&replaced);
    // Step 6: trim leading/trailing '-'.
    let trimmed = collapsed.trim_matches('-').to_string();

    // Step 7: if empty, use hash fallback.
    let mut candidate = if trimmed.is_empty() {
        slug_fallback_hash(raw, 8)
    } else {
        trimmed
    };

    // Step 8: truncate to budget.
    if candidate.len() > budget {
        candidate = candidate.chars().take(budget).collect();
    }

    // Step 9: trim trailing '-' post-truncation.
    while candidate.ends_with('-') {
        candidate.pop();
    }

    // Step 10: if the result would be <2 chars, append 6 chars of hash.
    if candidate.len() < 2 {
        // Reserve 6 chars from the hash (or as many as the budget allows).
        let hash_len = 6.min(budget.saturating_sub(candidate.len()));
        let hash = slug_fallback_hash(raw, hash_len);
        candidate.push_str(&hash);
        // Re-truncate in case we overflowed the budget on the join.
        if candidate.len() > budget {
            candidate = candidate.chars().take(budget).collect();
        }
        // Trim trailing '-' one more time.
        while candidate.ends_with('-') {
            candidate.pop();
        }
    }

    candidate
}

// ---------------------------------------------------------------------------
// tailscale_hostname() — §5.3
// ---------------------------------------------------------------------------

/// Prefix every truffle-managed Tailscale hostname starts with.
const HOSTNAME_PREFIX: &str = "truffle-";
/// The DNS label length limit Tailscale enforces.
const DNS_LABEL_LIMIT: usize = 63;

/// Compose the final Tailscale hostname `truffle-{app_id}-{slug}`.
///
/// Callers provide the parsed `AppId` and the (unsanitised) `DeviceName`.
/// Budget computation for the slug lives here so every caller gets the same
/// answer.
pub fn tailscale_hostname(app_id: &AppId, device_name: &DeviceName) -> String {
    prefixed_hostname(
        &format!("{HOSTNAME_PREFIX}{}", app_id.as_str()),
        device_name,
    )
}

/// Compose `{prefix}-{slug}` (T8: an app-chosen prefix), the slug cut to what
/// the 63-character DNS label leaves after the prefix and its hyphen.
/// [`tailscale_hostname`] is this with the prefix `truffle-{app_id}`. The
/// prefix is the caller's to validate (`NodeBuilder::hostname_prefix`).
pub fn prefixed_hostname(prefix: &str, device_name: &DeviceName) -> String {
    // Budget = 63 - len(prefix) - 1 (for the separating hyphen).
    let budget = DNS_LABEL_LIMIT.saturating_sub(prefix.len() + 1);
    let slug_part = slug(device_name.as_str(), budget);
    format!("{prefix}-{slug_part}")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── §5.3 examples table ──────────────────────────────────────────

    #[test]
    fn slug_alice_s_macbook_pro() {
        // Budget for "playground" (10 chars): 63 - 8 - 10 - 1 = 44.
        let budget = DNS_LABEL_LIMIT - HOSTNAME_PREFIX.len() - "playground".len() - 1;
        assert_eq!(budget, 44);
        let s = slug("Alice's MacBook Pro", budget);
        assert_eq!(s, "alice-s-macbook-pro");

        let host = tailscale_hostname(
            &AppId::parse("playground").unwrap(),
            &DeviceName::parse("Alice's MacBook Pro"),
        );
        assert_eq!(host, "truffle-playground-alice-s-macbook-pro");
    }

    #[test]
    fn slug_cjk_name_hashed() {
        // "田中's 部屋" — CJK runs become hyphens, leaving "s" and whatever
        // deunicode produces for the CJK codepoints (often romanised). We
        // want to prove determinism: the slug is hash-suffixed and stable.
        let budget = 44;
        let s1 = slug("田中's 部屋", budget);
        let s2 = slug("田中's 部屋", budget);
        assert_eq!(s1, s2, "slug must be deterministic");
        // Must be non-empty, lowercase, and only contain [a-z0-9-].
        assert!(!s1.is_empty());
        assert!(
            s1.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "slug must be ascii-only: {s1}"
        );
        assert!(!s1.starts_with('-'));
        assert!(!s1.ends_with('-'));
    }

    #[test]
    fn slug_rocket_emoji_deunicoded() {
        // "🚀" — `deunicode` maps this to a Latin string ("rocket"), so
        // the hash fallback does NOT fire. Verify determinism and that
        // the output is ASCII-clean. The exact string depends on the
        // `deunicode` crate's lookup table; we only assert invariants.
        let budget = 44;
        let s1 = slug("🚀", budget);
        let s2 = slug("🚀", budget);
        assert_eq!(s1, s2, "slug must be deterministic");
        assert!(!s1.is_empty());
        assert!(
            s1.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'),
            "slug must be ASCII-clean: {s1}"
        );
        assert!(!s1.starts_with('-'));
        assert!(!s1.ends_with('-'));
    }

    #[test]
    fn slug_pure_symbol_hash_fallback() {
        // A character that `deunicode` cannot map produces an empty
        // transliteration, which triggers the step-7 hash fallback.
        // The U+3013 "GETA MARK" (〓) typically has no Latin mapping.
        let budget = 44;
        let s1 = slug("〓", budget);
        let s2 = slug("〓", budget);
        assert_eq!(s1, s2, "hash fallback must be deterministic");
        assert!(!s1.is_empty());
        assert!(
            s1.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "hash fallback must be base36: {s1}"
        );
    }

    #[test]
    fn slug_james_mbp_16_strips_quote() {
        let budget = 44;
        let s = slug("JAMES-mbp-16\"", budget);
        assert_eq!(s, "james-mbp-16");
    }

    #[test]
    fn slug_long_name_truncated_no_trailing_hyphen() {
        // Budget 44 for the playground app (63 - "truffle-" - "playground" - 1).
        // The RFC §5.3 example table used an illustrative 47-char expectation
        // but the actual budget for `playground` is 44 chars; we assert the
        // correct truncation length here and leave the RFC as a doc-only
        // discrepancy for the follow-up RFC touch-up.
        let budget = 44;
        let input =
            "this-is-a-very-long-device-name-that-blows-past-the-dns-label-budget-and-then-some";
        let s = slug(input, budget);
        assert!(s.len() <= budget);
        assert!(
            !s.ends_with('-'),
            "slug must not have a trailing hyphen: {s}"
        );
        // First 44 chars of the hyphenated input; truncation lands inside
        // "past" and step 9 trims the trailing hyphen that would otherwise
        // have dangled.
        assert_eq!(s, "this-is-a-very-long-device-name-that-blows-p");
    }

    #[test]
    fn slug_empty_string_falls_back_to_hash() {
        // §5.3: empty input is expected to be rejected at the DeviceName
        // layer (callers default to OS hostname). But the raw slug() function
        // itself handles it gracefully by using the hash fallback, so we
        // verify that here.
        let budget = 44;
        let s = slug("", budget);
        assert!(!s.is_empty());
        assert!(s.len() >= 2);
    }

    // ── AppId validation ─────────────────────────────────────────────

    #[test]
    fn app_id_accepts_valid_names() {
        let valid = [
            "playground",
            "chat",
            "a1",
            "production-playground-v2",
            "a-b",
        ];
        for name in valid {
            AppId::parse(name).unwrap_or_else(|e| panic!("expected '{name}' to parse, got {e}"));
        }
    }

    #[test]
    fn app_id_rejects_invalid_names() {
        // Tuple pairs: (input, reason it fails)
        let invalid = [
            ("Playground", "uppercase letters rejected"),
            ("1foo", "must start with a letter, not a digit"),
            ("a", "too short (needs >= 2 chars)"),
            (
                "this-is-way-way-too-long-for-a-reasonable-app-id",
                "too long (> 32 chars)",
            ),
            ("foo_bar", "underscores not allowed"),
            ("foo.bar", "dots not allowed"),
            ("", "empty rejected"),
            ("-foo", "leading hyphen (does not start with a letter)"),
            ("foo-", "trailing hyphen rejected"),
            (
                "a-very-long-valid-chars-",
                "trailing hyphen rejected even mid-length",
            ),
        ];
        for (input, reason) in invalid {
            assert!(
                AppId::parse(input).is_err(),
                "expected '{input}' to be rejected: {reason}"
            );
        }
    }

    // ── DeviceId ─────────────────────────────────────────────────────

    #[test]
    fn device_id_generate_returns_26_chars() {
        let id = DeviceId::generate();
        assert_eq!(id.as_str().len(), 26);
    }

    #[test]
    fn device_id_generate_is_unique() {
        let a = DeviceId::generate();
        let b = DeviceId::generate();
        assert_ne!(a, b, "two generated DeviceIds should differ");
    }

    #[test]
    fn device_id_roundtrips_through_parse() {
        let original = DeviceId::generate();
        let parsed = DeviceId::parse(original.as_str()).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn device_id_rejects_invalid() {
        assert!(DeviceId::parse("not-a-ulid").is_err());
        assert!(DeviceId::parse("").is_err());
        // Wrong length (25 chars).
        assert!(DeviceId::parse("01J4K9M2Z8AB3RNYQPW6H5TC0").is_err());
    }

    // ── DeviceName truncation ────────────────────────────────────────

    #[test]
    fn device_name_truncates_to_256_graphemes_no_split_codepoints() {
        // A string of 300 multi-byte characters — uses the Greek letter
        // alpha ("α") which is 2 bytes in UTF-8. If we accidentally did
        // a byte-based truncation we'd end up with a broken UTF-8
        // sequence and a panic on `String` construction. Verify we don't.
        let long = "α".repeat(300);
        // Sanity check: byte length is 600 but char count is 300.
        assert_eq!(long.chars().count(), 300);
        assert_eq!(long.len(), 600);

        let name = DeviceName::parse(long);
        assert_eq!(name.as_str().chars().count(), DEVICE_NAME_MAX_GRAPHEMES);
        // Every character is still a valid Greek alpha.
        assert!(name.as_str().chars().all(|c| c == 'α'));
    }

    #[test]
    fn device_name_short_string_passes_through() {
        let name = DeviceName::parse("Alice's MacBook");
        assert_eq!(name.as_str(), "Alice's MacBook");
    }

    // ── Slug collision determinism ───────────────────────────────────

    #[test]
    fn slug_rocket_emoji_stable_across_calls() {
        let s1 = slug("🚀", 20);
        let s2 = slug("🚀", 20);
        assert_eq!(s1, s2);
    }

    // ── Budget edge cases ────────────────────────────────────────────

    #[test]
    fn slug_budget_two_forces_hash_fallback() {
        // Budget 2 can't hold the normal transliteration of "Alice's MacBook";
        // after truncation + trimming it'll end up truncated to something
        // short. Verify the function still returns within budget and has
        // at least 2 chars (step 10 pads with hash).
        let s = slug("Alice's MacBook", 2);
        assert!(s.len() <= 2);
        assert!(
            s.len() >= 2 || s.is_empty(),
            "slug with budget 2 must be either empty or >= 2 chars: {s}"
        );
    }

    #[test]
    fn slug_budget_zero_returns_empty() {
        let s = slug("Alice's MacBook", 0);
        assert_eq!(s, "");
    }

    // ── base36 spot check ────────────────────────────────────────────

    #[test]
    fn base36_encode_known_values() {
        // Zero encodes to "0".
        assert_eq!(base36_encode(&[0, 0, 0]), "0");
        // 35 encodes to "z".
        assert_eq!(base36_encode(&[35]), "z");
        // 36 encodes to "10".
        assert_eq!(base36_encode(&[36]), "10");
        // 255 encodes to "73" (255 = 7*36 + 3).
        assert_eq!(base36_encode(&[255]), "73");
    }
}
