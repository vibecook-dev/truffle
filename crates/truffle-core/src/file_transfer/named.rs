//! Named pull roots (RFC 032).
//!
//! A holder registers a directory under a name; a requester pulls a file by
//! `{root, path}`, where `path` is relative to that root. The holder's
//! absolute layout never leaves the holder, and a requester can never name a
//! path outside the roots the holder chose to share.
//!
//! This module holds the two grammars (root names and relative paths), the
//! holder's authorization of a named pull, and the requester's reading of a
//! refusal, which is how a holder that predates named roots is told apart.

use std::collections::BTreeMap;
use std::path::PathBuf;

use super::receiver::servable_file;
use super::types::{NamedPath, TransferError, PULL_DENIED_REASON};

/// Longest root name, in bytes (the grammar is ASCII).
pub(crate) const MAX_ROOT_NAME_LEN: usize = 64;

/// Longest relative path a named pull may carry, in bytes.
pub(crate) const MAX_RELATIVE_PATH_LEN: usize = 4096;

/// Check a root name against `[a-z0-9][a-z0-9._-]{0,63}`.
///
/// Lowercase only, so a name means one thing on every platform and a lookup
/// never folds case.
pub(crate) fn check_root_name(name: &str) -> Result<(), &'static str> {
    let bytes = name.as_bytes();
    let Some(first) = bytes.first() else {
        return Err("it is empty");
    };
    if !(first.is_ascii_lowercase() || first.is_ascii_digit()) {
        return Err("it must start with a lowercase letter or a digit");
    }
    if bytes.len() > MAX_ROOT_NAME_LEN {
        return Err("it is longer than 64 bytes");
    }
    let allowed = |b: &u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(b);
    if !bytes.iter().all(allowed) {
        return Err("only a-z, 0-9, '.', '_' and '-' are allowed");
    }
    Ok(())
}

/// Check a relative path for a named pull.
///
/// The path is `/`-separated and normalized: not empty, not absolute, no
/// empty, `.` or `..` segment, and no NUL. `\` and `:` are refused too, so
/// the path names the same file on every holder platform: on Windows `\` is a
/// separator and `:` starts a drive or an alternate data stream.
pub(crate) fn check_relative_path(path: &str) -> Result<(), &'static str> {
    if path.is_empty() {
        return Err("it is empty");
    }
    if path.len() > MAX_RELATIVE_PATH_LEN {
        return Err("it is longer than 4096 bytes");
    }
    if path.contains('\0') {
        return Err("it contains a NUL");
    }
    if path.contains('\\') {
        return Err("it contains a '\\' (the separator is '/')");
    }
    if path.contains(':') {
        return Err("it contains a ':' (a drive or stream designator)");
    }
    if path.starts_with('/') {
        return Err("it is absolute");
    }
    for segment in path.split('/') {
        match segment {
            "" => return Err("it has an empty segment"),
            "." => return Err("it has a '.' segment"),
            ".." => return Err("it has a '..' segment"),
            _ => {}
        }
    }
    Ok(())
}

/// An `InvalidInput` error naming the refused value and why.
pub(crate) fn invalid_input(what: &str, value: &str, reason: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!("invalid {what} {value:?}: {reason}"),
    )
}

/// Authorize a named pull against the holder's named roots.
///
/// The root is looked up by exact name. The relative path is checked again
/// (the requester's check is not trusted), joined under the root's canonical
/// directory and canonicalized, which resolves every symlink: the result must
/// still lie inside the root, and be a regular file no larger than
/// `max_size`. Every refusal is `Rejected`; the caller sends the requester
/// the one [`NAMED_PULL_DENIED_REASON`](super::types::NAMED_PULL_DENIED_REASON)
/// whatever the cause, so it learns nothing about what exists.
pub(crate) fn authorize_named_pull(
    roots: &BTreeMap<String, PathBuf>,
    target: &NamedPath,
    max_size: u64,
) -> Result<PathBuf, TransferError> {
    let Some(root) = roots.get(&target.root) else {
        return Err(TransferError::Rejected(format!(
            "no pull root is named {:?}",
            target.root
        )));
    };
    if let Err(reason) = check_relative_path(&target.path) {
        return Err(TransferError::Rejected(format!(
            "refused relative path {:?}: {reason}",
            target.path
        )));
    }
    let canon = std::fs::canonicalize(root.join(&target.path))
        .map_err(|_| TransferError::Rejected("requested path is not shared".into()))?;
    if !canon.starts_with(root) {
        return Err(TransferError::Rejected(
            "requested path is not shared".into(),
        ));
    }
    servable_file(canon, max_size)
}

/// What a requester makes of a holder's REJECT.
///
/// A holder that predates named roots answers a named pull with
/// [`PULL_DENIED_REASON`]: it ignored `named` and could not resolve the empty
/// `path` the pull carries, so it read no file. A current holder refuses a
/// named pull only with
/// [`NAMED_PULL_DENIED_REASON`](super::types::NAMED_PULL_DENIED_REASON) or,
/// when it is busy, its busy reason. So that one reason, on a named pull,
/// means the holder does not support named roots.
pub(crate) fn pull_rejection(named: bool, reason: String) -> TransferError {
    if named && reason == PULL_DENIED_REASON {
        TransferError::Unsupported("the holder predates named pull roots (RFC 032)".into())
    } else {
        TransferError::Rejected(reason)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_transfer::types::NAMED_PULL_DENIED_REASON;

    // ── Root names ─────────────────────────────────────────────────────

    #[test]
    fn root_names_follow_the_grammar() {
        let longest = "a".repeat(MAX_ROOT_NAME_LEN);
        for ok in [
            "a",
            "0",
            "objects",
            "drive-objects",
            "a.b_c-d",
            "9lives",
            longest.as_str(),
        ] {
            assert_eq!(check_root_name(ok), Ok(()), "{ok:?} must be accepted");
        }
    }

    #[test]
    fn root_names_outside_the_grammar_are_refused() {
        let too_long = "a".repeat(MAX_ROOT_NAME_LEN + 1);
        for bad in [
            "",
            "-a",
            ".a",
            "_a",
            "drive objects",
            "drive/objects",
            "drive\\objects",
            "drive:objects",
            "drive\0",
            "é",
            too_long.as_str(),
        ] {
            assert!(check_root_name(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn root_names_are_lowercase_only() {
        // No case folding anywhere: an uppercase name cannot be registered,
        // so "Objects" never reaches a root registered as "objects".
        for bad in ["A", "Objects", "OBJECTS", "drive-Objects"] {
            assert!(check_root_name(bad).is_err(), "{bad:?} must be refused");
        }
    }

    // ── Relative paths ─────────────────────────────────────────────────

    #[test]
    fn relative_paths_follow_the_grammar() {
        let longest = "a".repeat(MAX_RELATIVE_PATH_LEN);
        for ok in [
            "f.txt",
            "ab/ab01.bin",
            "a/b/c/d.bin",
            ".hidden",
            "...",
            "a/..b/c..",
            "with space.txt",
            longest.as_str(),
        ] {
            assert_eq!(check_relative_path(ok), Ok(()), "{ok:?} must be accepted");
        }
    }

    #[test]
    fn escaping_or_unnormalized_paths_are_refused() {
        let too_long = "a".repeat(MAX_RELATIVE_PATH_LEN + 1);
        for bad in [
            "",
            "/etc/passwd",
            "/",
            "..",
            "../secret.txt",
            "a/../../secret.txt",
            "a/..",
            ".",
            "./a",
            "a/./b",
            "a//b",
            "a/",
            "a\0b",
            "..\\secret.txt",
            "a\\b",
            "C:secret.txt",
            "c:/secret.txt",
            "f.txt:stream",
            too_long.as_str(),
        ] {
            assert!(check_relative_path(bad).is_err(), "{bad:?} must be refused");
        }
    }

    #[test]
    fn a_relative_path_is_a_wire_string_not_an_os_path() {
        // A content-addressed object, `<first 2 hex>/<64 hex>`, is named with
        // '/' on every platform. Built as a Windows path it is joined with
        // '\' instead, and refused before anything is sent.
        let hex = "ab".repeat(32);
        let wire = format!("{}/{hex}", &hex[..2]);
        assert_eq!(check_relative_path(&wire), Ok(()));
        let windows = format!("{}\\{hex}", &hex[..2]);
        assert!(check_relative_path(&windows).is_err());
    }

    #[test]
    fn invalid_input_names_the_value_and_the_reason() {
        let e = invalid_input("pull root name", "Objects", "lowercase only");
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(
            e.to_string(),
            "invalid pull root name \"Objects\": lowercase only"
        );
    }

    // ── Authorization ──────────────────────────────────────────────────

    /// A holder with one root named `objects` holding `ab/f.bin` (4 bytes).
    fn holder() -> (tempfile::TempDir, BTreeMap<String, PathBuf>, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let root = std::fs::canonicalize(dir.path()).unwrap();
        std::fs::create_dir(root.join("ab")).unwrap();
        let file = root.join("ab").join("f.bin");
        std::fs::write(&file, b"1234").unwrap();
        let mut roots = BTreeMap::new();
        roots.insert("objects".to_string(), root);
        (dir, roots, file)
    }

    /// Authorize `root`/`path` with no size limit.
    fn authorize(
        roots: &BTreeMap<String, PathBuf>,
        root: &str,
        path: &str,
    ) -> Result<PathBuf, TransferError> {
        let target = NamedPath {
            root: root.into(),
            path: path.into(),
        };
        authorize_named_pull(roots, &target, u64::MAX)
    }

    #[test]
    fn a_file_under_its_root_is_authorized() {
        let (_dir, roots, file) = holder();
        let got = authorize(&roots, "objects", "ab/f.bin").unwrap();
        assert_eq!(got, std::fs::canonicalize(file).unwrap());
    }

    #[test]
    fn an_unknown_root_is_refused() {
        let (_dir, roots, _file) = holder();
        let err = authorize(&roots, "other", "ab/f.bin").unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)), "{err}");
        // No roots at all: every named pull is refused (deny by default).
        let err = authorize(&BTreeMap::new(), "objects", "ab/f.bin").unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)), "{err}");
    }

    #[test]
    fn a_root_name_is_matched_exactly() {
        let (_dir, roots, _file) = holder();
        for name in ["Objects", "OBJECTS", "objects ", "object"] {
            let err = authorize(&roots, name, "ab/f.bin").unwrap_err();
            assert!(matches!(err, TransferError::Rejected(_)), "{name:?}: {err}");
        }
    }

    #[test]
    fn the_holder_rechecks_the_relative_path() {
        // The requester's own check is not trusted: a path that climbs out,
        // or names the holder absolutely, is refused before any lookup on disk.
        let (dir, roots, _file) = holder();
        let outside = dir.path().parent().unwrap().join("outside.txt");
        for path in [
            "../outside.txt",
            "ab/../../outside.txt",
            outside.to_str().unwrap(),
            "ab//f.bin",
            "./ab/f.bin",
        ] {
            let err = authorize(&roots, "objects", path).unwrap_err();
            assert!(matches!(err, TransferError::Rejected(_)), "{path:?}: {err}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_sibling_sharing_the_roots_prefix_is_refused() {
        // "shared-evil" shares a string prefix with the root's directory but
        // is not inside it. A symlink in the root leads there, so its
        // canonical path reaches the containment check, which only a
        // component-wise comparison refuses: as a string it starts with the
        // root.
        let base = tempfile::tempdir().unwrap();
        let shared = base.path().join("shared");
        let evil = base.path().join("shared-evil");
        std::fs::create_dir(&shared).unwrap();
        std::fs::create_dir(&evil).unwrap();
        std::fs::write(evil.join("f.txt"), b"x").unwrap();
        std::os::unix::fs::symlink(evil.join("f.txt"), shared.join("evil")).unwrap();
        let root = std::fs::canonicalize(&shared).unwrap();
        let escape = std::fs::canonicalize(evil.join("f.txt")).unwrap();
        let root_str = root.to_str().unwrap();
        assert!(escape.to_str().unwrap().starts_with(root_str));
        let mut roots = BTreeMap::new();
        roots.insert("shared".to_string(), root);

        let err = authorize(&roots, "shared", "evil").unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)), "{err}");
    }

    #[test]
    fn a_missing_file_or_a_directory_is_refused() {
        let (_dir, roots, _file) = holder();
        for path in ["ab/missing.bin", "ab", "nope/f.bin"] {
            let err = authorize(&roots, "objects", path).unwrap_err();
            assert!(matches!(err, TransferError::Rejected(_)), "{path:?}: {err}");
        }
    }

    #[test]
    fn an_oversize_file_is_refused() {
        let (_dir, roots, _file) = holder();
        let target = NamedPath {
            root: "objects".into(),
            path: "ab/f.bin".into(),
        };
        let err = authorize_named_pull(&roots, &target, 3).unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_file_that_leaves_the_root_is_refused() {
        let (_dir, roots, _file) = holder();
        let outside = tempfile::tempdir().unwrap();
        let secret = outside.path().join("secret.txt");
        std::fs::write(&secret, b"secret").unwrap();
        let ab = roots["objects"].join("ab");
        std::os::unix::fs::symlink(secret, ab.join("link.txt")).unwrap();

        let err = authorize(&roots, "objects", "ab/link.txt").unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_directory_that_leaves_the_root_is_refused() {
        let (_dir, roots, _file) = holder();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret.txt"), b"secret").unwrap();
        let link = roots["objects"].join("out");
        std::os::unix::fs::symlink(outside.path(), link).unwrap();

        let err = authorize(&roots, "objects", "out/secret.txt").unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_that_stays_inside_the_root_is_served_as_its_target() {
        // Symlinks are resolved, not refused: one whose target is inside the
        // root serves that target.
        let (_dir, roots, file) = holder();
        let link = roots["objects"].join("alias.bin");
        std::os::unix::fs::symlink(&file, link).unwrap();

        let got = authorize(&roots, "objects", "alias.bin").unwrap();
        assert_eq!(got, std::fs::canonicalize(file).unwrap());
    }

    // ── The requester's reading of a REJECT ────────────────────────────

    #[test]
    fn the_legacy_denial_on_a_named_pull_means_unsupported() {
        let err = pull_rejection(true, PULL_DENIED_REASON.to_string());
        assert!(matches!(err, TransferError::Unsupported(_)), "{err}");
    }

    #[test]
    fn a_named_denial_is_a_rejection() {
        let err = pull_rejection(true, NAMED_PULL_DENIED_REASON.to_string());
        assert!(
            matches!(&err, TransferError::Rejected(r) if r == NAMED_PULL_DENIED_REASON),
            "{err}"
        );
    }

    #[test]
    fn a_busy_holder_is_a_rejection_either_way() {
        for named in [true, false] {
            let err = pull_rejection(named, "receiver busy".to_string());
            assert!(
                matches!(&err, TransferError::Rejected(r) if r == "receiver busy"),
                "{err}"
            );
        }
    }

    #[test]
    fn an_absolute_pull_keeps_its_rejection() {
        // Absolute-path pulls read REJECTs exactly as before.
        let err = pull_rejection(false, PULL_DENIED_REASON.to_string());
        assert!(
            matches!(&err, TransferError::Rejected(r) if r == PULL_DENIED_REASON),
            "{err}"
        );
    }
}
