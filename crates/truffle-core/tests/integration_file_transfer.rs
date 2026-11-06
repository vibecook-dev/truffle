//! Integration tests for file transfer over a real tailnet pair.
//!
//! Exercises the Layer-7 file-transfer subsystem end-to-end: OFFER/ACCEPT
//! handshake on the `"ft"` namespace, TCP data channel over Tailscale,
//! and SHA-256 integrity verification.
//!
//! Skipped when `TRUFFLE_TEST_AUTHKEY` is not set.

mod common;

use std::time::Duration;

use truffle_core::file_transfer::NAMED_PULL_DENIED_REASON;
use truffle_core::TransferError;

const WAIT_FOR_FILE: Duration = Duration::from_secs(30);

/// Deterministic payload: `(i * 7 + 13) % 256`.
fn make_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| ((i * 7 + 13) % 256) as u8).collect()
}

/// Print a send's phase timings and check they are sane: every phase took
/// time, and the disjoint phases fit inside the end-to-end figure.
///
/// `ack_ok` is printed, not asserted: a missing ACK is a reported outcome
/// (the bridge may close before the byte propagates), not a failure.
fn check_timings(result: &truffle_core::TransferResult) {
    let t = result.timings;
    eprintln!(
        "  timings: hash {:.4}s, negotiate {:.4}s, transfer {:.4}s, ack_ok {} (elapsed {:.4}s)",
        t.hash_secs, t.negotiate_secs, t.transfer_secs, t.ack_ok, result.elapsed_secs
    );
    assert!(t.hash_secs > 0.0, "hashing takes time: {t:?}");
    assert!(
        t.negotiate_secs > 0.0,
        "the OFFER round trip takes time: {t:?}"
    );
    assert!(t.transfer_secs > 0.0, "the data transfer takes time: {t:?}");
    let phases = t.hash_secs + t.negotiate_secs + t.transfer_secs;
    assert!(
        phases <= result.elapsed_secs + 1e-6,
        "phases ({phases}s) must fit inside elapsed ({}s)",
        result.elapsed_secs
    );
}

// ---------------------------------------------------------------------------
// Test 1: Small file (64KB) — round-trip integrity
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_transfer_64kb_file() {
    let Some(authkey) = common::require_authkey("test_transfer_64kb_file") else {
        return;
    };
    common::init_test_tracing();

    let pair = common::make_truffle_pair(&authkey).await;

    let src_dir = tempfile::TempDir::new().expect("src tempdir");
    let dst_dir = tempfile::TempDir::new().expect("dst tempdir");

    let src_path = src_dir.path().join("small.bin");
    let content = make_payload(64 * 1024);
    tokio::fs::write(&src_path, &content)
        .await
        .expect("write source file");

    // Beta: auto-accept everything into dst_dir
    pair.beta
        .file_transfer()
        .auto_accept(
            pair.beta.clone(),
            dst_dir.path().to_str().expect("utf8 path"),
        )
        .await;

    // Alpha: send. Pass "." as remote_path so auto_accept treats it as
    // "use your configured output_dir + the file's basename" rather than
    // a literal path relative to the receiver's CWD.
    let file_name = src_path.file_name().unwrap().to_str().unwrap().to_string();
    let result = pair
        .alpha
        .file_transfer()
        .send_file(
            &pair.beta_device_id,
            src_path.to_str().expect("utf8 path"),
            ".",
        )
        .await
        .expect("send_file should succeed");

    eprintln!(
        "  sent {} bytes in {:.3}s, sha256={}",
        result.bytes_transferred, result.elapsed_secs, result.sha256
    );
    assert_eq!(result.bytes_transferred, content.len() as u64);
    check_timings(&result);

    let received_path = dst_dir.path().join(&file_name);
    let received = common::wait_for(WAIT_FOR_FILE, || async {
        tokio::fs::read(&received_path).await.ok()
    })
    .await
    .expect("file should land on beta within 30s");

    assert_eq!(
        received, content,
        "received file must match sent content byte-for-byte"
    );

    // Each running node reports its own sidecar child; none once stopped.
    let (alpha, beta) = (pair.alpha.clone(), pair.beta.clone());
    let alpha_pid = alpha.sidecar_pid().expect("running node has a sidecar pid");
    let beta_pid = beta.sidecar_pid().expect("running node has a sidecar pid");
    assert_ne!(alpha_pid, beta_pid, "each node runs its own sidecar");

    pair.stop().await;
    assert_eq!(alpha.sidecar_pid(), None, "stopped node reports no pid");
    assert_eq!(beta.sidecar_pid(), None, "stopped node reports no pid");
}

// ---------------------------------------------------------------------------
// Test 2: 1MB file — exercises chunked streaming path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_transfer_1mb_file() {
    let Some(authkey) = common::require_authkey("test_transfer_1mb_file") else {
        return;
    };
    common::init_test_tracing();

    let pair = common::make_truffle_pair(&authkey).await;

    let src_dir = tempfile::TempDir::new().expect("src tempdir");
    let dst_dir = tempfile::TempDir::new().expect("dst tempdir");

    let src_path = src_dir.path().join("medium.bin");
    let content = make_payload(1024 * 1024);
    tokio::fs::write(&src_path, &content)
        .await
        .expect("write source file");

    pair.beta
        .file_transfer()
        .auto_accept(
            pair.beta.clone(),
            dst_dir.path().to_str().expect("utf8 path"),
        )
        .await;

    let file_name = src_path.file_name().unwrap().to_str().unwrap().to_string();
    let result = pair
        .alpha
        .file_transfer()
        .send_file(
            &pair.beta_device_id,
            src_path.to_str().expect("utf8 path"),
            ".",
        )
        .await
        .expect("send_file should succeed");

    eprintln!(
        "  sent {} bytes in {:.3}s ({:.2} MB/s)",
        result.bytes_transferred,
        result.elapsed_secs,
        (result.bytes_transferred as f64 / result.elapsed_secs.max(0.001)) / 1_000_000.0
    );
    assert_eq!(result.bytes_transferred, content.len() as u64);
    check_timings(&result);

    let received_path = dst_dir.path().join(&file_name);
    let received = common::wait_for(WAIT_FOR_FILE, || async {
        let Ok(data) = tokio::fs::read(&received_path).await else {
            return None;
        };
        // Only accept fully written file
        if data.len() == content.len() {
            Some(data)
        } else {
            None
        }
    })
    .await
    .expect("file should land fully on beta within 30s");

    assert_eq!(received.len(), content.len());
    assert_eq!(received, content, "1MB file integrity check failed");

    pair.stop().await;
}

// ---------------------------------------------------------------------------
// Test 3: Rejected offer — sender sees an error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_transfer_rejected() {
    let Some(authkey) = common::require_authkey("test_transfer_rejected") else {
        return;
    };
    common::init_test_tracing();

    let pair = common::make_truffle_pair(&authkey).await;

    let src_dir = tempfile::TempDir::new().expect("src tempdir");
    let src_path = src_dir.path().join("rejected.bin");
    tokio::fs::write(&src_path, b"nope")
        .await
        .expect("write source file");

    // Beta: reject everything
    pair.beta
        .file_transfer()
        .auto_reject(pair.beta.clone())
        .await;

    let result = pair
        .alpha
        .file_transfer()
        .send_file(
            &pair.beta_device_id,
            src_path.to_str().expect("utf8 path"),
            "rejected.bin",
        )
        .await;

    assert!(
        result.is_err(),
        "send_file to a rejecting peer must return Err, got {result:?}"
    );

    pair.stop().await;
}

// ---------------------------------------------------------------------------
// Test 4: Named pull roots (RFC 032) — pull by {root, relative path}
// ---------------------------------------------------------------------------

/// `dir/name`, as the string the file-transfer API takes.
fn path_in(dir: &tempfile::TempDir, name: &str) -> String {
    dir.path().join(name).to_string_lossy().into_owned()
}

#[tokio::test]
async fn test_named_pull_roots() {
    let Some(authkey) = common::require_authkey("test_named_pull_roots") else {
        return;
    };
    common::init_test_tracing();

    let pair = common::make_truffle_pair(&authkey).await;

    // Beta shares one directory by name and another by absolute path.
    let named_root = tempfile::TempDir::new().expect("named root tempdir");
    let absolute_root = tempfile::TempDir::new().expect("absolute root tempdir");
    let object = make_payload(256 * 1024);
    std::fs::create_dir(named_root.path().join("ab")).expect("create ab/");
    let object_path = named_root.path().join("ab").join("object.bin");
    std::fs::write(&object_path, &object).expect("write the named object");
    let shared_path = path_in(&absolute_root, "shared.bin");
    std::fs::write(&shared_path, b"shared by absolute path").expect("write the shared file");

    let beta_ft = pair.beta.file_transfer();
    beta_ft
        .add_named_pull_root("objects", &named_root.path().to_string_lossy())
        .expect("register the named root");
    beta_ft
        .add_pull_root(&absolute_root.path().to_string_lossy())
        .expect("register the absolute root");
    // Beta answers pulls from its receiver, which a reject-all push policy starts.
    beta_ft.auto_reject(pair.beta.clone()).await;

    let dst = tempfile::TempDir::new().expect("dst tempdir");
    let alpha_ft = pair.alpha.file_transfer();
    let beta_id = pair.beta_device_id.as_str();

    // 1. A named pull lands the object byte for byte.
    let object_dst = path_in(&dst, "object.bin");
    let result = alpha_ft
        .pull_named_file(beta_id, "objects", "ab/object.bin", &object_dst)
        .await
        .expect("named pull should succeed");
    assert_eq!(result.bytes_transferred, object.len() as u64);
    let pulled = std::fs::read(object_dst).expect("read the pulled object");
    assert_eq!(
        pulled, object,
        "the named pull must land the object byte for byte"
    );

    // 2. An absolute-path pull is still served from an absolute root.
    let shared_dst = path_in(&dst, "shared.bin");
    alpha_ft
        .pull_file(beta_id, &shared_path, &shared_dst)
        .await
        .expect("an absolute-path pull should still succeed");
    let pulled = std::fs::read(shared_dst).expect("read the pulled shared file");
    assert_eq!(pulled, b"shared by absolute path");

    // 3. An unknown root is refused with the named denial: the holder speaks
    //    RFC 032, so this is a refusal, not Unsupported.
    let missing_dst = path_in(&dst, "missing.bin");
    let err = alpha_ft
        .pull_named_file(beta_id, "missing", "ab/object.bin", &missing_dst)
        .await
        .unwrap_err();
    assert!(
        matches!(&err, TransferError::Rejected(r) if r == NAMED_PULL_DENIED_REASON),
        "{err}"
    );

    // 4. The named object's absolute path is not served: a named root
    //    answers by its name only.
    let by_path_dst = path_in(&dst, "by-path.bin");
    let err = alpha_ft
        .pull_file(beta_id, &object_path.to_string_lossy(), &by_path_dst)
        .await
        .unwrap_err();
    assert!(matches!(err, TransferError::Rejected(_)), "{err}");

    // 5. A symlink inside the named root that leaves it is refused.
    #[cfg(unix)]
    {
        let outside = tempfile::TempDir::new().expect("outside tempdir");
        let secret = outside.path().join("secret.txt");
        std::fs::write(&secret, b"secret").expect("write the secret");
        let link = named_root.path().join("ab").join("link.txt");
        std::os::unix::fs::symlink(secret, link).expect("plant the symlink");
        let link_dst = path_in(&dst, "link.bin");
        let err = alpha_ft
            .pull_named_file(beta_id, "objects", "ab/link.txt", &link_dst)
            .await
            .unwrap_err();
        assert!(matches!(err, TransferError::Rejected(_)), "{err}");
    }

    for name in ["missing.bin", "by-path.bin", "link.bin"] {
        assert!(
            !dst.path().join(name).exists(),
            "{name} must not have landed"
        );
    }

    pair.stop().await;
}
