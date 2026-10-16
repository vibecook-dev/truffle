//! The legacy `transport` workload (RFC 027 §9.1), kept binary-only.
//!
//! Moved verbatim from the pre-RFC 027 crate: it still writes
//! `schema_version: 2` reports, stays byte-compatible with
//! `packages/sidecar-slim/cmd/tsnet-bench` (u64 size, payload, SHA-256 ACK,
//! never the TBN1 header) and still needs an auth key from the environment.
//! The only changes: the sidecar path comes from
//! [`truffle_bench::config::resolve_sidecar_with`], and the report params
//! additionally record `mode`, `port` and `app_id`.

pub mod error;
pub mod harness;
pub mod report_v2;
pub mod transport;

pub use error as anyhow_lite;

/// The auth key for the legacy workload: `TRUFFLE_TEST_AUTHKEY`, else
/// `TS_AUTHKEY` with a warning. This is a benchmark command, not a test, so a
/// missing key is an error rather than a silent skip.
pub fn require_authkey() -> anyhow_lite::Result<String> {
    if let Ok(k) = std::env::var("TRUFFLE_TEST_AUTHKEY") {
        if !k.is_empty() {
            return Ok(k);
        }
    }
    if let Ok(k) = std::env::var("TS_AUTHKEY") {
        if !k.is_empty() {
            eprintln!("[warn] using TS_AUTHKEY; TRUFFLE_TEST_AUTHKEY is preferred");
            return Ok(k);
        }
    }
    anyhow_lite::bail!(
        "TRUFFLE_TEST_AUTHKEY is not set.\n\
         \n\
         truffle-bench transport runs against a real Tailscale network and needs a valid auth key.\n\
         See docs/TESTING.md for setup, or set the env var directly:\n\
             TRUFFLE_TEST_AUTHKEY=tskey-auth-… cargo run -p truffle-bench -- transport"
    );
}
