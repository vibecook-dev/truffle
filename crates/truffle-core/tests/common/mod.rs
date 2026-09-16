//! Shared helpers for truffle-core integration tests.
//!
//! All real-network tests share one auth-key gate and one two-node pair
//! harness. If `TRUFFLE_TEST_AUTHKEY` (or the legacy `TS_AUTHKEY`) is not
//! set, tests skip gracefully with a one-line notice.
//!
//! See `docs/rfcs/019-local-testing-and-benchmarking.md` for the design.

#![allow(dead_code)] // not every integration test file uses every helper

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use truffle_core::network::tailscale::{TailscaleConfig, TailscaleProvider};
use truffle_core::network::NetworkProvider;
use truffle_core::{Node, NodeBuilder};

/// The env var that gates real-network tests and benchmarks.
pub const AUTHKEY_ENV: &str = "TRUFFLE_TEST_AUTHKEY";

/// Legacy fallback. Deprecated; prints a warning when used.
pub const LEGACY_AUTHKEY_ENV: &str = "TS_AUTHKEY";

/// Load `.env` from the repo root once per test binary. Idempotent.
fn ensure_dotenv() {
    static LOADED: OnceLock<()> = OnceLock::new();
    LOADED.get_or_init(|| {
        // `dotenvy::dotenv()` walks up from CWD, so it finds `.env` at the
        // workspace root even when `cargo test -p truffle-core` runs with
        // CWD inside the crate directory.
        let _ = dotenvy::dotenv();
    });
}

/// Returns `Some(authkey)` if real-network work should run, or `None` with a
/// single-line `[skip]` notice printed to stderr.
///
/// Checks `TRUFFLE_TEST_AUTHKEY` first, then `TS_AUTHKEY` (with a
/// deprecation warning).
///
/// The `test_name` parameter is only used for the skip message — pass the
/// test function's name as a literal string.
pub fn require_authkey(test_name: &str) -> Option<String> {
    ensure_dotenv();

    if let Ok(k) = std::env::var(AUTHKEY_ENV) {
        if !k.is_empty() {
            return Some(k);
        }
    }

    if let Ok(k) = std::env::var(LEGACY_AUTHKEY_ENV) {
        if !k.is_empty() {
            eprintln!(
                "[warn] {test_name}: using {LEGACY_AUTHKEY_ENV}; \
                 {AUTHKEY_ENV} is preferred and will be the only supported \
                 name in a future release"
            );
            return Some(k);
        }
    }

    eprintln!("[skip] {test_name}: {AUTHKEY_ENV} not set");
    None
}

/// Read an env var with a default.
pub fn env_or(name: &str, default: &str) -> String {
    ensure_dotenv();
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Read a boolean env var with a default.
pub fn env_bool(name: &str, default: bool) -> bool {
    ensure_dotenv();
    match std::env::var(name) {
        Ok(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"),
        Err(_) => default,
    }
}

/// Parse `TRUFFLE_TEST_TAGS` (comma-separated). Returns `None` if unset or empty.
///
/// Defaults to `None` — most users don't need to advertise tags explicitly.
/// The auth key's own bound tag (set when the key was generated in the
/// Tailscale admin panel) applies automatically. Only set `TRUFFLE_TEST_TAGS`
/// if you know your tailnet ACL has a matching `tagOwners` entry for every
/// tag listed — otherwise tsnet rejects the login with "requested tags are
/// invalid or not permitted".
pub fn test_tags() -> Option<Vec<String>> {
    ensure_dotenv();
    let raw = match std::env::var("TRUFFLE_TEST_TAGS") {
        Ok(v) if !v.is_empty() => v,
        _ => return None,
    };
    let tags: Vec<String> = raw
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if tags.is_empty() {
        None
    } else {
        Some(tags)
    }
}

/// Whether test nodes should register as ephemeral. Default: true.
pub fn test_ephemeral() -> bool {
    env_bool("TRUFFLE_TEST_EPHEMERAL", true)
}

/// Redact an auth key to its first 12 characters + `...` for safe logging.
pub fn redact_authkey(key: &str) -> String {
    if key.len() <= 12 {
        "****".to_string()
    } else {
        format!("{}...", &key[..12])
    }
}

/// Install a tracing subscriber for tests. Idempotent — safe to call from every test.
pub fn init_test_tracing() {
    let filter = env_or("TRUFFLE_TEST_LOG", "truffle_core=info");
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .try_init()
        .ok();
}

// =============================================================================
// Two-node pair harness (RFC 019 §7)
// =============================================================================

/// Base app-id for pair-harness nodes. Peer filter matches
/// `truffle-{app_id}-*` hostnames, so both nodes in a pair see each other;
/// `PairOpts::default()` appends a per-run suffix so runs are hermetic.
pub const TEST_APP_ID: &str = "test-integ";

/// How long to wait for the pair to see each other via `peers()` before
/// giving up. Tailscale netmap propagation can be lazy, especially right
/// after registration.
pub const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(45);

/// Default path to the compiled test-sidecar binary.
pub fn default_sidecar_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/test-sidecar")
}

/// Options for customising the pair.
#[derive(Clone)]
pub struct PairOpts {
    pub app_id: String,
    pub rendezvous_timeout: Duration,
    pub sidecar_path: PathBuf,
    /// When true (default), `make_truffle_pair*` fires `_pair_warmup` messages
    /// after rendezvous to force a WS + hello exchange, so tests can address
    /// peers by ULID immediately. Set false to observe RFC 022 §8 *eager*
    /// identity: with warm-up off, no application traffic is sent at all, so a
    /// non-null `device_id` can only come from the session dialing the bus on
    /// its own. Ignored by the Layer-3-only [`make_pair_of_nodes_with`] path,
    /// which never warms up.
    pub warm_up: bool,
}

impl Default for PairOpts {
    fn default() -> Self {
        // Unique per-run app id: RFC 017 namespacing filters discovery to
        // `truffle-{app_id}-*` hostnames, so ghost ephemerals from previous
        // runs (which linger on the tailnet ~30–60 min after their process
        // dies) stay invisible. With a shared app id, every run's
        // eager-identity dialer chases the accumulated dead ghosts and
        // convergence windows turn flaky.
        let short: String = uuid::Uuid::new_v4().to_string().chars().take(8).collect();
        Self {
            app_id: format!("{TEST_APP_ID}-{short}"),
            rendezvous_timeout: RENDEZVOUS_TIMEOUT,
            sidecar_path: default_sidecar_path(),
            warm_up: true,
        }
    }
}

/// Two TailscaleProviders registered on the same tailnet, known to have
/// discovered each other. Both nodes are ephemeral — they disappear from
/// the tailnet ~30–60 min after drop (or sooner when `stop()` is called).
///
/// TempDirs for per-node state are kept alive by the struct; on drop they
/// are removed. Child sidecar processes die via `kill_on_drop(true)`.
pub struct NodePair {
    pub alpha: TailscaleProvider,
    pub beta: TailscaleProvider,
    pub alpha_hostname: String,
    pub beta_hostname: String,
    _alpha_state: tempfile::TempDir,
    _beta_state: tempfile::TempDir,
}

impl NodePair {
    /// Gracefully stop both providers. Drop alone is enough for cleanup,
    /// but calling this gives a deterministic teardown point in tests.
    pub async fn stop(self) {
        let _ = self.alpha.stop().await;
        let _ = self.beta.stop().await;
    }

    /// Tailscale IP of the alpha node.
    pub async fn alpha_ip(&self) -> std::net::IpAddr {
        self.alpha
            .local_identity_async()
            .await
            .ip
            .expect("alpha should have a Tailscale IP after rendezvous")
    }

    /// Tailscale IP of the beta node.
    pub async fn beta_ip(&self) -> std::net::IpAddr {
        self.beta
            .local_identity_async()
            .await
            .ip
            .expect("beta should have a Tailscale IP after rendezvous")
    }
}

/// Create a pair of nodes with default options.
pub async fn make_pair_of_nodes(authkey: &str) -> NodePair {
    make_pair_of_nodes_with(authkey, PairOpts::default()).await
}

/// Create a pair of nodes with custom options.
pub async fn make_pair_of_nodes_with(authkey: &str, opts: PairOpts) -> NodePair {
    assert_sidecar_exists(&opts.sidecar_path);

    let run_id = uuid::Uuid::new_v4().to_string();
    let short_id: String = run_id.chars().take(8).collect();

    let alpha_state =
        tempfile::TempDir::with_prefix("truffle-test-alpha-").expect("create alpha tempdir");
    let beta_state =
        tempfile::TempDir::with_prefix("truffle-test-beta-").expect("create beta tempdir");

    let alpha_hostname = format!("truffle-{}-a{}", opts.app_id, short_id);
    let beta_hostname = format!("truffle-{}-b{}", opts.app_id, short_id);

    let alpha_config = build_config(
        authkey,
        &opts,
        &alpha_hostname,
        format!("alpha-{short_id}"),
        alpha_state.path(),
    );
    let beta_config = build_config(
        authkey,
        &opts,
        &beta_hostname,
        format!("beta-{short_id}"),
        beta_state.path(),
    );

    let mut alpha = TailscaleProvider::new(alpha_config);
    let mut beta = TailscaleProvider::new(beta_config);

    eprintln!(
        "[pair] starting alpha={alpha_hostname} beta={beta_hostname} \
         (authkey={})",
        redact_authkey(authkey)
    );

    let (alpha_start, beta_start) = tokio::join!(alpha.start(), beta.start());
    alpha_start.expect("alpha start");
    beta_start.expect("beta start");

    rendezvous(
        &alpha,
        &beta,
        &alpha_hostname,
        &beta_hostname,
        opts.rendezvous_timeout,
    )
    .await;

    eprintln!(
        "[pair] rendezvous complete — alpha_ip={} beta_ip={}",
        alpha
            .local_identity_async()
            .await
            .ip
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "?".to_string()),
        beta.local_identity_async()
            .await
            .ip
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "?".to_string()),
    );

    NodePair {
        alpha,
        beta,
        alpha_hostname,
        beta_hostname,
        _alpha_state: alpha_state,
        _beta_state: beta_state,
    }
}

fn build_config(
    authkey: &str,
    opts: &PairOpts,
    hostname: &str,
    device_name: String,
    state_dir: &Path,
) -> TailscaleConfig {
    TailscaleConfig {
        binary_path: opts.sidecar_path.clone(),
        app_id: opts.app_id.clone(),
        device_id: ulid::Ulid::new().to_string(),
        device_name,
        hostname: hostname.to_string(),
        state_dir: state_dir.to_string_lossy().into_owned(),
        auth_key: Some(authkey.to_string()),
        ephemeral: Some(test_ephemeral()),
        tags: test_tags(),
        idle_timeout_secs: None,
        login_allow: Vec::new(),
    }
}

async fn rendezvous(
    alpha: &TailscaleProvider,
    beta: &TailscaleProvider,
    alpha_hostname: &str,
    beta_hostname: &str,
    timeout_dur: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout_dur;
    let mut attempts = 0_u32;
    loop {
        attempts += 1;
        let alpha_peers = alpha.peers().await;
        let beta_peers = beta.peers().await;

        let alpha_sees_beta = alpha_peers.iter().any(|p| p.hostname == beta_hostname);
        let beta_sees_alpha = beta_peers.iter().any(|p| p.hostname == alpha_hostname);

        if alpha_sees_beta && beta_sees_alpha {
            return;
        }

        if tokio::time::Instant::now() >= deadline {
            eprintln!("[pair] rendezvous diagnostic after {attempts} polls:");
            eprintln!(
                "  alpha ({alpha_hostname}) sees {} peer(s): {:?}",
                alpha_peers.len(),
                alpha_peers.iter().map(|p| &p.hostname).collect::<Vec<_>>()
            );
            eprintln!(
                "  beta ({beta_hostname}) sees {} peer(s): {:?}",
                beta_peers.len(),
                beta_peers.iter().map(|p| &p.hostname).collect::<Vec<_>>()
            );
            panic!(
                "pair rendezvous timeout after {timeout_dur:?}: \
                 alpha_sees_beta={alpha_sees_beta} \
                 beta_sees_alpha={beta_sees_alpha}"
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

// =============================================================================
// Two-node Node-level pair (RFC 019 §7, §9)
// =============================================================================

/// Two full truffle Nodes on the same tailnet, wired through to Layer 7.
/// Each owns its own TailscaleProvider, WebSocket transport, session, and
/// envelope router.
///
/// Use this when the test exercises the Node API (SyncedStore, file transfer,
/// request/reply). For Layer-3-only tests, use [`NodePair`] instead.
pub struct TrufflePair {
    pub alpha: Arc<Node<TailscaleProvider>>,
    pub beta: Arc<Node<TailscaleProvider>>,
    pub alpha_device_name: String,
    pub beta_device_name: String,
    pub alpha_device_id: String,
    pub beta_device_id: String,
    _alpha_state: tempfile::TempDir,
    _beta_state: tempfile::TempDir,
}

impl TrufflePair {
    /// Gracefully stop both nodes. Drop is also safe (kill_on_drop +
    /// TempDir cleanup), but explicit stop gives deterministic teardown.
    pub async fn stop(self) {
        self.alpha.stop().await;
        self.beta.stop().await;
    }
}

/// Create a TrufflePair with default options.
pub async fn make_truffle_pair(authkey: &str) -> TrufflePair {
    make_truffle_pair_with(authkey, PairOpts::default()).await
}

/// Create a TrufflePair that skips the post-rendezvous `_pair_warmup` message
/// exchange (`warm_up: false`).
///
/// Rendezvous (Layer-3 mutual visibility) still runs, but **no application
/// traffic is sent**, so any `device_id` a side later learns must come from
/// RFC 022 §8 *eager identity* — the session dialing the envelope bus on its
/// own to exchange the RFC 017 hello. Used by
/// `tests/integration_eager_identity.rs` to prove eagerness end-to-end; the
/// default [`make_truffle_pair`] warm-up would mask it by sending traffic.
pub async fn make_truffle_pair_no_warmup(authkey: &str) -> TrufflePair {
    make_truffle_pair_with(
        authkey,
        PairOpts {
            warm_up: false,
            ..PairOpts::default()
        },
    )
    .await
}

/// Create a TrufflePair with custom options. Uses WS ports 9417/9418 by
/// default — each Node listens on its own tailnet IP so there is no port
/// conflict between the two.
pub async fn make_truffle_pair_with(authkey: &str, opts: PairOpts) -> TrufflePair {
    assert_sidecar_exists(&opts.sidecar_path);

    let run_id = uuid::Uuid::new_v4().to_string();
    let short_id: String = run_id.chars().take(8).collect();

    let alpha_state =
        tempfile::TempDir::with_prefix("truffle-test-alpha-").expect("create alpha tempdir");
    let beta_state =
        tempfile::TempDir::with_prefix("truffle-test-beta-").expect("create beta tempdir");

    let alpha_name = format!("alpha-{short_id}");
    let beta_name = format!("beta-{short_id}");

    eprintln!(
        "[truffle-pair] building alpha={alpha_name} beta={beta_name} \
         (authkey={})",
        redact_authkey(authkey)
    );

    // Both sides MUST use the same ws_port: a Node's WS transport dials
    // `peer_ip:self_port` when connecting, so mismatched ports mean the
    // dialer hits a closed port. Each Node listens on its own Tailscale
    // IP, so sharing the port is safe.
    let alpha_fut = NodeBuilder::default()
        .app_id(opts.app_id.clone())
        .expect("app_id should parse")
        .device_name(alpha_name.clone())
        .sidecar_path(opts.sidecar_path.clone())
        .state_dir(alpha_state.path().to_str().expect("utf8 path"))
        .auth_key(authkey)
        .ephemeral(true)
        .ws_port(9417)
        .build();
    let beta_fut = NodeBuilder::default()
        .app_id(opts.app_id.clone())
        .expect("app_id should parse")
        .device_name(beta_name.clone())
        .sidecar_path(opts.sidecar_path.clone())
        .state_dir(beta_state.path().to_str().expect("utf8 path"))
        .auth_key(authkey)
        .ephemeral(true)
        .ws_port(9417)
        .build();

    let (alpha_res, beta_res) = tokio::join!(alpha_fut, beta_fut);
    let alpha = Arc::new(alpha_res.expect("alpha Node build"));
    let beta = Arc::new(beta_res.expect("beta Node build"));

    let alpha_id = alpha.local_info().device_id;
    let beta_id = beta.local_info().device_id;

    rendezvous_nodes(
        &alpha,
        &beta,
        &alpha_name,
        &beta_name,
        opts.rendezvous_timeout,
    )
    .await;

    // Layer-3 peers are visible, but the Session layer hasn't received the
    // other side's hello yet — so `resolve_peer_id(<ULID>)` would still fail.
    // Force a WS + hello exchange so tests can address each other by ULID.
    //
    // Eager-identity tests (RFC 022 §8) turn this OFF via `warm_up: false`: a
    // learned `device_id` must then come only from the session's own eager
    // bus dial, never from warm-up app traffic. Rendezvous above still ran, so
    // the peers are mutually visible at Layer 3 either way.
    if opts.warm_up {
        warm_up_pair(
            &alpha,
            &beta,
            &alpha_name,
            &beta_name,
            opts.rendezvous_timeout,
        )
        .await;

        eprintln!(
            "[truffle-pair] warm-up complete — alpha_device_id={alpha_id} beta_device_id={beta_id}"
        );
    } else {
        eprintln!(
            "[truffle-pair] warm-up SKIPPED (eager-identity mode, no app sends) — \
             alpha_device_id={alpha_id} beta_device_id={beta_id}"
        );
    }

    TrufflePair {
        alpha,
        beta,
        alpha_device_name: alpha_name,
        beta_device_name: beta_name,
        alpha_device_id: alpha_id,
        beta_device_id: beta_id,
        _alpha_state: alpha_state,
        _beta_state: beta_state,
    }
}

async fn rendezvous_nodes(
    alpha: &Arc<Node<TailscaleProvider>>,
    beta: &Arc<Node<TailscaleProvider>>,
    alpha_name: &str,
    beta_name: &str,
    timeout_dur: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout_dur;
    loop {
        let alpha_peers = alpha.peers().await;
        let beta_peers = beta.peers().await;

        let alpha_sees_beta = alpha_peers.iter().any(|p| {
            p.hostname.contains(beta_name)
                || p.device_name
                    .as_deref()
                    .is_some_and(|n| n.contains(beta_name))
        });
        let beta_sees_alpha = beta_peers.iter().any(|p| {
            p.hostname.contains(alpha_name)
                || p.device_name
                    .as_deref()
                    .is_some_and(|n| n.contains(alpha_name))
        });

        if alpha_sees_beta && beta_sees_alpha {
            return;
        }

        if tokio::time::Instant::now() >= deadline {
            eprintln!("[truffle-pair] rendezvous diagnostic:");
            eprintln!(
                "  alpha ({alpha_name}) sees {} peer(s): {:?}",
                alpha_peers.len(),
                alpha_peers
                    .iter()
                    .map(|p| format!("{} ({})", p.hostname, p.display_name))
                    .collect::<Vec<_>>()
            );
            eprintln!(
                "  beta ({beta_name}) sees {} peer(s): {:?}",
                beta_peers.len(),
                beta_peers
                    .iter()
                    .map(|p| format!("{} ({})", p.hostname, p.display_name))
                    .collect::<Vec<_>>()
            );
            panic!(
                "truffle-pair rendezvous timeout after {timeout_dur:?}: \
                 alpha_sees_beta={alpha_sees_beta} beta_sees_alpha={beta_sees_alpha}"
            );
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Exchange a dummy message each direction and wait until both sides have
/// a live WS to the other — at which point the hello handshake has run and
/// the peer registry carries the real ULID device_id.
///
/// Retries once on timeout: simultaneous dual-dials on a fresh pair can race
/// (both connect, both tear down the "losing" socket) and leave
/// `ws_connected=false` even though hello briefly completed. Soft-failing used
/// to mask that and produce confusing failures deep inside app tests.
async fn warm_up_pair(
    alpha: &Arc<Node<TailscaleProvider>>,
    beta: &Arc<Node<TailscaleProvider>>,
    alpha_name: &str,
    beta_name: &str,
    timeout_dur: Duration,
) {
    // Find the tailscale_id of each peer from the other's Layer-3 view.
    let beta_ts_id = alpha
        .peers()
        .await
        .into_iter()
        .find(|p| {
            p.hostname.contains(beta_name)
                || p.device_name
                    .as_deref()
                    .is_some_and(|n| n.contains(beta_name))
        })
        .map(|p| p.tailscale_id)
        .expect("beta must be in alpha's peer list after rendezvous");
    let alpha_ts_id = beta
        .peers()
        .await
        .into_iter()
        .find(|p| {
            p.hostname.contains(alpha_name)
                || p.device_name
                    .as_deref()
                    .is_some_and(|n| n.contains(alpha_name))
        })
        .map(|p| p.tailscale_id)
        .expect("alpha must be in beta's peer list after rendezvous");

    // Fire warm-up messages. These go out on the `_pair_warmup` namespace,
    // which applications don't subscribe to — the receiver drops them, but
    // the Session layer still performs the WS connect + hello exchange.
    // Up to two attempts: dual-dial races are common on first try.
    for attempt in 1..=2u8 {
        let _ = alpha
            .send_typed(
                &beta_ts_id,
                "_pair_warmup",
                "ping",
                &serde_json::Value::Null,
            )
            .await;
        // Stagger the reverse dial slightly so both sides don't always race
        // the same simultaneous connect/accept teardown path.
        if attempt == 1 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let _ = beta
            .send_typed(
                &alpha_ts_id,
                "_pair_warmup",
                "ping",
                &serde_json::Value::Null,
            )
            .await;

        // Wait until both sides see the other as ws_connected.
        let per_attempt = if attempt == 1 {
            timeout_dur
        } else {
            // Second try is usually a reconnect; don't burn a full 45s again.
            timeout_dur.min(Duration::from_secs(20))
        };
        let deadline = tokio::time::Instant::now() + per_attempt;
        loop {
            let a_ok = alpha
                .peers()
                .await
                .iter()
                .any(|p| p.tailscale_id == beta_ts_id && p.ws_connected);
            let b_ok = beta
                .peers()
                .await
                .iter()
                .any(|p| p.tailscale_id == alpha_ts_id && p.ws_connected);
            if a_ok && b_ok {
                if attempt > 1 {
                    eprintln!("[truffle-pair] warm-up succeeded on attempt {attempt}");
                }
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                eprintln!(
                    "[truffle-pair] warm-up timeout attempt={attempt}: \
                     a_ok={a_ok} b_ok={b_ok} \
                     (ws connection didn't establish in {per_attempt:?})"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }

        if attempt < 2 {
            eprintln!("[truffle-pair] warm-up retrying after brief pause…");
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    panic!(
        "truffle-pair warm-up failed after 2 attempts: WS bus never stayed \
         connected between {alpha_name} and {beta_name}. \
         Synced-store / messaging tests require a live session hello."
    );
}

/// Poll `predicate` until it returns `Some(T)` or the deadline passes.
/// Returns `None` on timeout. Useful for "wait until converged" patterns
/// in integration tests.
pub async fn wait_for<T, F, Fut>(timeout: Duration, mut predicate: F) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(v) = predicate().await {
            return Some(v);
        }
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn assert_sidecar_exists(path: &Path) {
    if !path.exists() {
        panic!(
            "test-sidecar binary not found at {}.\n\
             \n\
             truffle-core's build.rs builds this automatically when Go 1.22+ \
             is on PATH. Possible causes:\n\
               - `go` is not installed — install from https://go.dev/dl/\n\
               - `go build` failed (check cargo warnings from the previous build)\n\
               - The packages/sidecar-slim/ directory is missing\n\
             \n\
             Fallback: cd packages/sidecar-slim && go build -o {} .",
            path.display(),
            path.display()
        );
    }
}
