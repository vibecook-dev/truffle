//! Pair harness for real-network benchmarks.
//!
//! NOTE: this is a slimmed-down copy of the pair-building logic that lives in
//! `truffle-core/tests/common/mod.rs`. A future cleanup (tracked alongside
//! RFC 019) will unify both behind a `test-harness` feature on truffle-core.
//! For now, duplication keeps truffle-bench independent of the test binaries
//! so `cargo run -p truffle-bench` doesn't need dev-dependencies.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use truffle_core::network::tailscale::TailscaleProvider;
use truffle_core::{Node, NodeBuilder};

pub const TEST_APP_ID: &str = "bench-integ";
pub const RENDEZVOUS_TIMEOUT: Duration = Duration::from_secs(60);

#[allow(dead_code)] // alpha_device_id + device_name fields are handy for future workloads
pub struct BenchPair {
    pub alpha: Arc<Node<TailscaleProvider>>,
    pub beta: Arc<Node<TailscaleProvider>>,
    pub alpha_device_id: String,
    pub beta_device_id: String,
    pub alpha_device_name: String,
    pub beta_device_name: String,
    _alpha_state: tempfile::TempDir,
    _beta_state: tempfile::TempDir,
}

impl BenchPair {
    pub async fn diagnostics(&self) -> serde_json::Value {
        diagnostics(&self.alpha, &self.beta_device_id, "same-host").await
    }

    pub async fn stop(self) {
        self.alpha.stop().await;
        self.beta.stop().await;
    }
}

pub async fn make_bench_pair(authkey: &str) -> Result<BenchPair, String> {
    let sidecar = default_sidecar_path();
    if !sidecar.exists() {
        return Err(format!(
            "test-sidecar not found at {}. Run `cargo build -p truffle-core` to auto-build it \
             (requires the Go version in packages/sidecar-slim/go.mod).",
            sidecar.display()
        ));
    }

    let run_id = uuid::Uuid::new_v4().to_string();
    let short: String = run_id.chars().take(8).collect();

    // Unique per-run app id keeps ghost ephemerals from previous runs out of
    // discovery (RFC 017 namespacing) — see tests/common/mod.rs in
    // truffle-core for the full rationale.
    let app_id = format!("{TEST_APP_ID}-{short}");

    let alpha_state =
        tempfile::TempDir::with_prefix("truffle-bench-alpha-").map_err(|e| e.to_string())?;
    let beta_state =
        tempfile::TempDir::with_prefix("truffle-bench-beta-").map_err(|e| e.to_string())?;

    let alpha_name = format!("alpha-{short}");
    let beta_name = format!("beta-{short}");

    eprintln!("[bench-pair] building alpha={alpha_name} beta={beta_name}");

    // Both sides use the same ws_port: WS transport dials `peer_ip:self_port`
    // so a mismatch = closed port. Each node is on its own tailnet IP.
    let alpha_fut = NodeBuilder::default()
        .app_id(app_id.clone())
        .map_err(|e| e.to_string())?
        .device_name(alpha_name.clone())
        .sidecar_path(sidecar.clone())
        .state_dir(alpha_state.path().to_str().ok_or("utf8 alpha path")?)
        .auth_key(authkey)
        .ephemeral(true)
        .ws_port(9417)
        .build();
    let beta_fut = NodeBuilder::default()
        .app_id(app_id.clone())
        .map_err(|e| e.to_string())?
        .device_name(beta_name.clone())
        .sidecar_path(sidecar)
        .state_dir(beta_state.path().to_str().ok_or("utf8 beta path")?)
        .auth_key(authkey)
        .ephemeral(true)
        .ws_port(9417)
        .build();

    let (alpha_res, beta_res) = tokio::join!(alpha_fut, beta_fut);
    let (alpha, beta) = match (alpha_res, beta_res) {
        (Ok(a), Ok(b)) => (Arc::new(a), Arc::new(b)),
        (Ok(a), Err(e)) => {
            a.stop().await;
            return Err(format!("beta build: {e}"));
        }
        (Err(e), Ok(b)) => {
            b.stop().await;
            return Err(format!("alpha build: {e}"));
        }
        (Err(e), Err(_)) => return Err(format!("alpha build: {e}")),
    };

    let alpha_id = alpha.local_info().device_id;
    let beta_id = beta.local_info().device_id;

    let ready = async {
        rendezvous(&alpha, &beta, &alpha_name, &beta_name, RENDEZVOUS_TIMEOUT).await?;
        warm_up(&alpha, &beta, &alpha_name, &beta_name, RENDEZVOUS_TIMEOUT).await
    }
    .await;
    if let Err(e) = ready {
        alpha.stop().await;
        beta.stop().await;
        return Err(e);
    }

    eprintln!("[bench-pair] ready — alpha={alpha_id} beta={beta_id}");

    Ok(BenchPair {
        alpha,
        beta,
        alpha_device_id: alpha_id,
        beta_device_id: beta_id,
        alpha_device_name: alpha_name,
        beta_device_name: beta_name,
        _alpha_state: alpha_state,
        _beta_state: beta_state,
    })
}

pub fn default_sidecar_path() -> PathBuf {
    // CARGO_MANIFEST_DIR is crates/truffle-bench; test-sidecar lives in
    // crates/truffle-core/tests/test-sidecar.
    std::env::var_os("TRUFFLE_TEST_SIDECAR_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../truffle-core/tests/test-sidecar")
        })
}

async fn warm_up(
    alpha: &Arc<Node<TailscaleProvider>>,
    beta: &Arc<Node<TailscaleProvider>>,
    alpha_name: &str,
    beta_name: &str,
    timeout: Duration,
) -> Result<(), String> {
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
        .ok_or("beta missing from alpha's peer list")?;
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
        .ok_or("alpha missing from beta's peer list")?;

    let _ = alpha
        .send_typed(
            &beta_ts_id,
            "_pair_warmup",
            "ping",
            &serde_json::Value::Null,
        )
        .await;
    let _ = beta
        .send_typed(
            &alpha_ts_id,
            "_pair_warmup",
            "ping",
            &serde_json::Value::Null,
        )
        .await;

    let deadline = tokio::time::Instant::now() + timeout;
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
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "warm-up: ws not connected within {timeout:?} (a_ok={a_ok} b_ok={b_ok})"
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn rendezvous(
    alpha: &Arc<Node<TailscaleProvider>>,
    beta: &Arc<Node<TailscaleProvider>>,
    alpha_name: &str,
    beta_name: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let alpha_peers = alpha.peers().await;
        let beta_peers = beta.peers().await;

        let a_ok = alpha_peers.iter().any(|p| {
            p.hostname.contains(beta_name)
                || p.device_name
                    .as_deref()
                    .is_some_and(|n| n.contains(beta_name))
        });
        let b_ok = beta_peers.iter().any(|p| {
            p.hostname.contains(alpha_name)
                || p.device_name
                    .as_deref()
                    .is_some_and(|n| n.contains(alpha_name))
        });
        if a_ok && b_ok {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "bench-pair rendezvous timeout after {timeout:?}: \
                 alpha_sees_beta={a_ok} beta_sees_alpha={b_ok}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Probe before/after a workload so relay transitions aren't hidden by averages.
pub async fn diagnostics(
    node: &Node<TailscaleProvider>,
    peer: &str,
    topology: &str,
) -> serde_json::Value {
    let ping = match tokio::time::timeout(Duration::from_secs(10), node.ping(peer)).await {
        Ok(Ok(p)) => {
            serde_json::json!({"path": p.connection, "rtt_ms": p.latency.as_secs_f64()*1000.0, "endpoint": p.peer_addr})
        }
        Ok(Err(e)) => serde_json::json!({"error": e.to_string()}),
        Err(_) => serde_json::json!({"error": "path probe timed out"}),
    };
    serde_json::json!({"topology": topology, "ping": ping})
}

pub async fn make_bench_node(
    authkey: &str,
    app_id: &str,
    name: &str,
) -> Result<(Arc<Node<TailscaleProvider>>, tempfile::TempDir), String> {
    let state = tempfile::TempDir::with_prefix("truffle-bench-").map_err(|e| e.to_string())?;
    let node = NodeBuilder::default()
        .app_id(app_id)
        .map_err(|e| e.to_string())?
        .device_name(name)
        .sidecar_path(default_sidecar_path())
        .state_dir(state.path().to_str().ok_or("non-UTF8 state path")?)
        .auth_key(authkey)
        .ephemeral(true)
        .build()
        .await
        .map_err(|e| e.to_string())?;
    Ok((Arc::new(node), state))
}
