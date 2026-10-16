//! Integration test: a peer that restarts (same Tailscale node, new process)
//! is reachable again at once, over real tsnet.
//!
//! The survivor keeps its WebSocket to the dead instance — `kill -9` behind
//! tsnet closes nothing, so no FIN or RST ever arrives. Before the session
//! fix, the survivor refused the restarted peer's new connection as a
//! duplicate of the dead one whenever the dead one was in the preferred
//! cross-dial direction or the same direction, so the restarted peer's
//! sends failed with "connection missing after connect" until the
//! survivor's heartbeat (10 s ping, RST from the new netstack) or pong
//! timeout (30 s) cleared it, and the survivor's own sends went into the
//! dead socket.
//!
//! Two restarts run back to back on one pair, covering both refused cases:
//!
//! 1. the survivor dialed the stale connection, and the survivor is the
//!    smaller id (its outbound side is the preferred one);
//! 2. the dead instance dialed it (same direction as the new connection).
//!
//! Skipped automatically when `TRUFFLE_TEST_AUTHKEY` is not set. Unix only
//! (the sidecar is killed with `kill -9`).
//!
//! ```bash
//! cargo test -p truffle-core --test integration_session_restart -- --nocapture
//! ```

#![cfg(unix)]

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::json;
use truffle_core::network::tailscale::TailscaleProvider;
use truffle_core::{Node, NodeBuilder};

/// Namespace the test talks on.
const NS: &str = "restart-test";

/// Upper bound for a restarted peer's first exchange. Generous for a real
/// tailnet (a fresh WireGuard handshake can take a few seconds) but well
/// under the pre-fix recovery time, which needed the survivor's heartbeat.
const RECOVERY_BUDGET: Duration = Duration::from_secs(8);

async fn build_node(
    authkey: &str,
    app_id: &str,
    device_name: &str,
    state_dir: &Path,
) -> Arc<Node<TailscaleProvider>> {
    let node = NodeBuilder::default()
        .app_id(app_id)
        .expect("app_id should parse")
        .device_name(device_name)
        .sidecar_path(common::default_sidecar_path())
        .state_dir(state_dir.to_str().expect("utf8 path"))
        .auth_key(authkey)
        .ephemeral(true)
        // Only the test's own sends dial, so it decides each connection's
        // direction.
        .eager_identity(false)
        .ws_port(9417)
        .build()
        .await
        .expect("node build");
    Arc::new(node)
}

/// Wait until `node` lists an online peer with Tailscale id `peer`.
async fn wait_for_peer(node: &Node<TailscaleProvider>, peer: &str) {
    let deadline = Instant::now() + common::RENDEZVOUS_TIMEOUT;
    loop {
        if node
            .peers()
            .await
            .iter()
            .any(|p| p.tailscale_id == peer && p.online)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{peer} never appeared online in {}'s peer list",
            node.local_info().device_name
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Send `n` from `from` to `to` and wait until `to` receives it. Returns
/// how long that took. The FIRST send must succeed: a refused connection
/// shows up as an error here.
async fn exchange(
    from: &Node<TailscaleProvider>,
    to: &Node<TailscaleProvider>,
    to_id: &str,
    n: u64,
) -> Duration {
    let mut rx = to.subscribe(NS);
    let started = Instant::now();
    from.send_json(to_id, NS, &json!({ "n": n }))
        .await
        .unwrap_or_else(|e| panic!("send #{n} to {to_id} failed: {e}"));
    tokio::time::timeout(RECOVERY_BUDGET, async {
        loop {
            let msg = rx.recv().await.expect("subscription open");
            if msg.payload["n"] == n {
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("message #{n} not delivered within {RECOVERY_BUDGET:?}"));
    started.elapsed()
}

/// Kill the node's sidecar with SIGKILL — its netstack vanishes without a
/// FIN — then drop the node.
async fn kill_dash_nine(node: Arc<Node<TailscaleProvider>>) {
    let pid = node.sidecar_pid().expect("a running node has a sidecar");
    let status = std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .expect("run kill");
    assert!(status.success(), "kill -9 {pid} failed");
    // Teardown of the Rust side; nothing it does can reach the survivor.
    let _ = tokio::time::timeout(Duration::from_secs(10), node.stop()).await;
    drop(node);
}

#[tokio::test]
async fn restarted_peer_is_reachable_at_once() {
    let Some(authkey) = common::require_authkey("restarted_peer_is_reachable_at_once") else {
        return;
    };
    common::init_test_tracing();

    let short: String = uuid::Uuid::new_v4().to_string().chars().take(8).collect();
    let app_id = format!("{}-{short}", common::TEST_APP_ID);
    let dir_a = tempfile::TempDir::with_prefix("truffle-restart-a-").unwrap();
    let dir_b = tempfile::TempDir::with_prefix("truffle-restart-b-").unwrap();
    let name_a = format!("rs-a-{short}");
    let name_b = format!("rs-b-{short}");

    let (a, b) = tokio::join!(
        build_node(&authkey, &app_id, &name_a, dir_a.path()),
        build_node(&authkey, &app_id, &name_b, dir_b.path()),
    );
    let id_a = a.local_info().tailscale_id;
    let id_b = b.local_info().tailscale_id;

    // The survivor is the smaller id, so its outbound side is the preferred
    // cross-dial direction — the case the old tie-break got wrong.
    let ((survivor, survivor_id), (victim, victim_id, victim_name, victim_dir)) = if id_a < id_b {
        ((a, id_a), (b, id_b, name_b, dir_b))
    } else {
        ((b, id_b), (a, id_a, name_a, dir_a))
    };
    wait_for_peer(&survivor, &victim_id).await;
    wait_for_peer(&victim, &survivor_id).await;

    // --- Restart 1: the survivor dialed the stale connection. ------------
    let warm = exchange(&survivor, &victim, &victim_id, 1).await;
    eprintln!("[restart] warm-up survivor→victim {warm:?} (survivor dialed)");
    kill_dash_nine(victim).await;

    let started = Instant::now();
    let victim2 = build_node(&authkey, &app_id, &victim_name, victim_dir.path()).await;
    assert_eq!(
        victim2.local_info().tailscale_id,
        victim_id,
        "the restarted node must come back as the same Tailscale node"
    );
    let up = started.elapsed();
    wait_for_peer(&victim2, &survivor_id).await;
    let first = exchange(&victim2, &survivor, &survivor_id, 2).await;
    let reply = exchange(&survivor, &victim2, &victim_id, 3).await;
    eprintln!(
        "[restart 1: survivor-dialed stale link] node up {up:?}; restarted→survivor {first:?}; \
         survivor→restarted {reply:?}"
    );

    // --- Restart 2: the dead instance dialed the stale connection. --------
    kill_dash_nine(victim2).await;
    let started = Instant::now();
    let victim3 = build_node(&authkey, &app_id, &victim_name, victim_dir.path()).await;
    let up = started.elapsed();
    wait_for_peer(&victim3, &survivor_id).await;
    let first = exchange(&victim3, &survivor, &survivor_id, 4).await;
    let reply = exchange(&survivor, &victim3, &victim_id, 5).await;
    eprintln!(
        "[restart 2: same-direction stale link] node up {up:?}; restarted→survivor {first:?}; \
         survivor→restarted {reply:?}"
    );

    victim3.stop().await;
    survivor.stop().await;
}
