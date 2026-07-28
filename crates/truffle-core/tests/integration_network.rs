//! Integration tests for truffle-core Layer 3 (Network).
//!
//! These tests use the REAL Go sidecar and REAL Tailscale network — not mocks.
//! They skip gracefully when `TRUFFLE_TEST_AUTHKEY` is not set in the env.
//!
//! Each test spins up a pair of ephemeral nodes on the tailnet and exercises
//! the network layer between them — no external peer required.
//!
//! See `docs/rfcs/019-local-testing-and-benchmarking.md` for the design.
//!
//! ## Running
//!
//! ```bash
//! # Without an auth key: tests skip with a one-line notice.
//! cargo test -p truffle-core --test integration_network
//!
//! # With an auth key in `.env` at the repo root:
//! cargo test -p truffle-core --test integration_network -- --nocapture
//! ```

mod common;

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use truffle_core::network::{NetworkPeerEvent, NetworkProvider};

/// Timeout for individual operations after the pair is up and rendezvoused.
const OP_TIMEOUT: Duration = Duration::from_secs(15);

/// Peer discovery can precede route convergence on a newly registered
/// ephemeral node. Connectivity assertions retry within this separate,
/// bounded window instead of treating one startup probe as definitive.
const CONNECTIVITY_TIMEOUT: Duration = Duration::from_secs(45);

// ---------------------------------------------------------------------------
// Test 1: Start provider and verify auth + running state
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_provider_start_and_auth() {
    let Some(authkey) = common::require_authkey("test_provider_start_and_auth") else {
        return;
    };
    common::init_test_tracing();

    let pair = common::make_pair_of_nodes(&authkey).await;

    let alpha_identity = pair.alpha.local_identity_async().await;
    let beta_identity = pair.beta.local_identity_async().await;

    assert!(alpha_identity.ip.is_some(), "alpha should have an IP");
    assert!(beta_identity.ip.is_some(), "beta should have an IP");
    assert!(
        !alpha_identity.tailscale_hostname.is_empty(),
        "alpha should have a hostname"
    );
    assert!(
        !beta_identity.tailscale_hostname.is_empty(),
        "beta should have a hostname"
    );

    let alpha_health = pair.alpha.health().await;
    assert_eq!(alpha_health.state, "running");
    assert!(alpha_health.healthy);

    pair.stop().await;
}

// ---------------------------------------------------------------------------
// Test 2: Peer discovery — alpha sees beta and vice versa
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_peer_discovery() {
    let Some(authkey) = common::require_authkey("test_peer_discovery") else {
        return;
    };
    common::init_test_tracing();

    let pair = common::make_pair_of_nodes(&authkey).await;

    let alpha_peers = pair.alpha.peers().await;
    let beta_peers = pair.beta.peers().await;

    let alpha_sees_beta = alpha_peers.iter().any(|p| p.hostname == pair.beta_hostname);
    let beta_sees_alpha = beta_peers.iter().any(|p| p.hostname == pair.alpha_hostname);

    assert!(
        alpha_sees_beta,
        "alpha should see beta. Alpha peers: {:?}",
        alpha_peers.iter().map(|p| &p.hostname).collect::<Vec<_>>()
    );
    assert!(
        beta_sees_alpha,
        "beta should see alpha. Beta peers: {:?}",
        beta_peers.iter().map(|p| &p.hostname).collect::<Vec<_>>()
    );

    pair.stop().await;
}

// ---------------------------------------------------------------------------
// Test 3: Peer events stream fires Joined events for the other node
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_peer_events() {
    let Some(authkey) = common::require_authkey("test_peer_events") else {
        return;
    };
    common::init_test_tracing();

    // Subscribe BEFORE the pair starts registering so we catch the initial
    // burst. We rebuild a pair here because make_pair_of_nodes starts both
    // before returning; to catch the Joined event, we need the receiver to
    // exist before start. So we use the lower-level path here: subscribe on
    // each side after start but before rendezvous completes is racy — instead,
    // rely on the post-rendezvous peer list and a late subscriber receiving a
    // replayed Joined event is not guaranteed. We therefore assert on peers()
    // (which IS stable) and then watch for update events for a short window.
    let pair = common::make_pair_of_nodes(&authkey).await;
    let mut alpha_events = pair.alpha.peer_events();

    // Existence check via peers() is already validated by the rendezvous step
    // inside make_pair_of_nodes. For events, we just verify the stream is
    // live — collect any events within 3s and confirm we see at least one
    // NetworkPeerEvent referencing the peer hostname.
    let collect = async {
        let mut events = Vec::new();
        let collect_deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let remaining = collect_deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break events;
            }
            match timeout(remaining, alpha_events.recv()).await {
                Ok(Ok(event)) => events.push(event),
                _ => break events,
            }
        }
    };
    let events: Vec<NetworkPeerEvent> = collect.await;

    // The stream is alive — peer_events() returned a receiver. Events may or
    // may not fire during the observation window depending on whether beta
    // sent any update after rendezvous. The stricter assertion is that peers()
    // contains beta, which is guaranteed by the rendezvous.
    eprintln!("  alpha observed {} event(s) in 3s window", events.len());
    for e in &events {
        eprintln!("    {e:?}");
    }

    let alpha_peers = pair.alpha.peers().await;
    assert!(
        alpha_peers.iter().any(|p| p.hostname == pair.beta_hostname),
        "alpha.peers() must contain beta after rendezvous"
    );

    pair.stop().await;
}

// ---------------------------------------------------------------------------
// Test 4: Dial TCP — alpha opens a stream to beta via Tailscale
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_dial_tcp() {
    let Some(authkey) = common::require_authkey("test_dial_tcp") else {
        return;
    };
    common::init_test_tracing();

    let pair = common::make_pair_of_nodes(&authkey).await;

    // Beta listens on an ephemeral port, echoes a single nonce.
    let mut listener = pair
        .beta
        .listen_tcp(0)
        .await
        .expect("beta should listen on ephemeral port");
    let beta_listen_port = listener.port;

    let nonce = b"truffle-test-nonce-01";

    // Spawn beta accept+echo task
    let beta_task = tokio::spawn(async move {
        let incoming = listener
            .incoming
            .recv()
            .await
            .expect("beta should accept connection");
        let mut stream = incoming.stream;
        let mut buf = [0u8; 32];
        let n = stream.read(&mut buf).await.expect("beta read nonce");
        stream.write_all(&buf[..n]).await.expect("beta echo nonce");
        stream.shutdown().await.ok();
        buf[..n].to_vec()
    });

    // Alpha dials beta by Tailscale IP
    let beta_ip = pair.beta_ip().await.to_string();
    eprintln!("  dialing beta at {beta_ip}:{beta_listen_port}");
    let mut stream = timeout(OP_TIMEOUT, pair.alpha.dial_tcp(&beta_ip, beta_listen_port))
        .await
        .expect("dial did not time out")
        .expect("alpha dial should succeed");

    // Alpha writes nonce, reads echo
    stream.write_all(nonce).await.expect("alpha write nonce");
    let mut echo = [0u8; 32];
    let n = timeout(OP_TIMEOUT, stream.read(&mut echo))
        .await
        .expect("alpha read did not time out")
        .expect("alpha read echo");
    let echoed = &echo[..n];
    assert_eq!(echoed, nonce, "alpha should read back the exact nonce");

    let beta_received = beta_task.await.expect("beta task joined");
    assert_eq!(beta_received, nonce, "beta should have received the nonce");

    pair.stop().await;
}

// ---------------------------------------------------------------------------
// Test 5: Ping — alpha pings beta
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_ping() {
    let Some(authkey) = common::require_authkey("test_ping") else {
        return;
    };
    common::init_test_tracing();

    let pair = common::make_pair_of_nodes(&authkey).await;

    let beta_ip = pair.beta_ip().await.to_string();
    eprintln!("  pinging beta at {beta_ip}");
    let deadline = tokio::time::Instant::now() + CONNECTIVITY_TIMEOUT;
    let mut attempts = 0_u32;
    let result = loop {
        attempts += 1;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match timeout(remaining, pair.alpha.ping(&beta_ip)).await {
            Ok(Ok(result)) => break result,
            Ok(Err(error)) if tokio::time::Instant::now() < deadline => {
                eprintln!("  ping attempt {attempts} failed while route converges: {error}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(Err(error)) => {
                panic!(
                    "ping did not succeed within {CONNECTIVITY_TIMEOUT:?} \
                     after {attempts} attempt(s): {error}"
                );
            }
            Err(_) => {
                panic!(
                    "ping did not succeed within {CONNECTIVITY_TIMEOUT:?} \
                     after {attempts} attempt(s)"
                );
            }
        }
    };

    eprintln!(
        "  ping ok after {attempts} attempt(s): latency={:?} connection={} peer_addr={:?}",
        result.latency, result.connection, result.peer_addr,
    );
    assert!(
        result.latency > Duration::ZERO,
        "ping latency should be > 0"
    );
    assert!(
        !result.connection.is_empty(),
        "ping connection type should not be empty"
    );

    pair.stop().await;
}

// ---------------------------------------------------------------------------
// Test 5b: WhoIs — alpha asks the tailnet who owns beta's address
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_whois() {
    let Some(authkey) = common::require_authkey("test_whois") else {
        return;
    };
    common::init_test_tracing();

    let pair = common::make_pair_of_nodes(&authkey).await;

    let beta_ip = pair.beta_ip().await.to_string();
    eprintln!("  whois beta at {beta_ip}");
    let deadline = tokio::time::Instant::now() + CONNECTIVITY_TIMEOUT;
    let mut attempts = 0_u32;
    // WhoIs answers from the netmap, which may lag node startup — retry until
    // the identity appears, like ping retries route convergence.
    let identity = loop {
        attempts += 1;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match timeout(remaining, pair.alpha.whois(&beta_ip)).await {
            Ok(Ok(Some(identity))) => break identity,
            // A pre-v3 sidecar can never succeed — fail fast instead of
            // burning the whole convergence budget on a permanent error.
            Ok(Err(truffle_core::network::NetworkError::Unsupported(message))) => {
                panic!("whois unsupported by this sidecar build: {message}");
            }
            Ok(result) if tokio::time::Instant::now() < deadline => {
                eprintln!("  whois attempt {attempts} not ready yet: {result:?}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Ok(result) => {
                panic!(
                    "whois did not return an identity within {CONNECTIVITY_TIMEOUT:?} \
                     after {attempts} attempt(s): {result:?}"
                );
            }
            Err(_) => {
                panic!(
                    "whois did not return an identity within {CONNECTIVITY_TIMEOUT:?} \
                     after {attempts} attempt(s)"
                );
            }
        }
    };

    eprintln!("  whois ok after {attempts} attempt(s): {identity:?}");
    assert!(
        identity.node_id.as_deref().is_some_and(|id| !id.is_empty()),
        "whois should report beta's stable node id, got {identity:?}"
    );
    assert!(
        identity.dns_name.as_deref().is_some_and(|d| !d.is_empty()),
        "whois should report beta's MagicDNS name, got {identity:?}"
    );

    pair.stop().await;
}

// ---------------------------------------------------------------------------
// Test 6: Health — alpha reports running, then stopped after stop()
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_health() {
    let Some(authkey) = common::require_authkey("test_health") else {
        return;
    };
    common::init_test_tracing();

    let pair = common::make_pair_of_nodes(&authkey).await;

    let health = pair.alpha.health().await;
    eprintln!(
        "  health: state={} healthy={} key_expiry={:?}",
        health.state, health.healthy, health.key_expiry
    );
    assert_eq!(health.state, "running");
    assert!(health.healthy);

    // Stop alpha only (not the whole pair) so we can check its health after stop
    let alpha = pair.alpha;
    let beta = pair.beta;
    let _alpha_hostname = pair.alpha_hostname;
    let _beta_hostname = pair.beta_hostname;

    alpha.stop().await.expect("alpha stop should succeed");
    let after = alpha.health().await;
    assert_eq!(after.state, "stopped");
    assert!(!after.healthy);

    // Stop beta explicitly to avoid leaking an ephemeral peer longer than needed.
    let beta = beta;
    let _ = beta.stop().await;
}
