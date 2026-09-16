//! Minimal Layer 3 peer test — start provider, discover peers, dial TCP.
use std::path::PathBuf;
use truffle_core::network::tailscale::{TailscaleConfig, TailscaleProvider};
use truffle_core::network::{NetworkPeerEvent, NetworkProvider};

#[tokio::main]
async fn main() {
    let sidecar = std::env::var("SIDECAR_PATH").unwrap_or_else(|_| "./sidecar-slim".to_string());
    let state_dir =
        std::env::var("STATE_DIR").unwrap_or_else(|_| "/tmp/truffle-peer-test-fixed".to_string());
    let hostname =
        std::env::var("HOSTNAME_OVERRIDE").unwrap_or_else(|_| "peer-test-v2".to_string());

    eprintln!("[peer-test] Starting with hostname={hostname}");
    eprintln!("[peer-test] Sidecar: {sidecar}");
    eprintln!("[peer-test] State dir: {state_dir}");

    let config = TailscaleConfig {
        binary_path: PathBuf::from(&sidecar),
        app_id: "peer-test".to_string(),
        device_id: "01J4K9M2Z8AB3RNYQPW6H5TC0X".to_string(),
        device_name: hostname.clone(),
        state_dir: state_dir.clone(),
        hostname: format!("truffle-peer-test-{hostname}"),
        auth_key: std::env::var("TS_AUTHKEY").ok(),
        ephemeral: None,
        tags: None,
        idle_timeout_secs: None,
        login_allow: Vec::new(),
    };
    let mut provider = TailscaleProvider::new(config);

    // Subscribe to events BEFORE start() so we see auth URLs
    let mut events = provider.peer_events();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(NetworkPeerEvent::AuthRequired { url }) => {
                    eprintln!();
                    eprintln!("  ┌─────────────────────────────────────────────┐");
                    eprintln!("  │  Authentication required. Open this URL:    │");
                    eprintln!("  │                                             │");
                    eprintln!("  │  {url}");
                    eprintln!("  │                                             │");
                    eprintln!("  │  Waiting for you to approve...              │");
                    eprintln!("  └─────────────────────────────────────────────┘");
                    eprintln!();
                }
                Ok(NetworkPeerEvent::Joined(peer)) => {
                    eprintln!("[event] Peer joined: {} ({})", peer.hostname, peer.ip);
                }
                Ok(NetworkPeerEvent::Left(id)) => {
                    eprintln!("[event] Peer left: {id}");
                }
                Ok(NetworkPeerEvent::Updated(peer)) => {
                    eprintln!("[event] Peer updated: {} ({})", peer.hostname, peer.ip);
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    // start() now waits up to 5 min for auth, emitting URLs via events
    if let Err(e) = provider.start().await {
        eprintln!("[peer-test] Start failed: {e}");
        std::process::exit(1);
    }
    eprintln!("[peer-test] Started successfully!");

    eprintln!("[peer-test] Waiting 10s for peer discovery...");
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;

    let identity = provider.local_identity();
    eprintln!(
        "[peer-test] Identity: tailscale_id={}, device_name={}, tailscale_hostname={}",
        identity.tailscale_id, identity.device_name, identity.tailscale_hostname
    );

    let addr = provider.local_addr();
    eprintln!(
        "[peer-test] Addr: ip={:?}, hostname={}",
        addr.ip, addr.hostname
    );

    let peers = provider.peers().await;
    eprintln!("[peer-test] Discovered {} peers", peers.len());
    for p in &peers {
        eprintln!(
            "  {} ({}): online={}, ip={}",
            p.hostname, p.id, p.online, p.ip
        );
    }

    let health = provider.health().await;
    eprintln!(
        "[peer-test] Health: state={}, healthy={}",
        health.state, health.healthy
    );

    let online_peers: Vec<_> = peers.iter().filter(|p| p.online).collect();
    eprintln!("[peer-test] {} online peers", online_peers.len());

    // Try dial_tcp to an online peer's SSH port
    if let Some(peer) = online_peers.first() {
        let ip = peer.ip.to_string();
        eprintln!("[peer-test] Dialing {ip}:22 (SSH)...");
        match provider.dial_tcp(&ip, 22).await {
            Ok(mut stream) => {
                use tokio::io::AsyncReadExt;
                let mut buf = [0u8; 64];
                match tokio::time::timeout(std::time::Duration::from_secs(5), stream.read(&mut buf))
                    .await
                {
                    Ok(Ok(n)) if n > 0 => {
                        eprintln!(
                            "[peer-test] SSH banner: {}",
                            String::from_utf8_lossy(&buf[..n]).trim()
                        );
                        eprintln!("[peer-test] ✓ dial_tcp WORKS!");
                    }
                    Ok(Ok(_)) => eprintln!("[peer-test] ✗ Connected but no data"),
                    Ok(Err(e)) => eprintln!("[peer-test] ✗ Read error: {e}"),
                    Err(_) => eprintln!("[peer-test] ✗ Read timeout"),
                }
            }
            Err(e) => eprintln!("[peer-test] ✗ dial_tcp failed: {e}"),
        }
    } else {
        eprintln!("[peer-test] No online peers to dial");
    }

    // Try ping
    if let Some(peer) = online_peers.first() {
        let ip = peer.ip.to_string();
        eprintln!("[peer-test] Pinging {ip}...");
        match provider.ping(&ip).await {
            Ok(result) => {
                eprintln!(
                    "[peer-test] ✓ Ping: {}ms ({})",
                    result.latency.as_millis(),
                    result.connection
                );
            }
            Err(e) => eprintln!("[peer-test] ✗ Ping failed: {e}"),
        }
    }

    eprintln!("[peer-test] Stopping...");
    provider.stop().await.ok();
    eprintln!("[peer-test] Done! (state preserved at {state_dir})");
}
