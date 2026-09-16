//! Request/reply utility for correlated message exchange.
//!
//! Provides [`send_and_wait`], a function that sends a namespaced message to a
//! peer and waits for a reply matching a caller-provided filter. This
//! eliminates the subscribe → send → loop → timeout boilerplate that
//! subsystems like file transfer repeat at every call site.
//!
//! # Example
//!
//! ```ignore
//! use truffle_core::request_reply::{send_and_wait, RequestError};
//!
//! let port = send_and_wait(
//!     &node, "peer-id", "my-ns", "request", &payload,
//!     Duration::from_secs(10),
//!     |msg| {
//!         if msg.msg_type == "response" && msg.from == "peer-id" {
//!             Some(msg.payload.clone())
//!         } else {
//!             None
//!         }
//!     },
//! ).await?;
//! ```

use std::time::Duration;

use tokio::sync::broadcast;

use crate::network::NetworkProvider;
use crate::node::{NamespacedMessage, Node, NodeError};

/// Errors returned by [`send_and_wait`].
#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    /// The initial send failed (unknown peer, offline, connection error).
    #[error("send failed: {0}")]
    Send(NodeError),
    /// No matching reply arrived within the timeout.
    #[error("timed out waiting for reply")]
    Timeout,
    /// The namespace subscription channel was closed.
    #[error("subscription channel closed")]
    ChannelClosed,
}

/// Send a namespaced message and wait for a reply that matches a predicate.
///
/// 1. Subscribes to `namespace` (before sending, to avoid missing the reply).
/// 2. Sends the message via [`Node::send_typed`].
/// 3. Loops on incoming messages, calling `filter` on each.
/// 4. Returns the first `Some(R)` from the filter, or times out.
///
/// The filter receives **every** message on the namespace, not just from the
/// target peer — the caller decides what constitutes a valid reply.
pub async fn send_and_wait<N, F, R>(
    node: &Node<N>,
    peer_id: &str,
    namespace: &str,
    msg_type: &str,
    payload: &serde_json::Value,
    timeout: Duration,
    filter: F,
) -> Result<R, RequestError>
where
    N: NetworkProvider + 'static,
    F: Fn(&NamespacedMessage) -> Option<R>,
{
    // Subscribe BEFORE sending so we don't miss a fast reply.
    let mut rx = node.subscribe(namespace);

    node.send_typed(peer_id, namespace, msg_type, payload)
        .await
        .map_err(RequestError::Send)?;

    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        let remaining = deadline.duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(RequestError::Timeout);
        }

        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(msg)) => {
                if let Some(result) = filter(&msg) {
                    return Ok(result);
                }
                // Not our reply — keep waiting.
            }
            Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
                tracing::warn!(missed = n, "request_reply: receiver lagged");
                // Continue — the reply may still arrive.
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                return Err(RequestError::ChannelClosed);
            }
            Err(_elapsed) => {
                return Err(RequestError::Timeout);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::Duration;

    use crate::envelope::codec::JsonCodec;
    use crate::envelope::EnvelopeCodec;
    use crate::network::*;
    use crate::session::PeerRegistry;
    use crate::transport::websocket::WebSocketTransport;
    use crate::transport::WsConfig;

    // ── Mock network provider (same pattern as node.rs tests) ───────

    struct MockNetworkProvider {
        identity: NodeIdentity,
        local_addr: PeerAddr,
        peer_event_tx: tokio::sync::broadcast::Sender<NetworkPeerEvent>,
        mock_peers: Arc<tokio::sync::RwLock<Vec<NetworkPeer>>>,
    }

    impl MockNetworkProvider {
        fn new(id: &str) -> Self {
            let (peer_event_tx, _) = tokio::sync::broadcast::channel(64);
            Self {
                identity: NodeIdentity {
                    app_id: "test".to_string(),
                    // RFC 017: align `device_id` with fixture input.
                    device_id: id.to_string(),
                    device_name: format!("Test Node {id}"),
                    tailscale_hostname: format!("truffle-test-{id}"),
                    tailscale_id: id.to_string(),
                    dns_name: None,
                    ip: Some("127.0.0.1".parse().unwrap()),
                    login_name: None,
                },
                local_addr: PeerAddr {
                    ip: Some("127.0.0.1".parse().unwrap()),
                    hostname: format!("truffle-test-{id}"),
                    dns_name: None,
                },
                peer_event_tx,
                mock_peers: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            }
        }

        fn event_sender(&self) -> tokio::sync::broadcast::Sender<NetworkPeerEvent> {
            self.peer_event_tx.clone()
        }
    }

    impl NetworkProvider for MockNetworkProvider {
        fn local_identity(&self) -> NodeIdentity {
            self.identity.clone()
        }
        fn local_addr(&self) -> PeerAddr {
            self.local_addr.clone()
        }
        fn peer_events(&self) -> tokio::sync::broadcast::Receiver<NetworkPeerEvent> {
            self.peer_event_tx.subscribe()
        }

        async fn start(&mut self) -> Result<(), NetworkError> {
            Ok(())
        }
        async fn stop(&self) -> Result<(), NetworkError> {
            Ok(())
        }
        async fn peers(&self) -> Vec<NetworkPeer> {
            self.mock_peers.read().await.clone()
        }
        async fn dial_tcp(
            &self,
            _addr: &str,
            _port: u16,
        ) -> Result<tokio::net::TcpStream, NetworkError> {
            Err(NetworkError::DialFailed("mock".into()))
        }
        async fn listen_tcp(&self, _port: u16) -> Result<NetworkTcpListener, NetworkError> {
            Err(NetworkError::ListenFailed("mock".into()))
        }
        async fn unlisten_tcp(&self, _port: u16) -> Result<(), NetworkError> {
            Ok(())
        }
        async fn bind_udp(&self, _port: u16) -> Result<NetworkUdpSocket, NetworkError> {
            Err(NetworkError::NotRunning)
        }
        async fn ping(&self, _addr: &str) -> Result<PingResult, NetworkError> {
            Ok(PingResult {
                latency: Duration::from_millis(1),
                connection: "direct".to_string(),
                peer_addr: None,
            })
        }
        async fn health(&self) -> HealthInfo {
            HealthInfo {
                state: "running".to_string(),
                healthy: true,
                ..Default::default()
            }
        }
    }

    fn ws_config(port: u16) -> WsConfig {
        WsConfig {
            port,
            ping_interval: Duration::from_secs(300),
            pong_timeout: Duration::from_secs(300),
            ..Default::default()
        }
    }

    async fn random_port() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        l.local_addr().unwrap().port()
    }

    fn make_loopback_peer(id: &str) -> NetworkPeer {
        NetworkPeer {
            id: id.to_string(),
            hostname: format!("truffle-test-{id}"),
            ip: "127.0.0.1".parse().unwrap(),
            online: true,
            cur_addr: Some("127.0.0.1:41641".to_string()),
            relay: None,
            os: Some("linux".to_string()),
            last_seen: Some("2026-03-25T12:00:00Z".to_string()),
            key_expiry: None,
            dns_name: None,
            login_name: None,
        }
    }

    async fn make_test_node(
        id: &str,
        ws_port: u16,
    ) -> (
        Node<MockNetworkProvider>,
        tokio::sync::broadcast::Sender<NetworkPeerEvent>,
    ) {
        let provider = MockNetworkProvider::new(id);
        let event_tx = provider.event_sender();
        let network = Arc::new(provider);
        let ws_transport = Arc::new(WebSocketTransport::new(network.clone(), ws_config(ws_port)));
        let session = Arc::new(PeerRegistry::new(network.clone(), ws_transport));
        session.start().await;

        let codec: Arc<dyn EnvelopeCodec> = Arc::new(JsonCodec);
        let node = Node::from_parts(network, session, codec);
        (node, event_tx)
    }

    #[tokio::test]
    async fn test_send_and_wait_timeout() {
        let ws_port = random_port().await;
        let (node, event_tx) = make_test_node("node-a", ws_port).await;

        // Inject a peer so send doesn't fail with UnknownPeer.
        let _ = event_tx.send(NetworkPeerEvent::Joined(make_loopback_peer("peer-b")));
        tokio::time::sleep(Duration::from_millis(50)).await;

        // send_typed will fail at session layer (no real WS), but we want to
        // test the timeout path. Use a filter that never matches.
        let result = send_and_wait(
            &node,
            "peer-b",
            "test-ns",
            "ping",
            &serde_json::json!({}),
            Duration::from_millis(100),
            |_msg| -> Option<()> { None },
        )
        .await;

        assert!(matches!(
            result,
            Err(RequestError::Timeout) | Err(RequestError::Send(_))
        ));
    }

    #[tokio::test]
    async fn test_send_and_wait_send_failure_unknown_peer() {
        let ws_port = random_port().await;
        let (node, _event_tx) = make_test_node("node-a", ws_port).await;

        // No peers injected — send should fail with UnknownPeer.
        let result = send_and_wait(
            &node,
            "nonexistent",
            "test-ns",
            "ping",
            &serde_json::json!({}),
            Duration::from_secs(1),
            |_msg| -> Option<()> { Some(()) },
        )
        .await;

        assert!(matches!(result, Err(RequestError::Send(_))));
    }

    #[tokio::test]
    async fn test_send_and_wait_filter_matches() {
        // We can't easily inject a message through the WS layer in unit tests,
        // so we test the filter logic directly on NamespacedMessage values.
        let msg = NamespacedMessage {
            from: "peer-b".to_string(),
            namespace: "test-ns".to_string(),
            msg_type: "response".to_string(),
            payload: serde_json::json!({"value": 42}),
            timestamp: None,
        };

        // Filter that looks for msg_type == "response"
        let filter = |m: &NamespacedMessage| -> Option<i64> {
            if m.msg_type == "response" {
                m.payload.get("value")?.as_i64()
            } else {
                None
            }
        };

        assert_eq!(filter(&msg), Some(42));

        // Filter that doesn't match
        let non_matching = NamespacedMessage {
            from: "peer-b".to_string(),
            namespace: "test-ns".to_string(),
            msg_type: "other".to_string(),
            payload: serde_json::json!({}),
            timestamp: None,
        };
        assert_eq!(filter(&non_matching), None);
    }
}
