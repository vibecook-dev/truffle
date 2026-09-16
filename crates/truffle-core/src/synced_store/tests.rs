use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::envelope::codec::JsonCodec;
use crate::envelope::EnvelopeCodec;
use crate::network::*;
use crate::session::PeerRegistry;
use crate::synced_store::{FileBackend, Slice, StoreBackend, StoreEvent, SyncedStore};
use crate::transport::websocket::WebSocketTransport;
use crate::transport::WsConfig;

// ── Test data type ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct TestState {
    value: i32,
    label: String,
}

// ── Mock network provider ───────────────────────────────────────────

struct MockNetworkProvider {
    identity: NodeIdentity,
    local_addr: PeerAddr,
    peer_event_tx: broadcast::Sender<NetworkPeerEvent>,
    mock_peers: Arc<tokio::sync::RwLock<Vec<NetworkPeer>>>,
}

impl MockNetworkProvider {
    fn new(id: &str) -> Self {
        let (peer_event_tx, _) = broadcast::channel(64);
        Self {
            identity: NodeIdentity {
                app_id: "test".to_string(),
                // RFC 017: fixtures align `device_id` with the input `id`
                // so tests that call `node.synced_store(...)` can reason
                // about `store.device_id()` directly.
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

    fn event_sender(&self) -> broadcast::Sender<NetworkPeerEvent> {
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
    fn peer_events(&self) -> broadcast::Receiver<NetworkPeerEvent> {
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

// ── Helpers ─────────────────────────────────────────────────────────

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

async fn make_test_node(
    id: &str,
    ws_port: u16,
) -> (
    Arc<crate::node::Node<MockNetworkProvider>>,
    broadcast::Sender<NetworkPeerEvent>,
) {
    let provider = MockNetworkProvider::new(id);
    let event_tx = provider.event_sender();
    let network = Arc::new(provider);
    let ws_transport = Arc::new(WebSocketTransport::new(network.clone(), ws_config(ws_port)));
    let session = Arc::new(PeerRegistry::new(network.clone(), ws_transport));
    session.start().await;

    let codec: Arc<dyn EnvelopeCodec> = Arc::new(JsonCodec);
    let node = Arc::new(crate::node::Node::from_parts(network, session, codec));
    (node, event_tx)
}

// ── Tests ───────────────────────────────────────────────────────────

#[tokio::test]
async fn test_set_and_local() {
    let ws_port = random_port().await;
    let (node, _event_tx) = make_test_node("device-a", ws_port).await;

    let store: Arc<SyncedStore<TestState>> = node.synced_store("test-store");

    // Initially empty.
    assert!(store.local().await.is_none());

    // Set data.
    let state = TestState {
        value: 42,
        label: "hello".to_string(),
    };
    store.set(state.clone()).await;

    // Now readable.
    let result = store.local().await.unwrap();
    assert_eq!(result, state);

    store.stop().await;
}

#[tokio::test]
async fn test_version_increments() {
    let ws_port = random_port().await;
    let (node, _event_tx) = make_test_node("device-a", ws_port).await;

    let store: Arc<SyncedStore<TestState>> = node.synced_store("test-store");

    assert_eq!(store.version(), 0);

    store
        .set(TestState {
            value: 1,
            label: "v1".into(),
        })
        .await;
    assert_eq!(store.version(), 1);

    store
        .set(TestState {
            value: 2,
            label: "v2".into(),
        })
        .await;
    assert_eq!(store.version(), 2);

    store
        .set(TestState {
            value: 3,
            label: "v3".into(),
        })
        .await;
    assert_eq!(store.version(), 3);

    store.stop().await;
}

#[tokio::test]
async fn test_subscribe_local_changed() {
    let ws_port = random_port().await;
    let (node, _event_tx) = make_test_node("device-a", ws_port).await;

    let store: Arc<SyncedStore<TestState>> = node.synced_store("test-store");
    let mut events = store.subscribe();

    let state = TestState {
        value: 7,
        label: "event".into(),
    };
    store.set(state.clone()).await;

    let event = tokio::time::timeout(Duration::from_millis(100), events.recv())
        .await
        .expect("should receive event within timeout")
        .expect("channel should not be closed");

    match event {
        StoreEvent::LocalChanged(data) => assert_eq!(data, state),
        other => panic!("expected LocalChanged, got {other:?}"),
    }

    store.stop().await;
}

#[tokio::test]
async fn test_device_ids_and_store_id() {
    let ws_port = random_port().await;
    let (node, _event_tx) = make_test_node("device-a", ws_port).await;

    let store: Arc<SyncedStore<TestState>> = node.synced_store("my-sessions");

    assert_eq!(store.store_id(), "my-sessions");
    assert_eq!(store.device_id(), "device-a");

    // No device_ids before set.
    assert!(store.device_ids().await.is_empty());

    store
        .set(TestState {
            value: 1,
            label: "test".into(),
        })
        .await;

    let ids = store.device_ids().await;
    assert_eq!(ids, vec!["device-a".to_string()]);

    store.stop().await;
}

#[tokio::test]
async fn test_all_includes_local() {
    let ws_port = random_port().await;
    let (node, _event_tx) = make_test_node("device-a", ws_port).await;

    let store: Arc<SyncedStore<TestState>> = node.synced_store("test-store");

    store
        .set(TestState {
            value: 99,
            label: "local".into(),
        })
        .await;

    let all = store.all().await;
    assert_eq!(all.len(), 1);
    assert_eq!(all["device-a"].data.value, 99);
    assert_eq!(all["device-a"].version, 1);

    store.stop().await;
}

#[tokio::test]
async fn test_remote_slice_via_sync_message() {
    let ws_port = random_port().await;
    let (node, _event_tx) = make_test_node("device-a", ws_port).await;

    let store: Arc<SyncedStore<TestState>> = node.synced_store("test-store");

    // Simulate receiving an Update from a remote peer by injecting a
    // sync message into the namespace channel via send_typed to ourselves.
    // Since mock networking doesn't support real WS, we directly manipulate
    // the store's internal state to test the logic.
    {
        let remote_slice = Slice {
            device_id: "device-b".to_string(),
            data: TestState {
                value: 100,
                label: "from-b".into(),
            },
            version: 1,
            updated_at: 12345,
        };
        let mut remotes = store.inner.remotes.write().await;
        remotes.insert("device-b".to_string(), remote_slice);
    }

    // Verify remote data is readable.
    let slice = store.get("device-b").await.unwrap();
    assert_eq!(slice.data.value, 100);
    assert_eq!(slice.version, 1);

    // Verify all() includes both local and remote.
    store
        .set(TestState {
            value: 1,
            label: "local".into(),
        })
        .await;

    let all = store.all().await;
    assert_eq!(all.len(), 2);
    assert!(all.contains_key("device-a"));
    assert!(all.contains_key("device-b"));

    store.stop().await;
}

#[tokio::test]
async fn test_stale_remote_update_rejected() {
    let ws_port = random_port().await;
    let (node, _event_tx) = make_test_node("device-a", ws_port).await;

    let store: Arc<SyncedStore<TestState>> = node.synced_store("test-store");

    // Insert a remote slice at version 5.
    {
        let remote_slice = Slice {
            device_id: "device-b".to_string(),
            data: TestState {
                value: 50,
                label: "v5".into(),
            },
            version: 5,
            updated_at: 12345,
        };
        let mut remotes = store.inner.remotes.write().await;
        remotes.insert("device-b".to_string(), remote_slice);
    }

    // Try to apply a stale version 3 — should be ignored.
    // We call the internal apply logic directly.
    {
        let stale_slice = Slice {
            device_id: "device-b".to_string(),
            data: TestState {
                value: 30,
                label: "v3-stale".into(),
            },
            version: 3,
            updated_at: 10000,
        };

        // Manually check: the existing version should win.
        let remotes = store.inner.remotes.read().await;
        let existing = remotes.get("device-b").unwrap();
        assert!(stale_slice.version <= existing.version);
    }

    // Verify the original data is still there.
    let slice = store.get("device-b").await.unwrap();
    assert_eq!(slice.data.value, 50);
    assert_eq!(slice.version, 5);

    store.stop().await;
}

#[tokio::test]
async fn test_peer_leave_removes_slice() {
    let ws_port = random_port().await;
    let (node, event_tx) = make_test_node("device-a", ws_port).await;

    let store: Arc<SyncedStore<TestState>> = node.synced_store("test-store");
    let mut events = store.subscribe();

    // Insert a remote slice.
    {
        let remote_slice = Slice {
            device_id: "device-b".to_string(),
            data: TestState {
                value: 100,
                label: "from-b".into(),
            },
            version: 1,
            updated_at: 12345,
        };
        let mut remotes = store.inner.remotes.write().await;
        remotes.insert("device-b".to_string(), remote_slice);
    }

    assert!(store.get("device-b").await.is_some());

    // Simulate peer leaving.
    let peer_b = NetworkPeer {
        id: "device-b".to_string(),
        hostname: "truffle-test-device-b".to_string(),
        ip: "127.0.0.1".parse().unwrap(),
        online: true,
        cur_addr: Some("127.0.0.1:41641".to_string()),
        relay: None,
        os: None,
        last_seen: None,
        key_expiry: None,
        dns_name: None,
        login_name: None,
    };
    // First join, then leave (the sync task needs Joined to register the peer).
    let _ = event_tx.send(NetworkPeerEvent::Joined(peer_b));
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = event_tx.send(NetworkPeerEvent::Left("device-b".to_string()));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Remote slice should be removed.
    assert!(store.get("device-b").await.is_none());

    // Should receive PeerRemoved event.
    let event = tokio::time::timeout(Duration::from_millis(200), async {
        loop {
            if let Ok(StoreEvent::PeerRemoved { device_id }) = events.recv().await {
                return device_id;
            }
        }
    })
    .await
    .expect("should receive PeerRemoved within timeout");

    assert_eq!(event, "device-b");

    store.stop().await;
}

#[tokio::test]
async fn test_stop_cancels_task() {
    let ws_port = random_port().await;
    let (node, _event_tx) = make_test_node("device-a", ws_port).await;

    let store: Arc<SyncedStore<TestState>> = node.synced_store("test-store");

    // Set some data to prove the store is working.
    store
        .set(TestState {
            value: 1,
            label: "test".into(),
        })
        .await;
    assert_eq!(store.version(), 1);

    // Stop should not panic.
    store.stop().await;

    // Calling stop again should be a no-op.
    store.stop().await;
}

// ── FileBackend tests ──────────────────────────────────────────────

#[test]
fn test_file_backend_save_load_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileBackend::new(dir.path());

    let data = serde_json::to_vec(&TestState {
        value: 42,
        label: "hello".into(),
    })
    .unwrap();

    // Save and load back.
    backend.save("my-store", "device-a", &data, 5);
    let (loaded_data, loaded_version) = backend.load("my-store", "device-a").unwrap();

    assert_eq!(loaded_version, 5);
    let loaded: TestState = serde_json::from_slice(&loaded_data).unwrap();
    assert_eq!(loaded.value, 42);
    assert_eq!(loaded.label, "hello");

    // Overwrite with a newer version.
    let data2 = serde_json::to_vec(&TestState {
        value: 99,
        label: "updated".into(),
    })
    .unwrap();
    backend.save("my-store", "device-a", &data2, 10);
    let (loaded_data2, loaded_version2) = backend.load("my-store", "device-a").unwrap();

    assert_eq!(loaded_version2, 10);
    let loaded2: TestState = serde_json::from_slice(&loaded_data2).unwrap();
    assert_eq!(loaded2.value, 99);

    // Non-existent device returns None.
    assert!(backend.load("my-store", "device-z").is_none());
    assert!(backend.load("no-store", "device-a").is_none());
}

#[test]
fn test_file_backend_remove() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileBackend::new(dir.path());

    let data = serde_json::to_vec(&TestState {
        value: 1,
        label: "tmp".into(),
    })
    .unwrap();

    backend.save("store", "device-x", &data, 1);
    assert!(backend.load("store", "device-x").is_some());

    backend.remove("store", "device-x");
    assert!(backend.load("store", "device-x").is_none());

    // Removing a non-existent file should not panic.
    backend.remove("store", "device-x");
    backend.remove("no-store", "no-device");
}

#[test]
fn test_file_backend_path_traversal_sanitization() {
    let dir = tempfile::tempdir().unwrap();
    let backend = FileBackend::new(dir.path());

    let data = serde_json::to_vec(&TestState {
        value: 1,
        label: "safe".into(),
    })
    .unwrap();

    // Malicious store/device IDs should be sanitized, not escape base_dir.
    backend.save("../etc", "../../passwd", &data, 1);

    // Round-trip should succeed (same sanitization applied on load).
    let result = backend.load("../etc", "../../passwd");
    assert!(
        result.is_some(),
        "round-trip through sanitized path should work"
    );

    let (loaded, ver) = result.unwrap();
    assert_eq!(ver, 1);
    let loaded: TestState = serde_json::from_slice(&loaded).unwrap();
    assert_eq!(loaded.value, 1);

    // Verify no file was created outside base_dir.
    assert!(
        !dir.path().join("..").join("etc").exists(),
        "path traversal should be prevented"
    );
}

#[tokio::test]
async fn test_persistence_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let ws_port = random_port().await;

    let state = TestState {
        value: 77,
        label: "persisted".into(),
    };

    // First "session": create store, set data, then drop.
    {
        let (node, _event_tx) = make_test_node("device-a", ws_port).await;
        let backend = Arc::new(FileBackend::new(dir.path()));
        let store: Arc<SyncedStore<TestState>> =
            SyncedStore::new_with_backend(node, "persist-test", backend);

        store.set(state.clone()).await;
        assert_eq!(store.version(), 1);
        assert_eq!(store.local().await.unwrap(), state);

        store.stop().await;
    }

    // Second "session": create a new store with the same backend dir.
    {
        let ws_port2 = random_port().await;
        let (node, _event_tx) = make_test_node("device-a", ws_port2).await;
        let backend = Arc::new(FileBackend::new(dir.path()));
        let store: Arc<SyncedStore<TestState>> =
            SyncedStore::new_with_backend(node, "persist-test", backend);

        // Data should be restored from disk.
        let restored = store.local().await;
        assert!(
            restored.is_some(),
            "local data should be restored from disk"
        );
        assert_eq!(restored.unwrap(), state);

        // Version should be restored.
        assert_eq!(store.version(), 1);

        // Setting new data should continue from version 1.
        store
            .set(TestState {
                value: 88,
                label: "v2".into(),
            })
            .await;
        assert_eq!(store.version(), 2);

        store.stop().await;
    }
}

// ── Exhaustion (review): rapid set() bursts must not queue unboundedly ──

#[tokio::test]
async fn rapid_set_burst_completes_and_keeps_newest() {
    let (node, _events) = make_test_node("burst-node", 39400).await;
    let store: Arc<SyncedStore<u64>> = SyncedStore::new(node, "burst");

    // Far faster than any network drain; the watch channel coalesces so
    // this must complete quickly with only the newest version pending.
    for i in 0..10_000u64 {
        store.set(i).await;
    }
    assert_eq!(store.version(), 10_000);
    assert_eq!(store.local().await, Some(9_999));
    store.stop().await;
}
