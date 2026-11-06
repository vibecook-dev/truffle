//! Pulls between two nodes in one process (RFC 032).
//!
//! The two nodes share a [`PairFabric`]: each listens for the session's
//! WebSocket on its own loopback port, and dials the other by a fake tailnet
//! IP, which the fabric maps to that port. Any other port, a pull's data
//! listener, is dialed on loopback as it is. So a pull here runs the whole
//! protocol between two real nodes (PULL_REQUEST, OFFER, ACCEPT, the raw TCP
//! stream and the ACK) with no tailnet.
//!
//! The last tests feed [`pull_answer`] single messages: what a pull's waiter
//! takes as its answer, with other pulls in flight to the same holder.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};

use crate::envelope::codec::JsonCodec;
use crate::envelope::EnvelopeCodec;
use crate::network::{
    HealthInfo, IncomingConnection, NetworkError, NetworkPeer, NetworkPeerEvent, NetworkProvider,
    NetworkTcpListener, NetworkUdpSocket, NodeIdentity, PeerAddr, PingResult,
};
use crate::node::{NamespacedMessage, Node};
use crate::session::{PeerRegistry, PeerRegistryOptions};
use crate::transport::websocket::WebSocketTransport;
use crate::transport::WsConfig;

use super::pull_answer;
use super::types::{
    FtMessage, TransferError, TransferResult, NAMED_PULL_DENIED_REASON, PULL_DENIED_REASON,
};

// ---------------------------------------------------------------------------
// The pair: two nodes on a loopback fabric
// ---------------------------------------------------------------------------

/// The session's WebSocket port on both nodes.
const WS_PORT: u16 = 9417;

/// A fake tailnet IP → the loopback port of that node's WebSocket listener.
#[derive(Default)]
struct PairFabric {
    ws_listeners: Mutex<HashMap<String, u16>>,
}

impl PairFabric {
    /// The WebSocket listener's port of the node at `ip`.
    fn ws_port(&self, ip: &str) -> Result<u16, NetworkError> {
        self.ws_listeners
            .lock()
            .unwrap()
            .get(ip)
            .copied()
            .ok_or_else(|| NetworkError::DialFailed(format!("pair: no node at {ip}")))
    }
}

/// One node's view of the fabric.
struct PairProvider {
    identity: NodeIdentity,
    ip: String,
    fabric: Arc<PairFabric>,
    peer_event_tx: broadcast::Sender<NetworkPeerEvent>,
}

impl PairProvider {
    fn new(fabric: &Arc<PairFabric>, id: &str) -> Self {
        let (peer_event_tx, _) = broadcast::channel(64);
        Self {
            identity: NodeIdentity {
                app_id: "test".to_string(),
                // RFC 022 I1: `device_id` must differ from `tailscale_id` or
                // validate_hello refuses the exchange (close 4002), so the
                // pair prefixes it with `dev-`, as the session fixtures do.
                // Peers stay addressed by their tailscale id.
                device_id: format!("dev-{id}"),
                device_name: format!("Pair Node {id}"),
                tailscale_hostname: format!("truffle-test-{id}"),
                tailscale_id: id.to_string(),
                dns_name: None,
                ip: Some(ip_of(id).parse().unwrap()),
                login_name: None,
            },
            ip: ip_of(id).to_string(),
            fabric: fabric.clone(),
            peer_event_tx,
        }
    }
}

impl NetworkProvider for PairProvider {
    fn trust_mode(&self) -> crate::network::NetworkTrust {
        crate::network::NetworkTrust::UnverifiedLoopback
    }

    async fn start(&mut self) -> Result<(), NetworkError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), NetworkError> {
        Ok(())
    }

    fn local_identity(&self) -> NodeIdentity {
        self.identity.clone()
    }

    fn local_addr(&self) -> PeerAddr {
        PeerAddr {
            ip: Some(self.ip.parse().unwrap()),
            hostname: self.identity.tailscale_hostname.clone(),
            dns_name: None,
        }
    }

    fn peer_events(&self) -> broadcast::Receiver<NetworkPeerEvent> {
        self.peer_event_tx.subscribe()
    }

    async fn peers(&self) -> Vec<NetworkPeer> {
        vec![]
    }

    /// The WebSocket port is reached through the fabric by the peer's IP;
    /// any other port (a pull's data listener) on loopback as it is.
    async fn dial_tcp(&self, addr: &str, port: u16) -> Result<TcpStream, NetworkError> {
        let target = if port == WS_PORT {
            self.fabric.ws_port(addr)?
        } else {
            port
        };
        TcpStream::connect(("127.0.0.1", target))
            .await
            .map_err(|e| NetworkError::DialFailed(format!("pair dial {addr}:{port}: {e}")))
    }

    async fn listen_tcp(&self, port: u16) -> Result<NetworkTcpListener, NetworkError> {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .map_err(|e| NetworkError::ListenFailed(format!("pair listen :{port}: {e}")))?;
        let actual = listener.local_addr().unwrap().port();
        if port == WS_PORT {
            self.fabric
                .ws_listeners
                .lock()
                .unwrap()
                .insert(self.ip.clone(), actual);
        }
        let (tx, rx) = mpsc::channel::<IncomingConnection>(16);
        tokio::spawn(async move {
            while let Ok((stream, addr)) = listener.accept().await {
                let conn = IncomingConnection {
                    stream,
                    remote_addr: addr.to_string(),
                    remote_identity: String::new(),
                    port: actual,
                };
                if tx.send(conn).await.is_err() {
                    break;
                }
            }
        });
        Ok(NetworkTcpListener {
            port: actual,
            incoming: rx,
        })
    }

    async fn unlisten_tcp(&self, _port: u16) -> Result<(), NetworkError> {
        Ok(())
    }

    async fn bind_udp(&self, _port: u16) -> Result<NetworkUdpSocket, NetworkError> {
        Err(NetworkError::Internal("pair: UDP not supported".into()))
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

/// The fake tailnet IP of each node.
fn ip_of(id: &str) -> &'static str {
    match id {
        "alpha" => "10.0.0.1",
        "beta" => "10.0.0.2",
        other => panic!("no pair IP for {other}"),
    }
}

fn network_peer(id: &str) -> NetworkPeer {
    NetworkPeer {
        id: id.to_string(),
        hostname: format!("truffle-test-{id}"),
        ip: ip_of(id).parse().unwrap(),
        online: true,
        cur_addr: Some(format!("{}:41641", ip_of(id))),
        active: true,
        peer_relay: None,
        relay: None,
        os: None,
        last_seen: None,
        key_expiry: None,
        dns_name: None,
        login_name: None,
    }
}

type PairNode = Arc<Node<PairProvider>>;

/// Start node `id` on `fabric`, knowing `peer` (Layer 3 Joined).
async fn start_node(fabric: &Arc<PairFabric>, id: &str, peer: &str) -> PairNode {
    let provider = PairProvider::new(fabric, id);
    let peer_events = provider.peer_event_tx.clone();
    let network = Arc::new(provider);
    let ws_config = WsConfig {
        port: WS_PORT,
        ping_interval: Duration::from_secs(300),
        pong_timeout: Duration::from_secs(300),
        ..Default::default()
    };
    let ws_transport = Arc::new(WebSocketTransport::new(network.clone(), ws_config));
    // No eager dials: the first pull opens the one connection, from alpha.
    let options = PeerRegistryOptions {
        eager_identity: false,
        eager_identity_jitter_ms: 0,
        ..Default::default()
    };
    let registry = PeerRegistry::with_options(network.clone(), ws_transport, options);
    let session = Arc::new(registry);
    session.start().await;
    peer_events
        .send(NetworkPeerEvent::Joined(network_peer(peer)))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while session.peers().await.is_empty() {
        assert!(Instant::now() < deadline, "{id} never registered {peer}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let codec: Arc<dyn EnvelopeCodec> = Arc::new(JsonCodec);
    Arc::new(Node::from_parts(network, session, codec))
}

/// `alpha` pulls from `beta`. Beta shares a directory as `objects` and runs
/// its file-transfer receiver (auto-reject: it serves pulls, takes no
/// pushes).
struct Pair {
    alpha: PairNode,
    /// Held for the pair's lifetime.
    _beta: PairNode,
    /// The directory beta shares as `objects`.
    objects: tempfile::TempDir,
    /// Where alpha lands its pulls.
    landing: tempfile::TempDir,
}

impl Pair {
    async fn start() -> Self {
        let fabric = Arc::new(PairFabric::default());
        let alpha = start_node(&fabric, "alpha", "beta").await;
        let beta = start_node(&fabric, "beta", "alpha").await;
        let objects = tempfile::tempdir().unwrap();
        beta.file_transfer()
            .add_named_pull_root("objects", objects.path().to_str().unwrap())
            .unwrap();
        beta.file_transfer().auto_reject(beta.clone()).await;
        Self {
            alpha,
            _beta: beta,
            objects,
            landing: tempfile::tempdir().unwrap(),
        }
    }

    /// Write `bytes` at `path` below beta's shared directory.
    fn share(&self, path: &str, bytes: &[u8]) {
        let file = self.objects.path().join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, bytes).unwrap();
    }

    /// Where alpha lands the pull named `name`.
    fn landed(&self, name: &str) -> PathBuf {
        self.landing.path().join(name)
    }

    /// Pull `path` below beta's root `root` to `local` on alpha.
    async fn pull(
        &self,
        root: &str,
        path: &str,
        local: &Path,
    ) -> Result<TransferResult, TransferError> {
        self.alpha
            .file_transfer()
            .pull_named_file("beta", root, path, local.to_str().unwrap())
            .await
    }
}

/// Whether `result` is the holder's refusal with `reason`.
fn refused_with(result: &Result<TransferResult, TransferError>, reason: &str) -> bool {
    matches!(result, Err(TransferError::Rejected(r)) if r == reason)
}

const ONE: &[u8] = b"the first object";
const TWO: &[u8] = b"the second, longer object";

// ---------------------------------------------------------------------------
// Pulls across the pair
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_named_pull_crosses_the_pair_byte_for_byte() {
    let pair = Pair::start().await;
    pair.share("ab/f.bin", ONE);
    let local = pair.landed("f.bin");

    let result = pair.pull("objects", "ab/f.bin", &local).await.unwrap();

    assert_eq!(result.bytes_transferred, ONE.len() as u64);
    assert_eq!(std::fs::read(&local).unwrap(), ONE);
}

#[tokio::test]
async fn a_refused_named_pull_gets_the_named_reason() {
    // The holder speaks RFC 032, so it refuses a named pull with its one
    // named reason, never the legacy one, which the requester would read as
    // a holder that predates named roots.
    let pair = Pair::start().await;
    pair.share("ab/f.bin", ONE);
    let local = pair.landed("f.bin");

    let unknown_root = pair.pull("missing", "ab/f.bin", &local).await;
    assert!(
        refused_with(&unknown_root, NAMED_PULL_DENIED_REASON),
        "{unknown_root:?}"
    );
    let missing_file = pair.pull("objects", "ab/missing.bin", &local).await;
    assert!(
        refused_with(&missing_file, NAMED_PULL_DENIED_REASON),
        "{missing_file:?}"
    );

    #[cfg(unix)]
    {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret.txt"), b"secret").unwrap();
        let link = pair.objects.path().join("link.txt");
        std::os::unix::fs::symlink(outside.path().join("secret.txt"), link).unwrap();
        let outside_root = pair.pull("objects", "link.txt", &local).await;
        assert!(
            refused_with(&outside_root, NAMED_PULL_DENIED_REASON),
            "{outside_root:?}"
        );
    }

    assert!(!local.exists());
}

#[tokio::test]
async fn a_named_root_never_answers_an_absolute_pull() {
    // The shared file's own absolute path, with no absolute-path root: the
    // absolute-path refusal, though a named root holds the file.
    let pair = Pair::start().await;
    pair.share("ab/f.bin", ONE);
    let shared = std::fs::canonicalize(pair.objects.path().join("ab/f.bin")).unwrap();
    let local = pair.landed("f.bin");

    let result = pair
        .alpha
        .file_transfer()
        .pull_file("beta", shared.to_str().unwrap(), local.to_str().unwrap())
        .await;

    assert!(refused_with(&result, PULL_DENIED_REASON), "{result:?}");
    assert!(!local.exists());
}

#[tokio::test]
async fn concurrent_named_pulls_each_take_their_own_offer() {
    // Both OFFERs reach both of alpha's waiters. Each waiter takes the one
    // that carries its pull's token, so each file lands where it was asked
    // for. Taken by the first OFFER alone, both waiters would accept it and
    // one pull would wait for a data connection that never comes.
    let pair = Pair::start().await;
    pair.share("one.bin", ONE);
    pair.share("two.bin", TWO);
    let one = pair.landed("one.bin");
    let two = pair.landed("two.bin");

    let (first, second) = tokio::join!(
        pair.pull("objects", "one.bin", &one),
        pair.pull("objects", "two.bin", &two)
    );

    first.unwrap();
    second.unwrap();
    assert_eq!(std::fs::read(&one).unwrap(), ONE);
    assert_eq!(std::fs::read(&two).unwrap(), TWO);
}

#[tokio::test]
async fn a_refusal_answers_only_the_pull_it_names() {
    // The holder refuses one pull while it serves another. Both waiters see
    // the REJECT; only the refused pull's waiter takes it.
    let pair = Pair::start().await;
    pair.share("one.bin", ONE);
    let missing = pair.landed("missing.bin");
    let one = pair.landed("one.bin");

    let (refused, served) = tokio::join!(
        pair.pull("objects", "missing.bin", &missing),
        pair.pull("objects", "one.bin", &one)
    );

    assert!(
        refused_with(&refused, NAMED_PULL_DENIED_REASON),
        "{refused:?}"
    );
    served.unwrap();
    assert_eq!(std::fs::read(&one).unwrap(), ONE);
    assert!(!missing.exists());
}

// ---------------------------------------------------------------------------
// What a pull's waiter takes as its answer
// ---------------------------------------------------------------------------

/// `msg` as a waiter on alpha sees it, sent by `from`.
fn from_peer(from: &str, msg: &FtMessage) -> NamespacedMessage {
    NamespacedMessage {
        from: from.to_string(),
        namespace: "ft".to_string(),
        msg_type: "reply".to_string(),
        payload: serde_json::to_value(msg).unwrap(),
        timestamp: None,
    }
}

fn offer(token: &str) -> FtMessage {
    FtMessage::Offer {
        file_name: "f.bin".to_string(),
        size: 4,
        sha256: "0".repeat(64),
        save_path: String::new(),
        token: token.to_string(),
        tcp_port: 0,
    }
}

fn reject(token: &str, reason: &str) -> FtMessage {
    FtMessage::Reject {
        token: token.to_string(),
        reason: reason.to_string(),
    }
}

#[test]
fn a_reject_answers_only_the_pull_whose_token_it_carries() {
    for named in [true, false] {
        // Another pull's refusal is not this pull's answer, not even the
        // legacy reason that a named pull would read as Unsupported.
        for reason in [PULL_DENIED_REASON, NAMED_PULL_DENIED_REASON] {
            let msg = from_peer("beta", &reject("other-pull", reason));
            assert!(pull_answer(&msg, "beta", "this-pull", named).is_none());
        }
        let msg = from_peer("beta", &reject("this-pull", NAMED_PULL_DENIED_REASON));
        let answer = pull_answer(&msg, "beta", "this-pull", named);
        assert!(
            matches!(&answer, Some(Err(TransferError::Rejected(_)))),
            "{answer:?}"
        );
    }
    // This named pull's own legacy refusal: a holder that predates named roots.
    let msg = from_peer("beta", &reject("this-pull", PULL_DENIED_REASON));
    let answer = pull_answer(&msg, "beta", "this-pull", true);
    assert!(
        matches!(&answer, Some(Err(TransferError::Unsupported(_)))),
        "{answer:?}"
    );
}

#[test]
fn a_named_pull_takes_only_the_offer_that_carries_its_token() {
    let msg = from_peer("beta", &offer("other-pull"));
    assert!(pull_answer(&msg, "beta", "this-pull", true).is_none());
    let msg = from_peer("beta", &offer("this-pull"));
    let answer = pull_answer(&msg, "beta", "this-pull", true);
    assert!(
        matches!(&answer, Some(Ok(o)) if o.token == "this-pull"),
        "{answer:?}"
    );
}

#[test]
fn an_absolute_pull_takes_its_peers_offer_whatever_its_token() {
    // Every holder offers an absolute-path pull under a fresh token, which
    // the requester cannot know.
    let msg = from_peer("beta", &offer("fresh-token"));
    let answer = pull_answer(&msg, "beta", "this-pull", false);
    assert!(
        matches!(&answer, Some(Ok(o)) if o.token == "fresh-token"),
        "{answer:?}"
    );
}

#[test]
fn another_peers_messages_answer_no_pull() {
    for named in [true, false] {
        for msg in [
            offer("this-pull"),
            reject("this-pull", NAMED_PULL_DENIED_REASON),
        ] {
            let msg = from_peer("gamma", &msg);
            assert!(pull_answer(&msg, "beta", "this-pull", named).is_none());
        }
    }
}
