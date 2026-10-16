//! Go sidecar process management.
//!
//! Spawns the Go sidecar binary, communicates via stdin/stdout JSON lines,
//! and manages the process lifecycle. This is Layer 1 in the architecture.
//!
//! All types and functions in this module are private to the `tailscale` module.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, Notify};

use super::protocol::*;
use crate::network::NetworkError;

/// Configuration for spawning the Go sidecar.
#[derive(Clone)]
pub(crate) struct SidecarConfig {
    /// Path to the Go sidecar binary.
    pub binary_path: PathBuf,
    /// Hostname for the tsnet node.
    pub hostname: String,
    /// State directory for tsnet.
    pub state_dir: String,
    /// Optional Tailscale auth key.
    pub auth_key: Option<String>,
    /// Bridge port that Rust is listening on.
    pub bridge_port: u16,
    /// Session token as hex string (64 hex chars = 32 bytes).
    pub session_token_hex: String,
    /// Whether the node is ephemeral.
    pub ephemeral: Option<bool>,
    /// ACL tags to advertise.
    pub tags: Option<Vec<String>>,
    /// Override the bridged-connection idle-reap deadline (seconds); `None`
    /// leaves the sidecar's 600s default (RFC 021 §6.5).
    pub idle_timeout_secs: Option<u64>,
}

/// Manual `Debug`: `auth_key` (tailnet credential) and `session_token_hex`
/// (bridge auth secret) must never reach logs, so both are redacted.
impl std::fmt::Debug for SidecarConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SidecarConfig")
            .field("binary_path", &self.binary_path)
            .field("hostname", &self.hostname)
            .field("state_dir", &self.state_dir)
            .field("auth_key", &self.auth_key.as_ref().map(|_| "[REDACTED]"))
            .field("bridge_port", &self.bridge_port)
            .field("session_token_hex", &"[REDACTED]")
            .field("ephemeral", &self.ephemeral)
            .field("tags", &self.tags)
            .field("idle_timeout_secs", &self.idle_timeout_secs)
            .finish()
    }
}

/// Internal events from the sidecar event processing loop.
#[derive(Debug, Clone)]
pub(crate) enum SidecarInternalEvent {
    /// Sidecar reached "running" state.
    Started {
        hostname: String,
        dns_name: String,
        tailscale_ip: String,
        node_id: String,
        /// The node's own tailnet login (RFC 025 §3.3); `None` below
        /// sidecar protocol 5 or when the status named no profile for us.
        login_name: Option<String>,
        /// Sidecar control-protocol version; `None` = pre-RFC-023 (v1).
        protocol_version: Option<u32>,
    },
    /// Sidecar stopped.
    Stopped,
    /// Auth required.
    AuthRequired { auth_url: String },
    /// Needs admin approval.
    NeedsApproval,
    /// State changed.
    StateChange { state: String },
    /// Key expiring.
    KeyExpiring { expires_at: String },
    /// Health warnings.
    HealthWarning { warnings: Vec<String> },
    /// Peer list received (from getPeers).
    PeersReceived(Vec<SidecarPeer>),
    /// A single peer changed (from WatchIPNBus).
    PeerChanged(PeerChangedEventData),
    /// Dial result (success — the bridge connection will arrive separately).
    ///
    /// No `request_id` field: correlation happens at the JSON level in
    /// [`GoSidecar::dispatch_event`], before typed parsing.
    DialSucceeded,
    /// Dial result (failure — no bridge connection coming).
    DialFailed { error: String },
    /// Listening on a port succeeded.
    Listening { port: u16 },
    /// Unlistened from a port.
    #[allow(dead_code)]
    Unlistened { port: u16 },
    /// UDP listening on a port succeeded. `local_port` is the localhost relay port.
    ListeningPacket { port: u16, local_port: u16 },
    /// Ping result.
    PingResult(PingResultEventData),
    /// WhoIs query result.
    WhoisResult(WhoisResultEventData),
    /// Error from sidecar.
    Error { code: String, message: String },
    /// A reverse proxy was successfully started.
    ProxyAdded {
        id: String,
        listen_port: u16,
        url: String,
    },
    /// A reverse proxy was stopped.
    ProxyRemoved { id: String },
    /// List of active proxies.
    ProxyList { proxies: Vec<ProxyInfoEventData> },
    /// A proxy encountered an error.
    ProxyError {
        id: String,
        code: String,
        message: String,
    },
    /// The node's loopback proxy (RFC 026 E1). Its data's `Debug` redacts
    /// both credentials.
    Loopback(LoopbackEventData),
    /// An egress route was bound (RFC 026 E2).
    EgressAdded { id: String, listen: String },
    /// An egress route was removed (RFC 026 E2).
    EgressRemoved { id: String },
    /// The live egress routes (RFC 026 E2).
    EgressList { routes: Vec<EgressRouteEventData> },
    /// The node's CONNECT proxy (RFC 026 E3). Its data's `Debug` redacts
    /// the credential.
    EgressProxy(EgressProxyEventData),
    /// An egress command failed, or (with an id and no `requestId` on the
    /// wire) a route refused or could not dial an accepted connection.
    EgressError {
        id: String,
        code: String,
        message: String,
    },
    /// Sidecar process exited unexpectedly.
    ProcessExited { exit_code: Option<i32> },
}

/// Pending request/reply slots, keyed by the wire-level `requestId`.
///
/// The stdout reader TEES a matching event into the slot's oneshot and still
/// broadcasts it unchanged: RPC waiters get lag-proof delivery (a oneshot
/// cannot be overwritten by an event burst, unlike a slot in the 256-entry
/// broadcast ring), while broadcast consumers — the provider's event
/// processor, the proxy runtime-error stream — observe exactly the stream
/// they always did.
///
/// A `std` mutex, deliberately: every critical section is a map op with no
/// await, and `ReplyGuard::drop` must be able to lock without an executor.
pub(crate) type PendingReplies =
    Arc<StdMutex<HashMap<String, oneshot::Sender<SidecarInternalEvent>>>>;

/// Test-only: a registered reply slot backed by its own pending map, so
/// broker semantics (routing, RAII unregistration) are testable without a
/// spawned sidecar process.
#[cfg(test)]
pub(crate) fn test_reply_slot(request_id: &str) -> (PendingReplies, ReplyGuard) {
    let pending: PendingReplies = Arc::new(StdMutex::new(HashMap::new()));
    let (tx, rx) = oneshot::channel();
    pending
        .lock()
        .expect("pending replies lock")
        .insert(request_id.to_string(), tx);
    let guard = ReplyGuard {
        request_id: request_id.to_string(),
        pending: pending.clone(),
        rx,
    };
    (pending, guard)
}

/// A registered reply slot for one in-flight command.
///
/// Obtain it via [`GoSidecar::register_reply`] BEFORE sending the correlated
/// command, so the answer can never race the registration. Dropping the
/// guard (timeout, cancellation) unregisters the slot, so the pending map
/// cannot accumulate entries for callers that gave up.
pub(crate) struct ReplyGuard {
    request_id: String,
    pending: PendingReplies,
    rx: oneshot::Receiver<SidecarInternalEvent>,
}

impl ReplyGuard {
    /// Await the routed reply. Errors when the slot's sender disappeared
    /// without a reply — the routed event failed typed parsing, or the
    /// sidecar is shutting down.
    pub async fn recv(&mut self) -> Result<SidecarInternalEvent, NetworkError> {
        (&mut self.rx).await.map_err(|_| {
            NetworkError::SidecarError("reply channel closed before a result arrived".into())
        })
    }
}

impl Drop for ReplyGuard {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&self.request_id);
        }
    }
}

/// Consecutive failed status checks (`try_wait` errors, one per second)
/// after which the process watcher stops watching the sidecar child.
const MAX_STATUS_CHECK_ERRORS: u32 = 3;

/// Manages the Go sidecar child process.
///
/// Handles spawning, command sending via stdin, event reading from stdout,
/// and provides a broadcast channel for internal event distribution.
pub(crate) struct GoSidecar {
    /// Channel for sending commands to the sidecar's stdin writer task.
    stdin_tx: mpsc::Sender<String>,
    /// Internal events broadcast from the stdout reader task.
    event_tx: broadcast::Sender<SidecarInternalEvent>,
    /// Reply slots for in-flight request/response commands.
    pending_replies: PendingReplies,
    /// Signal to shut down the sidecar.
    shutdown: Arc<Notify>,
    /// Handle to the child process (for kill on drop).
    child: Arc<Mutex<Option<Child>>>,
    /// Owner-supplied slot holding the child's pid while it runs (0 = none).
    pid_slot: Arc<AtomicU32>,
    /// The child's pid as captured at spawn (0 if the OS reported none).
    pid: u32,
}

impl GoSidecar {
    /// Spawn a new Go sidecar process.
    ///
    /// Returns the sidecar handle and a broadcast receiver for internal events.
    ///
    /// `pid_slot` receives the child's process id at spawn and is reset to 0
    /// when the process is seen to exit, when [`shutdown`](Self::shutdown)
    /// kills it, or when this handle is dropped (`kill_on_drop`). The reset
    /// only applies while the slot still holds this child's pid, so a slot
    /// reused by a later sidecar is never cleared by an earlier one.
    pub async fn spawn(
        config: SidecarConfig,
        pid_slot: Arc<AtomicU32>,
    ) -> Result<(Self, broadcast::Receiver<SidecarInternalEvent>), NetworkError> {
        let (event_tx, event_rx) = broadcast::channel(256);
        let (stdin_tx, stdin_rx) = mpsc::channel::<String>(64);
        let shutdown = Arc::new(Notify::new());
        let pending_replies: PendingReplies = Arc::new(StdMutex::new(HashMap::new()));

        // Spawn the child process
        let child = Self::spawn_child(&config)?;
        let pid = child.id().unwrap_or(0);
        pid_slot.store(pid, Ordering::Relaxed);
        let child = Arc::new(Mutex::new(Some(child)));

        let sidecar = GoSidecar {
            stdin_tx,
            event_tx: event_tx.clone(),
            pending_replies: pending_replies.clone(),
            shutdown: shutdown.clone(),
            child: child.clone(),
            pid_slot: pid_slot.clone(),
            pid,
        };

        // Take stdout/stdin from child before spawning tasks
        {
            let mut guard = child.lock().await;
            let child_proc = guard
                .as_mut()
                .ok_or_else(|| NetworkError::SidecarError("child process not available".into()))?;

            let stdout = child_proc
                .stdout
                .take()
                .ok_or_else(|| NetworkError::SidecarError("failed to capture stdout".into()))?;
            let stdin = child_proc
                .stdin
                .take()
                .ok_or_else(|| NetworkError::SidecarError("failed to capture stdin".into()))?;

            // Forward sidecar stderr so Go log.Printf output is visible.
            if let Some(stderr) = child_proc.stderr.take() {
                let stderr_shutdown = shutdown.clone();
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stderr).lines();
                    loop {
                        tokio::select! {
                            _ = stderr_shutdown.notified() => break,
                            result = reader.next_line() => {
                                match result {
                                    Ok(Some(line)) => {
                                        eprintln!("[sidecar-stderr] {line}");
                                    }
                                    Ok(None) => break,
                                    Err(e) => {
                                        tracing::warn!("sidecar stderr read error: {e}");
                                        break;
                                    }
                                }
                            }
                        }
                    }
                });
            }

            // Spawn stdin writer task
            Self::spawn_stdin_writer(stdin, stdin_rx, shutdown.clone());

            // Spawn stdout reader task
            Self::spawn_stdout_reader(stdout, event_tx.clone(), pending_replies, shutdown.clone());
        }

        // Spawn process watcher task
        Self::spawn_process_watcher(child.clone(), event_tx, shutdown, pid_slot, pid);

        Ok((sidecar, event_rx))
    }

    /// Reset `pid_slot` to 0 if it still holds `pid`.
    fn clear_pid(pid_slot: &AtomicU32, pid: u32) {
        if pid != 0 {
            let _ = pid_slot.compare_exchange(pid, 0, Ordering::Relaxed, Ordering::Relaxed);
        }
    }

    /// Record one failed status check (`try_wait` error) of the child and
    /// say whether the process watcher should give up.
    ///
    /// A single error may be transient, so the watcher keeps polling until
    /// [`MAX_STATUS_CHECK_ERRORS`] fail in a row. Then the child's state can
    /// no longer be known (typically it was reaped behind our back, which
    /// `ECHILD` reports), so the pid slot is cleared: reporting a pid nobody
    /// watches any more would hand samplers a dead or reused process.
    fn status_check_failed(consecutive_errors: &mut u32, pid_slot: &AtomicU32, pid: u32) -> bool {
        *consecutive_errors += 1;
        if *consecutive_errors < MAX_STATUS_CHECK_ERRORS {
            return false;
        }
        Self::clear_pid(pid_slot, pid);
        true
    }

    fn spawn_child(config: &SidecarConfig) -> Result<Child, NetworkError> {
        Command::new(&config.binary_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| NetworkError::SidecarError(format!("failed to spawn: {e}")))
    }

    fn spawn_stdin_writer(
        mut stdin: tokio::process::ChildStdin,
        mut rx: mpsc::Receiver<String>,
        shutdown: Arc<Notify>,
    ) {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.notified() => {
                        tracing::debug!("sidecar stdin writer shutting down");
                        break;
                    }
                    msg = rx.recv() => {
                        match msg {
                            Some(line) => {
                                if let Err(e) = stdin.write_all(line.as_bytes()).await {
                                    tracing::error!("failed to write to sidecar stdin: {e}");
                                    break;
                                }
                                if let Err(e) = stdin.write_all(b"\n").await {
                                    tracing::error!("failed to write newline to sidecar stdin: {e}");
                                    break;
                                }
                                if let Err(e) = stdin.flush().await {
                                    tracing::error!("failed to flush sidecar stdin: {e}");
                                    break;
                                }
                            }
                            None => {
                                tracing::debug!("sidecar stdin channel closed");
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    fn spawn_stdout_reader(
        stdout: tokio::process::ChildStdout,
        event_tx: broadcast::Sender<SidecarInternalEvent>,
        pending_replies: PendingReplies,
        shutdown: Arc<Notify>,
    ) {
        tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines = reader.lines();

            loop {
                tokio::select! {
                    _ = shutdown.notified() => {
                        tracing::debug!("sidecar stdout reader shutting down");
                        break;
                    }
                    result = lines.next_line() => {
                        match result {
                            Ok(Some(line)) => {
                                if line.trim().is_empty() {
                                    continue;
                                }
                                if let Some(event) = Self::parse_stdout_event(&line) {
                                    Self::dispatch_event(event, &pending_replies, &event_tx);
                                }
                            }
                            Ok(None) => {
                                tracing::info!("sidecar stdout closed (process exited)");
                                break;
                            }
                            Err(e) => {
                                tracing::error!("sidecar stdout read error: {e}");
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    fn parse_stdout_event(line: &str) -> Option<SidecarEvent> {
        match serde_json::from_str(line) {
            Ok(event) => Some(event),
            Err(error) => {
                // Both the payload and serde's Display can quote credentials.
                // Keep useful parse coordinates without including input values.
                tracing::warn!(
                    category = ?error.classify(),
                    line = error.line(),
                    column = error.column(),
                    "failed to parse sidecar event"
                );
                None
            }
        }
    }

    /// Route one parsed event: TEE it into a registered reply slot when its
    /// data carries a matching `requestId`, then broadcast it unchanged
    /// either way. The peek happens at the JSON level so the broker never
    /// needs per-variant plumbing — any event type the sidecar learns to
    /// echo an id on is routable for free.
    pub(crate) fn dispatch_event(
        event: SidecarEvent,
        pending_replies: &PendingReplies,
        event_tx: &broadcast::Sender<SidecarInternalEvent>,
    ) {
        let reply_tx = event
            .data
            .get("requestId")
            .and_then(|v| v.as_str())
            .filter(|id| !id.is_empty())
            .and_then(|id| match pending_replies.lock() {
                Ok(mut pending) => pending.remove(id),
                Err(_) => None,
            });
        if let Some(internal) = Self::map_event(event) {
            if let Some(tx) = reply_tx {
                let _ = tx.send(internal.clone());
            }
            let _ = event_tx.send(internal);
        }
        // A routed event that fails typed parsing drops its slot's sender,
        // so the waiter fails fast on a closed channel instead of timing
        // out — honest for what is necessarily a codec bug.
    }

    /// Register a reply slot for `request_id`. Call BEFORE sending the
    /// correlated command; hold the guard for the wait's whole lifetime.
    pub(crate) fn register_reply(&self, request_id: &str) -> ReplyGuard {
        let (tx, rx) = oneshot::channel();
        if let Ok(mut pending) = self.pending_replies.lock() {
            pending.insert(request_id.to_string(), tx);
        }
        ReplyGuard {
            request_id: request_id.to_string(),
            pending: self.pending_replies.clone(),
            rx,
        }
    }

    fn spawn_process_watcher(
        child: Arc<Mutex<Option<Child>>>,
        event_tx: broadcast::Sender<SidecarInternalEvent>,
        shutdown: Arc<Notify>,
        pid_slot: Arc<AtomicU32>,
        pid: u32,
    ) {
        tokio::spawn(async move {
            // One shutdown waiter for the task's whole life, registered up
            // front: a fresh `notified()` per iteration would miss a
            // `notify_waiters()` landing while the loop body runs.
            let shutdown_signal = shutdown.notified();
            tokio::pin!(shutdown_signal);
            shutdown_signal.as_mut().enable();
            let mut consecutive_errors = 0;
            loop {
                tokio::select! {
                    _ = &mut shutdown_signal => {
                        tracing::debug!("process watcher shutting down");
                        return;
                    }
                    _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
                        let mut guard = child.lock().await;
                        if let Some(ref mut c) = *guard {
                            match c.try_wait() {
                                Ok(Some(status)) => {
                                    let exit_code = status.code();
                                    tracing::warn!("sidecar process exited with code: {exit_code:?}");
                                    Self::clear_pid(&pid_slot, pid);
                                    let _ = event_tx.send(SidecarInternalEvent::ProcessExited { exit_code });
                                    *guard = None;
                                    return;
                                }
                                Ok(None) => {
                                    // Still running, continue polling
                                    consecutive_errors = 0;
                                }
                                Err(e) => {
                                    if Self::status_check_failed(&mut consecutive_errors, &pid_slot, pid) {
                                        tracing::error!(
                                            "failed to check sidecar process status \
                                             {consecutive_errors} times in a row, no longer \
                                             watching it: {e}"
                                        );
                                        return;
                                    }
                                    tracing::warn!("failed to check sidecar process status: {e}");
                                }
                            }
                        } else {
                            return;
                        }
                    }
                }
            }
        });
    }

    fn map_event(event: SidecarEvent) -> Option<SidecarInternalEvent> {
        match event.event.as_str() {
            event_type::STATUS | event_type::STARTED => {
                if let Ok(data) = serde_json::from_value::<StatusEventData>(event.data) {
                    if data.state == "running" {
                        Some(SidecarInternalEvent::Started {
                            hostname: data.hostname,
                            dns_name: data.dns_name,
                            tailscale_ip: data.tailscale_ip,
                            node_id: data.node_id,
                            login_name: data.login_name.filter(|l| !l.is_empty()),
                            protocol_version: data.protocol_version,
                        })
                    } else if data.state == "error" {
                        Some(SidecarInternalEvent::Error {
                            code: "STATUS_ERROR".to_string(),
                            message: data.error,
                        })
                    } else {
                        // Other status updates (starting, etc.)
                        Some(SidecarInternalEvent::StateChange { state: data.state })
                    }
                } else {
                    None
                }
            }
            event_type::STOPPED => Some(SidecarInternalEvent::Stopped),
            event_type::AUTH_REQUIRED => {
                serde_json::from_value::<AuthRequiredEventData>(event.data)
                    .ok()
                    .map(|d| SidecarInternalEvent::AuthRequired {
                        auth_url: d.auth_url,
                    })
            }
            event_type::NEEDS_APPROVAL => Some(SidecarInternalEvent::NeedsApproval),
            event_type::STATE_CHANGE => serde_json::from_value::<StateChangeEventData>(event.data)
                .ok()
                .map(|d| SidecarInternalEvent::StateChange { state: d.state }),
            event_type::KEY_EXPIRING => serde_json::from_value::<KeyExpiringEventData>(event.data)
                .ok()
                .map(|d| SidecarInternalEvent::KeyExpiring {
                    expires_at: d.expires_at,
                }),
            event_type::HEALTH_WARNING => {
                serde_json::from_value::<HealthWarningEventData>(event.data)
                    .ok()
                    .map(|d| SidecarInternalEvent::HealthWarning {
                        warnings: d.warnings,
                    })
            }
            event_type::PEERS => serde_json::from_value::<PeersEventData>(event.data)
                .ok()
                .map(|d| SidecarInternalEvent::PeersReceived(d.peers)),
            event_type::PEER_CHANGED => serde_json::from_value::<PeerChangedEventData>(event.data)
                .ok()
                .map(SidecarInternalEvent::PeerChanged),
            event_type::DIAL_RESULT => serde_json::from_value::<DialResultEventData>(event.data)
                .ok()
                .map(|d| {
                    if d.success {
                        SidecarInternalEvent::DialSucceeded
                    } else {
                        SidecarInternalEvent::DialFailed { error: d.error }
                    }
                }),
            event_type::LISTENING => serde_json::from_value::<ListeningEventData>(event.data)
                .ok()
                .map(|d| SidecarInternalEvent::Listening { port: d.port }),
            event_type::UNLISTENED => serde_json::from_value::<UnlistenedEventData>(event.data)
                .ok()
                .map(|d| SidecarInternalEvent::Unlistened { port: d.port }),
            event_type::LISTENING_PACKET => {
                serde_json::from_value::<ListeningPacketEventData>(event.data)
                    .ok()
                    .map(|d| SidecarInternalEvent::ListeningPacket {
                        port: d.port,
                        local_port: d.local_port,
                    })
            }
            event_type::PING_RESULT => serde_json::from_value::<PingResultEventData>(event.data)
                .ok()
                .map(SidecarInternalEvent::PingResult),
            event_type::WHOIS_RESULT => serde_json::from_value::<WhoisResultEventData>(event.data)
                .ok()
                .map(SidecarInternalEvent::WhoisResult),
            event_type::ERROR => serde_json::from_value::<ErrorEventData>(event.data)
                .ok()
                .map(|d| SidecarInternalEvent::Error {
                    code: d.code,
                    message: d.message,
                }),
            event_type::PROXY_ADDED => serde_json::from_value::<ProxyAddedEventData>(event.data)
                .ok()
                .map(|d| SidecarInternalEvent::ProxyAdded {
                    id: d.id,
                    listen_port: d.listen_port,
                    url: d.url,
                }),
            event_type::PROXY_REMOVED => {
                serde_json::from_value::<ProxyRemovedEventData>(event.data)
                    .ok()
                    .map(|d| SidecarInternalEvent::ProxyRemoved { id: d.id })
            }
            event_type::PROXY_LIST_RESULT => {
                serde_json::from_value::<ProxyListEventData>(event.data)
                    .ok()
                    .map(|d| SidecarInternalEvent::ProxyList { proxies: d.proxies })
            }
            event_type::PROXY_ERROR => serde_json::from_value::<ProxyErrorEventData>(event.data)
                .ok()
                .map(|d| SidecarInternalEvent::ProxyError {
                    id: d.id,
                    code: d.code,
                    message: d.message,
                }),
            event_type::LOOPBACK_RESULT => serde_json::from_value::<LoopbackEventData>(event.data)
                .ok()
                .map(SidecarInternalEvent::Loopback),
            event_type::EGRESS_ADDED => serde_json::from_value::<EgressAddedEventData>(event.data)
                .ok()
                .map(|d| SidecarInternalEvent::EgressAdded {
                    id: d.id,
                    listen: d.listen,
                }),
            event_type::EGRESS_REMOVED => {
                serde_json::from_value::<EgressRemovedEventData>(event.data)
                    .ok()
                    .map(|d| SidecarInternalEvent::EgressRemoved { id: d.id })
            }
            event_type::EGRESS_LIST_RESULT => {
                serde_json::from_value::<EgressListEventData>(event.data)
                    .ok()
                    .map(|d| SidecarInternalEvent::EgressList { routes: d.routes })
            }
            event_type::EGRESS_PROXY_RESULT => {
                serde_json::from_value::<EgressProxyEventData>(event.data)
                    .ok()
                    .map(SidecarInternalEvent::EgressProxy)
            }
            event_type::EGRESS_ERROR => serde_json::from_value::<EgressErrorEventData>(event.data)
                .ok()
                .map(|d| SidecarInternalEvent::EgressError {
                    id: d.id,
                    code: d.code,
                    message: d.message,
                }),
            other => {
                tracing::debug!("unhandled sidecar event type: {other}");
                None
            }
        }
    }

    /// Send a JSON command to the sidecar.
    pub async fn send_command(&self, cmd: SidecarCommand) -> Result<(), NetworkError> {
        let json = serde_json::to_string(&cmd)?;
        self.stdin_tx
            .send(json)
            .await
            .map_err(|e| NetworkError::SidecarError(format!("stdin channel closed: {e}")))
    }

    /// Send the tsnet:start command.
    pub async fn send_start(&self, config: &SidecarConfig) -> Result<(), NetworkError> {
        let data = StartCommandData {
            hostname: config.hostname.clone(),
            state_dir: config.state_dir.clone(),
            auth_key: config.auth_key.clone(),
            bridge_port: config.bridge_port,
            session_token: config.session_token_hex.clone(),
            ephemeral: config.ephemeral,
            tags: config.tags.clone(),
            idle_timeout_secs: config.idle_timeout_secs,
        };
        self.send_command(SidecarCommand {
            command: command_type::START,
            data: Some(serde_json::to_value(&data)?),
        })
        .await
    }

    /// Send the tsnet:stop command.
    pub async fn send_stop(&self) -> Result<(), NetworkError> {
        self.send_command(SidecarCommand {
            command: command_type::STOP,
            data: None,
        })
        .await
    }

    /// Send the tsnet:getPeers command.
    pub async fn send_get_peers(&self) -> Result<(), NetworkError> {
        self.send_command(SidecarCommand {
            command: command_type::GET_PEERS,
            data: None,
        })
        .await
    }

    /// Send the bridge:dial command.
    ///
    /// `tls` overrides TLS wrapping: `None` keeps the sidecar's legacy port==443
    /// behavior; `Some(_)` forces it on/off (RFC 021 §6.4).
    pub async fn send_dial(
        &self,
        request_id: String,
        target: String,
        port: u16,
        tls: Option<bool>,
    ) -> Result<(), NetworkError> {
        let data = DialCommandData {
            request_id,
            target,
            port,
            tls,
        };
        self.send_command(SidecarCommand {
            command: command_type::DIAL,
            data: Some(serde_json::to_value(&data)?),
        })
        .await
    }

    /// Send the tsnet:listen command.
    pub async fn send_listen(
        &self,
        port: u16,
        tls: Option<bool>,
        request_id: Option<String>,
    ) -> Result<(), NetworkError> {
        let data = ListenCommandData {
            port,
            tls,
            request_id,
        };
        self.send_command(SidecarCommand {
            command: command_type::LISTEN,
            data: Some(serde_json::to_value(&data)?),
        })
        .await
    }

    /// Send the tsnet:unlisten command.
    pub async fn send_unlisten(&self, port: u16) -> Result<(), NetworkError> {
        let data = UnlistenCommandData { port };
        self.send_command(SidecarCommand {
            command: command_type::UNLISTEN,
            data: Some(serde_json::to_value(&data)?),
        })
        .await
    }

    /// Send the tsnet:ping command.
    pub async fn send_ping(
        &self,
        target: String,
        ping_type: Option<String>,
        request_id: Option<String>,
    ) -> Result<(), NetworkError> {
        let data = PingCommandData {
            target,
            ping_type,
            request_id,
        };
        self.send_command(SidecarCommand {
            command: command_type::PING,
            data: Some(serde_json::to_value(&data)?),
        })
        .await
    }

    /// Send the tsnet:whois command.
    pub async fn send_whois(
        &self,
        addr: String,
        request_id: Option<String>,
    ) -> Result<(), NetworkError> {
        let data = WhoisCommandData { addr, request_id };
        self.send_command(SidecarCommand {
            command: command_type::WHOIS,
            data: Some(serde_json::to_value(&data)?),
        })
        .await
    }

    /// Send the tsnet:listenPacket command to bind a UDP socket via tsnet.
    pub async fn send_listen_packet(
        &self,
        port: u16,
        request_id: Option<String>,
    ) -> Result<(), NetworkError> {
        let data = ListenPacketCommandData {
            port,
            request_id,
            relay_version: 6,
        };
        self.send_command(SidecarCommand {
            command: command_type::LISTEN_PACKET,
            data: Some(serde_json::to_value(&data)?),
        })
        .await
    }

    /// Tie cleanup to this sidecar generation and binding ID, never a port.
    /// The fast path also works when a socket is dropped outside a runtime.
    pub(crate) fn udp_cleanup(&self, relay_id: &str) -> impl FnOnce() + Send + Sync + 'static {
        let tx = self.stdin_tx.clone();
        let runtime = tokio::runtime::Handle::current();
        let json =
            serde_json::json!({"command":"tsnet:unlistenPacket", "data":{"relayId":relay_id}})
                .to_string();
        move || {
            if let Err(mpsc::error::TrySendError::Full(json)) = tx.try_send(json) {
                runtime.spawn(async move {
                    let _ = tx.send(json).await;
                });
            }
        }
    }

    /// Send the tsnet:watchPeers command to start WatchIPNBus-based peer events.
    pub async fn send_watch_peers(&self) -> Result<(), NetworkError> {
        let data = WatchPeersCommandData { include_all: None };
        self.send_command(SidecarCommand {
            command: command_type::WATCH_PEERS,
            data: Some(serde_json::to_value(&data)?),
        })
        .await
    }

    /// Send the proxy:add command.
    pub async fn send_proxy_add(&self, data: ProxyAddCommandData) -> Result<(), NetworkError> {
        self.send_command(SidecarCommand {
            command: command_type::PROXY_ADD,
            data: Some(serde_json::to_value(&data)?),
        })
        .await
    }

    /// Send the proxy:remove command.
    pub async fn send_proxy_remove(
        &self,
        id: &str,
        request_id: Option<String>,
    ) -> Result<(), NetworkError> {
        let data = ProxyRemoveCommandData {
            id: id.to_string(),
            request_id,
        };
        self.send_command(SidecarCommand {
            command: command_type::PROXY_REMOVE,
            data: Some(serde_json::to_value(&data)?),
        })
        .await
    }

    /// Send the proxy:list command.
    pub async fn send_proxy_list(&self, request_id: Option<String>) -> Result<(), NetworkError> {
        // Pre-v4 wire shape was no payload at all — keep that exact shape
        // when no correlation is requested.
        let data = match request_id {
            Some(_) => {
                let payload = ProxyListCommandData { request_id };
                Some(serde_json::to_value(&payload)?)
            }
            None => None,
        };
        self.send_command(SidecarCommand {
            command: command_type::PROXY_LIST,
            data,
        })
        .await
    }

    /// Send one RFC 026 egress command (`tsnet:loopback`, `egress:*`). Every
    /// payload carries its `requestId`; the caller registers the reply slot
    /// first.
    pub async fn send_egress<T: serde::Serialize>(
        &self,
        command: &'static str,
        data: &T,
    ) -> Result<(), NetworkError> {
        self.send_command(SidecarCommand {
            command,
            data: Some(serde_json::to_value(data)?),
        })
        .await
    }

    /// Subscribe to sidecar internal events.
    pub fn subscribe(&self) -> broadcast::Receiver<SidecarInternalEvent> {
        self.event_tx.subscribe()
    }

    /// Shut down the sidecar process.
    pub async fn shutdown(&self) {
        // Try to send stop command gracefully
        let _ = self.send_stop().await;

        // Signal all tasks to stop
        self.shutdown.notify_waiters();

        // Give the process a moment to exit gracefully
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Force kill if still running
        let mut guard = self.child.lock().await;
        if let Some(ref mut child) = *guard {
            tracing::info!("force-killing sidecar process");
            let _ = child.kill().await;
            *guard = None;
        }
        Self::clear_pid(&self.pid_slot, self.pid);
    }
}

impl Drop for GoSidecar {
    fn drop(&mut self) {
        self.shutdown.notify_waiters();
        // The watcher task releases the last `Child` handle once notified,
        // and `kill_on_drop` ends the process with it.
        Self::clear_pid(&self.pid_slot, self.pid);
    }
}

#[cfg(test)]
mod config_debug_tests {
    use super::*;

    #[test]
    fn sidecar_config_debug_redacts_secrets() {
        let config = SidecarConfig {
            binary_path: PathBuf::from("/opt/sidecar"),
            hostname: "truffle-demo-dev".to_string(),
            state_dir: "/tmp/state".to_string(),
            auth_key: Some("dummy-auth-SECRET123".to_string()),
            bridge_port: 12345,
            session_token_hex: "deadbeef".repeat(8),
            ephemeral: None,
            tags: None,
            idle_timeout_secs: None,
        };
        let dbg = format!("{config:?}");
        assert!(!dbg.contains("SECRET123"));
        assert!(!dbg.contains("deadbeef"));
        assert!(dbg.contains("[REDACTED]"));
    }
}

#[cfg(test)]
mod stdout_log_tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex as StdMutex;

    #[derive(Clone)]
    struct Capture(Arc<StdMutex<Vec<u8>>>);

    impl Write for Capture {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn malformed_stdout_never_logs_credentials() {
        let output = Arc::new(StdMutex::new(Vec::new()));
        let sink = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || Capture(sink.clone()))
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            for line in [
                r#"{"event":"tsnet:loopbackResult","data":{"proxyCredential":"PROXY-SECRET","localAPICredential":"LOCALAPI-SECRET""#,
                r#"{"event":"egress:proxyResult","data":{"credential":"CONNECT-SECRET""#,
                // Serde's Display can quote the invalid input, even if the
                // log message stops including the original line explicitly.
                r#""ROOT-CREDENTIAL""#,
            ] {
                assert!(GoSidecar::parse_stdout_event(line).is_none());
            }
            assert!(GoSidecar::parse_stdout_event(
                r#"{"event":"tsnet:loopbackResult","data":{"proxyCredential":"VALID-SECRET"}}"#
            )
            .is_some());
        });
        let logged = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(logged.contains("failed to parse sidecar event"));
        for secret in [
            "PROXY-SECRET",
            "LOCALAPI-SECRET",
            "CONNECT-SECRET",
            "ROOT-CREDENTIAL",
            "VALID-SECRET",
        ] {
            assert!(
                !logged.contains(secret),
                "credential leaked into stdout diagnostics: {logged}"
            );
        }
    }
}

#[cfg(test)]
mod status_check_tests {
    use super::*;

    #[test]
    fn watcher_gives_up_and_clears_pid_after_repeated_status_errors() {
        let slot = AtomicU32::new(4242);
        let mut errors = 0;
        for _ in 1..MAX_STATUS_CHECK_ERRORS {
            assert!(
                !GoSidecar::status_check_failed(&mut errors, &slot, 4242),
                "a transient error must not stop the watcher"
            );
            assert_eq!(slot.load(Ordering::Relaxed), 4242, "still watched");
        }
        assert!(GoSidecar::status_check_failed(&mut errors, &slot, 4242));
        assert_eq!(
            slot.load(Ordering::Relaxed),
            0,
            "an unwatched pid must not keep being reported"
        );
    }

    #[test]
    fn giving_up_on_an_older_child_keeps_the_newer_pid() {
        let slot = AtomicU32::new(7);
        let mut errors = MAX_STATUS_CHECK_ERRORS - 1;
        assert!(GoSidecar::status_check_failed(&mut errors, &slot, 4242));
        assert_eq!(slot.load(Ordering::Relaxed), 7);
    }
}

/// The pid slot's lifecycle, driven by stand-in child processes (`cat`
/// lives until killed; `true` exits at once) rather than a real sidecar.
#[cfg(all(test, unix))]
mod pid_tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn config(binary: &str) -> SidecarConfig {
        SidecarConfig {
            binary_path: PathBuf::from(binary),
            hostname: "truffle-test-pid".to_string(),
            state_dir: "unused".to_string(),
            auth_key: None,
            bridge_port: 0,
            session_token_hex: String::new(),
            ephemeral: None,
            tags: None,
            idle_timeout_secs: None,
        }
    }

    #[tokio::test]
    async fn pid_slot_holds_child_pid_until_shutdown() {
        let slot = Arc::new(AtomicU32::new(0));
        let (sidecar, _events) = GoSidecar::spawn(config("cat"), slot.clone()).await.unwrap();
        let pid = slot.load(Ordering::Relaxed);
        assert_ne!(pid, 0, "a running child must publish its pid");
        assert_eq!(pid, sidecar.pid);

        sidecar.shutdown().await;
        assert_eq!(slot.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn pid_slot_clears_when_child_exits() {
        let slot = Arc::new(AtomicU32::new(0));
        let (_sidecar, _events) = GoSidecar::spawn(config("true"), slot.clone())
            .await
            .unwrap();
        // The process watcher polls once per second.
        let deadline = Instant::now() + Duration::from_secs(10);
        while slot.load(Ordering::Relaxed) != 0 {
            assert!(Instant::now() < deadline, "exited child still reported");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[tokio::test]
    async fn dropping_an_older_sidecar_keeps_the_newer_pid() {
        let slot = Arc::new(AtomicU32::new(0));
        let (first, _first_events) = GoSidecar::spawn(config("cat"), slot.clone()).await.unwrap();
        let (second, _second_events) = GoSidecar::spawn(config("cat"), slot.clone()).await.unwrap();
        assert_eq!(slot.load(Ordering::Relaxed), second.pid);

        drop(first);
        assert_eq!(slot.load(Ordering::Relaxed), second.pid);

        drop(second);
        assert_eq!(slot.load(Ordering::Relaxed), 0);
    }
}
