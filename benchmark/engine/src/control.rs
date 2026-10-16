//! The peer control protocol (RFC 027 §6).
//!
//! All control traffic is `send_typed(peer.tailscale_id, "bench", msg_type,
//! json)`. This module holds:
//!
//! - the typed messages ([`HelloReq`]/[`HelloAck`], [`ClockReq`]/[`ClockAck`],
//!   [`PrepareReq`]/[`PrepareAck`], [`StartReq`]/[`StartAck`],
//!   [`FinishReq`]/[`FinishAck`], [`CancelMsg`], [`TickMsg`], [`FtDoneMsg`]);
//! - [`request`]: UUID ids, matching on `from` + `"<req>.ack"` + `id`, with
//!   `ReconnectBackoff` retried until the request's deadline;
//! - the clock exchange ([`estimate_clock`]: 16 samples, 10 ms apart);
//! - [`Slots`]: one data-plane run per hub in either role (rule 3) and the
//!   overlap count;
//! - [`Initiator`]: the data-plane identity check (rule 5);
//! - the per-run temp directories (rule 6) and [`Teardown`] (rule 7);
//! - the responder every hub runs (rules 1–4 and 8), which also reads
//!   usermetrics and samples resources around `prepare` … `finish`.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use schemars::JsonSchema;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast::error::RecvError;
use tokio::time::Instant;
use truffle_core::session::{SessionError, DIAL_CANCELLED_SHUTTING_DOWN};
use truffle_core::transport::quic::QuicListener;
use truffle_core::transport::RawIncoming;
use truffle_core::{NamespacedMessage, NodeError, Peer};

use crate::clock::{self, now_ns, ClockSample};
use crate::config::Tool;
use crate::dataplane::{random_run_tag, run_tag_hex};
use crate::error::{AppError, ErrorCode};
use crate::hub::{BenchNode, Core, Live};
use crate::localapi::UserMetrics;
use crate::params::{Workload, WorkloadParams};
use crate::report::validate_run_id;
use crate::resources::{ResourceSampler, ResourceTargets};
use crate::types::{
    is_admitted, is_tagged_login, ClockOffset, HubEvent, PathBytes, PeerHello, RemoteRunState,
    ResourceStats,
};
use crate::workloads::{self, ResponderCtx, ResponderRun};
use crate::CONTROL_NS;

/// `hello` timeout (§6).
pub const HELLO_TIMEOUT: Duration = Duration::from_secs(30);
/// `hello` is re-sent this often within [`HELLO_TIMEOUT`] until answered.
/// It is idempotent, and a message written to a WebSocket that died with
/// the peer (a peer restarted, or a device name reused) is lost without an
/// error; the re-send goes out on the redialled connection.
///
/// Defence in depth: core now replaces such a stale connection on the
/// accepting side as soon as the restarted peer dials again, so against a
/// peer on this core the first hello is answered and nothing is re-sent. The
/// re-send matters for peers on older cores. Its timer starts once a send
/// has gone out, so a slow dial never triggers one.
pub const HELLO_RESEND: Duration = Duration::from_secs(5);
/// `prepare` timeout.
pub const PREPARE_TIMEOUT: Duration = Duration::from_secs(30);
/// `start` timeout.
pub const START_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-sample `clock` timeout.
pub const CLOCK_SAMPLE_TIMEOUT: Duration = clock::SAMPLE_TIMEOUT;
/// `finish` timeout before the workload's drain allowance.
pub const FINISH_TIMEOUT: Duration = Duration::from_secs(30);
/// How long the responder waits for Layer 3 to list a sender (rule 1).
pub const PEER_LOOKUP_MS: u64 = 5_000;
/// How long admission waits for this node's own login while the hub still
/// looks for it (rule 1); with [`PEER_LOOKUP_MS`] well inside
/// [`PREPARE_TIMEOUT`].
pub const OWN_LOGIN_WAIT: Duration = Duration::from_secs(10);
/// A `cancel` for an unknown run is remembered this long (rule 4).
pub const TOMBSTONE_TTL: Duration = Duration::from_secs(5 * 60);
/// Ticks are sent at most this often (≤ 4 Hz).
pub const TICK_INTERVAL: Duration = Duration::from_millis(250);

/// Control message types (the envelope `msg_type`); replies append `.ack`.
pub mod msg_type {
    /// `hello` → `hello.ack`.
    pub const HELLO: &str = "hello";
    /// `clock` → `clock.ack`.
    pub const CLOCK: &str = "clock";
    /// `prepare` → `prepare.ack`.
    pub const PREPARE: &str = "prepare";
    /// `start` → `start.ack`.
    pub const START: &str = "start";
    /// `finish` → `finish.ack`.
    pub const FINISH: &str = "finish";
    /// `cancel` (no reply).
    pub const CANCEL: &str = "cancel";
    /// `tick` (responder → initiator, no reply).
    pub const TICK: &str = "tick";
    /// `ft.done` (responder → initiator, no reply).
    pub const FT_DONE: &str = "ft.done";
    /// Suffix of every reply type.
    pub const ACK_SUFFIX: &str = ".ack";

    /// `"<req>.ack"`.
    pub fn ack(req: &str) -> String {
        format!("{req}{ACK_SUFFIX}")
    }
}

/// A fresh request id: a random UUID v4.
pub fn new_request_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// The status every `.ack` carries: `ok`, plus `error`/`code` when not ok.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
pub struct AckStatus {
    /// Whether the request succeeded.
    pub ok: bool,
    /// Why it failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Machine-readable failure code.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<ErrorCode>,
}

impl AckStatus {
    /// `ok: true`.
    pub fn ok() -> Self {
        Self {
            ok: true,
            error: None,
            code: None,
        }
    }

    /// `ok: false` with the error's code and message.
    pub fn err(e: &AppError) -> Self {
        Self {
            ok: false,
            error: Some(e.message.clone()),
            code: Some(e.code),
        }
    }

    /// The error this status reports, if it is not ok.
    pub fn to_error(&self) -> Option<AppError> {
        (!self.ok).then(|| {
            AppError::new(
                self.code.unwrap_or(ErrorCode::Internal),
                self.error
                    .clone()
                    .unwrap_or_else(|| "the peer refused the request".to_string()),
            )
        })
    }
}

/// `hello`: who I am.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HelloReq {
    /// Request id.
    pub id: String,
    /// The asking hub's hello.
    pub hello: PeerHello,
}

/// `hello.ack`: who I am, as far as you may know. Always answered, across accounts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct HelloAck {
    /// Request id.
    pub id: String,
    /// Status.
    #[serde(flatten)]
    pub status: AckStatus,
    /// The responder's hello (`host_fingerprint` omitted unless admitted).
    pub hello: Option<PeerHello>,
}

/// `clock`: one NTP-style exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ClockReq {
    /// Request id.
    pub id: String,
    /// Initiator `now_ns()` at send (informational; the initiator keeps its own stamp).
    pub t1_ns: u64,
}

/// `clock.ack`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ClockAck {
    /// Request id.
    pub id: String,
    /// Status.
    #[serde(flatten)]
    pub status: AckStatus,
    /// Echo of the request's `t1_ns`.
    #[serde(default)]
    pub t1_ns: u64,
    /// Responder `now_ns()` on receipt.
    #[serde(default)]
    pub t2_ns: u64,
    /// Responder `now_ns()` just before replying.
    #[serde(default)]
    pub t3_ns: u64,
}

/// `prepare`: set up a run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PrepareReq {
    /// Request id.
    pub id: String,
    /// The initiator's run id (`^[A-Za-z0-9-]{1,64}$`).
    pub run_id: String,
    /// Params; validated again by the responder.
    pub params: WorkloadParams,
    /// The workload's `Client` struct (`{}` when unused).
    pub client: Value,
}

/// `prepare.ack`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct PrepareAck {
    /// Request id.
    pub id: String,
    /// Status.
    #[serde(flatten)]
    pub status: AckStatus,
    /// Echo of the run id.
    pub run_id: String,
    /// 64 random bits chosen by the responder.
    #[serde(default)]
    pub run_tag: u64,
    /// Params as validated and used by the responder.
    #[serde(default)]
    pub params: Option<WorkloadParams>,
    /// The workload's `Setup` struct.
    #[serde(default)]
    pub setup: Value,
    /// Other runs active on the responder's hub right now.
    #[serde(default)]
    pub overlapping_runs: u32,
}

/// `start`: begin the data phase (e.g. udp download).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StartReq {
    /// Request id.
    pub id: String,
    /// Run id.
    pub run_id: String,
}

/// `start.ack`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StartAck {
    /// Request id.
    pub id: String,
    /// Status.
    #[serde(flatten)]
    pub status: AckStatus,
    /// Run id.
    pub run_id: String,
}

/// `finish`: the initiator is done; collect the responder's numbers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FinishReq {
    /// Request id.
    pub id: String,
    /// Run id.
    pub run_id: String,
    /// The workload's `Info` struct (sender-side counts).
    pub info: Value,
}

/// `finish.ack`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct FinishAck {
    /// Request id.
    pub id: String,
    /// Status.
    #[serde(flatten)]
    pub status: AckStatus,
    /// Run id.
    pub run_id: String,
    /// The workload's `Stats` struct (receiver-side numbers).
    #[serde(default)]
    pub stats: Value,
    /// The responder's CPU and memory.
    #[serde(default)]
    pub resources: Option<ResourceStats>,
    /// The responder's usermetrics byte deltas.
    #[serde(default)]
    pub path_bytes: Option<PathBytes>,
    /// Other runs that shared the responder's hub during this one.
    #[serde(default)]
    pub max_overlapping_runs: u32,
}

/// `cancel`: tear a run down (no reply).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CancelMsg {
    /// Run id.
    pub run_id: String,
}

/// `tick`: live receiver-side numbers (responder → initiator, ≤ 4 Hz, no reply).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TickMsg {
    /// Run id.
    pub run_id: String,
    /// Responder `now_ns()`.
    pub t_ns: u64,
    /// Values by sample key (e.g. `goodput_mbps`).
    pub values: BTreeMap<String, f64>,
}

/// `ft.done`: the responder saw a file-transfer iteration end (no reply).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct FtDoneMsg {
    /// Run id.
    pub run_id: String,
    /// Iteration, from 1.
    pub iteration: u32,
    /// `Completed` (true) or `Failed` (false).
    pub ok: bool,
    /// Bytes received.
    pub bytes: u64,
    /// SHA-256 of the received file (hex).
    pub sha256: Option<String>,
    /// Why it failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// A request type and its reply.
pub trait ControlRequest: Serialize {
    /// The envelope `msg_type`.
    const TYPE: &'static str;
    /// The reply payload.
    type Ack: DeserializeOwned;
    /// The request id.
    fn id(&self) -> &str;
}

macro_rules! control_request {
    ($req:ty, $ty:expr, $ack:ty) => {
        impl ControlRequest for $req {
            const TYPE: &'static str = $ty;
            type Ack = $ack;
            fn id(&self) -> &str {
                &self.id
            }
        }
    };
}

control_request!(HelloReq, msg_type::HELLO, HelloAck);
control_request!(ClockReq, msg_type::CLOCK, ClockAck);
control_request!(PrepareReq, msg_type::PREPARE, PrepareAck);
control_request!(StartReq, msg_type::START, StartAck);
control_request!(FinishReq, msg_type::FINISH, FinishAck);

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/// A reply plus when the request left and the reply arrived (`now_ns()`).
#[derive(Debug, Clone)]
pub struct Received<A> {
    /// The reply (its status was ok).
    pub ack: A,
    /// Just before the successful `send_typed`.
    pub sent_ns: u64,
    /// When the matching reply was taken off the subscription.
    pub recv_ns: u64,
}

/// Pause before re-sending after a WebSocket dial failed, or a fresh
/// connection was dropped.
const CONNECT_RETRY: Duration = Duration::from_millis(250);

/// Send `payload` as `msg_type` to `peer_id` until `deadline`, retrying
/// `SessionError::ReconnectBackoff` after its `retry_after`, and every
/// 250 ms `SessionError::ConnectFailed` and `SessionError::SendFailed`, and
/// never waiting on a full outbound queue past `deadline`. Returns
/// `now_ns()` just before the successful send. Errors carry no `msg_type`
/// prefix; callers add it.
///
/// The two retries are defence in depth. Older cores failed a send when two
/// hubs dialled each other at once ("connection already in progress"), and
/// a peer that kept its WebSocket to an earlier instance of this node (one
/// that reused this device name) dropped ours ("connection missing after
/// connect") until its 10 s ping cleared the dead one. Core now shares one
/// dial per peer among its callers and installs the new connection on the
/// accepting side, so against a peer on this core they fire only on a
/// genuine failure; they still cover peers on older cores. A dial that core
/// cancelled because the node is shutting down is not retried.
pub async fn send_with_backoff(
    node: &BenchNode,
    peer_id: &str,
    msg_type: &str,
    payload: &Value,
    deadline: Instant,
) -> Result<u64, AppError> {
    loop {
        let sent_ns = now_ns();
        // `send_typed` waits while the peer's outbound queue is full (e.g.
        // behind a ws stream's backlog); the deadline bounds that too.
        let sent = tokio::time::timeout_at(
            deadline,
            node.send_typed(peer_id, CONTROL_NS, msg_type, payload),
        )
        .await
        .map_err(|_| {
            AppError::timeout("the connection to the peer stayed busy past the deadline")
        })?;
        let (wait, why) = match sent {
            Ok(()) => return Ok(sent_ns),
            Err(NodeError::Session(SessionError::ReconnectBackoff { retry_after })) => (
                retry_after.max(Duration::from_millis(10)),
                format!(
                    "the connection to the peer is backing off (retry in {} ms)",
                    retry_after.as_millis()
                ),
            ),
            Err(NodeError::Session(SessionError::ConnectFailed(m))) => {
                if is_shutdown_cancel(&m) {
                    return Err(AppError::not_running());
                }
                (CONNECT_RETRY, format!("could not connect to the peer: {m}"))
            }
            Err(NodeError::Session(SessionError::SendFailed(m))) => {
                tracing::debug!(%msg_type, error = %m, "send failed; retrying");
                (CONNECT_RETRY, format!("could not send to the peer: {m}"))
            }
            Err(e) => return Err(AppError::from(e)),
        };
        let wake = Instant::now() + wait;
        if wake >= deadline {
            return Err(AppError::timeout(why));
        }
        tokio::time::sleep_until(wake).await;
    }
}

/// Core's `ConnectFailed` message for a dial it cancelled because the node
/// is shutting down: no retry can succeed. Matched exactly — other
/// `ConnectFailed` messages carry strings the remote peer picks (its
/// claimed id, its app id), so a substring match would let a peer stop our
/// retries and blame the local hub.
fn is_shutdown_cancel(message: &str) -> bool {
    message == DIAL_CANCELLED_SHUTTING_DOWN
}

/// Send a request and wait for its reply (§6).
///
/// Subscribes to `bench` before sending, then takes the first message with
/// `from == peer_id`, `msg_type == "<req>.ack"` and `payload.id == req.id()`.
/// A reply with `ok: false` becomes its `code`/`error` as an [`AppError`].
/// `timeout` bounds the whole exchange, including backoff retries.
pub async fn request<R: ControlRequest>(
    node: &BenchNode,
    peer_id: &str,
    req: &R,
    timeout: Duration,
) -> Result<Received<R::Ack>, AppError> {
    request_with(node, peer_id, req, timeout, None)
        .await
        .map_err(AppError::from)
}

/// How a [`request_or_refusal`] failed.
#[derive(Debug)]
pub enum RequestFailure {
    /// The peer answered, with `ok: false`: its code and reason.
    Refused(AppError),
    /// No answer to act on: this side could not send (its own registry does
    /// not know the peer, say), the reply never came, or it did not parse.
    Failed(AppError),
}

impl From<RequestFailure> for AppError {
    fn from(failure: RequestFailure) -> Self {
        match failure {
            RequestFailure::Refused(e) | RequestFailure::Failed(e) => e,
        }
    }
}

/// [`request`], telling the peer's refusal apart from a failure on this
/// side: both can carry the same code (`peer-not-found` is the peer saying
/// "I do not know you" or this node not knowing the peer).
pub async fn request_or_refusal<R: ControlRequest>(
    node: &BenchNode,
    peer_id: &str,
    req: &R,
    timeout: Duration,
) -> Result<Received<R::Ack>, RequestFailure> {
    request_with(node, peer_id, req, timeout, None).await
}

/// [`request`] for an idempotent request, re-sent (same id) every `every`
/// until a reply arrives or `timeout` passes. `sent_ns` is the last send's.
pub async fn request_resending<R: ControlRequest>(
    node: &BenchNode,
    peer_id: &str,
    req: &R,
    timeout: Duration,
    every: Duration,
) -> Result<Received<R::Ack>, AppError> {
    request_with(node, peer_id, req, timeout, Some(every))
        .await
        .map_err(AppError::from)
}

async fn request_with<R: ControlRequest>(
    node: &BenchNode,
    peer_id: &str,
    req: &R,
    timeout: Duration,
    resend_every: Option<Duration>,
) -> Result<Received<R::Ack>, RequestFailure> {
    use RequestFailure::Failed;
    let deadline = Instant::now() + timeout;
    let payload = serde_json::to_value(req)
        .map_err(|e| Failed(AppError::internal(format!("serialise {}: {e}", R::TYPE))))?;
    let ack_type = msg_type::ack(R::TYPE);
    let mut rx = node.subscribe(CONTROL_NS);
    let mut sent_ns = send_with_backoff(node, peer_id, R::TYPE, &payload, deadline)
        .await
        .map_err(Failed)?;
    let next_send = |from: Instant| resend_every.map_or(deadline, |e| (from + e).min(deadline));
    let mut resend_at = next_send(Instant::now());
    loop {
        let msg = match tokio::time::timeout_at(resend_at, rx.recv()).await {
            Err(_) if Instant::now() < deadline => {
                // No reply yet: send again. A send that cannot get out before
                // the deadline ends the request with that send's error.
                sent_ns = send_with_backoff(node, peer_id, R::TYPE, &payload, deadline)
                    .await
                    .map_err(Failed)?;
                tracing::debug!(%ack_type, "no reply yet; request re-sent");
                resend_at = next_send(Instant::now());
                continue;
            }
            Err(_) => {
                return Err(Failed(AppError::timeout(format!(
                    "no {ack_type} from the peer within {} s",
                    timeout.as_secs()
                ))))
            }
            Ok(Err(RecvError::Lagged(_))) => continue,
            Ok(Err(RecvError::Closed)) => return Err(Failed(AppError::not_running())),
            Ok(Ok(msg)) => msg,
        };
        if msg.from != peer_id
            || msg.msg_type != ack_type
            || msg.payload.get("id").and_then(Value::as_str) != Some(req.id())
        {
            continue;
        }
        let recv_ns = now_ns();
        let status: AckStatus = serde_json::from_value(msg.payload.clone()).unwrap_or_default();
        if let Some(err) = status.to_error() {
            return Err(RequestFailure::Refused(err));
        }
        let ack = serde_json::from_value(msg.payload).map_err(|e| {
            Failed(AppError::new(
                ErrorCode::ProtocolMismatch,
                format!("the peer's {ack_type} did not parse: {e}"),
            ))
        })?;
        return Ok(Received {
            ack,
            sent_ns,
            recv_ns,
        });
    }
}

/// Send a no-reply message (`cancel`, `tick`, `ft.done`), retrying backoff
/// for up to `timeout`.
pub async fn notify(
    node: &BenchNode,
    peer_id: &str,
    msg_type: &str,
    payload: &impl Serialize,
    timeout: Duration,
) -> Result<(), AppError> {
    let payload = serde_json::to_value(payload)
        .map_err(|e| AppError::internal(format!("serialise {msg_type}: {e}")))?;
    send_with_backoff(node, peer_id, msg_type, &payload, Instant::now() + timeout)
        .await
        .map(|_| ())
        .map_err(|e| e.context(msg_type))
}

/// One `hello` exchange: send ours, get theirs. The request is re-sent
/// every [`HELLO_RESEND`] within [`HELLO_TIMEOUT`] until answered.
pub async fn hello(
    node: &BenchNode,
    peer_id: &str,
    ours: &PeerHello,
) -> Result<PeerHello, AppError> {
    let req = HelloReq {
        id: new_request_id(),
        hello: ours.clone(),
    };
    let got = request_resending(node, peer_id, &req, HELLO_TIMEOUT, HELLO_RESEND)
        .await
        .map_err(|e| e.context("hello"))?;
    got.ack.hello.map(PeerHello::bounded).ok_or_else(|| {
        AppError::new(
            ErrorCode::ProtocolMismatch,
            "hello: the peer's hello.ack carried no hello",
        )
    })
}

/// One `clock` exchange: t1/t4 are this side's send and receipt stamps.
pub async fn clock_sample(node: &BenchNode, peer_id: &str) -> Result<ClockSample, AppError> {
    let req = ClockReq {
        id: new_request_id(),
        t1_ns: now_ns(),
    };
    let got = request(node, peer_id, &req, CLOCK_SAMPLE_TIMEOUT).await?;
    Ok(ClockSample {
        t1_ns: got.sent_ns,
        t2_ns: got.ack.t2_ns,
        t3_ns: got.ack.t3_ns,
        t4_ns: got.recv_ns,
    })
}

/// §6 Clock: 16 exchanges 10 ms apart; the one with the smallest delay wins.
pub async fn estimate_clock(node: &BenchNode, peer_id: &str) -> Result<ClockOffset, AppError> {
    clock::estimate(clock::DEFAULT_SAMPLES, clock::DEFAULT_SPACING, || {
        clock_sample(node, peer_id)
    })
    .await
    .map_err(|e| e.context("clock"))
}

// ---------------------------------------------------------------------------
// Rule 3: one data-plane run per hub, and the overlap count
// ---------------------------------------------------------------------------

/// Which side of a data-plane run this hub is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRole {
    /// This hub started the run.
    Initiating,
    /// A peer is measuring this hub.
    Responding,
}

/// Who holds the hub's data-plane slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotHolder {
    /// Initiator or responder.
    pub role: SlotRole,
    /// Run id.
    pub run_id: String,
    /// The other side's device name.
    pub peer_name: String,
    /// Workload.
    pub workload: Workload,
}

impl SlotHolder {
    /// "measuring studio · tcp" or "being measured by studio · tcp"
    /// (`PeerHello.busy`).
    pub fn describe(&self) -> String {
        match self.role {
            SlotRole::Initiating => format!("measuring {} · {}", self.peer_name, self.workload),
            SlotRole::Responding => {
                format!("being measured by {} · {}", self.peer_name, self.workload)
            }
        }
    }
}

#[derive(Debug, Default)]
struct SlotsInner {
    data: Option<(u64, SlotHolder)>,
    runs: HashMap<u64, Arc<AtomicU32>>,
    next: u64,
}

/// The hub's run bookkeeping (§6 rule 3).
///
/// At most one data-plane run per hub, in either role; `path` and probes are
/// exempt. Every run (data or exempt) holds a [`RunToken`]; each token counts
/// the other runs that were active on this hub at any time during its life,
/// which becomes `overlapping_runs`.
#[derive(Debug, Default)]
pub struct Slots {
    inner: Mutex<SlotsInner>,
}

impl Slots {
    /// Empty bookkeeping.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SlotsInner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn register(self: &Arc<Self>, inner: &mut SlotsInner, data: Option<SlotHolder>) -> RunToken {
        inner.next += 1;
        let id = inner.next;
        for c in inner.runs.values() {
            c.fetch_add(1, Ordering::Relaxed);
        }
        let overlap = Arc::new(AtomicU32::new(inner.runs.len() as u32));
        inner.runs.insert(id, overlap.clone());
        let is_data = data.is_some();
        if let Some(holder) = data {
            inner.data = Some((id, holder));
        }
        RunToken {
            slots: self.clone(),
            id,
            data: is_data,
            overlap,
        }
    }

    /// Take the data-plane slot. Refused with `busy` when this hub already
    /// runs or serves a data-plane run, or with `being-measured` when it is
    /// asked to initiate while a peer measures it.
    pub fn acquire_data(self: &Arc<Self>, holder: SlotHolder) -> Result<RunToken, AppError> {
        let mut inner = self.lock();
        if let Some((_, current)) = &inner.data {
            let err = match (holder.role, current.role) {
                (SlotRole::Initiating, SlotRole::Responding) => AppError::new(
                    ErrorCode::BeingMeasured,
                    format!(
                        "this device is being measured by {} ({})",
                        current.peer_name, current.workload
                    ),
                ),
                _ => AppError::new(ErrorCode::Busy, format!("busy: {}", current.describe())),
            };
            return Err(err);
        }
        Ok(self.register(&mut inner, Some(holder)))
    }

    /// Register an exempt run (path, probe): never refused.
    pub fn acquire_exempt(self: &Arc<Self>) -> RunToken {
        let mut inner = self.lock();
        self.register(&mut inner, None)
    }

    /// The current data-plane holder.
    pub fn holder(&self) -> Option<SlotHolder> {
        self.lock().data.as_ref().map(|(_, h)| h.clone())
    }

    /// `PeerHello.busy`: what holds the slot, if anything.
    pub fn busy(&self) -> Option<String> {
        self.holder().map(|h| h.describe())
    }

    /// Runs (data and exempt) active right now.
    pub fn active(&self) -> u32 {
        self.lock().runs.len() as u32
    }

    fn release(&self, id: u64) {
        let mut inner = self.lock();
        inner.runs.remove(&id);
        if inner.data.as_ref().is_some_and(|(d, _)| *d == id) {
            inner.data = None;
        }
    }
}

/// A run's place in [`Slots`]; dropping it frees the slot.
#[derive(Debug)]
pub struct RunToken {
    slots: Arc<Slots>,
    id: u64,
    data: bool,
    overlap: Arc<AtomicU32>,
}

impl RunToken {
    /// Other runs that were active on this hub at some time during this one.
    pub fn overlapping(&self) -> u32 {
        self.overlap.load(Ordering::Relaxed)
    }

    /// Whether this token holds the data-plane slot.
    pub fn is_data(&self) -> bool {
        self.data
    }
}

impl Drop for RunToken {
    fn drop(&mut self) {
        self.slots.release(self.id);
    }
}

// ---------------------------------------------------------------------------
// Rule 1: admission; rule 5: data-plane identity
// ---------------------------------------------------------------------------

/// Admission (§6 rule 1) of `peer` by a hub signed in as `local_login`.
pub fn admit(
    local_login: Option<&str>,
    peer: &Peer,
    allow_other_logins: bool,
    allow_tagged: bool,
) -> Result<(), AppError> {
    if is_admitted(
        local_login,
        peer.login_name.as_deref(),
        allow_other_logins,
        allow_tagged,
    ) {
        return Ok(());
    }
    let theirs = peer.login_name.as_deref().unwrap_or("an unknown account");
    let ours = local_login.unwrap_or("an unknown account");
    Err(AppError::new(
        ErrorCode::LoginRefused,
        format!(
            "this device ({ours}) does not accept runs from {} ({theirs})",
            peer.display_name
        ),
    ))
}

/// Rule 1 as this hub applies it: [`admit`] with the hub's settings, and a
/// refusal that says how to change them in this tool.
///
/// A tagged device (one signed in with an auth key, now or in an earlier
/// session whose state it restarted from) accepts other tagged devices:
/// `allow_tagged` follows the session's login, not only whether a key was
/// passed this time.
pub(crate) fn admit_on(core: &Core, live: &Live, peer: &Peer) -> Result<(), AppError> {
    let local = live.local_login();
    let allow_tagged = core.cfg.allow_tagged || is_tagged_login(local.as_deref());
    admit(
        local.as_deref(),
        peer,
        core.allow_other_logins(),
        allow_tagged,
    )
    .map_err(|mut e| {
        e.message.push_str(match core.cfg.tool {
            Tool::App => "; turn on Settings → “Accept runs from other accounts” on that device",
            Tool::Cli => "; start it with --allow-other-logins",
        });
        e
    })
}

/// The other side of a run as the data plane checks it (§6 rule 5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Initiator {
    /// Tailscale stable node id: the only peer key.
    pub tailscale_id: String,
    /// Tailnet IP.
    pub ip: IpAddr,
    /// Durable ULID, when hello completed.
    pub device_id: Option<String>,
    /// Device (display) name.
    pub device_name: String,
    /// Owner login.
    pub login_name: Option<String>,
}

impl Initiator {
    /// From a resolved core [`Peer`].
    pub fn from_peer(peer: &Peer) -> Self {
        Self {
            tailscale_id: peer.tailscale_id.clone(),
            ip: peer.ip,
            device_id: peer.device_id.clone(),
            device_name: peer
                .device_name
                .clone()
                .unwrap_or_else(|| peer.display_name.clone()),
            login_name: peer.login_name.clone(),
        }
    }

    /// Whether an IP is this peer's.
    pub fn is_ip(&self, ip: IpAddr) -> bool {
        ip == self.ip
    }

    /// Whether an address string (`"100.x.y.z:port"`) is this peer's. An
    /// unparsable or empty value is not.
    pub fn is_addr(&self, addr: &str) -> bool {
        addr.parse::<SocketAddr>()
            .map(|a| a.ip())
            .or_else(|_| addr.parse::<IpAddr>())
            .is_ok_and(|ip| self.is_ip(ip))
    }

    /// Rule 5 for an accepted TCP connection: kept iff the WhoIs node id is
    /// this peer's, or (without an identity) its remote IP is.
    pub fn accepts_tcp(&self, incoming: &RawIncoming) -> bool {
        match incoming
            .remote_identity
            .as_ref()
            .and_then(|i| i.node_id.as_deref())
        {
            Some(node_id) => node_id == self.tailscale_id,
            None => self.is_addr(&incoming.remote_addr),
        }
    }
}

// ---------------------------------------------------------------------------
// Rule 6: per-run temp directories
// ---------------------------------------------------------------------------

/// `<data_dir>/tmp/in-<run_tag_hex>/`: where a responder receives files.
pub fn in_dir(data_dir: &Path, run_tag: u64) -> PathBuf {
    data_dir
        .join("tmp")
        .join(format!("in-{}", run_tag_hex(run_tag)))
}

/// `<data_dir>/tmp/out-<run_tag_hex>/`: where an initiator writes source files.
pub fn out_dir(data_dir: &Path, run_tag: u64) -> PathBuf {
    data_dir
        .join("tmp")
        .join(format!("out-{}", run_tag_hex(run_tag)))
}

/// Create a run directory (and parents).
pub fn create_run_dir(dir: &Path) -> Result<(), AppError> {
    std::fs::create_dir_all(dir).map_err(|e| AppError::io(format!("create {}", dir.display()), &e))
}

/// Remove a run directory and everything in it; a missing one is fine.
/// Only `in-…`/`out-…` directories under a `tmp` parent are ever removed.
pub fn remove_run_dir(dir: &Path) {
    let name_ok = dir
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with("in-") || n.starts_with("out-"));
    let parent_ok = dir
        .parent()
        .and_then(Path::file_name)
        .is_some_and(|n| n == "tmp");
    if !(name_ok && parent_ok) {
        tracing::warn!(dir = %dir.display(), "refusing to remove a directory that is not a run dir");
        return;
    }
    match std::fs::remove_dir_all(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => tracing::warn!(dir = %dir.display(), error = %e, "could not remove run dir"),
    }
}

// ---------------------------------------------------------------------------
// Rule 7: teardown order
// ---------------------------------------------------------------------------

type StopFn = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

/// Resources a run holds, released in the order of §6 rule 7:
///
/// 1. per-stream acks (the workload sends these itself before [`run`](Self::run));
/// 2. `QuicListener::close()`;
/// 3. `unlisten_tcp(port)` for every `listen_tcp`;
/// 4. `egress_remove`;
/// 5. `proxy().remove`;
/// 6. `SyncedStore::stop()`;
/// 7. drop sockets (anything given to [`hold`](Self::hold)).
///
/// `Node::stop()` runs only after every run is torn down; the hub enforces that.
pub struct Teardown {
    node: Arc<BenchNode>,
    quic: Vec<QuicListener>,
    tcp_ports: Vec<u16>,
    egress: Vec<String>,
    proxies: Vec<String>,
    stores: Vec<StopFn>,
    held: Vec<Box<dyn Any + Send>>,
}

impl std::fmt::Debug for Teardown {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Teardown")
            .field("quic", &self.quic.len())
            .field("tcp_ports", &self.tcp_ports)
            .field("egress", &self.egress)
            .field("proxies", &self.proxies)
            .field("stores", &self.stores.len())
            .field("held", &self.held.len())
            .finish()
    }
}

impl Teardown {
    /// Nothing to release yet.
    pub fn new(node: Arc<BenchNode>) -> Self {
        Self {
            node,
            quic: Vec::new(),
            tcp_ports: Vec::new(),
            egress: Vec::new(),
            proxies: Vec::new(),
            stores: Vec::new(),
            held: Vec::new(),
        }
    }

    /// Close this QUIC listener (step 2).
    pub fn quic_listener(&mut self, listener: QuicListener) {
        self.quic.push(listener);
    }

    /// `unlisten_tcp(port)` (step 3).
    pub fn tcp_port(&mut self, port: u16) {
        self.tcp_ports.push(port);
    }

    /// `egress_remove(id)` (step 4).
    pub fn egress(&mut self, id: impl Into<String>) {
        self.egress.push(id.into());
    }

    /// `proxy().remove(id)` (step 5).
    pub fn proxy(&mut self, id: impl Into<String>) {
        self.proxies.push(id.into());
    }

    /// `SyncedStore::stop()` (step 6).
    pub fn store<T>(&mut self, store: Arc<truffle_core::SyncedStore<T>>)
    where
        T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    {
        self.stores.push(Box::new(move || {
            Box::pin(async move { store.stop().await })
        }));
    }

    /// Keep a socket or task handle alive until the end (step 7: dropped last).
    pub fn hold<T: Send + 'static>(&mut self, value: T) {
        self.held.push(Box::new(value));
    }

    /// Release everything, in order. Errors are logged, never returned.
    pub async fn run(self) {
        for l in &self.quic {
            l.close();
        }
        for port in self.tcp_ports {
            if let Err(e) = self.node.unlisten_tcp(port).await {
                tracing::debug!(port, error = %e, "teardown: unlisten_tcp");
            }
        }
        for id in self.egress {
            if let Err(e) = self.node.egress_remove(&id).await {
                tracing::debug!(id, error = %e, "teardown: egress_remove");
            }
        }
        for id in self.proxies {
            if let Err(e) = self.node.proxy().remove(&id).await {
                tracing::debug!(id, error = %e, "teardown: proxy remove");
            }
        }
        for stop in self.stores {
            stop().await;
        }
        drop(self.quic);
        drop(self.held);
    }
}

// ---------------------------------------------------------------------------
// The responder (rules 1–4, 8)
// ---------------------------------------------------------------------------

/// `(initiator tailscale_id, run_id)`: run state is keyed by both (rule 2).
pub type RunKey = (String, String);

/// A run a peer prepared on this hub.
pub(crate) struct RemoteRun {
    key: RunKey,
    workload: Workload,
    peer_name: String,
    ctx: ResponderCtx,
    run: tokio::sync::Mutex<Option<Box<dyn ResponderRun>>>,
    token: Mutex<Option<RunToken>>,
    watchdog: Mutex<Option<tokio::task::AbortHandle>>,
    metrics_before: Option<UserMetrics>,
    sampler: Mutex<Option<ResourceSampler>>,
    started_ns: AtomicU64,
}

impl RemoteRun {
    fn take_token(&self) -> Option<RunToken> {
        self.token
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn overlapping(&self) -> u32 {
        self.token
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map_or(0, RunToken::overlapping)
    }

    /// Forget the watchdog's handle, returning it.
    fn disarm_watchdog(&self) -> Option<tokio::task::AbortHandle> {
        self.watchdog
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    fn abort_watchdog(&self) {
        if let Some(h) = self.disarm_watchdog() {
            h.abort();
        }
    }

    fn take_sampler(&self) -> Option<ResourceSampler> {
        self.sampler
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

/// A `finish` the responder took, for duplicates of it (a re-sent request,
/// or a retry with a new id): they get the first one's outcome instead of
/// "no run …", and the handler never runs twice.
struct FinishRecord {
    /// The first finish's request id.
    id: String,
    /// Its ack, once known.
    ack: tokio::sync::watch::Receiver<Option<FinishAck>>,
    /// Forgotten after this; `None` while the first finish runs.
    expires: Option<Instant>,
}

impl FinishRecord {
    fn live(&self, now: Instant) -> bool {
        match self.expires {
            Some(t) => t > now,
            // Running, unless its handler went away without an outcome.
            None => self.ack.borrow().is_some() || self.ack.has_changed().is_ok(),
        }
    }
}

/// Runs prepared on this hub, plus cancel tombstones and finished runs.
#[derive(Default)]
pub(crate) struct RemoteRuns {
    runs: HashMap<RunKey, Arc<RemoteRun>>,
    tombstones: HashMap<RunKey, Instant>,
    finished: HashMap<RunKey, FinishRecord>,
}

impl RemoteRuns {
    fn prune(&mut self) {
        let now = Instant::now();
        self.tombstones.retain(|_, until| *until > now);
        self.finished.retain(|_, r| r.live(now));
    }

    fn tombstoned(&mut self, key: &RunKey) -> bool {
        self.prune();
        self.tombstones.contains_key(key)
    }

    /// Every prepared run's key.
    pub(crate) fn keys(&self) -> Vec<RunKey> {
        self.runs.keys().cloned().collect()
    }
}

fn remote_runs(core: &Core) -> std::sync::MutexGuard<'_, RemoteRuns> {
    core.remote.lock().unwrap_or_else(PoisonError::into_inner)
}

fn take_run(core: &Core, key: &RunKey) -> Option<Arc<RemoteRun>> {
    remote_runs(core).runs.remove(key)
}

fn emit_remote(core: &Core, run: &RemoteRun, state: RemoteRunState) {
    core.emit(HubEvent::RemoteRun {
        run_id: run.key.1.clone(),
        peer: run.peer_name.clone(),
        peer_id: run.key.0.clone(),
        workload: run.workload.as_str().to_string(),
        state,
    });
}

/// Tear a prepared run down without `finish` (cancel, watchdog, hub stop):
/// cancel the workload, free the slot, remove its directory, announce it.
pub(crate) async fn abort_remote(core: &Core, key: &RunKey, state: RemoteRunState) -> bool {
    let Some(run) = take_run(core, key) else {
        return false;
    };
    run.abort_watchdog();
    run.ctx.cancel_token().cancel();
    let workload = run.run.lock().await.take();
    if let Some(w) = workload {
        w.cancel().await;
    }
    drop(run.take_sampler());
    drop(run.take_token());
    remove_run_dir(&run.ctx.in_dir_path());
    emit_remote(core, &run, state);
    true
}

/// Rule 4: tear the run down when its watchdog expires (the initiator
/// vanished without `finish` or `cancel`). The expiry lives in the run's
/// [`ResponderCtx`] so a workload can push it out on progress
/// (file-transfer, per iteration). The task ends quietly when the run is
/// torn down any other way (its token is cancelled by finish, cancel and
/// hub stop).
fn arm_watchdog(core: &Arc<Core>, live: &Live, run: &Arc<RemoteRun>) {
    let core = core.clone();
    let key = run.key.clone();
    let ctx = run.ctx.clone();
    let handle = live.tracker.spawn(async move {
        let cancel = ctx.cancel_token();
        loop {
            let at = ctx.watchdog_at();
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tokio::time::sleep_until(at) => {}
            }
            if Instant::now() >= ctx.watchdog_at() {
                break;
            }
        }
        tracing::warn!(run_id = %key.1, "responder: watchdog expired; tearing the run down");
        // Disarm first: abort_remote() aborts the run's watchdog, and that is
        // this task; aborted, it would stop at the teardown's first real
        // await (a QUIC accept join, the synced-store echo join, a proxy
        // removal's reply) and skip the rest of rule 7 and the `expired`
        // event (rule 8).
        let run = remote_runs(&core).runs.get(&key).cloned();
        if let Some(run) = run {
            drop(run.disarm_watchdog());
        }
        abort_remote(&core, &key, RemoteRunState::Expired).await;
    });
    *run.watchdog.lock().unwrap_or_else(PoisonError::into_inner) = Some(handle.abort_handle());
}

/// Requests the responder handles at once, from all peers…
const MAX_INFLIGHT: usize = 64;
/// …and from one peer: a run has one request in flight at a time, plus
/// perhaps a hello, a re-sent request or a cancel.
const MAX_INFLIGHT_PER_PEER: usize = 4;
/// A request whose decoded payload is larger than this (roughly) is dropped
/// unread; the largest legitimate one, a udp `finish`, is a few KiB.
const MAX_REQUEST_BYTES: usize = 256 * 1024;
/// What one JSON node costs in [`within_budget`] (`size_of::<Value>()`).
const VALUE_NODE_BYTES: usize = 32;

/// Whether `v` fits in about `budget` bytes once decoded: every node costs
/// [`VALUE_NODE_BYTES`], strings and keys their length. Stops as soon as
/// the budget is spent, so the walk costs O(budget), not O(payload).
fn within_budget(v: &Value, budget: usize) -> bool {
    let mut left = budget;
    let mut charge = |n: usize| match left.checked_sub(n) {
        Some(l) => {
            left = l;
            true
        }
        None => false,
    };
    if !charge(VALUE_NODE_BYTES) {
        return false;
    }
    let mut stack = vec![v];
    while let Some(v) = stack.pop() {
        match v {
            Value::String(s) if !charge(s.len()) => return false,
            Value::Array(a) => {
                if !charge(a.len().saturating_mul(VALUE_NODE_BYTES)) {
                    return false;
                }
                stack.extend(a.iter());
            }
            Value::Object(o) => {
                if !charge(o.len().saturating_mul(VALUE_NODE_BYTES)) {
                    return false;
                }
                for (k, v) in o {
                    if !charge(k.len()) {
                        return false;
                    }
                    stack.push(v);
                }
            }
            _ => {}
        }
    }
    true
}

/// The responder's bounds on concurrent requests, kept for the hub's life.
#[derive(Debug)]
pub(crate) struct ResponderLimits {
    inflight: Arc<tokio::sync::Semaphore>,
    per_peer: Arc<Mutex<HashMap<String, usize>>>,
    /// Senders Layer 3 does not list yet, each with one 5 s lookup at most.
    unresolved: Mutex<std::collections::HashSet<String>>,
}

impl Default for ResponderLimits {
    fn default() -> Self {
        Self {
            inflight: Arc::new(tokio::sync::Semaphore::new(MAX_INFLIGHT)),
            per_peer: Arc::default(),
            unresolved: Mutex::default(),
        }
    }
}

/// One request's place in [`ResponderLimits`]; released on drop.
struct InflightSlot {
    per_peer: Arc<Mutex<HashMap<String, usize>>>,
    peer: String,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl ResponderLimits {
    /// A slot for a request from `peer`, or `None` when the responder (or
    /// that peer) already has as many requests in flight as it allows.
    fn admit(&self, peer: &str) -> Option<InflightSlot> {
        let permit = self.inflight.clone().try_acquire_owned().ok()?;
        let mut per_peer = self.per_peer.lock().unwrap_or_else(PoisonError::into_inner);
        let n = per_peer.entry(peer.to_string()).or_insert(0);
        if *n >= MAX_INFLIGHT_PER_PEER {
            return None;
        }
        *n += 1;
        Some(InflightSlot {
            per_peer: self.per_peer.clone(),
            peer: peer.to_string(),
            _permit: permit,
        })
    }
}

impl Drop for InflightSlot {
    fn drop(&mut self) {
        let mut per_peer = self.per_peer.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(n) = per_peer.get_mut(&self.peer) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                per_peer.remove(&self.peer);
            }
        }
    }
}

/// The responder task: one subscription to `bench` for the hub's life. Every
/// request is handled on its own tracked task, within [`ResponderLimits`]
/// (a request over the limits, or too large, is dropped unanswered: an
/// honest initiator never gets near them, and its idempotent requests are
/// re-sent); `*.ack`, `tick` and `ft.done` belong to this hub's own runs
/// and are ignored here.
pub(crate) async fn responder_loop(core: Arc<Core>, live: Arc<Live>) {
    let mut rx = live.node.subscribe(CONTROL_NS);
    let cancel = live.cancel.clone();
    let mut dropped: u64 = 0;
    loop {
        let msg = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            m = rx.recv() => m,
        };
        let msg = match msg {
            Ok(m) => m,
            Err(RecvError::Lagged(n)) => {
                tracing::warn!(missed = n, "responder: control subscription lagged");
                continue;
            }
            Err(RecvError::Closed) => break,
        };
        let received_ns = now_ns();
        if msg.msg_type.ends_with(msg_type::ACK_SUFFIX)
            || msg.msg_type == msg_type::TICK
            || msg.msg_type == msg_type::FT_DONE
        {
            continue;
        }
        let slot = if within_budget(&msg.payload, MAX_REQUEST_BYTES) {
            live.limits.admit(&msg.from)
        } else {
            None
        };
        let Some(slot) = slot else {
            dropped += 1;
            if dropped.is_power_of_two() {
                tracing::debug!(
                    from = %msg.from,
                    msg_type = %msg.msg_type,
                    dropped,
                    "responder: request dropped (too large, or too many in flight)"
                );
            }
            continue;
        };
        let core = core.clone();
        let live2 = live.clone();
        live.tracker.spawn(async move {
            let _slot = slot;
            handle_request(&core, &live2, msg, received_ns).await;
        });
    }
}

async fn reply(live: &Live, to: &str, req_type: &str, payload: &impl Serialize) {
    let ack_type = msg_type::ack(req_type);
    let value = match serde_json::to_value(payload) {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(%ack_type, error = %e, "responder: cannot serialise reply");
            return;
        }
    };
    let deadline = Instant::now() + Duration::from_secs(10);
    if let Err(e) = send_with_backoff(&live.node, to, &ack_type, &value, deadline).await {
        tracing::debug!(%ack_type, error = %e, "responder: reply not sent");
    }
}

fn status_of<T>(r: &Result<T, AppError>) -> AckStatus {
    match r {
        Ok(_) => AckStatus::ok(),
        Err(e) => AckStatus::err(e),
    }
}

/// Look the sender up. A sender Layer 3 already lists is found at once;
/// otherwise wait up to 5 s for it (rule 1), but only one such wait per
/// sender at a time (a sender that is never listed cannot pin a waiter per
/// message).
async fn lookup_sender(live: &Live, from: &str) -> Option<Peer> {
    if let Ok(Some(p)) = live.node.peer(from, None).await {
        return Some(p);
    }
    let first = live
        .limits
        .unresolved
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(from.to_string());
    if !first {
        return None;
    }
    let found = live
        .node
        .peer(from, Some(PEER_LOOKUP_MS))
        .await
        .ok()
        .flatten();
    live.limits
        .unresolved
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .remove(from);
    found
}

/// Look the sender up and apply rule 1. A refusal while this node's own
/// login is still on its way (a fast restart: core reports it a second or
/// two after `running`) waits up to [`OWN_LOGIN_WAIT`] for it first, rather
/// than fail a run that the login would admit.
async fn admitted_sender(core: &Core, live: &Live, from: &str) -> Result<Peer, AppError> {
    let peer = lookup_sender(live, from).await.ok_or_else(|| {
        AppError::new(
            ErrorCode::PeerNotFound,
            "this device does not know you yet; retry in a moment",
        )
    })?;
    if admit_on(core, live, &peer).is_err() {
        live.await_own_login(OWN_LOGIN_WAIT).await;
    }
    admit_on(core, live, &peer)?;
    Ok(peer)
}

fn parse<T: DeserializeOwned>(msg: &NamespacedMessage) -> Result<T, AppError> {
    T::deserialize(&msg.payload)
        .map_err(|e| AppError::invalid_params(format!("malformed {} request: {e}", msg.msg_type)))
}

fn run_id_of(msg: &NamespacedMessage) -> String {
    msg.payload
        .get("run_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

async fn handle_request(core: &Arc<Core>, live: &Arc<Live>, msg: NamespacedMessage, t2: u64) {
    let Some(id) = msg
        .payload
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        if msg.msg_type == msg_type::CANCEL {
            handle_cancel(core, live, &msg).await;
        }
        return;
    };
    match msg.msg_type.as_str() {
        msg_type::HELLO => handle_hello(core, live, &msg, id).await,
        msg_type::CLOCK => {
            let res = admitted_sender(core, live, &msg.from).await;
            let t1 = msg
                .payload
                .get("t1_ns")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let ack = ClockAck {
                id,
                status: status_of(&res),
                t1_ns: t1,
                t2_ns: if res.is_ok() { t2 } else { 0 },
                t3_ns: if res.is_ok() { now_ns() } else { 0 },
            };
            reply(live, &msg.from, msg_type::CLOCK, &ack).await;
        }
        msg_type::PREPARE => {
            let run_id = run_id_of(&msg);
            let ack = match handle_prepare(core, live, &msg, &id).await {
                Ok(ack) => ack,
                Err(e) => PrepareAck {
                    id,
                    status: AckStatus::err(&e),
                    run_id,
                    run_tag: 0,
                    params: None,
                    setup: Value::Null,
                    overlapping_runs: 0,
                },
            };
            reply(live, &msg.from, msg_type::PREPARE, &ack).await;
        }
        msg_type::START => {
            let run_id = run_id_of(&msg);
            let res = handle_start(core, live, &msg).await;
            let ack = StartAck {
                id,
                status: status_of(&res),
                run_id,
            };
            reply(live, &msg.from, msg_type::START, &ack).await;
        }
        msg_type::FINISH => {
            if let Some(ack) = handle_finish(core, live, &msg, &id).await {
                reply(live, &msg.from, msg_type::FINISH, &ack).await;
            }
        }
        msg_type::CANCEL => handle_cancel(core, live, &msg).await,
        other => {
            // A request type from a newer protocol: say so rather than let it time out.
            let ack = serde_json::json!({
                "id": id,
                "ok": false,
                "code": ErrorCode::ProtocolMismatch,
                "error": format!("unknown request “{other}”"),
            });
            reply(live, &msg.from, other, &ack).await;
        }
    }
}

async fn handle_hello(core: &Arc<Core>, live: &Arc<Live>, msg: &NamespacedMessage, id: String) {
    // Hello is always answered, across accounts; admission only decides what
    // it says (rule 1).
    let theirs = parse::<HelloReq>(msg).ok().map(|r| r.hello.bounded());
    let peer = lookup_sender(live, &msg.from).await;
    let admitted = peer
        .as_ref()
        .is_some_and(|p| admit_on(core, live, p).is_ok());
    if let Some(theirs) = theirs {
        core.remember_inbound_hello(&msg.from, theirs);
    }
    let mut hello = live.hello();
    hello.accepts_runs_from_you = admitted;
    hello.busy = core.slots.busy();
    if !admitted {
        hello.host_fingerprint = None;
        hello.host_fingerprint_weak = false;
    }
    let ack = HelloAck {
        id,
        status: AckStatus::ok(),
        hello: Some(hello),
    };
    reply(live, &msg.from, msg_type::HELLO, &ack).await;
}

/// The hub is stopping: no new runs are prepared (they would outlive it).
fn refuse_while_stopping(live: &Live) -> Result<(), AppError> {
    if live.stopping.load(Ordering::SeqCst) {
        return Err(AppError::new(
            ErrorCode::NotRunning,
            "this device's benchmark hub is stopping",
        ));
    }
    Ok(())
}

async fn handle_prepare(
    core: &Arc<Core>,
    live: &Arc<Live>,
    msg: &NamespacedMessage,
    id: &str,
) -> Result<PrepareAck, AppError> {
    let peer = admitted_sender(core, live, &msg.from).await?;
    let req: PrepareReq = parse(msg)?;
    validate_run_id(&req.run_id)?;
    refuse_while_stopping(live)?;
    let key: RunKey = (msg.from.clone(), req.run_id.clone());
    {
        let mut runs = remote_runs(core);
        if runs.tombstoned(&key) {
            return Err(AppError::new(
                ErrorCode::Cancelled,
                "this run was cancelled before it was prepared",
            ));
        }
        if runs.runs.contains_key(&key) || runs.finished.contains_key(&key) {
            return Err(AppError::invalid_params(format!(
                "run {} is already prepared",
                req.run_id
            )));
        }
    }
    let params = req.params;
    params.validate().map_err(AppError::invalid_params)?;
    let workload = params.workload();
    if workload == Workload::Path {
        return Err(AppError::invalid_params(
            "path runs only on the initiator; there is nothing to prepare",
        ));
    }
    if !core.offers_workload(workload) {
        return Err(AppError::new(
            ErrorCode::UnsupportedWorkload,
            format!("this device does not offer the {workload} workload yet"),
        ));
    }
    let initiator = Initiator::from_peer(&peer);
    let token = core.slots.acquire_data(SlotHolder {
        role: SlotRole::Responding,
        run_id: req.run_id.clone(),
        peer_name: initiator.device_name.clone(),
        workload,
    })?;
    let run_tag = random_run_tag()?;
    let ctx = ResponderCtx::new(
        live.node.clone(),
        initiator.clone(),
        req.run_id.clone(),
        run_tag,
        params.clone(),
        core.cfg.data_dir.clone(),
        live.offers.clone(),
        live.cancel.child_token(),
    );
    #[cfg(test)]
    if let Some(after) = core.watchdog_override() {
        ctx.set_watchdog_in(after);
    }
    let metrics_before = live.usermetrics().await;
    let sampler = ResourceSampler::start(ResourceTargets::this_process([live.node.sidecar_pid()]));
    let prepared = workloads::prepare_responder(&ctx, &params, req.client).await;
    let (setup, workload_run) = match prepared {
        Ok(p) => p,
        Err(e) => {
            ctx.cancel_token().cancel();
            remove_run_dir(&ctx.in_dir_path());
            return Err(e);
        }
    };
    let overlapping_runs = token.overlapping();
    let run = Arc::new(RemoteRun {
        key: key.clone(),
        workload,
        peer_name: initiator.device_name.clone(),
        ctx,
        run: tokio::sync::Mutex::new(Some(workload_run)),
        token: Mutex::new(Some(token)),
        watchdog: Mutex::new(None),
        metrics_before,
        sampler: Mutex::new(Some(sampler)),
        started_ns: AtomicU64::new(0),
    });
    // Checked under the same lock `Hub::stop` takes to list the runs it
    // tears down, so a run is either listed there or refused here.
    let refused = {
        let mut runs = remote_runs(core);
        if runs.tombstoned(&key) {
            Some(AppError::new(
                ErrorCode::Cancelled,
                "this run was cancelled while it was being prepared",
            ))
        } else if let Err(e) = refuse_while_stopping(live) {
            Some(e)
        } else {
            runs.runs.insert(key.clone(), run.clone());
            None
        }
    };
    if let Some(e) = refused {
        run.ctx.cancel_token().cancel();
        if let Some(w) = run.run.lock().await.take() {
            w.cancel().await;
        }
        remove_run_dir(&run.ctx.in_dir_path());
        return Err(e);
    }
    arm_watchdog(core, live, &run);
    emit_remote(core, &run, RemoteRunState::Prepared);
    Ok(PrepareAck {
        id: id.to_string(),
        status: AckStatus::ok(),
        run_id: req.run_id,
        run_tag,
        params: Some(params),
        setup,
        overlapping_runs,
    })
}

fn find_run(core: &Core, from: &str, msg: &NamespacedMessage) -> Result<Arc<RemoteRun>, AppError> {
    let run_id = run_id_of(msg);
    remote_runs(core)
        .runs
        .get(&(from.to_string(), run_id.clone()))
        .cloned()
        .ok_or_else(|| {
            AppError::invalid_params(format!("no run {run_id} prepared by you on this device"))
        })
}

/// `start` is idempotent: only the first one reaches the workload (a
/// duplicate, re-sent or not, is acknowledged without effect), so a peer
/// cannot start a run's senders or tickers twice.
async fn handle_start(
    core: &Arc<Core>,
    live: &Arc<Live>,
    msg: &NamespacedMessage,
) -> Result<(), AppError> {
    admitted_sender(core, live, &msg.from).await?;
    let run = find_run(core, &msg.from, msg)?;
    let first = run
        .started_ns
        .compare_exchange(0, now_ns().max(1), Ordering::AcqRel, Ordering::Relaxed)
        .is_ok();
    if !first {
        return Ok(());
    }
    let mut guard = run.run.lock().await;
    match guard.as_mut() {
        Some(w) => w.start().await,
        None => Err(AppError::invalid_params("the run is already finished")),
    }
}

/// How long a duplicate `finish` (a retry with another id) waits for the
/// first one's outcome.
const FINISH_DUPLICATE_WAIT: Duration = Duration::from_secs(300);

/// A failed `finish.ack`.
fn finish_error(id: &str, run_id: String, e: &AppError) -> FinishAck {
    FinishAck {
        id: id.to_string(),
        status: AckStatus::err(e),
        run_id,
        stats: Value::Null,
        resources: None,
        path_bytes: None,
        max_overlapping_runs: 0,
    }
}

/// `finish`: the reply to send, or `None` to say nothing (a re-sent copy
/// of a finish that is still running: the first one's reply answers it).
///
/// The first `finish` for a run takes it and runs the workload's finish;
/// its outcome (ok or not) is kept for [`TOMBSTONE_TTL`], and any
/// duplicate gets that same outcome under its own id.
async fn handle_finish(
    core: &Arc<Core>,
    live: &Arc<Live>,
    msg: &NamespacedMessage,
    id: &str,
) -> Option<FinishAck> {
    let run_id = run_id_of(msg);
    if let Err(e) = admitted_sender(core, live, &msg.from).await {
        return Some(finish_error(id, run_id, &e));
    }
    let req: FinishReq = match parse(msg) {
        Ok(r) => r,
        Err(e) => return Some(finish_error(id, run_id, &e)),
    };
    let key: RunKey = (msg.from.clone(), req.run_id.clone());
    let taken = {
        let mut runs = remote_runs(core);
        runs.prune();
        match runs.runs.remove(&key) {
            Some(run) => {
                let (tx, rx) = tokio::sync::watch::channel(None);
                runs.finished.insert(
                    key.clone(),
                    FinishRecord {
                        id: id.to_string(),
                        ack: rx,
                        expires: None,
                    },
                );
                Ok((run, tx))
            }
            None => Err(runs.finished.get(&key).map(|r| (r.id == id, r.ack.clone()))),
        }
    };
    let (run, tx) = match taken {
        Ok(t) => t,
        Err(None) => {
            let e = AppError::invalid_params(format!(
                "no run {} prepared by you on this device",
                req.run_id
            ));
            return Some(finish_error(id, req.run_id, &e));
        }
        Err(Some((same_id, mut ack))) => {
            // A duplicate. A re-sent copy of a finish that is still running
            // needs no answer of its own.
            if same_id && ack.borrow().is_none() {
                return None;
            }
            let outcome = tokio::select! {
                _ = live.cancel.cancelled() => return None,
                r = tokio::time::timeout(FINISH_DUPLICATE_WAIT, ack.wait_for(Option::is_some)) => r,
            };
            let first = match outcome {
                Ok(Ok(a)) => (*a).clone(),
                _ => return None,
            };
            return first.map(|mut a| {
                a.id = id.to_string();
                a
            });
        }
    };
    let ack = finish_run(core, live, &run, &req, id).await;
    tx.send_replace(Some(ack.clone()));
    if let Some(r) = remote_runs(core).finished.get_mut(&key) {
        r.expires = Some(Instant::now() + TOMBSTONE_TTL);
    }
    Some(ack)
}

/// The first `finish` of a run: the workload's finish, then resources,
/// usermetrics, the slot and the run directory (rule 3: the slot is free
/// before `finish.ack` goes out).
async fn finish_run(
    core: &Arc<Core>,
    live: &Arc<Live>,
    run: &Arc<RemoteRun>,
    req: &FinishReq,
    id: &str,
) -> FinishAck {
    run.abort_watchdog();
    let workload = run.run.lock().await.take();
    let result = match workload {
        Some(w) => w.finish(req.info.clone()).await,
        None => Err(AppError::invalid_params("the run is already finished")),
    };
    // Rule 7: everything still waiting on the run's token (tickers, the
    // watchdog) ends with the run.
    run.ctx.cancel_token().cancel();
    let finish_ns = now_ns();
    let samples = match run.take_sampler() {
        Some(s) => Some(s.stop().await),
        None => None,
    };
    let metrics_after = live.usermetrics().await;
    let path_bytes = match (&run.metrics_before, &metrics_after) {
        (Some(b), Some(a)) => Some(a.delta(b).path_bytes()),
        _ => None,
    };
    let (window, goodput_bytes) = match run.ctx.resource_window() {
        Some(w) => (Some((w.start_ns, w.end_ns)), w.goodput_bytes),
        None => {
            let started = run.started_ns.load(Ordering::Relaxed);
            ((started > 0).then_some((started, finish_ns)), None)
        }
    };
    let resources = samples.and_then(|s| s.stats(window, goodput_bytes));
    let max_overlapping_runs = run.overlapping();
    drop(run.take_token());
    remove_run_dir(&run.ctx.in_dir_path());
    emit_remote(core, run, RemoteRunState::Finished);
    match result {
        Ok(stats) => FinishAck {
            id: id.to_string(),
            status: AckStatus::ok(),
            run_id: req.run_id.clone(),
            stats,
            resources,
            path_bytes,
            max_overlapping_runs,
        },
        Err(e) => finish_error(id, req.run_id.clone(), &e),
    }
}

async fn handle_cancel(core: &Arc<Core>, live: &Arc<Live>, msg: &NamespacedMessage) {
    if admitted_sender(core, live, &msg.from).await.is_err() {
        return;
    }
    let Ok(req) = parse::<CancelMsg>(msg) else {
        return;
    };
    if validate_run_id(&req.run_id).is_err() {
        return;
    }
    let key: RunKey = (msg.from.clone(), req.run_id);
    if !abort_remote(core, &key, RemoteRunState::Cancelled).await {
        let mut runs = remote_runs(core);
        runs.prune();
        runs.tombstones.insert(key, Instant::now() + TOMBSTONE_TTL);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_a_shutdown_cancel_stops_the_connect_retry() {
        assert!(is_shutdown_cancel(
            "dial cancelled: the session is shutting down"
        ));
        for m in [
            "WS dial timed out after 45s",
            "connection already in progress",
            "dial failed: connection refused",
            // Remote-controlled fields that carry both old substrings.
            "hello identity mismatch: dialed nX, answerer claims dial cancelled / shutting down",
            "app mismatch: local=bench, remote=dial cancelled shutting down",
            "hello identity mismatch: dialed nX, answerer claims \
             dial cancelled: the session is shutting down",
        ] {
            assert!(!is_shutdown_cancel(m), "{m}");
        }
    }

    #[test]
    fn ack_status_is_flat_and_optional() {
        let ack = StartAck {
            id: "a".into(),
            status: AckStatus::ok(),
            run_id: "r".into(),
        };
        let v = serde_json::to_value(&ack).unwrap();
        assert_eq!(v, json!({"id": "a", "ok": true, "run_id": "r"}));
        let err = AppError::new(ErrorCode::Busy, "busy: measuring studio · tcp");
        let ack = StartAck {
            status: AckStatus::err(&err),
            ..ack
        };
        let v = serde_json::to_value(&ack).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["code"], "busy");
        assert_eq!(v["error"], "busy: measuring studio · tcp");
        let back: StartAck = serde_json::from_value(v).unwrap();
        assert_eq!(back.status.to_error(), Some(err));
        assert_eq!(AckStatus::ok().to_error(), None);
    }

    #[test]
    fn messages_round_trip() {
        let prepare = PrepareReq {
            id: new_request_id(),
            run_id: "run-1".into(),
            params: WorkloadParams::Tcp(Default::default()),
            client: json!({}),
        };
        let v = serde_json::to_value(&prepare).unwrap();
        assert_eq!(v["params"]["workload"], "tcp");
        assert_eq!(serde_json::from_value::<PrepareReq>(v).unwrap(), prepare);
        assert_eq!(prepare.id.len(), 36);
        assert_eq!(<PrepareReq as ControlRequest>::TYPE, "prepare");

        let ack = PrepareAck {
            id: "x".into(),
            status: AckStatus::ok(),
            run_id: "run-1".into(),
            run_tag: u64::MAX - 7,
            params: Some(WorkloadParams::Tcp(Default::default())),
            setup: json!({"port": 41000}),
            overlapping_runs: 1,
        };
        let back: PrepareAck = serde_json::from_value(serde_json::to_value(&ack).unwrap()).unwrap();
        assert_eq!(back, ack, "a u64 run tag survives JSON exactly");

        let finish = FinishAck {
            id: "y".into(),
            status: AckStatus::ok(),
            run_id: "run-1".into(),
            stats: json!({"bytes_total": 1}),
            resources: Some(ResourceStats::default()),
            path_bytes: Some(PathBytes {
                direct: 5,
                derp: 0,
                peer_relay: 0,
            }),
            max_overlapping_runs: 0,
        };
        let back: FinishAck =
            serde_json::from_value(serde_json::to_value(&finish).unwrap()).unwrap();
        assert_eq!(back, finish);

        // A failed reply without the success fields still parses.
        let v =
            json!({"id": "z", "ok": false, "code": "login-refused", "error": "no", "run_id": "r"});
        let failed: FinishAck = serde_json::from_value(v).unwrap();
        assert_eq!(
            failed.status.to_error().unwrap().code,
            ErrorCode::LoginRefused
        );

        let tick = TickMsg {
            run_id: "r".into(),
            t_ns: 5,
            values: [("goodput_mbps".to_string(), 912.5)].into_iter().collect(),
        };
        let v = serde_json::to_value(&tick).unwrap();
        assert_eq!(v["values"]["goodput_mbps"], 912.5);
        let done = FtDoneMsg {
            run_id: "r".into(),
            iteration: 3,
            ok: true,
            bytes: 1024,
            sha256: Some("ab".into()),
            error: None,
        };
        let v = serde_json::to_value(&done).unwrap();
        assert!(v.get("error").is_none());
        assert_eq!(serde_json::from_value::<FtDoneMsg>(v).unwrap(), done);
        assert_eq!(msg_type::ack("finish"), "finish.ack");
    }

    fn holder(role: SlotRole, run: &str) -> SlotHolder {
        SlotHolder {
            role,
            run_id: run.into(),
            peer_name: "studio".into(),
            workload: Workload::Tcp,
        }
    }

    #[test]
    fn one_data_run_per_hub_in_either_role() {
        let slots = Slots::new();
        let a = slots
            .acquire_data(holder(SlotRole::Responding, "a"))
            .unwrap();
        assert_eq!(
            slots.busy().as_deref(),
            Some("being measured by studio · tcp")
        );
        let e = slots
            .acquire_data(holder(SlotRole::Initiating, "b"))
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::BeingMeasured);
        let e = slots
            .acquire_data(holder(SlotRole::Responding, "c"))
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::Busy);
        // Exempt runs are never refused, and count as overlap.
        let p = slots.acquire_exempt();
        assert_eq!(p.overlapping(), 1);
        assert_eq!(a.overlapping(), 1);
        drop(p);
        assert_eq!(a.overlapping(), 1, "overlap is cumulative");
        drop(a);
        assert_eq!(slots.busy(), None);
        let i = slots
            .acquire_data(holder(SlotRole::Initiating, "d"))
            .unwrap();
        assert_eq!(i.overlapping(), 0);
        let e = slots
            .acquire_data(holder(SlotRole::Responding, "e"))
            .unwrap_err();
        assert_eq!(e.code, ErrorCode::Busy);
        assert!(e.message.contains("measuring studio"));
        assert_eq!(slots.active(), 1);
    }

    fn peer(login: Option<&str>) -> Peer {
        Peer {
            device_id: None,
            device_name: Some("studio".into()),
            display_name: "studio".into(),
            hostname: "truffle-truffle-bench-studio".into(),
            tailscale_id: "nPEER".into(),
            peer_ref: "nPEER:1".into(),
            generation: 1,
            ip: "100.64.0.7".parse().unwrap(),
            online: true,
            ws_connected: true,
            connection_type: "direct".into(),
            os: None,
            last_seen: None,
            login_name: login.map(str::to_string),
        }
    }

    #[test]
    fn admission_follows_rule_one() {
        let me = Some("alice@example.com");
        assert!(admit(me, &peer(Some("Alice@Example.com")), false, false).is_ok());
        let e = admit(me, &peer(Some("bob@example.com")), false, false).unwrap_err();
        assert_eq!(e.code, ErrorCode::LoginRefused);
        assert!(admit(me, &peer(Some("bob@example.com")), true, false).is_ok());
        assert!(admit(me, &peer(None), false, true).is_err());
        let tagged = Some("tagged-devices");
        assert!(admit(tagged, &peer(tagged), false, false).is_err());
        assert!(admit(tagged, &peer(tagged), false, true).is_ok());
    }

    #[test]
    fn request_budget_is_bounded_and_cheap() {
        assert!(within_budget(&json!({"id": "x", "run_id": "r"}), 1024));
        let info = json!({"sender": {"bucket_first_seq": (0..300).collect::<Vec<u64>>()}});
        assert!(within_budget(&info, MAX_REQUEST_BYTES));
        // Wide arrays, long strings and many keys are refused without
        // walking them.
        let wide = Value::Array(vec![Value::Null; 1_000_000]);
        assert!(!within_budget(&wide, MAX_REQUEST_BYTES));
        assert!(!within_budget(
            &json!({"id": "x", "pad": "y".repeat(MAX_REQUEST_BYTES)}),
            MAX_REQUEST_BYTES
        ));
        let keys: serde_json::Map<String, Value> = (0..20_000)
            .map(|i| (format!("k{i}"), Value::Null))
            .collect();
        assert!(!within_budget(&Value::Object(keys), MAX_REQUEST_BYTES));
    }

    #[test]
    fn responder_limits_cap_requests_per_peer_and_in_all() {
        let limits = ResponderLimits::default();
        let a: Vec<_> = (0..MAX_INFLIGHT_PER_PEER)
            .map(|_| limits.admit("a").expect("within the per-peer cap"))
            .collect();
        assert!(limits.admit("a").is_none(), "per-peer cap");
        drop(a);
        assert!(limits.admit("a").is_some(), "released on drop");
        assert!(limits.per_peer.lock().unwrap().is_empty());
        let all: Vec<_> = (0..MAX_INFLIGHT)
            .map(|i| {
                limits
                    .admit(&format!("p{i}"))
                    .expect("within the global cap")
            })
            .collect();
        assert!(limits.admit("other").is_none(), "global cap");
        drop(all);
        assert!(limits.admit("other").is_some());
    }

    #[test]
    fn finish_records_expire_once_answered() {
        let now = Instant::now();
        let (tx, rx) = tokio::sync::watch::channel(None);
        let mut rec = FinishRecord {
            id: "a".into(),
            ack: rx,
            expires: None,
        };
        assert!(rec.live(now), "running");
        rec.expires = Some(now + Duration::from_secs(1));
        assert!(rec.live(now));
        assert!(!rec.live(now + Duration::from_secs(2)));
        // A handler that went away without an outcome is forgotten.
        rec.expires = None;
        drop(tx);
        assert!(!rec.live(now));
    }

    #[test]
    fn data_plane_identity() {
        let init = Initiator::from_peer(&peer(None));
        assert!(init.is_addr("100.64.0.7:41641"));
        assert!(init.is_addr("100.64.0.7"));
        assert!(!init.is_addr("100.64.0.8:41641"));
        assert!(!init.is_addr(""));
        assert!(!init.is_addr("garbage"));
        assert_eq!(init.device_name, "studio");
    }

    #[test]
    fn run_dirs_are_named_by_tag_and_guarded() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = in_dir(tmp.path(), 0xab);
        assert!(dir.ends_with("tmp/in-00000000000000ab"));
        assert!(out_dir(tmp.path(), 1).ends_with("tmp/out-0000000000000001"));
        create_run_dir(&dir).unwrap();
        std::fs::write(dir.join("f"), b"x").unwrap();
        remove_run_dir(&dir);
        assert!(!dir.exists());
        remove_run_dir(&dir); // missing is fine
                              // Anything that is not a run dir is left alone.
        let other = tmp.path().join("keep");
        std::fs::create_dir_all(&other).unwrap();
        remove_run_dir(&other);
        assert!(other.exists());
    }
}
