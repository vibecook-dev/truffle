//! `udp` (RFC 027 §8.2 udp stream, udp echo and udp ramp).
//!
//! **Sockets.** The initiator binds `bind_udp(0)` before `prepare` and
//! sends `Client { udp_port }`; the responder binds its own in `prepare` and
//! answers `Setup { udp_port }`. The advertised port is always the
//! `tsnet_port()` of the `DatagramSocket::Network` variant, never
//! `local_addr()` (the loopback relay). Receivers raise `SO_RCVBUF` on the
//! relay socket (`inner()`) to 4 MiB with `socket2::SockRef`, best effort,
//! and record what they got. Every datagram starts with the 22-byte header
//! (`b"TB" · run_tag_lo · seq · send_ns`); `payload_bytes` includes it, and
//! goodput and offered load both count whole UDP payloads, so they share a
//! unit. Datagrams from any IP but the peer's, and datagrams with a wrong
//! magic or tag, are dropped and counted.
//!
//! **stream.** The sender paces `payload_bytes` datagrams at `rate_mbps` for
//! warmup + duration with a token bucket (depth `max(1, rate × 2 ms)`,
//! sleeping to 1 ms ticks; a sender that falls behind its schedule, e.g. on
//! timer lateness, catches up by up to 8 ms; see [`Pacer`]), stamping
//! `send_ns` just before `send_to`. A `send_to` error does not use up a
//! seq, so `sent` = seqs `0..sent`.
//! Upload: the initiator sends to `<responder Peer.ip>:<setup.udp_port>`.
//! Download: the responder sends only after `start`, and only to
//! `<initiator Peer.ip>:<client.udp_port>`.
//! After its last datagram the sender waits the drain period
//! `D = min(max(500 ms, 4 × before.rtt), 3 s)`; the receiver snapshots once
//! it has seen `last_seq`, or after `D` without arrivals (hard cap `D + 3 s`).
//! Receiver definitions (after the drain): `unique` distinct seqs in
//! `0..sent`, `dup` repeats, `reordered` a unique seq below the highest seen
//! before it (RFC 4737), `loss_pct = (sent − unique) / sent · 100`,
//! `max_loss_burst` the longest run of never-received seqs, `jitter_ms` the
//! RFC 3550 J (gain 1/16) over unique datagrams in arrival order on raw
//! clocks. Goodput uses the §8.1 receiver accounting (arrival-time 250 ms
//! buckets, warmup excluded on bucket edges). The loss series is exact and
//! clock-free: `finish.info.bucket_first_seq` holds the first seq of each
//! 250 ms send bucket. Loss locality comes from both sides' usermetrics
//! packet deltas (an approximation: other traffic counts too).
//!
//! **echo.** Paced at `pps` for `duration_secs`; the responder echoes each
//! datagram (only after `start`) to `<initiator Peer.ip>:<client.udp_port>`.
//! RTT = receipt − `send_ns`, on the initiator's clock. A datagram not echoed
//! within `D` of the last send counts as lost; the first `warmup_count`
//! seqs are discarded.
//!
//! **ramp.** Like stream, starting at `rate_mbps` (the warmup runs at that
//! rate), then steps of 3 s at `rate_mbps × 1.5^k`. It stops when the
//! receiver's live loss (per 250 ms receive interval: `1 − Δunique /
//! Δ(max seq)`) exceeds 2% twice in a row, or when a step's offered rate
//! falls under 98% of its target. Each step's loss is then computed exactly
//! from its seq range; `max_sustainable_mbps` is the last step's rate with
//! loss ≤ 2%. Upload ramps get the receiver's live loss from `tick`s; download
//! ramps stop the responder's sender with an early `finish`.
//!
//! Summary keys: stream `goodput_mbps`, `offered_mbps`, `loss_pct`,
//! `jitter_ms`, `cpu_ms_per_mb`; echo `rtt_p50_ms`, `rtt_p99_ms`, `loss_pct`;
//! ramp `max_sustainable_mbps`, `offered_mbps`, `loss_pct`.
//! Series: stream `goodput_mbps`, `offered_mbps`, `loss_pct`; ramp the same
//! (the RFC names `offered_mbps` and `loss_pct`); echo `rtt_ms`.
//! Live samples: receiver `goodput_mbps`, `loss_pct`, `jitter_ms` (ticked
//! by the responder when it receives), sender `offered_mbps`, ramp
//! `target_mbps` and `max_sustainable_mbps`, echo `rtt_ms`, `rtt_p50_ms`,
//! `loss_pct`.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use truffle_core::transport::DatagramSocket;

use crate::clock::now_ns;
use crate::dataplane::{
    payload_chunk, run_tag_lo, DatagramHeader, Pacer, ReceiverAccounting, ReceiverStats, BUCKET_NS,
    DATAGRAM_HEADER_LEN, MIN_FINAL_BUCKET_NS, RATE_ACHIEVED,
};
use crate::error::{AppError, ErrorCode};
use crate::hub::BenchNode;
use crate::localapi::{LocalApi, UserMetrics};
use crate::params::{OneWayDirection, UdpMode, UdpParams, WorkloadParams};
use crate::stats::{self, Jitter, SeqOutcome, SeqTracker};
use crate::types::{Better, Distribution, Phase, Series, SummaryItem};
use crate::workloads::common::{cpu_per_mb_item, dur_ms, LiveRtt, Tasks};
use crate::workloads::tcp::item;
use crate::workloads::{ResponderCtx, ResponderRun, RunCtx, WorkloadOutput};

/// Whether this build offers `udp` (drives `PeerHello.workloads`).
pub const IMPLEMENTED: bool = true;

/// `SO_RCVBUF` receivers ask for.
const RCVBUF_BYTES: usize = 4 << 20;
/// Receive buffer (payloads are at most 1400 B).
const RECV_BUF: usize = 2048;
/// Ramp step length.
const RAMP_STEP: Duration = Duration::from_secs(3);
/// Ramp rate factor per step.
const RAMP_FACTOR: f64 = 1.5;
/// Ramp loss threshold, %.
const RAMP_LOSS_PCT: f64 = 2.0;
/// Drain period bounds.
const DRAIN_MIN: Duration = Duration::from_millis(500);
const DRAIN_MAX: Duration = Duration::from_secs(3);
/// IPv4 + UDP headers, the tunnel MTU and the largest fragment payload.
const IP_UDP_HEADERS: usize = 28;
const TUNNEL_MTU: usize = 1280;
const FRAGMENT_DATA: usize = 1256;

/// The notes every udp run carries (§8.2).
const NOTE_IPV4: &str = "IPv4 only";
const NOTE_LOSS: &str = "loss is end to end, including loopback relay drops";
const NOTE_FRAGMENTS: &str = "payload > 1252 B fragments in netstack";
const NOTE_NOT_ACHIEVED: &str = crate::report::NOTE_RATE_NOT_ACHIEVED;
const NOTE_LOCALITY: &str =
    "loss locality is an approximation from usermetrics packet counts: other traffic on both \
     sidecars counts too";

// ---------------------------------------------------------------------------
// Control blocks
// ---------------------------------------------------------------------------

/// `prepare.client`: the initiator socket's tailnet port.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpClient")]
pub struct Client {
    /// `tsnet_port()` of the initiator's `bind_udp(0)` socket.
    pub udp_port: u16,
}

/// `prepare.ack.setup`: the responder socket's tailnet port.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpSetup")]
pub struct Setup {
    /// `tsnet_port()` of the responder's `bind_udp(0)` socket.
    pub udp_port: u16,
    /// `SO_RCVBUF` the responder got, when it receives.
    pub rcvbuf_bytes: Option<u64>,
}

/// usermetrics packet deltas (all paths) on one side over its part of the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpPacketCounts")]
pub struct PacketCounts {
    /// `tailscaled_outbound_packets_total` Δ.
    pub outbound: u64,
    /// `tailscaled_inbound_packets_total` Δ.
    pub inbound: u64,
}

/// One ramp step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpRampStep")]
pub struct RampStep {
    /// Target rate, Mb/s.
    pub rate_mbps: f64,
    /// What the sender offered during the step, Mb/s.
    pub offered_mbps: Option<f64>,
    /// First seq of the step.
    pub first_seq: u64,
    /// One past its last seq.
    pub end_seq: u64,
    /// Start, ms since the sender started.
    pub start_ms: f64,
    /// End, ms since the sender started.
    pub end_ms: f64,
    /// It ran its full 3 s.
    pub complete: bool,
    /// `offered ≥ 0.98 · rate`.
    pub achieved: bool,
    /// Exact loss of its seq range (filled from the receiver).
    pub loss_pct: Option<f64>,
    /// Counted as sustained: achieved, loss ≤ 2%, and not the step the
    /// loss stop cut short.
    pub sustained: bool,
}

/// What a sender reports (whichever side sent).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpSenderReport")]
pub struct SenderReport {
    /// Datagrams whose `send_to` returned Ok (seqs `0..sent`).
    pub sent: u64,
    /// `send_to` errors.
    pub send_errors: u64,
    /// The last error's text.
    pub last_error: Option<String>,
    /// Highest seq sent; `None` when nothing was.
    pub last_seq: Option<u64>,
    /// Payload bytes sent.
    pub bytes_sent: u64,
    /// First seq of each 250 ms send bucket.
    pub bucket_first_seq: Vec<u64>,
    /// Payload bytes sent in each bucket.
    pub bucket_bytes: Vec<u64>,
    /// Sender `now_ns()` at bucket 0's start.
    pub start_ns: u64,
    /// Sender `now_ns()` after the last datagram.
    pub end_ns: u64,
    /// Target rate (stream; ramp start rate; echo pps as Mb/s).
    pub target_mbps: f64,
    /// Offered load: bytes after the first datagram over first → last send.
    pub offered_mbps: Option<f64>,
    /// Largest number of datagrams released back to back.
    pub max_burst_datagrams: u64,
    /// Mean timer lateness of the pacer's sleeps, ms.
    pub pacer_late_ms_mean: Option<f64>,
    /// Largest timer lateness of the pacer's sleeps, ms.
    pub pacer_late_ms_max: Option<f64>,
    /// `offered ≥ 0.98 · target` (ramp: every complete step).
    pub rate_achieved: bool,
    /// Ramp steps.
    pub steps: Vec<RampStep>,
    /// Why a ramp stopped.
    pub stop_reason: Option<String>,
    /// Sender-side usermetrics packet deltas.
    pub packets: Option<PacketCounts>,
}

/// `finish.info`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpInfo")]
pub struct Info {
    /// The initiator's sender report (upload, echo).
    pub sender: Option<SenderReport>,
    /// Download ramp: why the initiator stopped the responder's sender.
    pub stop_reason: Option<String>,
    /// The drain period `D`, ms.
    pub drain_ms: f64,
}

/// One send bucket's (or ramp step's) loss, exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpBucketLoss")]
pub struct BucketLoss {
    /// Seqs in the range that never arrived.
    pub missing: u64,
    /// Seqs in the range.
    pub sent: u64,
}

impl BucketLoss {
    fn pct(&self) -> Option<f64> {
        stats::pct(self.missing as f64, self.sent as f64)
    }
}

/// What a receiver reports after its drain.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpReceiverReport")]
pub struct ReceiverReport {
    /// Distinct seqs received within `0..sent`.
    pub unique: u64,
    /// `sent − unique`.
    pub missing: u64,
    /// Repeat arrivals.
    pub dup: u64,
    /// Unique arrivals below the highest seq seen before them.
    pub reordered: u64,
    /// Arrivals with an impossible seq.
    pub out_of_range: u64,
    /// Datagrams with a short header, wrong magic or wrong tag.
    pub bad_datagrams: u64,
    /// `missing / sent · 100`.
    pub loss_pct: Option<f64>,
    /// Longest run of never-received seqs.
    pub max_loss_burst: u64,
    /// RFC 3550 J at the end of the window, ms.
    pub jitter_ms: Option<f64>,
    /// Loss per 250 ms send bucket.
    pub bucket_loss: Vec<BucketLoss>,
    /// Loss per ramp step (aligned with the sender's steps).
    pub step_loss: Vec<BucketLoss>,
    /// Unique payload bytes by arrival time (§8.1 accounting, stream 0).
    pub accounting: ReceiverStats,
    /// `last_seq`, `quiet` or `cap`.
    pub drained_by: String,
    /// The drain period used, ms.
    pub drain_ms: f64,
    /// `SO_RCVBUF` the receiver got.
    pub rcvbuf_bytes: Option<u64>,
    /// Receiver-side usermetrics packet deltas.
    pub packets: Option<PacketCounts>,
}

/// The echo responder's counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpEchoStats")]
pub struct EchoStats {
    /// Valid datagrams received.
    pub received: u64,
    /// Echoes sent.
    pub echoed: u64,
    /// Echo `send_to` errors.
    pub echo_errors: u64,
    /// Received before `start` (not echoed).
    pub before_start: u64,
}

/// `finish.ack.stats`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpStats")]
pub struct Stats {
    /// Upload: the responder's receiver report.
    pub receiver: Option<ReceiverReport>,
    /// Download: the responder's sender report.
    pub sender: Option<SenderReport>,
    /// Echo: the responder's counts.
    pub echo: Option<EchoStats>,
    /// Foreign datagrams the responder dropped (§6 rule 5).
    pub foreign_dropped: u64,
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

/// Where datagrams were lost (usermetrics on both sides; an approximation).
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpLossLocality")]
pub struct LossLocality {
    /// `sent − sender outbound Δ`, in datagrams.
    pub local_send_drops: i64,
    /// `sender outbound Δ − receiver inbound Δ`, in datagrams.
    pub network: i64,
    /// `receiver inbound Δ − unique`, in datagrams.
    pub local_recv_drops: i64,
    /// Tunnel packets per datagram used to convert (2 above 1252 B).
    pub packets_per_datagram: u32,
}

/// `results` of a stream run (and the common part of a ramp run).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpStreamResults")]
pub struct StreamResults {
    /// Upload or download.
    pub direction: OneWayDirection,
    /// Datagram size, header included.
    pub payload_bytes: u32,
    /// Target (ramp: start) rate, Mb/s.
    pub target_mbps: f64,
    /// Datagrams whose `send_to` returned Ok.
    pub sent: u64,
    /// Distinct seqs received.
    pub unique: u64,
    /// `sent − unique`.
    pub missing: u64,
    /// Repeat arrivals.
    pub dup: u64,
    /// RFC 4737 reordered arrivals.
    pub reordered: u64,
    /// Arrivals with an impossible seq.
    pub out_of_range: u64,
    /// Short, wrong-magic or wrong-tag datagrams.
    pub bad_datagrams: u64,
    /// `send_to` errors.
    pub send_errors: u64,
    /// `(sent − unique) / sent · 100`.
    pub loss_pct: Option<f64>,
    /// Longest run of never-received seqs.
    pub max_loss_burst: u64,
    /// RFC 3550 interarrival jitter, ms.
    pub jitter_ms: Option<f64>,
    /// Goodput over the measurement window, Mb/s.
    pub goodput_mbps: Option<f64>,
    /// Offered load, Mb/s.
    pub offered_mbps: Option<f64>,
    /// `offered ≥ 0.98 · target`.
    pub rate_achieved: bool,
    /// Largest number of datagrams the pacer released back to back.
    pub max_burst_datagrams: u64,
    /// Mean timer lateness of the sender's pacing sleeps, ms.
    pub pacer_late_ms_mean: Option<f64>,
    /// Payload bytes sent.
    pub sent_bytes: u64,
    /// Unique payload bytes received.
    pub received_bytes: u64,
    /// Payload bytes in the measurement window.
    pub goodput_bytes: u64,
    /// Measurement window, ms.
    pub window_ms: u64,
    /// First seq of each 250 ms send bucket.
    pub bucket_first_seq: Vec<u64>,
    /// Loss per send bucket.
    pub bucket_loss: Vec<BucketLoss>,
    /// The receiver's byte accounting (receiver timebase).
    pub receiver: ReceiverStats,
    /// usermetrics loss locality, when both sides had usermetrics.
    pub loss_locality: Option<LossLocality>,
    /// Drain period, ms.
    pub drain_ms: f64,
    /// How the receiver's drain ended.
    pub drained_by: String,
    /// `SO_RCVBUF` the receiver got.
    pub rcvbuf_bytes: Option<u64>,
    /// Foreign datagrams dropped on both sides.
    pub foreign_dropped: u64,
}

/// `results` of a ramp run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpRampResults")]
pub struct RampResults {
    /// Counts, loss and goodput over the whole ramp.
    #[serde(flatten)]
    pub stream: StreamResults,
    /// Every step.
    pub steps: Vec<RampStep>,
    /// Why the ramp stopped.
    pub stop_reason: String,
    /// The last step's rate with loss ≤ 2%. When `limit_found` is false this
    /// is only a lower bound: the run ended before any step lost more than
    /// 2% or missed its rate.
    pub max_sustainable_mbps: Option<f64>,
    /// Whether the ramp reached a limit: a step lost more than 2% or missed
    /// its rate (or the live loss stop fired). When false, the summary's
    /// `max_sustainable_mbps` is left empty, so a duration-bound ceiling is
    /// never trended as a capacity.
    #[serde(default)]
    pub limit_found: bool,
}

/// `results` of an echo run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "UdpEchoResults")]
pub struct EchoResults {
    /// Datagram size, header included.
    pub payload_bytes: u32,
    /// Target rate.
    pub pps: u32,
    /// Discarded seqs.
    pub warmup_count: u32,
    /// Datagrams sent.
    pub sent: u64,
    /// Measured datagrams echoed in time.
    pub completed: u64,
    /// Measured datagrams not echoed within the drain deadline.
    pub lost: u64,
    /// `lost / measured · 100`.
    pub loss_pct: Option<f64>,
    /// Duplicate echoes.
    pub dup: u64,
    /// Echoes after the deadline (counted as lost).
    pub late: u64,
    /// `send_to` errors.
    pub send_errors: u64,
    /// Round trips of measured datagrams (lost ones ranked as +∞).
    pub rtt_ms: Distribution,
    /// Raw round trips of measured datagrams, in seq order (for the CDF).
    pub samples_ms: Vec<f64>,
    /// Offered datagrams per second.
    pub offered_pps: Option<f64>,
    /// Drain period, ms.
    pub drain_ms: f64,
    /// The responder's counts.
    pub responder: EchoStats,
    /// `SO_RCVBUF` this side got.
    pub rcvbuf_bytes: Option<u64>,
    /// Foreign datagrams dropped on both sides.
    pub foreign_dropped: u64,
}

/// `RunReport.results` of a `udp` run, tagged by `mode`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "lowercase")]
#[schemars(rename = "UdpResults")]
pub enum Results {
    /// `mode: "stream"`.
    Stream(Box<StreamResults>),
    /// `mode: "echo"`.
    Echo(Box<EchoResults>),
    /// `mode: "ramp"`.
    Ramp(Box<RampResults>),
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn secs(s: u32) -> Duration {
    Duration::from_secs(u64::from(s))
}

/// `D = min(max(500 ms, 4 × rtt), 3 s)`.
pub(crate) fn drain_period(rtt: Option<Duration>) -> Duration {
    rtt.map_or(DRAIN_MIN, |r| r * 4).clamp(DRAIN_MIN, DRAIN_MAX)
}

/// Tunnel packets per datagram: 1 up to 1252 B, then IP fragments.
pub(crate) fn packets_per_datagram(payload: usize) -> u32 {
    if payload + IP_UDP_HEADERS <= TUNNEL_MTU {
        1
    } else {
        (payload + 8).div_ceil(FRAGMENT_DATA) as u32
    }
}

/// A generous bound on seqs for the tracker (it grows on demand).
fn seq_limit(p: &UdpParams) -> u64 {
    let payload = f64::from(p.payload_bytes.max(DATAGRAM_HEADER_LEN as u32));
    let secs_total = f64::from(p.warmup_secs + p.duration_secs);
    let pps = match p.mode {
        UdpMode::Echo => f64::from(p.pps),
        UdpMode::Stream => p.rate_mbps * 1e6 / 8.0 / payload,
        UdpMode::Ramp => {
            let steps = (f64::from(p.duration_secs) / RAMP_STEP.as_secs_f64()).ceil();
            (p.rate_mbps * RAMP_FACTOR.powf(steps) * 1e6 / 8.0 / payload).min(2e6)
        }
    };
    ((pps * secs_total * 1.25) as u64 + 10_000).min(1 << 28)
}

/// Checks a peer's sender report before the receiver's accounting walks
/// its ranges (§6 rule 2: nothing from the other side is trusted): bucket
/// starts and ramp steps must be in order, within `sent`, and no more than
/// the run's length allows. `sent` itself needs no bound: the seq tracker's
/// cost depends only on what it recorded.
pub(crate) fn check_sender_report(s: &SenderReport, p: &UdpParams) -> Result<(), String> {
    let secs = u64::from(p.warmup_secs) + u64::from(p.duration_secs);
    let max_buckets = secs * 1_000_000_000 / BUCKET_NS + 16;
    if s.bucket_first_seq.len() as u64 > max_buckets || s.bucket_bytes.len() as u64 > max_buckets {
        return Err(format!(
            "{} send buckets for a {secs} s run",
            s.bucket_first_seq.len().max(s.bucket_bytes.len())
        ));
    }
    if s.bucket_first_seq.windows(2).any(|w| w[0] > w[1])
        || s.bucket_first_seq.last().is_some_and(|&f| f > s.sent)
    {
        return Err("send buckets out of order or past the datagrams sent".into());
    }
    let max_steps = u64::from(p.duration_secs) / RAMP_STEP.as_secs() + 2;
    if s.steps.len() as u64 > max_steps {
        return Err(format!(
            "{} ramp steps for a {} s run",
            s.steps.len(),
            p.duration_secs
        ));
    }
    let mut prev_end = 0;
    for st in &s.steps {
        if st.first_seq < prev_end || st.end_seq < st.first_seq || st.end_seq > s.sent {
            return Err("ramp steps out of order or past the datagrams sent".into());
        }
        prev_end = st.end_seq;
    }
    Ok(())
}

fn v4_target(ip: IpAddr, port: u16) -> Result<SocketAddr, AppError> {
    match ip {
        IpAddr::V4(_) => Ok(SocketAddr::new(ip, port)),
        IpAddr::V6(_) => Err(AppError::invalid_params(
            "udp is IPv4 only, and the peer's tailnet address is IPv6",
        )),
    }
}

/// The port to advertise: `tsnet_port()` of the Network variant (the
/// Direct variant exists only in tests).
fn advertised_port(sock: &DatagramSocket) -> Result<u16, AppError> {
    match sock {
        DatagramSocket::Network { socket } => Ok(socket.tsnet_port()),
        DatagramSocket::Direct { socket } => socket
            .local_addr()
            .map(|a| a.port())
            .map_err(|e| AppError::io("udp local address", &e)),
    }
}

/// Raise `SO_RCVBUF` on the relay socket to 4 MiB (best effort); returns
/// what the OS reports afterwards.
fn raise_rcvbuf(sock: &DatagramSocket) -> Option<u64> {
    let inner = match sock {
        DatagramSocket::Network { socket } => socket.inner(),
        DatagramSocket::Direct { socket } => socket,
    };
    let s = socket2::SockRef::from(inner);
    let _ = s.set_recv_buffer_size(RCVBUF_BYTES);
    s.recv_buffer_size().ok().map(|n| n as u64)
}

async fn send_to(sock: &DatagramSocket, buf: &[u8], to: SocketAddr) -> Result<(), String> {
    match sock {
        DatagramSocket::Network { socket } => socket
            .send_to(buf, to)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
        DatagramSocket::Direct { socket } => socket
            .send_to(buf, to)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string()),
    }
}

async fn recv_from(sock: &DatagramSocket, buf: &mut [u8]) -> Result<(usize, SocketAddr), String> {
    match sock {
        DatagramSocket::Network { socket } => {
            socket.recv_from(buf).await.map_err(|e| e.to_string())
        }
        DatagramSocket::Direct { socket } => socket.recv_from(buf).await.map_err(|e| e.to_string()),
    }
}

async fn read_metrics(node: &BenchNode) -> Option<UserMetrics> {
    let api = LocalApi::from_node(node).await.ok()?;
    api.usermetrics().await.ok()
}

fn packet_delta(before: Option<UserMetrics>, after: Option<UserMetrics>) -> Option<PacketCounts> {
    let d = after?.delta(&before?);
    Some(PacketCounts {
        outbound: d.outbound_packets.total(),
        inbound: d.inbound_packets.total(),
    })
}

/// Loss locality from both sides' packet deltas (§8.2).
pub(crate) fn loss_locality(
    sent: u64,
    unique: u64,
    sender: PacketCounts,
    receiver: PacketCounts,
    payload: usize,
) -> LossLocality {
    let ppd = packets_per_datagram(payload);
    let out = sender.outbound as f64 / f64::from(ppd);
    let inb = receiver.inbound as f64 / f64::from(ppd);
    LossLocality {
        local_send_drops: (sent as f64 - out).round() as i64,
        network: (out - inb).round() as i64,
        local_recv_drops: (inb - unique as f64).round() as i64,
        packets_per_datagram: ppd,
    }
}

fn fmt_rate(mbps: f64) -> String {
    if mbps >= 100.0 {
        format!("{mbps:.0}")
    } else if mbps >= 10.0 {
        format!("{mbps:.1}")
    } else {
        format!("{mbps:.2}")
    }
}

fn to_value<T: Serialize>(v: &T) -> Result<Value, AppError> {
    serde_json::to_value(v).map_err(|e| AppError::internal(format!("serialise the results: {e}")))
}

/// Who may send to a socket, and where drops are counted.
#[derive(Clone)]
enum Gate {
    /// The responder: rule 5 against the run's initiator.
    Responder(Box<ResponderCtx>),
    /// The initiator: only the peer's IP.
    Initiator { ip: IpAddr, foreign: Arc<AtomicU64> },
}

impl Gate {
    fn admit(&self, from: SocketAddr) -> bool {
        match self {
            Self::Responder(r) => r.admit_ip(from.ip()),
            Self::Initiator { ip, foreign } => {
                let ok = from.ip() == *ip;
                if !ok {
                    foreign.fetch_add(1, Ordering::Relaxed);
                }
                ok
            }
        }
    }

    fn count_bad(&self) {
        match self {
            Self::Responder(r) => r.count_foreign(),
            Self::Initiator { foreign, .. } => {
                foreign.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Sender
// ---------------------------------------------------------------------------

/// What a sender does.
#[derive(Debug, Clone, Copy)]
struct SendPlan {
    mode: UdpMode,
    payload: usize,
    rate_mbps: f64,
    warmup: Duration,
    duration: Duration,
    run_tag: u64,
    target: SocketAddr,
}

impl SendPlan {
    fn new(p: &UdpParams, run_tag: u64, target: SocketAddr) -> Self {
        let payload = (p.payload_bytes as usize).max(DATAGRAM_HEADER_LEN);
        let (rate_mbps, warmup) = match p.mode {
            UdpMode::Echo => (
                f64::from(p.pps) * payload as f64 * 8.0 / 1e6,
                Duration::ZERO,
            ),
            UdpMode::Stream | UdpMode::Ramp => (p.rate_mbps, secs(p.warmup_secs)),
        };
        Self {
            mode: p.mode,
            payload,
            rate_mbps,
            warmup,
            duration: secs(p.duration_secs),
            run_tag,
            target,
        }
    }
}

/// A running sender's live view (for live values and the ramp stop).
#[derive(Debug, Default)]
struct SendLive {
    bytes: AtomicU64,
    target_bits: AtomicU64,
    stop: AtomicBool,
    stop_reason: Mutex<Option<String>>,
    /// Echo: `send_ns` per seq.
    send_ns: Mutex<Vec<u64>>,
}

impl SendLive {
    fn new(target_mbps: f64) -> Arc<Self> {
        let me = Self::default();
        me.target_bits
            .store(target_mbps.to_bits(), Ordering::Relaxed);
        Arc::new(me)
    }

    fn target(&self) -> f64 {
        f64::from_bits(self.target_bits.load(Ordering::Relaxed))
    }

    fn request_stop(&self, reason: Option<String>) {
        if !self.stop.swap(true, Ordering::SeqCst) {
            *lock(&self.stop_reason) = reason;
        }
    }
}

/// Close a ramp step. `next_began` = the next step's boundary closed it
/// (so it ran its full slot, even if it opened a little late).
fn close_step(
    step: &mut RampStep,
    seq: u64,
    bytes: u64,
    elapsed: Duration,
    since_ms: f64,
    next_began: bool,
) {
    step.end_seq = seq;
    step.end_ms = since_ms;
    step.offered_mbps = stats::mbps(bytes as f64, elapsed.as_nanos() as u64);
    step.complete = next_began || elapsed + Duration::from_millis(1) >= RAMP_STEP;
    step.achieved = step
        .offered_mbps
        .is_some_and(|o| o >= RATE_ACHIEVED * step.rate_mbps);
}

async fn send_loop(sock: Arc<DatagramSocket>, plan: SendPlan, live: Arc<SendLive>) -> SenderReport {
    let mut buf: Vec<u8> = payload_chunk()
        .iter()
        .copied()
        .cycle()
        .take(plan.payload)
        .collect();
    let mut pacer = Pacer::new(plan.rate_mbps, plan.payload);
    let start = Instant::now();
    let start_ns = now_ns();
    let end = start + plan.warmup + plan.duration;
    let ramp_start = start + plan.warmup;
    let since_ms = |t: Instant| dur_ms(t.saturating_duration_since(start));
    let (mut seq, mut send_errors, mut bytes_sent) = (0u64, 0u64, 0u64);
    let mut last_error = None;
    let mut bucket_first_seq: Vec<u64> = Vec::new();
    let mut bucket_bytes: Vec<u64> = Vec::new();
    let mut steps: Vec<RampStep> = Vec::new();
    // (step index, its start, bytes_sent at its start)
    let mut step: Option<(u64, Instant, u64)> = None;
    let mut end_ns = start_ns;
    let mut stop_reason = None;
    loop {
        if live.stop.load(Ordering::SeqCst) {
            stop_reason = lock(&live.stop_reason).clone();
            break;
        }
        let now = Instant::now();
        if now >= end {
            break;
        }
        if plan.mode == UdpMode::Ramp && now >= ramp_start {
            let k = (now.duration_since(ramp_start).as_nanos() / RAMP_STEP.as_nanos()) as u64;
            if step.map(|s| s.0) != Some(k) {
                if let (Some((_, at, b0)), Some(cur)) = (step, steps.last_mut()) {
                    close_step(cur, seq, bytes_sent - b0, now - at, since_ms(now), true);
                    if !cur.achieved {
                        stop_reason = Some(format!(
                            "the sender could not reach {} Mb/s",
                            fmt_rate(cur.rate_mbps)
                        ));
                        break;
                    }
                }
                let rate = plan.rate_mbps * RAMP_FACTOR.powi(k.min(64) as i32);
                pacer.set_rate(rate);
                live.target_bits.store(rate.to_bits(), Ordering::Relaxed);
                steps.push(RampStep {
                    rate_mbps: rate,
                    offered_mbps: None,
                    first_seq: seq,
                    end_seq: seq,
                    start_ms: since_ms(now),
                    end_ms: since_ms(now),
                    complete: false,
                    achieved: false,
                    loss_pct: None,
                    sustained: false,
                });
                step = Some((k, now, bytes_sent));
            }
        }
        pacer.wait(plan.payload).await;
        let at = Instant::now();
        if at >= end {
            break;
        }
        let b = (at.duration_since(start).as_nanos() / u128::from(BUCKET_NS)) as usize;
        while bucket_first_seq.len() <= b {
            bucket_first_seq.push(seq);
            bucket_bytes.push(0);
        }
        let send_ns = now_ns();
        let header = DatagramHeader {
            run_tag_lo: run_tag_lo(plan.run_tag),
            seq,
            send_ns,
        };
        let _ = header.encode_into(&mut buf);
        match send_to(&sock, &buf, plan.target).await {
            Ok(()) => {
                pacer.record_sent(plan.payload);
                bucket_bytes[b] += plan.payload as u64;
                bytes_sent += plan.payload as u64;
                seq += 1;
                end_ns = now_ns();
                live.bytes.fetch_add(plan.payload as u64, Ordering::Relaxed);
                if plan.mode == UdpMode::Echo {
                    lock(&live.send_ns).push(send_ns);
                }
            }
            Err(e) => {
                send_errors += 1;
                last_error = Some(e);
            }
        }
    }
    let now = Instant::now();
    if let (Some((_, at, b0)), Some(cur)) = (step, steps.last_mut()) {
        if cur.end_ms <= cur.start_ms {
            close_step(cur, seq, bytes_sent - b0, now - at, since_ms(now), false);
        }
    }
    if plan.mode == UdpMode::Ramp && stop_reason.is_none() {
        stop_reason = Some("the run ended first".into());
    }
    let rate_achieved = match plan.mode {
        UdpMode::Stream => pacer.rate_achieved(plan.rate_mbps),
        UdpMode::Ramp => steps.iter().filter(|s| s.complete).all(|s| s.achieved),
        UdpMode::Echo => pacer.rate_achieved(plan.rate_mbps),
    };
    SenderReport {
        sent: seq,
        send_errors,
        last_error,
        last_seq: seq.checked_sub(1),
        bytes_sent,
        bucket_first_seq,
        bucket_bytes,
        start_ns,
        end_ns,
        target_mbps: plan.rate_mbps,
        offered_mbps: pacer.offered_mbps(),
        max_burst_datagrams: pacer.max_burst_datagrams(),
        pacer_late_ms_mean: pacer.lateness_ms().0,
        pacer_late_ms_max: pacer.lateness_ms().1,
        rate_achieved,
        steps,
        stop_reason,
        packets: None,
    }
}

/// The live ramp decision: stop when the receiver's interval loss exceeds
/// 2% twice in a row; remembers the highest rate whose samples all stayed
/// at or under 2% (live display only; the report uses exact step loss).
#[derive(Debug, Default)]
pub(crate) struct RampControl {
    over: u32,
    current: f64,
    current_ok: bool,
    best: Option<f64>,
    stopped_at: Option<f64>,
}

impl RampControl {
    /// Feed one interval's loss at the sender's current target; `true` means stop.
    pub(crate) fn on_sample(&mut self, loss_pct: f64, target_mbps: f64) -> bool {
        if target_mbps != self.current {
            if self.current_ok && self.current > 0.0 {
                self.best = Some(self.best.map_or(self.current, |b| b.max(self.current)));
            }
            self.current = target_mbps;
            self.current_ok = true;
        }
        if loss_pct > RAMP_LOSS_PCT {
            self.over += 1;
            self.current_ok = false;
        } else {
            self.over = 0;
        }
        if self.over >= 2 && self.stopped_at.is_none() {
            self.stopped_at = Some(target_mbps);
        }
        self.over >= 2
    }

    /// The highest rate whose samples all stayed at or under 2%, so far.
    pub(crate) fn best(&self) -> Option<f64> {
        self.best
    }

    /// The stop reason once [`on_sample`](Self::on_sample) said stop.
    pub(crate) fn reason(&self) -> Option<String> {
        self.stopped_at.map(|r| {
            format!(
                "loss above 2% in two consecutive 250 ms buckets at {} Mb/s",
                fmt_rate(r)
            )
        })
    }
}

// ---------------------------------------------------------------------------
// Receiver
// ---------------------------------------------------------------------------

/// Live receiver-side numbers for one 250 ms interval.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct LiveUdp {
    pub(crate) goodput_mbps: Option<f64>,
    pub(crate) loss_pct: Option<f64>,
    pub(crate) jitter_ms: Option<f64>,
}

/// A stream/ramp receiver: seq accounting, jitter and §8.1 byte accounting.
#[derive(Debug)]
pub(crate) struct UdpReceiver {
    tracker: SeqTracker,
    jitter: Jitter,
    acc: ReceiverAccounting,
    bad: u64,
    last_arrival: Option<Instant>,
    prev_at: Option<Instant>,
    prev_bytes: u64,
    prev_unique: u64,
    prev_expected: u64,
}

impl UdpReceiver {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            tracker: SeqTracker::new(limit),
            jitter: Jitter::new(),
            acc: ReceiverAccounting::new(),
            bad: 0,
            last_arrival: None,
            prev_at: None,
            prev_bytes: 0,
            prev_unique: 0,
            prev_expected: 0,
        }
    }

    /// Count one datagram received at `recv_ns`; `false` for a bad header.
    pub(crate) fn on_datagram(
        &mut self,
        data: &[u8],
        run_tag: u64,
        recv_ns: u64,
        at: Instant,
    ) -> bool {
        let Ok(h) = DatagramHeader::decode_for(data, run_tag) else {
            self.bad += 1;
            return false;
        };
        self.last_arrival = Some(at);
        match self.tracker.record(h.seq) {
            SeqOutcome::New | SeqOutcome::Reordered => {
                self.jitter.push(h.send_ns, recv_ns);
                self.acc.record(0, data.len() as u64, recv_ns);
            }
            SeqOutcome::Duplicate | SeqOutcome::OutOfRange => {}
        }
        true
    }

    /// Whether `seq` arrived.
    pub(crate) fn has(&self, seq: u64) -> bool {
        self.tracker.contains(seq)
    }

    /// Goodput, loss and jitter over the interval since the last call.
    pub(crate) fn live(&mut self, now: Instant) -> LiveUdp {
        let bytes = self.acc.bytes_total();
        let unique = self.tracker.unique();
        let expected = self.tracker.max_seen().map_or(0, |m| m + 1);
        let goodput_mbps = self.prev_at.and_then(|t| {
            stats::mbps(
                (bytes - self.prev_bytes) as f64,
                now.saturating_duration_since(t).as_nanos() as u64,
            )
        });
        let exp = expected.saturating_sub(self.prev_expected);
        let got = unique - self.prev_unique;
        let loss_pct =
            (exp > 0).then(|| ((1.0 - got as f64 / exp as f64) * 100.0).clamp(0.0, 100.0));
        self.prev_at = Some(now);
        self.prev_bytes = bytes;
        self.prev_unique = unique;
        self.prev_expected = self.prev_expected.max(expected);
        LiveUdp {
            goodput_mbps: (bytes > 0).then_some(goodput_mbps).flatten(),
            loss_pct,
            jitter_ms: self.jitter.jitter_ms(),
        }
    }

    /// The report against the sender's counts (§8.2 receiver definitions).
    pub(crate) fn report(
        &self,
        sender: &SenderReport,
        drained_by: &str,
        drain: Duration,
    ) -> ReceiverReport {
        let loss = self.tracker.loss(sender.sent);
        let bucket_loss = self
            .tracker
            .bucket_loss(&sender.bucket_first_seq, sender.sent)
            .into_iter()
            .map(|(missing, sent)| BucketLoss { missing, sent })
            .collect();
        let step_loss = sender
            .steps
            .iter()
            .map(|s| {
                let end = s.end_seq.min(sender.sent);
                let start = s.first_seq.min(end);
                BucketLoss {
                    missing: self.tracker.missing_in(start, end),
                    sent: end - start,
                }
            })
            .collect();
        ReceiverReport {
            unique: loss.unique,
            missing: loss.missing,
            dup: self.tracker.dup(),
            reordered: self.tracker.reordered(),
            out_of_range: self.tracker.out_of_range(),
            bad_datagrams: self.bad,
            loss_pct: loss.loss_pct,
            max_loss_burst: loss.max_loss_burst,
            jitter_ms: self.jitter.jitter_ms(),
            bucket_loss,
            step_loss,
            accounting: self.acc.stats(),
            drained_by: drained_by.to_string(),
            drain_ms: dur_ms(drain),
            rcvbuf_bytes: None,
            packets: None,
        }
    }
}

async fn recv_count_loop(
    sock: Arc<DatagramSocket>,
    gate: Gate,
    run_tag: u64,
    rx: Arc<Mutex<UdpReceiver>>,
    stop: CancellationToken,
) {
    let mut buf = vec![0u8; RECV_BUF];
    loop {
        let got = tokio::select! {
            _ = stop.cancelled() => break,
            r = recv_from(&sock, &mut buf) => r,
        };
        let Ok((n, from)) = got else {
            tokio::time::sleep(Duration::from_millis(1)).await;
            continue;
        };
        if !gate.admit(from) {
            continue;
        }
        let recv_ns = now_ns();
        if !lock(&rx).on_datagram(&buf[..n], run_tag, recv_ns, Instant::now()) {
            gate.count_bad();
        }
    }
}

/// Wait until `last_seq` arrived, or `quiet` passed without arrivals
/// (hard cap `quiet + 3 s`). Returns how it ended.
async fn drain_receiver(
    rx: &Mutex<UdpReceiver>,
    last_seq: Option<u64>,
    quiet: Duration,
) -> &'static str {
    let start = Instant::now();
    loop {
        {
            let r = lock(rx);
            if last_seq.is_some_and(|s| r.has(s)) {
                return "last_seq";
            }
            if r.last_arrival.map_or(start, |t| t.max(start)).elapsed() >= quiet {
                return "quiet";
            }
        }
        if start.elapsed() >= quiet + DRAIN_MAX {
            return "cap";
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// ---------------------------------------------------------------------------
// Echo
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct EchoCounters {
    received: AtomicU64,
    echoed: AtomicU64,
    echo_errors: AtomicU64,
    before_start: AtomicU64,
}

impl EchoCounters {
    fn stats(&self) -> EchoStats {
        EchoStats {
            received: self.received.load(Ordering::Relaxed),
            echoed: self.echoed.load(Ordering::Relaxed),
            echo_errors: self.echo_errors.load(Ordering::Relaxed),
            before_start: self.before_start.load(Ordering::Relaxed),
        }
    }
}

async fn echo_responder_loop(
    sock: Arc<DatagramSocket>,
    gate: Gate,
    run_tag: u64,
    to: SocketAddr,
    started: Arc<AtomicBool>,
    counters: Arc<EchoCounters>,
    stop: CancellationToken,
) {
    let mut buf = vec![0u8; RECV_BUF];
    loop {
        let got = tokio::select! {
            _ = stop.cancelled() => break,
            r = recv_from(&sock, &mut buf) => r,
        };
        let Ok((n, from)) = got else {
            tokio::time::sleep(Duration::from_millis(1)).await;
            continue;
        };
        if !gate.admit(from) {
            continue;
        }
        if DatagramHeader::decode_for(&buf[..n], run_tag).is_err() {
            gate.count_bad();
            continue;
        }
        counters.received.fetch_add(1, Ordering::Relaxed);
        if !started.load(Ordering::SeqCst) {
            counters.before_start.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        match send_to(&sock, &buf[..n], to).await {
            Ok(()) => counters.echoed.fetch_add(1, Ordering::Relaxed),
            Err(_) => counters.echo_errors.fetch_add(1, Ordering::Relaxed),
        };
    }
}

/// The echo initiator's view: round trips by seq.
#[derive(Debug, Default)]
pub(crate) struct EchoState {
    rtt: Vec<Option<f64>>,
    limit: u64,
    warmup: u64,
    dup: u64,
    late: u64,
    echoed: u64,
    closed: bool,
    live: LiveRtt,
}

impl EchoState {
    pub(crate) fn new(limit: u64, warmup: u64) -> Self {
        Self {
            limit,
            warmup,
            ..Self::default()
        }
    }

    /// An echo of `seq` with round trip `rtt_ms`.
    pub(crate) fn on_echo(&mut self, seq: u64, rtt_ms: f64) {
        if self.closed {
            self.late += 1;
            return;
        }
        if seq >= self.limit {
            return;
        }
        let i = seq as usize;
        if i >= self.rtt.len() {
            self.rtt.resize(i + 1, None);
        }
        if self.rtt[i].is_some() {
            self.dup += 1;
            return;
        }
        self.rtt[i] = Some(rtt_ms);
        self.echoed += 1;
        if seq >= self.warmup {
            self.live.push(rtt_ms, false);
        }
    }

    /// Measured seqs sent before `cutoff_ns` and not echoed, over all
    /// measured seqs sent before it (live loss).
    pub(crate) fn live_loss(&self, send_ns: &[u64], cutoff_ns: u64) -> Option<f64> {
        let (mut due, mut lost) = (0u64, 0u64);
        for (seq, t) in send_ns.iter().enumerate().skip(self.warmup as usize) {
            if *t >= cutoff_ns {
                break;
            }
            due += 1;
            if self.rtt.get(seq).copied().flatten().is_none() {
                lost += 1;
            }
        }
        stats::pct(lost as f64, due as f64)
    }

    /// Samples of measured seqs `warmup..sent` (`None` = lost).
    pub(crate) fn samples(&self, sent: u64) -> Vec<Option<f64>> {
        (self.warmup..sent)
            .map(|s| self.rtt.get(s as usize).copied().flatten())
            .collect()
    }
}

async fn echo_initiator_loop(
    sock: Arc<DatagramSocket>,
    gate: Gate,
    run_tag: u64,
    state: Arc<Mutex<EchoState>>,
    stop: CancellationToken,
) {
    let mut buf = vec![0u8; RECV_BUF];
    loop {
        let got = tokio::select! {
            _ = stop.cancelled() => break,
            r = recv_from(&sock, &mut buf) => r,
        };
        let Ok((n, from)) = got else {
            tokio::time::sleep(Duration::from_millis(1)).await;
            continue;
        };
        let recv_ns = now_ns();
        if !gate.admit(from) {
            continue;
        }
        let Ok(h) = DatagramHeader::decode_for(&buf[..n], run_tag) else {
            gate.count_bad();
            continue;
        };
        let rtt_ms = recv_ns.saturating_sub(h.send_ns) as f64 / 1e6;
        lock(&state).on_echo(h.seq, rtt_ms);
    }
}

// ---------------------------------------------------------------------------
// Building the output (pure; unit-tested)
// ---------------------------------------------------------------------------

/// Everything [`build_stream`] needs besides the two reports.
struct StreamInputs<'a> {
    p: &'a UdpParams,
    origin_ns: u64,
    /// Sender time → initiator time.
    sender_to_local: &'a dyn Fn(u64) -> u64,
    /// Receiver time → initiator time.
    recv_to_local: &'a dyn Fn(u64) -> u64,
    foreign_dropped: u64,
}

/// The per-send-bucket series (offered or loss) at bucket ends.
fn send_bucket_series(
    sender: &SenderReport,
    to_local: &dyn Fn(u64) -> u64,
    origin_ns: u64,
    key: &str,
    label: &str,
    unit: &str,
    value: impl Fn(usize, u64) -> Option<f64>,
) -> Series {
    let rel_ms = |t: u64| (t as f64 - origin_ns as f64) / 1e6;
    let n = sender.bucket_first_seq.len();
    let mut points = Vec::with_capacity(n);
    for b in 0..n {
        let start = sender.start_ns + BUCKET_NS * b as u64;
        let end = if b + 1 == n {
            sender.end_ns.max(start)
        } else {
            start + BUCKET_NS
        };
        let len = end - start;
        if b + 1 == n && len < MIN_FINAL_BUCKET_NS {
            continue;
        }
        if let Some(v) = value(b, len) {
            points.push((rel_ms(to_local(end)), v));
        }
    }
    Series {
        key: key.into(),
        label: label.into(),
        unit: unit.into(),
        points,
        warmup_ms: None,
    }
}

fn build_stream(
    inp: &StreamInputs<'_>,
    sender: &SenderReport,
    recv: &ReceiverReport,
) -> Result<WorkloadOutput, AppError> {
    let p = inp.p;
    let w = p.warmup_secs;
    let warmup_ns = u64::from(w) * 1_000_000_000;
    let ramp = p.mode == UdpMode::Ramp;
    let acc = &recv.accounting;
    let goodput = if acc.bytes_total == 0 {
        None
    } else {
        match acc.goodput(w) {
            Ok(g) => Some(g),
            Err(e) if !ramp => return Err(e),
            Err(_) => None,
        }
    };
    let mut notes = vec![NOTE_IPV4.to_string(), NOTE_LOSS.to_string()];
    if p.payload_bytes as usize + IP_UDP_HEADERS > TUNNEL_MTU {
        notes.push(NOTE_FRAGMENTS.into());
    }
    if p.mode == UdpMode::Stream && !sender.rate_achieved {
        notes.push(NOTE_NOT_ACHIEVED.into());
    }
    if acc.bytes_total == 0 {
        notes.push("nothing arrived: every datagram was lost".into());
    }
    let loss_locality = match (sender.packets, recv.packets) {
        (Some(s), Some(r)) => {
            notes.push(NOTE_LOCALITY.into());
            Some(loss_locality(
                sender.sent,
                recv.unique,
                s,
                r,
                p.payload_bytes as usize,
            ))
        }
        _ => None,
    };
    // Series: goodput by arrival, offered and loss by send bucket.
    let mut goodput_series =
        acc.map_time(inp.recv_to_local)
            .series(inp.origin_ns, w, "goodput_mbps", "Goodput");
    if acc.bytes_total == 0 {
        goodput_series.points.clear();
    }
    let rel_ms = |t: u64| (t as f64 - inp.origin_ns as f64) / 1e6;
    let send_warm = Some(rel_ms((inp.sender_to_local)(sender.start_ns + warmup_ns)));
    let mut offered = send_bucket_series(
        sender,
        inp.sender_to_local,
        inp.origin_ns,
        "offered_mbps",
        "Offered",
        "Mb/s",
        |b, len| stats::mbps(sender.bucket_bytes.get(b).copied().unwrap_or(0) as f64, len),
    );
    offered.warmup_ms = send_warm;
    let mut loss = send_bucket_series(
        sender,
        inp.sender_to_local,
        inp.origin_ns,
        "loss_pct",
        "Loss",
        "%",
        |b, _| recv.bucket_loss.get(b).and_then(BucketLoss::pct),
    );
    loss.warmup_ms = send_warm;
    let series = if ramp {
        vec![offered, goodput_series, loss]
    } else {
        vec![goodput_series, offered, loss]
    };

    let (window_ns, goodput_bytes, window_ms) = match goodput {
        Some(g) => (
            (
                (inp.recv_to_local)(g.start_ns),
                (inp.recv_to_local)(g.end_ns),
            ),
            g.bytes,
            (g.end_ns - g.start_ns) / 1_000_000,
        ),
        None => {
            let s = (inp.sender_to_local)(sender.start_ns + warmup_ns);
            let e = (inp.sender_to_local)(sender.end_ns).max(s);
            ((s, e), 0, (e - s) / 1_000_000)
        }
    };
    let stream = StreamResults {
        direction: p.direction,
        payload_bytes: p.payload_bytes,
        target_mbps: sender.target_mbps,
        sent: sender.sent,
        unique: recv.unique,
        missing: recv.missing,
        dup: recv.dup,
        reordered: recv.reordered,
        out_of_range: recv.out_of_range,
        bad_datagrams: recv.bad_datagrams,
        send_errors: sender.send_errors,
        loss_pct: recv.loss_pct,
        max_loss_burst: recv.max_loss_burst,
        jitter_ms: recv.jitter_ms,
        goodput_mbps: goodput
            .map(|g| g.mbps)
            .or((acc.bytes_total == 0).then_some(0.0)),
        offered_mbps: sender.offered_mbps,
        rate_achieved: sender.rate_achieved,
        max_burst_datagrams: sender.max_burst_datagrams,
        pacer_late_ms_mean: sender.pacer_late_ms_mean,
        sent_bytes: sender.bytes_sent,
        received_bytes: acc.bytes_total,
        goodput_bytes,
        window_ms,
        bucket_first_seq: sender.bucket_first_seq.clone(),
        bucket_loss: recv.bucket_loss.clone(),
        receiver: acc.clone(),
        loss_locality,
        drain_ms: recv.drain_ms,
        drained_by: recv.drained_by.clone(),
        rcvbuf_bytes: recv.rcvbuf_bytes,
        foreign_dropped: inp.foreign_dropped,
    };
    if !ramp {
        let summary = vec![
            item(
                "goodput_mbps",
                "Goodput",
                stream.goodput_mbps,
                "Mb/s",
                Better::Higher,
            ),
            item(
                "offered_mbps",
                "Offered",
                stream.offered_mbps,
                "Mb/s",
                Better::Higher,
            ),
            item("loss_pct", "Loss", stream.loss_pct, "%", Better::Lower),
            item("jitter_ms", "Jitter", stream.jitter_ms, "ms", Better::Lower),
            cpu_per_mb_item(),
        ];
        return Ok(WorkloadOutput {
            summary,
            results: to_value(&Results::Stream(Box::new(stream)))?,
            series,
            notes,
            window_ns: Some(window_ns),
            goodput_bytes: Some(goodput_bytes),
        });
    }
    // Ramp: exact step loss, then the last sustained rate.
    let stop_reason = sender
        .stop_reason
        .clone()
        .unwrap_or_else(|| "the run ended first".into());
    let stopped_by_loss = stop_reason.starts_with("loss above");
    let mut steps = sender.steps.clone();
    for (i, s) in steps.iter_mut().enumerate() {
        s.loss_pct = recv.step_loss.get(i).and_then(BucketLoss::pct);
    }
    // Did the ramp find a limit, or did the run's length cap it?
    let measured = |s: &RampStep| s.complete || s.end_ms - s.start_ms >= 1000.0;
    let limit_found = stopped_by_loss
        || stop_reason.starts_with("the sender could not")
        || steps
            .iter()
            .any(|s| measured(s) && (!s.achieved || s.loss_pct.is_some_and(|l| l > RAMP_LOSS_PCT)));
    let last = steps.len().saturating_sub(1);
    let mut max_sustainable = None;
    let mut broken = false;
    for (i, s) in steps.iter_mut().enumerate() {
        // Without a limit, a final step cut short by the end of the run
        // never counts: whether it ran 999 or 1001 ms is timer jitter.
        let long_enough = s.complete || (limit_found && measured(s));
        s.sustained = !broken
            && long_enough
            && s.achieved
            && s.loss_pct.is_some_and(|l| l <= RAMP_LOSS_PCT)
            && !(stopped_by_loss && i == last);
        if s.sustained {
            max_sustainable = Some(s.rate_mbps);
        } else if long_enough || i == last {
            broken = true;
        }
    }
    notes.push(format!("ramp stopped: {stop_reason}"));
    if max_sustainable.is_none() {
        notes.push("no step held loss at or under 2%".into());
    } else if !limit_found {
        notes.push(
            "the ramp ended before reaching a loss limit: max_sustainable_mbps is a lower bound \
             (raise duration_secs or rate_mbps)"
                .into(),
        );
    }
    let peak_offered = steps
        .iter()
        .filter_map(|s| s.offered_mbps)
        .fold(None, |m: Option<f64>, v| Some(m.map_or(v, |m| m.max(v))));
    let summary = vec![
        item(
            "max_sustainable_mbps",
            "Highest rate at ≤ 2% loss",
            // A ceiling set by the run's length is not a capacity.
            max_sustainable.filter(|_| limit_found),
            "Mb/s",
            Better::Higher,
        ),
        item(
            "offered_mbps",
            "Highest offered",
            peak_offered,
            "Mb/s",
            Better::Higher,
        ),
        item("loss_pct", "Loss", stream.loss_pct, "%", Better::Lower),
    ];
    Ok(WorkloadOutput {
        summary,
        results: to_value(&Results::Ramp(Box::new(RampResults {
            stream,
            steps,
            stop_reason,
            max_sustainable_mbps: max_sustainable,
            limit_found,
        })))?,
        series,
        notes,
        window_ns: Some(window_ns),
        goodput_bytes: Some(goodput_bytes),
    })
}

// ---------------------------------------------------------------------------
// Initiator
// ---------------------------------------------------------------------------

fn echoed(ctx: &RunCtx<'_>, p: &UdpParams) -> UdpParams {
    match ctx.params() {
        WorkloadParams::Udp(e) => e.clone(),
        _ => p.clone(),
    }
}

/// Initiator side (§6 "Initiator flow" step 7).
pub async fn run(ctx: &mut RunCtx<'_>, p: &UdpParams) -> Result<WorkloadOutput, AppError> {
    // Bind before prepare; advertise the tailnet port.
    let sock = Arc::new(
        ctx.node
            .bind_udp(0)
            .await
            .map_err(|e| AppError::from(e).context("bind_udp"))?,
    );
    let udp_port = advertised_port(&sock)?;
    let receives = p.mode == UdpMode::Echo || p.direction == OneWayDirection::Download;
    let rcvbuf = receives.then(|| raise_rcvbuf(&sock)).flatten();
    let setup: Setup = ctx.prepare(&Client { udp_port }).await?;
    let p = echoed(ctx, p);
    match (p.mode, p.direction) {
        (UdpMode::Echo, _) => run_echo(ctx, &p, sock, &setup, rcvbuf).await,
        (_, OneWayDirection::Upload) => run_upload(ctx, &p, sock, &setup).await,
        (_, OneWayDirection::Download) => run_download(ctx, &p, sock, rcvbuf).await,
    }
}

async fn run_upload(
    ctx: &mut RunCtx<'_>,
    p: &UdpParams,
    sock: Arc<DatagramSocket>,
    setup: &Setup,
) -> Result<WorkloadOutput, AppError> {
    let run_tag = ctx.run_tag()?;
    let target = v4_target(ctx.peer_ip(), setup.udp_port)?;
    let drain = drain_period(ctx.before_rtt());
    let ramp = p.mode == UdpMode::Ramp;
    ctx.clock().await?;
    let metrics_before = read_metrics(&ctx.node).await;
    ctx.start().await?;
    let plan = SendPlan::new(p, run_tag, target);
    let live = SendLive::new(plan.rate_mbps);
    let mut sender: Tasks<SenderReport> = Tasks::new();
    sender.push(tokio::spawn(send_loop(sock.clone(), plan, live.clone())));
    let started = Instant::now();
    let warm_end = started + plan.warmup;
    let hard_end = started + plan.warmup + plan.duration + Duration::from_secs(10);
    let mut in_warmup = !plan.warmup.is_zero();
    ctx.phase(if in_warmup {
        Phase::Warmup
    } else {
        Phase::Running
    });
    let mut ctl = RampControl::default();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last = (Instant::now(), 0u64);
    let report = {
        let wait = async { sender.join().await.pop() };
        tokio::pin!(wait);
        loop {
            tokio::select! {
                r = &mut wait => break r,
                _ = tokio::time::sleep_until(warm_end), if in_warmup => {
                    in_warmup = false;
                    ctx.phase(Phase::Running);
                }
                _ = tokio::time::sleep_until(hard_end) => {
                    return Err(AppError::timeout("the udp sender did not stop on time"));
                }
                _ = tick.tick() => {
                    let now = (Instant::now(), live.bytes.load(Ordering::Relaxed));
                    let dt = now.0.saturating_duration_since(last.0).as_nanos() as u64;
                    if let Some(v) = stats::mbps((now.1 - last.1) as f64, dt) {
                        if now.1 > 0 {
                            ctx.sample("offered_mbps", v);
                        }
                    }
                    last = now;
                    while let Some(t) = ctx.try_tick() {
                        if !ramp || in_warmup {
                            continue;
                        }
                        if let Some(l) = t.values.get("loss_pct") {
                            if ctl.on_sample(*l, live.target()) {
                                live.request_stop(ctl.reason());
                            }
                        }
                    }
                    if ramp {
                        ctx.sample("target_mbps", live.target());
                        if let Some(b) = ctl.best() {
                            ctx.sample("max_sustainable_mbps", b);
                        }
                    }
                }
            }
        }
    };
    let mut report = match report {
        Some(Ok(r)) => r,
        Some(Err(e)) => return Err(AppError::internal(format!("the udp sender failed: {e}"))),
        None => return Err(AppError::internal("the udp sender did not run")),
    };
    // The sender waits D after its last datagram before `finish`.
    ctx.phase(Phase::Draining);
    tokio::time::sleep(drain).await;
    report.packets = packet_delta(metrics_before, read_metrics(&ctx.node).await);
    ctx.set_partial(sender_partial(p, &report, ctx.origin_ns()));
    ctx.phase(Phase::Finishing);
    let stats: Stats = ctx
        .finish(
            &Info {
                sender: Some(report.clone()),
                stop_reason: None,
                drain_ms: dur_ms(drain),
            },
            drain + DRAIN_MAX + Duration::from_secs(2),
        )
        .await?;
    ctx.clock().await?;
    let recv = stats.receiver.ok_or_else(|| {
        AppError::new(
            ErrorCode::ProtocolMismatch,
            "finish: the responder returned no receiver report",
        )
    })?;
    let map = ctx.clock_map();
    let to_local = move |t: u64| map.map_or(t, |m| m.to_local(t));
    let ident = |t: u64| t;
    build_stream(
        &StreamInputs {
            p,
            origin_ns: ctx.origin_ns(),
            sender_to_local: &ident,
            recv_to_local: &to_local,
            foreign_dropped: stats.foreign_dropped,
        },
        &report,
        &recv,
    )
}

async fn run_download(
    ctx: &mut RunCtx<'_>,
    p: &UdpParams,
    sock: Arc<DatagramSocket>,
    rcvbuf: Option<u64>,
) -> Result<WorkloadOutput, AppError> {
    let run_tag = ctx.run_tag()?;
    let drain = drain_period(ctx.before_rtt());
    let ramp = p.mode == UdpMode::Ramp;
    let rx = Arc::new(Mutex::new(UdpReceiver::new(seq_limit(p))));
    let foreign = Arc::new(AtomicU64::new(0));
    let stop = CancellationToken::new();
    let _stop_guard = stop.clone().drop_guard();
    let mut tasks: Tasks<()> = Tasks::new();
    tasks.push(tokio::spawn(recv_count_loop(
        sock.clone(),
        Gate::Initiator {
            ip: ctx.peer_ip(),
            foreign: foreign.clone(),
        },
        run_tag,
        rx.clone(),
        stop.clone(),
    )));
    ctx.clock().await?;
    let metrics_before = read_metrics(&ctx.node).await;
    ctx.start().await?;
    let started = Instant::now();
    let warm_end = started + secs(p.warmup_secs);
    let data_end = started + secs(p.warmup_secs + p.duration_secs);
    let mut in_warmup = p.warmup_secs > 0;
    ctx.phase(if in_warmup {
        Phase::Warmup
    } else {
        Phase::Running
    });
    let mut ctl = RampControl::default();
    let mut stop_reason = None;
    let mut target = p.rate_mbps;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(warm_end), if in_warmup => {
                in_warmup = false;
                ctx.phase(Phase::Running);
            }
            _ = tokio::time::sleep_until(data_end) => break,
            _ = tick.tick() => {
                let l = lock(&rx).live(Instant::now());
                for (k, v) in [
                    ("goodput_mbps", l.goodput_mbps),
                    ("loss_pct", l.loss_pct),
                    ("jitter_ms", l.jitter_ms),
                ] {
                    if let Some(v) = v {
                        ctx.sample(k, v);
                    }
                }
                while let Some(t) = ctx.try_tick() {
                    if let Some(v) = t.values.get("target_mbps") {
                        target = *v;
                    }
                }
                if ramp && !in_warmup {
                    if let Some(loss) = l.loss_pct {
                        if ctl.on_sample(loss, target) {
                            stop_reason = ctl.reason();
                            break;
                        }
                    }
                    if let Some(b) = ctl.best() {
                        ctx.sample("max_sustainable_mbps", b);
                    }
                }
            }
        }
    }
    ctx.phase(Phase::Draining);
    let origin = ctx.origin_ns();
    let partial = receiver_partial(p, &lock(&rx), origin);
    ctx.set_partial(partial);
    let stats: Stats = ctx
        .finish(
            &Info {
                sender: None,
                stop_reason,
                drain_ms: dur_ms(drain),
            },
            drain + Duration::from_secs(8),
        )
        .await?;
    let sender = stats.sender.ok_or_else(|| {
        AppError::new(
            ErrorCode::ProtocolMismatch,
            "finish: the responder returned no sender report",
        )
    })?;
    check_sender_report(&sender, p).map_err(|e| {
        AppError::new(
            ErrorCode::ProtocolMismatch,
            format!("finish: the responder's sender report is not plausible: {e}"),
        )
    })?;
    let drained_by = drain_receiver(&rx, sender.last_seq, drain).await;
    let packets = packet_delta(metrics_before, read_metrics(&ctx.node).await);
    stop.cancel();
    drop(tasks);
    ctx.phase(Phase::Finishing);
    ctx.clock().await?;
    let mut recv = lock(&rx).report(&sender, drained_by, drain);
    recv.rcvbuf_bytes = rcvbuf;
    recv.packets = packets;
    let map = ctx.clock_map();
    let to_local = move |t: u64| map.map_or(t, |m| m.to_local(t));
    let ident = |t: u64| t;
    build_stream(
        &StreamInputs {
            p,
            origin_ns: ctx.origin_ns(),
            sender_to_local: &to_local,
            recv_to_local: &ident,
            foreign_dropped: stats.foreign_dropped + foreign.load(Ordering::Relaxed),
        },
        &sender,
        &recv,
    )
}

/// What a failed download keeps: the local receiver's goodput series and counts.
fn receiver_partial(p: &UdpParams, rx: &UdpReceiver, origin_ns: u64) -> WorkloadOutput {
    let acc = rx.acc.stats();
    let goodput = acc.goodput(p.warmup_secs).ok().map(|g| g.mbps);
    WorkloadOutput {
        summary: vec![item(
            "goodput_mbps",
            "Goodput",
            goodput,
            "Mb/s",
            Better::Higher,
        )],
        results: serde_json::json!({
            "mode": p.mode,
            "direction": p.direction,
            "unique": rx.tracker.unique(),
            "received_bytes": acc.bytes_total,
        }),
        series: vec![acc.series(origin_ns, p.warmup_secs, "goodput_mbps", "Goodput")],
        notes: Vec::new(),
        window_ns: None,
        goodput_bytes: None,
    }
}

/// What a failed upload keeps: the offered series and the sender's counts.
fn sender_partial(p: &UdpParams, s: &SenderReport, origin_ns: u64) -> WorkloadOutput {
    let ident = |t: u64| t;
    let offered = send_bucket_series(
        s,
        &ident,
        origin_ns,
        "offered_mbps",
        "Offered",
        "Mb/s",
        |b, len| stats::mbps(s.bucket_bytes.get(b).copied().unwrap_or(0) as f64, len),
    );
    WorkloadOutput {
        summary: vec![item(
            "offered_mbps",
            "Offered",
            s.offered_mbps,
            "Mb/s",
            Better::Higher,
        )],
        results: serde_json::json!({
            "mode": p.mode,
            "direction": p.direction,
            "sent": s.sent,
            "send_errors": s.send_errors,
            "offered_mbps": s.offered_mbps,
            "rate_achieved": s.rate_achieved,
        }),
        series: vec![offered],
        notes: Vec::new(),
        window_ns: None,
        goodput_bytes: None,
    }
}

async fn run_echo(
    ctx: &mut RunCtx<'_>,
    p: &UdpParams,
    sock: Arc<DatagramSocket>,
    setup: &Setup,
    rcvbuf: Option<u64>,
) -> Result<WorkloadOutput, AppError> {
    let run_tag = ctx.run_tag()?;
    let target = v4_target(ctx.peer_ip(), setup.udp_port)?;
    let drain = drain_period(ctx.before_rtt());
    let warmup = u64::from(p.warmup_count);
    let state = Arc::new(Mutex::new(EchoState::new(seq_limit(p), warmup)));
    let foreign = Arc::new(AtomicU64::new(0));
    let stop = CancellationToken::new();
    let _stop_guard = stop.clone().drop_guard();
    let mut tasks: Tasks<()> = Tasks::new();
    tasks.push(tokio::spawn(echo_initiator_loop(
        sock.clone(),
        Gate::Initiator {
            ip: ctx.peer_ip(),
            foreign: foreign.clone(),
        },
        run_tag,
        state.clone(),
        stop.clone(),
    )));
    ctx.start().await?;
    let plan = SendPlan::new(p, run_tag, target);
    let live = SendLive::new(plan.rate_mbps);
    let mut sender: Tasks<SenderReport> = Tasks::new();
    sender.push(tokio::spawn(send_loop(sock.clone(), plan, live.clone())));
    let started = Instant::now();
    let hard_end = started + plan.duration + Duration::from_secs(10);
    ctx.phase(if warmup > 0 {
        Phase::Warmup
    } else {
        Phase::Running
    });
    let mut running = warmup == 0;
    let drain_ns = drain.as_nanos() as u64;
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let total = u64::from(p.pps) * u64::from(p.duration_secs);
    let report = {
        let wait = async { sender.join().await.pop() };
        tokio::pin!(wait);
        loop {
            tokio::select! {
                r = &mut wait => break r,
                _ = tokio::time::sleep_until(hard_end) => {
                    return Err(AppError::timeout("the udp sender did not stop on time"));
                }
                _ = tick.tick() => {
                    let sends = lock(&live.send_ns).clone();
                    let (interval, p50, loss) = {
                        let mut st = lock(&state);
                        let (i, m) = st.live.take();
                        (i, m, st.live_loss(&sends, now_ns().saturating_sub(drain_ns)))
                    };
                    if !running && sends.len() as u64 > warmup {
                        running = true;
                        ctx.phase(Phase::Running);
                    }
                    for (k, v) in [("rtt_ms", interval), ("rtt_p50_ms", p50), ("loss_pct", loss)] {
                        if let Some(v) = v {
                            ctx.sample(k, v);
                        }
                    }
                    if total > 0 {
                        ctx.fraction((sends.len() as f32 / total as f32).min(1.0));
                    }
                }
            }
        }
    };
    let report = match report {
        Some(Ok(r)) => r,
        Some(Err(e)) => return Err(AppError::internal(format!("the udp sender failed: {e}"))),
        None => return Err(AppError::internal("the udp sender did not run")),
    };
    // Echoes count until D after the last send.
    ctx.phase(Phase::Draining);
    let deadline = Instant::now() + drain;
    loop {
        let all = lock(&state).echoed >= report.sent;
        if all || Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    lock(&state).closed = true;
    let inputs = EchoInputs {
        p,
        report: &report,
        origin_ns: ctx.origin_ns(),
        sends: lock(&live.send_ns).clone(),
        payload: plan.payload,
        drain,
        rcvbuf,
    };
    // If finish fails, the report keeps what this side measured.
    let partial = echo_output(
        &inputs,
        &lock(&state),
        EchoStats::default(),
        foreign.load(Ordering::Relaxed),
    )?;
    ctx.set_partial(partial);
    ctx.phase(Phase::Finishing);
    let stats: Stats = ctx
        .finish(
            &Info {
                sender: Some(report.clone()),
                stop_reason: None,
                drain_ms: dur_ms(drain),
            },
            Duration::from_secs(5),
        )
        .await?;
    // Late echoes still arriving are counted (as late) until here.
    stop.cancel();
    drop(tasks);
    let st = lock(&state);
    echo_output(
        &inputs,
        &st,
        stats.echo.unwrap_or_default(),
        stats.foreign_dropped + foreign.load(Ordering::Relaxed),
    )
}

/// What [`echo_output`] needs besides the echo state.
struct EchoInputs<'a> {
    p: &'a UdpParams,
    report: &'a SenderReport,
    origin_ns: u64,
    /// `send_ns` per seq.
    sends: Vec<u64>,
    payload: usize,
    drain: Duration,
    rcvbuf: Option<u64>,
}

fn echo_output(
    inp: &EchoInputs<'_>,
    st: &EchoState,
    responder: EchoStats,
    foreign_dropped: u64,
) -> Result<WorkloadOutput, AppError> {
    let p = inp.p;
    let warmup = u64::from(p.warmup_count);
    let samples = st.samples(inp.report.sent);
    let dist = Distribution::from_samples(&samples);
    let measured: Vec<f64> = samples.iter().filter_map(|s| *s).collect();
    let lost = samples.iter().filter(|s| s.is_none()).count() as u64;
    let mut points = Vec::new();
    let mut warmup_ms = None;
    for (seq, t) in inp.sends.iter().enumerate() {
        let t_ms = (*t as f64 - inp.origin_ns as f64) / 1e6;
        if (seq as u64) < warmup {
            warmup_ms = Some(t_ms);
        }
        if let Some(Some(rtt)) = st.rtt.get(seq) {
            points.push((t_ms, *rtt));
        }
    }
    let results = EchoResults {
        payload_bytes: p.payload_bytes,
        pps: p.pps,
        warmup_count: p.warmup_count,
        sent: inp.report.sent,
        completed: measured.len() as u64,
        lost,
        loss_pct: stats::pct(lost as f64, samples.len() as f64),
        dup: st.dup,
        late: st.late,
        send_errors: inp.report.send_errors,
        rtt_ms: dist,
        samples_ms: measured,
        offered_pps: inp
            .report
            .offered_mbps
            .map(|m| m * 1e6 / 8.0 / inp.payload as f64),
        drain_ms: dur_ms(inp.drain),
        responder,
        rcvbuf_bytes: inp.rcvbuf,
        foreign_dropped,
    };
    let mut notes = vec![NOTE_IPV4.to_string(), NOTE_LOSS.to_string()];
    if p.payload_bytes as usize + IP_UDP_HEADERS > TUNNEL_MTU {
        notes.push(NOTE_FRAGMENTS.into());
    }
    Ok(WorkloadOutput {
        summary: echo_summary(&results),
        results: to_value(&Results::Echo(Box::new(results)))?,
        series: vec![Series {
            key: "rtt_ms".into(),
            label: "Round trip".into(),
            unit: "ms".into(),
            points,
            warmup_ms,
        }],
        notes,
        window_ns: None,
        goodput_bytes: None,
    })
}

fn echo_summary(r: &EchoResults) -> Vec<SummaryItem> {
    vec![
        item(
            "rtt_p50_ms",
            "Round trip p50",
            r.rtt_ms.p50,
            "ms",
            Better::Lower,
        ),
        item(
            "rtt_p99_ms",
            "Round trip p99",
            r.rtt_ms.p99,
            "ms",
            Better::Lower,
        ),
        item("loss_pct", "Lost", r.loss_pct, "%", Better::Lower),
    ]
}

// ---------------------------------------------------------------------------
// Responder
// ---------------------------------------------------------------------------

struct UdpRun {
    r: ResponderCtx,
    p: UdpParams,
    sock: Arc<DatagramSocket>,
    to: SocketAddr,
    rcvbuf: Option<u64>,
    metrics_before: Option<UserMetrics>,
    rx: Option<Arc<Mutex<UdpReceiver>>>,
    echo: Option<Arc<EchoCounters>>,
    started: Arc<AtomicBool>,
    stop: CancellationToken,
    tasks: Tasks<()>,
    sender: Option<(JoinHandle<SenderReport>, Arc<SendLive>)>,
}

/// Responder side: bind, start receiving (upload, echo), and return the
/// socket's tailnet port.
pub async fn prepare(
    r: &ResponderCtx,
    p: &UdpParams,
    client: Client,
) -> Result<(Setup, Box<dyn ResponderRun>), AppError> {
    let to = v4_target(r.initiator.ip, client.udp_port)?;
    if client.udp_port == 0 {
        return Err(AppError::invalid_params("udp: the client sent no udp_port"));
    }
    let sock = Arc::new(
        r.node
            .bind_udp(0)
            .await
            .map_err(|e| AppError::from(e).context("bind_udp"))?,
    );
    let udp_port = advertised_port(&sock)?;
    let receives = p.mode != UdpMode::Echo && p.direction == OneWayDirection::Upload;
    let rcvbuf = (receives || p.mode == UdpMode::Echo)
        .then(|| raise_rcvbuf(&sock))
        .flatten();
    let metrics_before = read_metrics(&r.node).await;
    let stop = CancellationToken::new();
    let mut tasks: Tasks<()> = Tasks::new();
    let started = Arc::new(AtomicBool::new(false));
    let (mut rx, mut echo) = (None, None);
    if p.mode == UdpMode::Echo {
        let counters = Arc::new(EchoCounters::default());
        tasks.push(tokio::spawn(echo_responder_loop(
            sock.clone(),
            Gate::Responder(Box::new(r.clone())),
            r.run_tag,
            to,
            started.clone(),
            counters.clone(),
            stop.clone(),
        )));
        echo = Some(counters);
    } else if receives {
        let state = Arc::new(Mutex::new(UdpReceiver::new(seq_limit(p))));
        tasks.push(tokio::spawn(recv_count_loop(
            sock.clone(),
            Gate::Responder(Box::new(r.clone())),
            r.run_tag,
            state.clone(),
            stop.clone(),
        )));
        rx = Some(state);
    }
    Ok((
        Setup {
            udp_port,
            rcvbuf_bytes: rcvbuf,
        },
        Box::new(UdpRun {
            r: r.clone(),
            p: p.clone(),
            sock,
            to,
            rcvbuf,
            metrics_before,
            rx,
            echo,
            started,
            stop,
            tasks,
            sender: None,
        }),
    ))
}

impl ResponderRun for UdpRun {
    fn start(&mut self) -> BoxFuture<'_, Result<(), AppError>> {
        Box::pin(async move {
            // Once only (the hub also drops duplicate starts): a second
            // start must never spawn a second full-rate sender.
            if self.started.swap(true, Ordering::SeqCst) {
                return Ok(());
            }
            if let Some(rx) = &self.rx {
                // Receiver-side live values go to the initiator as ticks.
                let (r, rx) = (self.r.clone(), rx.clone());
                self.tasks.push(tokio::spawn(async move {
                    let mut tick = tokio::time::interval(Duration::from_millis(250));
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tick.tick().await;
                        let l = lock(&rx).live(Instant::now());
                        let values: Vec<(&str, f64)> = [
                            ("goodput_mbps", l.goodput_mbps),
                            ("loss_pct", l.loss_pct),
                            ("jitter_ms", l.jitter_ms),
                        ]
                        .into_iter()
                        .filter_map(|(k, v)| v.map(|v| (k, v)))
                        .collect();
                        if !values.is_empty() {
                            r.tick(values).await;
                        }
                    }
                }));
            } else if self.p.mode != UdpMode::Echo {
                // Download: send only now, only to the initiator.
                let plan = SendPlan::new(&self.p, self.r.run_tag, self.to);
                let live = SendLive::new(plan.rate_mbps);
                let handle = tokio::spawn(send_loop(self.sock.clone(), plan, live.clone()));
                let (r, l2) = (self.r.clone(), live.clone());
                self.tasks.push(tokio::spawn(async move {
                    let mut tick = tokio::time::interval(Duration::from_millis(250));
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    let mut last = (Instant::now(), 0u64);
                    loop {
                        tick.tick().await;
                        let now = (Instant::now(), l2.bytes.load(Ordering::Relaxed));
                        let dt = now.0.saturating_duration_since(last.0).as_nanos() as u64;
                        let offered = stats::mbps((now.1 - last.1) as f64, dt).unwrap_or(0.0);
                        last = now;
                        r.tick([("offered_mbps", offered), ("target_mbps", l2.target())])
                            .await;
                    }
                }));
                self.sender = Some((handle, live));
            }
            Ok(())
        })
    }

    fn finish(self: Box<Self>, info: Value) -> BoxFuture<'static, Result<Value, AppError>> {
        Box::pin(async move {
            let UdpRun {
                r,
                p,
                sock,
                rcvbuf,
                metrics_before,
                rx,
                echo,
                started,
                stop,
                tasks,
                sender,
                ..
            } = *self;
            let info: Info = serde_json::from_value(info)
                .map_err(|e| AppError::invalid_params(format!("udp finish info: {e}")))?;
            let drain =
                Duration::from_secs_f64((info.drain_ms / 1000.0).clamp(0.0, 3.0)).max(DRAIN_MIN);
            let mut out = Stats::default();
            if let Some(rx) = rx {
                let s = info.sender.ok_or_else(|| {
                    AppError::invalid_params("udp finish: no sender report from the initiator")
                })?;
                check_sender_report(&s, &p).map_err(|e| {
                    AppError::invalid_params(format!("udp finish: implausible sender report: {e}"))
                })?;
                let drained_by = drain_receiver(&rx, s.last_seq, drain).await;
                let packets = packet_delta(metrics_before, read_metrics(&r.node).await);
                stop.cancel();
                let mut report = lock(&rx).report(&s, drained_by, drain);
                report.rcvbuf_bytes = rcvbuf;
                report.packets = packets;
                if let Ok(g) = report.accounting.goodput(p.warmup_secs) {
                    r.set_resource_window(g.start_ns, g.end_ns, Some(g.bytes));
                }
                out.receiver = Some(report);
            } else if let Some((handle, live)) = sender {
                live.request_stop(info.stop_reason);
                let mut handle = handle;
                let mut report =
                    match tokio::time::timeout(Duration::from_secs(5), &mut handle).await {
                        Ok(Ok(rep)) => rep,
                        Ok(Err(e)) => {
                            stop.cancel();
                            return Err(AppError::internal(format!("the udp sender failed: {e}")));
                        }
                        Err(_) => {
                            handle.abort();
                            stop.cancel();
                            return Err(AppError::timeout("the udp sender did not stop"));
                        }
                    };
                // The sender waits D after its last datagram.
                tokio::time::sleep(drain).await;
                report.packets = packet_delta(metrics_before, read_metrics(&r.node).await);
                let warm = report.start_ns + u64::from(p.warmup_secs) * 1_000_000_000;
                if report.end_ns > warm {
                    r.set_resource_window(warm, report.end_ns, None);
                }
                out.sender = Some(report);
            } else if let Some(counters) = echo {
                started.store(false, Ordering::SeqCst);
                out.echo = Some(counters.stats());
            }
            stop.cancel();
            drop(tasks);
            out.foreign_dropped = r.foreign_dropped();
            let mut td = r.teardown();
            td.hold(sock);
            td.run().await;
            to_value(&out)
        })
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let UdpRun {
                r,
                sock,
                stop,
                tasks,
                sender,
                ..
            } = *self;
            stop.cancel();
            if let Some((h, live)) = sender {
                live.request_stop(None);
                h.abort();
            }
            drop(tasks);
            let mut td = r.teardown();
            td.hold(sock);
            td.run().await;
        })
    }
}

// ---------------------------------------------------------------------------
// Example
// ---------------------------------------------------------------------------

/// Synthetic reports for a 10 Mb/s, 1200 B, 1 + 10 s upload with a few
/// losses (example output and tests).
fn synthetic(p: &UdpParams, lost: &[u64]) -> (SenderReport, ReceiverReport) {
    let start_ns = 1_760_601_601_000_000_000u64;
    let payload = p.payload_bytes as u64;
    let pps = p.rate_mbps * 1e6 / 8.0 / payload as f64;
    let total_s = f64::from(p.warmup_secs + p.duration_secs);
    let sent = (pps * total_s) as u64;
    let gap = 1e9 / pps;
    let mut bucket_first_seq = Vec::new();
    let mut bucket_bytes = Vec::new();
    let mut rx = UdpReceiver::new(sent + 10);
    let t0 = Instant::now();
    for seq in 0..sent {
        let t = start_ns + (seq as f64 * gap) as u64;
        let b = ((t - start_ns) / BUCKET_NS) as usize;
        while bucket_first_seq.len() <= b {
            bucket_first_seq.push(seq);
            bucket_bytes.push(0);
        }
        bucket_bytes[b] += payload;
        if lost.contains(&seq) {
            continue;
        }
        let mut d = vec![0u8; payload as usize];
        let _ = DatagramHeader {
            run_tag_lo: run_tag_lo(7),
            seq,
            send_ns: t,
        }
        .encode_into(&mut d);
        // A transit of 1.6 ms with ±0.2 ms of deterministic wobble.
        let wobble = [0u64, 120_000, 40_000, 200_000, 80_000][(seq % 5) as usize];
        let _ = rx.on_datagram(&d, 7, t + 1_600_000 + wobble, t0);
    }
    let end_ns = start_ns + ((sent - 1) as f64 * gap) as u64;
    let sender = SenderReport {
        sent,
        send_errors: 0,
        last_error: None,
        last_seq: sent.checked_sub(1),
        bytes_sent: sent * payload,
        bucket_first_seq,
        bucket_bytes,
        start_ns,
        end_ns,
        target_mbps: p.rate_mbps,
        offered_mbps: stats::mbps(((sent - 1) * payload) as f64, end_ns - start_ns),
        max_burst_datagrams: 2,
        pacer_late_ms_mean: Some(0.42),
        pacer_late_ms_max: Some(2.9),
        rate_achieved: true,
        steps: Vec::new(),
        stop_reason: None,
        packets: Some(PacketCounts {
            outbound: sent,
            inbound: 64,
        }),
    };
    let mut recv = rx.report(&sender, "last_seq", DRAIN_MIN);
    recv.rcvbuf_bytes = Some(4_194_304);
    recv.packets = Some(PacketCounts {
        outbound: 58,
        inbound: (sent + 1).saturating_sub(lost.len() as u64),
    });
    (sender, recv)
}

/// A canonical example output for the schema fixtures (stream, upload, 10 Mb/s).
pub fn example_output() -> WorkloadOutput {
    let p = UdpParams::default();
    let (sender, recv) = synthetic(&p, &[611, 612, 4_090, 9_731]);
    let shift = |t: u64| t - 1_284_310;
    let ident = |t: u64| t;
    let mut out = build_stream(
        &StreamInputs {
            p: &p,
            origin_ns: sender.start_ns - 650_000_000,
            sender_to_local: &ident,
            recv_to_local: &shift,
            foreign_dropped: 0,
        },
        &sender,
        &recv,
    )
    .unwrap_or_default();
    for s in &mut out.summary {
        if s.key == "cpu_ms_per_mb" {
            s.value = Some(9.8);
        }
    }
    out
}

/// A synthetic ramp upload from `p.rate_mbps`: the warmup at the start
/// rate, then ×1.5 every 3 s until the run ends; `lost` seqs never arrive.
fn synthetic_ramp(p: &UdpParams, lost: &[u64]) -> (SenderReport, ReceiverReport) {
    let start_ns = 1_760_601_601_000_000_000u64;
    let payload = p.payload_bytes as u64;
    let end_s = f64::from(p.warmup_secs + p.duration_secs);
    let step_s = RAMP_STEP.as_secs_f64();
    let rate_at = |t: f64| -> (Option<usize>, f64) {
        if t < f64::from(p.warmup_secs) {
            return (None, p.rate_mbps);
        }
        let k = ((t - f64::from(p.warmup_secs)) / step_s) as usize;
        (Some(k), p.rate_mbps * RAMP_FACTOR.powi(k as i32))
    };
    let mut times = Vec::new();
    let mut t = 0.0f64;
    while t < end_s {
        times.push(t);
        let (_, rate) = rate_at(t);
        t += payload as f64 * 8.0 / (rate * 1e6);
    }
    let sent = times.len() as u64;
    let mut bucket_first_seq = Vec::new();
    let mut bucket_bytes = Vec::new();
    let mut steps: Vec<RampStep> = Vec::new();
    for (seq, t) in times.iter().enumerate() {
        let seq = seq as u64;
        let b = (t * 4.0) as usize;
        while bucket_first_seq.len() <= b {
            bucket_first_seq.push(seq);
            bucket_bytes.push(0);
        }
        bucket_bytes[b] += payload;
        if let (Some(k), rate) = rate_at(*t) {
            if steps.len() <= k {
                if let Some(prev) = steps.last_mut() {
                    prev.end_seq = seq;
                    prev.end_ms = t * 1000.0;
                }
                steps.push(RampStep {
                    rate_mbps: rate,
                    offered_mbps: Some(rate * (0.997 + 0.001 * k as f64)),
                    first_seq: seq,
                    end_seq: seq,
                    start_ms: t * 1000.0,
                    end_ms: t * 1000.0,
                    complete: true,
                    achieved: true,
                    loss_pct: None,
                    sustained: false,
                });
            }
        }
    }
    if let Some(last) = steps.last_mut() {
        last.end_seq = sent;
        last.end_ms = times.last().copied().unwrap_or(0.0) * 1000.0;
        last.complete = last.end_ms - last.start_ms + 1.0 >= step_s * 1000.0;
    }
    let to_ns = |t: f64| start_ns + (t * 1e9) as u64;
    let end_ns = to_ns(times.last().copied().unwrap_or(0.0));
    let offered = stats::mbps(((sent - 1) * payload) as f64, end_ns - start_ns);
    let sender = SenderReport {
        sent,
        send_errors: 0,
        last_error: None,
        last_seq: sent.checked_sub(1),
        bytes_sent: sent * payload,
        bucket_first_seq,
        bucket_bytes,
        start_ns,
        end_ns,
        target_mbps: p.rate_mbps,
        offered_mbps: offered,
        max_burst_datagrams: 3,
        pacer_late_ms_mean: Some(0.38),
        pacer_late_ms_max: Some(3.1),
        rate_achieved: true,
        steps,
        stop_reason: Some("the run ended first".into()),
        packets: Some(PacketCounts {
            outbound: sent,
            inbound: 71,
        }),
    };
    let mut rx = UdpReceiver::new(sent + 10);
    let t0 = Instant::now();
    for (seq, t) in times.iter().enumerate() {
        let seq = seq as u64;
        if lost.contains(&seq) {
            continue;
        }
        let send = to_ns(*t);
        let mut d = vec![0u8; payload as usize];
        let _ = DatagramHeader {
            run_tag_lo: run_tag_lo(7),
            seq,
            send_ns: send,
        }
        .encode_into(&mut d);
        let wobble = [0u64, 90_000, 30_000, 160_000, 60_000][(seq % 5) as usize];
        let _ = rx.on_datagram(&d, 7, send + 1_600_000 + wobble, t0);
    }
    let mut recv = rx.report(&sender, "last_seq", DRAIN_MIN);
    recv.rcvbuf_bytes = Some(4_194_304);
    recv.packets = Some(PacketCounts {
        outbound: 66,
        inbound: (sent + 3).saturating_sub(lost.len() as u64),
    });
    (sender, recv)
}

/// Ramp upload from 10 Mb/s for 1 + 10 s: 10 and 15 Mb/s held; a burst
/// of lost datagrams (3% of the 22.5 Mb/s step, in one 250 ms bucket, so
/// the live two-bucket stop never fired) ends the sustained run, so the
/// limit is 15 Mb/s; 33.75 is cut short by the end of the run.
fn example_ramp() -> WorkloadOutput {
    let p = UdpParams {
        mode: UdpMode::Ramp,
        ..UdpParams::default()
    };
    let (probe, _) = synthetic_ramp(&p, &[]);
    let third = &probe.steps[2];
    let burst = (third.end_seq - third.first_seq) * 3 / 100;
    let from = third.first_seq + 1_000;
    let lost: Vec<u64> = (from..from + burst)
        .chain([third.first_seq - 2_100, third.first_seq - 300])
        .collect();
    let (sender, recv) = synthetic_ramp(&p, &lost);
    let shift = |t: u64| t - 1_284_310;
    let ident = |t: u64| t;
    let mut out = build_stream(
        &StreamInputs {
            p: &p,
            origin_ns: sender.start_ns - 650_000_000,
            sender_to_local: &ident,
            recv_to_local: &shift,
            foreign_dropped: 0,
        },
        &sender,
        &recv,
    )
    .unwrap_or_default();
    for s in &mut out.summary {
        if s.key == "cpu_ms_per_mb" {
            s.value = Some(10.4);
        }
    }
    out
}

/// Echo at 50 pps of 1200 B for 1 + 10 s; one echo lost.
fn example_echo() -> WorkloadOutput {
    let p = UdpParams {
        mode: UdpMode::Echo,
        ..UdpParams::default()
    };
    let origin = 1_760_601_600_350_000_000u64;
    let start_ns = origin + 650_000_000;
    let total = u64::from(p.pps) * u64::from(p.warmup_secs + p.duration_secs);
    let gap = 1_000_000_000 / u64::from(p.pps);
    let sends: Vec<u64> = (0..total).map(|i| start_ns + i * gap).collect();
    let mut st = EchoState::new(seq_limit(&p), u64::from(p.warmup_count));
    for seq in 0..total {
        if seq == 312 {
            continue; // never echoed
        }
        let rtt = 0.62
            + ((seq * 37) % 29) as f64 * 0.007
            + if seq % 83 == 5 { 0.9 } else { 0.0 }
            + if seq < u64::from(p.warmup_count) {
                0.4
            } else {
                0.0
            };
        st.on_echo(seq, rtt);
    }
    let payload = p.payload_bytes as u64;
    let end_ns = sends.last().copied().unwrap_or(start_ns);
    let report = SenderReport {
        sent: total,
        last_seq: total.checked_sub(1),
        bytes_sent: total * payload,
        start_ns,
        end_ns,
        target_mbps: f64::from(p.pps) * payload as f64 * 8.0 / 1e6,
        offered_mbps: stats::mbps(((total - 1) * payload) as f64, end_ns - start_ns),
        max_burst_datagrams: 1,
        pacer_late_ms_mean: Some(1.9),
        pacer_late_ms_max: Some(4.6),
        rate_achieved: true,
        ..SenderReport::default()
    };
    let inputs = EchoInputs {
        p: &p,
        report: &report,
        origin_ns: origin,
        sends,
        payload: p.payload_bytes as usize,
        drain: DRAIN_MIN,
        rcvbuf: Some(4_194_304),
    };
    let responder = EchoStats {
        received: total,
        echoed: total,
        echo_errors: 0,
        before_start: 0,
    };
    echo_output(&inputs, &st, responder, 0).unwrap_or_default()
}

/// One example per udp mode for the schema fixtures.
pub fn examples() -> Vec<super::ModeExample> {
    vec![
        super::ModeExample {
            mode: "udp-stream",
            params: WorkloadParams::Udp(UdpParams::default()),
            output: example_output(),
            clock: true,
        },
        super::ModeExample {
            mode: "udp-echo",
            params: WorkloadParams::Udp(UdpParams {
                mode: UdpMode::Echo,
                ..UdpParams::default()
            }),
            output: example_echo(),
            clock: false,
        },
        super::ModeExample {
            mode: "udp-ramp",
            params: WorkloadParams::Udp(UdpParams {
                mode: UdpMode::Ramp,
                ..UdpParams::default()
            }),
            output: example_ramp(),
            clock: true,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drain_period_is_clamped() {
        assert_eq!(drain_period(None), DRAIN_MIN);
        assert_eq!(drain_period(Some(Duration::from_millis(1))), DRAIN_MIN);
        assert_eq!(
            drain_period(Some(Duration::from_millis(200))),
            Duration::from_millis(800)
        );
        assert_eq!(drain_period(Some(Duration::from_secs(2))), DRAIN_MAX);
    }

    #[test]
    fn fragments_above_1252_bytes() {
        assert_eq!(packets_per_datagram(1200), 1);
        assert_eq!(packets_per_datagram(1252), 1);
        assert_eq!(packets_per_datagram(1253), 2);
        assert_eq!(packets_per_datagram(1400), 2);
        let l = loss_locality(
            1000,
            950,
            PacketCounts {
                outbound: 1990,
                inbound: 0,
            },
            PacketCounts {
                outbound: 0,
                inbound: 1920,
            },
            1400,
        );
        assert_eq!(l.packets_per_datagram, 2);
        assert_eq!(l.local_send_drops, 5);
        assert_eq!(l.network, 35);
        assert_eq!(l.local_recv_drops, 10);
        assert_eq!(l.local_send_drops + l.network + l.local_recv_drops, 50);
    }

    fn dgram(seq: u64, send_ns: u64) -> Vec<u8> {
        let mut d = vec![0u8; 100];
        DatagramHeader {
            run_tag_lo: run_tag_lo(42),
            seq,
            send_ns,
        }
        .encode_into(&mut d)
        .unwrap();
        d
    }

    #[test]
    fn receiver_counts_loss_reorder_dup_and_buckets() {
        let now = Instant::now();
        let mut rx = UdpReceiver::new(1000);
        // 20 sent; 5 and 12..=14 lost; 3 arrives after 4; 7 twice.
        let order = [0u64, 1, 2, 4, 3, 6, 7, 7, 8, 9, 10, 11, 15, 16, 17, 18, 19];
        for (i, seq) in order.iter().enumerate() {
            assert!(rx.on_datagram(&dgram(*seq, 1000 * seq), 42, 5000 + 1000 * i as u64, now));
        }
        assert!(!rx.on_datagram(&dgram(1, 0), 43, 0, now), "wrong tag");
        assert!(!rx.on_datagram(&[1, 2, 3], 42, 0, now), "short");
        let sender = SenderReport {
            sent: 20,
            last_seq: Some(19),
            bucket_first_seq: vec![0, 10],
            ..Default::default()
        };
        let r = rx.report(&sender, "last_seq", DRAIN_MIN);
        assert_eq!(r.unique, 16);
        assert_eq!(r.missing, 4);
        assert_eq!(r.unique + r.missing, sender.sent);
        assert_eq!(r.dup, 1);
        assert_eq!(r.reordered, 1);
        assert_eq!(r.bad_datagrams, 2);
        assert_eq!(r.max_loss_burst, 3);
        assert!((r.loss_pct.unwrap() - 20.0).abs() < 1e-9);
        assert_eq!(
            r.bucket_loss,
            vec![
                BucketLoss {
                    missing: 1,
                    sent: 10
                },
                BucketLoss {
                    missing: 3,
                    sent: 10
                }
            ]
        );
        // Bytes count unique datagrams only.
        assert_eq!(r.accounting.bytes_total, 16 * 100);
        r.accounting.check_invariants().unwrap();
        assert!(rx.has(19) && !rx.has(5));
    }

    #[test]
    fn live_loss_is_per_interval() {
        let t0 = Instant::now();
        let mut rx = UdpReceiver::new(1000);
        for seq in 0..10 {
            rx.on_datagram(&dgram(seq, 0), 42, seq, t0);
        }
        let l = rx.live(t0);
        assert_eq!(l.loss_pct, Some(0.0));
        // Next interval: seqs 10..20 sent, 15 and 16 lost.
        for seq in (10..20).filter(|s| *s != 15 && *s != 16) {
            rx.on_datagram(&dgram(seq, 0), 42, seq, t0);
        }
        let l = rx.live(t0 + Duration::from_millis(250));
        assert!((l.loss_pct.unwrap() - 20.0).abs() < 1e-9);
        assert!(l.goodput_mbps.is_some());
        // Nothing new: no loss figure.
        assert_eq!(rx.live(t0 + Duration::from_millis(500)).loss_pct, None);
        // A late 15 does not make the interval negative.
        rx.on_datagram(&dgram(15, 0), 42, 0, t0);
        assert_eq!(rx.live(t0 + Duration::from_millis(750)).loss_pct, None);
    }

    #[test]
    fn ramp_control_stops_on_two_bad_intervals() {
        let mut c = RampControl::default();
        assert!(!c.on_sample(0.0, 10.0));
        assert!(!c.on_sample(3.0, 10.0));
        assert!(!c.on_sample(1.0, 10.0), "one bad interval is not enough");
        assert!(!c.on_sample(0.5, 15.0));
        assert_eq!(c.best(), None, "10 had a bad interval");
        assert!(!c.on_sample(0.0, 22.5));
        assert_eq!(c.best(), Some(15.0));
        assert!(!c.on_sample(5.0, 22.5));
        assert!(c.on_sample(9.0, 22.5));
        assert!(
            c.reason().unwrap().contains("at 22.5 Mb/s"),
            "{:?}",
            c.reason()
        );
        assert_eq!(c.best(), Some(15.0));
    }

    #[test]
    fn echo_state_counts_dups_late_and_live_loss() {
        let mut st = EchoState::new(100, 2);
        st.on_echo(0, 1.0);
        st.on_echo(2, 2.0);
        st.on_echo(2, 2.5);
        st.on_echo(4, 4.0);
        assert_eq!(st.dup, 1);
        // Seqs 2..6 measured; 3 and 5 not echoed.
        let sends = [0u64, 10, 20, 30, 40, 50];
        assert_eq!(st.live_loss(&sends, 35), Some(50.0), "2 echoed, 3 lost");
        assert_eq!(st.samples(6), vec![Some(2.0), None, Some(4.0), None]);
        st.closed = true;
        st.on_echo(5, 1.0);
        assert_eq!(st.late, 1);
        assert_eq!(st.samples(6)[3], None);
        let (interval, p50) = st.live.take();
        assert_eq!(interval, Some(3.0));
        assert_eq!(p50, Some(2.0));
    }

    #[tokio::test(start_paused = true)]
    async fn sender_paces_buckets_and_ramps() {
        let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sink = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = sink.local_addr().unwrap();
        let drainer = tokio::spawn(async move {
            let mut buf = [0u8; 2048];
            loop {
                let _ = sink.recv_from(&mut buf).await;
            }
        });
        let sock = Arc::new(DatagramSocket::direct(sock));
        let p = UdpParams {
            rate_mbps: 1.0,
            payload_bytes: 125,
            duration_secs: 2,
            warmup_secs: 1,
            ..UdpParams::default()
        };
        let plan = SendPlan::new(&p, 9, target);
        let live = SendLive::new(plan.rate_mbps);
        let rep = send_loop(sock.clone(), plan, live.clone()).await;
        // 1 Mb/s of 125 B = 1000 datagrams/s for 3 s.
        assert!((2900..=3010).contains(&rep.sent), "{}", rep.sent);
        assert_eq!(rep.last_seq, Some(rep.sent - 1));
        assert_eq!(rep.bucket_first_seq.len(), 12);
        assert_eq!(rep.bucket_first_seq[0], 0);
        assert!(rep.bucket_first_seq.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(rep.bucket_bytes.iter().sum::<u64>(), rep.bytes_sent);
        assert!(rep.rate_achieved, "{:?}", rep.offered_mbps);
        assert!(rep.steps.is_empty());
        check_sender_report(&rep, &p).unwrap();
        // A ramp from 1 Mb/s: steps of 3 s at 1, 1.5, … after the warmup.
        let ramp = UdpParams {
            mode: UdpMode::Ramp,
            duration_secs: 7,
            ..p
        };
        let live = SendLive::new(ramp.rate_mbps);
        let rep = send_loop(sock.clone(), SendPlan::new(&ramp, 9, target), live.clone()).await;
        assert_eq!(rep.steps.len(), 3, "{:?}", rep.steps);
        assert_eq!(rep.steps[1].rate_mbps, 1.5);
        assert!(
            rep.steps[0].complete && rep.steps[0].achieved,
            "{:?}",
            rep.steps[0]
        );
        assert!(!rep.steps[2].complete);
        assert_eq!(rep.steps[1].first_seq, rep.steps[0].end_seq);
        assert_eq!(rep.stop_reason.as_deref(), Some("the run ended first"));
        check_sender_report(&rep, &ramp).unwrap();
        // A stop request ends the sender with its reason.
        let live = SendLive::new(ramp.rate_mbps);
        live.request_stop(Some("told to".into()));
        let rep = send_loop(sock, SendPlan::new(&ramp, 9, target), live).await;
        assert_eq!(rep.sent, 0);
        assert_eq!(rep.last_seq, None);
        assert_eq!(rep.stop_reason.as_deref(), Some("told to"));
        drainer.abort();
    }

    #[test]
    fn implausible_sender_reports_are_refused() {
        let p = UdpParams::default(); // 1 s warmup + 10 s
        let ok = SenderReport {
            sent: 100,
            bucket_first_seq: vec![0, 10, 10, 60],
            bucket_bytes: vec![1, 2, 3, 4],
            ..Default::default()
        };
        check_sender_report(&ok, &p).unwrap();
        // A huge `sent` alone is fine: the tracker's cost does not depend on it.
        check_sender_report(
            &SenderReport {
                sent: u64::MAX,
                ..ok.clone()
            },
            &p,
        )
        .unwrap();
        let bad = [
            SenderReport {
                bucket_first_seq: vec![0; 1000],
                ..ok.clone()
            },
            SenderReport {
                bucket_bytes: vec![0; 1000],
                ..ok.clone()
            },
            SenderReport {
                bucket_first_seq: vec![0, 50, 10],
                ..ok.clone()
            },
            SenderReport {
                bucket_first_seq: vec![0, 101],
                ..ok.clone()
            },
        ];
        for b in &bad {
            assert!(
                check_sender_report(b, &p).is_err(),
                "{:?}",
                b.bucket_first_seq
            );
        }
        let step = |first_seq: u64, end_seq: u64| RampStep {
            rate_mbps: 10.0,
            offered_mbps: None,
            first_seq,
            end_seq,
            start_ms: 0.0,
            end_ms: 0.0,
            complete: true,
            achieved: true,
            loss_pct: None,
            sustained: false,
        };
        let ramp = UdpParams {
            mode: UdpMode::Ramp,
            ..UdpParams::default()
        };
        let with_steps = |steps: Vec<RampStep>| SenderReport {
            steps,
            ..ok.clone()
        };
        check_sender_report(&with_steps(vec![step(0, 40), step(40, 100)]), &ramp).unwrap();
        for steps in [
            vec![step(0, 40), step(30, 100)],
            vec![step(0, 101)],
            vec![step(50, 40)],
            (0..50).map(|_| step(0, 0)).collect(),
        ] {
            assert!(check_sender_report(&with_steps(steps), &ramp).is_err());
        }
    }

    #[test]
    fn stream_output_follows_the_definitions() {
        let out = example_output();
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "goodput_mbps",
                "offered_mbps",
                "loss_pct",
                "jitter_ms",
                "cpu_ms_per_mb"
            ]
        );
        assert!(out.summary.iter().all(|s| s.value.is_some()));
        let r: Results = serde_json::from_value(out.results.clone()).unwrap();
        let Results::Stream(s) = r else {
            panic!("not stream")
        };
        assert_eq!(s.unique + s.missing, s.sent);
        assert_eq!(s.missing, 4);
        assert_eq!(s.max_loss_burst, 2);
        assert!(s.received_bytes <= s.sent_bytes);
        s.receiver.check_invariants().unwrap();
        let bucket_missing: u64 = s.bucket_loss.iter().map(|b| b.missing).sum();
        assert_eq!(bucket_missing, s.missing);
        let g = s.goodput_mbps.unwrap();
        assert!((9.5..=10.1).contains(&g), "{g}");
        assert!(s.rate_achieved);
        assert!(s.jitter_ms.unwrap() > 0.0);
        let loc = s.loss_locality.unwrap();
        assert_eq!(loc.local_send_drops + loc.network + loc.local_recv_drops, 4);
        let series: Vec<&str> = out.series.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(series, ["goodput_mbps", "offered_mbps", "loss_pct"]);
        assert!(out.notes.iter().any(|n| n == NOTE_LOSS));
        assert!(out.notes.iter().any(|n| n == NOTE_IPV4));
        assert!(!out.notes.iter().any(|n| n == NOTE_FRAGMENTS));
        // Loss points: one per send bucket, ≈ 0 except where seqs were lost.
        let loss = &out.series[2];
        assert_eq!(loss.points.len(), s.bucket_first_seq.len());
        assert!(loss.points.iter().filter(|p| p.1 > 0.0).count() == 3);
    }

    #[test]
    fn ramp_output_takes_the_last_sustained_step() {
        let p = UdpParams {
            mode: UdpMode::Ramp,
            ..UdpParams::default()
        };
        let (mut sender, _) = synthetic(&p, &[]);
        let step = |rate: f64, first: u64, end: u64, achieved: bool| RampStep {
            rate_mbps: rate,
            offered_mbps: Some(rate),
            first_seq: first,
            end_seq: end,
            start_ms: 0.0,
            end_ms: 3000.0,
            complete: true,
            achieved,
            loss_pct: None,
            sustained: false,
        };
        sender.steps = vec![
            step(10.0, 1000, 4000, true),
            step(15.0, 4000, 8000, true),
            step(22.5, 8000, 11000, true),
        ];
        sender.stop_reason = Some("the run ended first".into());
        // 1% loss in step 0, 3% in step 1 → only 10 Mb/s is sustained.
        let lost: Vec<u64> = (1000..1030).chain(4000..4120).collect();
        let (_, mut recv) = synthetic(&p, &lost);
        let mut rx = UdpReceiver::new(20_000);
        let t0 = Instant::now();
        for seq in (0..sender.sent).filter(|s| !lost.contains(s)) {
            rx.on_datagram(&dgram(seq, seq * 1000), 42, seq * 1000 + 5, t0);
        }
        recv.step_loss = rx.report(&sender, "quiet", DRAIN_MIN).step_loss;
        let ident = |t: u64| t;
        let out = build_stream(
            &StreamInputs {
                p: &p,
                origin_ns: sender.start_ns,
                sender_to_local: &ident,
                recv_to_local: &ident,
                foreign_dropped: 0,
            },
            &sender,
            &recv,
        )
        .unwrap();
        assert_eq!(out.summary[0].key, "max_sustainable_mbps");
        assert_eq!(out.summary[0].value, Some(10.0));
        let r: Results = serde_json::from_value(out.results).unwrap();
        let Results::Ramp(r) = r else {
            panic!("not ramp")
        };
        assert!(r.steps[0].sustained);
        assert!(!r.steps[1].sustained && !r.steps[2].sustained);
        assert!((r.steps[1].loss_pct.unwrap() - 3.0).abs() < 1e-9);
        assert_eq!(r.max_sustainable_mbps, Some(10.0));
        assert!(r.limit_found, "step 1 failed on loss: a genuine limit");
        assert_eq!(r.stop_reason, "the run ended first");
        assert_eq!(out.series[0].key, "offered_mbps");

        // Every step clean and the run ended first: no limit was found. The
        // result is a lower bound, the headline is empty, and the final step
        // cut short by the run's end never counts (999 vs 1001 ms is jitter).
        let partial = |end_ms: f64| {
            let mut s = sender.clone();
            s.steps[2].complete = false;
            s.steps[2].end_ms = end_ms;
            s
        };
        let mut clean = UdpReceiver::new(20_000);
        for seq in 0..sender.sent {
            clean.on_datagram(&dgram(seq, seq * 1000), 42, seq * 1000 + 5, t0);
        }
        let outs: Vec<RampResults> = [998.9, 1000.3]
            .into_iter()
            .map(|end_ms| {
                let s = partial(end_ms);
                let mut recv = recv.clone();
                recv.step_loss = clean.report(&s, "quiet", DRAIN_MIN).step_loss;
                let out = build_stream(
                    &StreamInputs {
                        p: &p,
                        origin_ns: s.start_ns,
                        sender_to_local: &ident,
                        recv_to_local: &ident,
                        foreign_dropped: 0,
                    },
                    &s,
                    &recv,
                )
                .unwrap();
                assert_eq!(out.summary[0].key, "max_sustainable_mbps");
                assert_eq!(out.summary[0].value, None, "a ceiling is not a capacity");
                assert!(out.notes.iter().any(|n| n.contains("lower bound")));
                match serde_json::from_value(out.results).unwrap() {
                    Results::Ramp(r) => *r,
                    _ => panic!("not ramp"),
                }
            })
            .collect();
        for r in &outs {
            assert!(!r.limit_found);
            assert_eq!(r.max_sustainable_mbps, Some(15.0), "{:?}", r.steps);
            assert!(!r.steps[2].sustained);
        }
    }
}
