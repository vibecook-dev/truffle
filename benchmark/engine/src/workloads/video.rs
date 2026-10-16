//! `video` (RFC 027 §8.2 video, the frame model; §6 Clock).
//!
//! A synthetic stream with exact frame sizes, initiator → responder.
//!
//! - **Frame sizes** (G = gop, K = keyframe_ratio, b = bitrate):
//!   `P = round(b/8 · G/fps / (K + G − 1))` bytes and `I = K·P`; frame k is
//!   a keyframe iff `k mod G == 0` ([`VideoParams::p_frame_bytes`] and
//!   friends). `offered_mbps = Σ frame bytes · 8 / (frames / fps) / 1e6`;
//!   goodput and loss are compared with offered, not the nominal profile.
//! - **Capture:** `capture_ns_k = t0 + k·1e9/fps` on the sender. It is the
//!   scheduled time; frames are never skipped, so sender stalls show up as
//!   latency.
//! - **UDP:** the 22-byte TB header, the 18-byte frame header, then the
//!   payload, ≤ 1200 B per datagram. `paced` spreads a frame's fragments
//!   over `[capture, capture + 1/fps)` on 1 ms ticks (the shared
//!   [`Pacer`], restarted at each capture at the frame's own rate);
//!   `burst` sends them back to back. **TCP:** one TBN1 stream of `[u32 BE len][frame header][payload]`.
//!   **QUIC:** one bidi stream per frame (TBN1 header, then the same
//!   framing); the receiver finishes its send half on accept. Pacing applies
//!   to UDP only.
//! - **Receiver** computes nothing clock-dependent: `finish.ack.stats.frames`
//!   holds raw `{frame_no, keyframe, bytes, capture_ns, first_recv_ns,
//!   complete_recv_ns | null}` in responder time, plus the receiver
//!   accounting of payload bytes (§8.1) for goodput.
//! - **Initiator** estimates the clock before `prepare` and after `finish`,
//!   then: `raw_k = complete_k − capture_k`, the relative delay
//!   `d_k = raw_k − min_j raw_j` (clock-free); `late` iff `d_k >
//!   jitter_buffer_ms`; `dropped` iff not complete by the receiver's drain
//!   deadline; `on_time` otherwise; `delivered_fps = on_time / (frames_sent
//!   / fps)`; `freezes` are maximal runs of ≥ 2 consecutive frames not on
//!   time (`freeze_ms` = Σ run length · 1000/fps); `frame_latency_ms =
//!   raw_k − offset(complete_k)`, shown ± the clock uncertainty and never
//!   used for late, dropped or freezes; `frame_spread_ms` is first to last
//!   fragment (clock-free); `keyframe_complete_ms` is the frame latency of
//!   keyframes. Frames captured during the warmup are sent and shown but not
//!   counted.
//! - **Frame strip:** `results.frames` keeps every frame when there are ≤
//!   3600, otherwise every ⌈n/3600⌉-th frame plus every frame not on time.
//!
//! Summary keys: `delivered_fps`, `late_pct`, `dropped_pct`, `freezes`,
//! `frame_latency_p50_ms` (± uncertainty), `goodput_mbps`.
//! Series: `frame_latency_ms`, `goodput_mbps`.
//! Live samples: from the receiver's ticks `frames_done`, `delivered_fps`,
//! `late_pct`, `dropped_pct`, `freezes`, `frame_latency_ms`,
//! `frame_latency_p50_ms`, `goodput_mbps`; from the sender `frames_sent`,
//! `offered_mbps`, `clock_uncertainty_ms`.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::task::JoinSet;
use tokio::time::Instant;
use truffle_core::transport::quic::{QuicConnection, QuicListener};
use truffle_core::transport::{DatagramSocket, RawListener};

use crate::clock::{instant_ns, now_ns, ClockMap};
use crate::dataplane::{
    payload_chunk, read_stream_ack, run_tag_lo, video_fragments, write_header, write_stream_ack,
    DatagramHeader, FrameHeader, FrameParser, Pacer, ParseEvent, ReceiverAccounting, ReceiverStats,
    StreamHeader, StreamKind, DATAGRAM_HEADER_LEN, FRAME_HEADER_LEN, IO_CHUNK, VIDEO_MAX_DATAGRAM,
    VIDEO_MAX_FRAGMENT,
};
use crate::error::{AppError, ErrorCode};
use crate::params::{Pacing, VideoParams, VideoTransport, WorkloadParams};
use crate::stats::{self, SeqTracker};
use crate::types::{Better, Distribution, Phase, Series, SummaryItem};
use crate::workloads::common::{read_tbn1, Tasks, Tbn1Acceptor};
use crate::workloads::{ResponderCtx, ResponderRun, RunCtx, WorkloadOutput};

/// `video` is implemented.
pub const IMPLEMENTED: bool = true;

/// The Progress sample keys a run with these params can set: the
/// initiator's own and the responder's `tick`s, which the hub merges in.
/// [`live_keys`](super::live_keys) adds the path sampler's `loaded_rtt_ms`
/// for time-based runs; `live-keys.json` in the schema fixtures is built
/// from it.
pub fn live_keys(_p: &VideoParams) -> Vec<&'static str> {
    vec![
        // the sender
        "clock_uncertainty_ms",
        "frames_sent",
        "offered_mbps",
        // the receiver's ticks
        "frames_done",
        "delivered_fps",
        "late_pct",
        "dropped_pct",
        "freezes",
        "frame_latency_ms",
        "frame_latency_p50_ms",
        "goodput_mbps",
    ]
}

/// `results.frames` keeps every frame up to this many (§8.2).
pub const MAX_STRIP: usize = 3600;
/// Raw latency samples kept in `results.samples_ms`.
const MAX_RAW_SAMPLES: usize = 4000;
/// The receiver stops draining after this long without arrivals…
const DRAIN_IDLE: Duration = Duration::from_millis(500);
/// …or this long in all.
const DRAIN_CAP: Duration = Duration::from_secs(3);
/// Frame 0 is captured this long after `start`.
const START_LEAD: Duration = Duration::from_millis(50);
/// The TCP sender waits this long for the receiver's stream ack.
const STREAM_ACK_TIMEOUT: Duration = Duration::from_secs(10);
/// The QUIC sender waits this long for its frame streams to be written.
const QUIC_JOIN_TIMEOUT: Duration = Duration::from_secs(10);
/// UDP receive buffer the receiver asks for (best effort).
const RECV_BUFFER: usize = 4 << 20;
/// Live: an incomplete frame this many frame intervals older than the
/// newest complete frame counts as dropped.
const LIVE_DROP_FRAMES_PER_FPS: u64 = 1;

// ---------------------------------------------------------------------------
// The frame model
// ---------------------------------------------------------------------------

/// The frame schedule and sizes of a run.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Model {
    /// Frames per second.
    pub fps: u32,
    /// Frames sent (warmup + duration).
    pub frames: u64,
    /// Frames captured during the warmup.
    pub warmup_frames: u64,
    /// P-frame bytes.
    pub p_bytes: u64,
    /// Keyframe bytes.
    pub i_bytes: u64,
    /// GOP length.
    pub gop: u32,
    /// Bitrate, Mb/s.
    pub bitrate_mbps: f64,
}

impl Model {
    /// The model of `p`.
    pub fn new(p: &VideoParams) -> Self {
        let fps = p.effective_fps().max(1);
        Self {
            fps,
            frames: p.total_frames(),
            warmup_frames: u64::from(fps) * u64::from(p.warmup_secs),
            p_bytes: p.p_frame_bytes(),
            i_bytes: p.i_frame_bytes(),
            gop: p.gop.max(1),
            bitrate_mbps: p.effective_bitrate_mbps(),
        }
    }

    /// Frame k is a keyframe iff `k mod G == 0`.
    pub fn is_keyframe(&self, k: u64) -> bool {
        k.is_multiple_of(u64::from(self.gop))
    }

    /// Bytes of frame k.
    pub fn frame_bytes(&self, k: u64) -> u64 {
        if self.is_keyframe(k) {
            self.i_bytes
        } else {
            self.p_bytes
        }
    }

    /// `k·1e9/fps`: frame k's capture time after t0, ns.
    pub fn capture_offset_ns(&self, k: u64) -> u64 {
        (u128::from(k) * 1_000_000_000 / u128::from(self.fps)) as u64
    }

    /// One frame interval, ns.
    pub fn period_ns(&self) -> u64 {
        1_000_000_000 / u64::from(self.fps)
    }

    /// Offered load of frames `a..b`: `Σ bytes · 8 / ((b − a) / fps) / 1e6`.
    pub fn offered_mbps(&self, a: u64, b: u64) -> Option<f64> {
        if b <= a {
            return None;
        }
        let bytes: u64 = (a..b).map(|k| self.frame_bytes(k)).sum();
        let secs = (b - a) as f64 / f64::from(self.fps);
        Some(bytes as f64 * 8.0 / secs / 1e6)
    }

    /// Datagrams the whole run sends over UDP.
    pub fn total_datagrams(&self) -> u64 {
        (0..self.frames)
            .map(|k| video_fragments(self.frame_bytes(k)))
            .sum()
    }

    /// Fail when a keyframe needs more fragments than the header can count.
    pub fn check_udp(&self) -> Result<(), AppError> {
        let n = video_fragments(self.i_bytes);
        if n > u64::from(u16::MAX) {
            return Err(AppError::invalid_params(format!(
                "a {}-byte keyframe needs {n} UDP fragments, more than the 65535 the frame header \
                 can count; lower the bitrate, gop or keyframe_ratio, or use tcp or quic",
                self.i_bytes
            )));
        }
        Ok(())
    }
}

/// Payload bytes of fragment `idx` of a `bytes`-byte frame.
pub fn fragment_len(bytes: u64, idx: u64) -> usize {
    let start = idx * VIDEO_MAX_FRAGMENT as u64;
    bytes.saturating_sub(start).min(VIDEO_MAX_FRAGMENT as u64) as usize
}

// ---------------------------------------------------------------------------
// Wire and report types
// ---------------------------------------------------------------------------

/// `prepare.client`.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "VideoClient")]
pub struct Client {
    /// The initiator's clock offset estimate (responder − initiator, ns),
    /// used only for the live latency ticks; the report maps latency on the
    /// initiator with the before and after estimates.
    pub clock_offset_ns: Option<i64>,
}

/// `prepare.ack.setup`: the receiver's tailnet port.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "VideoSetup")]
pub struct Setup {
    /// UDP: the socket's tsnet port; TCP: the `listen_tcp` port; QUIC: the `listen_quic` port.
    pub port: u16,
}

/// `finish.info`: the sender's counts.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "VideoInfo")]
pub struct Info {
    /// Capture time of frame 0, initiator `now_ns()`.
    pub t0_ns: u64,
    /// Frames sent (all of them; none is skipped).
    pub frames_sent: u64,
    /// UDP datagrams attempted (seqs `0..datagrams_sent`).
    pub datagrams_sent: u64,
    /// UDP `send_to` failures among them.
    pub send_errors: u64,
    /// Payload bytes sent.
    pub sent_bytes: u64,
}

/// One frame as the receiver saw it (responder time).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "VideoRxFrame")]
pub struct RxFrame {
    /// Frame number.
    pub frame_no: u32,
    /// Keyframe flag.
    pub keyframe: bool,
    /// Payload bytes received.
    pub bytes: u64,
    /// The sender's capture time from the frame header (initiator time).
    pub capture_ns: u64,
    /// First byte or fragment.
    pub first_recv_ns: u64,
    /// Last byte or fragment; `None` when it never completed.
    pub complete_recv_ns: Option<u64>,
    /// Fragments received (UDP; 1 on TCP/QUIC).
    pub frags: u16,
    /// Fragments in the frame.
    pub frag_cnt: u16,
}

/// UDP datagram accounting at the receiver.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "VideoRxDatagrams")]
pub struct RxDatagrams {
    /// Distinct seqs.
    pub unique: u64,
    /// Repeats.
    pub dup: u64,
    /// Unique arrivals below an earlier maximum.
    pub reordered: u64,
    /// Longest run of never-received seqs in `0..datagrams_sent`.
    pub max_loss_burst: u64,
    /// Datagrams with a bad frame header or out-of-range numbers.
    pub bad: u64,
}

/// `finish.ack.stats`: raw per-frame receive times and the receiver accounting.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "VideoStats")]
pub struct Stats {
    /// Every frame seen, by frame number.
    pub frames: Vec<RxFrame>,
    /// Payload bytes in 250 ms buckets (§8.1), responder time.
    pub receiver: ReceiverStats,
    /// UDP only.
    pub datagrams: Option<RxDatagrams>,
    /// Foreign connections, streams or datagrams dropped (rule 5).
    pub foreign_dropped: u64,
}

/// How a frame fared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[schemars(rename = "VideoFrameState")]
pub enum FrameState {
    /// Complete and within the jitter buffer.
    #[default]
    OnTime,
    /// Complete, later than the jitter buffer allows.
    Late,
    /// Never completed.
    Dropped,
}

/// One frame of the strip.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "VideoFrame")]
pub struct FrameCell {
    /// Frame number.
    pub frame_no: u32,
    /// Keyframe flag.
    pub keyframe: bool,
    /// Frame size.
    pub bytes: u64,
    /// On time, late or dropped.
    pub state: FrameState,
    /// `raw − offset(complete)`, ms; `None` when dropped.
    pub latency_ms: Option<f64>,
    /// `d_k`, the delay beyond the fastest frame, ms.
    pub delay_ms: Option<f64>,
    /// First to last fragment, ms.
    pub spread_ms: Option<f64>,
    /// Captured during the warmup (not counted).
    pub warmup: bool,
}

/// UDP datagram figures in the report.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "VideoDatagrams")]
pub struct Datagrams {
    /// Datagrams whose `send_to` returned Ok.
    pub sent: u64,
    /// `send_to` failures.
    pub send_errors: u64,
    /// Distinct datagrams received.
    pub unique: u64,
    /// `sent − unique`.
    pub missing: u64,
    /// Repeats.
    pub dup: u64,
    /// Reordered arrivals.
    pub reordered: u64,
    /// `missing / sent · 100`.
    pub loss_pct: Option<f64>,
    /// Longest run of lost datagrams.
    pub max_loss_burst: u64,
}

/// `RunReport.results` of a `video` run.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "VideoResults")]
pub struct Results {
    /// UDP, QUIC or TCP.
    pub transport: VideoTransport,
    /// Paced or burst (UDP only).
    pub pacing: Pacing,
    /// Frames per second.
    pub fps: u32,
    /// Bitrate, Mb/s.
    pub bitrate_mbps: f64,
    /// P-frame bytes.
    pub p_frame_bytes: u64,
    /// Keyframe bytes.
    pub i_frame_bytes: u64,
    /// Frames sent in all.
    pub frames_total: u64,
    /// Frames captured during the warmup (sent, not counted).
    pub warmup_frames: u64,
    /// Counted frames sent.
    pub frames_sent: u64,
    /// Counted frames on time.
    pub on_time: u64,
    /// Counted frames late.
    pub late: u64,
    /// Counted frames dropped.
    pub dropped: u64,
    /// `late / frames_sent · 100`.
    pub late_pct: Option<f64>,
    /// `dropped / frames_sent · 100`.
    pub dropped_pct: Option<f64>,
    /// `on_time / (frames_sent / fps)`.
    pub delivered_fps: Option<f64>,
    /// Runs of ≥ 2 consecutive frames not on time.
    pub freezes: u32,
    /// Σ freeze length · 1000/fps.
    pub freeze_ms: f64,
    /// Frame latency of counted frames, ms (dropped ones as lost).
    pub frame_latency_ms: Distribution,
    /// Raw counted frame latencies, ms, ascending (≤ 4000 order statistics).
    pub samples_ms: Vec<f64>,
    /// ± on every latency figure: the clock offset uncertainty, ms.
    pub latency_uncertainty_ms: Option<f64>,
    /// `d_k` of counted frames, ms (clock-free).
    pub relative_delay_ms: Distribution,
    /// First to last fragment of counted frames, ms (clock-free).
    pub frame_spread_ms: Distribution,
    /// p50 of `frame_spread_ms`.
    pub frame_spread_p50_ms: Option<f64>,
    /// Frame latency of counted keyframes, ms.
    pub keyframe_complete_ms: Distribution,
    /// p50 of `keyframe_complete_ms`.
    pub keyframe_complete_p50_ms: Option<f64>,
    /// Offered load of the counted frames, Mb/s.
    pub offered_mbps: Option<f64>,
    /// Receiver goodput after the warmup, Mb/s (§8.1).
    pub goodput_mbps: Option<f64>,
    /// Payload bytes sent (warmup included).
    pub sent_bytes: u64,
    /// Payload bytes received (warmup included).
    pub received_bytes: u64,
    /// The receiver accounting, responder time.
    pub receiver: ReceiverStats,
    /// UDP only.
    pub datagrams: Option<Datagrams>,
    /// `open_tcp` / `connect_quic` time, ms.
    pub connect_ms: Option<f64>,
    /// How far the sender fell behind its capture schedule at worst, ms.
    pub max_send_lag_ms: f64,
    /// TCP: the receiver's stream ack (payload bytes it counted).
    pub stream_ack: Option<u64>,
    /// The frame strip.
    pub frames: Vec<FrameCell>,
    /// Every `frames_stride`-th frame is in the strip (1 = all), plus those not on time.
    pub frames_stride: u64,
    /// Foreign connections, streams or datagrams the receiver dropped.
    pub foreign_dropped: u64,
}

// ---------------------------------------------------------------------------
// Receiver: reassembly and framing (no clocks beyond arrival stamps)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct RxSlot {
    keyframe: bool,
    frag_cnt: u16,
    bits: Vec<u64>,
    frags: u16,
    bytes: u64,
    capture_ns: u64,
    first_ns: u64,
    complete_ns: Option<u64>,
}

/// Frames as they arrive: UDP fragments or whole TCP/QUIC frames.
#[derive(Debug, Default)]
pub struct Reassembly {
    frames: BTreeMap<u32, RxSlot>,
    limit: u64,
    complete: u64,
    bad: u64,
    dup_frags: u64,
}

impl Reassembly {
    /// Accept frame numbers below `frames`.
    pub fn new(frames: u64) -> Self {
        Self {
            limit: frames,
            ..Self::default()
        }
    }

    /// A UDP fragment of `payload` bytes arrived at `now`. Returns the
    /// payload bytes newly counted (0 for a duplicate or a bad fragment).
    pub fn fragment(&mut self, h: &FrameHeader, payload: usize, now: u64) -> u64 {
        if u64::from(h.frame_no) >= self.limit || h.frag_cnt == 0 || h.frag_idx >= h.frag_cnt {
            self.bad += 1;
            return 0;
        }
        let slot = self.frames.entry(h.frame_no).or_insert_with(|| RxSlot {
            keyframe: h.keyframe,
            frag_cnt: h.frag_cnt,
            bits: vec![0; usize::from(h.frag_cnt).div_ceil(64)],
            frags: 0,
            bytes: 0,
            capture_ns: h.capture_ns,
            first_ns: now,
            complete_ns: None,
        });
        if slot.frag_cnt != h.frag_cnt {
            self.bad += 1;
            return 0;
        }
        let (word, bit) = (usize::from(h.frag_idx) / 64, 1u64 << (h.frag_idx % 64));
        if slot.bits[word] & bit != 0 {
            self.dup_frags += 1;
            return 0;
        }
        slot.bits[word] |= bit;
        slot.frags += 1;
        slot.bytes += payload as u64;
        if slot.frags == slot.frag_cnt {
            slot.complete_ns = Some(now);
            self.complete += 1;
        }
        payload as u64
    }

    /// A TCP/QUIC frame's prefix arrived (its first byte at `first_ns`).
    pub fn frame_started(&mut self, h: &FrameHeader, first_ns: u64) -> bool {
        if u64::from(h.frame_no) >= self.limit || self.frames.contains_key(&h.frame_no) {
            self.bad += 1;
            return false;
        }
        self.frames.insert(
            h.frame_no,
            RxSlot {
                keyframe: h.keyframe,
                frag_cnt: 1,
                bits: Vec::new(),
                frags: 0,
                bytes: 0,
                capture_ns: h.capture_ns,
                first_ns,
                complete_ns: None,
            },
        );
        true
    }

    /// Payload bytes of a started TCP/QUIC frame.
    pub fn frame_bytes(&mut self, frame_no: u32, bytes: u64) {
        if let Some(s) = self.frames.get_mut(&frame_no) {
            s.bytes += bytes;
        }
    }

    /// A TCP/QUIC frame's last byte arrived at `now`.
    pub fn frame_completed(&mut self, frame_no: u32, now: u64) {
        if let Some(s) = self.frames.get_mut(&frame_no) {
            if s.complete_ns.is_none() {
                s.frags = 1;
                s.complete_ns = Some(now);
                self.complete += 1;
            }
        }
    }

    /// Complete frames so far.
    pub fn complete(&self) -> u64 {
        self.complete
    }

    /// Fragments or frames refused.
    pub fn bad(&self) -> u64 {
        self.bad
    }

    /// Duplicate fragments.
    pub fn dup_frags(&self) -> u64 {
        self.dup_frags
    }

    /// The raw per-frame record, by frame number.
    pub fn frames(&self) -> Vec<RxFrame> {
        self.frames
            .iter()
            .map(|(no, s)| RxFrame {
                frame_no: *no,
                keyframe: s.keyframe,
                bytes: s.bytes,
                capture_ns: s.capture_ns,
                first_recv_ns: s.first_ns,
                complete_recv_ns: s.complete_ns,
                frags: s.frags,
                frag_cnt: s.frag_cnt,
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Analysis (initiator)
// ---------------------------------------------------------------------------

/// One frame after classification.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Classified {
    /// Frame number.
    pub frame_no: u64,
    /// Keyframe flag.
    pub keyframe: bool,
    /// Captured during the warmup.
    pub warmup: bool,
    /// On time, late or dropped.
    pub state: FrameState,
    /// `d_k`, ns.
    pub delay_ns: Option<i64>,
    /// `raw_k − offset(complete_k)`, ms.
    pub latency_ms: Option<f64>,
    /// First to last fragment, ms.
    pub spread_ms: Option<f64>,
}

/// Classify every frame `0..model.frames` (§8.2 video). `capture_ns(k)` is
/// the sender's schedule; `to_local` maps a responder time into initiator
/// time (`None`: no clock estimate).
pub fn classify(
    model: &Model,
    jitter_buffer_ms: u32,
    rx: &[RxFrame],
    capture_ns: impl Fn(u64) -> u64,
    to_local: Option<&dyn Fn(u64) -> u64>,
) -> Vec<Classified> {
    let by_no: BTreeMap<u64, &RxFrame> = rx.iter().map(|f| (u64::from(f.frame_no), f)).collect();
    let raw = |k: u64| -> Option<i64> {
        let f = by_no.get(&k)?;
        let c = f.complete_recv_ns?;
        Some(c as i64 - capture_ns(k) as i64)
    };
    let min_raw = (0..model.frames).filter_map(raw).min();
    let budget = i64::from(jitter_buffer_ms) * 1_000_000;
    (0..model.frames)
        .map(|k| {
            let f = by_no.get(&k);
            let complete = f.and_then(|f| f.complete_recv_ns);
            let delay_ns = raw(k).zip(min_raw).map(|(r, m)| r - m);
            let state = match (complete, delay_ns) {
                (None, _) => FrameState::Dropped,
                (Some(_), Some(d)) if d > budget => FrameState::Late,
                _ => FrameState::OnTime,
            };
            let latency_ms = match (complete, to_local) {
                (Some(c), Some(map)) => Some((map(c) as f64 - capture_ns(k) as f64) / 1e6),
                _ => None,
            };
            let spread_ms = f
                .zip(complete)
                .map(|(f, c)| c.saturating_sub(f.first_recv_ns) as f64 / 1e6);
            Classified {
                frame_no: k,
                keyframe: model.is_keyframe(k),
                warmup: k < model.warmup_frames,
                state,
                delay_ns,
                latency_ms,
                spread_ms,
            }
        })
        .collect()
}

/// Freezes over frames in order: maximal runs of ≥ 2 consecutive frames not
/// on time. Returns (count, Σ run lengths in frames).
pub fn freezes(states: impl IntoIterator<Item = FrameState>) -> (u32, u64) {
    let mut count = 0;
    let mut frames = 0;
    let mut run = 0u64;
    for s in states.into_iter().chain([FrameState::OnTime]) {
        if s != FrameState::OnTime {
            run += 1;
            continue;
        }
        if run >= 2 {
            count += 1;
            frames += run;
        }
        run = 0;
    }
    (count, frames)
}

/// Strip indices (§8.2): all of `0..n` when `n ≤ max`, else every
/// ⌈n/max⌉-th plus every index in `keep`. Returns (indices, stride).
pub fn strip_indices(n: usize, max: usize, keep: impl Fn(usize) -> bool) -> (Vec<usize>, usize) {
    if n <= max || max == 0 {
        return ((0..n).collect(), 1);
    }
    let stride = n.div_ceil(max);
    (
        (0..n).filter(|i| i % stride == 0 || keep(*i)).collect(),
        stride,
    )
}

/// Order statistics of an ascending slice, at most `max` evenly spaced.
fn downsample_sorted(sorted: &[f64], max: usize) -> Vec<f64> {
    if sorted.len() <= max || max < 2 {
        return sorted.to_vec();
    }
    (0..max)
        .map(|i| sorted[i * (sorted.len() - 1) / (max - 1)])
        .collect()
}

/// What the initiator knows besides the receiver's stats.
#[derive(Debug, Clone)]
pub struct SenderSide {
    /// Frame 0's capture time, initiator `now_ns()`.
    pub t0_ns: u64,
    /// Run origin (`t_ms` 0).
    pub origin_ns: u64,
    /// UDP datagrams attempted.
    pub datagrams_sent: u64,
    /// UDP send failures.
    pub send_errors: u64,
    /// Payload bytes sent.
    pub sent_bytes: u64,
    /// Connect time, ms.
    pub connect_ms: Option<f64>,
    /// Worst lag behind the capture schedule, ms.
    pub max_send_lag_ms: f64,
    /// TCP stream ack.
    pub stream_ack: Option<u64>,
}

/// Turn the receiver's raw frames into the workload output.
pub fn analyse(
    p: &VideoParams,
    stats: Stats,
    map: Option<&ClockMap>,
    sender: &SenderSide,
    extra_notes: Vec<String>,
) -> WorkloadOutput {
    let model = Model::new(p);
    let rel_ms = |t: u64| (t as f64 - sender.origin_ns as f64) / 1e6;
    let capture = |k: u64| sender.t0_ns + model.capture_offset_ns(k);
    let to_local_fn = map.map(|m| move |t: u64| m.to_local(t));
    let to_local: Option<&dyn Fn(u64) -> u64> =
        to_local_fn.as_ref().map(|f| f as &dyn Fn(u64) -> u64);
    let all = classify(&model, p.jitter_buffer_ms, &stats.frames, capture, to_local);
    let counted: Vec<&Classified> = all.iter().filter(|c| !c.warmup).collect();
    let frames_sent = counted.len() as u64;
    let count = |s: FrameState| counted.iter().filter(|c| c.state == s).count() as u64;
    let (on_time, late, dropped) = (
        count(FrameState::OnTime),
        count(FrameState::Late),
        count(FrameState::Dropped),
    );
    let fps = f64::from(model.fps);
    let delivered_fps = (frames_sent > 0).then(|| on_time as f64 / (frames_sent as f64 / fps));
    let late_pct = stats::pct(late as f64, frames_sent as f64);
    let dropped_pct = stats::pct(dropped as f64, frames_sent as f64);
    let (freeze_count, freeze_frames) = freezes(counted.iter().map(|c| c.state));
    let freeze_ms = freeze_frames as f64 * 1000.0 / fps;
    let lat_samples: Vec<Option<f64>> = counted
        .iter()
        .map(|c| match c.state {
            FrameState::Dropped => None,
            _ => c.latency_ms,
        })
        .collect();
    let frame_latency = if map.is_some() {
        Distribution::from_samples(&lat_samples)
    } else {
        Distribution::from_values(&[])
    };
    let mut raw_lat: Vec<f64> = lat_samples.iter().flatten().copied().collect();
    raw_lat.sort_by(f64::total_cmp);
    let uncertainty_ms = map.map(|m| m.uncertainty_ns() as f64 / 1e6);
    let delays: Vec<f64> = counted
        .iter()
        .filter_map(|c| c.delay_ns.map(|d| d as f64 / 1e6))
        .collect();
    let spreads: Vec<f64> = counted.iter().filter_map(|c| c.spread_ms).collect();
    let key_lat: Vec<f64> = counted
        .iter()
        .filter(|c| c.keyframe)
        .filter_map(|c| c.latency_ms)
        .collect();
    let frame_spread = Distribution::from_values(&spreads);
    let keyframe_complete = Distribution::from_values(&key_lat);
    let offered_mbps = model.offered_mbps(model.warmup_frames, model.frames);
    let goodput = stats.receiver.goodput(p.warmup_secs);
    let mut notes = extra_notes;
    let goodput_mbps = match &goodput {
        Ok(g) => Some(g.mbps),
        Err(e) => {
            notes.push(format!("no goodput figure: {}", e.message));
            None
        }
    };
    let summary = vec![
        SummaryItem::new(
            "delivered_fps",
            "Delivered",
            delivered_fps,
            "fps",
            Better::Higher,
        ),
        SummaryItem::new("late_pct", "Late", late_pct, "%", Better::Lower),
        SummaryItem::new("dropped_pct", "Dropped", dropped_pct, "%", Better::Lower),
        SummaryItem::new(
            "freezes",
            "Freezes",
            Some(f64::from(freeze_count)),
            "",
            Better::Lower,
        ),
        SummaryItem::new(
            "frame_latency_p50_ms",
            "Frame latency p50",
            frame_latency.p50,
            "ms",
            Better::Lower,
        )
        .with_uncertainty(uncertainty_ms),
        SummaryItem::new(
            "goodput_mbps",
            "Goodput",
            goodput_mbps,
            "Mb/s",
            Better::Higher,
        ),
    ];
    // Strip and latency series share the sampling.
    let (idx, stride) = strip_indices(all.len(), MAX_STRIP, |i| all[i].state != FrameState::OnTime);
    let frames: Vec<FrameCell> = idx
        .iter()
        .map(|&i| {
            let c = &all[i];
            FrameCell {
                frame_no: c.frame_no as u32,
                keyframe: c.keyframe,
                bytes: model.frame_bytes(c.frame_no),
                state: c.state,
                latency_ms: c.latency_ms.filter(|_| c.state != FrameState::Dropped),
                delay_ms: c.delay_ns.map(|d| d as f64 / 1e6),
                spread_ms: c.spread_ms,
                warmup: c.warmup,
            }
        })
        .collect();
    let warmup_ms = (p.warmup_secs > 0)
        .then(|| rel_ms(sender.t0_ns + model.capture_offset_ns(model.warmup_frames)));
    let latency_series = Series {
        key: "frame_latency_ms".into(),
        label: "Frame latency".into(),
        unit: "ms".into(),
        points: idx
            .iter()
            .filter_map(|&i| {
                let c = &all[i];
                c.latency_ms.map(|l| (rel_ms(capture(c.frame_no)), l))
            })
            .collect(),
        warmup_ms,
    };
    let mut goodput_series = match map {
        Some(m) => stats.receiver.map_time(|t| m.to_local(t)),
        None => stats.receiver.clone(),
    }
    .series(sender.origin_ns, p.warmup_secs, "goodput_mbps", "Goodput");
    if map.is_none() {
        goodput_series.points.clear();
    }
    let window_ns = match (&goodput, map) {
        (Ok(g), Some(m)) => Some((m.to_local(g.start_ns), m.to_local(g.end_ns))),
        _ => None,
    };
    let goodput_bytes = goodput.as_ref().ok().map(|g| g.bytes);
    let datagrams = stats.datagrams.as_ref().map(|d| {
        let sent = sender.datagrams_sent.saturating_sub(sender.send_errors);
        let missing = sent.saturating_sub(d.unique);
        Datagrams {
            sent,
            send_errors: sender.send_errors,
            unique: d.unique,
            missing,
            dup: d.dup,
            reordered: d.reordered,
            loss_pct: stats::pct(missing as f64, sent as f64),
            max_loss_burst: d.max_loss_burst,
        }
    });
    let results = Results {
        transport: p.transport,
        pacing: p.pacing,
        fps: model.fps,
        bitrate_mbps: model.bitrate_mbps,
        p_frame_bytes: model.p_bytes,
        i_frame_bytes: model.i_bytes,
        frames_total: model.frames,
        warmup_frames: model.warmup_frames,
        frames_sent,
        on_time,
        late,
        dropped,
        late_pct,
        dropped_pct,
        delivered_fps,
        freezes: freeze_count,
        freeze_ms,
        frame_latency_ms: frame_latency,
        samples_ms: downsample_sorted(&raw_lat, MAX_RAW_SAMPLES),
        latency_uncertainty_ms: uncertainty_ms,
        relative_delay_ms: Distribution::from_values(&delays),
        frame_spread_p50_ms: frame_spread.p50,
        frame_spread_ms: frame_spread,
        keyframe_complete_p50_ms: keyframe_complete.p50,
        keyframe_complete_ms: keyframe_complete,
        offered_mbps,
        goodput_mbps,
        sent_bytes: sender.sent_bytes,
        received_bytes: stats.receiver.bytes_total,
        receiver: stats.receiver,
        datagrams,
        connect_ms: sender.connect_ms,
        max_send_lag_ms: sender.max_send_lag_ms,
        stream_ack: sender.stream_ack,
        frames,
        frames_stride: stride as u64,
        foreign_dropped: stats.foreign_dropped,
    };
    WorkloadOutput {
        summary,
        results: serde_json::to_value(&results).unwrap_or_default(),
        series: vec![latency_series, goodput_series],
        notes,
        window_ns,
        goodput_bytes,
    }
}

// ---------------------------------------------------------------------------
// Responder
// ---------------------------------------------------------------------------

struct RxState {
    re: Reassembly,
    acc: ReceiverAccounting,
    seq: Option<SeqTracker>,
    bad_datagrams: u64,
    last_arrival_ns: u64,
    stream_done: bool,
}

type SharedRx = Arc<Mutex<RxState>>;

fn lock(s: &SharedRx) -> std::sync::MutexGuard<'_, RxState> {
    s.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Live receiver-side values (approximate: frames still in flight are not
/// decided yet). `offset_ns` (responder − initiator) maps capture times for
/// the live latency only.
fn live_values(
    re: &Reassembly,
    model: &Model,
    jitter_buffer_ms: u32,
    offset_ns: Option<i64>,
) -> Vec<(&'static str, f64)> {
    let complete: Vec<(u64, &RxSlot, u64)> = re
        .frames
        .iter()
        .filter_map(|(k, s)| s.complete_ns.map(|c| (u64::from(*k), s, c)))
        .collect();
    let Some(newest) = complete.iter().map(|(k, _, _)| *k).max() else {
        return Vec::new();
    };
    let min_raw = complete
        .iter()
        .map(|(_, s, c)| *c as i64 - s.capture_ns as i64)
        .min()
        .unwrap_or(0);
    let budget = i64::from(jitter_buffer_ms) * 1_000_000;
    let settle = u64::from(model.fps) * LIVE_DROP_FRAMES_PER_FPS;
    let mut states = Vec::new();
    for k in model.warmup_frames..=newest {
        let slot = re.frames.get(&(k as u32));
        let state = match slot.and_then(|s| s.complete_ns.map(|c| (s, c))) {
            Some((s, c)) => {
                if c as i64 - s.capture_ns as i64 - min_raw > budget {
                    FrameState::Late
                } else {
                    FrameState::OnTime
                }
            }
            None if newest - k > settle => FrameState::Dropped,
            None => continue,
        };
        states.push(state);
    }
    let mut out = vec![("frames_done", re.complete() as f64)];
    let decided = states.len() as f64;
    if decided > 0.0 {
        let n = |s: FrameState| states.iter().filter(|x| **x == s).count() as f64;
        let on_time = n(FrameState::OnTime);
        out.push(("delivered_fps", on_time / (decided / f64::from(model.fps))));
        out.push(("late_pct", n(FrameState::Late) / decided * 100.0));
        out.push(("dropped_pct", n(FrameState::Dropped) / decided * 100.0));
        out.push(("freezes", f64::from(freezes(states.iter().copied()).0)));
    }
    if let Some(off) = offset_ns {
        let lat = |s: &RxSlot, c: u64| (c as i64 - s.capture_ns as i64 - off) as f64 / 1e6;
        if let Some((_, s, c)) = complete.iter().max_by_key(|(k, _, _)| *k) {
            out.push(("frame_latency_ms", lat(s, *c)));
        }
        let mut l: Vec<f64> = complete
            .iter()
            .filter(|(k, _, _)| *k >= model.warmup_frames)
            .map(|(_, s, c)| lat(s, *c))
            .collect();
        if !l.is_empty() {
            let mid = l.len() / 2;
            let (_, m, _) = l.select_nth_unstable_by(mid, f64::total_cmp);
            out.push(("frame_latency_p50_ms", *m));
        }
    }
    out
}

enum RxTransport {
    Udp(Arc<DatagramSocket>),
    Tcp(u16),
    Quic(Arc<QuicListener>),
}

struct VideoResponder {
    r: ResponderCtx,
    p: VideoParams,
    state: SharedRx,
    transport: RxTransport,
    tasks: Tasks<()>,
}

/// Receive datagrams until cancelled.
async fn udp_receiver(r: ResponderCtx, sock: Arc<DatagramSocket>, state: SharedRx) {
    let cancel = r.cancel_token();
    let mut buf = vec![0u8; 2048];
    let mut errors = 0u32;
    loop {
        let got = tokio::select! {
            _ = cancel.cancelled() => return,
            g = sock.recv_from(&mut buf) => g,
        };
        let (n, addr) = match got {
            Ok(x) => {
                errors = 0;
                x
            }
            Err(_) => {
                errors += 1;
                if errors > 100 {
                    return;
                }
                continue;
            }
        };
        let now = now_ns();
        if !r.admit_addr(&addr) {
            continue;
        }
        let Ok(dh) = DatagramHeader::decode_for(&buf[..n], r.run_tag) else {
            r.count_foreign();
            continue;
        };
        let mut st = lock(&state);
        st.last_arrival_ns = now;
        let Ok(fh) = FrameHeader::decode(&buf[DATAGRAM_HEADER_LEN..n]) else {
            st.bad_datagrams += 1;
            continue;
        };
        if let Some(seq) = st.seq.as_mut() {
            seq.record(dh.seq);
        }
        let payload = n - DATAGRAM_HEADER_LEN - FRAME_HEADER_LEN;
        let counted = st.re.fragment(&fh, payload, now);
        if counted > 0 {
            st.acc.record(0, counted, now);
        }
    }
}

/// Read frames from one stream (header already checked) to EOF. Returns the
/// payload bytes read.
async fn read_frames<R: AsyncRead + Unpin>(
    stream: &mut R,
    state: &SharedRx,
    max_len: u64,
) -> io::Result<u64> {
    let mut parser = FrameParser::new(max_len);
    let mut buf = vec![0u8; IO_CHUNK];
    let mut events = Vec::with_capacity(8);
    let mut total = 0u64;
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            return if parser.at_boundary() {
                Ok(total)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "stream ended inside a frame",
                ))
            };
        }
        let now = now_ns();
        events.clear();
        let payload = parser
            .push(&buf[..n], now, &mut events)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        total += payload;
        let mut st = lock(state);
        st.last_arrival_ns = now;
        if payload > 0 {
            st.acc.record(0, payload, now);
        }
        for ev in &events {
            match *ev {
                ParseEvent::Started {
                    header, first_ns, ..
                } => {
                    st.re.frame_started(&header, first_ns);
                }
                ParseEvent::Payload { frame_no, bytes } => st.re.frame_bytes(frame_no, bytes),
                ParseEvent::Completed { frame_no } => st.re.frame_completed(frame_no, now),
            }
        }
    }
}

/// TCP: accept the initiator's frame stream (rule 5 + TBN1), read it, ack it.
async fn tcp_receiver(r: ResponderCtx, listener: RawListener, state: SharedRx, max_len: u64) {
    let mut acceptor = Tbn1Acceptor::new(&r, listener);
    let mut readers = JoinSet::new();
    loop {
        let accepted = tokio::select! {
            a = acceptor.next() => a,
            Some(_) = readers.join_next(), if !readers.is_empty() => continue,
        };
        let Some((mut stream, header)) = accepted else {
            break;
        };
        if header.kind != StreamKind::Frames {
            r.count_foreign();
            continue;
        }
        let _ = stream.set_nodelay(true);
        let st = state.clone();
        readers.spawn(async move {
            if let Ok(n) = read_frames(&mut stream, &st, max_len).await {
                let _ = write_stream_ack(&mut stream, n).await;
            }
            lock(&st).stream_done = true;
        });
    }
    while readers.join_next().await.is_some() {}
}

/// QUIC: accept the initiator's connection (rule 5), then one stream per frame.
async fn quic_receiver(
    r: ResponderCtx,
    listener: Arc<QuicListener>,
    state: SharedRx,
    max_len: u64,
) {
    let cancel = r.cancel_token();
    let mut conns = JoinSet::new();
    loop {
        let conn = tokio::select! {
            _ = cancel.cancelled() => break,
            c = listener.accept() => match c {
                Some(c) => c,
                None => break,
            },
            Some(_) = conns.join_next(), if !conns.is_empty() => continue,
        };
        if !r.admit_ip(conn.remote_address().ip()) {
            conn.close();
            continue;
        }
        let r2 = r.clone();
        let st = state.clone();
        conns.spawn(quic_connection(r2, conn, st, max_len));
    }
}

async fn quic_connection(r: ResponderCtx, conn: QuicConnection, state: SharedRx, max_len: u64) {
    let mut streams = JoinSet::new();
    loop {
        let next = tokio::select! {
            s = conn.accept_stream() => s,
            Some(_) = streams.join_next(), if !streams.is_empty() => continue,
        };
        let Ok(Some(s)) = next else {
            break;
        };
        let (mut send, mut recv) = s.into_split();
        // The receiver finishes its send half at once (§8.2 video).
        let _ = send.finish();
        let r2 = r.clone();
        let st = state.clone();
        streams.spawn(async move {
            match read_tbn1(&r2, &mut recv).await {
                Some(h) if h.kind == StreamKind::Frames => {
                    let _ = read_frames(&mut recv, &st, max_len).await;
                }
                Some(_) => r2.count_foreign(),
                None => {}
            }
            drop(send);
        });
    }
    while streams.join_next().await.is_some() {}
}

fn raise_recv_buffer(sock: &DatagramSocket) {
    let inner = match sock {
        DatagramSocket::Network { socket } => socket.inner(),
        DatagramSocket::Direct { socket } => socket,
    };
    let _ = socket2::SockRef::from(inner).set_recv_buffer_size(RECV_BUFFER);
}

/// Responder: bind the receiver for the run's transport.
pub async fn prepare(
    r: &ResponderCtx,
    p: &VideoParams,
    _client: Client,
) -> Result<(Setup, Box<dyn ResponderRun>), AppError> {
    let model = Model::new(p);
    let max_len = model.i_bytes.max(model.p_bytes);
    if p.transport == VideoTransport::Udp {
        model.check_udp()?;
    }
    let state: SharedRx = Arc::new(Mutex::new(RxState {
        re: Reassembly::new(model.frames),
        acc: ReceiverAccounting::new(),
        seq: (p.transport == VideoTransport::Udp).then(|| SeqTracker::new(model.total_datagrams())),
        bad_datagrams: 0,
        last_arrival_ns: 0,
        stream_done: false,
    }));
    let mut tasks = Vec::new();
    let (port, transport) = match p.transport {
        VideoTransport::Udp => {
            let sock = Arc::new(r.node.bind_udp(0).await?);
            raise_recv_buffer(&sock);
            let port = match &*sock {
                DatagramSocket::Network { socket } => socket.tsnet_port(),
                DatagramSocket::Direct { socket } => socket
                    .local_addr()
                    .map_err(|e| AppError::io("UDP socket address", &e))?
                    .port(),
            };
            tasks.push(tokio::spawn(udp_receiver(
                r.clone(),
                sock.clone(),
                state.clone(),
            )));
            (port, RxTransport::Udp(sock))
        }
        VideoTransport::Tcp => {
            let listener = r.node.listen_tcp(0).await?;
            let port = listener.port;
            tasks.push(tokio::spawn(tcp_receiver(
                r.clone(),
                listener,
                state.clone(),
                max_len,
            )));
            (port, RxTransport::Tcp(port))
        }
        VideoTransport::Quic => {
            let listener = Arc::new(r.node.listen_quic(0).await?);
            let port = listener.port();
            tasks.push(tokio::spawn(quic_receiver(
                r.clone(),
                listener.clone(),
                state.clone(),
                max_len,
            )));
            (port, RxTransport::Quic(listener))
        }
    };
    let tick = {
        let r = r.clone();
        let st = state.clone();
        let jitter = p.jitter_buffer_ms;
        let offset = _client.clock_offset_ns;
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(Duration::from_millis(250));
            iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut last = (Instant::now(), 0u64);
            loop {
                iv.tick().await;
                if r.is_cancelled() {
                    break;
                }
                let (mut values, bytes) = {
                    let s = lock(&st);
                    (
                        live_values(&s.re, &model, jitter, offset),
                        s.acc.bytes_total(),
                    )
                };
                if values.is_empty() {
                    continue;
                }
                let now = Instant::now();
                let dt = now.duration_since(last.0).as_secs_f64();
                if dt > 0.0 {
                    values.push((
                        "goodput_mbps",
                        bytes.saturating_sub(last.1) as f64 * 8.0 / dt / 1e6,
                    ));
                }
                last = (now, bytes);
                r.tick(values).await;
            }
        })
    };
    tasks.push(tick);
    Ok((
        Setup { port },
        Box::new(VideoResponder {
            r: r.clone(),
            p: p.clone(),
            state,
            transport,
            tasks: Tasks::from(tasks),
        }),
    ))
}

impl VideoResponder {
    /// Rule 7: close the QUIC listener, unlisten TCP, then drop the tasks and sockets.
    async fn teardown(self) -> (SharedRx, ResponderCtx) {
        let VideoResponder {
            r,
            state,
            transport,
            tasks,
            ..
        } = self;
        let mut t = r.teardown();
        match transport {
            RxTransport::Quic(listener) => {
                listener.close();
                t.hold(listener);
            }
            RxTransport::Tcp(port) => t.tcp_port(port),
            RxTransport::Udp(sock) => t.hold(sock),
        }
        t.hold(tasks);
        t.run().await;
        (state, r)
    }
}

impl ResponderRun for VideoResponder {
    fn start(&mut self) -> BoxFuture<'_, Result<(), AppError>> {
        Box::pin(async { Ok(()) })
    }

    fn finish(self: Box<Self>, info: Value) -> BoxFuture<'static, Result<Value, AppError>> {
        Box::pin(async move {
            let mut info: Info = serde_json::from_value(info)
                .map_err(|e| AppError::invalid_params(format!("video info: {e}")))?;
            // The initiator's counts, never more than the validated params
            // send (§6 rule 2).
            let model = Model::new(&self.p);
            info.frames_sent = info.frames_sent.min(model.frames);
            info.datagrams_sent = info.datagrams_sent.min(model.total_datagrams());
            // Drain: every frame complete, the TCP stream ended, 500 ms
            // without arrivals, or 3 s in all.
            let began = Instant::now();
            let began_ns = now_ns();
            loop {
                let (complete, last, done) = {
                    let s = lock(&self.state);
                    (s.re.complete(), s.last_arrival_ns, s.stream_done)
                };
                let idle_ns = now_ns().saturating_sub(last.max(began_ns));
                if complete >= info.frames_sent
                    || done
                    || idle_ns >= DRAIN_IDLE.as_nanos() as u64
                    || began.elapsed() >= DRAIN_CAP
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let warmup = self.p.warmup_secs;
            let (state, r) = self.teardown().await;
            let s = lock(&state);
            let receiver = s.acc.stats();
            let datagrams = s.seq.as_ref().map(|seq| {
                let loss = seq.loss(info.datagrams_sent);
                RxDatagrams {
                    unique: seq.unique(),
                    dup: seq.dup(),
                    reordered: seq.reordered(),
                    max_loss_burst: loss.max_loss_burst,
                    bad: s.bad_datagrams + s.re.bad() + seq.out_of_range(),
                }
            });
            match receiver.goodput(warmup) {
                Ok(g) => r.set_resource_window(g.start_ns, g.end_ns, Some(g.bytes)),
                Err(_) if receiver.last_ns > receiver.epoch_ns => r.set_resource_window(
                    receiver.epoch_ns,
                    receiver.last_ns,
                    Some(receiver.bytes_total),
                ),
                Err(_) => {}
            }
            let stats = Stats {
                frames: s.re.frames(),
                receiver,
                datagrams,
                foreign_dropped: r.foreign_dropped(),
            };
            drop(s);
            serde_json::to_value(stats).map_err(|e| AppError::internal(format!("video stats: {e}")))
        })
    }

    fn cancel(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.teardown().await;
        })
    }
}

// ---------------------------------------------------------------------------
// Initiator
// ---------------------------------------------------------------------------

/// Write one frame on a byte stream: `[u32 BE len][header][payload]`, the
/// prefix and the first payload bytes in one write.
async fn write_frame_fast<W: AsyncWrite + Unpin>(
    w: &mut W,
    header: &FrameHeader,
    len: u64,
) -> io::Result<()> {
    let len32 = u32::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame larger than 4 GiB"))?;
    let chunk = payload_chunk();
    let first = (len as usize).min(chunk.len() - 4 - FRAME_HEADER_LEN);
    let mut out = Vec::with_capacity(4 + FRAME_HEADER_LEN + first);
    out.extend_from_slice(&len32.to_be_bytes());
    out.extend_from_slice(&header.encode());
    out.extend_from_slice(&chunk[..first]);
    w.write_all(&out).await?;
    let mut left = len - first as u64;
    while left > 0 {
        let n = left.min(chunk.len() as u64) as usize;
        w.write_all(&chunk[..n]).await?;
        left -= n as u64;
    }
    Ok(())
}

async fn send_quic_frame(
    conn: Arc<QuicConnection>,
    stream: StreamHeader,
    header: FrameHeader,
    len: u64,
) -> Result<u64, String> {
    let s = conn.open_stream().await.map_err(|e| e.to_string())?;
    let (mut send, recv) = s.into_split();
    drop(recv);
    write_stream_frame(&mut send, &stream, &header, len)
        .await
        .map_err(|e| e.to_string())?;
    send.finish().map_err(|e| e.to_string())?;
    Ok(len)
}

async fn write_stream_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    stream: &StreamHeader,
    header: &FrameHeader,
    len: u64,
) -> io::Result<()> {
    write_header(w, stream).await?;
    write_frame_fast(w, header, len).await
}

enum Tx {
    Udp {
        sock: DatagramSocket,
        addr: String,
        buf: Vec<u8>,
        seq: u64,
        errors: u64,
        /// Boxed: by far the largest part of `Tx`.
        pacer: Box<Pacer>,
    },
    Tcp(tokio::net::TcpStream),
    Quic {
        conn: Arc<QuicConnection>,
        tasks: JoinSet<Result<u64, String>>,
        failures: Vec<String>,
    },
}

/// Initiator: send `warmup + duration` seconds of frames, then analyse.
pub async fn run(ctx: &mut RunCtx<'_>, p: &VideoParams) -> Result<WorkloadOutput, AppError> {
    if p.transport == VideoTransport::Udp {
        Model::new(p).check_udp()?;
    }
    // Before estimate first: its offset also feeds the receiver's live latency.
    let before = ctx.clock().await?;
    let setup: Setup = ctx
        .prepare(&Client {
            clock_offset_ns: Some(before.offset_ns),
        })
        .await?;
    let p = match ctx.params() {
        WorkloadParams::Video(used) => used.clone(),
        _ => p.clone(),
    };
    let model = Model::new(&p);
    let tag = ctx.run_tag()?;
    let peer_id = ctx.peer_id().to_string();

    // Connect.
    let connect_started = Instant::now();
    let mut tx = match p.transport {
        VideoTransport::Udp => Tx::Udp {
            sock: ctx.node.bind_udp(0).await?,
            addr: format!("{}:{}", ctx.peer_ip(), setup.port),
            buf: vec![0u8; DATAGRAM_HEADER_LEN + FRAME_HEADER_LEN + VIDEO_MAX_FRAGMENT],
            seq: 0,
            errors: 0,
            pacer: Box::new(Pacer::new(p.effective_bitrate_mbps(), VIDEO_MAX_DATAGRAM)),
        },
        VideoTransport::Tcp => {
            let mut s = ctx.node.open_tcp(&peer_id, setup.port).await?;
            let _ = s.set_nodelay(true);
            write_header(
                &mut s,
                &StreamHeader {
                    run_tag: tag,
                    stream_idx: 0,
                    kind: StreamKind::Frames,
                },
            )
            .await
            .map_err(|e| AppError::io("video: write the stream header", &e))?;
            Tx::Tcp(s)
        }
        VideoTransport::Quic => Tx::Quic {
            conn: Arc::new(ctx.node.connect_quic(&peer_id, setup.port).await?),
            tasks: JoinSet::new(),
            failures: Vec::new(),
        },
    };
    let connect_ms = (p.transport != VideoTransport::Udp)
        .then(|| connect_started.elapsed().as_secs_f64() * 1000.0);

    ctx.start().await?;
    let live = ctx.live();
    live.set("clock_uncertainty_ms", before.uncertainty_ns as f64 / 1e6);
    let t0 = Instant::now() + START_LEAD;
    let t0_ns = instant_ns(t0.into_std());
    ctx.phase(if model.warmup_frames > 0 {
        Phase::Warmup
    } else {
        Phase::Running
    });
    let cancel = ctx.cancel_token();
    let period_ns = model.period_ns();
    let mut sent_bytes = 0u64;
    let mut offered_bytes = 0u64;
    let mut max_lag_ns = 0u64;
    for k in 0..model.frames {
        if k == model.warmup_frames && k > 0 {
            ctx.phase(Phase::Running);
        }
        let offset = model.capture_offset_ns(k);
        let capture = t0 + Duration::from_nanos(offset);
        tokio::select! {
            _ = cancel.cancelled() => return Err(AppError::cancelled()),
            _ = tokio::time::sleep_until(capture) => {}
        }
        max_lag_ns =
            max_lag_ns.max(Instant::now().saturating_duration_since(capture).as_nanos() as u64);
        let bytes = model.frame_bytes(k);
        let keyframe = model.is_keyframe(k);
        let header = FrameHeader {
            frame_no: k as u32,
            frag_idx: 0,
            frag_cnt: 1,
            keyframe,
            capture_ns: t0_ns + offset,
        };
        match &mut tx {
            Tx::Udp {
                sock,
                addr,
                buf,
                seq,
                errors,
                pacer,
            } => {
                let frag_cnt = video_fragments(bytes);
                let start = DATAGRAM_HEADER_LEN + FRAME_HEADER_LEN;
                let paced = p.pacing == Pacing::Paced;
                if paced {
                    // This frame's datagrams, spread over [capture, capture + 1/fps).
                    let wire = bytes + frag_cnt * start as u64;
                    pacer.restart_at(capture, wire as f64 * 8.0 / period_ns as f64 * 1e3);
                }
                for idx in 0..frag_cnt {
                    let len = fragment_len(bytes, idx);
                    if paced {
                        tokio::select! {
                            _ = cancel.cancelled() => return Err(AppError::cancelled()),
                            _ = pacer.wait(start + len) => {}
                        }
                    }
                    let fh = FrameHeader {
                        frag_idx: idx as u16,
                        frag_cnt: frag_cnt as u16,
                        ..header
                    };
                    buf[DATAGRAM_HEADER_LEN..start].copy_from_slice(&fh.encode());
                    let chunk = payload_chunk();
                    buf[start..start + len].copy_from_slice(&chunk[..len]);
                    let dh = DatagramHeader {
                        run_tag_lo: run_tag_lo(tag),
                        seq: *seq,
                        send_ns: now_ns(),
                    };
                    buf[..DATAGRAM_HEADER_LEN].copy_from_slice(&dh.encode());
                    match sock.send_to(&buf[..start + len], addr).await {
                        Ok(_) => {
                            sent_bytes += len as u64;
                            pacer.record_sent(start + len);
                        }
                        Err(_) => *errors += 1,
                    }
                    *seq += 1;
                }
            }
            Tx::Tcp(stream) => {
                tokio::select! {
                    _ = cancel.cancelled() => return Err(AppError::cancelled()),
                    w = write_frame_fast(stream, &header, bytes) => {
                        w.map_err(|e| AppError::io(format!("video: write frame {k}"), &e))?;
                    }
                }
                sent_bytes += bytes;
            }
            Tx::Quic {
                conn,
                tasks,
                failures,
            } => {
                while let Some(done) = tasks.try_join_next() {
                    match done {
                        Ok(Ok(n)) => sent_bytes += n,
                        Ok(Err(e)) => failures.push(e),
                        Err(e) => failures.push(e.to_string()),
                    }
                }
                let stream = StreamHeader {
                    run_tag: tag,
                    stream_idx: k as u16,
                    kind: StreamKind::Frames,
                };
                tasks.spawn(send_quic_frame(conn.clone(), stream, header, bytes));
            }
        }
        offered_bytes += bytes;
        let sent_frames = k + 1;
        live.set("frames_sent", sent_frames as f64);
        live.set(
            "offered_mbps",
            offered_bytes as f64 * 8.0 / (sent_frames as f64 / f64::from(model.fps)) / 1e6,
        );
    }

    // Drain.
    ctx.phase(Phase::Draining);
    let rtt = ctx.before_rtt().unwrap_or_default();
    let wait = (rtt * 4).clamp(DRAIN_IDLE, DRAIN_CAP);
    let mut notes = Vec::new();
    let mut stream_ack = None;
    let mut quic = None;
    let (datagrams_sent, send_errors) = match tx {
        Tx::Udp { seq, errors, .. } => {
            tokio::time::sleep(wait).await;
            (seq, errors)
        }
        Tx::Tcp(mut stream) => {
            let acked = tokio::time::timeout(STREAM_ACK_TIMEOUT, async {
                stream.shutdown().await?;
                read_stream_ack(&mut stream).await
            })
            .await;
            match acked {
                Ok(Ok(n)) => {
                    stream_ack = Some(n);
                    if n != sent_bytes {
                        notes.push(format!(
                            "the receiver acknowledged {n} payload bytes of {sent_bytes} sent"
                        ));
                    }
                }
                Ok(Err(e)) => notes.push(format!("no stream ack from the receiver: {e}")),
                Err(_) => notes.push(format!(
                    "no stream ack from the receiver within {} s",
                    STREAM_ACK_TIMEOUT.as_secs()
                )),
            }
            (0, 0)
        }
        Tx::Quic {
            conn,
            mut tasks,
            mut failures,
        } => {
            let joined = tokio::time::timeout(QUIC_JOIN_TIMEOUT, async {
                while let Some(done) = tasks.join_next().await {
                    match done {
                        Ok(Ok(n)) => sent_bytes += n,
                        Ok(Err(e)) => failures.push(e),
                        Err(e) => failures.push(e.to_string()),
                    }
                }
            })
            .await;
            if joined.is_err() {
                notes.push(format!(
                    "{} frame streams were still being written after {} s",
                    tasks.len(),
                    QUIC_JOIN_TIMEOUT.as_secs()
                ));
            }
            if !failures.is_empty() {
                notes.push(format!(
                    "{} frame streams failed on the sender (first: {})",
                    failures.len(),
                    failures[0]
                ));
            }
            tokio::time::sleep(wait).await;
            // Keep the connection open until the receiver has drained.
            quic = Some(conn);
            (0, 0)
        }
    };
    let info = Info {
        t0_ns,
        frames_sent: model.frames,
        datagrams_sent,
        send_errors,
        sent_bytes,
    };
    ctx.set_partial(sender_only(&p, &info, connect_ms, max_lag_ns));
    let finished = finish_and_clock(ctx, &info).await;
    if let Some(conn) = quic {
        conn.close();
    }
    let (stats, after_note) = finished?;
    notes.extend(after_note);
    let received = stats.receiver.bytes_total;
    let out = conclude(
        ctx, &p, stats, t0_ns, &info, connect_ms, max_lag_ns, stream_ack, notes,
    );
    if received == 0 {
        ctx.set_partial(out);
        return Err(AppError::new(
            ErrorCode::Io,
            format!(
                "no video payload reached the peer over {} ({} frames sent)",
                transport_name(p.transport),
                model.frames
            ),
        ));
    }
    Ok(out)
}

fn transport_name(t: VideoTransport) -> &'static str {
    match t {
        VideoTransport::Udp => "UDP",
        VideoTransport::Quic => "QUIC",
        VideoTransport::Tcp => "TCP",
    }
}

/// What a failed run keeps when the receiver never answered `finish`: the
/// sender's side only, every receiver figure null.
fn sender_only(
    p: &VideoParams,
    info: &Info,
    connect_ms: Option<f64>,
    max_lag_ns: u64,
) -> WorkloadOutput {
    let model = Model::new(p);
    let results = Results {
        transport: p.transport,
        pacing: p.pacing,
        fps: model.fps,
        bitrate_mbps: model.bitrate_mbps,
        p_frame_bytes: model.p_bytes,
        i_frame_bytes: model.i_bytes,
        frames_total: model.frames,
        warmup_frames: model.warmup_frames,
        frames_sent: model.frames - model.warmup_frames,
        offered_mbps: model.offered_mbps(model.warmup_frames, model.frames),
        sent_bytes: info.sent_bytes,
        connect_ms,
        max_send_lag_ms: max_lag_ns as f64 / 1e6,
        ..Results::default()
    };
    WorkloadOutput {
        summary: vec![
            SummaryItem::new("delivered_fps", "Delivered", None, "fps", Better::Higher),
            SummaryItem::new("late_pct", "Late", None, "%", Better::Lower),
            SummaryItem::new("dropped_pct", "Dropped", None, "%", Better::Lower),
            SummaryItem::new("freezes", "Freezes", None, "", Better::Lower),
            SummaryItem::new(
                "frame_latency_p50_ms",
                "Frame latency p50",
                None,
                "ms",
                Better::Lower,
            ),
            SummaryItem::new("goodput_mbps", "Goodput", None, "Mb/s", Better::Higher),
        ],
        results: serde_json::to_value(&results).unwrap_or_default(),
        ..WorkloadOutput::default()
    }
}

/// `finish`, then the after clock estimate (a failed one only costs a note).
async fn finish_and_clock(
    ctx: &mut RunCtx<'_>,
    info: &Info,
) -> Result<(Stats, Option<String>), AppError> {
    let stats: Stats = ctx.finish(info, DRAIN_CAP + Duration::from_secs(2)).await?;
    let note = match ctx.clock().await {
        Ok(_) => None,
        Err(e) => Some(format!(
            "no clock estimate after the run ({}); frame latency uses the before estimate only",
            e.message
        )),
    };
    Ok((stats, note))
}

#[allow(clippy::too_many_arguments)]
fn conclude(
    ctx: &RunCtx<'_>,
    p: &VideoParams,
    stats: Stats,
    t0_ns: u64,
    info: &Info,
    connect_ms: Option<f64>,
    max_lag_ns: u64,
    stream_ack: Option<u64>,
    mut notes: Vec<String>,
) -> WorkloadOutput {
    let model = Model::new(p);
    notes.extend(pacing_note(p));
    let max_send_lag_ms = max_lag_ns as f64 / 1e6;
    if max_lag_ns > model.period_ns() {
        notes.push(format!(
            "the sender fell up to {max_send_lag_ms:.1} ms behind its capture schedule; frames are \
             never skipped, so that shows as latency"
        ));
    }
    let map = ctx.clock_map();
    let sender = SenderSide {
        t0_ns,
        origin_ns: ctx.origin_ns(),
        datagrams_sent: info.datagrams_sent,
        send_errors: info.send_errors,
        sent_bytes: info.sent_bytes,
        connect_ms,
        max_send_lag_ms,
        stream_ack,
    };
    analyse(p, stats, map.as_ref(), &sender, notes)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Over TCP and QUIC a `paced` run is written frame by frame: say so.
fn pacing_note(p: &VideoParams) -> Option<String> {
    (p.transport != VideoTransport::Udp && p.pacing == Pacing::Paced).then(|| {
        "pacing applies to UDP fragments; over TCP and QUIC each frame is written whole at its \
         capture time"
            .to_string()
    })
}

/// Synthetic receiver stats for `p`: every frame arrives `base_ms` plus a
/// little jitter after capture (a few late, one dropped), in responder time
/// `offset_ns` ahead.
fn synthetic_stats(p: &VideoParams, t0_ns: u64, offset_ns: i64, base_ms: f64) -> Stats {
    let model = Model::new(p);
    let mut re = Reassembly::new(model.frames);
    let mut acc = ReceiverAccounting::new();
    for k in 0..model.frames {
        if k == 211 {
            continue; // dropped
        }
        let capture = t0_ns + model.capture_offset_ns(k);
        let jitter_us = (k * 7919 % 900) as f64 + if k % 97 == 13 { 150_000.0 } else { 0.0 };
        let done = (capture as f64 + offset_ns as f64 + base_ms * 1e6 + jitter_us * 1e3) as u64;
        let bytes = model.frame_bytes(k);
        let h = FrameHeader {
            frame_no: k as u32,
            frag_idx: 0,
            frag_cnt: 1,
            keyframe: model.is_keyframe(k),
            capture_ns: capture,
        };
        // About 40 ns per byte between first and last fragment.
        let first = done.saturating_sub(bytes * 40).max(capture);
        re.frame_started(&h, first);
        re.frame_bytes(k as u32, bytes);
        re.frame_completed(k as u32, done);
        acc.record(0, bytes, done);
    }
    Stats {
        frames: re.frames(),
        receiver: acc.stats(),
        datagrams: None,
        foreign_dropped: 0,
    }
}

/// A canonical example output for the schema fixtures (1080p30 over UDP, paced, 10 s).
pub fn example_output() -> WorkloadOutput {
    example_for(&VideoParams::default(), 7.4, None)
}

/// 1080p30 over QUIC, one stream per frame (lower latency than paced UDP,
/// which spreads each frame over its interval).
fn example_quic() -> WorkloadOutput {
    let p = VideoParams {
        transport: VideoTransport::Quic,
        ..VideoParams::default()
    };
    example_for(&p, 2.7, Some(47.2))
}

/// One example per video mode for the schema fixtures.
pub fn examples() -> Vec<super::ModeExample> {
    vec![
        super::ModeExample {
            mode: "video-udp",
            params: WorkloadParams::Video(VideoParams::default()),
            output: example_output(),
            clock: true,
        },
        super::ModeExample {
            mode: "video-quic",
            params: WorkloadParams::Video(VideoParams {
                transport: VideoTransport::Quic,
                ..VideoParams::default()
            }),
            output: example_quic(),
            clock: true,
        },
    ]
}

/// The example for `p`: frames complete `base_ms` after capture (plus
/// jitter); `connect_ms` for the stream transports.
fn example_for(p: &VideoParams, base_ms: f64, connect_ms: Option<f64>) -> WorkloadOutput {
    let p = p.clone();
    let udp = p.transport == VideoTransport::Udp;
    let origin = 1_760_000_000_000_000_000u64;
    let t0 = origin + 600_000_000;
    let offset = 2_500_000i64;
    let mut stats = synthetic_stats(&p, t0, offset, base_ms);
    let model = Model::new(&p);
    if udp {
        stats.datagrams = Some(RxDatagrams {
            unique: model.total_datagrams() - 20,
            dup: 0,
            reordered: 3,
            max_loss_burst: 20,
            bad: 0,
        });
    }
    let before = crate::types::ClockOffset {
        offset_ns: offset,
        delay_ns: 1_200_000,
        uncertainty_ns: 600_000,
        at_ns: origin + 400_000_000,
        samples: 16,
    };
    let after = crate::types::ClockOffset {
        at_ns: origin + 12_400_000_000,
        offset_ns: offset + 40_000,
        ..before
    };
    let map = ClockMap::new(before, Some(after));
    let sent_bytes: u64 = (0..model.frames).map(|k| model.frame_bytes(k)).sum();
    let sender = SenderSide {
        t0_ns: t0,
        origin_ns: origin,
        datagrams_sent: if udp { model.total_datagrams() } else { 0 },
        send_errors: 0,
        sent_bytes,
        connect_ms,
        max_send_lag_ms: 1.3,
        stream_ack: None,
    };
    let notes = pacing_note(&p).into_iter().collect();
    let mut out = analyse(&p, stats, Some(&map), &sender, notes);
    // Keep the fixture small: about 30 frames, 40 CDF samples, 60 points a series.
    if let Some(obj) = out.results.as_object_mut() {
        for (key, keep) in [("frames", 30usize), ("samples_ms", 40)] {
            if let Some(arr) = obj.get_mut(key).and_then(Value::as_array_mut) {
                let n = arr.len();
                let stride = n.div_ceil(keep).max(1);
                let kept: Vec<Value> = arr.iter().step_by(stride).cloned().collect();
                *arr = kept;
                if key == "frames" {
                    obj.insert("frames_stride".into(), Value::from(stride as u64));
                }
            }
        }
    }
    for s in &mut out.series {
        let n = s.points.len();
        let stride = n.div_ceil(60).max(1);
        s.points = s.points.iter().step_by(stride).copied().collect();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::params::VideoProfile;

    #[test]
    fn frame_model_offers_the_bitrate_over_whole_gops() {
        for profile in [
            VideoProfile::P720p30,
            VideoProfile::P1080p30,
            VideoProfile::P1080p60,
            VideoProfile::P4k30,
        ] {
            let p = VideoParams {
                profile,
                ..VideoParams::default()
            };
            let m = Model::new(&p);
            let gop = u64::from(m.gop);
            let offered = m.offered_mbps(0, 2 * gop).unwrap();
            let want = m.bitrate_mbps;
            // P is rounded to whole bytes.
            assert!(
                (offered - want).abs() / want < 1e-3,
                "{profile:?}: {offered} vs {want}"
            );
            assert_eq!(m.i_bytes, 8 * m.p_bytes);
            assert!(m.is_keyframe(0) && m.is_keyframe(gop) && !m.is_keyframe(1));
        }
        let m = Model::new(&VideoParams::default());
        assert_eq!(m.fps, 30);
        assert_eq!(m.frames, 330);
        assert_eq!(m.warmup_frames, 30);
        assert_eq!(m.capture_offset_ns(3), 100_000_000);
        assert_eq!(m.period_ns(), 33_333_333);
        assert_eq!(m.offered_mbps(5, 5), None);
        // A keyframe too large for u16 fragment counts is refused for UDP.
        let huge = VideoParams {
            bitrate_mbps: Some(100.0),
            fps: Some(1),
            gop: 60,
            keyframe_ratio: 20,
            duration_secs: 60,
            ..VideoParams::default()
        };
        assert!(Model::new(&huge).check_udp().is_err());
        assert!(m.check_udp().is_ok());
    }

    #[test]
    fn fragments_and_pacing() {
        assert_eq!(fragment_len(2500, 0), 1160);
        assert_eq!(fragment_len(2500, 1), 1160);
        assert_eq!(fragment_len(2500, 2), 180);
        assert_eq!(fragment_len(2500, 3), 0);
        let total: usize = (0..video_fragments(2500))
            .map(|i| fragment_len(2500, i))
            .sum();
        assert_eq!(total, 2500);
    }

    #[tokio::test(start_paused = true)]
    async fn paced_fragments_spread_over_the_frame_interval() {
        // A 1080p30 keyframe: 155 fragments over 33.3 ms, as `run` paces them.
        let bytes = 155 * VIDEO_MAX_FRAGMENT as u64;
        let frag_cnt = video_fragments(bytes);
        let start = DATAGRAM_HEADER_LEN + FRAME_HEADER_LEN;
        let period_ns = 33_333_333u64;
        let mut pacer = Pacer::new(6.0, VIDEO_MAX_DATAGRAM);
        for frame in 0..3u64 {
            let capture = Instant::now() + Duration::from_millis(1);
            tokio::time::sleep_until(capture).await;
            let wire = bytes + frag_cnt * start as u64;
            pacer.restart_at(capture, wire as f64 * 8.0 / period_ns as f64 * 1e3);
            let mut per_ms = BTreeMap::new();
            for idx in 0..frag_cnt {
                pacer.wait(start + fragment_len(bytes, idx)).await;
                let ms = Instant::now().duration_since(capture).as_millis();
                *per_ms.entry(ms).or_insert(0u32) += 1;
            }
            let last = *per_ms.keys().last().unwrap();
            assert!((30..33).contains(&last), "frame {frame}: last at {last} ms");
            // About 4.7 datagrams per ms, after an initial bucket of ~9.
            assert!(per_ms[&0] <= 15, "{per_ms:?}");
            assert!(per_ms.values().skip(1).all(|n| *n <= 6), "{per_ms:?}");
        }
    }

    fn fh(frame_no: u32, frag_idx: u16, frag_cnt: u16) -> FrameHeader {
        FrameHeader {
            frame_no,
            frag_idx,
            frag_cnt,
            keyframe: frame_no == 0,
            capture_ns: 1000 + u64::from(frame_no),
        }
    }

    #[test]
    fn udp_reassembly_handles_reorder_dups_and_bad_fragments() {
        let mut re = Reassembly::new(10);
        assert_eq!(re.fragment(&fh(0, 2, 3), 100, 50), 100);
        assert_eq!(re.fragment(&fh(0, 0, 3), 1160, 51), 1160);
        assert_eq!(re.fragment(&fh(0, 0, 3), 1160, 52), 0); // duplicate
        assert_eq!(re.complete(), 0);
        assert_eq!(re.fragment(&fh(0, 1, 3), 1160, 60), 1160);
        assert_eq!(re.complete(), 1);
        assert_eq!(re.fragment(&fh(1, 0, 2), 10, 70), 10);
        assert_eq!(re.fragment(&fh(1, 2, 2), 10, 71), 0); // idx ≥ cnt
        assert_eq!(re.fragment(&fh(1, 1, 3), 10, 72), 0); // count changed
        assert_eq!(re.fragment(&fh(10, 0, 1), 10, 73), 0); // beyond the run
        assert_eq!(re.fragment(&fh(2, 0, 0), 10, 74), 0); // zero count
        assert_eq!(re.bad(), 4);
        assert_eq!(re.dup_frags(), 1);
        let frames = re.frames();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].bytes, 2420);
        assert_eq!(frames[0].first_recv_ns, 50);
        assert_eq!(frames[0].complete_recv_ns, Some(60));
        assert_eq!(frames[0].capture_ns, 1000);
        assert!(frames[0].keyframe);
        assert_eq!((frames[1].frags, frames[1].frag_cnt), (1, 2));
        assert_eq!(frames[1].complete_recv_ns, None);
        // A large frame (more than 64 fragments) uses several bitset words.
        let mut big = Reassembly::new(1);
        for i in (0..130).rev() {
            big.fragment(&fh(0, i, 130), 1, 100 + u64::from(i));
        }
        assert_eq!(big.complete(), 1);
        assert_eq!(big.frames()[0].complete_recv_ns, Some(100));
    }

    #[test]
    fn stream_parser_splits_frames_across_chunks() {
        let mut wire = Vec::new();
        for (no, len) in [(0u32, 5000u64), (1, 0), (2, 3)] {
            wire.extend_from_slice(&(len as u32).to_be_bytes());
            wire.extend_from_slice(&fh(no, 0, 1).encode());
            wire.extend(std::iter::repeat_n(7u8, len as usize));
        }
        for chunk in [1usize, 7, 22, 23, 999, 100_000] {
            let mut parser = FrameParser::new(10_000);
            let mut re = Reassembly::new(5);
            let mut events = Vec::new();
            let mut payload = 0;
            for (i, c) in wire.chunks(chunk).enumerate() {
                events.clear();
                payload += parser.push(c, i as u64, &mut events).unwrap();
                for ev in &events {
                    match *ev {
                        ParseEvent::Started {
                            header, first_ns, ..
                        } => {
                            assert!(re.frame_started(&header, first_ns));
                        }
                        ParseEvent::Payload { frame_no, bytes } => re.frame_bytes(frame_no, bytes),
                        ParseEvent::Completed { frame_no } => {
                            re.frame_completed(frame_no, i as u64)
                        }
                    }
                }
            }
            assert!(parser.at_boundary(), "chunk {chunk}");
            assert_eq!(payload, 5003, "chunk {chunk}");
            let frames = re.frames();
            assert_eq!(frames.len(), 3);
            assert!(frames.iter().all(|f| f.complete_recv_ns.is_some()));
            assert_eq!(frames[0].bytes, 5000);
            assert_eq!(frames[0].first_recv_ns, 0);
            if chunk == 1 {
                // Frame 0 completes on the chunk holding its last byte.
                assert_eq!(frames[0].complete_recv_ns, Some(22 + 5000 - 1));
                assert_eq!(frames[1].first_recv_ns, 22 + 5000);
            }
        }
        // Oversized and malformed frames are refused.
        let mut parser = FrameParser::new(100);
        let mut ev = Vec::new();
        let mut bad = (101u32).to_be_bytes().to_vec();
        bad.extend_from_slice(&fh(0, 0, 1).encode());
        assert!(parser.push(&bad, 0, &mut ev).is_err());
        let mut parser = FrameParser::new(100);
        assert!(parser.push(&[0, 0, 0, 1, 0], 0, &mut ev).is_ok());
        assert!(!parser.at_boundary());
    }

    fn rx(no: u32, capture: u64, first: u64, complete: Option<u64>) -> RxFrame {
        RxFrame {
            frame_no: no,
            keyframe: no == 0,
            bytes: 100,
            capture_ns: capture,
            first_recv_ns: first,
            complete_recv_ns: complete,
            frags: 1,
            frag_cnt: 1,
        }
    }

    #[test]
    fn classification_is_clock_free_and_latency_is_mapped() {
        // 10 fps, 1 s warmup + 2 s: 30 frames, 10 warmup.
        let p = VideoParams {
            fps: Some(10),
            gop: 10,
            duration_secs: 2,
            warmup_secs: 1,
            jitter_buffer_ms: 50,
            ..VideoParams::default()
        };
        let m = Model::new(&p);
        assert_eq!((m.frames, m.warmup_frames), (30, 10));
        let t0 = 1_000_000_000_000u64;
        let capture = |k: u64| t0 + m.capture_offset_ns(k);
        // Responder clock is 5 s ahead; base delay 3 ms; frame 15 late by
        // 80 ms, frames 20–21 dropped, frame 25 5 ms slower (on time).
        let offset: i64 = 5_000_000_000;
        let frames: Vec<RxFrame> = (0..30u64)
            .filter(|k| *k != 20 && *k != 21)
            .map(|k| {
                let extra = match k {
                    15 => 80_000_000,
                    25 => 5_000_000,
                    _ => 0,
                };
                let c = (capture(k) as i64 + offset) as u64 + 3_000_000 + extra;
                rx(k as u32, capture(k), c - 1_000_000, Some(c))
            })
            .collect();
        let to_local = |t: u64| (t as i64 - offset) as u64;
        let all = classify(&m, p.jitter_buffer_ms, &frames, capture, Some(&to_local));
        assert_eq!(all.len(), 30);
        assert_eq!(all[15].state, FrameState::Late);
        assert_eq!(all[20].state, FrameState::Dropped);
        assert_eq!(all[25].state, FrameState::OnTime);
        assert_eq!(all[25].delay_ns, Some(5_000_000));
        assert_eq!(all[0].latency_ms, Some(3.0));
        assert_eq!(all[15].latency_ms, Some(83.0));
        assert_eq!(all[3].spread_ms, Some(1.0));
        assert!(all[9].warmup && !all[10].warmup);
        // The same frames with another offset classify identically.
        let shifted: Vec<RxFrame> = frames
            .iter()
            .map(|f| RxFrame {
                first_recv_ns: f.first_recv_ns + 777_000_000_000,
                complete_recv_ns: f.complete_recv_ns.map(|c| c + 777_000_000_000),
                ..f.clone()
            })
            .collect();
        let again = classify(&m, p.jitter_buffer_ms, &shifted, capture, None);
        assert!(all
            .iter()
            .zip(&again)
            .all(|(a, b)| a.state == b.state && a.delay_ns == b.delay_ns));
        assert!(again.iter().all(|c| c.latency_ms.is_none()));
        // Freezes: 20–21 dropped is one freeze of 2 frames; 15 alone is not.
        assert_eq!(
            freezes(all.iter().filter(|c| !c.warmup).map(|c| c.state)),
            (1, 2)
        );
        assert_eq!(
            freezes([FrameState::Late, FrameState::Dropped, FrameState::Late]),
            (1, 3)
        );
        assert_eq!(freezes([FrameState::OnTime, FrameState::Late]), (0, 0));

        // Full analysis.
        let mut acc = ReceiverAccounting::new();
        for f in &frames {
            acc.record(0, f.bytes, f.complete_recv_ns.unwrap());
        }
        let stats = Stats {
            frames: frames.clone(),
            receiver: acc.stats(),
            datagrams: Some(RxDatagrams {
                unique: 90,
                dup: 1,
                reordered: 2,
                max_loss_burst: 4,
                bad: 0,
            }),
            foreign_dropped: 0,
        };
        let before = crate::types::ClockOffset {
            offset_ns: offset,
            delay_ns: 400_000,
            uncertainty_ns: 200_000,
            at_ns: t0 - 1_000_000,
            samples: 16,
        };
        let map = ClockMap::new(before, None);
        let sender = SenderSide {
            t0_ns: t0,
            origin_ns: t0 - 500_000_000,
            datagrams_sent: 100,
            send_errors: 2,
            sent_bytes: 3000,
            connect_ms: None,
            max_send_lag_ms: 0.2,
            stream_ack: None,
        };
        let out = analyse(&p, stats, Some(&map), &sender, Vec::new());
        let r: Results = serde_json::from_value(out.results.clone()).unwrap();
        assert_eq!(r.frames_sent, 20);
        assert_eq!((r.on_time, r.late, r.dropped), (17, 1, 2));
        assert_eq!(r.delivered_fps, Some(17.0 / 2.0));
        assert_eq!(r.late_pct, Some(5.0));
        assert_eq!(r.dropped_pct, Some(10.0));
        assert_eq!((r.freezes, r.freeze_ms), (1, 200.0));
        assert_eq!(r.frame_latency_ms.samples, 20);
        assert_eq!(r.frame_latency_ms.lost, 2);
        assert_eq!(r.frame_latency_ms.p50, Some(3.0));
        assert_eq!(r.latency_uncertainty_ms, Some(0.2));
        // Keyframes 10 and 20 are counted; 20 was dropped.
        assert_eq!(r.keyframe_complete_ms.samples, 1);
        assert_eq!(r.keyframe_complete_p50_ms, None);
        assert_eq!(r.frames.len(), 30);
        assert_eq!(r.frames_stride, 1);
        assert_eq!(r.frames[20].state, FrameState::Dropped);
        assert!(r.frames[20].latency_ms.is_none());
        let d = r.datagrams.unwrap();
        assert_eq!((d.sent, d.unique, d.missing), (98, 90, 8));
        assert!(r.received_bytes <= r.sent_bytes);
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "delivered_fps",
                "late_pct",
                "dropped_pct",
                "freezes",
                "frame_latency_p50_ms",
                "goodput_mbps"
            ]
        );
        assert_eq!(out.summary[4].uncertainty, Some(0.2));
        assert_eq!(out.series[0].key, "frame_latency_ms");
        assert_eq!(out.series[0].points.len(), 28);
        assert_eq!(out.series[0].warmup_ms, Some(1500.0));
        assert_eq!(out.series[1].key, "goodput_mbps");
        assert!(out.window_ns.is_some());
    }

    #[test]
    fn strip_sampling_keeps_problem_frames() {
        let (all, stride) = strip_indices(3600, 3600, |_| false);
        assert_eq!((all.len(), stride), (3600, 1));
        let (idx, stride) = strip_indices(8400, 3600, |i| i == 7 || i == 8399);
        assert_eq!(stride, 3);
        assert!(idx.contains(&7) && idx.contains(&8399) && idx.contains(&0) && idx.contains(&3));
        assert!(!idx.contains(&1));
        assert_eq!(idx.len(), 2800 + 2);
    }

    #[test]
    fn live_values_decide_only_settled_frames() {
        let p = VideoParams {
            fps: Some(10),
            gop: 10,
            duration_secs: 5,
            warmup_secs: 0,
            jitter_buffer_ms: 50,
            ..VideoParams::default()
        };
        let m = Model::new(&p);
        let mut re = Reassembly::new(m.frames);
        // Frames 0..30 complete except 5 (old: dropped) and 28 (recent: pending).
        for k in 0..30u32 {
            if k == 5 || k == 28 {
                continue;
            }
            let h = FrameHeader {
                frame_no: k,
                frag_idx: 0,
                frag_cnt: 1,
                keyframe: k % 10 == 0,
                capture_ns: u64::from(k) * 100_000_000,
            };
            let done = h.capture_ns + 2_000_000 + if k == 7 { 90_000_000 } else { 0 };
            re.fragment(&h, 10, done);
        }
        let v: BTreeMap<&str, f64> = live_values(&re, &m, 50, Some(0)).into_iter().collect();
        assert_eq!(v["frames_done"], 28.0);
        // Decided: 0..=29 minus 28 (pending) = 29 frames; 1 late, 1 dropped.
        assert!((v["late_pct"] - 100.0 / 29.0).abs() < 1e-9);
        assert!((v["dropped_pct"] - 100.0 / 29.0).abs() < 1e-9);
        assert!((v["delivered_fps"] - 27.0 / 2.9).abs() < 1e-9);
        assert_eq!(v["freezes"], 0.0);
        assert_eq!(v["frame_latency_ms"], 2.0);
        assert_eq!(v["frame_latency_p50_ms"], 2.0);
        assert!(live_values(&Reassembly::new(5), &m, 50, None).is_empty());
    }

    #[tokio::test]
    async fn frames_written_fast_parse_back() {
        let (mut a, mut b) = tokio::io::duplex(1 << 20);
        let h = fh(4, 0, 1);
        write_frame_fast(&mut a, &h, 200_000).await.unwrap();
        write_frame_fast(&mut a, &fh(5, 0, 1), 10).await.unwrap();
        drop(a);
        let mut wire = Vec::new();
        b.read_to_end(&mut wire).await.unwrap();
        let mut parser = FrameParser::new(1 << 20);
        let mut events = Vec::new();
        assert_eq!(parser.push(&wire, 0, &mut events).unwrap(), 200_010);
        assert!(parser.at_boundary());
        let started: Vec<(FrameHeader, u64)> = events
            .iter()
            .filter_map(|e| match *e {
                ParseEvent::Started { header, len, .. } => Some((header, len)),
                _ => None,
            })
            .collect();
        assert_eq!(started[0], (h, 200_000));
        assert_eq!((started[1].0.frame_no, started[1].1), (5, 10));
    }

    #[test]
    fn example_output_matches_the_results_type() {
        let out = example_output();
        let r: Results = serde_json::from_value(out.results.clone()).unwrap();
        assert!(r.frames.len() <= 30);
        assert!(r.samples_ms.len() <= 40);
        assert_eq!(r.dropped, 1);
        assert!(r.late > 0);
        assert!(
            out.summary.iter().all(|s| s.value.is_some()),
            "{:?}",
            out.summary
        );
        let lat = out.summary[4].value.unwrap();
        assert!((lat - 7.8).abs() < 1.0, "{lat}");
    }
}
