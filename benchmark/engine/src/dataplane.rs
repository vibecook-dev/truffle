//! Data-plane building blocks (RFC 027 §6 "Data plane", §8.1):
//!
//! - wire headers: the 16-byte `TBN1` stream header, the 22-byte UDP
//!   datagram header and the 18-byte video frame header;
//! - [`ReceiverAccounting`]: one run-level byte counter per direction in
//!   250 ms buckets, and the goodput and series computed from it;
//! - [`Pacer`]: the token bucket for UDP and video;
//! - small async helpers to write and read headers, pump payload for a
//!   duration, count received payload, and exchange per-stream acks.
//!
//! All times are [`clock::now_ns`](crate::clock::now_ns) nanoseconds.

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::Instant;

use crate::clock::now_ns;
use crate::error::AppError;
use crate::stats;
use crate::types::Series;

/// Magic of every initiator-opened TCP/QUIC/egress stream.
pub const STREAM_MAGIC: [u8; 4] = *b"TBN1";
/// Length of [`StreamHeader`] on the wire.
pub const STREAM_HEADER_LEN: usize = 16;
/// Magic of every benchmark datagram.
pub const DATAGRAM_MAGIC: [u8; 2] = *b"TB";
/// Length of [`DatagramHeader`] on the wire.
pub const DATAGRAM_HEADER_LEN: usize = 22;
/// Length of [`FrameHeader`] on the wire.
pub const FRAME_HEADER_LEN: usize = 18;
/// A video datagram (TB header + frame header + payload) never exceeds this.
pub const VIDEO_MAX_DATAGRAM: usize = 1200;
/// Payload bytes per video datagram: 1200 − 22 − 18.
pub const VIDEO_MAX_FRAGMENT: usize = VIDEO_MAX_DATAGRAM - DATAGRAM_HEADER_LEN - FRAME_HEADER_LEN;
/// Write/read size for bulk streams.
pub const IO_CHUNK: usize = 64 * 1024;
/// Receiver bucket width (§8.1).
pub const BUCKET_MS: u64 = 250;
/// [`BUCKET_MS`] in ns.
pub const BUCKET_NS: u64 = BUCKET_MS * 1_000_000;
/// No summed series spans more than this, whatever the inputs claim (runs
/// are at most a few minutes; peer stats are checked with
/// [`ReceiverStats::check_span`] first, this is the backstop).
pub const MAX_SERIES_SPAN_NS: u64 = 3_600 * 1_000_000_000;

/// The longest span a peer's receiver stats may cover for a run of
/// `warmup_secs + duration_secs`: the data phase, `grace` for bytes still
/// queued when the sender stops, and 5 s.
pub fn max_data_span_ns(warmup_secs: u32, duration_secs: u32, grace: std::time::Duration) -> u64 {
    (u64::from(warmup_secs) + u64::from(duration_secs) + 5) * 1_000_000_000
        + grace.as_nanos() as u64
}
/// The final partial bucket is left out of a series when shorter than this
/// (one tenth of a bucket): dividing one read by a few µs would plot a spike.
pub const MIN_FINAL_BUCKET_NS: u64 = BUCKET_NS / 10;

// ---------------------------------------------------------------------------
// Run tags
// ---------------------------------------------------------------------------

/// 64 random bits from the OS RNG: the responder's `run_tag` (§6 rule 2).
pub fn random_run_tag() -> Result<u64, AppError> {
    let mut buf = [0u8; 8];
    getrandom::getrandom(&mut buf)
        .map_err(|e| AppError::internal(format!("OS random number generator: {e}")))?;
    Ok(u64::from_be_bytes(buf))
}

/// `run_tag` as 16 lowercase hex digits, for names (`tb-<hex>-<iter>.bin`, `in-<hex>/`).
pub fn run_tag_hex(run_tag: u64) -> String {
    format!("{run_tag:016x}")
}

/// The low 32 bits carried in every datagram.
pub fn run_tag_lo(run_tag: u64) -> u32 {
    run_tag as u32
}

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

/// Why a header was refused. Refused streams are closed and refused
/// datagrams dropped, and both are counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderError {
    /// Fewer bytes than the header needs.
    Short(usize),
    /// Wrong magic.
    BadMagic,
    /// Unknown stream kind.
    BadKind(u8),
    /// The run tag is not this run's.
    WrongTag,
}

impl fmt::Display for HeaderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Short(n) => write!(f, "header too short ({n} bytes)"),
            Self::BadMagic => f.write_str("bad header magic"),
            Self::BadKind(k) => write!(f, "unknown stream kind {k}"),
            Self::WrongTag => f.write_str("run tag does not match this run"),
        }
    }
}

impl std::error::Error for HeaderError {}

impl From<HeaderError> for io::Error {
    fn from(e: HeaderError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, e)
    }
}

/// What a stream carries (`kind` byte of the TBN1 header).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
#[repr(u8)]
pub enum StreamKind {
    /// Initiator writes, responder counts.
    Upload = 0,
    /// Responder writes, initiator counts.
    Download = 1,
    /// Ping-pong.
    Echo = 2,
    /// Video frames.
    Frames = 3,
}

impl StreamKind {
    /// Decode the kind byte.
    pub fn from_u8(b: u8) -> Result<Self, HeaderError> {
        match b {
            0 => Ok(Self::Upload),
            1 => Ok(Self::Download),
            2 => Ok(Self::Echo),
            3 => Ok(Self::Frames),
            other => Err(HeaderError::BadKind(other)),
        }
    }
}

/// `b"TBN1" · run_tag u64 BE · stream_idx u16 BE · kind u8 · reserved u8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamHeader {
    /// The responder's run tag.
    pub run_tag: u64,
    /// Stream index within the run.
    pub stream_idx: u16,
    /// What the stream carries.
    pub kind: StreamKind,
}

impl StreamHeader {
    /// Encode to 16 bytes.
    pub fn encode(&self) -> [u8; STREAM_HEADER_LEN] {
        let mut b = [0u8; STREAM_HEADER_LEN];
        b[0..4].copy_from_slice(&STREAM_MAGIC);
        b[4..12].copy_from_slice(&self.run_tag.to_be_bytes());
        b[12..14].copy_from_slice(&self.stream_idx.to_be_bytes());
        b[14] = self.kind as u8;
        b
    }

    /// Decode 16 bytes.
    pub fn decode(b: &[u8]) -> Result<Self, HeaderError> {
        if b.len() < STREAM_HEADER_LEN {
            return Err(HeaderError::Short(b.len()));
        }
        if b[0..4] != STREAM_MAGIC {
            return Err(HeaderError::BadMagic);
        }
        Ok(Self {
            run_tag: u64::from_be_bytes(b[4..12].try_into().unwrap_or_default()),
            stream_idx: u16::from_be_bytes([b[12], b[13]]),
            kind: StreamKind::from_u8(b[14])?,
        })
    }

    /// Decode and require `run_tag`.
    pub fn decode_for(b: &[u8], run_tag: u64) -> Result<Self, HeaderError> {
        let h = Self::decode(b)?;
        if h.run_tag != run_tag {
            return Err(HeaderError::WrongTag);
        }
        Ok(h)
    }
}

/// `b"TB" · run_tag_lo u32 BE · seq u64 BE · send_ns u64 BE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramHeader {
    /// [`run_tag_lo`] of the run.
    pub run_tag_lo: u32,
    /// Sequence number from 0.
    pub seq: u64,
    /// Sender's `now_ns()` just before `send_to`.
    pub send_ns: u64,
}

impl DatagramHeader {
    /// Encode to 22 bytes.
    pub fn encode(&self) -> [u8; DATAGRAM_HEADER_LEN] {
        let mut b = [0u8; DATAGRAM_HEADER_LEN];
        b[0..2].copy_from_slice(&DATAGRAM_MAGIC);
        b[2..6].copy_from_slice(&self.run_tag_lo.to_be_bytes());
        b[6..14].copy_from_slice(&self.seq.to_be_bytes());
        b[14..22].copy_from_slice(&self.send_ns.to_be_bytes());
        b
    }

    /// Write the header into the start of `buf`.
    pub fn encode_into(&self, buf: &mut [u8]) -> Result<(), HeaderError> {
        if buf.len() < DATAGRAM_HEADER_LEN {
            return Err(HeaderError::Short(buf.len()));
        }
        buf[..DATAGRAM_HEADER_LEN].copy_from_slice(&self.encode());
        Ok(())
    }

    /// Decode the start of a datagram.
    pub fn decode(b: &[u8]) -> Result<Self, HeaderError> {
        if b.len() < DATAGRAM_HEADER_LEN {
            return Err(HeaderError::Short(b.len()));
        }
        if b[0..2] != DATAGRAM_MAGIC {
            return Err(HeaderError::BadMagic);
        }
        Ok(Self {
            run_tag_lo: u32::from_be_bytes([b[2], b[3], b[4], b[5]]),
            seq: u64::from_be_bytes(b[6..14].try_into().unwrap_or_default()),
            send_ns: u64::from_be_bytes(b[14..22].try_into().unwrap_or_default()),
        })
    }

    /// Decode and require the run's tag.
    pub fn decode_for(b: &[u8], run_tag: u64) -> Result<Self, HeaderError> {
        let h = Self::decode(b)?;
        if h.run_tag_lo != run_tag_lo(run_tag) {
            return Err(HeaderError::WrongTag);
        }
        Ok(h)
    }
}

/// `frame_no u32 BE · frag_idx u16 BE · frag_cnt u16 BE · flags u8 (bit 0 =
/// keyframe) · reserved u8 · capture_ns u64 BE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Frame number from 0.
    pub frame_no: u32,
    /// Fragment index (UDP); 0 on TCP/QUIC.
    pub frag_idx: u16,
    /// Fragments in this frame (UDP); 1 on TCP/QUIC.
    pub frag_cnt: u16,
    /// Keyframe flag.
    pub keyframe: bool,
    /// Scheduled capture time on the sender, `now_ns()`.
    pub capture_ns: u64,
}

impl FrameHeader {
    /// Encode to 18 bytes.
    pub fn encode(&self) -> [u8; FRAME_HEADER_LEN] {
        let mut b = [0u8; FRAME_HEADER_LEN];
        b[0..4].copy_from_slice(&self.frame_no.to_be_bytes());
        b[4..6].copy_from_slice(&self.frag_idx.to_be_bytes());
        b[6..8].copy_from_slice(&self.frag_cnt.to_be_bytes());
        b[8] = u8::from(self.keyframe);
        b[10..18].copy_from_slice(&self.capture_ns.to_be_bytes());
        b
    }

    /// Decode 18 bytes.
    pub fn decode(b: &[u8]) -> Result<Self, HeaderError> {
        if b.len() < FRAME_HEADER_LEN {
            return Err(HeaderError::Short(b.len()));
        }
        Ok(Self {
            frame_no: u32::from_be_bytes([b[0], b[1], b[2], b[3]]),
            frag_idx: u16::from_be_bytes([b[4], b[5]]),
            frag_cnt: u16::from_be_bytes([b[6], b[7]]),
            keyframe: b[8] & 1 == 1,
            capture_ns: u64::from_be_bytes(b[10..18].try_into().unwrap_or_default()),
        })
    }
}

/// Fragment count for a frame of `bytes` over UDP ([`VIDEO_MAX_FRAGMENT`] each, at least 1).
pub fn video_fragments(bytes: u64) -> u64 {
    bytes.div_ceil(VIDEO_MAX_FRAGMENT as u64).max(1)
}

// ---------------------------------------------------------------------------
// Receiver accounting (§8.1)
// ---------------------------------------------------------------------------

/// Per-stream receive counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct StreamStats {
    /// Stream index.
    pub stream_idx: u16,
    /// Payload bytes received.
    pub bytes: u64,
    /// First payload byte.
    pub first_ns: u64,
    /// Last payload byte.
    pub last_ns: u64,
}

/// The receiver's accounting for one direction of a run, as sent in
/// `finish.ack.stats` (§8.1). `epoch_ns`/`last_ns` are 0 when nothing arrived.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize, JsonSchema)]
pub struct ReceiverStats {
    /// Receiver `now_ns()` at the first payload byte of any stream.
    pub epoch_ns: u64,
    /// Bucket width, 250.
    pub bucket_ms: u64,
    /// Payload bytes read in `[epoch + 250·i ms, epoch + 250·(i+1) ms)`.
    pub buckets: Vec<u64>,
    /// Time of the last payload byte of any stream.
    pub last_ns: u64,
    /// All payload bytes.
    pub bytes_total: u64,
    /// Per-stream counters, by index.
    pub streams: Vec<StreamStats>,
}

/// Goodput over the measurement window `[epoch + warmup, last]`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Goodput {
    /// Mb/s.
    pub mbps: f64,
    /// Payload bytes in the window.
    pub bytes: u64,
    /// Window start (receiver time).
    pub start_ns: u64,
    /// Window end (receiver time).
    pub end_ns: u64,
}

impl ReceiverStats {
    fn bucket_ns(&self) -> u64 {
        if self.bucket_ms == 0 {
            BUCKET_NS
        } else {
            self.bucket_ms * 1_000_000
        }
    }

    /// `goodput_mbps = (bytes_total − Σ buckets[0 .. 4·warmup]) · 8 /
    /// (last − epoch − warmup) · 1e3` (§8.1). Fails with "window too short"
    /// when `last − epoch < warmup + 1 s`, and when nothing arrived.
    pub fn goodput(&self, warmup_secs: u32) -> Result<Goodput, AppError> {
        if self.bytes_total == 0 {
            return Err(AppError::internal("no payload was received"));
        }
        let warmup_ns = u64::from(warmup_secs) * 1_000_000_000;
        let span = self.last_ns.saturating_sub(self.epoch_ns);
        if span < warmup_ns + 1_000_000_000 {
            return Err(AppError::internal(format!(
                "window too short: data flowed for {:.3} s, which is under warmup + 1 s ({} s)",
                span as f64 / 1e9,
                warmup_secs + 1
            )));
        }
        let warm_buckets = (warmup_ns / self.bucket_ns()) as usize;
        let warm_bytes: u64 = self.buckets.iter().take(warm_buckets).sum();
        let bytes = self.bytes_total.saturating_sub(warm_bytes);
        let window = span - warmup_ns;
        Ok(Goodput {
            mbps: stats::mbps(bytes as f64, window).unwrap_or(0.0),
            bytes,
            start_ns: self.epoch_ns + warmup_ns,
            end_ns: self.last_ns,
        })
    }

    /// Payload bytes in `[start_ns, end_ns]`, prorating partial buckets
    /// linearly (for windows not on bucket edges, e.g. the bidirectional
    /// overlap window).
    pub fn bytes_between(&self, start_ns: u64, end_ns: u64) -> f64 {
        if end_ns <= start_ns {
            return 0.0;
        }
        let b = self.bucket_ns();
        let n = self.buckets.len();
        let mut total = 0.0;
        for (i, &bytes) in self.buckets.iter().enumerate() {
            let bs = self.epoch_ns + b * i as u64;
            let be = if i + 1 == n {
                self.last_ns.max(bs)
            } else {
                bs + b
            };
            if be == bs {
                if (start_ns..=end_ns).contains(&bs) {
                    total += bytes as f64;
                }
                continue;
            }
            let lo = bs.max(start_ns);
            let hi = be.min(end_ns);
            if hi > lo {
                total += bytes as f64 * (hi - lo) as f64 / (be - bs) as f64;
            }
        }
        total
    }

    /// Goodput over an arbitrary window (same timebase as these stats).
    pub fn goodput_between(&self, start_ns: u64, end_ns: u64) -> Option<f64> {
        stats::mbps(
            self.bytes_between(start_ns, end_ns),
            end_ns.saturating_sub(start_ns),
        )
    }

    /// These stats with every timestamp passed through `f` (e.g. a
    /// [`ClockMap::to_local`](crate::clock::ClockMap::to_local) mapping
    /// responder time into the initiator's).
    #[must_use]
    pub fn map_time(&self, f: impl Fn(u64) -> u64) -> Self {
        let empty = self.bytes_total == 0;
        let map = |t: u64| if empty { t } else { f(t) };
        Self {
            epoch_ns: map(self.epoch_ns),
            bucket_ms: self.bucket_ms,
            buckets: self.buckets.clone(),
            last_ns: map(self.last_ns),
            bytes_total: self.bytes_total,
            streams: self
                .streams
                .iter()
                .map(|s| StreamStats {
                    first_ns: map(s.first_ns),
                    last_ns: map(s.last_ns),
                    ..*s
                })
                .collect(),
        }
    }

    /// The goodput series (§8.1): point i is `buckets[i] · 8 / 0.25 / 1e6`
    /// at the bucket's end, `t_ms` relative to `origin_ns`. The final partial
    /// bucket is divided by its true length (and left out when shorter than
    /// [`MIN_FINAL_BUCKET_NS`]). `warmup_ms` marks the warmup points.
    pub fn series(&self, origin_ns: u64, warmup_secs: u32, key: &str, label: &str) -> Series {
        let b = self.bucket_ns();
        let n = self.buckets.len();
        let rel_ms = |t: u64| (t as f64 - origin_ns as f64) / 1e6;
        let mut points = Vec::with_capacity(n);
        for (i, &bytes) in self.buckets.iter().enumerate() {
            let start = self.epoch_ns + b * i as u64;
            let end = if i + 1 == n {
                self.last_ns.max(start)
            } else {
                start + b
            };
            let len = end - start;
            if i + 1 == n && len < MIN_FINAL_BUCKET_NS {
                continue;
            }
            if let Some(v) = stats::mbps(bytes as f64, len) {
                points.push((rel_ms(end), v));
            }
        }
        Series {
            key: key.to_string(),
            label: label.to_string(),
            unit: "Mb/s".to_string(),
            points,
            warmup_ms: (self.bytes_total > 0)
                .then(|| rel_ms(self.epoch_ns + u64::from(warmup_secs) * 1_000_000_000)),
        }
    }

    /// Σ buckets == bytes_total == Σ stream bytes (§12 invariants).
    pub fn check_invariants(&self) -> Result<(), String> {
        let buckets: u64 = self.buckets.iter().sum();
        let streams: u64 = self.streams.iter().map(|s| s.bytes).sum();
        if buckets != self.bytes_total || streams != self.bytes_total {
            return Err(format!(
                "receiver accounting mismatch: buckets {buckets}, streams {streams}, total {}",
                self.bytes_total
            ));
        }
        Ok(())
    }

    /// Rejects stats whose timeline the run could not have produced (a
    /// peer's stats are not trusted): a bucket width other than 250 ms, a
    /// last byte before the epoch, a span longer than `max_span_ns`, or more
    /// buckets than the span holds. Series and window math walk the span
    /// and the buckets, so both must be bounded before any of it runs.
    pub fn check_span(&self, max_span_ns: u64) -> Result<(), String> {
        if self.bucket_ms != 0 && self.bucket_ms != BUCKET_MS {
            return Err(format!(
                "bucket width {} ms, expected {BUCKET_MS}",
                self.bucket_ms
            ));
        }
        if self.bytes_total == 0 {
            return Ok(());
        }
        let span = self
            .last_ns
            .checked_sub(self.epoch_ns)
            .ok_or_else(|| "the last byte came before the first".to_string())?;
        if span > max_span_ns {
            return Err(format!(
                "a data span of {:.1} s, longer than the run's {:.1} s",
                span as f64 / 1e9,
                max_span_ns as f64 / 1e9
            ));
        }
        if self.buckets.len() as u64 > span / BUCKET_NS + 1 {
            return Err(format!(
                "{} buckets for a {:.1} s span",
                self.buckets.len(),
                span as f64 / 1e9
            ));
        }
        Ok(())
    }

    /// Received bytes of one stream.
    pub fn stream_bytes(&self, stream_idx: u16) -> u64 {
        self.streams
            .iter()
            .find(|s| s.stream_idx == stream_idx)
            .map_or(0, |s| s.bytes)
    }
}

/// The live counter behind [`ReceiverStats`]: one per run direction, shared
/// by every stream of that direction.
#[derive(Debug, Clone, Default)]
pub struct ReceiverAccounting {
    epoch_ns: Option<u64>,
    buckets: Vec<u64>,
    last_ns: u64,
    bytes_total: u64,
    streams: BTreeMap<u16, StreamStats>,
}

/// A [`ReceiverAccounting`] shared between stream tasks.
pub type SharedAccounting = Arc<Mutex<ReceiverAccounting>>;

impl ReceiverAccounting {
    /// An empty counter.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty counter behind `Arc<Mutex<_>>`.
    pub fn shared() -> SharedAccounting {
        Arc::new(Mutex::new(Self::new()))
    }

    /// Count `bytes` of payload on `stream_idx`, read at `at_ns`. The first
    /// call fixes the epoch; a time before the epoch counts in bucket 0.
    pub fn record(&mut self, stream_idx: u16, bytes: u64, at_ns: u64) {
        if bytes == 0 {
            return;
        }
        let epoch = *self.epoch_ns.get_or_insert(at_ns);
        let idx = (at_ns.saturating_sub(epoch) / BUCKET_NS) as usize;
        if idx >= self.buckets.len() {
            self.buckets.resize(idx + 1, 0);
        }
        self.buckets[idx] += bytes;
        self.bytes_total += bytes;
        self.last_ns = self.last_ns.max(at_ns);
        let s = self.streams.entry(stream_idx).or_insert(StreamStats {
            stream_idx,
            bytes: 0,
            first_ns: at_ns,
            last_ns: at_ns,
        });
        s.bytes += bytes;
        s.first_ns = s.first_ns.min(at_ns);
        s.last_ns = s.last_ns.max(at_ns);
    }

    /// [`record`](Self::record) stamped with `now_ns()` (taken under the
    /// caller's lock, so stamps stay ordered across streams).
    pub fn record_now(&mut self, stream_idx: u16, bytes: u64) {
        self.record(stream_idx, bytes, now_ns());
    }

    /// Payload bytes so far.
    pub fn bytes_total(&self) -> u64 {
        self.bytes_total
    }

    /// The epoch, once the first byte arrived.
    pub fn epoch_ns(&self) -> Option<u64> {
        self.epoch_ns
    }

    /// Snapshot as [`ReceiverStats`].
    pub fn stats(&self) -> ReceiverStats {
        ReceiverStats {
            epoch_ns: self.epoch_ns.unwrap_or(0),
            bucket_ms: BUCKET_MS,
            buckets: self.buckets.clone(),
            last_ns: self.last_ns,
            bytes_total: self.bytes_total,
            streams: self.streams.values().copied().collect(),
        }
    }
}

fn lock(acc: &SharedAccounting) -> std::sync::MutexGuard<'_, ReceiverAccounting> {
    acc.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Pacer
// ---------------------------------------------------------------------------

/// Pacer tick: the pacer sleeps to whole multiples of this.
pub const PACER_TICK: Duration = Duration::from_millis(1);
/// The most lag the pacer catches up after it fell behind its schedule
/// (bounds a catch-up burst to about `depth + 8 ms` of data).
pub const PACER_CATCH_UP: Duration = Duration::from_millis(8);
/// `rate_achieved` iff `offered ≥ RATE_ACHIEVED · target` (§8.2 udp).
pub const RATE_ACHIEVED: f64 = 0.98;

/// The §8.2 pacer (udp, video): a token bucket of depth `max(1 datagram,
/// rate × 2 ms)` that sleeps to 1 ms ticks, written in its equivalent
/// virtual-schedule form (GCRA). Each datagram has a theoretical send time
/// (`tat`); a datagram may go once `now ≥ tat − τ`, where the tolerance `τ`
/// is the depth minus one datagram, in time. It starts with a full bucket.
///
/// Time the pacer loses to its own wake-ups (rounding up to the 1 ms tick,
/// and timer lateness: tokio sleeps aimed at a 1 ms tick wake about 2.2 ms
/// late on macOS, measured) is not lost: `tat` only moves forward, so a late
/// sender catches up, by at most [`PACER_CATCH_UP`] of lag (older lag is
/// forgiven). A literal bucket capped at 2 ms loses that time to its cap: it
/// held a 10 Mb/s target at 6.9–9.6 Mb/s, and 500 pps echo near 370 pps.
/// Catch-up bursts show in [`max_burst_datagrams`](Self::max_burst_datagrams).
///
/// It also tracks the offered load: [`offered_mbps`](Self::offered_mbps),
/// [`max_burst_datagrams`](Self::max_burst_datagrams) and
/// [`rate_achieved`](Self::rate_achieved).
#[derive(Debug, Clone)]
pub struct Pacer {
    bytes_per_ns: f64,
    depth: f64,
    datagram: f64,
    tau_ns: f64,
    origin: Instant,
    tat_ns: Option<f64>,
    first_send: Option<(Instant, u64)>,
    last_send: Option<Instant>,
    bytes_sent: u64,
    datagrams_sent: u64,
    burst: u64,
    max_burst: u64,
    late_ns_total: f64,
    late_ns_max: f64,
    sleeps: u64,
}

impl Pacer {
    /// A pacer for `rate_mbps` of `datagram_bytes`-sized datagrams, starting
    /// with a full bucket.
    pub fn new(rate_mbps: f64, datagram_bytes: usize) -> Self {
        let mut p = Self {
            bytes_per_ns: 0.0,
            depth: 0.0,
            datagram: datagram_bytes.max(1) as f64,
            tau_ns: 0.0,
            origin: Instant::now(),
            tat_ns: None,
            first_send: None,
            last_send: None,
            bytes_sent: 0,
            datagrams_sent: 0,
            burst: 0,
            max_burst: 0,
            late_ns_total: 0.0,
            late_ns_max: 0.0,
            sleeps: 0,
        };
        p.set_rate(rate_mbps);
        p
    }

    /// Change the target rate (udp ramp); the schedule carries on.
    pub fn set_rate(&mut self, rate_mbps: f64) {
        self.bytes_per_ns = (rate_mbps.max(0.0) * 1e6 / 8.0 / 1e9).max(1e-12);
        self.depth = self.datagram.max(self.bytes_per_ns * 2_000_000.0);
        self.tau_ns = (self.depth - self.datagram) / self.bytes_per_ns;
    }

    /// Restart the schedule at `at` with a new rate and a full bucket, with
    /// no credit or debt from before (video: each frame's fragments are
    /// spread over its own interval from its capture time).
    pub fn restart_at(&mut self, at: Instant, rate_mbps: f64) {
        self.set_rate(rate_mbps);
        self.tat_ns = Some(at.saturating_duration_since(self.origin).as_nanos() as f64);
    }

    /// Bucket depth in bytes.
    pub fn depth_bytes(&self) -> f64 {
        self.depth
    }

    fn now_ns(&self) -> f64 {
        Instant::now()
            .saturating_duration_since(self.origin)
            .as_nanos() as f64
    }

    /// Whether `bytes` may go at `now_ns` (ns since the pacer was made); if
    /// so, books them on the schedule, otherwise returns when they may go.
    fn try_take(&mut self, bytes: usize, now_ns: f64) -> Result<(), f64> {
        let lag_cap = PACER_CATCH_UP.as_nanos() as f64;
        let tat = self.tat_ns.unwrap_or(now_ns).max(now_ns - lag_cap);
        let earliest = tat - self.tau_ns;
        if now_ns >= earliest {
            self.tat_ns = Some(tat + bytes as f64 / self.bytes_per_ns);
            Ok(())
        } else {
            Err(earliest)
        }
    }

    /// Wait until `bytes` may be sent, sleeping to 1 ms tick boundaries.
    pub async fn wait(&mut self, bytes: usize) {
        let mut slept = false;
        loop {
            match self.try_take(bytes, self.now_ns()) {
                Ok(()) => {
                    self.burst = if slept { 1 } else { self.burst + 1 };
                    self.max_burst = self.max_burst.max(self.burst);
                    return;
                }
                Err(earliest) => {
                    let tick = PACER_TICK.as_nanos() as f64;
                    let next = (earliest / tick).ceil() * tick;
                    let at = self.origin + Duration::from_nanos(next as u64);
                    tokio::time::sleep_until(at).await;
                    let late = (self.now_ns() - next).max(0.0);
                    self.late_ns_total += late;
                    self.late_ns_max = self.late_ns_max.max(late);
                    self.sleeps += 1;
                    slept = true;
                }
            }
        }
    }

    /// Note a datagram whose `send_to` returned Ok.
    pub fn record_sent(&mut self, bytes: usize) {
        let now = Instant::now();
        if self.first_send.is_none() {
            self.first_send = Some((now, bytes as u64));
        }
        self.last_send = Some(now);
        self.bytes_sent += bytes as u64;
        self.datagrams_sent += 1;
    }

    /// Bytes recorded as sent.
    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent
    }

    /// Datagrams recorded as sent.
    pub fn datagrams_sent(&self) -> u64 {
        self.datagrams_sent
    }

    /// Largest number of datagrams released without a sleep.
    pub fn max_burst_datagrams(&self) -> u64 {
        self.max_burst
    }

    /// Offered load: bytes after the first datagram over first → last send,
    /// Mb/s. `None` before two sends.
    pub fn offered_mbps(&self) -> Option<f64> {
        let (first, first_bytes) = self.first_send?;
        let last = self.last_send?;
        if self.datagrams_sent < 2 {
            return None;
        }
        stats::mbps(
            (self.bytes_sent - first_bytes) as f64,
            last.saturating_duration_since(first).as_nanos() as u64,
        )
    }

    /// `offered ≥ 0.98 · target` (§8.2 udp).
    pub fn rate_achieved(&self, target_mbps: f64) -> bool {
        self.offered_mbps()
            .is_some_and(|offered| offered >= RATE_ACHIEVED * target_mbps)
    }

    /// Mean and largest timer lateness of the pacer's sleeps, ms (`None`
    /// before the first sleep).
    pub fn lateness_ms(&self) -> (Option<f64>, Option<f64>) {
        if self.sleeps == 0 {
            return (None, None);
        }
        (
            Some(self.late_ns_total / self.sleeps as f64 / 1e6),
            Some(self.late_ns_max / 1e6),
        )
    }
}

// ---------------------------------------------------------------------------
// Async helpers
// ---------------------------------------------------------------------------

/// A 64 KiB deterministic payload buffer.
pub fn payload_chunk() -> &'static [u8] {
    static CHUNK: OnceLock<Vec<u8>> = OnceLock::new();
    CHUNK.get_or_init(|| (0..IO_CHUNK).map(|i| (i % 251) as u8).collect())
}

/// Write a TBN1 header.
pub async fn write_header<W: AsyncWrite + Unpin>(w: &mut W, h: &StreamHeader) -> io::Result<()> {
    w.write_all(&h.encode()).await
}

/// Read a TBN1 header (`InvalidData` on a bad magic or kind).
pub async fn read_header<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<StreamHeader> {
    let mut b = [0u8; STREAM_HEADER_LEN];
    r.read_exact(&mut b).await?;
    Ok(StreamHeader::decode(&b)?)
}

/// Read a TBN1 header and require `run_tag`.
pub async fn read_header_for<R: AsyncRead + Unpin>(
    r: &mut R,
    run_tag: u64,
) -> io::Result<StreamHeader> {
    let mut b = [0u8; STREAM_HEADER_LEN];
    r.read_exact(&mut b).await?;
    Ok(StreamHeader::decode_for(&b, run_tag)?)
}

/// Write 64 KiB chunks until `until`, adding each chunk to `sent` (a live
/// counter for `sent_mbps`). Returns this call's payload bytes. Does not
/// shut the stream down.
pub async fn write_for<W: AsyncWrite + Unpin>(
    w: &mut W,
    until: Instant,
    sent: &AtomicU64,
) -> io::Result<u64> {
    let chunk = payload_chunk();
    let mut total = 0u64;
    while Instant::now() < until {
        w.write_all(chunk).await?;
        total += chunk.len() as u64;
        sent.fetch_add(chunk.len() as u64, Ordering::Relaxed);
    }
    w.flush().await?;
    Ok(total)
}

/// Read to EOF, recording every read in `acc` under `stream_idx`. Returns
/// this stream's payload bytes.
pub async fn read_counting<R: AsyncRead + Unpin>(
    r: &mut R,
    stream_idx: u16,
    acc: &SharedAccounting,
) -> io::Result<u64> {
    let mut buf = vec![0u8; IO_CHUNK];
    let mut total = 0u64;
    loop {
        let n = r.read(&mut buf).await?;
        if n == 0 {
            return Ok(total);
        }
        total += n as u64;
        lock(acc).record_now(stream_idx, n as u64);
    }
}

/// After EOF on a stream, the receiver writes `[u64 BE stream_bytes]`: a
/// barrier and cross-check only (§8.1).
pub async fn write_stream_ack<W: AsyncWrite + Unpin>(w: &mut W, bytes: u64) -> io::Result<()> {
    w.write_all(&bytes.to_be_bytes()).await?;
    w.flush().await
}

/// Read the receiver's `[u64 BE stream_bytes]`.
pub async fn read_stream_ack<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<u64> {
    r.read_u64().await
}

/// Write one video frame on TCP/QUIC: `[u32 BE payload len][18-byte header][payload]`.
/// The length counts the payload only.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    header: &FrameHeader,
    payload: &[u8],
) -> io::Result<()> {
    let len = u32::try_from(payload.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame larger than 4 GiB"))?;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(&header.encode()).await?;
    w.write_all(payload).await
}

/// Write a frame of `len` payload bytes from the pattern buffer.
pub async fn write_frame_of<W: AsyncWrite + Unpin>(
    w: &mut W,
    header: &FrameHeader,
    len: u64,
) -> io::Result<()> {
    let len32 = u32::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame larger than 4 GiB"))?;
    w.write_all(&len32.to_be_bytes()).await?;
    w.write_all(&header.encode()).await?;
    let chunk = payload_chunk();
    let mut left = len;
    while left > 0 {
        let n = left.min(chunk.len() as u64) as usize;
        w.write_all(&chunk[..n]).await?;
        left -= n as u64;
    }
    Ok(())
}

/// What [`FrameParser::push`] saw in one chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseEvent {
    /// A frame's prefix completed; its first byte arrived at `first_ns`.
    Started {
        /// The frame header.
        header: FrameHeader,
        /// Payload length.
        len: u64,
        /// Arrival of the first prefix byte.
        first_ns: u64,
    },
    /// Payload bytes of the current frame.
    Payload {
        /// Frame number.
        frame_no: u32,
        /// Bytes.
        bytes: u64,
    },
    /// A frame's last byte.
    Completed {
        /// Frame number.
        frame_no: u32,
    },
}

/// Incremental parser for the TCP/QUIC video framing written by
/// [`write_frame`]/[`write_frame_of`]: `[u32 BE len][18-byte header][payload]`.
///
/// Fed every chunk as it is read (with its arrival time), it reports when
/// each frame's first byte arrived, its payload bytes chunk by chunk (so
/// the receiver can count them into [`ReceiverAccounting`] as they arrive)
/// and when its last byte arrived.
#[derive(Debug, Clone)]
pub struct FrameParser {
    prefix: [u8; 4 + FRAME_HEADER_LEN],
    have: usize,
    prefix_first_ns: u64,
    current: Option<u32>,
    remaining: u64,
    max_len: u64,
}

impl FrameParser {
    /// A parser that refuses frames longer than `max_len`.
    pub fn new(max_len: u64) -> Self {
        Self {
            prefix: [0; 4 + FRAME_HEADER_LEN],
            have: 0,
            prefix_first_ns: 0,
            current: None,
            remaining: 0,
            max_len,
        }
    }

    /// Whether the stream is between frames (a clean place to end).
    pub fn at_boundary(&self) -> bool {
        self.have == 0 && self.current.is_none()
    }

    /// Feed one chunk read at `now`; events go to `out`. Returns the payload
    /// bytes in the chunk.
    pub fn push(
        &mut self,
        mut data: &[u8],
        now: u64,
        out: &mut Vec<ParseEvent>,
    ) -> Result<u64, String> {
        let mut payload = 0u64;
        while !data.is_empty() {
            if let Some(frame_no) = self.current {
                let take = self.remaining.min(data.len() as u64);
                self.remaining -= take;
                payload += take;
                data = &data[take as usize..];
                out.push(ParseEvent::Payload {
                    frame_no,
                    bytes: take,
                });
                if self.remaining == 0 {
                    out.push(ParseEvent::Completed { frame_no });
                    self.current = None;
                }
                continue;
            }
            if self.have == 0 {
                self.prefix_first_ns = now;
            }
            let take = (self.prefix.len() - self.have).min(data.len());
            self.prefix[self.have..self.have + take].copy_from_slice(&data[..take]);
            self.have += take;
            data = &data[take..];
            if self.have < self.prefix.len() {
                break;
            }
            self.have = 0;
            let len = u64::from(u32::from_be_bytes([
                self.prefix[0],
                self.prefix[1],
                self.prefix[2],
                self.prefix[3],
            ]));
            if len > self.max_len {
                return Err(format!(
                    "a {len}-byte frame exceeds the {}-byte maximum",
                    self.max_len
                ));
            }
            let header = FrameHeader::decode(&self.prefix[4..]).map_err(|e| e.to_string())?;
            out.push(ParseEvent::Started {
                header,
                len,
                first_ns: self.prefix_first_ns,
            });
            if len == 0 {
                out.push(ParseEvent::Completed {
                    frame_no: header.frame_no,
                });
            } else {
                self.current = Some(header.frame_no);
                self.remaining = len;
            }
        }
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_header_round_trip_and_rejects() {
        let h = StreamHeader {
            run_tag: 0x0102_0304_0506_0708,
            stream_idx: 513,
            kind: StreamKind::Download,
        };
        let b = h.encode();
        assert_eq!(&b[0..4], b"TBN1");
        assert_eq!(
            b,
            [b'T', b'B', b'N', b'1', 1, 2, 3, 4, 5, 6, 7, 8, 2, 1, 1, 0]
        );
        assert_eq!(StreamHeader::decode(&b).unwrap(), h);
        assert_eq!(StreamHeader::decode_for(&b, h.run_tag).unwrap(), h);
        assert_eq!(StreamHeader::decode_for(&b, 7), Err(HeaderError::WrongTag));
        let mut bad = b;
        bad[0] = b'X';
        assert_eq!(StreamHeader::decode(&bad), Err(HeaderError::BadMagic));
        let mut bad = b;
        bad[14] = 9;
        assert_eq!(StreamHeader::decode(&bad), Err(HeaderError::BadKind(9)));
        assert_eq!(StreamHeader::decode(&b[..10]), Err(HeaderError::Short(10)));
    }

    #[test]
    fn datagram_header_round_trip() {
        let h = DatagramHeader {
            run_tag_lo: run_tag_lo(0xdead_beef_0000_0001),
            seq: 42,
            send_ns: 1_700_000_000_000_000_000,
        };
        assert_eq!(h.run_tag_lo, 1);
        let mut buf = [0u8; 1200];
        h.encode_into(&mut buf).unwrap();
        assert_eq!(&buf[0..2], b"TB");
        assert_eq!(DatagramHeader::decode(&buf).unwrap(), h);
        assert!(DatagramHeader::decode_for(&buf, 0xdead_beef_0000_0001).is_ok());
        assert_eq!(
            DatagramHeader::decode_for(&buf, 2),
            Err(HeaderError::WrongTag)
        );
        assert_eq!(
            DatagramHeader::decode(&buf[..21]),
            Err(HeaderError::Short(21))
        );
        assert!(h.encode_into(&mut [0u8; 5]).is_err());
    }

    #[test]
    fn frame_header_round_trip() {
        let h = FrameHeader {
            frame_no: 77,
            frag_idx: 3,
            frag_cnt: 9,
            keyframe: true,
            capture_ns: 123_456_789,
        };
        let b = h.encode();
        assert_eq!(b.len(), 18);
        assert_eq!(b[8], 1);
        assert_eq!(FrameHeader::decode(&b).unwrap(), h);
        let p = FrameHeader {
            keyframe: false,
            ..h
        };
        assert_eq!(FrameHeader::decode(&p.encode()).unwrap(), p);
        assert_eq!(VIDEO_MAX_FRAGMENT, 1160);
        assert_eq!(video_fragments(0), 1);
        assert_eq!(video_fragments(1160), 1);
        assert_eq!(video_fragments(1161), 2);
    }

    const S: u64 = 1_000_000_000;
    const MS: u64 = 1_000_000;

    /// 1000 bytes every 10 ms on two streams for `secs` seconds from `t0`.
    fn steady(t0: u64, secs: u64) -> ReceiverAccounting {
        let mut acc = ReceiverAccounting::new();
        let mut t = t0;
        while t < t0 + secs * S {
            acc.record(0, 500, t);
            acc.record(1, 500, t + MS);
            t += 10 * MS;
        }
        acc
    }

    #[test]
    fn accounting_buckets_and_invariants() {
        let acc = steady(5 * S, 3);
        let st = acc.stats();
        assert_eq!(st.epoch_ns, 5 * S);
        assert_eq!(st.bucket_ms, 250);
        assert_eq!(st.buckets.len(), 12);
        assert_eq!(st.buckets[0], 25 * 1000);
        assert_eq!(st.bytes_total, 300 * 1000);
        st.check_invariants().unwrap();
        assert_eq!(st.streams.len(), 2);
        assert_eq!(st.stream_bytes(1), 150_000);
        assert_eq!(st.streams[1].first_ns, 5 * S + MS);
        assert_eq!(st.last_ns, 5 * S + 2990 * MS + MS);
    }

    #[test]
    fn peer_stats_with_impossible_timelines_are_refused() {
        let st = steady(5 * S, 3).stats();
        let max = max_data_span_ns(1, 2, std::time::Duration::from_secs(15));
        assert_eq!(max, 23 * S);
        st.check_span(max).unwrap();
        let forged = |f: &dyn Fn(&mut ReceiverStats)| {
            let mut s = st.clone();
            f(&mut s);
            s.check_span(max)
        };
        // Honest bytes, forged times: what would make a series walk forever.
        assert!(forged(&|s| {
            s.epoch_ns = 0;
            s.last_ns = u64::MAX;
        })
        .is_err());
        assert!(forged(&|s| s.last_ns = s.epoch_ns + 24 * S).is_err());
        assert!(forged(&|s| s.last_ns = s.epoch_ns - 1).is_err());
        assert!(forged(&|s| s.bucket_ms = 1).is_err());
        assert!(forged(&|s| s.buckets.resize(10_000, 0)).is_err());
        // Nothing received: nothing to walk.
        ReceiverStats::default().check_span(max).unwrap();
    }

    #[test]
    fn goodput_excludes_warmup_on_bucket_edges() {
        // 100 kB/s = 0.8 Mb/s.
        let st = steady(0, 4).stats();
        let g = st.goodput(1).unwrap();
        assert_eq!(g.bytes, 300_000);
        assert_eq!(g.start_ns, S);
        let expected = 300_000.0 * 8.0 / (st.last_ns - S) as f64 * 1e3;
        assert!((g.mbps - expected).abs() < 1e-9);
        assert!((g.mbps - 0.8).abs() < 0.01, "{}", g.mbps);
        let g0 = st.goodput(0).unwrap();
        assert_eq!(g0.bytes, 400_000);
    }

    #[test]
    fn short_window_fails() {
        let st = steady(0, 2).stats(); // spans ~1.99 s
        let err = st.goodput(1).unwrap_err();
        assert!(err.message.contains("window too short"), "{}", err.message);
        assert!(st.goodput(0).is_ok());
        assert!(ReceiverStats::default().goodput(0).is_err());
    }

    #[test]
    fn series_points_and_partial_final_bucket() {
        let mut acc = ReceiverAccounting::new();
        acc.record(0, 250_000, 0); // bucket 0
        acc.record(0, 500_000, 300 * MS); // bucket 1
        acc.record(0, 100_000, 600 * MS); // bucket 2 (final, 100 ms long)
        let st = acc.stats();
        let s = st.series(0, 0, "goodput_mbps", "Goodput");
        assert_eq!(s.points.len(), 3);
        assert_eq!(s.points[0], (250.0, 8.0));
        assert_eq!(s.points[1], (500.0, 16.0));
        // Final bucket: 100 kB over its true 100 ms = 8 Mb/s, plotted at 600 ms.
        assert_eq!(s.points[2].0, 600.0);
        assert!((s.points[2].1 - 8.0).abs() < 1e-9);
        assert_eq!(s.warmup_ms, Some(0.0));
        // A final bucket of 10 ms is left out.
        acc.record(0, 1, 760 * MS);
        let s = acc.stats().series(0, 0, "k", "l");
        assert_eq!(s.points.len(), 3);
        // Origin shifts t_ms; warmup_ms follows epoch + warmup.
        let s = st.series(0, 1, "k", "l");
        assert_eq!(s.warmup_ms, Some(1000.0));
        let shifted = st.map_time(|t| t + 2 * S).series(S, 0, "k", "l");
        assert_eq!(shifted.points[0].0, 1250.0);
    }

    #[test]
    fn windows_off_bucket_edges_are_prorated() {
        let st = steady(0, 4).stats();
        let all = st.bytes_between(0, st.last_ns);
        assert!((all - 400_000.0).abs() < 1.0, "{all}");
        let half = st.bytes_between(S + 125 * MS, 2 * S + 125 * MS);
        assert!((half - 100_000.0).abs() < 1_000.0, "{half}");
        let g = st.goodput_between(S, 3 * S).unwrap();
        assert!((g - 0.8).abs() < 0.01, "{g}");
        assert_eq!(st.bytes_between(5, 5), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn pacer_holds_the_target_rate() {
        // 10 Mb/s of 1250-byte datagrams = 1000 datagrams/s; depth 2
        // datagrams (τ = 1 ms).
        let mut p = Pacer::new(10.0, 1250);
        assert!((p.depth_bytes() - 2500.0).abs() < 1e-9);
        assert!((p.tau_ns - 1e6).abs() < 1.0);
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            p.wait(1250).await;
            p.record_sent(1250);
        }
        let offered = p.offered_mbps().unwrap();
        assert!((offered - 10.0).abs() / 10.0 < 0.01, "offered {offered}");
        assert!(p.rate_achieved(10.0));
        assert!(!p.rate_achieved(20.0));
        assert!(p.max_burst_datagrams() >= 2, "starts with a full bucket");
        assert!(p.max_burst_datagrams() <= 3, "{}", p.max_burst_datagrams());
        assert_eq!(p.datagrams_sent(), p.bytes_sent() / 1250);
        // Depth is at least one datagram at low rates.
        let slow = Pacer::new(0.1, 1200);
        assert_eq!(slow.depth_bytes(), 1200.0);
        assert_eq!(slow.tau_ns, 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn pacer_catches_up_after_late_wakes_within_the_cap() {
        let mut p = Pacer::new(10.0, 1250);
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(100) {
            p.wait(1250).await;
            p.record_sent(1250);
        }
        let back_to_back = |p: &mut Pacer| {
            let mut n = 0;
            while p.try_take(1250, p.now_ns()).is_ok() {
                n += 1;
            }
            n
        };
        // A 5 ms stall is caught up at once: 5 datagrams owed plus the 1 ms
        // tolerance, then pacing resumes.
        tokio::time::advance(Duration::from_millis(5)).await;
        let n = back_to_back(&mut p);
        assert!((5..=7).contains(&n), "{n}");
        // A 50 ms stall is forgiven beyond 8 ms of lag.
        tokio::time::advance(Duration::from_millis(50)).await;
        let n = back_to_back(&mut p);
        assert!((8..=10).contains(&n), "{n}");
        // One datagram deep (500 pps of 1200 B): every tick rounding is
        // caught up, so the rate holds.
        let mut e = Pacer::new(4.8, 1200);
        assert_eq!(e.tau_ns, 0.0);
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            e.wait(1200).await;
            e.record_sent(1200);
        }
        assert!(e.rate_achieved(4.8), "{:?}", e.offered_mbps());
        assert!(e.lateness_ms().0.is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn restart_spreads_a_burst_over_its_interval() {
        // 50 datagrams of 1000 B over 25 ms = 16 Mb/s.
        let mut p = Pacer::new(1.0, 1000);
        let t0 = Instant::now() + Duration::from_millis(3);
        tokio::time::sleep_until(t0).await;
        p.restart_at(t0, 50.0 * 1000.0 * 8.0 / 0.025 / 1e6);
        let mut times = Vec::new();
        for _ in 0..50 {
            p.wait(1000).await;
            times.push(Instant::now().duration_since(t0));
        }
        assert_eq!(times[0], Duration::ZERO);
        let last = *times.last().unwrap();
        assert!(last < Duration::from_millis(25), "{last:?}");
        assert!(last >= Duration::from_millis(22), "{last:?}");
        // A restart drops what the previous schedule still owed (its last
        // datagram was booked until 25 ms) and paces at the new rate.
        let t1 = Instant::now();
        p.restart_at(t1, 0.08);
        p.wait(1000).await;
        assert_eq!(Instant::now(), t1);
        p.wait(1000).await;
        let gap = Instant::now().duration_since(t1);
        assert!(
            (Duration::from_millis(100)..=Duration::from_millis(101)).contains(&gap),
            "{gap:?}"
        );
    }

    #[tokio::test]
    async fn stream_helpers_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(256 * 1024);
        let h = StreamHeader {
            run_tag: 9,
            stream_idx: 0,
            kind: StreamKind::Upload,
        };
        let acc = ReceiverAccounting::shared();
        let sent = AtomicU64::new(0);
        let writer = async {
            write_header(&mut a, &h).await.unwrap();
            let until = Instant::now() + Duration::from_millis(30);
            let n = write_for(&mut a, until, &sent).await.unwrap();
            a.shutdown().await.unwrap();
            let ack = read_stream_ack(&mut a).await.unwrap();
            (n, ack)
        };
        let reader = async {
            let got = read_header_for(&mut b, 9).await.unwrap();
            assert_eq!(got, h);
            let n = read_counting(&mut b, got.stream_idx, &acc).await.unwrap();
            write_stream_ack(&mut b, n).await.unwrap();
            n
        };
        let ((sent_bytes, ack), received) = tokio::join!(writer, reader);
        assert!(sent_bytes > 0);
        assert_eq!(sent_bytes, received);
        assert_eq!(ack, received);
        assert_eq!(sent.load(Ordering::Relaxed), sent_bytes);
        let st = lock(&acc).stats();
        st.check_invariants().unwrap();
        assert_eq!(st.bytes_total, received);
    }

    #[tokio::test]
    async fn wrong_tag_is_invalid_data() {
        let (mut a, mut b) = tokio::io::duplex(64);
        let h = StreamHeader {
            run_tag: 1,
            stream_idx: 0,
            kind: StreamKind::Upload,
        };
        write_header(&mut a, &h).await.unwrap();
        let err = read_header_for(&mut b, 2).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// Parse everything in `wire` with a [`FrameParser`], in chunks of `chunk`.
    fn parse_all(wire: &[u8], chunk: usize, max: u64) -> Result<Vec<ParseEvent>, String> {
        let mut parser = FrameParser::new(max);
        let mut events = Vec::new();
        for (i, c) in wire.chunks(chunk).enumerate() {
            parser.push(c, i as u64, &mut events)?;
        }
        if !parser.at_boundary() {
            return Err("ended inside a frame".into());
        }
        Ok(events)
    }

    #[tokio::test]
    async fn frames_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(1 << 20);
        let h = FrameHeader {
            frame_no: 1,
            frag_idx: 0,
            frag_cnt: 1,
            keyframe: true,
            capture_ns: 5,
        };
        let writer = async {
            write_frame_of(&mut a, &h, 100_000).await.unwrap();
            write_frame(
                &mut a,
                &FrameHeader {
                    frame_no: 2,
                    keyframe: false,
                    ..h
                },
                &[7u8; 10],
            )
            .await
            .unwrap();
            drop(a);
        };
        let reader = async {
            let mut wire = Vec::new();
            b.read_to_end(&mut wire).await.unwrap();
            wire
        };
        let ((), wire) = tokio::join!(writer, reader);
        assert_eq!(wire.len(), 2 * (4 + FRAME_HEADER_LEN) + 100_010);
        for chunk in [1usize, 13, 22, 64 * 1024] {
            let events = parse_all(&wire, chunk, 1 << 20).unwrap();
            let started: Vec<(FrameHeader, u64)> = events
                .iter()
                .filter_map(|e| match *e {
                    ParseEvent::Started { header, len, .. } => Some((header, len)),
                    _ => None,
                })
                .collect();
            assert_eq!(started.len(), 2, "chunk {chunk}");
            assert_eq!(started[0], (h, 100_000));
            assert_eq!((started[1].0.frame_no, started[1].1), (2, 10));
            let payload: u64 = events
                .iter()
                .map(|e| match *e {
                    ParseEvent::Payload { bytes, .. } => bytes,
                    _ => 0,
                })
                .sum();
            assert_eq!(payload, 100_010);
            let completed = events
                .iter()
                .filter(|e| matches!(e, ParseEvent::Completed { .. }))
                .count();
            assert_eq!(completed, 2);
        }
        // Oversized and truncated frames are refused.
        assert!(parse_all(&wire, 64, 1_000).is_err());
        assert!(parse_all(&wire[..wire.len() - 1], 64, 1 << 20).is_err());
    }

    #[test]
    fn run_tags() {
        let a = random_run_tag().unwrap();
        let b = random_run_tag().unwrap();
        assert_ne!(a, b);
        assert_eq!(run_tag_hex(0xab), "00000000000000ab");
    }
}
