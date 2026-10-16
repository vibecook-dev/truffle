//! Workload parameters (RFC 027 §7.3).
//!
//! [`WorkloadParams`] is internally tagged by `"workload"` (kebab-case), and
//! each variant's struct has a default for every field and rejects unknown
//! fields. [`WorkloadParams::validate`] rejects out-of-bounds values and
//! never clamps them; the responder runs it again on what it receives.
//!
//! Bounds the RFC annotates with a mode (`latency`, `echo`, `rr`, `stream`)
//! are checked only in that mode, because the field is unused otherwise.
//! Cross-field rules (`warmup_secs < duration_secs − 1`, …) apply where the
//! fields are used.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::error::AppError;

/// `KiB`.
pub const KIB: u64 = 1024;
/// `MiB`.
pub const MIB: u64 = 1024 * KIB;
/// `GiB`.
pub const GIB: u64 = 1024 * MIB;

/// The responder watchdog adds this to [`WorkloadParams::deadline`] …
pub const WATCHDOG_GRACE: Duration = Duration::from_secs(60);
/// … and never waits longer than this.
pub const WATCHDOG_CAP: Duration = Duration::from_secs(30 * 60);

// ---------------------------------------------------------------------------
// Workload names
// ---------------------------------------------------------------------------

/// A workload name (the `"workload"` tag).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum Workload {
    /// Path RTT and lifecycle timings.
    Path,
    /// Raw TCP throughput / latency.
    Tcp,
    /// UDP datagrams: stream, echo, ramp.
    Udp,
    /// QUIC streams: throughput / latency.
    Quic,
    /// Session-plane WebSocket messages: request/reply, stream.
    Ws,
    /// `send_file`.
    FileTransfer,
    /// SyncedStore convergence.
    SyncedStore,
    /// HTTP through the sidecar proxy or a raw listener.
    Http,
    /// Synthetic video frames.
    Video,
    /// Egress route vs raw TCP.
    Egress,
}

impl Workload {
    /// Every workload, in the app's rail order.
    pub const ALL: [Workload; 10] = [
        Workload::Path,
        Workload::Tcp,
        Workload::Udp,
        Workload::Quic,
        Workload::Ws,
        Workload::FileTransfer,
        Workload::SyncedStore,
        Workload::Http,
        Workload::Video,
        Workload::Egress,
    ];

    /// The wire spelling, e.g. `"file-transfer"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Path => "path",
            Self::Tcp => "tcp",
            Self::Udp => "udp",
            Self::Quic => "quic",
            Self::Ws => "ws",
            Self::FileTransfer => "file-transfer",
            Self::SyncedStore => "synced-store",
            Self::Http => "http",
            Self::Video => "video",
            Self::Egress => "egress",
        }
    }

    /// The name people read, e.g. `"File transfer"`.
    pub fn title(self) -> &'static str {
        match self {
            Self::Path => "Path",
            Self::Tcp => "TCP",
            Self::Udp => "UDP",
            Self::Quic => "QUIC",
            Self::Ws => "WebSocket",
            Self::FileTransfer => "File transfer",
            Self::SyncedStore => "SyncedStore",
            Self::Http => "HTTP",
            Self::Video => "Video",
            Self::Egress => "Egress",
        }
    }

    /// This workload's default params.
    pub fn default_params(self) -> WorkloadParams {
        match self {
            Self::Path => WorkloadParams::Path(PathParams::default()),
            Self::Tcp => WorkloadParams::Tcp(TcpParams::default()),
            Self::Udp => WorkloadParams::Udp(UdpParams::default()),
            Self::Quic => WorkloadParams::Quic(QuicParams::default()),
            Self::Ws => WorkloadParams::Ws(WsParams::default()),
            Self::FileTransfer => WorkloadParams::FileTransfer(FileTransferParams::default()),
            Self::SyncedStore => WorkloadParams::SyncedStore(StoreParams::default()),
            Self::Http => WorkloadParams::Http(HttpParams::default()),
            Self::Video => WorkloadParams::Video(VideoParams::default()),
            Self::Egress => WorkloadParams::Egress(EgressParams::default()),
        }
    }

    /// Every workload's names as strings (for `PeerHello.workloads`).
    pub fn all_names() -> Vec<String> {
        Self::ALL.iter().map(|w| w.as_str().to_string()).collect()
    }
}

impl fmt::Display for Workload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Workload {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|w| w.as_str() == s)
            .ok_or_else(|| {
                AppError::invalid_params(format!(
                    "unknown workload “{s}” (expected one of: {})",
                    Self::all_names().join(", ")
                ))
            })
    }
}

// ---------------------------------------------------------------------------
// Mode enums
// ---------------------------------------------------------------------------

/// tcp / quic mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StreamMode {
    /// Bulk transfer for a duration.
    #[default]
    Throughput,
    /// Ping-pong of small messages.
    Latency,
}

/// tcp / quic direction, seen from the initiator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    /// Initiator → responder.
    #[default]
    Upload,
    /// Responder → initiator.
    Download,
    /// Both at once (`streams` each way).
    Bidirectional,
}

/// udp / egress direction, seen from the initiator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum OneWayDirection {
    /// Initiator → responder.
    #[default]
    Upload,
    /// Responder → initiator.
    Download,
}

/// udp mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum UdpMode {
    /// Paced one-way stream at `rate_mbps`.
    #[default]
    Stream,
    /// Echo at `pps`.
    Echo,
    /// Rate ramp from `rate_mbps` until loss.
    Ramp,
}

/// ws mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WsMode {
    /// Request/reply.
    #[default]
    Rr,
    /// One-way message stream for a duration.
    Stream,
}

/// synced-store mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StoreMode {
    /// One write, one echo, repeat.
    #[default]
    Pingpong,
    /// `count` writes back to back.
    Burst,
}

/// http target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum HttpTarget {
    /// The sidecar's reverse proxy (`proxy().add`).
    #[default]
    Proxy,
    /// A Rust handler on `listen_tcp`.
    Direct,
}

/// http connection reuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum HttpConnection {
    /// One connection per worker.
    #[default]
    Keepalive,
    /// A connection per request.
    New,
}

/// video transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum VideoTransport {
    /// Fragmented datagrams.
    #[default]
    Udp,
    /// One QUIC stream per frame.
    Quic,
    /// One TCP stream.
    Tcp,
}

/// video profile: frame rate and nominal bitrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
pub enum VideoProfile {
    /// 30 fps, 2.5 Mb/s.
    #[serde(rename = "720p30")]
    P720p30,
    /// 30 fps, 6 Mb/s.
    #[default]
    #[serde(rename = "1080p30")]
    P1080p30,
    /// 60 fps, 12 Mb/s.
    #[serde(rename = "1080p60")]
    P1080p60,
    /// 30 fps, 25 Mb/s.
    #[serde(rename = "4k30")]
    P4k30,
}

impl VideoProfile {
    /// The wire spelling, e.g. `"1080p30"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::P720p30 => "720p30",
            Self::P1080p30 => "1080p30",
            Self::P1080p60 => "1080p60",
            Self::P4k30 => "4k30",
        }
    }

    /// Frames per second.
    pub fn fps(self) -> u32 {
        match self {
            Self::P720p30 | Self::P1080p30 | Self::P4k30 => 30,
            Self::P1080p60 => 60,
        }
    }

    /// Nominal bitrate, Mb/s.
    pub fn bitrate_mbps(self) -> f64 {
        match self {
            Self::P720p30 => 2.5,
            Self::P1080p30 => 6.0,
            Self::P1080p60 => 12.0,
            Self::P4k30 => 25.0,
        }
    }
}

/// video pacing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Pacing {
    /// Fragments spread over the frame interval.
    #[default]
    Paced,
    /// Fragments back to back.
    Burst,
}

// ---------------------------------------------------------------------------
// Param structs
// ---------------------------------------------------------------------------

/// `path`: pings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct PathParams {
    /// Pings, 2–200.
    pub count: u32,
    /// Spacing, 50–5000 ms; `count × interval_ms ≤ 300 s`.
    pub interval_ms: u64,
}

impl Default for PathParams {
    fn default() -> Self {
        Self {
            count: 20,
            interval_ms: 250,
        }
    }
}

/// `tcp`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TcpParams {
    /// Throughput or latency.
    pub mode: StreamMode,
    /// Upload, download or bidirectional (throughput).
    pub direction: Direction,
    /// Streams per direction, 1–16 (throughput).
    pub streams: u32,
    /// Measured seconds, 2–60.
    pub duration_secs: u32,
    /// Unmeasured lead-in, 0–10 and `< duration_secs − 1`.
    pub warmup_secs: u32,
    /// Latency message size, 1–65536.
    pub payload_bytes: u32,
    /// Latency samples, 10–10000.
    pub count: u32,
    /// Discarded latency samples, 0–1000 and `< count`.
    pub warmup_count: u32,
    /// `TCP_NODELAY` on both Rust sockets.
    pub nodelay: bool,
}

impl Default for TcpParams {
    fn default() -> Self {
        Self {
            mode: StreamMode::Throughput,
            direction: Direction::Upload,
            streams: 1,
            duration_secs: 10,
            warmup_secs: 1,
            payload_bytes: 64,
            count: 200,
            warmup_count: 10,
            nodelay: true,
        }
    }
}

/// `udp`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct UdpParams {
    /// Stream, echo or ramp.
    pub mode: UdpMode,
    /// Upload or download (stream, ramp).
    pub direction: OneWayDirection,
    /// Datagram payload including the 22-byte header, 30–1400.
    pub payload_bytes: u32,
    /// Target rate, 0.1–1000 Mb/s; the start rate for ramp.
    pub rate_mbps: f64,
    /// Echo rate, 1–2000 datagrams/s.
    pub pps: u32,
    /// Seconds, 2–60.
    pub duration_secs: u32,
    /// Unmeasured lead-in (stream, ramp), 0–10 and `< duration_secs − 1`.
    pub warmup_secs: u32,
    /// Discarded echo samples, 0–1000 and `< pps × duration_secs`.
    pub warmup_count: u32,
}

impl Default for UdpParams {
    fn default() -> Self {
        Self {
            mode: UdpMode::Stream,
            direction: OneWayDirection::Upload,
            payload_bytes: 1200,
            rate_mbps: 10.0,
            pps: 50,
            duration_secs: 10,
            warmup_secs: 1,
            warmup_count: 10,
        }
    }
}

/// `quic`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct QuicParams {
    /// Throughput or latency.
    pub mode: StreamMode,
    /// Upload, download or bidirectional (throughput).
    pub direction: Direction,
    /// Streams per direction on one connection, 1–32.
    pub streams: u32,
    /// Measured seconds, 2–60.
    pub duration_secs: u32,
    /// Unmeasured lead-in, 0–10 and `< duration_secs − 1`.
    pub warmup_secs: u32,
    /// Latency message size, 1–65536.
    pub payload_bytes: u32,
    /// Latency samples, 10–10000.
    pub count: u32,
    /// Discarded latency samples, 0–1000 and `< count`.
    pub warmup_count: u32,
}

impl Default for QuicParams {
    fn default() -> Self {
        Self {
            mode: StreamMode::Throughput,
            direction: Direction::Upload,
            streams: 4,
            duration_secs: 10,
            warmup_secs: 1,
            payload_bytes: 64,
            count: 200,
            warmup_count: 10,
        }
    }
}

/// `ws`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WsParams {
    /// Request/reply or stream.
    pub mode: WsMode,
    /// Decoded payload: rr 0–1048576, stream 0–262144.
    pub payload_bytes: u32,
    /// Requests (rr), 10–100000; `count × payload_bytes ≤ 1 GiB`.
    pub count: u32,
    /// Requests in flight (rr), 1–64.
    pub concurrency: u32,
    /// Discarded requests (rr), 0–1000 and `< count`.
    pub warmup_count: u32,
    /// Seconds (stream), 2–60.
    pub duration_secs: u32,
    /// Messages per second (stream), 1–100000; `None` = as fast as backpressure allows.
    pub rate_per_sec: Option<u32>,
}

impl Default for WsParams {
    fn default() -> Self {
        Self {
            mode: WsMode::Rr,
            payload_bytes: 64,
            count: 500,
            concurrency: 1,
            warmup_count: 10,
            duration_secs: 10,
            rate_per_sec: None,
        }
    }
}

/// `file-transfer`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct FileTransferParams {
    /// File size, 1024–1073741824 bytes.
    pub size_bytes: u64,
    /// Transfers, 1–20; `size_bytes × iterations ≤ 4 GiB`.
    pub iterations: u32,
}

impl Default for FileTransferParams {
    fn default() -> Self {
        Self {
            size_bytes: 10 * MIB,
            iterations: 3,
        }
    }
}

/// `synced-store`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct StoreParams {
    /// Ping-pong or burst.
    pub mode: StoreMode,
    /// Writes, 10–5000.
    pub count: u32,
    /// Padding per write, 0–65536 bytes.
    pub payload_bytes: u32,
    /// Discarded ping-pong samples, 0–1000 and `< count`.
    pub warmup_count: u32,
}

impl Default for StoreParams {
    fn default() -> Self {
        Self {
            mode: StoreMode::Pingpong,
            count: 100,
            payload_bytes: 64,
            warmup_count: 5,
        }
    }
}

/// `http`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct HttpParams {
    /// Through the sidecar proxy or a raw listener.
    pub target: HttpTarget,
    /// Keep-alive or a connection per request.
    pub connection: HttpConnection,
    /// Workers, 1–64.
    pub concurrency: u32,
    /// Measured seconds, 2–60.
    pub duration_secs: u32,
    /// Unmeasured lead-in, 0–10 and `< duration_secs − 1`.
    pub warmup_secs: u32,
    /// Response body, 0–10485760 bytes.
    pub response_bytes: u64,
}

impl Default for HttpParams {
    fn default() -> Self {
        Self {
            target: HttpTarget::Proxy,
            connection: HttpConnection::Keepalive,
            concurrency: 4,
            duration_secs: 10,
            warmup_secs: 1,
            response_bytes: 1024,
        }
    }
}

/// `video`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct VideoParams {
    /// UDP, QUIC or TCP.
    pub transport: VideoTransport,
    /// Frame rate and nominal bitrate.
    pub profile: VideoProfile,
    /// Bitrate override, 0.5–100 Mb/s.
    pub bitrate_mbps: Option<f64>,
    /// Frame-rate override, 1–120.
    pub fps: Option<u32>,
    /// Frames per GOP, 1–600 and `≤ fps × duration_secs`.
    pub gop: u32,
    /// Keyframe size ÷ P-frame size, 1–20.
    pub keyframe_ratio: u32,
    /// Measured seconds, 2–60.
    pub duration_secs: u32,
    /// Unmeasured lead-in, 0–10 and `< duration_secs − 1`.
    pub warmup_secs: u32,
    /// Paced or burst.
    pub pacing: Pacing,
    /// Playout deadline beyond the fastest frame, 0–1000 ms.
    pub jitter_buffer_ms: u32,
}

impl Default for VideoParams {
    fn default() -> Self {
        Self {
            transport: VideoTransport::Udp,
            profile: VideoProfile::P1080p30,
            bitrate_mbps: None,
            fps: None,
            gop: 60,
            keyframe_ratio: 8,
            duration_secs: 10,
            warmup_secs: 1,
            pacing: Pacing::Paced,
            jitter_buffer_ms: 100,
        }
    }
}

impl VideoParams {
    /// `fps`, else the profile's.
    pub fn effective_fps(&self) -> u32 {
        self.fps.unwrap_or_else(|| self.profile.fps())
    }

    /// `bitrate_mbps`, else the profile's.
    pub fn effective_bitrate_mbps(&self) -> f64 {
        self.bitrate_mbps
            .unwrap_or_else(|| self.profile.bitrate_mbps())
    }

    /// P-frame size: `round(b/8 · G/fps / (K + G − 1))` bytes (§8.2 video), at least 1.
    pub fn p_frame_bytes(&self) -> u64 {
        let b = self.effective_bitrate_mbps() * 1e6;
        let g = f64::from(self.gop.max(1));
        let k = f64::from(self.keyframe_ratio.max(1));
        let fps = f64::from(self.effective_fps().max(1));
        let p = (b / 8.0 * g / fps / (k + g - 1.0)).round();
        (p as u64).max(1)
    }

    /// Keyframe size: `K · P`.
    pub fn i_frame_bytes(&self) -> u64 {
        u64::from(self.keyframe_ratio.max(1)) * self.p_frame_bytes()
    }

    /// Frame `k` is a keyframe iff `k mod G == 0`.
    pub fn is_keyframe(&self, frame_no: u64) -> bool {
        frame_no.is_multiple_of(u64::from(self.gop.max(1)))
    }

    /// Size of frame `k`.
    pub fn frame_bytes(&self, frame_no: u64) -> u64 {
        if self.is_keyframe(frame_no) {
            self.i_frame_bytes()
        } else {
            self.p_frame_bytes()
        }
    }

    /// Frames sent over warmup + duration.
    pub fn total_frames(&self) -> u64 {
        u64::from(self.effective_fps()) * u64::from(self.warmup_secs + self.duration_secs)
    }

    /// Offered load of frames `0..frames`: `Σ bytes · 8 / (frames / fps) / 1e6`.
    pub fn offered_mbps(&self, frames: u64) -> Option<f64> {
        if frames == 0 {
            return None;
        }
        let bytes: u64 = (0..frames).map(|k| self.frame_bytes(k)).sum();
        let secs = frames as f64 / f64::from(self.effective_fps().max(1));
        Some(bytes as f64 * 8.0 / secs / 1e6)
    }
}

/// `egress`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct EgressParams {
    /// Upload or download.
    pub direction: OneWayDirection,
    /// Streams, 1–8.
    pub streams: u32,
    /// Measured seconds, 2–60.
    pub duration_secs: u32,
    /// Unmeasured lead-in, 0–10 and `< duration_secs − 1`.
    pub warmup_secs: u32,
}

impl Default for EgressParams {
    fn default() -> Self {
        Self {
            direction: OneWayDirection::Upload,
            streams: 1,
            duration_secs: 10,
            warmup_secs: 1,
        }
    }
}

// ---------------------------------------------------------------------------
// WorkloadParams
// ---------------------------------------------------------------------------

/// Parameters of one run, tagged by `"workload"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "workload", rename_all = "kebab-case")]
pub enum WorkloadParams {
    /// `path`.
    Path(PathParams),
    /// `tcp`.
    Tcp(TcpParams),
    /// `udp`.
    Udp(UdpParams),
    /// `quic`.
    Quic(QuicParams),
    /// `ws`.
    Ws(WsParams),
    /// `file-transfer`.
    FileTransfer(FileTransferParams),
    /// `synced-store`.
    SyncedStore(StoreParams),
    /// `http`.
    Http(HttpParams),
    /// `video`.
    Video(VideoParams),
    /// `egress`.
    Egress(EgressParams),
}

impl Default for WorkloadParams {
    fn default() -> Self {
        Self::Path(PathParams::default())
    }
}

fn range<T: PartialOrd + fmt::Display>(
    workload: &str,
    field: &str,
    value: T,
    lo: T,
    hi: T,
) -> Result<(), String> {
    if value < lo || value > hi {
        return Err(format!(
            "{workload}.{field} must be between {lo} and {hi} (got {value})"
        ));
    }
    Ok(())
}

fn warmup_rule(workload: &str, warmup: u32, duration: u32) -> Result<(), String> {
    range(workload, "warmup_secs", warmup, 0, 10)?;
    if warmup + 1 >= duration {
        return Err(format!(
            "{workload}.warmup_secs must be less than duration_secs − 1 (got warmup {warmup} s, duration {duration} s)"
        ));
    }
    Ok(())
}

fn warmup_count_rule(workload: &str, warmup_count: u32, count: u64) -> Result<(), String> {
    range(workload, "warmup_count", warmup_count, 0, 1000)?;
    if u64::from(warmup_count) >= count {
        return Err(format!(
            "{workload}.warmup_count must be less than the sample count (got {warmup_count} of {count})"
        ));
    }
    Ok(())
}

fn secs(s: u32) -> Duration {
    Duration::from_secs(u64::from(s))
}

/// Time to move `bytes` at 1 Mb/s.
fn at_one_mbps(bytes: u64) -> Duration {
    Duration::from_secs_f64(bytes as f64 * 8.0 / 1e6)
}

/// Format a byte count: exact binary multiples as KiB/MiB/GiB, else bytes.
pub fn fmt_bytes(n: u64) -> String {
    if n >= GIB && n.is_multiple_of(GIB) {
        format!("{} GiB", n / GIB)
    } else if n >= MIB && n.is_multiple_of(MIB) {
        format!("{} MiB", n / MIB)
    } else if n >= KIB && n.is_multiple_of(KIB) {
        format!("{} KiB", n / KIB)
    } else {
        format!("{n} B")
    }
}

fn plural(n: u64, one: &str, many: &str) -> String {
    if n == 1 {
        format!("{n} {one}")
    } else {
        format!("{n} {many}")
    }
}

fn fmt_rate(mbps: f64) -> String {
    format!("{mbps} Mb/s")
}

impl WorkloadParams {
    /// Which workload this is.
    pub fn workload(&self) -> Workload {
        match self {
            Self::Path(_) => Workload::Path,
            Self::Tcp(_) => Workload::Tcp,
            Self::Udp(_) => Workload::Udp,
            Self::Quic(_) => Workload::Quic,
            Self::Ws(_) => Workload::Ws,
            Self::FileTransfer(_) => Workload::FileTransfer,
            Self::SyncedStore(_) => Workload::SyncedStore,
            Self::Http(_) => Workload::Http,
            Self::Video(_) => Workload::Video,
            Self::Egress(_) => Workload::Egress,
        }
    }

    /// The defaults of every workload, in [`Workload::ALL`] order (`workload_defaults`).
    pub fn defaults() -> Vec<WorkloadParams> {
        Workload::ALL.iter().map(|w| w.default_params()).collect()
    }

    /// Parse and validate params from JSON. Errors are `invalid-params`.
    pub fn from_value(value: serde_json::Value) -> Result<Self, AppError> {
        let params: Self = serde_json::from_value(value)
            .map_err(|e| AppError::invalid_params(format!("invalid params: {e}")))?;
        params.validate().map_err(AppError::invalid_params)?;
        Ok(params)
    }

    /// Apply `field = value` overrides (from `--param k=v` or a suite's
    /// `"<workload>.<field>"`), re-checking field names and types. Does not
    /// validate bounds; call [`validate`](Self::validate) afterwards.
    pub fn apply_overrides<'a, I>(&mut self, fields: I) -> Result<(), AppError>
    where
        I: IntoIterator<Item = (&'a str, &'a serde_json::Value)>,
    {
        let workload = self.workload();
        let mut obj = match serde_json::to_value(&*self) {
            Ok(serde_json::Value::Object(map)) => map,
            _ => return Err(AppError::internal("params did not serialise to an object")),
        };
        for (field, value) in fields {
            if field == "workload" {
                return Err(AppError::invalid_params(
                    "the workload cannot be changed by an override",
                ));
            }
            obj.insert(field.to_string(), value.clone());
        }
        *self = serde_json::from_value(serde_json::Value::Object(obj)).map_err(|e| {
            AppError::invalid_params(format!("invalid {} params: {e}", workload.as_str()))
        })?;
        Ok(())
    }

    /// Check every bound and cross-field rule (§7.3). Rejects, never clamps.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Path(p) => {
                range("path", "count", p.count, 2, 200)?;
                range("path", "interval_ms", p.interval_ms, 50, 5000)?;
                if u64::from(p.count) * p.interval_ms > 300_000 {
                    return Err(format!(
                        "path.count × path.interval_ms must be at most 300 s (got {} ms)",
                        u64::from(p.count) * p.interval_ms
                    ));
                }
            }
            Self::Tcp(p) => {
                range("tcp", "streams", p.streams, 1, 16)?;
                range("tcp", "duration_secs", p.duration_secs, 2, 60)?;
                match p.mode {
                    StreamMode::Throughput => warmup_rule("tcp", p.warmup_secs, p.duration_secs)?,
                    StreamMode::Latency => {
                        range("tcp", "warmup_secs", p.warmup_secs, 0, 10)?;
                        range("tcp", "payload_bytes", p.payload_bytes, 1, 65536)?;
                        range("tcp", "count", p.count, 10, 10_000)?;
                        warmup_count_rule("tcp", p.warmup_count, u64::from(p.count))?;
                    }
                }
            }
            Self::Udp(p) => {
                range("udp", "payload_bytes", p.payload_bytes, 30, 1400)?;
                if !p.rate_mbps.is_finite() {
                    return Err("udp.rate_mbps must be a finite number".into());
                }
                range("udp", "rate_mbps", p.rate_mbps, 0.1, 1000.0)?;
                range("udp", "duration_secs", p.duration_secs, 2, 60)?;
                match p.mode {
                    UdpMode::Stream | UdpMode::Ramp => {
                        warmup_rule("udp", p.warmup_secs, p.duration_secs)?
                    }
                    UdpMode::Echo => {
                        range("udp", "warmup_secs", p.warmup_secs, 0, 10)?;
                        range("udp", "pps", p.pps, 1, 2000)?;
                        warmup_count_rule(
                            "udp",
                            p.warmup_count,
                            u64::from(p.pps) * u64::from(p.duration_secs),
                        )?;
                    }
                }
            }
            Self::Quic(p) => {
                range("quic", "streams", p.streams, 1, 32)?;
                range("quic", "duration_secs", p.duration_secs, 2, 60)?;
                match p.mode {
                    StreamMode::Throughput => warmup_rule("quic", p.warmup_secs, p.duration_secs)?,
                    StreamMode::Latency => {
                        range("quic", "warmup_secs", p.warmup_secs, 0, 10)?;
                        range("quic", "payload_bytes", p.payload_bytes, 1, 65536)?;
                        range("quic", "count", p.count, 10, 10_000)?;
                        warmup_count_rule("quic", p.warmup_count, u64::from(p.count))?;
                    }
                }
            }
            Self::Ws(p) => match p.mode {
                WsMode::Rr => {
                    range("ws", "payload_bytes", p.payload_bytes, 0, 1_048_576)?;
                    range("ws", "count", p.count, 10, 100_000)?;
                    range("ws", "concurrency", p.concurrency, 1, 64)?;
                    warmup_count_rule("ws", p.warmup_count, u64::from(p.count))?;
                    let total = u64::from(p.count) * u64::from(p.payload_bytes);
                    if total > GIB {
                        return Err(format!(
                            "ws.count × ws.payload_bytes must be at most 1 GiB (got {})",
                            fmt_bytes(total)
                        ));
                    }
                }
                WsMode::Stream => {
                    range("ws", "payload_bytes", p.payload_bytes, 0, 262_144)?;
                    range("ws", "duration_secs", p.duration_secs, 2, 60)?;
                    if let Some(rate) = p.rate_per_sec {
                        range("ws", "rate_per_sec", rate, 1, 100_000)?;
                    }
                }
            },
            Self::FileTransfer(p) => {
                range("file-transfer", "size_bytes", p.size_bytes, 1024, GIB)?;
                range("file-transfer", "iterations", p.iterations, 1, 20)?;
                let total = p.size_bytes * u64::from(p.iterations);
                if total > 4 * GIB {
                    return Err(format!(
                        "file-transfer.size_bytes × iterations must be at most 4 GiB (got {})",
                        fmt_bytes(total)
                    ));
                }
            }
            Self::SyncedStore(p) => {
                range("synced-store", "count", p.count, 10, 5000)?;
                range("synced-store", "payload_bytes", p.payload_bytes, 0, 65536)?;
                if p.mode == StoreMode::Pingpong {
                    warmup_count_rule("synced-store", p.warmup_count, u64::from(p.count))?;
                }
            }
            Self::Http(p) => {
                range("http", "concurrency", p.concurrency, 1, 64)?;
                range("http", "duration_secs", p.duration_secs, 2, 60)?;
                warmup_rule("http", p.warmup_secs, p.duration_secs)?;
                range("http", "response_bytes", p.response_bytes, 0, 10 * MIB)?;
            }
            Self::Video(p) => {
                if let Some(b) = p.bitrate_mbps {
                    if !b.is_finite() {
                        return Err("video.bitrate_mbps must be a finite number".into());
                    }
                    range("video", "bitrate_mbps", b, 0.5, 100.0)?;
                }
                if let Some(fps) = p.fps {
                    range("video", "fps", fps, 1, 120)?;
                }
                range("video", "duration_secs", p.duration_secs, 2, 60)?;
                warmup_rule("video", p.warmup_secs, p.duration_secs)?;
                range("video", "gop", p.gop, 1, 600)?;
                let max_gop = p.effective_fps() * p.duration_secs;
                if p.gop > max_gop {
                    return Err(format!(
                        "video.gop must be at most fps × duration_secs ({max_gop}; got {})",
                        p.gop
                    ));
                }
                range("video", "keyframe_ratio", p.keyframe_ratio, 1, 20)?;
                range("video", "jitter_buffer_ms", p.jitter_buffer_ms, 0, 1000)?;
            }
            Self::Egress(p) => {
                range("egress", "streams", p.streams, 1, 8)?;
                range("egress", "duration_secs", p.duration_secs, 2, 60)?;
                warmup_rule("egress", p.warmup_secs, p.duration_secs)?;
            }
        }
        Ok(())
    }

    /// Whether the run is bounded by a duration (and so gets a path sampler,
    /// `loaded_rtt_ms` and a warmup window).
    pub fn time_based(&self) -> bool {
        match self {
            Self::Path(_) | Self::FileTransfer(_) | Self::SyncedStore(_) => false,
            Self::Tcp(p) => p.mode == StreamMode::Throughput,
            Self::Quic(p) => p.mode == StreamMode::Throughput,
            Self::Ws(p) => p.mode == WsMode::Stream,
            Self::Udp(_) | Self::Http(_) | Self::Video(_) | Self::Egress(_) => true,
        }
    }

    /// Measured seconds, for time-based runs.
    pub fn duration_secs(&self) -> Option<u32> {
        if !self.time_based() {
            return None;
        }
        Some(match self {
            Self::Tcp(p) => p.duration_secs,
            Self::Udp(p) => p.duration_secs,
            Self::Quic(p) => p.duration_secs,
            Self::Ws(p) => p.duration_secs,
            Self::Http(p) => p.duration_secs,
            Self::Video(p) => p.duration_secs,
            Self::Egress(p) => p.duration_secs,
            Self::Path(_) | Self::FileTransfer(_) | Self::SyncedStore(_) => return None,
        })
    }

    /// Warmup seconds, for time-based runs that have a warmup window
    /// (`ws` stream and `udp` echo have none: 0).
    pub fn warmup_secs(&self) -> Option<u32> {
        if !self.time_based() {
            return None;
        }
        Some(match self {
            Self::Tcp(p) => p.warmup_secs,
            Self::Quic(p) => p.warmup_secs,
            Self::Udp(p) if p.mode == UdpMode::Echo => 0,
            Self::Udp(p) => p.warmup_secs,
            Self::Http(p) => p.warmup_secs,
            Self::Video(p) => p.warmup_secs,
            Self::Egress(p) => p.warmup_secs,
            Self::Ws(_) | Self::Path(_) | Self::FileTransfer(_) | Self::SyncedStore(_) => 0,
        })
    }

    /// When the initiator aborts (§7.3). The responder watchdog uses
    /// [`responder_watchdog`](Self::responder_watchdog).
    ///
    /// Count-based runs get a fixed allowance per round trip, so a slow
    /// path is not a failure: tcp/quic latency get
    /// `30 s + count × (250 ms + payload × 8 / 1 Mb/s)`, and ws rr gets
    /// `30 s + ⌈n / concurrency⌉ × 250 ms + n × max(payload, 1024) × 8 / 1 Mb/s`
    /// with `n = warmup_count + count`. ws runs also get their whole
    /// `finish` budget on top (the 30 s finish timeout plus the workload's
    /// drain allowance: 5 s rr, at most 120 s stream), so a finish whose
    /// request or reply was lost can still be re-sent before the deadline.
    pub fn deadline(&self) -> Duration {
        const SLACK: Duration = Duration::from_secs(30);
        let timed = |warmup: u32, duration: u32| secs(warmup) + secs(duration) + SLACK;
        let latency = |count: u32, payload: u32| {
            SLACK + (Duration::from_millis(250) + at_one_mbps(u64::from(payload))) * count
        };
        match self {
            Self::Path(p) => Duration::from_millis(p.interval_ms + 5000) * p.count,
            Self::Tcp(p) => match p.mode {
                StreamMode::Throughput => timed(p.warmup_secs, p.duration_secs),
                StreamMode::Latency => latency(p.count, p.payload_bytes),
            },
            Self::Quic(p) => match p.mode {
                StreamMode::Throughput => timed(p.warmup_secs, p.duration_secs),
                StreamMode::Latency => latency(p.count, p.payload_bytes),
            },
            Self::Udp(p) => match p.mode {
                UdpMode::Echo => timed(0, p.duration_secs),
                UdpMode::Stream | UdpMode::Ramp => timed(p.warmup_secs, p.duration_secs),
            },
            Self::Ws(p) => match p.mode {
                WsMode::Rr => {
                    let n = p.warmup_count.saturating_add(p.count);
                    let rounds = n.div_ceil(p.concurrency.max(1));
                    SLACK
                        + Duration::from_millis(250) * rounds
                        + at_one_mbps(u64::from(p.payload_bytes.max(1024))) * n
                        + crate::control::FINISH_TIMEOUT
                        + crate::workloads::ws::RR_DRAIN
                }
                WsMode::Stream => {
                    timed(0, p.duration_secs)
                        + crate::control::FINISH_TIMEOUT
                        + crate::workloads::ws::STREAM_DRAIN_CAP
                }
            },
            Self::FileTransfer(p) => {
                (Duration::from_secs(90) + at_one_mbps(p.size_bytes)) * p.iterations
            }
            Self::SyncedStore(p) => Duration::from_secs(10) * p.count,
            Self::Http(p) => timed(p.warmup_secs, p.duration_secs),
            Self::Video(p) => timed(p.warmup_secs, p.duration_secs),
            Self::Egress(p) => timed(p.warmup_secs, p.duration_secs),
        }
    }

    /// The responder's watchdog: `min(deadline + 60 s, 30 min)` (§6 rule 4),
    /// armed at `prepare`.
    ///
    /// A file-transfer run may legitimately outlast the 30 min cap (up to
    /// 4 GiB), so its watchdog is pushed out each time an iteration ends:
    /// see [`responder_watchdog_after`](Self::responder_watchdog_after).
    pub fn responder_watchdog(&self) -> Duration {
        (self.deadline() + WATCHDOG_GRACE).min(WATCHDOG_CAP)
    }

    /// How far the responder's watchdog reaches after `done` file-transfer
    /// iterations ended, from that moment: the remaining iterations'
    /// deadlines plus 60 s, at most 30 min. The cap then bounds the time
    /// without progress, not the whole run. `None` for other workloads
    /// (their watchdog is never extended).
    pub fn responder_watchdog_after(&self, done: u32) -> Option<Duration> {
        match self {
            Self::FileTransfer(p) => {
                let left = p.iterations.saturating_sub(done);
                let per_iteration = Duration::from_secs(90) + at_one_mbps(p.size_bytes);
                Some((per_iteration * left + WATCHDOG_GRACE).min(WATCHDOG_CAP))
            }
            _ => None,
        }
    }

    /// Rough wall time under nominal conditions, for suite planning. Not a
    /// deadline: count-based workloads assume a fast LAN.
    pub fn nominal_secs(&self) -> f64 {
        match self {
            Self::Path(p) => f64::from(p.count) * p.interval_ms as f64 / 1000.0,
            Self::Tcp(p) if p.mode == StreamMode::Latency => f64::from(p.count) * 0.01,
            Self::Quic(p) if p.mode == StreamMode::Latency => f64::from(p.count) * 0.01,
            Self::Ws(p) if p.mode == WsMode::Rr => {
                f64::from(p.count) * 0.005 / f64::from(p.concurrency.max(1))
            }
            Self::FileTransfer(p) => {
                f64::from(p.iterations) * (1.0 + p.size_bytes as f64 * 8.0 / 100e6)
            }
            Self::SyncedStore(p) => f64::from(p.count) * 0.02,
            other => {
                f64::from(other.warmup_secs().unwrap_or(0))
                    + f64::from(other.duration_secs().unwrap_or(0))
            }
        }
    }

    /// A label for people, e.g. `"TCP throughput · upload · 4 streams · 10 s"`.
    pub fn label(&self) -> String {
        let parts: Vec<String> = match self {
            Self::Path(p) => vec![
                "Path".into(),
                plural(u64::from(p.count), "ping", "pings"),
                format!("{} ms apart", p.interval_ms),
            ],
            Self::Tcp(p) => {
                let mut v = stream_label(
                    "TCP",
                    p.mode,
                    p.direction,
                    p.streams,
                    p.duration_secs,
                    p.payload_bytes,
                    p.count,
                );
                if !p.nodelay {
                    v.push("Nagle on".into());
                }
                v
            }
            Self::Quic(p) => stream_label(
                "QUIC",
                p.mode,
                p.direction,
                p.streams,
                p.duration_secs,
                p.payload_bytes,
                p.count,
            ),
            Self::Udp(p) => match p.mode {
                UdpMode::Stream => vec![
                    "UDP stream".into(),
                    dir_str(p.direction).into(),
                    fmt_rate(p.rate_mbps),
                    format!("{} B", p.payload_bytes),
                    format!("{} s", p.duration_secs),
                ],
                UdpMode::Echo => vec![
                    "UDP echo".into(),
                    format!("{} pps", p.pps),
                    format!("{} B", p.payload_bytes),
                    format!("{} s", p.duration_secs),
                ],
                UdpMode::Ramp => vec![
                    "UDP ramp".into(),
                    dir_str(p.direction).into(),
                    format!("from {}", fmt_rate(p.rate_mbps)),
                    format!("{} B", p.payload_bytes),
                    format!("{} s", p.duration_secs),
                ],
            },
            Self::Ws(p) => match p.mode {
                WsMode::Rr => {
                    let mut v = vec![
                        "WebSocket request/reply".into(),
                        fmt_bytes(u64::from(p.payload_bytes)),
                        plural(u64::from(p.count), "request", "requests"),
                    ];
                    if p.concurrency > 1 {
                        v.push(format!("{} in flight", p.concurrency));
                    }
                    v
                }
                WsMode::Stream => {
                    let mut v = vec![
                        "WebSocket stream".into(),
                        fmt_bytes(u64::from(p.payload_bytes)),
                    ];
                    if let Some(rate) = p.rate_per_sec {
                        v.push(format!("{rate} msg/s"));
                    }
                    v.push(format!("{} s", p.duration_secs));
                    v
                }
            },
            Self::FileTransfer(p) => vec![
                "File transfer".into(),
                format!("{} × {}", fmt_bytes(p.size_bytes), p.iterations),
            ],
            Self::SyncedStore(p) => vec![
                match p.mode {
                    StoreMode::Pingpong => "SyncedStore ping-pong".into(),
                    StoreMode::Burst => "SyncedStore burst".into(),
                },
                plural(u64::from(p.count), "write", "writes"),
                fmt_bytes(u64::from(p.payload_bytes)),
            ],
            Self::Http(p) => vec![
                "HTTP".into(),
                match p.target {
                    HttpTarget::Proxy => "proxy".into(),
                    HttpTarget::Direct => "direct".into(),
                },
                match p.connection {
                    HttpConnection::Keepalive => "keep-alive".into(),
                    HttpConnection::New => "new connections".into(),
                },
                plural(u64::from(p.concurrency), "worker", "workers"),
                fmt_bytes(p.response_bytes),
                format!("{} s", p.duration_secs),
            ],
            Self::Video(p) => vec![
                "Video".into(),
                p.profile.as_str().into(),
                match p.transport {
                    VideoTransport::Udp => "UDP".into(),
                    VideoTransport::Quic => "QUIC".into(),
                    VideoTransport::Tcp => "TCP".into(),
                },
                fmt_rate(p.effective_bitrate_mbps()),
                match p.pacing {
                    Pacing::Paced => "paced".into(),
                    Pacing::Burst => "burst".into(),
                },
                format!("{} s", p.duration_secs),
            ],
            Self::Egress(p) => vec![
                "Egress".into(),
                dir_str(p.direction).into(),
                plural(u64::from(p.streams), "stream", "streams"),
                format!("{} s", p.duration_secs),
            ],
        };
        parts.join(" · ")
    }
}

fn dir_str(d: OneWayDirection) -> &'static str {
    match d {
        OneWayDirection::Upload => "upload",
        OneWayDirection::Download => "download",
    }
}

fn stream_label(
    name: &str,
    mode: StreamMode,
    direction: Direction,
    streams: u32,
    duration_secs: u32,
    payload_bytes: u32,
    count: u32,
) -> Vec<String> {
    match mode {
        StreamMode::Throughput => vec![
            format!("{name} throughput"),
            match direction {
                Direction::Upload => "upload".into(),
                Direction::Download => "download".into(),
                Direction::Bidirectional => "bidirectional".into(),
            },
            plural(u64::from(streams), "stream", "streams"),
            format!("{duration_secs} s"),
        ],
        StreamMode::Latency => vec![
            format!("{name} latency"),
            fmt_bytes(u64::from(payload_bytes)),
            plural(u64::from(count), "sample", "samples"),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn every_variant() -> Vec<WorkloadParams> {
        let mut v = WorkloadParams::defaults();
        v.push(WorkloadParams::Tcp(TcpParams {
            mode: StreamMode::Latency,
            nodelay: false,
            ..TcpParams::default()
        }));
        v.push(WorkloadParams::Tcp(TcpParams {
            direction: Direction::Bidirectional,
            streams: 4,
            ..TcpParams::default()
        }));
        v.push(WorkloadParams::Udp(UdpParams {
            mode: UdpMode::Echo,
            ..UdpParams::default()
        }));
        v.push(WorkloadParams::Udp(UdpParams {
            mode: UdpMode::Ramp,
            direction: OneWayDirection::Download,
            ..UdpParams::default()
        }));
        v.push(WorkloadParams::Quic(QuicParams {
            mode: StreamMode::Latency,
            ..QuicParams::default()
        }));
        v.push(WorkloadParams::Ws(WsParams {
            mode: WsMode::Stream,
            rate_per_sec: Some(1000),
            ..WsParams::default()
        }));
        v.push(WorkloadParams::SyncedStore(StoreParams {
            mode: StoreMode::Burst,
            ..StoreParams::default()
        }));
        v.push(WorkloadParams::Http(HttpParams {
            target: HttpTarget::Direct,
            connection: HttpConnection::New,
            ..HttpParams::default()
        }));
        for (transport, profile) in [
            (VideoTransport::Quic, VideoProfile::P720p30),
            (VideoTransport::Tcp, VideoProfile::P1080p60),
            (VideoTransport::Udp, VideoProfile::P4k30),
        ] {
            v.push(WorkloadParams::Video(VideoParams {
                transport,
                profile,
                pacing: Pacing::Burst,
                ..VideoParams::default()
            }));
        }
        v.push(WorkloadParams::Video(VideoParams {
            bitrate_mbps: Some(3.5),
            fps: Some(24),
            ..VideoParams::default()
        }));
        v.push(WorkloadParams::Egress(EgressParams {
            direction: OneWayDirection::Download,
            ..EgressParams::default()
        }));
        v
    }

    #[test]
    fn every_variant_round_trips_and_validates() {
        for p in every_variant() {
            p.validate().unwrap_or_else(|e| panic!("{p:?}: {e}"));
            let json = serde_json::to_value(&p).unwrap();
            assert_eq!(json["workload"], p.workload().as_str());
            let back: WorkloadParams = serde_json::from_value(json.clone()).unwrap();
            assert_eq!(back, p, "{json}");
            assert!(!p.label().is_empty());
            assert!(p.deadline() > Duration::ZERO);
            assert!(p.responder_watchdog() <= WATCHDOG_CAP);
        }
    }

    #[test]
    fn tag_only_gives_defaults() {
        for w in Workload::ALL {
            let p: WorkloadParams =
                serde_json::from_value(json!({"workload": w.as_str()})).unwrap();
            assert_eq!(p, w.default_params());
            assert_eq!(w.as_str().parse::<Workload>().unwrap(), w);
        }
        assert!("tcpp".parse::<Workload>().is_err());
    }

    #[test]
    fn enums_use_rfc_spellings() {
        let v = serde_json::to_value(WorkloadParams::Video(VideoParams::default())).unwrap();
        assert_eq!(v["profile"], "1080p30");
        assert_eq!(v["transport"], "udp");
        assert_eq!(v["pacing"], "paced");
        assert_eq!(v["bitrate_mbps"], serde_json::Value::Null);
        let v = serde_json::to_value(WorkloadParams::Http(HttpParams::default())).unwrap();
        assert_eq!(v["connection"], "keepalive");
        let v = serde_json::to_value(WorkloadParams::SyncedStore(StoreParams::default())).unwrap();
        assert_eq!(v["mode"], "pingpong");
        let v = serde_json::to_value(WorkloadParams::Ws(WsParams::default())).unwrap();
        assert_eq!(v["mode"], "rr");
        let p: WorkloadParams =
            serde_json::from_value(json!({"workload": "tcp", "direction": "bidirectional"}))
                .unwrap();
        assert!(matches!(
            p,
            WorkloadParams::Tcp(TcpParams {
                direction: Direction::Bidirectional,
                ..
            })
        ));
        let p: WorkloadParams =
            serde_json::from_value(json!({"workload": "video", "profile": "4k30"})).unwrap();
        assert!(matches!(
            p,
            WorkloadParams::Video(VideoParams {
                profile: VideoProfile::P4k30,
                ..
            })
        ));
        assert_eq!(
            serde_json::to_value(WorkloadParams::FileTransfer(FileTransferParams::default()))
                .unwrap(),
            json!({"workload": "file-transfer", "size_bytes": 10485760, "iterations": 3})
        );
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let err = serde_json::from_value::<WorkloadParams>(json!({"workload": "tcp", "stream": 4}));
        assert!(err.is_err());
        let err = WorkloadParams::from_value(json!({"workload": "path", "cnt": 4})).unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::InvalidParams);
        assert!(serde_json::from_value::<WorkloadParams>(json!({"workload": "nope"})).is_err());
        assert!(serde_json::from_value::<WorkloadParams>(json!({"count": 3})).is_err());
    }

    fn rejects(p: WorkloadParams, needle: &str) {
        let err = p
            .validate()
            .expect_err(&format!("{p:?} should be rejected"));
        assert!(err.contains(needle), "{err} does not mention {needle}");
    }

    #[test]
    fn bounds_are_rejected_not_clamped() {
        rejects(
            WorkloadParams::Path(PathParams {
                count: 1,
                ..Default::default()
            }),
            "path.count",
        );
        rejects(
            WorkloadParams::Path(PathParams {
                count: 201,
                ..Default::default()
            }),
            "path.count",
        );
        rejects(
            WorkloadParams::Path(PathParams {
                interval_ms: 49,
                ..Default::default()
            }),
            "interval_ms",
        );
        rejects(
            WorkloadParams::Path(PathParams {
                count: 200,
                interval_ms: 1600,
            }),
            "300 s",
        );
        WorkloadParams::Path(PathParams {
            count: 200,
            interval_ms: 1500,
        })
        .validate()
        .unwrap();
        rejects(
            WorkloadParams::Tcp(TcpParams {
                streams: 0,
                ..Default::default()
            }),
            "streams",
        );
        rejects(
            WorkloadParams::Tcp(TcpParams {
                streams: 17,
                ..Default::default()
            }),
            "streams",
        );
        rejects(
            WorkloadParams::Tcp(TcpParams {
                duration_secs: 61,
                ..Default::default()
            }),
            "duration_secs",
        );
        rejects(
            WorkloadParams::Quic(QuicParams {
                streams: 33,
                ..Default::default()
            }),
            "quic.streams",
        );
        rejects(
            WorkloadParams::Udp(UdpParams {
                payload_bytes: 1401,
                ..Default::default()
            }),
            "payload_bytes",
        );
        rejects(
            WorkloadParams::Udp(UdpParams {
                rate_mbps: 0.05,
                ..Default::default()
            }),
            "rate_mbps",
        );
        rejects(
            WorkloadParams::Udp(UdpParams {
                rate_mbps: f64::NAN,
                ..Default::default()
            }),
            "finite",
        );
        rejects(
            WorkloadParams::Udp(UdpParams {
                mode: UdpMode::Echo,
                pps: 2001,
                ..Default::default()
            }),
            "pps",
        );
        rejects(
            WorkloadParams::Ws(WsParams {
                concurrency: 65,
                ..Default::default()
            }),
            "concurrency",
        );
        rejects(
            WorkloadParams::Ws(WsParams {
                mode: WsMode::Stream,
                payload_bytes: 262_145,
                ..Default::default()
            }),
            "payload_bytes",
        );
        rejects(
            WorkloadParams::Ws(WsParams {
                mode: WsMode::Stream,
                rate_per_sec: Some(0),
                ..Default::default()
            }),
            "rate_per_sec",
        );
        rejects(
            WorkloadParams::FileTransfer(FileTransferParams {
                size_bytes: 1023,
                iterations: 1,
            }),
            "size_bytes",
        );
        rejects(
            WorkloadParams::FileTransfer(FileTransferParams {
                size_bytes: 1024,
                iterations: 21,
            }),
            "iterations",
        );
        rejects(
            WorkloadParams::SyncedStore(StoreParams {
                count: 5001,
                ..Default::default()
            }),
            "count",
        );
        rejects(
            WorkloadParams::Http(HttpParams {
                response_bytes: 10 * MIB + 1,
                ..Default::default()
            }),
            "response_bytes",
        );
        rejects(
            WorkloadParams::Video(VideoParams {
                bitrate_mbps: Some(0.4),
                ..Default::default()
            }),
            "bitrate_mbps",
        );
        rejects(
            WorkloadParams::Video(VideoParams {
                fps: Some(121),
                ..Default::default()
            }),
            "fps",
        );
        rejects(
            WorkloadParams::Video(VideoParams {
                keyframe_ratio: 21,
                ..Default::default()
            }),
            "keyframe_ratio",
        );
        rejects(
            WorkloadParams::Video(VideoParams {
                jitter_buffer_ms: 1001,
                ..Default::default()
            }),
            "jitter_buffer_ms",
        );
        rejects(
            WorkloadParams::Egress(EgressParams {
                streams: 9,
                ..Default::default()
            }),
            "streams",
        );
    }

    #[test]
    fn cross_field_rules() {
        // warmup < duration − 1
        rejects(
            WorkloadParams::Tcp(TcpParams {
                duration_secs: 2,
                warmup_secs: 1,
                ..Default::default()
            }),
            "warmup_secs",
        );
        WorkloadParams::Tcp(TcpParams {
            duration_secs: 2,
            warmup_secs: 0,
            ..Default::default()
        })
        .validate()
        .unwrap();
        WorkloadParams::Tcp(TcpParams {
            duration_secs: 3,
            warmup_secs: 1,
            ..Default::default()
        })
        .validate()
        .unwrap();
        rejects(
            WorkloadParams::Tcp(TcpParams {
                duration_secs: 60,
                warmup_secs: 11,
                ..Default::default()
            }),
            "warmup_secs",
        );
        rejects(
            WorkloadParams::Egress(EgressParams {
                duration_secs: 3,
                warmup_secs: 2,
                ..Default::default()
            }),
            "warmup_secs",
        );
        // warmup_count < count
        rejects(
            WorkloadParams::Tcp(TcpParams {
                mode: StreamMode::Latency,
                count: 10,
                warmup_count: 10,
                ..Default::default()
            }),
            "warmup_count",
        );
        rejects(
            WorkloadParams::Udp(UdpParams {
                mode: UdpMode::Echo,
                pps: 1,
                duration_secs: 2,
                warmup_count: 2,
                ..Default::default()
            }),
            "warmup_count",
        );
        // Latency-only bounds are not checked in throughput mode.
        WorkloadParams::Tcp(TcpParams {
            payload_bytes: 0,
            count: 0,
            ..Default::default()
        })
        .validate()
        .unwrap();
        rejects(
            WorkloadParams::Tcp(TcpParams {
                mode: StreamMode::Latency,
                payload_bytes: 0,
                ..Default::default()
            }),
            "payload_bytes",
        );
        // gop ≤ fps × duration (effective fps)
        rejects(
            WorkloadParams::Video(VideoParams {
                fps: Some(10),
                duration_secs: 5,
                warmup_secs: 1,
                gop: 51,
                ..Default::default()
            }),
            "gop",
        );
        WorkloadParams::Video(VideoParams {
            fps: Some(10),
            duration_secs: 5,
            warmup_secs: 1,
            gop: 50,
            ..Default::default()
        })
        .validate()
        .unwrap();
        // totals
        rejects(
            WorkloadParams::Ws(WsParams {
                payload_bytes: 1_048_576,
                count: 1025,
                ..Default::default()
            }),
            "1 GiB",
        );
        WorkloadParams::Ws(WsParams {
            payload_bytes: 1_048_576,
            count: 1024,
            ..Default::default()
        })
        .validate()
        .unwrap();
        rejects(
            WorkloadParams::FileTransfer(FileTransferParams {
                size_bytes: GIB,
                iterations: 5,
            }),
            "4 GiB",
        );
        WorkloadParams::FileTransfer(FileTransferParams {
            size_bytes: GIB,
            iterations: 4,
        })
        .validate()
        .unwrap();
    }

    #[test]
    fn deadlines_follow_the_rfc_table() {
        let d = |p: WorkloadParams| p.deadline().as_secs_f64();
        assert_eq!(d(WorkloadParams::Tcp(TcpParams::default())), 41.0);
        assert_eq!(d(WorkloadParams::Video(VideoParams::default())), 41.0);
        assert_eq!(d(WorkloadParams::Path(PathParams::default())), 20.0 * 5.25);
        // ws rr: 30 s + 510 rounds × 250 ms + 510 × 1024 × 8 / 1e6, plus
        // the finish budget (30 s + 5 s drain).
        let ws = d(WorkloadParams::Ws(WsParams::default()));
        let expect = 30.0 + 510.0 * 0.25 + 510.0 * 8192.0 / 1e6 + 35.0;
        assert!((ws - expect).abs() < 1e-6, "{ws} vs {expect}");
        // Requests in flight share a round: ⌈510 / 4⌉ = 128.
        let ws4 = d(WorkloadParams::Ws(WsParams {
            concurrency: 4,
            ..Default::default()
        }));
        let expect = 30.0 + 128.0 * 0.25 + 510.0 * 8192.0 / 1e6 + 35.0;
        assert!((ws4 - expect).abs() < 1e-6, "{ws4} vs {expect}");
        // A 100 ms path fits the default rr run (510 round trips ≈ 51 s).
        assert!(ws > 51.0 + 30.0);
        // ws stream: duration + 30 s, plus the finish budget (30 s + 120 s).
        assert_eq!(
            d(WorkloadParams::Ws(WsParams {
                mode: WsMode::Stream,
                ..Default::default()
            })),
            190.0
        );
        assert_eq!(
            d(WorkloadParams::SyncedStore(StoreParams::default())),
            1000.0
        );
        let ft = d(WorkloadParams::FileTransfer(FileTransferParams::default()));
        assert!(
            (ft - 3.0 * (90.0 + 10.0 * MIB as f64 * 8.0 / 1e6)).abs() < 1e-6,
            "{ft}"
        );
        let watchdog = WorkloadParams::Tcp(TcpParams::default()).responder_watchdog();
        assert_eq!(watchdog, Duration::from_secs(101));
        let big = WorkloadParams::FileTransfer(FileTransferParams {
            size_bytes: GIB,
            iterations: 4,
        });
        assert_eq!(big.responder_watchdog(), WATCHDOG_CAP);
        // A file-transfer watchdog is pushed out per iteration instead: each
        // 1 GiB file gets up to 30 min of its own.
        assert_eq!(big.responder_watchdog_after(1), Some(WATCHDOG_CAP));
        assert_eq!(big.responder_watchdog_after(4), Some(WATCHDOG_GRACE));
        let small = WorkloadParams::FileTransfer(FileTransferParams {
            size_bytes: MIB,
            iterations: 2,
        });
        let per = 90.0 + MIB as f64 * 8.0 / 1e6;
        let after1 = small.responder_watchdog_after(1).unwrap().as_secs_f64();
        assert!((after1 - (per + 60.0)).abs() < 1e-6, "{after1}");
        assert_eq!(
            WorkloadParams::Tcp(TcpParams::default()).responder_watchdog_after(1),
            None
        );
    }

    #[test]
    fn time_based_and_windows() {
        assert!(WorkloadParams::Tcp(TcpParams::default()).time_based());
        assert!(!WorkloadParams::Tcp(TcpParams {
            mode: StreamMode::Latency,
            ..Default::default()
        })
        .time_based());
        assert!(!WorkloadParams::Path(PathParams::default()).time_based());
        assert!(!WorkloadParams::Ws(WsParams::default()).time_based());
        assert!(WorkloadParams::Ws(WsParams {
            mode: WsMode::Stream,
            ..Default::default()
        })
        .time_based());
        assert!(WorkloadParams::Udp(UdpParams {
            mode: UdpMode::Echo,
            ..Default::default()
        })
        .time_based());
        assert_eq!(
            WorkloadParams::Udp(UdpParams {
                mode: UdpMode::Echo,
                ..Default::default()
            })
            .warmup_secs(),
            Some(0)
        );
        assert_eq!(
            WorkloadParams::Http(HttpParams::default()).duration_secs(),
            Some(10)
        );
        assert_eq!(
            WorkloadParams::FileTransfer(FileTransferParams::default()).duration_secs(),
            None
        );
    }

    #[test]
    fn labels() {
        assert_eq!(
            WorkloadParams::Tcp(TcpParams {
                streams: 4,
                ..Default::default()
            })
            .label(),
            "TCP throughput · upload · 4 streams · 10 s"
        );
        assert_eq!(
            WorkloadParams::Tcp(TcpParams::default()).label(),
            "TCP throughput · upload · 1 stream · 10 s"
        );
        assert_eq!(
            WorkloadParams::Tcp(TcpParams {
                mode: StreamMode::Latency,
                payload_bytes: 32768,
                nodelay: false,
                ..Default::default()
            })
            .label(),
            "TCP latency · 32 KiB · 200 samples · Nagle on"
        );
        assert_eq!(
            WorkloadParams::FileTransfer(FileTransferParams::default()).label(),
            "File transfer · 10 MiB × 3"
        );
        assert_eq!(
            WorkloadParams::Video(VideoParams::default()).label(),
            "Video · 1080p30 · UDP · 6 Mb/s · paced · 10 s"
        );
        assert_eq!(
            WorkloadParams::Udp(UdpParams::default()).label(),
            "UDP stream · upload · 10 Mb/s · 1200 B · 10 s"
        );
        assert_eq!(fmt_bytes(1200), "1200 B");
        assert_eq!(fmt_bytes(1024), "1 KiB");
        assert_eq!(fmt_bytes(GIB), "1 GiB");
    }

    #[test]
    fn overrides_merge_and_recheck_names() {
        let mut p = Workload::Tcp.default_params();
        let v4 = json!(4);
        let vdl = json!("download");
        p.apply_overrides([("streams", &v4), ("direction", &vdl)])
            .unwrap();
        assert_eq!(
            p,
            WorkloadParams::Tcp(TcpParams {
                streams: 4,
                direction: Direction::Download,
                ..Default::default()
            })
        );
        let bad = json!(1);
        let err = p.apply_overrides([("nope", &bad)]).unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::InvalidParams);
        let wl = json!("udp");
        assert!(p.apply_overrides([("workload", &wl)]).is_err());
        let wrong_type = json!("four");
        assert!(p.apply_overrides([("streams", &wrong_type)]).is_err());
        // Overrides do not validate bounds; validate() does.
        let big = json!(99);
        p.apply_overrides([("streams", &big)]).unwrap();
        assert!(p.validate().is_err());
    }

    #[test]
    fn video_profiles_and_frame_model() {
        let p = VideoParams::default();
        assert_eq!(p.effective_fps(), 30);
        assert_eq!(p.effective_bitrate_mbps(), 6.0);
        assert!(p.is_keyframe(0) && p.is_keyframe(60) && !p.is_keyframe(1));
        assert_eq!(p.i_frame_bytes(), 8 * p.p_frame_bytes());
        // Mean over whole GOPs equals the target bitrate (up to rounding of P).
        for profile in [
            VideoProfile::P720p30,
            VideoProfile::P1080p30,
            VideoProfile::P1080p60,
            VideoProfile::P4k30,
        ] {
            let p = VideoParams {
                profile,
                ..Default::default()
            };
            let offered = p.offered_mbps(u64::from(p.gop) * 5).unwrap();
            let target = profile.bitrate_mbps();
            assert!(
                (offered - target).abs() / target < 1e-3,
                "{profile:?}: {offered} vs {target}"
            );
        }
        let p = VideoParams {
            bitrate_mbps: Some(0.5),
            fps: Some(120),
            gop: 1,
            keyframe_ratio: 20,
            ..Default::default()
        };
        assert!(p.p_frame_bytes() >= 1);
        assert_eq!(p.offered_mbps(0), None);
        assert_eq!(VideoParams::default().total_frames(), 330);
    }
}
