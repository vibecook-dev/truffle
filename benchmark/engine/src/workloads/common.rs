//! Helpers shared by the data-plane workloads (tcp, quic, egress, video, …).
//!
//! - TBN1 streams: [`Tbn1Acceptor`] (rule 5 identity check, then each
//!   connection's header read on its own task, so a silent connection never
//!   holds up the others), [`read_tbn1`] (one stream's header with the
//!   run's tag, within [`HEADER_TIMEOUT`]), [`receive_stream`] (count to EOF
//!   into a [`ReceiverAccounting`](crate::dataplane::ReceiverAccounting),
//!   then ack) and [`send_stream`] (write for a duration, shut down, read
//!   the ack);
//! - [`Tasks`]: spawned tasks aborted when dropped (a run's future is
//!   dropped on cancel and at its deadline; teardown step 7);
//! - live values: [`spawn_tick_goodput`] (responder → `tick` at 4 Hz),
//!   [`spawn_live_goodput`] (initiator-side receiver) and
//!   [`spawn_live_rate`] (sender-side `sent_mbps`);
//! - latency samples of the count-based workloads (`RttSample`, `LiveRtt`,
//!   `rtt_series`, …; crate-private);
//! - summary placeholders the hub fills after the run
//!   ([`cpu_per_mb_item`], [`loaded_rtt_item`]) and small formatting helpers.

use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tokio::time::Instant;
use truffle_core::transport::{RawIncoming, RawListener};

use crate::dataplane::{
    read_counting, read_header_for, read_stream_ack, write_for, write_stream_ack, SharedAccounting,
    StreamHeader, BUCKET_MS,
};
use crate::stats;
use crate::types::{Better, ResourceStats, Series, SummaryItem};
use crate::workloads::{LiveSamples, ResponderCtx};

/// Summary key the hub fills from resources (total CPU of both ends per MB of goodput).
pub const KEY_CPU_PER_MB: &str = "cpu_ms_per_mb";
/// Summary key the hub fills from the path sampler (p50 of `loaded_rtt_ms`).
pub const KEY_LOADED_RTT_P50: &str = "loaded_rtt_p50_ms";
/// Live Progress key of time-based runs: the path sampler's latest RTT
/// (latency under load, 2 Hz).
pub const KEY_LOADED_RTT: &str = "loaded_rtt_ms";
/// How long a just-accepted connection may take to send its TBN1 header.
pub const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// `cpu_ms_per_mb` with no value yet: the hub fills it (process plus sidecar
/// CPU of both ends ÷ goodput MB) once resources are known.
pub fn cpu_per_mb_item() -> SummaryItem {
    SummaryItem::new(KEY_CPU_PER_MB, "CPU per MB", None, "ms/MB", Better::Lower)
}

/// `loaded_rtt_p50_ms` with no value yet: the hub fills it from the 2 Hz
/// path sampler (latency under load).
pub fn loaded_rtt_item() -> SummaryItem {
    SummaryItem::new(
        KEY_LOADED_RTT_P50,
        "RTT p50 under load",
        None,
        "ms",
        Better::Lower,
    )
}

/// Fill the per-MB fields of `stats` from `goodput_bytes` when they are missing.
pub fn fill_per_mb(stats: &mut ResourceStats, goodput_bytes: Option<u64>) {
    let Some(mb) = goodput_bytes.filter(|b| *b > 0).map(|b| b as f64 / 1e6) else {
        return;
    };
    if stats.cpu_ms_per_mb.is_none() {
        stats.cpu_ms_per_mb = Some(stats.cpu_ms / mb);
    }
    if stats.sidecar_cpu_ms_per_mb.is_none() {
        stats.sidecar_cpu_ms_per_mb = stats.sidecar_cpu_ms.map(|c| c / mb);
    }
}

/// A duration as milliseconds.
pub fn dur_ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Take one of `limit` slots counted by `taken`: `true` and the count goes
/// up, or `false` when all are taken. A compare-exchange loop rather than
/// `fetch_update` (deprecated in Rust 1.99) or `try_update` (newer than the
/// workspace MSRV).
pub(crate) fn take_slot(taken: &AtomicU32, limit: u32) -> bool {
    let mut n = taken.load(Ordering::Relaxed);
    while n < limit {
        match taken.compare_exchange_weak(n, n + 1, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(now) => n = now,
        }
    }
    false
}

/// A series for `example_output()` fixtures: `values[i]` plotted at
/// `t0_ms + step_ms·(i+1)`, with points up to `warmup_ms` flagged as warmup.
pub fn example_series(
    key: &str,
    label: &str,
    unit: &str,
    t0_ms: f64,
    step_ms: f64,
    values: &[f64],
    warmup_ms: Option<f64>,
) -> Series {
    Series {
        key: key.to_string(),
        label: label.to_string(),
        unit: unit.to_string(),
        points: values
            .iter()
            .enumerate()
            .map(|(i, v)| (t0_ms + step_ms * (i + 1) as f64, *v))
            .collect(),
        warmup_ms,
    }
}

/// Read a just-opened stream's TBN1 header within [`HEADER_TIMEOUT`] and
/// require this run's tag (§6 "Data plane"). A stream whose header is
/// malformed, slow or carries another run's tag is counted as foreign
/// ([`ResponderCtx::foreign_dropped`]) and `None` is returned: drop it.
pub async fn read_tbn1<S: AsyncRead + Unpin>(
    r: &ResponderCtx,
    stream: &mut S,
) -> Option<StreamHeader> {
    read_tbn1_with(stream, r.run_tag, HEADER_TIMEOUT, &|| r.count_foreign()).await
}

async fn read_tbn1_with<S: AsyncRead + Unpin>(
    stream: &mut S,
    run_tag: u64,
    timeout: Duration,
    on_foreign: &(dyn Fn() + Send + Sync),
) -> Option<StreamHeader> {
    match tokio::time::timeout(timeout, read_header_for(stream, run_tag)).await {
        Ok(Ok(header)) => Some(header),
        _ => {
            on_foreign();
            None
        }
    }
}

/// Rule 5 for a raw connection (true: keep it).
type AdmitFn = Box<dyn Fn(&RawIncoming) -> bool + Send + Sync>;
/// Counts one foreign connection or bad header.
type ForeignFn = Arc<dyn Fn() + Send + Sync>;

/// The initiator's TBN1 streams on a `listen_tcp` listener (§6 rule 5 and
/// "Data plane"). Connections from anyone but the initiator are dropped and
/// counted; every other connection's header is read on its own task
/// ([`read_tbn1`]), so one that never sends its header (or sends it slowly)
/// cannot hold up the next.
pub struct Tbn1Acceptor {
    listener: RawListener,
    run_tag: u64,
    header_timeout: Duration,
    cancel: tokio_util::sync::CancellationToken,
    admit: AdmitFn,
    on_foreign: ForeignFn,
    open: bool,
    pending: JoinSet<Option<(TcpStream, StreamHeader)>>,
}

impl std::fmt::Debug for Tbn1Acceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tbn1Acceptor")
            .field("port", &self.listener.port)
            .field("open", &self.open)
            .field("pending", &self.pending.len())
            .finish()
    }
}

impl Tbn1Acceptor {
    /// Accept on `listener` for the run of `r`.
    pub fn new(r: &ResponderCtx, listener: RawListener) -> Self {
        let (admit_r, foreign_r) = (r.clone(), r.clone());
        Self::with(
            listener,
            r.run_tag,
            HEADER_TIMEOUT,
            r.cancel_token(),
            Box::new(move |incoming| admit_r.admit_tcp(incoming)),
            Arc::new(move || foreign_r.count_foreign()),
        )
    }

    fn with(
        listener: RawListener,
        run_tag: u64,
        header_timeout: Duration,
        cancel: tokio_util::sync::CancellationToken,
        admit: AdmitFn,
        on_foreign: ForeignFn,
    ) -> Self {
        Self {
            listener,
            run_tag,
            header_timeout,
            cancel,
            admit,
            on_foreign,
            open: true,
            pending: JoinSet::new(),
        }
    }

    /// The next stream whose header checked out, in the order headers
    /// complete. `None` once the run is cancelled, or the listener closed
    /// and every pending header was read.
    pub async fn next(&mut self) -> Option<(TcpStream, StreamHeader)> {
        let cancel = self.cancel.clone();
        loop {
            if !self.open && self.pending.is_empty() {
                return None;
            }
            tokio::select! {
                _ = cancel.cancelled() => return None,
                incoming = self.listener.accept(), if self.open => match incoming {
                    None => self.open = false,
                    Some(incoming) => {
                        if (self.admit)(&incoming) {
                            let (tag, timeout) = (self.run_tag, self.header_timeout);
                            let foreign = self.on_foreign.clone();
                            let mut stream = incoming.stream;
                            self.pending.spawn(async move {
                                read_tbn1_with(&mut stream, tag, timeout, &*foreign)
                                    .await
                                    .map(|h| (stream, h))
                            });
                        }
                    }
                },
                Some(done) = self.pending.join_next(), if !self.pending.is_empty() => {
                    if let Ok(Some(accepted)) = done {
                        return Some(accepted);
                    }
                }
            }
        }
    }
}

/// Spawned tasks, aborted when dropped: a run's future is dropped on cancel
/// and at its deadline, and a responder drops its tasks at teardown step 7.
#[derive(Debug)]
pub struct Tasks<T>(Vec<JoinHandle<T>>);

impl<T> Default for Tasks<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> From<Vec<JoinHandle<T>>> for Tasks<T> {
    fn from(handles: Vec<JoinHandle<T>>) -> Self {
        Self(handles)
    }
}

impl<T> Tasks<T> {
    /// No tasks yet.
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Add a task.
    pub fn push(&mut self, h: JoinHandle<T>) {
        self.0.push(h);
    }

    /// Await every task, in the order they were added.
    pub async fn join(&mut self) -> Vec<Result<T, JoinError>> {
        let mut out = Vec::with_capacity(self.0.len());
        for h in self.0.iter_mut() {
            out.push(h.await);
        }
        out
    }
}

impl<T> Drop for Tasks<T> {
    fn drop(&mut self) {
        for h in &self.0 {
            h.abort();
        }
    }
}

/// Receive one payload stream (the header already read): count every read
/// into `acc` under `stream_idx` until EOF, then write the per-stream ack
/// `[u64 BE bytes]` (§8.1). Returns the stream's payload bytes.
pub async fn receive_stream<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    stream_idx: u16,
    acc: &SharedAccounting,
) -> io::Result<u64> {
    let n = read_counting(stream, stream_idx, acc).await?;
    write_stream_ack(stream, n).await?;
    Ok(n)
}

/// What [`send_stream`] sent and what the receiver acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SentStream {
    /// Payload bytes written.
    pub sent: u64,
    /// The receiver's `[u64 BE stream_bytes]` ack.
    pub acked: u64,
}

/// Send one payload stream (any header already written): 64 KiB writes until
/// `until` (each added to `sent`), shut down the write half, then read the
/// receiver's ack.
pub async fn send_stream<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    until: Instant,
    sent: &AtomicU64,
) -> io::Result<SentStream> {
    let n = write_for(stream, until, sent).await?;
    stream.shutdown().await?;
    let acked = read_stream_ack(stream).await?;
    Ok(SentStream { sent: n, acked })
}

/// Mb/s from a byte delta over `secs`.
fn rate_mbps(bytes: u64, secs: f64) -> f64 {
    if secs <= 0.0 {
        0.0
    } else {
        bytes as f64 * 8.0 / secs / 1e6
    }
}

fn bytes_of(acc: &SharedAccounting) -> u64 {
    acc.lock()
        .unwrap_or_else(PoisonError::into_inner)
        .bytes_total()
}

/// Every 250 ms, send the receiver's live goodput (bytes counted in the last
/// interval) to the initiator as a `tick` under `key` (usually
/// `goodput_mbps`). Abort the handle at finish.
pub fn spawn_tick_goodput(
    r: ResponderCtx,
    acc: SharedAccounting,
    key: &'static str,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let period = Duration::from_millis(BUCKET_MS);
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = (Instant::now(), bytes_of(&acc));
        loop {
            tick.tick().await;
            if r.is_cancelled() {
                break;
            }
            let now = (Instant::now(), bytes_of(&acc));
            let mbps = rate_mbps(now.1 - last.1, (now.0 - last.0).as_secs_f64());
            last = now;
            if now.1 > 0 {
                r.tick([(key, mbps)]).await;
            }
        }
    })
}

/// Every 250 ms, set `key` to the live goodput of an initiator-side receiver.
pub fn spawn_live_goodput(
    live: LiveSamples,
    acc: SharedAccounting,
    key: &'static str,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let period = Duration::from_millis(BUCKET_MS);
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = (Instant::now(), bytes_of(&acc));
        loop {
            tick.tick().await;
            let now = (Instant::now(), bytes_of(&acc));
            if now.1 > 0 {
                live.set(
                    key,
                    rate_mbps(now.1 - last.1, (now.0 - last.0).as_secs_f64()),
                );
            }
            last = now;
        }
    })
}

/// Every 250 ms, set `key` (e.g. `sent_mbps`) from a sender's byte counter.
pub fn spawn_live_rate(
    live: LiveSamples,
    counter: Arc<AtomicU64>,
    key: &'static str,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let period = Duration::from_millis(BUCKET_MS);
        let mut tick = tokio::time::interval(period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last = (Instant::now(), counter.load(Ordering::Relaxed));
        loop {
            tick.tick().await;
            let now = (Instant::now(), counter.load(Ordering::Relaxed));
            live.set(
                key,
                rate_mbps(now.1 - last.1, (now.0 - last.0).as_secs_f64()),
            );
            last = now;
        }
    })
}

// ---------------------------------------------------------------------------
// Latency samples (quic, ws, synced-store; LiveRtt also tcp and udp)
// ---------------------------------------------------------------------------

/// At most this many raw samples (`samples_ms`) and series points per report.
pub(crate) const MAX_POINTS: usize = 5000;

/// One request/response sample of a count-based latency workload.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct RttSample {
    /// Local `now_ns()` just before the send call.
    pub(crate) send_ns: u64,
    /// Round trip, ms; `None` when it was lost or timed out.
    pub(crate) rtt_ms: Option<f64>,
    /// One of the first `warmup_count` samples (discarded from the stats).
    pub(crate) warmup: bool,
}

/// Keep at most `max` items, evenly strided, always keeping the first and
/// the last.
pub(crate) fn thin<T: Copy>(items: &[T], max: usize) -> Vec<T> {
    let n = items.len();
    if n <= max || max < 2 {
        return items.to_vec();
    }
    (0..max).map(|i| items[i * (n - 1) / (max - 1)]).collect()
}

/// Nearest-rank median of unsorted values (`sorted[⌈n/2⌉ − 1]`), for live
/// numbers; `None` when empty.
pub(crate) fn p50_of(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut v: Vec<f64> = values.to_vec();
    let k = v.len().div_ceil(2) - 1;
    let (_, m, _) = v.select_nth_unstable_by(k, f64::total_cmp);
    Some(*m)
}

/// Whether `count` measured samples can define a p99 (n ≥ 100, §7.4). Runs
/// of `ws` rr and `synced-store` pingpong with fewer samples leave the p99
/// summary item out instead of reporting it as null (the distribution in
/// `results` still has `p99: null`).
pub(crate) fn p99_defined(count: u32) -> bool {
    u64::from(count) >= stats::min_samples(stats::P99.0, stats::P99.1)
}

/// The measured samples (warmup excluded), each scaled; lost ones are `None`.
pub(crate) fn measured(samples: &[RttSample], scale: f64) -> Vec<Option<f64>> {
    samples
        .iter()
        .filter(|s| !s.warmup)
        .map(|s| s.rtt_ms.map(|v| v * scale))
        .collect()
}

/// The received measured values, scaled and thinned, for the CDF (`samples_ms`).
pub(crate) fn raw_ms(samples: &[RttSample], scale: f64) -> Vec<f64> {
    let v: Vec<f64> = measured(samples, scale).into_iter().flatten().collect();
    thin(&v, MAX_POINTS)
}

/// A latency series: one point per answered sample at its send time
/// (ms since `origin_ns`), scaled; warmup points are flagged with
/// `warmup_ms` (the last warmup send). Thinned to [`MAX_POINTS`].
pub(crate) fn rtt_series(
    samples: &[RttSample],
    origin_ns: u64,
    key: &str,
    label: &str,
    scale: f64,
) -> Series {
    let rel = |t: u64| (t as f64 - origin_ns as f64) / 1e6;
    let points: Vec<(f64, f64)> = samples
        .iter()
        .filter_map(|s| s.rtt_ms.map(|v| (rel(s.send_ns), v * scale)))
        .collect();
    let warmup_ms = samples
        .iter()
        .filter(|s| s.warmup)
        .map(|s| rel(s.send_ns))
        .reduce(f64::max);
    Series {
        key: key.to_string(),
        label: label.to_string(),
        unit: "ms".to_string(),
        points: thin(&points, MAX_POINTS),
        warmup_ms,
    }
}

/// The measurement window of measured samples: first send → last answer
/// (local `now_ns()`), when at least one was answered.
pub(crate) fn sample_window(samples: &[RttSample]) -> Option<(u64, u64)> {
    let first = samples
        .iter()
        .filter(|s| !s.warmup)
        .map(|s| s.send_ns)
        .min()?;
    let last = samples
        .iter()
        .filter(|s| !s.warmup)
        .filter_map(|s| s.rtt_ms.map(|r| s.send_ns + (r * 1e6) as u64))
        .max()?;
    Some((first, last.max(first)))
}

/// Live latency numbers: the median of the samples since the last
/// emission (the live series value) and the running nearest-rank p50 of
/// every measured sample.
#[derive(Debug)]
pub(crate) struct LiveRtt {
    recent: Vec<f64>,
    measured: Vec<f64>,
    last: Instant,
}

impl Default for LiveRtt {
    fn default() -> Self {
        Self::new()
    }
}

impl LiveRtt {
    /// Nothing seen yet.
    pub(crate) fn new() -> Self {
        Self {
            recent: Vec::new(),
            measured: Vec::new(),
            last: Instant::now(),
        }
    }

    /// One answered sample, ms (warmup samples count only for the interval).
    pub(crate) fn push(&mut self, ms: f64, warmup: bool) {
        self.recent.push(ms);
        if !warmup {
            self.measured.push(ms);
        }
    }

    /// `(median since the last take, running p50)`; starts a new interval.
    pub(crate) fn take(&mut self) -> (Option<f64>, Option<f64>) {
        let interval = stats::median(&self.recent);
        self.recent.clear();
        self.last = Instant::now();
        (interval, p50_of(&self.measured))
    }

    /// [`emit`](Self::emit) if 250 ms passed since the last interval began.
    pub(crate) fn emit_due(&mut self, live: &LiveSamples, key: &str, p50_key: &str) {
        if self.last.elapsed() >= Duration::from_millis(BUCKET_MS) {
            self.emit(live, key, p50_key);
        }
    }

    /// Set `key` (median since the last call) and `p50_key` (running p50).
    pub(crate) fn emit(&mut self, live: &LiveSamples, key: &str, p50_key: &str) {
        let (interval, p50) = self.take();
        if let Some(m) = interval {
            live.set(key, m);
        }
        if let Some(m) = p50 {
            live.set(p50_key, m);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataplane::{write_header, ReceiverAccounting, StreamKind};

    #[test]
    fn per_mb_is_filled_only_when_missing() {
        let mut s = ResourceStats {
            cpu_ms: 500.0,
            sidecar_cpu_ms: Some(250.0),
            ..Default::default()
        };
        fill_per_mb(&mut s, Some(100_000_000));
        assert_eq!(s.cpu_ms_per_mb, Some(5.0));
        assert_eq!(s.sidecar_cpu_ms_per_mb, Some(2.5));
        let mut t = ResourceStats {
            cpu_ms_per_mb: Some(1.0),
            ..Default::default()
        };
        fill_per_mb(&mut t, Some(1_000_000));
        assert_eq!(t.cpu_ms_per_mb, Some(1.0));
        fill_per_mb(&mut t, Some(0));
        assert_eq!(t.sidecar_cpu_ms_per_mb, None);
        assert_eq!(cpu_per_mb_item().value, None);
        assert_eq!(loaded_rtt_item().key, KEY_LOADED_RTT_P50);
    }

    #[tokio::test]
    async fn send_and_receive_streams_agree() {
        let (mut a, mut b) = tokio::io::duplex(256 * 1024);
        let acc = ReceiverAccounting::shared();
        let sent = AtomicU64::new(0);
        let header = StreamHeader {
            run_tag: 7,
            stream_idx: 3,
            kind: StreamKind::Upload,
        };
        let sender = async {
            write_header(&mut a, &header).await.unwrap();
            send_stream(&mut a, Instant::now() + Duration::from_millis(30), &sent)
                .await
                .unwrap()
        };
        let receiver = async {
            let h = read_header_for(&mut b, 7).await.unwrap();
            receive_stream(&mut b, h.stream_idx, &acc).await.unwrap()
        };
        let (out, got) = tokio::join!(sender, receiver);
        assert_eq!(out.sent, got);
        assert_eq!(out.acked, got);
        let st = acc.lock().unwrap().stats();
        st.check_invariants().unwrap();
        assert_eq!(st.stream_bytes(3), got);
        assert!((rate_mbps(1_000_000, 1.0) - 8.0).abs() < 1e-12);
        assert_eq!(rate_mbps(1, 0.0), 0.0);
    }

    #[test]
    fn helpers() {
        let v: Vec<u32> = (0..10_000).collect();
        let t = thin(&v, 5000);
        assert_eq!(t.len(), 5000);
        assert_eq!((t[0], t[4999]), (0, 9999));
        assert_eq!(thin(&v[..10], 5000).len(), 10);
        assert_eq!(p50_of(&[3.0, 1.0, 2.0, 4.0]), Some(2.0));
        assert_eq!(p50_of(&[5.0]), Some(5.0));
        assert_eq!(p50_of(&[]), None);
        let s = [
            RttSample {
                send_ns: 2_000_000,
                rtt_ms: Some(4.0),
                warmup: true,
            },
            RttSample {
                send_ns: 3_000_000,
                rtt_ms: Some(2.0),
                warmup: false,
            },
            RttSample {
                send_ns: 4_000_000,
                rtt_ms: None,
                warmup: false,
            },
        ];
        let series = rtt_series(&s, 1_000_000, "convergence_ms", "x", 0.5);
        assert_eq!(series.points, vec![(1.0, 2.0), (2.0, 1.0)]);
        assert_eq!(series.warmup_ms, Some(1.0));
        assert_eq!(measured(&s, 1.0), vec![Some(2.0), None]);
        assert_eq!(sample_window(&s), Some((3_000_000, 5_000_000)));
    }

    #[test]
    fn live_rtt_reports_interval_median_and_running_p50() {
        let mut l = LiveRtt::default();
        assert_eq!(l.take(), (None, None));
        for v in [1.0, 3.0, 2.0] {
            l.push(v, false);
        }
        assert_eq!(l.take(), (Some(2.0), Some(2.0)));
        l.push(10.0, false);
        let (interval, p50) = l.take();
        assert_eq!(interval, Some(10.0));
        assert_eq!(p50, Some(2.0), "nearest rank of [1,2,3,10]");
    }

    #[tokio::test]
    async fn a_silent_connection_does_not_hold_up_the_next() {
        use std::sync::atomic::AtomicU32;
        use tokio::net::TcpListener;
        let server = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = server.local_addr().unwrap();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let foreign = Arc::new(AtomicU32::new(0));
        let f2 = foreign.clone();
        let cancel = tokio_util::sync::CancellationToken::new();
        let mut acceptor = Tbn1Acceptor::with(
            RawListener::new(rx, 9),
            7,
            Duration::from_millis(400),
            cancel.clone(),
            Box::new(|i: &RawIncoming| i.remote_addr != "198.51.100.9:1"),
            Arc::new(move || {
                f2.fetch_add(1, Ordering::Relaxed);
            }),
        );
        let incoming = |stream: TcpStream, remote: &str| RawIncoming {
            stream,
            remote_addr: remote.to_string(),
            remote_identity: None,
        };
        // A silent connection, a foreign one, one with another run's tag,
        // then a good one.
        let silent = TcpStream::connect(addr).await.unwrap();
        let (s1, _) = server.accept().await.unwrap();
        tx.send(incoming(s1, "100.64.0.2:1")).await.unwrap();
        let _foreign_client = TcpStream::connect(addr).await.unwrap();
        let (s2, _) = server.accept().await.unwrap();
        tx.send(incoming(s2, "198.51.100.9:1")).await.unwrap();
        let mut wrong = TcpStream::connect(addr).await.unwrap();
        let (s3, _) = server.accept().await.unwrap();
        tx.send(incoming(s3, "100.64.0.2:2")).await.unwrap();
        let header = |run_tag| StreamHeader {
            run_tag,
            stream_idx: 3,
            kind: crate::dataplane::StreamKind::Upload,
        };
        crate::dataplane::write_header(&mut wrong, &header(8))
            .await
            .unwrap();
        let mut good = TcpStream::connect(addr).await.unwrap();
        let (s4, _) = server.accept().await.unwrap();
        tx.send(incoming(s4, "100.64.0.2:3")).await.unwrap();
        crate::dataplane::write_header(&mut good, &header(7))
            .await
            .unwrap();
        let t = Instant::now();
        let (_, h) = acceptor.next().await.unwrap();
        assert_eq!(h, header(7));
        assert!(
            t.elapsed() < Duration::from_millis(300),
            "{:?}",
            t.elapsed()
        );
        // The wrong tag is refused at once; the silent one times out.
        drop(tx);
        assert!(acceptor.next().await.is_none());
        assert_eq!(foreign.load(Ordering::Relaxed), 2);
        drop(silent);
    }
}
