//! Path honesty (RFC 027 §6 flow, §7.4 `path_label`, §8.3): settle the path
//! before a run, snapshot it, sample it at 2 Hz during time-based runs, and
//! label which path carried the bytes.
//!
//! Every ping is `Node::ping` (a disco ping) wrapped in a 5 s timeout; a
//! timeout counts as lost. The generic `*_with` functions take any ping
//! closure so they can be tested without a tailnet.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use truffle_core::network::{NetworkProvider, PingResult};
use truffle_core::Node;

use crate::clock::{now_ns, now_rfc3339};
use crate::types::{Distribution, PathBytes, PathLabel, PathSnapshot, PathTimelinePoint};

/// Settling gives up starting new pings after this.
pub const SETTLE_MAX: Duration = Duration::from_secs(3);
/// Per-ping timeout.
pub const PING_TIMEOUT: Duration = Duration::from_secs(5);
/// Path sampler period (2 Hz).
pub const SAMPLER_INTERVAL: Duration = Duration::from_millis(500);
/// Pause after a failed ping while settling.
const SETTLE_RETRY: Duration = Duration::from_millis(100);
/// Below this many usermetrics bytes (both sides, all paths) `path_label`
/// trusts the ping samples instead. A run this small (a path run, a short
/// latency run) moves mostly disco pings, keepalives and control traffic,
/// and a few hundred DERP bytes of that would otherwise label a direct path
/// `relay` or `mixed`.
pub const PATH_BYTES_MIN: u64 = 256 * 1024;

/// One ping's result: the core result, or why it failed.
pub type PingOutcome = Result<PingResult, String>;

/// Ping `peer` (a tailscale id) once, with [`PING_TIMEOUT`].
pub async fn ping_once<N: NetworkProvider + 'static>(node: &Node<N>, peer: &str) -> PingOutcome {
    match tokio::time::timeout(PING_TIMEOUT, node.ping(peer)).await {
        Ok(Ok(r)) => Ok(r),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!("ping timed out after {} s", PING_TIMEOUT.as_secs())),
    }
}

/// A [`PathSnapshot`] stamped now.
pub fn snapshot_from(outcome: &PingOutcome) -> PathSnapshot {
    match outcome {
        Ok(r) => PathSnapshot {
            at: now_rfc3339(),
            connection: Some(r.connection.clone()),
            rtt_ms: Some(r.latency.as_secs_f64() * 1000.0),
            endpoint: r.peer_addr.clone(),
            error: None,
        },
        Err(e) => PathSnapshot {
            at: now_rfc3339(),
            connection: None,
            rtt_ms: None,
            endpoint: None,
            error: Some(e.clone()),
        },
    }
}

/// One ping as a snapshot (the `after` snapshot, probes).
pub async fn snapshot<N: NetworkProvider + 'static>(node: &Node<N>, peer: &str) -> PathSnapshot {
    snapshot_from(&ping_once(node, peer).await)
}

/// Result of [`settle`].
#[derive(Debug, Clone, PartialEq)]
pub struct Settled {
    /// Time from the first ping until settled (or until giving up).
    pub settle_ms: u64,
    /// The last ping: the run's `before` snapshot.
    pub snapshot: PathSnapshot,
    /// Pings sent.
    pub pings: u32,
    /// Two consecutive pings agreed on the connection.
    pub settled: bool,
}

/// Settle the path to `peer` (§6 flow step 4): ping until two consecutive
/// pings report the same connection. No new ping starts after 3 s; each ping
/// may take up to 5 s.
pub async fn settle<N: NetworkProvider + 'static>(node: &Node<N>, peer: &str) -> Settled {
    settle_with(|| ping_once(node, peer)).await
}

/// [`settle`] over any ping function.
pub async fn settle_with<F, Fut>(mut ping: F) -> Settled
where
    F: FnMut() -> Fut,
    Fut: Future<Output = PingOutcome>,
{
    let start = Instant::now();
    let mut prev: Option<String> = None;
    let mut pings = 0u32;
    let mut last: PingOutcome = Err("no ping was sent".to_string());
    let mut settled = false;
    loop {
        if pings > 0 && start.elapsed() >= SETTLE_MAX {
            break;
        }
        last = ping().await;
        pings += 1;
        match &last {
            Ok(r) => {
                if prev.as_deref() == Some(r.connection.as_str()) {
                    settled = true;
                    break;
                }
                prev = Some(r.connection.clone());
            }
            Err(_) => {
                prev = None;
                tokio::time::sleep(SETTLE_RETRY).await;
            }
        }
    }
    Settled {
        settle_ms: start.elapsed().as_millis() as u64,
        snapshot: snapshot_from(&last),
        pings,
        settled,
    }
}

/// Coarse class of a connection label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PathClass {
    /// `direct`.
    Direct,
    /// `relay:<region>` (DERP).
    Derp,
    /// `peer-relay:…`.
    PeerRelay,
    /// `unknown`, empty, or anything else.
    Unknown,
}

impl PathClass {
    /// DERP or peer relay.
    pub fn is_relayed(self) -> bool {
        matches!(self, Self::Derp | Self::PeerRelay)
    }
}

/// Classify a connection label from `Node::ping` / `Peer.connection_type`.
pub fn classify(connection: &str) -> PathClass {
    if connection == "direct" {
        PathClass::Direct
    } else if connection.starts_with("peer-relay") {
        PathClass::PeerRelay
    } else if connection.starts_with("relay") {
        PathClass::Derp
    } else {
        PathClass::Unknown
    }
}

/// What the path sampler saw.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct PathTimeline {
    /// One point per sample.
    pub points: Vec<PathTimelinePoint>,
    /// RTT under load; lost pings count as lost. `None` without samples.
    pub loaded_rtt_ms: Option<Distribution>,
}

impl PathTimeline {
    /// Build from points.
    pub fn from_points(points: Vec<PathTimelinePoint>) -> Self {
        let rtts: Vec<Option<f64>> = points.iter().map(|p| p.rtt_ms).collect();
        let loaded_rtt_ms = (!rtts.is_empty()).then(|| Distribution::from_samples(&rtts));
        Self {
            points,
            loaded_rtt_ms,
        }
    }
}

/// Called with each sampler point as it is taken (the hub publishes its RTT
/// live as `loaded_rtt_ms`).
pub type OnPoint = Box<dyn Fn(&PathTimelinePoint) + Send + 'static>;

/// A 2 Hz path sampler running for a time-based run.
pub struct PathSampler {
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Vec<PathTimelinePoint>>,
}

impl PathSampler {
    /// Sample `peer` (a tailscale id) on `node`; `t_ms` is relative to
    /// `origin_ns`; `on_point` sees every point as it is taken.
    pub fn for_node<N: NetworkProvider + 'static>(
        node: Arc<Node<N>>,
        peer: String,
        origin_ns: u64,
        on_point: OnPoint,
    ) -> Self {
        Self::start_with(
            origin_ns,
            move || {
                let node = node.clone();
                let peer = peer.clone();
                async move { ping_once(&node, &peer).await }
            },
            on_point,
        )
    }

    /// Sample with any ping function (each call is also bounded by
    /// [`PING_TIMEOUT`]). Pings never overlap; a slow ping delays the next.
    pub fn start<F, Fut>(origin_ns: u64, ping: F) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = PingOutcome> + Send + 'static,
    {
        Self::start_with(origin_ns, ping, Box::new(|_| {}))
    }

    /// [`start`](Self::start), calling `on_point` with every point as it is taken.
    pub fn start_with<F, Fut>(origin_ns: u64, mut ping: F, on_point: OnPoint) -> Self
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = PingOutcome> + Send + 'static,
    {
        let (stop_tx, mut stop_rx) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let mut points = Vec::new();
            let mut tick = tokio::time::interval(SAMPLER_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stop_rx => break,
                    _ = tick.tick() => {}
                }
                let t = now_ns();
                let outcome = tokio::select! {
                    biased;
                    _ = &mut stop_rx => break,
                    r = tokio::time::timeout(PING_TIMEOUT, ping()) => r
                        .unwrap_or_else(|_| Err("ping timed out".to_string())),
                };
                let point = PathTimelinePoint {
                    t_ms: t.saturating_sub(origin_ns) / 1_000_000,
                    connection: outcome.as_ref().ok().map(|r| r.connection.clone()),
                    rtt_ms: outcome
                        .as_ref()
                        .ok()
                        .map(|r| r.latency.as_secs_f64() * 1000.0),
                };
                // A callback that panics must not lose the timeline.
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| on_point(&point)));
                points.push(point);
            }
            points
        });
        Self {
            stop: Some(stop_tx),
            task,
        }
    }

    /// Stop (an in-flight ping is abandoned) and return the timeline.
    pub async fn stop(mut self) -> PathTimeline {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let points = (&mut self.task).await.unwrap_or_default();
        PathTimeline::from_points(points)
    }
}

impl Drop for PathSampler {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The verdict on which path carried a run.
#[derive(Debug, Clone, PartialEq)]
pub struct PathVerdict {
    /// The label.
    pub label: PathLabel,
    /// DERP bytes ÷ all bytes, from usermetrics.
    pub derp_share: Option<f64>,
    /// Notes for the report (path transitions, fallbacks).
    pub notes: Vec<String>,
}

/// Label the path (§7.4):
///
/// - with at least [`PATH_BYTES_MIN`] usermetrics bytes: `direct` iff the
///   relayed share (DERP plus peer relay) is under 1 % and the samples show
///   no transition, `relay` iff it is over 99 %, otherwise `mixed` with a
///   note;
/// - without them, or with fewer bytes than that (a tiny run's bytes are
///   mostly pings and control traffic, not the run's data): from the samples
///   (`before`, the timeline, `after`), or `unknown` when none of them named
///   a path, with a note saying why.
///
/// `derp_share` is DERP bytes only (and `None` when the label comes from the
/// samples); peer-relay bytes count as relayed for the label.
pub fn path_label(
    path_bytes: Option<&PathBytes>,
    timeline: &[PathTimelinePoint],
    before: &PathSnapshot,
    after: &PathSnapshot,
) -> PathVerdict {
    // Known path samples in time order, with a when-label for notes.
    let mut seq: Vec<(String, String, PathClass)> = Vec::new();
    let mut push = |when: String, conn: &Option<String>| {
        if let Some(c) = conn {
            let class = classify(c);
            if class != PathClass::Unknown {
                seq.push((when, c.clone(), class));
            }
        }
    };
    push("before".into(), &before.connection);
    for p in timeline {
        push(format!("{:.1} s", p.t_ms as f64 / 1000.0), &p.connection);
    }
    push("after".into(), &after.connection);

    let transitions: Vec<String> = seq
        .windows(2)
        .filter(|w| w[0].2 != w[1].2)
        .map(|w| format!("{} → {} ({})", w[0].1, w[1].1, w[1].0))
        .collect();
    let transition_note = (!transitions.is_empty())
        .then(|| format!("path changed during the run: {}", transitions.join(", ")));

    let bytes = path_bytes.filter(|b| b.total() >= PATH_BYTES_MIN);
    if let Some(b) = bytes {
        let total = b.total() as f64;
        let derp_share = b.derp as f64 / total;
        let relayed = (b.derp + b.peer_relay) as f64 / total;
        let mut notes = Vec::new();
        let label = if relayed < 0.01 && transitions.is_empty() {
            PathLabel::Direct
        } else if relayed > 0.99 {
            PathLabel::Relay
        } else {
            notes.push(format!(
                "mixed path: {:.1} % of the bytes were relayed",
                relayed * 100.0
            ));
            PathLabel::Mixed
        };
        if label != PathLabel::Direct {
            notes.extend(transition_note);
        }
        return PathVerdict {
            label,
            derp_share: Some(derp_share),
            notes,
        };
    }

    let mut notes = Vec::new();
    let label = if seq.is_empty() {
        PathLabel::Unknown
    } else if seq.iter().all(|s| s.2 == PathClass::Direct) {
        PathLabel::Direct
    } else if seq.iter().all(|s| s.2.is_relayed()) {
        PathLabel::Relay
    } else {
        PathLabel::Mixed
    };
    if label != PathLabel::Unknown {
        notes.push(match path_bytes.map(PathBytes::total) {
            // Disco pings and loopback-only runs move no counted bytes.
            Some(0) => "path label from ping samples; usermetrics counted no bytes".to_string(),
            Some(n) => format!(
                "path label from ping samples; usermetrics counted only {n} bytes, too few to \
                 tell the run's path"
            ),
            None => "path label from ping samples; usermetrics were unavailable".to_string(),
        });
    }
    notes.extend(transition_note);
    PathVerdict {
        label,
        derp_share: None,
        notes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn ok(conn: &str, ms: u64) -> PingOutcome {
        Ok(PingResult {
            latency: Duration::from_millis(ms),
            connection: conn.to_string(),
            peer_addr: (conn == "direct").then(|| "192.0.2.10:41641".to_string()),
        })
    }

    fn snap(conn: Option<&str>) -> PathSnapshot {
        PathSnapshot {
            at: "2026-10-16T00:00:00.000Z".into(),
            connection: conn.map(str::to_string),
            rtt_ms: conn.map(|_| 1.0),
            endpoint: None,
            error: None,
        }
    }

    fn point(t_ms: u64, conn: Option<&str>) -> PathTimelinePoint {
        PathTimelinePoint {
            t_ms,
            connection: conn.map(str::to_string),
            rtt_ms: conn.map(|_| 2.0),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn settles_on_two_equal_pings() {
        let seq = ["relay:sfo", "direct", "direct", "direct"];
        let i = AtomicU32::new(0);
        let s = settle_with(|| {
            let n = i.fetch_add(1, Ordering::Relaxed) as usize;
            async move { ok(seq[n], 5) }
        })
        .await;
        assert!(s.settled);
        assert_eq!(s.pings, 3);
        assert_eq!(s.snapshot.connection.as_deref(), Some("direct"));
        assert_eq!(s.snapshot.endpoint.as_deref(), Some("192.0.2.10:41641"));
        assert_eq!(s.snapshot.rtt_ms, Some(5.0));
    }

    #[tokio::test(start_paused = true)]
    async fn gives_up_after_three_seconds() {
        let s =
            settle_with(|| async { Err::<PingResult, _>("peer unreachable".to_string()) }).await;
        assert!(!s.settled);
        assert!(s.settle_ms >= 3000 && s.settle_ms < 3200, "{}", s.settle_ms);
        assert_eq!(s.snapshot.error.as_deref(), Some("peer unreachable"));
        assert_eq!(s.snapshot.connection, None);
        // Flapping paths never settle either.
        let i = AtomicU32::new(0);
        let s = settle_with(|| {
            let n = i.fetch_add(1, Ordering::Relaxed);
            async move {
                tokio::time::sleep(Duration::from_millis(400)).await;
                ok(
                    if n.is_multiple_of(2) {
                        "direct"
                    } else {
                        "relay:sfo"
                    },
                    1,
                )
            }
        })
        .await;
        assert!(!s.settled);
        assert!(s.pings >= 8);
    }

    #[tokio::test(start_paused = true)]
    async fn sampler_records_two_points_per_second() {
        let i = AtomicU32::new(0);
        let i = Arc::new(i);
        let origin = now_ns();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let on_point: OnPoint = Box::new(move |p| seen2.lock().unwrap().push(p.rtt_ms));
        let sampler = PathSampler::start_with(
            origin,
            move || {
                let n = i.fetch_add(1, Ordering::Relaxed);
                async move {
                    if n == 3 {
                        Err("lost".to_string())
                    } else {
                        ok("direct", 3)
                    }
                }
            },
            on_point,
        );
        tokio::time::sleep(Duration::from_millis(2_100)).await;
        let tl = sampler.stop().await;
        assert_eq!(tl.points.len(), 5, "{:?}", tl.points);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![Some(3.0), Some(3.0), Some(3.0), None, Some(3.0)]
        );
        assert_eq!(tl.points[3].connection, None);
        let d = tl.loaded_rtt_ms.unwrap();
        assert_eq!((d.samples, d.lost), (5, 1));
        assert_eq!(d.p50, Some(3.0));
    }

    #[test]
    fn classification() {
        assert_eq!(classify("direct"), PathClass::Direct);
        assert_eq!(classify("relay:sfo"), PathClass::Derp);
        assert_eq!(classify("peer-relay:192.0.2.1:7"), PathClass::PeerRelay);
        assert_eq!(classify("unknown"), PathClass::Unknown);
        assert_eq!(classify(""), PathClass::Unknown);
    }

    #[test]
    fn labels_from_usermetrics() {
        let direct = snap(Some("direct"));
        let pb = |direct, derp, peer_relay| PathBytes {
            direct,
            derp,
            peer_relay,
        };
        let v = path_label(Some(&pb(1_000_000, 5_000, 0)), &[], &direct, &direct);
        assert_eq!(v.label, PathLabel::Direct);
        assert!((v.derp_share.unwrap() - 5_000.0 / 1_005_000.0).abs() < 1e-12);
        assert!(v.notes.is_empty());
        let v = path_label(Some(&pb(1_000, 999_000, 0)), &[], &direct, &direct);
        assert_eq!(v.label, PathLabel::Relay);
        // Peer-relay bytes count as relayed even though derp_share is 0.
        let v = path_label(Some(&pb(0, 0, 1_000_000)), &[], &direct, &direct);
        assert_eq!(v.label, PathLabel::Relay);
        assert_eq!(v.derp_share, Some(0.0));
        let v = path_label(Some(&pb(500_000, 500_000, 0)), &[], &direct, &direct);
        assert_eq!(v.label, PathLabel::Mixed);
        assert!(v.notes[0].contains("50.0 %"));
        // A transition makes an all-direct byte count mixed.
        let tl = [
            point(500, Some("direct")),
            point(1000, Some("relay:sfo")),
            point(1500, Some("direct")),
        ];
        let v = path_label(Some(&pb(1_000_000, 0, 0)), &tl, &direct, &direct);
        assert_eq!(v.label, PathLabel::Mixed);
        assert!(
            v.notes
                .iter()
                .any(|n| n.contains("direct → relay:sfo (1.0 s)")),
            "{:?}",
            v.notes
        );
    }

    #[test]
    fn labels_without_usermetrics() {
        let tl = [
            point(500, Some("direct")),
            point(1000, None),
            point(1500, Some("direct")),
        ];
        let v = path_label(None, &tl, &snap(Some("direct")), &snap(None));
        assert_eq!(v.label, PathLabel::Direct);
        assert_eq!(v.derp_share, None);
        let v = path_label(
            None,
            &[],
            &snap(Some("relay:sfo")),
            &snap(Some("relay:nyc")),
        );
        assert_eq!(v.label, PathLabel::Relay);
        let v = path_label(None, &[], &snap(Some("relay:sfo")), &snap(Some("direct")));
        assert_eq!(v.label, PathLabel::Mixed);
        assert!(v
            .notes
            .iter()
            .any(|n| n.contains("relay:sfo → direct (after)")));
        let v = path_label(None, &[], &snap(None), &snap(Some("unknown")));
        assert_eq!(v.label, PathLabel::Unknown);
        // Zero bytes counts as no usermetrics.
        let v = path_label(
            Some(&PathBytes::default()),
            &[],
            &snap(Some("direct")),
            &snap(Some("direct")),
        );
        assert_eq!(v.label, PathLabel::Direct);
        assert_eq!(v.derp_share, None);
        assert!(v.notes[0].contains("counted no bytes"), "{:?}", v.notes);
        // A tiny run (148 B of DERP keepalives, pings said direct) is labelled
        // from the pings, not `relay`.
        let tiny = PathBytes {
            direct: 0,
            derp: 148,
            peer_relay: 0,
        };
        let v = path_label(
            Some(&tiny),
            &[],
            &snap(Some("direct")),
            &snap(Some("direct")),
        );
        assert_eq!(v.label, PathLabel::Direct);
        assert_eq!(v.derp_share, None);
        assert!(v.notes[0].contains("only 148 bytes"), "{:?}", v.notes);
        // Just over the threshold, the bytes decide.
        let big = PathBytes {
            direct: 0,
            derp: PATH_BYTES_MIN,
            peer_relay: 0,
        };
        let v = path_label(
            Some(&big),
            &[],
            &snap(Some("direct")),
            &snap(Some("direct")),
        );
        assert_eq!(v.label, PathLabel::Relay);
    }
}
