//! `path` (RFC 027 §8.2): ping RTT, loss and connection per sample, plus the
//! hub's lifecycle timings. Initiator only; nothing is prepared on the peer.
//!
//! - `count` pings `interval_ms` apart with `node.ping` (a disco ping), each
//!   wrapped in a 5 s timeout; a timeout counts as lost. Pings never overlap.
//! - Sample 0 is reported as `first_rtt_ms` and excluded from the
//!   distribution and the loss count.
//! - Every sample records its `connection`; consecutive known connections
//!   that differ are `transitions`. The samples also go into the report's
//!   `path_timeline`, so `path_label` sees them.
//! - `hub_start_ms` (hub start → Running), `discovery_ms` (Running → this
//!   peer first listed) and `ws_connect_ms` (first `hello` while the peer
//!   was not ws_connected → `hello.ack`) come from the hub.
//!
//! Summary keys: `rtt_min_ms`, `rtt_p50_ms`, `rtt_max_ms`, `loss_pct`.
//! Series: `rtt_ms` (sample 0 flagged as warmup).

use std::collections::BTreeMap;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::time::MissedTickBehavior;

use crate::clock::now_ns;
use crate::error::AppError;
use crate::params::PathParams;
use crate::pathwatch::ping_once;
use crate::stats;
use crate::types::{Better, Distribution, PathTimelinePoint, Phase, Series, SummaryItem};
use crate::workloads::{RunCtx, WorkloadOutput};

/// `path` is implemented.
pub const IMPLEMENTED: bool = true;

/// One ping.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "PathPingSample")]
pub struct PingSample {
    /// Index from 0.
    pub i: u32,
    /// When it was sent, ms since the run started.
    pub t_ms: f64,
    /// RTT; `None` when lost.
    pub rtt_ms: Option<f64>,
    /// `direct`, `relay:<region>`, `peer-relay:…`; `None` when lost.
    pub connection: Option<String>,
    /// Direct endpoint, if any.
    pub endpoint: Option<String>,
    /// Why it was lost.
    pub error: Option<String>,
}

/// A change of connection between consecutive answered pings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "PathTransition")]
pub struct Transition {
    /// Index of the first ping on the new connection.
    pub i: u32,
    /// Its send time, ms since the run started.
    pub t_ms: u64,
    /// Previous connection.
    pub from: String,
    /// New connection.
    pub to: String,
}

/// `results` of a path run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "PathResults")]
pub struct Results {
    /// Every ping, in order.
    pub samples: Vec<PingSample>,
    /// RTT of sample 0 (first contact after settling).
    pub first_rtt_ms: Option<f64>,
    /// RTT distribution of samples 1… (lost ones ranked as +∞).
    pub rtt_ms: Distribution,
    /// Pings sent.
    pub sent: u32,
    /// Pings 1… that were lost.
    pub lost: u32,
    /// `lost ÷ (sent − 1) × 100`.
    pub loss_pct: Option<f64>,
    /// Connection changes.
    pub transitions: Vec<Transition>,
    /// Answered pings per connection label.
    pub connections: BTreeMap<String, u32>,
    /// Hub start → Running, ms.
    pub hub_start_ms: Option<u64>,
    /// Running → this peer first listed, ms.
    pub discovery_ms: Option<u64>,
    /// First `hello` while the peer was not ws_connected → `hello.ack`, ms.
    pub ws_connect_ms: Option<u64>,
}

/// Turn the pings into results, summary and series.
fn assemble(samples: Vec<PingSample>, timings: crate::workloads::PeerTimings) -> WorkloadOutput {
    let sent = samples.len() as u32;
    let first_rtt_ms = samples.first().and_then(|s| s.rtt_ms);
    let rest: Vec<Option<f64>> = samples.iter().skip(1).map(|s| s.rtt_ms).collect();
    let rtt = Distribution::from_samples(&rest);
    let loss_pct = stats::pct(f64::from(rtt.lost), f64::from(rtt.samples));
    let mut transitions = Vec::new();
    let mut prev: Option<&str> = None;
    let mut connections: BTreeMap<String, u32> = BTreeMap::new();
    for s in &samples {
        if let Some(c) = s.connection.as_deref() {
            *connections.entry(c.to_string()).or_default() += 1;
            if let Some(p) = prev.filter(|p| *p != c) {
                transitions.push(Transition {
                    i: s.i,
                    t_ms: s.t_ms.max(0.0).round() as u64,
                    from: p.to_string(),
                    to: c.to_string(),
                });
            }
            prev = Some(c);
        }
    }
    let summary = vec![
        SummaryItem::new("rtt_min_ms", "RTT min", rtt.min, "ms", Better::Lower),
        SummaryItem::new("rtt_p50_ms", "RTT p50", rtt.p50, "ms", Better::Lower),
        SummaryItem::new("rtt_max_ms", "RTT max", rtt.max, "ms", Better::Lower),
        SummaryItem::new("loss_pct", "Loss", loss_pct, "%", Better::Lower),
    ];
    let series = vec![Series {
        key: "rtt_ms".into(),
        label: "RTT".into(),
        unit: "ms".into(),
        points: samples
            .iter()
            .filter_map(|s| s.rtt_ms.map(|r| (s.t_ms, r)))
            .collect(),
        warmup_ms: samples.first().map(|s| s.t_ms),
    }];
    let mut notes = Vec::new();
    if !transitions.is_empty() {
        notes.push(format!(
            "the connection changed {} time{} during the pings",
            transitions.len(),
            if transitions.len() == 1 { "" } else { "s" }
        ));
    }
    let results = Results {
        samples,
        first_rtt_ms,
        rtt_ms: rtt,
        sent,
        lost: rtt.lost,
        loss_pct,
        transitions,
        connections,
        hub_start_ms: timings.hub_start_ms,
        discovery_ms: timings.discovery_ms,
        ws_connect_ms: timings.ws_connect_ms,
    };
    WorkloadOutput {
        summary,
        results: serde_json::to_value(&results).unwrap_or_default(),
        series,
        notes,
        window_ns: None,
        goodput_bytes: None,
    }
}

/// Initiator: ping the peer `count` times.
pub async fn run(ctx: &mut RunCtx<'_>, p: &PathParams) -> Result<WorkloadOutput, AppError> {
    ctx.phase(Phase::Running);
    let cancel = ctx.cancel_token();
    let mut tick = tokio::time::interval(Duration::from_millis(p.interval_ms));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut samples = Vec::with_capacity(p.count as usize);
    let window_start = now_ns();
    for i in 0..p.count {
        tokio::select! {
            _ = cancel.cancelled() => return Err(AppError::cancelled()),
            _ = tick.tick() => {}
        }
        let sent_ns = now_ns();
        let outcome = tokio::select! {
            _ = cancel.cancelled() => return Err(AppError::cancelled()),
            o = ping_once(&ctx.node, &ctx.peer.tailscale_id) => o,
        };
        let t_ms = ctx.rel_ms(sent_ns);
        let sample = match &outcome {
            Ok(r) => PingSample {
                i,
                t_ms,
                rtt_ms: Some(r.latency.as_secs_f64() * 1000.0),
                connection: Some(r.connection.clone()),
                endpoint: r.peer_addr.clone(),
                error: None,
            },
            Err(e) => PingSample {
                i,
                t_ms,
                rtt_ms: None,
                connection: None,
                endpoint: None,
                error: Some(e.clone()),
            },
        };
        ctx.add_timeline_point(PathTimelinePoint {
            t_ms: t_ms.max(0.0) as u64,
            connection: sample.connection.clone(),
            rtt_ms: sample.rtt_ms,
        });
        if let Some(rtt) = sample.rtt_ms {
            ctx.sample("rtt_ms", rtt);
        }
        ctx.fraction((i + 1) as f32 / p.count as f32);
        samples.push(sample);
    }
    let window_end = now_ns();
    let mut out = assemble(samples, ctx.timings());
    if out
        .summary
        .iter()
        .all(|s| s.value.is_none() || s.key == "loss_pct")
    {
        return Err(AppError::timeout(format!(
            "none of the {} pings after the first was answered",
            p.count.saturating_sub(1)
        )));
    }
    out.window_ns = Some((window_start, window_end));
    Ok(out)
}

/// A canonical example (20 pings on a direct LAN path).
pub fn example_output() -> WorkloadOutput {
    let rtts = [
        3.41, 1.22, 1.18, 1.31, 1.19, 1.25, 1.42, 1.2, 1.17, 1.28, 1.33, 1.21, 1.19, 1.24, 1.5,
        1.23, 1.2, 1.26, 1.18, 1.29,
    ];
    let samples = rtts
        .iter()
        .enumerate()
        .map(|(i, &r)| PingSample {
            i: i as u32,
            t_ms: 420.0 + 250.0 * i as f64,
            rtt_ms: (i != 7).then_some(r),
            connection: (i != 7).then(|| "direct".to_string()),
            endpoint: (i != 7).then(|| "192.0.2.10:41641".to_string()),
            error: (i == 7).then(|| "ping timed out after 5 s".to_string()),
        })
        .collect();
    let mut out = assemble(
        samples,
        crate::workloads::PeerTimings {
            hub_start_ms: Some(2_310),
            discovery_ms: Some(140),
            ws_connect_ms: Some(85),
        },
    );
    out.window_ns = Some((1_760_000_000_420_000_000, 1_760_000_005_190_000_000));
    out
}

/// The path mode's example for the schema fixtures.
pub fn examples() -> Vec<super::ModeExample> {
    vec![super::ModeExample {
        mode: "path",
        params: crate::params::WorkloadParams::Path(PathParams::default()),
        output: example_output(),
        clock: false,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_sample_is_excluded_and_transitions_counted() {
        let mk = |i: u32, rtt: Option<f64>, conn: Option<&str>| PingSample {
            i,
            t_ms: f64::from(i) * 250.0,
            rtt_ms: rtt,
            connection: conn.map(str::to_string),
            endpoint: None,
            error: rtt.is_none().then(|| "lost".to_string()),
        };
        let samples = vec![
            mk(0, Some(40.0), Some("relay:sfo")),
            mk(1, Some(2.0), Some("direct")),
            mk(2, None, None),
            mk(3, Some(4.0), Some("direct")),
            mk(4, Some(30.0), Some("relay:sfo")),
        ];
        let out = assemble(samples, Default::default());
        let r: Results = serde_json::from_value(out.results).unwrap();
        assert_eq!(r.first_rtt_ms, Some(40.0));
        assert_eq!(r.sent, 5);
        assert_eq!((r.rtt_ms.samples, r.rtt_ms.lost), (4, 1));
        assert_eq!(r.rtt_ms.min, Some(2.0));
        assert_eq!(r.rtt_ms.max, Some(30.0));
        assert_eq!(r.loss_pct, Some(25.0));
        assert_eq!(r.transitions.len(), 2);
        assert_eq!(r.transitions[0].from, "relay:sfo");
        assert_eq!(r.transitions[1].to, "relay:sfo");
        assert_eq!(r.connections["direct"], 2);
        let keys: Vec<&str> = out.summary.iter().map(|s| s.key.as_str()).collect();
        assert_eq!(keys, ["rtt_min_ms", "rtt_p50_ms", "rtt_max_ms", "loss_pct"]);
        assert_eq!(out.series[0].points.len(), 4);
        assert_eq!(out.series[0].warmup_ms, Some(0.0));
        assert_eq!(out.notes.len(), 1);
    }

    #[test]
    fn example_is_complete() {
        let out = example_output();
        assert!(out.summary.iter().all(|s| s.value.is_some()));
        let r: Results = serde_json::from_value(out.results).unwrap();
        assert_eq!(r.sent, 20);
        assert_eq!(r.lost, 1);
    }
}
