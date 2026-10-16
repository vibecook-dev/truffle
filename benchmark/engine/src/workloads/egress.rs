//! `egress` (RFC 027 §8.2 egress, with the tcp accounting of §8.1).
//!
//! - The responder runs the tcp sink ([`tcp::prepare_sink`](super::tcp)):
//!   `listen_tcp(0)`, the rule 5 identity check, the TBN1 header, one
//!   run-level receiver.
//! - The initiator calls `egress_add(EgressRoute { id: "bench-<run_tag_hex>",
//!   target: "<responder Peer.ip>:<port>", login: None })`, then opens
//!   `streams` plain `tokio::net::TcpStream::connect(bound.listen)`
//!   connections, each starting with the TBN1 header, and runs the same
//!   throughput data phase and goodput accounting as tcp (upload or
//!   download).
//! - `NodeError::Network(NetworkError::Unsupported(_))` from `egress_add`
//!   fails the run with "sidecar protocol < 7".
//! - The route is removed at teardown, also when the run fails, is
//!   cancelled or passes its deadline.
//! - Egress removes only the initiator-side Rust bridge; the responder's sink
//!   still uses one. So tcp crosses 2 bridges and egress 1, and egress − tcp
//!   is the cost of one bridge.
//!
//! Summary keys: `goodput_mbps`, `cpu_ms_per_mb`, `loaded_rtt_p50_ms`.
//! Series: `goodput_mbps`. Live samples: `goodput_mbps` (responder ticks for
//! upload, local for download), sender-side `sent_mbps`.

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use truffle_core::egress::EgressRoute;
use truffle_core::network::NetworkError;
use truffle_core::NodeError;

use crate::dataplane::{ReceiverStats, StreamStats};
use crate::error::{AppError, ErrorCode};
use crate::hub::BenchNode;
use crate::params::{Direction, EgressParams, OneWayDirection, WorkloadParams};
use crate::types::{Better, Distribution};
use crate::workloads::common::{cpu_per_mb_item, loaded_rtt_item};
use crate::workloads::tcp::{
    self, item, prepare_sink, run_throughput, socket_addr, Dial, SinkConfig, StreamResult,
    ThroughputPlan, ThroughputResults,
};
use crate::workloads::{ResponderCtx, ResponderRun, RunCtx, WorkloadOutput};

/// Whether this build offers `egress` (drives `PeerHello.workloads`).
pub const IMPLEMENTED: bool = true;

/// `prepare.client`: the tcp sink's (nothing beyond the params).
pub type Client = tcp::Client;

/// `prepare.ack.setup`: the tcp sink's tailnet `port`.
pub type Setup = tcp::Setup;

/// `finish.info`: the initiator's sender-side counts (`sent_bytes` per stream).
pub type Info = tcp::Info;

/// `finish.ack.stats`: the tcp sink's numbers.
pub type Stats = tcp::Stats;

/// The note every egress run carries (§8.2 egress).
pub const BRIDGE_NOTE: &str = "egress removes only the initiator-side Rust bridge, and the \
                               responder's sink still uses one. So tcp crosses 2 bridges and \
                               egress 1, and egress − tcp is the cost of one bridge.";

/// `RunReport.results` of an `egress` run: the tcp throughput results plus
/// the route.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(rename = "EgressResults")]
pub struct Results {
    /// Streams, bytes, window and receiver accounting, as for tcp.
    #[serde(flatten)]
    pub throughput: ThroughputResults,
    /// Rust loopback bridges on the data path: 1 (tcp has 2).
    pub bridges: u32,
    /// The egress route's id, `bench-<run_tag_hex>`.
    pub route_id: String,
}

/// Removes the egress route when dropped (a cancelled or timed-out run's
/// future is dropped by the hub), unless [`remove`](Self::remove) ran.
struct RouteGuard {
    node: Arc<BenchNode>,
    id: Option<String>,
}

impl RouteGuard {
    async fn remove(mut self) {
        if let Some(id) = self.id.take() {
            if let Err(e) = self.node.egress_remove(&id).await {
                tracing::debug!(id, error = %e, "egress: could not remove the route");
            }
        }
    }
}

impl Drop for RouteGuard {
    fn drop(&mut self) {
        if let Some(id) = self.id.take() {
            let node = self.node.clone();
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                rt.spawn(async move {
                    let _ = node.egress_remove(&id).await;
                });
            }
        }
    }
}

/// `egress_add`'s error as the run reports it.
fn egress_error(e: NodeError) -> AppError {
    match e {
        NodeError::Network(NetworkError::Unsupported(m)) => AppError::new(
            ErrorCode::UnsupportedWorkload,
            format!("sidecar protocol < 7: this sidecar has no egress routes ({m})"),
        ),
        other => AppError::from(other).context("egress_add"),
    }
}

/// Initiator side (§6 "Initiator flow" step 7).
pub async fn run(ctx: &mut RunCtx<'_>, p: &EgressParams) -> Result<WorkloadOutput, AppError> {
    let setup: Setup = ctx.prepare(&Client {}).await?;
    let p = match ctx.params() {
        WorkloadParams::Egress(e) => e.clone(),
        _ => p.clone(),
    };
    let route_id = format!("bench-{}", ctx.run_tag_hex()?);
    let route = EgressRoute {
        id: route_id.clone(),
        target: socket_addr(ctx.peer_ip(), setup.port),
        login: None,
    };
    let bound = ctx.node.egress_add(route).await.map_err(egress_error)?;
    let guard = RouteGuard {
        node: ctx.node.clone(),
        id: Some(route_id.clone()),
    };
    let plan = ThroughputPlan {
        direction: direction(p.direction),
        streams: p.streams,
        warmup_secs: p.warmup_secs,
        duration_secs: p.duration_secs,
        nodelay: true,
    };
    let measured = run_throughput(ctx, &plan, &Dial::Loopback { addr: bound.listen }).await;
    guard.remove().await;
    let m = measured?;
    let results = Results {
        throughput: m.results,
        bridges: 1,
        route_id,
    };
    Ok(WorkloadOutput {
        summary: vec![
            item(
                "goodput_mbps",
                "Goodput",
                Some(m.goodput_mbps),
                "Mb/s",
                Better::Higher,
            ),
            cpu_per_mb_item(),
            loaded_rtt_item(),
        ],
        results: serde_json::to_value(&results)
            .map_err(|e| AppError::internal(format!("serialise the egress results: {e}")))?,
        series: m.series,
        notes: vec![BRIDGE_NOTE.into()],
        window_ns: Some(m.window_ns),
        goodput_bytes: Some(m.goodput_bytes),
    })
}

fn direction(d: OneWayDirection) -> Direction {
    match d {
        OneWayDirection::Upload => Direction::Upload,
        OneWayDirection::Download => Direction::Download,
    }
}

/// Responder side: the tcp sink (`TCP_NODELAY` on, as tcp's default).
pub async fn prepare(
    r: &ResponderCtx,
    p: &EgressParams,
    _client: Client,
) -> Result<(Setup, Box<dyn ResponderRun>), AppError> {
    let cfg = SinkConfig {
        direction: direction(p.direction),
        streams: p.streams,
        warmup_secs: p.warmup_secs,
        duration_secs: p.duration_secs,
        nodelay: true,
        echo_payload: None,
    };
    let (port, run) = prepare_sink(r, cfg).await?;
    Ok((Setup { port }, run))
}

/// A canonical example output for the schema fixtures (upload, 1 stream, 10 s).
pub fn example_output() -> WorkloadOutput {
    let mut cpu = cpu_per_mb_item();
    cpu.value = Some(3.2);
    let mut rtt = loaded_rtt_item();
    rtt.value = Some(5.9);
    let epoch = 1_760_601_600_900_000_000u64;
    let buckets: Vec<u64> = [
        21_875_000u64,
        31_562_500,
        32_625_000,
        32_875_000,
        32_437_500,
    ]
    .iter()
    .copied()
    .cycle()
    .take(44)
    .collect();
    let total: u64 = buckets.iter().sum();
    let last = epoch + 10_997_000_000;
    let receiver = ReceiverStats {
        epoch_ns: epoch,
        bucket_ms: 250,
        buckets,
        last_ns: last,
        bytes_total: total,
        streams: vec![StreamStats {
            stream_idx: 0,
            bytes: total,
            first_ns: epoch,
            last_ns: last,
        }],
    };
    let (mbps, bytes) = receiver
        .goodput(1)
        .map(|g| (g.mbps, g.bytes))
        .unwrap_or((0.0, 0));
    let results = Results {
        throughput: ThroughputResults {
            direction: Direction::Upload,
            streams: vec![StreamResult {
                stream_idx: 0,
                direction: OneWayDirection::Upload,
                connect_ms: 0.4,
                sent_bytes: total + 4_194_304,
                received_bytes: total,
                acked_bytes: total,
            }],
            bytes_total: total,
            received_bytes: total,
            sent_bytes: total + 4_194_304,
            goodput_bytes: bytes,
            window_ms: 9_997,
            connect_ms: Distribution::from_values(&[0.4]),
            receiver: Some(receiver.clone()),
            upload: None,
            download: None,
            foreign_dropped: 0,
            nodelay: true,
        },
        bridges: 1,
        route_id: "bench-6f1c2a9e4b7d3058".into(),
    };
    WorkloadOutput {
        summary: vec![
            item(
                "goodput_mbps",
                "Goodput",
                Some((mbps * 10.0).round() / 10.0),
                "Mb/s",
                Better::Higher,
            ),
            cpu,
            rtt,
        ],
        results: serde_json::to_value(results).unwrap_or_default(),
        series: vec![receiver.series(epoch - 400_000_000, 1, "goodput_mbps", "Goodput")],
        notes: vec![BRIDGE_NOTE.into()],
        window_ns: Some((epoch + 1_000_000_000, last)),
        goodput_bytes: Some(bytes),
    }
}

/// The egress example (upload) for the schema fixtures.
pub fn examples() -> Vec<super::ModeExample> {
    vec![super::ModeExample {
        mode: "egress",
        params: WorkloadParams::Egress(EgressParams::default()),
        output: example_output(),
        clock: true,
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_sidecars_fail_with_the_protocol_message() {
        let e = egress_error(NodeError::Network(NetworkError::Unsupported(
            "egress requires sidecar protocol v7".into(),
        )));
        assert_eq!(e.code, ErrorCode::UnsupportedWorkload);
        assert!(
            e.message.starts_with("sidecar protocol < 7"),
            "{}",
            e.message
        );
        let other = egress_error(NodeError::Stopped);
        assert_ne!(other.code, ErrorCode::UnsupportedWorkload);
        assert!(other.message.contains("egress_add"), "{}", other.message);
    }

    #[test]
    fn example_results_flatten_the_tcp_results() {
        let out = example_output();
        assert_eq!(out.results["bridges"], 1);
        assert_eq!(out.results["direction"], "upload");
        assert!(out.results.get("bytes_total").is_some());
        let r: Results = serde_json::from_value(out.results.clone()).unwrap();
        r.throughput
            .receiver
            .as_ref()
            .unwrap()
            .check_invariants()
            .unwrap();
        assert!(r.throughput.received_bytes <= r.throughput.sent_bytes);
        assert!(r.route_id.starts_with("bench-"));
        assert_eq!(out.summary[0].key, "goodput_mbps");
        assert!(out.summary.iter().all(|s| s.value.is_some()));
        assert_eq!(direction(OneWayDirection::Download), Direction::Download);
        assert!(BRIDGE_NOTE.contains("egress − tcp is the cost of one bridge"));
    }

    /// On the real tailnet (skipped without a key): an egress run is `ok`,
    /// its accounting holds, and its route is gone afterwards.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_egress_route_is_removed_after_the_run() {
        let _ = dotenvy::dotenv();
        let Some(key) = [
            "TRUFFLE_BENCH_AUTH_KEY",
            "TRUFFLE_TEST_AUTHKEY",
            "TS_AUTHKEY",
        ]
        .iter()
        .find_map(|v| std::env::var(v).ok().filter(|k| !k.trim().is_empty())) else {
            eprintln!("[skip] live_egress_route_is_removed_after_the_run: no auth key");
            return;
        };
        // Not while the hub's control-protocol test has its egress double in.
        let _serial = crate::workloads::test_double::LIVE_PAIR_LOCK.lock().await;
        let pair = crate::Pair::start(&key, crate::Tool::Cli, None)
            .await
            .expect("pair");
        let params = WorkloadParams::Egress(EgressParams {
            duration_secs: 2,
            warmup_secs: 0,
            ..EgressParams::default()
        });
        let report = pair
            .run("egress-live".into(), params, std::sync::Arc::new(|_| {}))
            .await;
        let routes = pair
            .initiator
            .node()
            .expect("running")
            .egress_list()
            .await
            .expect("egress_list");
        pair.stop().await;
        assert_eq!(
            report.status,
            crate::RunStatus::Ok,
            "{:?} {:?}",
            report.error_code,
            report.error
        );
        assert!(
            report.results.get("route_id").is_some(),
            "the run went through a test double: {}",
            report.results
        );
        assert!(routes.is_empty(), "routes left behind: {routes:?}");
        let r: Results = serde_json::from_value(report.results).unwrap();
        r.throughput.receiver.unwrap().check_invariants().unwrap();
        assert!(r.throughput.received_bytes <= r.throughput.sent_bytes);
        assert!(report.summary.iter().all(|s| s.value.is_some()));
    }
}
