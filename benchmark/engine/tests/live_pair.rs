//! Real-network PR regression (RFC 027 §12).
//!
//! Without an auth key this prints `[skip]` and passes. With one
//! (`TRUFFLE_BENCH_AUTH_KEY`, `TRUFFLE_TEST_AUTHKEY` or `TS_AUTHKEY`, from the
//! environment or the repo-root `.env`), it starts one in-process `pair` and
//! runs:
//!
//! - `path` 5 pings, always;
//! - tcp throughput 2 s, udp stream 2 s, ws rr 50, file-transfer
//!   1024 B × 12 (the pending-offer regression) and synced-store pingpong
//!   10, each only once its module sets `IMPLEMENTED = true`, so the test
//!   tightens on its own as workloads land.
//!
//! Every run must end `ok` and satisfy the §12 invariants.

use std::sync::Arc;

use serde_json::json;
use truffle_bench::workloads;
use truffle_bench::{Pair, RunReport, RunStatus, Tool, Topology, Workload, WorkloadParams};

fn auth_key() -> Option<String> {
    let _ = dotenvy::dotenv();
    [
        "TRUFFLE_BENCH_AUTH_KEY",
        "TRUFFLE_TEST_AUTHKEY",
        "TS_AUTHKEY",
    ]
    .iter()
    .find_map(|v| std::env::var(v).ok().filter(|k| !k.trim().is_empty()))
}

/// The §12 invariants every real-network report must satisfy.
fn check_invariants(r: &RunReport) {
    let id = format!("{} ({})", r.label, r.run_id);
    assert_eq!(
        r.status,
        RunStatus::Ok,
        "{id}: {:?} {:?}; notes {:?}",
        r.error_code,
        r.error,
        r.notes
    );
    assert_eq!(r.schema_version, 3, "{id}");
    assert_eq!(r.compare_key, r.compute_compare_key(), "{id}");
    assert_eq!(r.network.topology, Topology::SameProcess, "{id}");
    assert!(r.peer.is_some(), "{id}: no peer environment");
    // Summary keys are non-null at defaults.
    for s in &r.summary {
        assert!(s.value.is_some(), "{id}: summary {} is null", s.key);
    }
    assert!(!r.summary.is_empty(), "{id}: empty summary");
    // In same-process runs, |offset| ≤ uncertainty.
    if let Some(clock) = &r.network.clock {
        assert!(
            clock.before.offset_ns.unsigned_abs() <= clock.before.uncertainty_ns,
            "{id}: clock offset {} ns exceeds its uncertainty {} ns in one process",
            clock.before.offset_ns,
            clock.before.uncertainty_ns
        );
    }
    // Receiver accounting: Σ buckets == bytes_total, received ≤ sent. A
    // receiver block that does not parse is a failure, not a skip.
    if let Some(stats) = r.results.get("receiver").filter(|v| !v.is_null()) {
        let st = serde_json::from_value::<truffle_bench::dataplane::ReceiverStats>(stats.clone())
            .unwrap_or_else(|e| panic!("{id}: results.receiver did not parse: {e}"));
        st.check_invariants()
            .unwrap_or_else(|e| panic!("{id}: {e}"));
    }
    if let (Some(sent), Some(recv)) = (
        r.results.get("sent_bytes").and_then(|v| v.as_u64()),
        r.results.get("received_bytes").and_then(|v| v.as_u64()),
    ) {
        assert!(recv <= sent, "{id}: received {recv} > sent {sent}");
    }
    // UDP: unique + missing == sent.
    if let (Some(sent), Some(unique), Some(missing)) = (
        r.results.get("sent").and_then(|v| v.as_u64()),
        r.results.get("unique").and_then(|v| v.as_u64()),
        r.results.get("missing").and_then(|v| v.as_u64()),
    ) {
        assert_eq!(unique + missing, sent, "{id}: unique + missing != sent");
    }
}

fn params(v: serde_json::Value) -> WorkloadParams {
    WorkloadParams::from_value(v).expect("valid params")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_pair() {
    let Some(key) = auth_key() else {
        eprintln!(
            "[skip] live_pair: no TRUFFLE_BENCH_AUTH_KEY / TRUFFLE_TEST_AUTHKEY / TS_AUTHKEY"
        );
        return;
    };
    let pair = match Pair::start(&key, Tool::Cli, None).await {
        Ok(p) => p,
        Err(e) => panic!("pair did not start: {} ({})", e.message, e.code),
    };
    eprintln!("[live_pair] pair up · app id {}", pair.app_id);

    let mut plan: Vec<(Workload, WorkloadParams)> = vec![(
        Workload::Path,
        params(json!({"workload": "path", "count": 5})),
    )];
    let optional = [
        (
            Workload::Tcp,
            json!({"workload": "tcp", "duration_secs": 2, "warmup_secs": 0}),
        ),
        (
            Workload::Udp,
            json!({"workload": "udp", "mode": "stream", "duration_secs": 2, "warmup_secs": 0}),
        ),
        (Workload::Ws, json!({"workload": "ws", "count": 50})),
        (
            Workload::FileTransfer,
            json!({"workload": "file-transfer", "size_bytes": 1024, "iterations": 12}),
        ),
        (
            Workload::SyncedStore,
            json!({"workload": "synced-store", "count": 10}),
        ),
    ];
    for (w, p) in optional {
        if workloads::implemented(w) {
            plan.push((w, params(p)));
        } else {
            eprintln!("[live_pair] {w}: not implemented yet, skipped");
        }
    }

    let mut failures = Vec::new();
    for (w, p) in plan {
        let report = pair
            .run(uuid::Uuid::new_v4().to_string(), p, Arc::new(|_| {}))
            .await;
        eprintln!(
            "[live_pair] {w}: {:?} {}",
            report.status,
            report
                .summary
                .iter()
                .map(|s| format!("{}={:?}", s.key, s.value))
                .collect::<Vec<_>>()
                .join(" ")
        );
        let checked = std::panic::catch_unwind(|| {
            check_invariants(&report);
            if w == Workload::FileTransfer {
                let n = report
                    .results
                    .get("iterations")
                    .and_then(|v| v.as_array())
                    .map_or(0, Vec::len);
                assert_eq!(n, 12, "file-transfer: expected 12 iterations, got {n}");
            }
            if w == Workload::Udp {
                // The UDP invariant above must have had something to check.
                for k in ["sent", "unique", "missing"] {
                    assert!(
                        report.results.get(k).and_then(|v| v.as_u64()).is_some(),
                        "udp: results.{k} missing"
                    );
                }
            }
            if w == Workload::Path {
                let r: workloads::path::Results =
                    serde_json::from_value(report.results.clone()).expect("path results");
                assert_eq!(r.sent, 5);
                assert!(r.hub_start_ms.is_some(), "path: no hub_start_ms");
                assert!(r.discovery_ms.is_some(), "path: no discovery_ms");
            }
        });
        if let Err(e) = checked {
            let msg = e
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            failures.push(format!("{w}: {msg}"));
        }
    }
    pair.stop().await;
    assert!(
        failures.is_empty(),
        "live_pair failures:\n{}",
        failures.join("\n")
    );
}
