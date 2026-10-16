//! Debug builds only: `TRUFFLE_BENCH_SELFTEST=<peer>` runs the `path`
//! workload once (or the params in `TRUFFLE_BENCH_SELFTEST_PARAMS`: a JSON
//! object, or an array of them run one after another) against `<peer>` as
//! soon as the hub is running, through the same [`AppState::run`] that
//! `bench_run` calls, and logs each report.
//!
//! This is an end-to-end check of the command layer on a real tailnet,
//! without driving the UI. It is compiled out of release builds.

use std::sync::Arc;
use std::time::{Duration, Instant};

use truffle_bench::{NodeStatus, Progress, ProgressSink, RunRequest, Workload, WorkloadParams};

use crate::state::AppState;

/// How long to wait for the hub to reach `running`.
const WAIT_RUNNING: Duration = Duration::from_secs(180);
/// How long to wait for the peer to be listed with a cached hello.
const WAIT_HELLO: Duration = Duration::from_secs(90);

/// Spawn the self-test.
pub fn spawn(state: AppState, peer: String) {
    tauri::async_runtime::spawn(async move {
        tracing::info!(%peer, "selftest: waiting for the hub");
        let deadline = Instant::now() + WAIT_RUNNING;
        loop {
            let st = state.node_state().state;
            if st == NodeStatus::Running {
                break;
            }
            if Instant::now() > deadline || st == NodeStatus::Error {
                tracing::error!(state = ?st, "selftest: the hub is not running; giving up");
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        // The run itself is under test, not discovery: wait until the hello
        // cache has answered for the peer (it retries a failed hello).
        let deadline = Instant::now() + WAIT_HELLO;
        loop {
            let listed = state.hub().peers().await.into_iter().find(|p| {
                p.online && (p.device_name == peer || p.id == peer || p.hostname == peer)
            });
            if let Some(p) = &listed {
                if p.hello.is_some() {
                    tracing::info!(peer = %p.device_name, id = %p.id, "selftest: the peer said hello");
                    break;
                }
            }
            if Instant::now() > deadline {
                tracing::warn!(%peer, listed = listed.is_some(), "selftest: no hello from the peer yet; running anyway");
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        // `TRUFFLE_BENCH_SELFTEST_PARAMS` may name other params as JSON.
        let runs = match std::env::var("TRUFFLE_BENCH_SELFTEST_PARAMS") {
            Ok(json) => match params_list(&json) {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(error = %e, "selftest: TRUFFLE_BENCH_SELFTEST_PARAMS is not valid");
                    return;
                }
            },
            Err(_) => vec![Workload::Path.default_params()],
        };
        for params in runs {
            run_one(&state, &peer, params).await;
        }
        tracing::info!("selftest: done");
    });
}

/// One params object, or an array of them.
fn params_list(json: &str) -> Result<Vec<WorkloadParams>, String> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let items = match value {
        serde_json::Value::Array(items) => items,
        one => vec![one],
    };
    items
        .into_iter()
        .map(|v| WorkloadParams::from_value(v).map_err(|e| e.message))
        .collect()
}

/// Run `params` against `peer` and log the report.
async fn run_one(state: &AppState, peer: &str, params: WorkloadParams) {
    let run_id = format!("selftest-{}", uuid::Uuid::new_v4().simple());
    let sink: ProgressSink = Arc::new(|p: Progress| {
        tracing::debug!(phase = ?p.phase, t_ms = p.t_ms, fraction = ?p.fraction, "selftest progress");
    });
    let req = RunRequest {
        run_id: run_id.clone(),
        peer: peer.to_string(),
        params,
        suite_id: None,
    };
    tracing::info!(%peer, %run_id, label = %req.params.label(), "selftest: running");
    let report = state.run(req, sink).await;
    let summary: Vec<String> = report
        .summary
        .iter()
        .map(|s| match s.value {
            Some(v) => format!("{}={v:.3}{}", s.key, s.unit),
            None => format!("{}=null", s.key),
        })
        .collect();
    let file = state.hub().store().dir().join(report.file_name());
    let stored = state.hub().store().get(&report.run_id).ok();
    tracing::info!(
        status = ?report.status,
        schema_version = report.schema_version,
        tool = ?report.tool,
        topology = ?report.network.topology,
        path_label = ?report.network.path_label,
        foreground_pct = ?report.local.app_foreground_pct,
        stored_foreground_pct = ?stored.as_ref().and_then(|r| r.local.app_foreground_pct),
        error = report.error.as_deref().unwrap_or("-"),
        summary = %summary.join(" "),
        stored = file.is_file(),
        file = %file.display(),
        "selftest: report"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selftest_params_are_one_object_or_a_list() {
        let one = params_list(r#"{"workload":"path","count":5}"#).unwrap();
        assert_eq!(one.len(), 1);
        let two = params_list(
            r#"[{"workload":"path"},{"workload":"tcp","duration_secs":3,"warmup_secs":0}]"#,
        )
        .unwrap();
        assert_eq!(
            two.iter().map(|p| p.workload()).collect::<Vec<_>>(),
            [Workload::Path, Workload::Tcp]
        );
        assert!(params_list(r#"[{"workload":"tcp","streams":99}]"#).is_err());
        assert!(params_list("nope").is_err());
    }
}
