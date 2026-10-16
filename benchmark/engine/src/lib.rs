//! `truffle_bench`: the benchmark engine for truffle (RFC 027).
//!
//! One engine owns every two-node workload, the peer control protocol, the
//! report format and the statistics; the `truffle-bench` CLI and the
//! "truffle bench" desktop app both drive it.
//!
//! Foundation modules:
//!
//! - [`error`]: [`AppError`] / [`ErrorCode`];
//! - [`types`]: every type shared with the app (§7.1–§7.4);
//! - [`params`]: [`WorkloadParams`] with bounds, deadlines and labels (§7.3);
//! - [`config`]: [`HubConfig`] and sidecar resolution (§5.3);
//! - [`env`](mod@env): [`NodeEnv`] capture and the local [`PeerHello`];
//! - [`stats`]: distributions, jitter, sequence accounting (§7.4, §8.2);
//! - [`clock`]: the process timebase and NTP-style offsets (§6 Clock);
//! - [`dataplane`]: wire headers, receiver accounting, pacing (§6, §8.1);
//! - [`resources`]: CPU/RSS sampling (§7.4);
//! - [`localapi`]: the tsnet LocalAPI client (usermetrics, logout);
//! - [`pathwatch`]: path settle, snapshots, the 2 Hz sampler, `path_label`;
//! - [`report`]: the v3 [`RunReport`], `compare_key`, the [`RunStore`] and import;
//! - [`suites`]: the suite profiles (§8.4).
//!
//! Hub modules:
//!
//! - [`control`]: the peer control protocol and the responder (§6);
//! - [`hub`]: the [`Hub`], its offer router and in-process [`Pair`]s (§5.4);
//! - [`workloads`]: the workload framework and every workload (§8);
//! - [`schema`]: the generated JSON Schema and fixtures for the app (§7).
//!
//! Params arrive as JSON, are validated (never clamped) and label themselves:
//!
//! ```
//! use truffle_bench::{Workload, WorkloadParams};
//!
//! let p = WorkloadParams::from_value(serde_json::json!({"workload": "tcp", "streams": 4}))?;
//! assert_eq!(p.workload(), Workload::Tcp);
//! assert_eq!(p.label(), "TCP throughput · upload · 4 streams · 10 s");
//! assert_eq!(p.deadline().as_secs(), 41);
//! assert!(WorkloadParams::from_value(serde_json::json!({"workload": "tcp", "streams": 17})).is_err());
//! # Ok::<(), truffle_bench::AppError>(())
//! ```

pub mod clock;
pub mod config;
pub mod control;
pub mod dataplane;
pub mod env;
pub mod error;
pub mod hub;
pub mod localapi;
pub mod params;
pub mod pathwatch;
pub mod report;
pub mod resources;
pub mod schema;
pub mod stats;
pub mod suites;
pub mod types;
pub mod workloads;

pub use config::{HubConfig, Tool};
pub use error::{AppError, ErrorCode};
pub use hub::{BenchNode, Hub, Pair, ProgressSink, RunRequest, SuiteProgressSink, SuiteRequest};
pub use params::{Workload, WorkloadParams};
pub use report::{NetworkInfo, Resources, RunReport, RunStore};
pub use types::*;
pub use workloads::WorkloadOutput;

/// truffle app id every hub uses (except per-run ids in `pair`).
pub const APP_ID: &str = "truffle-bench";
/// Namespace of the control protocol (§6).
pub const CONTROL_NS: &str = "bench";
/// Namespace of the `ws` workload's data messages.
pub const WS_NS: &str = "bench.ws";
/// Control protocol version; peers with another version cannot be measured.
pub const PROTOCOL: u32 = 1;
/// Engine version, recorded in reports and hellos.
pub const TOOL_VERSION: &str = env!("CARGO_PKG_VERSION");
/// The Tailscale admin console's machine list, where a device waiting in
/// [`NodeStatus::NeedsApproval`] is approved.
pub const TAILSCALE_ADMIN_MACHINES_URL: &str = "https://login.tailscale.com/admin/machines";

#[cfg(test)]
mod tests {
    use super::*;

    /// Every shared type yields a JSON schema (the `schema_is_current` test
    /// and the app's types depend on it).
    #[test]
    fn shared_types_have_schemas() {
        macro_rules! schema_of {
            ($($t:ty),* $(,)?) => {
                $(
                    let schema = serde_json::to_value(schemars::schema_for!($t)).unwrap();
                    assert!(schema.is_object(), "{}", stringify!($t));
                )*
            };
        }
        schema_of!(
            RunReport,
            RunSummary,
            HubEvent,
            NodeState,
            WorkloadParams,
            Progress,
            SuiteProgress,
            SuiteResult,
            SuiteProfile,
            AppInfo,
            Settings,
            BenchPeer,
            PeerProbe,
            PeerHello,
            ActiveRun,
            ImportResult,
            AppError,
            dataplane::ReceiverStats,
        );
        let s = serde_json::to_value(schemars::schema_for!(WorkloadParams)).unwrap();
        let text = s.to_string();
        for w in Workload::ALL {
            assert!(text.contains(&format!("\"{}\"", w.as_str())), "{w}");
        }
    }
}
