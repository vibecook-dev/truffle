//! SyncedStore macro benchmark (RFC 019 §10.2.2).
//!
//! Two scenarios, loosely modelled on `dmonad/crdt-benchmarks`:
//!
//! - **b1-sequential**: alpha performs N updates; beta waits for convergence
//!   to the terminal value. Measures one-directional propagation.
//! - **b2-concurrent**: both sides perform N updates in parallel. Measures
//!   round-trip convergence under concurrent writes.

use std::time::Instant;

use clap::{Args as ClapArgs, ValueEnum};
use serde::{Deserialize, Serialize};

use crate::anyhow_lite;
use crate::harness::make_bench_pair;
use crate::report;

#[derive(ClapArgs)]
pub struct Args {
    /// Scenario to run.
    #[arg(long, value_enum, default_value_t = Scenario::B1Sequential)]
    scenario: Scenario,

    /// Number of updates to emit per side.
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u32).range(1..))]
    ops: u32,

    /// Convergence wait budget (seconds).
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u64).range(1..=3600))]
    converge_secs: u64,
}

#[derive(Clone, Copy, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Scenario {
    B1Sequential,
    B2Concurrent,
}

#[derive(Serialize)]
struct Params {
    scenario: Scenario,
    ops: u32,
    converge_secs: u64,
}

#[derive(Serialize)]
struct Summary {
    valid: bool,
    convergence_ms: u128,
    ops_per_sec_during_writes: f64,
    final_local_count: u64,
    observed_final_count_on_peer: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Counter {
    owner: String,
    count: u64,
}

pub async fn run(args: Args, authkey: &str, out_dir: &str) -> anyhow_lite::Result {
    eprintln!(
        "[synced-store] scenario={:?} ops={} converge_budget={}s",
        args.scenario as u8, args.ops, args.converge_secs
    );

    let pair = make_bench_pair(authkey)
        .await
        .map_err(|e| anyhow_lite::Error(format!("failed to build bench pair: {e}")))?;

    let before = pair.diagnostics().await;
    let result = match args.scenario {
        Scenario::B1Sequential => run_b1(&pair, args.ops, args.converge_secs).await,
        Scenario::B2Concurrent => run_b2(&pair, args.ops, args.converge_secs).await,
    };

    let after = pair.diagnostics().await;
    pair.stop().await;
    let summary = result?;
    let valid = summary.valid;
    eprintln!(
        "\n[synced-store] convergence={}ms | writes={:.2} ops/s | peer_count={}",
        summary.convergence_ms,
        summary.ops_per_sec_during_writes,
        summary.observed_final_count_on_peer
    );

    let params = Params {
        scenario: args.scenario,
        ops: args.ops,
        converge_secs: args.converge_secs,
    };
    let path = report::write(
        out_dir,
        "synced-store",
        serde_json::json!({"topology":"same-host","before":before,"after":after}),
        params,
        summary,
    )?;
    eprintln!("[synced-store] report written to {}", path.display());

    if !valid {
        anyhow_lite::bail!("store did not converge within the budget (see report)");
    }
    Ok(())
}

async fn run_b1(
    pair: &crate::harness::BenchPair,
    ops: u32,
    converge_secs: u64,
) -> anyhow_lite::Result<Summary> {
    let alpha_store = pair.alpha.synced_store::<Counter>("bench-b1");
    let beta_store = pair.beta.synced_store::<Counter>("bench-b1");
    let alpha_id = pair.alpha_device_id.clone();

    // Give the sync subscription a beat to register on both sides.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let target = ops as u64;

    // Phase 1: alpha writes N updates as fast as possible.
    let writes_t0 = Instant::now();
    for i in 1..=target {
        alpha_store
            .set(Counter {
                owner: "alpha".into(),
                count: i,
            })
            .await;
    }
    let writes_elapsed = writes_t0.elapsed();
    let ops_per_sec = target as f64 / writes_elapsed.as_secs_f64().max(1e-6);
    eprintln!(
        "  [b1] alpha wrote {} updates in {:.3}s ({:.2} ops/s)",
        target,
        writes_elapsed.as_secs_f64(),
        ops_per_sec
    );

    // Phase 2: wait for beta's view to reach `target`.
    let converge_deadline = Instant::now() + std::time::Duration::from_secs(converge_secs);
    let converge_t0 = Instant::now();
    let mut observed: u64 = 0;
    loop {
        if let Some(slice) = beta_store.get(&alpha_id).await {
            observed = slice.data.count;
            if observed >= target {
                break;
            }
        }
        if Instant::now() >= converge_deadline {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let convergence = converge_t0.elapsed();

    if observed < target {
        eprintln!(
            "  [b1] WARN: beta only observed count={} out of target={} within {}s",
            observed, target, converge_secs
        );
    }

    Ok(Summary {
        valid: observed == target,
        convergence_ms: convergence.as_millis(),
        ops_per_sec_during_writes: ops_per_sec,
        final_local_count: target,
        observed_final_count_on_peer: observed,
    })
}

async fn run_b2(
    pair: &crate::harness::BenchPair,
    ops: u32,
    converge_secs: u64,
) -> anyhow_lite::Result<Summary> {
    let alpha_store = pair.alpha.synced_store::<Counter>("bench-b2");
    let beta_store = pair.beta.synced_store::<Counter>("bench-b2");
    let alpha_id = pair.alpha_device_id.clone();
    let beta_id = pair.beta_device_id.clone();

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let target = ops as u64;

    let writes_t0 = Instant::now();
    let a_task = {
        let s = alpha_store.clone();
        tokio::spawn(async move {
            for i in 1..=target {
                s.set(Counter {
                    owner: "alpha".into(),
                    count: i,
                })
                .await;
            }
        })
    };
    let b_task = {
        let s = beta_store.clone();
        tokio::spawn(async move {
            for i in 1..=target {
                s.set(Counter {
                    owner: "beta".into(),
                    count: i,
                })
                .await;
            }
        })
    };
    let (a, b) = tokio::join!(a_task, b_task);
    a?;
    b?;
    let writes_elapsed = writes_t0.elapsed();
    let ops_per_sec = (2 * target) as f64 / writes_elapsed.as_secs_f64().max(1e-6);
    eprintln!(
        "  [b2] both sides wrote {} updates in {:.3}s ({:.2} combined ops/s)",
        2 * target,
        writes_elapsed.as_secs_f64(),
        ops_per_sec
    );

    let converge_deadline = Instant::now() + std::time::Duration::from_secs(converge_secs);
    let converge_t0 = Instant::now();
    let observed_peer: u64 = loop {
        let a_sees_b = alpha_store
            .get(&beta_id)
            .await
            .map(|s| s.data.count)
            .unwrap_or(0);
        let b_sees_a = beta_store
            .get(&alpha_id)
            .await
            .map(|s| s.data.count)
            .unwrap_or(0);
        if a_sees_b >= target && b_sees_a >= target {
            break a_sees_b.min(b_sees_a);
        }
        if Instant::now() >= converge_deadline {
            break a_sees_b.min(b_sees_a);
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    let convergence = converge_t0.elapsed();

    Ok(Summary {
        valid: observed_peer == target,
        convergence_ms: convergence.as_millis(),
        ops_per_sec_during_writes: ops_per_sec,
        final_local_count: target,
        observed_final_count_on_peer: observed_peer,
    })
}
