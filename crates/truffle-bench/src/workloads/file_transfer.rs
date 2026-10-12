//! File transfer macro benchmark (RFC 019 §10.2.1).
//!
//! Sends a deterministic payload from alpha → beta repeatedly and reports
//! throughput + latency percentiles across iterations.

use std::time::Instant;

use clap::Args as ClapArgs;
use serde::Serialize;
use sha2::{Digest, Sha256};
use truffle_core::file_transfer::{FileTransferEvent, TransferDirection};

use crate::anyhow_lite;
use crate::harness::make_bench_pair;
use crate::report::{self, Distribution};

#[derive(ClapArgs)]
pub struct Args {
    /// Payload size per transfer. Accepts plain bytes or a suffix: K, M, G (binary).
    /// Examples: `1048576`, `1M`, `100M`, `1G`.
    #[arg(long, default_value = "10M")]
    size: String,

    /// Number of transfers to run.
    #[arg(long, default_value_t = 5, value_parser = clap::value_parser!(u32).range(1..))]
    iterations: u32,

    /// Warm-up iterations that don't count toward results.
    #[arg(long, default_value_t = 1)]
    warmup: u32,
}

#[derive(Serialize)]
struct Params {
    size_bytes: u64,
    iterations: u32,
    warmup: u32,
}

#[derive(Serialize)]
struct IterationResult {
    index: u32,
    bytes: u64,
    elapsed_secs: f64,
    throughput_mb_per_sec: f64,
    sha256: String,
}

#[derive(Serialize)]
struct Summary {
    valid: bool,
    iterations: Vec<IterationResult>,
    latency_ms: Option<Distribution>,
    throughput_mb_per_sec: Option<Distribution>,
    mean_throughput_mb_per_sec: f64,
}

pub async fn run(args: Args, authkey: &str, out_dir: &str) -> anyhow_lite::Result {
    let size = parse_size(&args.size)?;

    eprintln!(
        "[file-transfer] size={} bytes | iterations={} | warmup={}",
        size, args.iterations, args.warmup
    );

    let pair = make_bench_pair(authkey)
        .await
        .map_err(|e| anyhow_lite::Error(format!("failed to build bench pair: {e}")))?;

    let before = pair.diagnostics().await;
    let outcome = async {
        // Prepare source content once.
        let src_dir = tempfile::TempDir::new()?;
        let dst_dir = tempfile::TempDir::new()?;
        let src_path = src_dir.path().join("payload.bin");
        let dst_path = dst_dir.path().join("payload.bin");
        let content = make_payload(size as usize);
        tokio::fs::write(&src_path, &content).await?;
        let expected_hash = hex::encode(Sha256::digest(&content));

        // Beta: auto-accept into a persistent dir for the duration of this run.
        pair.beta
            .file_transfer()
            .auto_accept(
                pair.beta.clone(),
                dst_dir.path().to_str().expect("utf8 path"),
            )
            .await;

        let mut iterations = Vec::with_capacity(args.iterations as usize);

        let total_runs = args
            .warmup
            .checked_add(args.iterations)
            .ok_or_else(|| anyhow_lite::Error("iteration count overflow".into()))?;
        for i in 0..total_runs {
            let is_warmup = i < args.warmup;
            let label = if is_warmup { "warmup" } else { "measure" };

            // Pass "." so auto_accept resolves to `{output_dir}/{file_name}` rather
            // than a relative path interpreted against the receiver's CWD.
            let mut receiver_events = pair.beta.file_transfer().subscribe();
            let sender_id = pair.alpha.local_info().tailscale_id;
            let t0 = Instant::now();
            let sending = async {
                let result = pair
                    .alpha
                    .file_transfer()
                    .send_file(
                        &pair.beta_device_id,
                        src_path.to_str().expect("utf8 path"),
                        ".",
                    )
                    .await
                    .map_err(|e| anyhow_lite::Error(format!("send_file failed: {e}")))?;
                Ok((result, t0.elapsed()))
            };
            // Drain events concurrently so slow transfers cannot overflow the
            // bounded broadcast queue. The sender timing ends at its ACK, but
            // the next sample waits for the receiver's completion and cleanup.
            let receiving = wait_for_receive(
                &mut receiver_events,
                &sender_id,
                "payload.bin",
                size,
                &expected_hash,
            );
            let ((r, elapsed), ()) =
                tokio::time::timeout(std::time::Duration::from_secs(300), async {
                    tokio::try_join!(sending, receiving)
                })
                .await??;
            let secs = elapsed.as_secs_f64().max(1e-6);
            let mb_per_sec = (r.bytes_transferred as f64 / 1_000_000.0) / secs;

            eprintln!(
                "  [{label}] iter {i:>2}: {} bytes in {:.3}s → {:.2} MB/s",
                r.bytes_transferred, secs, mb_per_sec
            );

            // Verify receiver bytes outside the timed interval; a sender ACK alone
            // must not turn a corrupt/partial transfer into a throughput result.
            let received = tokio::fs::read(&dst_path).await?;
            if r.bytes_transferred != size
                || received.len() as u64 != size
                || r.sha256 != expected_hash
                || hex::encode(Sha256::digest(&received)) != expected_hash
            {
                anyhow_lite::bail!("transfer {i} failed size/hash verification");
            }
            // The receiver's production-safe default rejects overwriting an
            // existing destination. Remove the completed output outside the timed
            // interval so every benchmark iteration starts from the same state.
            tokio::fs::remove_file(&dst_path).await.map_err(|e| {
                anyhow_lite::Error(format!(
                    "failed to clean received file {} after iteration {i}: {e}",
                    dst_path.display()
                ))
            })?;

            if !is_warmup {
                iterations.push(IterationResult {
                    index: i - args.warmup,
                    bytes: r.bytes_transferred,
                    elapsed_secs: secs,
                    throughput_mb_per_sec: mb_per_sec,
                    sha256: r.sha256.clone(),
                });
            }
        }

        let latency = Distribution::new(iterations.iter().map(|r| r.elapsed_secs * 1000.0));
        let tp = Distribution::new(iterations.iter().map(|r| r.throughput_mb_per_sec));
        let mean = if iterations.is_empty() {
            0.0
        } else {
            iterations
                .iter()
                .map(|r| r.throughput_mb_per_sec)
                .sum::<f64>()
                / iterations.len() as f64
        };

        let summary = Summary {
            valid: true,
            iterations,
            latency_ms: latency,
            throughput_mb_per_sec: tp,
            mean_throughput_mb_per_sec: mean,
        };
        let params = Params {
            size_bytes: size,
            iterations: args.iterations,
            warmup: args.warmup,
        };

        let after = pair.diagnostics().await;
        let path = report::write(
            out_dir,
            "file-transfer",
            serde_json::json!({"topology":"same-host", "before":before, "after":after}),
            params,
            summary,
        )?;
        eprintln!("[file-transfer] report written to {}", path.display());

        Ok(())
    }
    .await;
    pair.stop().await;
    outcome
}

async fn wait_for_receive(
    events: &mut tokio::sync::broadcast::Receiver<FileTransferEvent>,
    sender_id: &str,
    file_name: &str,
    size: u64,
    expected_hash: &str,
) -> anyhow_lite::Result {
    let mut offered_token = None;
    loop {
        match events.recv().await? {
            FileTransferEvent::OfferReceived(offer)
                if offer.from_peer == sender_id && offer.file_name == file_name =>
            {
                if offer.size != size || offer.sha256 != expected_hash {
                    anyhow_lite::bail!("receiver offer differs from benchmark payload");
                }
                offered_token = Some(offer.token);
            }
            FileTransferEvent::Completed {
                token,
                direction: TransferDirection::Receive,
                bytes_transferred,
                sha256,
                ..
            } if offered_token.as_deref() == Some(token.as_str()) => {
                if bytes_transferred != size || sha256 != expected_hash {
                    anyhow_lite::bail!("receiver completion failed size/hash verification");
                }
                return Ok(());
            }
            FileTransferEvent::Failed { token, reason, .. }
            | FileTransferEvent::Rejected { token, reason, .. }
                if offered_token.as_deref() == Some(token.as_str()) =>
            {
                anyhow_lite::bail!("receiver failed: {reason}");
            }
            _ => {}
        }
    }
}

fn make_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| ((i * 7 + 13) % 256) as u8).collect()
}

/// Parse a byte-size string: plain number OR trailing K/M/G (binary).
fn parse_size(s: &str) -> anyhow_lite::Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        anyhow_lite::bail!("size is empty");
    }
    let (num_part, mult): (&str, u64) = match s.chars().last().unwrap() {
        'K' | 'k' => (&s[..s.len() - 1], 1024),
        'M' | 'm' => (&s[..s.len() - 1], 1024 * 1024),
        'G' | 'g' => (&s[..s.len() - 1], 1024 * 1024 * 1024),
        _ => (s, 1),
    };
    let n: u64 = num_part
        .parse()
        .map_err(|e| anyhow_lite::Error(format!("invalid size `{s}`: {e}")))?;
    n.checked_mul(mult)
        .filter(|size| *size > 0 && *size <= 1024 * 1024 * 1024)
        .ok_or_else(|| anyhow_lite::Error("size must be between 1 byte and 1 GiB".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sequential_transfers_drain_receiver_before_next_iteration() {
        let _ = dotenvy::dotenv();
        let Ok(authkey) = crate::require_authkey() else {
            eprintln!("[skip] sequential file benchmark requires TRUFFLE_TEST_AUTHKEY");
            return;
        };
        let reports = tempfile::tempdir().unwrap();
        // Small transfers finish before the receiver's ACK-drain period.
        // More than four back-to-back sends expose a pending-offer backlog
        // unless the harness awaits receiver completion between iterations.
        run(
            Args {
                size: "1".into(),
                iterations: 12,
                warmup: 1,
            },
            &authkey,
            reports.path().to_str().unwrap(),
        )
        .await
        .expect("sequential samples must not overlap receiver work");
        let report = std::fs::read_dir(reports.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(report.path()).unwrap()).unwrap();
        assert_eq!(report["results"]["valid"], true);
        assert_eq!(
            report["results"]["iterations"].as_array().unwrap().len(),
            12
        );
    }

    #[test]
    fn sizes_reject_zero_overflow_and_unbounded_allocations() {
        assert_eq!(parse_size("1M").unwrap(), 1_048_576);
        for value in ["0", "", "-1", "18446744073709551615G", "2G"] {
            assert!(parse_size(value).is_err(), "{value}");
        }
    }
}
