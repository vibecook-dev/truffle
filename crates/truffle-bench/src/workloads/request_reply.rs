//! Correlated, validated request/reply RTT samples over the envelope bus.
use crate::{
    anyhow_lite,
    harness::make_bench_pair,
    report::{self, Distribution},
};
use clap::Args as ClapArgs;
use serde::Serialize;
use serde_json::{json, Value};
use std::time::{Duration, Instant};
use truffle_core::{
    node::NamespacedMessage,
    request_reply::{send_and_wait, RequestError},
};

#[derive(ClapArgs)]
pub struct Args {
    #[arg(long, default_value_t = 500, value_parser = clap::value_parser!(u32).range(1..))]
    count: u32,
    #[arg(long, default_value_t = 1024, value_parser = clap::value_parser!(u32).range(1..=1_048_576))]
    payload_bytes: u32,
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u64).range(1..=3600))]
    timeout_secs: u64,
    /// Untimed requests used to warm connections and the Tailscale path.
    #[arg(long, default_value_t = 20)]
    warmup: u32,
}
#[derive(Serialize)]
struct Sample {
    sequence: u64,
    elapsed_ms: f64,
    outcome: String,
}

fn matches_reply(msg: &NamespacedMessage, peer: &str, payload: &Value) -> bool {
    msg.from == peer && msg.msg_type == "pong" && &msg.payload == payload
}

pub async fn run(args: Args, authkey: &str, out_dir: &str) -> anyhow_lite::Result {
    let pair = make_bench_pair(authkey).await.map_err(anyhow_lite::Error)?;
    // Register synchronously before starting the sender; no scheduling sleep.
    let beta = pair.beta.clone();
    let mut rx = beta.subscribe("bench-rr");
    let responder = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(msg) if msg.msg_type == "ping" => {
                    if beta
                        .send_typed(&msg.from, "bench-rr", "pong", &msg.payload)
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
                Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => break,
            }
        }
    });
    let expected_sender = pair.beta.local_info().tailscale_id;
    let before = pair.diagnostics().await;
    let mut samples = Vec::new();
    let run_id = uuid::Uuid::new_v4().to_string();
    let mut warmup_errors = 0;
    let mut measured_start = Instant::now();
    for sequence in 0..u64::from(args.warmup) + u64::from(args.count) {
        if sequence == u64::from(args.warmup) {
            measured_start = Instant::now();
        }
        let payload = json!({"run": run_id, "sequence": sequence, "blob": "x".repeat(args.payload_bytes as usize)});
        let start = Instant::now();
        // The outer deadline includes the send, unlike send_and_wait's reply timer.
        let result = tokio::time::timeout(
            Duration::from_secs(args.timeout_secs),
            send_and_wait(
                &pair.alpha,
                &pair.beta_device_id,
                "bench-rr",
                "ping",
                &payload,
                Duration::from_secs(args.timeout_secs),
                |msg| matches_reply(msg, &expected_sender, &payload).then_some(()),
            ),
        )
        .await;
        let outcome = match result {
            Ok(Ok(())) => "ok",
            Err(_) | Ok(Err(RequestError::Timeout)) => "timeout",
            Ok(Err(RequestError::Send(_))) => "send_error",
            Ok(Err(RequestError::ChannelClosed)) => "channel_closed",
        };
        if sequence < u64::from(args.warmup) {
            if outcome != "ok" {
                warmup_errors += 1;
            }
        } else {
            samples.push(Sample {
                sequence: sequence - u64::from(args.warmup),
                elapsed_ms: start.elapsed().as_secs_f64() * 1000.0,
                outcome: outcome.into(),
            });
        }
    }
    let total_elapsed_secs = measured_start.elapsed().as_secs_f64();
    responder.abort();
    let _ = responder.await;
    let after = pair.diagnostics().await;
    pair.stop().await;
    let successes = samples.iter().filter(|s| s.outcome == "ok").count();
    let rtt_ms = Distribution::new(
        samples
            .iter()
            .filter(|s| s.outcome == "ok")
            .map(|s| s.elapsed_ms),
    );
    let valid = successes == args.count as usize && warmup_errors == 0;
    let path = report::write(
        out_dir,
        "request-reply",
        json!({"topology":"same-host", "before":before, "after":after}),
        json!({"count":args.count,"payload_bytes":args.payload_bytes,"warmup":args.warmup,"timeout_secs":args.timeout_secs}),
        json!({"valid":valid,"successes":successes,"warmup_errors":warmup_errors,"total_elapsed_secs":total_elapsed_secs,"rtt_ms":rtt_ms,"samples":samples}),
    )?;
    eprintln!(
        "[request-reply] {successes}/{} succeeded; report {}",
        args.count,
        path.display()
    );
    if !valid {
        anyhow_lite::bail!("request/reply benchmark failed validation (see report)");
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn delayed_wrong_peer_and_corrupt_replies_do_not_count() {
        let expected = json!({"run":"a","sequence":2,"blob":"xxx"});
        let mut msg = NamespacedMessage {
            from: "peer".into(),
            namespace: "bench-rr".into(),
            msg_type: "pong".into(),
            payload: expected.clone(),
            timestamp: None,
        };
        assert!(matches_reply(&msg, "peer", &expected));
        msg.payload["sequence"] = json!(1);
        assert!(!matches_reply(&msg, "peer", &expected));
        msg.payload = expected.clone();
        msg.from = "imposter".into();
        assert!(!matches_reply(&msg, "peer", &expected));
        msg.from = "peer".into();
        msg.payload["blob"] = json!("corrupt");
        assert!(!matches_reply(&msg, "peer", &expected));
    }
}
