//! Raw TCP/QUIC workloads, with a same-host smoke mode and separate host roles.
use crate::{
    anyhow_lite, harness,
    report::{self, Distribution},
};
use clap::{Args as ClapArgs, ValueEnum};
use serde::Serialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use truffle_core::{network::tailscale::TailscaleProvider, Node};

const MAX_BYTES: u64 = 1 << 30;
const CHUNK: usize = 64 * 1024;
#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Mode {
    Pair,
    Serve,
    Client,
}
#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
enum Protocol {
    Tcp,
    Quic,
}
#[derive(ClapArgs)]
pub struct Args {
    /// Pair runs on one host; serve/client run independently on two hosts.
    #[arg(long, value_enum, default_value_t=Mode::Pair)]
    mode: Mode,
    #[arg(long, value_enum, default_value_t=Protocol::Tcp)]
    protocol: Protocol,
    /// Use the same, unique app ID on both hosts.
    #[arg(long, default_value = "bench-wire")]
    app_id: String,
    /// The server's advertised device name (or the client's own name).
    #[arg(long)]
    name: Option<String>,
    /// Server device name/IP, required in client mode.
    #[arg(long, required_if_eq("mode", "client"))]
    peer: Option<String>,
    #[arg(long, default_value_t=9421, value_parser=clap::value_parser!(u16).range(1..))]
    port: u16,
    #[arg(long, default_value_t=10_485_760, value_parser=clap::value_parser!(u64).range(1..=MAX_BYTES))]
    bytes: u64,
    #[arg(long, default_value_t=5, value_parser=clap::value_parser!(u32).range(1..=10_000))]
    iterations: u32,
    #[arg(long, default_value_t=1, value_parser=clap::value_parser!(u32).range(0..=1000))]
    warmup: u32,
    #[arg(long, default_value_t=1, value_parser=clap::value_parser!(u32).range(1..=64))]
    concurrency: u32,
    #[arg(long, default_value_t=120, value_parser=clap::value_parser!(u64).range(1..=3600))]
    timeout_secs: u64,
}
#[derive(Debug, Serialize)]
struct Sample {
    connect_ms: f64,
    transfer_ms: f64,
    bytes: u64,
    mb_per_sec: f64,
}

pub async fn run(args: Args, authkey: &str, out_dir: &str) -> anyhow_lite::Result {
    if args.port == 9417 {
        anyhow_lite::bail!("port 9417 is reserved for discovery");
    }
    if matches!(args.mode, Mode::Pair) {
        let pair = harness::make_bench_pair(authkey)
            .await
            .map_err(anyhow_lite::Error)?;
        let server = match start_server(
            pair.beta.clone(),
            args.protocol,
            args.port,
            args.timeout_secs,
        )
        .await
        {
            Ok(server) => server,
            Err(e) => {
                pair.stop().await;
                return Err(e);
            }
        };
        let result = measure(
            &args,
            pair.alpha.clone(),
            &pair.beta_device_id,
            "same-host",
            out_dir,
        )
        .await;
        server.abort();
        let _ = server.await;
        pair.stop().await;
        return result;
    }
    let name = args
        .name
        .clone()
        .unwrap_or_else(|| format!("bench-{}", uuid::Uuid::new_v4().simple()));
    let (node, _state) = harness::make_bench_node(authkey, &args.app_id, &name)
        .await
        .map_err(anyhow_lite::Error)?;
    let result = async {
        match args.mode {
            Mode::Serve => {
                let server =
                    start_server(node.clone(), args.protocol, args.port, args.timeout_secs).await?;
                eprintln!(
                    "Ready: peer={name}, IP={:?}, port={}; Ctrl-C to stop",
                    node.local_info().ip,
                    args.port
                );
                let result = tokio::signal::ctrl_c()
                    .await
                    .map_err(anyhow_lite::Error::from);
                server.abort();
                let _ = server.await;
                result
            }
            Mode::Client => {
                measure(
                    &args,
                    node.clone(),
                    args.peer
                        .as_deref()
                        .ok_or_else(|| anyhow_lite::Error("--peer required".into()))?,
                    "separate-processes",
                    out_dir,
                )
                .await
            }
            Mode::Pair => unreachable!(),
        }
    }
    .await;
    node.stop().await;
    result
}

async fn start_server(
    node: Arc<Node<TailscaleProvider>>,
    protocol: Protocol,
    port: u16,
    timeout_secs: u64,
) -> anyhow_lite::Result<tokio::task::JoinHandle<()>> {
    // Bind before returning so measurement never races listener setup.
    match protocol {
        Protocol::Tcp => {
            let mut listener = node.listen_tcp(port).await?;
            Ok(tokio::spawn(async move {
                let mut tasks = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        incoming = listener.accept(), if tasks.len()<64 => {
                            let Some(incoming) = incoming else {break};
                            tasks.spawn(async move {
                                let (r,w)=incoming.stream.into_split();
                                let _ = tokio::time::timeout(Duration::from_secs(timeout_secs), receive(r,w)).await;
                            });
                        },
                        _ = tasks.join_next(), if !tasks.is_empty() => {},
                    }
                }
            }))
        }
        Protocol::Quic => {
            let listener = node.listen_quic(port).await?;
            Ok(tokio::spawn(async move {
                let mut tasks = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        incoming = listener.accept(), if tasks.len()<64 => {
                            let Some(conn) = incoming else {break};
                            tasks.spawn(async move {
                                let _ = tokio::time::timeout(Duration::from_secs(timeout_secs), async {
                                    if let Ok(Some(stream)) = conn.accept_stream().await {
                                        let (w,r)=stream.into_split();
                                        let _ = receive(r,w).await;
                                    }
                                    // Keep the connection alive until the client receives its ACK.
                                    while let Ok(Some(_)) = conn.accept_stream().await {}
                                }).await;
                            });
                        },
                        _ = tasks.join_next(), if !tasks.is_empty() => {},
                    }
                }
            }))
        }
    }
}

async fn measure(
    args: &Args,
    node: Arc<Node<TailscaleProvider>>,
    peer: &str,
    topology: &str,
    out_dir: &str,
) -> anyhow_lite::Result {
    tokio::time::timeout(Duration::from_secs(60), async {
        while node.resolve_peer_ip(peer).await.is_err() {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await?;
    let before = harness::diagnostics(&node, peer, topology).await;
    let mut samples = Vec::new();
    let mut errors = Vec::new();
    let mut wall = Instant::now();
    for iteration in 0..u64::from(args.iterations) + u64::from(args.warmup) {
        if iteration == u64::from(args.warmup) {
            wall = Instant::now();
        }
        let mut group = tokio::task::JoinSet::new();
        for _ in 0..args.concurrency {
            let node = node.clone();
            let peer = peer.to_string();
            let (protocol, port, bytes, secs) =
                (args.protocol, args.port, args.bytes, args.timeout_secs);
            group.spawn(async move {
                tokio::time::timeout(
                    Duration::from_secs(secs),
                    sample(&node, &peer, protocol, port, bytes),
                )
                .await
            });
        }
        while let Some(result) = group.join_next().await {
            match result {
                Ok(Ok(Ok(value))) if iteration >= u64::from(args.warmup) => samples.push(value),
                Ok(Ok(Ok(_))) => {}
                other => errors.push(format!("iteration {iteration}: {other:?}")),
            }
        }
    }
    let elapsed = wall.elapsed().as_secs_f64();
    let after = harness::diagnostics(&node, peer, topology).await;
    let valid =
        errors.is_empty() && samples.len() == args.iterations as usize * args.concurrency as usize;
    let path = report::write(
        out_dir,
        "transport",
        json!({"topology":topology,"before":before,"after":after}),
        json!({"protocol":args.protocol,"bytes":args.bytes,"iterations":args.iterations,"warmup":args.warmup,"concurrency":args.concurrency,"timeout_secs":args.timeout_secs}),
        json!({"valid":valid,"samples":samples,"errors":errors,"elapsed_secs":elapsed,
            "aggregate_mb_per_sec":samples.iter().map(|s|s.bytes as f64).sum::<f64>()/1e6/elapsed,
            "connect_ms":Distribution::new(samples.iter().map(|s|s.connect_ms)),
            "transfer_ms":Distribution::new(samples.iter().map(|s|s.transfer_ms)),
            "mb_per_sec":Distribution::new(samples.iter().map(|s|s.mb_per_sec))}),
    )?;
    eprintln!("[transport] report {}", path.display());
    if !valid {
        anyhow_lite::bail!("transport benchmark failed validation (see report)");
    }
    Ok(())
}
async fn sample(
    node: &Node<TailscaleProvider>,
    peer: &str,
    protocol: Protocol,
    port: u16,
    bytes: u64,
) -> anyhow_lite::Result<Sample> {
    let start = Instant::now();
    match protocol {
        Protocol::Tcp => {
            let stream = node.open_tcp(peer, port).await?;
            let connect_ms = start.elapsed().as_secs_f64() * 1000.0;
            stream.set_nodelay(true)?;
            let (r, w) = stream.into_split();
            send(r, w, bytes, connect_ms).await
        }
        Protocol::Quic => {
            let conn = node.connect_quic(peer, port).await?;
            let connect_ms = start.elapsed().as_secs_f64() * 1000.0;
            let (w, r) = conn.open_stream().await?.into_split();
            let result = send(r, w, bytes, connect_ms).await;
            conn.close();
            result
        }
    }
}
// Protocol shared with cmd/tsnet-bench: u64 BE size, payload, then SHA-256 ACK.
async fn receive<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut r: R,
    mut w: W,
) -> std::io::Result<()> {
    let size = r.read_u64().await?;
    if size == 0 || size > MAX_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid payload size",
        ));
    }
    let mut left = size;
    let mut buf = vec![0u8; CHUNK];
    let mut hash = Sha256::new();
    while left > 0 {
        let n = (left as usize).min(buf.len());
        r.read_exact(&mut buf[..n]).await?;
        hash.update(&buf[..n]);
        left -= n as u64;
    }
    w.write_all(&hash.finalize()).await?;
    w.flush().await?;
    Ok(())
}
async fn send<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut r: R,
    mut w: W,
    size: u64,
    connect_ms: f64,
) -> anyhow_lite::Result<Sample> {
    let buf = vec![0x5a; CHUNK];
    let mut hash = Sha256::new();
    let start = Instant::now();
    w.write_u64(size).await?;
    let mut left = size;
    while left > 0 {
        let n = (left as usize).min(buf.len());
        w.write_all(&buf[..n]).await?;
        hash.update(&buf[..n]);
        left -= n as u64;
    }
    w.flush().await?;
    let mut ack = [0u8; 32];
    r.read_exact(&mut ack).await?;
    if hash.finalize().as_slice() != ack {
        anyhow_lite::bail!("receiver checksum mismatch");
    }
    let seconds = start.elapsed().as_secs_f64();
    Ok(Sample {
        connect_ms,
        transfer_ms: seconds * 1000.0,
        bytes: size,
        mb_per_sec: size as f64 / 1e6 / seconds,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn transfer_requires_complete_validated_receiver_ack() {
        let (a, b) = tokio::io::duplex(1024);
        let (ar, aw) = tokio::io::split(a);
        let (br, bw) = tokio::io::split(b);
        let (sent, received) = tokio::join!(send(ar, aw, CHUNK as u64 + 17, 0.0), receive(br, bw));
        assert_eq!(sent.unwrap().bytes, CHUNK as u64 + 17);
        received.unwrap();
        let (a, mut b) = tokio::io::duplex(1024);
        let (r, w) = tokio::io::split(a);
        let corrupt = tokio::spawn(async move {
            let mut buf = [0; 9];
            b.read_exact(&mut buf).await.unwrap();
            b.write_all(&[0; 32]).await.unwrap();
        });
        assert!(send(r, w, 1, 0.0).await.is_err());
        corrupt.await.unwrap();
    }
    #[tokio::test]
    async fn partial_transfer_is_not_success() {
        let (mut a, b) = tokio::io::duplex(1024);
        a.write_u64(5).await.unwrap();
        a.write_all(&[1, 2]).await.unwrap();
        drop(a);
        let (r, w) = tokio::io::split(b);
        assert!(receive(r, w).await.is_err());
    }
}
