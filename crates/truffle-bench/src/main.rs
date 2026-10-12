//! truffle-bench — real-network macro benchmarks for truffle-core.
//!
//! See RFC 019 §10.2. Spins up a pair of ephemeral tsnet nodes on a real
//! tailnet and drives workloads through the Node API. Emits structured JSON
//! to `target/bench-results/`.
//!
//! Requires `TRUFFLE_TEST_AUTHKEY` in the env (or in `.env` at the repo root).

mod harness;
mod report;
mod workloads;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "truffle-bench",
    about = "Real-network macro benchmarks for truffle-core",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Where to write bench result JSON. Defaults to `target/bench-results/`.
    #[arg(long, global = true)]
    out_dir: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    /// File transfer workload: send a file from alpha → beta and measure throughput.
    FileTransfer(workloads::file_transfer::Args),
    /// Raw TCP/QUIC throughput with pair, server and client modes.
    Transport(workloads::transport::Args),
    /// SyncedStore convergence workload (B1 sequential / B2 concurrent).
    SyncedStore(workloads::synced_store::Args),
    /// Request/reply RTT workload across the pair.
    RequestReply(workloads::request_reply::Args),
}

#[tokio::main]
async fn main() -> anyhow_lite::Result {
    // Load `.env` from CWD upward.
    let _ = dotenvy::dotenv();

    // Install a default tracing subscriber.
    let filter =
        std::env::var("TRUFFLE_TEST_LOG").unwrap_or_else(|_| "truffle_core=info".to_string());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .try_init();

    let cli = Cli::parse();
    let out_dir = cli
        .out_dir
        .unwrap_or_else(|| "target/bench-results".to_string());

    // Enforce that a real-network key is present — this is a benchmark
    // command, not a test, so silent skip would hide misconfiguration.
    let authkey = require_authkey()?;

    match cli.command {
        Command::Transport(args) => workloads::transport::run(args, &authkey, &out_dir).await?,
        Command::FileTransfer(args) => {
            workloads::file_transfer::run(args, &authkey, &out_dir).await?;
        }
        Command::SyncedStore(args) => {
            workloads::synced_store::run(args, &authkey, &out_dir).await?;
        }
        Command::RequestReply(args) => {
            workloads::request_reply::run(args, &authkey, &out_dir).await?;
        }
    }

    Ok(())
}

fn require_authkey() -> anyhow_lite::Result<String> {
    if let Ok(k) = std::env::var("TRUFFLE_TEST_AUTHKEY") {
        if !k.is_empty() {
            return Ok(k);
        }
    }
    if let Ok(k) = std::env::var("TS_AUTHKEY") {
        if !k.is_empty() {
            eprintln!("[warn] using TS_AUTHKEY; TRUFFLE_TEST_AUTHKEY is preferred");
            return Ok(k);
        }
    }
    anyhow_lite::bail!(
        "TRUFFLE_TEST_AUTHKEY is not set.\n\
         \n\
         truffle-bench runs against a real Tailscale network and needs a valid auth key.\n\
         See docs/TESTING.md for setup, or set the env var directly:\n\
             TRUFFLE_TEST_AUTHKEY=tskey-... cargo run -p truffle-bench -- <subcommand>"
    );
}

/// Tiny error-type shim so the crate doesn't depend on `anyhow` directly.
mod anyhow_lite {
    pub type Result<T = ()> = std::result::Result<T, Error>;

    pub struct Error(pub String);

    impl std::fmt::Display for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.0)
        }
    }
    impl std::fmt::Debug for Error {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.0)
        }
    }
    impl<E: std::error::Error> From<E> for Error {
        fn from(e: E) -> Self {
            Error(e.to_string())
        }
    }

    #[macro_export]
    macro_rules! bail {
        ($($arg:tt)*) => {
            return Err($crate::anyhow_lite::Error(format!($($arg)*)))
        };
    }
    pub(crate) use bail;
}
