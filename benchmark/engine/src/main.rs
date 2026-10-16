//! `truffle-bench`: the CLI over the benchmark engine (RFC 027 §10).
//!
//! `serve`, `peers`, `run`, `pair`, `suite` and `report` drive the
//! [`truffle_bench`] library ([`cli`]). `transport` is the legacy,
//! tsnet-bench compatible workload kept binary-only in [`legacy`].
//!
//! `dotenvy` loads `.env` from the current directory upward, so the repo-root
//! `.env` supplies the auth key for local runs.

mod cli;
mod legacy;

use std::process::ExitCode;

use clap::Parser;

use cli::{Cli, Command};

#[tokio::main]
async fn main() -> ExitCode {
    // Load `.env` from CWD upward.
    let _ = dotenvy::dotenv();

    let cli = Cli::parse();

    // Install a default tracing subscriber. The legacy workload keeps its
    // old `info` default; the hub commands print their own progress, so core
    // logs only warnings unless TRUFFLE_TEST_LOG says otherwise.
    let default_filter = match cli.command {
        Command::Transport(_) => "truffle_core=info",
        _ => "truffle_core=warn,truffle_bench=warn",
    };
    let filter = std::env::var("TRUFFLE_TEST_LOG").unwrap_or_else(|_| default_filter.to_string());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .try_init();
    match cli.command {
        Command::Transport(args) => {
            let out_dir = cli
                .out_dir
                .as_deref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| cli::DEFAULT_OUT_DIR.to_string());
            let result = match legacy::require_authkey() {
                Ok(authkey) => legacy::transport::run(args, &authkey, &out_dir).await,
                Err(e) => Err(e),
            };
            match result {
                Ok(()) => ExitCode::SUCCESS,
                Err(e) => {
                    eprintln!("Error: {e}");
                    ExitCode::FAILURE
                }
            }
        }
        Command::Serve(args) => cli::serve(args).await,
        Command::Peers(args) => cli::peers(args).await,
        Command::Run(args) => cli::run(args, &cli.out_dir).await,
        Command::Pair(args) => cli::pair(args, &cli.out_dir).await,
        Command::Suite(args) => cli::suite(args, &cli.out_dir).await,
        Command::Report(args) => cli::report(args),
    }
}
