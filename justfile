# truffle — common developer tasks.
#
# Install `just` (https://github.com/casey/just) and run `just` to see the list.
# `just --list` shows the comment line directly above each recipe; notes go above it.

# Pass recipe arguments to the shell as "$@" as well, so `*ARGS` recipes can forward
# them verbatim ({{ARGS}} is re-split by sh, which mangles JSON and spaces).
set positional-arguments

default:
    @just --list

# The benchmark app (truffle-bench-app) is a GUI crate and stays out, as in CI.
# Keys are set empty, not unset: the live tests load .env via dotenvy, which never
# overrides an existing variable, and treat an empty key as missing (as run-all does).
# Run only the fast unit tests (no real-Tailscale tests).
test:
    TRUFFLE_TEST_AUTHKEY= TS_AUTHKEY= TRUFFLE_BENCH_AUTH_KEY= cargo test --locked --workspace --exclude truffle-bench-app

# Requires TRUFFLE_TEST_AUTHKEY (in .env or shell env).
# Run the real-network integration tests with extra logging.
test-real:
    TRUFFLE_TEST_LOG="truffle_core=debug,integration_network=debug,integration_synced_store=debug,integration_request_reply=debug,integration_file_transfer=debug" \
        cargo test --locked -p truffle-core --tests -- --nocapture

# CRITERION_HOME keeps output in this repo's target/criterion even when the
# cargo target dir is shared or redirected (bench binaries run in crates/truffle-core).
# Criterion micro benchmarks (no network).
bench-micro:
    CRITERION_HOME="{{justfile_directory()}}/target/criterion" cargo bench --locked -p truffle-core

# Requires an auth key (TRUFFLE_BENCH_AUTH_KEY or TRUFFLE_TEST_AUTHKEY, in .env or the shell).
# Profiles: smoke | standard | full | nightly. Example: `just bench-macro smoke`.
# Benchmark suite on a same-process pair (RFC 027).
bench-macro PROFILE="standard":
    cargo run --locked -p truffle-bench --release -- suite --profile {{PROFILE}} --pair

# Requires an auth key. Example: `just bench-one tcp --param streams=4`, or
# `just bench-one video --params-json '{"transport":"quic","profile":"1080p60"}'`.
# Run one workload on a same-process pair.
bench-one *ARGS:
    cargo run --locked -p truffle-bench --release -- pair "$@"

# Without a key it prints a login URL. Example: `just bench-serve --profile b`.
# Start a responder that other machines can benchmark against.
bench-serve *ARGS:
    cargo run --locked -p truffle-bench --release -- serve "$@"

# Run the truffle bench desktop app in development (profile "dev").
app-dev:
    pnpm --filter @vibecook/truffle-bench-app app:dev

# Bundle the truffle bench desktop app for this machine (builds the sidecar first).
app-build:
    pnpm --filter @vibecook/truffle-bench-app app:build

# Force-rebuild the test-sidecar binary (normally handled by build.rs).
build-test-sidecar:
    cd packages/sidecar-slim && go build -o ../../crates/truffle-core/tests/test-sidecar .

# Repo-relative paths only: the cargo target dir may be shared with other projects.
# Clean criterion + bench-results output.
bench-clean:
    rm -rf target/criterion target/bench-results

# Lint: clippy across the workspace (minus the GUI app), warnings as errors.
lint:
    cargo clippy --locked --workspace --all-targets --exclude truffle-bench-app -- -D warnings

# Format everything.
fmt:
    cargo fmt --all

# Run the secret scanner once over the current staged diff (useful before `git commit`).
check-secrets:
    scripts/check-for-secrets.sh
