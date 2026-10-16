# Measuring the Tailscale stack

How to measure truffle — the methodology, every workload, the report format and
the desktop app — now lives in [benchmark/README.md](../benchmark/README.md)
(RFC 027). This page keeps the commands for a release-build measurement and the
native Go/tsnet baseline, which has no Rust bridge.

Use the pinned Go toolchain and a release Rust build. Never put keys in
benchmark command lines or reports: `truffle-bench` reads `--auth-key-file`,
`TRUFFLE_BENCH_AUTH_KEY`, `TRUFFLE_TEST_AUTHKEY` or the repository `.env`; its
legacy `transport` subcommand reads only `TRUFFLE_TEST_AUTHKEY` (then
`TS_AUTHKEY`) from the environment or `.env`; and `tsnet-bench` reads
`TRUFFLE_TEST_AUTHKEY` from the environment only. Use an isolated test tailnet
or scoped test-node ACLs.

## Application workloads

```sh
cargo build --locked -p truffle-bench --release
# Every workload, two hubs in one process (needs a key)
cargo run --locked -p truffle-bench --release -- suite --profile standard --pair
# Single workloads with overrides (needs a key)
cargo run --locked -p truffle-bench --release -- pair ws --param count=500
cargo run --locked -p truffle-bench --release -- pair file-transfer --param size_bytes=10485760 --param iterations=10
cargo run --locked -p truffle-bench --release -- pair tcp --param streams=4
cargo run --locked -p truffle-bench --release -- pair quic --param streams=4
```

These pair runs put two tsnet nodes on **one host** (topology `same-process`).
They measure bridge and application overhead, and make useful smoke tests. They
do not establish WAN throughput, NAT traversal quality, or maximum Tailscale
performance. For a real link, run `truffle-bench serve` on one machine and
`truffle-bench run --peer <name> <workload>` (or the app) on the other; see
[benchmark/README.md](../benchmark/README.md).

## Two hosts and a native baseline

The legacy `transport` subcommand (schema 2) and `tsnet-bench` speak the same
length/payload/checksum protocol, so together they isolate the cost of
truffle's Rust bridge. Choose a unique shared app ID. Start the server on host
B, then the client on A:

```sh
# B
cargo run --locked -p truffle-bench --release -- transport --mode serve --protocol tcp --app-id bench-run42 --name server
# A
cargo run --locked -p truffle-bench --release -- transport --mode client --protocol tcp --app-id bench-run42 --peer server --iterations 20 --warmup 3 --concurrency 4
```

Repeat with `--protocol quic` on both ends. `transport` needs
`TRUFFLE_TEST_AUTHKEY` (or `TS_AUTHKEY`) in the environment or `.env`; it does
not read `TRUFFLE_BENCH_AUTH_KEY` and has no `--auth-key-file`. Separate-process mode is labeled as such in the report: the
caller must actually use different hosts. Each sample opens a new connection,
recording connection establishment separately from transfer plus receiver
acknowledgement. Concurrency creates independent connections and stresses
ephemeral socket allocation. Aggregate throughput uses measured bytes divided
by elapsed wall time for the complete measured batches. Both client and server
hash the payload during the timed transfer; CPU cost therefore includes
integrity validation. The server caps concurrent connections and applies
per-connection deadlines. Throughputs are decimal MB/s (the hub's importer
converts them to Mb/s and labels them `fixed_size_mbps`, a different method
from the v3 `goodput_mbps`).

Compare against Go/tsnet without the Rust bridge, using the same two hosts,
payload size, number of iterations and network conditions:

```sh
cd packages/sidecar-slim
# B; prints the ephemeral tailnet IP
 go run ./cmd/tsnet-bench -mode serve -name truffle-bench-wire-reference
# A; substitute B's printed IP
 go run ./cmd/tsnet-bench -mode client -addr 100.x.y.z:9421 -bytes 10485760 -iterations 20 -warmup 3 > native.json
```

The baseline uses the same length/payload/checksum protocol and payload bytes.
It runs sequential connections, so compare with Rust `--concurrency 1`.
`go run` compilation is outside the measured intervals; prebuild it when
collecting external process profiles. The native report includes actual module
build info and raw Tailscale path probes. `truffle-bench report native.json`
prints it, and the truffle bench app imports it as a "native Go/tsnet
baseline" next to v3 runs.

Run hygiene (idle powered machines, repeated trials, constant toolchains and
sidecar hash, checking which path carried the bytes) is in the methodology
section of [benchmark/README.md](../benchmark/README.md#methodology).
