# Measuring the Tailscale stack

Use the pinned Go toolchain and a release Rust build. Configure an ephemeral
`TRUFFLE_TEST_AUTHKEY` in the environment or the repository `.env`. Never put
keys in benchmark command lines or reports. The baseline Go command reads the
environment only. Use an isolated test tailnet or scoped test-node ACLs.

## Reproducible application workloads

```sh
cargo build --locked -p truffle-bench --release
cargo run --locked -p truffle-bench --release -- request-reply --count 500 --warmup 20
cargo run --locked -p truffle-bench --release -- file-transfer --size 10M --iterations 10 --warmup 2
cargo run --locked -p truffle-bench --release -- synced-store --scenario b2-concurrent --ops 1000
cargo run --locked -p truffle-bench --release -- transport --protocol tcp --bytes 10485760 --iterations 10
cargo run --locked -p truffle-bench --release -- transport --protocol quic --bytes 10485760 --iterations 10 --concurrency 4
```

These pair runs put two tsnet nodes on **one host**. They measure bridge and
application overhead, and provide useful smoke tests. They do not establish
WAN throughput, NAT traversal quality, or maximum Tailscale performance.

JSON schema 2 records the full source revision and tracked dirty state,
platform, logical CPUs, build mode, actual sidecar SHA-256 and Go build info,
workload parameters, and path probes before and after measurement. Keep the
exact source diff when benchmarking a dirty tree. Each output has a unique ID;
repeated runs never overwrite each other. Latencies are in milliseconds,
throughputs in decimal MB/s. Raw samples retain fractional precision; summary
quantiles use the nearest-rank method. Failed requests retain their outcome,
and incomplete runs exit unsuccessfully. Do not compare only successful-sample
percentiles without also checking failure counts and `valid`.

Request/reply validates the sender, run ID, sequence, and payload. Warmups are
excluded. File transfers validate both the returned and received SHA-256 and
byte counts; verification and output deletion occur outside the timed interval.
Store runs fail unless both required views converge. Their convergence polling
interval is 25 ms, so sub-25-ms comparisons are not meaningful.

## Two hosts and a native baseline

Choose a unique shared app ID. Start the server on host B, then the client on A:

```sh
# B
cargo run --locked -p truffle-bench --release -- transport --mode serve --protocol tcp --app-id bench-run42 --name server
# A
cargo run --locked -p truffle-bench --release -- transport --mode client --protocol tcp --app-id bench-run42 --peer server --iterations 20 --warmup 3 --concurrency 4
```

Repeat with `--protocol quic` on both ends. Separate-process mode is labeled
as such in the report: the caller must actually use different hosts. Each
sample opens a new connection, recording connection establishment separately
from transfer plus receiver acknowledgement. Concurrency creates independent
connections and stresses ephemeral socket allocation. Aggregate throughput
uses measured bytes divided by elapsed wall time for the complete measured
batches. Both client and server hash the payload during the timed transfer;
CPU cost therefore includes integrity validation. The server caps concurrent
connections and applies per-connection deadlines.

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
build info and raw Tailscale path probes.

Run at least five trials on idle, powered machines. Keep OS, CPU, toolchains,
sidecar hash, payload, concurrency, and placement constant. Exercise direct,
peer-relay, and DERP paths using controlled test network conditions, and verify
the probes describe the intended path. Probe snapshots cannot detect every
mid-run transition. Record network conditions alongside the results. Collect
CPU/RSS and profiles using the platform's profiler or `/usr/bin/time -l` on
macOS (`/usr/bin/time -v` on Linux); include both sidecar and Rust processes.
Do not attribute host-loopback or cached-file throughput to the physical link.
