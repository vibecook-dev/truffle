// Types shared with the engine (RFC 027 §7) and the app commands (§11.2).
//
// Rust is the source of truth: every type here mirrors a serde struct in `truffle_bench`
// (snake_case fields; `Option<T>` is `T | null`). `benchmark/schema/fixtures/*.json` holds a
// canonical example of each, and `fixtures.test.ts` checks them against these types with
// `satisfies`, so drift fails `tsc` and CI. Keep field names and unions exactly as in the RFC.

// ---------------------------------------------------------------- constants (§5.2)

export const APP_ID = 'truffle-bench';
/** Control protocol version this app speaks. A peer whose hello says otherwise cannot be measured. */
export const PROTOCOL = 1;

// ---------------------------------------------------------------- errors (§5.2)

export type ErrorCode =
  | 'not-running'
  | 'peer-not-found'
  | 'peer-ambiguous'
  | 'peer-offline'
  | 'login-refused'
  | 'busy'
  | 'being-measured'
  | 'protocol-mismatch'
  | 'unsupported-workload'
  | 'invalid-params'
  | 'timeout'
  | 'state-in-use'
  | 'sidecar-not-found'
  | 'cancelled'
  | 'io'
  | 'internal';

/** Every command rejects with this shape. */
export interface AppError {
  code: ErrorCode;
  message: string;
}

// ---------------------------------------------------------------- node, peers, events (§7.1)

export interface HealthSummary {
  state: string;
  healthy: boolean;
  warnings: string[];
  key_expiry: string | null;
}

export interface LocalInfo {
  device_name: string;
  device_id: string;
  tailscale_id: string;
  tailscale_hostname: string;
  tailscale_ip: string | null;
  dns_name: string | null;
  /** Derived from dns_name. */
  tailnet: string | null;
  login_name: string | null;
  app_id: string;
  health: HealthSummary;
  sidecar_path: string;
  hub_start_ms: number | null;
}

/** §6: what a peer says about itself in `hello.ack`. */
export interface PeerHello {
  protocol: number;
  /** "truffle-bench-app" | "truffle-bench-cli" */
  app: string;
  tool_version: string;
  truffle_version: string;
  build_profile: BuildProfile;
  git_sha: string | null;
  sidecar_sha256: string | null;
  device_name: string;
  device_id: string | null;
  os: string;
  arch: string;
  logical_cpus: number | null;
  login_name: string | null;
  /** Omitted (null) for peers that are not admitted. */
  host_fingerprint: string | null;
  host_fingerprint_weak: boolean;
  workloads: string[];
  accepts_runs_from_you: boolean;
  /** Set while the peer is in a data-plane run (its own description of what it is doing). */
  busy: string | null;
}

export interface BenchPeer {
  /** tailscale_id: the only peer key used for runs. */
  id: string;
  device_id: string | null;
  device_name: string;
  hostname: string;
  ip: string;
  os: string | null;
  online: boolean;
  ws_connected: boolean;
  /** "direct" | "relay:<region>" | "peer-relay:…" | "unknown", as reported by Tailscale. */
  connection: string;
  login_name: string | null;
  same_login: boolean;
  tagged: boolean;
  last_seen: string | null;
  hello: PeerHello | null;
  hello_error: AppError | null;
  rtt_ms: number | null;
  /**
   * The engine's verdict from both host fingerprints: true when both are strong and equal, false
   * when they differ (strong, or both weak), null when it can't tell (no hello, or one weak).
   */
  same_host: boolean | null;
}

export interface PathSnapshot {
  at: string;
  connection: string | null;
  rtt_ms: number | null;
  endpoint: string | null;
  error: string | null;
}

export interface PeerProbe {
  peer: BenchPeer;
  hello: PeerHello;
  path: PathSnapshot;
}

export type NodeStateName =
  | 'idle'
  | 'starting'
  | 'needs-login'
  /** The tailnet requires device approval and an admin has not approved this device yet. */
  | 'needs-approval'
  | 'running'
  | 'stopping'
  | 'stopped'
  | 'error';

export interface NodeState {
  state: NodeStateName;
  auth_url: string | null;
  error: AppError | null;
  local: LocalInfo | null;
  tailscale_hostname: string | null;
  /** When the hub entered this state, in Unix milliseconds. */
  since_ms: number;
}

export type RemoteRunState = 'prepared' | 'finished' | 'cancelled' | 'expired';

/** `#[serde(tag = "type", rename_all = "kebab-case")]`; `Node(NodeState)` flattens into the tag object. */
export type HubEvent =
  | ({ type: 'node' } & NodeState)
  | { type: 'peers'; peers: BenchPeer[] }
  | {
      type: 'remote-run';
      run_id: string;
      /** Device name of the peer measuring this device. */
      peer: string;
      peer_id: string;
      workload: string;
      state: RemoteRunState;
    };

export interface ActiveRun {
  run_id: string;
  suite_id: string | null;
  workload: string;
  peer_name: string;
  started_at: string;
  last: Progress | null;
  /** The suite's profile id (suite items only), so a reload can name the suite. */
  suite_profile: string | null;
  /** This item's index in the suite, from 0 (suite items only). */
  suite_index: number | null;
  /** How many items the suite has (suite items only). */
  suite_total: number | null;
}

// ---------------------------------------------------------------- progress (§7.2)

export type Phase =
  | 'preparing'
  | 'settling'
  | 'clock'
  | 'warmup'
  | 'running'
  | 'draining'
  | 'finishing'
  | 'done'
  | 'failed'
  | 'cancelled';

export interface Sample {
  key: string;
  value: number;
}

/**
 * Live sample key of the path sampler's latest round trip (ms), set during every time-based run
 * (the engine's `common::KEY_LOADED_RTT`). Its summary counterpart is `loaded_rtt_p50_ms`.
 */
export const KEY_LOADED_RTT = 'loaded_rtt_ms';

export interface Progress {
  run_id: string;
  suite_id: string | null;
  t_ms: number;
  phase: Phase;
  fraction: number | null;
  samples: Sample[];
  message: string | null;
}

export interface SuiteProgress {
  suite_id: string;
  index: number;
  total: number;
  item_label: string;
  run: Progress;
}

export interface SuiteResult {
  suite_id: string;
  run_ids: string[];
  failed: number;
  cancelled: boolean;
}

// ---------------------------------------------------------------- workload parameters (§7.3)

export type Direction = 'upload' | 'download';
export type Direction3 = 'upload' | 'download' | 'bidirectional';

export interface PathParams {
  count: number;
  interval_ms: number;
}

export interface TcpParams {
  mode: 'throughput' | 'latency';
  direction: Direction3;
  streams: number;
  duration_secs: number;
  warmup_secs: number;
  payload_bytes: number;
  count: number;
  warmup_count: number;
  nodelay: boolean;
}

export interface UdpParams {
  mode: 'stream' | 'echo' | 'ramp';
  direction: Direction;
  payload_bytes: number;
  rate_mbps: number;
  pps: number;
  duration_secs: number;
  warmup_secs: number;
  warmup_count: number;
}

export interface QuicParams {
  mode: 'throughput' | 'latency';
  direction: Direction3;
  streams: number;
  duration_secs: number;
  warmup_secs: number;
  payload_bytes: number;
  count: number;
  warmup_count: number;
}

export interface WsParams {
  mode: 'rr' | 'stream';
  payload_bytes: number;
  count: number;
  concurrency: number;
  warmup_count: number;
  duration_secs: number;
  /** null: as fast as backpressure allows. */
  rate_per_sec: number | null;
}

export interface FileTransferParams {
  size_bytes: number;
  iterations: number;
}

export interface StoreParams {
  mode: 'pingpong' | 'burst';
  count: number;
  payload_bytes: number;
  warmup_count: number;
}

export interface HttpParams {
  target: 'proxy' | 'direct';
  connection: 'keepalive' | 'new';
  concurrency: number;
  duration_secs: number;
  warmup_secs: number;
  response_bytes: number;
}

export type VideoProfile = '720p30' | '1080p30' | '1080p60' | '4k30';

export interface VideoParams {
  transport: 'udp' | 'quic' | 'tcp';
  profile: VideoProfile;
  /** null: the profile's bitrate. */
  bitrate_mbps: number | null;
  /** null: the profile's frame rate. */
  fps: number | null;
  gop: number;
  keyframe_ratio: number;
  duration_secs: number;
  warmup_secs: number;
  pacing: 'paced' | 'burst';
  jitter_buffer_ms: number;
}

export interface EgressParams {
  direction: Direction;
  streams: number;
  duration_secs: number;
  warmup_secs: number;
}

/** `#[serde(tag = "workload", rename_all = "kebab-case")]` */
export type WorkloadParams =
  | ({ workload: 'path' } & PathParams)
  | ({ workload: 'tcp' } & TcpParams)
  | ({ workload: 'udp' } & UdpParams)
  | ({ workload: 'quic' } & QuicParams)
  | ({ workload: 'ws' } & WsParams)
  | ({ workload: 'file-transfer' } & FileTransferParams)
  | ({ workload: 'synced-store' } & StoreParams)
  | ({ workload: 'http' } & HttpParams)
  | ({ workload: 'video' } & VideoParams)
  | ({ workload: 'egress' } & EgressParams);

export type WorkloadKind = WorkloadParams['workload'];

/** The params variant for one workload. */
export type ParamsOf<K extends WorkloadKind> = Extract<WorkloadParams, { workload: K }>;

export const WORKLOADS: readonly WorkloadKind[] = [
  'path',
  'tcp',
  'udp',
  'quic',
  'ws',
  'file-transfer',
  'synced-store',
  'http',
  'video',
  'egress',
];

// ---------------------------------------------------------------- report, schema v3 (§7.4)

export type RunStatus = 'ok' | 'failed' | 'cancelled';
export type Tool = 'truffle-bench-app' | 'truffle-bench-cli' | 'imported';
export type Topology = 'same-process' | 'same-host' | 'separate-hosts';
export type PathLabel = 'direct' | 'relay' | 'mixed' | 'unknown';
/** `unknown` only in imported v1 reports, which recorded no build profile. */
export type BuildProfile = 'debug' | 'release' | 'unknown';

export interface NodeEnv {
  device_name: string;
  os: string;
  arch: string;
  logical_cpus: number | null;
  tool: string;
  tool_version: string;
  truffle_version: string;
  build_profile: BuildProfile;
  rustc: string | null;
  git_sha: string | null;
  git_dirty: boolean | null;
  sidecar_path: string | null;
  sidecar_sha256: string | null;
  tailscale_ip: string | null;
  host_fingerprint: string | null;
  host_fingerprint_weak: boolean;
  /** macOS `pmset -g batt`; null elsewhere. */
  on_battery: boolean | null;
  /** App only: share of the run with the window key. */
  app_foreground_pct: number | null;
}

export interface ClockOffset {
  /** peer − local */
  offset_ns: number;
  delay_ns: number;
  uncertainty_ns: number;
  at_ns: number;
  samples: number;
}

export interface ClockInfo {
  before: ClockOffset;
  after: ClockOffset | null;
  drift_ppm: number | null;
  uncertainty_ns: number;
}

export interface PathPoint {
  t_ms: number;
  connection: string | null;
  rtt_ms: number | null;
}

/** Sum of both sides' usermetrics deltas, in bytes. */
export interface PathBytes {
  direct: number;
  derp: number;
  peer_relay: number;
}

/** Nearest-rank percentiles; a percentile is null until n is large enough (§7.4). */
export interface Distribution {
  samples: number;
  lost: number;
  min: number | null;
  mean: number | null;
  p50: number | null;
  p90: number | null;
  p95: number | null;
  p99: number | null;
  p999: number | null;
  max: number | null;
  stddev: number | null;
}

export interface NetworkInfo {
  topology: Topology;
  settle_ms: number;
  before: PathSnapshot;
  after: PathSnapshot;
  clock: ClockInfo | null;
  path_timeline: PathPoint[];
  loaded_rtt_ms: Distribution | null;
  path_bytes: PathBytes | null;
  derp_share: number | null;
  path_label: PathLabel;
}

export interface SummaryItem {
  key: string;
  label: string;
  value: number | null;
  unit: string;
  better: 'higher' | 'lower';
  uncertainty: number | null;
}

export interface Series {
  key: string;
  label: string;
  unit: string;
  /** [t_ms, value] */
  points: [number, number][];
  warmup_ms: number | null;
}

export interface ResourceStats {
  cpu_ms: number;
  /** 100 = one logical CPU. */
  cpu_pct_avg: number;
  cpu_pct_max: number;
  rss_mb_max: number;
  cpu_ms_per_mb: number | null;
  sidecar_cpu_ms: number | null;
  sidecar_cpu_pct_avg: number | null;
  sidecar_cpu_pct_max: number | null;
  sidecar_rss_mb_max: number | null;
  sidecar_cpu_ms_per_mb: number | null;
}

export interface Resources {
  local: ResourceStats | null;
  peer: ResourceStats | null;
  combined: ResourceStats | null;
}

// ---------------------------------------------------------------- workload results (§8)
//
// `RunReport.results` per workload, mirroring the engine's `<Workload>Results` types in
// benchmark/schema/report.v3.schema.json. tcp, udp, quic, ws and synced-store tag theirs with
// `mode` (a serde internally tagged enum); path, file-transfer, http, video and egress are flat.
// Every field is always present: an `Option` arrives as null, never missing.

/** The receiver's run-level accounting (§8.1): payload bytes per 250 ms bucket. */
export interface ReceiverStats {
  epoch_ns: number;
  bucket_ms: number;
  buckets: number[];
  last_ns: number;
  bytes_total: number;
  streams: StreamStats[];
}

export interface StreamStats {
  stream_idx: number;
  bytes: number;
  first_ns: number;
  last_ns: number;
}

/** One disco ping of a path run. */
export interface PathPingSample {
  i: number;
  t_ms: number;
  connection: string | null;
  rtt_ms: number | null;
  endpoint: string | null;
  error: string | null;
}

export interface PathTransition {
  i: number;
  t_ms: number;
  from: string;
  to: string;
}

export interface PathResults {
  sent: number;
  lost: number;
  loss_pct: number | null;
  /** Ping 0, kept out of the distribution. */
  first_rtt_ms: number | null;
  rtt_ms: Distribution;
  samples: PathPingSample[];
  /** connection → pings that took it */
  connections: Record<string, number>;
  transitions: PathTransition[];
  hub_start_ms: number | null;
  discovery_ms: number | null;
  ws_connect_ms: number | null;
}

/** One stream of a tcp (or egress) throughput run. */
export interface TcpStreamResult {
  stream_idx: number;
  direction: Direction;
  connect_ms: number;
  sent_bytes: number;
  received_bytes: number;
  acked_bytes: number;
}

/** One direction of a bidirectional run. */
export interface TcpDirectionResult {
  goodput_mbps: number;
  window_bytes: number;
  receiver: ReceiverStats;
}

interface TcpThroughputFields {
  direction: Direction3;
  streams: TcpStreamResult[];
  bytes_total: number;
  received_bytes: number;
  sent_bytes: number;
  goodput_bytes: number;
  window_ms: number;
  /** Over streams. */
  connect_ms: Distribution;
  /** One-way runs. */
  receiver: ReceiverStats | null;
  /** Bidirectional runs. */
  upload: TcpDirectionResult | null;
  download: TcpDirectionResult | null;
  foreign_dropped: number;
  nodelay: boolean;
}

export interface TcpThroughputResults extends TcpThroughputFields {
  mode: 'throughput';
}

export interface TcpLatencyResults {
  mode: 'latency';
  payload_bytes: number;
  count: number;
  warmup_count: number;
  nodelay: boolean;
  connect_ms: number;
  rtt_ms: Distribution;
  samples_ms: number[];
  completed: number;
  sent_bytes: number;
  received_bytes: number;
  echoed: number;
  foreign_dropped: number;
}

export type TcpResults = TcpThroughputResults | TcpLatencyResults;

export interface UdpBucketLoss {
  missing: number;
  sent: number;
}

/** Where datagrams were lost, from usermetrics packet counts (an approximation). */
export interface UdpLossLocality {
  local_send_drops: number;
  network: number;
  local_recv_drops: number;
  packets_per_datagram: number;
}

interface UdpStreamFields {
  direction: Direction;
  payload_bytes: number;
  target_mbps: number;
  sent: number;
  unique: number;
  missing: number;
  dup: number;
  reordered: number;
  out_of_range: number;
  bad_datagrams: number;
  send_errors: number;
  loss_pct: number | null;
  max_loss_burst: number;
  jitter_ms: number | null;
  goodput_mbps: number | null;
  offered_mbps: number | null;
  rate_achieved: boolean;
  max_burst_datagrams: number;
  pacer_late_ms_mean: number | null;
  sent_bytes: number;
  received_bytes: number;
  goodput_bytes: number;
  window_ms: number;
  bucket_first_seq: number[];
  bucket_loss: UdpBucketLoss[];
  receiver: ReceiverStats;
  loss_locality: UdpLossLocality | null;
  drain_ms: number;
  drained_by: string;
  rcvbuf_bytes: number | null;
  foreign_dropped: number;
}

export interface UdpStreamResults extends UdpStreamFields {
  mode: 'stream';
}

export interface UdpRampStep {
  rate_mbps: number;
  offered_mbps: number | null;
  first_seq: number;
  end_seq: number;
  start_ms: number;
  end_ms: number;
  complete: boolean;
  achieved: boolean;
  loss_pct: number | null;
  sustained: boolean;
}

export interface UdpRampResults extends UdpStreamFields {
  mode: 'ramp';
  steps: UdpRampStep[];
  stop_reason: string;
  /** The last step's rate at ≤ 2% loss; only a lower bound when `limit_found` is false. */
  max_sustainable_mbps: number | null;
  /**
   * A step lost more than 2% or missed its rate (or the sender gave out). When false the run's
   * length capped the ramp: the summary's `max_sustainable_mbps` is null and the results value
   * is a lower bound.
   */
  limit_found: boolean;
}

/** What the echoing responder saw. */
export interface UdpEchoStats {
  received: number;
  echoed: number;
  echo_errors: number;
  before_start: number;
}

export interface UdpEchoResults {
  mode: 'echo';
  payload_bytes: number;
  pps: number;
  warmup_count: number;
  sent: number;
  completed: number;
  lost: number;
  loss_pct: number | null;
  dup: number;
  late: number;
  send_errors: number;
  rtt_ms: Distribution;
  samples_ms: number[];
  offered_pps: number | null;
  drain_ms: number;
  responder: UdpEchoStats;
  rcvbuf_bytes: number | null;
  foreign_dropped: number;
}

export type UdpResults = UdpStreamResults | UdpEchoResults | UdpRampResults;

/** `TBN1` stream kinds (§6 data plane). */
export type StreamKind = 'upload' | 'download' | 'echo' | 'frames';

export interface QuicStreamResult {
  stream_idx: number;
  kind: StreamKind;
  sent_bytes: number;
  received_bytes: number;
  acked_bytes: number | null;
}

export interface QuicThroughputResults {
  mode: 'throughput';
  direction: Direction3;
  /** The stream count; the per-stream rows are `stream_results`. */
  streams: number;
  connect_ms: number;
  goodput_mbps: number;
  upload_mbps: number | null;
  download_mbps: number | null;
  window_bytes: number;
  window_ms: number;
  bytes_total: number;
  received_bytes: number;
  sent_bytes: number;
  foreign_dropped: number;
  flow_control_ceiling_mbps: number | null;
  receiver: ReceiverStats;
  receiver_download: ReceiverStats | null;
  stream_results: QuicStreamResult[];
}

export interface QuicLatencyResults {
  mode: 'latency';
  connect_ms: number;
  payload_bytes: number;
  count: number;
  warmup_count: number;
  completed: number;
  rtt_ms: Distribution;
  samples_ms: number[];
  echoes: number;
  foreign_dropped: number;
}

export type QuicResults = QuicThroughputResults | QuicLatencyResults;

export interface WsRrResults {
  mode: 'rr';
  payload_bytes: number;
  count: number;
  warmup_count: number;
  concurrency: number;
  sent: number;
  completed: number;
  timeouts: number;
  rtt_ms: Distribution;
  samples_ms: number[];
  msgs_per_sec: number | null;
  window_ms: number;
  wire_bytes_per_msg: number;
  lag_dropped: number;
  peer_lag_dropped: number;
  bad_replies: number;
}

export interface WsStreamResults {
  mode: 'stream';
  payload_bytes: number;
  duration_secs: number;
  rate_per_sec: number | null;
  sent: number;
  unique: number;
  missing: number;
  dup: number;
  reordered: number;
  /** Σ Lagged(n) on the receiver's subscription. */
  lag_dropped: number;
  /** missing − lag_dropped: lost before the subscription (the envelope router). */
  router_dropped: number;
  drop_pct: number | null;
  msgs_per_sec: number;
  goodput_mbps: number;
  sent_per_sec: number;
  wire_bytes_per_msg: number;
  wire_mbps: number;
  window_ms: number;
  sent_bytes: number;
  received_bytes: number;
  bad_payload: number;
  saw_last_seq: boolean;
}

export type WsResults = WsRrResults | WsStreamResults;

export interface FileTransferIteration {
  /** From 1. */
  index: number;
  file_name: string;
  bytes: number;
  ok: boolean;
  ack_ok: boolean;
  sha256_match: boolean | null;
  hash_ms: number | null;
  negotiate_ms: number | null;
  transfer_ms: number | null;
  elapsed_ms: number | null;
  wire_mbps: number | null;
  e2e_mbps: number | null;
  /** Waiting for the responder's `ft.done`. */
  done_wait_ms: number | null;
  /** Left out of the summary (iteration 1 of 3 or more, or a lost ACK). */
  excluded: boolean;
  excluded_reason: string | null;
  error: string | null;
  /** When the iteration ended, ms from the run's start. */
  t_ms: number;
}

export interface FileTransferResults {
  size_bytes: number;
  timed_iterations: number;
  ack_failures: number;
  sent_bytes: number;
  received_bytes: number;
  source_write_ms: number | null;
  iterations: FileTransferIteration[];
}

interface SyncedStoreFields {
  count: number;
  payload_bytes: number;
  store_id: string;
  ready_ms: number;
  readiness_writes: number;
  echoes: number;
  lagged: number;
  peer_lagged: number;
  ws_peers_start: number;
  ws_peers_end: number;
  peer_ws_peers_start: number;
  peer_ws_peers_end: number;
}

export interface SyncedStorePingpongResults extends SyncedStoreFields {
  mode: 'pingpong';
  warmup_count: number;
  /** RTT/2 estimate. */
  convergence_ms: Distribution;
  rtt_ms: Distribution;
  /** Convergence samples, ms. */
  samples_ms: number[];
  completed: number;
  timeouts: number;
}

export interface SyncedStoreBurstResults extends SyncedStoreFields {
  mode: 'burst';
  burst_write_ms: number;
  final_echo_ms: number | null;
  coalescing_pct: number;
  distinct_versions_seen: number;
  echoes_seen: number;
}

export type SyncedStoreResults = SyncedStorePingpongResults | SyncedStoreBurstResults;

export interface HttpErrors {
  non_2xx: number;
  io: number;
  timeout: number;
}

/** What the responder's tiny server answered. */
export interface HttpServerStats {
  connections: number;
  requests: number;
  ok: number;
  bad_request: number;
  not_found: number;
  forbidden: number;
  body_bytes: number;
  foreign_dropped: number;
}

export interface HttpResults {
  port: number;
  attempts: number;
  ok: number;
  errors: HttpErrors;
  /** error kind → count */
  error_kinds: Record<string, number>;
  /** HTTP status → count */
  status_counts: Record<string, number>;
  error_pct: number | null;
  req_per_sec: number | null;
  latency_ms: Distribution;
  samples_ms: number[];
  samples_total: number;
  /** Median connect time (new connections). */
  connect_ms: number | null;
  connect: Distribution;
  connections_opened: number;
  body_bytes: number;
  goodput_mbps: number | null;
  warmup_requests: number;
  server: HttpServerStats | null;
}

export type VideoFrameState = 'on-time' | 'late' | 'dropped';

/** One frame of the strip (every frame up to 3600, then a sample plus every frame not on time). */
export interface VideoFrame {
  frame_no: number;
  keyframe: boolean;
  bytes: number;
  state: VideoFrameState;
  latency_ms: number | null;
  delay_ms: number | null;
  spread_ms: number | null;
  warmup: boolean;
}

export interface VideoDatagrams {
  sent: number;
  unique: number;
  missing: number;
  dup: number;
  reordered: number;
  send_errors: number;
  loss_pct: number | null;
  max_loss_burst: number;
}

export interface VideoResults {
  transport: VideoParams['transport'];
  pacing: VideoParams['pacing'];
  fps: number;
  bitrate_mbps: number;
  p_frame_bytes: number;
  i_frame_bytes: number;
  frames_total: number;
  warmup_frames: number;
  frames_sent: number;
  on_time: number;
  late: number;
  dropped: number;
  late_pct: number | null;
  dropped_pct: number | null;
  delivered_fps: number | null;
  freezes: number;
  freeze_ms: number;
  frame_latency_ms: Distribution;
  relative_delay_ms: Distribution;
  frame_spread_ms: Distribution;
  keyframe_complete_ms: Distribution;
  samples_ms: number[];
  latency_uncertainty_ms: number | null;
  frame_spread_p50_ms: number | null;
  keyframe_complete_p50_ms: number | null;
  offered_mbps: number | null;
  goodput_mbps: number | null;
  sent_bytes: number;
  received_bytes: number;
  receiver: ReceiverStats;
  datagrams: VideoDatagrams | null;
  connect_ms: number | null;
  max_send_lag_ms: number;
  stream_ack: number | null;
  foreign_dropped: number;
  frames: VideoFrame[];
  frames_stride: number;
}

/** tcp throughput's fields (no mode tag), plus the bridge count and the egress route. */
export interface EgressResults extends TcpThroughputFields {
  bridges: number;
  route_id: string;
}

/** `results` of an ok run, by workload. */
export interface WorkloadResults {
  path: PathResults;
  tcp: TcpResults;
  udp: UdpResults;
  quic: QuicResults;
  ws: WsResults;
  'file-transfer': FileTransferResults;
  'synced-store': SyncedStoreResults;
  http: HttpResults;
  video: VideoResults;
  egress: EgressResults;
}

export type ResultsOf<K extends WorkloadKind> = WorkloadResults[K];

/** `{}`: a run that failed or was cancelled before it measured anything. */
export type NoResults = Record<never, never>;

/** An old report normalised to v3 (§7.5) keeps its original params and results here. */
export interface ImportedResults {
  source: 'v1' | 'v2' | 'tsnet-bench';
  source_file: string;
  params: unknown;
  results: unknown;
}

interface RunReportBase {
  schema_version: 3;
  run_id: string;
  suite_id: string | null;
  tool: Tool;
  tool_version: string;
  truffle_version: string;
  /** "v3", or for imported reports v2-transport | v2-file-transfer | v2-request-reply | v2-synced-store | native-tsnet */
  methodology: string;
  label: string;
  status: RunStatus;
  error: string | null;
  error_code: ErrorCode | null;
  started_at: string;
  finished_at: string;
  duration_ms: number;
  git_sha: string | null;
  git_dirty: boolean | null;
  /** 16 hex. Runs are only trended or compared within one key. */
  compare_key: string;
  local: NodeEnv;
  peer: NodeEnv | null;
  network: NetworkInfo;
  summary: SummaryItem[];
  series: Series[];
  resources: Resources;
  overlapping_runs: number;
  notes: string[];
}

/** A report of one workload. */
export interface RunReportOf<K extends WorkloadKind> extends RunReportBase {
  workload: K;
  /** As validated and used by the responder. */
  params: ParamsOf<K>;
  /**
   * Workload-specific (§8). A failed or cancelled run keeps what it measured (partial results)
   * or has `{}`; an imported report has `ImportedResults`. Read them with `resultsOf`.
   */
  results: ResultsOf<K> | NoResults | ImportedResults;
}

/** A report of any workload; narrow on `workload`. */
export type RunReport = { [K in WorkloadKind]: RunReportOf<K> }[WorkloadKind];

/** A report's own workload results, or null when it has none (`{}`) or was imported. */
export function resultsOf<K extends WorkloadKind>(r: RunReportOf<K>): ResultsOf<K> | null;
export function resultsOf(r: RunReport): ResultsOf<WorkloadKind> | null;
export function resultsOf(r: RunReport): ResultsOf<WorkloadKind> | null {
  if (r.tool === 'imported' || Object.keys(r.results).length === 0) return null;
  return r.results as ResultsOf<WorkloadKind>;
}

// ---------------------------------------------------------------- app commands (§11.2)

export interface AppInfo {
  app_version: string;
  truffle_version: string;
  data_dir: string;
  profile: string | null;
  os: string;
  build_profile: BuildProfile;
  sidecar_path: string | null;
  sidecar_sha256: string | null;
}

export interface HubStartRequest {
  device_name?: string;
  auth_key?: string;
}

export interface RunRequest {
  run_id: string;
  /** A peer reference: tailscale_id, device_id or name. */
  peer: string;
  params: WorkloadParams;
}

export interface SuiteRunRequest {
  suite_id: string;
  profile: string;
  peer: string;
}

export interface SuiteProfile {
  id: string;
  label: string;
  description: string;
  items: WorkloadParams[];
}

/**
 * The engine's `report::flag` set (`RunReport::flags()`): debug build on either side, other runs
 * shared the machine, the sender couldn't sustain its target rate, imported, the topology, a
 * changed or partly relayed path, and a failed or cancelled run that kept partial results.
 */
export type RunFlag =
  | 'debug'
  | 'overlap'
  | 'rate-not-achieved'
  | 'imported'
  | 'same-host'
  | 'same-process'
  | 'mixed-path'
  | 'partial';

export interface RunSummary {
  run_id: string;
  suite_id: string | null;
  workload: WorkloadKind;
  /**
   * `params.mode` (`throughput`, `latency`, `stream`, `echo`, `ramp`, `rr`, `pingpong`, `burst`)
   * for tcp, udp, quic, ws and synced-store; null for the workloads without modes.
   */
  mode: string | null;
  label: string;
  status: RunStatus;
  error_code: ErrorCode | null;
  started_at: string;
  peer_name: string | null;
  path_label: PathLabel;
  topology: Topology;
  build_profile: BuildProfile;
  compare_key: string;
  summary: SummaryItem[];
  overlapping_runs: number;
  tool: Tool;
  /** Computed by the engine; a newer engine may add values this app ignores. */
  flags: RunFlag[];
}

export interface ImportResult {
  imported: number;
  skipped: number;
}

export type ThemeSetting = 'system' | 'light' | 'dark';

export interface Settings {
  device_name: string;
  allow_other_logins: boolean;
  theme: ThemeSetting;
  /** device_id, else tailscale_id. */
  target_peer: string | null;
}
