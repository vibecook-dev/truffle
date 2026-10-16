// The engine's per-mode report examples (`workloads::mode_examples()`, written by the engine's
// `schema_is_current` test to benchmark/schema/fixtures/report-<mode>.json), typed. Each is built
// through its workload's own output code, so its summary keys, series keys, results fields and
// notes are what a real run of that mode reports.
//
// The casts here are checked: fixtures.test.ts proves at compile time that every one of these
// files satisfies its exact report type (`ModeReport<'tcp', TcpLatencyResults>`, …), and at run
// time that its workload and `results.mode` match the entry below. The mock builds its reports
// on these (so it can't drift from the engine), and the registry contract test checks every
// registry entry against them.

import reportEgress from '../../../schema/fixtures/report-egress.json';
import reportFileTransfer from '../../../schema/fixtures/report-file-transfer.json';
import reportHttpDirectNew from '../../../schema/fixtures/report-http-direct-new.json';
import reportHttpProxyKeepalive from '../../../schema/fixtures/report-http-proxy-keepalive.json';
import reportPath from '../../../schema/fixtures/report-path.json';
import reportQuicLatency from '../../../schema/fixtures/report-quic-latency.json';
import reportQuicThroughput from '../../../schema/fixtures/report-quic-throughput.json';
import reportSyncedStoreBurst from '../../../schema/fixtures/report-synced-store-burst.json';
import reportSyncedStorePingpong from '../../../schema/fixtures/report-synced-store-pingpong.json';
import reportTcpLatency from '../../../schema/fixtures/report-tcp-latency.json';
import reportTcpThroughputBidirectional from '../../../schema/fixtures/report-tcp-throughput-bidirectional.json';
import reportTcpThroughputUpload from '../../../schema/fixtures/report-tcp-throughput-upload.json';
import reportUdpEcho from '../../../schema/fixtures/report-udp-echo.json';
import reportUdpRamp from '../../../schema/fixtures/report-udp-ramp.json';
import reportUdpStream from '../../../schema/fixtures/report-udp-stream.json';
import reportVideoQuic from '../../../schema/fixtures/report-video-quic.json';
import reportVideoUdp from '../../../schema/fixtures/report-video-udp.json';
import reportWsRr from '../../../schema/fixtures/report-ws-rr.json';
import reportWsStream from '../../../schema/fixtures/report-ws-stream.json';
import { KEY_LOADED_RTT, type RunReport, type WorkloadKind, type WorkloadParams } from './types';

export interface ModeExample {
  workload: WorkloadKind;
  /** `results.mode` for the mode-tagged workloads (tcp, udp, quic, ws, synced-store). */
  mode: string | null;
  report: RunReport;
}

const ex = (workload: WorkloadKind, mode: string | null, report: unknown): ModeExample => ({
  workload,
  mode,
  report: report as RunReport,
});

/** Every per-mode example, by file name. */
export const MODE_EXAMPLES: Record<string, ModeExample> = {
  'report-path.json': ex('path', null, reportPath),
  'report-tcp-throughput-upload.json': ex('tcp', 'throughput', reportTcpThroughputUpload),
  'report-tcp-throughput-bidirectional.json': ex(
    'tcp',
    'throughput',
    reportTcpThroughputBidirectional,
  ),
  'report-tcp-latency.json': ex('tcp', 'latency', reportTcpLatency),
  'report-udp-stream.json': ex('udp', 'stream', reportUdpStream),
  'report-udp-echo.json': ex('udp', 'echo', reportUdpEcho),
  'report-udp-ramp.json': ex('udp', 'ramp', reportUdpRamp),
  'report-quic-throughput.json': ex('quic', 'throughput', reportQuicThroughput),
  'report-quic-latency.json': ex('quic', 'latency', reportQuicLatency),
  'report-ws-rr.json': ex('ws', 'rr', reportWsRr),
  'report-ws-stream.json': ex('ws', 'stream', reportWsStream),
  'report-file-transfer.json': ex('file-transfer', null, reportFileTransfer),
  'report-synced-store-pingpong.json': ex('synced-store', 'pingpong', reportSyncedStorePingpong),
  'report-synced-store-burst.json': ex('synced-store', 'burst', reportSyncedStoreBurst),
  'report-http-proxy-keepalive.json': ex('http', null, reportHttpProxyKeepalive),
  'report-http-direct-new.json': ex('http', null, reportHttpDirectNew),
  'report-video-udp.json': ex('video', null, reportVideoUdp),
  'report-video-quic.json': ex('video', null, reportVideoQuic),
  'report-egress.json': ex('egress', null, reportEgress),
};

/**
 * The example whose shape a run with these params has: same workload and mode, and for tcp the
 * bidirectional example when both directions run (it has the upload and download series).
 */
export function exampleFor(p: WorkloadParams): RunReport {
  const pick = (name: string) => MODE_EXAMPLES[name].report;
  switch (p.workload) {
    case 'tcp':
      return p.mode === 'latency'
        ? pick('report-tcp-latency.json')
        : p.direction === 'bidirectional'
          ? pick('report-tcp-throughput-bidirectional.json')
          : pick('report-tcp-throughput-upload.json');
    case 'udp':
      return pick(`report-udp-${p.mode}.json`);
    case 'quic':
      return pick(`report-quic-${p.mode}.json`);
    case 'ws':
      return pick(`report-ws-${p.mode}.json`);
    case 'synced-store':
      return pick(`report-synced-store-${p.mode}.json`);
    case 'http':
      return pick(
        p.target === 'direct' && p.connection === 'new'
          ? 'report-http-direct-new.json'
          : 'report-http-proxy-keepalive.json',
      );
    case 'video':
      return pick(p.transport === 'quic' ? 'report-video-quic.json' : 'report-video-udp.json');
    default:
      return pick(`report-${p.workload}.json`);
  }
}

const THROUGHPUT_LIVE = ['goodput_mbps', 'sent_mbps', KEY_LOADED_RTT];

/**
 * The Progress sample keys each mode sets while it runs, by example: the initiator's own samples,
 * the responder's ticks (merged in by the hub), and the path sampler's latest round trip
 * (`loaded_rtt_ms`) in every time-based mode. Read from benchmark/engine/src/workloads/*.rs (the
 * engine has no fixture for them); the registry's live keys and the mock's samples must be among
 * them.
 */
export const LIVE_KEYS: Record<string, string[]> = {
  'report-path.json': ['rtt_ms'],
  'report-tcp-throughput-upload.json': THROUGHPUT_LIVE,
  'report-tcp-throughput-bidirectional.json': [...THROUGHPUT_LIVE, 'upload_mbps', 'download_mbps'],
  'report-tcp-latency.json': ['rtt_ms', 'rtt_p50_ms'],
  'report-udp-stream.json': [
    'goodput_mbps',
    'loss_pct',
    'jitter_ms',
    'offered_mbps',
    'target_mbps',
    KEY_LOADED_RTT,
  ],
  'report-udp-echo.json': ['rtt_ms', 'rtt_p50_ms', 'loss_pct', KEY_LOADED_RTT],
  'report-udp-ramp.json': [
    'goodput_mbps',
    'loss_pct',
    'jitter_ms',
    'offered_mbps',
    'target_mbps',
    'max_sustainable_mbps',
    KEY_LOADED_RTT,
  ],
  'report-quic-throughput.json': THROUGHPUT_LIVE,
  'report-quic-latency.json': ['rtt_ms', 'rtt_p50_ms'],
  'report-ws-rr.json': ['rtt_ms', 'rtt_p50_ms', 'msgs_per_sec'],
  'report-ws-stream.json': [
    'msgs_per_sec',
    'goodput_mbps',
    'drop_pct',
    'received_msgs',
    'sent_msgs_per_sec',
    KEY_LOADED_RTT,
  ],
  'report-file-transfer.json': [
    'wire_mbps',
    'e2e_mbps',
    'hash_ms',
    'negotiate_ms',
    'transfer_ms',
    'iterations_done',
  ],
  'report-synced-store-pingpong.json': ['convergence_ms', 'convergence_p50_ms'],
  'report-synced-store-burst.json': [],
  'report-http-proxy-keepalive.json': [
    'req_per_sec',
    'latency_p50_ms',
    'error_pct',
    KEY_LOADED_RTT,
  ],
  'report-http-direct-new.json': ['req_per_sec', 'latency_p50_ms', 'error_pct', KEY_LOADED_RTT],
  'report-video-udp.json': [
    'frames_done',
    'delivered_fps',
    'late_pct',
    'dropped_pct',
    'freezes',
    'frame_latency_ms',
    'frame_latency_p50_ms',
    'goodput_mbps',
    'frames_sent',
    'offered_mbps',
    'clock_uncertainty_ms',
    KEY_LOADED_RTT,
  ],
  'report-video-quic.json': [
    'frames_done',
    'delivered_fps',
    'late_pct',
    'dropped_pct',
    'freezes',
    'frame_latency_ms',
    'frame_latency_p50_ms',
    'goodput_mbps',
    'frames_sent',
    'offered_mbps',
    'clock_uncertainty_ms',
    KEY_LOADED_RTT,
  ],
  'report-egress.json': THROUGHPUT_LIVE,
};
