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
import liveKeys from '../../../schema/fixtures/live-keys.json';
import type { RunReport, WorkloadKind, WorkloadParams } from './types';

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

/**
 * The Progress sample keys each mode can set, by example file name: the engine's
 * `workloads::live_keys()` (the initiator's own samples, the responder's ticks the hub merges in,
 * and the path sampler's `loaded_rtt_ms` in every time-based mode), which its `schema_is_current`
 * test writes to benchmark/schema/fixtures/live-keys.json. The registry's live keys and the
 * mock's samples must be among them.
 */
export const LIVE_KEYS: Record<string, string[]> = Object.fromEntries(
  Object.entries(liveKeys as Record<string, string[]>).map(([mode, keys]) => [
    `report-${mode}.json`,
    keys,
  ]),
);
