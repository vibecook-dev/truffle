// Planned: one card per item in RFC 027 §8.6, drawn with a dashed outline, each with its reason.

import { Page } from '../components/Misc';
import { Status } from '../components/Status';
import { useConstructionFlag } from '../charts/construction';

const PLANNED: { title: string; why: string }[] = [
  {
    title: 'QUIC datagrams',
    why: 'Unreliable datagrams inside a QUIC connection (RFC 9221) wait for truffle-core to expose them.',
  },
  {
    title: 'QUIC connection stats',
    why: 'RTT, congestion window and lost packets from quinn, once QuicConnection reports them.',
  },
  {
    title: 'JavaScript binding lane',
    why: 'The same workloads through the Node.js binding (NAPI), to measure what JS apps see.',
  },
  {
    title: 'Swift peers',
    why: 'Peers running the Swift plane (RFC 024), so iPhones and iPads can be measured too.',
  },
  {
    title: 'tsnet ceiling',
    why: 'A benchmark inside the sidecar (protocol 8); until then, tsnet-bench is the native Go baseline.',
  },
  {
    title: 'Latency waterfall',
    why: 'Disco, TSMP, ICMP and a truffle echo side by side, once core stops pinning disco pings.',
  },
  {
    title: 'HTTPS serving',
    why: 'HTTP with tls on, to time the handshake and the first certificate.',
  },
  { title: 'Pulling files', why: 'pull_file, the receiver-initiated side of file transfer.' },
  {
    title: 'SyncedStore on disk',
    why: 'The FileBackend, so the cost of persistence shows next to sync.',
  },
  {
    title: 'Egress through proxies',
    why: 'Egress via SOCKS5 and HTTP CONNECT, not only a direct listener.',
  },
  {
    title: 'Broadcast fan-out',
    why: 'One sender and many receivers, which needs three or more nodes.',
  },
  {
    title: 'Live camera',
    why: 'A real camera through WebCodecs instead of the synthetic frame model.',
  },
  {
    title: 'Taildrop comparison',
    why: 'The same file through Tailscale’s Taildrop, as a reference for file transfer.',
  },
  {
    title: 'Device approval in core',
    why: 'Sign-in can only hint at device approval until core forwards tsnet’s needsApproval event.',
  },
  {
    title: 'Video ramp',
    why: 'Raise the bitrate until frames start arriving late, as UDP ramp does for loss.',
  },
];

export function Planned() {
  const construction = useConstructionFlag();
  return (
    <Page
      construction={construction}
      eyebrow="Benchmarks"
      title="Planned"
      lede={`What truffle bench doesn’t measure yet, and why: ${PLANNED.length} items, each waiting on a change somewhere else first.`}
    >
      <div className="planned-grid">
        {PLANNED.map((p) => (
          <article key={p.title} className="card planned" data-measure={p.title}>
            <Status kind="planned" />
            <h3 className="pl-title">{p.title}</h3>
            <p className="ink2">{p.why}</p>
          </article>
        ))}
      </div>
    </Page>
  );
}
