/**
 * @truffle/tauri-plugin — Guest JS bindings for the Truffle Tauri v2 plugin.
 *
 * Typed wrappers around Tauri's `invoke()` for the mesh commands and `listen()`
 * for the mesh events. The command names, argument shapes, and payload types
 * mirror `src/commands.rs`, `src/events.rs`, and `src/types.rs` exactly — those
 * are the source of truth. Argument keys are camelCase (Tauri v2 maps them to
 * the snake_case Rust parameters automatically).
 *
 * Raw transport (RFC 021 §7) is exposed as small handle classes that hold a
 * registry id and forward to the `tcp_*` / `udp_*` / `quic_*` commands — the
 * Tauri-IPC parity of the NAPI `NapiTcpSocket` / `NapiUdpSocket` / QUIC classes
 * (live handles cannot cross IPC, so the plugin parks them and hands back ids).
 */

import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';

// ═══════════════════════════════════════════════════════════════════════════
// Types (mirror src/types.rs — all camelCase)
// ═══════════════════════════════════════════════════════════════════════════

/** Configuration for starting a node. Mirrors `StartConfig`. */
export interface StartConfig {
  /** Application namespace id. Required. Matches `^[a-z][a-z0-9-]{1,31}$`. */
  appId: string;
  /** Human-readable device name. Defaults to the OS hostname. */
  deviceName?: string;
  /** ULID override for the stable `deviceId`. */
  deviceId?: string;
  /** Path to the Go sidecar binary. Required. */
  sidecarPath: string;
  /** Tailscale state directory. */
  stateDir?: string;
  /** Tailscale auth key for headless authentication. */
  authKey?: string;
  /** Whether the node is ephemeral (auto-removed on shutdown). */
  ephemeral?: boolean;
  /** WebSocket listen port (defaults to 9417). */
  wsPort?: number;
}

/** Local node identity. Mirrors `NodeIdentityJs`. */
export interface NodeIdentity {
  appId: string;
  deviceId: string;
  deviceName: string;
  tailscaleHostname: string;
  tailscaleId: string;
  dnsName?: string;
  ip?: string;
}

/** A peer on the mesh. Mirrors `PeerJs` / `PeerStateJs` (RFC 022). */
export interface Peer {
  /**
   * Durable ULID once identity is learned; `null` until then — never a
   * Tailscale id fallback (RFC 022 I1). Use only for persistence.
   */
  deviceId: string | null;
  /** Hello identity name; `null` until identity is learned. */
  deviceName: string | null;
  /** Best UI label: identity name → hostname slug → short tailscale id. */
  displayName: string;
  /** L3 Tailscale hostname (`truffle-{appId}-{slug}`). */
  hostname: string;
  ip: string;
  online: boolean;
  wsConnected: boolean;
  connectionType: string;
  os: string | null;
  lastSeen: string | null;
  /** Tailscale stable node id — routing key (advanced). */
  tailscaleId: string;
  /** Process-local `{tailscaleId}:{generation}` token; do not persist. */
  peerRef: string;
  /** Registry entry generation — bumped when the same node rejoins. */
  generation: number;
}

/** Ping result. Mirrors `PingResultJs`. */
export interface PingResult {
  latencyMs: number;
  connection: string;
  peerAddr?: string;
}

/** Network health. Mirrors `HealthInfoJs`. */
export interface HealthInfo {
  state: string;
  keyExpiry?: string;
  warnings: string[];
  healthy: boolean;
}

/** File transfer result. Mirrors `TransferResultJs`. */
export interface TransferResult {
  bytesTransferred: number;
  sha256: string;
  elapsedSecs: number;
}

/** An incoming file offer. Mirrors `FileOfferJs`. */
export interface FileOffer {
  fromPeer: string;
  fromName: string;
  fileName: string;
  size: number;
  sha256: string;
  suggestedPath: string;
  token: string;
}

/** Peer lifecycle event. Mirrors `PeerEventJs` (serde tag = "type"). */
export type PeerEvent =
  | { type: 'joined'; peer: Peer }
  | { type: 'left'; id: string }
  | { type: 'updated'; peer: Peer }
  /** `deviceId` was learned or rotated — the only carrier of the durable ULID. */
  | { type: 'identity'; peer: Peer }
  | { type: 'wsConnected'; id: string }
  | { type: 'wsDisconnected'; id: string }
  | { type: 'authRequired'; url: string };

/** File transfer event. Mirrors `FileTransferEventJs` (serde tag = "type"). */
export type FileTransferEvent =
  | { type: 'offerReceived'; offer: FileOffer }
  | {
      type: 'hashing';
      token: string;
      fileName: string;
      bytesHashed: number;
      totalBytes: number;
    }
  | { type: 'waitingForAccept'; token: string; fileName: string }
  | {
      type: 'progress';
      token: string;
      direction: string;
      fileName: string;
      bytesTransferred: number;
      totalBytes: number;
      speedBps: number;
    }
  | {
      type: 'completed';
      token: string;
      direction: string;
      fileName: string;
      bytesTransferred: number;
      sha256: string;
      elapsedSecs: number;
    }
  | { type: 'rejected'; token: string; fileName: string; reason: string }
  | {
      type: 'failed';
      token: string;
      direction: string;
      fileName: string;
      reason: string;
    };

/** One response header of a serve or a route (RFC 030). Mirrors core's `ProxyHeader`.
 *  The core checks names and values and refuses a bad list. */
export interface ProxyHeader {
  /** Header name, e.g. "Content-Security-Policy" (compared case-insensitively). */
  name: string;
  /** Header value: visible ASCII, spaces and tabs. */
  value: string;
}

/** One path-prefix route of a v2 proxy. Mirrors `ProxyRouteJs` (RFC 023 §7).
 *  Exactly one of `targetUrl` / `dir` is set; longest prefix wins. */
export interface ProxyRoute {
  /** Path prefix to match (must start with "/"). */
  prefix: string;
  /** Proxy target URL, e.g. "http://localhost:8000". Mutually exclusive with `dir`. */
  targetUrl?: string;
  /** Static directory to serve (absolute path). Mutually exclusive with `targetUrl`. */
  dir?: string;
  /** SPA fallback rewritten on static misses (e.g. "/index.html"); requires `dir`. */
  fallback?: string;
  /** Strip the matched prefix before proxying (default false); requires `targetUrl`. */
  stripPrefix?: boolean;
  /** Per-route loginName globs; overrides the config-level `allow`. */
  allow?: string[];
  /** Response headers for this route (RFC 030); each replaces the config-level one of that name. */
  headers?: ProxyHeader[];
}

/** Reverse-proxy config. Mirrors `ProxyConfigJs`. */
export interface ProxyConfig {
  id: string;
  name: string;
  listenPort: number;
  targetHost?: string;
  targetPort: number;
  targetScheme?: string;
  announce?: boolean;
  /** Terminate TLS on the listener (default true; `false` needs a v2 sidecar). */
  tls?: boolean;
  /** Permit non-loopback targets (default false = deny the LAN-pivot risk). */
  allowNonLoopback?: boolean;
  /** loginName allow globs; empty/absent = the whole tailnet. */
  allow?: string[];
  /** Path-prefix routes; when non-empty they replace the single target. */
  routes?: ProxyRoute[];
  /** Response headers on every response of the serve (RFC 030; sidecar protocol 13). */
  headers?: ProxyHeader[];
}

/** Reverse-proxy info. Mirrors `ProxyInfoJs`. */
export interface ProxyInfo {
  id: string;
  name: string;
  listenPort: number;
  targetHost: string;
  targetPort: number;
  targetScheme: string;
  url: string;
  status: string;
}

/** Proxy lifecycle event. Mirrors `ProxyEventJs` (serde tag = "type"). */
export type ProxyEvent =
  | { type: 'started'; id: string; url: string; listenPort: number }
  | { type: 'stopped'; id: string }
  | { type: 'error'; id: string; code: string; message: string };

// ═══════════════════════════════════════════════════════════════════════════
// Lifecycle
// ═══════════════════════════════════════════════════════════════════════════

export async function start(config: StartConfig): Promise<NodeIdentity> {
  return invoke('plugin:truffle|start', { config });
}

export async function stop(): Promise<void> {
  return invoke('plugin:truffle|stop');
}

// ═══════════════════════════════════════════════════════════════════════════
// Identity & discovery
// ═══════════════════════════════════════════════════════════════════════════

export async function getLocalInfo(): Promise<NodeIdentity> {
  return invoke('plugin:truffle|get_local_info');
}

export async function getPeers(): Promise<Peer[]> {
  return invoke('plugin:truffle|get_peers');
}

// ═══════════════════════════════════════════════════════════════════════════
// Diagnostics
// ═══════════════════════════════════════════════════════════════════════════

export async function ping(peerId: string): Promise<PingResult> {
  return invoke('plugin:truffle|ping', { peerId });
}

export async function health(): Promise<HealthInfo> {
  return invoke('plugin:truffle|health');
}

// ═══════════════════════════════════════════════════════════════════════════
// Messaging
// ═══════════════════════════════════════════════════════════════════════════

export async function sendMessage(
  peerId: string,
  namespace: string,
  data: Uint8Array | number[],
): Promise<void> {
  return invoke('plugin:truffle|send_message', {
    peerId,
    namespace,
    data: Array.from(data),
  });
}

export async function broadcast(
  namespace: string,
  data: Uint8Array | number[],
): Promise<void> {
  return invoke('plugin:truffle|broadcast', {
    namespace,
    data: Array.from(data),
  });
}

/** Outcome of a broadcastJson / broadcastBytes call. "Queued" means handed
 *  to a peer's connection task — delivery is not confirmed. Broadcasts
 *  reach only currently connected peers. */
export interface BroadcastReport {
  /** Peers with an active WS connection at broadcast time. */
  attempted: number;
  /** Messages successfully queued to a connection task. */
  queued: number;
  /** Tailscale ids of peers whose connection task was already closed. */
  failed: string[];
}

/** Send a JSON payload to a peer (explicit wire type — unlike sendMessage,
 *  the representation never depends on the data's contents). */
export async function sendJson(
  peerId: string,
  namespace: string,
  payload: unknown,
): Promise<void> {
  return invoke('plugin:truffle|send_json', { peerId, namespace, payload });
}

/** Send opaque binary data to a peer (explicit wire type; base64 "bytes"
 *  envelope — receivers see msgType === "bytes"). */
export async function sendBytes(
  peerId: string,
  namespace: string,
  data: Uint8Array | number[],
): Promise<void> {
  return invoke('plugin:truffle|send_bytes', {
    peerId,
    namespace,
    data: Array.from(data),
  });
}

/** Broadcast a JSON payload to all connected peers, reporting how many
 *  peers it was queued to. */
export async function broadcastJson(
  namespace: string,
  payload: unknown,
): Promise<BroadcastReport> {
  return invoke('plugin:truffle|broadcast_json', { namespace, payload });
}

/** Broadcast opaque binary data to all connected peers, reporting how many
 *  peers it was queued to. */
export async function broadcastBytes(
  namespace: string,
  data: Uint8Array | number[],
): Promise<BroadcastReport> {
  return invoke('plugin:truffle|broadcast_bytes', {
    namespace,
    data: Array.from(data),
  });
}

// ═══════════════════════════════════════════════════════════════════════════
// File transfer
// ═══════════════════════════════════════════════════════════════════════════

export async function sendFile(
  peerId: string,
  localPath: string,
  remotePath: string,
): Promise<TransferResult> {
  return invoke('plugin:truffle|send_file', { peerId, localPath, remotePath });
}

export async function pullFile(
  peerId: string,
  remotePath: string,
  localPath: string,
): Promise<TransferResult> {
  return invoke('plugin:truffle|pull_file', { peerId, remotePath, localPath });
}

export async function autoAccept(outputDir: string): Promise<void> {
  return invoke('plugin:truffle|auto_accept', { outputDir });
}

export async function autoReject(): Promise<void> {
  return invoke('plugin:truffle|auto_reject');
}

export async function acceptOffer(token: string, savePath: string): Promise<void> {
  return invoke('plugin:truffle|accept_offer', { token, savePath });
}

export async function rejectOffer(token: string, reason: string): Promise<void> {
  return invoke('plugin:truffle|reject_offer', { token, reason });
}

export async function addPullRoot(root: string): Promise<void> {
  return invoke('plugin:truffle|add_pull_root', { root });
}

export async function pullRoots(): Promise<string[]> {
  return invoke('plugin:truffle|pull_roots');
}

export async function clearPullRoots(): Promise<void> {
  return invoke('plugin:truffle|clear_pull_roots');
}

// ═══════════════════════════════════════════════════════════════════════════
// Reverse proxy
// ═══════════════════════════════════════════════════════════════════════════

export async function proxyAdd(config: ProxyConfig): Promise<ProxyInfo> {
  return invoke('plugin:truffle|proxy_add', { config });
}

export async function proxyRemove(id: string): Promise<void> {
  return invoke('plugin:truffle|proxy_remove', { id });
}

export async function proxyList(): Promise<ProxyInfo[]> {
  return invoke('plugin:truffle|proxy_list');
}

// ═══════════════════════════════════════════════════════════════════════════
// Raw transport (RFC 021 §7) — NAPI parity via id-keyed handles
// ═══════════════════════════════════════════════════════════════════════════

interface TcpOpenResult {
  socketId: string;
  remoteAddress: string;
  remotePeerId?: string;
}

interface TcpAcceptResult {
  socketId: string;
  remoteAddress: string;
  remotePeerId?: string;
  remotePeerName?: string;
}

interface TcpListenResult {
  listenerId: string;
  port: number;
}

interface UdpBindResult {
  socketId: string;
  port: number;
}

/** A datagram received via {@link TruffleUdpSocket.recv}. */
export interface Datagram {
  data: Uint8Array;
  address: string;
  port: number;
}

interface QuicConnectResult {
  connectionId: string;
  remoteAddress: string;
  remotePeerId?: string;
}

interface QuicStreamResult {
  streamId: string;
}

interface QuicListenResult {
  listenerId: string;
  port: number;
}

/**
 * A raw TCP connection over the mesh. Pull-model: `read()` resolves the next
 * chunk or `null` on EOF. Mirrors `NapiTcpSocket`.
 */
export class TruffleTcpSocket {
  constructor(
    readonly socketId: string,
    readonly remoteAddress: string,
    readonly remotePeerId: string | null = null,
    readonly remotePeerName: string | null = null,
  ) {}

  /** Read up to `maxBytes` (default 64 KiB). Resolves `null` on clean EOF. */
  async read(maxBytes?: number): Promise<Uint8Array | null> {
    const bytes = await invoke<number[] | null>('plugin:truffle|tcp_read', {
      socketId: this.socketId,
      maxBytes,
    });
    return bytes === null ? null : Uint8Array.from(bytes);
  }

  /** Write all of `data` to the socket. */
  async write(data: Uint8Array | number[]): Promise<void> {
    return invoke('plugin:truffle|tcp_write', {
      socketId: this.socketId,
      data: Array.from(data),
    });
  }

  /** Half-close the write side (send FIN). Reading remains possible. */
  async end(): Promise<void> {
    return invoke('plugin:truffle|tcp_end', { socketId: this.socketId });
  }

  /** Fully close the socket (both directions). Idempotent. */
  async close(): Promise<void> {
    return invoke('plugin:truffle|tcp_close', { socketId: this.socketId });
  }
}

/** A listener for raw TCP connections. Mirrors `NapiTcpListener`. */
export class TruffleTcpListener {
  constructor(readonly listenerId: string, readonly port: number) {}

  /** Accept the next connection. Resolves `null` once the listener is closed. */
  async accept(): Promise<TruffleTcpSocket | null> {
    const res = await invoke<TcpAcceptResult | null>('plugin:truffle|tcp_accept', {
      listenerId: this.listenerId,
    });
    if (res === null) return null;
    return new TruffleTcpSocket(
      res.socketId,
      res.remoteAddress,
      res.remotePeerId ?? null,
      res.remotePeerName ?? null,
    );
  }

  /** Stop listening and release the tsnet port. Idempotent. */
  async unlisten(): Promise<void> {
    return invoke('plugin:truffle|tcp_unlisten', { listenerId: this.listenerId });
  }
}

/** A UDP socket over the mesh. Mirrors `NapiUdpSocket`. */
export class TruffleUdpSocket {
  constructor(readonly socketId: string, readonly port: number) {}

  /** Send a datagram to `host:port`. `host` is a peer ref or IP. */
  async send(
    data: Uint8Array | number[],
    host: string,
    port: number,
  ): Promise<number> {
    return invoke('plugin:truffle|udp_send', {
      socketId: this.socketId,
      data: Array.from(data),
      host,
      port,
    });
  }

  /** Receive the next datagram. Resolves `null` once the socket is closed. */
  async recv(): Promise<Datagram | null> {
    const res = await invoke<{
      data: number[];
      address: string;
      port: number;
    } | null>('plugin:truffle|udp_recv', { socketId: this.socketId });
    if (res === null) return null;
    return { data: Uint8Array.from(res.data), address: res.address, port: res.port };
  }

  /** Close the socket; a pending `recv()` resolves `null`. Idempotent. */
  async close(): Promise<void> {
    return invoke('plugin:truffle|udp_close', { socketId: this.socketId });
  }
}

/** A bidirectional byte stream on a QUIC connection. Mirrors `NapiQuicStream`. */
export class TruffleQuicStream {
  constructor(readonly streamId: string) {}

  /** Read up to `maxBytes` (default 64 KiB). Resolves `null` on clean EOF. */
  async read(maxBytes?: number): Promise<Uint8Array | null> {
    const bytes = await invoke<number[] | null>('plugin:truffle|quic_stream_read', {
      streamId: this.streamId,
      maxBytes,
    });
    return bytes === null ? null : Uint8Array.from(bytes);
  }

  /** Write all of `data` to the stream. */
  async write(data: Uint8Array | number[]): Promise<void> {
    return invoke('plugin:truffle|quic_stream_write', {
      streamId: this.streamId,
      data: Array.from(data),
    });
  }

  /** Half-close: finish the write side (clean EOF). Reading remains possible. */
  async finish(): Promise<void> {
    return invoke('plugin:truffle|quic_stream_finish', { streamId: this.streamId });
  }

  /** Fully close the stream (finish writes, stop reads). Idempotent. */
  async close(): Promise<void> {
    return invoke('plugin:truffle|quic_stream_close', { streamId: this.streamId });
  }
}

/** A raw QUIC connection carrying many streams. Mirrors `NapiQuicConnection`. */
export class TruffleQuicConnection {
  constructor(
    readonly connectionId: string,
    readonly remoteAddress: string,
    readonly remotePeerId: string | null = null,
  ) {}

  /** Open a new bidirectional byte stream. */
  async openStream(): Promise<TruffleQuicStream> {
    const res = await invoke<QuicStreamResult>('plugin:truffle|quic_open_stream', {
      connectionId: this.connectionId,
    });
    return new TruffleQuicStream(res.streamId);
  }

  /** Accept the next peer-opened stream. Resolves `null` once closed. */
  async acceptStream(): Promise<TruffleQuicStream | null> {
    const res = await invoke<QuicStreamResult | null>(
      'plugin:truffle|quic_accept_stream',
      { connectionId: this.connectionId },
    );
    return res === null ? null : new TruffleQuicStream(res.streamId);
  }

  /** Close the connection and all its streams. Idempotent. */
  async close(): Promise<void> {
    return invoke('plugin:truffle|quic_close', { connectionId: this.connectionId });
  }
}

/** A listener for raw QUIC connections. Mirrors `NapiQuicListener`. */
export class TruffleQuicListener {
  constructor(readonly listenerId: string, readonly port: number) {}

  /** Accept the next connection. Resolves `null` once the listener is closed. */
  async accept(): Promise<TruffleQuicConnection | null> {
    const res = await invoke<QuicConnectResult | null>('plugin:truffle|quic_accept', {
      listenerId: this.listenerId,
    });
    if (res === null) return null;
    return new TruffleQuicConnection(
      res.connectionId,
      res.remoteAddress,
      res.remotePeerId ?? null,
    );
  }

  /** Close the listener (and connections accepted from it). Idempotent. */
  async close(): Promise<void> {
    return invoke('plugin:truffle|quic_listener_close', {
      listenerId: this.listenerId,
    });
  }
}

/** Open a raw TCP connection to `host:port`. `host` is a peer ref or IP. */
export async function tcpOpen(host: string, port: number): Promise<TruffleTcpSocket> {
  const res = await invoke<TcpOpenResult>('plugin:truffle|tcp_open', { host, port });
  return new TruffleTcpSocket(res.socketId, res.remoteAddress, res.remotePeerId ?? null);
}

/** Listen for raw TCP connections. Port 0 binds an ephemeral port. */
export async function tcpListen(port: number): Promise<TruffleTcpListener> {
  const res = await invoke<TcpListenResult>('plugin:truffle|tcp_listen', { port });
  return new TruffleTcpListener(res.listenerId, res.port);
}

/** Bind a UDP socket. Port 0 binds an ephemeral relay port. */
export async function udpBind(port: number): Promise<TruffleUdpSocket> {
  const res = await invoke<UdpBindResult>('plugin:truffle|udp_bind', { port });
  return new TruffleUdpSocket(res.socketId, res.port);
}

/** Open a raw QUIC connection to `host:port` (same-app peers only). */
export async function quicConnect(
  host: string,
  port: number,
): Promise<TruffleQuicConnection> {
  const res = await invoke<QuicConnectResult>('plugin:truffle|quic_connect', {
    host,
    port,
  });
  return new TruffleQuicConnection(
    res.connectionId,
    res.remoteAddress,
    res.remotePeerId ?? null,
  );
}

/** Listen for raw QUIC connections. Requires an explicit (non-zero) port. */
export async function quicListen(port: number): Promise<TruffleQuicListener> {
  const res = await invoke<QuicListenResult>('plugin:truffle|quic_listen', { port });
  return new TruffleQuicListener(res.listenerId, res.port);
}

// ═══════════════════════════════════════════════════════════════════════════
// Event listeners (mirror src/events.rs + src/commands.rs emit sites)
// ═══════════════════════════════════════════════════════════════════════════

export async function onPeerEvent(
  callback: (event: PeerEvent) => void,
): Promise<UnlistenFn> {
  return listen<PeerEvent>('truffle://peer-event', (e) => callback(e.payload));
}

export async function onFileTransferEvent(
  callback: (event: FileTransferEvent) => void,
): Promise<UnlistenFn> {
  return listen<FileTransferEvent>('truffle://file-transfer-event', (e) =>
    callback(e.payload),
  );
}

export async function onFileOffer(
  callback: (offer: FileOffer) => void,
): Promise<UnlistenFn> {
  return listen<FileOffer>('truffle://file-offer', (e) => callback(e.payload));
}

export async function onAuthRequired(
  callback: (url: string) => void,
): Promise<UnlistenFn> {
  return listen<string>('truffle://auth-required', (e) => callback(e.payload));
}

export async function onProxyEvent(
  callback: (event: ProxyEvent) => void,
): Promise<UnlistenFn> {
  return listen<ProxyEvent>('truffle://proxy-event', (e) => callback(e.payload));
}
