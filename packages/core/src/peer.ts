/**
 * RFC 022 — user-facing Peer handle for `@vibecook/truffle`.
 *
 * Discovery, events, and messages yield `Peer` instances. Networking takes
 * `Peer` (or a query string). Identity resolution stays inside the library.
 *
 * Same-instance guarantee: for a given mesh node and live registry entry
 * (`peerRef`), `getPeers()`, peer-change events, and `message.from` return
 * the **same** object so `Map<Peer, T>` and `===` work.
 */

import type {
  NapiNode,
  NapiPeer,
  NapiPeerIdentity,
  NapiPingResult,
} from '@vibecook/truffle-native';

/** Opaque process-local peer token (`{tailscaleId}:{generation}`). */
export type PeerRef = string & { readonly __trufflePeerRef: unique symbol };

/** Snapshot fields shared with the native `NapiPeer` object. */
export type PeerSnapshot = NapiPeer;

/**
 * Live peer handle. Prefer this over raw device / Tailscale id strings
 * for all networking.
 */
export class Peer {
  readonly #node: NapiNode;
  #snap: PeerSnapshot;

  /** @internal */
  private constructor(node: NapiNode, snap: PeerSnapshot) {
    this.#node = node;
    this.#snap = snap;
  }

  /** @internal — used by {@link PeerRegistry} only. */
  static _create(node: NapiNode, snap: PeerSnapshot): Peer {
    return new Peer(node, snap);
  }

  /** @internal */
  _update(snap: PeerSnapshot): void {
    this.#snap = snap;
  }

  /** @internal — registry only: pin the final offline view after `left`. */
  _markLeft(): void {
    this.#snap = { ...this.#snap, online: false, wsConnected: false };
  }

  // ── Display ────────────────────────────────────────────────────────────

  get displayName(): string {
    return this.#snap.displayName;
  }

  get online(): boolean {
    return this.#snap.online;
  }

  /** Envelope-bus WebSocket up. Advanced — do not gate `send` on this. */
  get wsConnected(): boolean {
    return this.#snap.wsConnected;
  }

  get ip(): string {
    return this.#snap.ip;
  }

  get os(): string | null | undefined {
    return this.#snap.os ?? null;
  }

  get hostname(): string {
    return this.#snap.hostname;
  }

  get connectionType(): string {
    return this.#snap.connectionType;
  }

  get lastSeen(): string | null | undefined {
    return this.#snap.lastSeen ?? null;
  }

  // ── Identity ───────────────────────────────────────────────────────────

  /**
   * Durable ULID once known; `null` until identity is learned.
   * Never equals `tailscaleId`. Use only for persistence across restarts.
   */
  get deviceId(): string | null {
    return this.#snap.deviceId ?? null;
  }

  get deviceName(): string | null {
    return this.#snap.deviceName ?? null;
  }

  /** Advanced — Tailscale routing key. */
  get tailscaleId(): string {
    return this.#snap.tailscaleId;
  }

  /**
   * Process-local token for Map keys / IPC. Stable for one generation;
   * do not persist across restarts.
   */
  get ref(): PeerRef {
    return this.#snap.peerRef as PeerRef;
  }

  get generation(): number {
    return this.#snap.generation;
  }

  /** Same registry entry generation (left + rejoin → false). */
  equals(other: Peer): boolean {
    return this.ref === other.ref;
  }

  // ── Networking sugar ───────────────────────────────────────────────────

  /**
   * Routing selector for native calls: the peer ref, so the core can
   * generation-check the handle — a stale handle (peer left/rejoined)
   * rejects with PeerGone instead of silently reaching the new generation
   * (RFC 022 I5) — and routes by the authenticated Tailscale key rather
   * than the self-declared ULID.
   */
  get #routeId(): string {
    return this.ref;
  }

  /**
   * Send raw bytes with the legacy content-sniffing wire behavior: data
   * that parses as UTF-8 JSON travels as that JSON value, anything else
   * as a byte array. Prefer {@link sendJson} / {@link sendBytes}, whose
   * wire type never depends on the data's contents.
   */
  send(namespace: string, data: Buffer | Uint8Array): Promise<void> {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    return this.#node.send(this.#routeId, namespace, buf);
  }

  /** Send a JSON payload (explicit wire type). */
  sendJson(namespace: string, payload: unknown): Promise<void> {
    return this.#node.sendJson(this.#routeId, namespace, payload);
  }

  /**
   * Send opaque binary data (explicit wire type; base64 `"bytes"`
   * envelope — receivers see `msgType === "bytes"`).
   */
  sendBytes(namespace: string, data: Buffer | Uint8Array): Promise<void> {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
    return this.#node.sendBytes(this.#routeId, namespace, buf);
  }

  ping(): Promise<NapiPingResult> {
    return this.#node.ping(this.#routeId);
  }

  /**
   * The peer's tailnet WhoIs identity (login, display name, MagicDNS name,
   * node id) — transport-derived from the control plane, never the peer's
   * claim about itself. `null` when the tailnet has no identity for it.
   */
  whois(): Promise<NapiPeerIdentity | null> {
    return this.#node.whois(this.#routeId);
  }
}

/**
 * Per-mesh-node table that interns {@link Peer} instances by `peerRef`.
 */
export class PeerRegistry {
  readonly #node: NapiNode;
  readonly #byRef = new Map<string, Peer>();
  /** Latest generation per Tailscale id — for message `from` attribution. */
  readonly #byTailscale = new Map<string, Peer>();

  constructor(node: NapiNode) {
    this.#node = node;
  }

  /** Insert or refresh a handle from a native snapshot. */
  upsert(snap: PeerSnapshot): Peer {
    const key = snap.peerRef;
    const existing = this.#byRef.get(key);
    if (existing) {
      existing._update(snap);
      this.#byTailscale.set(existing.tailscaleId, existing);
      return existing;
    }
    const peer = Peer._create(this.#node, snap);
    // A fresh ref for a tailscale id we already track is a new generation —
    // retire the superseded handle so message attribution cannot resurrect
    // it and #byRef cannot grow across rejoins.
    const prev = this.#byTailscale.get(peer.tailscaleId);
    if (prev && prev.ref !== key) {
      prev._markLeft();
      this.#byRef.delete(prev.ref);
    }
    this.#byRef.set(key, peer);
    this.#byTailscale.set(peer.tailscaleId, peer);
    return peer;
  }

  /** Resolve a stored handle by ref (no create). */
  get(ref: string): Peer | undefined {
    return this.#byRef.get(ref);
  }

  /** Latest interned handle for a Tailscale id, if any. */
  getByTailscaleId(tailscaleId: string): Peer | undefined {
    return this.#byTailscale.get(tailscaleId);
  }

  /** Drop a generation after `left` so rejoin allocates a new object. */
  remove(ref: string): void {
    const peer = this.#byRef.get(ref);
    this.#byRef.delete(ref);
    if (peer && this.#byTailscale.get(peer.tailscaleId) === peer) {
      this.#byTailscale.delete(peer.tailscaleId);
    }
  }

  /** Drop by tailscale id (all generations) — used when only id is known. */
  removeByTailscaleId(tailscaleId: string): void {
    for (const [key, peer] of this.#byRef) {
      if (peer.tailscaleId === tailscaleId) {
        this.#byRef.delete(key);
      }
    }
    this.#byTailscale.delete(tailscaleId);
  }

  clear(): void {
    this.#byRef.clear();
    this.#byTailscale.clear();
  }

  /** Map a snapshot list, interning each. */
  upsertAll(snaps: PeerSnapshot[]): Peer[] {
    return snaps.map((s) => this.upsert(s));
  }
}

/** `Peer` or a query string resolved by the node. */
export type PeerLike = Peer | string;

export function isPeer(value: unknown): value is Peer {
  return value instanceof Peer;
}

/** Resolve PeerLike to a native route / query string. */
export function peerLikeToQuery(to: PeerLike): string {
  if (isPeer(to)) {
    // Route handles by ref: generation-checked (stale handle → PeerGone,
    // RFC 022 I5) and keyed by the authenticated Tailscale id, not the
    // self-declared ULID.
    return to.ref;
  }
  return to;
}
