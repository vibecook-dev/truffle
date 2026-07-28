// mesh.quic — modern QUIC API over the mesh (RFC 021 Phase 3).
//
// Shape follows the RFC (and iroh's precedent): connect a peer → a
// connection carrying many concurrent bidirectional byte streams with no
// head-of-line blocking between them. Connections and streams are async
// iterable; each stream is a real `stream.Duplex` (same pull-model
// backpressure as `mesh.net` sockets).

import { Duplex } from 'node:stream';
import type {
  NapiNode,
  NapiPeerIdentity,
  NapiQuicConnection,
  NapiQuicListener,
  NapiQuicStream,
} from '@vibecook/truffle-native';
import { peerLikeToQuery, type PeerLike } from './peer.js';

/**
 * A bidirectional byte stream on a QUIC connection, as a `stream.Duplex`.
 *
 * Behaves like an independent TCP connection (ordered, reliable,
 * flow-controlled) without head-of-line blocking against sibling streams.
 * `end()` half-closes (the peer sees clean EOF); `destroy()` also stops
 * the read side.
 */
export class TruffleQuicStream extends Duplex {
  #native: NapiQuicStream;
  #reading = false;

  constructor(native: NapiQuicStream) {
    super({ allowHalfOpen: true });
    this.#native = native;
  }

  override _read(size: number): void {
    if (this.#reading) return;
    this.#reading = true;
    this.#native
      .read(size > 0 ? size : undefined)
      .then((chunk) => {
        this.#reading = false;
        this.push(chunk);
      })
      .catch((err) => {
        this.#reading = false;
        this.destroy(err as Error);
      });
  }

  override _write(
    chunk: Buffer,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ): void {
    this.#native.write(chunk).then(() => callback(null), callback);
  }

  override _final(callback: (error?: Error | null) => void): void {
    this.#native.finish().then(() => callback(null), callback);
  }

  override _destroy(error: Error | null, callback: (error: Error | null) => void): void {
    const finish = () => callback(error);
    this.#native.close().then(finish, finish);
  }
}

/**
 * A QUIC connection to a peer. Open streams with {@link openStream};
 * iterate peer-opened streams with `for await (const stream of conn)`.
 *
 * Streams are lazy: the peer's iterator does not yield a stream until the
 * opener writes its first bytes. Keep the connection object alive while
 * its streams are in use — closing it closes all of them.
 */
export class TruffleQuicConnection implements AsyncIterable<TruffleQuicStream> {
  #native: NapiQuicConnection;

  /** The peer's tailnet address (`100.x.x.x:port`). */
  readonly remoteAddress: string;
  /** The peer's stable device id when known. */
  readonly remotePeerId?: string;

  constructor(native: NapiQuicConnection) {
    this.#native = native;
    this.remoteAddress = native.remoteAddress();
    this.remotePeerId = native.remotePeerId() ?? undefined;
  }

  /**
   * The peer's full WhoIs identity — the tailnet's answer about the
   * connection's WireGuard-authenticated remote address, never anything the
   * stream claims about itself. Resolved lazily on first call and cached
   * for the connection's lifetime; works on outbound connections too.
   * `null` for anonymous callers, on pre-v3 sidecars, and when the lookup
   * fails (failures are retried on the next call).
   */
  remoteIdentity(): Promise<NapiPeerIdentity | null> {
    return this.#native.remoteIdentity();
  }

  /** Open a new bidirectional byte stream on this connection. */
  async openStream(): Promise<TruffleQuicStream> {
    return new TruffleQuicStream(await this.#native.openStream());
  }

  /**
   * Accept the next stream the peer opens; `null` once the connection has
   * closed. Prefer iteration: `for await (const stream of conn.streams())`.
   */
  async acceptStream(): Promise<TruffleQuicStream | null> {
    const native = await this.#native.acceptStream();
    return native === null ? null : new TruffleQuicStream(native);
  }

  /** Async iterator over peer-opened streams, ending when the connection closes. */
  streams(): AsyncIterableIterator<TruffleQuicStream> {
    return this[Symbol.asyncIterator]();
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<TruffleQuicStream> {
    for (;;) {
      const stream = await this.acceptStream();
      if (stream === null) return;
      yield stream;
    }
  }

  /** Close the connection and all its streams. */
  close(): void {
    this.#native.close();
  }
}

/**
 * A QUIC server on the mesh. Iterate incoming connections:
 *
 * ```ts
 * const server = await mesh.quic.listen(4433);
 * for await (const conn of server) {
 *   for await (const stream of conn.streams()) {
 *     stream.pipe(stream); // echo
 *   }
 * }
 * ```
 */
export class TruffleQuicServer implements AsyncIterable<TruffleQuicConnection> {
  #native: NapiQuicListener;

  /** The port this server is bound to. */
  readonly port: number;

  constructor(native: NapiQuicListener) {
    this.#native = native;
    this.port = native.port();
  }

  /** Accept the next incoming connection; `null` once the server is closed. */
  async accept(): Promise<TruffleQuicConnection | null> {
    const native = await this.#native.accept();
    return native === null ? null : new TruffleQuicConnection(native);
  }

  /** Alias of async iteration, mirroring the RFC's `sessions()` sketch. */
  connections(): AsyncIterableIterator<TruffleQuicConnection> {
    return this[Symbol.asyncIterator]();
  }

  async *[Symbol.asyncIterator](): AsyncIterableIterator<TruffleQuicConnection> {
    for (;;) {
      const conn = await this.accept();
      if (conn === null) return;
      yield conn;
    }
  }

  /** Close the server and every connection accepted from it. */
  close(): void {
    this.#native.close();
  }
}

/** QUIC namespace bound to a mesh node. */
export interface TruffleQuic {
  /**
   * Open a QUIC connection to a peer. `host` is a {@link PeerLike}
   * (handle or query string). Only same-app peers can complete the
   * handshake (ALPN scoping).
   */
  connect(host: PeerLike, port: number): Promise<TruffleQuicConnection>;
  /**
   * Listen for QUIC connections. Ports 443/9417 are reserved; port 0 is
   * not supported over the tsnet relay — choose an explicit port
   * (suggested default: 9420+).
   */
  listen(port: number): Promise<TruffleQuicServer>;
}

export function createQuicNamespace(node: NapiNode): TruffleQuic {
  return {
    async connect(host: PeerLike, port: number): Promise<TruffleQuicConnection> {
      return new TruffleQuicConnection(await node.connectQuic(peerLikeToQuery(host), port));
    },
    async listen(port: number): Promise<TruffleQuicServer> {
      return new TruffleQuicServer(await node.listenQuic(port));
    },
  };
}
