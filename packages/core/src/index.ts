// Re-export classes (values) from the native Rust addon
export {
  NapiNode,
  NapiFileTransfer,
  NapiOfferResponder,
  NapiSyncedStore,
  NapiSubscription,
  NapiProxy,
  NapiTcpSocket,
  NapiTcpListener,
  NapiUdpSocket,
  NapiQuicConnection,
  NapiQuicListener,
  NapiQuicStream,
} from '@vibecook/truffle-native';

// Re-export interfaces/types from the native Rust addon
export type {
  NapiNodeConfig,
  NapiNodeIdentity,
  NapiPeer,
  NapiPeerIdentity,
  NapiPingResult,
  NapiHealthInfo,
  NapiPeerEvent,
  NapiNamespacedMessage,
  NapiFileOffer,
  NapiTransferResult,
  NapiTransferProgress,
  NapiFileTransferEvent,
  NapiSlice,
  NapiStoreEvent,
  NapiProxyConfig,
  NapiProxyInfo,
  NapiProxyEvent,
  NapiDatagram,
} from '@vibecook/truffle-native';

// Sidecar binary resolution
export { resolveSidecarPath } from './sidecar.js';

// node:net-shaped raw TCP API over the mesh (RFC 021)
export {
  TruffleSocket,
  TruffleServer,
  createNetNamespace,
  type TruffleNet,
  type NetConnectOptions,
  type NetServerOptions,
  type NetHooks,
  type ConnectionListener,
} from './net.js';

// node:http interop over the mesh (RFC 021; server half RFC 023)
export {
  MeshAgent,
  createHttpNamespace,
  createMeshHttpServer,
  type TruffleHttp,
  type MeshHttpServer,
  type MeshHttpServerOptions,
} from './http.js';

// QUIC over the mesh (RFC 021)
export {
  TruffleQuicStream,
  TruffleQuicConnection,
  TruffleQuicServer,
  createQuicNamespace,
  type TruffleQuic,
} from './quic.js';

// node:dgram-shaped raw UDP API over the mesh (RFC 021)
export {
  TruffleDgramSocket,
  createDgramNamespace,
  type TruffleDgram,
  type TruffleRemoteInfo,
} from './dgram.js';

// WebSocket over the mesh via the `ws` package (RFC 021)
export {
  createWsNamespace,
  type TruffleWs,
  type TruffleWsServer,
  type TruffleWsServerOptions,
  type WsLoader,
} from './ws.js';

// Declarative HTTP serving over the tailnet (RFC 023 §6.2)
export {
  ServeHandle,
  createServeNamespace,
  normalizeServeConfig,
  type TruffleServe,
  type ServeConfig,
  type ServeOptions,
  type ServeTargetConfig,
  type ServeStaticConfig,
  type ServeRoutesConfig,
  type ServeRouteValue,
  type ServeErrorInfo,
} from './serve.js';

// High-level API
export {
  createMeshNode,
  type CreateMeshNodeOptions,
  type MeshNode,
  type MeshPeerEvent,
  type MeshNamespacedMessage,
} from './create-mesh-node.js';

// RFC 022 Peer handles
export {
  Peer,
  PeerRegistry,
  isPeer,
  peerLikeToQuery,
  type PeerLike,
  type PeerRef,
  type PeerSnapshot,
} from './peer.js';
