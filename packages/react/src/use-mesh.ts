import { useState, useEffect, useCallback, useRef } from 'react';
import type {
  MeshNode,
  NapiNode,
  NapiPeer,
  NapiPeerEvent,
  NapiNodeIdentity,
} from '@vibecook/truffle';
import { getNativeNode } from './native-node.js';

export interface UseMeshResult {
  /** All known peers (from Tailscale discovery). */
  peers: NapiPeer[];
  /** Local node identity (id, hostname, name, ip). */
  localInfo: NapiNodeIdentity | null;
  /** Whether the node has been started. */
  isStarted: boolean;
  /** Send a namespaced JSON message to a specific peer. */
  send: (peerId: string, namespace: string, payload: unknown) => Promise<void>;
  /** Broadcast a namespaced JSON message to all connected peers. */
  broadcast: (namespace: string, payload: unknown) => Promise<void>;
}

/**
 * React hook for Truffle mesh networking.
 *
 * Pass the mesh returned by `createMeshNode()` or an already-started
 * `NapiNode`. The hook returns native peer snapshots.
 *
 * @example
 * ```tsx
 * const { peers, send, broadcast } = useMesh(node);
 *
 * await send('peer-id', 'chat', { text: 'hello' });
 * await broadcast('chat', { text: 'hello everyone' });
 * ```
 */
export function useMesh(input: MeshNode | NapiNode | null): UseMeshResult {
  const node = getNativeNode(input);
  const [peers, setPeers] = useState<NapiPeer[]>([]);
  const [localInfo, setLocalInfo] = useState<NapiNodeIdentity | null>(null);
  const [isStarted, setIsStarted] = useState(false);
  const nodeRef = useRef<NapiNode | null>(null);

  useEffect(() => {
    setIsStarted(false);
    setLocalInfo(null);
    setPeers([]);
    if (!node) {
      return;
    }

    nodeRef.current = node;
    let cancelled = false;

    // Fetch initial state
    const init = async () => {
      try {
        const info = node.getLocalInfo();
        if (!cancelled) {
          setLocalInfo(info);
          setIsStarted(true);
        }

        const currentPeers = await node.getPeers();
        if (!cancelled) setPeers(currentPeers);
      } catch {
        if (!cancelled) setIsStarted(false);
      }
    };
    init();

    // Subscribe to peer change events. Rows are keyed by the Tailscale
    // stable id (`event.peerId`): it is the one identifier present on every
    // peer-scoped event and stable for the entry's lifetime, while
    // `deviceId` is null until identity is learned (RFC 022) — keying on it
    // would collide all pre-identity peers and never match `left` events.
    const subscription = node.onPeerChange((event: NapiPeerEvent) => {
      if (cancelled) return;
      const key = event.peerId;
      if (!key) return; // auth_required has no peer

      switch (event.eventType) {
        case 'left':
          setPeers((prev) => prev.filter((p) => p.tailscaleId !== key));
          break;

        case 'joined':
        case 'updated':
        case 'identity': // the only carrier of the durable deviceId
        case 'ws_connected':
        case 'ws_disconnected': {
          const snap = event.peer;
          setPeers((prev) => {
            if (snap) {
              const idx = prev.findIndex((p) => p.tailscaleId === key);
              if (idx >= 0) {
                const next = [...prev];
                next[idx] = snap;
                return next;
              }
              return [...prev, snap];
            }
            // ws_* events can arrive without a snapshot — patch the flag.
            const wsConnected = event.eventType === 'ws_connected';
            return prev.map((p) => (p.tailscaleId === key ? { ...p, wsConnected } : p));
          });
          break;
        }
      }
    });

    return () => {
      cancelled = true;
      nodeRef.current = null;
      subscription.close();
    };
  }, [node]);

  const send = useCallback(async (peerId: string, namespace: string, payload: unknown) => {
    if (!nodeRef.current) return;
    const data = Buffer.from(JSON.stringify(payload));
    await nodeRef.current.send(peerId, namespace, data);
  }, []);

  const broadcast = useCallback(async (namespace: string, payload: unknown) => {
    if (!nodeRef.current) return;
    const data = Buffer.from(JSON.stringify(payload));
    await nodeRef.current.broadcast(namespace, data);
  }, []);

  return { peers, localInfo, isStarted, send, broadcast };
}
