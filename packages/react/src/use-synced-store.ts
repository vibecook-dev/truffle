import { useState, useEffect, useCallback, useRef } from 'react';
import type { MeshNode, NapiNode } from '@vibecook/truffle';
import { getNativeNode } from './native-node.js';
import {
  subscribeSyncedStore,
  type SyncedStoreController,
  type SyncedStoreLike,
} from './synced-store-controller.js';

/** A device-owned slice of data. */
export interface Slice<T> {
  deviceId: string;
  data: T;
  version: number;
  updatedAt: number;
}

/** Events emitted by the synced store. */
export type StoreEvent<T> =
  | { type: 'local_changed'; data: T }
  | { type: 'peer_updated'; deviceId: string; data: T; version: number }
  | { type: 'peer_removed'; deviceId: string };

export interface UseSyncedStoreResult<T> {
  /** This device's current data, or null if not yet set. */
  localData: T | null;
  /** All device slices (local + remote), keyed by device ID. */
  allSlices: Map<string, Slice<T>>;
  /** List of device IDs with data in this store. */
  deviceIds: string[];
  /** Current local version number. */
  version: number;
  /** Update this device's data (broadcasts automatically). */
  set: (data: T) => Promise<void>;
}

/**
 * React hook for a Truffle synced store.
 *
 * Provides reactive access to the native RFC 016 synced-store implementation,
 * including current snapshots, peer removal, and automatic peer synchronization.
 *
 * @example
 * ```tsx
 * const { localData, allSlices, set } = useSyncedStore<MyState>(node, 'app-state');
 *
 * // Update local state (broadcasts to all peers)
 * set({ activeProject: 'truffle', status: 'coding' });
 *
 * // Read all peers' state
 * for (const [deviceId, slice] of allSlices) {
 *   console.log(`${deviceId}: ${slice.data.activeProject}`);
 * }
 * ```
 */
export function useSyncedStore<T>(
  input: MeshNode | NapiNode | null,
  storeId: string,
): UseSyncedStoreResult<T> {
  const node = getNativeNode(input);
  const [localData, setLocalData] = useState<T | null>(null);
  const [allSlices, setAllSlices] = useState<Map<string, Slice<T>>>(new Map());
  const [version, setVersion] = useState(0);
  const storeRef = useRef<{
    store: SyncedStoreLike<T>;
    controller: SyncedStoreController;
  } | null>(null);

  useEffect(() => {
    setLocalData(null);
    setAllSlices(new Map());
    setVersion(0);

    if (!node) {
      return;
    }

    let store: SyncedStoreLike<T>;
    try {
      store = node.syncedStore(storeId) as unknown as SyncedStoreLike<T>;
    } catch (error) {
      console.error('[truffle-react] Failed to create synced store:', error);
      return;
    }

    const controller = subscribeSyncedStore(
      store,
      (snapshot) => {
        setLocalData(snapshot.localData);
        setAllSlices(snapshot.allSlices);
        setVersion(snapshot.version);
      },
      (error) => {
        console.error('[truffle-react] Synced store failed:', error);
      },
    );
    storeRef.current = { store, controller };

    return () => {
      if (storeRef.current?.controller === controller) {
        storeRef.current = null;
      }
      void controller.close();
    };
  }, [node, storeId]);

  const set = useCallback(async (data: T): Promise<void> => {
    const current = storeRef.current;
    if (!current) return;
    await current.store.set(data);
    await current.controller.refresh();
  }, []);

  return {
    localData,
    allSlices,
    deviceIds: Array.from(allSlices.keys()),
    version,
    set,
  };
}
