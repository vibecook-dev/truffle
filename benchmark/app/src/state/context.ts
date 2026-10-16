import { createContext, useContext, useSyncExternalStore } from 'react';
import type { State, Store } from './store';

export const StoreContext = createContext<Store | null>(null);

export function useAppStore(): Store {
  const s = useContext(StoreContext);
  if (!s) throw new Error('StoreContext is missing');
  return s;
}

/** The whole state; re-renders on every change (the state object is replaced, never mutated). */
export function useAppState(): State {
  const store = useAppStore();
  return useSyncExternalStore(store.subscribe, store.get);
}
