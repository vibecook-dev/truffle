// The ⌥ construction flag, shared with charts through context so the app-level hook stays the
// only source (RFC 027 §11.4).

import { createContext, useContext } from 'react';

export const ConstructionContext = createContext(false);

export function useConstructionFlag(): boolean {
  return useContext(ConstructionContext);
}
