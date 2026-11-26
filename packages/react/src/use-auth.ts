import { useState, useEffect } from 'react';
import type { MeshNode, NapiNode, NapiPeerEvent } from '@vibecook/truffle';
import { getNativeNode } from './native-node.js';

export type AuthStatus = 'unknown' | 'required' | 'authenticated';

export interface UseAuthResult {
  /** Current auth status. */
  status: AuthStatus;
  /** The Tailscale auth URL, if auth is required. */
  authUrl: string | null;
  /** Whether auth is currently required. */
  isAuthRequired: boolean;
  /** Whether the node is fully authenticated. */
  isAuthenticated: boolean;
}

/**
 * React hook for tracking Tailscale auth state.
 *
 * Listens for `auth_required` peer events on a started mesh or native node.
 * Use the factory's `onAuthRequired` option for login prompts during startup.
 * Once peers start arriving, auth is considered complete.
 *
 * @example
 * ```tsx
 * const { isAuthRequired, authUrl } = useAuth(node);
 *
 * if (isAuthRequired && authUrl) {
 *   return <a href={authUrl}>Authenticate with Tailscale</a>;
 * }
 * ```
 */
export function useAuth(input: MeshNode | NapiNode | null): UseAuthResult {
  const node = getNativeNode(input);
  const [status, setStatus] = useState<AuthStatus>('unknown');
  const [authUrl, setAuthUrl] = useState<string | null>(null);

  useEffect(() => {
    setStatus('unknown');
    setAuthUrl(null);
    if (!node) return;
    let cancelled = false;

    // Check if we're already authenticated by trying to get peers
    const init = async () => {
      try {
        node.getLocalInfo(); // throws if not started
        if (!cancelled) setStatus('authenticated');
      } catch {
        // Not started yet — wait for events
      }
    };
    init();

    // Listen for auth events via onPeerChange
    const subscription = node.onPeerChange((event: NapiPeerEvent) => {
      if (cancelled) return;

      if (event.eventType === 'auth_required' && event.authUrl) {
        setStatus('required');
        setAuthUrl(event.authUrl);
      } else if (event.eventType === 'joined') {
        // Peers arriving means auth succeeded
        setStatus('authenticated');
        setAuthUrl(null);
      }
    });

    return () => {
      cancelled = true;
      subscription.close();
    };
  }, [node]);

  return {
    status,
    authUrl,
    isAuthRequired: status === 'required',
    isAuthenticated: status === 'authenticated',
  };
}
