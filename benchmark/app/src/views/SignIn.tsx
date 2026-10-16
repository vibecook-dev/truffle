// Sign in: the whole window until the node runs (RFC 027 §11.3). With a saved state dir the
// hub starts on its own and this says "Connecting as <device>"; the form shows otherwise, and
// the login URL only on needs-login.

import { useEffect, useRef, useState, type ReactNode } from 'react';
import { useAppState, useAppStore } from '../state/context';
import { Button } from '../components/Button';
import { Field, TextInput } from '../components/Controls';
import { Notice } from '../components/Notice';
import { Icon } from '../components/Icon';
import { Loader, Mark } from '../components/Misc';
import { useNow } from '../hooks/useNow';
import { fmtClock } from '../lib/format';

const LOGIN_WINDOW_S = 300;
const APPROVAL_HINT_AFTER_S = 30;
const ADMIN_URL = 'https://login.tailscale.com/admin/machines';

/** Dev-only: ?opened=1 pretends the login link was already opened (for screenshots). */
const DEV_OPENED =
  import.meta.env.DEV &&
  typeof window !== 'undefined' &&
  new URLSearchParams(window.location.search).get('opened') === '1';

export function SignIn() {
  const store = useAppStore();
  const s = useAppState();
  const node = s.node;
  const [device, setDevice] = useState(s.settings?.device_name ?? '');
  const [key, setKey] = useState('');
  const [initiated, setInitiated] = useState(false);
  const [openedAt, setOpenedAt] = useState<number | null>(DEV_OPENED ? node.since_ms : null);
  const [copied, setCopied] = useState(false);
  const now = useNow(1000);
  const urlRef = useRef<HTMLInputElement | null>(null);

  // seed the field once with the saved name (settings may load after this mounts); after that
  // the field is the user's, so emptying it to type a new name doesn't bring the old one back
  const seeded = useRef(!!s.settings?.device_name);
  useEffect(() => {
    const n = s.settings?.device_name;
    if (seeded.current || !n) return;
    seeded.current = true;
    setDevice((d) => d || n);
  }, [s.settings?.device_name]);

  const hostname =
    node.tailscale_hostname ?? (device ? `truffle-truffle-bench-${device}` : 'this device');
  const deviceName = s.settings?.device_name || device || 'this device';

  const start = (withKey: boolean) => {
    setInitiated(true);
    setOpenedAt(null);
    void store.signIn(device, withKey ? key : undefined);
    if (withKey) setKey('');
  };

  let body: ReactNode;
  if (node.state === 'starting') {
    body = (
      <div className="si-wait" role="status">
        <Loader size={10} label="Connecting" />
        <span>{initiated ? 'Starting Tailscale…' : `Connecting as ${deviceName}`}</span>
      </div>
    );
  } else if (node.state === 'stopping') {
    body = (
      <div className="si-wait" role="status">
        <Loader size={10} label="Signing out" />
        <span>Signing out…</span>
      </div>
    );
  } else if (node.state === 'needs-login' && node.auth_url) {
    const url = node.auth_url;
    const left = LOGIN_WINDOW_S - (now - node.since_ms) / 1000;
    const showApproval = openedAt != null && (now - openedAt) / 1000 >= APPROVAL_HINT_AFTER_S;
    body = (
      <div className="si-step">
        <h1 className="si-title">Sign in with Tailscale</h1>
        <p className="si-lede">
          This adds a device called <span className="mono si-host">{hostname}</span> to your
          tailnet.
        </p>
        <div className="si-url">
          <input
            ref={urlRef}
            className="input mono"
            readOnly
            value={url}
            aria-label="Sign-in link"
            onFocus={(e) => e.currentTarget.select()}
          />
          <div className="inline">
            <Button
              variant="primary"
              icon="arrow-up-right"
              onClick={() => {
                setOpenedAt((t) => t ?? Date.now());
                void store.openUrl(url);
              }}
            >
              Open
            </Button>
            <Button
              icon="copy"
              onClick={async () => {
                setOpenedAt((t) => t ?? Date.now());
                try {
                  await navigator.clipboard.writeText(url);
                  setCopied(true);
                  window.setTimeout(() => setCopied(false), 2000);
                } catch {
                  urlRef.current?.select();
                }
              }}
            >
              {copied ? 'Copied' : 'Copy'}
            </Button>
          </div>
        </div>
        <p className="si-count">
          {left > 0 ? (
            <>
              The link works for <span className="mono">{fmtClock(left)}</span>
            </>
          ) : (
            'The link has expired.'
          )}
        </p>
        {showApproval ? (
          <Notice
            title="Waiting for approval?"
            actions={
              <Button
                size="small"
                icon="arrow-up-right"
                onClick={() => void store.openUrl(ADMIN_URL)}
              >
                Open admin console
              </Button>
            }
          >
            If your tailnet requires device approval, approve{' '}
            <span className="mono nowrap">{hostname}</span> in the admin console.
          </Notice>
        ) : null}
        <div className="si-foot">
          <Button variant="quiet" size="small" onClick={() => void store.stopHub()}>
            Cancel
          </Button>
        </div>
      </div>
    );
  } else {
    // idle, stopped, error, or needs-login without a URL yet
    const error = node.state === 'error' ? node.error : null;
    // another instance holds this profile: a new name here would only start a second node in the
    // same profile, so the form offers just "Try again" (Rust keeps the name too, §11.1)
    const inUse = error?.code === 'state-in-use';
    body = (
      <form
        className="si-step"
        onSubmit={(e) => {
          e.preventDefault();
          start(false);
        }}
      >
        <h1 className="si-title">Sign in to your tailnet</h1>
        <p className="si-lede">
          truffle bench joins your tailnet as its own device, then finds the other copies of it
          you’ve signed in with the same account.
        </p>
        {error ? (
          <Notice
            kind="error"
            title="Sign-in didn’t finish"
            actions={
              <Button variant="primary" size="small" icon="refresh" onClick={() => start(false)}>
                Try again
              </Button>
            }
          >
            {error.message}
          </Notice>
        ) : null}
        {inUse ? null : (
          <Field label="Device name" htmlFor="si-device" hint="Your other devices see this name.">
            <TextInput
              id="si-device"
              value={device}
              onChange={setDevice}
              autoComplete="off"
              placeholder="This computer’s name"
            />
          </Field>
        )}
        {!error ? (
          <Button type="submit" variant="primary" size="large" wide disabled={!device.trim()}>
            Sign in with Tailscale
          </Button>
        ) : null}
        {inUse ? null : (
          <details className="disclosure si-key">
            <summary>
              <Icon name="chevron-right" size={16} />
              Use an auth key
            </summary>
            <div className="stack">
              <p className="muted">
                A key makes this a tagged device. It only matches peers that also use a key, unless
                both accept other accounts.
              </p>
              <Field label="Auth key" htmlFor="si-key">
                <input
                  id="si-key"
                  className="input mono"
                  type="password"
                  value={key}
                  autoComplete="off"
                  spellCheck={false}
                  placeholder="tskey-auth-…"
                  onChange={(e) => setKey(e.target.value)}
                />
              </Field>
              <Button disabled={!key.trim() || !device.trim()} onClick={() => start(true)}>
                Sign in with the key
              </Button>
            </div>
          </details>
        )}
      </form>
    );
  }

  return (
    <div className="signin">
      <div className="si-box">
        <div className="si-brand">
          <Mark size={24} reveal title="" />
          <span className="si-product">truffle bench</span>
        </div>
        {body}
      </div>
    </div>
  );
}
