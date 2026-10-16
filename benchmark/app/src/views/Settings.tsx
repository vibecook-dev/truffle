// Settings (RFC 027 §11.3): device name, accepting runs from other accounts, theme, the data
// folder, signing out, and about: versions, the sidecar, the JY lockup and the licences.

import { useEffect, useState, type ReactNode } from 'react';
import type { ThemeSetting } from '../api';
import { useAppState, useAppStore } from '../state/context';
import { Button, ConfirmButton } from '../components/Button';
import { Seg, Switch, TextInput } from '../components/Controls';
import { Facts } from '../components/Data';
import { Page, Section } from '../components/Misc';
import { Icon } from '../components/Icon';
import { useConstructionFlag } from '../charts/construction';
import { jy } from '../lib/jy';
import { DASH, fmtOs } from '../lib/format';
import notice from '../brand/NOTICE?raw';
import jostOfl from '../brand/fonts/Jost-OFL.txt?raw';
import dmMonoOfl from '../brand/fonts/DMMono-OFL.txt?raw';

function Row({
  label,
  hint,
  children,
  htmlFor,
}: {
  label: ReactNode;
  hint?: ReactNode;
  children: ReactNode;
  htmlFor?: string;
}) {
  return (
    <div className="form-row">
      <div className="fr-label">
        {htmlFor ? <label htmlFor={htmlFor}>{label}</label> : <span>{label}</span>}
        {hint ? <span className="fr-hint">{hint}</span> : null}
      </div>
      <div className="fr-control">{children}</div>
    </div>
  );
}

export function Settings() {
  const store = useAppStore();
  const s = useAppState();
  const construction = useConstructionFlag();
  const settings = s.settings;
  const info = s.appInfo;
  const local = s.node.local;
  const [name, setName] = useState(settings?.device_name ?? '');
  useEffect(() => {
    if (settings?.device_name) setName(settings.device_name);
  }, [settings?.device_name]);
  const dirty = !!settings && name.trim() !== '' && name.trim() !== settings.device_name;
  const J = jy();

  return (
    <Page
      construction={construction}
      eyebrow="Settings"
      title="Settings"
      lede="How this device appears on your tailnet, who may measure it, and what it runs."
    >
      <Section label="This device">
        <div className="form-rows">
          <Row
            label="Device name"
            htmlFor="set-name"
            hint="The name peers see. A new name applies the next time this device connects; it stays signed in."
          >
            <div className="inline">
              <TextInput
                id="set-name"
                value={name}
                onChange={setName}
                className="fr-input"
                aria-describedby="set-name-hint"
              />
              <Button
                disabled={!dirty}
                onClick={() => void store.saveSettings({ device_name: name.trim() })}
              >
                Save
              </Button>
            </div>
            <span id="set-name-hint" className="caption muted">
              On the tailnet as {local?.tailscale_hostname ?? s.node.tailscale_hostname ?? DASH}
            </span>
          </Row>
          <Row
            label="Accept runs from other accounts"
            hint="Off by default. When it’s on, peers signed in as someone else can measure this device. Peers of your own account always can."
          >
            <Switch
              checked={!!settings?.allow_other_logins}
              onChange={(v) => void store.saveSettings({ allow_other_logins: v })}
            >
              {settings?.allow_other_logins ? 'Accepting runs from any account' : 'Only my account'}
            </Switch>
          </Row>
          <Row label="Theme" hint="System follows macOS.">
            <Seg
              label="Theme"
              value={settings?.theme ?? 'system'}
              options={[
                { value: 'system', label: 'System' },
                { value: 'light', label: 'Light' },
                { value: 'dark', label: 'Dark' },
              ]}
              onChange={(v) => void store.saveSettings({ theme: v as ThemeSetting })}
            />
          </Row>
          <Row label="Data folder" hint="Run reports, imports and this device’s Tailscale state.">
            <div className="stack tight">
              <span className="mono wrap">{info?.data_dir ?? DASH}</span>
              <span>
                <Button size="small" icon="folder" onClick={() => void store.openDataDir()}>
                  Open the data folder
                </Button>
              </span>
            </div>
          </Row>
          <Row
            label="Sign out"
            hint="Logs this device out of Tailscale, stops it, and deletes its state. Run reports stay."
          >
            <span>
              <ConfirmButton
                size="md"
                confirm="Sign out and delete the state"
                onConfirm={() => void store.signOut()}
              >
                Sign out
              </ConfirmButton>
            </span>
          </Row>
        </div>
      </Section>

      <Section label="About">
        <div className="about">
          <div className="about-mark">
            {J?.Lockup ? (
              <J.Lockup layout="inline" size={14} title="James Yong" />
            ) : (
              <span>James Yong</span>
            )}
            <p className="ink2">
              <span className="product">truffle bench</span> measures truffle between your own
              machines.
            </p>
          </div>
          <article className="card tight">
            <Facts
              rows={[
                ['truffle bench', info?.app_version ?? DASH, true],
                ['truffle', info?.truffle_version ?? DASH, true],
                [
                  'Build',
                  info ? (
                    info.build_profile === 'debug' ? (
                      <span className="chip strong">debug</span>
                    ) : (
                      'release'
                    )
                  ) : (
                    DASH
                  ),
                  true,
                ],
                ['Profile', info?.profile ?? 'default', true],
                ['System', fmtOs(info?.os), true],
                ['Tailnet', local?.tailnet ?? DASH, true],
              ]}
            />
          </article>
          <article className="card tight">
            <dl className="facts long">
              <div>
                <dt>Sidecar</dt>
                <dd className="num wrap">{info?.sidecar_path ?? DASH}</dd>
              </div>
              <div>
                <dt>Sidecar SHA-256</dt>
                <dd className="num wrap">{info?.sidecar_sha256 ?? DASH}</dd>
              </div>
            </dl>
          </article>
        </div>
      </Section>

      <Section label="Licences">
        <ul className="licences">
          <li>
            <span className="lc-name">truffle bench and truffle</span>
            <span className="caption">MIT</span>
          </li>
          <li>
            <span className="lc-name">The JY mark, wordmark and lockups</span>
            <span className="caption">James Yong, all rights reserved; not covered by MIT</span>
          </li>
          <li>
            <span className="lc-name">Jost</span>
            <span className="caption">SIL Open Font License 1.1</span>
          </li>
          <li>
            <span className="lc-name">DM Mono</span>
            <span className="caption">SIL Open Font License 1.1</span>
          </li>
        </ul>
        <details className="disclosure">
          <summary>
            <Icon name="chevron-right" size={16} />
            Brand files notice
          </summary>
          <pre className="code-block">{notice}</pre>
        </details>
        <details className="disclosure">
          <summary>
            <Icon name="chevron-right" size={16} />
            Jost licence
          </summary>
          <pre className="code-block">{jostOfl}</pre>
        </details>
        <details className="disclosure">
          <summary>
            <Icon name="chevron-right" size={16} />
            DM Mono licence
          </summary>
          <pre className="code-block">{dmMonoOfl}</pre>
        </details>
      </Section>
    </Page>
  );
}
