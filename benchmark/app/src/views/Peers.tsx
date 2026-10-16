// Peers: every other truffle bench on the tailnet (RFC 027 §11.3). Other-account peers are a
// local view filter; whether this device accepts runs from other accounts is a Setting.

import { useState } from 'react';
import type { BenchPeer } from '../api';
import { useAppState, useAppStore } from '../state/context';
import { runBlockReason } from '../state/store';
import { load, save } from '../hooks/storage';
import { Button } from '../components/Button';
import { Switch } from '../components/Controls';
import { Facts, Table, type Column } from '../components/Data';
import { Empty, Notice } from '../components/Notice';
import { Page, Section } from '../components/Misc';
import { Status } from '../components/Status';
import { useConstructionFlag } from '../charts/construction';
import { fmtAgo, fmtConnection, fmtNumber, fmtOs, plural, DASH } from '../lib/format';
import { BuildOf } from './History';

export function Peers() {
  const store = useAppStore();
  const s = useAppState();
  const construction = useConstructionFlag();
  const [showOthers, setShowOthers] = useState<boolean>(() => load('peers/show-others', false));
  const [selected, setSelected] = useState<string | null>(null);
  const local = s.node.local;
  const target = store.target();

  const others = s.peers.filter((p) => !p.same_login);
  const shown = s.peers.filter((p) => p.same_login || showOthers);
  const hidden = showOthers ? 0 : others.length;
  const sameOnline = s.peers.filter((p) => p.same_login && p.online);
  const sel = shown.find((p) => p.id === selected) ?? target ?? shown[0] ?? null;

  const columns: Column<BenchPeer>[] = [
    {
      key: 'state',
      label: 'State',
      width: 104,
      render: (p) =>
        p.online ? <Status kind="live">Online</Status> : <Status kind="off">Offline</Status>,
    },
    {
      key: 'name',
      label: 'Name',
      render: (p) => (
        <span className="pr-name">
          <span className="pr-n">{p.device_name}</span>
          {target?.id === p.id ? <span className="chip">target</span> : null}
          {!p.same_login ? (
            <span className="chip quiet">{p.login_name ?? 'other account'}</span>
          ) : null}
        </span>
      ),
    },
    { key: 'os', label: 'OS', render: (p) => fmtOs(p.os) },
    {
      key: 'path',
      label: 'Path',
      render: (p) => (
        <span>
          {p.online
            ? fmtConnection(p.connection)
            : p.last_seen
              ? `seen ${fmtAgo(p.last_seen)}`
              : DASH}
        </span>
      ),
    },
    {
      key: 'rtt',
      label: 'RTT',
      num: true,
      render: (p) => (p.rtt_ms == null ? DASH : `${fmtNumber(p.rtt_ms)} ms`),
    },
    {
      key: 'app',
      label: 'Runs',
      render: (p) =>
        p.hello ? (
          <span className="mono">
            {p.hello.app.replace('truffle-bench-', '')} {p.hello.tool_version}
          </span>
        ) : (
          DASH
        ),
    },
    {
      key: 'build',
      label: 'Build',
      // its own column: a debug peer makes every run against it unrepresentative
      render: (p) => (
        <span className="mono">
          <BuildOf profile={p.hello?.build_profile} />
        </span>
      ),
    },
    {
      key: 'host',
      label: 'Same host',
      // the engine's verdict from both host fingerprints; null when it can't tell
      render: (p) =>
        p.same_host == null ? DASH : p.same_host ? 'Yes' : <span className="muted">No</span>,
    },
    {
      key: 'accepts',
      label: 'Accepts your runs',
      render: (p) =>
        p.hello ? p.hello.accepts_runs_from_you ? 'Yes' : <span className="muted">No</span> : DASH,
    },
  ];

  const tailnet = local?.tailnet ?? 'your tailnet';
  const login = local?.login_name ?? 'your account';

  return (
    <Page
      construction={construction}
      eyebrow="Peers"
      title="Peers"
      lede={`Tailnet ${tailnet} · signed in as ${login}`}
      actions={
        <Switch
          checked={showOthers}
          onChange={(v) => {
            setShowOthers(v);
            save('peers/show-others', v);
          }}
        >
          Show other accounts
        </Switch>
      }
    >
      <Section
        label="On this tailnet"
        meta={[
          plural(sameOnline.length, 'peer') + ' online',
          hidden ? `${plural(hidden, 'peer')} on other accounts hidden` : null,
        ]
          .filter(Boolean)
          .join(' · ')}
      >
        {sameOnline.length === 0 ? (
          <Empty title="No peers yet">
            No other truffle bench is signed in to {tailnet} as {login}. Open truffle bench on
            another machine and sign in with the same account, or run{' '}
            <code>truffle-bench serve</code> there.
          </Empty>
        ) : null}
        {shown.length ? (
          <Table
            caption="Peers"
            columns={columns}
            rows={shown}
            rowKey={(p) => p.id}
            selected={sel?.id ?? null}
            onRowClick={(p) => setSelected(p.id)}
            rowClass={(p) => (p.online ? '' : 'dim')}
          />
        ) : null}
      </Section>

      {sel ? peerDetail(sel, target?.id === sel.id) : null}
    </Page>
  );

  function peerDetail(peer: BenchPeer, isTarget: boolean) {
    const h = peer.hello;
    const reason = isTarget ? runBlockReason(s, peer, 'tcp') : null;
    return (
      <Section
        label={peer.device_name}
        meta={isTarget ? 'The target' : undefined}
        actions={
          <>
            <Button
              size="small"
              icon="refresh"
              disabled={!peer.online}
              onClick={() => void store.probe(peer)}
            >
              Probe
            </Button>
            <Button
              size="small"
              variant={isTarget ? 'quiet' : 'secondary'}
              disabled={isTarget}
              onClick={() => store.setTarget(peer)}
            >
              {isTarget ? 'Target' : 'Set as target'}
            </Button>
          </>
        }
      >
        <div className="peer-detail">
          <article className="card">
            <Facts
              rows={[
                ['Tailscale IP', peer.ip, true],
                ['Hostname', peer.hostname, true],
                ['Login', peer.login_name ?? DASH, true],
                ['Path', peer.online ? fmtConnection(peer.connection) : 'offline', true],
                ['RTT', peer.rtt_ms == null ? DASH : `${fmtNumber(peer.rtt_ms)} ms`, true],
              ]}
            />
          </article>
          <article className="card">
            <Facts
              rows={
                h
                  ? [
                      ['Tool', `${h.app} ${h.tool_version}`, true],
                      ['truffle', h.truffle_version, true],
                      ['Build', <BuildOf profile={h.build_profile} />, true],
                      [
                        'System',
                        `${fmtOs(h.os)} ${h.arch}${h.logical_cpus ? ` · ${h.logical_cpus} CPUs` : ''}`,
                        true,
                      ],
                      ['Workloads', String(h.workloads.length), true],
                    ]
                  : [['Hello', peer.online ? 'Waiting for an answer' : 'Offline']]
              }
            />
          </article>
        </div>
        {isTarget && reason ? (
          <Notice kind="quiet" title="Runs can’t start yet">
            {reason}
          </Notice>
        ) : null}
        {h && !h.accepts_runs_from_you ? (
          <Notice kind="quiet" title={`${peer.device_name} doesn’t accept runs from you`}>
            It is signed in as {peer.login_name ?? 'another account'}. It accepts your runs only if
            its owner turns on “Accept runs from other accounts” in its settings.
          </Notice>
        ) : null}
      </Section>
    );
  }
}
