// Overview: this device, the target, the latest result per workload (RFC 027 §11.3).

import { useMemo } from 'react';
import type { RunSummary, WorkloadKind } from '../api';
import { useAppState, useAppStore } from '../state/context';
import { runBlockReason } from '../state/store';
import { Button } from '../components/Button';
import { FactCard, Facts, Figure, Meter } from '../components/Data';
import { Empty, Notice } from '../components/Notice';
import { Page, Section } from '../components/Misc';
import { Status } from '../components/Status';
import { Icon } from '../components/Icon';
import { TopologyStrip } from '../components/Chrome';
import { FluidSparkline } from '../charts';
import { useConstructionFlag } from '../charts/construction';
import { REGISTRY, WORKLOAD_ORDER, headlineOf } from '../workloads/registry';
import { fmtAgo, fmtConnection, fmtNumber, relayRegion, DASH } from '../lib/format';
import { trendable } from '../lib/compare';
import { StatusOf } from './History';
import { phaseWords } from './RunParts';
import type { Page as PageId } from '../hooks/useRoute';

interface Latest {
  workload: WorkloadKind;
  run: RunSummary;
  trend: number[];
  trendCount: number;
}

/** Latest run per workload, with the last ten trendable runs that share its compare_key. */
function latestResults(runs: RunSummary[]): Latest[] {
  const out: Latest[] = [];
  for (const w of WORKLOAD_ORDER) {
    const mine = runs.filter((r) => r.workload === w && r.tool !== 'imported');
    const run = mine[0];
    if (!run) continue;
    const key = headlineOf(run)?.key;
    const same = mine
      .filter((r) => r.compare_key === run.compare_key && trendable(r))
      .slice(0, 10)
      .reverse();
    const trend = same
      .map((r) => r.summary.find((x) => x.key === key)?.value)
      .filter((v): v is number => v != null && Number.isFinite(v));
    out.push({ workload: w, run, trend, trendCount: same.length });
  }
  return out;
}

export function Overview({ go }: { go: (p: PageId, id?: string) => void }) {
  const store = useAppStore();
  const s = useAppState();
  const construction = useConstructionFlag();
  const local = s.node.local;
  const target = store.target();
  const latest = useMemo(() => latestResults(s.runs), [s.runs]);
  const suiteBlock = runBlockReason(s, target, 'tcp');
  const suite = s.suite;
  const suiteRunning = !!suite && !suite.result && !suite.error;
  const region = target?.online ? relayRegion(target.connection) : null;

  return (
    <Page
      construction={construction}
      eyebrow={local?.tailnet ?? 'Overview'}
      title="Overview"
      lede={
        target
          ? `Signed in as ${local?.login_name ?? 'unknown'}. Measuring against ${target.device_name}.`
          : `Signed in as ${local?.login_name ?? 'unknown'}. Choose a target to measure against.`
      }
      actions={
        suiteRunning ? (
          <Button icon="stop" onClick={() => suite && void store.cancel(suite.suite_id)}>
            Cancel the suite
          </Button>
        ) : (
          <Button
            variant="primary"
            icon="play"
            disabled={!!suiteBlock || !target}
            reason={suiteBlock}
            onClick={() => target && void store.runSuite('smoke', target)}
          >
            Run the smoke suite
          </Button>
        )
      }
    >
      {suiteRunning && suite ? (
        <Notice
          title={
            suite.progress
              ? `Running the ${suite.profile ?? ''} suite: ${suite.progress.index + 1} of ${suite.progress.total}`
              : suite.reattached
                ? 'A suite is running'
                : `Starting the ${suite.profile ?? ''} suite`
          }
          actions={
            <Button
              size="small"
              variant="quiet"
              trailingIcon="arrow-right"
              onClick={() => go('suite')}
            >
              Follow it in Suite
            </Button>
          }
        >
          <div className="stack ov-suite">
            <span>{suite.progress?.item_label ?? 'Preparing'}</span>
            <Meter
              wide
              label="Suite progress"
              fraction={
                suite.progress
                  ? (suite.progress.index + (suite.progress.run.fraction ?? 0)) /
                    suite.progress.total
                  : 0
              }
            >
              {phaseWords(suite.progress ? suite.progress.run.phase : 'preparing')}
            </Meter>
          </div>
        </Notice>
      ) : !suiteRunning && suiteBlock && target && s.remote.length ? (
        <Notice title={suiteBlock}>Runs on this device wait until the other run finishes.</Notice>
      ) : null}

      <div className="ov-grid">
        <Section label="This device" measure="this device">
          <article className="card">
            <div className="card-head">
              <Icon name="peer" />
              <h3>{local?.device_name ?? DASH}</h3>
              {local?.health.healthy ? (
                <Status kind="live">Connected</Status>
              ) : (
                <Status kind="waiting">Not healthy</Status>
              )}
            </div>
            <Facts
              rows={[
                ['Signed in as', local?.login_name ?? DASH, true],
                ['Tailnet', local?.tailnet ?? DASH, true],
                ['Tailscale IP', local?.tailscale_ip ?? DASH, true],
                ['DNS name', local?.dns_name ?? DASH, true],
                ['Health', local ? (local.health.warnings[0] ?? local.health.state) : DASH],
              ]}
            />
          </article>
        </Section>

        <Section
          label="Target"
          measure="target"
          actions={
            <>
              {target?.online ? (
                <Button
                  size="small"
                  variant="quiet"
                  icon="refresh"
                  onClick={() => void store.probe(target)}
                >
                  Probe
                </Button>
              ) : null}
              <Button
                size="small"
                variant="quiet"
                trailingIcon="arrow-right"
                onClick={() => go('peers')}
              >
                {target ? 'Change' : 'Choose a target'}
              </Button>
            </>
          }
        >
          <article className="card">
            <TopologyStrip local={local} target={target} />
            <Facts
              rows={
                target
                  ? [
                      ['Path', target.online ? fmtConnection(target.connection) : 'offline', true],
                      [
                        'Round trip',
                        target.rtt_ms == null ? DASH : `${fmtNumber(target.rtt_ms)} ms`,
                        true,
                      ],
                      [
                        'Runs',
                        target.hello
                          ? `${target.hello.app.replace('truffle-bench-', '')} ${target.hello.tool_version}, ${target.hello.build_profile}`
                          : DASH,
                        true,
                      ],
                      [
                        'Accepts runs from you',
                        target.hello ? (target.hello.accepts_runs_from_you ? 'Yes' : 'No') : DASH,
                      ],
                    ]
                  : [['Target', 'None yet']]
              }
            />
          </article>
        </Section>
      </div>

      {region ? (
        <Notice title={`This path is relayed through DERP ${region}.`}>
          If both machines share a network, check System Settings → Privacy &amp; Security → Local
          Network → truffle bench.
        </Notice>
      ) : null}

      <Section
        label="Latest results"
        meta="A trend only joins runs with the same settings, path and build"
      >
        {latest.length ? (
          <div className="results-grid">
            {latest.map((l) => {
              const meta = REGISTRY[l.workload];
              const head = headlineOf(l.run);
              // redline only for a failure: a cancelled run is the user's choice (§11.4)
              const failed = l.run.status === 'failed';
              const cancelled = l.run.status === 'cancelled';
              return (
                <FactCard
                  key={l.workload}
                  icon={meta.icon}
                  title={meta.label}
                  className={failed ? 'failed' : ''}
                  state={<StatusOf run={l.run} />}
                  onClick={() => go(meta.page)}
                  label="open"
                >
                  <div className="fc-value">
                    {failed ? (
                      <span className="fc-failed">
                        {l.run.error_code ? l.run.error_code.replace(/-/g, ' ') : 'failed'}
                      </span>
                    ) : cancelled ? (
                      <span className="fc-cancelled">Cancelled</span>
                    ) : (
                      <Figure value={head?.value} unit={head?.unit} />
                    )}
                    <span className="fc-head-label">{head?.label}</span>
                  </div>
                  <FluidSparkline
                    values={l.trend}
                    label={`${head?.label ?? 'Result'} over the last ${l.trend.length} comparable runs`}
                  />
                  <span className="fc-sub caption">{l.run.label}</span>
                  <Facts
                    rows={[
                      ['Peer', l.run.peer_name ?? DASH],
                      ['Path', l.run.path_label, true],
                      ['When', fmtAgo(l.run.started_at), true],
                      ['Comparable runs', String(l.trendCount), true],
                    ]}
                  />
                </FactCard>
              );
            })}
          </div>
        ) : (
          <Empty
            title="No runs yet"
            actions={
              target ? (
                <Button
                  icon="play"
                  onClick={() => void store.runSuite('smoke', target)}
                  disabled={!!suiteBlock}
                  reason={suiteBlock}
                >
                  Run the smoke suite
                </Button>
              ) : null
            }
          >
            Run the smoke suite to measure every workload once against{' '}
            {target?.device_name ?? 'your target'}. It takes about a minute.
          </Empty>
        )}
      </Section>
    </Page>
  );
}
