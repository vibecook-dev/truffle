//! CPU and memory of this process and its sidecar(s) during a run
//! (RFC 027 §7.4 `ResourceStats`).
//!
//! A [`ResourceSampler`] refreshes only the given PIDs every 500 ms through
//! sysinfo and records cumulative CPU time (user + sys) and RSS. The first
//! sample primes the counters. [`ResourceSamples::stats`] then trims to the
//! measurement window: CPU time is the difference of the cumulative counter
//! (interpolated at the window edges), `cpu_pct_max` is the busiest 500 ms
//! interval after the priming one, and per-MB figures divide by goodput.
//!
//! In `same-process` topology (`pair`), sample this process plus both hubs'
//! sidecars: the sidecar fields then hold their sum, and the result is the
//! report's `resources.combined`.

use std::time::Duration;

use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::clock::now_ns;
use crate::types::ResourceStats;

/// Sampling interval.
pub const SAMPLE_INTERVAL: Duration = Duration::from_millis(500);

/// Which processes to sample.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceTargets {
    /// The measuring process.
    pub process: u32,
    /// Sidecar PIDs (`Node::sidecar_pid()`); summed. Empty when unknown.
    pub sidecars: Vec<u32>,
}

impl ResourceTargets {
    /// This process plus whatever sidecar PIDs are known (`None`s are skipped).
    pub fn this_process(sidecars: impl IntoIterator<Item = Option<u32>>) -> Self {
        Self {
            process: std::process::id(),
            sidecars: sidecars.into_iter().flatten().collect(),
        }
    }
}

/// One sample: cumulative CPU time and RSS.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResourcePoint {
    /// `now_ns()` of the sample.
    pub t_ns: u64,
    /// Process cumulative CPU time, ms.
    pub cpu_ms: Option<f64>,
    /// Process RSS, bytes.
    pub rss_bytes: Option<u64>,
    /// Sum of the sidecars' cumulative CPU time, ms (`None` if any is gone).
    pub sidecar_cpu_ms: Option<f64>,
    /// Sum of the sidecars' RSS, bytes.
    pub sidecar_rss_bytes: Option<u64>,
}

fn sample(sys: &mut System, targets: &ResourceTargets, pids: &[Pid]) -> ResourcePoint {
    sys.refresh_processes_specifics(
        ProcessesToUpdate::Some(pids),
        true,
        ProcessRefreshKind::nothing()
            .with_cpu()
            .with_memory()
            .without_tasks(),
    );
    let t_ns = now_ns();
    let read = |pid: u32| {
        sys.process(Pid::from_u32(pid))
            .map(|p| (p.accumulated_cpu_time() as f64, p.memory()))
    };
    let own = read(targets.process);
    let (sidecar_cpu_ms, sidecar_rss_bytes) = if targets.sidecars.is_empty() {
        (None, None)
    } else {
        let all: Option<Vec<(f64, u64)>> = targets.sidecars.iter().map(|&p| read(p)).collect();
        match all {
            Some(v) => (
                Some(v.iter().map(|x| x.0).sum()),
                Some(v.iter().map(|x| x.1).sum()),
            ),
            None => (None, None),
        }
    };
    ResourcePoint {
        t_ns,
        cpu_ms: own.map(|x| x.0),
        rss_bytes: own.map(|x| x.1),
        sidecar_cpu_ms,
        sidecar_rss_bytes,
    }
}

/// A running sampler task. Dropping it stops sampling (the samples are lost).
pub struct ResourceSampler {
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Vec<ResourcePoint>>,
}

impl ResourceSampler {
    /// Start sampling now (the first sample primes the counters). Must be
    /// called inside a Tokio runtime.
    pub fn start(targets: ResourceTargets) -> Self {
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let pids: Vec<Pid> = std::iter::once(targets.process)
                .chain(targets.sidecars.iter().copied())
                .map(Pid::from_u32)
                .collect();
            let mut sys = System::new();
            let mut points = Vec::new();
            let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = tick.tick() => points.push(sample(&mut sys, &targets, &pids)),
                    _ = &mut stop_rx => break,
                }
            }
            points.push(sample(&mut sys, &targets, &pids));
            points
        });
        Self {
            stop: Some(stop_tx),
            task,
        }
    }

    /// Take a final sample and return everything collected.
    pub async fn stop(mut self) -> ResourceSamples {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let points = (&mut self.task).await.unwrap_or_default();
        ResourceSamples { points }
    }
}

impl Drop for ResourceSampler {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Samples collected by a [`ResourceSampler`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResourceSamples {
    /// In time order; the first is the priming sample.
    pub points: Vec<ResourcePoint>,
}

/// Linear interpolation of a cumulative counter at `t`.
fn cumulative_at(series: &[(u64, f64)], t: u64) -> f64 {
    match series.iter().position(|&(ts, _)| ts >= t) {
        None => series.last().map_or(0.0, |p| p.1),
        Some(0) => series[0].1,
        Some(i) => {
            let (t0, v0) = series[i - 1];
            let (t1, v1) = series[i];
            if t1 == t0 {
                v1
            } else {
                v0 + (v1 - v0) * (t - t0) as f64 / (t1 - t0) as f64
            }
        }
    }
}

/// CPU figures for one cumulative series over `[start, end]`:
/// `(cpu_ms, pct_avg, pct_max)`.
fn cpu_over(series: &[(u64, f64)], start: u64, end: u64) -> Option<(f64, f64, f64)> {
    if series.len() < 2 || end <= start {
        return None;
    }
    let cpu_ms = (cumulative_at(series, end) - cumulative_at(series, start)).max(0.0);
    let window_ms = (end - start) as f64 / 1e6;
    let avg = cpu_ms / window_ms * 100.0;
    // Busiest interval overlapping the window, skipping the priming interval.
    let max = series
        .windows(2)
        .skip(1)
        .filter(|w| w[1].0 > start && w[0].0 < end && w[1].0 > w[0].0)
        .map(|w| (w[1].1 - w[0].1).max(0.0) / ((w[1].0 - w[0].0) as f64 / 1e6) * 100.0)
        .fold(None, |acc: Option<f64>, v| {
            Some(acc.map_or(v, |a| a.max(v)))
        })
        .unwrap_or(avg);
    Some((cpu_ms, avg, max))
}

fn rss_max(points: &[(u64, u64)], start: u64, end: u64) -> Option<f64> {
    let inside = points
        .iter()
        .filter(|(t, _)| (start..=end).contains(t))
        .map(|p| p.1)
        .max();
    inside
        .or_else(|| points.iter().map(|p| p.1).max())
        .map(|b| b as f64 / 1e6)
}

fn per_mb(cpu_ms: f64, goodput_bytes: Option<u64>) -> Option<f64> {
    goodput_bytes
        .filter(|b| *b > 0)
        .map(|b| cpu_ms / (b as f64 / 1e6))
}

impl ResourceSamples {
    /// Window covered by the samples.
    pub fn span(&self) -> Option<(u64, u64)> {
        Some((self.points.first()?.t_ns, self.points.last()?.t_ns))
    }

    /// Stats over `window` (`now_ns()` bounds; the whole span when `None`,
    /// clamped to it otherwise). `goodput_bytes` feeds the per-MB figures.
    /// `None` with fewer than two process samples.
    pub fn stats(
        &self,
        window: Option<(u64, u64)>,
        goodput_bytes: Option<u64>,
    ) -> Option<ResourceStats> {
        let (first, last) = self.span()?;
        let (start, end) = match window {
            Some((s, e)) => (s.clamp(first, last), e.clamp(first, last)),
            None => (first, last),
        };
        let cpu: Vec<(u64, f64)> = self
            .points
            .iter()
            .filter_map(|p| p.cpu_ms.map(|c| (p.t_ns, c)))
            .collect();
        let rss: Vec<(u64, u64)> = self
            .points
            .iter()
            .filter_map(|p| p.rss_bytes.map(|r| (p.t_ns, r)))
            .collect();
        let (cpu_ms, cpu_pct_avg, cpu_pct_max) = cpu_over(&cpu, start, end)?;

        let sidecar_complete = self.points.iter().all(|p| p.sidecar_cpu_ms.is_some());
        let sidecar_cpu: Vec<(u64, f64)> = if sidecar_complete {
            self.points
                .iter()
                .filter_map(|p| p.sidecar_cpu_ms.map(|c| (p.t_ns, c)))
                .collect()
        } else {
            Vec::new()
        };
        let sidecar_rss: Vec<(u64, u64)> = self
            .points
            .iter()
            .filter_map(|p| p.sidecar_rss_bytes.map(|r| (p.t_ns, r)))
            .collect();
        let sidecar = cpu_over(&sidecar_cpu, start, end);

        Some(ResourceStats {
            cpu_ms,
            cpu_pct_avg,
            cpu_pct_max,
            rss_mb_max: rss_max(&rss, start, end).unwrap_or(0.0),
            cpu_ms_per_mb: per_mb(cpu_ms, goodput_bytes),
            sidecar_cpu_ms: sidecar.map(|s| s.0),
            sidecar_cpu_pct_avg: sidecar.map(|s| s.1),
            sidecar_cpu_pct_max: sidecar.map(|s| s.2),
            sidecar_rss_mb_max: sidecar.and_then(|_| rss_max(&sidecar_rss, start, end)),
            sidecar_cpu_ms_per_mb: sidecar.and_then(|s| per_mb(s.0, goodput_bytes)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    fn pt(t_ms: u64, cpu: f64, rss: u64, sidecar: Option<(f64, u64)>) -> ResourcePoint {
        ResourcePoint {
            t_ns: t_ms * MS,
            cpu_ms: Some(cpu),
            rss_bytes: Some(rss),
            sidecar_cpu_ms: sidecar.map(|s| s.0),
            sidecar_rss_bytes: sidecar.map(|s| s.1),
        }
    }

    #[test]
    fn cpu_from_cumulative_time_with_priming_discarded() {
        // Priming interval 0–500 ms burns 400 ms (startup), then 100 ms per 500 ms,
        // with one busy interval of 600 ms (two CPUs).
        let s = ResourceSamples {
            points: vec![
                pt(0, 1000.0, 10_000_000, Some((50.0, 30_000_000))),
                pt(500, 1400.0, 11_000_000, Some((100.0, 31_000_000))),
                pt(1000, 1500.0, 12_000_000, Some((150.0, 32_000_000))),
                pt(1500, 2100.0, 20_000_000, Some((200.0, 33_000_000))),
                pt(2000, 2200.0, 12_000_000, Some((250.0, 32_000_000))),
            ],
        };
        let st = s.stats(None, Some(100_000_000)).unwrap();
        assert!((st.cpu_ms - 1200.0).abs() < 1e-9);
        assert!((st.cpu_pct_avg - 60.0).abs() < 1e-9);
        assert!(
            (st.cpu_pct_max - 120.0).abs() < 1e-9,
            "busy interval, can exceed 100"
        );
        assert!((st.rss_mb_max - 20.0).abs() < 1e-9);
        assert!((st.cpu_ms_per_mb.unwrap() - 12.0).abs() < 1e-9);
        assert!((st.sidecar_cpu_ms.unwrap() - 200.0).abs() < 1e-9);
        assert!((st.sidecar_cpu_pct_avg.unwrap() - 10.0).abs() < 1e-9);
        assert!((st.sidecar_cpu_pct_max.unwrap() - 10.0).abs() < 1e-9);
        assert!((st.sidecar_rss_mb_max.unwrap() - 33.0).abs() < 1e-9);
        assert!((st.sidecar_cpu_ms_per_mb.unwrap() - 2.0).abs() < 1e-9);

        // Window trimming interpolates the counter at the edges.
        let w = s.stats(Some((750 * MS, 1250 * MS)), None).unwrap();
        // 1450 → 1800 over 500 ms
        assert!((w.cpu_ms - 350.0).abs() < 1e-9, "{}", w.cpu_ms);
        assert!((w.cpu_pct_avg - 70.0).abs() < 1e-9);
        assert_eq!(w.cpu_ms_per_mb, None);
        assert!((w.rss_mb_max - 12.0).abs() < 1e-9);
        // A window beyond the samples is clamped.
        let c = s.stats(Some((0, 99_000 * MS)), None).unwrap();
        assert!((c.cpu_ms - 1200.0).abs() < 1e-9);
    }

    #[test]
    fn missing_sidecar_means_null_sidecar_fields() {
        let s = ResourceSamples {
            points: vec![
                pt(0, 0.0, 1, None),
                pt(500, 10.0, 1, None),
                pt(1000, 20.0, 1, None),
            ],
        };
        let st = s.stats(None, Some(1)).unwrap();
        assert_eq!(st.sidecar_cpu_ms, None);
        assert_eq!(st.sidecar_rss_mb_max, None);
        assert_eq!(st.sidecar_cpu_ms_per_mb, None);
        // Only the priming interval: the max falls back to the average.
        let s = ResourceSamples {
            points: vec![pt(0, 0.0, 1, None), pt(500, 100.0, 1, None)],
        };
        let st = s.stats(None, None).unwrap();
        assert!((st.cpu_pct_max - st.cpu_pct_avg).abs() < 1e-9);
        assert!(ResourceSamples::default().stats(None, None).is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn samples_this_process() {
        let sampler = ResourceSampler::start(ResourceTargets::this_process([None]));
        // Burn some CPU on a blocking thread for ~1.2 s.
        tokio::task::spawn_blocking(|| {
            let until = std::time::Instant::now() + Duration::from_millis(1200);
            let mut x = 0u64;
            while std::time::Instant::now() < until {
                x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
            }
            std::hint::black_box(x);
        })
        .await
        .unwrap();
        let samples = sampler.stop().await;
        assert!(samples.points.len() >= 3, "{}", samples.points.len());
        let st = samples.stats(None, None).unwrap();
        assert!(st.cpu_ms > 100.0, "{st:?}");
        assert!(st.rss_mb_max > 0.0);
        assert_eq!(st.sidecar_cpu_ms, None);
    }
}
