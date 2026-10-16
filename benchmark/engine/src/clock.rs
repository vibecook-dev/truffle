//! The process timebase and NTP-style clock offsets (RFC 027 §6 Clock).
//!
//! Every engine timestamp used for intervals and clock offsets is
//! [`now_ns`]: the wall clock read once (at first use; call [`init`] at hub
//! start) plus the monotonic time elapsed since. NTP steps cannot move it,
//! and two processes' timebases differ by a constant offset plus drift,
//! which [`estimate`] measures.
//!
//! `now_ns` does not advance while the machine sleeps (Rust's `Instant`
//! excludes suspend on macOS and Linux), so in a long-lived process it falls
//! behind the wall clock by the time spent asleep. Calendar fields (report
//! `started_at`/`finished_at`, `ActiveRun.started_at`, snapshot `at`, report
//! file names) therefore come from [`wall_ns`] / [`now_rfc3339`], never from
//! `now_ns`.
//!
//! The network exchange itself lives in the control protocol: [`estimate`]
//! takes a closure that performs one `clock` request and returns its four
//! timestamps.

use std::future::Future;
use std::sync::OnceLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::error::AppError;
use crate::types::{ClockInfo, ClockOffset};

/// Samples per estimate (§6).
pub const DEFAULT_SAMPLES: usize = 16;
/// Spacing between samples (§6).
pub const DEFAULT_SPACING: Duration = Duration::from_millis(10);
/// Per-sample timeout the control layer should apply (§6).
pub const SAMPLE_TIMEOUT: Duration = Duration::from_secs(5);

struct Timebase {
    wall_ns: u64,
    mono: Instant,
}

static TIMEBASE: OnceLock<Timebase> = OnceLock::new();

fn timebase() -> &'static Timebase {
    TIMEBASE.get_or_init(|| Timebase {
        wall_ns: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0),
        mono: Instant::now(),
    })
}

/// Fix the timebase now (idempotent). Hubs call this at start so the wall
/// clock is read before any measurement.
pub fn init() {
    let _ = timebase();
}

/// Unix-epoch nanoseconds on the process timebase: wall clock at [`init`] +
/// monotonic elapsed. Never goes backwards; for intervals and clock-offset
/// math only (it lags the wall clock by any time spent suspended).
pub fn now_ns() -> u64 {
    let tb = timebase();
    tb.wall_ns + tb.mono.elapsed().as_nanos() as u64
}

/// The [`now_ns`] value of a monotonic instant.
pub fn instant_ns(at: Instant) -> u64 {
    let tb = timebase();
    match at.checked_duration_since(tb.mono) {
        Some(d) => tb.wall_ns + d.as_nanos() as u64,
        None => tb
            .wall_ns
            .saturating_sub(tb.mono.duration_since(at).as_nanos() as u64),
    }
}

/// RFC 3339 (UTC, millisecond precision) of a [`now_ns`] timestamp.
pub fn rfc3339(ns: u64) -> String {
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = (ns % 1_000_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nanos)
        .unwrap_or_default()
        .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

/// Unix-epoch nanoseconds of the wall clock right now (`SystemTime`): for
/// calendar fields. It can step (NTP, sleep), so never subtract two of these.
pub fn wall_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// RFC 3339 of the wall clock now ([`wall_ns`]).
pub fn now_rfc3339() -> String {
    rfc3339(wall_ns())
}

/// One `clock` exchange: t1 local send, t2 peer receive, t3 peer reply,
/// t4 local receive (all on each side's [`now_ns`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClockSample {
    /// Local, just before sending.
    pub t1_ns: u64,
    /// Peer, on receipt.
    pub t2_ns: u64,
    /// Peer, just before replying.
    pub t3_ns: u64,
    /// Local, on receipt of the reply.
    pub t4_ns: u64,
}

impl ClockSample {
    /// δ = (t4 − t1) − (t3 − t2).
    pub fn delay_ns(&self) -> i64 {
        let rtt = i128::from(self.t4_ns) - i128::from(self.t1_ns);
        let peer = i128::from(self.t3_ns) - i128::from(self.t2_ns);
        (rtt - peer) as i64
    }

    /// θ = ((t2 − t1) + (t3 − t4)) / 2: peer − local.
    pub fn offset_ns(&self) -> i64 {
        let a = i128::from(self.t2_ns) - i128::from(self.t1_ns);
        let b = i128::from(self.t3_ns) - i128::from(self.t4_ns);
        ((a + b) / 2) as i64
    }

    /// Local times ordered and peer times ordered.
    pub fn is_consistent(&self) -> bool {
        self.t4_ns >= self.t1_ns && self.t3_ns >= self.t2_ns
    }
}

/// Keep the sample with the smallest δ (§6): `ClockOffset { offset_ns: θ,
/// delay_ns: δ, uncertainty_ns: ⌈δ/2⌉, at_ns: t4, samples }`. Inconsistent
/// samples are ignored; `None` when none is left.
pub fn best_offset(samples: &[ClockSample]) -> Option<ClockOffset> {
    let usable: Vec<&ClockSample> = samples.iter().filter(|s| s.is_consistent()).collect();
    let best = usable.iter().min_by_key(|s| s.delay_ns())?;
    let delay = best.delay_ns();
    Some(ClockOffset {
        offset_ns: best.offset_ns(),
        delay_ns: delay,
        uncertainty_ns: (delay.max(0) as u64).div_ceil(2),
        at_ns: best.t4_ns,
        samples: usable.len() as u32,
    })
}

/// Run `samples` exchanges `spacing` apart and return [`best_offset`].
///
/// `exchange` performs one `clock` request (with its own per-sample timeout,
/// [`SAMPLE_TIMEOUT`]) and stamps t1/t4 with [`now_ns`] itself, as close to
/// the send and the receipt as it can. Failed samples are skipped; if all
/// fail, the last error is returned.
pub async fn estimate<F, Fut>(
    samples: usize,
    spacing: Duration,
    mut exchange: F,
) -> Result<ClockOffset, AppError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<ClockSample, AppError>>,
{
    let mut got = Vec::with_capacity(samples);
    let mut last_err = None;
    for i in 0..samples {
        if i > 0 && !spacing.is_zero() {
            tokio::time::sleep(spacing).await;
        }
        match exchange().await {
            Ok(s) => got.push(s),
            Err(e) => last_err = Some(e),
        }
    }
    best_offset(&got).ok_or_else(|| {
        last_err.unwrap_or_else(|| AppError::internal("clock estimate: no consistent samples"))
    })
}

/// Maps between a peer's timebase and ours using a before (and optional
/// after) estimate, interpolating the offset linearly in local time.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ClockMap {
    /// Estimate before the data phase.
    pub before: ClockOffset,
    /// Estimate after it.
    pub after: Option<ClockOffset>,
}

impl ClockMap {
    /// A map from one or two estimates.
    pub fn new(before: ClockOffset, after: Option<ClockOffset>) -> Self {
        Self { before, after }
    }

    /// Drift between the estimates, ppm; `None` without an after estimate
    /// or when both were taken at the same instant.
    pub fn drift_ppm(&self) -> Option<f64> {
        let after = self.after?;
        let dt = after.at_ns as f64 - self.before.at_ns as f64;
        if dt <= 0.0 {
            return None;
        }
        Some((after.offset_ns as f64 - self.before.offset_ns as f64) / dt * 1e6)
    }

    /// Offset (peer − local) at local time `t_local_ns`, interpolated (and
    /// extrapolated) linearly between the estimates.
    pub fn offset_at(&self, t_local_ns: u64) -> f64 {
        match self.after {
            Some(after) if after.at_ns > self.before.at_ns => {
                let span = (after.at_ns - self.before.at_ns) as f64;
                let frac = (t_local_ns as f64 - self.before.at_ns as f64) / span;
                self.before.offset_ns as f64
                    + frac * (after.offset_ns as f64 - self.before.offset_ns as f64)
            }
            _ => self.before.offset_ns as f64,
        }
    }

    /// Peer time → local time: `t_peer − offset(t)`, evaluating the offset
    /// at the approximate local time.
    pub fn to_local(&self, t_peer_ns: u64) -> u64 {
        let approx = t_peer_ns as f64 - self.before.offset_ns as f64;
        let local = t_peer_ns as f64 - self.offset_at(approx.max(0.0) as u64);
        local.max(0.0).round() as u64
    }

    /// Local time → peer time.
    pub fn to_peer(&self, t_local_ns: u64) -> u64 {
        (t_local_ns as f64 + self.offset_at(t_local_ns))
            .max(0.0)
            .round() as u64
    }

    /// The larger uncertainty of the two estimates, ns.
    pub fn uncertainty_ns(&self) -> u64 {
        self.after.map_or(self.before.uncertainty_ns, |a| {
            a.uncertainty_ns.max(self.before.uncertainty_ns)
        })
    }

    /// The report's `network.clock`.
    pub fn info(&self) -> ClockInfo {
        ClockInfo {
            before: self.before,
            after: self.after,
            drift_ppm: self.drift_ppm(),
            uncertainty_ns: self.uncertainty_ns(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(
        t1: u64,
        one_way_out: u64,
        peer_proc: u64,
        one_way_back: u64,
        offset: i64,
    ) -> ClockSample {
        let t2 = (t1 + one_way_out) as i64 + offset;
        let t3 = t2 + peer_proc as i64;
        let t4 = t1 + one_way_out + peer_proc + one_way_back;
        ClockSample {
            t1_ns: t1,
            t2_ns: t2 as u64,
            t3_ns: t3 as u64,
            t4_ns: t4,
        }
    }

    #[test]
    fn ntp_math_symmetric_path_recovers_the_offset() {
        let s = sample(1_000_000, 500, 100, 500, 7_000);
        assert_eq!(s.delay_ns(), 1000);
        assert_eq!(s.offset_ns(), 7_000);
        let s = sample(1_000_000, 500, 100, 500, -42_000);
        assert_eq!(s.offset_ns(), -42_000);
    }

    #[test]
    fn best_sample_has_the_smallest_delay() {
        let samples = [
            sample(1_000, 5_000, 100, 1_000, 10_000), // asymmetric, δ = 6000
            sample(20_000, 300, 50, 300, 10_000),     // δ = 600, exact
            sample(40_000, 2_000, 50, 2_000, 10_000), // δ = 4000
        ];
        let best = best_offset(&samples).unwrap();
        assert_eq!(best.delay_ns, 600);
        assert_eq!(best.offset_ns, 10_000);
        assert_eq!(best.uncertainty_ns, 300);
        assert_eq!(best.samples, 3);
        assert_eq!(best.at_ns, samples[1].t4_ns);
        // ⌈δ/2⌉ rounds up.
        let odd = best_offset(&[sample(0, 300, 0, 301, 0)]).unwrap();
        assert_eq!(odd.uncertainty_ns, 301);
        assert_eq!(best_offset(&[]), None);
        let broken = ClockSample {
            t1_ns: 10,
            t2_ns: 5,
            t3_ns: 6,
            t4_ns: 9,
        };
        assert_eq!(best_offset(&[broken]), None);
    }

    #[tokio::test(start_paused = true)]
    async fn estimate_skips_failures_and_keeps_the_best() {
        let mut i = 0u64;
        let got = estimate(5, DEFAULT_SPACING, || {
            i += 1;
            let n = i;
            async move {
                if n.is_multiple_of(2) {
                    Err(AppError::timeout("clock sample"))
                } else {
                    Ok(sample(n * 1_000_000, 1_000 * n, 10, 1_000 * n, 5_000))
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(got.samples, 3);
        assert_eq!(got.delay_ns, 2_000);
        assert_eq!(got.offset_ns, 5_000);
        let err = estimate(3, Duration::ZERO, || async { Err(AppError::timeout("x")) })
            .await
            .unwrap_err();
        assert_eq!(err.code, crate::ErrorCode::Timeout);
    }

    #[test]
    fn interpolation_and_drift() {
        let off = |offset_ns: i64, at_ns: u64| ClockOffset {
            offset_ns,
            delay_ns: 1000,
            uncertainty_ns: 500,
            at_ns,
            samples: 16,
        };
        // 30 µs of drift over 10 s = 3 ppm.
        let map = ClockMap::new(
            off(1_000_000, 10_000_000_000),
            Some(off(1_030_000, 20_000_000_000)),
        );
        assert!((map.drift_ppm().unwrap() - 3.0).abs() < 1e-9);
        assert!((map.offset_at(15_000_000_000) - 1_015_000.0).abs() < 1e-6);
        assert!((map.offset_at(10_000_000_000) - 1_000_000.0).abs() < 1e-6);
        // Round trip local → peer → local.
        let t = 12_345_678_901u64;
        let back = map.to_local(map.to_peer(t));
        assert!(back.abs_diff(t) <= 1, "{back} vs {t}");
        let info = map.info();
        assert_eq!(info.uncertainty_ns, 500);
        assert!(info.drift_ppm.is_some());
        let single = ClockMap::new(off(-5_000, 1), None);
        assert_eq!(single.drift_ppm(), None);
        assert_eq!(single.offset_at(999), -5_000.0);
        assert_eq!(single.to_local(10_000), 15_000);
    }

    #[test]
    fn timebase_is_monotonic_and_formats() {
        init();
        let a = now_ns();
        let b = now_ns();
        assert!(b >= a);
        assert!(a > 1_600_000_000_000_000_000, "wall-clock based");
        let s = rfc3339(1_700_000_000_123_000_000);
        assert_eq!(s, "2023-11-14T22:13:20.123Z");
        let now = Instant::now();
        assert!(instant_ns(now).abs_diff(now_ns()) < 50_000_000);
        // Calendar stamps are the wall clock itself.
        let wall = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        assert!(wall_ns().abs_diff(wall) < 1_000_000_000);
        let stamp = chrono::DateTime::parse_from_rfc3339(&now_rfc3339()).unwrap();
        let skew = (stamp.timestamp_millis() - chrono::Utc::now().timestamp_millis()).abs();
        assert!(skew < 1_000, "{skew} ms");
    }
}
