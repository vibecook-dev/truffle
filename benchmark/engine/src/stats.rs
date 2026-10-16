//! Statistics (RFC 027 §7.4, §8.2): distributions, RFC 3550 jitter,
//! datagram sequence accounting and small rate helpers.
//!
//! Units: Mb/s is 10⁶ bits/s; times are nanoseconds unless a name says
//! otherwise.

use crate::types::Distribution;

/// p50 as an exact fraction for [`percentile`].
pub const P50: (u64, u64) = (1, 2);
/// p90.
pub const P90: (u64, u64) = (9, 10);
/// p95.
pub const P95: (u64, u64) = (19, 20);
/// p99.
pub const P99: (u64, u64) = (99, 100);
/// p99.9.
pub const P999: (u64, u64) = (999, 1000);

/// Minimum n for percentile `num/den` to be reported: `⌈1 / (1 − p)⌉`
/// (2 for p50, 10 for p90, 20 for p95, 100 for p99, 1000 for p99.9).
pub fn min_samples(num: u64, den: u64) -> u64 {
    assert!(num < den && den > 0, "percentile must be below 1");
    den.div_ceil(den - num)
}

/// Nearest-rank percentile `num/den` of an ascending slice: `sorted[⌈p·n⌉ − 1]`.
///
/// `None` when n is below [`min_samples`] or when the rank lands on a
/// non-finite value (a lost sample ranked as +∞).
pub fn percentile(sorted: &[f64], num: u64, den: u64) -> Option<f64> {
    let n = sorted.len() as u64;
    if n == 0 || n < min_samples(num, den) {
        return None;
    }
    let rank = (num * n).div_ceil(den).max(1);
    let v = sorted[(rank - 1) as usize];
    v.is_finite().then_some(v)
}

/// Arithmetic mean; `None` when empty.
pub fn mean(values: &[f64]) -> Option<f64> {
    (!values.is_empty()).then(|| values.iter().sum::<f64>() / values.len() as f64)
}

/// Sample standard deviation (n − 1); `None` when n < 2.
pub fn sample_stddev(values: &[f64]) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let m = mean(values)?;
    let var = values.iter().map(|v| (v - m) * (v - m)).sum::<f64>() / (values.len() - 1) as f64;
    Some(var.sqrt())
}

/// Conventional median (mean of the two middle values for even n); `None`
/// when empty. For summaries such as "median over iterations"; distributions
/// use the nearest-rank [`percentile`] instead.
pub fn median(values: &[f64]) -> Option<f64> {
    let mut v: Vec<f64> = values.iter().copied().filter(|x| x.is_finite()).collect();
    if v.is_empty() {
        return None;
    }
    v.sort_by(f64::total_cmp);
    let mid = v.len() / 2;
    Some(if v.len().is_multiple_of(2) {
        (v[mid - 1] + v[mid]) / 2.0
    } else {
        v[mid]
    })
}

/// `bytes · 8 / window` in Mb/s; `None` for an empty window.
pub fn mbps(bytes: f64, window_ns: u64) -> Option<f64> {
    (window_ns > 0).then(|| bytes * 8.0 / window_ns as f64 * 1e3)
}

/// Events per second over a window; `None` for an empty window.
pub fn per_sec(count: f64, window_ns: u64) -> Option<f64> {
    (window_ns > 0).then(|| count / (window_ns as f64 / 1e9))
}

/// `part / whole · 100`; `None` when `whole` is 0.
pub fn pct(part: f64, whole: f64) -> Option<f64> {
    (whole != 0.0).then(|| part / whole * 100.0)
}

/// Nanoseconds to milliseconds.
pub fn ns_to_ms(ns: f64) -> f64 {
    ns / 1e6
}

impl Distribution {
    /// Summarise samples where `None` (or a non-finite value) is a lost or
    /// timed-out sample (§7.4): lost samples rank as +∞ for percentiles and
    /// are counted in `lost`; `min`, `mean`, `max` and `stddev` cover the
    /// received samples.
    pub fn from_samples(samples: &[Option<f64>]) -> Self {
        let mut ranked: Vec<f64> = samples
            .iter()
            .map(|s| match s {
                Some(v) if v.is_finite() => *v,
                _ => f64::INFINITY,
            })
            .collect();
        ranked.sort_by(f64::total_cmp);
        let received: Vec<f64> = ranked.iter().copied().filter(|v| v.is_finite()).collect();
        let p = |(num, den): (u64, u64)| percentile(&ranked, num, den);
        Self {
            samples: ranked.len() as u32,
            lost: (ranked.len() - received.len()) as u32,
            min: received.first().copied(),
            mean: mean(&received),
            p50: p(P50),
            p90: p(P90),
            p95: p(P95),
            p99: p(P99),
            p999: p(P999),
            max: received.last().copied(),
            stddev: sample_stddev(&received),
        }
    }

    /// Summarise samples that were all received.
    pub fn from_values(values: &[f64]) -> Self {
        let samples: Vec<Option<f64>> = values.iter().copied().map(Some).collect();
        Self::from_samples(&samples)
    }
}

/// RFC 3550 interarrival jitter: `J += (|D| − J) / 16` with
/// `D = (Rj − Ri) − (Sj − Si)` over datagrams in arrival order.
///
/// Uses raw send and receive clocks, so a constant offset between the two
/// machines cancels out.
#[derive(Debug, Clone, Default)]
pub struct Jitter {
    last_transit: Option<i128>,
    j_ns: f64,
    count: u64,
}

impl Jitter {
    /// A fresh estimator.
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed one datagram: its sender timestamp and its arrival time.
    pub fn push(&mut self, send_ns: u64, recv_ns: u64) {
        let transit = i128::from(recv_ns) - i128::from(send_ns);
        if let Some(prev) = self.last_transit {
            let d = (transit - prev).unsigned_abs() as f64;
            self.j_ns += (d - self.j_ns) / 16.0;
        }
        self.last_transit = Some(transit);
        self.count += 1;
    }

    /// Datagrams fed so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Current J in ms; `None` before two datagrams.
    pub fn jitter_ms(&self) -> Option<f64> {
        (self.count >= 2).then(|| ns_to_ms(self.j_ns))
    }
}

/// What [`SeqTracker::record`] made of a sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeqOutcome {
    /// First arrival, in order.
    New,
    /// First arrival, below the highest seq seen before it (RFC 4737 reordered).
    Reordered,
    /// Seen before.
    Duplicate,
    /// At or above the tracker's limit; ignored.
    OutOfRange,
}

/// Loss figures over the sent range `0..sent` (§8.2 udp).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SeqLoss {
    /// Datagrams the sender reported sent.
    pub sent: u64,
    /// Distinct seqs received within `0..sent`.
    pub unique: u64,
    /// `sent − unique`.
    pub missing: u64,
    /// `missing / sent · 100`; `None` when nothing was sent.
    pub loss_pct: Option<f64>,
    /// Longest run of never-received seqs.
    pub max_loss_burst: u64,
}

/// Receiver-side datagram accounting: unique, duplicate and reordered
/// arrivals, plus loss, loss bursts and per-bucket loss against the sender's
/// counts. Seqs are tracked in a bitset up to `limit`.
#[derive(Debug, Clone)]
pub struct SeqTracker {
    bits: Vec<u64>,
    limit: u64,
    max_seen: Option<u64>,
    unique: u64,
    dup: u64,
    reordered: u64,
    out_of_range: u64,
}

impl SeqTracker {
    /// Track seqs below `limit` (a generous bound from the params; anything
    /// at or above it is counted as out of range and ignored).
    pub fn new(limit: u64) -> Self {
        Self {
            bits: Vec::new(),
            limit,
            max_seen: None,
            unique: 0,
            dup: 0,
            reordered: 0,
            out_of_range: 0,
        }
    }

    /// Record one arrival.
    pub fn record(&mut self, seq: u64) -> SeqOutcome {
        if seq >= self.limit {
            self.out_of_range += 1;
            return SeqOutcome::OutOfRange;
        }
        let word = (seq / 64) as usize;
        let bit = 1u64 << (seq % 64);
        if word >= self.bits.len() {
            self.bits.resize(word + 1, 0);
        }
        if self.bits[word] & bit != 0 {
            self.dup += 1;
            return SeqOutcome::Duplicate;
        }
        self.bits[word] |= bit;
        self.unique += 1;
        let outcome = match self.max_seen {
            Some(max) if seq < max => {
                self.reordered += 1;
                SeqOutcome::Reordered
            }
            _ => SeqOutcome::New,
        };
        self.max_seen = Some(self.max_seen.map_or(seq, |m| m.max(seq)));
        outcome
    }

    /// Whether `seq` arrived.
    pub fn contains(&self, seq: u64) -> bool {
        let word = (seq / 64) as usize;
        word < self.bits.len() && self.bits[word] & (1u64 << (seq % 64)) != 0
    }

    /// Distinct seqs received.
    pub fn unique(&self) -> u64 {
        self.unique
    }

    /// Repeat arrivals.
    pub fn dup(&self) -> u64 {
        self.dup
    }

    /// Unique arrivals below an earlier maximum.
    pub fn reordered(&self) -> u64 {
        self.reordered
    }

    /// Arrivals at or above the limit.
    pub fn out_of_range(&self) -> u64 {
        self.out_of_range
    }

    /// Highest seq seen.
    pub fn max_seen(&self) -> Option<u64> {
        self.max_seen
    }

    /// One past the highest seq the bitset can hold: nothing at or above
    /// it ever arrived.
    fn recorded_end(&self) -> u64 {
        (self.bits.len() as u64).saturating_mul(64)
    }

    /// Seqs in `start..end` that arrived: a popcount per word, so the cost
    /// is O(range / 64) and never more than the bitset, whatever the range.
    fn present_in(&self, start: u64, end: u64) -> u64 {
        let end = end.min(self.recorded_end());
        if start >= end {
            return 0;
        }
        let (first, last) = (start / 64, (end - 1) / 64);
        (first..=last)
            .map(|w| {
                let mut word = self.bits[w as usize];
                if w == first {
                    word &= !0u64 << (start % 64);
                }
                let top = end - w * 64;
                if w == last && top < 64 {
                    word &= (1u64 << top) - 1;
                }
                u64::from(word.count_ones())
            })
            .sum()
    }

    /// Seqs in `start..end` that never arrived. The cost depends on the
    /// tracker, not on the range (which may come from a peer).
    pub fn missing_in(&self, start: u64, end: u64) -> u64 {
        end.saturating_sub(start) - self.present_in(start, end)
    }

    /// Loss against `sent` datagrams (seqs `0..sent`). Only the recorded
    /// part is scanned (word by word); seqs beyond it are missing by
    /// construction, so a peer-supplied `sent` cannot make this slow.
    pub fn loss(&self, sent: u64) -> SeqLoss {
        let scan = sent.min(self.recorded_end());
        let mut unique = 0u64;
        let mut burst = 0u64;
        let mut max_burst = 0u64;
        let mut seq = 0u64;
        while seq < scan {
            let n = (scan - seq).min(64);
            let mut word = self.bits[(seq / 64) as usize];
            if n < 64 {
                word &= (1u64 << n) - 1;
            }
            if word == 0 {
                burst += n;
                max_burst = max_burst.max(burst);
            } else if n == 64 && word == u64::MAX {
                unique += 64;
                burst = 0;
            } else {
                for b in 0..n {
                    if word >> b & 1 == 1 {
                        unique += 1;
                        burst = 0;
                    } else {
                        burst += 1;
                        max_burst = max_burst.max(burst);
                    }
                }
            }
            seq += n;
        }
        if sent > scan {
            burst = burst.saturating_add(sent - scan);
            max_burst = max_burst.max(burst);
        }
        let missing = sent - unique;
        SeqLoss {
            sent,
            unique,
            missing,
            loss_pct: pct(missing as f64, sent as f64),
            max_loss_burst: max_burst,
        }
    }

    /// Per-bucket loss from the sender's `bucket_first_seq` (the first seq of
    /// each 250 ms send bucket): bucket b covers `[first_b, first_{b+1})`, the
    /// last one ends at `sent`. Returns `(missing, sent_in_bucket)` per
    /// bucket. Exact and clock-free.
    pub fn bucket_loss(&self, bucket_first_seq: &[u64], sent: u64) -> Vec<(u64, u64)> {
        bucket_first_seq
            .iter()
            .enumerate()
            .map(|(i, &start)| {
                let end = bucket_first_seq
                    .get(i + 1)
                    .copied()
                    .unwrap_or(sent)
                    .min(sent);
                let start = start.min(end);
                (self.missing_in(start, end), end - start)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn validity_thresholds() {
        assert_eq!(min_samples(1, 2), 2);
        assert_eq!(min_samples(9, 10), 10);
        assert_eq!(min_samples(19, 20), 20);
        assert_eq!(min_samples(99, 100), 100);
        assert_eq!(min_samples(999, 1000), 1000);

        let d = Distribution::from_values(&[5.0]);
        assert_eq!(d.samples, 1);
        assert_eq!(d.p50, None, "p50 needs 2 samples");
        assert_eq!((d.min, d.max, d.mean), (Some(5.0), Some(5.0), Some(5.0)));
        assert_eq!(d.stddev, None);

        let v: Vec<f64> = (1..=9).map(f64::from).collect();
        let d = Distribution::from_values(&v);
        assert!(d.p50.is_some() && d.p90.is_none());
        let v: Vec<f64> = (1..=10).map(f64::from).collect();
        let d = Distribution::from_values(&v);
        assert_eq!(d.p90, Some(9.0));
        assert_eq!(d.p95, None);
        let v: Vec<f64> = (1..=99).map(f64::from).collect();
        assert_eq!(Distribution::from_values(&v).p99, None);
        let v: Vec<f64> = (1..=100).map(f64::from).collect();
        let d = Distribution::from_values(&v);
        assert_eq!(d.p99, Some(99.0));
        assert_eq!(d.p95, Some(95.0));
        assert_eq!(d.p50, Some(50.0));
        assert_eq!(d.p999, None);
        let v: Vec<f64> = (1..=1000).map(f64::from).collect();
        assert_eq!(Distribution::from_values(&v).p999, Some(999.0));
    }

    #[test]
    fn nearest_rank() {
        // sorted[⌈p·n⌉ − 1]
        let v = [10.0, 20.0, 30.0, 40.0];
        assert_eq!(percentile(&v, 1, 2), Some(20.0));
        let v = [10.0, 20.0, 30.0, 40.0, 50.0];
        assert_eq!(percentile(&v, 1, 2), Some(30.0));
        // Unsorted input is sorted by from_samples.
        let d = Distribution::from_values(&[3.0, 1.0, 2.0]);
        assert_eq!(d.p50, Some(2.0));
        assert_eq!(d.min, Some(1.0));
        assert_eq!(d.max, Some(3.0));
    }

    #[test]
    fn stddev_is_sample_stddev() {
        let d = Distribution::from_values(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        // population sd = 2; sample sd = sqrt(32/7)
        assert!(close(d.stddev.unwrap(), (32.0f64 / 7.0).sqrt()));
        assert!(close(d.mean.unwrap(), 5.0));
        assert_eq!(sample_stddev(&[1.0]), None);
    }

    #[test]
    fn lost_samples_rank_as_infinity() {
        // 10 samples, 1 lost: p90 = sorted[8] is finite, max is the max received.
        let mut s: Vec<Option<f64>> = (1..=9).map(|i| Some(f64::from(i))).collect();
        s.push(None);
        let d = Distribution::from_samples(&s);
        assert_eq!(d.samples, 10);
        assert_eq!(d.lost, 1);
        assert_eq!(d.p90, Some(9.0));
        assert_eq!(d.max, Some(9.0));
        assert!(close(d.mean.unwrap(), 5.0));
        // 2 lost of 10: p90 lands on +∞ → null.
        let mut s: Vec<Option<f64>> = (1..=8).map(|i| Some(f64::from(i))).collect();
        s.extend([None, Some(f64::NAN)]);
        let d = Distribution::from_samples(&s);
        assert_eq!(d.lost, 2);
        assert_eq!(d.p90, None);
        assert_eq!(d.p50, Some(5.0));
        // Everything lost.
        let d = Distribution::from_samples(&[None, None]);
        assert_eq!((d.samples, d.lost), (2, 2));
        assert_eq!((d.min, d.mean, d.p50, d.max), (None, None, None, None));
        // Nothing at all.
        assert_eq!(Distribution::from_samples(&[]), Distribution::default());
    }

    #[test]
    fn jitter_is_offset_invariant() {
        // Sender sends every 20 ms; arrivals wobble by a fixed pattern.
        let wobble = [0i64, 3, 1, 7, 2, 0, 5, 1, 4, 2];
        let run = |offset: i64| {
            let mut j = Jitter::new();
            for (i, w) in wobble.iter().cycle().take(200).enumerate() {
                let send = 1_000_000_000u64 + i as u64 * 20_000_000;
                let recv = (send as i64 + 5_000_000 + w * 1_000_000 + offset) as u64;
                j.push(send, recv);
            }
            j.jitter_ms().unwrap()
        };
        let base = run(0);
        assert!(base > 0.0);
        for offset in [1_000_000_000i64, -900_000_000, 123_456_789] {
            assert!(close(run(offset), base), "offset {offset} changed jitter");
        }
        // Constant transit → zero jitter.
        let mut j = Jitter::new();
        for i in 0..50u64 {
            j.push(i * 1000, i * 1000 + 777);
        }
        assert_eq!(j.jitter_ms(), Some(0.0));
        assert_eq!(Jitter::new().jitter_ms(), None);
    }

    #[test]
    fn jitter_follows_the_rfc_3550_recurrence() {
        let mut j = Jitter::new();
        j.push(0, 10_000_000); // transit 10 ms
        j.push(20_000_000, 34_000_000); // transit 14 ms, |D| = 4 ms
        assert!(close(j.jitter_ms().unwrap(), 4.0 / 16.0));
        j.push(40_000_000, 50_000_000); // transit 10 ms, |D| = 4 ms
        let expected = 0.25 + (4.0 - 0.25) / 16.0;
        assert!(close(j.jitter_ms().unwrap(), expected));
    }

    #[test]
    fn seq_accounting() {
        let mut t = SeqTracker::new(1000);
        for seq in [0, 1, 3, 2, 2, 5, 4, 9, 2000] {
            t.record(seq);
        }
        assert_eq!(t.unique(), 7);
        assert_eq!(t.dup(), 1);
        assert_eq!(t.reordered(), 2, "2 after 3, 4 after 5");
        assert_eq!(t.out_of_range(), 1);
        assert_eq!(t.max_seen(), Some(9));
        let loss = t.loss(10);
        assert_eq!(loss.unique, 7);
        assert_eq!(loss.missing, 3);
        assert_eq!(loss.unique + loss.missing, loss.sent);
        assert!(close(loss.loss_pct.unwrap(), 30.0));
        assert_eq!(loss.max_loss_burst, 3, "6, 7, 8");
        assert_eq!(t.loss(0).loss_pct, None);
    }

    #[test]
    fn seq_loss_matches_a_seq_by_seq_count() {
        // Patterns across word boundaries, partial words and the limit.
        let mut t = SeqTracker::new(1000);
        let mut x = 0x9e37_79b9_7f4a_7c15u64;
        for seq in 0..1000u64 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let keep = if (128..256).contains(&seq) {
                true
            } else if (320..448).contains(&seq) {
                false
            } else {
                !x.is_multiple_of(10)
            };
            if keep {
                t.record(seq);
            }
        }
        for sent in [0, 1, 63, 64, 65, 127, 200, 333, 640, 999, 1000, 1001, 5000] {
            let unique = (0..sent).filter(|s| t.contains(*s)).count() as u64;
            let mut burst = 0;
            let mut max_burst = 0;
            for s in 0..sent {
                if t.contains(s) {
                    burst = 0;
                } else {
                    burst += 1;
                    max_burst = max_burst.max(burst);
                }
            }
            let l = t.loss(sent);
            assert_eq!(
                (l.unique, l.missing),
                (unique, sent - unique),
                "sent {sent}"
            );
            assert_eq!(l.max_loss_burst, max_burst, "sent {sent}");
            for start in [0, 1, 63, 64, 100, 319, 320, 700] {
                let naive = (start..sent).filter(|s| !t.contains(*s)).count() as u64;
                assert_eq!(t.missing_in(start, sent), naive, "{start}..{sent}");
            }
        }
    }

    #[test]
    fn peer_supplied_counts_cost_nothing_extra() {
        // finish.info carries `sent` and the bucket bounds from the peer.
        let mut t = SeqTracker::new(1000);
        for seq in 0..900 {
            t.record(seq);
        }
        let t0 = std::time::Instant::now();
        let l = t.loss(u64::MAX);
        assert_eq!(l.unique, 900);
        assert_eq!(l.missing, u64::MAX - 900);
        assert_eq!(l.max_loss_burst, u64::MAX - 900);
        assert_eq!(t.missing_in(0, u64::MAX), u64::MAX - 900);
        assert_eq!(t.missing_in(u64::MAX - 5, u64::MAX), 5);
        assert_eq!(t.missing_in(10, 5), 0);
        let buckets = t.bucket_loss(&[0, u64::MAX / 2, 3, u64::MAX], u64::MAX);
        assert_eq!(buckets.len(), 4);
        assert!(t0.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn bucket_loss_is_exact() {
        let mut t = SeqTracker::new(100);
        for seq in (0..30).filter(|s| *s != 5 && !(12..15).contains(s)) {
            t.record(seq);
        }
        let buckets = t.bucket_loss(&[0, 10, 20], 30);
        assert_eq!(buckets, vec![(1, 10), (3, 10), (0, 10)]);
        let total: u64 = buckets.iter().map(|b| b.0).sum();
        assert_eq!(total, t.loss(30).missing);
    }

    #[test]
    fn rate_helpers() {
        assert!(close(mbps(1_000_000.0, 1_000_000_000).unwrap(), 8.0));
        assert_eq!(mbps(1.0, 0), None);
        assert!(close(per_sec(500.0, 2_000_000_000).unwrap(), 250.0));
        assert_eq!(pct(1.0, 0.0), None);
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 1.0, 2.0, 3.0]), Some(2.5));
        assert_eq!(median(&[]), None);
    }
}
