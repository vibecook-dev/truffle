//! Versioned, reproducible benchmark reports. Units belong to field names.
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    io::{self, Write},
    path::PathBuf,
    process::Command,
};

#[derive(Serialize)]
struct BenchReport<P, R> {
    schema_version: u32,
    workload: String,
    timestamp_utc: String,
    git_sha: Option<String>,
    git_dirty: Option<bool>,
    environment: serde_json::Value,
    network: serde_json::Value,
    params: P,
    results: R,
}

/// Exact sample quantiles: do not round sub-millisecond or sub-MB/s samples.
#[derive(Debug, Serialize)]
pub struct Distribution {
    pub samples: usize,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
}
impl Distribution {
    pub fn new(samples: impl IntoIterator<Item = f64>) -> Option<Self> {
        let mut values: Vec<_> = samples.into_iter().collect();
        if values.is_empty() || values.iter().any(|v| !v.is_finite() || *v < 0.0) {
            return None;
        }
        values.sort_by(f64::total_cmp);
        let quantile =
            |q: f64| values[((values.len() as f64 * q).ceil() as usize).saturating_sub(1)];
        Some(Self {
            samples: values.len(),
            p50: quantile(0.5),
            p95: quantile(0.95),
            p99: quantile(0.99),
            p999: quantile(0.999),
            min: values[0],
            max: values[values.len() - 1],
            mean: values.iter().sum::<f64>() / values.len() as f64,
        })
    }
}

pub fn write<P: Serialize, R: Serialize>(
    out_dir: &str,
    workload: &str,
    network: serde_json::Value,
    params: P,
    results: R,
) -> io::Result<PathBuf> {
    std::fs::create_dir_all(out_dir)?;
    let now = chrono::Utc::now();
    let path = PathBuf::from(out_dir).join(format!(
        "{}-{workload}-{}.json",
        now.format("%Y%m%dT%H%M%SZ"),
        uuid::Uuid::new_v4()
    ));
    let sidecar = crate::harness::default_sidecar_path();
    let sidecar_hash = std::fs::read(&sidecar)
        .ok()
        .map(|data| hex::encode(Sha256::digest(data)));
    let report = BenchReport {
        schema_version: 2,
        workload: workload.into(),
        timestamp_utc: now.to_rfc3339(),
        git_sha: command("git", &["rev-parse", "HEAD"]),
        git_dirty: command("git", &["status", "--porcelain", "--untracked-files=no"])
            .map(|s| !s.is_empty()),
        environment: serde_json::json!({
            "os": std::env::consts::OS, "arch": std::env::consts::ARCH,
            "logical_cpus": std::thread::available_parallelism().map(|n| n.get()).ok(),
            "rustc": command("rustc", &["--version"]), "debug_assertions": cfg!(debug_assertions),
            "sidecar_sha256": sidecar_hash,
            "sidecar_build_info": command("go", &["version", "-m", &sidecar.to_string_lossy()]),
        }),
        network,
        params,
        results,
    };
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)?;
    serde_json::to_writer_pretty(&mut file, &report)?;
    file.write_all(b"\n")?;
    Ok(path)
}
fn command(program: &str, args: &[&str]) -> Option<String> {
    let out = Command::new(program).args(args).output().ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn distributions_preserve_fractional_units_and_reject_invalid_samples() {
        let d = Distribution::new([0.001, 0.002, 0.003, 0.004]).unwrap();
        assert_eq!(d.p50, 0.002);
        assert_eq!(d.p99, 0.004);
        assert!(Distribution::new([]).is_none());
        assert!(Distribution::new([f64::NAN]).is_none());
        assert!(Distribution::new([f64::INFINITY]).is_none());
        assert!(Distribution::new([-1.0]).is_none());
    }
    #[test]
    fn reports_do_not_overwrite_runs_and_record_schema() {
        let dir = tempfile::tempdir().unwrap();
        let a = write(
            dir.path().to_str().unwrap(),
            "test",
            serde_json::json!({"topology":"same-host"}),
            (),
            (),
        )
        .unwrap();
        let b = write(
            dir.path().to_str().unwrap(),
            "test",
            serde_json::Value::Null,
            (),
            (),
        )
        .unwrap();
        assert_ne!(a, b);
        let report: serde_json::Value = serde_json::from_slice(&std::fs::read(a).unwrap()).unwrap();
        assert_eq!(report["schema_version"], 2);
        assert_eq!(report["network"]["topology"], "same-host");
    }
}
