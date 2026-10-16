//! Suite profiles (RFC 027 §8.4), shared by the CLI and the app.
//!
//! A suite runs its items in order on one hub (or one `pair`), with
//! [`COOLDOWN_MS`] between items. Profiles are plain lists of
//! [`WorkloadParams`]; [`resolve`] applies `"<workload>.<field>"` overrides
//! and validates every item.

use serde_json::Value;

use crate::error::AppError;
use crate::params::{
    Direction, FileTransferParams, HttpConnection, HttpParams, HttpTarget, PathParams, QuicParams,
    StoreMode, StoreParams, StreamMode, TcpParams, UdpMode, UdpParams, VideoParams, VideoProfile,
    VideoTransport, Workload, WorkloadParams, WsMode, WsParams, MIB,
};
use crate::types::SuiteProfile;

/// Pause between suite items.
pub const COOLDOWN_MS: u64 = 1000;
/// Profile ids, in the order the app lists them.
pub const PROFILE_IDS: [&str; 4] = ["smoke", "standard", "full", "nightly"];
/// `nightly`'s budget for nominal workload time (§8.4: ≤ 12 min).
pub const NIGHTLY_MAX_SECS: f64 = 12.0 * 60.0;
/// Cap on every duration in `nightly`.
const NIGHTLY_DURATION_CAP: u32 = 10;
/// The `full` UDP payload sweep.
const UDP_SWEEP: [u32; 5] = [200, 1200, 1252, 1253, 1400];

/// The `full`/`nightly` UDP ramp: from 50 Mb/s for 30 s, ten ×1.5 steps up
/// to ~1.9 Gb/s, so it ends at a loss (or rate) limit rather than when the
/// run does. At the 10 Mb/s / 10 s defaults it could never report more
/// than 33.75 Mb/s.
fn suite_ramp() -> UdpParams {
    UdpParams {
        mode: UdpMode::Ramp,
        rate_mbps: 50.0,
        duration_secs: 30,
        ..UdpParams::default()
    }
}

/// Every profile.
pub fn profiles() -> Vec<SuiteProfile> {
    PROFILE_IDS
        .iter()
        .map(|id| profile(id).expect("every listed profile exists"))
        .collect()
}

/// One profile by id.
pub fn profile(id: &str) -> Result<SuiteProfile, AppError> {
    let (label, description, items) = match id {
        "smoke" => (
            "Smoke",
            "Every workload once, briefly: about a minute.",
            smoke(),
        ),
        "standard" => (
            "Standard",
            "Every workload at its defaults.",
            standard(),
        ),
        "full" => (
            "Full",
            "Standard plus directions, stream counts, payload and Nagle sweeps, every UDP mode and every video transport.",
            full(),
        ),
        "nightly" => (
            "Nightly",
            "Full without the UDP payload sweep or video over TCP, every duration capped at 10 s. Release builds only.",
            nightly(),
        ),
        other => {
            return Err(AppError::invalid_params(format!(
                "unknown suite profile “{other}” (expected one of: {})",
                PROFILE_IDS.join(", ")
            )))
        }
    };
    Ok(SuiteProfile {
        id: id.to_string(),
        label: label.to_string(),
        description: description.to_string(),
        items,
    })
}

/// A profile's items with `"<workload>.<field>"` overrides applied to every
/// item of that workload, all validated. An override naming a workload the
/// suite does not contain, or a field that workload does not have, is an
/// `invalid-params` error.
pub fn resolve(id: &str, overrides: &[(String, Value)]) -> Result<Vec<WorkloadParams>, AppError> {
    let mut items = profile(id)?.items;
    apply_overrides(&mut items, overrides)?;
    for item in &items {
        item.validate().map_err(|e| {
            AppError::invalid_params(format!("suite {id}, item “{}”: {e}", item.label()))
        })?;
    }
    Ok(items)
}

/// Apply `"<workload>.<field>"` overrides to every matching item.
pub fn apply_overrides(
    items: &mut [WorkloadParams],
    overrides: &[(String, Value)],
) -> Result<(), AppError> {
    for (key, value) in overrides {
        let (workload, field) = key.split_once('.').ok_or_else(|| {
            AppError::invalid_params(format!(
                "suite override “{key}” must look like <workload>.<field>"
            ))
        })?;
        let workload: Workload = workload.parse()?;
        let mut matched = false;
        for item in items.iter_mut().filter(|i| i.workload() == workload) {
            item.apply_overrides([(field, value)])?;
            matched = true;
        }
        if !matched {
            return Err(AppError::invalid_params(format!(
                "suite override “{key}”: the suite has no {workload} items"
            )));
        }
    }
    Ok(())
}

/// Sum of [`WorkloadParams::nominal_secs`] plus cooldowns.
pub fn total_nominal_secs(items: &[WorkloadParams]) -> f64 {
    let work: f64 = items.iter().map(WorkloadParams::nominal_secs).sum();
    let cooldowns = items.len().saturating_sub(1) as f64 * COOLDOWN_MS as f64 / 1000.0;
    work + cooldowns
}

fn smoke() -> Vec<WorkloadParams> {
    vec![
        WorkloadParams::Path(PathParams {
            count: 10,
            ..PathParams::default()
        }),
        WorkloadParams::Tcp(TcpParams {
            duration_secs: 3,
            ..TcpParams::default()
        }),
        WorkloadParams::Udp(UdpParams {
            duration_secs: 3,
            rate_mbps: 5.0,
            ..UdpParams::default()
        }),
        WorkloadParams::Quic(QuicParams {
            duration_secs: 3,
            ..QuicParams::default()
        }),
        WorkloadParams::Ws(WsParams {
            count: 100,
            ..WsParams::default()
        }),
        WorkloadParams::FileTransfer(FileTransferParams {
            size_bytes: MIB,
            iterations: 2,
        }),
        WorkloadParams::SyncedStore(StoreParams {
            count: 20,
            ..StoreParams::default()
        }),
        WorkloadParams::Http(HttpParams {
            duration_secs: 3,
            ..HttpParams::default()
        }),
        WorkloadParams::Video(VideoParams {
            profile: VideoProfile::P720p30,
            duration_secs: 3,
            ..VideoParams::default()
        }),
        WorkloadParams::Egress(crate::params::EgressParams {
            duration_secs: 3,
            ..Default::default()
        }),
    ]
}

fn standard() -> Vec<WorkloadParams> {
    WorkloadParams::defaults()
}

/// Everything `full` adds to `standard`, in groups (the UDP sweep and video
/// transports are tagged so `nightly` can drop them).
struct FullExtras {
    general: Vec<WorkloadParams>,
    udp_sweep: Vec<WorkloadParams>,
    video: Vec<(VideoTransport, WorkloadParams)>,
}

fn full_extras() -> FullExtras {
    let tcp = TcpParams::default();
    let mut general = vec![
        WorkloadParams::Tcp(TcpParams {
            direction: Direction::Download,
            ..tcp.clone()
        }),
        WorkloadParams::Tcp(TcpParams {
            direction: Direction::Bidirectional,
            ..tcp.clone()
        }),
        WorkloadParams::Tcp(TcpParams {
            streams: 4,
            ..tcp.clone()
        }),
    ];
    for payload_bytes in [64, 1024, 32_768] {
        for nodelay in [true, false] {
            general.push(WorkloadParams::Tcp(TcpParams {
                mode: StreamMode::Latency,
                payload_bytes,
                nodelay,
                ..tcp.clone()
            }));
        }
    }
    general.extend([
        WorkloadParams::Udp(UdpParams {
            mode: UdpMode::Echo,
            ..UdpParams::default()
        }),
        WorkloadParams::Udp(suite_ramp()),
        WorkloadParams::Quic(QuicParams {
            mode: StreamMode::Latency,
            ..QuicParams::default()
        }),
        WorkloadParams::Ws(WsParams {
            mode: WsMode::Stream,
            ..WsParams::default()
        }),
        WorkloadParams::SyncedStore(StoreParams {
            mode: StoreMode::Burst,
            ..StoreParams::default()
        }),
        WorkloadParams::Http(HttpParams {
            target: HttpTarget::Direct,
            ..HttpParams::default()
        }),
        WorkloadParams::Http(HttpParams {
            connection: HttpConnection::New,
            ..HttpParams::default()
        }),
        WorkloadParams::Http(HttpParams {
            concurrency: 1,
            ..HttpParams::default()
        }),
        WorkloadParams::Http(HttpParams {
            concurrency: 2,
            ..HttpParams::default()
        }),
    ]);
    let udp_sweep = UDP_SWEEP
        .iter()
        .map(|&payload_bytes| {
            WorkloadParams::Udp(UdpParams {
                payload_bytes,
                ..UdpParams::default()
            })
        })
        .collect();
    let video = [
        VideoTransport::Udp,
        VideoTransport::Quic,
        VideoTransport::Tcp,
    ]
    .into_iter()
    .map(|transport| {
        (
            transport,
            WorkloadParams::Video(VideoParams {
                transport,
                ..VideoParams::default()
            }),
        )
    })
    .collect();
    FullExtras {
        general,
        udp_sweep,
        video,
    }
}

/// Append items not already present (a suite is a set union, in order).
fn union(items: &mut Vec<WorkloadParams>, more: impl IntoIterator<Item = WorkloadParams>) {
    for item in more {
        if !items.contains(&item) {
            items.push(item);
        }
    }
}

fn full() -> Vec<WorkloadParams> {
    let extras = full_extras();
    let mut items = standard();
    union(&mut items, extras.general);
    union(&mut items, extras.udp_sweep);
    union(&mut items, extras.video.into_iter().map(|(_, p)| p));
    items
}

/// `nightly`'s duration cap. A UDP ramp is exempt: its duration decides
/// how high it can climb, so capping it would cap its result.
fn cap_duration(p: &mut WorkloadParams) {
    let cap = |d: &mut u32| *d = (*d).min(NIGHTLY_DURATION_CAP);
    match p {
        WorkloadParams::Tcp(x) => cap(&mut x.duration_secs),
        WorkloadParams::Udp(x) if x.mode == UdpMode::Ramp => {}
        WorkloadParams::Udp(x) => cap(&mut x.duration_secs),
        WorkloadParams::Quic(x) => cap(&mut x.duration_secs),
        WorkloadParams::Ws(x) => cap(&mut x.duration_secs),
        WorkloadParams::Http(x) => cap(&mut x.duration_secs),
        WorkloadParams::Video(x) => cap(&mut x.duration_secs),
        WorkloadParams::Egress(x) => cap(&mut x.duration_secs),
        WorkloadParams::Path(_)
        | WorkloadParams::FileTransfer(_)
        | WorkloadParams::SyncedStore(_) => {}
    }
}

fn nightly() -> Vec<WorkloadParams> {
    let extras = full_extras();
    let mut items = standard();
    union(&mut items, extras.general);
    union(
        &mut items,
        extras
            .video
            .into_iter()
            .filter(|(t, _)| *t != VideoTransport::Tcp)
            .map(|(_, p)| p),
    );
    for item in &mut items {
        cap_duration(item);
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn every_item_of_every_profile_validates() {
        for p in profiles() {
            assert!(!p.items.is_empty(), "{}", p.id);
            assert!(!p.label.is_empty() && !p.description.is_empty());
            for item in &p.items {
                item.validate()
                    .unwrap_or_else(|e| panic!("{}: {}: {e}", p.id, item.label()));
            }
            // No duplicates.
            for (i, a) in p.items.iter().enumerate() {
                assert!(
                    !p.items[i + 1..].contains(a),
                    "{}: duplicate {}",
                    p.id,
                    a.label()
                );
            }
        }
        assert_eq!(profiles().len(), 4);
        assert!(profile("bogus").is_err());
    }

    #[test]
    fn smoke_and_standard_cover_every_workload_once() {
        for id in ["smoke", "standard"] {
            let p = profile(id).unwrap();
            let workloads: Vec<Workload> = p.items.iter().map(WorkloadParams::workload).collect();
            assert_eq!(workloads, Workload::ALL.to_vec(), "{id}");
        }
        assert_eq!(
            profile("standard").unwrap().items,
            WorkloadParams::defaults()
        );
        let smoke = profile("smoke").unwrap().items;
        assert!(smoke.contains(&WorkloadParams::Udp(UdpParams {
            duration_secs: 3,
            rate_mbps: 5.0,
            ..UdpParams::default()
        })));
        assert!(
            smoke.contains(&WorkloadParams::FileTransfer(FileTransferParams {
                size_bytes: MIB,
                iterations: 2
            }))
        );
        assert!(
            total_nominal_secs(&smoke) < 90.0,
            "{}",
            total_nominal_secs(&smoke)
        );
    }

    #[test]
    fn full_extends_standard() {
        let full = profile("full").unwrap().items;
        for item in WorkloadParams::defaults() {
            assert!(full.contains(&item), "{}", item.label());
        }
        let latency = full
            .iter()
            .filter(|p| {
                matches!(
                    p,
                    WorkloadParams::Tcp(TcpParams {
                        mode: StreamMode::Latency,
                        ..
                    })
                )
            })
            .count();
        assert_eq!(latency, 6);
        let udp_stream = full
            .iter()
            .filter(|p| {
                matches!(
                    p,
                    WorkloadParams::Udp(UdpParams {
                        mode: UdpMode::Stream,
                        ..
                    })
                )
            })
            .count();
        assert_eq!(udp_stream, 5, "sweep incl. the standard 1200 B item");
        let video = full
            .iter()
            .filter(|p| p.workload() == Workload::Video)
            .count();
        assert_eq!(video, 3);
        // The ramp starts high and runs long enough to reach a limit.
        assert!(full.contains(&WorkloadParams::Udp(suite_ramp())));
        let reach =
            suite_ramp().rate_mbps * 1.5f64.powi((suite_ramp().duration_secs / 3) as i32 - 1);
        assert!(reach > 1000.0, "the ramp only reaches {reach} Mb/s");
        assert!(full.iter().any(|p| matches!(
            p,
            WorkloadParams::Http(HttpParams {
                concurrency: 2,
                connection: HttpConnection::Keepalive,
                target: HttpTarget::Proxy,
                ..
            })
        )));
    }

    #[test]
    fn nightly_fits_in_twelve_minutes() {
        let nightly = profile("nightly").unwrap().items;
        let total = total_nominal_secs(&nightly);
        assert!(total <= NIGHTLY_MAX_SECS, "nightly takes {total} s");
        // Every duration is capped except the ramp's, which sets its reach.
        assert!(nightly
            .iter()
            .filter(|p| !matches!(
                p,
                WorkloadParams::Udp(UdpParams {
                    mode: UdpMode::Ramp,
                    ..
                })
            ))
            .all(|p| p.duration_secs().unwrap_or(0) <= 10));
        assert!(nightly.contains(&WorkloadParams::Udp(suite_ramp())));
        assert!(!nightly.iter().any(|p| matches!(
            p,
            WorkloadParams::Video(VideoParams {
                transport: VideoTransport::Tcp,
                ..
            })
        )));
        let udp_stream = nightly
            .iter()
            .filter(|p| {
                matches!(
                    p,
                    WorkloadParams::Udp(UdpParams {
                        mode: UdpMode::Stream,
                        ..
                    })
                )
            })
            .count();
        assert_eq!(udp_stream, 1, "no payload sweep");
        let full = profile("full").unwrap().items;
        assert!(nightly.len() < full.len());
    }

    #[test]
    fn overrides() {
        let items = resolve("smoke", &[("tcp.streams".into(), json!(2))]).unwrap();
        assert!(items.contains(&WorkloadParams::Tcp(TcpParams {
            duration_secs: 3,
            streams: 2,
            ..TcpParams::default()
        })));
        let full = resolve("full", &[("http.duration_secs".into(), json!(5))]).unwrap();
        assert!(full
            .iter()
            .filter_map(|p| match p {
                WorkloadParams::Http(h) => Some(h.duration_secs),
                _ => None,
            })
            .all(|d| d == 5));
        // Out of bounds after merging: rejected, not clamped.
        assert!(resolve("smoke", &[("tcp.streams".into(), json!(99))]).is_err());
        assert!(resolve("smoke", &[("tcp.nope".into(), json!(1))]).is_err());
        assert!(resolve("smoke", &[("nope.count".into(), json!(1))]).is_err());
        assert!(resolve("smoke", &[("count".into(), json!(1))]).is_err());
        assert!(resolve("smoke", &[("workload.x".into(), json!(1))]).is_err());
    }
}
