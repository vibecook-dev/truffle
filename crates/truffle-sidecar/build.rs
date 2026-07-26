use std::env;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    println!("cargo:rerun-if-env-changed=TRUFFLE_SIDECAR_PATH");
    println!("cargo:rerun-if-env-changed=TRUFFLE_SIDECAR_SKIP_DOWNLOAD");

    // If user provides their own sidecar, skip download
    if let Ok(path) = env::var("TRUFFLE_SIDECAR_PATH") {
        println!("cargo:rustc-env=TRUFFLE_SIDECAR_PATH={path}");
        return;
    }

    // Allow skipping download (e.g., for CI where sidecar is provided separately)
    if env::var("TRUFFLE_SIDECAR_SKIP_DOWNLOAD").is_ok() {
        println!("cargo:rustc-env=TRUFFLE_SIDECAR_PATH=truffle-sidecar");
        return;
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let target = env::var("TARGET").unwrap();
    let version = env!("CARGO_PKG_VERSION");

    let (asset_name, bin_name) = match target.as_str() {
        "aarch64-apple-darwin" => ("tsnet-sidecar-darwin-arm64", "sidecar-slim"),
        "x86_64-apple-darwin" => ("tsnet-sidecar-darwin-amd64", "sidecar-slim"),
        "x86_64-unknown-linux-gnu" => ("tsnet-sidecar-linux-amd64", "sidecar-slim"),
        "aarch64-unknown-linux-gnu" => ("tsnet-sidecar-linux-arm64", "sidecar-slim"),
        "x86_64-pc-windows-msvc" => ("tsnet-sidecar-windows-amd64.exe", "sidecar-slim.exe"),
        other => {
            panic!(
                "truffle-sidecar: no verified prebuilt sidecar for target {other}. Set \
                 TRUFFLE_SIDECAR_PATH to a trusted binary, or set \
                 TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1 to opt into resolving truffle-sidecar from PATH"
            );
        }
    };

    // Look up the pinned checksum for this version/asset from the committed
    // trust anchor. A malformed file or missing entry is fatal: build scripts
    // must never download and execute a binary whose digest was not shipped in
    // the crate.
    println!("cargo:rerun-if-changed=sidecar-checksums.json");
    let expected =
        match lookup_checksum(include_str!("sidecar-checksums.json"), version, asset_name) {
            Ok(Some(value)) => value,
            Ok(None) => panic!(
                "truffle-sidecar: SECURITY: no pinned checksum for {version}/{asset_name}. \
                 Refusing to download an unverified executable"
            ),
            Err(e) => panic!("truffle-sidecar: invalid sidecar-checksums.json: {e}"),
        };

    let sidecar_path = out_dir.join(bin_name);

    // Skip download if already present (cached build) — but only trust the
    // cached OUT_DIR binary if it still matches the pinned checksum. A cached
    // file that no longer verifies (tampered, or a stale hash) is re-downloaded
    // rather than used.
    if sidecar_path.exists() {
        let cached_ok = match fs::read(&sidecar_path) {
            Ok(bytes) => match verify_sha256(&bytes, &expected) {
                Ok(()) => true,
                Err(e) => {
                    eprintln!(
                        "truffle-sidecar: cached sidecar failed integrity check ({e}); re-downloading"
                    );
                    false
                }
            },
            Err(e) => {
                eprintln!("truffle-sidecar: could not read cached sidecar ({e}); re-downloading");
                false
            }
        };
        if cached_ok {
            println!(
                "cargo:rustc-env=TRUFFLE_SIDECAR_PATH={}",
                sidecar_path.display()
            );
            return;
        }
    }

    let url = format!(
        "https://github.com/vibecook-dev/truffle/releases/download/truffle-v{version}/{asset_name}"
    );

    eprintln!("truffle-sidecar: downloading {url}");

    match download(&url, &sidecar_path, &expected) {
        Ok(()) => {
            #[cfg(unix)]
            set_executable(&sidecar_path);

            println!(
                "cargo:rustc-env=TRUFFLE_SIDECAR_PATH={}",
                sidecar_path.display()
            );
            eprintln!("truffle-sidecar: downloaded to {}", sidecar_path.display());
        }
        Err(e) => {
            panic!(
                "truffle-sidecar: failed to download the verified sidecar: {e}. Set \
                 TRUFFLE_SIDECAR_PATH to a trusted binary, or set \
                 TRUFFLE_SIDECAR_SKIP_DOWNLOAD=1 to explicitly use PATH"
            );
        }
    }
}

fn download(
    url: &str,
    dest: &Path,
    expected_sha256: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let response = ureq::get(url).call()?;

    let status = response.status();
    if status != 200 {
        return Err(format!("HTTP {status} from {url}").into());
    }

    let mut reader = response.into_body().into_reader();
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut reader, &mut bytes)?;

    // Integrity gate. Verified BEFORE writing so bad bytes never touch disk.
    if let Err(e) = verify_sha256(&bytes, expected_sha256) {
        return Err(format!(
            "integrity check FAILED for {url}: {e}. Refusing to install the binary"
        )
        .into());
    }
    eprintln!("truffle-sidecar: sha256 verified for {url}");

    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(dest, &bytes)?;

    Ok(())
}

#[cfg(unix)]
fn set_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(metadata) = fs::metadata(path) {
        let mut perms = metadata.permissions();
        perms.set_mode(0o755);
        let _ = fs::set_permissions(path, perms);
    }
}

// Shared pure integrity helpers: `sha256_hex`, `verify_sha256`,
// `lookup_checksum`. Pulled in textually so the exact code exercised by the
// build script is also unit-tested via `#[cfg(test)] mod integrity;` in lib.rs.
// The file's `#[cfg(test)] mod tests` is stripped here (build scripts never see
// cfg(test)).
include!("src/integrity.rs");
