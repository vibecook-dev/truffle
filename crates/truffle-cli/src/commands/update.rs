//! `truffle update` -- self-update by downloading the latest GitHub release.
//!
//! Core update logic (version checking, downloading, extraction, binary
//! replacement) is exposed as public helpers so that the background
//! auto-updater can reuse them.

use crate::exit_codes;
use crate::json_output;
use crate::output;
use futures_util::StreamExt;
use std::io::Write;
use std::path::{Path, PathBuf};

// ==========================================================================
// GitHub release types
// ==========================================================================

#[derive(serde::Deserialize)]
pub struct GitHubRelease {
    pub tag_name: String,
    pub assets: Vec<GitHubAsset>,
}

#[derive(serde::Deserialize)]
pub struct GitHubAsset {
    pub name: String,
    pub browser_download_url: String,
    pub size: u64,
}

// ==========================================================================
// Constants
// ==========================================================================

pub const GITHUB_RELEASES_URL: &str =
    "https://api.github.com/repos/vibecook-dev/truffle/releases/latest";

// ==========================================================================
// Platform detection
// ==========================================================================

pub fn platform_asset() -> &'static str {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        "truffle-darwin-arm64.tar.gz"
    }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        "truffle-darwin-x64.tar.gz"
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        "truffle-linux-x64.tar.gz"
    }
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        "truffle-linux-arm64.tar.gz"
    }
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        "truffle-win32-x64.zip"
    }
}

// ==========================================================================
// Version helpers
// ==========================================================================

/// Parse a version tag like "truffle-v0.3.0" or "v0.3.0" into (major, minor, patch).
fn parse_version(tag: &str) -> Option<(u32, u32, u32)> {
    // Strip prefixes like "truffle-v" or "v"
    let v = tag
        .strip_prefix("truffle-v")
        .or_else(|| tag.strip_prefix("v"))
        .unwrap_or(tag);

    let parts: Vec<&str> = v.split('.').collect();
    if parts.len() != 3 {
        return None;
    }
    Some((
        parts[0].parse().ok()?,
        parts[1].parse().ok()?,
        parts[2].parse().ok()?,
    ))
}

/// Returns true if `latest` is strictly newer than `current`.
pub fn is_newer(current: &str, latest: &str) -> bool {
    match (parse_version(current), parse_version(latest)) {
        (Some(cur), Some(lat)) => lat > cur,
        _ => false,
    }
}

/// Strip version prefixes to get the bare version string (e.g. "0.3.1").
pub fn strip_version_prefix(tag: &str) -> &str {
    tag.strip_prefix("truffle-v")
        .or_else(|| tag.strip_prefix("v"))
        .unwrap_or(tag)
}

// ==========================================================================
// HTTP client
// ==========================================================================

/// Build a reusable reqwest client with proper user-agent.
pub fn build_http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .user_agent("truffle-cli")
        .build()
        .map_err(|e| format!("Failed to create HTTP client: {e}"))
}

// ==========================================================================
// GitHub API
// ==========================================================================

/// Fetch the latest release metadata from GitHub.
pub async fn check_latest_version(client: &reqwest::Client) -> Result<GitHubRelease, String> {
    client
        .get(GITHUB_RELEASES_URL)
        .send()
        .await
        .map_err(|e| format!("Failed to fetch latest release: {e}"))?
        .error_for_status()
        .map_err(|e| format!("GitHub API error: {e}"))?
        .json()
        .await
        .map_err(|e| format!("Failed to parse release info: {e}"))
}

// ==========================================================================
// Download
// ==========================================================================

/// Download a release asset to `dest_path`. Shows a progress bar.
pub async fn download_asset_with_progress(
    client: &reqwest::Client,
    asset: &GitHubAsset,
    dest_path: &Path,
) -> Result<(), String> {
    let response = client
        .get(&asset.browser_download_url)
        .send()
        .await
        .map_err(|e| format!("Failed to download release: {e}"))?
        .error_for_status()
        .map_err(|e| format!("Download failed: {e}"))?;

    let total_size = response.content_length().unwrap_or(asset.size);
    let mut stream = response.bytes_stream();

    let mut file =
        std::fs::File::create(dest_path).map_err(|e| format!("Failed to create temp file: {e}"))?;

    let mut downloaded: u64 = 0;
    let start = std::time::Instant::now();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| format!("Download error: {e}"))?;
        file.write_all(&chunk)
            .map_err(|e| format!("Write error: {e}"))?;
        downloaded += chunk.len() as u64;

        let elapsed = start.elapsed().as_secs_f64();
        let speed = if elapsed > 0.0 {
            downloaded as f64 / elapsed
        } else {
            0.0
        };
        output::print_progress(downloaded, total_size, speed);
    }

    let elapsed = start.elapsed().as_secs_f64();
    output::print_progress_complete(downloaded, elapsed);
    drop(file);

    Ok(())
}

/// Download a release asset silently (no progress bar). For background use.
pub async fn download_asset_silent(
    client: &reqwest::Client,
    asset: &GitHubAsset,
    dest_path: &Path,
) -> Result<(), String> {
    let response = client
        .get(&asset.browser_download_url)
        .send()
        .await
        .map_err(|e| format!("Failed to download release: {e}"))?
        .error_for_status()
        .map_err(|e| format!("Download failed: {e}"))?;

    let bytes = response
        .bytes()
        .await
        .map_err(|e| format!("Failed to read response body: {e}"))?;

    std::fs::write(dest_path, &bytes).map_err(|e| format!("Failed to write archive: {e}"))?;

    Ok(())
}

// ==========================================================================
// Extraction
// ==========================================================================

fn extract_tar_gz(archive_path: &Path, dest: &Path) -> Result<(), String> {
    let file =
        std::fs::File::open(archive_path).map_err(|e| format!("Failed to open archive: {e}"))?;
    let gz = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(gz);
    archive
        .unpack(dest)
        .map_err(|e| format!("Failed to extract tar.gz: {e}"))?;
    Ok(())
}

#[cfg(target_os = "windows")]
fn extract_zip(archive_path: &Path, dest: &Path) -> Result<(), String> {
    let file =
        std::fs::File::open(archive_path).map_err(|e| format!("Failed to open archive: {e}"))?;
    let mut archive =
        zip::ZipArchive::new(file).map_err(|e| format!("Failed to read zip archive: {e}"))?;
    archive
        .extract(dest)
        .map_err(|e| format!("Failed to extract zip: {e}"))?;
    Ok(())
}

pub fn extract_archive(archive_path: &Path, dest: &Path) -> Result<(), String> {
    let name = archive_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
        extract_tar_gz(archive_path, dest)
    } else if name.ends_with(".zip") {
        #[cfg(target_os = "windows")]
        {
            extract_zip(archive_path, dest)
        }
        #[cfg(not(target_os = "windows"))]
        {
            Err("Zip extraction is only supported on Windows".to_string())
        }
    } else {
        Err(format!("Unsupported archive format: {name}"))
    }
}

// ==========================================================================
// Binary replacement
// ==========================================================================

pub fn find_install_dir() -> Result<PathBuf, String> {
    let exe =
        std::env::current_exe().map_err(|e| format!("Cannot find current executable: {e}"))?;
    exe.parent()
        .map(|p| p.to_path_buf())
        .ok_or_else(|| "Cannot determine install directory".to_string())
}

/// Find a file by name recursively in a directory (one level deep).
pub fn find_file_in_dir(dir: &Path, name: &str) -> Option<PathBuf> {
    // Check top level
    let top = dir.join(name);
    if top.exists() {
        return Some(top);
    }
    // Check one level of subdirectories
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                let nested = entry.path().join(name);
                if nested.exists() {
                    return Some(nested);
                }
            }
        }
    }
    None
}

pub fn replace_binary(
    new_binary: &Path,
    install_dir: &Path,
    binary_name: &str,
) -> Result<(), String> {
    let target = install_dir.join(binary_name);
    let backup = install_dir.join(format!("{binary_name}.bak"));

    // Back up old binary if it exists
    if target.exists() {
        std::fs::rename(&target, &backup)
            .map_err(|e| format!("Failed to back up {binary_name}: {e}"))?;
    }

    // Move new binary into place
    std::fs::copy(new_binary, &target)
        .map_err(|e| format!("Failed to install {binary_name}: {e}"))?;

    // Set executable permission on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o755))
            .map_err(|e| format!("Failed to set permissions on {binary_name}: {e}"))?;
    }

    // Remove backup
    if backup.exists() {
        let _ = std::fs::remove_file(&backup);
    }

    Ok(())
}

/// Find and replace truffle + sidecar binaries from an extracted archive directory.
pub fn replace_binaries(extract_dir: &Path, install_dir: &Path) -> Result<(), String> {
    #[cfg(not(target_os = "windows"))]
    let (truffle_bin, sidecar_bin) = ("truffle", "sidecar-slim");
    #[cfg(target_os = "windows")]
    let (truffle_bin, sidecar_bin) = ("truffle.exe", "sidecar-slim.exe");

    if let Some(new_truffle) = find_file_in_dir(extract_dir, truffle_bin) {
        replace_binary(&new_truffle, install_dir, truffle_bin)?;
    }
    if let Some(new_sidecar) = find_file_in_dir(extract_dir, sidecar_bin) {
        replace_binary(&new_sidecar, install_dir, sidecar_bin)?;
    }
    Ok(())
}

// ==========================================================================
// Main command
// ==========================================================================

pub async fn run(json: bool) -> Result<(), (i32, String)> {
    let current_version = env!("CARGO_PKG_VERSION");

    if !json {
        println!();
        println!("  {}", output::bold("truffle update"));
        println!("  {}", output::dim(&"\u{2500}".repeat(39)));
        println!();

        println!(
            "  Current version: {}",
            output::cyan(&format!("v{current_version}"))
        );
        println!("  Checking for updates...");
    }

    // 1. Query GitHub releases API for latest
    let client = build_http_client().map_err(|e| (exit_codes::ERROR, e))?;
    let release = check_latest_version(&client)
        .await
        .map_err(|e| (exit_codes::ERROR, e))?;

    let latest_tag = &release.tag_name;
    let latest_version = strip_version_prefix(latest_tag);

    // 2. Compare versions
    if !is_newer(current_version, latest_tag) {
        if json {
            let mut map = json_output::envelope("");
            map.insert("updated".to_string(), serde_json::json!(false));
            map.insert("from".to_string(), serde_json::json!(current_version));
            map.insert("to".to_string(), serde_json::json!(current_version));
            json_output::print_json(&serde_json::Value::Object(map));
        } else {
            println!();
            output::print_success(&format!("Already up to date (v{current_version})"));
            println!();
        }
        return Ok(());
    }

    if !json {
        println!(
            "  Latest version:  {}",
            output::green(&format!("v{latest_version}"))
        );
        println!();
    }

    // 3. Find the platform-specific asset
    let asset_name = platform_asset();
    let asset = release
        .assets
        .iter()
        .find(|a| a.name == asset_name)
        .ok_or_else(|| {
            (
                exit_codes::NOT_FOUND,
                format!(
                    "No release asset found for this platform ({asset_name}). \
                     Available assets: {}",
                    release
                        .assets
                        .iter()
                        .map(|a| a.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )
        })?;

    if !json {
        println!(
            "  Downloading {} ({})",
            output::bold(asset_name),
            output::format_bytes(asset.size)
        );
    }

    // 4. Download the asset with progress
    let tmp_dir = tempfile::tempdir().map_err(|e| {
        (
            exit_codes::ERROR,
            format!("Failed to create temp directory: {e}"),
        )
    })?;
    let archive_path = tmp_dir.path().join(asset_name);

    if json {
        download_asset_silent(&client, asset, &archive_path)
            .await
            .map_err(|e| (exit_codes::ERROR, e))?;
    } else {
        download_asset_with_progress(&client, asset, &archive_path)
            .await
            .map_err(|e| (exit_codes::ERROR, e))?;
    }

    // 5. Extract to a temp directory
    if !json {
        println!("  Extracting...");
    }
    let extract_dir = tmp_dir.path().join("extracted");
    std::fs::create_dir_all(&extract_dir).map_err(|e| {
        (
            exit_codes::ERROR,
            format!("Failed to create extraction dir: {e}"),
        )
    })?;

    extract_archive(&archive_path, &extract_dir).map_err(|e| (exit_codes::ERROR, e))?;

    // 6. Replace current binaries
    let install_dir = find_install_dir().map_err(|e| (exit_codes::ERROR, e))?;
    if !json {
        println!(
            "  Installing to {}",
            output::dim(&install_dir.display().to_string())
        );
    }

    // Determine binary names
    #[cfg(not(target_os = "windows"))]
    let (truffle_bin, sidecar_bin) = ("truffle", "sidecar-slim");
    #[cfg(target_os = "windows")]
    let (truffle_bin, sidecar_bin) = ("truffle.exe", "sidecar-slim.exe");

    // Find and replace truffle binary
    if let Some(new_truffle) = find_file_in_dir(&extract_dir, truffle_bin) {
        replace_binary(&new_truffle, &install_dir, truffle_bin)
            .map_err(|e| (exit_codes::ERROR, e))?;
        if !json {
            output::print_check(output::Indicator::Pass, "Updated truffle binary", "");
        }
    } else if !json {
        output::print_check(
            output::Indicator::Warn,
            "truffle binary",
            "not found in archive",
        );
    }

    // Find and replace sidecar binary
    if let Some(new_sidecar) = find_file_in_dir(&extract_dir, sidecar_bin) {
        replace_binary(&new_sidecar, &install_dir, sidecar_bin)
            .map_err(|e| (exit_codes::ERROR, e))?;
        if !json {
            output::print_check(output::Indicator::Pass, "Updated sidecar", "");
        }
    } else if !json {
        output::print_check(
            output::Indicator::Warn,
            "sidecar binary",
            "not found in archive (skipped)",
        );
    }

    // 7. Output result
    if json {
        let mut map = json_output::envelope("");
        map.insert("updated".to_string(), serde_json::json!(true));
        map.insert("from".to_string(), serde_json::json!(current_version));
        map.insert("to".to_string(), serde_json::json!(latest_version));
        json_output::print_json(&serde_json::Value::Object(map));
    } else {
        println!();
        output::print_success(&format!(
            "Updated truffle v{current_version} -> v{latest_version}"
        ));
        println!();
    }

    Ok(())
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_platform_asset_returns_valid_name() {
        let name = platform_asset();
        assert!(
            name.starts_with("truffle-"),
            "Asset name should start with 'truffle-'"
        );
        assert!(
            name.ends_with(".tar.gz") || name.ends_with(".zip"),
            "Asset name should end with .tar.gz or .zip"
        );
    }

    #[test]
    fn test_version_comparison_newer() {
        assert!(is_newer("0.3.0", "v0.4.0"));
        assert!(is_newer("0.3.0", "truffle-v0.3.1"));
        assert!(is_newer("0.3.0", "1.0.0"));
        assert!(is_newer("0.3.9", "0.4.0"));
        assert!(is_newer("1.2.3", "1.2.4"));
        assert!(is_newer("1.2.3", "1.3.0"));
        assert!(is_newer("1.2.3", "2.0.0"));
    }

    #[test]
    fn test_version_comparison_not_newer() {
        assert!(!is_newer("0.3.0", "v0.3.0"));
        assert!(!is_newer("0.3.0", "truffle-v0.2.0"));
        assert!(!is_newer("1.0.0", "0.9.9"));
        assert!(!is_newer("0.3.0", "0.3.0"));
    }

    #[test]
    fn test_version_comparison_invalid() {
        assert!(!is_newer("0.3.0", "invalid"));
        assert!(!is_newer("invalid", "0.3.0"));
        assert!(!is_newer("abc", "def"));
    }

    #[test]
    fn test_parse_version_formats() {
        assert_eq!(parse_version("truffle-v0.3.0"), Some((0, 3, 0)));
        assert_eq!(parse_version("v1.2.3"), Some((1, 2, 3)));
        assert_eq!(parse_version("0.3.0"), Some((0, 3, 0)));
        assert_eq!(parse_version("invalid"), None);
        assert_eq!(parse_version("1.2"), None);
        assert_eq!(parse_version("1.2.3.4"), None);
    }

    #[test]
    fn test_strip_version_prefix() {
        assert_eq!(strip_version_prefix("truffle-v0.3.0"), "0.3.0");
        assert_eq!(strip_version_prefix("v1.2.3"), "1.2.3");
        assert_eq!(strip_version_prefix("0.3.0"), "0.3.0");
    }
}
