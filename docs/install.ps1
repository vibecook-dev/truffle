# Install truffle CLI + sidecar for Windows
# Usage: iwr -useb https://truffle.sh/install.ps1 | iex
#
# Or download and run locally:
#   .\install.ps1
#   .\install.ps1 -Version v0.1.0
#   .\install.ps1 -Dir "C:\tools\truffle"
#   .\install.ps1 -NoVerify

[CmdletBinding()]
param(
    [string]$Version = "latest",
    [string]$Dir = "",
    [switch]$NoVerify
)

$ErrorActionPreference = "Stop"

# ═══════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════

$repo = "vibecook-dev/truffle"

# Resolve install directory
if ($Dir) {
    $installDir = $Dir
} elseif ($env:TRUFFLE_INSTALL_DIR) {
    $installDir = $env:TRUFFLE_INSTALL_DIR
} else {
    $installDir = Join-Path $env:LOCALAPPDATA "truffle\bin"
}

# ═══════════════════════════════════════════════════════════════════════════
# Detect architecture
# ═══════════════════════════════════════════════════════════════════════════

$arch = if ([System.Environment]::Is64BitOperatingSystem) { "x64" } else { "x86" }

if ($arch -ne "x64") {
    Write-Host "Error: truffle only supports 64-bit Windows (x64)." -ForegroundColor Red
    Write-Host "Your system architecture: $arch"
    exit 1
}

# ═══════════════════════════════════════════════════════════════════════════
# Build download URL
# ═══════════════════════════════════════════════════════════════════════════

$asset = "truffle-win32-x64.zip"

if ($Version -eq "latest") {
    $url = "https://github.com/$repo/releases/latest/download/$asset"
} else {
    $url = "https://github.com/$repo/releases/download/$Version/$asset"
}

# ═══════════════════════════════════════════════════════════════════════════
# Download and install
# ═══════════════════════════════════════════════════════════════════════════

Write-Host ""
Write-Host "truffle installer" -ForegroundColor Cyan
Write-Host ("=" * 39)
Write-Host ""
Write-Host "  Platform:  win32-x64"
Write-Host "  Version:   $Version"
Write-Host "  Directory: $installDir"
Write-Host ""

# Create install directory
New-Item -ItemType Directory -Force -Path $installDir | Out-Null

# Download
$tempZip = Join-Path $env:TEMP "truffle-install.zip"

Write-Host "Downloading $asset..."
try {
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    Invoke-WebRequest -Uri $url -OutFile $tempZip -UseBasicParsing
} catch {
    Write-Host ""
    Write-Host "Error: Failed to download $asset" -ForegroundColor Red
    Write-Host ""
    Write-Host "  URL: $url"
    Write-Host ""
    if ($Version -ne "latest") {
        Write-Host "  Check that version '$Version' exists at:"
        Write-Host "  https://github.com/$repo/releases"
    } else {
        Write-Host "  Check that a release exists at:"
        Write-Host "  https://github.com/$repo/releases/latest"
    }
    Write-Host ""
    exit 1
}

# Extract
Write-Host "Extracting to $installDir..."
try {
    Expand-Archive -Path $tempZip -DestinationPath $installDir -Force
} catch {
    Write-Host "Error: Failed to extract archive." -ForegroundColor Red
    Write-Host "The downloaded file may be corrupted. Try again."
    exit 1
} finally {
    # Clean up temp file
    if (Test-Path $tempZip) {
        Remove-Item $tempZip -Force
    }
}

Write-Host ""
Write-Host "Installed:" -ForegroundColor Green
$trufflePath = Join-Path $installDir "truffle.exe"
$sidecarPath = Join-Path $installDir "sidecar-slim.exe"
if (Test-Path $trufflePath) {
    Write-Host "  truffle.exe      -> $trufflePath"
}
if (Test-Path $sidecarPath) {
    Write-Host "  sidecar-slim.exe -> $sidecarPath"
}
Write-Host ""

# ═══════════════════════════════════════════════════════════════════════════
# Add to PATH (user-level, permanent)
# ═══════════════════════════════════════════════════════════════════════════

$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if ($userPath -notlike "*$installDir*") {
    Write-Host "Adding to user PATH..."
    [Environment]::SetEnvironmentVariable("Path", "$installDir;$userPath", "User")
    # Also update current session PATH
    $env:Path = "$installDir;$env:Path"
    Write-Host "  Added $installDir to user PATH." -ForegroundColor Green
    Write-Host "  Restart your terminal for the change to take effect in new windows."
    Write-Host ""
} else {
    Write-Host "  $installDir is already in PATH." -ForegroundColor Green
    Write-Host ""
}

# ═══════════════════════════════════════════════════════════════════════════
# Verify installation
# ═══════════════════════════════════════════════════════════════════════════

if (-not $NoVerify) {
    if (Test-Path $trufflePath) {
        Write-Host "Running 'truffle doctor' to verify installation..."
        Write-Host ""
        try {
            & $trufflePath doctor
        } catch {
            # doctor failures are non-fatal for installation
            Write-Host "  (doctor check encountered issues -- see above)" -ForegroundColor Yellow
        }
        Write-Host ""
    }
}

Write-Host "Run 'truffle up' to start your node and join the mesh." -ForegroundColor Cyan
