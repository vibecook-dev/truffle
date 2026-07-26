#!/bin/sh
# Install truffle CLI + sidecar
# Usage: curl -fsSL https://vibecook-dev.github.io/truffle/install.sh | sh
#
# Options:
#   --version <tag>    Install a specific version (e.g., v0.1.0)
#   --dir <path>       Custom install directory
#   --no-verify        Skip running 'truffle doctor' after install
#   --uninstall        Remove truffle and clean up PATH
#
# Environment:
#   TRUFFLE_INSTALL_DIR   Override default install directory

set -e

# ═══════════════════════════════════════════════════════════════════════════
# Defaults
# ═══════════════════════════════════════════════════════════════════════════

REPO="vibecook-dev/truffle"
VERSION="latest"
INSTALL_DIR=""
VERIFY=true

# ═══════════════════════════════════════════════════════════════════════════
# Parse arguments
# ═══════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════
# Uninstall mode: curl ... | sh -s -- --uninstall
# ═══════════════════════════════════════════════════════════════════════════

if [ "${1:-}" = "--uninstall" ] || [ "${UNINSTALL:-}" = "1" ]; then
    echo ""
    echo "truffle uninstaller"
    echo "═══════════════════════════════════════"
    echo ""

    UNINSTALL_DIR="${TRUFFLE_INSTALL_DIR:-$HOME/.config/truffle/bin}"
    CONFIG_DIR="$HOME/.config/truffle"

    # Remove binaries
    if [ -d "$UNINSTALL_DIR" ]; then
        echo "  Removing binaries from $UNINSTALL_DIR..."
        rm -f "$UNINSTALL_DIR/truffle" "$UNINSTALL_DIR/sidecar-slim" "$UNINSTALL_DIR/truffle-sidecar"
        rmdir "$UNINSTALL_DIR" 2>/dev/null || true
        echo "  ✓ Binaries removed"
    else
        echo "  No binaries found at $UNINSTALL_DIR"
    fi

    # Remove state (optional)
    if [ -d "$CONFIG_DIR/state" ]; then
        printf "  Remove Tailscale state at %s? [y/N] " "$CONFIG_DIR/state"
        read -r REPLY
        if [ "$REPLY" = "y" ] || [ "$REPLY" = "Y" ]; then
            rm -rf "$CONFIG_DIR/state"
            echo "  ✓ State removed"
        else
            echo "  State kept"
        fi
    fi

    # Remove config
    if [ -f "$CONFIG_DIR/config.toml" ]; then
        printf "  Remove config file? [y/N] "
        read -r REPLY
        if [ "$REPLY" = "y" ] || [ "$REPLY" = "Y" ]; then
            rm -f "$CONFIG_DIR/config.toml"
            echo "  ✓ Config removed"
        fi
    fi

    rmdir "$CONFIG_DIR" 2>/dev/null || true

    # Clean up shell profile
    SHELL_NAME=$(basename "${SHELL:-/bin/sh}")
    case "$SHELL_NAME" in
        zsh)  PROFILE="$HOME/.zshrc" ;;
        bash)
            if [ -f "$HOME/.bashrc" ]; then PROFILE="$HOME/.bashrc"
            else PROFILE="$HOME/.bash_profile"; fi ;;
        *)    PROFILE="$HOME/.profile" ;;
    esac

    if [ -n "$PROFILE" ] && [ -f "$PROFILE" ]; then
        if grep -q "# Added by truffle installer" "$PROFILE" 2>/dev/null; then
            sed -i.bak '/# Added by truffle installer/d' "$PROFILE"
            sed -i.bak "\|\.config/truffle/bin|d" "$PROFILE"
            rm -f "${PROFILE}.bak"
            echo "  ✓ Removed PATH entry from $PROFILE"
        fi
    fi

    echo ""
    echo "  ✓ truffle uninstalled"
    echo ""
    echo "  Open a new terminal for PATH changes to take effect."
    echo ""
    exit 0
fi

# ═══════════════════════════════════════════════════════════════════════════
# Parse install arguments
# ═══════════════════════════════════════════════════════════════════════════

while [ $# -gt 0 ]; do
    case "$1" in
        --version)
            VERSION="$2"
            shift 2
            ;;
        --dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --no-verify)
            VERIFY=false
            shift
            ;;
        --help|-h)
            echo "Usage: install.sh [--version <tag>] [--dir <path>] [--no-verify]"
            echo ""
            echo "Options:"
            echo "  --version <tag>    Install a specific version (e.g., v0.1.0)"
            echo "  --dir <path>       Custom install directory"
            echo "  --no-verify        Skip running 'truffle doctor' after install"
            echo ""
            echo "Environment:"
            echo "  TRUFFLE_INSTALL_DIR   Override default install directory"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Run with --help for usage."
            exit 1
            ;;
    esac
done

# ═══════════════════════════════════════════════════════════════════════════
# Detect platform
# ═══════════════════════════════════════════════════════════════════════════

OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$OS" in
    darwin|linux) ;;
    *)
        echo "Error: Unsupported operating system: $OS"
        echo "truffle supports macOS (darwin) and Linux."
        echo "For Windows, use install.ps1 instead."
        exit 1
        ;;
esac

case "$ARCH" in
    x86_64|amd64) ARCH="x64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *)
        echo "Error: Unsupported architecture: $ARCH"
        echo "truffle supports x86_64 (x64) and aarch64 (arm64)."
        exit 1
        ;;
esac

# ═══════════════════════════════════════════════════════════════════════════
# Resolve install directory
# ═══════════════════════════════════════════════════════════════════════════

if [ -z "$INSTALL_DIR" ]; then
    INSTALL_DIR="${TRUFFLE_INSTALL_DIR:-$HOME/.config/truffle/bin}"
fi

# ═══════════════════════════════════════════════════════════════════════════
# Check dependencies
# ═══════════════════════════════════════════════════════════════════════════

if ! command -v curl >/dev/null 2>&1; then
    echo "Error: curl is required but not installed."
    echo "Install curl and try again."
    exit 1
fi

if ! command -v tar >/dev/null 2>&1; then
    echo "Error: tar is required but not installed."
    echo "Install tar and try again."
    exit 1
fi

# ═══════════════════════════════════════════════════════════════════════════
# Build download URL
# ═══════════════════════════════════════════════════════════════════════════

ASSET="truffle-${OS}-${ARCH}.tar.gz"

if [ "$VERSION" = "latest" ]; then
    URL="https://github.com/${REPO}/releases/latest/download/${ASSET}"
else
    URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET}"
fi

# ═══════════════════════════════════════════════════════════════════════════
# Download and install
# ═══════════════════════════════════════════════════════════════════════════

echo "truffle installer"
echo "═══════════════════════════════════════"
echo ""
echo "  Platform:  ${OS}-${ARCH}"
echo "  Version:   ${VERSION}"
echo "  Directory: ${INSTALL_DIR}"
echo ""

mkdir -p "$INSTALL_DIR"

echo "Downloading ${ASSET}..."
TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

HTTP_CODE=$(curl -fsSL -w "%{http_code}" -o "$TEMP_DIR/$ASSET" "$URL" 2>/dev/null) || true

if [ ! -f "$TEMP_DIR/$ASSET" ] || [ "$HTTP_CODE" = "404" ]; then
    echo ""
    echo "Error: Failed to download ${ASSET}"
    echo ""
    echo "  URL: ${URL}"
    echo ""
    if [ "$VERSION" != "latest" ]; then
        echo "  Check that version '${VERSION}' exists at:"
        echo "  https://github.com/${REPO}/releases"
    else
        echo "  Check that a release exists at:"
        echo "  https://github.com/${REPO}/releases/latest"
    fi
    echo ""
    echo "  This platform/arch combination may not be available yet."
    exit 1
fi

echo "Extracting to ${INSTALL_DIR}..."
tar xzf "$TEMP_DIR/$ASSET" -C "$INSTALL_DIR"

# Make binaries executable
if [ -f "$INSTALL_DIR/truffle" ]; then
    chmod +x "$INSTALL_DIR/truffle"
fi
if [ -f "$INSTALL_DIR/sidecar-slim" ]; then
    chmod +x "$INSTALL_DIR/sidecar-slim"
fi

echo ""
echo "Installed:"
[ -f "$INSTALL_DIR/truffle" ] && echo "  truffle      -> ${INSTALL_DIR}/truffle"
[ -f "$INSTALL_DIR/sidecar-slim" ] && echo "  sidecar-slim -> ${INSTALL_DIR}/sidecar-slim"
echo ""

# ═══════════════════════════════════════════════════════════════════════════
# PATH check
# ═══════════════════════════════════════════════════════════════════════════

case ":$PATH:" in
    *":$INSTALL_DIR:"*)
        # Already in PATH — nothing to do
        ;;
    *)
        # Auto-add to shell profile (like Claude Code, rustup, etc.)
        SHELL_NAME=$(basename "${SHELL:-/bin/sh}")
        PATH_LINE="export PATH=\"${INSTALL_DIR}:\$PATH\""

        case "$SHELL_NAME" in
            zsh)
                PROFILE="$HOME/.zshrc"
                ;;
            bash)
                # Prefer .bashrc, fall back to .bash_profile (macOS)
                if [ -f "$HOME/.bashrc" ]; then
                    PROFILE="$HOME/.bashrc"
                else
                    PROFILE="$HOME/.bash_profile"
                fi
                ;;
            fish)
                # Fish uses a different syntax
                PROFILE=""
                fish -c "fish_add_path ${INSTALL_DIR}" 2>/dev/null || true
                echo "  Added to fish PATH"
                ;;
            *)
                PROFILE="$HOME/.profile"
                ;;
        esac

        if [ -n "$PROFILE" ]; then
            # Check if already in the profile
            if ! grep -q "$INSTALL_DIR" "$PROFILE" 2>/dev/null; then
                echo "" >> "$PROFILE"
                echo "# Added by truffle installer" >> "$PROFILE"
                echo "$PATH_LINE" >> "$PROFILE"
                echo "  Added to $PROFILE"
            fi
        fi

        # Make available in current session immediately
        export PATH="${INSTALL_DIR}:$PATH"
        echo ""
        ;;
esac

# ═══════════════════════════════════════════════════════════════════════════
# Verify installation
# ═══════════════════════════════════════════════════════════════════════════

if [ "$VERIFY" = true ]; then
    if [ -f "$INSTALL_DIR/truffle" ]; then
        echo "Running 'truffle doctor' to verify installation..."
        echo ""
        "$INSTALL_DIR/truffle" doctor || true
    fi
fi

echo "Run 'truffle up' to start your node and join the mesh."

