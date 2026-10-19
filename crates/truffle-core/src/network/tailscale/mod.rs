//! Tailscale network provider implementation.
//!
//! This module contains `TailscaleProvider`, which implements [`NetworkProvider`](super::NetworkProvider)
//! using a Go sidecar (tsnet) and a local TCP bridge for data plane connections.
//!
//! ## Internal architecture (Layers 1-2, invisible above Layer 3)
//!
//! - **Sidecar** (`sidecar.rs`): Spawns the Go process, sends JSON commands via
//!   stdin, receives JSON events via stdout.
//! - **Bridge** (`bridge.rs`): Listens on a local TCP port. The Go sidecar connects
//!   back to this port with a binary header to bridge Tailscale TCP streams.
//! - **Header** (`header.rs`): Binary header format for bridge connections
//!   (magic, version, session token, direction, port, request ID, etc.).
//! - **Protocol** (`protocol.rs`): JSON command/event types for sidecar communication.
//!
//! Only [`TailscaleProvider`] and [`TailscaleConfig`] are public. Everything else
//! is an implementation detail.

mod bridge;
mod header;
mod protocol;
mod provider;
mod sidecar;

#[cfg(test)]
mod tests;

pub use provider::{TailscaleConfig, TailscaleProvider, DEFAULT_START_TIMEOUT};
