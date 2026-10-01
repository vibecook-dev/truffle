//! NAPI-RS bindings for truffle-core.
//!
//! Exposes the truffle-core `Node<TailscaleProvider>` API to Node.js via
//! two main classes:
//!
//! - [`NapiNode`] — peer discovery, messaging, diagnostics
//! - [`NapiFileTransfer`] — file send/receive/pull

pub mod egress;
pub mod file_transfer;
pub mod node;
pub mod peer;
pub mod proxy;
pub mod quic;
pub mod raw_socket;
pub mod subscription;
pub mod synced_store;
pub mod types;
pub mod udp_socket;
