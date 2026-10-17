//! NAPI-RS bindings for truffle-core.
//!
//! Exposes the truffle-core `Node<TailscaleProvider>` API to Node.js via
//! two main classes:
//!
//! - [`NapiNode`] — peer discovery, messaging, diagnostics
//! - [`NapiFileTransfer`] — file send/receive/pull
//!
//! @atlas layer bindings · domain bindings
//! @atlas rule napi-pull-model "Raw-transport handles pass bytes only as the answer to a JS read, never a ThreadsafeFunction" {
//!   forbid text "ThreadsafeFunction" in mod:truffle_napi::{raw_socket, udp_socket, quic}
//!   see "docs/rfcs/021-raw-transport-js-api.md#12-decided-questions"
//!   see "docs/rfcs/021-raw-transport-js-api.md#5-design-overview"
//! }

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
