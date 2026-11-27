//! Public HTTP serving through the node's Tailscale Funnel (RFC 033).
pub use crate::network::{FunnelState, FunnelStatus};
use crate::{network::NetworkProvider, NodeError};
use tokio::sync::broadcast;

/// The local node's public-serving capability. Obtain it with `Node::funnel()`.
pub struct Funnel<'a, N: NetworkProvider> {
    pub(crate) network: &'a N,
}
impl<N: NetworkProvider> Funnel<'_, N> {
    /// Read the node's current grant and any approval instructions from control.
    pub async fn status(&self) -> Result<FunnelStatus, NodeError> {
        Ok(self.network.funnel_status().await?)
    }
    /// Subscribe before reading status to avoid missing a change between the two.
    /// A provider without Funnel support returns a closed channel.
    pub fn subscribe(&self) -> broadcast::Receiver<FunnelStatus> {
        self.network.funnel_changes()
    }
}
