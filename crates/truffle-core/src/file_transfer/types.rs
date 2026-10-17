//! Types for the file transfer protocol.
//!
//! This module defines:
//! - Wire protocol messages (`FtMessage`)
//! - Transfer results and errors
//! - The offer/decision/responder API for accept/reject workflows
//! - Progress and event types for observability

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Wire protocol messages (sent via WS namespace "ft")
// ---------------------------------------------------------------------------

// @atlas contract ft-wire "File-transfer signaling on namespace \"ft\"" {
//   produced-by crate:truffle-core each-variant
//   see "docs/rfcs/014-file-transfer-core.md#2-goals"
// }
/// File transfer signaling message types (sent via WS namespace "ft").
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FtMessage {
    /// Sender offers a file to the receiver.
    #[serde(rename = "offer")]
    Offer {
        file_name: String,
        size: u64,
        sha256: String,
        save_path: String,
        token: String,
        /// TCP port the sender/receiver should use (0 = any).
        tcp_port: u16,
    },

    /// Receiver accepts the offer.
    #[serde(rename = "accept")]
    Accept {
        token: String,
        /// TCP port the receiver is listening on (for downloads).
        tcp_port: u16,
    },

    /// Receiver rejects the offer.
    #[serde(rename = "reject")]
    Reject { token: String, reason: String },

    /// Request a file from a remote peer (for pull/download).
    #[serde(rename = "pull_request")]
    PullRequest {
        path: String,
        requester_id: String,
        token: String,
    },
}

// ---------------------------------------------------------------------------
// Transfer result
// ---------------------------------------------------------------------------

/// Result of a completed file transfer.
///
/// `#[non_exhaustive]`: produced only by truffle-core, so later fields can
/// be added without breaking code outside the crate.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TransferResult {
    /// Number of bytes transferred.
    pub bytes_transferred: u64,
    /// SHA-256 hash of the transferred file.
    pub sha256: String,
    /// Elapsed time in seconds, end to end: peer resolution, hashing,
    /// negotiation and the data transfer itself. See [`timings`](Self::timings)
    /// for the individual phases.
    pub elapsed_secs: f64,
    /// Per-phase timings of this transfer.
    pub timings: TransferTimings,
}

/// Per-phase timings of a file transfer, in seconds.
///
/// [`TransferResult::elapsed_secs`] spans the whole call; these split it into
/// the phases that matter for throughput, so `bytes / transfer_secs` is not
/// diluted by the up-front hash or the OFFER/ACCEPT round trip. It is not a
/// pure wire figure either: see the per-direction notes below. Each value is
/// measured on one monotonic clock on the local side.
///
/// For [`send_file`](super::FileTransfer::send_file) (push):
///
/// - `hash_secs`: SHA-256 of the local file before the OFFER is sent.
/// - `negotiate_secs`: OFFER sent → TCP data stream open (the peer's
///   decision, its listener, the ACCEPT, and the TCP dial).
/// - `transfer_secs`: first write to the data stream (the 72-byte
///   `[size][sha256]` header that precedes the payload) → ACK read, or the ACK
///   wait ending in a timeout or read error. It includes the receiver's
///   verification and placement of the file, which precede its ACK. It also
///   includes the receiving peer's per-connection accept handoff: the data
///   stream opens once the local sidecar's tsnet dial completes, but the
///   receiver's sidecar first resolves the dialler's identity (a LocalAPI
///   WhoIs lookup, not cached) and only then bridges the connection to its
///   listener, so the header and first data wait in buffers through that.
///   This is a fixed cost per transfer, about 40 ms per new connection in
///   same-host benchmark runs, of which the WhoIs is the likely main part;
///   it dominates `transfer_secs` for small files.
/// - `ack_ok`: the receiver's ACK byte was read. `false` means the transfer
///   was still treated as successful because every byte was written, but the
///   ACK never arrived (the connection closed early, or the 10 s ACK wait
///   timed out, which `transfer_secs` then includes).
///
/// For [`pull_file`](super::FileTransfer::pull_file) the local side is the
/// receiver: `hash_secs` is `0.0` (the file is hashed inline while it is
/// received, inside `transfer_secs`); `negotiate_secs` runs from the
/// PULL_REQUEST to the serving peer's TCP connection being accepted (it
/// includes that peer hashing the file); `transfer_secs` runs from that
/// accept to the ACK being written; and `ack_ok` is `true` once the ACK was
/// written. Here the local side does the accept handoff described above, so
/// its cost falls in `negotiate_secs`, not `transfer_secs`: push and pull
/// `transfer_secs` are therefore not directly comparable.
///
/// `#[non_exhaustive]`: produced only by truffle-core, so later phases can be
/// added without breaking code outside the crate (start from
/// [`Default`] to build one).
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct TransferTimings {
    /// Time spent hashing the file before the transfer, in seconds.
    pub hash_secs: f64,
    /// OFFER sent → TCP data stream open, in seconds.
    pub negotiate_secs: f64,
    /// First data-stream write → ACK read (or the ACK wait's end), in seconds.
    pub transfer_secs: f64,
    /// Whether a valid ACK was read from the receiver.
    pub ack_ok: bool,
}

// ---------------------------------------------------------------------------
// Transfer error
// ---------------------------------------------------------------------------

/// Errors during file transfer.
#[derive(Debug)]
pub enum TransferError {
    /// File I/O error.
    Io(std::io::Error),
    /// Network/Node error.
    Node(String),
    /// Peer rejected the transfer.
    Rejected(String),
    /// SHA-256 mismatch.
    IntegrityError { expected: String, actual: String },
    /// Timeout waiting for peer response.
    Timeout,
    /// Protocol error (unexpected message, etc.).
    Protocol(String),
}

impl std::fmt::Display for TransferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransferError::Io(e) => write!(f, "I/O error: {e}"),
            TransferError::Node(e) => write!(f, "Node error: {e}"),
            TransferError::Rejected(reason) => write!(f, "Transfer rejected: {reason}"),
            TransferError::IntegrityError { expected, actual } => {
                write!(f, "SHA-256 mismatch: expected {expected}, got {actual}")
            }
            TransferError::Timeout => write!(f, "Timed out waiting for peer response"),
            TransferError::Protocol(e) => write!(f, "Protocol error: {e}"),
        }
    }
}

impl std::error::Error for TransferError {}

impl From<std::io::Error> for TransferError {
    fn from(e: std::io::Error) -> Self {
        TransferError::Io(e)
    }
}

// ---------------------------------------------------------------------------
// Overwrite policy
// ---------------------------------------------------------------------------

/// What to do when a received file's destination path already exists.
///
/// Applies to both incoming pushes (offers we accept) and
/// [`pull_file`](super::FileTransfer::pull_file) downloads. Set via
/// [`set_overwrite_policy`](super::FileTransfer::set_overwrite_policy).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverwritePolicy {
    /// Fail the transfer if the destination already exists (default).
    #[default]
    Reject,
    /// Replace the existing file (the pre-0.7 behavior).
    Replace,
    /// Save under a deduplicated name like `file (1).txt`.
    Rename,
}

// ---------------------------------------------------------------------------
// Offer API — accept/reject workflow
// ---------------------------------------------------------------------------

/// An incoming file offer from a remote peer.
///
/// The application inspects this and decides whether to accept (providing a
/// save path) or reject (providing a reason). Use the paired
/// [`OfferResponder`] to communicate the decision.
#[derive(Debug, Clone)]
pub struct FileOffer {
    /// Stable node ID of the sending peer.
    pub from_peer: String,
    /// Human-readable name of the sending peer.
    pub from_name: String,
    /// File name being offered.
    pub file_name: String,
    /// File size in bytes.
    pub size: u64,
    /// Expected SHA-256 hash (hex).
    pub sha256: String,
    /// Suggested save path from the sender.
    pub suggested_path: String,
    /// Unique token for this transfer.
    pub token: String,
}

/// The application's decision on a file offer.
#[derive(Debug, Clone)]
pub enum OfferDecision {
    /// Accept the file, saving it to the given path.
    Accept { save_path: String },
    /// Reject the file with a reason.
    Reject { reason: String },
}

/// One-shot responder for an incoming file offer.
///
/// Created as a pair with a [`FileOffer`]. The application calls either
/// `accept()` or `reject()` exactly once. If the responder is dropped
/// without a response, the transfer times out on the receiver side.
pub struct OfferResponder {
    tx: tokio::sync::oneshot::Sender<OfferDecision>,
}

impl OfferResponder {
    /// Create a new responder from a oneshot sender.
    pub(crate) fn new(tx: tokio::sync::oneshot::Sender<OfferDecision>) -> Self {
        Self { tx }
    }

    /// Accept the file, saving it to `save_path`.
    pub fn accept(self, save_path: &str) {
        let _ = self.tx.send(OfferDecision::Accept {
            save_path: save_path.to_string(),
        });
    }

    /// Reject the file with a reason.
    pub fn reject(self, reason: &str) {
        let _ = self.tx.send(OfferDecision::Reject {
            reason: reason.to_string(),
        });
    }
}

impl std::fmt::Debug for OfferResponder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OfferResponder").finish()
    }
}

// ---------------------------------------------------------------------------
// Transfer direction
// ---------------------------------------------------------------------------

/// Direction of a file transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferDirection {
    /// Sending a file to a peer.
    Send,
    /// Receiving a file from a peer.
    Receive,
}

// ---------------------------------------------------------------------------
// Transfer progress
// ---------------------------------------------------------------------------

/// Progress update for an in-flight file transfer.
#[derive(Debug, Clone)]
pub struct TransferProgress {
    /// Unique token for this transfer.
    pub token: String,
    /// Direction of the transfer.
    pub direction: TransferDirection,
    /// File name being transferred.
    pub file_name: String,
    /// Bytes transferred so far.
    pub bytes_transferred: u64,
    /// Total file size in bytes.
    pub total_bytes: u64,
    /// Current transfer speed in bytes per second.
    pub speed_bps: f64,
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// Events emitted by the file transfer subsystem.
///
/// Subscribe via [`FileTransfer::subscribe`](super::FileTransfer::subscribe)
/// to observe transfer lifecycle events.
#[derive(Debug, Clone)]
pub enum FileTransferEvent {
    /// A new file offer has been received (informational; the offer channel
    /// is the primary mechanism for handling offers).
    OfferReceived(FileOffer),

    /// File is being hashed before transfer (pre-transfer phase).
    Hashing {
        token: String,
        file_name: String,
        bytes_hashed: u64,
        total_bytes: u64,
    },

    /// Offer sent, waiting for the receiver to accept or reject.
    WaitingForAccept { token: String, file_name: String },

    /// Progress update for an in-flight transfer.
    Progress(TransferProgress),

    /// A transfer completed successfully.
    Completed {
        token: String,
        direction: TransferDirection,
        file_name: String,
        bytes_transferred: u64,
        sha256: String,
        elapsed_secs: f64,
    },

    /// A transfer was rejected by the receiver.
    Rejected {
        token: String,
        file_name: String,
        reason: String,
    },

    /// A transfer failed due to an error.
    Failed {
        token: String,
        direction: TransferDirection,
        file_name: String,
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transfer_timings_default_is_zero_and_unacknowledged() {
        let t = TransferTimings::default();
        assert_eq!(t.hash_secs, 0.0);
        assert_eq!(t.negotiate_secs, 0.0);
        assert_eq!(t.transfer_secs, 0.0);
        assert!(!t.ack_ok, "a default timing must not claim an ACK");
    }

    #[test]
    fn transfer_timings_serde_round_trip() {
        let t = TransferTimings {
            hash_secs: 0.25,
            negotiate_secs: 0.5,
            transfer_secs: 1.5,
            ack_ok: true,
        };
        let v = serde_json::to_value(t).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "hash_secs": 0.25,
                "negotiate_secs": 0.5,
                "transfer_secs": 1.5,
                "ack_ok": true,
            })
        );
        let back: TransferTimings = serde_json::from_value(v).unwrap();
        assert_eq!(back, t);
    }
}
