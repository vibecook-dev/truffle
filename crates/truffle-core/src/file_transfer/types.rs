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
    ///
    /// A pull names its file one of two ways:
    ///
    /// - **By absolute path**: `path` is a path on the holder, served only if
    ///   it lies inside one of the holder's absolute-path pull roots
    ///   ([`add_pull_root`](super::FileTransfer::add_pull_root)), and `named`
    ///   is absent.
    /// - **By name** (RFC 032): `named` carries a root name and a path relative
    ///   to that root ([`add_named_pull_root`](super::FileTransfer::add_named_pull_root)),
    ///   and `path` is empty. A holder that predates named roots ignores
    ///   `named`, cannot resolve the empty path, and refuses with
    ///   [`PULL_DENIED_REASON`], which the requester reports as
    ///   [`TransferError::Unsupported`]. It never serves a file for it.
    #[serde(rename = "pull_request")]
    PullRequest {
        /// The holder's absolute path (absolute-path pull), or empty (named pull).
        path: String,
        requester_id: String,
        token: String,
        /// The named root and relative path (named pull, RFC 032). Absent from
        /// an absolute-path pull, so its JSON is unchanged.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        named: Option<NamedPath>,
    },
}

/// The REJECT reason a holder sends for every refused absolute-path pull,
/// whatever the refusal's cause (an unknown path, one outside every root, a
/// directory, an oversize file), so a requester learns nothing about what
/// exists on the holder.
pub const PULL_DENIED_REASON: &str = "pull denied by peer";

/// The REJECT reason a holder sends for every refused named pull (RFC 032),
/// with the same single-answer rule as [`PULL_DENIED_REASON`]. It differs
/// from that reason so a requester can tell a refusal from a holder that
/// predates named roots.
pub const NAMED_PULL_DENIED_REASON: &str = "named pull denied by peer";

/// A file below a holder's named pull root (RFC 032), as a named pull
/// carries it in [`FtMessage::PullRequest`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamedPath {
    /// The root's name, as the holder registered it. Matched exactly: names
    /// are lowercase by grammar, so there is no case folding.
    pub root: String,
    /// The path below the root: relative, `/`-separated and normalized (no
    /// empty, `.` or `..` segment, no NUL, `\` or `:`).
    pub path: String,
}

/// A registered named pull root, as
/// [`named_pull_roots`](super::FileTransfer::named_pull_roots) lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedPullRoot {
    /// The name peers pull by.
    pub name: String,
    /// The directory the name serves, canonicalized at registration.
    pub path: std::path::PathBuf,
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
    /// The peer runs a version that predates what this request needs, so it
    /// cannot answer it (a named pull sent to a holder that predates RFC 032).
    /// Nothing was transferred.
    Unsupported(String),
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
            TransferError::Unsupported(what) => write!(f, "Unsupported by peer: {what}"),
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

    // ── PULL_REQUEST wire shape (RFC 032) ──────────────────────────────

    /// The PULL_REQUEST as truffle 0.4.8 through 0.7.26 parse it, field for
    /// field: what a holder that predates named roots sees.
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type")]
    enum HolderBeforeNamedRoots {
        #[serde(rename = "pull_request")]
        PullRequest {
            path: String,
            requester_id: String,
            token: String,
        },
    }

    #[test]
    fn absolute_pull_json_is_unchanged() {
        // An absolute-path pull carries no `named` key, so its JSON is the
        // same bytes every earlier holder already parses.
        let msg = FtMessage::PullRequest {
            path: "/srv/shared/f.txt".into(),
            requester_id: "01HZZZZZZZZZZZZZZZZZZZZZZZ".into(),
            token: "tok-1".into(),
            named: None,
        };
        assert_eq!(
            serde_json::to_value(&msg).unwrap(),
            serde_json::json!({
                "type": "pull_request",
                "path": "/srv/shared/f.txt",
                "requester_id": "01HZZZZZZZZZZZZZZZZZZZZZZZ",
                "token": "tok-1",
            })
        );
    }

    #[test]
    fn earlier_requesters_parse_as_absolute_pulls() {
        // An earlier requester's JSON has no `named`: a current holder reads
        // it as an absolute-path pull, exactly as before.
        let raw = r#"{"type":"pull_request","path":"/s/f.txt","requester_id":"r","token":"t"}"#;
        match serde_json::from_str::<FtMessage>(raw).unwrap() {
            FtMessage::PullRequest { path, named, .. } => {
                assert_eq!(path, "/s/f.txt");
                assert_eq!(named, None);
            }
            other => panic!("expected a PULL_REQUEST, got {other:?}"),
        }
    }

    #[test]
    fn named_pull_round_trips_and_sends_an_empty_path() {
        let msg = FtMessage::PullRequest {
            path: String::new(),
            requester_id: "r".into(),
            token: "t".into(),
            named: Some(NamedPath {
                root: "drive-objects".into(),
                path: "ab/ab01.bin".into(),
            }),
        };
        let value = serde_json::to_value(&msg).unwrap();
        assert_eq!(value["path"], "");
        assert_eq!(value["named"]["root"], "drive-objects");
        assert_eq!(value["named"]["path"], "ab/ab01.bin");

        match serde_json::from_value::<FtMessage>(value).unwrap() {
            FtMessage::PullRequest { path, named, .. } => {
                assert_eq!(path, "");
                assert_eq!(
                    named,
                    Some(NamedPath {
                        root: "drive-objects".into(),
                        path: "ab/ab01.bin".into(),
                    })
                );
            }
            other => panic!("expected a PULL_REQUEST, got {other:?}"),
        }
    }

    #[test]
    fn a_holder_before_named_roots_sees_only_the_empty_path() {
        // The earlier parser ignores `named` and keeps the empty `path`, the
        // one value its authorization can never resolve to a file (see the
        // receiver's `holder_before_named_roots_refuses_the_empty_path`).
        let msg = FtMessage::PullRequest {
            path: String::new(),
            requester_id: "r".into(),
            token: "t".into(),
            named: Some(NamedPath {
                root: "objects".into(),
                path: "f.txt".into(),
            }),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let parsed: HolderBeforeNamedRoots = serde_json::from_str(&json).unwrap();
        let HolderBeforeNamedRoots::PullRequest {
            path,
            requester_id,
            token,
        } = parsed;
        assert_eq!(path, "");
        assert_eq!(requester_id, "r");
        assert_eq!(token, "t");
    }

    #[test]
    fn the_two_denial_reasons_differ() {
        // The requester tells an earlier holder from a refusal by this
        // difference alone.
        assert_ne!(PULL_DENIED_REASON, NAMED_PULL_DENIED_REASON);
    }

    #[test]
    fn unsupported_displays_its_cause() {
        let e = TransferError::Unsupported("named pull roots".into());
        assert_eq!(e.to_string(), "Unsupported by peer: named pull roots");
    }
}
