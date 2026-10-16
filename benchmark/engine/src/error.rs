//! The engine's one error type (RFC 027 §5.2).
//!
//! Every fallible engine call, every app command and every control-protocol
//! reply carries an [`AppError`]: a stable machine-readable [`ErrorCode`]
//! plus a human sentence. On the wire it is `{"code": "kebab-case", "message": "…"}`.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use truffle_core::file_transfer::TransferError;
use truffle_core::network::NetworkError;
use truffle_core::session::SessionError;
use truffle_core::{NodeError, RequestError};

/// Stable error codes. Serialised in kebab-case (`peer-not-found`, …), so the
/// app and the control protocol can branch on them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum ErrorCode {
    /// The hub (or its node) is not running.
    NotRunning,
    /// No peer matches the reference, or the sender of a request is unknown.
    PeerNotFound,
    /// The peer reference matches more than one peer.
    PeerAmbiguous,
    /// The peer is known but offline or gone.
    PeerOffline,
    /// The responder's admission rule refused this peer (§6 rule 1).
    LoginRefused,
    /// The responder is initiating its own run (§6 rule 3).
    Busy,
    /// This hub is being measured by a peer and cannot start a run (§6 rule 3).
    BeingMeasured,
    /// The peer speaks a different control-protocol version.
    ProtocolMismatch,
    /// The peer does not offer the requested workload.
    UnsupportedWorkload,
    /// Parameters are out of bounds or malformed; never clamped.
    InvalidParams,
    /// A deadline passed.
    Timeout,
    /// Another process holds this hub's state directory.
    StateInUse,
    /// No sidecar binary could be found (§5.3).
    SidecarNotFound,
    /// The run or suite was cancelled.
    Cancelled,
    /// A local I/O error.
    Io,
    /// Anything else.
    Internal,
}

impl ErrorCode {
    /// The wire spelling, e.g. `"peer-not-found"`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotRunning => "not-running",
            Self::PeerNotFound => "peer-not-found",
            Self::PeerAmbiguous => "peer-ambiguous",
            Self::PeerOffline => "peer-offline",
            Self::LoginRefused => "login-refused",
            Self::Busy => "busy",
            Self::BeingMeasured => "being-measured",
            Self::ProtocolMismatch => "protocol-mismatch",
            Self::UnsupportedWorkload => "unsupported-workload",
            Self::InvalidParams => "invalid-params",
            Self::Timeout => "timeout",
            Self::StateInUse => "state-in-use",
            Self::SidecarNotFound => "sidecar-not-found",
            Self::Cancelled => "cancelled",
            Self::Io => "io",
            Self::Internal => "internal",
        }
    }

    /// Whether a caller may reasonably retry the same request later
    /// (`peer-not-found` covers a peer that discovery has not listed yet).
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::PeerNotFound
                | Self::PeerOffline
                | Self::Busy
                | Self::BeingMeasured
                | Self::Timeout
        )
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An engine error: a stable code plus a human-readable message.
///
/// `Display` prints only the message; use [`AppError::code`] to branch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AppError {
    /// Machine-readable code.
    pub code: ErrorCode,
    /// One plain sentence for people.
    pub message: String,
}

impl AppError {
    /// Build an error from a code and a message.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// `invalid-params` with the given reason.
    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidParams, message)
    }

    /// `internal` with the given reason.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    /// `timeout` naming what timed out.
    pub fn timeout(what: impl Into<String>) -> Self {
        Self::new(ErrorCode::Timeout, what)
    }

    /// `cancelled`.
    pub fn cancelled() -> Self {
        Self::new(ErrorCode::Cancelled, "cancelled")
    }

    /// `not-running`.
    pub fn not_running() -> Self {
        Self::new(ErrorCode::NotRunning, "the hub is not running")
    }

    /// `io`, prefixing the I/O error with what was being done.
    pub fn io(context: impl fmt::Display, err: &std::io::Error) -> Self {
        Self::new(ErrorCode::Io, format!("{context}: {err}"))
    }

    /// Prefix the message with context, keeping the code.
    #[must_use]
    pub fn context(mut self, context: impl fmt::Display) -> Self {
        self.message = format!("{context}: {}", self.message);
        self
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AppError {}

impl From<std::io::Error> for AppError {
    fn from(e: std::io::Error) -> Self {
        let code = if e.kind() == std::io::ErrorKind::TimedOut {
            ErrorCode::Timeout
        } else {
            ErrorCode::Io
        };
        Self::new(code, e.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for AppError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        Self::timeout("timed out")
    }
}

impl From<NodeError> for AppError {
    fn from(e: NodeError) -> Self {
        let message = e.to_string();
        let code = match &e {
            NodeError::PeerNotFound(_) => ErrorCode::PeerNotFound,
            NodeError::AmbiguousPeer { query, candidates } => {
                return Self::new(
                    ErrorCode::PeerAmbiguous,
                    format!(
                        "“{query}” matches more than one peer: {}",
                        candidates.join(", ")
                    ),
                );
            }
            NodeError::PeerGone(_) => ErrorCode::PeerOffline,
            NodeError::Stopped => ErrorCode::NotRunning,
            NodeError::Io(_) => ErrorCode::Io,
            NodeError::Session(s) => match s {
                SessionError::UnknownPeer(_) => ErrorCode::PeerNotFound,
                SessionError::PeerGone(_) | SessionError::PeerOffline(_) => ErrorCode::PeerOffline,
                SessionError::LoginUnknown(_) => ErrorCode::LoginRefused,
                _ => ErrorCode::Internal,
            },
            NodeError::Network(n) => match n {
                NetworkError::NotRunning => ErrorCode::NotRunning,
                NetworkError::DialTimeout(_) => ErrorCode::Timeout,
                NetworkError::Io(_) => ErrorCode::Io,
                _ => ErrorCode::Internal,
            },
            _ => ErrorCode::Internal,
        };
        Self::new(code, message)
    }
}

impl From<RequestError> for AppError {
    fn from(e: RequestError) -> Self {
        match e {
            RequestError::Send(n) => n.into(),
            RequestError::Timeout => Self::timeout("timed out waiting for the peer's reply"),
            RequestError::ChannelClosed => Self::not_running(),
        }
    }
}

impl From<TransferError> for AppError {
    fn from(e: TransferError) -> Self {
        let code = match &e {
            TransferError::Io(_) => ErrorCode::Io,
            TransferError::Timeout => ErrorCode::Timeout,
            _ => ErrorCode::Internal,
        };
        Self::new(code, e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_serialise_kebab_case_and_match_as_str() {
        let all = [
            ErrorCode::NotRunning,
            ErrorCode::PeerNotFound,
            ErrorCode::PeerAmbiguous,
            ErrorCode::PeerOffline,
            ErrorCode::LoginRefused,
            ErrorCode::Busy,
            ErrorCode::BeingMeasured,
            ErrorCode::ProtocolMismatch,
            ErrorCode::UnsupportedWorkload,
            ErrorCode::InvalidParams,
            ErrorCode::Timeout,
            ErrorCode::StateInUse,
            ErrorCode::SidecarNotFound,
            ErrorCode::Cancelled,
            ErrorCode::Io,
            ErrorCode::Internal,
        ];
        for code in all {
            let json = serde_json::to_value(code).unwrap();
            assert_eq!(json, serde_json::Value::String(code.as_str().into()));
            let back: ErrorCode = serde_json::from_value(json).unwrap();
            assert_eq!(back, code);
        }
    }

    #[test]
    fn app_error_wire_shape() {
        let e = AppError::new(ErrorCode::PeerNotFound, "no peer called studio");
        let json = serde_json::to_value(&e).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"code": "peer-not-found", "message": "no peer called studio"})
        );
        assert_eq!(e.to_string(), "no peer called studio");
        assert_eq!(
            e.context("run tcp").message,
            "run tcp: no peer called studio"
        );
    }

    #[test]
    fn node_errors_map_to_codes() {
        let e: AppError = NodeError::PeerNotFound("x".into()).into();
        assert_eq!(e.code, ErrorCode::PeerNotFound);
        let e: AppError = NodeError::AmbiguousPeer {
            query: "mac".into(),
            candidates: vec!["mac-a".into(), "mac-b".into()],
        }
        .into();
        assert_eq!(e.code, ErrorCode::PeerAmbiguous);
        assert!(e.message.contains("mac-a, mac-b"));
        let e: AppError = NodeError::Stopped.into();
        assert_eq!(e.code, ErrorCode::NotRunning);
        let e: AppError = NodeError::Session(SessionError::PeerOffline("p".into())).into();
        assert_eq!(e.code, ErrorCode::PeerOffline);
        let e: AppError =
            NodeError::Network(NetworkError::DialTimeout(std::time::Duration::from_secs(1))).into();
        assert_eq!(e.code, ErrorCode::Timeout);
        let e: AppError = std::io::Error::other("disk").into();
        assert_eq!(e.code, ErrorCode::Io);
        let e: AppError = RequestError::Timeout.into();
        assert_eq!(e.code, ErrorCode::Timeout);
    }
}
