//! Binary header format for bridge connections.
//!
//! Every connection from the Go sidecar to the Rust bridge begins with a binary
//! header that identifies the session, direction, port, and request metadata.
//!
//! ## Wire format (big-endian):
//!
//! ```text
//! ┌─────────────┬───────┬──────────────────┬───────────┬─────────────┐
//! │ Magic (4B)  │ Ver   │ SessionToken     │ Direction │ ServicePort │
//! │ 0x54524646  │ 0x01  │ (32 bytes)       │ (1 byte)  │ (2 bytes)   │
//! ├─────────────┴───────┴──────────────────┴───────────┴─────────────┤
//! │ RequestIdLen (2B) │ RequestId (N bytes)                          │
//! ├───────────────────┼──────────────────────────────────────────────┤
//! │ RemoteAddrLen (2B)│ RemoteAddr (M bytes)                         │
//! ├───────────────────┼──────────────────────────────────────────────┤
//! │ RemoteDNSLen (2B) │ RemoteDNSName (K bytes)                      │
//! └───────────────────┴──────────────────────────────────────────────┘
//! ```

use std::fmt;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Magic bytes: "TRFF" (0x54524646)
pub(crate) const MAGIC: u32 = 0x5452_4646;

/// Current protocol version.
pub(crate) const VERSION: u8 = 0x01;

/// Maximum length for RequestId field.
pub(crate) const MAX_REQUEST_ID_LEN: u16 = 128;

/// Maximum length for RemoteAddr field.
pub(crate) const MAX_REMOTE_ADDR_LEN: u16 = 256;

/// Maximum length for RemoteDNSName field. This field carries the JSON-encoded
/// PeerIdentity from the sidecar's WhoIs lookup (dnsName, loginName, displayName,
/// profilePicUrl, nodeId and, since sidecar protocol 9, userId, tags and the
/// app-capability map), not just a DNS name. Must match `maxRemoteDNSNameLen`
/// in packages/sidecar-slim/main.go. 16 KiB leaves room for a tailnet
/// policy's capability grants; the sidecar drops (and flags) an even larger
/// capability map rather than exceed it.
pub(crate) const MAX_REMOTE_DNS_NAME_LEN: u16 = 16 * 1024;

/// Minimum header size: magic(4) + version(1) + token(32) + direction(1) + port(2) + 3 length fields(2 each).
/// Used in tests to verify wire format sizes.
#[allow(dead_code)]
pub(crate) const MIN_HEADER_SIZE: usize = 4 + 1 + 32 + 1 + 2 + 2 + 2 + 2;

/// Direction of a bridge connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub(crate) enum Direction {
    /// Connection initiated by a remote peer to us.
    Incoming = 0x01,
    /// Connection initiated by us to a remote peer.
    Outgoing = 0x02,
}

impl Direction {
    pub(crate) fn from_byte(b: u8) -> Result<Self, HeaderError> {
        match b {
            0x01 => Ok(Direction::Incoming),
            0x02 => Ok(Direction::Outgoing),
            _ => Err(HeaderError::InvalidDirection(b)),
        }
    }
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::Incoming => write!(f, "incoming"),
            Direction::Outgoing => write!(f, "outgoing"),
        }
    }
}

/// Errors from reading or writing a bridge header.
#[derive(Debug, thiserror::Error)]
pub(crate) enum HeaderError {
    #[error("invalid magic: expected 0x54524646, got 0x{0:08X}")]
    InvalidMagic(u32),

    #[error("unsupported version: expected 1, got {0}")]
    UnsupportedVersion(u8),

    #[error("invalid direction byte: 0x{0:02X}")]
    InvalidDirection(u8),

    #[error("RequestIdLen {0} exceeds maximum {MAX_REQUEST_ID_LEN}")]
    RequestIdTooLong(u16),

    #[error("RemoteAddrLen {0} exceeds maximum {MAX_REMOTE_ADDR_LEN}")]
    RemoteAddrTooLong(u16),

    #[error("RemoteDNSNameLen {0} exceeds maximum {MAX_REMOTE_DNS_NAME_LEN}")]
    RemoteDnsNameTooLong(u16),

    #[error("incoming direction must have RequestIdLen == 0, got {0}")]
    IncomingWithRequestId(u16),

    #[error("invalid UTF-8 in {field}: {source}")]
    InvalidUtf8 {
        field: &'static str,
        source: std::string::FromUtf8Error,
    },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Binary header sent at the start of every bridge connection.
///
/// See the module-level docs for the wire layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BridgeHeader {
    pub session_token: [u8; 32],
    pub direction: Direction,
    pub service_port: u16,
    pub request_id: String,
    pub remote_addr: String,
    pub remote_dns_name: String,
}

impl BridgeHeader {
    /// Read a BridgeHeader from an async reader.
    ///
    /// The caller is responsible for applying timeouts.
    pub async fn read_from<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Self, HeaderError> {
        // Magic (4 bytes, big-endian)
        let magic = reader.read_u32().await?;
        if magic != MAGIC {
            return Err(HeaderError::InvalidMagic(magic));
        }

        // Version (1 byte)
        let version = reader.read_u8().await?;
        if version != VERSION {
            return Err(HeaderError::UnsupportedVersion(version));
        }

        // SessionToken (32 bytes)
        let mut session_token = [0u8; 32];
        reader.read_exact(&mut session_token).await?;

        // Direction (1 byte)
        let direction = Direction::from_byte(reader.read_u8().await?)?;

        // ServicePort (2 bytes, big-endian)
        let service_port = reader.read_u16().await?;

        // RequestIdLen (2 bytes)
        let request_id_len = reader.read_u16().await?;
        if request_id_len > MAX_REQUEST_ID_LEN {
            return Err(HeaderError::RequestIdTooLong(request_id_len));
        }

        // Incoming direction invariant
        if direction == Direction::Incoming && request_id_len != 0 {
            return Err(HeaderError::IncomingWithRequestId(request_id_len));
        }

        // RequestId (N bytes, UTF-8)
        let request_id = if request_id_len > 0 {
            let mut buf = vec![0u8; request_id_len as usize];
            reader.read_exact(&mut buf).await?;
            String::from_utf8(buf).map_err(|e| HeaderError::InvalidUtf8 {
                field: "RequestId",
                source: e,
            })?
        } else {
            String::new()
        };

        // RemoteAddrLen (2 bytes)
        let remote_addr_len = reader.read_u16().await?;
        if remote_addr_len > MAX_REMOTE_ADDR_LEN {
            return Err(HeaderError::RemoteAddrTooLong(remote_addr_len));
        }

        // RemoteAddr (M bytes, UTF-8)
        let remote_addr = if remote_addr_len > 0 {
            let mut buf = vec![0u8; remote_addr_len as usize];
            reader.read_exact(&mut buf).await?;
            String::from_utf8(buf).map_err(|e| HeaderError::InvalidUtf8 {
                field: "RemoteAddr",
                source: e,
            })?
        } else {
            String::new()
        };

        // RemoteDNSNameLen (2 bytes)
        let remote_dns_name_len = reader.read_u16().await?;
        if remote_dns_name_len > MAX_REMOTE_DNS_NAME_LEN {
            return Err(HeaderError::RemoteDnsNameTooLong(remote_dns_name_len));
        }

        // RemoteDNSName (K bytes, UTF-8)
        let remote_dns_name = if remote_dns_name_len > 0 {
            let mut buf = vec![0u8; remote_dns_name_len as usize];
            reader.read_exact(&mut buf).await?;
            String::from_utf8(buf).map_err(|e| HeaderError::InvalidUtf8 {
                field: "RemoteDNSName",
                source: e,
            })?
        } else {
            String::new()
        };

        Ok(BridgeHeader {
            session_token,
            direction,
            service_port,
            request_id,
            remote_addr,
            remote_dns_name,
        })
    }

    /// Write a BridgeHeader to an async writer.
    #[allow(dead_code)]
    pub async fn write_to<W: AsyncWrite + Unpin>(&self, writer: &mut W) -> Result<(), HeaderError> {
        let request_id_bytes = self.request_id.as_bytes();
        let remote_addr_bytes = self.remote_addr.as_bytes();
        let remote_dns_name_bytes = self.remote_dns_name.as_bytes();

        // Validate lengths
        if request_id_bytes.len() > MAX_REQUEST_ID_LEN as usize {
            return Err(HeaderError::RequestIdTooLong(request_id_bytes.len() as u16));
        }
        if remote_addr_bytes.len() > MAX_REMOTE_ADDR_LEN as usize {
            return Err(HeaderError::RemoteAddrTooLong(
                remote_addr_bytes.len() as u16
            ));
        }
        if remote_dns_name_bytes.len() > MAX_REMOTE_DNS_NAME_LEN as usize {
            return Err(HeaderError::RemoteDnsNameTooLong(
                remote_dns_name_bytes.len() as u16,
            ));
        }

        // Validate incoming direction invariant
        if self.direction == Direction::Incoming && !self.request_id.is_empty() {
            return Err(HeaderError::IncomingWithRequestId(
                request_id_bytes.len() as u16
            ));
        }

        // Build complete header into a single buffer
        let total_len = MIN_HEADER_SIZE
            + request_id_bytes.len()
            + remote_addr_bytes.len()
            + remote_dns_name_bytes.len();
        let mut buf = Vec::with_capacity(total_len);

        buf.extend_from_slice(&MAGIC.to_be_bytes());
        buf.push(VERSION);
        buf.extend_from_slice(&self.session_token);
        buf.push(self.direction as u8);
        buf.extend_from_slice(&self.service_port.to_be_bytes());
        buf.extend_from_slice(&(request_id_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(request_id_bytes);
        buf.extend_from_slice(&(remote_addr_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(remote_addr_bytes);
        buf.extend_from_slice(&(remote_dns_name_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(remote_dns_name_bytes);

        writer.write_all(&buf).await?;
        writer.flush().await?;

        Ok(())
    }

    /// Returns the serialized byte length of this header.
    #[allow(dead_code)]
    pub fn wire_len(&self) -> usize {
        MIN_HEADER_SIZE
            + self.request_id.len()
            + self.remote_addr.len()
            + self.remote_dns_name.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn test_token() -> [u8; 32] {
        let mut token = [0u8; 32];
        for (i, byte) in token.iter_mut().enumerate() {
            *byte = i as u8;
        }
        token
    }

    fn make_incoming_header() -> BridgeHeader {
        BridgeHeader {
            session_token: test_token(),
            direction: Direction::Incoming,
            service_port: 443,
            request_id: String::new(),
            remote_addr: "100.64.0.2:12345".to_string(),
            remote_dns_name: "peer-host.tailnet.ts.net".to_string(),
        }
    }

    fn make_outgoing_header() -> BridgeHeader {
        BridgeHeader {
            session_token: test_token(),
            direction: Direction::Outgoing,
            service_port: 9417,
            request_id: "req-abc-123".to_string(),
            remote_addr: "100.64.0.3:9417".to_string(),
            remote_dns_name: "target.tailnet.ts.net".to_string(),
        }
    }

    #[tokio::test]
    async fn roundtrip_incoming_header() {
        let original = make_incoming_header();
        let mut buf = Vec::new();
        original.write_to(&mut buf).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let parsed = BridgeHeader::read_from(&mut cursor).await.unwrap();
        assert_eq!(original, parsed);
    }

    #[tokio::test]
    async fn roundtrip_outgoing_header() {
        let original = make_outgoing_header();
        let mut buf = Vec::new();
        original.write_to(&mut buf).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let parsed = BridgeHeader::read_from(&mut cursor).await.unwrap();
        assert_eq!(original, parsed);
    }

    #[tokio::test]
    async fn wire_len_matches_serialized_len() {
        let header = make_outgoing_header();
        let mut buf = Vec::new();
        header.write_to(&mut buf).await.unwrap();
        assert_eq!(header.wire_len(), buf.len());
    }

    #[tokio::test]
    async fn reject_invalid_magic() {
        let mut buf = vec![0xDE, 0xAD, 0xBE, 0xEF]; // wrong magic
        buf.push(VERSION);
        buf.extend_from_slice(&[0u8; 32]); // token
        buf.push(0x01); // direction
        buf.extend_from_slice(&443u16.to_be_bytes()); // port
        buf.extend_from_slice(&0u16.to_be_bytes()); // req_id_len
        buf.extend_from_slice(&0u16.to_be_bytes()); // remote_addr_len
        buf.extend_from_slice(&0u16.to_be_bytes()); // remote_dns_len

        let mut cursor = Cursor::new(buf);
        let err = BridgeHeader::read_from(&mut cursor).await.unwrap_err();
        assert!(matches!(err, HeaderError::InvalidMagic(0xDEADBEEF)));
    }

    #[tokio::test]
    async fn reject_unsupported_version() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC.to_be_bytes());
        buf.push(0xFF); // wrong version
        buf.extend_from_slice(&[0u8; 32]); // token
        buf.push(0x01); // direction
        buf.extend_from_slice(&443u16.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());

        let mut cursor = Cursor::new(buf);
        let err = BridgeHeader::read_from(&mut cursor).await.unwrap_err();
        assert!(matches!(err, HeaderError::UnsupportedVersion(0xFF)));
    }

    #[tokio::test]
    async fn reject_invalid_direction() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC.to_be_bytes());
        buf.push(VERSION);
        buf.extend_from_slice(&[0u8; 32]);
        buf.push(0x99); // invalid direction
        buf.extend_from_slice(&443u16.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());
        buf.extend_from_slice(&0u16.to_be_bytes());

        let mut cursor = Cursor::new(buf);
        let err = BridgeHeader::read_from(&mut cursor).await.unwrap_err();
        assert!(matches!(err, HeaderError::InvalidDirection(0x99)));
    }

    #[tokio::test]
    async fn reject_incoming_with_request_id() {
        let header = BridgeHeader {
            session_token: test_token(),
            direction: Direction::Incoming,
            service_port: 443,
            request_id: "should-be-empty".to_string(),
            remote_addr: String::new(),
            remote_dns_name: String::new(),
        };
        let mut buf = Vec::new();
        let err = header.write_to(&mut buf).await.unwrap_err();
        assert!(matches!(err, HeaderError::IncomingWithRequestId(_)));
    }

    #[tokio::test]
    async fn empty_fields_roundtrip() {
        let header = BridgeHeader {
            session_token: test_token(),
            direction: Direction::Incoming,
            service_port: 9417,
            request_id: String::new(),
            remote_addr: String::new(),
            remote_dns_name: String::new(),
        };
        let mut buf = Vec::new();
        header.write_to(&mut buf).await.unwrap();
        assert_eq!(buf.len(), MIN_HEADER_SIZE);

        let mut cursor = Cursor::new(buf);
        let parsed = BridgeHeader::read_from(&mut cursor).await.unwrap();
        assert_eq!(header, parsed);
    }

    #[test]
    fn direction_display() {
        assert_eq!(format!("{}", Direction::Incoming), "incoming");
        assert_eq!(format!("{}", Direction::Outgoing), "outgoing");
    }

    #[test]
    fn direction_from_byte() {
        assert_eq!(Direction::from_byte(0x01).unwrap(), Direction::Incoming);
        assert_eq!(Direction::from_byte(0x02).unwrap(), Direction::Outgoing);
        assert!(Direction::from_byte(0x00).is_err());
        assert!(Direction::from_byte(0x03).is_err());
    }

    #[tokio::test]
    async fn roundtrip_identity_json_at_cap() {
        // A realistic identity JSON padded to exactly MAX_REMOTE_DNS_NAME_LEN
        // bytes. `overhead` is the same literal with an empty padding value, so
        // the arithmetic is exact regardless of the fixed text's length. Before
        // the cap was raised from 512, write_to rejected this with
        // RemoteDnsNameTooLong; at the cap (16 KiB since protocol 9) it
        // roundtrips intact.
        let overhead =
            r#"{"dnsName":"peer.ts.net","nodeId":"nWXYZ1","profilePicUrl":"https://p.example/"}"#
                .len();
        let padding = "a".repeat(MAX_REMOTE_DNS_NAME_LEN as usize - overhead);
        let dns = format!(
            r#"{{"dnsName":"peer.ts.net","nodeId":"nWXYZ1","profilePicUrl":"https://p.example/{padding}"}}"#
        );
        assert_eq!(dns.len(), MAX_REMOTE_DNS_NAME_LEN as usize);

        let mut original = make_incoming_header();
        original.remote_dns_name = dns;

        let mut buf = Vec::new();
        original.write_to(&mut buf).await.unwrap();

        let mut cursor = Cursor::new(buf);
        let parsed = BridgeHeader::read_from(&mut cursor).await.unwrap();
        assert_eq!(original, parsed);
    }

    #[tokio::test]
    async fn reject_oversize_remote_dns_name() {
        // A RemoteDNSName length field one byte over the cap must be rejected
        // BEFORE the body is allocated/read, proving the pre-allocation bound
        // still holds after the cap was raised.
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC.to_be_bytes());
        buf.push(VERSION);
        buf.extend_from_slice(&[0u8; 32]); // token
        buf.push(0x01); // direction = incoming
        buf.extend_from_slice(&443u16.to_be_bytes()); // port
        buf.extend_from_slice(&0u16.to_be_bytes()); // req_id_len
        buf.extend_from_slice(&0u16.to_be_bytes()); // remote_addr_len
        buf.extend_from_slice(&(MAX_REMOTE_DNS_NAME_LEN + 1).to_be_bytes()); // remote_dns_len over cap
                                                                             // No body follows: read_from must reject on the length check first.

        let mut cursor = Cursor::new(buf);
        let err = BridgeHeader::read_from(&mut cursor).await.unwrap_err();
        assert!(matches!(err, HeaderError::RemoteDnsNameTooLong(_)));
    }
}
