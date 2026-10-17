//! Envelope codecs — pluggable serialization for [`Envelope`].
//!
//! The default codec is [`JsonCodec`], which produces human-readable JSON.
//! Future codecs (MessagePack, Protobuf) can be added by implementing the
//! [`EnvelopeCodec`] trait.

use super::{Envelope, EnvelopeError};

// ---------------------------------------------------------------------------
// EnvelopeCodec trait
// ---------------------------------------------------------------------------

// @atlas contract envelope-codec "Pluggable envelope codec" {
//   implemented-by crate:truffle-core
//   see "docs/rfcs/012-layered-architecture-redesign.md#32-layer-6-envelope"
// }
/// A codec that serializes and deserializes [`Envelope`]s.
///
/// Implementations must be `Send + Sync` so they can be shared across
/// async tasks behind an `Arc`.
///
/// # Default implementation
///
/// [`JsonCodec`] — produces compact JSON. Suitable for debugging and
/// low-to-medium throughput applications.
pub trait EnvelopeCodec: Send + Sync {
    /// Encode an envelope into a byte buffer.
    fn encode(&self, envelope: &Envelope) -> Result<Vec<u8>, EnvelopeError>;

    /// Decode an envelope from a byte buffer.
    fn decode(&self, data: &[u8]) -> Result<Envelope, EnvelopeError>;
}

// ---------------------------------------------------------------------------
// JsonCodec
// ---------------------------------------------------------------------------

/// JSON envelope codec (the default).
///
/// Produces compact (non-pretty-printed) JSON. Unknown fields in the input
/// are silently ignored during deserialization for forward compatibility.
#[derive(Debug, Clone, Default)]
pub struct JsonCodec;

impl EnvelopeCodec for JsonCodec {
    fn encode(&self, envelope: &Envelope) -> Result<Vec<u8>, EnvelopeError> {
        serde_json::to_vec(envelope).map_err(EnvelopeError::Serialization)
    }

    fn decode(&self, data: &[u8]) -> Result<Envelope, EnvelopeError> {
        serde_json::from_slice(data).map_err(EnvelopeError::Deserialization)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_json_codec_encode_decode_roundtrip() {
        let codec = JsonCodec;

        let envelope = Envelope::new("chat", "message", json!({"text": "hello world"}));
        let encoded = codec.encode(&envelope).expect("encode");
        let decoded = codec.decode(&encoded).expect("decode");

        assert_eq!(decoded.namespace, "chat");
        assert_eq!(decoded.msg_type, "message");
        assert_eq!(decoded.payload["text"], "hello world");
    }

    #[test]
    fn test_json_codec_preserves_all_fields() {
        let codec = JsonCodec;

        let envelope = Envelope {
            namespace: "ft".into(),
            msg_type: "offer".into(),
            payload: json!({"file": "data.bin"}),
            timestamp: Some(1711234567890),
            from: Some("peer-abc".into()),
        };

        let encoded = codec.encode(&envelope).expect("encode");
        let decoded = codec.decode(&encoded).expect("decode");

        assert_eq!(decoded.namespace, "ft");
        assert_eq!(decoded.msg_type, "offer");
        assert_eq!(decoded.payload["file"], "data.bin");
        assert_eq!(decoded.timestamp, Some(1711234567890));
        assert_eq!(decoded.from.as_deref(), Some("peer-abc"));
    }

    #[test]
    fn test_json_codec_unknown_fields_forward_compat() {
        let codec = JsonCodec;

        // JSON with extra fields a future version might add.
        let json_bytes = br#"{
            "namespace": "v2",
            "msg_type": "new_feature",
            "payload": {},
            "priority": "high",
            "trace_id": "abc-123"
        }"#;

        let decoded = codec
            .decode(json_bytes)
            .expect("should decode with unknown fields");
        assert_eq!(decoded.namespace, "v2");
        assert_eq!(decoded.msg_type, "new_feature");
    }

    #[test]
    fn test_json_codec_decode_error() {
        let codec = JsonCodec;
        let result = codec.decode(b"{{invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_json_codec_empty_payload() {
        let codec = JsonCodec;

        let envelope = Envelope::new("ping", "request", json!(null));
        let encoded = codec.encode(&envelope).expect("encode");
        let decoded = codec.decode(&encoded).expect("decode");

        assert!(decoded.payload.is_null());
    }

    #[test]
    fn test_json_codec_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<JsonCodec>();
    }

    #[test]
    fn test_json_codec_trait_object() {
        // Verify EnvelopeCodec can be used as a trait object behind Arc.
        let codec: std::sync::Arc<dyn EnvelopeCodec> = std::sync::Arc::new(JsonCodec);
        let envelope = Envelope::new("test", "msg", json!("data"));
        let encoded = codec.encode(&envelope).expect("encode");
        let decoded = codec.decode(&encoded).expect("decode");
        assert_eq!(decoded.namespace, "test");
    }

    // ── Property tests (review: protocol fuzzing) ───────────────────────

    use proptest::prelude::*;

    proptest! {
        /// Arbitrary bytes must never panic the decoder — the worst
        /// acceptable outcome is a clean Deserialization error.
        #[test]
        fn decode_never_panics_on_arbitrary_bytes(
            data in proptest::collection::vec(any::<u8>(), 0..2048)
        ) {
            let _ = JsonCodec.decode(&data);
        }

        /// Valid envelopes roundtrip losslessly whatever the field contents
        /// (unicode, quotes, control characters, ...).
        #[test]
        fn roundtrip_arbitrary_fields(
            ns in ".{0,64}",
            mt in ".{0,64}",
            text in ".{0,256}"
        ) {
            let envelope = Envelope::new(&ns, &mt, json!({ "text": text.clone() }));
            let decoded = JsonCodec
                .decode(&JsonCodec.encode(&envelope).unwrap())
                .unwrap();
            prop_assert_eq!(decoded.namespace, ns);
            prop_assert_eq!(decoded.msg_type, mt);
            prop_assert_eq!(&decoded.payload["text"], &serde_json::Value::from(text));
        }
    }
}
