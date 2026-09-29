use std::io::Cursor;
use std::sync::Arc;

use codex_protocol::ThreadId;
use codex_rollout::RolloutItem;
use codex_thread_store::CreateThreadParams;

const FRAME_MAGIC: &[u8; 8] = b"CJR1TURN";
const FRAME_END_MAGIC: &[u8; 8] = b"CJR1END!";
const FRAME_VERSION: u16 = 1;
const TURN_RECORD_TYPE: u16 = 1;
const FIXED_HEADER_BYTES: usize = 8 + 2 + 2 + 8 + 8 + 32;
const FIXED_FOOTER_BYTES: usize = 8;

#[derive(Debug, thiserror::Error)]
pub enum JournalFormatError {
    #[error("failed to encode turn payload: {0}")]
    Json(#[from] serde_json::Error),
    #[error("failed to compress or decompress turn payload: {0}")]
    Compression(#[from] std::io::Error),
    #[error("truncated CJR frame: need {needed} bytes, have {available}")]
    Truncated { needed: usize, available: usize },
    #[error("invalid CJR frame magic")]
    InvalidMagic,
    #[error("unsupported CJR frame version {0}")]
    UnsupportedVersion(u16),
    #[error("unsupported CJR record type {0}")]
    UnsupportedRecordType(u16),
    #[error("invalid CJR frame footer")]
    InvalidFooter,
    #[error("CJR frame digest mismatch")]
    DigestMismatch,
    #[error("CJR uncompressed length mismatch: declared {declared}, decoded {decoded}")]
    UncompressedLengthMismatch { declared: u64, decoded: usize },
    #[error("invalid CJR payload schema")]
    InvalidSchema,
    #[error("CJR payload is missing field {0}")]
    MissingField(&'static str),
    #[error("invalid CJR thread id: {0}")]
    InvalidThreadId(String),
}

impl JournalFormatError {
    pub fn is_truncated(&self) -> bool {
        matches!(self, Self::Truncated { .. })
    }
}

/// One completely assembled terminal-turn frame.
///
/// JOURNAL-NOTE: bytes is the exact payload submitted to the journal writer.
/// The writer is not allowed to serialize, append sidecars, or discover more
/// metadata after this point. One immutable frame is how "one turn, one append"
/// remains an invariant instead of a slogan.
#[derive(Clone, Debug)]
pub struct EncodedTurnFrame {
    pub thread_id: ThreadId,
    pub turn_id: String,
    pub sequence: u64,
    pub previous_digest: Option<[u8; 32]>,
    pub bytes: Arc<[u8]>,
    pub digest: [u8; 32],
    pub compressed_len: u64,
    pub uncompressed_len: u64,
}

/// Encode one terminal turn into an independently compressed CJR V1 frame.
///
/// The semantic payload intentionally contains upstream RolloutItem values
/// rather than a fork-specific event language. We are changing the durable
/// envelope, not volunteering to maintain a second interpretation of every
/// conversation event upstream invents.
pub fn encode_turn_frame(
    thread_id: ThreadId,
    turn_id: &str,
    sequence: u64,
    previous_digest: Option<[u8; 32]>,
    bootstrap: Option<&CreateThreadParams>,
    items: &[RolloutItem],
) -> Result<EncodedTurnFrame, JournalFormatError> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "schema": "codex.ram_journal.turn.v1",
        "thread_id": thread_id.to_string(),
        "turn_id": turn_id,
        "sequence": sequence,
        "previous_digest": previous_digest,
        "bootstrap": bootstrap,
        "items": items,
    }))?;
    let compressed = zstd::stream::encode_all(Cursor::new(&payload), 1)?;
    let digest = *blake3::hash(&compressed).as_bytes();

    let compressed_len = u64::try_from(compressed.len()).expect("usize should fit in u64");
    let uncompressed_len = u64::try_from(payload.len()).expect("usize should fit in u64");

    let mut bytes =
        Vec::with_capacity(FIXED_HEADER_BYTES + compressed.len() + FIXED_FOOTER_BYTES);
    bytes.extend_from_slice(FRAME_MAGIC);
    bytes.extend_from_slice(&FRAME_VERSION.to_le_bytes());
    bytes.extend_from_slice(&TURN_RECORD_TYPE.to_le_bytes());
    bytes.extend_from_slice(&compressed_len.to_le_bytes());
    bytes.extend_from_slice(&uncompressed_len.to_le_bytes());
    bytes.extend_from_slice(&digest);
    bytes.extend_from_slice(&compressed);
    bytes.extend_from_slice(FRAME_END_MAGIC);

    Ok(EncodedTurnFrame {
        thread_id,
        turn_id: turn_id.to_string(),
        sequence,
        previous_digest,
        bytes: Arc::from(bytes),
        digest,
        compressed_len,
        uncompressed_len,
    })
}


/// Decoded semantic contents of one terminal-turn frame.
#[derive(Debug)]
pub struct DecodedTurnFrame {
    pub thread_id: ThreadId,
    pub turn_id: String,
    pub sequence: u64,
    pub previous_digest: Option<[u8; 32]>,
    pub bootstrap: Option<CreateThreadParams>,
    pub items: Vec<RolloutItem>,
    pub digest: [u8; 32],
}

/// Decode exactly one CJR V1 frame from the beginning of `bytes`.
///
/// Returns the semantic frame plus the number of bytes consumed, allowing the
/// recovery scanner to walk a concatenated journal without teaching the codec
/// anything about files.
pub fn decode_turn_frame(
    bytes: &[u8],
) -> Result<(DecodedTurnFrame, usize), JournalFormatError> {
    if bytes.len() < FIXED_HEADER_BYTES {
        return Err(JournalFormatError::Truncated {
            needed: FIXED_HEADER_BYTES,
            available: bytes.len(),
        });
    }
    if &bytes[..8] != FRAME_MAGIC {
        return Err(JournalFormatError::InvalidMagic);
    }

    let version = u16::from_le_bytes(bytes[8..10].try_into().expect("fixed header slice"));
    if version != FRAME_VERSION {
        return Err(JournalFormatError::UnsupportedVersion(version));
    }

    let record_type = u16::from_le_bytes(bytes[10..12].try_into().expect("fixed header slice"));
    if record_type != TURN_RECORD_TYPE {
        return Err(JournalFormatError::UnsupportedRecordType(record_type));
    }

    let compressed_len =
        u64::from_le_bytes(bytes[12..20].try_into().expect("fixed header slice"));
    let uncompressed_len =
        u64::from_le_bytes(bytes[20..28].try_into().expect("fixed header slice"));
    let declared_digest: [u8; 32] = bytes[28..60].try_into().expect("fixed header digest");

    let compressed_len_usize =
        usize::try_from(compressed_len).map_err(|_| JournalFormatError::Truncated {
            needed: usize::MAX,
            available: bytes.len(),
        })?;
    let payload_end = FIXED_HEADER_BYTES
        .checked_add(compressed_len_usize)
        .ok_or(JournalFormatError::Truncated {
            needed: usize::MAX,
            available: bytes.len(),
        })?;
    let frame_end = payload_end
        .checked_add(FIXED_FOOTER_BYTES)
        .ok_or(JournalFormatError::Truncated {
            needed: usize::MAX,
            available: bytes.len(),
        })?;

    if bytes.len() < frame_end {
        return Err(JournalFormatError::Truncated {
            needed: frame_end,
            available: bytes.len(),
        });
    }
    if &bytes[payload_end..frame_end] != FRAME_END_MAGIC {
        return Err(JournalFormatError::InvalidFooter);
    }

    let compressed = &bytes[FIXED_HEADER_BYTES..payload_end];
    let computed_digest = *blake3::hash(compressed).as_bytes();
    if computed_digest != declared_digest {
        return Err(JournalFormatError::DigestMismatch);
    }

    let payload = zstd::stream::decode_all(Cursor::new(compressed))?;
    if u64::try_from(payload.len()).ok() != Some(uncompressed_len) {
        return Err(JournalFormatError::UncompressedLengthMismatch {
            declared: uncompressed_len,
            decoded: payload.len(),
        });
    }

    let value: serde_json::Value = serde_json::from_slice(&payload)?;
    if value.get("schema").and_then(serde_json::Value::as_str)
        != Some("codex.ram_journal.turn.v1")
    {
        return Err(JournalFormatError::InvalidSchema);
    }

    let thread_id_raw = value
        .get("thread_id")
        .and_then(serde_json::Value::as_str)
        .ok_or(JournalFormatError::MissingField("thread_id"))?;
    let thread_id = ThreadId::from_string(thread_id_raw)
        .map_err(|error| JournalFormatError::InvalidThreadId(error.to_string()))?;
    let turn_id = value
        .get("turn_id")
        .and_then(serde_json::Value::as_str)
        .ok_or(JournalFormatError::MissingField("turn_id"))?
        .to_string();
    let sequence = value
        .get("sequence")
        .and_then(serde_json::Value::as_u64)
        .ok_or(JournalFormatError::MissingField("sequence"))?;
    let previous_digest = serde_json::from_value(
        value
            .get("previous_digest")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )?;
    let bootstrap = serde_json::from_value(
        value
            .get("bootstrap")
            .cloned()
            .unwrap_or(serde_json::Value::Null),
    )?;
    let items = serde_json::from_value(
        value
            .get("items")
            .cloned()
            .ok_or(JournalFormatError::MissingField("items"))?,
    )?;

    Ok((
        DecodedTurnFrame {
            thread_id,
            turn_id,
            sequence,
            previous_digest,
            bootstrap,
            items,
            digest: declared_digest,
        },
        frame_end,
    ))
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_terminal_turns_encode_identically() {
        let thread_id = ThreadId::new();
        let first =
            encode_turn_frame(thread_id, "turn-1", 1, None, None, &[]).expect("first frame should encode");
        let second =
            encode_turn_frame(thread_id, "turn-1", 1, None, None, &[]).expect("second frame should encode");

        // JOURNAL-NOTE: deterministic bytes make duplicate-commit detection a
        // content check rather than an interpretive exercise involving clocks.
        assert_eq!(first.bytes.as_ref(), second.bytes.as_ref());
        assert_eq!(first.digest, second.digest);
    }

    #[test]
    fn encoded_frame_round_trips_semantics_and_chain_digest() {
        let thread_id = ThreadId::new();
        let previous_digest = Some([7_u8; 32]);
        let frame = encode_turn_frame(
            thread_id,
            "turn-roundtrip",
            9,
            previous_digest,
            None,
            &[],
        )
        .expect("frame should encode");

        let (decoded, consumed) =
            decode_turn_frame(&frame.bytes).expect("frame should decode");

        assert_eq!(consumed, frame.bytes.len());
        assert_eq!(decoded.thread_id, thread_id);
        assert_eq!(decoded.turn_id, "turn-roundtrip");
        assert_eq!(decoded.sequence, 9);
        assert_eq!(decoded.previous_digest, previous_digest);
        assert_eq!(decoded.digest, frame.digest);
        assert!(decoded.bootstrap.is_none());
        assert!(decoded.items.is_empty());
    }

    #[test]
    fn truncated_frame_is_distinct_from_corruption() {
        let frame =
            encode_turn_frame(ThreadId::new(), "turn-1", 1, None, None, &[])
                .expect("frame should encode");
        let truncated = &frame.bytes[..frame.bytes.len() - 1];

        let err = decode_turn_frame(truncated).expect_err("tail should be truncated");
        assert!(err.is_truncated());
    }

    #[test]
    fn frame_has_explicit_boundaries_and_declared_payload_length() {
        let frame =
            encode_turn_frame(ThreadId::new(), "turn-1", 7, None, None, &[]).expect("frame should encode");

        assert!(frame.bytes.starts_with(FRAME_MAGIC));
        assert!(frame.bytes.ends_with(FRAME_END_MAGIC));
        assert_eq!(
            frame.bytes.len(),
            FIXED_HEADER_BYTES
                + usize::try_from(frame.compressed_len).expect("compressed length should fit")
                + FIXED_FOOTER_BYTES
        );
    }
}
