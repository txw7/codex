use std::io::Cursor;

use codex_protocol::ThreadId;
use codex_rollout::RolloutItem;

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
    #[error("failed to compress turn payload: {0}")]
    Compression(#[from] std::io::Error),
}

/// One completely assembled terminal-turn frame.
///
/// JOURNAL-NOTE: bytes is the exact payload submitted to the journal writer.
/// The writer is not allowed to serialize, append sidecars, or discover more
/// metadata after this point. One immutable frame is how "one turn, one append"
/// remains an invariant instead of a slogan.
#[derive(Debug)]
pub struct EncodedTurnFrame {
    pub bytes: Vec<u8>,
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
    items: &[RolloutItem],
) -> Result<EncodedTurnFrame, JournalFormatError> {
    let payload = serde_json::to_vec(&serde_json::json!({
        "schema": "codex.ram_journal.turn.v1",
        "thread_id": thread_id.to_string(),
        "turn_id": turn_id,
        "sequence": sequence,
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
        bytes,
        digest,
        compressed_len,
        uncompressed_len,
    })
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_terminal_turns_encode_identically() {
        let thread_id = ThreadId::new();
        let first =
            encode_turn_frame(thread_id, "turn-1", 1, &[]).expect("first frame should encode");
        let second =
            encode_turn_frame(thread_id, "turn-1", 1, &[]).expect("second frame should encode");

        // JOURNAL-NOTE: deterministic bytes make duplicate-commit detection a
        // content check rather than an interpretive exercise involving clocks.
        assert_eq!(first.bytes, second.bytes);
        assert_eq!(first.digest, second.digest);
    }

    #[test]
    fn frame_has_explicit_boundaries_and_declared_payload_length() {
        let frame =
            encode_turn_frame(ThreadId::new(), "turn-1", 7, &[]).expect("frame should encode");

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
