use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use codex_protocol::ThreadId;
use codex_rollout::RolloutItem;
use codex_thread_store::CreateThreadParams;

use super::DecodedTurnFrame;
use super::JournalFormatError;
use super::decode_turn_frame;
use crate::pending_turn::terminal_event_identity;

#[derive(Debug, thiserror::Error)]
pub enum JournalReadError {
    #[error("journal I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("corrupt CJR frame at byte {offset}: {source}")]
    Corrupt {
        offset: usize,
        #[source]
        source: JournalFormatError,
    },
    #[error("CJR frame at byte {offset} belongs to thread {actual}, expected {expected}")]
    ThreadMismatch {
        offset: usize,
        expected: ThreadId,
        actual: ThreadId,
    },
    #[error("CJR frame at byte {offset} has sequence {actual}, expected {expected}")]
    SequenceMismatch {
        offset: usize,
        expected: u64,
        actual: u64,
    },
    #[error("CJR frame at byte {offset} breaks the previous-digest chain")]
    ChainMismatch { offset: usize },
    #[error("invalid CJR journal filename {path}")]
    InvalidJournalFilename { path: PathBuf },
    #[error("CJR frame at byte {offset} has invalid terminal semantics: {message}")]
    InvalidTerminal { offset: usize, message: String },
    #[error(
        "CJR frame at byte {offset} declares turn {declared} but terminates turn {actual}"
    )]
    TerminalTurnMismatch {
        offset: usize,
        declared: String,
        actual: String,
    },
}

/// Longest verified journal prefix.
///
/// A truncated final frame is not included in `frames`; `valid_bytes` points
/// to the exact byte boundary after the last complete verified frame.
#[derive(Debug)]
pub struct RecoveredJournal {
    pub frames: Vec<DecodedTurnFrame>,
    pub valid_bytes: usize,
    pub truncated_tail: bool,
}

impl RecoveredJournal {
    pub fn bootstrap(&self) -> Option<&CreateThreadParams> {
        self.frames.first().and_then(|frame| frame.bootstrap.as_ref())
    }

    pub fn items(&self) -> Vec<RolloutItem> {
        self.frames
            .iter()
            .flat_map(|frame| frame.items.iter().cloned())
            .collect()
    }

    pub fn last_digest(&self) -> Option<[u8; 32]> {
        self.frames.last().map(|frame| frame.digest)
    }

    pub fn next_sequence(&self) -> u64 {
        self.frames
            .last()
            .and_then(|frame| frame.sequence.checked_add(1))
            .unwrap_or(1)
    }
}

#[derive(Clone, Debug)]
pub struct JournalReader {
    root: Arc<PathBuf>,
}

impl JournalReader {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root: Arc::new(root),
        }
    }

    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    pub fn recover_thread(
        &self,
        thread_id: ThreadId,
    ) -> Result<Option<RecoveredJournal>, JournalReadError> {
        let path = super::journal_thread_path(self.root(), thread_id);
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        recover_bytes(thread_id, &bytes).map(Some)
    }

    /// Discover durable thread ids from the journal placement tree.
    ///
    /// COMPAT-NOTE: Phase 02 uses filename discovery only to bootstrap the
    /// in-memory catalog after process restart. The pathname is still placement,
    /// not semantic thread identity; every discovered id is revalidated against
    /// the frame contents during recovery.
    pub fn list_thread_ids(&self) -> Result<Vec<ThreadId>, JournalReadError> {
        let v1 = self.root().join("v1");
        let prefixes = match std::fs::read_dir(&v1) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error.into()),
        };

        let mut ids = Vec::new();
        for prefix in prefixes {
            let prefix = prefix?;
            if !prefix.file_type()?.is_dir() {
                continue;
            }
            for entry in std::fs::read_dir(prefix.path())? {
                let entry = entry?;
                if !entry.file_type()?.is_file()
                    || entry.path().extension().and_then(|value| value.to_str()) != Some("cjr")
                {
                    continue;
                }
                let path = entry.path();
                let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
                    return Err(JournalReadError::InvalidJournalFilename {
                        path: path.clone(),
                    });
                };
                let thread_id = ThreadId::from_string(stem).map_err(|_| {
                    JournalReadError::InvalidJournalFilename {
                        path: path.clone(),
                    }
                })?;
                ids.push(thread_id);
            }
        }

        ids.sort_by_key(|thread_id| thread_id.to_string());
        ids.dedup();
        Ok(ids)
    }
}

/// Recover the longest valid frame prefix from one journal byte stream.
///
/// JOURNAL-NOTE: only a truncated final frame is recoverable. Invalid magic,
/// digest mismatch, sequence discontinuity, or chain discontinuity inside the
/// verified prefix is corruption and remains an error.
pub fn recover_bytes(
    thread_id: ThreadId,
    bytes: &[u8],
) -> Result<RecoveredJournal, JournalReadError> {
    let mut frames = Vec::new();
    let mut offset = 0_usize;
    let mut expected_sequence = 1_u64;
    let mut expected_previous_digest = None;

    while offset < bytes.len() {
        let (frame, consumed) = match decode_turn_frame(&bytes[offset..]) {
            Ok(decoded) => decoded,
            Err(error) if error.is_truncated() => {
                return Ok(RecoveredJournal {
                    frames,
                    valid_bytes: offset,
                    truncated_tail: true,
                });
            }
            Err(source) => {
                return Err(JournalReadError::Corrupt { offset, source });
            }
        };

        if frame.thread_id != thread_id {
            return Err(JournalReadError::ThreadMismatch {
                offset,
                expected: thread_id,
                actual: frame.thread_id,
            });
        }
        if frame.sequence != expected_sequence {
            return Err(JournalReadError::SequenceMismatch {
                offset,
                expected: expected_sequence,
                actual: frame.sequence,
            });
        }
        if frame.previous_digest != expected_previous_digest {
            return Err(JournalReadError::ChainMismatch { offset });
        }

        let terminal = terminal_event_identity(&frame.items).map_err(|error| {
            JournalReadError::InvalidTerminal {
                offset,
                message: error.to_string(),
            }
        })?;
        let Some(terminal) = terminal else {
            return Err(JournalReadError::InvalidTerminal {
                offset,
                message: "terminal frame contains no terminal RolloutItem".to_string(),
            });
        };
        if terminal.turn_id != frame.turn_id {
            return Err(JournalReadError::TerminalTurnMismatch {
                offset,
                declared: frame.turn_id.clone(),
                actual: terminal.turn_id,
            });
        }

        expected_previous_digest = Some(frame.digest);
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(JournalReadError::SequenceMismatch {
                offset,
                expected: u64::MAX,
                actual: frame.sequence,
            })?;
        offset = offset
            .checked_add(consumed)
            .ok_or(JournalReadError::Corrupt {
                offset,
                source: JournalFormatError::Truncated {
                    needed: usize::MAX,
                    available: bytes.len(),
                },
            })?;
        frames.push(frame);
    }

    Ok(RecoveredJournal {
        frames,
        valid_bytes: offset,
        truncated_tail: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::encode_turn_frame;

    #[test]
    fn recovers_a_verified_two_turn_chain() {
        let thread_id = ThreadId::new();
        let first = encode_turn_frame(thread_id, "turn-1", 1, None, None, &[])
            .expect("first frame");
        let second = encode_turn_frame(
            thread_id,
            "turn-2",
            2,
            Some(first.digest),
            None,
            &[],
        )
        .expect("second frame");
        let mut bytes = first.bytes.to_vec();
        bytes.extend_from_slice(&second.bytes);

        let recovered = recover_bytes(thread_id, &bytes).expect("journal should recover");

        assert_eq!(recovered.frames.len(), 2);
        assert_eq!(recovered.valid_bytes, bytes.len());
        assert!(!recovered.truncated_tail);
        assert_eq!(recovered.next_sequence(), 3);
        assert_eq!(recovered.last_digest(), Some(second.digest));
    }

    #[test]
    fn frame_without_terminal_semantics_is_rejected() {
        let thread_id = ThreadId::new();
        let frame = encode_turn_frame(thread_id, "turn-1", 1, None, None, &[])
            .expect("frame");

        let error = recover_bytes(thread_id, &frame.bytes)
            .expect_err("a frame without a terminal RolloutItem is not a durable turn");
        assert!(matches!(error, JournalReadError::InvalidTerminal { .. }));
    }

    #[test]
    fn incomplete_final_frame_preserves_the_complete_prefix() {
        let thread_id = ThreadId::new();
        let first = encode_turn_frame(thread_id, "turn-1", 1, None, None, &[])
            .expect("first frame");
        let second = encode_turn_frame(
            thread_id,
            "turn-2",
            2,
            Some(first.digest),
            None,
            &[],
        )
        .expect("second frame");
        let mut bytes = first.bytes.to_vec();
        bytes.extend_from_slice(&second.bytes[..second.bytes.len() / 2]);

        let recovered = recover_bytes(thread_id, &bytes).expect("tail should be recoverable");

        assert_eq!(recovered.frames.len(), 1);
        assert_eq!(recovered.valid_bytes, first.bytes.len());
        assert!(recovered.truncated_tail);
    }

    #[test]
    fn discovers_thread_ids_from_the_v1_placement_tree() {
        let temp = tempfile::tempdir().expect("tempdir");
        let reader = JournalReader::new(temp.path().to_path_buf());
        let thread_id = ThreadId::new();
        let path = super::super::journal_thread_path(reader.root(), thread_id);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("create parent");
        std::fs::write(path, b"placeholder").expect("write placeholder");

        let ids = reader.list_thread_ids().expect("list thread ids");
        assert_eq!(ids, vec![thread_id]);
    }

    #[test]
    fn interior_corruption_is_not_rebranded_as_a_truncated_tail() {
        let thread_id = ThreadId::new();
        let frame = encode_turn_frame(thread_id, "turn-1", 1, None, None, &[])
            .expect("frame");
        let mut bytes = frame.bytes.to_vec();
        bytes[0] ^= 0xff;

        let error = recover_bytes(thread_id, &bytes).expect_err("corruption must fail");
        assert!(matches!(error, JournalReadError::Corrupt { .. }));
    }
}
