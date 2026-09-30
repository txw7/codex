use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::PoisonError;

use codex_protocol::ThreadId;

use crate::journal::EncodedTurnFrame;

/// Complete compressed committed history for one loaded thread.
///
/// RESIDENCY-NOTE: These are the exact immutable frame allocations handed to
/// the journal writer. Loaded canonical history therefore has one compressed
/// RAM representation, not a decoded object graph plus a second reserialized
/// cache because memory apparently looked underutilized.
#[derive(Debug, Default)]
pub struct ResidentHistory {
    frames: Vec<EncodedTurnFrame>,
    compressed_bytes: usize,
}

impl ResidentHistory {
    pub fn push(&mut self, frame: EncodedTurnFrame) {
        self.compressed_bytes = self
            .compressed_bytes
            .saturating_add(frame.bytes.len());
        self.frames.push(frame);
    }

    pub fn frames(&self) -> &[EncodedTurnFrame] {
        &self.frames
    }

    pub fn compressed_bytes(&self) -> usize {
        self.compressed_bytes
    }

    pub fn last(&self) -> Option<&EncodedTurnFrame> {
        self.frames.last()
    }
}

#[derive(Debug, Default)]
pub struct ResidentHistories {
    threads: Mutex<HashMap<ThreadId, ResidentHistory>>,
}

impl ResidentHistories {
    pub fn push(&self, thread_id: ThreadId, frame: EncodedTurnFrame) {
        self.threads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(thread_id)
            .or_default()
            .push(frame);
    }

    /// Replace one thread's committed resident history after a verified cold load.
    ///
    /// RESIDENCY-NOTE: replacement is one metadata operation after the full
    /// frame set is validated. Readers do not get to observe a half-hydrated
    /// conversation assembled one frame at a time like a live migration demo.
    pub fn replace(
        &self,
        thread_id: ThreadId,
        frames: impl IntoIterator<Item = EncodedTurnFrame>,
    ) {
        let mut history = ResidentHistory::default();
        for frame in frames {
            history.push(frame);
        }
        self.threads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(thread_id, history);
    }

    pub fn with<R>(
        &self,
        thread_id: ThreadId,
        f: impl FnOnce(Option<&ResidentHistory>) -> R,
    ) -> R {
        let threads = self
            .threads
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        f(threads.get(&thread_id))
    }

    /// Clone frame metadata and Arc handles only.
    ///
    /// The compressed payload remains one allocation shared by the returned
    /// handles. Explicit decode callers can borrow the bytes without inventing
    /// a second resident copy.
    pub fn frames(&self, thread_id: ThreadId) -> Vec<EncodedTurnFrame> {
        self.with(thread_id, |history| {
            history
                .map(|history| history.frames().to_vec())
                .unwrap_or_default()
        })
    }

    pub fn compressed_bytes(&self, thread_id: ThreadId) -> usize {
        self.with(thread_id, |history| {
            history.map(ResidentHistory::compressed_bytes).unwrap_or(0)
        })
    }

    pub fn remove(&self, thread_id: ThreadId) -> Option<ResidentHistory> {
        self.threads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&thread_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::encode_turn_frame;

    #[test]
    fn resident_history_reuses_the_exact_compressed_frame_allocation() {
        let thread_id = ThreadId::new();
        let frame =
            encode_turn_frame(thread_id, "turn-1", 1, None, None, &[]).expect("frame");
        let expected = frame.bytes.len();
        let pointer = frame.bytes.as_ptr();

        let mut history = ResidentHistory::default();
        history.push(frame);

        assert_eq!(history.compressed_bytes(), expected);
        assert_eq!(history.frames().len(), 1);

        // RESIDENCY-NOTE: Metadata was copied. Compressed bytes were not.
        assert_eq!(history.frames()[0].bytes.as_ptr(), pointer);
    }
}
