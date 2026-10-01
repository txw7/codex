use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use codex_protocol::ThreadId;

use super::EncodedTurnFrame;

#[derive(Debug, thiserror::Error)]
pub enum JournalWriteError {
    #[error("journal I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("short journal append: wrote {written} of {expected} bytes")]
    ShortWrite { written: usize, expected: usize },
}

/// Persistent sink for immutable CJR frames.
///
/// IO-NOTE: this object owns the *only* Phase 02 persistent session-data edge.
/// Directory creation and file open may cause filesystem metadata traffic, but
/// each successful terminal turn submits exactly one positive-length data
/// write followed by sync_data().
///
/// The writer does not retry a short positive write. Retrying would turn
/// "one turn, one append" into "one turn, one append unless the kernel had
/// opinions," which is not a particularly useful invariant.
#[derive(Clone, Debug)]
pub struct JournalWriter {
    root: Arc<PathBuf>,
}

impl JournalWriter {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root: Arc::new(root),
        }
    }

    pub fn root(&self) -> &Path {
        self.root.as_ref()
    }

    pub fn append_turn_frame(
        &self,
        thread_id: ThreadId,
        frame: &EncodedTurnFrame,
    ) -> Result<(), JournalWriteError> {
        let path = self.thread_path(thread_id);
        let parent = path.parent().expect("journal thread path should have a parent");
        std::fs::create_dir_all(parent)?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .open(path)?;

        write_frame_once(&mut file, &frame.bytes)?;
        file.sync_data()?;
        Ok(())
    }

    pub fn thread_path(&self, thread_id: ThreadId) -> PathBuf {
        super::journal_thread_path(self.root(), thread_id)
    }

    /// Re-run the durability fence for an already complete journal tail.
    ///
    /// JOURNAL-NOTE: A prior append can write the full frame and then fail
    /// during sync_data(). Recovery may therefore prove that the exact frame is
    /// already present. In that case retrying the data append would duplicate
    /// history; sync the existing bytes instead.
    pub fn sync_thread(&self, thread_id: ThreadId) -> Result<(), JournalWriteError> {
        let path = self.thread_path(thread_id);
        let file = OpenOptions::new().write(true).open(path)?;
        file.sync_data()?;
        Ok(())
    }

    /// Truncate an incomplete terminal tail to the last verified frame boundary.
    ///
    /// JOURNAL-NOTE: callers may use this only after JournalReader proved the
    /// complete prefix. This is tail cleanup, not a general-purpose history
    /// editing API. Canonical journals are append-only; corruption does not get
    /// a convenient rewrite function because that would be extremely tempting.
    pub fn truncate_to_verified_prefix(
        &self,
        thread_id: ThreadId,
        valid_bytes: usize,
    ) -> Result<(), JournalWriteError> {
        let path = self.thread_path(thread_id);
        let file = OpenOptions::new().write(true).open(path)?;
        file.set_len(u64::try_from(valid_bytes).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "verified journal length does not fit in u64",
            )
        })?)?;
        file.sync_data()?;
        Ok(())
    }
}

fn write_frame_once(
    writer: &mut impl Write,
    bytes: &[u8],
) -> Result<(), JournalWriteError> {
    let written = writer.write(bytes)?;
    if written != bytes.len() {
        return Err(JournalWriteError::ShortWrite {
            written,
            expected: bytes.len(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::encode_turn_frame;

    #[derive(Default)]
    struct CountingWriter {
        calls: usize,
        bytes: Vec<u8>,
        short_by: usize,
    }

    impl Write for CountingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.calls += 1;
            let accepted = buf.len().saturating_sub(self.short_by);
            self.bytes.extend_from_slice(&buf[..accepted]);
            Ok(accepted)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn one_frame_uses_one_application_write_call() {
        let frame =
            encode_turn_frame(ThreadId::new(), "turn-1", 1, None, None, &[]).expect("frame should encode");
        let mut writer = CountingWriter::default();

        write_frame_once(&mut writer, &frame.bytes).expect("write should succeed");

        // IO-NOTE: If this becomes 2, somebody has changed the measured
        // persistence contract. Explain why before teaching the test humility.
        assert_eq!(writer.calls, 1);
        assert_eq!(writer.bytes, frame.bytes);
    }

    #[test]
    fn short_write_faults_instead_of_looping() {
        let frame =
            encode_turn_frame(ThreadId::new(), "turn-1", 1, None, None, &[]).expect("frame should encode");
        let mut writer = CountingWriter {
            short_by: 1,
            ..Default::default()
        };

        let err = write_frame_once(&mut writer, &frame.bytes)
            .expect_err("short write must fault the commit");

        assert!(matches!(err, JournalWriteError::ShortWrite { .. }));
        assert_eq!(writer.calls, 1);
    }

    #[test]
    fn append_creates_one_thread_journal_and_preserves_frame_bytes() {
        let temp = tempfile::tempdir().expect("tempdir");
        let writer = JournalWriter::new(temp.path().to_path_buf());
        let thread_id = ThreadId::new();
        let frame =
            encode_turn_frame(thread_id, "turn-1", 1, None, None, &[]).expect("frame should encode");

        writer
            .append_turn_frame(thread_id, &frame)
            .expect("journal append should succeed");

        let bytes = std::fs::read(writer.thread_path(thread_id)).expect("read journal");
        assert_eq!(bytes, frame.bytes);
    }
}
