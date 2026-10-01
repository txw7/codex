use std::path::Path;
use std::path::PathBuf;

use codex_protocol::ThreadId;

mod format;
mod reader;
mod writer;

pub use format::DecodedTurnFrame;
pub use format::EncodedTurnFrame;
pub use format::JournalFormatError;
pub use format::decode_turn_frame;
pub use format::encode_turn_frame;
pub use reader::JournalReadError;
pub use reader::JournalReader;
pub use reader::RecoveredJournal;
pub use reader::recover_bytes;
pub use writer::JournalWriteError;
pub use writer::JournalWriter;

pub(crate) fn journal_thread_path(root: &Path, thread_id: ThreadId) -> PathBuf {
    let id = thread_id.to_string();
    let prefix = id.get(..2).unwrap_or("00");
    root.join("v1").join(prefix).join(format!("{id}.cjr"))
}
