use std::error::Error;
use std::fmt;

/// Semantic revision of the durable journal head used to validate a preloaded
/// resident snapshot after live authority is acquired.
///
/// FORK-RAM: Upstream LocalThreadStore currently derives reusable snapshot
/// revisions from filesystem identity and metadata. RamJournal owns an
/// append-only semantic log, so its revision is the durable sequence plus the
/// digest of the committed head.
///
/// Same question, substantially less séance.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ResidentRevisionV1 {
    pub durable_sequence: u64,
    pub durable_head_digest: [u8; 32],
}

impl ResidentRevisionV1 {
    const PREFIX: &'static str = "ram-journal:v1:";

    pub const fn new(durable_sequence: u64, durable_head_digest: [u8; 32]) -> Self {
        Self {
            durable_sequence,
            durable_head_digest,
        }
    }

    /// Encode the revision into the opaque string slot exposed by upstream
    /// ThreadStore snapshot contracts.
    pub fn to_opaque_string(self) -> String {
        let mut out = String::with_capacity(Self::PREFIX.len() + 20 + 1 + 64);
        out.push_str(Self::PREFIX);
        out.push_str(&self.durable_sequence.to_string());
        out.push(':');
        push_hex(&mut out, &self.durable_head_digest);
        out
    }

    /// Parse only RamJournal-owned revision strings.
    ///
    /// COMPAT-NOTE: An unrecognized upstream/local revision must not be treated
    /// as evidence that our resident snapshot is current. Callers should reload
    /// canonical durable state instead of getting creative.
    pub fn parse_opaque(value: &str) -> Result<Self, ResidentRevisionParseError> {
        let rest = value
            .strip_prefix(Self::PREFIX)
            .ok_or(ResidentRevisionParseError::WrongNamespace)?;
        let (sequence, digest) = rest
            .split_once(':')
            .ok_or(ResidentRevisionParseError::Malformed)?;

        let durable_sequence = sequence
            .parse::<u64>()
            .map_err(|_| ResidentRevisionParseError::Malformed)?;
        let durable_head_digest = parse_digest(digest)?;

        Ok(Self {
            durable_sequence,
            durable_head_digest,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResidentRevisionParseError {
    WrongNamespace,
    Malformed,
}

impl fmt::Display for ResidentRevisionParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongNamespace => f.write_str("revision does not belong to RamJournal"),
            Self::Malformed => f.write_str("malformed RamJournal revision"),
        }
    }
}

impl Error for ResidentRevisionParseError {}

fn push_hex(out: &mut String, bytes: &[u8; 32]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
}

fn parse_digest(value: &str) -> Result<[u8; 32], ResidentRevisionParseError> {
    if value.len() != 64 {
        return Err(ResidentRevisionParseError::Malformed);
    }

    let mut digest = [0_u8; 32];
    let bytes = value.as_bytes();
    for (index, slot) in digest.iter_mut().enumerate() {
        let high = decode_nibble(bytes[index * 2])?;
        let low = decode_nibble(bytes[index * 2 + 1])?;
        *slot = (high << 4) | low;
    }
    Ok(digest)
}

fn decode_nibble(value: u8) -> Result<u8, ResidentRevisionParseError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(ResidentRevisionParseError::Malformed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resident_revision_round_trips_through_upstream_opaque_slot() {
        let revision = ResidentRevisionV1::new(42, [0xab; 32]);

        let encoded = revision.to_opaque_string();
        let decoded =
            ResidentRevisionV1::parse_opaque(&encoded).expect("revision should round trip");

        assert_eq!(decoded, revision);
        assert_eq!(
            encoded,
            format!("ram-journal:v1:42:{}", "ab".repeat(32))
        );
    }

    #[test]
    fn foreign_revision_is_not_accidentally_authoritative() {
        let error = ResidentRevisionV1::parse_opaque("local:/tmp/thread.jsonl:123")
            .expect_err("foreign revision must be rejected");

        // FORK-INVARIANT: Unknown revision namespaces force canonical reload.
        // "Looks revision-ish" is not a consistency model.
        assert_eq!(error, ResidentRevisionParseError::WrongNamespace);
    }
}
