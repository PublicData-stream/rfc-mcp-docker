//! Identifiers and original-byte coordinates shared by the RFC services.

use std::fmt;
use std::num::NonZero;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RfcNumber(NonZero<u32>);

impl RfcNumber {
    /// Creates an RFC number in the positive PostgreSQL INTEGER domain.
    pub fn new(value: i32) -> Option<Self> {
        NonZero::new(u32::try_from(value).ok()?).map(Self)
    }

    pub fn get(self) -> u32 {
        self.0.get()
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct GenerationId(u64);

impl GenerationId {
    /// Creates an ID in the nonnegative PostgreSQL BIGINT domain.
    pub fn new(value: i64) -> Option<Self> {
        u64::try_from(value).ok().map(Self)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BlobDigest([u8; 32]);

impl BlobDigest {
    pub fn hash(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_hex(value: &str) -> Option<Self> {
        if value.len() != 64
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return None;
        }
        blake3::Hash::from_hex(value)
            .ok()
            .map(|hash| Self(*hash.as_bytes()))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for BlobDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&blake3::Hash::from_bytes(self.0).to_hex())
    }
}

/// Zero-based half-open range in the original blob.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ByteRange {
    start: u64,
    end: u64,
}

impl ByteRange {
    pub fn new(start: i64, end: i64) -> Option<Self> {
        let start = u64::try_from(start).ok()?;
        let end = u64::try_from(end).ok()?;
        (start <= end).then_some(Self { start, end })
    }

    pub fn start(self) -> u64 {
        self.start
    }

    pub fn end(self) -> u64 {
        self.end
    }

    pub fn fits(self, byte_length: u64) -> bool {
        self.end <= byte_length
    }

    pub fn select(self, bytes: &[u8]) -> Option<&[u8]> {
        let start = usize::try_from(self.start).ok()?;
        let end = usize::try_from(self.end).ok()?;
        bytes.get(start..end)
    }
}

/// An opaque identifier scoped to a particular document version.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SectionId(String);

impl SectionId {
    pub fn new(value: String) -> Option<Self> {
        (!value.is_empty()).then_some(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RelationKind {
    NormativeReference,
    InformativeReference,
    UnclassifiedReference,
    Updates,
    Obsoletes,
}

impl RelationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NormativeReference => "normative_reference",
            Self::InformativeReference => "informative_reference",
            Self::UnclassifiedReference => "unclassified_reference",
            Self::Updates => "updates",
            Self::Obsoletes => "obsoletes",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Relation {
    pub source: RfcNumber,
    pub kind: RelationKind,
    pub target: RfcNumber,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Coverage {
    Complete,
    Partial,
    Unavailable,
}

impl Coverage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Partial => "partial",
            Self::Unavailable => "unavailable",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_and_ranges_validate_database_bounds() {
        assert_eq!(RfcNumber::new(i32::MAX).unwrap().get(), i32::MAX as u32);
        assert_eq!(RfcNumber::new(0), None);
        assert_eq!(RfcNumber::new(-1), None);
        assert_eq!(GenerationId::new(i64::MAX).unwrap().get(), i64::MAX as u64);
        assert_eq!(GenerationId::new(-1), None);
        assert_eq!(ByteRange::new(-1, 1), None);
        assert_eq!(ByteRange::new(2, 1), None);
        assert_eq!(
            ByteRange::new(1, 3).unwrap().select(b"abc"),
            Some(&b"bc"[..])
        );
        assert_eq!(ByteRange::new(3, 4).unwrap().select(b"abc"), None);
        assert_eq!(ByteRange::new(3, 3).unwrap().select(b"abc"), Some(&b""[..]));
        assert_eq!(SectionId::new("".into()), None);
    }

    #[test]
    fn blake3_identity_retains_every_digest_byte() {
        assert_eq!(
            BlobDigest::hash(b"").to_string(),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
        assert_ne!(BlobDigest::hash(b"line\r\n"), BlobDigest::hash(b"line\n"));
    }
}
