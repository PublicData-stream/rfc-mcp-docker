//! Byte-preserving offline assets shared by the test suite.

/// Synthetic RFC-like text containing both composed and decomposed Unicode.
pub const HEADINGS: &[u8] = include_bytes!("../../../fixtures/synthetic/headings.txt");
pub const INVALID_UTF8: &[u8] = include_bytes!("../../../fixtures/synthetic/invalid-utf8.bin");
pub const RELATIONS: &str = include_str!("../../../fixtures/synthetic/relations.json");
pub const NOTICES: &str = include_str!("../../../fixtures/synthetic/notices.json");
