# 0002 — Original byte coordinates

Use zero-based half-open ranges in official RFC TXT, with checked Rust and PostgreSQL conversions. Keep CRLF, form feeds, page furniture and every Unicode byte sequence. Return text only when the slice is valid UTF-8. When an explicit byte-oriented result is required, use base64 and the same offsets. LF defines line boundaries while all bytes count toward offsets.

The oracle is equality between the original slice and the decoded response bytes. The source and its offsets remain authoritative for headings, snippets, matches, and notices.
