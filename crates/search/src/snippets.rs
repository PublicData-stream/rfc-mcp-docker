use std::ops::Range;

use rfc_core::{BlobDigest, RfcNumber};
use tokio_postgres::{Row, Transaction};

use crate::{
    Excerpt, NoticeGroup, OriginalRange, SearchError,
    query::{Field, Match, Predicate},
    sql::Plan,
};

pub(crate) const MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TOKEN_SOURCE_BYTES: usize = 256 * 1024;
const MAX_NOTICE_BYTES: usize = 64 * 1024;
const MAX_NOTICES: usize = 128;
const MAX_EXCERPTS: usize = 8;
const START: u8 = 1;
const STOP: u8 = 2;
const HEADLINE: &str = "SELECT pg_catalog.ts_headline('pg_catalog.simple'::regconfig, $1::text,
    pg_catalog.plainto_tsquery('pg_catalog.simple'::regconfig, $2::text),
    'HighlightAll=true,StartSel=\x01,StopSel=\x02')";

pub(crate) struct Source {
    pub rfc: RfcNumber,
    pub title: Option<String>,
    pub stream: Option<String>,
    pub digest: BlobDigest,
    pub length: usize,
    pub url: String,
    version: Vec<u8>,
    encoding: String,
    policy: String,
    notice_state: String,
}

impl Source {
    pub fn from_row(row: &Row) -> Result<Self, SearchError> {
        Ok(Self {
            rfc: RfcNumber::new(row.get(0)).ok_or(SearchError::Unavailable)?,
            title: row.get(1),
            stream: row.get(2),
            digest: BlobDigest::from_bytes(
                row.get::<_, &[u8]>(3)
                    .try_into()
                    .map_err(|_| SearchError::Unavailable)?,
            ),
            length: usize::try_from(row.get::<_, i64>(4)).map_err(|_| SearchError::Unavailable)?,
            encoding: row.get(5),
            policy: row.get(6),
            notice_state: row.get(7),
            url: row.get(8),
            version: row.get(9),
        })
    }

    pub fn permits_excerpt(&self) -> bool {
        self.encoding == "utf8"
            && self.policy == "excerpt_allowed"
            && self.notice_state == "complete"
            && self.stream.is_some()
    }
}

pub(crate) fn fulltext_witnesses<'a>(plan: &'a Plan, witnesses: &[usize]) -> Vec<(Match, &'a str)> {
    let mut literals = Vec::new();
    for &slot in witnesses {
        if let Predicate::Text(Field::Fulltext, mode, literal) = &plan.leaves[slot].predicate
            && !literals.contains(&(*mode, literal.as_str()))
        {
            literals.push((*mode, literal.as_str()));
        }
    }
    literals
}

pub(crate) async fn collect(
    tx: &Transaction<'_>,
    source: &Source,
    bytes: &[u8],
    witnesses: &[(Match, &str)],
) -> Result<Option<(Vec<Excerpt>, NoticeGroup)>, SearchError> {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Ok(None);
    };
    let notices = tx
        .query(
            "SELECT start_byte::bigint,end_byte::bigint,extraction_state::text,review_state
         FROM rfc.notice_ranges WHERE document_version=$1::bytea ORDER BY ordinal LIMIT 129",
            &[&source.version],
        )
        .await?;
    if notices.is_empty() || notices.len() > MAX_NOTICES {
        return Ok(None);
    }
    let mut notice_bytes = 0_usize;
    let mut notice_ranges = Vec::with_capacity(notices.len());
    for row in notices {
        let start = usize::try_from(row.get::<_, i64>(0)).map_err(|_| SearchError::Unavailable)?;
        let end = usize::try_from(row.get::<_, i64>(1)).map_err(|_| SearchError::Unavailable)?;
        if row.get::<_, &str>(2) != "complete" || row.get::<_, &str>(3) == "restricted" {
            return Ok(None);
        }
        let Some(notice) = text.get(start..end) else {
            return Ok(None);
        };
        notice_bytes += notice.len();
        if notice_bytes > MAX_NOTICE_BYTES {
            return Ok(None);
        }
        notice_ranges.push(original(start..end, notice));
    }
    let id = format!("rfc-{}-notices", source.rfc.get());
    let allow_markers =
        bytes.len() <= MAX_TOKEN_SOURCE_BYTES && !bytes.contains(&START) && !bytes.contains(&STOP);
    let mut ranges = Vec::new();
    for &(mode, literal) in witnesses {
        match mode {
            Match::Exact => {
                if let Some(start) = memchr::memmem::find(bytes, literal.as_bytes()) {
                    ranges.push(start..start + literal.len());
                }
            }
            Match::Token if allow_markers => {
                let row = tx.query_one(HEADLINE, &[&text, &literal]).await?;
                if let Some(mut marked) = marked_ranges(bytes, row.get::<_, &str>(0).as_bytes()) {
                    ranges.append(&mut marked);
                }
            }
            Match::Token => {}
        }
    }
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    ranges.dedup();
    let excerpts: Vec<_> = ranges
        .into_iter()
        .take(MAX_EXCERPTS)
        .filter_map(|range| {
            let content = text.get(range.clone())?;
            let match_range = [range.start as u64, range.end as u64];
            Some(Excerpt {
                original: original(range, content),
                encoding: "utf8",
                notice_group_id: id.clone(),
                match_range,
            })
        })
        .collect();
    Ok((!excerpts.is_empty()).then(|| {
        (
            excerpts,
            NoticeGroup {
                id,
                rfc_id: source.rfc.get(),
                source_url: source.url.clone(),
                source_name: "RFC Editor",
                stream: source.stream.clone(),
                attribution: if source.stream.as_deref() == Some("IETF") {
                    "RFC Editor, IETF stream"
                } else {
                    "RFC Editor"
                },
                digest: source.digest.to_string(),
                notices: notice_ranges,
            },
        )
    }))
}

fn original(range: Range<usize>, content: &str) -> OriginalRange {
    OriginalRange {
        start_byte: range.start as u64,
        end_byte: range.end as u64,
        content: content.to_owned(),
    }
}

fn marked_ranges(original: &[u8], marked: &[u8]) -> Option<Vec<Range<usize>>> {
    let mut offset = 0;
    let mut start = None;
    let mut ranges = Vec::new();
    for &byte in marked {
        match byte {
            START if start.is_none() => start = Some(offset),
            STOP => {
                let begin = start.take()?;
                if begin == offset {
                    return None;
                }
                if ranges.len() < MAX_EXCERPTS {
                    ranges.push(begin..offset);
                }
            }
            START => return None,
            byte if original.get(offset) == Some(&byte) => offset += 1,
            _ => return None,
        }
    }
    (offset == original.len() && start.is_none()).then_some(ranges)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_offsets_require_a_lossless_round_trip() {
        assert_eq!(
            marked_ranges(
                "é TLS\r\nTLS".as_bytes(),
                "é \x01TLS\x02\r\n\x01TLS\x02".as_bytes()
            ),
            Some(vec![3..6, 8..11])
        );
        assert_eq!(marked_ranges(b"<tag>TLS", b"\x01TLS\x02"), None);
        assert_eq!(marked_ranges(b"TLS", b"\x01\x01TLS\x02"), None);
        assert_eq!(marked_ranges(b"TLS", b"\x01TLS"), None);
    }
}
