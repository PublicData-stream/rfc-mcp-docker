use std::ops::Range;

use base64::{Engine, engine::general_purpose::STANDARD};
use rfc_core::{BlobDigest, RfcNumber};
use rfc_storage::policy::{MAX_NOTICE_BYTES, MAX_NOTICES, Policy, Receipt};
use serde::Serialize;
use serde_json::{Value, json};
use tokio_postgres::{Row, Transaction};

use crate::{
    Excerpt, NoticeGroup, OriginalRange, SearchError,
    engine::Representation,
    query::{Field, Match, Predicate},
    sql::Plan,
};

pub(crate) const MAX_SOURCE_BYTES: usize = 4 * 1024 * 1024;
const MAX_TOKEN_SOURCE_BYTES: usize = 256 * 1024;
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
    pub version: Vec<u8>,
    pub encoding: String,
    pub policy: Policy,
    receipt: Option<Receipt>,
    pub notice_state: String,
    pub verification: Option<SourceVerification>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SourceVerification {
    method: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    algorithm: Option<String>,
}

impl SourceVerification {
    pub(crate) fn read(
        row: &Row,
        method: &str,
        algorithm: &str,
    ) -> Result<Option<Self>, SearchError> {
        let method = match row.try_get::<_, Option<&str>>(method)? {
            Some("rsync_checksum_match") => "rsync_checksum_match",
            Some("dual_source_byte_match") => "dual_source_byte_match",
            Some("dual_source") => "dual_source",
            Some("legacy_https_verified") => "legacy_https_verified",
            Some("rsync_integrity") => "rsync_integrity",
            Some("https_capture") => "https_capture",
            Some("unclassified") | None => return Ok(None),
            _ => return Err(SearchError::Unavailable),
        };
        Ok(Some(Self {
            method,
            algorithm: row.try_get(algorithm)?,
        }))
    }

    pub(crate) fn add(value: &mut Value, verification: Option<&Self>) {
        if let Some(verification) = verification {
            value["source_verification"] = json!(verification);
        }
    }
}

impl Source {
    pub fn from_row(row: &Row) -> Result<Self, SearchError> {
        let rfc = RfcNumber::new(row.get(0)).ok_or(SearchError::Unavailable)?;
        let digest = BlobDigest::from_bytes(
            row.get::<_, &[u8]>(3)
                .try_into()
                .map_err(|_| SearchError::Unavailable)?,
        );
        let base = Policy::parse(row.get::<_, &str>(6)).unwrap_or(Policy::MetadataOnly);
        let receipt = match (
            row.try_get::<_, Option<&[u8]>>("policy_identity")?,
            row.try_get::<_, Option<&str>>("policy_evidence")?,
        ) {
            (None, None) => None,
            (Some(identity), Some(evidence)) => {
                let receipt: Receipt =
                    serde_json::from_str(evidence).map_err(|_| SearchError::Unavailable)?;
                if receipt
                    .digest()
                    .map_err(|_| SearchError::Unavailable)?
                    .as_bytes()
                    .as_slice()
                    != identity
                    || receipt.rfc != rfc.get()
                    || receipt.source_digest != digest.to_string()
                    || receipt.decision != base
                {
                    return Err(SearchError::Unavailable);
                }
                Some(receipt)
            }
            _ => return Err(SearchError::Unavailable),
        };
        let ceiling = row
            .try_get::<_, Option<&str>>("policy_ceiling")?
            .map_or(Ok(Policy::ExcerptAllowed), |cap| {
                Policy::parse(cap).ok_or(SearchError::Unavailable)
            })?;
        let policy = if base == Policy::ExcerptAllowed
            && !receipt.as_ref().is_some_and(Receipt::eligible_for_excerpt)
        {
            Policy::MetadataOnly
        } else {
            base.min(ceiling)
        };
        Ok(Self {
            rfc,
            title: row.get(1),
            stream: row.get(2),
            digest,
            length: usize::try_from(row.get::<_, i64>(4)).map_err(|_| SearchError::Unavailable)?,
            encoding: row.get(5),
            policy,
            receipt,
            notice_state: row.get(7),
            url: row.get(8),
            version: row.get(9),
            verification: SourceVerification::read(
                row,
                "verification_method",
                "verification_algorithm",
            )?,
        })
    }

    pub fn permits_excerpt(&self) -> bool {
        self.policy == Policy::ExcerptAllowed
            && self.notice_state == "complete"
            && self.stream.is_some()
    }

    pub fn notice_group(
        &self,
        notices: Vec<OriginalRange>,
        included_in_whole: bool,
    ) -> NoticeGroup {
        NoticeGroup {
            id: format!("rfc-{}-notices", self.rfc.get()),
            rfc_id: self.rfc.get(),
            source_url: self.url.clone(),
            source_verification: self.verification.clone(),
            source_name: "RFC Editor",
            stream: self.stream.clone(),
            attribution: self
                .receipt
                .as_ref()
                .and_then(|receipt| receipt.attribution.clone())
                .unwrap_or_else(|| {
                    if self.stream.as_deref() == Some("IETF") {
                        "RFC Editor, IETF stream".into()
                    } else {
                        "RFC Editor".into()
                    }
                }),
            license: self
                .receipt
                .as_ref()
                .and_then(|receipt| receipt.license.clone()),
            policy: self
                .receipt
                .as_ref()
                .map(|receipt| crate::engine::NoticePolicy {
                    decision: self.policy,
                    approval_method: receipt.approval_method,
                    rule_id: receipt.rule_id.clone(),
                }),
            digest: self.digest.to_string(),
            notices,
            included_in_whole,
        }
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

pub(crate) async fn notices(
    tx: &Transaction<'_>,
    source: &Source,
    bytes: &[u8],
    require_review: bool,
) -> Result<Option<Vec<OriginalRange>>, SearchError> {
    let notices = tx
        .query(
            "SELECT start_byte::bigint,end_byte::bigint,extraction_state::text,review_state,category
         FROM rfc.notice_ranges WHERE document_version=$1::bytea ORDER BY ordinal LIMIT 129",
            &[&source.version],
        )
        .await?;
    if notices.is_empty() || notices.len() > MAX_NOTICES {
        return Ok(None);
    }
    if source
        .receipt
        .as_ref()
        .is_some_and(|receipt| receipt.notices.len() != notices.len())
    {
        return Ok(None);
    }
    let mut notice_bytes = 0_usize;
    let mut notice_ranges = Vec::with_capacity(notices.len());
    for (ordinal, row) in notices.into_iter().enumerate() {
        if source.receipt.as_ref().is_some_and(|receipt| {
            let span = &receipt.notices[ordinal];
            (span.start_byte, span.end_byte, span.category.as_str())
                != (row.get(0), row.get(1), row.get(4))
        }) {
            return Ok(None);
        }
        let start = usize::try_from(row.get::<_, i64>(0)).map_err(|_| SearchError::Unavailable)?;
        let end = usize::try_from(row.get::<_, i64>(1)).map_err(|_| SearchError::Unavailable)?;
        let review = row.get::<_, &str>(3);
        if row.get::<_, &str>(2) != "complete"
            || review == "restricted"
            || (require_review && review != "reviewed")
        {
            return Ok(None);
        }
        let Some(notice) = bytes.get(start..end) else {
            return Ok(None);
        };
        notice_bytes += notice.len();
        if notice_bytes > MAX_NOTICE_BYTES {
            return Ok(None);
        }
        notice_ranges.push(original(start..end, notice, false));
    }
    Ok(Some(notice_ranges))
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
    let Some(notices) = notices(tx, source, bytes, false).await? else {
        return Ok(None);
    };
    let group = source.notice_group(notices, false);
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
                original: original(range, content.as_bytes(), false),
                encoding: "utf8",
                notice_group_id: group.id.clone(),
                match_range,
            })
        })
        .collect();
    Ok((!excerpts.is_empty()).then_some((excerpts, group)))
}

pub(crate) fn original(range: Range<usize>, bytes: &[u8], base64: bool) -> OriginalRange {
    let representation = match std::str::from_utf8(bytes) {
        Ok(content) if !base64 => Representation::Text {
            content: content.to_owned(),
        },
        _ => Representation::Base64 {
            content_base64: STANDARD.encode(bytes),
        },
    };
    OriginalRange {
        start_byte: range.start as u64,
        end_byte: range.end as u64,
        representation,
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
