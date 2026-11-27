use std::ops::Range;

use base64::{Engine, engine::general_purpose::STANDARD};
use rfc_core::{BlobDigest, RfcNumber};
use rfc_storage::policy::{
    DENY_EXCERPT, DENY_PAGINATION, DENY_SEARCH, DENY_WHOLE, Grant, MAX_NOTICE_BYTES, Method,
    Policy, RIGHTS_REVISION, Rights,
};
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
    rights: Option<Rights>,
    denied: i32,
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
        let length = usize::try_from(row.get::<_, i64>(4)).map_err(|_| SearchError::Unavailable)?;
        let identity = row.try_get::<_, Option<&[u8]>>("policy_identity")?;
        let metadata = row.try_get::<_, &[u8]>("policy_metadata")?;
        let rights = row
            .try_get::<_, Option<&str>>("policy_evidence")?
            .and_then(|value| serde_json::from_str::<Rights>(value).ok())
            .filter(|rights| {
                rights.valid_for(rfc.get(), digest, metadata, length)
                    && rights
                        .digest()
                        .ok()
                        .as_ref()
                        .map(|digest| digest.as_bytes().as_slice())
                        == identity
            });
        Ok(Self {
            rfc,
            title: row.get(1),
            stream: row.get(2),
            digest,
            length,
            encoding: row.get(5),
            rights,
            denied: row.try_get("policy_denied")?,
            url: row.get(8),
            version: row.get(9),
            verification: SourceVerification::read(
                row,
                "verification_method",
                "verification_algorithm",
            )?,
        })
    }

    pub fn permits_search(&self) -> bool {
        self.denied & DENY_SEARCH == 0 && self.rights.as_ref().is_some_and(|rights| rights.search)
    }

    pub fn permits_excerpt(&self) -> bool {
        self.denied & DENY_EXCERPT == 0
            && self
                .rights
                .as_ref()
                .is_some_and(|rights| !rights.quotes.is_empty())
    }

    pub fn quote(&self, range: &Range<usize>) -> Option<&Grant> {
        if self.denied & DENY_EXCERPT != 0 {
            return None;
        }
        self.rights.as_ref()?.quote(range)
    }

    pub fn cover(&self, range: &Range<usize>) -> Option<Vec<&Grant>> {
        if self.denied & DENY_EXCERPT != 0 {
            return None;
        }
        self.rights.as_ref()?.cover(range)
    }

    pub fn whole(&self) -> Option<bool> {
        let rights = self.rights.as_ref()?;
        if self.denied & DENY_WHOLE != 0 || rights.whole.is_none() {
            return None;
        }
        Some(self.denied & DENY_PAGINATION == 0 && rights.pages)
    }

    pub fn notices(&self, bytes: &[u8], grants: &[&Grant]) -> Option<Vec<OriginalRange>> {
        let mut total = 0;
        let ranges = self
            .rights
            .as_ref()?
            .notices
            .iter()
            .chain(grants.iter().flat_map(|grant| &grant.notices))
            .map(|notice| (notice.start_byte as usize, notice.end_byte as usize))
            .collect::<std::collections::BTreeSet<_>>();
        if ranges.len() > rfc_storage::policy::MAX_NOTICES {
            return None;
        }
        ranges
            .into_iter()
            .map(|(start, end)| {
                let content = bytes.get(start..end)?;
                total += content.len();
                (total <= MAX_NOTICE_BYTES).then(|| original(start..end, content, false))
            })
            .collect()
    }

    pub fn notice_group(
        &self,
        notices: Vec<OriginalRange>,
        grants: &[&Grant],
        whole: Option<bool>,
    ) -> NoticeGroup {
        let mut attribution = self
            .rights
            .as_ref()
            .map(|rights| rights.attribution.clone())
            .unwrap_or_else(|| format!("rfc{}.txt, RFC Editor", self.rfc.get()));
        let mut legends = std::collections::BTreeSet::new();
        for legend in grants.iter().filter_map(|grant| grant.legend.as_deref()) {
            if legends.insert(legend) {
                attribution.push('\n');
                attribution.push_str(legend);
            }
        }
        let license = if whole.is_some() {
            self.rights.as_ref().and_then(|rights| rights.whole.clone())
        } else {
            grants
                .first()
                .filter(|first| grants.iter().all(|grant| grant.license == first.license))
                .map(|grant| grant.license.clone())
        };
        let decision = if !grants.is_empty() {
            Policy::ExcerptAllowed
        } else {
            match whole {
                Some(true) => Policy::WholeOnly,
                Some(false) => Policy::WholePaginated,
                None => Policy::MetadataOnly,
            }
        };
        NoticeGroup {
            id: format!("rfc-{}-notices", self.rfc.get()),
            rfc_id: self.rfc.get(),
            source_url: self.url.clone(),
            source_verification: self.verification.clone(),
            source_name: "RFC Editor",
            stream: self.stream.clone(),
            attribution,
            license,
            policy: self.rights.as_ref().map(|_| crate::engine::NoticePolicy {
                decision,
                approval_method: if decision == Policy::MetadataOnly {
                    Method::Fallback
                } else {
                    Method::Automatic
                },
                rule_id: RIGHTS_REVISION.into(),
            }),
            digest: self.digest.to_string(),
            notices,
            included_in_whole: whole == Some(true),
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

pub(crate) async fn collect(
    tx: &Transaction<'_>,
    source: &Source,
    bytes: &[u8],
    witnesses: &[(Match, &str)],
) -> Result<Option<(Vec<Excerpt>, NoticeGroup)>, SearchError> {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Ok(None);
    };
    let group_id = format!("rfc-{}-notices", source.rfc.get());
    let allow_markers =
        bytes.len() <= MAX_TOKEN_SOURCE_BYTES && !bytes.contains(&START) && !bytes.contains(&STOP);
    let mut ranges = Vec::new();
    for &(mode, literal) in witnesses {
        match mode {
            Match::Exact => {
                if let Some(start) = memchr::memmem::find_iter(bytes, literal.as_bytes())
                    .find(|&start| source.cover(&(start..start + literal.len())).is_some())
                {
                    ranges.push(start..start + literal.len());
                }
            }
            Match::Token if allow_markers => {
                let row = tx.query_one(HEADLINE, &[&text, &literal]).await?;
                if let Some(mut marked) =
                    marked_ranges(bytes, row.get::<_, &str>(0).as_bytes(), |range| {
                        source.cover(range).is_some()
                    })
                {
                    ranges.append(&mut marked);
                }
            }
            Match::Token => {}
        }
    }
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    ranges.dedup();
    let mut grants = Vec::new();
    let excerpts: Vec<_> = ranges
        .into_iter()
        .filter_map(|range| {
            let covered = source.cover(&range)?;
            let content = text.get(range.clone())?;
            let match_range = [range.start as u64, range.end as u64];
            grants.extend(covered);
            Some(Excerpt {
                original: original(range, content.as_bytes(), false),
                encoding: "utf8",
                notice_group_id: group_id.clone(),
                match_range,
            })
        })
        .take(MAX_EXCERPTS)
        .collect();
    if excerpts.is_empty() {
        return Ok(None);
    }
    Ok(source
        .notices(bytes, &grants)
        .map(|notices| (excerpts, source.notice_group(notices, &grants, None))))
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

fn marked_ranges(
    original: &[u8],
    marked: &[u8],
    permits: impl Fn(&Range<usize>) -> bool,
) -> Option<Vec<Range<usize>>> {
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
                if ranges.len() < MAX_EXCERPTS && permits(&(begin..offset)) {
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
                "é \x01TLS\x02\r\n\x01TLS\x02".as_bytes(),
                |_| true,
            ),
            Some(vec![3..6, 8..11])
        );
        assert_eq!(marked_ranges(b"<tag>TLS", b"\x01TLS\x02", |_| true), None);
        assert_eq!(marked_ranges(b"TLS", b"\x01\x01TLS\x02", |_| true), None);
        assert_eq!(marked_ranges(b"TLS", b"\x01TLS", |_| true), None);
    }
}
