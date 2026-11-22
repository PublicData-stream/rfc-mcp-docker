use crate::{Notice, xml::Entry};
use rfc_core::{BlobDigest, ByteRange, Coverage, RfcNumber};
use rfc_storage::postgres::Result;
use serde::Deserialize;

#[derive(Deserialize)]
pub(crate) struct Registry {
    pub version: String,
    families: Vec<Family>,
    overrides: Vec<Override>,
}

#[derive(Deserialize)]
struct Family {
    period: String,
    stream: String,
    family: String,
    policy: String,
}

#[derive(Deserialize)]
struct Override {
    rfc: u32,
    digest: String,
    policy: String,
    review: String,
    #[serde(default)]
    notices: Vec<ReviewedNotice>,
}

#[derive(Deserialize)]
struct ReviewedNotice {
    category: String,
    start_byte: i64,
    end_byte: i64,
}

impl Registry {
    pub(crate) fn load() -> Result<Self> {
        Ok(serde_json::from_str(include_str!("policy.json"))?)
    }

    fn reviewed_override(&self, id: RfcNumber, digest: BlobDigest) -> Option<&Override> {
        self.overrides.iter().find(|rule| {
            rule.rfc == id.get()
                && BlobDigest::from_hex(&rule.digest) == Some(digest)
                && !rule.review.trim().is_empty()
        })
    }

    pub(crate) fn pagination_notices(
        &self,
        id: RfcNumber,
        digest: BlobDigest,
        bytes: &[u8],
    ) -> Result<Option<Vec<Notice<'_>>>> {
        let Some(rule) = self
            .reviewed_override(id, digest)
            .filter(|rule| rule.policy == "whole_paginated")
        else {
            return Ok(None);
        };
        if rule.notices.is_empty() || rule.notices.len() > 128 {
            return Err("reviewed pagination requires a complete bounded notice inventory".into());
        }
        let mut previous_end = 0;
        let mut total = 0;
        let mut notices = Vec::with_capacity(rule.notices.len());
        for (ordinal, notice) in rule.notices.iter().enumerate() {
            let range = ByteRange::new(notice.start_byte, notice.end_byte)
                .filter(|range| range.start() >= previous_end && range.start() < range.end())
                .ok_or("invalid or overlapping reviewed notice range")?;
            let span = range
                .select(bytes)
                .ok_or("reviewed notice exceeds the original")?;
            total += span.len();
            if total > 64 * 1024
                || !matches!(
                    notice.category.as_str(),
                    "copyright" | "license" | "authorship" | "disclaimer" | "other"
                )
            {
                return Err("invalid or oversized reviewed notice inventory".into());
            }
            previous_end = range.end();
            notices.push(Notice {
                ordinal: i32::try_from(ordinal)?,
                category: &notice.category,
                range,
                extraction: Coverage::Complete,
                review: "reviewed",
            });
        }
        Ok(Some(notices))
    }

    pub(crate) fn decide(
        &self,
        id: RfcNumber,
        digest: BlobDigest,
        entry: &Entry,
        family: &str,
        extracted: Coverage,
    ) -> &str {
        if let Some(rule) = self.reviewed_override(id, digest) {
            match rule.policy.as_str() {
                "whole_only" => return &rule.policy,
                "excerpt_allowed" if extracted == Coverage::Complete => return &rule.policy,
                _ => {}
            }
        }
        let period = match (entry.date.as_deref(), entry.month.as_deref()) {
            (Some(date), _) if date < "2015-03-25" => "before-2015-03-25",
            (Some(date), _) if date > "2015-03-25" => "after-2015-03-25",
            (_, Some(month)) if month < "2015-03-01" => "before-2015-03-25",
            (_, Some(month)) if month > "2015-03-01" => "after-2015-03-25",
            _ => "unknown",
        };
        self.families
            .iter()
            .find(|rule| {
                (rule.period == "any" || rule.period == period)
                    && (rule.stream == "any"
                        || Some(rule.stream.as_str()) == entry.stream.as_deref())
                    && (rule.family == "any" || rule.family == family)
                    && rule.policy == "metadata_only"
            })
            .map_or("metadata_only", |rule| rule.policy.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reviewed_pagination_is_digest_bound_and_validates_original_spans() {
        let bytes = b"Copyright\r\n\xff body";
        let digest = BlobDigest::hash(bytes);
        let id = RfcNumber::new(102).unwrap();
        let mut registry: Registry = serde_json::from_value(serde_json::json!({
            "version": "fixture",
            "families": [],
            "overrides": [{
                "rfc": 102, "digest": digest.to_string(), "policy": "whole_paginated",
                "review": "Synthetic fixture review",
                "notices": [{"category": "copyright", "start_byte": 0, "end_byte": 11}]
            }]
        }))
        .unwrap();
        let notices = registry
            .pagination_notices(id, digest, bytes)
            .unwrap()
            .unwrap();
        assert_eq!(
            notices[0].range.select(bytes),
            Some(b"Copyright\r\n".as_slice())
        );
        assert_eq!(notices[0].review, "reviewed");
        assert!(
            registry
                .pagination_notices(id, BlobDigest::hash(b"changed"), bytes)
                .unwrap()
                .is_none()
        );
        registry.overrides[0].notices[0].end_byte = 99;
        assert!(registry.pagination_notices(id, digest, bytes).is_err());
    }

    #[test]
    fn fixture_reviews_bind_to_the_original_digest() {
        let registry = Registry::load().unwrap();
        for (id, bytes) in [
            (
                2119,
                include_bytes!("../../../fixtures/rfc/rfc2119.txt").as_slice(),
            ),
            (
                8174,
                include_bytes!("../../../fixtures/rfc/rfc8174.txt").as_slice(),
            ),
        ] {
            let entry = Entry {
                title: String::new(),
                stream: Some("IETF".into()),
                date: None,
                month: None,
                status: None,
                has_txt: true,
            };
            let id = RfcNumber::new(id).unwrap();
            assert_eq!(
                registry.decide(
                    id,
                    BlobDigest::hash(bytes),
                    &entry,
                    "unknown",
                    Coverage::Unavailable
                ),
                "whole_only"
            );
            assert_eq!(
                registry.decide(
                    id,
                    BlobDigest::hash(b"changed"),
                    &entry,
                    "ietf-trust",
                    Coverage::Complete
                ),
                "metadata_only"
            );
        }
    }
}
