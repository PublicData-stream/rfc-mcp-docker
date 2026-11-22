use crate::xml::Entry;
use rfc_core::{BlobDigest, Coverage, RfcNumber};
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
}

impl Registry {
    pub(crate) fn load() -> Result<Self> {
        Ok(serde_json::from_str(include_str!("policy.json"))?)
    }

    pub(crate) fn decide(
        &self,
        id: RfcNumber,
        digest: BlobDigest,
        entry: &Entry,
        family: &str,
        extracted: Coverage,
    ) -> &str {
        if let Some(rule) = self.overrides.iter().find(|rule| {
            rule.rfc == id.get()
                && BlobDigest::from_hex(&rule.digest) == Some(digest)
                && !rule.review.trim().is_empty()
        }) {
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
