//! Reviewed source grants and deterministic decision receipts.

mod templates;

use crate::{Notice, xml::Entry};
use rfc_core::{BlobDigest, ByteRange, Coverage, RfcNumber};
use rfc_storage::{
    manifest::identity,
    policy::{CLASSIFIER, License, MAX_NOTICE_BYTES, MAX_NOTICES, Method, Policy, Receipt, Span},
    postgres::Result,
};
use serde::Deserialize;
use std::collections::BTreeSet;

const REGISTRY: &str = include_str!("policy.json");
const TEMPLATES: &str = include_str!("policy/templates.rs");
const CLASSIFIER_SOURCE: &str = include_str!("policy.rs");

pub(crate) struct Registry {
    data: Rules,
    pub digest: BlobDigest,
    pub approved: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rules {
    schema_version: u8,
    version: String,
    rules: Vec<Rule>,
    overrides: Vec<Override>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rule {
    id: String,
    stream: String,
    from: Option<String>,
    until: Option<String>,
    license: Option<License>,
    template: Option<String>,
    decision: Policy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Override {
    rfc: u32,
    digest: String,
    policy: Policy,
    review: String,
    #[serde(default)]
    fallback: bool,
    #[serde(default)]
    notices: Vec<Span>,
    license: Option<License>,
    attribution: Option<String>,
}

impl Registry {
    pub(crate) fn load() -> Result<Self> {
        Self::parse(REGISTRY)
    }

    #[cfg(feature = "phase3-fixtures")]
    pub(crate) fn fixture(json: &str) -> Result<Self> {
        Self::parse(json)
    }

    fn parse(json: &str) -> Result<Self> {
        let data: Rules = serde_json::from_str(json)?;
        let mut ids = BTreeSet::new();
        let mut overrides = BTreeSet::new();
        if data.schema_version != 2
            || data.version.is_empty()
            || data.rules.iter().any(|rule| {
                rule.id.is_empty()
                    || !ids.insert(&rule.id)
                    || rule.stream.is_empty()
                    || rule.from.as_deref().is_some_and(|date| !valid_date(date))
                    || rule.until.as_deref().is_some_and(|date| !valid_date(date))
                    || rule
                        .from
                        .as_ref()
                        .zip(rule.until.as_ref())
                        .is_some_and(|(a, b)| a >= b)
                    || match rule.decision {
                        Policy::MetadataOnly => rule.template.is_some() || rule.license.is_some(),
                        Policy::ExcerptAllowed => {
                            rule.template.as_deref() != Some(templates::TLP5)
                                || rule
                                    .license
                                    .as_ref()
                                    .is_none_or(|license| !valid_license(license))
                        }
                        _ => true,
                    }
            })
            || data.rules.iter().enumerate().any(|(i, rule)| {
                data.rules[..i].iter().any(|other| {
                    rule.stream == other.stream
                        && overlaps(rule.from.as_deref(), rule.until.as_deref(), other)
                })
            })
            || data.overrides.iter().any(|rule| {
                rule.rfc == 0
                    || BlobDigest::from_hex(&rule.digest).is_none()
                    || !overrides.insert((rule.rfc, &rule.digest))
                    || rule.review.trim().is_empty()
                    || (rule.fallback && rule.policy != Policy::WholeOnly)
                    || (rule.policy == Policy::ExcerptAllowed
                        && (rule
                            .license
                            .as_ref()
                            .is_none_or(|license| !valid_license(license))
                            || rule.attribution.as_deref().is_none_or(str::is_empty)))
            })
        {
            return Err("invalid source policy registry".into());
        }
        Ok(Self {
            data,
            digest: identity(
                "rfc-policy-bundle-v1",
                &(CLASSIFIER, json, TEMPLATES, CLASSIFIER_SOURCE),
            )?,
            approved: false,
        })
    }

    pub(crate) fn classify(
        &self,
        rfc: RfcNumber,
        digest: BlobDigest,
        entry: &Entry,
        bytes: &[u8],
        prospective: bool,
    ) -> Result<Receipt> {
        let mut receipt = Receipt {
            rfc: rfc.get(),
            source_digest: digest.to_string(),
            metadata_digest: metadata_digest(entry)?.to_string(),
            registry_digest: self.digest.to_string(),
            classifier: CLASSIFIER.into(),
            rule_id: "unknown".into(),
            template: None,
            license: None,
            decision: Policy::MetadataOnly,
            approval_method: Method::Fallback,
            review: None,
            reason: Some("unknown_license_family".into()),
            attribution: None,
            notices: Vec::new(),
            inventory_digest: String::new(),
        };
        self.decide(&mut receipt, entry, bytes, prospective);
        receipt.inventory_digest = Receipt::inventory_digest(&receipt.notices)?.to_string();
        Ok(receipt)
    }

    fn decide(&self, receipt: &mut Receipt, entry: &Entry, bytes: &[u8], prospective: bool) {
        let manual = self
            .data
            .overrides
            .iter()
            .find(|rule| rule.rfc == receipt.rfc && rule.digest == receipt.source_digest);
        let text = std::str::from_utf8(bytes)
            .ok()
            .filter(|text| !text.contains('\0'));
        let restrictions = text.and_then(templates::restriction);
        // These reviews expressly cover whole unmodified delivery for the exact digest.
        if let Some(rule) = manual.filter(|rule| !rule.fallback) {
            if restrictions.is_none() || rule.policy == Policy::MetadataOnly {
                apply_manual(receipt, rule, bytes);
            } else {
                receipt.reason = restrictions.map(str::to_owned);
            }
            return;
        }
        let result = if let Some(reason) = restrictions {
            Err(reason)
        } else {
            self.automatic(receipt, entry, text, prospective)
        };
        if let Err(reason) = result {
            if let Some(rule) = manual.filter(|_| restrictions.is_none()) {
                apply_manual(receipt, rule, bytes);
            }
            receipt.reason = Some(reason.into());
        }
    }

    fn automatic(
        &self,
        receipt: &mut Receipt,
        entry: &Entry,
        text: Option<&str>,
        prospective: bool,
    ) -> std::result::Result<(), &'static str> {
        let stream = entry.stream.as_deref().ok_or("unknown_stream")?;
        let start = entry
            .date
            .as_deref()
            .or(entry.month.as_deref())
            .ok_or("ambiguous_publication_date")?;
        let end = if entry.date.is_some() {
            start.to_owned()
        } else {
            format!("{}-{}", &start[..7], month_days(start))
        };
        let mut matches = self.data.rules.iter().filter(|rule| {
            rule.stream == stream
                && overlaps(Some(start), None, rule)
                && rule.from.as_deref().is_none_or(|date| date <= end.as_str())
        });
        let rule = matches.next().ok_or("unknown_license_family")?;
        if matches.next().is_some() {
            return Err("ambiguous_publication_date");
        }
        receipt.rule_id.clone_from(&rule.id);
        receipt.template.clone_from(&rule.template);
        receipt.license.clone_from(&rule.license);
        if rule.decision == Policy::MetadataOnly {
            return Err("manual_review_required");
        }
        let text = text.ok_or("unsupported_source_encoding")?;
        let (notices, attribution) = templates::verify_tlp5(text, receipt.rfc, entry, start)?;
        if !inventory_valid(&notices, text.as_bytes(), true) {
            return Err("notice_budget_exceeded");
        }
        if !self.approved && !prospective {
            return Err("registry_unapproved");
        }
        receipt.decision = rule.decision;
        receipt.approval_method = Method::Automatic;
        receipt.notices = notices;
        receipt.attribution = Some(attribution);
        receipt.reason = None;
        Ok(())
    }
}

fn apply_manual(receipt: &mut Receipt, rule: &Override, bytes: &[u8]) {
    if matches!(rule.policy, Policy::WholePaginated | Policy::ExcerptAllowed)
        && !inventory_valid(&rule.notices, bytes, rule.policy == Policy::ExcerptAllowed)
    {
        receipt.reason = Some("incomplete_notice_inventory".into());
        return;
    }
    receipt.rule_id = format!("manual-rfc-{}", rule.rfc);
    receipt.approval_method = Method::Manual;
    receipt.review = Some(rule.review.clone());
    receipt.decision = rule.policy;
    receipt.template = None;
    receipt.license.clone_from(&rule.license);
    receipt.attribution.clone_from(&rule.attribution);
    receipt.notices.clone_from(&rule.notices);
    receipt.reason = None;
}

pub(crate) fn metadata_digest(entry: &Entry) -> Result<BlobDigest> {
    Ok(identity(
        "rfc-policy-metadata-v1",
        &(&entry.stream, &entry.date, &entry.month, &entry.authors),
    )?)
}

fn valid_license(license: &License) -> bool {
    !license.family.is_empty()
        && !license.version.is_empty()
        && !license.section.is_empty()
        && license.url.starts_with("https://")
}

fn overlaps(from: Option<&str>, until: Option<&str>, rule: &Rule) -> bool {
    from.zip(rule.until.as_deref()).is_none_or(|(a, b)| a < b)
        && rule.from.as_deref().zip(until).is_none_or(|(a, b)| a < b)
}

fn month_days(date: &str) -> u8 {
    let year = date[..4].parse::<u16>().unwrap_or_default();
    match &date[5..7] {
        "02" if year.is_multiple_of(4)
            && (!year.is_multiple_of(100) || year.is_multiple_of(400)) =>
        {
            29
        }
        "02" => 28,
        "04" | "06" | "09" | "11" => 30,
        _ => 31,
    }
}

fn valid_date(date: &str) -> bool {
    let bytes = date.as_bytes();
    bytes.len() == 10
        && bytes.iter().enumerate().all(|(i, byte)| {
            if i == 4 || i == 7 {
                *byte == b'-'
            } else {
                byte.is_ascii_digit()
            }
        })
        && ("01"..="12").contains(&&date[5..7])
        && date[8..]
            .parse::<u8>()
            .is_ok_and(|day| (1..=month_days(date)).contains(&day))
}

fn inventory_valid(spans: &[Span], bytes: &[u8], utf8: bool) -> bool {
    if spans.is_empty() || spans.len() > MAX_NOTICES {
        return false;
    }
    let mut end = 0;
    let mut total = 0;
    for span in spans {
        let Some(range) = ByteRange::new(span.start_byte, span.end_byte)
            .filter(|range| range.start() >= end && range.start() < range.end())
            .and_then(|range| range.select(bytes).map(|bytes| (range, bytes)))
        else {
            return false;
        };
        if !matches!(
            span.category.as_str(),
            "copyright"
                | "license"
                | "authorship"
                | "disclaimer"
                | "legend"
                | "restriction"
                | "third_party"
                | "other"
        ) || (utf8 && std::str::from_utf8(range.1).is_err())
        {
            return false;
        }
        end = range.0.end();
        total += range.1.len();
    }
    total <= MAX_NOTICE_BYTES
}

pub(crate) fn notice_rows(receipt: &Receipt) -> Vec<Notice<'_>> {
    receipt
        .notices
        .iter()
        .enumerate()
        .map(|(ordinal, span)| Notice {
            ordinal: ordinal as i32,
            category: &span.category,
            range: ByteRange::new(span.start_byte, span.end_byte).expect("validated notice"),
            extraction: Coverage::Complete,
            review: "reviewed",
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const RFC_8174: &[u8] = include_bytes!("../../../fixtures/rfc/rfc8174.txt");
    const RFC_2119: &[u8] = include_bytes!("../../../fixtures/rfc/rfc2119.txt");

    fn entry() -> Entry {
        Entry {
            title: "Ambiguity of Uppercase vs Lowercase in RFC 2119 Key Words".into(),
            stream: Some("IETF".into()),
            date: None,
            month: Some("2017-05-01".into()),
            status: None,
            has_txt: true,
            authors: vec!["B. Leiba".into()],
        }
    }

    #[test]
    fn official_excerpt_requires_the_reviewed_template_and_pin() {
        let registry = Registry::load().unwrap();
        let rfc = RfcNumber::new(8174).unwrap();
        let digest = BlobDigest::hash(RFC_8174);
        let entry = entry();
        let active = registry
            .classify(rfc, digest, &entry, RFC_8174, false)
            .unwrap();
        assert_eq!(active.decision, Policy::WholeOnly);
        let proposed = registry
            .classify(rfc, digest, &entry, RFC_8174, true)
            .unwrap();
        assert!(proposed.eligible_for_excerpt(), "{proposed:?}");
        assert_eq!(
            proposed.attribution.as_deref(),
            Some("IETF, RFC 8174, Barry Leiba")
        );
        assert!(inventory_valid(&proposed.notices, RFC_8174, true));
        assert_eq!(
            proposed,
            registry
                .classify(rfc, digest, &entry, RFC_8174, true)
                .unwrap()
        );
        let whole = registry
            .classify(
                RfcNumber::new(2119).unwrap(),
                BlobDigest::hash(RFC_2119),
                &entry,
                RFC_2119,
                true,
            )
            .unwrap();
        assert_eq!(whole.decision, Policy::WholeOnly);
    }

    #[test]
    fn original_contributor_and_address_blocks_survive_crlf() {
        let registry = Registry::load().unwrap();
        let rfc = RfcNumber::new(8174).unwrap();
        let amended = std::str::from_utf8(RFC_8174).unwrap().replace(
            "\nAuthor's Address\n",
            "\nContributors\n\n   Élan Example contributed to this document.\n\nAuthor's Address\n",
        ).replace('\n', "\r\n");
        let receipt = registry
            .classify(
                rfc,
                BlobDigest::hash(amended.as_bytes()),
                &entry(),
                amended.as_bytes(),
                true,
            )
            .unwrap();
        assert!(receipt.eligible_for_excerpt(), "{receipt:?}");
        let original: Vec<_> = receipt
            .notices
            .iter()
            .filter(|span| span.category == "authorship")
            .map(|span| &amended[span.start_byte as usize..span.end_byte as usize])
            .collect();
        assert!(original.iter().any(
            |notice| notice.starts_with("Contributors\r\n") && notice.contains("Élan Example")
        ));
        assert!(original.iter().any(|notice| notice.starts_with("Author's Address\r\n")
            && notice.contains("URI:")));
    }

    #[test]
    fn evidence_changes_and_unsupported_terms_fail_closed() {
        let registry = Registry::load().unwrap();
        let rfc = RfcNumber::new(8174).unwrap();
        let source = std::str::from_utf8(RFC_8174).unwrap();
        let entry = entry();
        let digest = BlobDigest::hash(RFC_8174);
        for (changed, reason) in [
            (
                source.replace("Copyright Notice", "Technical discussion of Copyright"),
                "incomplete_notice_inventory",
            ),
            (
                source.replace("All rights reserved.", "All rights may be waived."),
                "unknown_license_template",
            ),
            (
                format!("{source}\nReproduced with permission of a third party.\n"),
                "unresolved_third_party_rights",
            ),
            (
                format!("{source}\nThis may contain Pre-5378 material.\n"),
                "unresolved_pre5378_material",
            ),
        ] {
            let receipt = registry
                .classify(
                    rfc,
                    BlobDigest::hash(changed.as_bytes()),
                    &entry,
                    changed.as_bytes(),
                    true,
                )
                .unwrap();
            assert_eq!(receipt.decision, Policy::MetadataOnly);
            assert_eq!(receipt.reason.as_deref(), Some(reason));
            assert_ne!(receipt.source_digest, digest.to_string());
        }
        let mut ambiguous = entry;
        ambiguous.month = Some("2015-03-01".into());
        let receipt = registry
            .classify(rfc, digest, &ambiguous, RFC_8174, true)
            .unwrap();
        assert_eq!(receipt.decision, Policy::WholeOnly);
        assert_eq!(
            receipt.reason.as_deref(),
            Some("ambiguous_publication_date")
        );
    }
}
