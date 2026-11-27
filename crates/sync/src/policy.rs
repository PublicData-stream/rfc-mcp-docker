//! Outbound grants applied to authenticated RFC text.

use crate::xml::Entry;
use rfc_core::{BlobDigest, RfcNumber};
use rfc_storage::{
    policy::{
        Grant, License, MAX_GRANTS, MAX_NOTICE_BYTES, MAX_NOTICES, Policy, RIGHTS_REVISION, Rights,
        Span,
    },
    postgres::Result,
};
use serde::Deserialize;
use tokio_postgres::GenericClient;

const RFC_USE: &str = "https://www.rfc-editor.org/series/rfc-use/";
const TLP5: &str = "https://trustee.ietf.org/documents/trust-legal-provisions/tlp-5/";

#[derive(Deserialize)]
pub(crate) struct FixtureRules {
    overrides: Vec<FixtureGrant>,
}

#[derive(Deserialize)]
struct FixtureGrant {
    rfc: u32,
    digest: String,
    policy: Policy,
    #[serde(default)]
    notices: Vec<Span>,
    license: Option<License>,
    attribution: Option<String>,
}

impl FixtureRules {
    #[cfg(feature = "phase3-fixtures")]
    pub fn load(json: &str) -> Result<Self> {
        Ok(serde_json::from_str(json)?)
    }
}

fn license(family: &str, version: &str, section: &str, url: &str) -> License {
    License {
        family: family.into(),
        version: version.into(),
        section: section.into(),
        url: url.into(),
    }
}

fn span(category: &str, start: usize, end: usize) -> Span {
    Span {
        category: category.into(),
        start_byte: start as i64,
        end_byte: end as i64,
    }
}

fn tlp(date: Option<&str>, month: Option<&str>) -> Vec<u8> {
    let (Some(first), Some(last)) = (
        date.or(month),
        date.map(str::to_owned)
            .or_else(|| month.map(|m| format!("{}-31", &m[..7]))),
    ) else {
        return Vec::new();
    };
    let starts = [
        "2008-11-10",
        "2009-02-15",
        "2009-09-12",
        "2009-12-28",
        "2015-03-25",
    ];
    starts
        .iter()
        .enumerate()
        .filter_map(|(i, start)| {
            (*start <= last.as_str() && starts.get(i + 1).is_none_or(|end| first < *end))
                .then_some((i + 1) as u8)
        })
        .collect()
}

fn tlp_license(versions: &[u8], section: &str) -> License {
    let version = versions
        .iter()
        .map(|n| format!("{n}.0"))
        .collect::<Vec<_>>()
        .join("/");
    let url = match versions {
        [5] => TLP5.into(),
        [n] => format!("https://trustee.ietf.org/wp-content/uploads/IETF-TLP-{n}.pdf"),
        _ => "https://trustee.ietf.org/documents/trust-legal-provisions/".into(),
    };
    license("ietf-tlp", &version, section, &url)
}

pub(crate) fn classify(
    rfc: RfcNumber,
    digest: BlobDigest,
    metadata: BlobDigest,
    entry: Option<&Entry>,
    bytes: &[u8],
    official: bool,
    fixture: Option<&FixtureRules>,
) -> Rights {
    let stream = entry.and_then(|e| e.stream.as_deref());
    let credit = match stream {
        Some("IETF") => "IETF",
        Some("IAB") => "IAB Stream",
        Some("IRTF") => "IRTF Stream",
        Some("INDEPENDENT") => "Independent Submission Stream",
        Some("Editorial") => "Editorial Stream",
        _ => "RFC Editor",
    };
    let mut attribution = format!("rfc{}.txt, {credit}", rfc.get());
    if let Some(entry) = entry.filter(|entry| !entry.authors.is_empty()) {
        attribution.push_str(", ");
        for (index, author) in entry.authors.iter().enumerate() {
            if attribution.len() + author.len() + 2 > 4096 {
                break;
            }
            if index != 0 {
                attribution.push_str(", ");
            }
            attribution.push_str(author);
        }
    }
    let mut rights = Rights {
        revision: RIGHTS_REVISION.into(),
        rfc: rfc.get(),
        source: digest.to_string(),
        metadata: metadata.to_string(),
        search: official,
        whole: None,
        pages: false,
        quotes: Vec::new(),
        notices: Vec::new(),
        attribution,
        diagnostics: Vec::new(),
    };
    if official {
        rights.whole = Some(license(
            "rfc-series",
            "published-guidance",
            "complete reproduction",
            RFC_USE,
        ));
    }
    if let Some(rule) = fixture.and_then(|rules| {
        rules
            .overrides
            .iter()
            .find(|rule| rule.rfc == rfc.get() && rule.digest == rights.source)
    }) {
        let basis = rule
            .license
            .clone()
            .unwrap_or_else(|| license("fixture", "1", "test grant", RFC_USE));
        rights.search = true;
        if rule.policy >= Policy::WholeOnly {
            rights.whole = Some(basis.clone());
        }
        rights.pages = rule.policy >= Policy::WholePaginated;
        if rule.policy == Policy::ExcerptAllowed && !bytes.is_empty() {
            rights.quotes.push(Grant {
                start: 0,
                end: bytes.len(),
                license: basis,
                legend: None,
                notices: Vec::new(),
            });
        }
        if let Some(credit) = &rule.attribution {
            rights.attribution = format!("{}, {credit}", rights.attribution);
        }
        rights.notices.clone_from(&rule.notices);
        return rights;
    }
    let Ok(text) = std::str::from_utf8(bytes) else {
        rights.diagnostics.push("notice_layout_unresolved".into());
        return rights;
    };
    let blocks = legal_blocks(text);
    let legal = blocks
        .iter()
        .filter(|block| block.legal)
        .collect::<Vec<_>>();
    let contains = |phrase: &str| legal.iter().any(|block| block.folded.contains(phrase));
    let tlp_notice = contains("legal provisions relating to ietf documents")
        && (contains("trustee.ietf.org/license-info") || contains("ietf-trust.org/license-info"));
    let versions = entry.map_or_else(Vec::new, |e| tlp(e.date.as_deref(), e.month.as_deref()));
    let pre5378 =
        contains("before november 10, 2008") && contains("outside the ietf standards process");
    if pre5378 {
        rights.diagnostics.push("pre_5378_legend".into());
    }
    let no_derivatives = legal.iter().any(|block| {
        (block
            .folded
            .starts_with("this document may not be modified")
            || block.folded.starts_with("this document may not be changed"))
            && block.folded.contains("derivative works")
    });
    let published_as_draft_only = contains("may not be published except as an internet-draft");
    let copyright = legal
        .iter()
        .any(|block| block.category == "copyright" && block.folded.starts_with("copyright (c)"));
    let mut quote_basis = None;
    if tlp_notice && !versions.is_empty() && !published_as_draft_only {
        match stream {
            Some("IETF") => {
                rights.whole = Some(tlp_license(&versions, "3.c(i)"));
                quote_basis = Some(tlp_license(&versions, "3.c(iii)"));
            }
            Some("IAB") | Some("IRTF") | Some("INDEPENDENT")
                if versions.iter().all(|v| *v >= 4) =>
            {
                let section = match stream {
                    Some("IAB") => "8.c, 8.e and 3.a(i)",
                    Some("IRTF") => "8.c, 8.g and 3.a(i)",
                    _ => "8.c, 8.f and 3.a(i)",
                };
                let basis = tlp_license(&versions, section);
                rights.whole = Some(basis.clone());
                quote_basis = Some(basis);
            }
            _ => {}
        }
    }
    if contains("this document and translations of it may be copied and furnished to others") {
        let basis = license(
            "rfc-2026",
            "1996",
            "10.4",
            "https://www.rfc-editor.org/rfc/rfc2026.html#section-10.4",
        );
        rights.whole = Some(basis.clone());
        if contains("in whole or in part") && contains("comment on or otherwise explain it") {
            quote_basis = Some(basis);
        }
    }
    if contains("this document may be copied and distributed in whole or in part.") {
        let basis = license(
            "rfc-notice",
            "original",
            "explicit reproduction and quotation",
            RFC_USE,
        );
        rights.whole = Some(basis.clone());
        quote_basis = Some(basis);
    }
    if rights.whole.is_none() && contains("distribution of this memo is unlimited") {
        rights.whole = Some(license(
            "rfc-notice",
            "original",
            "Status of This Memo",
            RFC_USE,
        ));
    }
    // Interpret express terms in a legal block, not occurrences in prose discussing a license.
    let absolute = |phrase: &str| legal.iter().any(|block| block.folded.starts_with(phrase));
    let no_copying = absolute("this document may not be copied or searched.")
        || absolute("this document may not be reproduced in any form.");
    let no_reproduction = no_copying
        || published_as_draft_only
        || absolute("this document may not be reproduced.")
        || absolute("redistribution of this document is prohibited.");
    if no_reproduction {
        rights.whole = None;
        quote_basis = None;
        rights
            .diagnostics
            .push("document_reproduction_restricted".into());
    }
    if no_copying || absolute("this document may not be searched.") {
        rights.search = false;
        rights.diagnostics.push("internal_search_restricted".into());
    } else {
        rights.search |= rights.whole.is_some();
    }
    if absolute("excerpts of this document may not be redistributed.") {
        quote_basis = None;
        rights
            .diagnostics
            .push("document_quotation_restricted".into());
    }
    let mut scoped = Vec::new();
    let mut restricted = Vec::new();
    let mut pending = None;
    let year = entry
        .and_then(|e| e.date.as_ref().or(e.month.as_ref()))
        .map(|date| &date[..4]);
    for block in &blocks {
        if let Some((start, kind)) = pending {
            let end = block.start;
            let end_marker = match kind {
                "code" => "<code ends>",
                "inherited" => "<end pre-5378 material>",
                _ => "<end third-party material>",
            };
            if block.folded == end_marker {
                if start < end && (kind == "third-party" || kind == "inherited" && pre5378) {
                    restricted.push(start..end);
                }
                if start < end
                    && kind == "code"
                    && stream == Some("IETF")
                    && !no_reproduction
                    && !no_derivatives
                    && let Some(year) = year
                {
                    let code_grant = if tlp_notice && !versions.is_empty() {
                        Some((
                            tlp_license(&versions, "4.c, 4.e and 6.d"),
                            format!(
                                "Copyright (c) {year} IETF Trust and the persons identified as authors of the code. All rights reserved.\nRedistribution and use in source and binary forms, with or without modification, is permitted pursuant to, and subject to the license terms contained in, the {} BSD License set forth in Section 4.c of the IETF Trust’s Legal Provisions Relating to IETF Documents (https://trustee.ietf.org/license-info).",
                                if versions == [5] {
                                    "Revised"
                                } else {
                                    "Simplified"
                                }
                            ),
                        ))
                    } else {
                        historical_code(entry, &text[start..end], rfc)
                    };
                    if let Some((license, legend)) = code_grant {
                        scoped.push(Grant {
                            start,
                            end,
                            license,
                            legend: Some(legend),
                            notices: Vec::new(),
                        });
                    }
                }
                // A separate grant to this exact component can support quotation
                // regardless of the grant for the surrounding RFC.
                if kind == "third-party"
                    && start < end
                    && let Some(notice) = bsd_notice(&text[start..end], start)
                {
                    scoped.push(Grant {
                        start,
                        end,
                        license: license(
                            "source-notice",
                            "original",
                            "BSD terms in the identified range",
                            RFC_USE,
                        ),
                        legend: None,
                        notices: vec![notice],
                    });
                }
                pending = None;
            }
        } else if block.folded == "<code begins>" || block.folded.starts_with("<code begins> ") {
            pending = Some((block.end, "code"));
        } else if block.folded == "<begin third-party material>" {
            pending = Some((block.end, "third-party"));
        } else if block.folded == "<begin pre-5378 material>" {
            pending = Some((block.end, "inherited"));
        }
    }
    if let Some((start, kind)) = pending
        && (kind == "third-party" || kind == "inherited" && pre5378)
    {
        restricted.push(start..bytes.len());
    }
    let mut legend_budget = 24 * 1024;
    scoped.retain(|grant| {
        let size = grant.legend.as_ref().map_or(0, String::len);
        if size > legend_budget {
            return false;
        }
        legend_budget -= size;
        true
    });
    if !restricted.is_empty() {
        rights
            .diagnostics
            .push("range_requires_independent_grant".into());
    }
    for block in &blocks {
        if !block.category.is_empty()
            && !restricted
                .iter()
                .any(|range| range.start < block.end && block.start < range.end)
        {
            rights
                .notices
                .push(span(block.category, block.start, block.end));
        }
    }
    rights.notices.sort_by_key(|n| (n.start_byte, n.end_byte));
    rights.notices.dedup_by_key(|n| (n.start_byte, n.end_byte));
    let notices_complete = rights.notices.len() <= MAX_NOTICES
        && scoped
            .len()
            .saturating_add(restricted.len())
            .saturating_add(1)
            <= MAX_GRANTS
        && rights
            .notices
            .iter()
            .map(|n| (n.end_byte - n.start_byte) as usize)
            .sum::<usize>()
            <= MAX_NOTICE_BYTES;
    if !notices_complete {
        rights.notices.clear();
        rights.diagnostics.push("notice_inventory_limit".into());
    }
    if notices_complete {
        rights.pages = rights.whole.is_some()
            && !contains("only as a single complete copy")
            && (!tlp_notice || copyright);
        if let Some(basis) = quote_basis.filter(|_| !tlp_notice || copyright) {
            let mut start = 0;
            for stop in restricted {
                if start < stop.start {
                    rights.quotes.push(Grant {
                        start,
                        end: stop.start,
                        license: basis.clone(),
                        legend: None,
                        notices: Vec::new(),
                    });
                }
                start = stop.end;
            }
            if start < bytes.len() {
                rights.quotes.push(Grant {
                    start,
                    end: bytes.len(),
                    license: basis,
                    legend: None,
                    notices: Vec::new(),
                });
            }
        }
        rights.quotes.extend(scoped);
    }
    if rights.quotes.is_empty() {
        rights.diagnostics.push("quotation_grant_unresolved".into());
    }
    rights
}

fn bsd_notice(text: &str, base: usize) -> Option<Span> {
    let folded = text.to_ascii_lowercase();
    let grant = folded.find("redistribution and use in source and binary forms")?;
    let start = folded[..grant].rfind("copyright")?;
    let terms = folded[grant..].find("redistributions of source code must retain")? + grant;
    let disclaimer = folded[terms..].find("this software is provided")? + terms;
    let lines = text[disclaimer..].split_inclusive('\n');
    let end = disclaimer
        + lines
            .take_while(|line| !line.trim().is_empty())
            .map(str::len)
            .sum::<usize>();
    let start = folded[..start].rfind('\n').map_or(0, |index| index + 1);
    (end - start <= MAX_NOTICE_BYTES).then(|| span("third_party", base + start, base + end))
}

fn historical_code(entry: Option<&Entry>, code: &str, rfc: RfcNumber) -> Option<(License, String)> {
    let entry = entry?;
    let date = entry.date.as_deref().or(entry.month.as_deref())?;
    let (family, url) = if ("2004-03-01"..="2005-02-28").contains(&date) {
        ("rfc-3667", "https://www.rfc-editor.org/rfc/rfc3667.html")
    } else if ("2005-04-01"..="2008-10-31").contains(&date) {
        ("rfc-3978", "https://www.rfc-editor.org/rfc/rfc3978.html")
    } else {
        return None;
    };
    let notice = code
        .split("\n\n")
        .find(|block| block.to_ascii_lowercase().contains("copyright (c)"))?;
    let lower = notice
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    let required = format!(
        "rfc {}; see the rfc itself for full legal notices",
        rfc.get()
    );
    if notice.len() <= 4096 && lower.contains("copyright (c)") && lower.contains(&required) {
        Some((
            license(family, "BCP 78", "3.3(a)(E) and 5.6", url),
            notice.into(),
        ))
    } else {
        None
    }
}

struct Block {
    start: usize,
    end: usize,
    folded: String,
    category: &'static str,
    legal: bool,
}

fn legal_blocks(text: &str) -> Vec<Block> {
    let mut blocks: Vec<Block> = Vec::new();
    let mut start = 0;
    let mut end = 0;
    let mut section = "";
    let mut collect = |start: usize, end: usize| {
        if start >= end {
            return;
        }
        let folded = text[start..end]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        let category = match folded.as_str() {
            "status of this memo" => "legend",
            "copyright notice" | "copyright statement" | "full copyright statement" => "copyright",
            "intellectual property"
            | "intellectual property statement"
            | "legal notice"
            | "license" => "license",
            "author's address" | "authors' addresses" | "editor's address"
            | "editors' addresses" | "contributors" | "acknowledgements" | "acknowledgments" => {
                "authorship"
            }
            _ => "",
        };
        let heading = text[start..end]
            .lines()
            .next()
            .is_some_and(|line| !line.starts_with(char::is_whitespace));
        if heading {
            section = category;
        }
        let category = if folded.starts_with("copyright (c)") || folded.starts_with("copyright ©")
        {
            "copyright"
        } else if blocks.is_empty() && start < 1024 {
            "authorship"
        } else {
            section
        };
        let legal = matches!(section, "legend" | "copyright" | "license");
        blocks.push(Block {
            start,
            end,
            folded,
            category,
            legal,
        });
    };
    for line in text.split_inclusive('\n') {
        let next = end + line.len();
        let trimmed = line.trim();
        if trimmed.is_empty()
            || trimmed.starts_with("<CODE BEGINS>")
            || matches!(
                trimmed,
                "<CODE ENDS>"
                    | "<BEGIN THIRD-PARTY MATERIAL>"
                    | "<END THIRD-PARTY MATERIAL>"
                    | "<BEGIN PRE-5378 MATERIAL>"
                    | "<END PRE-5378 MATERIAL>"
            )
        {
            collect(start, end);
            if !trimmed.is_empty() {
                collect(end, next);
            }
            start = next;
        }
        end = next;
    }
    collect(start, end);
    blocks
}

pub(crate) async fn store(db: &impl GenericClient, generation: i64, rights: &Rights) -> Result<()> {
    let digest = rights.digest()?;
    db.execute(
        "INSERT INTO rfc.source_rights (generation_id,rfc_id,identity,evidence)
         VALUES ($1,$2::integer,$3::bytea,$4::text::jsonb)
         ON CONFLICT (generation_id,rfc_id) DO UPDATE SET identity=excluded.identity,evidence=excluded.evidence
         WHERE rfc.source_rights.identity IS DISTINCT FROM excluded.identity",
        &[&generation, &(rights.rfc as i32), &digest.as_bytes().as_slice(), &serde_json::to_string(rights)?],
    ).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const GRANT: &str = "Copyright Notice\r\n\r\n   Copyright (c) 2022 IETF Trust.\r\n\r\n   This document is subject to BCP 78 and the IETF Trust's Legal Provisions Relating to IETF Documents (https://trustee.ietf.org/license-info).\r\n\r\n";
    const PRE_5378: &str = "   This document may contain material from IETF Contributions published before November 10, 2008. Modification outside the IETF Standards Process may be limited.\r\n\r\n";

    fn evaluate(text: &str, date: &str, stream: &str, official: bool) -> Rights {
        let entry = Entry {
            title: "Example".into(),
            stream: Some(stream.into()),
            date: Some(date.into()),
            month: None,
            status: None,
            has_txt: true,
            authors: Vec::new(),
        };
        classify(
            RfcNumber::new(100).unwrap(),
            BlobDigest::hash(text.as_bytes()),
            BlobDigest::hash(b"metadata"),
            Some(&entry),
            text.as_bytes(),
            official,
            None,
        )
    }

    #[test]
    fn the_general_quotation_grant_survives_modification_legends_and_prose() {
        let original = format!(
            "{GRANT}{PRE_5378}   This document may not be modified, and derivative works of it may not be created.\r\n\r\n1. Discussion\r\n\r\n   The phrase redistribution of this document is prohibited. is an example.\r\n"
        );
        let rights = evaluate(&original, "2022-06-01", "IETF", true);
        assert!(rights.search && rights.pages && rights.whole.is_some());
        assert!(rights.diagnostics.contains(&"pre_5378_legend".to_owned()));
        assert!(rights.quote(&(0..original.len())).is_some());
        assert!(rights.notices.iter().any(|notice| {
            original[notice.start_byte as usize..notice.end_byte as usize]
                .contains("derivative works")
        }));
    }

    #[test]
    fn a_concrete_inherited_range_has_independent_search_and_quote_rights() {
        let original = format!(
            "{GRANT}{PRE_5378}1. Discussion\r\n\r\nBefore\r\n<BEGIN PRE-5378 MATERIAL>\r\nInherited bytes\r\n<END PRE-5378 MATERIAL>\r\nAfter\r\n"
        );
        let rights = evaluate(&original, "2022-06-01", "IETF", true);
        let affected = original.find("Inherited bytes").unwrap();
        assert!(rights.search && rights.whole.is_some());
        assert!(rights.cover(&(affected..affected + 5)).is_none());
        let unaffected = original.find("After").unwrap();
        assert!(rights.cover(&(unaffected..unaffected + 5)).is_some());
    }

    #[test]
    fn a_document_can_be_searchable_without_a_quotation_grant() {
        let original = "Original archival document\r\n";
        let rights = evaluate(original, "1970-01-01", "Legacy", true);
        assert!(rights.search && rights.whole.is_some());
        assert!(rights.quotes.is_empty());
        assert!(
            evaluate(original, "1970-01-01", "Legacy", false)
                .whole
                .is_none()
        );
    }

    #[test]
    fn code_grant_is_independent_and_keeps_the_applicable_terms() {
        let original = format!(
            "{GRANT}   Excerpts of this document may not be redistributed.\r\n\r\n1. Example\r\n\r\n<CODE BEGINS>\r\nfn main() {{}}\r\n<CODE ENDS>\r\n"
        );
        let rights = evaluate(&original, "2022-06-01", "IETF", true);
        let start = original.find("fn main").unwrap();
        let code = rights.quote(&(start..start + 7)).unwrap();
        assert!(code.legend.as_ref().unwrap().contains("Copyright (c) 2022"));
        assert!(rights.quote(&(0..7)).is_none());
    }

    #[test]
    fn express_copying_and_search_limits_apply_to_the_operations_they_cover() {
        let original = format!(
            "{GRANT}   This document may not be reproduced in any form.\r\n\r\n1. Example\r\n\r\n<CODE BEGINS>\r\nfn main() {{}}\r\n<CODE ENDS>\r\n"
        );
        let rights = evaluate(&original, "2022-06-01", "IETF", true);
        assert!(!rights.search && !rights.pages && rights.whole.is_none());
        assert!(rights.quotes.is_empty());

        let original = format!("{GRANT}   This document may not be searched.\r\n");
        let rights = evaluate(&original, "2022-06-01", "IETF", true);
        assert!(!rights.search && rights.pages && rights.whole.is_some());
        assert!(rights.quote(&(0..7)).is_some());
    }

    #[test]
    fn publication_month_crossing_an_update_selects_common_tlp_grant() {
        assert_eq!(tlp(None, Some("2015-03-01")), [4, 5]);
        assert_eq!(tlp(Some("2015-03-24"), None), [4]);
        assert_eq!(tlp(Some("2015-03-25"), None), [5]);
    }
}
