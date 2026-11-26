//! Recognition is restricted to the original surrounding boilerplate and byline.

use crate::xml::Entry;
use rfc_storage::policy::Span;

pub(super) const TLP5: &str = "ietf-tlp-5-copyright-2";

pub(super) fn restriction(text: &str) -> Option<&'static str> {
    for block in text.split("\n\n") {
        let folded = normalize(block).to_ascii_lowercase();
        if folded.contains("before november 10, 2008") || folded.contains("pre-5378 material") {
            return Some("unresolved_pre5378_material");
        }
        if folded.contains("derivative works")
            || folded.contains("may not be modified")
            || folded.contains("redistribution is prohibited")
            || folded.contains("may not be reproduced")
        {
            return Some("conflicting_license_terms");
        }
        if folded.contains("used with permission")
            || folded.contains("reproduced with permission")
            || folded.contains("third-party copyright")
            || folded.contains("third party copyright")
        {
            return Some("unresolved_third_party_rights");
        }
    }
    None
}

pub(super) fn verify_tlp5(
    text: &str,
    rfc: u32,
    entry: &Entry,
    start_date: &str,
) -> Result<(Vec<Span>, String), &'static str> {
    if text
        .bytes()
        .filter(|byte| matches!(byte, b'\n' | b'\x0c'))
        .take(65_537)
        .count()
        > 65_536
    {
        return Err("notice_inventory_work_limit");
    }
    let lines = lines(text);
    let header_at = lines
        .iter()
        .position(|(_, line)| !line.trim_start_matches('\u{feff}').trim().is_empty())
        .ok_or("incomplete_notice_inventory")?;
    let (header_start, first_line) = lines[header_at];
    if !first_line
        .trim_start_matches('\u{feff}')
        .starts_with("Internet Engineering Task Force (IETF)")
    {
        return Err("stream_metadata_conflict");
    }
    let header_end = (header_at..lines.len())
        .find(|&index| lines[index].1.trim().is_empty())
        .ok_or("incomplete_notice_inventory")?;
    let header = &text[header_start..lines[header_end].0];
    if header_end - header_at > 16
        || !header.lines().any(|line| {
            line.strip_prefix("Request for Comments:")
                .and_then(|value| value.split_whitespace().next())
                == Some(rfc.to_string().as_str())
        })
        || !header.contains(&format!(
            "{} {}",
            month_name(&start_date[5..7]),
            &start_date[..4]
        ))
    {
        return Err("publication_metadata_conflict");
    }
    if entry.authors.is_empty() || entry.authors.len() > 16 {
        return Err("incomplete_authorship");
    }
    let status = lines
        .iter()
        .position(|(_, line)| line.trim() == "Status of This Memo")
        .ok_or("incomplete_notice_inventory")?;
    let mut copyright = lines
        .iter()
        .enumerate()
        .filter(|(_, (_, line))| line.trim() == "Copyright Notice");
    let (copyright_at, (copyright_start, _)) =
        copyright.next().ok_or("incomplete_notice_inventory")?;
    if copyright.next().is_some() || status >= copyright_at || *copyright_start > 12 * 1024 {
        return Err("unknown_license_template");
    }
    let mut next = copyright_at + 1;
    let (holder, holder_range) =
        paragraph(&lines, &mut next).ok_or("incomplete_notice_inventory")?;
    let expected_holder = format!(
        "Copyright (c) {} IETF Trust and the persons identified as the document authors. All rights reserved.",
        &start_date[..4],
    );
    if holder != expected_holder {
        return Err("unknown_license_template");
    }
    let (terms, terms_range) = paragraph(&lines, &mut next).ok_or("incomplete_notice_inventory")?;
    if !standard_terms(&terms) {
        return Err("unknown_license_template");
    }
    let holder_end = lines[holder_range.end - 1].0 + lines[holder_range.end - 1].1.len();
    let terms_end = lines[terms_range.end - 1].0 + lines[terms_range.end - 1].1.len();
    let mut authors = Vec::new();
    // The title page supplies authorship and the terminal address block verifies the names.
    let at = lines
        .iter()
        .rposition(|(_, line)| {
            matches!(
                line.trim(),
                "Author's Address"
                    | "Authors' Addresses"
                    | "Editor's Address"
                    | "Editors' Addresses"
            )
        })
        .ok_or("incomplete_authorship")?;
    if at <= terms_range.end || at < lines.len().saturating_sub(240) {
        return Err("incomplete_authorship");
    }
    let mut seen = std::collections::BTreeSet::new();
    for author in &entry.authors {
        if author.len() > 160 || author.chars().any(char::is_control) {
            return Err("incomplete_authorship");
        }
        let author = without_role(author);
        let last_name = author
            .split_whitespace()
            .last()
            .ok_or("incomplete_authorship")?;
        if !header.lines().any(|line| {
            line.split_whitespace()
                .any(|part| part.trim_end_matches(',') == last_name)
        }) {
            return Err("incomplete_authorship");
        }
        let mut matches = lines[at + 1..]
            .iter()
            .enumerate()
            .filter_map(|(found, (_, line))| {
                let name = without_role(line.trim());
                names_match(author, name).then_some((at + 1 + found, name))
            });
        let Some((found, name)) = matches.next() else {
            return Err("incomplete_authorship");
        };
        if matches.next().is_some() || !seen.insert(found) {
            return Err("incomplete_authorship");
        }
        authors.push(name);
    }
    let mut extra = lines
        .iter()
        .filter(|(offset, _)| *offset >= terms_end || *offset < *copyright_start);
    if extra.any(|(_, line)| {
        let lower = line.trim_start().to_ascii_lowercase();
        lower.starts_with("copyright ")
            || lower.starts_with("copyright:")
            || lower.starts_with("license:")
            || lower.starts_with("license notice")
            || lower.starts_with("legal notice")
            || lower.contains("all rights reserved")
            || lower.contains("<code begins>")
            || lower.contains("spdx-license-identifier:")
            || lower.contains("permission is hereby granted")
            || lower.contains("licensed under")
            || lower.contains("redistribution and use")
            || lower.contains("copyright holder")
            || lower.contains("copyright owner")
            || lower.contains("patent and trademark notice")
    }) {
        return Err("unresolved_third_party_rights");
    }
    if !text[lines[status].0..*copyright_start].contains("RFC")
        || !text[lines[status].0..*copyright_start].contains("document is a product")
    {
        return Err("unknown_license_template");
    }
    let mut notices = vec![
        span("authorship", header_start, lines[header_end].0),
        span("legend", lines[status].0, *copyright_start),
        span("copyright", *copyright_start, holder_end),
        span("license", lines[terms_range.start].0, terms_end),
    ];
    for (index, (offset, line)) in lines[..at].iter().enumerate() {
        if !matches!(
            line.trim_end(),
            "Contributors"
                | "Contributing Authors"
                | "Additional Authors"
                | "Acknowledgments"
                | "Acknowledgements"
        ) {
            continue;
        }
        if *offset < terms_end {
            return Err("unknown_authorship_layout");
        }
        let end = (index + 1..=at)
            .find(|&next| {
                let line = lines[next].1;
                !line.trim().is_empty() && !line.starts_with(char::is_whitespace)
            })
            .unwrap_or(at);
        if lines[index + 1..end]
            .iter()
            .any(|(_, line)| line.contains('\x0c'))
        {
            return Err("unknown_authorship_layout");
        }
        notices.push(span("authorship", *offset, lines[end].0));
    }
    // The reviewed address block is terminal. Keeping it whole retains roles,
    // affiliations and every byline even when some names occupy separate pages.
    let terminal_end = text.trim_end_matches(char::is_whitespace).len();
    if terminal_end <= lines[at].0 {
        return Err("incomplete_authorship");
    }
    notices.push(span("authorship", lines[at].0, terminal_end));
    Ok((notices, format!("IETF, RFC {rfc}, {}", authors.join(", "))))
}

fn without_role(name: &str) -> &str {
    [", Ed.", ", ed.", " (editor)", " (Editor)"]
        .iter()
        .find_map(|role| name.strip_suffix(role))
        .unwrap_or(name)
}

fn names_match(catalog: &str, original: &str) -> bool {
    if catalog == original {
        return true;
    }
    let Some((initial, surname)) = catalog.split_once(". ") else {
        return false;
    };
    initial.len() == 1
        && original.starts_with(initial)
        && original
            .strip_suffix(surname)
            .is_some_and(|prefix| prefix.ends_with(' '))
}

fn span(category: &str, start: usize, end: usize) -> Span {
    Span {
        category: category.into(),
        start_byte: start as i64,
        end_byte: end as i64,
    }
}

fn month_name(month: &str) -> &'static str {
    const MONTHS: [&str; 12] = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ];
    MONTHS[month.parse::<usize>().expect("validated month") - 1]
}

fn lines(text: &str) -> Vec<(usize, &str)> {
    let mut offset = 0;
    text.split_inclusive(['\n', '\x0c'])
        .map(|line| {
            let start = offset;
            offset += line.len();
            (start, line)
        })
        .collect()
}

fn paragraph(
    lines: &[(usize, &str)],
    next: &mut usize,
) -> Option<(String, std::ops::Range<usize>)> {
    let start = (*next..lines.len()).find(|&index| !lines[index].1.trim().is_empty())?;
    let end = (start..lines.len())
        .find(|&index| lines[index].1.trim().is_empty())
        .unwrap_or(lines.len());
    *next = end;
    let paragraph = lines[start..end]
        .iter()
        .flat_map(|(_, line)| line.split_whitespace())
        .collect::<Vec<_>>()
        .join(" ");
    Some((paragraph, start..end))
}

fn normalize(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn standard_terms(terms: &str) -> bool {
    for url in [
        "http://trustee.ietf.org/license-info",
        "https://trustee.ietf.org/license-info",
        "https://trustee.ietf.org/license-info/",
    ] {
        for bsd in ["Simplified", "Revised"] {
            let expected = format!(
                "This document is subject to BCP 78 and the IETF Trust's Legal Provisions Relating to IETF Documents ({url}) in effect on the date of publication of this document. Please review these documents carefully, as they describe your rights and restrictions with respect to this document. Code Components extracted from this document must include {bsd} BSD License text as described in Section 4.e of the Trust Legal Provisions and are provided without warranty as described in the {bsd} BSD License."
            );
            if terms == expected {
                return true;
            }
        }
    }
    false
}
