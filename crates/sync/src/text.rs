//! Deterministic indexes over the exact RFC TXT bytes.

use crate::{Chunk, Notice, Section, xml};
use rfc_core::{ByteRange, Coverage, RelationKind, RfcNumber, SectionId};
use rfc_storage::postgres::Result;
use std::collections::BTreeSet;

const CHUNK: usize = 16 * 1024;

pub(crate) struct TextIndex<'a> {
    pub text: Option<&'a str>,
    pub line_starts: Vec<i64>,
    pub sections: Vec<Section<'a>>,
    pub headings: Vec<String>,
    pub section_state: Coverage,
    pub notices: Vec<Notice<'a>>,
    pub notice_state: Coverage,
    pub notice_family: &'static str,
    pub chunks: Vec<Chunk<'a>>,
    pub references: Vec<TextReference>,
    pub all_references: Coverage,
    pub classified_references: Coverage,
}

pub(crate) struct TextReference {
    pub kind: RelationKind,
    pub target: RfcNumber,
    pub range: ByteRange,
}

struct Heading<'a> {
    raw: ByteRange,
    number: Option<&'a str>,
    title: String,
    depth: usize,
}

#[derive(Clone, Copy)]
struct Line<'a> {
    start: usize,
    raw: &'a str,
}

impl<'a> Line<'a> {
    fn end(self) -> usize {
        self.start + self.raw.len()
    }

    fn text(self) -> &'a str {
        self.raw.trim_end_matches(['\r', '\n'])
    }
}

fn span(start: usize, end: usize) -> Result<ByteRange> {
    ByteRange::new(i64::try_from(start)?, i64::try_from(end)?)
        .ok_or_else(|| "invalid TXT range".into())
}

pub(crate) fn index(bytes: &[u8]) -> Result<TextIndex<'_>> {
    let prefix = bytes
        .get(..64)
        .unwrap_or(bytes)
        .trim_ascii_start()
        .to_ascii_lowercase();
    if bytes.is_empty()
        || bytes.contains(&0)
        || [
            b"%pdf-".as_slice(),
            b"pk\x03\x04",
            b"<!doctype html",
            b"<html",
            b"\x1f\x8b",
        ]
        .iter()
        .any(|magic| prefix.starts_with(magic))
    {
        return Err("input is not RFC TXT".into());
    }
    let mut line_starts = vec![0];
    for (offset, byte) in bytes[..bytes.len() - 1].iter().enumerate() {
        if *byte == b'\n' {
            line_starts.push(i64::try_from(offset + 1)?);
        }
    }
    let mut result = TextIndex {
        text: std::str::from_utf8(bytes).ok(),
        line_starts,
        sections: Vec::new(),
        headings: Vec::new(),
        section_state: Coverage::Unavailable,
        notices: Vec::new(),
        notice_state: Coverage::Unavailable,
        notice_family: "unknown",
        chunks: Vec::new(),
        references: Vec::new(),
        all_references: Coverage::Unavailable,
        classified_references: Coverage::Unavailable,
    };
    let Some(text) = result.text else {
        return Ok(result);
    };
    result.chunks = chunks(text)?;
    // Preserve line and search indexes for extremely line-dense inputs without
    // allocating a second, large representation of the line table.
    if result.line_starts.len() > 200_000 {
        return Ok(result);
    }
    let lines: Vec<_> = text
        .split_inclusive('\n')
        .zip(&result.line_starts)
        .map(|(raw, &start)| Line {
            start: start as usize,
            raw,
        })
        .collect();
    let (headings, uncertain) = headings(&lines)?;
    let (sections, section_state) = sections(&headings, text.len(), uncertain)?;
    result.section_state = section_state;
    let mut bibliography = Bibliography::default();
    for (heading, section) in headings.into_iter().zip(&sections) {
        bibliography.read(&lines, &heading, section.body)?;
        if let Some(category) = notice_category(&heading.title) {
            push_notice(&mut result.notices, category, section.body, section_state)?;
        }
        result.headings.push(heading.title);
    }
    add_inline_notices(&lines, &mut result.notices)?;
    result
        .notices
        .sort_by_key(|notice| (notice.range.start(), notice.range.end(), notice.category));
    result
        .notices
        .dedup_by(|a, b| a.range == b.range && a.category == b.category);
    for (ordinal, notice) in result.notices.iter_mut().enumerate() {
        notice.ordinal = i32::try_from(ordinal)?;
    }
    let lower = text.to_ascii_lowercase();
    let trust = lower.contains("ietf trust") && lower.contains("legal provisions");
    result.notice_family = if trust {
        "ietf-trust"
    } else if lower.contains("distribution of this memo is unlimited") {
        "unlimited-distribution"
    } else if lower.contains("copyright") {
        "legacy-copyright"
    } else {
        "unknown"
    };
    if !result.notices.is_empty() {
        result.notice_state = Coverage::Partial;
        if trust
            && result
                .notices
                .iter()
                .any(|notice| notice.category == "authorship")
            && result
                .notices
                .iter()
                .any(|notice| notice.category == "copyright")
            && result
                .notices
                .iter()
                .any(|notice| notice.category == "license")
            && result.section_state == Coverage::Complete
        {
            result.notice_state = Coverage::Complete;
        }
    }
    let (references, all, classified) = bibliography.finish();
    result.sections = sections;
    result.references = references;
    result.all_references = all;
    result.classified_references = classified;
    Ok(result)
}

fn chunks(text: &str) -> Result<Vec<Chunk<'_>>> {
    let mut start = 0;
    let mut chunks = Vec::new();
    while start < text.len() {
        let limit = (start + CHUNK).min(text.len());
        let mut end = text.floor_char_boundary(limit);
        if end != text.len()
            && let Some(line) = text.as_bytes()[start + CHUNK / 2..end]
                .iter()
                .rposition(|&byte| byte == b'\n')
        {
            end = start + CHUNK / 2 + line + 1;
        }
        chunks.push(Chunk {
            range: span(start, end)?,
            text: &text[start..end],
        });
        start = end;
    }
    Ok(chunks)
}

fn headings<'a>(lines: &[Line<'a>]) -> Result<(Vec<Heading<'a>>, bool)> {
    let mut result = Vec::new();
    let mut contents = false;
    let mut code = false;
    let mut uncertain = false;
    let mut seen = BTreeSet::new();
    for (i, &line) in lines.iter().enumerate() {
        let label = line.text().trim_end();
        let trimmed = label.trim_start_matches('\u{feff}');
        if matches!(trimmed.trim(), "<CODE BEGINS>" | "<CODE ENDS>") || trimmed.starts_with("```") {
            code = match trimmed.trim() {
                "<CODE BEGINS>" => true,
                "<CODE ENDS>" => false,
                _ => !code,
            };
            continue;
        }
        if code
            || trimmed.starts_with(char::is_whitespace)
            || trimmed.is_empty()
            || furniture(trimmed)
        {
            continue;
        }
        if matches!(trimmed, "Table of Contents" | "Contents") {
            contents = true;
            continue;
        }
        let (number, title, depth) = match numbered(trimmed) {
            Some((number, title, depth)) => (Some(number), title, depth),
            None if known_heading(trimmed) => (None, trimmed, 0),
            None => continue,
        };
        if contents {
            let boundary = i
                .checked_sub(1)
                .and_then(|index| lines.get(index))
                .is_some_and(|previous| {
                    previous.text().trim().is_empty() || furniture(previous.text())
                });
            let next_is_body = lines[i + 1..]
                .iter()
                .map(|line| line.text())
                .find(|next| !next.trim().is_empty() && !furniture(next))
                .is_some_and(|next| numbered(next).is_none() && !known_heading(next));
            if toc_entry(trimmed) || !boundary || !next_is_body {
                continue;
            }
        }
        contents = false;
        if title.is_empty() || toc_entry(title) || title.contains('\t') {
            uncertain = true;
            continue;
        }
        if let Some(number) = number
            && !seen.insert(number)
        {
            uncertain = true;
            continue;
        }
        let mut heading = title.to_owned();
        let mut end = line.end();
        if let Some(next) = lines.get(i + 1) {
            let raw = next.text();
            let continuation = raw.trim_start_matches(' ');
            let indent = raw.len() - continuation.len();
            if (1..=3).contains(&indent)
                && continuation.chars().next().is_some_and(char::is_lowercase)
                && continuation.len() <= 72
                && !continuation.ends_with(['.', ':'])
                && lines.get(i + 2).is_none_or(|after| {
                    after.text().trim().is_empty() || !after.text().starts_with(char::is_whitespace)
                })
            {
                heading.push(' ');
                heading.push_str(continuation.trim_end());
                end = next.end();
            }
        }
        result.push(Heading {
            raw: span(line.start, end)?,
            number,
            title: heading,
            depth,
        });
    }
    Ok((result, uncertain || code || contents))
}

fn toc_entry(line: &str) -> bool {
    let line = line.trim_end();
    let prefix = line.trim_end_matches(|ch: char| ch.is_ascii_digit());
    prefix.len() < line.len()
        && prefix
            .chars()
            .rev()
            .take_while(|c| matches!(c, '.' | ' '))
            .filter(|c| *c == '.')
            .count()
            >= 2
}

fn furniture(line: &str) -> bool {
    line.contains("[Page ") || (line.starts_with("RFC ") && line.contains("   "))
}

fn numbered(line: &str) -> Option<(&str, &str, usize)> {
    let (label, rest) = if let Some(rest) = line.strip_prefix("Appendix ") {
        let (label, title) = rest.split_once(' ')?;
        if !label.as_bytes().first().is_some_and(u8::is_ascii_uppercase) {
            return None;
        }
        (label, title)
    } else {
        line.split_once(' ')?
    };
    let terminal = label.ends_with('.');
    let label = label.strip_suffix('.').unwrap_or(label);
    let mut parts = label.split('.');
    let root = parts.next()?;
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    if !(digits(root) || root.len() == 1 && root.as_bytes()[0].is_ascii_uppercase()) {
        return None;
    }
    let mut depth = 0;
    for part in parts {
        if !digits(part) || depth == 15 {
            return None;
        }
        depth += 1;
    }
    if !terminal && depth == 0 && !line.starts_with("Appendix ") {
        return None;
    }
    let title = rest.trim_start();
    let title = title
        .split_once("   ")
        .filter(|(first, _)| {
            [
                "MUST",
                "MUST NOT",
                "SHOULD",
                "SHOULD NOT",
                "SHALL",
                "SHALL NOT",
                "REQUIRED",
                "RECOMMENDED",
                "MAY",
                "OPTIONAL",
            ]
            .contains(first)
        })
        .map_or(title, |(first, _)| first);
    Some((label, title, depth))
}

fn known_heading(title: &str) -> bool {
    notice_category(title).is_some()
        || matches!(
            title.to_ascii_lowercase().as_str(),
            "abstract"
                | "references"
                | "normative references"
                | "informative references"
                | "security considerations"
                | "iana considerations"
                | "introduction"
        )
}

fn sections<'a>(
    headings: &[Heading<'a>],
    length: usize,
    uncertain: bool,
) -> Result<(Vec<Section<'a>>, Coverage)> {
    let mut sections: Vec<Section<'a>> = Vec::new();
    let mut parents: Vec<usize> = Vec::new();
    let mut incomplete = uncertain;
    for (index, heading) in headings.iter().enumerate() {
        let depth = i32::try_from(heading.depth)?;
        while let Some(&previous) = parents.last() {
            if sections[previous].depth < depth {
                break;
            }
            sections[previous].body = ByteRange::new(
                sections[previous].body.start().try_into()?,
                heading.raw.start().try_into()?,
            )
            .ok_or("invalid section end")?;
            parents.pop();
        }
        let parent = parents.last().map(|&index| sections[index].id.clone());
        if heading.depth > 0
            && parents.last().is_none_or(|&index| {
                let expected = heading
                    .number
                    .and_then(|number| number.rsplit_once('.'))
                    .map(|(parent, _)| parent);
                sections[index].number != expected
            })
        {
            incomplete = true;
        }
        let start: usize = heading.raw.start().try_into()?;
        sections.push(Section {
            id: SectionId::new(format!("s-{start}")).ok_or("empty section ID")?,
            parent,
            ordinal: i32::try_from(index)?,
            depth,
            number: heading.number,
            heading: "",
            body: span(start, length)?,
            raw_heading: heading.raw,
            parse_state: Coverage::Complete,
        });
        parents.push(index);
    }
    let state = if sections.is_empty() {
        Coverage::Unavailable
    } else if incomplete || !headings.iter().any(|heading| heading.number.is_some()) {
        Coverage::Partial
    } else {
        Coverage::Complete
    };
    for section in &mut sections {
        section.parse_state = state;
    }
    Ok((sections, state))
}

fn notice_category(title: &str) -> Option<&'static str> {
    match title.to_ascii_lowercase().as_str() {
        "status of this memo" => Some("other"),
        "copyright notice" | "copyright statement" | "full copyright statement" => {
            Some("copyright")
        }
        "license" | "intellectual property" | "intellectual property statement" => Some("license"),
        "disclaimer" => Some("disclaimer"),
        "contributors" | "acknowledgment" | "acknowledgments" | "acknowledgements" | "authors"
        | "author's address" | "authors' addresses" | "editor's address" | "author information" => {
            Some("authorship")
        }
        _ => None,
    }
}

fn push_notice(
    notices: &mut Vec<Notice<'_>>,
    category: &'static str,
    range: ByteRange,
    extraction: Coverage,
) -> Result<()> {
    notices.push(Notice {
        ordinal: i32::try_from(notices.len())?,
        category,
        range,
        extraction,
        review: "unreviewed",
    });
    Ok(())
}

fn add_inline_notices(lines: &[Line<'_>], notices: &mut Vec<Notice<'_>>) -> Result<()> {
    if let Some(index) = lines.iter().take(40).position(|line| {
        line.text()
            .trim_start()
            .starts_with("Request for Comments:")
    }) {
        let mut start = index;
        let mut end = index + 1;
        while start > 0 && !lines[start - 1].text().trim().is_empty() {
            start -= 1;
        }
        while end < lines.len() && !lines[end].text().trim().is_empty() {
            end += 1;
        }
        push_notice(
            notices,
            "authorship",
            span(lines[start].start, lines[end - 1].end())?,
            Coverage::Complete,
        )?;
    }
    let mut start = 0;
    while start < lines.len() {
        let mut end = start + 1;
        if lines[start].text().trim().is_empty() {
            start = end;
            continue;
        }
        while end < lines.len()
            && !lines[end].text().trim().is_empty()
            && !lines[end].text().contains('\u{c}')
        {
            end += 1;
        }
        let paragraph = &lines[start..end];
        let normalized = paragraph
            .iter()
            .map(|line| line.text().trim())
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        for (category, markers) in [
            ("copyright", &["copyright (c)", "copyright ©"][..]),
            (
                "license",
                &[
                    "legal provisions relating to ietf documents",
                    "distribution of this memo is unlimited",
                    "code components extracted from this document",
                    "permission is granted",
                ][..],
            ),
            (
                "disclaimer",
                &[
                    "without warranty",
                    "provided \"as is\"",
                    "disclaims all warranties",
                ][..],
            ),
        ] {
            if markers.iter().any(|marker| normalized.contains(marker)) {
                push_notice(
                    notices,
                    category,
                    span(paragraph[0].start, paragraph[paragraph.len() - 1].end())?,
                    Coverage::Complete,
                )?;
            }
        }
        start = end;
    }
    Ok(())
}

#[derive(Default)]
struct Bibliography {
    found: bool,
    incomplete: bool,
    edges: Vec<TextReference>,
}

impl Bibliography {
    fn read(&mut self, lines: &[Line<'_>], heading: &Heading<'_>, body: ByteRange) -> Result<()> {
        let kind = xml::classification(&heading.title);
        if kind.is_none() && !heading.title.eq_ignore_ascii_case("references") {
            return Ok(());
        }
        self.found = true;
        let start = lines.partition_point(|line| (line.start as u64) < heading.raw.end());
        let end = lines.partition_point(|line| (line.start as u64) < body.end());
        let first_child = lines[start..end].iter().position(|line| {
            numbered(line.text()).is_some_and(|(_, title, _)| xml::classification(title).is_some())
                || xml::classification(line.text()).is_some()
        });
        let end = first_child.map_or(end, |offset| start + offset);
        let mut index = start;
        let mut saw_record = false;
        while index < end {
            let line = lines[index];
            index += 1;
            let value = line.text().trim_start();
            if value.is_empty() || furniture(value) {
                continue;
            }
            let Some((label, rest)) = value
                .strip_prefix('[')
                .and_then(|value| value.split_once(']'))
            else {
                self.incomplete = true;
                continue;
            };
            saw_record = true;
            let mut stop = index;
            while stop < end {
                let next = lines[stop].text().trim_start();
                if next.is_empty() || next.starts_with('[') || furniture(next) {
                    break;
                }
                stop += 1;
            }
            let mut targets = BTreeSet::new();
            targets.extend(label.strip_prefix("RFC").and_then(xml::number));
            let records =
                std::iter::once(rest).chain(lines[index..stop].iter().map(|line| line.text()));
            let mut external = false;
            let mut possible_rfc = label.starts_with("RFC");
            let mut in_title = false;
            for record in records {
                possible_rfc |= xml::rfc_link(record);
                external |= [
                    "DOI ",
                    "http://",
                    "https://",
                    "Internet-Draft",
                    "ISBN ",
                    "W3C",
                    "ITU-T",
                    "IEEE",
                ]
                .iter()
                .any(|marker| record.contains(marker));
                for (index, field) in record.split('"').enumerate() {
                    in_title ^= index != 0;
                    if !in_title {
                        for (offset, marker) in field.match_indices(", RFC ") {
                            let value = &field[offset + marker.len()..];
                            let digits = value.bytes().take_while(u8::is_ascii_digit).count();
                            if value
                                .as_bytes()
                                .get(digits)
                                .is_none_or(|byte| matches!(byte, b',' | b'.' | b' ' | b'\t'))
                                && let Some(id) = xml::number(&value[..digits])
                            {
                                targets.insert(id);
                            }
                        }
                    }
                }
            }
            if targets.len() > 1 || (targets.is_empty() && (!external || possible_rfc)) {
                self.incomplete = true;
            } else if let Some(target) = targets.into_iter().next() {
                let last = if stop == index { line } else { lines[stop - 1] };
                self.edges.push(TextReference {
                    kind: kind.unwrap_or(RelationKind::UnclassifiedReference),
                    target,
                    range: span(line.start, last.end())?,
                });
            }
            index = stop;
        }
        if !saw_record && first_child.is_none() {
            self.incomplete = true;
        }
        Ok(())
    }

    fn finish(self) -> (Vec<TextReference>, Coverage, Coverage) {
        let all = if !self.found {
            Coverage::Unavailable
        } else if self.incomplete {
            Coverage::Partial
        } else {
            Coverage::Complete
        };
        let classified = if all == Coverage::Complete
            && self
                .edges
                .iter()
                .any(|edge| edge.kind == RelationKind::UnclassifiedReference)
        {
            Coverage::Partial
        } else {
            all
        };
        (self.edges, all, classified)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sections_are_slices_of_the_unchanged_txt() {
        let bytes = include_bytes!("../../../fixtures/synthetic/headings.txt");
        let indexed = index(bytes).unwrap();
        assert_eq!(
            indexed.headings,
            [
                "Redundant title",
                "café and café",
                "Redundant title",
                "Wrapped title",
                "Contributors",
                "Copyright notice"
            ]
        );
        for section in &indexed.sections {
            let body = section.body.select(bytes).unwrap();
            assert!(body.starts_with(section.raw_heading.select(bytes).unwrap()));
            assert!(std::str::from_utf8(body).is_ok());
        }
        assert_eq!(
            indexed.sections[1].parent.as_ref(),
            Some(&indexed.sections[0].id)
        );
        assert!(indexed.notices.iter().any(|n| n.category == "copyright"));
        assert_eq!(
            indexed
                .chunks
                .iter()
                .flat_map(|chunk| chunk.text.bytes())
                .collect::<Vec<_>>(),
            bytes
        );
        assert!(
            indexed
                .line_starts
                .iter()
                .skip(1)
                .all(|offset| bytes[*offset as usize - 1] == b'\n')
        );
    }

    #[test]
    fn validity_and_boundary_choices_preserve_source() {
        let legacy = index(b"RFC 1\nlegacy\xff\n").unwrap();
        assert!(legacy.text.is_none());
        assert_eq!(legacy.line_starts, [0, 6]);
        assert_eq!(legacy.section_state, Coverage::Unavailable);
        assert!(index(b"%PDF-1.7").is_err());
        let bytes = format!("{}é-tail", "x".repeat(CHUNK - 1));
        let indexed = index(bytes.as_bytes()).unwrap();
        assert_eq!(indexed.chunks[0].range.end(), (CHUNK - 1) as u64);
        assert_eq!(indexed.chunks[1].text, "é-tail");
    }

    #[test]
    fn txt_bibliography_separates_known_and_unknown() {
        let source = b"Prose RFC 999\n\n1. References\n\n   [RFC0010] Test\n   [RFC20] \"A comparison, RFC 777, and future work\"\n   [Incomplete] Unresolved\n";
        let parsed = index(source).unwrap();
        assert_eq!(parsed.all_references, Coverage::Partial);
        assert_eq!(parsed.classified_references, Coverage::Partial);
        assert_eq!(
            parsed
                .references
                .iter()
                .map(|edge| edge.target.get())
                .collect::<Vec<_>>(),
            [10, 20]
        );
        assert!(
            parsed.references[0]
                .range
                .select(source)
                .unwrap()
                .starts_with(b"   [RFC0010]")
        );
    }

    #[test]
    fn undotted_contents_entries_do_not_produce_sections() {
        let source = b"Contents\n\n1. Introduction\n2. Contents-only heading\n\n1. Introduction\n\n   Body.\n";
        let parsed = index(source).unwrap();
        assert_eq!(parsed.headings, ["Introduction"]);
        let heading = parsed.sections[0].raw_heading;
        let label = b"1. Introduction\n";
        assert_eq!(
            heading.start() as usize,
            source
                .windows(label.len())
                .rposition(|slice| slice == label)
                .unwrap()
        );
    }
}
