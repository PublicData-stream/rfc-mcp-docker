//! Bounded parsing of official catalog, RFCXML and feed inputs.

use crate::parse::{Budget, BudgetExceeded};
use crate::upstream::CATALOG_LIMIT;
use crate::upstream::XML_LIMIT;
use rfc_core::Coverage;
use rfc_core::{RelationKind, RfcNumber};
use rfc_storage::postgres::Result;
use roxmltree::{Document, Node, ParsingOptions};
use std::collections::{BTreeMap, BTreeSet};

const CATALOG_NS: &str = "https://www.rfc-editor.org/rfc-index";
const INCLUDE_NS: &str = "http://www.w3.org/2001/XInclude";
const ATOM_NS: &str = "http://www.w3.org/2005/Atom";
type Element<'a, 'input> = Node<'a, 'input>;

pub(crate) struct Catalog {
    pub entries: BTreeMap<RfcNumber, Entry>,
    pub edges: Vec<CatalogEdge>,
    pub incomplete: BTreeSet<(RfcNumber, &'static str)>,
}

#[derive(serde::Serialize)]
pub(crate) struct Entry {
    pub title: String,
    pub stream: Option<String>,
    pub date: Option<String>,
    pub month: Option<String>,
    pub status: Option<String>,
    pub has_txt: bool,
}

pub(crate) struct CatalogEdge {
    pub source: RfcNumber,
    pub kind: RelationKind,
    pub target: RfcNumber,
    pub locator: String,
}

pub(crate) fn catalog(bytes: &[u8]) -> Result<Catalog> {
    let document = parse(bytes, CATALOG_LIMIT, 1_500_000)?;
    let root = document.root_element();
    if !root.has_tag_name((CATALOG_NS, "rfc-index"))
        || root
            .descendants()
            .any(|node| node.has_tag_name((INCLUDE_NS, "include")))
    {
        return Err("unexpected RFC catalog XML structure".into());
    }
    let mut result = Catalog {
        entries: BTreeMap::new(),
        edges: Vec::new(),
        incomplete: BTreeSet::new(),
    };
    let mut seen = BTreeSet::new();
    for node in root.children().filter(Node::is_element) {
        if node.has_tag_name((CATALOG_NS, "rfc-not-issued-entry")) {
            let id = catalog_id(node)?;
            if !seen.insert(id) {
                return Err("duplicate RFC catalog number".into());
            }
            continue;
        }
        if !node.has_tag_name((CATALOG_NS, "rfc-entry")) {
            if ["std-entry", "bcp-entry", "fyi-entry"]
                .iter()
                .any(|name| node.has_tag_name((CATALOG_NS, *name)))
            {
                continue;
            }
            return Err("unexpected RFC catalog entry type".into());
        }
        let id = catalog_id(node)?;
        if !seen.insert(id) {
            return Err("duplicate RFC catalog number".into());
        }
        let title = cat_text(node, "title")?.ok_or("missing RFC catalog title")?;
        let stream = cat_text(node, "stream")?.map(str::to_owned);
        let status = cat_text(node, "current-status")?.map(str::to_owned);
        let (date, month) = publication_date(node)?;
        let has_txt = match single(node, (CATALOG_NS, "format"))? {
            Some(format) => {
                let formats: Vec<_> = format
                    .children()
                    .filter(|n| n.has_tag_name((CATALOG_NS, "file-format")))
                    .collect();
                formats.is_empty()
                    || formats
                        .iter()
                        .any(|n| n.text().is_some_and(|value| value.trim() == "TXT"))
            }
            None => true,
        };
        for (field, kind, inverse) in [
            ("updates", RelationKind::Updates, false),
            ("updated-by", RelationKind::Updates, true),
            ("obsoletes", RelationKind::Obsoletes, false),
            ("obsoleted-by", RelationKind::Obsoletes, true),
        ] {
            let Some(list) = single(node, (CATALOG_NS, field))? else {
                continue;
            };
            let mut nonempty = false;
            for target in list.children().filter(Node::is_element) {
                nonempty = true;
                let other = target
                    .has_tag_name((CATALOG_NS, "doc-id"))
                    .then(|| target.text().and_then(catalog_number))
                    .flatten();
                if let Some(other) = other {
                    let (source, target_id) = if inverse { (other, id) } else { (id, other) };
                    result.edges.push(CatalogEdge {
                        source,
                        target: target_id,
                        kind,
                        locator: locator(target),
                    });
                } else {
                    result.incomplete.insert((id, kind.as_str()));
                }
            }
            if !nonempty {
                result.incomplete.insert((id, kind.as_str()));
            }
        }
        result.entries.insert(
            id,
            Entry {
                title: title.into(),
                stream,
                date,
                month,
                status,
                has_txt,
            },
        );
    }
    if result.entries.is_empty() {
        return Err("RFC catalog contains no issued RFCs".into());
    }
    Ok(result)
}

fn catalog_id(node: Element<'_, '_>) -> Result<RfcNumber> {
    cat_text(node, "doc-id")?
        .and_then(catalog_number)
        .ok_or_else(|| "invalid RFC catalog document number".into())
}

fn catalog_number(text: &str) -> Option<RfcNumber> {
    number(text.trim().strip_prefix("RFC")?)
}

pub(crate) fn number(text: &str) -> Option<RfcNumber> {
    if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()) {
        RfcNumber::new(text.parse().ok()?)
    } else {
        None
    }
}

fn cat_text<'a>(node: Element<'a, '_>, name: &str) -> Result<Option<&'a str>> {
    let Some(field) = single(node, (CATALOG_NS, name))? else {
        return Ok(None);
    };
    if field.children().any(|child| child.is_element()) {
        return Err("nested content in RFC catalog scalar".into());
    }
    Ok(field
        .text()
        .map(str::trim)
        .filter(|value| !value.is_empty()))
}

fn single<'a, 'i>(node: Element<'a, 'i>, tag: (&str, &str)) -> Result<Option<Element<'a, 'i>>> {
    let mut children = node.children().filter(|child| child.has_tag_name(tag));
    let first = children.next();
    if children.next().is_some() {
        return Err(format!("duplicate XML field {}", tag.1).into());
    }
    Ok(first)
}

fn publication_date(node: Element<'_, '_>) -> Result<(Option<String>, Option<String>)> {
    let Some(date) = single(node, (CATALOG_NS, "date"))? else {
        return Ok((None, None));
    };
    let months = [
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
    let month = cat_text(date, "month")?
        .and_then(|name| months.iter().position(|month| *month == name))
        .ok_or("invalid RFC catalog publication month")?
        + 1;
    let year: u16 = cat_text(date, "year")?
        .ok_or("missing RFC catalog publication year")?
        .parse()?;
    if !(1..=9999).contains(&year) {
        return Err("invalid RFC catalog publication year".into());
    }
    let Some(day) = cat_text(date, "day")? else {
        return Ok((None, Some(format!("{year:04}-{month:02}-01"))));
    };
    let day: u8 = day.parse()?;
    let month_days = match month {
        2 if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) => {
            29
        }
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if !(1..=month_days).contains(&day) {
        return Err("invalid RFC catalog publication day".into());
    }
    Ok((Some(format!("{year:04}-{month:02}-{day:02}")), None))
}

pub(crate) struct XmlReference {
    pub kind: RelationKind,
    pub target: RfcNumber,
    #[cfg_attr(not(feature = "phase2-fixtures"), allow(dead_code))]
    pub locator: String,
}

pub(crate) struct XmlReferences {
    pub edges: Vec<XmlReference>,
    pub all: Coverage,
    pub classified: Coverage,
}

pub(crate) fn references(
    bytes: &[u8],
    expected: RfcNumber,
    budget: &Budget<'_>,
) -> Result<XmlReferences> {
    let document = parse(bytes, XML_LIMIT, 250_000)?;
    let root = document.root_element();
    if !root.has_tag_name("rfc")
        || root.tag_name().namespace().is_some()
        || root.attribute("number").and_then(number) != Some(expected)
    {
        return Err("RFCXML document identity does not match expected RFC".into());
    }
    let back = root.children().find(|node| node.has_tag_name("back"));
    let mut incomplete = (back.is_none()
        && !root.children().any(|node| node.has_tag_name("middle")))
        || root
            .descendants()
            .any(|node| node.has_tag_name((INCLUDE_NS, "include")));
    let mut unclassified = false;
    let mut edges = Vec::new();
    let mut work = 8_000_000usize;
    let mut visit = || -> Result<()> {
        budget.shutdown.check()?;
        work = work.checked_sub(1).ok_or(BudgetExceeded)?;
        Ok(())
    };
    for reference in back.into_iter().flat_map(|back| back.descendants()) {
        visit()?;
        if !reference.has_tag_name("reference") {
            continue;
        }
        let mut in_group = false;
        let mut classification_from_parent = None;
        for group in reference.ancestors() {
            visit()?;
            if !group.has_tag_name("references") {
                continue;
            }
            in_group = true;
            let mut title = group.attribute("title");
            if title.is_none() {
                for name in group.children() {
                    visit()?;
                    if name.has_tag_name("name") {
                        title = name.text();
                        break;
                    }
                }
            }
            classification_from_parent = title.and_then(classification);
            if classification_from_parent.is_some() {
                break;
            }
        }
        if !in_group {
            continue;
        }
        let mut explicit_rfc = false;
        let mut non_rfc = false;
        let mut targets = BTreeSet::new();
        for series in reference.descendants() {
            visit()?;
            if !series.has_tag_name("seriesInfo") {
                continue;
            }
            match series.attribute("name") {
                Some("RFC") => {
                    explicit_rfc = true;
                    if let Some(target) = series
                        .attribute("value")
                        .and_then(|value| number(value).or_else(|| catalog_number(value)))
                    {
                        budget.reserve(targets.len() * 64 + edges.len() * 128 + 64)?;
                        targets.insert(target);
                    } else {
                        incomplete = true;
                    }
                }
                Some(_)
                    if series
                        .attribute("value")
                        .is_some_and(|value| !value.is_empty()) =>
                {
                    non_rfc = true
                }
                _ => {}
            }
        }
        if !explicit_rfc {
            let anchor = reference.attribute("anchor").unwrap_or("");
            let looks_like_rfc = anchor
                .strip_prefix("RFC")
                .is_some_and(|tail| tail.as_bytes().first().is_some_and(u8::is_ascii_digit))
                || reference.attribute("target").is_some_and(rfc_link);
            if looks_like_rfc
                || (!non_rfc && reference.attribute("target").is_none_or(str::is_empty))
            {
                incomplete = true;
            }
        }
        let kind = classification_from_parent.unwrap_or(RelationKind::UnclassifiedReference);
        unclassified |= !targets.is_empty() && kind == RelationKind::UnclassifiedReference;
        let target_cost = targets.len() * 64;
        for target in targets {
            budget.reserve(target_cost + (edges.len() + 1) * 128)?;
            edges.push(XmlReference {
                kind,
                target,
                locator: locator(reference),
            });
        }
    }
    let all = if incomplete {
        Coverage::Partial
    } else {
        Coverage::Complete
    };
    let classified = if incomplete || unclassified {
        Coverage::Partial
    } else {
        Coverage::Complete
    };
    Ok(XmlReferences {
        edges,
        all,
        classified,
    })
}

pub(crate) fn classification(name: &str) -> Option<RelationKind> {
    if name.trim().eq_ignore_ascii_case("normative references") {
        Some(RelationKind::NormativeReference)
    } else if name.trim().eq_ignore_ascii_case("informative references") {
        Some(RelationKind::InformativeReference)
    } else {
        None
    }
}

pub(crate) fn rfc_link(value: &str) -> bool {
    value.contains("/rfc/") || value.contains("/info/rfc") || value.contains("10.17487/RFC")
}

fn locator(node: Element<'_, '_>) -> String {
    let range = node.range();
    format!("xml-bytes:{}:{}", range.start, range.end)
}

fn parse(bytes: &[u8], byte_limit: usize, nodes_limit: u32) -> Result<Document<'_>> {
    if bytes.is_empty() || bytes.len() > byte_limit {
        return Err("XML input exceeds bounds".into());
    }
    let text = std::str::from_utf8(bytes)?;
    Ok(Document::parse_with_options(
        text,
        ParsingOptions {
            allow_dtd: false,
            nodes_limit,
            ..ParsingOptions::default()
        },
    )?)
}

/// Checks feed syntax and its envelope. Feed contents never select a fetch URL.
pub fn validate_feed(bytes: &[u8]) -> Result<()> {
    let document = parse(bytes, 768 * 1024, 25_000)?;
    let root = document.root_element();
    if root
        .descendants()
        .any(|node| node.has_tag_name((INCLUDE_NS, "include")))
    {
        return Err("external feed includes are unsupported".into());
    }
    if root.has_tag_name((ATOM_NS, "feed")) {
        for entry in root
            .children()
            .filter(|node| node.has_tag_name((ATOM_NS, "entry")))
        {
            if single(entry, (ATOM_NS, "id"))?
                .and_then(|id| id.text())
                .is_none_or(|id| id.trim().is_empty())
            {
                return Err("Atom entry lacks an identifier".into());
            }
        }
    } else if root.has_tag_name(("", "rss"))
        || (root.has_tag_name("rss") && root.tag_name().namespace().is_none())
    {
        if root
            .children()
            .filter(|node| node.has_tag_name("channel") && node.tag_name().namespace().is_none())
            .count()
            != 1
        {
            return Err("RSS feed must contain one channel".into());
        }
    } else {
        return Err("unknown feed document type".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_xml_has_no_coverage() {
        assert!(validate_feed(b"<rss><channel></rss>").is_err());
        assert!(validate_feed(b"<rss/>").is_err());
        assert!(validate_feed(b"<rss><channel/></rss>").is_ok());
        assert!(
            validate_feed(br#"<feed xmlns="http://www.w3.org/2005/Atom"><entry/></feed>"#).is_err()
        );
        assert!(
            catalog(br#"<!DOCTYPE rfc-index [<!ENTITY x SYSTEM "file:///tmp/no">]><rfc-index/>"#)
                .is_err()
        );
        assert!(
            catalog(br#"<rfc-index xmlns="https://www.rfc-editor.org/rfc-index"><rfc-entry>"#)
                .is_err()
        );
    }

    #[test]
    fn rfcxml_classifies_nested_groups_and_retains_incomplete_state() {
        let bytes = br#"<rfc number="100"><back><references><name>References</name>
        <references><name>Normative References</name><reference><seriesInfo name="RFC" value="101"/></reference></references>
        <references title="Informative References"><referencegroup><reference><seriesInfo name="RFC" value="102"/></reference></referencegroup></references>
        <reference><seriesInfo name="RFC" value="104"/></reference><reference anchor="RFC999"/>
        </references></back></rfc>"#;
        let parsed = references(
            bytes,
            RfcNumber::new(100).unwrap(),
            &Budget {
                bytes: crate::parse::DOCUMENT_BYTES,
                shutdown: &crate::shutdown::Shutdown::new(),
            },
        )
        .unwrap();
        assert_eq!(parsed.all, Coverage::Partial);
        assert_eq!(parsed.classified, Coverage::Partial);
        assert_eq!(
            parsed
                .edges
                .iter()
                .map(|edge| (edge.kind, edge.target.get()))
                .collect::<Vec<_>>(),
            [
                (RelationKind::NormativeReference, 101),
                (RelationKind::InformativeReference, 102),
                (RelationKind::UnclassifiedReference, 104)
            ]
        );
    }
}
