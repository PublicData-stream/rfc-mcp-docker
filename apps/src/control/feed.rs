use roxmltree::{Document, Node, ParsingOptions};
use std::collections::BTreeSet;

const ATOM_NS: &str = "http://www.w3.org/2005/Atom";
pub const FEED_LIMIT: usize = 768 * 1024;
pub(super) const ENTRY_LIMIT: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedFormat {
    Atom,
    Rss,
}

impl FeedFormat {
    pub fn path(self) -> &'static str {
        match self {
            Self::Atom => "/rfcatom.xml",
            Self::Rss => "/rfcrss.xml",
        }
    }

    pub fn accepts(self, value: &str) -> bool {
        let media_type = value.split(';').next().unwrap_or_default().trim();
        let allowed = match self {
            Self::Atom => "application/atom+xml",
            Self::Rss => "application/rss+xml",
        };
        [allowed, "application/xml", "text/xml"]
            .iter()
            .any(|expected| media_type.eq_ignore_ascii_case(expected))
    }
}

pub fn publications(format: FeedFormat, body: &[u8]) -> super::Result<BTreeSet<u32>> {
    if body.len() > FEED_LIMIT {
        return Err("feed exceeds byte limit".into());
    }
    let text = std::str::from_utf8(body)?;
    let doc = Document::parse_with_options(
        text,
        ParsingOptions {
            nodes_limit: 25_000,
            ..Default::default()
        },
    )?;
    let root = doc.root_element();
    if root
        .descendants()
        .any(|node| node.has_tag_name(("http://www.w3.org/2001/XInclude", "include")))
    {
        return Err("unsupported external feed include".into());
    }
    let (parent, entry, identifier) = match format {
        FeedFormat::Atom if root.has_tag_name((ATOM_NS, "feed")) => {
            (root, (ATOM_NS, "entry"), (ATOM_NS, "id"))
        }
        FeedFormat::Rss if has_tag(root, ("", "rss")) => {
            (single(root, ("", "channel"))?, ("", "item"), ("", "guid"))
        }
        _ => return Err("unexpected feed document type".into()),
    };
    let mut publications = BTreeSet::new();
    for (index, item) in parent
        .children()
        .filter(|node| has_tag(*node, entry))
        .enumerate()
    {
        if index >= ENTRY_LIMIT {
            return Err("feed entry count exceeds limit".into());
        }
        let id = single(item, identifier)?;
        let value = id.text().ok_or("feed identifier lacks text")?.trim();
        let number = value
            .strip_prefix("https://www.rfc-editor.org/info/rfc")
            .ok_or("unrecognized RFC publication identifier")?;
        let number = number.strip_suffix('/').unwrap_or(number);
        let rfc: i32 = number.parse()?;
        if rfc <= 0 || rfc.to_string() != number || id.children().count() != 1 {
            return Err("invalid RFC publication identifier".into());
        }
        publications.insert(rfc as u32);
    }
    Ok(publications)
}

fn has_tag(node: Node<'_, '_>, (namespace, name): (&str, &str)) -> bool {
    node.is_element()
        && node.tag_name().name() == name
        && node.tag_name().namespace().unwrap_or_default() == namespace
}

fn single<'a, 'input>(
    parent: Node<'a, 'input>,
    tag: (&str, &str),
) -> super::Result<Node<'a, 'input>> {
    let mut children = parent.children().filter(|node| has_tag(*node, tag));
    let first = children.next().ok_or("missing required feed field")?;
    if children.next().is_some() {
        return Err("duplicate feed field".into());
    }
    Ok(first)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::SyncRequested;

    #[test]
    fn rss_and_atom_produce_the_same_stable_ids() {
        let atom = br#"<feed xmlns="http://www.w3.org/2005/Atom"><entry><id>https://www.rfc-editor.org/info/rfc8174/</id></entry><entry><id>https://www.rfc-editor.org/info/rfc2119/</id></entry></feed>"#;
        let rss = br#"<rss><channel><item><guid>https://www.rfc-editor.org/info/rfc2119/</guid></item><item><guid>https://www.rfc-editor.org/info/rfc8174/</guid></item><item><guid>https://www.rfc-editor.org/info/rfc2119/</guid></item></channel></rss>"#;
        assert_eq!(
            publications(FeedFormat::Atom, atom).unwrap(),
            publications(FeedFormat::Rss, rss).unwrap()
        );
        assert_eq!(
            SyncRequested::publication(2119, 123).event_id,
            SyncRequested::publication(2119, 456).event_id
        );
        assert!(publications(FeedFormat::Rss, atom).is_err());
    }

    #[test]
    fn feed_rejects_malformed_oversized_and_external_xml() {
        for body in [
            b"<rss><channel></rss>".as_slice(),
            br#"<!DOCTYPE rss [<!ENTITY leak SYSTEM "file:///etc/passwd">]><rss><channel>&leak;</channel></rss>"#,
            br#"<rss xmlns:xi="http://www.w3.org/2001/XInclude"><channel><xi:include href="/etc/passwd"/></channel></rss>"#,
            br#"<rss><channel><item><guid>https://example.org/info/rfc2119/</guid></item></channel></rss>"#,
        ] {
            assert!(publications(FeedFormat::Rss, body).is_err());
        }
        assert!(publications(FeedFormat::Rss, &vec![0; FEED_LIMIT + 1]).is_err());
    }
}
