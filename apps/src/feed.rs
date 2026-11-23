use reqwest::{StatusCode, header};
use rfc_mcp_docker::{
    ProbeResult,
    control::{self, Client, Keys, Link, SyncRequested, jitter, unix_seconds},
};
use roxmltree::{Document, Node, ParsingOptions};
use std::{
    collections::BTreeSet,
    env,
    time::{Duration, SystemTime},
};
use tokio::time::sleep;

const ATOM_NS: &str = "http://www.w3.org/2005/Atom";
const ORIGIN: &str = "https://www.rfc-editor.org";
const FEED_LIMIT: usize = 768 * 1024;
const ENTRY_LIMIT: usize = 256;
const MIN_RETRY: Duration = Duration::from_secs(30);
const MAX_RETRY: Duration = Duration::from_secs(3600);
const MAX_RETRY_AFTER: Duration = Duration::from_secs(86400);

#[derive(Clone, Copy, Debug)]
enum Format {
    Atom,
    Rss,
}

impl Format {
    fn path(self) -> &'static str {
        match self {
            Self::Atom => "/rfcatom.xml",
            Self::Rss => "/rfcrss.xml",
        }
    }

    fn accepts(self, value: &str) -> bool {
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

#[derive(Default)]
struct Validators {
    etag: Option<header::HeaderValue>,
    modified: Option<header::HeaderValue>,
}

impl Validators {
    fn enabled(&self) -> bool {
        self.etag.is_some() || self.modified.is_some()
    }
}

enum Poll {
    Unchanged,
    Changed(BTreeSet<u32>, Validators),
    RetryAfter(Duration),
}

struct Feed {
    http: reqwest::Client,
    format: Format,
    url: String,
    interval: Duration,
    min_retry: Duration,
    validators: Validators,
    acknowledged: BTreeSet<u32>,
    pending: Option<(BTreeSet<u32>, Vec<SyncRequested>, Validators)>,
}

impl Feed {
    fn new() -> control::Result<Self> {
        let format = match env::var("RFC_FEED_FORMAT").as_deref() {
            Ok("atom") | Err(env::VarError::NotPresent) => Format::Atom,
            Ok("rss") => Format::Rss,
            _ => return Err("RFC_FEED_FORMAT requires atom or rss".into()),
        };
        let builder = reqwest::Client::builder()
            .https_only(true)
            .no_proxy()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .user_agent("rfc-mcp-docker-feed/0.0.0");
        let (builder, origin, interval, min_retry) = test_settings(builder)?;
        Ok(Self {
            http: builder.build()?,
            format,
            url: format!("{origin}{}", format.path()),
            interval,
            min_retry,
            validators: Validators::default(),
            acknowledged: BTreeSet::new(),
            pending: None,
        })
    }

    async fn poll(&self) -> control::Result<Poll> {
        let mut request = self
            .http
            .get(&self.url)
            .header(header::ACCEPT_ENCODING, "identity");
        if let Some(value) = &self.validators.etag {
            request = request.header(header::IF_NONE_MATCH, value);
        }
        if let Some(value) = &self.validators.modified {
            request = request.header(header::IF_MODIFIED_SINCE, value);
        }
        let mut response = request.send().await?;
        let status = response.status();
        if status == StatusCode::NOT_MODIFIED && self.validators.enabled() {
            return Ok(Poll::Unchanged);
        }
        if matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
        ) {
            return Ok(Poll::RetryAfter(retry_after(response.headers())));
        }
        if status != StatusCode::OK {
            return Err("unexpected feed HTTP status".into());
        }
        let headers = response.headers();
        let encoding = only(headers, header::CONTENT_ENCODING)?;
        if encoding.is_some_and(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity")) {
            return Err("unsupported feed encoding".into());
        }
        if !only(headers, header::CONTENT_TYPE)?
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| self.format.accepts(value))
        {
            return Err("unexpected feed media type".into());
        }
        let validators = Validators {
            etag: only(headers, header::ETAG)?.cloned(),
            modified: only(headers, header::LAST_MODIFIED)?.cloned(),
        };
        if response
            .content_length()
            .is_some_and(|length| length > FEED_LIMIT as u64)
        {
            return Err("feed exceeds byte limit".into());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if chunk.len() > FEED_LIMIT - body.len() {
                return Err("feed exceeds byte limit".into());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Poll::Changed(publications(self.format, &body)?, validators))
    }

    async fn cycle(&mut self, client: &mut Client) -> control::Result<Option<Duration>> {
        if self.pending.is_none() {
            let (publications, validators) = match self.poll().await? {
                Poll::Unchanged => return Ok(None),
                Poll::RetryAfter(duration) => return Ok(Some(duration)),
                Poll::Changed(publications, validators) => (publications, validators),
            };
            let now = unix_seconds()?;
            let events = publications
                .difference(&self.acknowledged)
                .map(|&rfc| SyncRequested::publication(rfc, now))
                .collect();
            self.pending = Some((publications, events, validators));
        }
        let (_, events, _) = self.pending.as_mut().expect("pending feed poll");
        while let Some(event) = events.last() {
            client.request_sync(event).await?;
            events.pop();
        }
        let (publications, _, validators) = self.pending.take().expect("completed feed poll");
        self.acknowledged = publications;
        self.validators = validators;
        Ok(None)
    }
}

fn only(
    headers: &header::HeaderMap,
    name: header::HeaderName,
) -> control::Result<Option<&header::HeaderValue>> {
    let values = headers.get_all(name);
    let mut values = values.iter();
    let first = values.next();
    if values.next().is_some() || first.is_some_and(|value| value.as_bytes().len() > 4096) {
        return Err("duplicate or oversized feed header".into());
    }
    Ok(first)
}

fn retry_after(headers: &header::HeaderMap) -> Duration {
    let value = only(headers, header::RETRY_AFTER)
        .ok()
        .flatten()
        .and_then(|value| value.to_str().ok());
    let duration = value.and_then(|value| {
        value.parse().map(Duration::from_secs).ok().or_else(|| {
            httpdate::parse_http_date(value)
                .ok()?
                .duration_since(SystemTime::now())
                .ok()
        })
    });
    duration.unwrap_or_default().min(MAX_RETRY_AFTER)
}

fn publications(format: Format, body: &[u8]) -> control::Result<BTreeSet<u32>> {
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
        Format::Atom if root.has_tag_name((ATOM_NS, "feed")) => {
            (root, (ATOM_NS, "entry"), (ATOM_NS, "id"))
        }
        Format::Rss if has_tag(root, ("", "rss")) => {
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
) -> control::Result<Node<'a, 'input>> {
    let mut children = parent.children().filter(|node| has_tag(*node, tag));
    let first = children.next().ok_or("missing required feed field")?;
    if children.next().is_some() {
        return Err("duplicate feed field".into());
    }
    Ok(first)
}

fn test_settings(
    builder: reqwest::ClientBuilder,
) -> control::Result<(reqwest::ClientBuilder, &'static str, Duration, Duration)> {
    #[cfg(feature = "phase6-fixtures")]
    if env::var("RFC_PHASE6_FIXTURES").as_deref() == Ok("1") {
        let cert = reqwest::Certificate::from_pem(&std::fs::read("/phase2/ca.crt")?)?;
        return Ok((
            builder
                .tls_built_in_root_certs(false)
                .add_root_certificate(cert),
            "https://fixtures:8443",
            Duration::from_secs(2),
            Duration::from_secs(1),
        ));
    }
    Ok((builder, ORIGIN, Duration::from_secs(900), MIN_RETRY))
}

pub async fn run() -> ProbeResult {
    let mut feed = Feed::new()?;
    let target = control::address(&env::var("RFC_FEED_SYNC_ADDR")?)?;
    let mut client = Client::new(target, Link::FeedSync, Keys::load(Link::FeedSync)?);
    let mut retry = feed.min_retry;
    loop {
        match feed.cycle(&mut client).await {
            Ok(None) => {
                retry = feed.min_retry;
                sleep(jitter(feed.interval)).await;
            }
            result => {
                let delay = match result {
                    Ok(Some(delay)) => delay,
                    Err(_) => {
                        eprintln!("feed poll or hint delivery failed");
                        Duration::ZERO
                    }
                    Ok(None) => unreachable!(),
                };
                sleep(jitter(delay.max(retry))).await;
                retry = (retry * 2).min(MAX_RETRY);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rss_and_atom_produce_the_same_stable_ids() {
        let atom = br#"<feed xmlns="http://www.w3.org/2005/Atom"><entry><id>https://www.rfc-editor.org/info/rfc8174/</id></entry><entry><id>https://www.rfc-editor.org/info/rfc2119/</id></entry></feed>"#;
        let rss = br#"<rss><channel><item><guid>https://www.rfc-editor.org/info/rfc2119/</guid></item><item><guid>https://www.rfc-editor.org/info/rfc8174/</guid></item><item><guid>https://www.rfc-editor.org/info/rfc2119/</guid></item></channel></rss>"#;
        assert_eq!(
            publications(Format::Atom, atom).unwrap(),
            publications(Format::Rss, rss).unwrap()
        );
        assert_eq!(
            SyncRequested::publication(2119, 123).event_id,
            SyncRequested::publication(2119, 456).event_id
        );
        assert!(publications(Format::Rss, atom).is_err());
    }

    #[test]
    fn feed_rejects_malformed_oversized_and_external_xml() {
        for body in [
            b"<rss><channel></rss>".as_slice(),
            br#"<!DOCTYPE rss [<!ENTITY leak SYSTEM "file:///etc/passwd">]><rss><channel>&leak;</channel></rss>"#,
            br#"<rss xmlns:xi="http://www.w3.org/2001/XInclude"><channel><xi:include href="/etc/passwd"/></channel></rss>"#,
            br#"<rss><channel><item><guid>https://example.org/info/rfc2119/</guid></item></channel></rss>"#,
        ] {
            assert!(publications(Format::Rss, body).is_err());
        }
        assert!(publications(Format::Rss, &vec![0; FEED_LIMIT + 1]).is_err());
    }
}
