#[cfg(feature = "phase6-fixtures")]
use reqwest::{StatusCode, header};
#[cfg(feature = "phase6-fixtures")]
use rfc_mcp_docker::control::{FEED_LIMIT, publications};
use rfc_mcp_docker::{
    ProbeResult,
    control::{
        self, Client, FeedFormat, FeedPoll, FeedReply, Keys, Link, SyncRequested, feed_snapshot,
        jitter, unix_seconds,
    },
};
use std::{collections::BTreeSet, env, time::Duration};
use tokio::time::sleep;

const MIN_RETRY: Duration = Duration::from_secs(30);
const MAX_RETRY: Duration = Duration::from_secs(3600);
#[cfg(feature = "phase6-fixtures")]
const MAX_RETRY_AFTER: Duration = Duration::from_secs(86400);

#[cfg(feature = "phase6-fixtures")]
#[derive(Default)]
struct Validators {
    etag: Option<header::HeaderValue>,
    modified: Option<header::HeaderValue>,
}

#[cfg(feature = "phase6-fixtures")]
impl Validators {
    fn enabled(&self) -> bool {
        self.etag.is_some() || self.modified.is_some()
    }
}

enum Poll {
    Pending,
    Unchanged,
    Changed(BTreeSet<u32>),
    RetryAfter(Duration),
}

struct Feed {
    #[cfg(feature = "phase6-fixtures")]
    direct: Option<Direct>,
    format: FeedFormat,
    interval: Duration,
    min_retry: Duration,
    acknowledged: BTreeSet<u32>,
    snapshot: Option<String>,
    pending: Option<(BTreeSet<u32>, Vec<SyncRequested>)>,
}

impl Feed {
    fn new() -> control::Result<Self> {
        let format = match env::var("RFC_FEED_FORMAT").as_deref() {
            Ok("atom") | Err(env::VarError::NotPresent) => FeedFormat::Atom,
            Ok("rss") => FeedFormat::Rss,
            _ => return Err("RFC_FEED_FORMAT requires atom or rss".into()),
        };
        let interval = Duration::from_secs(900);
        let min_retry = MIN_RETRY;
        #[cfg(feature = "phase6-fixtures")]
        let (direct, interval, min_retry) = fixture_poll(interval, min_retry)?;
        Ok(Self {
            #[cfg(feature = "phase6-fixtures")]
            direct,
            format,
            interval,
            min_retry,
            acknowledged: BTreeSet::new(),
            snapshot: None,
            pending: None,
        })
    }

    async fn poll(&mut self, client: &mut Client) -> control::Result<Poll> {
        #[cfg(feature = "phase6-fixtures")]
        if let Some(direct) = &mut self.direct {
            return direct.poll(self.format).await;
        }
        let request = FeedPoll {
            format: self.format,
            known: self.snapshot.clone(),
        };
        Ok(match client.poll_feed(&request).await? {
            FeedReply::Unchanged => Poll::Unchanged,
            FeedReply::Pending => Poll::Pending,
            FeedReply::Retry(seconds) => Poll::RetryAfter(Duration::from_secs(seconds)),
            FeedReply::Publications(ids) => Poll::Changed(ids.into_iter().collect()),
        })
    }

    async fn cycle(&mut self, client: &mut Client) -> control::Result<Option<Duration>> {
        if self.pending.is_none() {
            let publications = match self.poll(client).await? {
                Poll::Pending => return Ok(Some(Duration::from_secs(2))),
                Poll::Unchanged => return Ok(None),
                Poll::RetryAfter(duration) => return Ok(Some(duration)),
                Poll::Changed(publications) => publications,
            };
            let now = unix_seconds()?;
            let events = publications
                .difference(&self.acknowledged)
                .map(|&rfc| SyncRequested::publication(rfc, now))
                .collect();
            self.pending = Some((publications, events));
        }
        let (_, events) = self.pending.as_mut().expect("pending feed poll");
        while let Some(event) = events.last() {
            client.request_sync(event).await?;
            events.pop();
        }
        let (publications, _) = self.pending.take().expect("completed feed poll");
        self.snapshot = Some(feed_snapshot(publications.iter().copied()));
        self.acknowledged = publications;
        Ok(None)
    }
}

#[cfg(feature = "phase6-fixtures")]
struct Direct {
    http: reqwest::Client,
    validators: Validators,
    publications: BTreeSet<u32>,
}

#[cfg(feature = "phase6-fixtures")]
impl Direct {
    async fn poll(&mut self, format: FeedFormat) -> control::Result<Poll> {
        let mut request = self
            .http
            .get(format!("https://fixtures:8443{}", format.path()))
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
            return Ok(Poll::Changed(self.publications.clone()));
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
            .is_some_and(|value| format.accepts(value))
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
        self.publications = publications(format, &body)?;
        self.validators = validators;
        Ok(Poll::Changed(self.publications.clone()))
    }
}

#[cfg(feature = "phase6-fixtures")]
fn fixture_poll(
    interval: Duration,
    retry: Duration,
) -> control::Result<(Option<Direct>, Duration, Duration)> {
    if env::var("RFC_DISCOVERY_FIXTURES").as_deref() == Ok("1") {
        return Ok((None, Duration::from_secs(3), Duration::from_secs(1)));
    }
    if env::var("RFC_PHASE6_FIXTURES").as_deref() != Ok("1") {
        return Ok((None, interval, retry));
    }
    let cert = reqwest::Certificate::from_pem(&std::fs::read("/phase2/ca.crt")?)?;
    let http = reqwest::Client::builder()
        .https_only(true)
        .no_proxy()
        .retry(reqwest::retry::never())
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .tls_built_in_root_certs(false)
        .add_root_certificate(cert)
        .build()?;
    Ok((
        Some(Direct {
            http,
            validators: Validators::default(),
            publications: BTreeSet::new(),
        }),
        Duration::from_secs(2),
        Duration::from_secs(1),
    ))
}

#[cfg(feature = "phase6-fixtures")]
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

#[cfg(feature = "phase6-fixtures")]
fn retry_after(headers: &header::HeaderMap) -> Duration {
    let value = only(headers, header::RETRY_AFTER)
        .ok()
        .flatten()
        .and_then(|value| value.to_str().ok());
    let duration = value.and_then(|value| {
        value.parse().map(Duration::from_secs).ok().or_else(|| {
            httpdate::parse_http_date(value)
                .ok()?
                .duration_since(std::time::SystemTime::now())
                .ok()
        })
    });
    duration.unwrap_or_default().min(MAX_RETRY_AFTER)
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
            Ok(Some(delay)) => sleep(jitter(delay)).await,
            Err(_) => {
                eprintln!("feed poll or hint delivery failed");
                sleep(jitter(retry)).await;
                retry = (retry * 2).min(MAX_RETRY);
            }
        }
    }
}
