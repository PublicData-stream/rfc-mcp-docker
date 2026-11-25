//! Fixed upstream destinations and bounded, private transfers.

use crate::{
    acquisition::{Accepted, LocalFailure},
    config::Https,
    egress::{Cooldown, Lease},
    metrics::{Metrics, Stage},
    request_order::{Lane, Order},
    shutdown::Shutdown,
};
#[cfg(feature = "phase2-fixtures")]
use crate::{config::scratch_stats, legacy_config::SyncConfig, metrics::StageLease};
use reqwest::{Client, StatusCode, header};
use rfc_core::{BlobDigest, RfcNumber};
use rfc_mcp_docker::control::{FEED_LIMIT, FeedFormat, jitter};
#[cfg(feature = "phase2-fixtures")]
use rfc_storage::blob::BlobWriter;
use rfc_storage::postgres::Result;
#[cfg(feature = "phase2-fixtures")]
use std::{collections::BTreeSet, io::Read, process::Stdio};
use std::{
    future::{Future, poll_fn},
    path::Path,
    sync::Arc,
    task::Poll,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[cfg(feature = "phase2-fixtures")]
use tokio::process::Command;
use tokio::{
    io::AsyncWriteExt,
    sync::{Mutex, Semaphore},
    time,
};

pub(crate) const TXT_LIMIT: usize = 4 * 1024 * 1024;
pub(crate) const CATALOG_LIMIT: usize = 32 * 1024 * 1024;
pub(crate) const XML_LIMIT: usize = 8 * 1024 * 1024;
const OFFICIAL_HTTPS: &str = "https://www.rfc-editor.org";
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);
pub(crate) const OFFICIAL_RSYNC: &str = "rsync://rsync.rfc-editor.org/rfcs-text-only/";
pub(crate) const MAX_CATALOG_ENTRIES: usize = 20_000;

#[derive(Clone, Copy)]
pub(crate) enum Source {
    Catalog,
    Txt(RfcNumber),
    Xml(RfcNumber),
    Feed(FeedFormat),
}

impl Source {
    fn path(self) -> String {
        match self {
            Self::Catalog => "/rfc-index.xml".into(),
            Self::Feed(format) => format.path().into(),
            Self::Txt(id) => format!("/rfc/rfc{}.txt", id.get()),
            Self::Xml(id) => format!("/rfc/rfc{}.xml", id.get()),
        }
    }

    pub(crate) fn url(self) -> String {
        format!("{OFFICIAL_HTTPS}{}", self.path())
    }

    pub(crate) fn audit_state(self) -> &'static str {
        match self {
            Self::Txt(_) => "pending",
            _ => "not_applicable",
        }
    }

    pub(crate) fn kind(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Feed(_) => "feed",
            Self::Txt(_) => "txt",
            Self::Xml(_) => "rfcxml",
        }
    }

    pub(crate) fn limit(self) -> usize {
        match self {
            Self::Catalog => CATALOG_LIMIT,
            Self::Feed(_) => FEED_LIMIT,
            Self::Txt(_) => TXT_LIMIT,
            Self::Xml(_) => XML_LIMIT,
        }
    }

    fn accepts(self, media_type: &str) -> bool {
        let media_type = media_type.split(';').next().unwrap_or("").trim();
        match self {
            Self::Txt(_) => media_type.eq_ignore_ascii_case("text/plain"),
            Self::Feed(format) => format.accepts(media_type),
            _ => ["application/xml", "text/xml", "application/octet-stream"]
                .iter()
                .any(|allowed| media_type.eq_ignore_ascii_case(allowed)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Capture {
    pub bytes: Arc<Vec<u8>>,
    pub staged: Option<(BlobDigest, u64)>,
    pub unchanged: bool,
    pub observed_us: i64,
    pub fetched_us: i64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

pub(crate) enum Download {
    Captured(Capture),
    Missing(MissingXml),
}

pub(crate) enum MissingXml {
    NotFound,
    Gone,
}

type Error = Box<dyn std::error::Error + Send + Sync>;

pub(crate) enum FetchAttempts {
    Exhausted(Error),
    Terminal(Error),
}

enum FetchFailure {
    Retryable {
        error: Error,
        retry_after: Option<Duration>,
    },
    Terminal(Error),
}

impl FetchFailure {
    fn terminal(error: impl Into<Error>) -> Self {
        Self::Terminal(error.into())
    }

    fn transport(source: Source, error: reqwest::Error) -> Self {
        let retryable = error.is_timeout()
            || error.is_connect()
            || error.is_request()
            || error.is_body()
            || error.is_decode();
        let error = error.without_url();
        let reason = if error.is_timeout() {
            "timeout"
        } else {
            "request failed"
        };
        let detail = std::error::Error::source(&error).unwrap_or(&error);
        let message = format!("{} HTTPS transport: {reason}: {detail}", source.kind());
        if retryable {
            Self::Retryable {
                error: message.into(),
                retry_after: None,
            }
        } else {
            Self::Terminal(message.into())
        }
    }
}

#[derive(Clone)]
pub(crate) struct Upstream {
    https: Https,
    #[cfg(feature = "phase2-fixtures")]
    legacy: Option<SyncConfig>,
    http: Client,
    https_origin: &'static str,
    rsync_module: &'static str,
    paced: bool,
    metrics: Metrics,
    #[cfg(feature = "phase2-fixtures")]
    fixture_clock: Option<Arc<std::sync::atomic::AtomicI64>>,
    // None represents a Retry-After beyond the range of the monotonic clock.
    next_request: Arc<Mutex<Option<time::Instant>>>,
    in_flight: Arc<Semaphore>,
    order: Arc<Order>,
    parsers: Arc<Semaphore>,
    cooldown: Option<Cooldown>,
}

impl Upstream {
    pub(crate) fn official(https: Https) -> Result<Self> {
        Self::new(https, OFFICIAL_HTTPS, OFFICIAL_RSYNC, None)
    }

    #[cfg(feature = "phase6-fixtures")]
    pub(crate) fn discovery_fixture(https: Https) -> Result<Self> {
        Self::fixture_client(https)
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn fixture(config: SyncConfig) -> Result<Self> {
        let legacy = config.ingestion.https;
        let mut upstream = Self::fixture_client(Https {
            max_in_flight_requests: legacy.max_in_flight_requests,
            min_request_start_interval_ms: legacy.min_request_start_interval_ms,
            request_timeout_seconds: legacy.request_timeout_seconds,
            max_attempts: legacy.rfcxml_max_attempts,
        })?;
        upstream.legacy = Some(config);
        upstream.paced = std::env::var("RFC_PHASE2_PACED").as_deref() == Ok("1");
        if std::env::var("RFC_PHASE2_FIXED_CLOCK").as_deref() == Ok("1") {
            upstream.fixture_clock = Some(Arc::new(1_790_000_000_000_000.into()));
        }
        Ok(upstream)
    }

    #[cfg(feature = "phase2-fixtures")]
    fn fixture_client(https: Https) -> Result<Self> {
        Self::new(
            https,
            "https://fixtures:8443",
            "rsync://fixtures:8873/rfc/",
            Some(reqwest::Certificate::from_pem(&std::fs::read(
                "/phase2/ca.crt",
            )?)?),
        )
    }

    fn new(
        https: Https,
        https_origin: &'static str,
        rsync_module: &'static str,
        ca: Option<reqwest::Certificate>,
    ) -> Result<Self> {
        let timeout = Duration::from_secs(https.request_timeout_seconds);
        #[cfg(feature = "phase2-fixtures")]
        let timeout = if ca.is_some()
            && let Ok(value) = std::env::var("RFC_PHASE2_HTTP_TIMEOUT_MS")
        {
            let value: u64 = value.parse()?;
            if !(1..=30_000).contains(&value) {
                return Err("invalid fixture HTTPS timeout".into());
            }
            Duration::from_millis(value)
        } else {
            timeout
        };
        let mut builder = Client::builder()
            .https_only(true)
            .no_proxy()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .pool_max_idle_per_host(https.max_in_flight_requests)
            .timeout(timeout)
            .user_agent("rfc-mcp-docker-sync/0.0.0");
        if let Some(ca) = ca {
            builder = builder
                .tls_built_in_root_certs(false)
                .add_root_certificate(ca);
        }
        Ok(Self {
            https,
            #[cfg(feature = "phase2-fixtures")]
            legacy: None,
            http: builder.build()?,
            https_origin,
            rsync_module,
            paced: true,
            metrics: Metrics::default(),
            #[cfg(feature = "phase2-fixtures")]
            fixture_clock: None,
            next_request: Arc::new(Mutex::new(Some(time::Instant::now()))),
            in_flight: Arc::new(Semaphore::new(https.max_in_flight_requests)),
            order: Arc::default(),
            parsers: Arc::new(Semaphore::new(2)),
            cooldown: None,
        })
    }

    pub(crate) fn with_parsers(mut self, parser_workers: u64) -> Self {
        self.parsers = Arc::new(Semaphore::new(parser_workers as usize));
        self
    }

    pub(crate) async fn parse_slot(
        &self,
        shutdown: &Shutdown,
    ) -> Result<tokio::sync::SemaphorePermit<'_>> {
        Ok(cancellable(Some(shutdown), self.parsers.acquire()).await??)
    }

    pub(crate) fn with_lease(mut self, lease: &Lease) -> Result<Self> {
        let (cooldown, remaining) = lease.cooldown()?;
        self.next_request = Arc::new(Mutex::new(
            time::Instant::now().checked_add(remaining.max(self.start_interval())),
        ));
        self.cooldown = Some(cooldown);
        Ok(self)
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn config(&self) -> &SyncConfig {
        self.legacy.as_ref().expect("legacy fixture upstream")
    }

    pub(crate) fn with_fresh_metrics(&self) -> Self {
        Self {
            metrics: Metrics::default(),
            ..self.clone()
        }
    }

    fn start_interval(&self) -> Duration {
        Duration::from_millis(self.https.min_request_start_interval_ms)
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn is_fixture(&self) -> bool {
        self.https_origin != OFFICIAL_HTTPS
    }

    pub(crate) fn metrics(&self) -> Metrics {
        self.metrics.clone()
    }

    pub(crate) fn rsync_module(&self) -> &'static str {
        self.rsync_module
    }

    pub(crate) async fn after_fence(&self) {
        if self.paced {
            self.delay_starts(self.start_interval()).await;
        }
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) async fn fetch(&self, source: Source) -> Result<Download> {
        self.fetch_once(source, false, None, None, None)
            .await
            .map_err(|failure| match failure {
                FetchFailure::Retryable { error, .. } | FetchFailure::Terminal(error) => error,
            })
    }

    pub(crate) async fn catalog(&self) -> Result<Capture> {
        match self.fetch_with_retries(Source::Catalog, None, None).await {
            Ok(Download::Captured(capture)) => Ok(capture),
            Ok(Download::Missing(_)) => Err("required catalog is unavailable".into()),
            Err(FetchAttempts::Exhausted(error) | FetchAttempts::Terminal(error)) => Err(error),
        }
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) async fn fetch_xml(
        &self,
        id: RfcNumber,
    ) -> std::result::Result<Download, FetchAttempts> {
        self.fetch_with_retries(Source::Xml(id), None, None).await
    }

    pub(crate) async fn artifact(
        &self,
        source: Source,
        path: &Path,
        shutdown: &Shutdown,
        accepted: &Accepted,
    ) -> std::result::Result<Download, FetchAttempts> {
        self.fetch_with_retries(source, Some((path, accepted)), Some(shutdown))
            .await
    }

    pub(crate) async fn wait_repair_egress(&self, shutdown: &Shutdown) -> Result<()> {
        loop {
            let next = self
                .next_request
                .lock()
                .await
                .ok_or("HTTPS cooldown exceeds clock range")?;
            if next.saturating_duration_since(time::Instant::now())
                <= MAX_RETRY_AFTER.max(self.start_interval())
            {
                return Ok(());
            }
            cancellable(Some(shutdown), time::sleep_until(next)).await?;
        }
    }

    pub(crate) async fn repair_txt(
        &self,
        id: RfcNumber,
        path: &Path,
        shutdown: &Shutdown,
        accepted: &Accepted,
    ) -> Result<Capture> {
        match self
            .fetch_once(
                Source::Txt(id),
                true,
                Some((path, accepted)),
                Some(shutdown),
                None,
            )
            .await
        {
            Ok(Download::Captured(capture)) => Ok(capture),
            Ok(Download::Missing(_)) => Err("required repair TXT is unavailable".into()),
            Err(FetchFailure::Retryable { error, .. } | FetchFailure::Terminal(error)) => {
                Err(error)
            }
        }
    }

    pub(crate) async fn feed(
        &self,
        format: FeedFormat,
        validators: (&Option<String>, &Option<String>),
    ) -> Result<Capture> {
        match self
            .fetch_once(Source::Feed(format), false, None, None, Some(validators))
            .await
        {
            Ok(Download::Captured(capture)) => Ok(capture),
            Ok(Download::Missing(_)) => Err("feed returned an invalid result".into()),
            Err(FetchFailure::Terminal(error) | FetchFailure::Retryable { error, .. }) => {
                Err(error)
            }
        }
    }

    async fn fetch_with_retries(
        &self,
        source: Source,
        path: Option<(&Path, &Accepted)>,
        shutdown: Option<&Shutdown>,
    ) -> std::result::Result<Download, FetchAttempts> {
        let max_attempts = self.https.max_attempts;
        let mut attempt = 1;
        loop {
            let result = self
                .fetch_once(source, attempt > 1, path, shutdown, None)
                .await;
            if let Some((partial, _)) = path
                && result.is_err()
            {
                match tokio::fs::remove_file(partial).await {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => {
                        return Err(FetchAttempts::Terminal(Box::new(LocalFailure(
                            error.into(),
                        ))));
                    }
                }
            }
            match result {
                Ok(download) => return Ok(download),
                Err(FetchFailure::Terminal(error)) => return Err(FetchAttempts::Terminal(error)),
                Err(FetchFailure::Retryable { error, retry_after }) => {
                    if attempt >= max_attempts
                        || retry_after.is_some_and(|delay| delay > MAX_RETRY_AFTER)
                    {
                        return Err(FetchAttempts::Exhausted(error));
                    }
                    cancellable(
                        shutdown,
                        time::sleep(jitter(Duration::from_secs(1 << (attempt - 1)))),
                    )
                    .await
                    .map_err(FetchAttempts::Terminal)?;
                    attempt += 1;
                }
            }
        }
    }

    async fn delay_starts(&self, delay: Duration) {
        let mut next = self.next_request.lock().await;
        *next = next
            .zip(time::Instant::now().checked_add(delay))
            .map(|(current, requested)| current.max(requested));
    }

    async fn fetch_once(
        &self,
        source: Source,
        retry: bool,
        path: Option<(&Path, &Accepted)>,
        shutdown: Option<&Shutdown>,
        conditional: Option<(&Option<String>, &Option<String>)>,
    ) -> std::result::Result<Download, FetchFailure> {
        let queue = self.metrics.span(match source {
            Source::Txt(_) => Stage::TxtQueue,
            Source::Xml(_) => Stage::XmlQueue,
            _ => Stage::Queue,
        });
        let lane = match source {
            Source::Txt(_) => Lane::Txt,
            Source::Xml(_) => Lane::Xml,
            Source::Catalog => Lane::Catalog,
            Source::Feed(_) => Lane::Feed,
        };
        let queued = cancellable(shutdown, self.order.wait(lane))
            .await
            .map_err(FetchFailure::terminal)?;
        let mut next;
        let _permit;
        loop {
            let permit = cancellable(shutdown, self.in_flight.acquire())
                .await
                .map_err(FetchFailure::terminal)?
                .map_err(FetchFailure::terminal)?;
            let gate = self.next_request.lock().await;
            let Some(deadline) = *gate else {
                drop(gate);
                drop(permit);
                return Err(FetchFailure::terminal("HTTPS cooldown exceeds clock range"));
            };
            if deadline <= time::Instant::now() {
                next = gate;
                _permit = permit;
                break;
            }
            drop(gate);
            drop(permit);
            if self.cooldown.is_some()
                && deadline.saturating_duration_since(time::Instant::now())
                    > MAX_RETRY_AFTER.max(self.start_interval())
            {
                return Err(FetchFailure::terminal(
                    "HTTPS deployment is in upstream cooldown",
                ));
            }
            cancellable(shutdown, time::sleep_until(deadline))
                .await
                .map_err(FetchFailure::terminal)?;
        }
        let clock = self.capture_clock(source);
        let observed_us = clock
            .map_or_else(now_us, Ok)
            .map_err(FetchFailure::terminal)?;
        let mut request = self
            .http
            .get(format!("{}{}", self.https_origin, source.path()))
            .header(header::ACCEPT_ENCODING, "identity");
        if retry && matches!(source, Source::Txt(_)) {
            request = request.header(header::CACHE_CONTROL, "no-cache");
        }
        if let Some((etag, last_modified)) = conditional {
            if let Some(value) = etag {
                request = request.header(header::IF_NONE_MATCH, value);
            }
            if let Some(value) = last_modified {
                request = request.header(header::IF_MODIFIED_SINCE, value);
            }
        }
        let request = request.build().map_err(FetchFailure::terminal)?;
        drop(queue);
        if retry {
            self.metrics.retry();
        }
        let _http = self.metrics.http_start(match source {
            Source::Catalog => Stage::Catalog,
            Source::Txt(_) => Stage::Txt,
            Source::Xml(_) => Stage::Xml,
            Source::Feed(_) => Stage::Feed,
        });
        let response = self.http.execute(request);
        tokio::pin!(response);
        // First-poll under the start gate, then release it before awaiting anything
        // from the network. Late scheduling cannot create a burst of starts.
        let initial = poll_fn(|context| Poll::Ready(response.as_mut().poll(context))).await;
        if self.paced {
            *next = Some(time::Instant::now() + self.start_interval());
        }
        drop(next);
        drop(queued);
        let mut response = match initial {
            Poll::Ready(result) => result,
            Poll::Pending => cancellable(shutdown, response)
                .await
                .map_err(FetchFailure::terminal)?,
        }
        .map_err(|error| FetchFailure::transport(source, error))?;
        let status = response.status();
        if status == StatusCode::NOT_MODIFIED
            && conditional.is_some_and(|(etag, modified)| etag.is_some() || modified.is_some())
        {
            return Ok(Download::Captured(Capture {
                bytes: Arc::default(),
                staged: None,
                unchanged: true,
                observed_us,
                fetched_us: now_us().map_err(FetchFailure::terminal)?,
                etag: None,
                last_modified: None,
            }));
        }
        if status != StatusCode::OK {
            let error = format!("{} unexpected HTTPS status {status}", source.kind());
            if matches!(source, Source::Xml(_)) {
                match status {
                    StatusCode::NOT_FOUND => return Ok(Download::Missing(MissingXml::NotFound)),
                    StatusCode::GONE => return Ok(Download::Missing(MissingXml::Gone)),
                    _ => (),
                }
            }
            match status {
                StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => {
                    let retry_after = retry_after(response.headers(), SystemTime::now());
                    let delay = retry_after.unwrap_or(self.start_interval());
                    let mut next = self.next_request.lock().await;
                    *next = next
                        .zip(time::Instant::now().checked_add(delay))
                        .map(|(current, proposed)| current.max(proposed));
                    if let Some(cooldown) = &self.cooldown
                        && let Err(error) = cooldown.record(delay)
                    {
                        *next = None;
                        return Err(FetchFailure::terminal(LocalFailure(error)));
                    }
                    return Err(FetchFailure::Retryable {
                        error: error.into(),
                        retry_after,
                    });
                }
                StatusCode::INTERNAL_SERVER_ERROR
                | StatusCode::BAD_GATEWAY
                | StatusCode::GATEWAY_TIMEOUT => {
                    return Err(FetchFailure::Retryable {
                        error: error.into(),
                        retry_after: None,
                    });
                }
                _ => (),
            }
            return Err(FetchFailure::terminal(error));
        }
        let headers = response.headers();
        let encoding = headers.get_all(header::CONTENT_ENCODING);
        if encoding.iter().count() > 1
            || encoding
                .iter()
                .any(|value| !value.as_bytes().eq_ignore_ascii_case(b"identity"))
        {
            return Err(FetchFailure::terminal("unsupported HTTPS content encoding"));
        }
        if !headers
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| source.accepts(value))
        {
            return Err(FetchFailure::terminal(format!(
                "{} unexpected HTTPS media type",
                source.kind()
            )));
        }
        let etag = validator(headers, header::ETAG).map_err(FetchFailure::terminal)?;
        let last_modified =
            validator(headers, header::LAST_MODIFIED).map_err(FetchFailure::terminal)?;
        let limit = source.limit();
        if response
            .content_length()
            .is_some_and(|size| size > limit as u64)
        {
            return Err(FetchFailure::terminal("HTTPS transfer exceeds byte limit"));
        }
        let result = self
            .read_body(source, &mut response, path, shutdown)
            .await?;
        Ok(Download::Captured(Capture {
            bytes: Arc::new(result.bytes),
            staged: result.staged,
            unchanged: false,
            observed_us,
            fetched_us: clock
                .map_or_else(now_us, |us| Ok(us + 1))
                .map_err(FetchFailure::terminal)?,
            etag,
            last_modified,
        }))
    }

    async fn read_body(
        &self,
        source: Source,
        response: &mut reqwest::Response,
        path: Option<(&Path, &Accepted)>,
        shutdown: Option<&Shutdown>,
    ) -> std::result::Result<Body, FetchFailure> {
        let limit = source.limit();
        let mut bytes = Vec::new();
        let mut charge = path.map(|(_, accepted)| accepted.attempt());
        let mut file = match path {
            Some((path, _)) => Some(
                tokio::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .mode(0o600)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(path)
                    .await
                    .map_err(local_failure)?,
            ),
            None => {
                bytes
                    .try_reserve_exact(limit)
                    .map_err(FetchFailure::terminal)?;
                None
            }
        };
        let expected = response.content_length();
        let mut length = 0;
        let mut hash = blake3::Hasher::new();
        while let Some(chunk) = cancellable(shutdown, response.chunk())
            .await
            .map_err(FetchFailure::terminal)?
            .map_err(|error| FetchFailure::transport(source, error))?
        {
            if chunk.len() > limit - length {
                return Err(FetchFailure::terminal("HTTPS transfer exceeds byte limit"));
            }
            self.metrics.received(
                match source {
                    Source::Txt(_) => Stage::Txt,
                    Source::Xml(_) => Stage::Xml,
                    Source::Catalog => Stage::Catalog,
                    Source::Feed(_) => Stage::Feed,
                },
                chunk.len(),
            );
            length += chunk.len();
            if let Some(file) = &mut file {
                charge
                    .as_mut()
                    .expect("staged payload budget")
                    .charge(chunk.len() as u64)
                    .map_err(FetchFailure::terminal)?;
                hash.update(&chunk);
                file.write_all(&chunk).await.map_err(local_failure)?;
            } else {
                bytes.extend_from_slice(&chunk);
            }
        }
        if length == 0 || expected.is_some_and(|size| size != length as u64) {
            return Err(FetchFailure::terminal("empty or truncated HTTPS body"));
        }
        let staged = if let Some(mut file) = file {
            let _timing = self.metrics.span(Stage::Install);
            file.flush().await.map_err(local_failure)?;
            file.sync_all().await.map_err(local_failure)?;
            charge.expect("staged payload budget").retain();
            Some((
                BlobDigest::from_bytes(*hash.finalize().as_bytes()),
                length as u64,
            ))
        } else {
            None
        };
        Ok(Body { bytes, staged })
    }

    fn capture_clock(&self, _source: Source) -> Option<i64> {
        #[cfg(feature = "phase2-fixtures")]
        if let Some(base) = self.fixture_clock.as_ref() {
            return Some(match _source {
                Source::Catalog => 1 + base.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                Source::Txt(id) => 1_790_000_010_000_000 + i64::from(id.get()),
                Source::Xml(id) => 1_790_000_020_000_000 + i64::from(id.get()),
                Source::Feed(_) => return None,
            });
        }
        None
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn rsync_observed_us(&self) -> Result<i64> {
        #[cfg(feature = "phase2-fixtures")]
        if self.fixture_clock.is_some() {
            return Ok(1_790_000_009_000_000);
        }
        now_us()
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) async fn stage(&self, ids: &[RfcNumber], shutdown: &Shutdown) -> Result<RsyncBatch> {
        let _timing = self.metrics.span(Stage::Rsync);
        let rsync = self.config().ingestion.rsync;
        if ids.is_empty() || ids.len() > rsync.batch_size {
            return Err("rsync batch count exceeds bounds".into());
        }
        let lease = self.metrics.stage_start();
        let temp = tempfile::Builder::new()
            .prefix("rfc-rsync-")
            .tempdir_in("/tmp")?;
        let files = temp.path().join("files");
        let incoming = temp.path().join("incoming");
        std::fs::create_dir(&incoming)?;
        let names: BTreeSet<_> = ids.iter().copied().map(filename).collect();
        std::fs::write(
            &files,
            names
                .iter()
                .map(|name| format!("{name}\n"))
                .collect::<String>(),
        )?;

        let mut command = Command::new("/usr/bin/rsync");
        command
            .env_clear()
            .arg("--no-motd")
            .arg("--contimeout=5")
            .arg("--timeout=30")
            .arg(format!("--max-size={TXT_LIMIT}"))
            .arg("--files-from")
            .arg(&files)
            .arg("--")
            .arg(self.rsync_module)
            .arg(&incoming)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .kill_on_drop(true);
        // Bound writes even if the peer supplies an incorrect file size.
        unsafe {
            command.pre_exec(|| {
                let limit = libc::rlimit {
                    rlim_cur: TXT_LIMIT as _,
                    rlim_max: TXT_LIMIT as _,
                };
                if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn()?;
        let mut group = ProcessGroup(child.id().map(i32::try_from).transpose()?);
        let status = tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                group.kill();
                child.wait().await?;
                return Err("rsync batch cancelled".into());
            }
            _ = time::sleep(Duration::from_secs(rsync.transfer_timeout_seconds)) => {
                group.kill();
                child.wait().await?;
                return Err("rsync batch deadline exceeded".into());
            }
            status = child.wait() => status?,
        };
        if status.success() {
            group.0 = None;
        } else {
            group.kill();
        }
        if !status.success() && !matches!(status.code(), Some(23 | 24)) {
            return Err(format!("rsync batch failed ({status})").into());
        }
        let mut seen = BTreeSet::new();
        let mut staged_bytes = 0;
        for entry in std::fs::read_dir(&incoming)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| "non-ASCII rsync filename")?;
            if !names.contains(&name) || !entry.file_type()?.is_file() {
                return Err("unexpected or non-regular rsync input".into());
            }
            let length = entry.metadata()?.len();
            if length > TXT_LIMIT as u64 {
                return Err("rsync TXT size exceeds bounds".into());
            }
            staged_bytes += length;
            seen.insert(name);
        }
        if let Some(missing) = names.difference(&seen).next() {
            return Err(format!("rsync missing expected TXT {missing}").into());
        }
        if !status.success() {
            return Err(format!("rsync batch failed ({status})").into());
        }
        self.metrics
            .staged(staged_bytes, staged_bytes, scratch_used(), seen.len(), 0);
        Ok(RsyncBatch {
            temp,
            incoming,
            observed_us: self.rsync_observed_us()?,
            _lease: lease,
        })
    }
}

fn local_failure(error: std::io::Error) -> FetchFailure {
    FetchFailure::terminal(LocalFailure(error.into()))
}

struct Body {
    bytes: Vec<u8>,
    staged: Option<(BlobDigest, u64)>,
}

async fn cancellable<T>(shutdown: Option<&Shutdown>, work: impl Future<Output = T>) -> Result<T> {
    match shutdown {
        Some(shutdown) => tokio::select! {
            biased;
            _ = shutdown.cancelled() => Err("HTTPS request cancelled".into()),
            result = work => Ok(result),
        },
        None => Ok(work.await),
    }
}

#[cfg(feature = "phase2-fixtures")]
fn scratch_used() -> Option<u64> {
    let stats = scratch_stats()?;
    stats
        .f_blocks
        .checked_sub(stats.f_bfree)?
        .checked_mul(stats.f_frsize)
}

pub(crate) fn filename(id: RfcNumber) -> String {
    format!("rfc{}.txt", id.get())
}

fn validator(headers: &header::HeaderMap, name: header::HeaderName) -> Result<Option<String>> {
    let all = headers.get_all(name);
    let mut values = all.iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    let value = value.to_str()?;
    if values.next().is_some() || value.len() > 4096 {
        return Err("duplicate or oversized HTTPS validator".into());
    }
    Ok(Some(value.to_owned()))
}

fn retry_after(headers: &header::HeaderMap, now: SystemTime) -> Option<Duration> {
    let all = headers.get_all(header::RETRY_AFTER);
    let mut values = all.iter();
    let value = values.next()?.to_str().ok()?.trim();
    if values.next().is_some() || value.is_empty() {
        return None;
    }
    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Some(Duration::from_secs(value.parse().unwrap_or(u64::MAX)));
    }
    let date = httpdate::parse_http_date(value).ok()?;
    Some(date.duration_since(now).unwrap_or_default())
}

pub(crate) fn now_us() -> Result<i64> {
    Ok(i64::try_from(
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros(),
    )?)
}

pub(crate) struct ProcessGroup(pub(crate) Option<i32>);

impl ProcessGroup {
    pub(crate) fn kill(&mut self) {
        if let Some(pid) = self.0.take() {
            // The spawned child is the leader of this private process group.
            unsafe { libc::kill(-pid, libc::SIGKILL) };
        }
    }
}

impl Drop for ProcessGroup {
    fn drop(&mut self) {
        self.kill();
    }
}

#[cfg(feature = "phase2-fixtures")]
pub(crate) struct RsyncBatch {
    temp: tempfile::TempDir,
    incoming: std::path::PathBuf,
    pub(crate) observed_us: i64,
    _lease: StageLease,
}

#[cfg(feature = "phase2-fixtures")]
impl RsyncBatch {
    pub(crate) fn take(&self, store: &BlobWriter, id: RfcNumber) -> Result<Vec<u8>> {
        let name = filename(id);
        let bytes = read_staged(store, &self.incoming, &name, None)?;
        std::fs::remove_file(self.incoming.join(name))?;
        Ok(bytes)
    }

    pub(crate) fn close(self) -> Result<()> {
        self.temp.close()?;
        Ok(())
    }
}

#[cfg(feature = "phase2-fixtures")]
pub(crate) fn read_staged(
    store: &BlobWriter,
    directory: &std::path::Path,
    name: &str,
    expected_length: Option<usize>,
) -> Result<Vec<u8>> {
    let mut file = store.open_candidate(directory, name)?;
    let length = usize::try_from(file.metadata()?.len())?;
    if length == 0
        || length > TXT_LIMIT
        || expected_length.is_some_and(|expected| expected != length)
    {
        return Err("rsync TXT size exceeds bounds or changed after inventory check".into());
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length)?;
    bytes.resize(length, 0);
    file.read_exact(&mut bytes)?;
    if file.read(&mut [0])? != 0 {
        return Err("rsync TXT changed during read".into());
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_accepts_dates_and_delays() {
        let now = UNIX_EPOCH + Duration::from_secs(1_790_000_000);
        let mut headers = header::HeaderMap::new();
        assert_eq!(retry_after(&headers, now), None);
        headers.insert(header::RETRY_AFTER, header::HeaderValue::from_static("2"));
        assert_eq!(retry_after(&headers, now), Some(Duration::from_secs(2)));
        headers.append(header::RETRY_AFTER, header::HeaderValue::from_static("1"));
        assert_eq!(retry_after(&headers, now), None);
        for offset in [0, 3, 60] {
            let date = httpdate::fmt_http_date(now + Duration::from_secs(offset));
            headers.insert(header::RETRY_AFTER, date.parse().unwrap());
            assert_eq!(
                retry_after(&headers, now),
                Some(Duration::from_secs(offset))
            );
        }
        let past = httpdate::fmt_http_date(now - Duration::from_secs(60));
        headers.insert(header::RETRY_AFTER, past.parse().unwrap());
        assert_eq!(retry_after(&headers, now), Some(Duration::ZERO));
        headers.insert(header::RETRY_AFTER, "18446744073709551616".parse().unwrap());
        assert_eq!(
            retry_after(&headers, now),
            Some(Duration::from_secs(u64::MAX))
        );
        headers.insert(header::RETRY_AFTER, "-1".parse().unwrap());
        assert_eq!(retry_after(&headers, now), None);
    }
}
