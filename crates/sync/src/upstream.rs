//! Fixed upstream destinations and bounded, private transfers.

use crate::{
    config::{SyncConfig, scratch_stats},
    metrics::{Metrics, Stage, StageLease},
    pipeline::Shutdown,
};
use reqwest::{Client, StatusCode, header};
use rfc_core::RfcNumber;
use rfc_mcp_docker::control::jitter;
use rfc_storage::{blob::BlobWriter, postgres::Result};
use std::{
    collections::BTreeSet,
    future::{Future, poll_fn},
    io::Read,
    process::Stdio,
    sync::Arc,
    task::Poll,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    process::Command,
    sync::{Mutex, Semaphore},
    time,
};

pub(crate) const TXT_LIMIT: usize = 4 * 1024 * 1024;
pub(crate) const CATALOG_LIMIT: usize = 32 * 1024 * 1024;
pub(crate) const XML_LIMIT: usize = 8 * 1024 * 1024;
const OFFICIAL_HTTPS: &str = "https://www.rfc-editor.org";
const MAX_XML_RETRY_AFTER: Duration = Duration::from_secs(30);
pub(crate) const OFFICIAL_RSYNC: &str = "rsync://rsync.rfc-editor.org/rfcs-text-only/";
pub(crate) const MAX_CATALOG_ENTRIES: usize = 20_000;

#[derive(Clone, Copy)]
pub(crate) enum Source {
    Catalog,
    Txt(RfcNumber),
    Xml(RfcNumber),
}

impl Source {
    fn path(self) -> String {
        match self {
            Self::Catalog => "/rfc-index.xml".into(),
            Self::Txt(id) => format!("/rfc/rfc{}.txt", id.get()),
            Self::Xml(id) => format!("/rfc/rfc{}.xml", id.get()),
        }
    }

    pub(crate) fn url(self) -> String {
        format!("{OFFICIAL_HTTPS}{}", self.path())
    }

    pub(crate) fn kind(self) -> &'static str {
        match self {
            Self::Catalog => "catalog",
            Self::Txt(_) => "txt",
            Self::Xml(_) => "rfcxml",
        }
    }

    pub(crate) fn limit(self) -> usize {
        match self {
            Self::Catalog => CATALOG_LIMIT,
            Self::Txt(_) => TXT_LIMIT,
            Self::Xml(_) => XML_LIMIT,
        }
    }

    fn accepts(self, media_type: &str) -> bool {
        let media_type = media_type.split(';').next().unwrap_or("").trim();
        match self {
            Self::Txt(_) => media_type.eq_ignore_ascii_case("text/plain"),
            _ => ["application/xml", "text/xml", "application/octet-stream"]
                .iter()
                .any(|allowed| media_type.eq_ignore_ascii_case(allowed)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Capture {
    pub bytes: Arc<Vec<u8>>,
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

pub(crate) enum XmlFailure {
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
    config: SyncConfig,
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
}

impl Upstream {
    pub(crate) fn official(config: SyncConfig) -> Result<Self> {
        Self::new(config, OFFICIAL_HTTPS, OFFICIAL_RSYNC, None)
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn fixture(config: SyncConfig) -> Result<Self> {
        let mut upstream = Self::new(
            config,
            "https://fixtures:8443",
            "rsync://fixtures:8873/rfc/",
            Some(reqwest::Certificate::from_pem(&std::fs::read(
                "/phase2/ca.crt",
            )?)?),
        )?;
        upstream.paced = std::env::var("RFC_PHASE2_PACED").as_deref() == Ok("1");
        if std::env::var("RFC_PHASE2_FIXED_CLOCK").as_deref() == Ok("1") {
            upstream.fixture_clock = Some(Arc::new(1_790_000_000_000_000.into()));
        }
        Ok(upstream)
    }

    fn new(
        config: SyncConfig,
        https_origin: &'static str,
        rsync_module: &'static str,
        ca: Option<reqwest::Certificate>,
    ) -> Result<Self> {
        let https = config.ingestion.https;
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
            .timeout(timeout)
            .user_agent("rfc-mcp-docker-sync/0.0.0");
        let paced = ca.is_none();
        if let Some(ca) = ca {
            builder = builder
                .tls_built_in_root_certs(false)
                .add_root_certificate(ca);
        }
        Ok(Self {
            config,
            http: builder.build()?,
            https_origin,
            rsync_module,
            paced,
            metrics: Metrics::default(),
            #[cfg(feature = "phase2-fixtures")]
            fixture_clock: None,
            next_request: Arc::new(Mutex::new(Some(time::Instant::now()))),
            in_flight: Arc::new(Semaphore::new(https.max_in_flight_requests)),
        })
    }

    pub(crate) fn config(&self) -> &SyncConfig {
        &self.config
    }

    fn start_interval(&self) -> Duration {
        Duration::from_millis(self.config.ingestion.https.min_request_start_interval_ms)
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

    pub(crate) async fn fetch(&self, source: Source) -> Result<Download> {
        self.fetch_once(source, false)
            .await
            .map_err(|failure| match failure {
                FetchFailure::Retryable { error, .. } | FetchFailure::Terminal(error) => error,
            })
    }

    pub(crate) async fn fetch_xml(
        &self,
        id: RfcNumber,
    ) -> std::result::Result<Download, XmlFailure> {
        let source = Source::Xml(id);
        let max_attempts = self.config.ingestion.https.rfcxml_max_attempts;
        let mut attempt = 1;
        loop {
            match self.fetch_once(source, attempt > 1).await {
                Ok(download) => return Ok(download),
                Err(FetchFailure::Terminal(error)) => return Err(XmlFailure::Terminal(error)),
                Err(FetchFailure::Retryable { error, retry_after }) => {
                    if attempt >= max_attempts
                        || retry_after.is_some_and(|delay| delay > MAX_XML_RETRY_AFTER)
                    {
                        return Err(XmlFailure::Exhausted(error));
                    }
                    time::sleep(jitter(Duration::from_secs(1 << (attempt - 1)))).await;
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
    ) -> std::result::Result<Download, FetchFailure> {
        let queue = self.metrics.span(Stage::Queue);
        let mut next;
        let _permit;
        loop {
            let permit = self
                .in_flight
                .acquire()
                .await
                .map_err(FetchFailure::terminal)?;
            let gate = self.next_request.lock().await;
            let Some(deadline) = *gate else {
                drop(gate);
                drop(permit);
                return std::future::pending().await;
            };
            if deadline <= time::Instant::now() {
                next = gate;
                _permit = permit;
                break;
            }
            drop(gate);
            drop(permit);
            time::sleep_until(deadline).await;
        }
        let clock = self.capture_clock(source);
        let observed_us = clock
            .map_or_else(now_us, Ok)
            .map_err(FetchFailure::terminal)?;
        let request = self
            .http
            .get(format!("{}{}", self.https_origin, source.path()))
            .header(header::ACCEPT_ENCODING, "identity")
            .build()
            .map_err(FetchFailure::terminal)?;
        drop(queue);
        if retry {
            self.metrics.xml_retry();
        }
        let _http = self.metrics.http_start(match source {
            Source::Catalog => Stage::Catalog,
            Source::Txt(_) => Stage::Txt,
            Source::Xml(_) => Stage::Xml,
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
        let mut response = match initial {
            Poll::Ready(result) => result,
            Poll::Pending => response.await,
        }
        .map_err(|error| FetchFailure::transport(source, error))?;
        let status = response.status();
        if status != StatusCode::OK {
            let error = format!("{} unexpected HTTPS status {status}", source.kind());
            if matches!(source, Source::Xml(_)) {
                match status {
                    StatusCode::NOT_FOUND => return Ok(Download::Missing(MissingXml::NotFound)),
                    StatusCode::GONE => return Ok(Download::Missing(MissingXml::Gone)),
                    StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => {
                        let retry_after = retry_after(response.headers(), SystemTime::now());
                        if let Some(delay) = retry_after {
                            self.delay_starts(delay).await;
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
                    _ => {}
                }
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
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(limit)
            .map_err(FetchFailure::terminal)?;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| FetchFailure::transport(source, error))?
        {
            if chunk.len() > limit - bytes.len() {
                return Err(FetchFailure::terminal("HTTPS transfer exceeds byte limit"));
            }
            self.metrics.received(chunk.len());
            bytes.extend_from_slice(&chunk);
        }
        if bytes.is_empty() {
            return Err(FetchFailure::terminal("HTTPS body is empty"));
        }
        Ok(Download::Captured(Capture {
            bytes: Arc::new(bytes),
            observed_us,
            fetched_us: clock
                .map_or_else(now_us, |us| Ok(us + 1))
                .map_err(FetchFailure::terminal)?,
            etag,
            last_modified,
        }))
    }

    fn capture_clock(&self, _source: Source) -> Option<i64> {
        #[cfg(feature = "phase2-fixtures")]
        if let Some(base) = self.fixture_clock.as_ref() {
            return Some(match _source {
                Source::Catalog => 1 + base.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                Source::Txt(id) => 1_790_000_010_000_000 + i64::from(id.get()),
                Source::Xml(id) => 1_790_000_020_000_000 + i64::from(id.get()),
            });
        }
        None
    }

    pub(crate) fn rsync_observed_us(&self) -> Result<i64> {
        #[cfg(feature = "phase2-fixtures")]
        if self.fixture_clock.is_some() {
            return Ok(1_790_000_009_000_000);
        }
        now_us()
    }

    pub(crate) async fn stage(&self, ids: &[RfcNumber], shutdown: &Shutdown) -> Result<RsyncBatch> {
        let _timing = self.metrics.span(Stage::Rsync);
        let rsync = self.config.ingestion.rsync;
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
    headers
        .get(name)
        .map(|value| {
            let value = value.to_str()?;
            if value.len() > 4096 {
                return Err("HTTPS validator exceeds byte limit".into());
            }
            Ok(value.to_owned())
        })
        .transpose()
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

pub(crate) struct RsyncBatch {
    temp: tempfile::TempDir,
    incoming: std::path::PathBuf,
    pub(crate) observed_us: i64,
    _lease: StageLease,
}

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
