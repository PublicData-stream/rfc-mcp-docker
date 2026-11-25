use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use rfc_core::{GenerationId, RfcNumber};
use rfc_storage::postgres::{
    GenerationReadGuard, GenerationSelection, GenerationUnavailable, Reader,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::Semaphore,
    time::{Instant, timeout_at},
};
use tokio_postgres::error::SqlState;

use crate::{
    cursor::{Cursor, CursorKey, MAX_AGE_SECONDS},
    query::{self, Predicate, QueryError},
    snippets::{self, Source},
    sql::{self, Plan},
};

const DEFAULT_LIMIT: u16 = 20;
const MAX_LIMIT: u16 = 100;
const MAX_DOCUMENTS: usize = 512;
const BATCH: usize = 32;
const MAX_READ_BYTES: usize = 32 * 1024 * 1024;
const MAX_COMPARISON_BYTES: usize = 512 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 256 * 1024;
const RESPONSE_HEADROOM: usize = 1024;
const DEADLINE: Duration = Duration::from_secs(15);

#[derive(Debug)]
pub enum SearchError {
    InvalidArguments,
    RfcNotFound,
    SectionNotFound,
    AmbiguousSection(Vec<String>),
    InvalidByteRange,
    InvalidUtf8Boundary,
    UnsupportedSourceEncoding,
    SourcePolicyRestricted,
    SourceUnavailable,
    Query(QueryError),
    InvalidLimit,
    CursorInvalid,
    CursorExpired,
    GenerationUnavailable,
    CorpusNotReady,
    InvalidRegex,
    SandboxUnavailable,
    Cancelled,
    Deadline,
    ResourceLimit,
    Unavailable,
}

impl SearchError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidArguments => "invalid_arguments",
            Self::RfcNotFound => "rfc_not_found",
            Self::SectionNotFound => "section_not_found",
            Self::AmbiguousSection(_) => "ambiguous_section",
            Self::InvalidByteRange => "invalid_byte_range",
            Self::InvalidUtf8Boundary => "invalid_utf8_boundary",
            Self::UnsupportedSourceEncoding => "unsupported_source_encoding",
            Self::SourcePolicyRestricted => "source_policy_restricted",
            Self::SourceUnavailable => "source_unavailable",
            Self::Query(error) => error.code,
            Self::InvalidLimit => "resource_limit_exceeded",
            Self::CursorInvalid => "cursor_invalid",
            Self::CursorExpired => "cursor_expired",
            Self::GenerationUnavailable => "generation_unavailable",
            Self::CorpusNotReady => "corpus_not_ready",
            Self::InvalidRegex => "invalid_regex",
            Self::SandboxUnavailable => "sandbox_unavailable",
            Self::Cancelled => "deadline_exceeded",
            Self::Deadline => "deadline_exceeded",
            Self::ResourceLimit => "resource_limit_exceeded",
            Self::Unavailable => "search_unavailable",
        }
    }
}

impl fmt::Display for SearchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Self::Query(error) = self {
            return error.fmt(f);
        }
        let message = match self {
            Self::InvalidArguments => "Check the tool schema and send a single valid selection",
            Self::RfcNotFound => "Choose an RFC with original TXT in this generation",
            Self::SectionNotFound => {
                "Use table to find an indexed section or try an explicit range"
            }
            Self::AmbiguousSection(_) => {
                "Choose a candidate section ID instead of the heading text"
            }
            Self::InvalidByteRange => "Use zero-based half-open byte offsets within the original",
            Self::InvalidUtf8Boundary => "Choose UTF-8 character boundaries or request base64",
            Self::UnsupportedSourceEncoding => "Request base64 for this original",
            Self::SourcePolicyRestricted => "Use the official source URL for this selection",
            Self::SourceUnavailable => {
                "The original could not be verified; use the official source URL"
            }
            Self::Query(_) => unreachable!(),
            Self::InvalidLimit => "Choose a page limit from 1 to 100",
            Self::CursorInvalid => "Restart with the original search arguments and no cursor",
            Self::CursorExpired | Self::GenerationUnavailable => {
                "Restart the search without a cursor"
            }
            Self::CorpusNotReady => "Retry when a corpus generation is published",
            Self::InvalidRegex => "Use a valid single-line default-engine regular expression",
            Self::SandboxUnavailable => "The ripgrep worker sandbox is unavailable",
            Self::Cancelled => "Search was cancelled",
            Self::Deadline => "Retry with a narrower query",
            Self::ResourceLimit => "Search exceeded its resource budget. Try a narrower query",
            Self::Unavailable => "Search is temporarily unavailable",
        };
        write!(f, "{}: {message}", self.code())
    }
}

impl std::error::Error for SearchError {}

impl From<QueryError> for SearchError {
    fn from(error: QueryError) -> Self {
        Self::Query(error)
    }
}

impl From<tokio_postgres::Error> for SearchError {
    fn from(error: tokio_postgres::Error) -> Self {
        match error.code() {
            Some(code)
                if *code == SqlState::QUERY_CANCELED || *code == SqlState::LOCK_NOT_AVAILABLE =>
            {
                Self::Deadline
            }
            _ => Self::Unavailable,
        }
    }
}

pub(crate) fn repository_error(
    error: Box<dyn std::error::Error + Send + Sync>,
    pinned: bool,
) -> SearchError {
    if error.is::<GenerationUnavailable>() {
        return if pinned {
            SearchError::GenerationUnavailable
        } else {
            SearchError::CorpusNotReady
        };
    }
    if error.is::<tokio::time::error::Elapsed>() {
        return SearchError::Deadline;
    }
    match error.downcast::<SearchError>() {
        Ok(error) => *error,
        Err(error) => error
            .downcast::<tokio_postgres::Error>()
            .map_or(SearchError::Unavailable, |error| (*error).into()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    pub query: String,
    pub limit: Option<u16>,
    pub cursor: Option<String>,
    #[serde(default)]
    pub include_snippets: bool,
}

#[derive(Debug, Serialize)]
pub struct SearchPage {
    pub schema_version: u16,
    pub generation_id: String,
    pub publication_sequence: String,
    pub hits: Vec<Hit>,
    pub coverage: Coverage,
    pub scan_truncated: bool,
    pub response_truncated: bool,
    pub next_cursor: Option<String>,
    pub notice_groups: Vec<NoticeGroup>,
}

#[derive(Debug, Default, Serialize)]
pub struct Coverage {
    pub evaluated: usize,
    pub unknown: usize,
    pub unknown_text: usize,
    pub unknown_relationships: usize,
    pub catalog_only: Option<i32>,
    pub snippets_omitted: usize,
    pub warnings: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub struct Hit {
    pub id: u32,
    pub title: String,
    pub stream: Option<String>,
    pub source_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_verification: Option<crate::snippets::SourceVerification>,
    pub digest: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub excerpts: Vec<Excerpt>,
}

#[derive(Debug, Serialize)]
pub struct OriginalRange {
    pub start_byte: u64,
    pub end_byte: u64,
    #[serde(flatten)]
    pub(crate) representation: Representation,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub(crate) enum Representation {
    Text { content: String },
    Base64 { content_base64: String },
}

#[derive(Debug, Serialize)]
pub struct Excerpt {
    #[serde(flatten)]
    pub original: OriginalRange,
    pub encoding: &'static str,
    pub notice_group_id: String,
    pub match_range: [u64; 2],
}

#[derive(Debug, Serialize)]
pub struct NoticeGroup {
    pub id: String,
    pub rfc_id: u32,
    pub source_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_verification: Option<crate::snippets::SourceVerification>,
    pub source_name: &'static str,
    pub stream: Option<String>,
    pub attribution: &'static str,
    pub digest: String,
    pub notices: Vec<OriginalRange>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub(crate) included_in_whole: bool,
}

pub struct Search {
    pub(crate) key: CursorKey,
    blocking_reads: Arc<Semaphore>,
}

impl Search {
    /// Create a shared engine with its own dedicated cursor key.
    pub fn new(cursor_key: &[u8; 48]) -> Self {
        Self {
            key: CursorKey::new(cursor_key),
            blocking_reads: Arc::new(Semaphore::new(2)),
        }
    }

    pub async fn run(&self, request: SearchRequest) -> Result<SearchPage, SearchError> {
        self.run_with_budget(request, |page| within_page_budget(page).unwrap_or(false))
            .await
    }

    /// The predicate reserves transport framing and continuation space as needed.
    pub async fn run_with_budget(
        &self,
        request: SearchRequest,
        fits: impl Fn(&SearchPage) -> bool + Send + Sync,
    ) -> Result<SearchPage, SearchError> {
        let deadline = Instant::now() + DEADLINE;
        timeout_at(deadline, self.run_bounded(request, &fits))
            .await
            .map_err(|_| SearchError::Deadline)?
    }

    #[cfg(feature = "phase4-fixtures")]
    #[doc(hidden)]
    pub async fn explain_fixture(query: &str) -> Result<Vec<String>, SearchError> {
        let mut plan = Plan::new(query::parse(query)?);
        let reader = Reader::connect()
            .await
            .map_err(|error| repository_error(error, false))?;
        reader
            .with_generation(GenerationSelection::Active, async |guard| {
                let generation = i64::try_from(guard.generation().get())?;
                let tx = guard.transaction();
                plan.normalize(tx).await?;
                let evaluation = plan.compile();
                let ids: Vec<i32> = tx
                    .query(sql::WINDOW, &[&generation, &0_i32, &(BATCH as i64)])
                    .await?
                    .iter()
                    .map(|row| row.get(0))
                    .collect();
                let through = ids.last().copied().unwrap_or(0);
                let raw = vec![vec![2_u8; plan.exact.len()]; ids.len()];
                let statement = format!("EXPLAIN (ANALYZE, BUFFERS) {}", evaluation.sql);
                Ok(tx
                    .query(
                        &statement,
                        &evaluation.bind([&generation, &0_i32, &through, &ids, &raw]),
                    )
                    .await?
                    .iter()
                    .map(|row| row.get(0))
                    .collect())
            })
            .await
            .map_err(|error| repository_error(error, false))
    }

    async fn run_bounded(
        &self,
        request: SearchRequest,
        fits: &(impl Fn(&SearchPage) -> bool + Sync),
    ) -> Result<SearchPage, SearchError> {
        let limit = request.limit.unwrap_or(DEFAULT_LIMIT);
        if !(1..=MAX_LIMIT).contains(&limit) {
            return Err(SearchError::InvalidLimit);
        }
        let plan = Plan::new(query::parse(&request.query)?);
        let fingerprint = plan.fingerprint(limit, request.include_snippets);
        let now = unix_time()?;
        let cursor = request
            .cursor
            .as_deref()
            .map(|encoded| self.key.decode(encoded, &fingerprint, now))
            .transpose()?;
        let selection = match &cursor {
            Some(cursor) => GenerationSelection::Pinned(
                GenerationId::new(cursor.generation).ok_or(SearchError::CursorInvalid)?,
            ),
            None => GenerationSelection::Active,
        };
        let pinned = cursor.is_some();
        let reader = Reader::connect()
            .await
            .map_err(|error| repository_error(error, pinned))?;
        let mut scan = Scan {
            options: PageOptions {
                limit,
                include_snippets: request.include_snippets,
            },
            plan,
            budget: Budget::default(),
            fits,
        };
        reader
            .with_generation(selection, async |guard| {
                let generation = i64::try_from(guard.generation().get())
                    .map_err(|_| SearchError::Unavailable)?;
                let sequence = cursor
                    .as_ref()
                    .map_or(guard.publication_sequence(), |value| value.sequence);
                let expiry = cursor
                    .as_ref()
                    .map_or(now + MAX_AGE_SECONDS, |value| value.expiry);
                let after = cursor.as_ref().map_or(0, |value| value.after);
                let (mut page, stop) = scan.run(self, guard, generation, sequence, after).await?;
                if let Some(after) = stop {
                    if expiry <= unix_time()? {
                        return Err(SearchError::CursorExpired.into());
                    }
                    page.next_cursor = Some(self.key.encode(&Cursor::new(
                        fingerprint,
                        generation,
                        sequence,
                        after,
                        expiry,
                    ))?);
                }
                if serialized_size(&page)? > MAX_RESPONSE_BYTES {
                    return Err(SearchError::ResourceLimit.into());
                }
                Ok(page)
            })
            .await
            .map_err(|error| repository_error(error, pinned))
    }

    pub(crate) async fn read_original<T: Send + 'static>(
        &self,
        guard: &GenerationReadGuard<'_>,
        id: RfcNumber,
        length: usize,
        process: impl FnOnce(Vec<u8>) -> T + Send + 'static,
    ) -> Result<Option<T>, SearchError> {
        let permit = self
            .blocking_reads
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| SearchError::Unavailable)?;
        let original = match guard.open_original(id).await {
            Ok(file) => file,
            Err(error) if error.is::<std::io::Error>() => return Ok(None),
            Err(error) => return Err(repository_error(error, true)),
        };
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            original.read_verified(length).ok().map(process)
        })
        .await
        .map_err(|_| SearchError::Unavailable)
    }
}

pub(crate) fn unix_time() -> Result<u64, SearchError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| SearchError::Unavailable)
}

#[derive(Default)]
struct Budget {
    bytes_read: usize,
    bytes_compared: usize,
}

impl Budget {
    fn charge(&mut self, bytes: usize, comparisons: usize) -> bool {
        let bytes_read = self.bytes_read.saturating_add(bytes);
        let bytes_compared = self
            .bytes_compared
            .saturating_add(bytes.saturating_mul(comparisons));
        if bytes_read > MAX_READ_BYTES || bytes_compared > MAX_COMPARISON_BYTES {
            return false;
        }
        self.bytes_read = bytes_read;
        self.bytes_compared = bytes_compared;
        true
    }
}

struct PageOptions {
    limit: u16,
    include_snippets: bool,
}

struct Scan<'a> {
    options: PageOptions,
    plan: Plan,
    budget: Budget,
    fits: &'a (dyn Fn(&SearchPage) -> bool + Sync),
}

impl Scan<'_> {
    async fn run(
        &mut self,
        search: &Search,
        guard: &GenerationReadGuard<'_>,
        generation: i64,
        sequence: i64,
        mut after: i32,
    ) -> Result<(SearchPage, Option<i32>), SearchError> {
        let tx = guard.transaction();
        self.plan.normalize(tx).await?;
        let evaluation = self.plan.compile();
        let sql = tx.prepare(&evaluation.sql).await?;
        let window = tx.prepare(sql::WINDOW).await?;
        let mut page = SearchPage {
            schema_version: 1,
            generation_id: generation.to_string(),
            publication_sequence: sequence.to_string(),
            hits: Vec::new(),
            coverage: Coverage::default(),
            scan_truncated: false,
            response_truncated: false,
            next_cursor: None,
            notice_groups: Vec::new(),
        };
        page.coverage.catalog_only = tx.query_opt(
            "SELECT jsonb_array_length(catalog_only) FROM rfc.generation_ingestion WHERE generation_id=$1",
            &[&generation],
        ).await?.map(|row| row.get(0));

        let patterns = Arc::new(
            self.plan
                .exact
                .iter()
                .map(|&slot| {
                    let Predicate::Text(_, _, literal) = &self.plan.leaves[slot].predicate else {
                        unreachable!()
                    };
                    memchr::memmem::Finder::new(literal.as_bytes()).into_owned()
                })
                .collect::<Vec<_>>(),
        );

        let more = 'scan: loop {
            let count = (MAX_DOCUMENTS - page.coverage.evaluated).min(BATCH);
            let rows = tx
                .query(&window, &[&generation, &after, &((count + 1) as i64)])
                .await?;
            if rows.is_empty() {
                break false;
            }
            if count == 0 {
                page.scan_truncated = true;
                break true;
            }

            let mut ids = Vec::new();
            let mut raw = Vec::new();
            let mut cut = false;
            let mut through = after;
            for row in rows.iter().take(count) {
                let id: i32 = row.get(0);
                if !patterns.is_empty() {
                    let length = usize::try_from(row.get::<_, i64>(1))
                        .map_err(|_| SearchError::Unavailable)?;
                    if !self.budget.charge(length, patterns.len()) {
                        cut = true;
                        break;
                    }
                    let patterns = Arc::clone(&patterns);
                    let rfc = RfcNumber::new(id).ok_or(SearchError::Unavailable)?;
                    let bytes = search
                        .read_original(guard, rfc, length, move |bytes| {
                            patterns
                                .iter()
                                .map(|pattern| u8::from(pattern.find(&bytes).is_some()))
                                .collect::<Vec<_>>()
                        })
                        .await?
                        .unwrap_or_else(|| vec![2; self.plan.exact.len()]);
                    ids.push(id);
                    raw.push(bytes);
                }
                through = id;
            }
            if through == after {
                if page.coverage.evaluated == 0 {
                    return Err(SearchError::ResourceLimit);
                }
                page.scan_truncated = true;
                break true;
            }
            let batch = tx
                .query(
                    &sql,
                    &evaluation.bind([&generation, &after, &through, &ids, &raw]),
                )
                .await?;
            if batch.is_empty() || batch.last().map(|row| row.get::<_, i32>(0)) != Some(through) {
                return Err(SearchError::Unavailable);
            }
            for (index, row) in batch.iter().enumerate() {
                let truth: Option<bool> = row.get(13);
                let facts: Vec<Option<bool>> = row.get(12);
                if truth == Some(true) {
                    let source = Source::from_row(row)?;
                    if !push_hit(&mut page, &source, self.fits) {
                        if page.hits.is_empty() {
                            return Err(SearchError::ResourceLimit);
                        }
                        page.response_truncated = true;
                        break 'scan true;
                    }
                    if self.options.include_snippets {
                        self.add_snippets(search, guard, &source, &facts, &mut page)
                            .await?;
                    }
                }
                self.record_coverage(&mut page.coverage, truth, &facts);
                after = row.get(0);
                if page.hits.len() == usize::from(self.options.limit) {
                    break 'scan index + 1 < rows.len();
                }
            }
            if cut {
                page.scan_truncated = true;
                break true;
            }
            if rows.len() <= count {
                break false;
            }
        };
        if page.coverage.unknown_text > 0 || page.coverage.unknown_relationships > 0 {
            page.coverage.warnings.push("incomplete_index");
        }
        if page.coverage.catalog_only != Some(0) {
            page.coverage.warnings.push("incomplete_catalog");
        }
        if page.coverage.snippets_omitted != 0 {
            page.coverage.warnings.push("snippets_omitted");
        }
        Ok((page, more.then_some(after)))
    }

    fn record_coverage(
        &self,
        coverage: &mut Coverage,
        truth: Option<bool>,
        facts: &[Option<bool>],
    ) {
        let mut text = false;
        let mut relationships = false;
        for (leaf, fact) in self.plan.leaves.iter().zip(facts) {
            if fact.is_none() {
                match leaf.predicate {
                    Predicate::Text(..) => text = true,
                    _ => relationships = true,
                }
            }
        }
        coverage.evaluated += 1;
        coverage.unknown += usize::from(truth.is_none());
        coverage.unknown_text += usize::from(text);
        coverage.unknown_relationships += usize::from(relationships);
    }

    async fn add_snippets(
        &mut self,
        search: &Search,
        guard: &GenerationReadGuard<'_>,
        source: &Source,
        facts: &[Option<bool>],
        page: &mut SearchPage,
    ) -> Result<(), SearchError> {
        let mut witnesses = Vec::new();
        self.plan.logic.witnesses(facts, true, &mut witnesses);
        let literals = snippets::fulltext_witnesses(&self.plan, &witnesses);
        if literals.is_empty() {
            return Ok(());
        }
        if page.notice_groups.len() < 10
            && source.permits_excerpt()
            && source.length <= snippets::MAX_SOURCE_BYTES
            && self.budget.charge(source.length, literals.len())
            && let Some(bytes) = search
                .read_original(guard, source.rfc, source.length, |bytes| bytes)
                .await?
            && let Some((excerpts, notices)) =
                snippets::collect(guard.transaction(), source, &bytes, &literals).await?
        {
            page.hits.last_mut().expect("hit was appended").excerpts = excerpts;
            page.notice_groups.push(notices);
            if (self.fits)(page) {
                return Ok(());
            }
            page.hits
                .last_mut()
                .expect("hit was appended")
                .excerpts
                .clear();
            page.notice_groups.pop();
        }
        page.coverage.snippets_omitted += 1;
        Ok(())
    }
}

fn push_hit(page: &mut SearchPage, source: &Source, fits: &dyn Fn(&SearchPage) -> bool) -> bool {
    let Some(title) = &source.title else {
        return false;
    };
    page.hits.push(Hit {
        id: source.rfc.get(),
        title: title.clone(),
        stream: source.stream.clone(),
        source_url: source.url.clone(),
        source_verification: source.verification.clone(),
        digest: source.digest.to_string(),
        excerpts: Vec::new(),
    });
    if fits(page) {
        return true;
    }
    page.hits.pop();
    false
}

fn serialized_size(page: &SearchPage) -> Result<usize, SearchError> {
    serde_json::to_vec(page)
        .map(|bytes| bytes.len())
        .map_err(|_| SearchError::Unavailable)
}

fn within_page_budget(page: &SearchPage) -> Result<bool, SearchError> {
    Ok(serialized_size(page)? <= MAX_RESPONSE_BYTES - RESPONSE_HEADROOM)
}
