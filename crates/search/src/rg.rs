//! Bounded, generation-pinned ripgrep pages.

use std::{
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rfc_core::{BlobDigest, GenerationId};
use rfc_storage::postgres::{GenerationReadGuard, GenerationSelection, PoolClass, ReadOutcome};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, watch},
    time::{Instant, timeout_at},
};

use crate::{
    Search, SearchError,
    cursor::{MAX_AGE_SECONDS, validate_context},
    engine::{clean_rejection, repository_error, unix_time},
    snippets::{self, MAX_SOURCE_BYTES, Source, SourceVerification},
    worker::{self, Match, Output, Pattern, WorkerError},
};

const MAX_DOCUMENTS: usize = 128;
const MAX_SCAN_BYTES: usize = 32 * 1024 * 1024;
const MAX_EXCERPT_BYTES: usize = 16 * 1024;
const RESPONSE_TAIL: usize = 1024;
const DEADLINE: Duration = Duration::from_secs(5);
const SOURCES: &str = "SELECT m.rfc_id::integer,m.title,m.stream,d.txt_digest::bytea,
    d.byte_length::bigint,d.encoding,d.notice_policy,d.notice_state::text,
    o.source_url,d.identity::bytea,
    coalesce(p.method, o.verification_method) AS verification_method, p.algorithm AS verification_algorithm
    FROM rfc.generation_documents m JOIN rfc.document_versions d ON d.identity=m.document_version
    JOIN rfc.source_observations o ON o.identity=m.txt_observation
    LEFT JOIN LATERAL (
        SELECT method, algorithm FROM rfc.generation_source_evidence p
        WHERE (p.generation_id,p.rfc_id) = (m.generation_id,m.rfc_id) LIMIT 1
    ) p ON true
    WHERE m.generation_id=$1 AND m.rfc_id >= $2::integer
      AND ($3::integer[] IS NULL OR m.rfc_id=ANY($3))
    ORDER BY m.rfc_id LIMIT 129";

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    #[default]
    Regex,
    Literal,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RgRequest {
    pattern: String,
    #[serde(default)]
    mode: Mode,
    case_sensitive: Option<bool>,
    ids: Option<Vec<i32>>,
    context_lines: Option<u8>,
    limit: Option<usize>,
    cursor: Option<String>,
}

impl RgRequest {
    fn normalize(&mut self) -> Result<String, SearchError> {
        if !(1..=2048).contains(&self.pattern.len())
            || self.pattern.contains(['\0', '\n'])
            || self.ids.as_ref().is_some_and(|ids| {
                ids.is_empty() || ids.len() > 100 || ids.iter().any(|id| *id <= 0)
            })
            || self.context_lines.is_some_and(|lines| lines > 5)
            || self.limit.is_some_and(|limit| !(1..=100).contains(&limit))
            || self
                .cursor
                .as_ref()
                .is_some_and(|cursor| cursor.is_empty() || cursor.len() > 512)
        {
            return Err(SearchError::InvalidArguments);
        }
        if let Some(ids) = &mut self.ids {
            ids.sort_unstable();
            ids.dedup();
        }
        let args = serde_json::to_vec(&(
            &self.pattern,
            self.mode,
            self.case_sensitive.unwrap_or(true),
            &self.ids,
            self.context_lines.unwrap_or_default(),
            self.limit.unwrap_or(20),
        ))
        .map_err(|_| SearchError::Unavailable)?;
        Ok(BlobDigest::hash(&args).to_string())
    }

    fn pattern(&self) -> Pattern<'_> {
        Pattern {
            text: &self.pattern,
            literal: matches!(self.mode, Mode::Literal),
            sensitive: self.case_sensitive.unwrap_or(true),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
struct Position {
    rfc: i32,
    after: Option<(usize, usize)>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    v: u8,
    tool: String,
    args: String,
    generation: i64,
    sequence: i64,
    expiry: u64,
    pos: Position,
    coverage_gap: bool,
}

pub struct Rg {
    available: AtomicBool,
    workers: Arc<Semaphore>,
}

impl Rg {
    pub async fn initialize() -> Self {
        let (_sender, mut cancelled) = watch::channel(());
        let mut output = Output {
            remaining: 4096,
            matches: Vec::new(),
            after: None,
            wanted: 2,
        };
        let pattern = Pattern {
            text: "rfc-worker-check",
            literal: true,
            sensitive: true,
        };
        let available = worker::scan(
            pattern,
            b"rfc-worker-check\n",
            &mut output,
            Instant::now() + DEADLINE,
            &mut cancelled,
            None,
        )
        .await
        .is_ok()
            && output.matches.len() == 1;
        if !available {
            eprintln!("ripgrep worker is unavailable: startup confinement check failed");
        }
        Self {
            available: AtomicBool::new(available),
            workers: Arc::new(Semaphore::new(2)),
        }
    }

    pub fn available(&self) -> bool {
        self.available.load(Ordering::Relaxed)
    }

    pub async fn reap(&self) {
        let _ = self.workers.acquire_many(2).await;
    }

    pub async fn run(
        &self,
        search: &Search,
        mut request: RgRequest,
        request_deadline: Instant,
        fits: impl Fn(&Value, usize) -> bool + Send + Sync,
    ) -> Result<Value, SearchError> {
        let deadline = request_deadline.min(Instant::now() + DEADLINE);
        let scan_deadline = deadline - Duration::from_millis(100);
        let args = request.normalize()?;
        if !self.available() {
            return Err(SearchError::SandboxUnavailable);
        }
        let permit = Arc::new(
            self.workers
                .clone()
                .try_acquire_owned()
                .map_err(|_| SearchError::ResourceLimit)?,
        );
        let (_caller, mut cancelled) = watch::channel(());
        let now = unix_time()?;
        let checkpoint = request
            .cursor
            .as_deref()
            .map(|token| search.key.open::<Cursor>(token))
            .transpose()?;
        let selection = if let Some(saved) = &checkpoint {
            if saved.v != 1 || saved.tool != "rg" || saved.args != args || saved.pos.rfc < 1 {
                return Err(SearchError::CursorInvalid);
            }
            validate_context(saved.generation, saved.sequence, saved.expiry, now)?;
            GenerationSelection::Pinned(
                GenerationId::new(saved.generation).ok_or(SearchError::CursorInvalid)?,
            )
        } else {
            GenerationSelection::Active
        };
        let pinned = checkpoint.is_some();
        let result = search
            .pool
            .with_generation(PoolClass::Public, deadline, selection, async |guard| {
                let mut scan = Scan {
                    request: &request,
                    search,
                    guard,
                    fits: &fits,
                    deadline: scan_deadline,
                    cancelled: &mut cancelled,
                    remaining: worker::OUTPUT_BYTES,
                    permit,
                };
                match scan.page(args, now, checkpoint).await {
                    Ok(page)
                        if page["truncation_reason"] == "deadline"
                            || Instant::now() >= scan_deadline =>
                    {
                        Ok(ReadOutcome::Interrupted(page))
                    }
                    Ok(page) => Ok(ReadOutcome::Complete(page)),
                    Err(error) => clean_rejection(error.into()),
                }
            })
            .await
            .map_err(|error| repository_error(error, pinned));
        if matches!(result, Err(SearchError::SandboxUnavailable)) {
            self.available.store(false, Ordering::Relaxed);
        }
        result
    }
}

struct Scan<'a, 'g> {
    request: &'a RgRequest,
    search: &'a Search,
    guard: &'a GenerationReadGuard<'g>,
    fits: &'a (dyn Fn(&Value, usize) -> bool + Sync),
    deadline: Instant,
    cancelled: &'a mut watch::Receiver<()>,
    remaining: usize,
    permit: Arc<OwnedSemaphorePermit>,
}

impl Scan<'_, '_> {
    async fn scan(
        &mut self,
        input: &[u8],
        after: Option<(usize, usize)>,
        wanted: usize,
    ) -> (Vec<Match>, Result<(), WorkerError>) {
        let mut output = Output {
            remaining: self.remaining,
            matches: Vec::new(),
            after,
            wanted,
        };
        let fence = match self.guard.worker_fence() {
            Ok(fence) => fence,
            Err(_) => return (Vec::new(), Err(WorkerError::Unavailable)),
        };
        let result = worker::scan(
            self.request.pattern(),
            input,
            &mut output,
            self.deadline,
            self.cancelled,
            Some((fence, self.permit.clone())),
        )
        .await;
        self.remaining = output.remaining;
        (output.matches, result)
    }

    async fn page(
        &mut self,
        args: String,
        now: u64,
        saved: Option<Cursor>,
    ) -> Result<Value, SearchError> {
        let generation = self.guard.generation().get() as i64;
        let sequence = saved
            .as_ref()
            .map_or(self.guard.publication_sequence(), |c| c.sequence);
        let initial = saved.as_ref().map_or(Position::default(), |c| c.pos);
        let mut position = initial;
        let mut coverage_gap = saved.as_ref().is_some_and(|c| c.coverage_gap);
        let first = i64::from(initial.rfc) + i64::from(initial.after.is_none());
        let mut page = json!({
            "schema_version": 1, "generation_id": generation.to_string(),
            "publication_sequence": sequence.to_string(),
            "matches": [], "notice_groups": [], "omitted": [], "scan_complete": false,
            "documents_scanned": 0, "source_bytes_charged": 0,
            "truncation_reason": null, "next_cursor": null
        });
        let mut documents = 0;
        let mut scanned_bytes = 0;
        let mut seen = 0;
        let mut reason = None;
        let limit = self.request.limit.unwrap_or(20);
        // Validate using the same confined binary even if the scope has no eligible originals.
        if saved.is_none() && !self.request.pattern().literal {
            self.scan(&[], None, 1).await.1.map_err(worker_error)?;
        }
        let rows = if let Ok(first) = i32::try_from(first) {
            bound(
                self.cancelled,
                self.deadline,
                self.guard
                    .transaction()
                    .query(SOURCES, &[&generation, &first, &self.request.ids]),
            )
            .await??
        } else {
            Vec::new()
        };
        'documents: for (index, row) in rows.iter().enumerate() {
            if index == MAX_DOCUMENTS || Instant::now() >= self.deadline {
                reason = Some("scan_budget");
                break;
            }
            let source = Source::from_row(row)?;
            let id = source.rfc.get() as i32;
            let completed = Position {
                rfc: id,
                after: None,
            };
            let mut omission = if !source.permits_excerpt() {
                Some("source_policy_restricted")
            } else if source.length > MAX_SOURCE_BYTES {
                Some("resource_limit_exceeded")
            } else {
                None
            };
            if omission.is_none() {
                if scanned_bytes + source.length > MAX_SCAN_BYTES {
                    reason = Some("scan_budget");
                    break;
                }
                let read =
                    self.search
                        .read_original(self.guard, source.rfc, source.length, |bytes| bytes);
                let bytes = match bound(self.cancelled, self.deadline, read).await {
                    Err(SearchError::Deadline) => {
                        reason = Some("deadline");
                        break;
                    }
                    result => result??,
                };
                if let Some(bytes) = bytes {
                    let read_notices =
                        snippets::notices(self.guard.transaction(), &source, &bytes, false);
                    let notices = match bound(self.cancelled, self.deadline, read_notices).await {
                        Err(SearchError::Deadline) => {
                            reason = Some("deadline");
                            break;
                        }
                        result => result??,
                    };
                    if let Some(notices) = notices {
                        let group = json!(source.notice_group(notices, false));
                        let after = (position.rfc == id).then_some(position.after).flatten();
                        let (hits, status) = self.scan(&bytes, after, limit - seen).await;
                        if let Err(
                            error @ (WorkerError::Sandbox
                            | WorkerError::InvalidRegex
                            | WorkerError::Unavailable
                            | WorkerError::Cancelled),
                        ) = status
                        {
                            return Err(worker_error(error));
                        }
                        scanned_bytes += source.length;
                        if status.is_ok() {
                            documents += 1;
                        }
                        let mut group_added = false;
                        for hit in hits {
                            if hit.end - hit.start > MAX_EXCERPT_BYTES {
                                reason = Some("response_budget");
                                break 'documents;
                            }
                            let context = self.request.context_lines.unwrap_or_default();
                            let range = excerpt(&bytes, &hit, context);
                            let line = excerpt(&bytes, &hit, 0);
                            if !group_added {
                                page["notice_groups"]
                                    .as_array_mut()
                                    .unwrap()
                                    .push(group.clone());
                            }
                            let matches = page["matches"].as_array_mut().unwrap();
                            matches.push(hit_value(&source, &bytes, &hit, range));
                            if !(self.fits)(&page, RESPONSE_TAIL) {
                                *page["matches"].as_array_mut().unwrap().last_mut().unwrap() =
                                    hit_value(&source, &bytes, &hit, line);
                            }
                            if !(self.fits)(&page, RESPONSE_TAIL) {
                                *page["matches"].as_array_mut().unwrap().last_mut().unwrap() =
                                    hit_value(&source, &bytes, &hit, hit.start..hit.end);
                            }
                            if !(self.fits)(&page, RESPONSE_TAIL) {
                                page["matches"].as_array_mut().unwrap().pop();
                                if !group_added {
                                    page["notice_groups"].as_array_mut().unwrap().pop();
                                }
                                reason = Some("response_budget");
                                break 'documents;
                            }
                            group_added = true;
                            seen += 1;
                            position = Position {
                                rfc: id,
                                after: Some((hit.start, hit.end)),
                            };
                        }
                        if let Err(error) = status {
                            reason = Some(match error {
                                WorkerError::Deadline => "deadline",
                                WorkerError::Page => "match_limit",
                                _ => "worker_budget",
                            });
                            break;
                        }
                    } else {
                        omission = Some("source_policy_restricted");
                    }
                } else {
                    omission = Some("source_unavailable");
                }
            }
            if let Some(code) = omission {
                page["omitted"].as_array_mut().unwrap().push(json!({
                    "id": id, "source_url": source.url, "code": code
                }));
                if !(self.fits)(&page, RESPONSE_TAIL) {
                    page["omitted"].as_array_mut().unwrap().pop();
                    reason = Some("response_budget");
                    break;
                }
                coverage_gap = true;
            }
            position = completed;
        }
        page["documents_scanned"] = json!(documents);
        page["source_bytes_charged"] = json!(scanned_bytes);
        page["scan_complete"] = json!(reason.is_none() && !coverage_gap);
        if let Some(reason) = reason {
            if position == initial {
                return Err(if reason == "deadline" {
                    SearchError::Deadline
                } else {
                    SearchError::ResourceLimit
                });
            }
            page["truncation_reason"] = json!(reason);
            page["next_cursor"] = json!(self.search.key.seal(&Cursor {
                v: 1,
                tool: "rg".into(),
                args,
                generation,
                sequence,
                pos: position,
                coverage_gap,
                expiry: saved.map_or(now + MAX_AGE_SECONDS, |c| c.expiry),
            })?);
        } else if coverage_gap {
            page["truncation_reason"] = json!("source_coverage");
        }
        Ok(page)
    }
}

async fn bound<T>(
    cancelled: &mut watch::Receiver<()>,
    deadline: Instant,
    operation: impl Future<Output = T>,
) -> Result<T, SearchError> {
    tokio::select! {
        _ = cancelled.changed() => Err(SearchError::Cancelled),
        result = timeout_at(deadline, operation) => result.map_err(|_| SearchError::Deadline),
    }
}

fn worker_error(error: WorkerError) -> SearchError {
    match error {
        WorkerError::Sandbox => SearchError::SandboxUnavailable,
        WorkerError::InvalidRegex => SearchError::InvalidRegex,
        WorkerError::Budget | WorkerError::Page => SearchError::ResourceLimit,
        WorkerError::Deadline => SearchError::Deadline,
        WorkerError::Cancelled => SearchError::Cancelled,
        WorkerError::Unavailable => SearchError::Unavailable,
    }
}

fn excerpt(bytes: &[u8], hit: &Match, context: u8) -> Range<usize> {
    let mut start = hit.line.start;
    let mut end = hit.line.end;
    for _ in 0..context {
        start =
            memchr::memrchr(b'\n', &bytes[..start.saturating_sub(1)]).map_or(0, |index| index + 1);
        end = memchr::memchr(b'\n', &bytes[end..]).map_or(bytes.len(), |index| end + index + 1);
    }
    let min = hit.end.saturating_sub(MAX_EXCERPT_BYTES);
    if end - start > MAX_EXCERPT_BYTES
        && min <= hit.start
        && hit.end - hit.start <= MAX_EXCERPT_BYTES
    {
        start = start
            .max(min)
            .max(hit.start.saturating_sub(MAX_EXCERPT_BYTES / 2));
        end = end.min(start + MAX_EXCERPT_BYTES);
    }
    start..end
}

fn hit_value(source: &Source, bytes: &[u8], hit: &Match, range: Range<usize>) -> Value {
    let force_base64 = std::str::from_utf8(&bytes[range.clone()]).is_err();
    let original = snippets::original(range.clone(), &bytes[range], force_base64);
    let mut value = json!(original);
    value["id"] = json!(source.rfc.get());
    value["source_url"] = json!(source.url);
    SourceVerification::add(&mut value, source.verification.as_ref());
    value["digest"] = json!(source.digest.to_string());
    value["encoding"] = json!(if force_base64 { "base64" } else { "utf8" });
    value["notice_group_id"] = json!(format!("rfc-{}-notices", source.rfc.get()));
    value["match_range"] = json!([hit.start, hit.end]);
    value
}
