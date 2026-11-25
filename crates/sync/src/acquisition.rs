//! Bounded HTTPS acquisition under the discovery and publication fence.

use crate::{
    config::SyncConfig,
    egress::ROOT,
    parse::{Budget, BudgetExceeded, DOCUMENT_BYTES},
    shutdown::Shutdown,
    text,
    upstream::{Capture, Download, FetchAttempts, MissingXml, Source, Upstream},
    xml,
};
use rfc_core::{BlobDigest, Coverage, RfcNumber};
use rfc_storage::{
    blob::BlobReader,
    postgres::{Result, mirror_root},
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::{self, Read},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{io::AsyncWriteExt, task::JoinSet};

pub(crate) struct Accepted {
    used: AtomicU64,
    limit: u64,
}

impl Accepted {
    pub(crate) fn new(limit: u64) -> Self {
        Self {
            used: AtomicU64::new(0),
            limit,
        }
    }

    pub(crate) fn charge(&self, bytes: u64) -> Result<()> {
        self.used
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|&next| next <= self.limit)
            })
            .map_err(|_| LocalFailure("acquisition exceeds accepted byte budget".into()))?;
        Ok(())
    }

    pub(crate) fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    pub(crate) fn attempt(&self) -> Attempt<'_> {
        Attempt {
            accepted: self,
            bytes: 0,
        }
    }
}

pub(crate) struct Attempt<'a> {
    accepted: &'a Accepted,
    bytes: u64,
}

impl Attempt<'_> {
    pub(crate) fn charge(&mut self, bytes: u64) -> Result<()> {
        self.accepted.charge(bytes)?;
        self.bytes += bytes;
        Ok(())
    }

    pub(crate) fn retain(mut self) {
        self.bytes = 0;
    }
}

impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        self.accepted.used.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub(crate) struct LocalFailure(pub(crate) Box<dyn std::error::Error + Send + Sync>);

impl std::fmt::Display for LocalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for LocalFailure {}

#[derive(Clone, Serialize)]
pub(crate) struct Original {
    #[serde(serialize_with = "serialize_digest")]
    pub digest: BlobDigest,
    pub length: u64,
    pub observed_us: i64,
    pub fetched_us: i64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub observation: Option<String>,
}

fn serialize_digest<S: serde::Serializer>(
    digest: &BlobDigest,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.collect_str(digest)
}

impl Original {
    pub(crate) fn captured(capture: Capture) -> Result<Self> {
        let (digest, length) = capture.staged.ok_or("missing staged HTTPS body")?;
        Ok(Self {
            digest,
            length,
            observed_us: capture.observed_us,
            fetched_us: capture.fetched_us,
            etag: capture.etag,
            last_modified: capture.last_modified,
            observation: None,
        })
    }
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Classification {
    Captured,
    Reused,
    NotFound,
    Gone,
    Malformed,
    RetriesExhausted,
    FetchFailed,
}

#[derive(Serialize)]
struct Receipt {
    rfc: u32,
    kind: &'static str,
    source_url: String,
    classification: Classification,
    audit_state: &'static str,
    original: Option<Original>,
    file: Option<String>,
    coverage: Option<(&'static str, &'static str)>,
    contains_nul: bool,
    invalid_utf8: bool,
    reference_digest: Option<String>,
}

#[derive(Default, Serialize)]
pub(crate) struct Counts {
    txt_downloaded: usize,
    txt_reused: usize,
    txt_sealed: usize,
    txt_audited: usize,
    txt_contains_nul: usize,
    txt_invalid_utf8: usize,
    xml_classified: usize,
    xml_captured: usize,
    xml_reused: usize,
    xml_not_found: usize,
    xml_gone: usize,
    xml_malformed: usize,
    xml_retries_exhausted: usize,
    xml_fetch_failed: usize,
    membership_committed: usize,
    pub accepted_bytes: u64,
    staged_body_bytes: u64,
    stage_peak_jobs: usize,
    raw_reservation_peak_bytes: u64,
    parser_reservation_peak_bytes: u64,
    receipt_bytes: u64,
    content_digest: Option<String>,
    txt_queue_peak: usize,
    xml_queue_peak: usize,
}

impl Counts {
    fn record(&mut self, receipt: &Receipt) {
        if receipt.kind == "txt" {
            match receipt.classification {
                Classification::Captured => self.txt_downloaded += 1,
                Classification::Reused => self.txt_reused += 1,
                _ => unreachable!("required TXT reached the ledger without an original"),
            }
            self.txt_sealed += 1;
            self.txt_contains_nul += usize::from(receipt.contains_nul);
            self.txt_invalid_utf8 += usize::from(receipt.invalid_utf8);
        } else {
            self.xml_classified += 1;
            match receipt.classification {
                Classification::Captured => self.xml_captured += 1,
                Classification::Reused => self.xml_reused += 1,
                Classification::NotFound => self.xml_not_found += 1,
                Classification::Gone => self.xml_gone += 1,
                Classification::Malformed => self.xml_malformed += 1,
                Classification::RetriesExhausted => self.xml_retries_exhausted += 1,
                Classification::FetchFailed => self.xml_fetch_failed += 1,
            }
        }
        if let Some(original) = &receipt.original
            && receipt.file.is_some()
        {
            self.staged_body_bytes += original.length;
        }
    }
}

/// At most one run directory exists under the corpus coordinator.
pub(crate) struct Spool(PathBuf);

impl Spool {
    pub(crate) async fn sweep() -> Result<()> {
        tokio::task::spawn_blocking(|| {
            let path = Path::new(ROOT).join("acquisition");
            match fs::symlink_metadata(&path) {
                Ok(metadata)
                    if metadata.is_dir() && metadata.permissions().mode() & 0o777 == 0o700 =>
                {
                    fs::remove_dir_all(&path)?;
                    File::open(ROOT)?.sync_all()?;
                    Ok(())
                }
                Ok(_) => Err("unexpected HTTPS acquisition spool entry".into()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            }
        })
        .await?
    }

    pub(crate) async fn open() -> Result<Self> {
        tokio::task::spawn_blocking(|| {
            let path = Path::new(ROOT).join("acquisition");
            fs::DirBuilder::new().mode(0o700).create(&path)?;
            File::open(ROOT)?.sync_all()?;
            Ok(Self(path))
        })
        .await?
    }

    pub(crate) async fn stage_catalog(&self, name: &'static str, capture: &Capture) -> Result<()> {
        let path = self.0.join(name);
        let root = self.0.clone();
        let metadata_path = self.0.join(format!("{name}.json"));
        let bytes = capture.bytes.clone();
        let observed_us = capture.observed_us;
        let fetched_us = capture.fetched_us;
        let etag = capture.etag.clone();
        let last_modified = capture.last_modified.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o400)
                .open(path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            let receipt = Original {
                digest: BlobDigest::hash(&bytes),
                length: bytes.len() as u64,
                observed_us,
                fetched_us,
                etag,
                last_modified,
                observation: None,
            };
            let mut metadata = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o400)
                .open(metadata_path)?;
            serde_json::to_writer(&mut metadata, &receipt)?;
            metadata.sync_all()?;
            File::open(root)?.sync_all()?;
            Ok(())
        })
        .await?
    }

    pub(crate) async fn remove(self) -> Result<()> {
        tokio::task::spawn_blocking(move || {
            fs::remove_dir_all(self.0)?;
            File::open(ROOT)?.sync_all()?;
            Ok(())
        })
        .await?
    }
}

pub(crate) struct Selection {
    pub header: serde_json::Value,
    pub expected: Vec<RfcNumber>,
    pub txt: BTreeMap<RfcNumber, Original>,
    pub xml: BTreeMap<RfcNumber, Original>,
    pub accepted: Arc<Accepted>,
}

impl Spool {
    pub(crate) async fn acquire(
        &self,
        selection: Selection,
        config: &SyncConfig,
        upstream: &Upstream,
        shutdown: &Shutdown,
        counts: &mut Counts,
    ) -> Result<()> {
        let options = &config.ingestion.pipeline;
        let jobs = options
            .max_active_documents
            .min(options.result_queue_items)
            .min(options.raw_payload_budget_bytes / crate::config::RAW_PER_DOCUMENT)
            .min(options.parse_budget_bytes / DOCUMENT_BYTES as u64) as usize;
        let mut ledger = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(self.0.join("receipts.jsonl"))
            .await?;
        let mut ledger_bytes = 0;
        write_record(&mut ledger, &mut ledger_bytes, &selection.header).await?;
        let mut txt = selection.expected.iter().copied();
        let mut xml = selection.expected.iter().copied();
        let mut txt_queue = VecDeque::new();
        let mut xml_queue = VecDeque::new();
        let mut work = JoinSet::new();
        let mut failure = None;
        // Fixed-size summaries suffice to check duplicates and derive a stable
        // digest without retaining response bodies or validators in memory.
        let mut summaries = BTreeMap::new();
        let mut prefer_txt = true;
        while failure.is_none() || !work.is_empty() {
            if failure.is_none() {
                for (queue, ids, cap) in [
                    (&mut txt_queue, &mut txt, options.txt_queue_items as usize),
                    (&mut xml_queue, &mut xml, options.xml_queue_items as usize),
                ] {
                    queue.extend(ids.take(cap - queue.len()));
                }
                counts.txt_queue_peak = counts.txt_queue_peak.max(txt_queue.len());
                counts.xml_queue_peak = counts.xml_queue_peak.max(xml_queue.len());
                while work.len() < jobs && shutdown.check().is_ok() {
                    let pick_txt = if prefer_txt {
                        !txt_queue.is_empty()
                    } else {
                        xml_queue.is_empty()
                    };
                    let source = if pick_txt {
                        txt_queue.pop_front().map(Source::Txt)
                    } else {
                        xml_queue.pop_front().map(Source::Xml)
                    };
                    let Some(source) = source else { break };
                    prefer_txt = !pick_txt;
                    let prior = match source {
                        Source::Txt(id) => selection.txt.get(&id),
                        Source::Xml(id) => selection.xml.get(&id),
                        _ => unreachable!(),
                    }
                    .cloned();
                    let worker = Worker {
                        root: self.0.clone(),
                        upstream: upstream.clone(),
                        shutdown: shutdown.clone(),
                        accepted: selection.accepted.clone(),
                    };
                    work.spawn(async move { worker.acquire(source, prior).await });
                }
                counts.stage_peak_jobs = counts.stage_peak_jobs.max(work.len());
            }
            match work.join_next().await {
                Some(Ok(Ok(receipt))) if failure.is_none() => {
                    let semantics = serde_json::to_vec(&(
                        receipt.classification,
                        receipt
                            .original
                            .as_ref()
                            .map(|original| original.digest.to_string()),
                        &receipt.coverage,
                        &receipt.reference_digest,
                        receipt.contains_nul,
                        receipt.invalid_utf8,
                    ));
                    match semantics {
                        Ok(semantics)
                            if summaries
                                .insert((receipt.rfc, receipt.kind), BlobDigest::hash(&semantics))
                                .is_none() => {}
                        Ok(_) => {
                            failure = Some("duplicate HTTPS artifact receipt".into());
                            shutdown.stop();
                            continue;
                        }
                        Err(error) => {
                            failure = Some(error.into());
                            shutdown.stop();
                            continue;
                        }
                    }
                    counts.record(&receipt);
                    if let Err(error) = write_record(&mut ledger, &mut ledger_bytes, &receipt).await
                    {
                        failure = Some(error);
                        shutdown.stop();
                    }
                }
                Some(Ok(Err(error))) => {
                    failure.get_or_insert(error);
                    shutdown.stop();
                }
                Some(Err(error)) => {
                    failure.get_or_insert(error.into());
                    shutdown.stop();
                }
                Some(Ok(Ok(_))) => {}
                None => break,
            }
        }
        counts.accepted_bytes = selection.accepted.used();
        counts.raw_reservation_peak_bytes =
            counts.stage_peak_jobs as u64 * crate::config::RAW_PER_DOCUMENT;
        counts.parser_reservation_peak_bytes =
            counts.stage_peak_jobs.min(options.parser_workers as usize) as u64
                * DOCUMENT_BYTES as u64;
        counts.receipt_bytes = ledger_bytes;
        if let Some(error) = failure {
            return Err(error);
        }
        shutdown.check()?;
        if counts.txt_sealed != selection.expected.len()
            || counts.xml_classified != selection.expected.len()
        {
            return Err("HTTPS artifact selection did not finish".into());
        }
        let mut digest = blake3::Hasher::new_derive_key("rfc acquisition content summary v1");
        for ((rfc, kind), summary) in summaries {
            digest.update(&rfc.to_be_bytes());
            digest.update(kind.as_bytes());
            digest.update(summary.as_bytes());
        }
        counts.content_digest = Some(digest.finalize().to_hex().to_string());
        ledger.sync_all().await?;
        let root = self.0.clone();
        tokio::task::spawn_blocking(move || File::open(root)?.sync_all()).await??;
        Ok(())
    }
}

async fn write_record(
    file: &mut tokio::fs::File,
    total: &mut u64,
    value: &impl Serialize,
) -> Result<()> {
    let mut line = serde_json::to_vec(value)?;
    line.push(b'\n');
    *total += line.len() as u64;
    if *total > 448 * 1024 * 1024 {
        return Err("HTTPS receipt ledger exceeds spool budget".into());
    }
    file.write_all(&line).await?;
    Ok(())
}

struct Worker {
    root: PathBuf,
    upstream: Upstream,
    shutdown: Shutdown,
    accepted: Arc<Accepted>,
}

impl Worker {
    async fn acquire(&self, source: Source, prior: Option<Original>) -> Result<Receipt> {
        let id = match source {
            Source::Txt(id) | Source::Xml(id) => id,
            _ => return Err("unexpected HTTPS artifact kind".into()),
        };
        let mut receipt = Receipt {
            rfc: id.get(),
            kind: source.kind(),
            source_url: source.url(),
            classification: Classification::Captured,
            audit_state: source.audit_state(),
            original: None,
            file: None,
            coverage: None,
            contains_nul: false,
            invalid_utf8: false,
            reference_digest: None,
        };
        if let Some(prior) = prior
            && (!matches!(source, Source::Xml(_))
                || crate::upstream::now_us()?.saturating_sub(prior.fetched_us) <= 86_400_000_000)
        {
            let mut attempt = self.accepted.attempt();
            attempt.charge(prior.length)?;
            let status = self.classify(source, None, &prior).await?;
            if let Some(status) = status {
                status.apply(&mut receipt);
                receipt.original = Some(prior);
                receipt.classification = Classification::Reused;
                attempt.retain();
                return Ok(receipt);
            }
        }

        let filename = format!("{}-{}", id.get(), source.kind());
        let partial = self.root.join(format!("{filename}.part"));
        let result = self
            .upstream
            .artifact(source, &partial, &self.shutdown, &self.accepted)
            .await;
        self.shutdown.check()?;
        match result {
            Ok(Download::Captured(capture)) => {
                let original = Original::captured(capture)?;
                match self
                    .classify(source, Some(partial.clone()), &original)
                    .await?
                {
                    Some(status) => status.apply(&mut receipt),
                    None => receipt.classification = Classification::Malformed,
                }
                let target = self.root.join(&filename);
                let temporary = partial.clone();
                let _timing = self.upstream.metrics().span(crate::metrics::Stage::Install);
                tokio::task::spawn_blocking(move || {
                    fs::set_permissions(&temporary, fs::Permissions::from_mode(0o400))?;
                    File::open(&temporary)?.sync_all()?;
                    fs::rename(temporary, target)
                })
                .await??;
                receipt.file = Some(filename);
                receipt.original = Some(original);
            }
            Ok(Download::Missing(missing)) => {
                receipt.classification = match missing {
                    MissingXml::NotFound => Classification::NotFound,
                    MissingXml::Gone => Classification::Gone,
                };
            }
            Err(FetchAttempts::Exhausted(error) | FetchAttempts::Terminal(error))
                if matches!(source, Source::Txt(_)) || error.is::<LocalFailure>() =>
            {
                return Err(error);
            }
            Err(FetchAttempts::Exhausted(_)) => {
                receipt.classification = Classification::RetriesExhausted
            }
            Err(FetchAttempts::Terminal(_)) => receipt.classification = Classification::FetchFailed,
        }
        if receipt.file.is_none() {
            match tokio::fs::remove_file(&partial).await {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(receipt)
    }

    async fn classify(
        &self,
        source: Source,
        path: Option<PathBuf>,
        original: &Original,
    ) -> Result<Option<Parsed>> {
        let _permit = self.upstream.parse_slot(&self.shutdown).await?;
        let original = original.clone();
        let shutdown = self.shutdown.clone();
        let metrics = self.upstream.metrics();
        tokio::task::spawn_blocking(move || {
            let _timing = metrics.parse_start();
            let budget = Budget {
                bytes: DOCUMENT_BYTES,
                shutdown: &shutdown,
            };
            shutdown.check()?;
            let file = match path {
                Some(path) => OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NOFOLLOW)
                    .open(path)?,
                None => BlobReader::open(&mirror_root(), false)?.open_blob(original.digest)?,
            };
            let mut bytes = Vec::new();
            file.take(original.length + 1).read_to_end(&mut bytes)?;
            if bytes.len() as u64 != original.length || BlobDigest::hash(&bytes) != original.digest
            {
                return Err(
                    format!("original {} failed canonical verification", original.digest).into(),
                );
            }
            match source {
                Source::Txt(_) => {
                    let index = text::index_with_budget(&bytes, &budget)?;
                    crate::parse::validate_ranges(&bytes, &index)?;
                    Ok(Some(Parsed {
                        coverage: (index.all_references, index.classified_references),
                        contains_nul: index.contains_nul,
                        invalid_utf8: index.invalid_utf8,
                        reference_digest: reference_digest(index.references.iter().map(|edge| {
                            (
                                edge.kind,
                                edge.target,
                                format!("{}:{}", edge.range.start(), edge.range.end()),
                            )
                        })),
                    }))
                }
                Source::Xml(id) => {
                    #[cfg(feature = "phase6-fixtures")]
                    if crate::daemon::acquisition_fixture()
                        && std::env::var("RFC_PHASE2_PANIC_XML")
                            .ok()
                            .and_then(|value| value.parse().ok())
                            == Some(id.get())
                    {
                        panic!("fixture RFCXML parser panic");
                    }
                    match xml::references(&bytes, id, &budget) {
                        Ok(parsed) => Ok(Some(Parsed {
                            coverage: (parsed.all, parsed.classified),
                            contains_nul: false,
                            invalid_utf8: false,
                            reference_digest: reference_digest(
                                parsed
                                    .edges
                                    .into_iter()
                                    .map(|edge| (edge.kind, edge.target, edge.locator)),
                            ),
                        })),
                        Err(error) if error.is::<BudgetExceeded>() => Err(error),
                        Err(_) => {
                            shutdown.check()?;
                            Ok(None)
                        }
                    }
                }
                _ => Err("unexpected HTTPS parser input".into()),
            }
        })
        .await?
    }
}

struct Parsed {
    coverage: (Coverage, Coverage),
    contains_nul: bool,
    invalid_utf8: bool,
    reference_digest: String,
}

impl Parsed {
    fn apply(self, receipt: &mut Receipt) {
        receipt.coverage = Some((self.coverage.0.as_str(), self.coverage.1.as_str()));
        receipt.contains_nul = self.contains_nul;
        receipt.invalid_utf8 = self.invalid_utf8;
        receipt.reference_digest = Some(self.reference_digest);
    }
}

fn reference_digest(
    references: impl Iterator<Item = (rfc_core::RelationKind, RfcNumber, String)>,
) -> String {
    let mut hash = blake3::Hasher::new_derive_key("rfc acquisition reference summary v1");
    for (kind, target, locator) in references {
        hash.update(kind.as_str().as_bytes());
        hash.update(&target.get().to_be_bytes());
        hash.update(locator.as_bytes());
        hash.update(b"\0");
    }
    hash.finalize().to_hex().to_string()
}
