//! Bounded HTTPS acquisition under the discovery and publication fence.

use crate::{
    Observation, Writer, audit,
    checkpoint::{Context, Kind},
    config::SyncConfig,
    egress::ROOT,
    insert_observation,
    parse::{Budget, BudgetExceeded, DOCUMENT_BYTES, OwnedText},
    policy::Registry,
    register_blob,
    shutdown::Shutdown,
    upstream::{Capture, Download, FetchAttempts, MissingXml, Source, Upstream},
    xml,
};
use rfc_core::{BlobDigest, Coverage, RfcNumber};
use rfc_storage::{
    blob::{BlobReader, BlobWriter, DurableBlob},
    manifest::identity,
    postgres::{Result, mirror_root},
};
use serde::{Deserialize, Serialize};
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
use tokio::{io::AsyncWriteExt, sync::mpsc, task::JoinSet};
use tokio_postgres::GenericClient;

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

    fn replace(&self, previous: u64, current: u64) -> Result<()> {
        if current > previous {
            self.charge(current - previous)?;
        } else {
            self.used.fetch_sub(previous - current, Ordering::Relaxed);
        }
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

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct Original {
    #[serde(
        serialize_with = "serialize_digest",
        deserialize_with = "deserialize_digest"
    )]
    pub digest: BlobDigest,
    pub length: u64,
    pub observed_us: i64,
    pub fetched_us: i64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub observation: Option<String>,
    #[serde(skip)]
    pub checkpoint_expiry: Option<i64>,
    #[serde(skip)]
    pub checkpoint_version: Option<BlobDigest>,
}

fn serialize_digest<S: serde::Serializer>(
    digest: &BlobDigest,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    serializer.collect_str(digest)
}

fn deserialize_digest<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BlobDigest, D::Error> {
    let digest = String::deserialize(deserializer)?;
    BlobDigest::from_hex(&digest).ok_or_else(|| serde::de::Error::custom("invalid BLAKE3 digest"))
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
            checkpoint_expiry: None,
            checkpoint_version: None,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
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
    #[serde(skip)]
    selection: BlobDigest,
    #[serde(skip)]
    prepared: Option<Prepared>,
    document_version: Option<String>,
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
    txt_audit_pending: usize,
    checkpoint_txt: usize,
    checkpoint_xml: usize,
    documents_joined: usize,
    join_backlog_peak: usize,
    db_commits: usize,
    db_batch_peak: usize,
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
    processing_digest: Option<String>,
    txt_queue_peak: usize,
    xml_queue_peak: usize,
}

impl Counts {
    fn record(&mut self, receipt: &Receipt) {
        let checkpoint = receipt
            .original
            .as_ref()
            .is_some_and(|source| source.checkpoint_expiry.is_some());
        if receipt.kind == "txt" {
            match receipt.classification {
                Classification::Captured => self.txt_downloaded += 1,
                Classification::Reused => self.txt_reused += 1,
                _ => unreachable!("required TXT reached the ledger without an original"),
            }
            self.txt_sealed += 1;
            self.txt_audit_pending += 1;
            self.checkpoint_txt += usize::from(checkpoint);
            self.txt_contains_nul += usize::from(receipt.contains_nul);
            self.txt_invalid_utf8 += usize::from(receipt.invalid_utf8);
        } else {
            self.xml_classified += 1;
            self.checkpoint_xml += usize::from(checkpoint);
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
                checkpoint_expiry: None,
                checkpoint_version: None,
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

pub(crate) struct Selection<'a> {
    pub checkpoint: Context,
    pub catalog: &'a xml::Catalog,
    pub registry: &'a Registry,
    pub header: serde_json::Value,
    pub expected: Vec<RfcNumber>,
    pub txt: BTreeMap<RfcNumber, Original>,
    pub xml: BTreeMap<RfcNumber, Original>,
    pub accepted: Arc<Accepted>,
}

impl Spool {
    pub(crate) async fn acquire(
        &self,
        selection: Selection<'_>,
        writer: &mut Writer,
        config: &SyncConfig,
        upstream: &Upstream,
        shutdown: &Shutdown,
        (counts, audit_report, audit): (&mut Counts, &mut audit::Report, audit::Audit),
    ) -> Result<audit::Completion> {
        let options = &config.ingestion.pipeline;
        let store = tokio::task::spawn_blocking(|| {
            let store = BlobWriter::open(&mirror_root())?;
            store.recover_staging()?;
            Ok::<_, std::io::Error>(Arc::new(store))
        })
        .await??;
        let mut join = Join::new(&selection.expected);
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
        // No receipt can add more than one fixed-size descriptor per selected
        // RFC, so the audit queue cannot block the independent XML lane.
        let (audit_tx, audit_rx) = mpsc::channel(selection.expected.len().max(1));
        let (resolved_tx, mut resolved_rx) = mpsc::channel(1);
        let audit_task =
            tokio::spawn(audit.run(audit_rx, resolved_tx, upstream.clone(), shutdown.clone()));
        let repair_worker = Worker {
            root: self.0.clone(),
            upstream: upstream.clone(),
            shutdown: shutdown.clone(),
            accepted: selection.accepted.clone(),
            store: store.clone(),
            selection: selection.checkpoint.key,
        };
        let mut audit_aborted = false;
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
                        store: store.clone(),
                        selection: selection.checkpoint.key,
                    };
                    work.spawn(async move { worker.acquire(source, prior).await });
                }
                counts.stage_peak_jobs = counts.stage_peak_jobs.max(work.len());
            }
            if work.is_empty() {
                break;
            }
            let next = tokio::select! {
                Some(repair) = resolved_rx.recv(), if failure.is_none() => {
                    if let Err(error) = apply_repair(
                        &repair_worker, writer, &selection, repair, &mut join, &mut summaries, counts
                    ).await {
                        failure = Some(error);
                        shutdown.stop();
                    }
                    continue;
                }
                _ = audit_tx.closed(), if failure.is_none() => {
                    audit_aborted = true;
                    failure = Some("rsync audit stopped before TXT acquisition finished".into());
                    shutdown.stop();
                    continue;
                }
                next = work.join_next() => next,
            };
            match next {
                Some(Ok(Ok(receipt))) if failure.is_none() => {
                    let mut batch = vec![receipt];
                    while batch.len() < config.ingestion.database.batch_size as usize {
                        match work.try_join_next() {
                            Some(Ok(Ok(receipt))) => batch.push(receipt),
                            Some(Ok(Err(error))) => {
                                failure = Some(error);
                                break;
                            }
                            Some(Err(error)) => {
                                failure = Some(error.into());
                                break;
                            }
                            None => break,
                        }
                    }
                    if failure.is_none() {
                        let result = async {
                            shutdown.check()?;
                            let mut keys = std::collections::BTreeSet::new();
                            for receipt in &batch {
                                join.validate(selection.checkpoint, receipt)?;
                                if !keys.insert((receipt.rfc, receipt.kind)) {
                                    return Err("duplicate HTTPS result in database batch".into());
                                }
                            }
                            let database = upstream.metrics().span(crate::metrics::Stage::Database);
                            let tx = writer.client.transaction().await?;
                            tx.batch_execute("SET LOCAL synchronous_commit = on")
                                .await?;
                            for receipt in &mut batch {
                                receipt.persist(&tx, &selection).await?;
                            }
                            rfc_storage::fixture_boundary("before_https_checkpoint_commit");
                            tx.commit().await?;
                            rfc_storage::fixture_boundary("after_https_checkpoint_commit");
                            drop(database);
                            counts.db_commits += 1;
                            counts.db_batch_peak = counts.db_batch_peak.max(batch.len());
                            for receipt in batch {
                                let key = (receipt.rfc, receipt.kind);
                                let digest = receipt.content_digest()?;
                                if summaries.insert(key, digest).is_some() {
                                    return Err("duplicate HTTPS artifact receipt".into());
                                }
                                counts.record(&receipt);
                                join.record(&receipt, counts)?;
                                if receipt.kind == "txt" {
                                    let input = audit::Input::new(
                                        receipt.rfc,
                                        receipt
                                            .original
                                            .as_ref()
                                            .ok_or("audited TXT is missing its original")?,
                                    )?;
                                    // A closed receiver already has a terminal error.
                                    let _ = audit_tx.send(input).await;
                                }
                                write_record(&mut ledger, &mut ledger_bytes, &receipt).await?;
                            }
                            Result::Ok(())
                        }
                        .await;
                        if let Err(error) = result {
                            failure = Some(error);
                        }
                    }
                    if failure.is_some() {
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
        drop(audit_tx);
        while let Some(repair) = resolved_rx.recv().await {
            if failure.is_none() {
                if let Err(error) = apply_repair(
                    &repair_worker,
                    writer,
                    &selection,
                    repair,
                    &mut join,
                    &mut summaries,
                    counts,
                )
                .await
                {
                    failure = Some(error);
                    shutdown.stop();
                }
            } else {
                let _ = repair
                    .acknowledge
                    .send(Err("repair coordinator cancelled".into()));
            }
        }
        let (audit_result, completion) = audit_task.await?;
        *audit_report = audit_result;
        counts.txt_audited = audit_report.matched + audit_report.repaired;
        counts.txt_audit_pending = counts.txt_sealed.saturating_sub(counts.txt_audited);
        counts.accepted_bytes = selection.accepted.used();
        counts.raw_reservation_peak_bytes =
            counts.stage_peak_jobs as u64 * crate::config::RAW_PER_DOCUMENT;
        counts.parser_reservation_peak_bytes =
            counts.stage_peak_jobs as u64 * DOCUMENT_BYTES as u64;
        counts.receipt_bytes = ledger_bytes;
        if audit_aborted && let Err(error) = completion {
            return Err(error);
        }
        if let Some(error) = failure {
            if let Some(invalid) = error.downcast_ref::<InvalidCheckpoint>() {
                selection
                    .checkpoint
                    .remove(&writer.client, Some((invalid.id, invalid.kind)))
                    .await?;
            }
            return Err(error);
        }
        let completion = completion?;
        shutdown.check()?;
        if counts.txt_audit_pending != 0
            || counts.txt_sealed != selection.expected.len()
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
        counts.processing_digest = Some(join.finish()?);
        ledger.sync_all().await?;
        let root = self.0.clone();
        tokio::task::spawn_blocking(move || File::open(root)?.sync_all()).await??;
        Ok(completion)
    }
}

async fn apply_repair(
    worker: &Worker,
    writer: &mut Writer,
    selection: &Selection<'_>,
    repair: audit::Resolved,
    join: &mut Join,
    summaries: &mut BTreeMap<(u32, &'static str), BlobDigest>,
    counts: &mut Counts,
) -> Result<()> {
    let result = async {
        let id = RfcNumber::new(i32::try_from(repair.input.rfc)?).ok_or("invalid repaired RFC")?;
        let source = Source::Txt(id);
        let mut ready = worker
            .prepare(source, Some(repair.path), &repair.original)
            .await?;
        let parsed = ready
            .parsed
            .take()
            .ok_or("repaired TXT lacks parsed indexes")?;
        selection
            .accepted
            .replace(repair.input.length, repair.original.length)?;
        let mut receipt = Receipt {
            selection: selection.checkpoint.key,
            prepared: Some(ready),
            document_version: None,
            rfc: id.get(),
            kind: "txt",
            source_url: source.url(),
            classification: Classification::Captured,
            audit_state: "repaired",
            original: Some(repair.original),
            file: None,
            coverage: None,
            contains_nul: false,
            invalid_utf8: false,
            reference_digest: None,
        };
        parsed.apply(&mut receipt);
        let _database = worker
            .upstream
            .metrics()
            .span(crate::metrics::Stage::Database);
        let tx = writer.client.transaction().await?;
        tx.batch_execute("SET LOCAL synchronous_commit = on")
            .await?;
        receipt.persist(&tx, selection).await?;
        tx.commit().await?;
        counts.db_commits += 1;
        rfc_storage::fixture_boundary("after_rsync_repair_checkpoint");
        join.replace_txt(&receipt, counts)?;
        *summaries
            .get_mut(&(id.get(), "txt"))
            .ok_or("missing repaired TXT summary")? = receipt.content_digest()?;
        audit::Input::new(
            id.get(),
            receipt.original.as_ref().ok_or("missing repair original")?,
        )
    }
    .await;
    match result {
        Ok(sealed) => {
            let _ = repair.acknowledge.send(Ok(sealed));
            Ok(())
        }
        Err(error) => {
            let _ = repair
                .acknowledge
                .send(Err("could not seal repaired TXT".into()));
            Err(error)
        }
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
    store: Arc<BlobWriter>,
    selection: BlobDigest,
}

impl Worker {
    async fn acquire(&self, source: Source, prior: Option<Original>) -> Result<Receipt> {
        let id = match source {
            Source::Txt(id) | Source::Xml(id) => id,
            _ => return Err("unexpected HTTPS artifact kind".into()),
        };
        let mut receipt = Receipt {
            selection: self.selection,
            prepared: None,
            document_version: None,
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
        if let Some(prior) = prior {
            let now = crate::upstream::now_us()?;
            let deadline = prior.checkpoint_expiry.or_else(|| {
                matches!(source, Source::Xml(_)).then_some(prior.fetched_us + 86_400_000_000)
            });
            if deadline.is_none_or(|expiry| now < expiry) {
                let mut attempt = self.accepted.attempt();
                attempt.charge(prior.length)?;
                let result = self.prepare(source, None, &prior).await;
                match result {
                    Ok(mut ready) => {
                        let status = ready
                            .parsed
                            .take()
                            .ok_or("cached RFCXML no longer parses")?;
                        status.apply(&mut receipt);
                        receipt.prepared = Some(ready);
                        receipt.original = Some(prior);
                        receipt.classification = Classification::Reused;
                        attempt.retain();
                        return Ok(receipt);
                    }
                    Err(error) if prior.checkpoint_expiry.is_some() => {
                        if !error
                            .downcast_ref::<io::Error>()
                            .is_some_and(|e| e.kind() == io::ErrorKind::NotFound)
                        {
                            self.shutdown.check()?;
                            return Err(InvalidCheckpoint {
                                id,
                                kind: kind(source),
                                cause: error,
                            }
                            .into());
                        }
                    }
                    Err(error) => return Err(error),
                }
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
                let mut ready = self
                    .prepare(source, Some(partial.clone()), &original)
                    .await?;
                match ready.parsed.take() {
                    Some(status) => status.apply(&mut receipt),
                    None => receipt.classification = Classification::Malformed,
                }
                receipt.prepared = Some(ready);
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

    async fn prepare(
        &self,
        source: Source,
        path: Option<PathBuf>,
        original: &Original,
    ) -> Result<Prepared> {
        let metrics = self.upstream.metrics();
        let queue = metrics.span(crate::metrics::Stage::ParserQueue);
        let _permit = self.upstream.parse_slot(&self.shutdown).await?;
        drop(queue);
        let original = original.clone();
        let shutdown = self.shutdown.clone();
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let timing = metrics.parse_on_thread();
            let budget = Budget {
                bytes: DOCUMENT_BYTES,
                shutdown: &shutdown,
            };
            shutdown.check()?;
            let mut file = match &path {
                Some(path) => OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
                    .open(path)?,
                None => BlobReader::open(&mirror_root(), false)?.open_blob(original.digest)?,
            };
            if original.length == 0
                || original.length > source.limit() as u64
                || !file.metadata()?.is_file()
            {
                return Err("HTTPS artifact is outside its original-byte budget".into());
            }
            let mut bytes = Vec::with_capacity(original.length as usize);
            (&mut file).take(original.length).read_to_end(&mut bytes)?;
            if bytes.len() as u64 != original.length
                || file.read(&mut [0])? != 0
                || BlobDigest::hash(&bytes) != original.digest
            {
                return Err(
                    format!("original {} failed canonical verification", original.digest).into(),
                );
            }
            let (parsed, text) = match source {
                Source::Txt(_) => {
                    let text = OwnedText::parse(&bytes, &budget)?;
                    let index = text.summary();
                    let summary = Parsed {
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
                    };
                    (Some(summary), Some(text))
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
                    let parsed = match xml::references(&bytes, id, &budget) {
                        Ok(parsed) => Some(Parsed {
                            coverage: (parsed.all, parsed.classified),
                            contains_nul: false,
                            invalid_utf8: false,
                            reference_digest: reference_digest(
                                parsed
                                    .edges
                                    .into_iter()
                                    .map(|edge| (edge.kind, edge.target, edge.locator)),
                            ),
                        }),
                        Err(error) if error.is::<BudgetExceeded>() => return Err(error),
                        Err(_) => {
                            shutdown.check()?;
                            None
                        }
                    };
                    (parsed, None)
                }
                _ => return Err("unexpected HTTPS parser input".into()),
            };
            drop(timing);
            shutdown.check()?;
            let _install = metrics.span(crate::metrics::Stage::Cas);
            let blob = if path.is_some() {
                rfc_storage::fixture_boundary("before_https_cas_install");
                store
                    .install(bytes.as_slice(), source.limit() as u64)?
                    .blob()
            } else {
                if parsed.is_none() {
                    return Err("cached RFCXML no longer parses".into());
                }
                store.verify_durable(original.digest, i64::try_from(original.length)?)?
            };
            if blob.digest() != original.digest || blob.byte_length() as u64 != original.length {
                return Err("CAS installation differs from the sealed HTTPS original".into());
            }
            Ok(Prepared {
                blob,
                parsed,
                text: text.map(|text| (bytes, text)),
            })
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

struct Prepared {
    blob: DurableBlob,
    parsed: Option<Parsed>,
    text: Option<(Vec<u8>, OwnedText)>,
}

fn kind(source: Source) -> Kind {
    match source {
        Source::Txt(_) => Kind::Txt,
        Source::Xml(_) => Kind::Xml,
        _ => unreachable!("only RFC originals enter processing"),
    }
}

#[derive(Debug)]
struct InvalidCheckpoint {
    id: RfcNumber,
    kind: Kind,
    cause: Box<dyn std::error::Error + Send + Sync>,
}

impl std::fmt::Display for InvalidCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RFC {} checkpoint failed verification: {}",
            self.id.get(),
            self.cause
        )
    }
}

impl std::error::Error for InvalidCheckpoint {}

impl Receipt {
    fn content_digest(&self) -> Result<BlobDigest> {
        let semantics = serde_json::to_vec(&(
            self.classification,
            self.original
                .as_ref()
                .map(|original| original.digest.to_string()),
            self.coverage,
            &self.reference_digest,
            self.contains_nul,
            self.invalid_utf8,
        ))?;
        Ok(BlobDigest::hash(&semantics))
    }

    async fn persist(&mut self, tx: &impl GenericClient, selection: &Selection<'_>) -> Result<()> {
        let Some(prepared) = self.prepared.take() else {
            if self.kind == "rfcxml"
                && self.original.is_none()
                && matches!(
                    self.classification,
                    Classification::NotFound
                        | Classification::Gone
                        | Classification::RetriesExhausted
                        | Classification::FetchFailed
                )
            {
                return Ok(());
            }
            return Err("HTTPS result is missing its prepared original".into());
        };
        let original = self
            .original
            .as_mut()
            .ok_or("HTTPS result lacks source metadata")?;
        let id = RfcNumber::new(self.rfc as i32).ok_or("invalid result RFC number")?;
        let kind = if self.kind == "txt" {
            Kind::Txt
        } else {
            Kind::Xml
        };
        if prepared.blob.digest() != original.digest
            || u64::try_from(prepared.blob.byte_length())? != original.length
        {
            return Err("prepared blob disagrees with its source receipt".into());
        }
        register_blob(tx, prepared.blob).await?;
        let observation = match original.observation.as_deref() {
            Some(value) => {
                BlobDigest::from_hex(value).ok_or("invalid existing observation identity")?
            }
            None => {
                insert_observation(
                    tx,
                    &Observation {
                        kind: self.kind,
                        url: &self.source_url,
                        observed_us: original.observed_us,
                        fetched_us: Some(original.fetched_us),
                        etag: original.etag.as_deref(),
                        last_modified: original.last_modified.as_deref(),
                        raw_digest: original.digest,
                        https_verification: "verified",
                    },
                    None,
                    matches!(kind, Kind::Txt),
                )
                .await?
            }
        };
        let version = match prepared.text {
            Some((bytes, text)) if matches!(kind, Kind::Txt) => Some(
                crate::parse::store_text(
                    tx,
                    id,
                    prepared.blob,
                    selection.registry,
                    &selection.catalog.entries[&id],
                    &bytes,
                    text,
                )
                .await?,
            ),
            None if matches!(kind, Kind::Xml) => None,
            _ => return Err("parsed result has the wrong artifact kind".into()),
        };
        if original
            .checkpoint_version
            .is_some_and(|saved| version != Some(saved))
        {
            return Err(InvalidCheckpoint {
                id,
                kind,
                cause: "sealed TXT index changed".into(),
            }
            .into());
        }
        selection
            .checkpoint
            .record_https(
                tx,
                id,
                kind,
                observation,
                version,
                self.classification == Classification::Malformed,
            )
            .await?;
        original.observation = Some(observation.to_string());
        self.document_version = version.map(|digest| digest.to_string());
        Ok(())
    }
}

#[derive(Default)]
struct Pair {
    txt: Option<Part>,
    xml: Option<Part>,
}

#[derive(Serialize)]
struct Part {
    original: Option<String>,
    version: Option<String>,
    coverage: Option<(&'static str, &'static str)>,
    references: Option<String>,
    outcome: Classification,
    contains_nul: bool,
    invalid_utf8: bool,
}

struct Join {
    pairs: BTreeMap<u32, Pair>,
    unmatched: usize,
}

impl Join {
    fn new(expected: &[RfcNumber]) -> Self {
        Self {
            pairs: expected
                .iter()
                .map(|id| (id.get(), Pair::default()))
                .collect(),
            unmatched: 0,
        }
    }

    fn validate(&self, context: Context, receipt: &Receipt) -> Result<()> {
        if receipt.selection != context.key {
            return Err("HTTPS result belongs to another selection".into());
        }
        let pair = self
            .pairs
            .get(&receipt.rfc)
            .ok_or("HTTPS result is outside the selection")?;
        let id = RfcNumber::new(receipt.rfc as i32).ok_or("invalid result RFC number")?;
        let source = match receipt.kind {
            "txt" if pair.txt.is_none() => Source::Txt(id),
            "rfcxml" if pair.xml.is_none() => Source::Xml(id),
            _ => return Err("duplicate or unknown HTTPS artifact result".into()),
        };
        if receipt.source_url != source.url() || receipt.audit_state != source.audit_state() {
            return Err("HTTPS result disagrees with the selected source".into());
        }
        Ok(())
    }

    fn record(&mut self, receipt: &Receipt, counts: &mut Counts) -> Result<()> {
        let pair = self
            .pairs
            .get_mut(&receipt.rfc)
            .ok_or("unexpected join result")?;
        let slot = if receipt.kind == "txt" {
            &mut pair.txt
        } else {
            &mut pair.xml
        };
        if slot.replace(Part::from(receipt)).is_some() {
            return Err("duplicate HTTPS join result".into());
        }
        if pair.txt.is_some() && pair.xml.is_some() {
            counts.documents_joined += 1;
            self.unmatched -= 1;
        } else {
            self.unmatched += 1;
            counts.join_backlog_peak = counts.join_backlog_peak.max(self.unmatched);
        }
        Ok(())
    }

    fn replace_txt(&mut self, receipt: &Receipt, counts: &mut Counts) -> Result<()> {
        let part = self
            .pairs
            .get_mut(&receipt.rfc)
            .and_then(|pair| pair.txt.as_mut())
            .ok_or("repaired TXT has no matching sealed result")?;
        counts.txt_contains_nul = counts.txt_contains_nul - usize::from(part.contains_nul)
            + usize::from(receipt.contains_nul);
        counts.txt_invalid_utf8 = counts.txt_invalid_utf8 - usize::from(part.invalid_utf8)
            + usize::from(receipt.invalid_utf8);
        *part = Part::from(receipt);
        Ok(())
    }

    fn finish(self) -> Result<String> {
        let mut hash = blake3::Hasher::new_derive_key("rfc https processing selection v1");
        for (rfc, pair) in self.pairs {
            let (Some(txt), Some(xml)) = (pair.txt, pair.xml) else {
                return Err("incomplete HTTPS document join".into());
            };
            let (all, classified) = txt.coverage.ok_or("missing TXT reference coverage")?;
            let (coverage, use_txt) = match xml.coverage {
                Some((xml_all, xml_classified)) => {
                    ((xml_all, xml_classified), xml_all != "complete")
                }
                None if matches!(xml.outcome, Classification::NotFound | Classification::Gone) => {
                    ((all, classified), true)
                }
                None => ((cap_coverage(all), cap_coverage(classified)), true),
            };
            let joined = identity(
                "rfc-https-processing-document-v1",
                &(
                    rfc,
                    &txt.original,
                    &txt.version,
                    &xml.original,
                    xml.outcome,
                    coverage,
                    &xml.references,
                    use_txt.then_some(&txt.references),
                ),
            )?;
            hash.update(&rfc.to_be_bytes());
            hash.update(joined.as_bytes());
        }
        Ok(hash.finalize().to_hex().to_string())
    }
}

impl From<&Receipt> for Part {
    fn from(receipt: &Receipt) -> Self {
        Self {
            contains_nul: receipt.contains_nul,
            invalid_utf8: receipt.invalid_utf8,
            original: receipt
                .original
                .as_ref()
                .map(|value| value.digest.to_string()),
            version: receipt.document_version.clone(),
            coverage: receipt.coverage,
            references: receipt.reference_digest.clone(),
            outcome: if receipt.classification == Classification::Reused {
                Classification::Captured
            } else {
                receipt.classification
            },
        }
    }
}

fn cap_coverage(state: &'static str) -> &'static str {
    if state == "complete" {
        "partial"
    } else {
        state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receipt(context: Context, id: RfcNumber, source: Source) -> Receipt {
        Receipt {
            selection: context.key,
            prepared: None,
            document_version: None,
            rfc: id.get(),
            kind: source.kind(),
            source_url: source.url(),
            classification: if matches!(source, Source::Txt(_)) {
                Classification::Captured
            } else {
                Classification::NotFound
            },
            audit_state: source.audit_state(),
            original: None,
            file: None,
            coverage: matches!(source, Source::Txt(_)).then_some(("partial", "partial")),
            contains_nul: false,
            invalid_utf8: false,
            reference_digest: None,
        }
    }

    #[test]
    fn join_rejects_wrong_selection_and_duplicate_and_is_order_independent() {
        let id = RfcNumber::new(100).unwrap();
        let ctx = Context {
            scope: "full",
            key: BlobDigest::hash(b"selected"),
        };
        let stale = Context {
            key: BlobDigest::hash(b"stale"),
            ..ctx
        };
        let txt = receipt(ctx, id, Source::Txt(id));
        let xml = receipt(ctx, id, Source::Xml(id));
        let mut digests = Vec::new();
        for order in [[&txt, &xml], [&xml, &txt]] {
            let mut join = Join::new(&[id]);
            let mut counts = Counts::default();
            assert!(join.validate(stale, &txt).is_err());
            for result in order {
                join.validate(ctx, result).unwrap();
                join.record(result, &mut counts).unwrap();
                assert!(join.validate(ctx, result).is_err());
            }
            assert_eq!(counts.documents_joined, 1);
            assert_eq!(counts.join_backlog_peak, 1);
            digests.push(join.finish().unwrap());
        }
        assert_eq!(digests[0], digests[1]);
        assert!(Join::new(&[id]).finish().is_err());
    }
}
