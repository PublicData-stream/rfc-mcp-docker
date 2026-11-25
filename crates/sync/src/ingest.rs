use crate::{
    Candidate, Document, Evidence, IndexedText, Membership, Observation, Writer,
    checkpoint::{self, Context, Kind, SavedSources},
    discovery::{INDEX, PARSER, SAMPLE, Scope},
    legacy_config::{RsyncMode, SyncConfig, TxtProvenance},
    metrics::{Metrics, Record, Stage},
    pipeline::{
        Event, Pipeline, Prepared, PreparedXml, PriorTxt, Selection, StagedTxt, TxtEvidence,
        TxtVerification, WorkId,
    },
    policy::Registry,
    recovery::recover_before_ingestion,
    register_blob,
    shutdown::Shutdown,
    text::TextReference,
    upstream::{self, Capture, Download, FetchAttempts, MissingXml, Source, Upstream},
    xml,
};
use rfc_core::{BlobDigest, ByteRange, Coverage, GenerationId, Relation, RelationKind, RfcNumber};
use rfc_storage::{
    blob::{BlobReader, BlobWriter, DurableBlob},
    manifest::identity,
    postgres::{Result, mirror_root},
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

const FAMILIES: [&str; 4] = [
    "all_references",
    "classified_references",
    "updates",
    "obsoletes",
];
#[derive(Clone, Copy)]
enum TxtPolicy {
    Strict,
    Bootstrap,
    Published(i64),
}

impl TxtPolicy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "dual_source",
            Self::Bootstrap => "rsync_bootstrap",
            Self::Published(_) => "active_rsync_or_dual_source",
        }
    }
}

#[derive(Serialize)]
struct Report {
    scope: &'static str,
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    job_id: Option<String>,
    generation_id: Option<String>,
    manifest_digest: Option<String>,
    catalog_digest: Option<String>,
    current_rfc: Option<u32>,
    expected_txt: usize,
    verified_txt: usize,
    rsync_only_txt: usize,
    reused_txt: usize,
    checkpoint_txt: usize,
    txt_audit: TxtAudit,
    catalog_only: usize,
    invalid_utf8: usize,
    contains_nul: usize,
    incomplete_sections: usize,
    incomplete_notices: usize,
    metadata_only: usize,
    partial_references: usize,
    unclassified_or_incomplete: usize,
    xml_unavailable: usize,
    rfcxml: XmlCounts,
    catalog_dangling_observations: usize,
    reference_targets_without_txt: usize,
    accepted_bytes: u64,
    diagnostics: Vec<String>,
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    metrics: Option<Record>,
}

#[derive(Default, Serialize)]
struct XmlCounts {
    captured: usize,
    reused: usize,
    not_found: usize,
    gone: usize,
    malformed: usize,
    retries_exhausted: usize,
    terminal_failure: usize,
}

#[derive(Default, Serialize)]
struct TxtAudit {
    matched: usize,
    mismatched: usize,
    reused_dual_source: usize,
    reused_legacy: usize,
}

impl Report {
    fn new(scope: Scope) -> Self {
        Self {
            scope: scope.as_str(),
            state: "failed",
            job_id: None,
            generation_id: None,
            manifest_digest: None,
            catalog_digest: None,
            current_rfc: None,
            expected_txt: 0,
            verified_txt: 0,
            rsync_only_txt: 0,
            reused_txt: 0,
            checkpoint_txt: 0,
            txt_audit: TxtAudit::default(),
            catalog_only: 0,
            invalid_utf8: 0,
            contains_nul: 0,
            incomplete_sections: 0,
            incomplete_notices: 0,
            metadata_only: 0,
            partial_references: 0,
            unclassified_or_incomplete: 0,
            xml_unavailable: 0,
            rfcxml: XmlCounts::default(),
            catalog_dangling_observations: 0,
            reference_targets_without_txt: 0,
            accepted_bytes: 0,
            diagnostics: Vec::new(),
            error: None,
            metrics: None,
        }
    }

    fn accept(&mut self, bytes: usize, max_accepted_bytes: u64) -> Result<()> {
        self.accepted_bytes = self
            .accepted_bytes
            .checked_add(u64::try_from(bytes)?)
            .filter(|bytes| *bytes <= max_accepted_bytes)
            .ok_or("run input exceeds byte limit")?;
        Ok(())
    }

    fn diagnostic(&mut self, error: impl std::fmt::Display) {
        if self.diagnostics.len() < 16 {
            self.diagnostics.push(bounded_error(error));
        }
    }
}

fn bounded_error(error: impl std::fmt::Display) -> String {
    error.to_string().chars().take(512).collect()
}

/// Fixture endpoints are compiled only into the dedicated test build.
#[cfg(feature = "phase2-fixtures")]
pub async fn reconcile_fixture(scope: Scope) -> Result<()> {
    execute(
        Upstream::fixture(SyncConfig::load()?),
        Registry::load(),
        scope,
    )
    .await
}

/// Only the disposable fixture driver passes synthetic review receipts.
#[cfg(feature = "phase3-fixtures")]
pub async fn reconcile_fixture_with_reviews(scope: Scope, reviews: &str) -> Result<()> {
    execute(
        Upstream::fixture(SyncConfig::load()?),
        serde_json::from_str(reviews).map_err(Into::into),
        scope,
    )
    .await
}

async fn execute(
    upstream: Result<Upstream>,
    registry: Result<Registry>,
    scope: Scope,
) -> Result<()> {
    let mut report = Report::new(scope);
    let shutdown = Shutdown::new();
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let result = async {
        let execution = async {
            let upstream = upstream?;
            let mut writer = Writer::connect().await?;
            let _coordinator = writer.acquire_coordinator().await?;
            run(
                &mut writer,
                upstream,
                registry,
                scope,
                None,
                &mut report,
                &shutdown,
            )
            .await
        };
        tokio::pin!(execution);
        tokio::select! {
            result = &mut execution => result,
            _ = term.recv() => {
                shutdown.stop();
                execution.await
            }
        }
    }
    .await;
    print_report(report, &result)?;
    result.map(|_| ())
}

#[cfg(feature = "phase6-fixtures")]
pub(crate) async fn ingest_locked(
    writer: &mut Writer,
    upstream: Result<Upstream>,
    registry: Result<Registry>,
    scope: Scope,
    job: Option<i64>,
    shutdown: &Shutdown,
) -> Result<(GenerationId, BlobDigest)> {
    let mut report = Report::new(scope);
    report.job_id = job.map(|id| id.to_string());
    let result = match upstream {
        Ok(upstream) => {
            run(
                writer,
                upstream,
                registry,
                scope,
                job,
                &mut report,
                shutdown,
            )
            .await
        }
        Err(error) => Err(error),
    };
    print_report(report, &result)?;
    result
}

fn print_report(mut report: Report, result: &Result<(GenerationId, BlobDigest)>) -> Result<()> {
    match result {
        Ok((id, digest)) => {
            report.state = "ready";
            report.generation_id = Some(id.get().to_string());
            report.manifest_digest = Some(digest.to_string());
        }
        Err(error) => report.error = Some(bounded_error(error)),
    }
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

async fn run(
    writer: &mut Writer,
    upstream: Upstream,
    registry: Result<Registry>,
    scope: Scope,
    job: Option<i64>,
    report: &mut Report,
    shutdown: &Shutdown,
) -> Result<(GenerationId, BlobDigest)> {
    let started = Instant::now();
    let metrics = upstream.metrics();
    let cadence = Duration::from_secs(60);
    #[cfg(feature = "phase2-fixtures")]
    let cadence = if upstream.is_fixture() {
        Duration::from_secs(1)
    } else {
        cadence
    };
    let root = mirror_root();
    let store = BlobWriter::open(&root)?;
    shutdown.check()?;
    recover_before_ingestion(writer, &store).await?;
    let reader = Arc::new(BlobReader::open(&root, false)?);
    shutdown.check()?;
    upstream.after_fence().await;
    let registry = registry?;
    let mut pipeline = Pipeline::new(upstream.clone())?;
    let id = writer.start_ingestion(PARSER, INDEX, job).await?;
    report.generation_id = Some(id.get().to_string());
    let mut run = Run {
        writer,
        id,
        scope,
        store: Arc::new(store),
        reader,
        installing: None,
        upstream,
        registry,
        report,
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs(12 * 60 * 60);
    let reconciliation = async {
        let candidate = shutdown.until(deadline, run.collect(&mut pipeline)).await?;
        let _validation = metrics.span(Stage::Validation);
        let digest = shutdown
            .until(deadline, candidate.prepare_readiness())
            .await?;
        shutdown.check()?;
        if tokio::time::Instant::now() >= deadline {
            return Err("reconciliation deadline exceeded".into());
        }
        // Once submitted, observe the COMMIT outcome before releasing the fence.
        candidate.commit_readiness(digest).await
    };
    let result = tokio::select! {
        biased;
        result = reconciliation => result,
        _ = log_progress(&metrics, started, cadence, scope, id, job) => unreachable!(),
    };
    pipeline.drain().await;
    run.drain_install().await;
    if result.is_err()
        && let Err(error) = run.writer.fail(id).await
    {
        run.report
            .diagnostic(format!("failed to mark candidate failed: {error}"));
    }
    run.report.metrics = Some(metrics.snapshot(started.elapsed()));
    result
}

async fn log_progress(
    metrics: &Metrics,
    started: Instant,
    cadence: Duration,
    scope: Scope,
    id: GenerationId,
    job: Option<i64>,
) -> ! {
    let mut ticks = tokio::time::interval_at(tokio::time::Instant::now() + cadence, cadence);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let generation_id = id.get().to_string();
    let job_id = job.map(|id| id.to_string());
    loop {
        ticks.tick().await;
        eprintln!(
            "{}",
            serde_json::json!({
                "event": "ingestion_progress",
                "scope": scope.as_str(),
                "state": "building",
                "generation_id": generation_id,
                "job_id": job_id,
                "elapsed_seconds": started.elapsed().as_secs_f64(),
                "progress": metrics.progress(),
            })
        );
    }
}

struct Run<'a> {
    writer: &'a mut Writer,
    id: GenerationId,
    scope: Scope,
    store: Arc<BlobWriter>,
    reader: Arc<BlobReader>,
    installing: Option<tokio::task::JoinHandle<Result<DurableBlob>>>,
    upstream: Upstream,
    registry: Registry,
    report: &'a mut Report,
}

#[derive(Clone, Copy)]
struct CatalogWork {
    selection: Selection,
    before: BlobDigest,
    txt_policy: TxtPolicy,
    checkpoint: Context,
}

struct SealedTxt {
    version: BlobDigest,
    observation: BlobDigest,
    reused: bool,
    rsync_only: bool,
    references: Vec<TextReference>,
    all: Coverage,
    classified: Coverage,
}

fn pending_slot(
    pending: &mut BTreeMap<RfcNumber, Option<SealedTxt>>,
    selection: Selection,
    id: WorkId,
) -> Result<&mut Option<SealedTxt>> {
    if id.selection != selection {
        return Err("document result belongs to another catalog selection".into());
    }
    pending
        .get_mut(&id.rfc)
        .ok_or_else(|| "document result has no pending slot".into())
}

impl Run<'_> {
    async fn collect(&mut self, pipeline: &mut Pipeline) -> Result<Candidate<'_>> {
        let (txt_policy, sequence) = self.txt_policy().await?;
        let start = self.catalog_capture().await?;
        let catalog = {
            let _parse = self.upstream.metrics().span(Stage::Parse);
            xml::catalog(&start.bytes)?
        };
        if catalog.entries.len() > upstream::MAX_CATALOG_ENTRIES {
            return Err("catalog entry count exceeds bounds".into());
        }
        let selected: BTreeSet<_> = if self.scope == Scope::Full {
            catalog
                .entries
                .iter()
                .filter(|(_, entry)| entry.has_txt)
                .map(|(&id, _)| id)
                .collect()
        } else {
            SAMPLE
                .into_iter()
                .map(|number| RfcNumber::new(number).expect("fixed sample ID"))
                .collect()
        };
        if selected.is_empty()
            || selected
                .iter()
                .any(|id| catalog.entries.get(id).is_none_or(|entry| !entry.has_txt))
        {
            return Err("catalog lacks an expected TXT entry".into());
        }
        let ordered: Vec<_> = selected.iter().copied().collect();
        let selection = Selection {
            generation: self.id,
            catalog: BlobDigest::hash(&start.bytes),
        };
        let checkpoint = Context {
            scope: self.scope.as_str(),
            key: identity(
                "rfc-ingestion-checkpoint-v1",
                &(
                    self.scope.as_str(),
                    selection.catalog.to_string(),
                    txt_policy.as_str(),
                    sequence,
                    PARSER,
                    INDEX,
                    &self.registry.version,
                ),
            )?,
        };
        let checkpoints = checkpoint.load(self.writer).await?;
        pipeline.prefetch_xml(selection, &ordered, 0, &checkpoints, &self.reader);
        let baseline = self.baseline(txt_policy).await?;
        if self.scope == Scope::Full
            && let Some(missing) = baseline.keys().find(|id| !selected.contains(id))
        {
            return Err(format!("catalog lost previously accepted RFC {}", missing.get()).into());
        }
        let catalog_only: Vec<_> = catalog
            .entries
            .iter()
            .filter(|(_, entry)| !entry.has_txt)
            .map(|(id, entry)| (id.get(), entry))
            .collect();
        self.report.catalog_only = catalog_only.len();
        self.report.expected_txt = selected.len();
        self.upstream.metrics().selected(selected.len());
        let start_digest = self.install(Source::Catalog, start.bytes.clone()).await?;
        if start_digest != selection.catalog {
            return Err("installed catalog differs from selected catalog".into());
        }
        self.report.catalog_digest = Some(start_digest.to_string());
        let before = self.store_catalog(&start, start_digest).await?;
        drop(start);
        self.store_catalog_graph(&catalog, &selected, before)
            .await?;

        self.collect_documents(
            pipeline,
            &catalog,
            &ordered,
            &baseline,
            &checkpoints,
            CatalogWork {
                selection,
                before,
                txt_policy,
                checkpoint,
            },
        )
        .await?;
        pipeline.finish().await?;

        let end = self.catalog_capture().await?;
        let end_digest = self.install(Source::Catalog, end.bytes.clone()).await?;
        let after = self.store_catalog(&end, end_digest).await?;
        if end_digest != start_digest {
            checkpoint.remove(&self.writer.client, None).await?;
            return Err("RFC catalog changed during reconciliation".into());
        }
        let _database = self.upstream.metrics().span(Stage::Database);
        let candidate = self.writer.batch(self.id).await?;
        candidate
            .tx
            .batch_execute("SET LOCAL statement_timeout = '120s'")
            .await?;
        for (family, incomplete) in [
            (
                FAMILIES[0],
                self.report.partial_references != 0 || !catalog_only.is_empty(),
            ),
            (
                FAMILIES[1],
                self.report.unclassified_or_incomplete != 0 || !catalog_only.is_empty(),
            ),
            (
                FAMILIES[2],
                catalog
                    .incomplete
                    .iter()
                    .any(|(_, family)| *family == FAMILIES[2]),
            ),
            (
                FAMILIES[3],
                catalog
                    .incomplete
                    .iter()
                    .any(|(_, family)| *family == FAMILIES[3]),
            ),
        ] {
            candidate
                .coverage(
                    None,
                    family,
                    if self.scope == Scope::Sample || incomplete {
                        Coverage::Partial
                    } else {
                        Coverage::Complete
                    },
                )
                .await?;
        }
        candidate.tx.execute(
            "INSERT INTO rfc.generation_ingestion (generation_id, scope, catalog_before, catalog_after, policy_registry, catalog_only)
             VALUES ($1, $2, $3::bytea, $4::bytea, $5, $6::text::jsonb)",
            &[&candidate.generation(), &self.scope.as_str(), &before.as_bytes().as_slice(),
              &after.as_bytes().as_slice(), &self.registry.version, &serde_json::to_string(&catalog_only)?],
        ).await?;
        let expected: Vec<_> = selected.iter().map(|id| id.get() as i32).collect();
        let validated: bool = candidate.tx.query_one(
            "SELECT count(*) = $2 AND coalesce(bool_and(
                gd.catalog_observation = $3::bytea AND d.sealed AND d.section_state IS NOT NULL
                AND d.parser_version = $4 AND d.index_version = $5
                AND gd.rfc_id = ANY($6::integer[])
                AND (SELECT count(*) FROM rfc.relation_coverage c
                     WHERE c.generation_id = gd.generation_id AND c.scope = 'document' AND c.rfc_id = gd.rfc_id) = 4
             ), false)
             FROM rfc.generation_documents gd JOIN rfc.document_versions d ON d.identity = gd.document_version
             WHERE gd.generation_id = $1",
            &[&candidate.generation(), &i64::try_from(selected.len())?, &before.as_bytes().as_slice(), &PARSER, &INDEX, &expected],
        ).await?.try_get(0)?;
        if !validated {
            return Err("candidate failed membership or coverage validation".into());
        }
        checkpoint.remove(&candidate.tx, None).await?;
        Ok(candidate)
    }

    async fn collect_documents(
        &mut self,
        pipeline: &mut Pipeline,
        catalog: &xml::Catalog,
        ordered: &[RfcNumber],
        baseline: &BTreeMap<RfcNumber, Option<PriorTxt>>,
        checkpoints: &SavedSources,
        work: CatalogWork,
    ) -> Result<()> {
        let config = self.upstream.config().ingestion;
        let metrics = self.upstream.metrics();
        let mut batch: Option<StagedTxt> = None;
        let mut stage_end = 0;
        let mut admitted = 0;
        let mut committed = 0;
        let mut pending = BTreeMap::<RfcNumber, Option<SealedTxt>>::new();
        while committed < ordered.len() {
            pipeline.prefetch_xml(
                work.selection,
                ordered,
                committed,
                checkpoints,
                &self.reader,
            );
            if batch.is_none() && admitted == stage_end && admitted < ordered.len() {
                stage_end = match config.rsync.mode {
                    RsyncMode::Batched => (admitted + config.rsync.batch_size).min(ordered.len()),
                    RsyncMode::PersistentBulk => ordered.len(),
                };
                pipeline.stage(
                    self.scope,
                    ordered[admitted..stage_end].to_vec(),
                    config.max_accepted_bytes - self.report.accepted_bytes,
                );
            }
            if let Some(staged) = batch.as_ref() {
                while admitted < stage_end && pending.len() < config.pipeline.max_pending_documents
                {
                    let rfc = ordered[admitted];
                    self.report.current_rfc = Some(rfc.get());
                    let bytes = staged.take(&self.store, rfc)?;
                    admitted += 1;
                    pending.insert(rfc, None);
                    pipeline.prepare(
                        WorkId {
                            selection: work.selection,
                            rfc,
                        },
                        bytes,
                        TxtVerification {
                            baseline: baseline.get(&rfc).copied().flatten(),
                            checkpoint: checkpoints.txt.get(&rfc).copied(),
                            bootstrap: matches!(work.txt_policy, TxtPolicy::Bootstrap),
                        },
                        self.reader.clone(),
                        pending.len(),
                    )?;
                }
                if admitted == stage_end {
                    batch.take().expect("staged batch").close()?;
                    // The next rsync may overlap the remaining admitted documents.
                    continue;
                }
            }
            match pipeline.next().await? {
                Event::Staged(result) => {
                    batch = Some(result?);
                }
                Event::Txt(id, prepared) => {
                    let slot = pending_slot(&mut pending, work.selection, id)?;
                    if slot.is_some() {
                        return Err("duplicate TXT result".into());
                    }
                    self.report.current_rfc = Some(id.rfc.get());
                    *slot = Some(
                        self.store_txt(
                            id.rfc,
                            &catalog.entries[&id.rfc],
                            *prepared,
                            work.checkpoint,
                        )
                        .await?,
                    );
                    metrics.txt_completed();
                }
                Event::Xml(id, xml) => {
                    let out_of_order = pending
                        .first_key_value()
                        .is_some_and(|(rfc, _)| *rfc != id.rfc);
                    let txt = pending_slot(&mut pending, work.selection, id)?
                        .take()
                        .ok_or("RFCXML result arrived before TXT was sealed")?;
                    self.report.current_rfc = Some(id.rfc.get());
                    self.store_membership(
                        id.rfc,
                        &catalog.entries[&id.rfc],
                        work,
                        txt,
                        xml,
                        ordered,
                    )
                    .await?;
                    pending.remove(&id.rfc);
                    metrics.xml_completed(pending.len());
                    if out_of_order {
                        metrics.out_of_order();
                    }
                    committed += 1;
                }
                Event::Invalidated(id, kind) => {
                    if id.selection != work.selection {
                        return Err("checkpoint result belongs to another catalog selection".into());
                    }
                    work.checkpoint
                        .remove(&self.writer.client, Some((id.rfc, kind)))
                        .await?;
                }
                Event::Failed(id, error) => {
                    pending_slot(&mut pending, work.selection, id)?;
                    self.report.current_rfc = Some(id.rfc.get());
                    if let Some(invalid) = error.downcast_ref::<checkpoint::Invalid>() {
                        work.checkpoint
                            .remove(&self.writer.client, Some((id.rfc, invalid.kind)))
                            .await?;
                    }
                    return Err(error);
                }
            }
        }
        self.report.current_rfc = None;
        Ok(())
    }

    async fn txt_policy(&self) -> Result<(TxtPolicy, i64)> {
        let row = self.writer.client.query_one(
            "SELECT generation_id, publication_sequence FROM rfc.active_generation WHERE singleton",
            &[],
        ).await?;
        let sequence: i64 = row.try_get(1)?;
        if self.scope == Scope::Sample
            || self.upstream.config().ingestion.txt_provenance == TxtProvenance::DualSource
        {
            return Ok((TxtPolicy::Strict, sequence));
        }
        let policy = match (row.try_get::<_, Option<i64>>(0)?, sequence) {
            (None, 0) => TxtPolicy::Bootstrap,
            (Some(id), 1..) => TxtPolicy::Published(id),
            _ => return Err("invalid active publication state for TXT provenance policy".into()),
        };
        Ok((policy, sequence))
    }

    async fn baseline(&self, policy: TxtPolicy) -> Result<BTreeMap<RfcNumber, Option<PriorTxt>>> {
        let active = match policy {
            TxtPolicy::Published(id) => Some(id),
            _ => None,
        };
        let rows = self.writer.client.query(
            "SELECT d.rfc_id::integer, encode(o.raw_digest, 'hex'), encode(o.identity, 'hex'),
                CASE WHEN o.https_verification = 'verified'
                         AND o.source_url = 'https://www.rfc-editor.org/rfc/rfc' || d.rfc_id || '.txt'
                     THEN false
                     WHEN $1::bigint IS NOT NULL AND o.source_transport = 'rsync'
                         AND o.source_url = 'rsync://rsync.rfc-editor.org/rfcs-text-only/rfc' || d.rfc_id || '.txt'
                     THEN true
                END, o.verification_method = 'legacy_https_verified'
             FROM rfc.generation_documents d JOIN rfc.source_observations o ON o.identity = d.txt_observation
             WHERE o.kind = 'txt' AND d.generation_id = coalesce($1, (SELECT g.id FROM rfc.generations g
                 JOIN rfc.generation_ingestion i ON i.generation_id = g.id
                 WHERE i.scope = 'full' AND g.state IN ('ready', 'published', 'retained')
                 ORDER BY g.id DESC LIMIT 1))",
            &[&active],
        ).await?;
        rows.into_iter()
            .map(|row| {
                let prior = if let Some(rsync_only) = row.try_get::<_, Option<bool>>(3)? {
                    Some(PriorTxt {
                        digest: BlobDigest::from_hex(row.try_get(1)?)
                            .ok_or("invalid baseline TXT digest")?,
                        observation: BlobDigest::from_hex(row.try_get(2)?)
                            .ok_or("invalid baseline observation")?,
                        rsync_only,
                        legacy: row.try_get(4)?,
                    })
                } else {
                    None
                };
                Ok((
                    RfcNumber::new(row.try_get(0)?).ok_or("invalid baseline RFC number")?,
                    prior,
                ))
            })
            .collect()
    }

    async fn catalog_capture(&mut self) -> Result<Capture> {
        match self.upstream.fetch(Source::Catalog).await? {
            Download::Captured(capture) => {
                self.report.accept(
                    capture.bytes.len(),
                    self.upstream.config().ingestion.max_accepted_bytes,
                )?;
                Ok(capture)
            }
            Download::Missing(_) => Err("required catalog is unavailable".into()),
        }
    }

    async fn install(&mut self, source: Source, bytes: Arc<Vec<u8>>) -> Result<BlobDigest> {
        let durable = self.install_durable(source, bytes).await?;
        let _database = self.upstream.metrics().span(Stage::Database);
        self.writer.register_blob(durable).await
    }

    async fn install_durable(
        &mut self,
        source: Source,
        bytes: Arc<Vec<u8>>,
    ) -> Result<DurableBlob> {
        let store = self.store.clone();
        let reader = self.reader.clone();
        let metrics = self.upstream.metrics();
        // The coordinator submits at most one install. Retain the handle so
        // deadline or signal cancellation can join it before releasing the fence.
        assert!(self.installing.is_none());
        self.installing = Some(tokio::task::spawn_blocking(move || {
            let _install = metrics.span(Stage::Install);
            let durable = store
                .install(bytes.as_slice(), source.limit() as u64)?
                .blob();
            if matches!(source, Source::Txt(_))
                && reader.read_range(
                    durable.digest(),
                    ByteRange::new(0, durable.byte_length()).ok_or("invalid blob range")?,
                    source.limit(),
                )? != bytes.as_slice()
            {
                return Err("canonical TXT differs after durable installation".into());
            }
            Ok(durable)
        }));
        let result = self.installing.as_mut().expect("active blob install").await;
        self.installing.take();
        result?
    }

    async fn drain_install(&mut self) {
        if let Some(install) = self.installing.take() {
            let _ = install.await;
        }
    }

    async fn store_catalog(&mut self, capture: &Capture, digest: BlobDigest) -> Result<BlobDigest> {
        let _database = self.upstream.metrics().span(Stage::Database);
        let candidate = self.writer.batch(self.id).await?;
        let observation = observe(&candidate, Source::Catalog, capture, digest).await?;
        candidate.commit().await?;
        Ok(observation)
    }

    async fn store_catalog_graph(
        &mut self,
        catalog: &xml::Catalog,
        selected: &BTreeSet<RfcNumber>,
        catalog_id: BlobDigest,
    ) -> Result<()> {
        for edges in catalog.edges.chunks(128) {
            let batch = self.writer.batch(self.id).await?;
            for edge in edges {
                if self.scope == Scope::Sample
                    && !selected.contains(&edge.source)
                    && !selected.contains(&edge.target)
                {
                    continue;
                }
                if !catalog.entries.contains_key(&edge.source)
                    || !catalog.entries.contains_key(&edge.target)
                {
                    self.report.catalog_dangling_observations += 1;
                }
                let evidence = batch
                    .evidence(&Evidence {
                        observation: Some(catalog_id),
                        version: None,
                        range: None,
                        method: "catalog",
                        xml_locator: Some(&edge.locator),
                    })
                    .await?;
                batch
                    .relation(
                        Relation {
                            source: edge.source,
                            kind: edge.kind,
                            target: edge.target,
                        },
                        &[evidence],
                    )
                    .await?;
            }
            batch.commit().await?;
        }
        let coverage_ids: Vec<_> = catalog
            .entries
            .keys()
            .copied()
            .filter(|id| self.scope == Scope::Full || selected.contains(id))
            .collect();
        for ids in coverage_ids.chunks(128) {
            let batch = self.writer.batch(self.id).await?;
            for &id in ids {
                for family in &FAMILIES[2..] {
                    let state = if catalog.incomplete.contains(&(id, family))
                        || (self.scope == Scope::Sample
                            && catalog.incomplete.iter().any(|(_, item)| item == family))
                    {
                        Coverage::Partial
                    } else {
                        Coverage::Complete
                    };
                    batch.coverage(Some(id), family, state).await?;
                }
                if !selected.contains(&id) {
                    for family in &FAMILIES[..2] {
                        batch
                            .coverage(Some(id), family, Coverage::Unavailable)
                            .await?;
                    }
                }
            }
            batch.commit().await?;
        }
        Ok(())
    }

    async fn store_txt(
        &mut self,
        id: RfcNumber,
        entry: &xml::Entry,
        prepared: Prepared,
        checkpoint: Context,
    ) -> Result<SealedTxt> {
        self.upstream
            .metrics()
            .queue_wait(prepared.queued_at.elapsed());
        let Prepared {
            bytes,
            digest,
            txt_evidence,
            parsed,
            ..
        } = prepared;
        self.report.accept(
            bytes.len(),
            self.upstream.config().ingestion.max_accepted_bytes,
        )?;
        let source = Source::Txt(id);
        if let TxtEvidence::Mismatch(capture) = &txt_evidence {
            self.report.txt_audit.mismatched += 1;
            self.store
                .quarantine_mismatch(&bytes, &capture.bytes, source.limit())?;
            return Err(format!("RFC {} rsync and HTTPS TXT disagree", id.get()).into());
        }
        let parsed = parsed.ok_or("missing TXT parser output")?;
        let mut indexed = parsed.data.into_view(&bytes, &parsed.headings)?;
        let durable = self.install_durable(source, bytes.clone()).await?;
        let (policy, notices) =
            crate::parse::index_notices(id, digest, &self.registry, entry, &bytes, &mut indexed)?;
        let _database = self.upstream.metrics().span(Stage::Database);
        let candidate = self.writer.batch(self.id).await?;
        register_blob(&candidate.tx, durable).await?;
        let (txt, reused, rsync_only) = match txt_evidence {
            TxtEvidence::Prior(prior) => {
                if prior.legacy {
                    self.report.txt_audit.reused_legacy += 1;
                } else if !prior.rsync_only {
                    self.report.txt_audit.reused_dual_source += 1;
                }
                (prior.observation, true, prior.rsync_only)
            }
            TxtEvidence::Checkpoint(saved) => {
                self.report.checkpoint_txt += 1;
                self.report.txt_audit.reused_dual_source += usize::from(!saved.rsync_only);
                (saved.observation, true, saved.rsync_only)
            }
            TxtEvidence::HttpsMatch(capture, proof) => {
                let rsync = observe_rsync(&candidate, id, proof, digest).await?;
                let observation = candidate
                    .observe_comparison(
                        &https_observation(source.kind(), &source.url(), &capture, digest),
                        rsync,
                    )
                    .await?;
                self.report.txt_audit.matched += 1;
                (observation, false, false)
            }
            TxtEvidence::Rsync(proof) => {
                let observation = observe_rsync(&candidate, id, proof, digest).await?;
                (observation, false, true)
            }
            TxtEvidence::Mismatch(_) => unreachable!("mismatches are quarantined before commit"),
        };
        self.report.invalid_utf8 += usize::from(indexed.invalid_utf8);
        self.report.contains_nul += usize::from(indexed.contains_nul);
        self.report.incomplete_sections += usize::from(indexed.section_state != Coverage::Complete);
        self.report.incomplete_notices += usize::from(indexed.notice_state != Coverage::Complete);
        self.report.metadata_only += usize::from(policy == "metadata_only");
        let (encoding, index_state) = if indexed.text.is_some() {
            ("utf8", Coverage::Complete)
        } else {
            ("bytes", Coverage::Unavailable)
        };
        let version = candidate
            .document_with_index(
                Document {
                    rfc: id,
                    txt: durable,
                    parser_version: PARSER,
                    index_version: INDEX,
                    encoding,
                    index_state,
                    notice_state: indexed.notice_state,
                    notice_policy: policy,
                    sections: indexed.sections,
                    chunks: indexed.chunks,
                    notices,
                },
                Some(IndexedText {
                    section_state: indexed.section_state,
                    line_starts: &indexed.line_starts,
                    text: indexed.text,
                }),
            )
            .await?;
        if !reused {
            checkpoint.record(&candidate.tx, id, Kind::Txt, txt).await?;
            rfc_storage::fixture_boundary("before_txt_checkpoint_commit");
        }
        candidate.commit().await?;
        if !reused {
            rfc_storage::fixture_boundary("after_txt_checkpoint_commit");
        }
        Ok(SealedTxt {
            version,
            observation: txt,
            reused,
            rsync_only,
            references: indexed.references,
            all: indexed.all_references,
            classified: indexed.classified_references,
        })
    }

    async fn store_membership(
        &mut self,
        id: RfcNumber,
        entry: &xml::Entry,
        work: CatalogWork,
        txt: SealedTxt,
        xml: PreparedXml,
        selected: &[RfcNumber],
    ) -> Result<()> {
        let optional = self.rfcxml(id, xml, work.checkpoint).await?;
        let _database = self.upstream.metrics().span(Stage::Database);
        let candidate = self.writer.batch(self.id).await?;
        let xml_observation = optional.observation;
        candidate
            .member_with_sources(
                &Membership {
                    rfc: id,
                    version: txt.version,
                    catalog: work.before,
                    txt: txt.observation,
                    title: &entry.title,
                    stream: entry.stream.as_deref(),
                    publication_date: entry.date.as_deref(),
                    status: entry.status.as_deref(),
                },
                entry.month.as_deref(),
                xml_observation,
                true,
            )
            .await?;
        let mut all = txt.all;
        let mut classified = txt.classified;
        let mut use_txt = true;
        if let Some(xml) = optional.references {
            all = xml.all;
            classified = xml.classified;
            use_txt = all != Coverage::Complete;
            for edge in xml.edges {
                self.report.reference_targets_without_txt +=
                    usize::from(selected.binary_search(&edge.target).is_err());
                store_edge(
                    &candidate,
                    id,
                    edge.kind,
                    edge.target,
                    Evidence {
                        observation: xml_observation,
                        version: None,
                        range: None,
                        method: "rfcxml",
                        xml_locator: Some(&edge.locator),
                    },
                )
                .await?;
            }
        } else if optional.failed {
            all = cap_coverage(all);
            classified = cap_coverage(classified);
        }
        if use_txt {
            for edge in txt.references {
                self.report.reference_targets_without_txt +=
                    usize::from(selected.binary_search(&edge.target).is_err());
                store_edge(
                    &candidate,
                    id,
                    edge.kind,
                    edge.target,
                    Evidence {
                        observation: None,
                        version: Some(txt.version),
                        range: Some(edge.range),
                        method: "txt",
                        xml_locator: None,
                    },
                )
                .await?;
            }
        }
        candidate.coverage(Some(id), FAMILIES[0], all).await?;
        candidate
            .coverage(Some(id), FAMILIES[1], classified)
            .await?;
        self.report.partial_references += usize::from(all != Coverage::Complete);
        self.report.unclassified_or_incomplete += usize::from(classified != Coverage::Complete);
        candidate.commit().await?;
        self.report.reused_txt += usize::from(txt.reused);
        self.report.verified_txt += usize::from(!txt.rsync_only);
        self.report.rsync_only_txt += usize::from(txt.rsync_only);
        Ok(())
    }

    async fn rfcxml(
        &mut self,
        id: RfcNumber,
        xml: PreparedXml,
        checkpoint: Context,
    ) -> Result<OptionalXml> {
        let mut result = OptionalXml::default();
        let capture = match xml {
            PreparedXml::Captured(capture, references) => {
                self.report.rfcxml.captured += 1;
                result.references = Some(references);
                Some(capture)
            }
            PreparedXml::Reused(saved, references) => {
                self.report.rfcxml.reused += 1;
                self.report.accept(
                    saved.length,
                    self.upstream.config().ingestion.max_accepted_bytes,
                )?;
                result.observation = Some(saved.observation);
                result.references = Some(references);
                None
            }
            PreparedXml::Malformed(capture, error) => {
                self.report.rfcxml.malformed += 1;
                self.report
                    .diagnostic(format!("RFC {} RFCXML parse: {error}", id.get()));
                result.failed = true;
                Some(capture)
            }
            PreparedXml::Missing(status) => {
                match status {
                    MissingXml::NotFound => self.report.rfcxml.not_found += 1,
                    MissingXml::Gone => self.report.rfcxml.gone += 1,
                }
                self.report.xml_unavailable += 1;
                None
            }
            PreparedXml::Failed(failure) => {
                let error = match failure {
                    FetchAttempts::Exhausted(error) => {
                        self.report.rfcxml.retries_exhausted += 1;
                        error
                    }
                    FetchAttempts::Terminal(error) => {
                        self.report.rfcxml.terminal_failure += 1;
                        error
                    }
                };
                self.report.xml_unavailable += 1;
                self.report
                    .diagnostic(format!("RFC {} RFCXML fetch: {error}", id.get()));
                result.failed = true;
                None
            }
        };
        if let Some(capture) = capture {
            self.report.accept(
                capture.bytes.len(),
                self.upstream.config().ingestion.max_accepted_bytes,
            )?;
            let durable = self
                .install_durable(Source::Xml(id), capture.bytes.clone())
                .await?;
            let _database = self.upstream.metrics().span(Stage::Database);
            let candidate = self.writer.batch(self.id).await?;
            let digest = register_blob(&candidate.tx, durable).await?;
            let observation = observe(&candidate, Source::Xml(id), &capture, digest).await?;
            if result.references.is_some() {
                checkpoint
                    .record(&candidate.tx, id, Kind::Xml, observation)
                    .await?;
                rfc_storage::fixture_boundary("before_xml_checkpoint_commit");
            }
            candidate.commit().await?;
            if result.references.is_some() {
                rfc_storage::fixture_boundary("after_xml_checkpoint_commit");
            }
            result.observation = Some(observation);
        }
        Ok(result)
    }
}

#[derive(Default)]
struct OptionalXml {
    observation: Option<BlobDigest>,
    references: Option<xml::XmlReferences>,
    failed: bool,
}

fn cap_coverage(state: Coverage) -> Coverage {
    if state == Coverage::Complete {
        Coverage::Partial
    } else {
        state
    }
}

async fn observe(
    candidate: &Candidate<'_>,
    source: Source,
    capture: &Capture,
    digest: BlobDigest,
) -> Result<BlobDigest> {
    candidate
        .observe(&https_observation(
            source.kind(),
            &source.url(),
            capture,
            digest,
        ))
        .await
}

fn https_observation<'a>(
    kind: &'a str,
    url: &'a str,
    capture: &'a Capture,
    digest: BlobDigest,
) -> Observation<'a> {
    Observation {
        kind,
        url,
        observed_us: capture.observed_us,
        fetched_us: Some(capture.fetched_us),
        etag: capture.etag.as_deref(),
        last_modified: capture.last_modified.as_deref(),
        raw_digest: digest,
        https_verification: "verified",
    }
}

async fn observe_rsync(
    candidate: &Candidate<'_>,
    id: RfcNumber,
    proof: crate::bulk::Proof,
    digest: BlobDigest,
) -> Result<BlobDigest> {
    candidate
        .observe(&Observation {
            kind: "txt",
            url: &format!("{}{}", upstream::OFFICIAL_RSYNC, upstream::filename(id)),
            observed_us: proof.observed_us,
            fetched_us: proof.fetched_us,
            etag: None,
            last_modified: None,
            raw_digest: digest,
            https_verification: "unverified",
        })
        .await
}

async fn store_edge(
    candidate: &Candidate<'_>,
    source: RfcNumber,
    kind: RelationKind,
    target: RfcNumber,
    evidence: Evidence<'_>,
) -> Result<()> {
    let evidence = candidate.evidence(&evidence).await?;
    candidate
        .relation(
            Relation {
                source,
                kind,
                target,
            },
            &[evidence],
        )
        .await
}
