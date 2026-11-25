//! Discovery and acquisition under the corpus and publication fence.

use crate::{
    Writer,
    acquisition::{Accepted, Counts, Original, Selection, Spool},
    checkpoint::Context,
    config::SyncConfig,
    egress::Lease,
    inventory,
    metrics::{Record, Stage},
    policy::Registry,
    shutdown::Shutdown,
    upstream::{Capture, MAX_CATALOG_ENTRIES, Source, TXT_LIMIT, Upstream, XML_LIMIT},
    xml,
};
use rfc_core::{BlobDigest, RfcNumber};
use rfc_mcp_docker::control::SyncRequested;
use rfc_storage::{
    blob::BlobReader,
    manifest::identity,
    postgres::{Result, mirror_root},
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;

pub(crate) const PARSER: &str = "rfc-txt-catalog-1";
pub(crate) const INDEX: &str = "rfc-original-bytes-pg19-1";
pub(crate) const SAMPLE: [i32; 3] = [2119, 8174, 9110];
pub(crate) const PUBLICATION_GATE: &str = "https_txt_audit_pending";
const POLICY: &str = "https-first-rsync-audit-v1";
const DEADLINE: Duration = Duration::from_secs(600);

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Full,
    Sample,
}

impl Scope {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Sample => "sample",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    Complete,
    Deferred,
}

#[derive(Default, Serialize)]
struct Report {
    event: &'static str,
    state: &'static str,
    scope: &'static str,
    publication_gate: &'static str,
    job_id: Option<i64>,
    active_generation_id: Option<i64>,
    publication_sequence: i64,
    catalog_before: Option<Observation>,
    catalog_after: Option<Observation>,
    selection_id: Option<String>,
    expected_txt: usize,
    catalog_without_txt: usize,
    inventory_txt: usize,
    inventory_digest: Option<String>,
    missing_expected_txt: usize,
    first_missing_expected: Option<u32>,
    mirror_only_txt: usize,
    new_txt: usize,
    local_https_txt_audit_pending: usize,
    local_txt_needs_https: usize,
    inventory_size_changes: usize,
    hints_awaiting_catalog: usize,
    hints_awaiting_mirror: usize,
    hints_present: usize,
    acquisition: Counts,
    metrics: Record,
    error: Option<String>,
}

#[derive(Serialize)]
struct Observation {
    digest: String,
    source_url: String,
    observed_us: i64,
    fetched_us: i64,
    has_etag: bool,
    has_last_modified: bool,
}

struct PriorTxt {
    digest: BlobDigest,
    length: u64,
    https: Option<Original>,
}

pub(crate) fn upstream(config: &SyncConfig) -> Result<Upstream> {
    #[cfg(feature = "phase6-fixtures")]
    if crate::daemon::acquisition_fixture() {
        return Ok(Upstream::discovery_fixture(config.ingestion.https)?
            .with_parsers(config.ingestion.pipeline.parser_workers));
    }
    Ok(Upstream::official(config.ingestion.https)?
        .with_parsers(config.ingestion.pipeline.parser_workers))
}

pub async fn reconcile(scope: Scope) -> Result<()> {
    if let Some(reply) = crate::manual::request(scope).await? {
        println!("{}", reply.report);
        return if reply.success {
            Ok(())
        } else {
            Err("daemon acquisition did not complete".into())
        };
    }
    let config = SyncConfig::load()?;
    let lease = Lease::acquire()?;
    let leader = Writer::connect().await?;
    leader.acquire_egress_leader().await?;
    let upstream = upstream(&config)?.with_lease(&lease)?;
    let shutdown = Shutdown::new();
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let stopping = async {
        tokio::select! {
            _ = term.recv() => (),
            _ = interrupt.recv() => (),
        }
    };
    let work = execute(scope, None, &config, &upstream, &shutdown);
    tokio::pin!(work);
    let outcome = tokio::select! {
        result = &mut work => result?,
        monitor = crate::daemon::watch_leader(&leader) => {
            shutdown.stop();
            work.await?;
            monitor?;
            return Err("HTTPS lease monitor stopped".into());
        }
        _ = stopping => {
            shutdown.stop();
            work.await?
        }
    };
    println!("{}", outcome.report);
    match outcome.outcome? {
        Outcome::Complete => Ok(()),
        Outcome::Deferred => Err("acquisition deferred while sources converge".into()),
    }
}

pub(crate) struct Execution {
    pub outcome: Result<Outcome>,
    pub report: String,
}

impl Execution {
    pub(crate) fn operator(&self) -> crate::manual::Reply {
        crate::manual::Reply {
            success: matches!(self.outcome, Ok(Outcome::Complete)),
            report: self.report.clone(),
        }
    }
}

pub(crate) async fn execute(
    scope: Scope,
    job: Option<i64>,
    config: &SyncConfig,
    upstream: &Upstream,
    shutdown: &Shutdown,
) -> Result<Execution> {
    let started = Instant::now();
    let upstream = upstream.with_fresh_metrics();
    let mut report = Report {
        event: "acquisition",
        scope: scope.as_str(),
        publication_gate: PUBLICATION_GATE,
        job_id: job,
        ..Report::default()
    };
    let result = async {
        let deadline = started + DEADLINE;
        let mut writer = shutdown.until(deadline, Writer::connect()).await?;
        let _corpus = shutdown
            .until(deadline, writer.acquire_coordinator())
            .await?;
        Spool::sweep().await?;
        crate::egress::preflight(config.ingestion.max_accepted_bytes)?;
        discover(
            &mut writer,
            config,
            &upstream,
            shutdown,
            deadline,
            scope,
            &mut report,
        )
        .await
    }
    .await;
    report.state = match &result {
        Ok(Outcome::Complete) => "acquisition_complete",
        Ok(Outcome::Deferred) => "acquisition_deferred",
        Err(error) => {
            report.error = Some(error.to_string().chars().take(512).collect());
            "acquisition_failed"
        }
    };
    report.metrics = upstream.metrics().snapshot(started.elapsed());
    Ok(Execution {
        outcome: result,
        report: serde_json::to_string(&report)?,
    })
}

async fn discover(
    writer: &mut Writer,
    config: &SyncConfig,
    upstream: &Upstream,
    shutdown: &Shutdown,
    deadline: Instant,
    scope: Scope,
    report: &mut Report,
) -> Result<Outcome> {
    let (active, sequence, priors, hints) = {
        let _stage = upstream.metrics().span(Stage::Database);
        shutdown.until(deadline, async {
            let row = writer.client.query_one(
                "SELECT generation_id, publication_sequence FROM rfc.active_generation WHERE singleton",
                &[],
            ).await?;
            let active: Option<i64> = row.try_get(0)?;
            let priors = match active {
                Some(generation) => load_prior(writer, generation).await?,
                None => BTreeMap::new(),
            };
            let hints = match report.job_id {
                Some(job) => load_hints(writer, job).await?,
                None => BTreeSet::new(),
            };
            Ok((active, row.try_get::<_, i64>(1)?, priors, hints))
        }).await?
    };
    report.active_generation_id = active;
    report.publication_sequence = sequence;

    shutdown
        .until(deadline, async {
            upstream.after_fence().await;
            Ok(())
        })
        .await?;
    let before = shutdown.until(deadline, upstream.catalog()).await?;
    let catalog_digest = BlobDigest::hash(&before.bytes);
    report.catalog_before = Some(observation(&before, catalog_digest));
    let accepted = config.ingestion.max_accepted_bytes;
    remaining(accepted, before.bytes.len() as u64)?;
    let catalog = {
        let _permit = upstream.parse_slot(shutdown).await?;
        let _stage = upstream.metrics().parse_start();
        let bytes = before.bytes.clone();
        tokio::task::spawn_blocking(move || parse_catalog(&bytes)).await??
    };
    let expected = expected(&catalog, scope)?;
    report.expected_txt = expected.len();
    report.catalog_without_txt = catalog
        .entries
        .values()
        .filter(|entry| !entry.has_txt)
        .count();
    if scope == Scope::Full
        && let Some(id) = priors.keys().find(|id| !expected.contains(id))
    {
        return Err(format!("catalog lost previously accepted RFC {}", id.get()).into());
    }

    let inventory = {
        let _stage = upstream.metrics().span(Stage::Rsync);
        let timeout = Duration::from_secs(config.ingestion.discovery.inventory_timeout_seconds)
            .min(deadline.saturating_duration_since(Instant::now()));
        inventory::list(upstream.rsync_module(), timeout, shutdown).await?
    };
    report.inventory_txt = inventory.len();
    let metadata: Vec<_> = inventory
        .iter()
        .map(|(id, entry)| (id.get(), entry.length, &entry.modified_utc))
        .collect();
    report.inventory_digest = Some(identity("rfc-rsync-inventory-v1", &metadata)?.to_string());
    report.mirror_only_txt = inventory
        .keys()
        .filter(|id| !catalog.entries.contains_key(id))
        .count();
    for id in expected.iter().filter(|id| !inventory.contains_key(id)) {
        report.missing_expected_txt += 1;
        report.first_missing_expected.get_or_insert(id.get());
        if priors.contains_key(id) {
            return Err(
                format!("rsync inventory lost previously accepted RFC {}", id.get()).into(),
            );
        }
    }
    for id in catalog
        .entries
        .iter()
        .filter_map(|(&id, entry)| entry.has_txt.then_some(id))
    {
        let event = SyncRequested::publication(id.get(), 0);
        let digest =
            BlobDigest::from_hex(&event.observation_digest).expect("generated event digest");
        if hints.contains(&digest) {
            if inventory.contains_key(&id) {
                report.hints_present += 1;
            } else {
                report.hints_awaiting_mirror += 1;
            }
        }
    }
    report.hints_awaiting_catalog =
        hints.len() - report.hints_present - report.hints_awaiting_mirror;
    if report.missing_expected_txt > 0
        || report.hints_awaiting_catalog > 0
        || report.hints_awaiting_mirror > 0
    {
        return Ok(Outcome::Deferred);
    }

    let mut local = BTreeMap::new();
    let mut reuse_txt = BTreeMap::new();
    let mut local_with_https = 0;
    let mut local_without_https = 0;
    for (&id, listed) in &inventory {
        if expected.contains(&id) {
            match priors.get(&id) {
                Some(prior) => {
                    if prior.length != listed.length {
                        report.inventory_size_changes += 1;
                    }
                    local.insert(prior.digest, prior.length);
                    if let Some(original) = &prior.https
                        && original.length == listed.length
                    {
                        local_with_https += 1;
                        reuse_txt.insert(id, original.clone());
                    } else {
                        local_without_https += 1;
                    }
                }
                None => report.new_txt += 1,
            }
        }
    }
    let available = remaining(accepted, before.bytes.len() as u64)?;
    if !local.is_empty() {
        let shutdown = shutdown.clone();
        tokio::task::spawn_blocking(move || verify_local(local, available, &shutdown, deadline))
            .await??;
    }
    report.local_https_txt_audit_pending = local_with_https;
    report.local_txt_needs_https = local_without_https;
    let registry = Registry::load()?;
    let scope_name = scope.as_str();
    let checkpoint = Context {
        scope: scope_name,
        key: identity(
            "rfc-https-processing-selection-v1",
            &(
                scope_name,
                catalog_digest.to_string(),
                expected.iter().map(|id| id.get()).collect::<Vec<_>>(),
                POLICY,
                PARSER,
                INDEX,
                &registry.version,
                active,
                sequence,
            ),
        )?,
    };
    let selection_id = checkpoint.key.to_string();
    report.selection_id = Some(selection_id.clone());

    let mut reuse_xml = if let Some(active) = active {
        shutdown
            .until(
                deadline,
                load_prior_xml(writer, active, catalog_digest, &registry.version),
            )
            .await?
    } else {
        BTreeMap::new()
    };
    let (saved_txt, saved_xml) = shutdown
        .until(deadline, checkpoint.load_https(writer))
        .await?;
    reuse_txt.extend(saved_txt.into_iter().filter(|(id, prior)| {
        expected.contains(id)
            && inventory
                .get(id)
                .is_some_and(|entry| entry.length == prior.length)
    }));
    reuse_xml.extend(
        saved_xml
            .into_iter()
            .filter(|(id, _)| expected.contains(id)),
    );
    let accepted = Arc::new(Accepted::new(accepted));
    accepted.charge(before.bytes.len() as u64)?;
    let spool = Spool::open().await?;
    let result = async {
        spool.stage_catalog("catalog-before", &before).await?;
        let work_stop = Shutdown::new();
        let selection = Selection {
            checkpoint,
            catalog: &catalog,
            registry: &registry,
            header: serde_json::json!({
                "selection_id": selection_id,
                "catalog_digest": catalog_digest.to_string(),
                "inventory_digest": report.inventory_digest,
                "scope": scope.as_str(),
                "active_generation_id": active,
                "publication_sequence": sequence,
                "policy_version": POLICY,
                "parser_version": PARSER,
                "index_version": INDEX,
                "registry_version": registry.version,
            }),
            expected: expected.into_iter().collect(),
            txt: reuse_txt,
            xml: reuse_xml,
            accepted: accepted.clone(),
        };
        {
            let work = spool.acquire(
                selection,
                writer,
                config,
                upstream,
                &work_stop,
                &mut report.acquisition,
            );
            tokio::pin!(work);
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    work_stop.stop();
                    work.await?;
                    shutdown.check()?;
                }
                result = &mut work => result?,
            }
        }
        rfc_storage::fixture_boundary("before_https_catalog_validation");
        let fence_deadline = Instant::now()
            + Duration::from_secs(config.ingestion.https.request_timeout_seconds * 3 + 120);
        let after = shutdown.until(fence_deadline, upstream.catalog()).await?;
        accepted.charge(after.bytes.len() as u64)?;
        report.acquisition.accepted_bytes = accepted.used();
        let end_digest = BlobDigest::hash(&after.bytes);
        report.catalog_after = Some(observation(&after, end_digest));
        if catalog_digest != end_digest || before.bytes != after.bytes {
            checkpoint.remove(&writer.client, None).await?;
            return Err("catalog changed during acquisition".into());
        }
        spool.stage_catalog("catalog-after", &after).await?;
        rfc_storage::fixture_boundary("after_https_catalog_validation");
        Ok(Outcome::Complete)
    }
    .await;
    let cleanup = spool.remove().await;
    cleanup?;
    result
}

fn observation(capture: &Capture, digest: BlobDigest) -> Observation {
    Observation {
        digest: digest.to_string(),
        source_url: Source::Catalog.url(),
        observed_us: capture.observed_us,
        fetched_us: capture.fetched_us,
        has_etag: capture.etag.is_some(),
        has_last_modified: capture.last_modified.is_some(),
    }
}

pub(crate) fn parse_catalog(bytes: &[u8]) -> Result<xml::Catalog> {
    let catalog = xml::catalog(bytes)?;
    if catalog.entries.len() > MAX_CATALOG_ENTRIES {
        return Err("catalog entry count exceeds bounds".into());
    }
    Ok(catalog)
}

fn expected(catalog: &xml::Catalog, scope: Scope) -> Result<BTreeSet<RfcNumber>> {
    let expected: BTreeSet<_> = match scope {
        Scope::Full => catalog
            .entries
            .iter()
            .filter_map(|(&id, entry)| entry.has_txt.then_some(id))
            .collect(),
        Scope::Sample => SAMPLE
            .into_iter()
            .map(|id| RfcNumber::new(id).expect("fixed sample ID"))
            .collect(),
    };
    if expected.is_empty()
        || expected
            .iter()
            .any(|id| catalog.entries.get(id).is_none_or(|entry| !entry.has_txt))
    {
        return Err("catalog lacks an expected TXT entry".into());
    }
    Ok(expected)
}

async fn load_prior(writer: &Writer, generation: i64) -> Result<BTreeMap<RfcNumber, PriorTxt>> {
    let rows = writer.client.query(
        "SELECT d.rfc_id::integer, b.digest::bytea, b.byte_length::bigint, b.integrity_state,
            coalesce(o.https_verification = 'verified' AND o.kind = 'txt'
                AND o.raw_digest = v.txt_digest
                AND o.source_url = 'https://www.rfc-editor.org/rfc/rfc' || d.rfc_id || '.txt', false),
            o.identity::bytea, (extract(epoch FROM o.observed_at) * 1000000)::bigint,
            (extract(epoch FROM o.fetched_at) * 1000000)::bigint
         FROM rfc.generation_documents d
         JOIN rfc.document_versions v ON v.identity = d.document_version
         JOIN rfc.blobs b ON b.digest = v.txt_digest
         LEFT JOIN rfc.source_observations o ON o.identity = d.txt_observation
         WHERE d.generation_id = $1 ORDER BY d.rfc_id LIMIT $2",
        &[&generation, &(MAX_CATALOG_ENTRIES as i64 + 1)],
    ).await?;
    if rows.len() > MAX_CATALOG_ENTRIES {
        return Err("active TXT membership exceeds discovery bounds".into());
    }
    rows.into_iter()
        .map(|row| {
            let id = RfcNumber::new(row.try_get(0)?).ok_or("invalid active RFC number")?;
            let digest = BlobDigest::from_bytes(row.try_get::<_, &[u8]>(1)?.try_into()?);
            let length = u64::try_from(row.try_get::<_, i64>(2)?)?;
            if length == 0 || length > TXT_LIMIT as u64 || row.try_get::<_, &str>(3)? != "verified"
            {
                return Err(format!(
                    "canonical TXT for RFC {} is outside verified bounds",
                    id.get()
                )
                .into());
            }
            Ok((
                id,
                PriorTxt {
                    digest,
                    length,
                    https: if row.try_get(4)? {
                        Some(original(&row, digest, length, 5)?)
                    } else {
                        None
                    },
                },
            ))
        })
        .collect()
}

async fn load_prior_xml(
    writer: &Writer,
    generation: i64,
    catalog: BlobDigest,
    registry: &str,
) -> Result<BTreeMap<RfcNumber, Original>> {
    let rows = writer
        .client
        .query(
            "SELECT d.rfc_id::integer, b.digest::bytea, b.byte_length::bigint, b.integrity_state,
            o.identity::bytea, (extract(epoch FROM o.observed_at) * 1000000)::bigint,
            (extract(epoch FROM o.fetched_at) * 1000000)::bigint
         FROM rfc.generation_documents d
         JOIN rfc.generations g ON g.id = d.generation_id
         JOIN rfc.generation_ingestion i ON i.generation_id = g.id
         JOIN rfc.source_observations c ON c.identity = i.catalog_before
         JOIN rfc.source_observations o ON o.identity = d.rfcxml_observation
         JOIN rfc.blobs b ON b.digest = o.raw_digest
         WHERE d.generation_id = $1 AND c.raw_digest = $2::bytea
           AND g.parser_version = $3 AND g.index_version = $4 AND i.policy_registry = $5
           AND o.kind = 'rfcxml' AND o.https_verification = 'verified'
           AND o.source_url = 'https://www.rfc-editor.org/rfc/rfc' || d.rfc_id || '.xml'
           AND o.fetched_at BETWEEN clock_timestamp() - interval '24 hours' AND clock_timestamp()
         ORDER BY d.rfc_id LIMIT $6",
            &[
                &generation,
                &catalog.as_bytes().as_slice(),
                &PARSER,
                &INDEX,
                &registry,
                &(MAX_CATALOG_ENTRIES as i64 + 1),
            ],
        )
        .await?;
    if rows.len() > MAX_CATALOG_ENTRIES {
        return Err("active RFCXML membership exceeds discovery bounds".into());
    }
    rows.into_iter()
        .map(|row| {
            let id = RfcNumber::new(row.try_get(0)?).ok_or("invalid active RFC number")?;
            let digest = BlobDigest::from_bytes(row.try_get::<_, &[u8]>(1)?.try_into()?);
            let length = u64::try_from(row.try_get::<_, i64>(2)?)?;
            if length == 0 || length > XML_LIMIT as u64 || row.try_get::<_, &str>(3)? != "verified"
            {
                return Err(format!(
                    "canonical RFCXML for RFC {} is outside verified bounds",
                    id.get()
                )
                .into());
            }
            Ok((id, original(&row, digest, length, 4)?))
        })
        .collect()
}

fn original(
    row: &tokio_postgres::Row,
    digest: BlobDigest,
    length: u64,
    index: usize,
) -> Result<Original> {
    let observation = BlobDigest::from_bytes(row.try_get::<_, &[u8]>(index)?.try_into()?);
    Ok(Original {
        digest,
        length,
        observation: Some(observation.to_string()),
        observed_us: row.try_get(index + 1)?,
        fetched_us: row.try_get(index + 2)?,
        etag: None,
        last_modified: None,
        checkpoint_expiry: None,
        checkpoint_version: None,
    })
}

async fn load_hints(writer: &Writer, job: i64) -> Result<BTreeSet<BlobDigest>> {
    let rows = writer
        .client
        .query(
            "SELECT observation_digest::bytea FROM rfc.control_events
         WHERE job_id = $1 AND status IN ('accepted', 'coalesced') LIMIT $2",
            &[&job, &(MAX_CATALOG_ENTRIES as i64 + 1)],
        )
        .await?;
    if rows.len() > MAX_CATALOG_ENTRIES {
        return Err("discovery Feed hints exceed bounds".into());
    }
    rows.into_iter()
        .map(|row| {
            Ok(BlobDigest::from_bytes(
                row.try_get::<_, &[u8]>(0)?.try_into()?,
            ))
        })
        .collect()
}

fn remaining(available: u64, charged: u64) -> Result<u64> {
    available
        .checked_sub(charged)
        .ok_or_else(|| "discovery exceeds accepted byte limit".into())
}

fn verify_local(
    objects: BTreeMap<BlobDigest, u64>,
    available: u64,
    shutdown: &Shutdown,
    deadline: Instant,
) -> Result<u64> {
    let reader = BlobReader::open(&mirror_root(), false)?;
    let mut unspent = available;
    for (digest, length) in objects {
        shutdown.check()?;
        if Instant::now() >= deadline {
            return Err("discovery deadline exceeded while verifying local TXT".into());
        }
        unspent = remaining(unspent, length)?;
        reader
            .verify_blob(digest, i64::try_from(length)?)
            .map_err(|error| format!("canonical TXT {digest} failed verification: {error}"))?;
    }
    Ok(available - unspent)
}
