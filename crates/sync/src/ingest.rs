use crate::{
    Candidate, Document, Evidence, IndexedText, Membership, Observation, Writer,
    policy::Registry,
    recovery::recover_before_ingestion,
    text::{self, TextIndex},
    upstream::{self, Capture, Download, Source, Upstream},
    xml,
};
use rfc_core::{BlobDigest, ByteRange, Coverage, GenerationId, Relation, RelationKind, RfcNumber};
use rfc_storage::{
    blob::{BlobReader, BlobWriter},
    postgres::{Result, mirror_root},
};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

const PARSER: &str = "rfc-txt-catalog-1";
const INDEX: &str = "rfc-original-bytes-pg19-1";
const FAMILIES: [&str; 4] = [
    "all_references",
    "classified_references",
    "updates",
    "obsoletes",
];
const SAMPLE: [i32; 3] = [2119, 8174, 9110];
const MAX_INPUT: u64 = 4 * 1024 * 1024 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Full,
    Sample,
}

impl Scope {
    fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Sample => "sample",
        }
    }
}

#[derive(Serialize)]
struct Report {
    scope: &'static str,
    state: &'static str,
    generation_id: Option<String>,
    manifest_digest: Option<String>,
    catalog_digest: Option<String>,
    current_rfc: Option<u32>,
    expected_txt: usize,
    verified_txt: usize,
    reused_txt: usize,
    catalog_only: usize,
    invalid_utf8: usize,
    contains_nul: usize,
    incomplete_sections: usize,
    incomplete_notices: usize,
    metadata_only: usize,
    partial_references: usize,
    unclassified_or_incomplete: usize,
    xml_unavailable: usize,
    catalog_dangling_observations: usize,
    reference_targets_without_txt: usize,
    accepted_bytes: u64,
    diagnostics: Vec<String>,
    error: Option<String>,
}

impl Report {
    fn new(scope: Scope) -> Self {
        Self {
            scope: scope.as_str(),
            state: "failed",
            generation_id: None,
            manifest_digest: None,
            catalog_digest: None,
            current_rfc: None,
            expected_txt: 0,
            verified_txt: 0,
            reused_txt: 0,
            catalog_only: 0,
            invalid_utf8: 0,
            contains_nul: 0,
            incomplete_sections: 0,
            incomplete_notices: 0,
            metadata_only: 0,
            partial_references: 0,
            unclassified_or_incomplete: 0,
            xml_unavailable: 0,
            catalog_dangling_observations: 0,
            reference_targets_without_txt: 0,
            accepted_bytes: 0,
            diagnostics: Vec::new(),
            error: None,
        }
    }

    fn accept(&mut self, bytes: usize) -> Result<()> {
        self.accepted_bytes = self
            .accepted_bytes
            .checked_add(u64::try_from(bytes)?)
            .filter(|bytes| *bytes <= MAX_INPUT)
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

/// Reconciles a candidate using the official upstream endpoints.
pub async fn reconcile(scope: Scope) -> Result<()> {
    execute(Upstream::official(), Registry::load(), scope).await
}

/// Fixture endpoints are compiled only into the dedicated test build.
#[cfg(feature = "phase2-fixtures")]
pub async fn reconcile_fixture(scope: Scope) -> Result<()> {
    execute(Upstream::fixture(), Registry::load(), scope).await
}

/// Only the disposable fixture driver passes synthetic review receipts.
#[cfg(feature = "phase3-fixtures")]
pub async fn reconcile_fixture_with_reviews(scope: Scope, reviews: &str) -> Result<()> {
    execute(
        Upstream::fixture(),
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
    let result = async {
        let upstream = upstream?;
        let mut writer = Writer::connect().await?;
        let _coordinator = writer.acquire_coordinator().await?;
        run(&mut writer, upstream, registry, scope, None, &mut report).await
    }
    .await;
    print_report(report, &result)?;
    result.map(|_| ())
}

pub(crate) async fn ingest_locked(
    writer: &mut Writer,
    upstream: Result<Upstream>,
    registry: Result<Registry>,
    scope: Scope,
    job: Option<i64>,
) -> Result<(GenerationId, BlobDigest)> {
    let mut report = Report::new(scope);
    let result = match upstream {
        Ok(upstream) => run(writer, upstream, registry, scope, job, &mut report).await,
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
) -> Result<(GenerationId, BlobDigest)> {
    let root = mirror_root();
    let store = BlobWriter::open(&root)?;
    recover_before_ingestion(writer, &store).await?;
    let reader = BlobReader::open(&root, false)?;
    let registry = registry?;
    let id = writer.start_ingestion(PARSER, INDEX, job).await?;
    report.generation_id = Some(id.get().to_string());
    let mut run = Run {
        writer,
        id,
        scope,
        store,
        reader,
        upstream,
        registry,
        report,
    };
    let result = match tokio::time::timeout(Duration::from_secs(12 * 60 * 60), run.collect()).await
    {
        Ok(result) => result,
        Err(_) => Err("reconciliation deadline exceeded".into()),
    };
    if result.is_err()
        && let Err(error) = run.writer.fail(id).await
    {
        run.report
            .diagnostic(format!("failed to mark candidate failed: {error}"));
    }
    result
}

struct Run<'a> {
    writer: &'a mut Writer,
    id: GenerationId,
    scope: Scope,
    store: BlobWriter,
    reader: BlobReader,
    upstream: Upstream,
    registry: Registry,
    report: &'a mut Report,
}

#[derive(Clone, Copy)]
struct PriorTxt {
    digest: BlobDigest,
    observation: BlobDigest,
}

impl Run<'_> {
    async fn collect(&mut self) -> Result<(GenerationId, BlobDigest)> {
        let start = self.catalog_capture().await?;
        let catalog = xml::catalog(&start.bytes)?;
        if catalog.entries.len() > 20_000 {
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
        let baseline = self.baseline().await?;
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
        let start_digest = self.install(Source::Catalog, &start.bytes).await?;
        self.report.catalog_digest = Some(start_digest.to_string());
        let before = self.store_catalog(&start, start_digest).await?;
        drop(start);
        self.store_catalog_graph(&catalog, &selected, before)
            .await?;

        let ordered: Vec<_> = selected.iter().copied().collect();
        for ids in ordered.chunks(upstream::BATCH) {
            let batch = self.upstream.stage(ids).await?;
            for &id in ids {
                self.report.current_rfc = Some(id.get());
                let bytes = batch.read(&self.store, id)?;
                self.store_document(
                    id,
                    &catalog.entries[&id],
                    before,
                    baseline.get(&id).copied(),
                    &bytes,
                    &selected,
                )
                .await?;
            }
        }
        self.report.current_rfc = None;

        let end = self.catalog_capture().await?;
        let end_digest = self.install(Source::Catalog, &end.bytes).await?;
        let after = self.store_catalog(&end, end_digest).await?;
        if end_digest != start_digest {
            return Err("RFC catalog changed during reconciliation".into());
        }
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
        let validated: bool = candidate.tx.query_one(
            "SELECT count(*) = $2 AND coalesce(bool_and(
                gd.catalog_observation = $3::bytea AND d.sealed AND d.section_state IS NOT NULL
                AND d.parser_version = $4 AND d.index_version = $5
                AND (SELECT count(*) FROM rfc.relation_coverage c
                     WHERE c.generation_id = gd.generation_id AND c.scope = 'document' AND c.rfc_id = gd.rfc_id) = 4
             ), false)
             FROM rfc.generation_documents gd JOIN rfc.document_versions d ON d.identity = gd.document_version
             WHERE gd.generation_id = $1",
            &[&candidate.generation(), &i64::try_from(selected.len())?, &before.as_bytes().as_slice(), &PARSER, &INDEX],
        ).await?.try_get(0)?;
        if !validated {
            return Err("candidate failed membership or coverage validation".into());
        }
        candidate.ready().await
    }

    async fn baseline(&self) -> Result<BTreeMap<RfcNumber, PriorTxt>> {
        let rows = self.writer.client.query(
            "SELECT d.rfc_id::integer, encode(o.raw_digest, 'hex'), encode(o.identity, 'hex')
             FROM rfc.generation_documents d JOIN rfc.source_observations o ON o.identity = d.txt_observation
             WHERE d.generation_id = (SELECT g.id FROM rfc.generations g
                 JOIN rfc.generation_ingestion i ON i.generation_id = g.id
                 WHERE i.scope = 'full' AND g.state IN ('ready', 'published', 'retained')
                 ORDER BY g.id DESC LIMIT 1)
               AND o.kind = 'txt' AND o.https_verification = 'verified'
               AND o.source_url = 'https://www.rfc-editor.org/rfc/rfc' || d.rfc_id || '.txt'",
            &[],
        ).await?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    RfcNumber::new(row.try_get(0)?).ok_or("invalid baseline RFC number")?,
                    PriorTxt {
                        digest: BlobDigest::from_hex(row.try_get(1)?)
                            .ok_or("invalid baseline TXT digest")?,
                        observation: BlobDigest::from_hex(row.try_get(2)?)
                            .ok_or("invalid baseline observation")?,
                    },
                ))
            })
            .collect()
    }

    async fn catalog_capture(&mut self) -> Result<Capture> {
        match self.upstream.fetch(Source::Catalog).await? {
            Download::Captured(capture) => {
                self.report.accept(capture.bytes.len())?;
                Ok(capture)
            }
            Download::Unavailable => Err("required catalog is unavailable".into()),
        }
    }

    async fn install(&self, source: Source, bytes: &[u8]) -> Result<BlobDigest> {
        let durable = self.store.install(bytes, source.limit() as u64)?.blob();
        self.writer.register_blob(durable).await
    }

    async fn store_catalog(&mut self, capture: &Capture, digest: BlobDigest) -> Result<BlobDigest> {
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

    async fn store_document(
        &mut self,
        id: RfcNumber,
        entry: &xml::Entry,
        catalog: BlobDigest,
        prior: Option<PriorTxt>,
        bytes: &[u8],
        selected: &BTreeSet<RfcNumber>,
    ) -> Result<()> {
        self.report.accept(bytes.len())?;
        let digest = BlobDigest::hash(bytes);
        let mut prior = prior.filter(|prior| prior.digest == digest);
        if let Some(previous) = prior
            && let Err(error) = self.reader.open_blob(previous.digest)
        {
            if error.kind() == std::io::ErrorKind::NotFound {
                prior = None;
            } else {
                return Err(error.into());
            }
        }
        let source = Source::Txt(id);
        let fresh_txt = if prior.is_some() {
            None
        } else {
            let Download::Captured(capture) = self.upstream.fetch(source).await? else {
                return Err("required HTTPS TXT is unavailable".into());
            };
            if bytes != capture.bytes {
                self.store
                    .quarantine_mismatch(bytes, &capture.bytes, source.limit())?;
                return Err(format!("RFC {} rsync and HTTPS TXT disagree", id.get()).into());
            }
            Some(capture)
        };
        let mut indexed = text::index(bytes)?;
        validate_ranges(bytes, &indexed)?;
        for (section, heading) in indexed.sections.iter_mut().zip(&indexed.headings) {
            section.heading = heading;
        }
        let durable = self.store.install(bytes, source.limit() as u64)?.blob();
        if self.reader.read_range(
            durable.digest(),
            ByteRange::new(0, durable.byte_length()).ok_or("invalid blob range")?,
            source.limit(),
        )? != bytes
        {
            return Err("canonical TXT differs after durable installation".into());
        }
        self.writer.register_blob(durable).await?;
        let optional = self.rfcxml(id).await?;
        let policy = if let Some(notices) = self.registry.pagination_notices(id, digest, bytes)? {
            indexed.notice_state = Coverage::Complete;
            indexed.notices = notices;
            "whole_paginated"
        } else {
            self.registry.decide(
                id,
                digest,
                entry,
                indexed.notice_family,
                indexed.notice_state,
            )
        };
        let candidate = self.writer.batch(self.id).await?;
        let txt = match (prior, fresh_txt.as_ref()) {
            (Some(prior), None) => prior.observation,
            (None, Some(capture)) => observe(&candidate, source, capture, digest).await?,
            _ => return Err("missing verified TXT observation".into()),
        };
        let xml_observation = match &optional.capture {
            Some((capture, digest)) => {
                Some(observe(&candidate, Source::Xml(id), capture, *digest).await?)
            }
            None => None,
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
                    notices: indexed.notices,
                },
                Some(IndexedText {
                    section_state: indexed.section_state,
                    line_starts: &indexed.line_starts,
                    text: indexed.text,
                }),
            )
            .await?;
        candidate
            .member_with_sources(
                &Membership {
                    rfc: id,
                    version,
                    catalog,
                    txt,
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
        let mut all = indexed.all_references;
        let mut classified = indexed.classified_references;
        let mut use_txt = true;
        if let Some(xml) = optional.references {
            all = xml.all;
            classified = xml.classified;
            use_txt = all != Coverage::Complete;
            for edge in xml.edges {
                self.report.reference_targets_without_txt +=
                    usize::from(!selected.contains(&edge.target));
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
            for edge in indexed.references {
                self.report.reference_targets_without_txt +=
                    usize::from(!selected.contains(&edge.target));
                store_edge(
                    &candidate,
                    id,
                    edge.kind,
                    edge.target,
                    Evidence {
                        observation: None,
                        version: Some(version),
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
        self.report.reused_txt += usize::from(prior.is_some());
        self.report.verified_txt += 1;
        Ok(())
    }

    async fn rfcxml(&mut self, id: RfcNumber) -> Result<OptionalXml> {
        let source = Source::Xml(id);
        let mut result = OptionalXml::default();
        match self.upstream.fetch(source).await {
            Ok(Download::Captured(capture)) => {
                self.report.accept(capture.bytes.len())?;
                match xml::references(&capture.bytes, id) {
                    Ok(references) => result.references = Some(references),
                    Err(error) => {
                        self.report
                            .diagnostic(format!("RFC {} RFCXML parse: {error}", id.get()));
                        result.failed = true;
                    }
                }
                let digest = self.install(source, &capture.bytes).await?;
                result.capture = Some((capture, digest));
            }
            Ok(Download::Unavailable) => self.report.xml_unavailable += 1,
            Err(error) => {
                self.report.xml_unavailable += 1;
                self.report
                    .diagnostic(format!("RFC {} RFCXML fetch: {error}", id.get()));
                result.failed = true;
            }
        }
        Ok(result)
    }
}

#[derive(Default)]
struct OptionalXml {
    capture: Option<(Capture, BlobDigest)>,
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
        .observe(&Observation {
            kind: source.kind(),
            url: &source.url(),
            observed_us: capture.observed_us,
            fetched_us: capture.fetched_us,
            etag: capture.etag.as_deref(),
            last_modified: capture.last_modified.as_deref(),
            raw_digest: digest,
            https_verification: "verified",
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

fn validate_ranges(bytes: &[u8], indexed: &TextIndex<'_>) -> Result<()> {
    let valid = |range: ByteRange| {
        range
            .select(bytes)
            .is_some_and(|slice| !slice.is_empty() && std::str::from_utf8(slice).is_ok())
    };
    if indexed.sections.iter().any(|section| {
        !valid(section.raw_heading)
            || !valid(section.body)
            || section.raw_heading.start() != section.body.start()
            || section.raw_heading.end() > section.body.end()
    }) || indexed.notices.iter().any(|notice| !valid(notice.range))
        || indexed
            .references
            .iter()
            .any(|evidence| !valid(evidence.range))
    {
        return Err("TXT parser produced an invalid source range".into());
    }
    let mut cursor = 0;
    for chunk in &indexed.chunks {
        if chunk.range.start() != cursor || chunk.range.select(bytes) != Some(chunk.text.as_bytes())
        {
            return Err("TXT chunk does not match its exact source bytes".into());
        }
        cursor = chunk.range.end();
    }
    if indexed.text.is_some() && cursor != bytes.len() as u64 {
        return Err("TXT chunks do not cover the verified blob".into());
    }
    Ok(())
}
