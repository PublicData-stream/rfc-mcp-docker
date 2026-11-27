//! Materialize an audited selection into an immutable generation.

use crate::{
    Candidate, Evidence, Membership, Observation, Writer,
    acquisition::{Classification, Join, Pair},
    audit::{Completion, Input, Proof},
    checkpoint::Context,
    discovery::{INDEX, PARSER, POLICY, Scope},
    insert_observation,
    parse::{Budget, DOCUMENT_BYTES},
    register_blob,
    shutdown::Shutdown,
    upstream::{Capture, Source, TXT_LIMIT, XML_LIMIT},
    xml,
};
use rfc_core::{BlobDigest, ByteRange, Coverage, GenerationId, Relation, RfcNumber};
use rfc_storage::{
    blob::{BlobReader, BlobWriter},
    manifest::identity,
    postgres::{Result, mirror_root},
};
use std::{collections::BTreeSet, sync::Arc};
use tokio_postgres::GenericClient;

const FAMILIES: [&str; 4] = [
    "all_references",
    "classified_references",
    "updates",
    "obsoletes",
];

pub(crate) struct Selection<'a> {
    pub scope: Scope,
    pub context: Context,
    pub inventory: &'a str,
    pub sequence: i64,
    pub catalog: &'a xml::Catalog,
    pub fixture_rules: Option<Arc<crate::policy::FixtureRules>>,
    pub fixture: bool,
    pub before: &'a Capture,
    pub after: &'a Capture,
    pub documents: Join,
    pub batch_size: usize,
}

pub(crate) async fn seal(
    writer: &mut Writer,
    selection: Selection<'_>,
    audit: &mut Completion,
    job: Option<i64>,
    shutdown: &Shutdown,
) -> Result<(GenerationId, BlobDigest)> {
    shutdown.check()?;
    let generation = writer.start_ingestion(PARSER, INDEX, job).await?;
    let result = build(writer, generation, selection, audit, shutdown).await;
    if result.is_err() && writer.fail(generation).await.is_err() {
        crate::diagnostics::event(
            crate::diagnostics::Level::Warn,
            "candidate_failure_record_failed",
            || serde_json::json!({"generation_id": generation.get(), "job_id": job}),
        );
    }
    result
}

async fn build(
    writer: &mut Writer,
    generation: GenerationId,
    selection: Selection<'_>,
    audit: &mut Completion,
    shutdown: &Shutdown,
) -> Result<(GenerationId, BlobDigest)> {
    let chosen = selection
        .documents
        .pairs
        .keys()
        .map(|&id| rfc(id))
        .collect::<Result<BTreeSet<_>>>()?;
    let expected = chosen.iter().map(|id| id.get() as i32).collect::<Vec<_>>();
    let catalog_only = selection
        .catalog
        .entries
        .iter()
        .filter(|(_, entry)| !entry.has_txt)
        .map(|(id, entry)| (id.get(), entry))
        .collect::<Vec<_>>();
    let reader = Arc::new(BlobReader::open(&mirror_root(), false)?);

    let (before_blob, after_blob) = {
        let before = selection.before.bytes.clone();
        let after = selection.after.bytes.clone();
        tokio::task::spawn_blocking(move || {
            let writer = BlobWriter::open(&mirror_root())?;
            let limit = Source::Catalog.limit() as u64;
            Ok::<_, std::io::Error>((
                writer.install(before.as_slice(), limit)?.blob(),
                writer.install(after.as_slice(), limit)?.blob(),
            ))
        })
        .await??
    };
    let candidate = writer.batch(generation).await?;
    durable(&candidate.tx).await?;
    let before = catalog_observation(
        &candidate.tx,
        selection.before,
        register_blob(&candidate.tx, before_blob).await?,
    )
    .await?;
    let after = catalog_observation(
        &candidate.tx,
        selection.after,
        register_blob(&candidate.tx, after_blob).await?,
    )
    .await?;
    candidate
        .tx
        .execute(
            "INSERT INTO rfc.generation_ingestion
           (generation_id, scope, catalog_before, catalog_after, catalog_only)
         VALUES ($1, $2, $3::bytea, $4::bytea, $5::text::jsonb)",
            &[
                &candidate.generation(),
                &selection.scope.as_str(),
                &bytes(&before),
                &bytes(&after),
                &serde_json::to_string(&catalog_only)?,
            ],
        )
        .await?;
    let (started, expires) = audit.window();
    let inventory = digest(selection.inventory)?;
    candidate.tx.execute(
        "INSERT INTO rfc.generation_audit
           (generation_id, selection, inventory, policy, expected_rfc_ids, expected_sequence, started_us, expires_us, fixture)
         VALUES ($1, $2::bytea, $3::bytea, $4, $5, $6, $7, $8, $9)",
        &[&candidate.generation(), &bytes(&selection.context.key), &bytes(&inventory), &POLICY,
          &expected, &selection.sequence, &started, &expires, &selection.fixture],
    ).await?;
    candidate.commit().await?;

    let mut incomplete = [
        !catalog_only.is_empty(),
        !catalog_only.is_empty(),
        false,
        false,
    ];
    let catalog = selection.catalog;
    let full = selection.scope == Scope::Full;
    for edges in catalog.edges.chunks(128) {
        shutdown.check()?;
        let candidate = writer.batch(generation).await?;
        for edge in edges {
            if full || chosen.contains(&edge.source) || chosen.contains(&edge.target) {
                record_edge(
                    &candidate,
                    Relation {
                        source: edge.source,
                        kind: edge.kind,
                        target: edge.target,
                    },
                    Evidence {
                        observation: Some(before),
                        version: None,
                        range: None,
                        method: "catalog",
                        xml_locator: Some(&edge.locator),
                    },
                )
                .await?;
            }
        }
        candidate.commit().await?;
    }
    let catalog_ids: Vec<_> = catalog
        .entries
        .keys()
        .copied()
        .filter(|id| full || chosen.contains(id))
        .collect();
    for ids in catalog_ids.chunks(128) {
        shutdown.check()?;
        let candidate = writer.batch(generation).await?;
        for &id in ids {
            for family in &FAMILIES[2..] {
                let state = if catalog.incomplete.contains(&(id, family))
                    || (!full && catalog.incomplete.iter().any(|(_, item)| item == family))
                {
                    Coverage::Partial
                } else {
                    Coverage::Complete
                };
                candidate.coverage(Some(id), family, state).await?;
            }
            if !chosen.contains(&id) {
                for family in &FAMILIES[..2] {
                    candidate
                        .coverage(Some(id), family, Coverage::Unavailable)
                        .await?;
                }
            }
        }
        candidate.commit().await?;
    }
    for (flag, family) in incomplete[2..].iter_mut().zip(&FAMILIES[2..]) {
        *flag = catalog.incomplete.iter().any(|(_, item)| item == family);
    }

    let mut documents = selection.documents.pairs.into_iter().peekable();
    while documents.peek().is_some() {
        let candidate = writer.batch(generation).await?;
        durable(&candidate.tx).await?;
        for (id, pair) in documents.by_ref().take(selection.batch_size) {
            shutdown.check()?;
            let id = rfc(id)?;
            let coverage = store_document(
                &candidate,
                reader.clone(),
                id,
                &catalog.entries[&id],
                before,
                pair,
                (
                    selection.context.key,
                    audit,
                    selection.fixture,
                    selection.fixture_rules.clone(),
                ),
                shutdown,
            )
            .await?;
            incomplete[0] |= coverage.0 != Coverage::Complete;
            incomplete[1] |= coverage.1 != Coverage::Complete;
        }
        candidate.commit().await?;
    }

    let candidate = writer.batch(generation).await?;
    durable(&candidate.tx).await?;
    for (&family, partial) in FAMILIES.iter().zip(incomplete) {
        let state = if partial || !full {
            Coverage::Partial
        } else {
            Coverage::Complete
        };
        candidate.coverage(None, family, state).await?;
    }
    let manifest = candidate.prepare_readiness().await?;
    shutdown.check()?;
    selection.context.remove(&candidate.tx, None).await?;
    audit.prepare(candidate.generation())?;
    candidate.commit_readiness(manifest).await
}

async fn durable(tx: &impl GenericClient) -> Result<()> {
    tx.batch_execute("SET LOCAL synchronous_commit = on; SET LOCAL statement_timeout = '120s'")
        .await?;
    Ok(())
}

async fn catalog_observation(
    tx: &impl GenericClient,
    capture: &Capture,
    raw_digest: BlobDigest,
) -> Result<BlobDigest> {
    insert_observation(
        tx,
        &Observation {
            kind: "catalog",
            url: &Source::Catalog.url(),
            observed_us: capture.observed_us,
            fetched_us: Some(capture.fetched_us),
            etag: capture.etag.as_deref(),
            last_modified: capture.last_modified.as_deref(),
            raw_digest,
            https_verification: "verified",
        },
        None,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn store_document(
    candidate: &Candidate<'_>,
    reader: Arc<BlobReader>,
    id: RfcNumber,
    entry: &xml::Entry,
    catalog: BlobDigest,
    pair: Pair,
    (selection, audit, fixture, fixture_rules): (
        BlobDigest,
        &Completion,
        bool,
        Option<Arc<crate::policy::FixtureRules>>,
    ),
    shutdown: &Shutdown,
) -> Result<(Coverage, Coverage)> {
    let txt = pair.txt.ok_or("missing joined TXT")?;
    let xml = pair.xml.ok_or("missing joined RFCXML")?;
    let input = Input {
        rfc: id.get(),
        digest: txt.original.ok_or("missing TXT digest")?,
        length: txt.length.ok_or("missing TXT length")?,
        observation: txt.observation.ok_or("missing TXT observation")?,
    };
    let raw = digest(&input.digest)?;
    let observation = digest(&input.observation)?;
    let version = digest(&txt.version.ok_or("missing TXT document version")?)?;
    let xml_observation = xml.observation.as_deref().map(digest).transpose()?;
    let rsync_url = format!(
        "rsync://rsync.rfc-editor.org/rfcs-text-only/rfc{}.txt",
        id.get()
    );
    let (method, receipt, start, end, choice, algorithm, peer) = match audit.proof(&input)? {
        Proof::Checksum(receipt) => (
            "rsync_checksum_match",
            digest(&receipt.id)?,
            receipt.started_us,
            receipt.observed_us,
            Some(receipt.checksum_choice.as_str()),
            receipt.algorithm.as_deref(),
            None,
        ),
        Proof::Repair(proof) => {
            if proof.rsync_digest != input.digest || proof.rsync_length != input.length {
                return Err("sealed repair disagrees with the candidate original".into());
            }
            let peer = insert_observation(
                &candidate.tx,
                &Observation {
                    kind: "txt",
                    url: &rsync_url,
                    observed_us: proof.rsync_observed_us,
                    fetched_us: Some(proof.rsync_observed_us),
                    etag: None,
                    last_modified: None,
                    raw_digest: raw,
                    https_verification: "unverified",
                },
                None,
                false,
            )
            .await?;
            let receipt = identity(
                "rfc-rsync-exact-repair-v1",
                &(&input, peer.to_string(), proof.rsync_observed_us),
            )?;
            (
                "dual_source_byte_match",
                receipt,
                proof.original.observed_us,
                proof.rsync_observed_us,
                None,
                None,
                Some(peer),
            )
        }
    };
    candidate
        .tx
        .execute(
            "INSERT INTO rfc.generation_source_evidence
           (generation_id, rfc_id, txt_observation, document_version, txt_digest, byte_length,
            method, receipt, rsync_url, started_us, observed_us, checksum_choice,
            algorithm, rsync_observation, rfcxml_outcome, rfcxml_observation, selection)
         VALUES ($1, $2::integer, $3::bytea, $4::bytea, $5::bytea, $6::bigint,
            $7, $8::bytea, $9, $10, $11, $12, $13, $14::bytea, $15, $16::bytea, $17::bytea)",
            &[
                &candidate.generation(),
                &(id.get() as i32),
                &bytes(&observation),
                &bytes(&version),
                &bytes(&raw),
                &i64::try_from(input.length)?,
                &method,
                &bytes(&receipt),
                &rsync_url,
                &start,
                &end,
                &choice,
                &algorithm,
                &peer.as_ref().map(bytes),
                &xml.outcome.sealed_name(),
                &xml_observation.as_ref().map(bytes),
                &bytes(&selection),
            ],
        )
        .await?;
    let metadata = candidate
        .member_with_sources(
            &Membership {
                rfc: id,
                version,
                catalog,
                txt: observation,
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

    let xml_raw = xml.original.as_deref().map(digest).transpose()?;
    let stop = shutdown.clone();
    let txt_length = i64::try_from(input.length)?;
    let entry = entry.clone();
    let (coverage, xml_edges, txt_edges, rights) = tokio::task::spawn_blocking(move || {
        let budget = Budget {
            bytes: DOCUMENT_BYTES,
            shutdown: &stop,
        };
        stop.check()?;
        let mut coverage = None;
        let mut xml_edges = Vec::new();
        if xml.outcome == Classification::Captured {
            let bytes = canonical(
                &reader,
                xml_raw.ok_or("missing RFCXML digest")?,
                i64::try_from(xml.length.ok_or("missing RFCXML length")?)?,
                XML_LIMIT,
            )?;
            let parsed = xml::references(&bytes, id, &budget)?;
            coverage = Some((parsed.all, parsed.classified));
            xml_edges = parsed.edges;
        }
        let bytes = canonical(&reader, raw, txt_length, TXT_LIMIT)?;
        let mut rights = crate::policy::classify(
            id,
            raw,
            metadata,
            Some(&entry),
            &bytes,
            !fixture,
            fixture_rules.as_deref(),
        );
        rights.search |= fixture;
        let mut txt_edges = Vec::new();
        if !coverage.is_some_and(|(all, _)| all == Coverage::Complete) {
            let parsed = crate::text::index_with_budget(&bytes, &budget)?;
            txt_edges = parsed.references;
            if coverage.is_none() {
                let fallback = (parsed.all_references, parsed.classified_references);
                coverage = Some(
                    if matches!(xml.outcome, Classification::NotFound | Classification::Gone) {
                        fallback
                    } else {
                        (cap(fallback.0), cap(fallback.1))
                    },
                );
            }
        }
        Result::Ok((
            coverage.ok_or("document lacks reference coverage")?,
            xml_edges,
            txt_edges,
            rights,
        ))
    })
    .await??;
    crate::policy::store(&candidate.tx, candidate.generation(), &rights).await?;
    for edge in xml_edges {
        record_edge(
            candidate,
            Relation {
                source: id,
                kind: edge.kind,
                target: edge.target,
            },
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
    for edge in txt_edges {
        record_edge(
            candidate,
            Relation {
                source: id,
                kind: edge.kind,
                target: edge.target,
            },
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
    candidate
        .coverage(Some(id), FAMILIES[0], coverage.0)
        .await?;
    candidate
        .coverage(Some(id), FAMILIES[1], coverage.1)
        .await?;
    Ok(coverage)
}

async fn record_edge(
    candidate: &Candidate<'_>,
    relation: Relation,
    evidence: Evidence<'_>,
) -> Result<()> {
    let proof = candidate.evidence(&evidence).await?;
    candidate.relation(relation, &[proof]).await
}

pub(crate) fn canonical(
    reader: &BlobReader,
    digest: BlobDigest,
    length: i64,
    cap: usize,
) -> Result<Vec<u8>> {
    let span = ByteRange::new(0, length).ok_or("invalid artifact length")?;
    let bytes = reader.read_range(digest, span, cap)?;
    if i64::try_from(reader.open_blob(digest)?.metadata()?.len())? != length
        || BlobDigest::hash(&bytes) != digest
    {
        return Err(format!("canonical original {digest} failed candidate verification").into());
    }
    Ok(bytes)
}

fn rfc(value: u32) -> Result<RfcNumber> {
    RfcNumber::new(i32::try_from(value)?).ok_or_else(|| "invalid selected RFC number".into())
}

fn digest(value: &str) -> Result<BlobDigest> {
    BlobDigest::from_hex(value).ok_or_else(|| "invalid artifact digest".into())
}

fn bytes(value: &BlobDigest) -> &[u8] {
    value.as_bytes().as_slice()
}

fn cap(coverage: Coverage) -> Coverage {
    if coverage == Coverage::Complete {
        Coverage::Partial
    } else {
        coverage
    }
}
