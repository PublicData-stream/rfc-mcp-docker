//! Candidate-writing repository for the synchronization executable.

use rfc_core::{BlobDigest, ByteRange, Coverage, GenerationId, Relation, RfcNumber, SectionId};
use rfc_storage::{
    blob::{BlobWriter, DurableBlob},
    manifest::identity,
    postgres::{Result, RuntimeRole, connect, load_manifest, mirror_root},
};
use std::os::fd::OwnedFd;
use tokio_postgres::{Client, Transaction};

mod bulk;
mod config;
mod daemon;
mod ingest;
mod metrics;
mod parse;
mod pipeline;
mod policy;
mod publication;
mod recovery;
mod text;
mod upstream;
mod xml;

pub use config::check_config;
pub use daemon::run_daemon;
#[cfg(feature = "phase2-fixtures")]
pub use ingest::reconcile_fixture;
#[cfg(feature = "phase3-fixtures")]
pub use ingest::reconcile_fixture_with_reviews;
pub use ingest::{Scope, reconcile};
pub use publication::promote;
pub use recovery::{Maintenance, maintain};
pub use xml::validate_feed;

pub struct Writer {
    client: Client,
}

impl Writer {
    pub async fn connect() -> Result<Self> {
        Ok(Self {
            client: connect(RuntimeRole::Sync).await?,
        })
    }

    /// Registers an already durable blob and checks an existing row before reusing it.
    pub async fn register_blob(&self, blob: DurableBlob) -> Result<BlobDigest> {
        let digest = blob.digest();
        let length = blob.byte_length();
        self.client.execute(
            "INSERT INTO rfc.blobs (digest, byte_length) VALUES ($1::bytea, $2::bigint) ON CONFLICT DO NOTHING",
            &[&digest.as_bytes().as_slice(), &length],
        ).await?;
        let row = self.client.query_one(
            "SELECT byte_length::bigint, integrity_state FROM rfc.blobs WHERE digest = $1::bytea",
            &[&digest.as_bytes().as_slice()],
        ).await?;
        if row.try_get::<_, i64>(0)? != length || row.try_get::<_, &str>(1)? != "verified" {
            return Err("registry has an incompatible blob record".into());
        }
        Ok(digest)
    }

    /// Starts a candidate transaction. Dropping it rolls back all candidate rows.
    pub async fn begin(&mut self, parser: &str, index: &str) -> Result<Candidate<'_>> {
        let tx = self.client.transaction().await?;
        let row = tx.query_one(
            "INSERT INTO rfc.generations (parser_version, index_version) VALUES ($1, $2) RETURNING id",
            &[&parser, &index],
        ).await?;
        let id = GenerationId::new(row.try_get(0)?).ok_or("invalid candidate ID")?;
        Ok(Candidate { id, tx })
    }

    async fn acquire_coordinator(&self) -> Result<OwnedFd> {
        let fence = BlobWriter::coordinate(&mirror_root()).map_err(|error| {
            if error.kind() == std::io::ErrorKind::WouldBlock {
                std::io::Error::other("another synchronization holds the corpus lock")
            } else {
                error
            }
        })?;
        let held: bool = self
            .client
            .query_one("SELECT pg_catalog.pg_try_advisory_lock(-1::bigint)", &[])
            .await?
            .try_get(0)?;
        if !held {
            return Err("another synchronization holds the corpus lock".into());
        }
        Ok(fence)
    }

    async fn start_ingestion(
        &mut self,
        parser: &str,
        index: &str,
        job: Option<i64>,
    ) -> Result<GenerationId> {
        rfc_storage::fixture_boundary("before_candidate_insert");
        let tx = self.client.transaction().await?;
        let row = tx.query_one(
            "INSERT INTO rfc.generations (parser_version, index_version) VALUES ($1, $2) RETURNING id",
            &[&parser, &index],
        ).await?;
        let id: i64 = row.try_get(0)?;
        if let Some(job) = job {
            let attached = tx.execute(
                "UPDATE rfc.sync_jobs SET generation_id = $2,
                    expected_publication_sequence = (SELECT publication_sequence FROM rfc.active_generation WHERE singleton)
                 WHERE id = $1 AND state = 'running'",
                &[&job, &id],
            ).await?;
            if attached != 1 {
                return Err("synchronization job is no longer running".into());
            }
        }
        tx.commit().await?;
        rfc_storage::fixture_boundary("after_candidate_insert");
        GenerationId::new(id).ok_or_else(|| "invalid candidate ID".into())
    }

    async fn batch(&mut self, id: GenerationId) -> Result<Candidate<'_>> {
        Ok(Candidate {
            id,
            tx: self.client.transaction().await?,
        })
    }

    async fn fail(&self, id: GenerationId) -> Result<()> {
        self.client
            .execute(
                "UPDATE rfc.generations SET state = 'failed' WHERE id = $1 AND state = 'building'",
                &[&i64::try_from(id.get())?],
            )
            .await?;
        Ok(())
    }
}

pub struct Observation<'a> {
    pub kind: &'a str,
    pub url: &'a str,
    pub observed_us: i64,
    pub fetched_us: Option<i64>,
    pub etag: Option<&'a str>,
    pub last_modified: Option<&'a str>,
    pub raw_digest: BlobDigest,
    pub https_verification: &'a str,
}

pub struct Document<'a> {
    pub rfc: RfcNumber,
    pub txt: DurableBlob,
    pub parser_version: &'a str,
    pub index_version: &'a str,
    pub encoding: &'a str,
    pub index_state: Coverage,
    pub notice_state: Coverage,
    pub notice_policy: &'a str,
    pub sections: Vec<Section<'a>>,
    pub chunks: Vec<Chunk<'a>>,
    pub notices: Vec<Notice<'a>>,
}

pub struct Section<'a> {
    pub id: SectionId,
    pub parent: Option<SectionId>,
    pub ordinal: i32,
    pub depth: i32,
    pub number: Option<&'a str>,
    pub heading: &'a str,
    pub body: ByteRange,
    pub raw_heading: ByteRange,
    pub parse_state: Coverage,
}

pub struct Chunk<'a> {
    pub range: ByteRange,
    pub text: &'a str,
}

pub struct Notice<'a> {
    pub ordinal: i32,
    pub category: &'a str,
    pub range: ByteRange,
    pub extraction: Coverage,
    pub review: &'a str,
}

pub struct Membership<'a> {
    pub rfc: RfcNumber,
    pub version: BlobDigest,
    pub catalog: BlobDigest,
    pub txt: BlobDigest,
    pub title: &'a str,
    pub stream: Option<&'a str>,
    pub publication_date: Option<&'a str>,
    pub status: Option<&'a str>,
}

pub struct Evidence<'a> {
    pub observation: Option<BlobDigest>,
    pub version: Option<BlobDigest>,
    pub range: Option<ByteRange>,
    pub method: &'a str,
    pub xml_locator: Option<&'a str>,
}

pub struct Candidate<'a> {
    id: GenerationId,
    tx: Transaction<'a>,
}

struct IndexedText<'a> {
    section_state: Coverage,
    line_starts: &'a [i64],
    text: Option<&'a str>,
}

impl Candidate<'_> {
    fn generation(&self) -> i64 {
        self.id.get() as i64
    }

    async fn rfc_identity(&self, id: i32) -> Result<()> {
        self.tx
            .execute(
                "INSERT INTO rfc.rfc_identities VALUES ($1::integer) ON CONFLICT DO NOTHING",
                &[&id],
            )
            .await?;
        Ok(())
    }

    pub async fn observe(&self, observation: &Observation<'_>) -> Result<BlobDigest> {
        self.insert_observation(observation, None).await
    }

    /// Records the HTTPS side of a byte comparison and binds the rsync proof.
    pub async fn observe_comparison(
        &self,
        https: &Observation<'_>,
        rsync: BlobDigest,
    ) -> Result<BlobDigest> {
        self.insert_observation(https, Some(rsync)).await
    }

    async fn insert_observation(
        &self,
        observation: &Observation<'_>,
        comparison_rsync: Option<BlobDigest>,
    ) -> Result<BlobDigest> {
        let inputs = (
            observation.kind,
            observation.url,
            observation.observed_us,
            observation.fetched_us,
            observation.etag,
            observation.last_modified,
            observation.raw_digest.to_string(),
            observation.https_verification,
        );
        let id = match comparison_rsync {
            Some(rsync) => identity("rfc-source-observation-v2", &(inputs, rsync.to_string()))?,
            None => identity("rfc-source-observation-v1", &inputs)?,
        };
        self.tx.execute(
            "INSERT INTO rfc.source_observations (identity, kind, source_url, observed_at, fetched_at, etag, last_modified, raw_digest, https_verification, comparison_rsync)
             VALUES ($1::bytea, $2, $3,
               timestamptz 'epoch' + ($4::bigint / 1000000) * interval '1 second' + ($4::bigint % 1000000) * interval '1 microsecond',
               timestamptz 'epoch' + ($5::bigint / 1000000) * interval '1 second' + ($5::bigint % 1000000) * interval '1 microsecond',
               $6, $7, $8::bytea, $9, $10::bytea) ON CONFLICT DO NOTHING",
            &[&id.as_bytes().as_slice(), &observation.kind, &observation.url, &observation.observed_us,
              &observation.fetched_us, &observation.etag, &observation.last_modified, &observation.raw_digest.as_bytes().as_slice(),
              &observation.https_verification, &comparison_rsync.as_ref().map(|peer| peer.as_bytes().as_slice())],
        ).await?;
        Ok(id)
    }

    pub async fn document(&self, document: Document<'_>) -> Result<BlobDigest> {
        self.document_with_index(document, None).await
    }

    async fn document_with_index(
        &self,
        mut document: Document<'_>,
        indexed: Option<IndexedText<'_>>,
    ) -> Result<BlobDigest> {
        let rfc = document.rfc.get() as i32;
        let length = document.txt.byte_length();
        let digest = document.txt.digest();
        document
            .sections
            .sort_unstable_by_key(|section| section.ordinal);
        document
            .chunks
            .sort_unstable_by_key(|chunk| chunk.range.start());
        document
            .notices
            .sort_unstable_by_key(|notice| notice.ordinal);
        let mut index_identity = index_identity(&document)?;
        if let Some(indexed) = &indexed {
            index_identity = identity(
                "rfc-document-index-v2",
                &(
                    index_identity.to_string(),
                    indexed.section_state.as_str(),
                    indexed.line_starts,
                ),
            )?;
        }
        let id = identity(
            "rfc-document-version-v1",
            &(
                rfc,
                digest.to_string(),
                document.parser_version,
                document.index_version,
                document.encoding,
                document.index_state.as_str(),
                document.notice_state.as_str(),
                document.notice_policy,
                index_identity.to_string(),
            ),
        )?;
        let key = id.as_bytes().as_slice();
        self.rfc_identity(rfc).await?;
        let inserted = self.tx.execute(
            "INSERT INTO rfc.document_versions (identity, rfc_id, txt_digest, byte_length, parser_version, index_version,
                encoding, indexing_state, notice_state, notice_policy, index_identity, section_state, line_starts)
             VALUES ($1::bytea, $2::integer, $3::bytea, $4::bigint, $5, $6, $7, $8::text, $9::text, $10, $11::bytea, $12::text, $13::bigint[])
             ON CONFLICT DO NOTHING",
            &[&key, &rfc, &digest.as_bytes().as_slice(), &length, &document.parser_version, &document.index_version,
              &document.encoding, &document.index_state.as_str(), &document.notice_state.as_str(),
              &document.notice_policy, &index_identity.as_bytes().as_slice(),
              &indexed.as_ref().map(|index| index.section_state.as_str()),
              &indexed.as_ref().map(|index| index.line_starts)],
        ).await?;
        if inserted == 0 {
            let sealed: bool = self
                .tx
                .query_one(
                    "SELECT sealed FROM rfc.document_versions WHERE identity = $1::bytea",
                    &[&key],
                )
                .await?
                .try_get(0)?;
            if !sealed {
                return Err("cannot reuse an unsealed document version".into());
            }
            return Ok(id);
        }
        for section in document.sections {
            let (start, end) = bounds(section.body);
            let (heading_start, heading_end) = bounds(section.raw_heading);
            self.tx.execute(
                "INSERT INTO rfc.sections (document_version, blob_length, section_id, parent_section_id, ordinal, depth,
                  display_number, heading_text, start_byte, end_byte, heading_start_byte, heading_end_byte, parse_state)
                 VALUES ($1::bytea, $2::bigint, $3, $4, $5, $6, $7, $8, $9::bigint, $10::bigint, $11::bigint, $12::bigint, $13::text)",
                &[&key, &length, &section.id.as_str(), &section.parent.as_ref().map(SectionId::as_str), &section.ordinal,
                  &section.depth, &section.number, &section.heading, &start, &end, &heading_start, &heading_end, &section.parse_state.as_str()],
            ).await?;
        }
        for chunk in document.chunks {
            let (start, end) = bounds(chunk.range);
            self.tx.execute(
                "INSERT INTO rfc.text_chunks (document_version, blob_length, start_byte, end_byte, search_text)
                 VALUES ($1::bytea, $2::bigint, $3::bigint, $4::bigint, $5)",
                &[&key, &length, &start, &end, &chunk.text],
            ).await?;
        }
        for notice in document.notices {
            let (start, end) = bounds(notice.range);
            self.tx.execute(
                "INSERT INTO rfc.notice_ranges (document_version, blob_length, ordinal, category, start_byte, end_byte, extraction_state, review_state)
                 VALUES ($1::bytea, $2::bigint, $3, $4, $5::bigint, $6::bigint, $7::text, $8)",
                &[&key, &length, &notice.ordinal, &notice.category, &start, &end, &notice.extraction.as_str(), &notice.review],
            ).await?;
        }
        if let Some(text) = indexed.and_then(|index| index.text) {
            self.tx
                .batch_execute("SET LOCAL statement_timeout = '30s'")
                .await?;
            self.tx.execute(
                "INSERT INTO rfc.document_lexemes (document_version, lexeme)
                 SELECT DISTINCT $1::bytea, term.lexeme COLLATE pg_catalog.\"C\"
                 FROM pg_catalog.ts_debug('pg_catalog.simple'::regconfig, $2::text) AS token
                 CROSS JOIN LATERAL pg_catalog.unnest(token.lexemes) AS term(lexeme)
                 WHERE octet_length(token.token) < 2048 AND octet_length(term.lexeme) BETWEEN 1 AND 2047",
                &[&key, &text],
            ).await?;
        }
        self.tx
            .execute(
                "UPDATE rfc.document_versions SET sealed = true WHERE identity = $1::bytea",
                &[&key],
            )
            .await?;
        Ok(id)
    }

    pub async fn member(&self, member: &Membership<'_>) -> Result<()> {
        self.member_with_sources(member, None, None, false).await
    }

    async fn member_with_sources(
        &self,
        member: &Membership<'_>,
        month: Option<&str>,
        rfcxml: Option<BlobDigest>,
        ingestion: bool,
    ) -> Result<()> {
        let mut metadata = identity(
            "rfc-catalog-metadata-v1",
            &(
                member.catalog.to_string(),
                member.txt.to_string(),
                member.title,
                member.stream,
                member.publication_date,
                member.status,
            ),
        )?;
        if ingestion {
            metadata = identity(
                "rfc-catalog-metadata-v2",
                &(
                    metadata.to_string(),
                    month,
                    rfcxml.map(|digest| digest.to_string()),
                ),
            )?;
        }
        let rfcxml = rfcxml.as_ref().map(|digest| digest.as_bytes().as_slice());
        self.tx.execute(
            "INSERT INTO rfc.generation_documents (generation_id, rfc_id, document_version, metadata_identity,
                catalog_observation, txt_observation, title, stream, publication_date, current_status, publication_month, rfcxml_observation)
             VALUES ($1, $2::integer, $3::bytea, $4::bytea, $5::bytea, $6::bytea, $7, $8, ($9::text)::date, $10,
                ($11::text)::date, $12::bytea)",
            &[&self.generation(), &(member.rfc.get() as i32), &member.version.as_bytes().as_slice(), &metadata.as_bytes().as_slice(),
              &member.catalog.as_bytes().as_slice(), &member.txt.as_bytes().as_slice(), &member.title, &member.stream,
              &member.publication_date, &member.status, &month, &rfcxml],
        ).await?;
        Ok(())
    }

    pub async fn evidence(&self, evidence: &Evidence<'_>) -> Result<BlobDigest> {
        let id = identity(
            "rfc-relation-evidence-v1",
            &(
                evidence.observation.map(|id| id.to_string()),
                evidence.version.map(|id| id.to_string()),
                evidence.range.map(bounds),
                evidence.method,
                evidence.xml_locator,
            ),
        )?;
        self.tx.execute(
            "INSERT INTO rfc.relation_evidence (identity, observation, document_version, blob_length, start_byte, end_byte, method, xml_locator)
             VALUES ($1::bytea, $2::bytea, $3::bytea, (SELECT byte_length FROM rfc.document_versions WHERE identity = $3::bytea),
                     $4::bigint, $5::bigint, $6, $7)
             ON CONFLICT DO NOTHING",
            &[&id.as_bytes().as_slice(), &evidence.observation.as_ref().map(|id| id.as_bytes().as_slice()),
              &evidence.version.as_ref().map(|id| id.as_bytes().as_slice()), &evidence.range.map(|r| bounds(r).0),
              &evidence.range.map(|r| bounds(r).1), &evidence.method, &evidence.xml_locator],
        ).await?;
        Ok(id)
    }

    pub async fn relation(&self, relation: Relation, evidence: &[BlobDigest]) -> Result<()> {
        if evidence.is_empty() {
            return Err("forward relation requires evidence".into());
        }
        let source = relation.source.get() as i32;
        let target = relation.target.get() as i32;
        let kind = relation.kind.as_str();
        self.rfc_identity(source).await?;
        self.rfc_identity(target).await?;
        self.tx
            .execute(
                "INSERT INTO rfc.rfc_relations (generation_id, source_rfc, kind, target_rfc)
             VALUES ($1, $2::integer, $3, $4::integer) ON CONFLICT DO NOTHING",
                &[&self.generation(), &source, &kind, &target],
            )
            .await?;
        for id in evidence {
            self.tx.execute(
                "INSERT INTO rfc.edge_evidence (generation_id, source_rfc, kind, target_rfc, evidence)
                 VALUES ($1, $2::integer, $3, $4::integer, $5::bytea) ON CONFLICT DO NOTHING",
                &[&self.generation(), &source, &kind, &target, &id.as_bytes().as_slice()],
            ).await?;
        }
        Ok(())
    }

    pub async fn coverage(
        &self,
        rfc: Option<RfcNumber>,
        family: &str,
        coverage: Coverage,
    ) -> Result<()> {
        let rfc = rfc.map(|id| id.get() as i32);
        if let Some(rfc) = rfc {
            self.rfc_identity(rfc).await?;
        }
        let scope = if rfc.is_some() { "document" } else { "catalog" };
        self.tx
            .execute(
                "INSERT INTO rfc.relation_coverage (generation_id, scope, rfc_id, family, state)
             VALUES ($1, $2, $3::integer, $4, $5::text)",
                &[
                    &self.generation(),
                    &scope,
                    &rfc,
                    &family,
                    &coverage.as_str(),
                ],
            )
            .await?;
        Ok(())
    }

    /// Freezes the candidate with the deterministic serialization of its inputs.
    pub async fn ready(self) -> Result<(GenerationId, BlobDigest)> {
        let digest = self.prepare_readiness().await?;
        self.commit_readiness(digest).await
    }

    async fn prepare_readiness(&self) -> Result<BlobDigest> {
        rfc_storage::fixture_boundary("before_readiness");
        let generation = self.generation();
        self.tx.query_one("SELECT id FROM rfc.generations WHERE id = $1 AND state = 'building' FOR NO KEY UPDATE",
            &[&generation]).await?;
        let manifest = load_manifest(&self.tx, self.id).await?;
        let format = manifest.version();
        let (bytes, digest) = manifest.encode();
        self.tx.execute(
            "UPDATE rfc.generations SET state = 'ready', manifest_format = $2, manifest_bytes = $3, manifest_digest = $4::bytea WHERE id = $1",
            &[&generation, &format, &bytes, &digest.as_bytes().as_slice()],
        ).await?;
        Ok(digest)
    }

    async fn commit_readiness(self, digest: BlobDigest) -> Result<(GenerationId, BlobDigest)> {
        self.tx.commit().await?;
        rfc_storage::fixture_boundary("after_readiness");
        Ok((self.id, digest))
    }

    async fn commit(self) -> Result<()> {
        Ok(self.tx.commit().await?)
    }
}

fn bounds(range: ByteRange) -> (i64, i64) {
    (range.start() as i64, range.end() as i64)
}

fn index_identity(document: &Document<'_>) -> Result<BlobDigest> {
    let sections: Vec<_> = document
        .sections
        .iter()
        .map(|section| {
            (
                section.id.as_str(),
                section.parent.as_ref().map(SectionId::as_str),
                section.ordinal,
                section.depth,
                section.number,
                section.heading,
                bounds(section.body),
                bounds(section.raw_heading),
                section.parse_state.as_str(),
            )
        })
        .collect();
    let chunks: Vec<_> = document
        .chunks
        .iter()
        .map(|chunk| (bounds(chunk.range), chunk.text))
        .collect();
    let notices: Vec<_> = document
        .notices
        .iter()
        .map(|notice| {
            (
                notice.ordinal,
                notice.category,
                bounds(notice.range),
                notice.extraction.as_str(),
                notice.review,
            )
        })
        .collect();
    Ok(identity(
        "rfc-document-index-v1",
        &(sections, chunks, notices),
    )?)
}
