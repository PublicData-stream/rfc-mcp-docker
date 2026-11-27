//! Synthetic inputs for the low-level storage integration driver.

use crate::{
    Candidate, Document, IndexedText, Observation, Writer, discovery::POLICY, persist_document,
};
use rfc_core::{BlobDigest, Coverage};
use rfc_storage::{manifest::identity, postgres::Result};
use std::os::fd::OwnedFd;

impl Writer {
    pub async fn fixture_coordinator(&self) -> Result<OwnedFd> {
        self.acquire_coordinator().await
    }
}

impl Candidate<'_> {
    pub async fn fixture_catalog(
        &self,
        observation: BlobDigest,
        ids: &[i32],
        started: i64,
    ) -> Result<()> {
        let catalog = observation.as_bytes().as_slice();
        let selection = identity(
            "rfc-storage-fixture-selection-v1",
            &(observation.to_string(), ids),
        )?;
        self.tx
            .execute(
                "INSERT INTO rfc.generation_ingestion
             (generation_id, scope, catalog_before, catalog_after, catalog_only)
             VALUES ($1, 'sample', $2::bytea, $2::bytea, '[]')",
                &[&self.generation(), &catalog],
            )
            .await?;
        self.tx
            .execute(
                "INSERT INTO rfc.generation_audit VALUES
             ($1, $2::bytea, $2::bytea, $3, $4, 0, $5, $5 + 3600000000, true)",
                &[
                    &self.generation(),
                    &selection.as_bytes().as_slice(),
                    &POLICY,
                    &ids,
                    &started,
                ],
            )
            .await?;
        Ok(())
    }

    pub async fn fixture_capture(&self, observation: &Observation<'_>) -> Result<BlobDigest> {
        crate::insert_observation(&self.tx, observation, None, true).await
    }

    pub async fn fixture_document(
        &self,
        document: Document<'_>,
        source: &str,
        observation: BlobDigest,
    ) -> Result<BlobDigest> {
        let id = document.rfc.get() as i32;
        let line_starts: Vec<_> = [0_i64]
            .into_iter()
            .chain(
                source
                    .bytes()
                    .enumerate()
                    .filter(|&(index, byte)| byte == b'\n' && index + 1 < source.len())
                    .map(|(index, _)| (index + 1) as i64),
            )
            .collect();
        let version = persist_document(
            &self.tx,
            document,
            Some(IndexedText {
                section_state: Coverage::Partial,
                line_starts: &line_starts,
                text: Some(source),
            }),
        )
        .await?;
        self.tx.execute(
            "INSERT INTO rfc.generation_source_evidence (generation_id, selection, rfc_id, txt_observation,
                document_version, txt_digest, byte_length, method, receipt, rsync_url, started_us,
                observed_us, checksum_choice, rfcxml_outcome)
             SELECT a.generation_id, a.selection, $2::integer, $3::bytea, d.identity, d.txt_digest,
                d.byte_length, 'rsync_checksum_match', a.selection,
                'rsync://rsync.rfc-editor.org/rfcs-text-only/rfc' || $2::integer || '.txt',
                a.started_us, a.started_us, 'auto', 'not_found'
             FROM rfc.generation_audit a CROSS JOIN rfc.document_versions d
             WHERE a.generation_id=$1 AND d.identity=$4::bytea",
            &[&self.generation(), &id, &observation.as_bytes().as_slice(), &version.as_bytes().as_slice()],
        ).await?;
        Ok(version)
    }
}
