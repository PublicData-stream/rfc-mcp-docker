#[cfg(feature = "phase2-fixtures")]
use crate::upstream;
use crate::{
    Writer,
    acquisition::Original,
    discovery::{INDEX, PARSER},
    upstream::MAX_CATALOG_ENTRIES,
};
use rfc_core::{BlobDigest, RfcNumber};
#[cfg(feature = "phase2-fixtures")]
use rfc_storage::blob::BlobReader;
use rfc_storage::postgres::Result;
use std::collections::BTreeMap;
#[cfg(feature = "phase2-fixtures")]
use std::io::{self, Read};
use tokio_postgres::GenericClient;

#[derive(Clone, Copy, Debug)]
pub(crate) enum Kind {
    Txt,
    Xml,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Txt => "txt",
            Self::Xml => "rfcxml",
        }
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) fn invalid(
        self,
        saved: Saved,
        cause: Box<dyn std::error::Error + Send + Sync>,
    ) -> Invalid {
        Invalid {
            kind: self,
            digest: saved.digest,
            cause,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Context {
    pub scope: &'static str,
    pub key: BlobDigest,
}

#[cfg(feature = "phase2-fixtures")]
#[derive(Clone, Copy)]
pub(crate) struct Saved {
    pub observation: BlobDigest,
    pub digest: BlobDigest,
    pub length: usize,
    pub rsync_only: bool,
    expires_us: i64,
}

#[cfg(feature = "phase2-fixtures")]
impl Saved {
    pub(crate) fn fresh(self) -> Result<bool> {
        Ok(upstream::now_us()? < self.expires_us)
    }

    pub(crate) fn read_xml(self, reader: &BlobReader) -> Result<Option<Vec<u8>>> {
        if !self.fresh()? {
            return Ok(None);
        }
        let mut file = match reader.open_blob(self.digest) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if self.length > upstream::XML_LIMIT || file.metadata()?.len() != self.length as u64 {
            return Err("canonical RFCXML length differs from checkpoint".into());
        }
        let mut bytes = vec![0; self.length];
        file.read_exact(&mut bytes)?;
        if file.read(&mut [0])? != 0 || BlobDigest::hash(&bytes) != self.digest {
            return Err("canonical RFCXML digest differs from checkpoint".into());
        }
        Ok(Some(bytes))
    }
}

#[cfg(feature = "phase2-fixtures")]
#[derive(Debug)]
pub(crate) struct Invalid {
    pub kind: Kind,
    digest: BlobDigest,
    cause: Box<dyn std::error::Error + Send + Sync>,
}

#[cfg(feature = "phase2-fixtures")]
impl std::fmt::Display for Invalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} checkpoint {} failed verification: {}",
            self.kind.as_str(),
            self.digest,
            self.cause
        )
    }
}

#[cfg(feature = "phase2-fixtures")]
impl std::error::Error for Invalid {}

#[cfg(feature = "phase2-fixtures")]
#[derive(Default)]
pub(crate) struct SavedSources {
    pub txt: BTreeMap<RfcNumber, Saved>,
    pub xml: BTreeMap<RfcNumber, Saved>,
}

impl Context {
    #[cfg(feature = "phase2-fixtures")]
    pub(crate) async fn load(self, writer: &Writer) -> Result<SavedSources> {
        let key = self.key.as_bytes().as_slice();
        writer
            .client
            .query_one(
                "SELECT rfc.prune_ingestion_checkpoints($1, $2::bytea)",
                &[&self.scope, &key],
            )
            .await?;
        let mut sources = SavedSources::default();
        let rows = writer.client.query(
            "SELECT c.rfc_id::integer, c.kind, c.observation::bytea, o.raw_digest::bytea, c.byte_length::bigint,
                    o.verification_method = 'rsync_integrity',
                    (extract(epoch FROM c.expires_at) * 1000000)::bigint
             FROM rfc.ingestion_checkpoints c JOIN rfc.source_observations o ON o.identity = c.observation
             JOIN rfc.blobs b ON b.digest = o.raw_digest AND b.integrity_state = 'verified'
             WHERE c.scope = $1 AND c.context = $2::bytea AND c.expires_at > statement_timestamp()",
            &[&self.scope, &key],
        ).await?;
        for row in rows {
            let rfc = RfcNumber::new(row.try_get(0)?).ok_or("invalid checkpoint RFC number")?;
            let target = match row.try_get::<_, &str>(1)? {
                "txt" => &mut sources.txt,
                "rfcxml" => &mut sources.xml,
                _ => return Err("invalid checkpoint source kind".into()),
            };
            target.insert(
                rfc,
                Saved {
                    observation: BlobDigest::from_bytes(row.try_get::<_, &[u8]>(2)?.try_into()?),
                    digest: BlobDigest::from_bytes(row.try_get::<_, &[u8]>(3)?.try_into()?),
                    length: usize::try_from(row.try_get::<_, i64>(4)?)?,
                    rsync_only: row.try_get(5)?,
                    expires_us: row.try_get(6)?,
                },
            );
        }
        Ok(sources)
    }

    #[cfg(feature = "phase2-fixtures")]
    pub(crate) async fn record(
        self,
        db: &impl GenericClient,
        id: RfcNumber,
        kind: Kind,
        proof: BlobDigest,
    ) -> Result<()> {
        db.query_one(
            "SELECT rfc.store_ingestion_checkpoint($1, $2::bytea, $3::integer, $4, $5::bytea)",
            &[
                &self.scope,
                &self.key.as_bytes().as_slice(),
                &(id.get() as i32),
                &kind.as_str(),
                &proof.as_bytes().as_slice(),
            ],
        )
        .await?;
        Ok(())
    }

    pub(crate) async fn load_https(
        self,
        writer: &Writer,
    ) -> Result<(BTreeMap<RfcNumber, Original>, BTreeMap<RfcNumber, Original>)> {
        writer
            .client
            .query_one("SELECT rfc.prune_ingestion_checkpoints(NULL, NULL)", &[])
            .await?;
        let rows = writer.client.query(
            "SELECT c.rfc_id::integer, c.kind, c.observation::bytea, o.raw_digest::bytea,
                    c.byte_length::bigint, (extract(epoch FROM c.expires_at) * 1000000)::bigint,
                    (extract(epoch FROM o.observed_at) * 1000000)::bigint,
                    (extract(epoch FROM o.fetched_at) * 1000000)::bigint,
                    o.etag, o.last_modified, c.document_version::bytea
             FROM rfc.ingestion_checkpoints c JOIN rfc.source_observations o ON o.identity = c.observation
             JOIN rfc.blobs b ON b.digest = o.raw_digest AND b.integrity_state = 'verified'
             LEFT JOIN rfc.document_versions d ON d.identity = c.document_version
             WHERE c.scope = $1 AND c.context = $2::bytea AND c.evidence_state = 'https'
               AND c.expires_at > statement_timestamp() AND o.fetched_at <= statement_timestamp()
               AND c.byte_length = b.byte_length AND o.kind = c.kind AND o.https_verification = 'verified'
               AND CASE WHEN c.kind = 'txt' THEN
                   d.sealed AND d.rfc_id = c.rfc_id AND d.txt_digest = o.raw_digest
                   AND d.parser_version = $3 AND d.index_version = $4
                   AND o.source_url = 'https://www.rfc-editor.org/rfc/rfc' || c.rfc_id || '.txt'
               ELSE o.source_url = 'https://www.rfc-editor.org/rfc/rfc' || c.rfc_id || '.xml' END
             LIMIT $5",
            &[&self.scope, &self.key.as_bytes().as_slice(), &PARSER, &INDEX,
              &(2 * MAX_CATALOG_ENTRIES as i64 + 1)],
        ).await?;
        if rows.len() > 2 * MAX_CATALOG_ENTRIES {
            return Err("HTTPS checkpoints exceed selection bounds".into());
        }
        let mut txt = BTreeMap::new();
        let mut xml = BTreeMap::new();
        for row in rows {
            let id = RfcNumber::new(row.try_get(0)?).ok_or("invalid checkpoint RFC number")?;
            let sources = match row.try_get::<_, &str>(1)? {
                "txt" => &mut txt,
                "rfcxml" => &mut xml,
                _ => return Err("invalid checkpoint source kind".into()),
            };
            let version = row
                .try_get::<_, Option<&[u8]>>(10)?
                .map(|bytes| bytes.try_into().map(BlobDigest::from_bytes))
                .transpose()?;
            sources.insert(
                id,
                Original {
                    observation: Some(
                        BlobDigest::from_bytes(row.try_get::<_, &[u8]>(2)?.try_into()?).to_string(),
                    ),
                    digest: BlobDigest::from_bytes(row.try_get::<_, &[u8]>(3)?.try_into()?),
                    length: u64::try_from(row.try_get::<_, i64>(4)?)?,
                    checkpoint_expiry: Some(row.try_get(5)?),
                    observed_us: row.try_get(6)?,
                    fetched_us: row.try_get(7)?,
                    etag: row.try_get(8)?,
                    last_modified: row.try_get(9)?,
                    checkpoint_version: version,
                },
            );
        }
        Ok((txt, xml))
    }

    pub(crate) async fn record_https(
        self,
        db: &impl GenericClient,
        id: RfcNumber,
        kind: Kind,
        observation: BlobDigest,
        version: Option<BlobDigest>,
        malformed: bool,
    ) -> Result<()> {
        db.query_one(
            "SELECT rfc.store_source_checkpoint($1, $2::bytea, $3::integer, $4, $5::bytea, $6, $7::bytea)",
            &[
                &self.scope, &self.key.as_bytes().as_slice(), &(id.get() as i32),
                &kind.as_str(), &observation.as_bytes().as_slice(),
                &if malformed { "malformed" } else { "https" },
                &version.as_ref().map(|value| value.as_bytes().as_slice()),
            ],
        ).await?;
        Ok(())
    }

    pub(crate) async fn remove(
        self,
        db: &impl GenericClient,
        source: Option<(RfcNumber, Kind)>,
    ) -> Result<()> {
        db.query_one(
            "SELECT rfc.remove_ingestion_checkpoints($1, $2::bytea, $3::integer, $4)",
            &[
                &self.scope,
                &self.key.as_bytes().as_slice(),
                &source.map(|(id, _)| id.get() as i32),
                &source.map(|(_, kind)| kind.as_str()),
            ],
        )
        .await?;
        Ok(())
    }
}
