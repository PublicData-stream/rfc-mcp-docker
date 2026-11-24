use crate::{Writer, upstream};
use rfc_core::{BlobDigest, RfcNumber};
use rfc_storage::{blob::BlobReader, postgres::Result};
use std::{
    collections::BTreeMap,
    io::{self, Read},
};
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

#[derive(Clone, Copy)]
pub(crate) struct Saved {
    pub observation: BlobDigest,
    pub digest: BlobDigest,
    pub length: usize,
    pub rsync_only: bool,
    expires_us: i64,
}

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

#[derive(Debug)]
pub(crate) struct Invalid {
    pub kind: Kind,
    digest: BlobDigest,
    cause: Box<dyn std::error::Error + Send + Sync>,
}

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

impl std::error::Error for Invalid {}

#[derive(Default)]
pub(crate) struct SavedSources {
    pub txt: BTreeMap<RfcNumber, Saved>,
    pub xml: BTreeMap<RfcNumber, Saved>,
}

impl Context {
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
