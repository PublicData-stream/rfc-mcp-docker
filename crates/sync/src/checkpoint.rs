use crate::{
    Writer,
    acquisition::Original,
    discovery::{INDEX, PARSER},
    upstream::MAX_CATALOG_ENTRIES,
};
use rfc_core::{BlobDigest, RfcNumber};
use rfc_storage::postgres::Result;
use std::collections::BTreeMap;
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
}

#[derive(Clone, Copy)]
pub(crate) struct Context {
    pub scope: &'static str,
    pub key: BlobDigest,
}

impl Context {
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
