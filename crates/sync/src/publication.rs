use crate::Writer;
use rfc_core::{BlobDigest, GenerationId};
use rfc_storage::{
    blob::BlobWriter,
    postgres::{Result, mirror_root, verify_stored_manifest},
};

/// Promote a verified full candidate or roll back to a retained generation.
pub async fn promote(generation: GenerationId, expected: i64, rollback: bool) -> Result<i64> {
    if expected < 0 {
        return Err("publication sequence must be nonnegative".into());
    }
    let mut writer = Writer::connect().await?;
    let _coordinator = writer.acquire_coordinator().await?;
    let mirror = BlobWriter::open(&mirror_root())?;
    let id = i64::try_from(generation.get())?;
    let target_state = if rollback { "retained" } else { "ready" };

    let tx = writer
        .client
        .build_transaction()
        .read_only(true)
        .start()
        .await?;
    tx.batch_execute("SET LOCAL statement_timeout = '120s'")
        .await?;
    let sequence: i64 = tx
        .query_one(
            "SELECT publication_sequence FROM rfc.active_generation WHERE singleton",
            &[],
        )
        .await?
        .try_get(0)?;
    if sequence != expected {
        return Err(format!("publication sequence conflict (current {sequence})").into());
    }
    tx.query_opt(
        "SELECT g.id FROM rfc.generations g JOIN rfc.generation_ingestion i ON i.generation_id = g.id
         WHERE g.id = $1 AND g.state = $2 AND i.scope = 'full'
           AND EXISTS (SELECT FROM rfc.generation_documents WHERE generation_id = g.id)", &[&id, &target_state],
    ).await?.ok_or("generation is not eligible for promotion")?;
    verify_stored_manifest(&tx, generation).await?;
    let blobs = tx
        .query(
            "SELECT b.digest::bytea, b.byte_length::bigint, b.integrity_state
         FROM rfc.generation_blobs($1) AS reachable(digest) JOIN rfc.blobs b USING (digest)
         ORDER BY b.digest",
            &[&id],
        )
        .await?;
    tx.commit().await?;
    if blobs.is_empty() {
        return Err("candidate has no source blobs".into());
    }
    for row in blobs {
        let digest = BlobDigest::from_bytes(row.try_get::<_, &[u8]>(0)?.try_into()?);
        if row.try_get::<_, &str>(2)? != "verified" {
            return Err(format!("blob {digest} is not verified").into());
        }
        mirror
            .verify_durable(digest, row.try_get(1)?)
            .map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!("blob {digest} failed verification: {error}"),
                )
            })?;
    }
    if rollback {
        mirror.verify_generation_fence(generation)?;
    } else {
        mirror.prepare_generation_fence(generation)?;
    }
    rfc_storage::fixture_boundary("before_promotion");
    let tx = writer.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit = on; SET LOCAL statement_timeout = '120s'")
        .await?;
    let sequence = tx
        .query_one(
            "SELECT rfc.promote_generation($1, $2, $3)",
            &[&id, &expected, &rollback],
        )
        .await?
        .try_get(0)?;
    rfc_storage::fixture_boundary("before_publication_commit");
    tx.commit().await?;
    rfc_storage::fixture_boundary("after_publication_commit");
    Ok(sequence)
}
