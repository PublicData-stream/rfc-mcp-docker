use crate::Writer;
use rfc_core::{BlobDigest, GenerationId};
use rfc_storage::{
    blob::BlobWriter,
    postgres::{Result, mirror_root},
};
use serde::Serialize;
use std::env;

const DELETE_BATCH: i64 = 256;
const RETIRE_BATCH: i64 = 128;
const DURABLE_TRANSACTION: &str =
    "SET LOCAL synchronous_commit = on; SET LOCAL statement_timeout = '120s'";

#[derive(Clone, Copy)]
pub enum Maintenance {
    Recover,
    Collect,
    Discard(GenerationId),
}

#[derive(Default, Serialize)]
struct Report {
    abandoned_candidates: u64,
    retired_generations: u64,
    cleaned_stages: usize,
    completed_deletions: usize,
    blocked_deletions: usize,
    queued_deletions: i64,
}

struct Retention {
    count: i32,
    hours: i32,
}

impl Retention {
    fn from_environment() -> Result<Self> {
        Ok(Self {
            count: retention_setting("RFC_RETAIN_GENERATIONS", 3)?,
            hours: retention_setting("RFC_RETAIN_HOURS", 24)?,
        })
    }
}

fn retention_setting(name: &str, minimum: i32) -> Result<i32> {
    match env::var(name) {
        Ok(value) => {
            let value: i32 = value.parse()?;
            if value < minimum {
                return Err(format!("{name} must be at least {minimum}").into());
            }
            Ok(value)
        }
        Err(env::VarError::NotPresent) => Ok(minimum),
        Err(error) => Err(error.into()),
    }
}

pub async fn maintain(operation: Maintenance) -> Result<()> {
    let mut writer = Writer::connect().await?;
    let _coordinator = writer.acquire_coordinator().await?;
    let mirror = BlobWriter::open(&mirror_root())?;
    let report = recover(&mut writer, &mirror, operation).await?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

pub(crate) async fn recover_before_ingestion(
    writer: &mut Writer,
    mirror: &BlobWriter,
) -> Result<()> {
    recover(writer, mirror, Maintenance::Recover).await?;
    Ok(())
}

async fn recover(
    writer: &mut Writer,
    mirror: &BlobWriter,
    operation: Maintenance,
) -> Result<Report> {
    let retention = Retention::from_environment()?;
    let mut report = Report::default();
    let tx = writer.client.transaction().await?;
    tx.batch_execute(DURABLE_TRANSACTION).await?;
    report.abandoned_candidates = tx
        .execute(
            "UPDATE rfc.generations SET state = 'failed' WHERE state = 'building'",
            &[],
        )
        .await?;
    tx.commit().await?;
    report.cleaned_stages = mirror.recover_staging()?;

    if let Maintenance::Discard(target) = operation {
        if !retire(writer, mirror, target, false, &retention, true).await? {
            return Err("generation is not ready for discard or is busy".into());
        }
        report.retired_generations += 1;
    }
    let collect = matches!(operation, Maintenance::Collect);
    let mut after = -1i64;
    loop {
        let rows = writer.client.query(
            "WITH ranked AS (
                SELECT id, retired_at, row_number() OVER (ORDER BY last_publication_sequence DESC) AS rank
                FROM rfc.generations WHERE state = 'retained'
             ), eligible AS (
                SELECT id, false AS retained FROM rfc.generations WHERE state = 'failed'
                UNION ALL SELECT id, true FROM ranked WHERE $1 AND rank > $2::integer
                    AND retired_at <= clock_timestamp() - make_interval(hours => $3::integer)
             ) SELECT id, retained FROM eligible WHERE id > $4 ORDER BY id LIMIT $5",
            &[&collect, &retention.count, &retention.hours, &after, &RETIRE_BATCH],
        ).await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            after = row.try_get(0)?;
            let generation = GenerationId::new(after).ok_or("invalid generation ID")?;
            if retire(
                writer,
                mirror,
                generation,
                row.try_get(1)?,
                &retention,
                false,
            )
            .await?
            {
                report.retired_generations += 1;
            }
        }
    }

    let tx = writer.client.transaction().await?;
    tx.batch_execute(DURABLE_TRANSACTION).await?;
    tx.query_one("SELECT rfc.prune_ingestion_checkpoints(NULL, NULL)", &[])
        .await?;
    tx.query_one("SELECT rfc.mark_unreachable_blobs()", &[])
        .await?;
    tx.commit().await?;

    for shard in 0..=u8::MAX {
        let digests = mirror.inventory_shard(shard)?;
        for chunk in digests.chunks(DELETE_BATCH as usize) {
            let candidates: Vec<_> = chunk
                .iter()
                .map(|digest| digest.as_bytes().as_slice())
                .collect();
            let tx = writer.client.transaction().await?;
            tx.batch_execute(DURABLE_TRANSACTION).await?;
            tx.query_one("SELECT rfc.queue_orphan_blobs($1)", &[&candidates])
                .await?;
            tx.commit().await?;
        }
    }

    let mut after: Option<Vec<u8>> = None;
    loop {
        let rows = writer
            .client
            .query(
                "SELECT digest::bytea, byte_length::bigint FROM rfc.blob_deletion_queue
             WHERE ($1::bytea IS NULL OR digest > $1::bytea) ORDER BY digest LIMIT $2",
                &[&after, &DELETE_BATCH],
            )
            .await?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let raw: &[u8] = row.try_get(0)?;
            after = Some(raw.to_vec());
            let digest = BlobDigest::from_bytes(raw.try_into()?);
            let unreachable: bool = writer.client.query_one(
                "SELECT NOT EXISTS (SELECT FROM rfc.blobs WHERE digest = $1::bytea)
                    AND NOT EXISTS (SELECT FROM rfc.control_events WHERE observation_digest = $1::bytea)",
                &[&raw],
            ).await?.try_get(0)?;
            if !unreachable {
                report.blocked_deletions += 1;
                continue;
            }
            if mirror.remove_queued_blob(digest, row.try_get(1)?).is_err() {
                crate::diagnostics::event(
                    crate::diagnostics::Level::Warn,
                    "blob_deletion_deferred",
                    || serde_json::json!({"blob_digest": digest.to_string()}),
                );
                report.blocked_deletions += 1;
                continue;
            }
            let tx = writer.client.transaction().await?;
            tx.batch_execute(DURABLE_TRANSACTION).await?;
            tx.query_one("SELECT rfc.finish_blob_deletion($1)", &[&raw])
                .await?;
            tx.commit().await?;
            report.completed_deletions += 1;
        }
    }
    report.queued_deletions = writer
        .client
        .query_one("SELECT count(*) FROM rfc.blob_deletion_queue", &[])
        .await?
        .try_get(0)?;
    Ok(report)
}

async fn retire(
    writer: &mut Writer,
    mirror: &BlobWriter,
    generation: GenerationId,
    retained: bool,
    retention: &Retention,
    discard: bool,
) -> Result<bool> {
    let id = i64::try_from(generation.get())?;
    let tx = writer.client.transaction().await?;
    tx.batch_execute(DURABLE_TRANSACTION).await?;
    let locked: bool = tx
        .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&id])
        .await?
        .try_get(0)?;
    if !locked {
        return Ok(false);
    }
    let _fence = if retained {
        let Some(fence) = mirror.try_retirement_fence(generation)? else {
            return Ok(false);
        };
        Some(fence)
    } else {
        None
    };
    rfc_storage::fixture_boundary("retirement_locked");
    let result = tx
        .query_one(
            "SELECT rfc.retire_generation($1, $2, $3, $4)",
            &[&id, &retention.count, &retention.hours, &discard],
        )
        .await?
        .try_get(0)?;
    rfc_storage::fixture_boundary("before_retirement_commit");
    tx.commit().await?;
    rfc_storage::fixture_boundary("after_retirement_commit");
    Ok(result)
}
