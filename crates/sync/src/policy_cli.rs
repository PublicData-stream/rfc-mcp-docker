//! Private diagnostics, local reclassification, and emergency restrictions.

use crate::{
    Writer,
    candidate::canonical,
    policy,
    upstream::{CATALOG_LIMIT, TXT_LIMIT},
    xml,
};
use rfc_core::{BlobDigest, GenerationId, RfcNumber};
use rfc_storage::{
    blob::BlobReader,
    policy::{DENY_EXCERPT, DENY_PAGINATION, DENY_SEARCH, DENY_WHOLE, RIGHTS_LOCK, Rights},
    postgres::{Result, mirror_root, verify_stored_manifest},
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use tokio_postgres::{GenericClient, Row};

const USAGE: &str = "policy audit [--generation ID] [--format text|json] [--status unresolved]\n\
    policy explain --rfc ID [--generation ID]\n\
    policy refresh\n\
    policy restrict --rfc ID (--ceiling POLICY | --deny OPERATION) --reason CODE --review-ref REF --expect-epoch EPOCH";

const MEMBERS: &str =
    "SELECT m.rfc_id::integer, d.txt_digest::bytea, d.byte_length::bigint, m.metadata_identity::bytea,
         c.raw_digest::bytea, cb.byte_length::bigint, m.stream, m.title, m.current_status,
         m.publication_date::text, m.publication_month::text,
         (t.https_verification = 'verified' AND t.raw_digest = d.txt_digest
             AND t.source_url = 'https://www.rfc-editor.org/rfc/rfc' || m.rfc_id || '.txt'
             OR t.raw_digest = d.txt_digest AND t.verification_method = 'rsync_integrity'
               AND t.source_url = 'rsync://rsync.rfc-editor.org/rfcs-text-only/rfc' || m.rfc_id || '.txt'
         ) AS official,
         coalesce(a.fixture, false) OR coalesce(p.fixture, false) AS fixture,
         s.evidence::text, coalesce(r.denied,0), s.identity::bytea
     FROM rfc.generation_documents m JOIN rfc.document_versions d ON d.identity = m.document_version
     JOIN rfc.source_observations c ON c.identity = m.catalog_observation
     JOIN rfc.blobs cb ON cb.digest = c.raw_digest
     JOIN rfc.source_observations t ON t.identity = m.txt_observation
     LEFT JOIN rfc.generation_audit a ON a.generation_id = m.generation_id
     LEFT JOIN rfc.generation_policy p ON p.generation_id = m.generation_id
     LEFT JOIN rfc.source_rights s ON (s.generation_id,s.rfc_id)=(m.generation_id,m.rfc_id)
     LEFT JOIN rfc.source_restrictions r ON r.rfc_id=m.rfc_id
     WHERE m.generation_id=$1 AND ($2::integer IS NULL OR m.rfc_id=$2)
     ORDER BY m.rfc_id";

pub async fn run(args: &[String]) -> Result<()> {
    let command = args.first().map(String::as_str).ok_or(USAGE)?;
    if command == "--help" && args.len() == 1 {
        println!("{USAGE}");
        return Ok(());
    }
    let mut flags = BTreeMap::new();
    for pair in args[1..].chunks(2) {
        if pair.len() != 2
            || !pair[0].starts_with("--")
            || flags.insert(pair[0].as_str(), pair[1].as_str()).is_some()
        {
            return Err(USAGE.into());
        }
    }
    let take = |flags: &mut BTreeMap<_, _>, key| flags.remove(key).ok_or(USAGE);
    let generation = flags.remove("--generation").map(str::parse).transpose()?;
    let id = flags.remove("--rfc").map(str::parse).transpose()?;
    match command {
        "refresh" if flags.is_empty() && generation.is_none() && id.is_none() => {
            refresh(false).await?
        }
        "refresh-startup" if flags.is_empty() && generation.is_none() && id.is_none() => {
            refresh(true).await?;
        }
        "explain" if flags.is_empty() && id.is_some() => {
            println!(
                "{}",
                serde_json::to_string_pretty(&diagnose(generation, id, false).await?)?
            );
        }
        "audit" if id.is_none() => {
            let unresolved = match flags.remove("--status") {
                None => false,
                Some("unresolved") => true,
                _ => return Err(USAGE.into()),
            };
            let format = flags.remove("--format").unwrap_or("text");
            if !flags.is_empty() || !matches!(format, "text" | "json") {
                return Err(USAGE.into());
            }
            let report = diagnose(generation, None, unresolved).await?;
            if format == "json" {
                println!("{}", serde_json::to_string(&report)?);
            } else {
                println!(
                    "Generation {} at rights epoch {}: {} documents, {} reported",
                    report["generation"],
                    report["rights_epoch"],
                    report["documents"],
                    report["rows"].as_array().map_or(0, Vec::len)
                );
            }
        }
        "restrict" if generation.is_none() => {
            let id = id.and_then(RfcNumber::new).ok_or(USAGE)?;
            let denial = match (flags.remove("--ceiling"), flags.remove("--deny")) {
                (Some("metadata_only"), None) => 7,
                (Some("whole_only"), None) => DENY_PAGINATION | DENY_EXCERPT,
                (Some("whole_paginated"), None) | (None, Some("excerpt")) => DENY_EXCERPT,
                (None, Some("full_reproduction")) => DENY_WHOLE,
                (None, Some("pagination")) => DENY_PAGINATION,
                (None, Some("internal_search")) => DENY_SEARCH,
                _ => return Err(USAGE.into()),
            };
            let reason = take(&mut flags, "--reason")?;
            let reference = take(&mut flags, "--review-ref")?;
            let expected = take(&mut flags, "--expect-epoch")?.parse()?;
            if !flags.is_empty() || reference.trim().is_empty() {
                return Err(USAGE.into());
            }
            restrict(id, denial, reason, reference, expected).await?;
        }
        _ => return Err(USAGE.into()),
    }
    Ok(())
}

fn digest(row: &Row, index: usize) -> Result<BlobDigest> {
    Ok(BlobDigest::from_bytes(
        row.try_get::<_, &[u8]>(index)?.try_into()?,
    ))
}

async fn generation(db: &impl GenericClient, requested: Option<i64>) -> Result<i64> {
    Ok(db.query_one(
        "SELECT id FROM rfc.generations WHERE id = coalesce($1,(SELECT generation_id FROM rfc.active_generation))
         AND state IN ('published','retained')", &[&requested]
    ).await?.try_get(0)?)
}

async fn diagnose(requested: Option<i64>, id: Option<i32>, unresolved: bool) -> Result<Value> {
    let mut writer = Writer::connect().await?;
    let tx = writer
        .client
        .build_transaction()
        .read_only(true)
        .isolation_level(tokio_postgres::IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let generation = generation(&tx, requested).await?;
    let epoch: i64 = tx
        .query_one(
            "SELECT rights_epoch FROM rfc.policy_state WHERE singleton",
            &[],
        )
        .await?
        .try_get(0)?;
    let rows = tx.query(MEMBERS, &[&generation, &id]).await?;
    if id.is_some() && rows.is_empty() {
        return Err("RFC is absent from the requested generation".into());
    }
    let total = rows.len();
    let mut details = Vec::new();
    for row in rows {
        let value: Option<Value> = row
            .try_get::<_, Option<&str>>(13)?
            .map(serde_json::from_str)
            .transpose()?;
        let rights = value
            .as_ref()
            .and_then(|value| serde_json::from_value::<Rights>(value.clone()).ok());
        let denied: i32 = row.try_get(14)?;
        if !unresolved
            || denied != 0
            || rights.as_ref().is_none_or(|rights| {
                rights
                    .diagnostics
                    .iter()
                    .any(|code| code != "pre_5378_legend")
            })
        {
            details
                .push(json!({"rfc": row.try_get::<_, i32>(0)?, "rights": value, "denied": denied}));
        }
    }
    tx.commit().await?;
    Ok(
        json!({"schema_version":2, "generation":generation, "rights_epoch":epoch, "documents":total, "rows":details}),
    )
}

async fn fence(db: &impl GenericClient) -> Result<()> {
    if !db
        .query_one(
            "SELECT pg_catalog.pg_try_advisory_xact_lock($1)",
            &[&RIGHTS_LOCK],
        )
        .await?
        .try_get::<_, bool>(0)?
    {
        return Err("another operation holds the rights fence".into());
    }
    Ok(())
}

async fn refresh(only_if_missing: bool) -> Result<()> {
    let mut writer = Writer::connect().await?;
    if only_if_missing
        && !writer
            .client
            .query_one(
                "SELECT EXISTS (
                   SELECT FROM rfc.generation_documents d
                   JOIN rfc.generations g ON g.id = d.generation_id
                   LEFT JOIN rfc.source_rights r ON (r.generation_id, r.rfc_id) = (d.generation_id, d.rfc_id)
                   WHERE g.state IN ('published', 'retained') AND r.generation_id IS NULL
                 )",
                &[],
            )
            .await?
            .try_get::<_, bool>(0)?
    {
        println!("Source rights already cover the readable generations");
        return Ok(());
    }
    let _coordinator = writer.acquire_coordinator().await?;
    let reader = BlobReader::open(&mirror_root(), false)?;
    let tx = writer.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit = on")
        .await?;
    fence(&tx).await?;
    let generations = tx
        .query(
            "SELECT id FROM rfc.generations WHERE state IN ('published','retained') ORDER BY id",
            &[],
        )
        .await?;
    let mut count = 0;
    for generation in generations {
        let generation: i64 = generation.try_get(0)?;
        let id = GenerationId::new(generation).ok_or("invalid generation ID")?;
        let _fence = reader.generation_fence(id)?;
        verify_stored_manifest(&tx, id).await?;
        let mut catalogs = HashMap::new();
        for row in tx.query(MEMBERS, &[&generation, &None::<i32>]).await? {
            let rfc = RfcNumber::new(row.try_get(0)?).ok_or("invalid RFC number")?;
            let source = digest(&row, 1)?;
            let length: i64 = row.try_get(2)?;
            let metadata = digest(&row, 3)?;
            let cached: Option<&str> = row.try_get(13)?;
            if cached
                .and_then(|value| serde_json::from_str::<Rights>(value).ok())
                .is_some_and(|rights| {
                    rights.valid_for(rfc.get(), source, metadata.as_bytes(), length as usize)
                        && rights
                            .digest()
                            .ok()
                            .as_ref()
                            .map(|digest| digest.as_bytes().as_slice())
                            == row.get::<_, Option<&[u8]>>(15)
                })
            {
                continue;
            }
            let catalog = digest(&row, 4)?;
            let catalog_length: i64 = row.try_get(5)?;
            let fixture: bool = row.try_get(12)?;
            let catalog = match catalogs.entry(catalog) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let bytes = canonical(&reader, catalog, catalog_length, CATALOG_LIMIT)?;
                    entry.insert(xml::catalog(&bytes)?)
                }
            };
            let mut membership = catalog.entries.get(&rfc).cloned().unwrap_or(xml::Entry {
                title: row.try_get(7)?,
                status: row.try_get(8)?,
                stream: row.try_get(6)?,
                date: row.try_get(9)?,
                month: row.try_get(10)?,
                has_txt: true,
                authors: Vec::new(),
            });
            if membership.stream.is_none() {
                membership.stream = row.try_get(6)?;
            }
            let bytes = canonical(&reader, source, length, TXT_LIMIT)?;
            let mut rights = policy::classify(
                rfc,
                source,
                metadata,
                Some(&membership),
                &bytes,
                row.try_get::<_, Option<bool>>(11)?.unwrap_or(false) && !fixture,
                None,
            );
            rights.search |= fixture;
            policy::store(&tx, generation, &rights).await?;
            count += 1;
        }
    }
    tx.commit().await?;
    println!("Refreshed source rights for {count} generation members");
    Ok(())
}

async fn restrict(
    rfc: RfcNumber,
    denial: i32,
    reason: &str,
    reference: &str,
    expected: i64,
) -> Result<()> {
    let mut writer = Writer::connect().await?;
    let tx = writer.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit = on")
        .await?;
    fence(&tx).await?;
    let epoch: i64 = tx
        .query_one(
            "SELECT rfc.deny_source_rights($1::integer,$2::integer,$3,$4,$5)",
            &[&(rfc.get() as i32), &denial, &reason, &reference, &expected],
        )
        .await?
        .try_get(0)?;
    tx.commit().await?;
    println!("RFC {} restricted at rights epoch {epoch}", rfc.get());
    Ok(())
}

#[cfg(feature = "phase3-fixtures")]
pub fn qualify_sample(catalog: &str, original: &str, number: &str) -> Result<()> {
    let id = RfcNumber::new(number.parse()?).ok_or("invalid RFC number")?;
    let catalog = xml::catalog(&std::fs::read(catalog)?)?;
    let bytes = std::fs::read(original)?;
    let rights = policy::classify(
        id,
        BlobDigest::hash(&bytes),
        BlobDigest::hash(b"official-sample"),
        catalog.entries.get(&id),
        &bytes,
        true,
        None,
    );
    println!("{}", serde_json::to_string_pretty(&rights)?);
    Ok(())
}
