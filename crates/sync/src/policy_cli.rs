//! Private audit, approval and rights restriction commands.

use crate::{
    Writer,
    candidate::canonical,
    policy::Registry,
    upstream::{CATALOG_LIMIT, MAX_CATALOG_ENTRIES, TXT_LIMIT},
    xml,
};
use rfc_core::{BlobDigest, GenerationId, RfcNumber};
use rfc_storage::{
    blob::BlobReader,
    manifest::identity,
    policy::{CLASSIFIER, Policy, RIGHTS_LOCK, Receipt},
    postgres::{Result, mirror_root, verify_stored_manifest},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, io::Read};
use tokio_postgres::{GenericClient, IsolationLevel};

const REPORT_LIMIT: u64 = 64 * 1024 * 1024;
const USAGE: &str = "policy audit [--generation ID] [--format text|json] [--status unresolved]\n\
    policy explain --rfc ID [--generation ID]\n\
    policy approve --registry DIGEST --audit-report PATH --review-ref REF\n\
    policy restrict --rfc ID --ceiling POLICY --reason CODE --review-ref REF --expect-epoch N";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Audit {
    schema_version: u8,
    generation: i64,
    manifest: String,
    registry_digest: String,
    classifier: String,
    complete: bool,
    full: bool,
    fixture: bool,
    rights_epoch: i64,
    unresolved_only: bool,
    total_documents: usize,
    eligible_family_documents: usize,
    eligible_family_excerpt_approvals: usize,
    obligations_changed: usize,
    decisions: BTreeMap<String, usize>,
    methods: BTreeMap<String, usize>,
    reasons: BTreeMap<String, usize>,
    transitions: BTreeMap<String, usize>,
    streams: BTreeMap<String, usize>,
    by_period: BTreeMap<String, usize>,
    by_rule: BTreeMap<String, usize>,
    by_family: BTreeMap<String, usize>,
    by_template: BTreeMap<String, usize>,
    review_queue: BTreeMap<String, Vec<i32>>,
    rows: Vec<AuditRow>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AuditRow {
    rfc: i32,
    current: String,
    proposed: Option<Policy>,
    receipt: Option<String>,
    method: Option<String>,
    reason: Option<String>,
    rule: Option<String>,
    template: Option<String>,
}

pub async fn run(args: &[String]) -> Result<()> {
    let Some((command, options)) = args.split_first() else {
        return Err(USAGE.into());
    };
    if command == "--help" && options.is_empty() {
        println!("{USAGE}");
        return Ok(());
    }
    let mut flags = flags(options)?;
    match command.as_str() {
        "audit" => {
            let generation = take_generation(&mut flags)?;
            let json = match flags.remove("--format").unwrap_or("text") {
                "json" => true,
                "text" => false,
                _ => return Err("expected --format text or json".into()),
            };
            let unresolved = match flags.remove("--status") {
                Some("unresolved") => true,
                None => false,
                _ => return Err("expected --status unresolved".into()),
            };
            no_flags(&flags)?;
            let report = audit(generation, unresolved).await?;
            let excerpt = report
                .decisions
                .get("excerpt_allowed")
                .copied()
                .unwrap_or(0);
            if json {
                println!("{}", serde_json::to_string(&report)?);
            } else {
                println!(
                    "Generation {} manifest {}",
                    report.generation, report.manifest
                );
                println!(
                    "Registry {} classifier {}",
                    report.registry_digest, report.classifier
                );
                println!(
                    "Complete={} full={} fixture={}",
                    report.complete, report.full, report.fixture
                );
                println!(
                    "Excerpt approvals {excerpt}/{} overall, {}/{} eligible family documents",
                    report.total_documents,
                    report.eligible_family_excerpt_approvals,
                    report.eligible_family_documents
                );
                for (label, counts) in [
                    ("decision", &report.decisions),
                    ("transition", &report.transitions),
                    ("unresolved", &report.reasons),
                ] {
                    for (name, count) in counts {
                        println!("{label} {name} {count}");
                    }
                }
                if unresolved {
                    for (reason, ids) in report.review_queue {
                        println!("Review {reason} {ids:?}");
                    }
                }
            }
        }
        "explain" => {
            let rfc = take_rfc(&mut flags)?;
            let generation = take_generation(&mut flags)?;
            no_flags(&flags)?;
            explain(rfc, generation).await?;
        }
        "approve" => {
            let registry = take(&mut flags, "--registry")?;
            let path = take(&mut flags, "--audit-report")?;
            let reference = reference(&mut flags)?;
            no_flags(&flags)?;
            approve(registry, path, reference).await?;
        }
        "restrict" => {
            let rfc = take_rfc(&mut flags)?;
            let ceiling = Policy::parse(take(&mut flags, "--ceiling")?)
                .filter(|policy| *policy != Policy::ExcerptAllowed)
                .ok_or("invalid restricting policy ceiling")?;
            let reason = take(&mut flags, "--reason")?;
            if !matches!(
                reason,
                "rights_revoked" | "legal_obligation_changed" | "unresolved_rights"
            ) {
                return Err("invalid restriction reason".into());
            }
            let reference = reference(&mut flags)?;
            let expected = take(&mut flags, "--expect-epoch")?.parse::<i64>()?;
            no_flags(&flags)?;
            restrict(rfc, ceiling, reason, reference, expected).await?;
        }
        _ => return Err(USAGE.into()),
    }
    Ok(())
}

fn flags(args: &[String]) -> Result<BTreeMap<&str, &str>> {
    let mut parsed = BTreeMap::new();
    let (pairs, remainder) = args.as_chunks::<2>();
    for pair in pairs {
        if !pair[0].starts_with("--")
            || pair[1].starts_with("--")
            || parsed.insert(pair[0].as_str(), pair[1].as_str()).is_some()
        {
            return Err(USAGE.into());
        }
    }
    if !remainder.is_empty() {
        return Err(USAGE.into());
    }
    Ok(parsed)
}

fn no_flags(flags: &BTreeMap<&str, &str>) -> Result<()> {
    if flags.is_empty() {
        Ok(())
    } else {
        Err(USAGE.into())
    }
}

fn take<'a>(flags: &mut BTreeMap<&str, &'a str>, key: &str) -> Result<&'a str> {
    flags
        .remove(key)
        .ok_or_else(|| format!("missing {key}").into())
}

fn take_generation(flags: &mut BTreeMap<&str, &str>) -> Result<Option<i64>> {
    flags
        .remove("--generation")
        .map(|id| {
            let id = id.parse::<i64>()?;
            GenerationId::new(id).ok_or("invalid generation ID")?;
            Ok(id)
        })
        .transpose()
}

fn take_rfc(flags: &mut BTreeMap<&str, &str>) -> Result<RfcNumber> {
    RfcNumber::new(take(flags, "--rfc")?.parse()?).ok_or_else(|| "invalid RFC number".into())
}

fn reference<'a>(flags: &mut BTreeMap<&str, &'a str>) -> Result<&'a str> {
    let reference = take(flags, "--review-ref")?;
    if reference.trim().is_empty()
        || reference.len() > 1024
        || reference.chars().any(char::is_control)
    {
        return Err("invalid review reference".into());
    }
    Ok(reference)
}

async fn resolve(db: &impl GenericClient, generation: Option<i64>) -> Result<i64> {
    match generation {
        Some(id) => Ok(id),
        None => db
            .query_one(
                "SELECT generation_id FROM rfc.active_generation WHERE singleton",
                &[],
            )
            .await?
            .try_get::<_, Option<i64>>(0)?
            .ok_or_else(|| "no active generation".into()),
    }
}

async fn source(
    db: &impl GenericClient,
    reader: &BlobReader,
    generation: i64,
) -> Result<(xml::Catalog, bool, bool)> {
    let row = db.query_one(
        "SELECT o.raw_digest::bytea, b.byte_length::bigint, i.scope,
                g.manifest_format, coalesce(p.fixture, false), a.expected_rfc_ids
         FROM rfc.generations g JOIN rfc.generation_ingestion i ON i.generation_id = g.id
         JOIN rfc.source_observations o ON o.identity = i.catalog_before
         JOIN rfc.blobs b ON b.digest = o.raw_digest AND b.integrity_state = 'verified'
         LEFT JOIN rfc.generation_policy p ON p.generation_id = g.id
         LEFT JOIN rfc.generation_audit a ON a.generation_id = g.id
         JOIN rfc.source_observations closing ON closing.identity = i.catalog_after AND closing.raw_digest = o.raw_digest
         WHERE g.id = $1 AND g.state IN ('published','retained')
           AND o.kind = 'catalog' AND o.https_verification = 'verified'",
        &[&generation],
    ).await?;
    let digest = parse_digest(&row, 0)?;
    let catalog = xml::catalog(&canonical(reader, digest, row.try_get(1)?, CATALOG_LIMIT)?)?;
    let expected: Vec<_> = catalog
        .entries
        .iter()
        .filter(|(_, entry)| entry.has_txt)
        .map(|(id, _)| id.get() as i32)
        .collect();
    let sealed = row.try_get::<_, Option<Vec<i32>>>(5)?;
    let full = row.try_get::<_, &str>(2)? == "full"
        && row.try_get::<_, i32>(3)? >= 3
        && sealed.as_ref() == Some(&expected);
    Ok((catalog, full, row.try_get(4)?))
}

const MEMBERS: &str =
    "SELECT m.rfc_id::integer, d.notice_policy, d.txt_digest::bytea, d.byte_length::bigint,
            (b.integrity_state = 'verified' AND proof.rfc_id IS NOT NULL
                AND proof.txt_digest = d.txt_digest AND proof.document_version = d.identity
                AND origin.https_verification = 'verified' AND origin.raw_digest = d.txt_digest
                AND origin.source_url = 'https://www.rfc-editor.org/rfc/rfc' || m.rfc_id || '.txt')
              IS TRUE AS verified,
            receipt.receipt::text,
            (SELECT ceiling FROM rfc.policy_restrictions r WHERE r.rfc_id = m.rfc_id
             ORDER BY epoch DESC LIMIT 1) AS ceiling
     FROM rfc.generation_documents m JOIN rfc.document_versions d ON d.identity = m.document_version
     JOIN rfc.blobs b ON b.digest = d.txt_digest
     JOIN rfc.source_observations origin ON origin.identity = m.txt_observation
     LEFT JOIN rfc.generation_source_evidence proof ON (proof.generation_id,proof.rfc_id) = (m.generation_id,m.rfc_id)
     LEFT JOIN rfc.policy_receipts receipt ON receipt.identity = d.policy_receipt
     WHERE m.generation_id = $1 AND ($2::integer IS NULL OR m.rfc_id = $2::integer)
     ORDER BY m.rfc_id LIMIT 20001";

fn parse_digest(row: &tokio_postgres::Row, column: usize) -> Result<BlobDigest> {
    Ok(BlobDigest::from_bytes(
        row.try_get::<_, &[u8]>(column)?.try_into()?,
    ))
}

fn classify(
    reader: &BlobReader,
    registry: &Registry,
    catalog: &xml::Catalog,
    row: &tokio_postgres::Row,
) -> Result<Option<Receipt>> {
    let id = RfcNumber::new(row.try_get(0)?).ok_or("invalid RFC number")?;
    let digest = parse_digest(row, 2)?;
    let Some(entry) = catalog.entries.get(&id).filter(|_| row.get::<_, bool>(4)) else {
        return Ok(None);
    };
    let Ok(bytes) = canonical(reader, digest, row.try_get(3)?, TXT_LIMIT) else {
        return Ok(None);
    };
    Ok(Some(registry.classify(id, digest, entry, &bytes, true)?))
}

async fn audit(generation: Option<i64>, unresolved: bool) -> Result<Audit> {
    let mut writer = Writer::connect().await?;
    let tx = writer
        .client
        .build_transaction()
        .read_only(true)
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let generation = resolve(&tx, generation).await?;
    let id = GenerationId::new(generation).ok_or("invalid generation ID")?;
    let reader = BlobReader::open(&mirror_root(), false)?;
    let _fence = reader.generation_fence(id)?;
    let manifest = verify_stored_manifest(&tx, id).await?;
    let (catalog, full, fixture) = source(&tx, &reader, generation).await?;
    let registry = Registry::load()?;
    let rights_epoch = tx
        .query_one(
            "SELECT rights_epoch FROM rfc.policy_state WHERE singleton",
            &[],
        )
        .await?
        .try_get(0)?;
    let mut report = Audit {
        schema_version: 1,
        generation,
        manifest: manifest.to_string(),
        registry_digest: registry.digest.to_string(),
        classifier: CLASSIFIER.into(),
        complete: true,
        full,
        fixture,
        rights_epoch,
        unresolved_only: unresolved,
        total_documents: 0,
        eligible_family_documents: 0,
        eligible_family_excerpt_approvals: 0,
        obligations_changed: 0,
        decisions: BTreeMap::new(),
        methods: BTreeMap::new(),
        reasons: BTreeMap::new(),
        transitions: BTreeMap::new(),
        streams: BTreeMap::new(),
        by_period: BTreeMap::new(),
        by_rule: BTreeMap::new(),
        by_family: BTreeMap::new(),
        by_template: BTreeMap::new(),
        review_queue: BTreeMap::new(),
        rows: Vec::new(),
    };
    let rows = tx
        .query(MEMBERS, &[&generation, &Option::<i32>::None])
        .await?;
    if rows.is_empty() || rows.len() > MAX_CATALOG_ENTRIES {
        return Err("generation membership outside audit bounds".into());
    }
    report.total_documents = rows.len();
    report.full &= rows.len()
        == catalog
            .entries
            .values()
            .filter(|entry| entry.has_txt)
            .count();
    for row in rows {
        let rfc: i32 = row.try_get(0)?;
        let current: &str = row.try_get(1)?;
        let entry = RfcNumber::new(rfc).and_then(|id| catalog.entries.get(&id));
        let receipt = classify(&reader, &registry, &catalog, &row)?;
        let mut detail = AuditRow {
            rfc,
            current: current.into(),
            proposed: None,
            receipt: None,
            method: None,
            reason: None,
            rule: None,
            template: None,
        };
        bump(
            &mut report.streams,
            entry.and_then(|v| v.stream.as_deref()).unwrap_or("unknown"),
        );
        bump(
            &mut report.by_period,
            entry
                .and_then(|v| v.date.as_deref().or(v.month.as_deref()))
                .map_or("unknown", |date| &date[..4]),
        );
        let ceiling = row.try_get::<_, Option<&str>>(6)?.and_then(Policy::parse);
        if let Some(receipt) = receipt {
            let method = serde_json::to_value(receipt.approval_method)?;
            let method = method.as_str().ok_or("invalid approval method")?;
            let proposed = ceiling.map_or(receipt.decision, |cap| cap.min(receipt.decision));
            if receipt.template.is_some() {
                report.eligible_family_documents += 1;
                report.eligible_family_excerpt_approvals +=
                    usize::from(proposed == Policy::ExcerptAllowed);
            }
            let current_receipt: Option<Receipt> = row
                .try_get::<_, Option<&str>>(5)?
                .map(serde_json::from_str)
                .transpose()?;
            if current_receipt.as_ref().is_some_and(|current| {
                current.license != receipt.license
                    || current.template != receipt.template
                    || current.inventory_digest != receipt.inventory_digest
                    || current.attribution != receipt.attribution
            }) {
                report.obligations_changed += 1;
            }
            bump(&mut report.decisions, proposed.name());
            bump(&mut report.methods, method);
            bump(
                &mut report.transitions,
                format!("{current}->{}", proposed.name()),
            );
            bump(&mut report.by_rule, &receipt.rule_id);
            bump(
                &mut report.by_family,
                receipt
                    .license
                    .as_ref()
                    .map_or("unresolved", |license| &license.family),
            );
            bump(
                &mut report.by_template,
                receipt.template.as_deref().unwrap_or("unresolved"),
            );
            detail.proposed = Some(proposed);
            detail.method = Some(method.into());
            detail.receipt = Some(receipt.digest()?.to_string());
            detail.reason = if proposed < receipt.decision {
                Some("active_rights_restriction".into())
            } else {
                receipt.reason
            };
            detail.rule = Some(receipt.rule_id);
            detail.template = receipt.template;
        } else {
            report.complete = false;
            detail.reason = Some("source_verification_failed".into());
        }
        if let Some(reason) = &detail.reason {
            bump(&mut report.reasons, reason);
            let template = detail.template.as_deref().unwrap_or("unresolved");
            report
                .review_queue
                .entry(format!("{reason}/{template}"))
                .or_default()
                .push(rfc);
        }
        if !unresolved || detail.reason.is_some() {
            report.rows.push(detail);
        }
    }
    tx.commit().await?;
    Ok(report)
}

fn bump(map: &mut BTreeMap<String, usize>, key: impl Into<String>) {
    *map.entry(key.into()).or_default() += 1;
}

async fn explain(rfc: RfcNumber, requested: Option<i64>) -> Result<()> {
    let mut writer = Writer::connect().await?;
    let tx = writer
        .client
        .build_transaction()
        .read_only(true)
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let generation = resolve(&tx, requested).await?;
    let id = GenerationId::new(generation).ok_or("invalid generation ID")?;
    let reader = BlobReader::open(&mirror_root(), false)?;
    let _fence = reader.generation_fence(id)?;
    let (catalog, _, _) = source(&tx, &reader, generation).await?;
    let registry = Registry::load()?;
    let row = tx
        .query_opt(MEMBERS, &[&generation, &Some(rfc.get() as i32)])
        .await?
        .ok_or("RFC is absent from the requested generation")?;
    let current: Option<Receipt> = row
        .try_get::<_, Option<&str>>(5)?
        .map(serde_json::from_str)
        .transpose()?;
    let proposed = classify(&reader, &registry, &catalog, &row)?;
    let epoch: i64 = tx
        .query_one(
            "SELECT rights_epoch FROM rfc.policy_state WHERE singleton",
            &[],
        )
        .await?
        .try_get(0)?;
    let output = serde_json::json!({
        "schema_version": 1, "generation": generation, "rfc": rfc.get(), "registry_digest": registry.digest.to_string(),
        "rights_epoch": epoch, "current_policy": row.try_get::<_, &str>(1)?,
        "restriction": row.try_get::<_, Option<&str>>(6)?,
        "current_receipt": current, "proposed_receipt": proposed,
        "source_verified": proposed.is_some(),
    });
    tx.commit().await?;
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

async fn approve(registry: &str, path: &str, reference: &str) -> Result<()> {
    let bundle = Registry::load()?;
    if registry != bundle.digest.to_string() {
        return Err("registry digest does not match this Sync build".into());
    }
    let mut bytes = Vec::new();
    File::open(path)?
        .take(REPORT_LIMIT + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > REPORT_LIMIT {
        return Err("policy audit report exceeds the size limit".into());
    }
    let report: Audit = serde_json::from_slice(&bytes)?;
    if report.schema_version != 1
        || !report.complete
        || !report.full
        || report.fixture
        || report.unresolved_only
        || report.registry_digest != registry
        || report.classifier != CLASSIFIER
        || report.total_documents == 0
        || report.rows.len() != report.total_documents
        || report
            .rows
            .windows(2)
            .any(|pair| pair[0].rfc >= pair[1].rfc)
        || report.rows.iter().any(|row| {
            row.proposed.is_none()
                || row
                    .receipt
                    .as_deref()
                    .and_then(BlobDigest::from_hex)
                    .is_none()
        })
    {
        return Err(
            "approval requires a complete, unfiltered official-corpus report for this bundle"
                .into(),
        );
    }
    let manifest = BlobDigest::from_hex(&report.manifest).ok_or("invalid audited manifest")?;
    let report = identity("rfc-policy-audit-v1", &report)?;
    let writer = Writer::connect().await?;
    writer
        .client
        .query_one(
            "SELECT rfc.approve_policy($1::bytea,$2,$3::bytea,$4::bytea,$5)",
            &[
                &bundle.digest.as_bytes().as_slice(),
                &CLASSIFIER,
                &report.as_bytes().as_slice(),
                &manifest.as_bytes().as_slice(),
                &reference,
            ],
        )
        .await?;
    println!("Approved registry {registry} with audit {report}");
    Ok(())
}

async fn restrict(
    rfc: RfcNumber,
    ceiling: Policy,
    reason: &str,
    reference: &str,
    expected: i64,
) -> Result<()> {
    let mut writer = Writer::connect().await?;
    let tx = writer.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit = on")
        .await?;
    let locked: bool = tx
        .query_one(
            "SELECT pg_catalog.pg_try_advisory_xact_lock($1)",
            &[&RIGHTS_LOCK],
        )
        .await?
        .try_get(0)?;
    if !locked {
        return Err("another operation holds the rights fence; retry the restriction".into());
    }
    let epoch: i64 = tx
        .query_one(
            "SELECT rfc.restrict_policy($1::integer,$2,$3,$4,$5)",
            &[
                &(rfc.get() as i32),
                &ceiling.name(),
                &reason,
                &reference,
                &expected,
            ],
        )
        .await?
        .try_get(0)?;
    tx.commit().await?;
    println!(
        "RFC {} ceiling {} at rights epoch {epoch}",
        rfc.get(),
        ceiling.name()
    );
    Ok(())
}

/// Classify one supplied official sample without writing to the repository.
#[cfg(feature = "phase3-fixtures")]
pub fn qualify_sample(catalog: &str, original: &str, number: &str) -> Result<()> {
    let id = RfcNumber::new(number.parse()?).ok_or("invalid RFC number")?;
    let read = |path, limit| -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        File::open(path)?
            .take(limit as u64 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > limit {
            return Err("policy sample exceeds the source size limit".into());
        }
        Ok(bytes)
    };
    let catalog = xml::catalog(&read(catalog, CATALOG_LIMIT)?)?;
    let entry = catalog
        .entries
        .get(&id)
        .filter(|entry| entry.has_txt)
        .ok_or("sample is absent from the official TXT catalog")?;
    let bytes = read(original, TXT_LIMIT)?;
    let receipt = Registry::load()?.classify(id, BlobDigest::hash(&bytes), entry, &bytes, true)?;
    println!("{}", serde_json::to_string(&receipt)?);
    Ok(())
}
