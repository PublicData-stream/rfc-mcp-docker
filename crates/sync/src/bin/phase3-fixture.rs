//! Drives the production repositories in a disposable Compose project.

use rfc_core::{BlobDigest, ByteRange, GenerationId, RfcNumber};
use rfc_mcp_docker_sync::{
    Maintenance, Scope, maintain, promote, reconcile_fixture, reconcile_fixture_with_reviews,
};
use rfc_storage::{
    blob::{BlobReader, BlobWriter},
    fixture_boundary,
    postgres::{
        GenerationSelection, Reader, Result, RuntimeRole, connect, mirror_root,
        verify_stored_manifest,
    },
};
use std::{collections::BTreeMap, io::Read, process::Stdio, time::Duration};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [scope] if scope == "full" => reconcile_fixture(Scope::Full).await?,
        [scope] if scope == "sample" => reconcile_fixture(Scope::Sample).await?,
        [scope, review] if scope == "full" && review == "--review-fixture" => {
            let reviews = std::fs::read_to_string("/phase5/fixture-policy.json")?;
            reconcile_fixture_with_reviews(Scope::Full, &reviews).await?;
        }
        [action] if action == "digest" => {
            let mut bytes = Vec::new();
            std::io::stdin()
                .take(4 * 1024 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            assert!(bytes.len() <= 4 * 1024 * 1024);
            println!("{}", BlobDigest::hash(&bytes));
        }
        [action, id, sequence] if matches!(action.as_str(), "publish" | "rollback") => {
            println!(
                "{}",
                promote(generation(id)?, sequence.parse()?, action == "rollback").await?
            );
        }
        [action] if action == "recover" => maintain(Maintenance::Recover).await?,
        [action] if action == "gc" => maintain(Maintenance::Collect).await?,
        [action, id] if action == "discard" => {
            maintain(Maintenance::Discard(generation(id)?)).await?
        }
        [action, id] if action == "read" => read(id).await?,
        [action] if action == "cancel-worker" => cancel_worker().await?,
        [action, digest] if action == "restore-blob" => {
            let writer = BlobWriter::open(&mirror_root())?;
            let source = writer.open_candidate(std::path::Path::new("/fixture"), digest)?;
            assert_eq!(
                writer
                    .install(source, 64 * 1024 * 1024)?
                    .blob()
                    .digest()
                    .to_string(),
                *digest
            );
        }
        [action, id] if action == "restore-fence" => {
            BlobWriter::open(&mirror_root())?.prepare_generation_fence(generation(id)?)?;
        }
        [action] if action == "verify-restore" => verify_restore().await?,
        _ => return Err("unknown publication fixture command".into()),
    }
    Ok(())
}

// The caller keeps writers stopped for the entire verification.
async fn verify_restore() -> Result<()> {
    const CAP: usize = 64 * 1024 * 1024;
    let db = connect(RuntimeRole::Serve).await?;
    let store = BlobReader::open(&mirror_root(), true)?;
    let mut inventory = BTreeMap::new();
    for row in db
        .query(
            "SELECT digest::bytea, byte_length::bigint FROM rfc.blobs",
            &[],
        )
        .await?
    {
        let digest = BlobDigest::from_bytes(row.try_get::<_, &[u8]>(0)?.try_into()?);
        let length: i64 = row.try_get(1)?;
        let bytes = store.read_range(
            digest,
            ByteRange::new(0, length).ok_or("invalid length")?,
            CAP,
        )?;
        assert_eq!(
            store.open_blob(digest)?.metadata()?.len(),
            u64::try_from(length)?
        );
        assert_eq!(BlobDigest::hash(&bytes), digest, "restored blob {digest}");
        inventory.insert(digest.to_string(), bytes);
    }
    let mut manifests = BTreeMap::new();
    for row in db
        .query(
            "SELECT id FROM rfc.generations WHERE state IN ('ready','published','retained')",
            &[],
        )
        .await?
    {
        let id = row.try_get::<_, i64>(0)?.to_string();
        manifests.insert(
            id.clone(),
            verify_stored_manifest(&db, generation(&id)?)
                .await?
                .to_string(),
        );
    }
    let mut ranges = 0;
    for row in db.query(
        "SELECT encode(d.txt_digest,'hex'), r.first::bigint, r.last::bigint
         FROM rfc.document_versions d JOIN (
            SELECT document_version, start_byte AS first, end_byte AS last FROM rfc.sections
            UNION ALL SELECT document_version, heading_start_byte, heading_end_byte FROM rfc.sections
            UNION ALL SELECT document_version, start_byte, end_byte FROM rfc.text_chunks
            UNION ALL SELECT document_version, start_byte, end_byte FROM rfc.notice_ranges
            UNION ALL SELECT document_version, start_byte, end_byte FROM rfc.relation_evidence
                WHERE start_byte IS NOT NULL
         ) r ON r.document_version = d.identity", &[],
    ).await? {
        let hex: String = row.try_get(0)?;
        let digest = BlobDigest::from_hex(&hex).ok_or("invalid original digest")?;
        let range = ByteRange::new(row.try_get(1)?, row.try_get(2)?).ok_or("invalid byte range")?;
        let expected = range.select(&inventory[&hex]).ok_or("range outside restored original")?;
        assert_eq!(store.read_range(digest, range, CAP)?, expected);
        ranges += 1;
    }
    for row in db
        .query(
            "SELECT encode(txt_digest,'hex'), line_starts FROM rfc.document_versions
             WHERE line_starts IS NOT NULL",
            &[],
        )
        .await?
    {
        let hex: String = row.try_get(0)?;
        let actual: Vec<i64> = row.try_get(1)?;
        let bytes = &inventory[&hex];
        let mut expected = vec![0];
        for (index, &byte) in bytes.iter().enumerate().take(bytes.len().saturating_sub(1)) {
            if byte == b'\n' {
                expected.push(i64::try_from(index + 1)?);
            }
        }
        assert_eq!(actual, expected);
        ranges += actual.len();
    }
    println!(
        "{}",
        serde_json::json!({
            "blobs": inventory.len(), "ranges": ranges, "manifests": manifests,
        })
    );
    Ok(())
}

fn generation(value: &str) -> Result<GenerationId> {
    GenerationId::new(value.parse()?).ok_or_else(|| "invalid generation ID".into())
}

async fn read(selector: &str) -> Result<()> {
    let selection = if selector == "active" {
        GenerationSelection::Active
    } else {
        GenerationSelection::Pinned(generation(selector)?)
    };
    let report = Reader::connect().await?.with_generation(selection, async |guard| {
        let id = i64::try_from(guard.generation().get())?;
        let manifest = verify_stored_manifest(guard.transaction(), guard.generation()).await?;
        let metadata = guard.transaction().query_one(
            "SELECT gd.title, d.byte_length::bigint,
                (SELECT count(*) FROM rfc.rfc_relations WHERE generation_id = $1),
                (SELECT count(*) FROM rfc.notice_ranges WHERE document_version = d.identity)
             FROM rfc.generation_documents gd JOIN rfc.document_versions d ON d.identity = gd.document_version
             WHERE gd.generation_id = $1 AND gd.rfc_id = 101", &[&id],
        ).await?;
        let rfc = RfcNumber::new(101).unwrap();
        let range = ByteRange::new(0, metadata.try_get(1)?).ok_or("invalid fixture range")?;
        let original = guard.read_document(rfc, range, 64 * 1024).await?;
        let mut files = guard.open_worker_files(&[rfc], 1).await?;
        fixture_boundary("reader_accepted");
        let mut worker_bytes = Vec::new();
        files.remove(0).read_to_end(&mut worker_bytes)?;
        assert_eq!(worker_bytes, original);
        eprintln!("worker read {}", BlobDigest::hash(&worker_bytes));
        Ok(serde_json::json!({
            "generation": id, "sequence": guard.publication_sequence(), "manifest": manifest.to_string(),
            "title": metadata.try_get::<_, &str>(0)?, "body": original,
            "relations": metadata.try_get::<_, i64>(2)?, "notices": metadata.try_get::<_, i64>(3)?,
        }))
    }).await?;
    println!("{report}");
    Ok(())
}

async fn cancel_worker() -> Result<()> {
    Reader::connect()
        .await?
        .with_generation(GenerationSelection::Active, async |guard| {
            let mut launcher = tokio::process::Command::new("/bin/sh");
            launcher
                .args(["-ec", "echo $$ > /tmp/rfc-phase3-worker-pid; exec sleep 60"])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let outcome =
                tokio::time::timeout(Duration::from_secs(1), guard.run_worker(&mut launcher)).await;
            assert!(outcome.is_err(), "worker should have been cancelled");
            let pid = std::fs::read_to_string("/tmp/rfc-phase3-worker-pid")?;
            let path = format!("/proc/{}", pid.trim());
            tokio::time::timeout(Duration::from_secs(3), async {
                while std::path::Path::new(&path).exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            Ok(())
        })
        .await
}
