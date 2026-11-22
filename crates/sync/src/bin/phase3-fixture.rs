//! Drives the production repositories in a disposable Compose project.

use rfc_core::{BlobDigest, ByteRange, GenerationId, RfcNumber};
use rfc_mcp_docker_sync::{
    Maintenance, Scope, maintain, promote, reconcile_fixture, reconcile_fixture_with_reviews,
};
use rfc_storage::{
    blob::BlobWriter,
    fixture_boundary,
    postgres::{GenerationSelection, Reader, Result, mirror_root, verify_stored_manifest},
};
use std::{io::Read, process::Stdio, time::Duration};

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
        _ => return Err("unknown publication fixture command".into()),
    }
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
