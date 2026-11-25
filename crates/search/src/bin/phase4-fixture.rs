//! Drives the actual search library as the serve role in private Compose.

use rfc_core::GenerationId;
use rfc_search::{Search, SearchError, SearchRequest};
use rfc_storage::{
    blob::BlobWriter,
    postgres::{Result as RepositoryResult, RuntimeRole, connect, load_manifest, mirror_root},
};
use serde_json::{Value, json};
use std::{
    io::{self, BufRead, Write},
    time::Instant,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if let Some(generation) = std::env::args().nth(1) {
        ready(generation.parse()?)
            .await
            .map_err(|error| error.to_string())?;
        return Ok(());
    }
    let key: [u8; 48] = std::fs::read("/phase4/cursor-key")?
        .try_into()
        .map_err(|_| "fixture cursor key length differs")?;
    let search = Search::new(&key);
    let (lines, mut input) = tokio::sync::mpsc::channel(1);
    std::thread::spawn(move || {
        for line in io::stdin().lock().lines() {
            if lines.blocking_send(line).is_err() {
                break;
            }
        }
    });
    let mut output = io::stdout().lock();
    while let Some(line) = input.recv().await {
        let line = line?;
        let value: Value = serde_json::from_str(&line)?;
        let start = Instant::now();
        let result = if let Some(query) = value.get("explain").and_then(Value::as_str) {
            Search::explain_fixture(query).await.map(|plan| json!(plan))
        } else {
            let request: SearchRequest = serde_json::from_str(&line)?;
            search.run(request).await.map(|page| json!(page))
        };
        let elapsed_us = start.elapsed().as_micros();
        let response = match result {
            Ok(result) => json!({"result": result, "elapsed_us": elapsed_us}),
            Err(error) => {
                let span = match &error {
                    SearchError::Query(error) => json!([error.span.start, error.span.end]),
                    _ => Value::Null,
                };
                json!({"error": error.code(), "message": error.to_string(), "span": span, "elapsed_us": elapsed_us})
            }
        };
        serde_json::to_writer(&mut output, &response)?;
        writeln!(output)?;
        output.flush()?;
    }
    Ok(())
}

async fn ready(id: i64) -> RepositoryResult<()> {
    let generation = GenerationId::new(id).ok_or("invalid fixture generation ID")?;
    let mut client = connect(RuntimeRole::Sync).await?;
    let _coordinator = BlobWriter::coordinate(&mirror_root())?;
    let held: bool = client
        .query_one("SELECT pg_try_advisory_lock(-1::bigint)", &[])
        .await?
        .try_get(0)?;
    if !held {
        return Err("another synchronization holds the corpus lock".into());
    }
    let tx = client.transaction().await?;
    tx.query_one(
        "SELECT id FROM rfc.generations WHERE id=$1 AND state='building' FOR NO KEY UPDATE",
        &[&id],
    )
    .await?;
    let manifest = load_manifest(&tx, generation).await?;
    let format = manifest.version();
    let (bytes, digest) = manifest.encode();
    let updated = tx
        .execute(
            "UPDATE rfc.generations SET state='ready',manifest_format=$2,manifest_bytes=$3,
        manifest_digest=$4::bytea WHERE id=$1",
            &[&id, &format, &bytes, &digest.as_bytes().as_slice()],
        )
        .await?;
    assert_eq!(updated, 1);
    tx.commit().await?;
    Ok(())
}
