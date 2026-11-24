//! Exercises the production ingestion pipeline against only the private fixture endpoints.

use rfc_core::{BlobDigest, ByteRange, GenerationId};
use rfc_mcp_docker_sync::{Scope, check_legacy_fixture_config, promote_fixture, reconcile_fixture};
use rfc_storage::{
    blob::BlobReader,
    postgres::{Reader, Result, mirror_root},
};
use std::io::{self, Write};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [command] if command == "full" => reconcile_fixture(Scope::Full).await?,
        [command] if command == "sample" => reconcile_fixture(Scope::Sample).await?,
        [command] if command == "check-config" => check_legacy_fixture_config()?,
        [action, id, sequence] if action == "publish" || action == "rollback" => {
            let id = GenerationId::new(id.parse()?).ok_or("invalid generation ID")?;
            println!("{}", promote_fixture(id, sequence.parse()?, action == "rollback").await?);
        }
        [command, id] if command == "verify" => {
            let id = GenerationId::new(id.parse()?).ok_or("invalid generation ID")?;
            let digest = Reader::connect().await?.verify_manifest(id).await?;
            println!("verified {} {digest}", id.get());
        }
        [command, digest, length] if command == "read-blob" => {
            let digest = BlobDigest::from_hex(digest).ok_or("invalid blob digest")?;
            let range = ByteRange::new(0, length.parse()?).ok_or("invalid blob range")?;
            let bytes =
                BlobReader::open(&mirror_root(), true)?.read_range(digest, range, 64 * 1024)?;
            if BlobDigest::hash(&bytes) != digest {
                return Err("canonical blob differs from its digest".into());
            }
            io::stdout().lock().write_all(&bytes)?;
        }
        _ => return Err("usage: full | sample | check-config | publish/rollback ID SEQUENCE | verify ID | read-blob DIGEST LENGTH".into()),
    }
    Ok(())
}
