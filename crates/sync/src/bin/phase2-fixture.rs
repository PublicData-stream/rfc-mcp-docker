//! Exercises the production ingestion pipeline against only the private fixture endpoints.

use rfc_core::GenerationId;
use rfc_mcp_docker_sync::{Scope, reconcile_fixture};
use rfc_storage::postgres::{Reader, Result};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [command] if command == "full" => reconcile_fixture(Scope::Full).await?,
        [command] if command == "sample" => reconcile_fixture(Scope::Sample).await?,
        [command, id] if command == "verify" => {
            let id = GenerationId::new(id.parse()?).ok_or("invalid generation ID")?;
            let digest = Reader::connect().await?.verify_manifest(id).await?;
            println!("verified {} {digest}", id.get());
        }
        _ => return Err("usage: full | sample | verify GENERATION".into()),
    }
    Ok(())
}
