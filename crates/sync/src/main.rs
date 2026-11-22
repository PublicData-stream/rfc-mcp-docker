use rfc_mcp_docker::{ProbeResult, https_probe};
use rfc_mcp_docker_sync::Writer;
use rfc_storage::{blob::BlobWriter, postgres::mirror_root};
use std::path::Path;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ProbeResult {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [option] if option == "--version" => {
            println!("rfc-mcp-docker-sync {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        [option] if option == "--phase1-check-storage" => {
            Writer::connect().await?;
            BlobWriter::open(&mirror_root())?;
            println!("sync storage checks passed");
            Ok(())
        }
        [option, url, ca, expected] if option == "--phase0-https" => {
            https_probe(url, Path::new(ca), expected).await
        }
        _ => Err("rfc-mcp-docker-sync: expected --version or --phase1-check-storage".into()),
    }
}
