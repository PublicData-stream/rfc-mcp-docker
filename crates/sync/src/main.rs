use rfc_core::GenerationId;
use rfc_mcp_docker::{ProbeResult, https_probe};
use rfc_mcp_docker_sync::{Scope, Writer, promote, reconcile};
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
        [option, scope] if option == "--phase2-reconcile" => match scope.as_str() {
            "--full" => reconcile(Scope::Full).await,
            "--sample" => reconcile(Scope::Sample).await,
            _ => Err("reconciliation requires --full or --sample".into()),
        },
        [option, generation, flag, sequence]
            if matches!(option.as_str(), "--phase3-publish" | "--phase3-rollback")
                && flag == "--expect-sequence" =>
        {
            let generation =
                GenerationId::new(generation.parse()?).ok_or("invalid generation ID")?;
            let sequence =
                promote(generation, sequence.parse()?, option == "--phase3-rollback").await?;
            println!(
                "{{\"generation_id\":\"{}\",\"publication_sequence\":\"{sequence}\"}}",
                generation.get()
            );
            Ok(())
        }
        _ => Err(
            "expected --version, --phase1-check-storage, --phase2-reconcile (--full|--sample), \
             --phase3-(publish|rollback) GENERATION --expect-sequence SEQUENCE"
                .into(),
        ),
    }
}
