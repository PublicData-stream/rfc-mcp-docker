use rfc_core::GenerationId;
use rfc_mcp_docker::{ProbeResult, https_probe};
use rfc_mcp_docker_sync::{
    Maintenance, Scope, Writer, check_config, maintain, promote, reconcile, run_daemon,
};
use rfc_storage::{blob::BlobWriter, postgres::mirror_root};
use std::path::Path;

#[tokio::main(worker_threads = 2)]
async fn main() -> ProbeResult {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => run_daemon().await,
        [option] if option == "--check-config" => check_config(),
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
        [option] if option == "--phase3-recover" => maintain(Maintenance::Recover).await,
        [option] if option == "--phase3-gc" => maintain(Maintenance::Collect).await,
        [option, generation] if option == "--phase3-discard" => {
            let generation =
                GenerationId::new(generation.parse()?).ok_or("invalid generation ID")?;
            maintain(Maintenance::Discard(generation)).await
        }
        _ => Err(
            "expected --version, --check-config, --phase1-check-storage, --phase2-reconcile (--full|--sample), \
             --phase3-(publish|rollback) GENERATION --expect-sequence SEQUENCE, \
             --phase3-(recover|gc), or --phase3-discard GENERATION"
                .into(),
        ),
    }
}
