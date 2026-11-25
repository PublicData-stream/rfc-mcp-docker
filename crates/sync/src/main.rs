use rfc_core::GenerationId;
use rfc_mcp_docker::{ProbeResult, https_probe};
use rfc_mcp_docker_sync::{
    Maintenance, MonitorCommand, Scope, Writer, check_config, inspect, maintain, promote,
    reconcile, run_daemon,
};
use rfc_storage::{blob::BlobWriter, postgres::mirror_root};
use std::{path::Path, process::ExitCode};

const USAGE: &str = "rfc-mcp-docker-sync [status|watch|diagnose] [--json]\n\
     Run without arguments to start the daemon.\n\
     Use --check-config, --version, --phase1-check-storage, --phase2-reconcile (--full|--sample),\n\
     --phase3-(publish|rollback) GENERATION --expect-sequence SEQUENCE,\n\
     --phase3-(recover|gc), or --phase3-discard GENERATION for existing operations.";

#[tokio::main(worker_threads = 2)]
async fn main() -> ExitCode {
    let args: Vec<_> = std::env::args().skip(1).collect();
    let command = match args.first().map(String::as_str) {
        Some("status") => Some(MonitorCommand::Status),
        Some("watch") => Some(MonitorCommand::Watch),
        Some("diagnose") => Some(MonitorCommand::Diagnose),
        _ => None,
    };
    if let Some(command) = command {
        return match &args[1..] {
            [] => ExitCode::from(inspect(command, false).await),
            [flag] if flag == "--json" => ExitCode::from(inspect(command, true).await),
            [flag] if flag == "--help" || flag == "-h" => {
                println!("rfc-mcp-docker-sync {} [--json]", args[0]);
                ExitCode::SUCCESS
            }
            _ => {
                eprintln!("Expected rfc-mcp-docker-sync {} [--json]", args[0]);
                ExitCode::from(2)
            }
        };
    }
    match run(&args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Error: {error:?}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &[String]) -> ProbeResult {
    match args {
        [] => run_daemon().await,
        [option] if option == "--help" || option == "-h" => {
            println!("{USAGE}");
            Ok(())
        }
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
        [option, scope] if option == "--phase2-reconcile" => {
            let scope = match scope.as_str() {
                "--full" => Scope::Full,
                "--sample" => Scope::Sample,
                _ => return Err("reconciliation requires --full or --sample".into()),
            };
            reconcile(scope).await
        }
        [option, generation, flag, sequence]
            if matches!(option.as_str(), "--phase3-publish" | "--phase3-rollback")
                && flag == "--expect-sequence" =>
        {
            let generation =
                GenerationId::new(generation.parse()?).ok_or("invalid generation ID")?;
            let expected = sequence.parse()?;
            let rollback = option == "--phase3-rollback";
            let sequence = promote(generation, expected, rollback).await?;
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
        _ => Err(USAGE.into()),
    }
}
