#[path = "../mcp.rs"]
mod mcp;

#[path = "../public.rs"]
mod public;

use rfc_mcp_docker::ProbeResult;
use rfc_storage::{
    blob::BlobReader,
    postgres::{GenerationSelection, Reader, mirror_root},
};

fn main() -> ProbeResult {
    let args: Vec<_> = std::env::args().skip(1).collect();
    #[cfg(target_os = "linux")]
    match args.as_slice() {
        [option] if option == "--phase0-sandbox" => return Ok(rfc_search::sandbox_probe()?),
        [option, mode, case, pattern]
            if option == "--rg-worker"
                && matches!(mode.as_str(), "literal" | "regex")
                && matches!(case.as_str(), "sensitive" | "insensitive") =>
        {
            if let Err(error) =
                rfc_search::sandbox_worker(pattern, mode == "literal", case == "sensitive")
            {
                eprintln!("ripgrep confinement setup failed: {error}");
                std::process::exit(125);
            }
            return Ok(());
        }
        _ => {}
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(&args))
}

async fn run(args: &[String]) -> ProbeResult {
    match args {
        [] => public::run().await,
        [option] if option == "--version" => {
            println!("rfc-mcp-docker-serve {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        [option] if option == "--phase1-check-storage" => {
            let reader = Reader::connect().await?;
            BlobReader::open(&mirror_root(), true)?;
            let (active, mut sequence) = reader.active().await?;
            if active.is_some() {
                sequence = reader
                    .with_generation(GenerationSelection::Active, async |guard| {
                        Ok(guard.publication_sequence())
                    })
                    .await?;
            }
            println!("serve storage checks passed, publication sequence {sequence}");
            Ok(())
        }
        [option, port] if option == "--phase0-mcp" => {
            let listener = tokio::net::TcpListener::bind((
                std::net::Ipv4Addr::LOCALHOST,
                port.parse::<u16>()?,
            ))
            .await?;
            println!("{}", listener.local_addr()?);
            axum::serve(listener, mcp::router()).await?;
            Ok(())
        }
        _ => Err("rfc-mcp-docker-serve: unexpected argument".into()),
    }
}
