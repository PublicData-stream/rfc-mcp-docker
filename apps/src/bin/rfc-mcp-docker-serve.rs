#[path = "../mcp.rs"]
mod mcp;

#[cfg(target_os = "linux")]
#[path = "../sandbox.rs"]
mod sandbox;

use rfc_mcp_docker::ProbeResult;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ProbeResult {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [option] if option == "--version" => {
            println!("rfc-mcp-docker-serve {}", env!("CARGO_PKG_VERSION"));
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
        #[cfg(target_os = "linux")]
        [option] if option == "--phase0-sandbox" => Ok(sandbox::run()?),
        _ => Err("rfc-mcp-docker-serve is a Phase 0 skeleton; use --version".into()),
    }
}
