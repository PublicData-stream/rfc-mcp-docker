#[path = "../feed.rs"]
mod feed;

use rfc_mcp_docker::{ProbeResult, https_probe};
use std::path::Path;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ProbeResult {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [] => feed::run().await,
        [option] if option == "--version" => {
            println!("rfc-mcp-docker-feed {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        [option, url, ca, expected] if option == "--phase0-https" => {
            https_probe(url, Path::new(ca), expected).await
        }
        _ => Err("rfc-mcp-docker-feed: unexpected argument".into()),
    }
}
