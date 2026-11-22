//! Local-only Phase 0 application probes.

use std::path::Path;
use std::time::Duration;

pub type ProbeResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Makes a bounded HTTPS request to a test fixture using the normal TLS verifier.
pub async fn https_probe(url: &str, extra_ca: &Path, expected: &str) -> ProbeResult {
    let pem = std::fs::read(extra_ca)?;
    let client = reqwest::Client::builder()
        .use_rustls_tls()
        .https_only(true)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .add_root_certificate(reqwest::Certificate::from_pem(&pem)?)
        .build()?;

    match client.get(url).send().await {
        Ok(response) if expected == "ok" && response.status().is_success() => Ok(()),
        Err(error) if expected == "certificate-error" && is_certificate_error(&error) => Ok(()),
        Ok(response) => Err(format!("unexpected HTTPS response: {}", response.status()).into()),
        Err(error) => Err(error.into()),
    }
}

fn is_certificate_error(error: &reqwest::Error) -> bool {
    let mut cause = std::error::Error::source(error);
    while let Some(error) = cause {
        if error.to_string().contains("invalid peer certificate") {
            return true;
        }
        cause = error.source();
    }
    false
}
