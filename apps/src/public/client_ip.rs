use std::{
    env, fs,
    io::{self, Read},
    net::IpAddr,
    path::Path,
    str,
};

use axum::http::{HeaderMap, HeaderName, StatusCode};
use serde::Deserialize;

use super::invalid;

const CONFIG_FILE: &str = "RFC_SERVE_CONFIG_FILE";
const HEADER: &str = "RFC_MCP_CLIENT_IP_HEADER";
const MODE: &str = "RFC_MCP_CLIENT_IP_HEADER_MODE";
const CONFIG_BYTES: u64 = 16 * 1024;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct FileSettings {
    client_ip_header: Option<String>,
    client_ip_header_mode: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum Mode {
    #[default]
    Single,
    Chain,
}

#[derive(Debug)]
struct Settings {
    header: HeaderName,
    mode: Mode,
}

impl Settings {
    fn resolve(
        file: FileSettings,
        env_header: Option<&str>,
        env_mode: Option<&str>,
    ) -> io::Result<Self> {
        let file_header = file
            .client_ip_header
            .as_deref()
            .map(|value| header(value, "client_ip_header"))
            .transpose()?;
        let file_mode = file
            .client_ip_header_mode
            .as_deref()
            .map(|value| mode(value, "client_ip_header_mode"))
            .transpose()?;
        Ok(Self {
            header: env_header
                .map(|value| header(value, HEADER))
                .transpose()?
                .or(file_header)
                .unwrap_or_else(|| HeaderName::from_static("x-real-ip")),
            mode: env_mode
                .map(|value| mode(value, MODE))
                .transpose()?
                .or(file_mode)
                .unwrap_or_default(),
        })
    }
}

pub(super) struct ClientIp {
    proxies: Vec<IpAddr>,
    settings: Settings,
}

impl ClientIp {
    pub(super) fn load(proxies: Vec<IpAddr>) -> io::Result<Self> {
        let file = optional_env(CONFIG_FILE)?
            .as_deref()
            .map(Path::new)
            .map(read_settings)
            .transpose()?
            .unwrap_or_default();
        let settings = Settings::resolve(
            file,
            optional_env(HEADER)?.as_deref(),
            optional_env(MODE)?.as_deref(),
        )?;
        Ok(Self { proxies, settings })
    }

    pub(super) fn source(&self, peer: IpAddr, headers: &HeaderMap) -> Result<IpAddr, StatusCode> {
        if !self.proxies.contains(&peer) {
            return Ok(peer);
        }
        let mut values = headers.get_all(&self.settings.header).iter();
        let value = values.next().ok_or(StatusCode::BAD_REQUEST)?;
        if values.next().is_some() {
            return Err(StatusCode::BAD_REQUEST);
        }
        let value = str::from_utf8(value.as_bytes()).map_err(|_| StatusCode::BAD_REQUEST)?;
        match self.settings.mode {
            Mode::Single => value
                .parse::<IpAddr>()
                .map(|ip| ip.to_canonical())
                .map_err(|_| StatusCode::BAD_REQUEST),
            Mode::Chain => self.chain_source(value),
        }
    }

    fn chain_source(&self, value: &str) -> Result<IpAddr, StatusCode> {
        let mut source = None;
        // Validate the complete field, retaining the nearest untrusted hop.
        for entry in value.rsplit(',') {
            let ip = entry
                .trim_matches([' ', '\t'])
                .parse::<IpAddr>()
                .map_err(|_| StatusCode::BAD_REQUEST)?
                .to_canonical();
            if source.is_none() && !self.proxies.contains(&ip) {
                source = Some(ip);
            }
        }
        source.ok_or(StatusCode::BAD_REQUEST)
    }
}

fn optional_env(name: &str) -> io::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(invalid(name)),
    }
}

fn header(value: &str, setting: &str) -> io::Result<HeaderName> {
    HeaderName::from_bytes(value.as_bytes()).map_err(|_| invalid(setting))
}

fn mode(value: &str, setting: &str) -> io::Result<Mode> {
    match value {
        "single" => Ok(Mode::Single),
        "chain" => Ok(Mode::Chain),
        _ => Err(invalid(setting)),
    }
}

fn read_settings(path: &Path) -> io::Result<FileSettings> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(path).map_err(|_| invalid(CONFIG_FILE))?;
    if !file.metadata().map_err(|_| invalid(CONFIG_FILE))?.is_file() {
        return Err(invalid(CONFIG_FILE));
    }
    let mut text = String::new();
    file.take(CONFIG_BYTES + 1)
        .read_to_string(&mut text)
        .map_err(|_| invalid(CONFIG_FILE))?;
    if text.len() as u64 > CONFIG_BYTES {
        return Err(invalid(CONFIG_FILE));
    }
    // TOML decoding errors can contain source lines with configuration values.
    toml::from_str(&text).map_err(|_| invalid(CONFIG_FILE))
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn ip(value: &str) -> IpAddr {
        value.parse().unwrap()
    }

    fn proxy_settings(header: Option<&str>, mode: Option<&str>) -> ClientIp {
        ClientIp {
            proxies: vec![ip("203.0.113.1"), ip("2001:db8::2")],
            settings: Settings::resolve(FileSettings::default(), header, mode).unwrap(),
        }
    }

    #[test]
    fn resolves_defaults_file_and_environment_per_setting() {
        let defaults = Settings::resolve(FileSettings::default(), None, None).unwrap();
        assert_eq!(defaults.header, "x-real-ip");
        assert_eq!(defaults.mode, Mode::Single);

        let file = || {
            toml::from_str(
                r#"
                client_ip_header = "X-Forwarded-For"
                client_ip_header_mode = "chain"
                "#,
            )
            .unwrap()
        };
        let settings = Settings::resolve(file(), Some("Cf-Connecting-IP"), None).unwrap();
        assert_eq!(settings.header, "cf-connecting-ip");
        assert_eq!(settings.mode, Mode::Chain);
        let settings = Settings::resolve(file(), None, Some("single")).unwrap();
        assert_eq!(settings.header, "x-forwarded-for");
        assert_eq!(settings.mode, Mode::Single);
    }

    #[test]
    fn rejects_invalid_explicit_settings() {
        for name in ["", " X-Real-IP", "X-Real-IP:", "X-Real-IP\n", "X-Üser"] {
            let error = Settings::resolve(FileSettings::default(), Some(name), None).unwrap_err();
            assert_eq!(error.to_string(), format!("invalid {HEADER}"));
        }
        for value in ["", "CHAIN", "forwarded"] {
            let error = Settings::resolve(FileSettings::default(), None, Some(value)).unwrap_err();
            assert_eq!(error.to_string(), format!("invalid {MODE}"));
        }
        let file = toml::from_str(r#"client_ip_header = """#).unwrap();
        let error = Settings::resolve(file, Some("X-Real-IP"), None).unwrap_err();
        assert_eq!(error.to_string(), "invalid client_ip_header");
        assert!(toml::from_str::<FileSettings>("unknown_key = 1").is_err());
        assert!(read_settings(Path::new("")).is_err());
        assert!(read_settings(Path::new(".")).is_err());
    }

    #[test]
    fn single_mode_uses_only_the_selected_header_for_trusted_peers() {
        let peer = ip("203.0.113.1");
        let direct = ip("203.0.113.7");
        let mut headers = HeaderMap::new();
        headers.append("x-real-ip", HeaderValue::from_static("192.0.2.10"));
        headers.append(
            "cf-connecting-ip",
            HeaderValue::from_static("::ffff:192.0.2.11"),
        );
        let default = proxy_settings(None, None);
        let alternate = proxy_settings(Some("CF-Connecting-IP"), None);
        assert_eq!(default.source(peer, &headers), Ok(ip("192.0.2.10")));
        assert_eq!(alternate.source(peer, &headers), Ok(ip("192.0.2.11")));
        headers.append("cf-connecting-ip", HeaderValue::from_static("2001:db8::9"));
        assert_eq!(
            alternate.source(peer, &headers),
            Err(StatusCode::BAD_REQUEST)
        );
        assert_eq!(alternate.source(direct, &headers), Ok(direct));
        headers.remove("cf-connecting-ip");
        assert_eq!(
            alternate.source(peer, &headers),
            Err(StatusCode::BAD_REQUEST)
        );

        headers.insert(
            "x-real-ip",
            HeaderValue::from_static("192.0.2.1, 192.0.2.2"),
        );
        assert_eq!(default.source(peer, &headers), Err(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn chain_mode_selects_the_rightmost_untrusted_address() {
        let config = proxy_settings(Some("X-Forwarded-For"), Some("chain"));
        let peer = ip("203.0.113.1");
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static(
                "192.0.2.10,\t::ffff:192.0.2.11 , 2001:db8::2, ::ffff:203.0.113.1",
            ),
        );
        assert_eq!(config.source(peer, &headers), Ok(ip("192.0.2.11")));
        headers.insert("x-forwarded-for", HeaderValue::from_static("2001:db8::9"));
        assert_eq!(config.source(peer, &headers), Ok(ip("2001:db8::9")));
        headers.append("x-forwarded-for", HeaderValue::from_static("192.0.2.10"));
        assert_eq!(config.source(peer, &headers), Err(StatusCode::BAD_REQUEST));
    }

    #[test]
    fn chain_mode_rejects_malformed_and_all_trusted_lists() {
        let config = proxy_settings(Some("x-forwarded-for"), Some("chain"));
        let peer = ip("203.0.113.1");
        let mut headers = HeaderMap::new();
        for value in [
            "",
            "203.0.113.1, 2001:db8::2",
            "192.0.2.1,,203.0.113.1",
            "invalid, 192.0.2.1, 203.0.113.1",
            "192.0.2.1, [2001:db8::2]:8000",
            "192.0.2.1:8000",
            "for=192.0.2.1",
        ] {
            headers.insert("x-forwarded-for", HeaderValue::from_static(value));
            assert_eq!(config.source(peer, &headers), Err(StatusCode::BAD_REQUEST));
        }
    }
}
