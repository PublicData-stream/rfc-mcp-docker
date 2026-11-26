use std::{
    env, fs,
    io::{self, Read},
    path::Path,
    str,
};

use rfc_storage::postgres::PoolConfig;
use serde::Deserialize;

use super::invalid;

const CONFIG_FILE: &str = "RFC_SERVE_CONFIG_FILE";
const CONFIG_BYTES: u64 = 16 * 1024;

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct FileSettings {
    pub(super) client_ip_header: Option<String>,
    pub(super) client_ip_header_mode: Option<String>,
    database: Option<DatabaseSettings>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatabaseSettings {
    pool: Option<PoolSettings>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PoolSettings {
    min_idle: Option<usize>,
    max_connections: Option<usize>,
    acquire_timeout_ms: Option<u64>,
    idle_timeout_seconds: Option<u64>,
    max_lifetime_seconds: Option<u64>,
    shutdown_timeout_ms: Option<u64>,
}

impl PoolSettings {
    fn load(self) -> io::Result<PoolConfig> {
        let defaults = PoolConfig::default();
        let file = PoolConfig {
            min_idle: self.min_idle.unwrap_or(defaults.min_idle),
            max_connections: self.max_connections.unwrap_or(defaults.max_connections),
            acquire_timeout_ms: self
                .acquire_timeout_ms
                .unwrap_or(defaults.acquire_timeout_ms),
            idle_timeout_seconds: self
                .idle_timeout_seconds
                .unwrap_or(defaults.idle_timeout_seconds),
            max_lifetime_seconds: self
                .max_lifetime_seconds
                .unwrap_or(defaults.max_lifetime_seconds),
            shutdown_timeout_ms: self
                .shutdown_timeout_ms
                .unwrap_or(defaults.shutdown_timeout_ms),
        }
        .validate()?;
        PoolConfig {
            min_idle: override_value("RFC_MCP_DB_POOL_MIN_IDLE", file.min_idle)?,
            max_connections: override_value(
                "RFC_MCP_DB_POOL_MAX_CONNECTIONS",
                file.max_connections,
            )?,
            acquire_timeout_ms: override_value(
                "RFC_MCP_DB_POOL_ACQUIRE_TIMEOUT_MS",
                file.acquire_timeout_ms,
            )?,
            idle_timeout_seconds: override_value(
                "RFC_MCP_DB_POOL_IDLE_TIMEOUT_SECONDS",
                file.idle_timeout_seconds,
            )?,
            max_lifetime_seconds: override_value(
                "RFC_MCP_DB_POOL_MAX_LIFETIME_SECONDS",
                file.max_lifetime_seconds,
            )?,
            shutdown_timeout_ms: override_value(
                "RFC_MCP_DB_POOL_SHUTDOWN_TIMEOUT_MS",
                file.shutdown_timeout_ms,
            )?,
        }
        .validate()
    }
}

fn override_value<T: str::FromStr>(name: &str, fallback: T) -> io::Result<T> {
    optional_env(name)?.map_or(Ok(fallback), |value| {
        value.parse().map_err(|_| invalid(name))
    })
}

pub(super) fn load() -> io::Result<(FileSettings, PoolConfig)> {
    let mut file = optional_env(CONFIG_FILE)?
        .as_deref()
        .map(Path::new)
        .map(read_settings)
        .transpose()?
        .unwrap_or_default();
    let pool = file
        .database
        .take()
        .and_then(|database| database.pool)
        .unwrap_or_default()
        .load()?;
    Ok((file, pool))
}

pub(super) fn optional_env(name: &str) -> io::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(invalid(name)),
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
    use super::*;

    #[test]
    fn pool_table_is_optional_and_strict() {
        assert!(read_settings(Path::new("")).is_err());
        assert!(read_settings(Path::new(".")).is_err());
        let settings: FileSettings =
            toml::from_str("[database.pool]\nmax_connections = 5\nmin_idle = 0").unwrap();
        let pool = settings.database.unwrap().pool.unwrap();
        assert_eq!(pool.max_connections, Some(5));
        assert_eq!(pool.min_idle, Some(0));
        assert!(
            toml::from_str::<FileSettings>("[database.pool]\nverification_interval_seconds = 60",)
                .is_err()
        );
    }
}
