//! Versioned discovery settings and startup resource checks.

use crate::{
    parse::DOCUMENT_BYTES,
    upstream::{CATALOG_LIMIT, TXT_LIMIT, XML_LIMIT},
};
use rfc_storage::postgres::Result;
use serde::{Deserialize, Serialize};
use std::{
    env, fs,
    io::{self, Read},
    os::unix::fs::OpenOptionsExt,
    path::{Component, Path},
};

const CONFIG_FILE: &str = "RFC_SYNC_CONFIG_FILE";
pub(crate) const CONFIG_BYTES: u64 = 16 * 1024;
const MIB: u64 = 1024 * 1024;
const MEMORY_LIMIT: u64 = 2048 * MIB;
const SCRATCH_LIMIT: u64 = 512 * MIB;
const SCRATCH_REQUIRED: u64 = 8 * MIB;
const MIN_HTTPS_INTERVAL_MS: u64 = 1000;
pub(crate) const RAW_PER_DOCUMENT: u64 = (2 * TXT_LIMIT + XML_LIMIT) as u64;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SyncConfig {
    schema_version: u32,
    #[serde(default)]
    pub ingestion: Ingestion,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Ingestion {
    pub target_bootstrap_seconds: u64,
    pub max_accepted_bytes: u64,
    pub discovery: Discovery,
    pub https: Https,
    pub pipeline: Pipeline,
    pub revalidation: Revalidation,
    pub database: Database,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Discovery {
    pub inventory_timeout_seconds: u64,
    pub full_reconcile_interval_seconds: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Https {
    pub max_in_flight_requests: usize,
    pub min_request_start_interval_ms: u64,
    pub request_timeout_seconds: u64,
    pub max_attempts: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Pipeline {
    pub txt_queue_items: u64,
    pub xml_queue_items: u64,
    pub result_queue_items: u64,
    pub max_active_documents: u64,
    pub parser_workers: u64,
    pub raw_payload_budget_bytes: u64,
    pub parse_budget_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Revalidation {
    pub max_rsync_sessions: u64,
    pub batch_size: u64,
    pub session_timeout_seconds: u64,
    pub interval_seconds: u64,
    pub max_attempts: usize,
    pub retry_backoff_seconds: Vec<u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Database {
    pub batch_size: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            schema_version: 2,
            ingestion: Ingestion::default(),
        }
    }
}

impl Default for Ingestion {
    fn default() -> Self {
        Self {
            target_bootstrap_seconds: 900,
            max_accepted_bytes: 4096 * MIB,
            discovery: Discovery::default(),
            https: Https::default(),
            pipeline: Pipeline::default(),
            revalidation: Revalidation::default(),
            database: Database::default(),
        }
    }
}

impl Default for Discovery {
    fn default() -> Self {
        Self {
            inventory_timeout_seconds: 120,
            full_reconcile_interval_seconds: 86_400,
        }
    }
}

impl Default for Https {
    fn default() -> Self {
        Self {
            max_in_flight_requests: 2,
            min_request_start_interval_ms: MIN_HTTPS_INTERVAL_MS,
            request_timeout_seconds: 30,
            max_attempts: 3,
        }
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self {
            txt_queue_items: 64,
            xml_queue_items: 64,
            result_queue_items: 32,
            max_active_documents: 3,
            parser_workers: 2,
            raw_payload_budget_bytes: 48 * MIB,
            parse_budget_bytes: 384 * MIB,
        }
    }
}

impl Default for Revalidation {
    fn default() -> Self {
        Self {
            max_rsync_sessions: 1,
            batch_size: 512,
            session_timeout_seconds: 120,
            interval_seconds: 86_400,
            max_attempts: 3,
            retry_backoff_seconds: vec![5, 30],
        }
    }
}

impl Default for Database {
    fn default() -> Self {
        Self { batch_size: 32 }
    }
}

#[derive(Serialize)]
struct Summary {
    source: &'static str,
    #[serde(flatten)]
    config: SyncConfig,
    phase: &'static str,
    publication_gate: &'static str,
    pending_consumers: [&'static str; 0],
    informational: [&'static str; 1],
    resources: Resources,
}

#[derive(Debug, Serialize)]
struct Resources {
    scratch_required_bytes: u64,
    scratch_available_bytes: u64,
    memory_required_bytes: u64,
    memory_available_bytes: u64,
    cgroup_memory_limit_bytes: Option<u64>,
    physical_memory_bytes: u64,
}

pub fn check_config() -> Result<()> {
    #[cfg(feature = "phase6-fixtures")]
    if crate::daemon::legacy_fixtures() {
        return crate::legacy_config::check_config();
    }
    let summary = SyncConfig::load_summary()?;
    let (spool_available_bytes, spool_available_inodes) =
        crate::egress::preflight(summary.config.ingestion.max_accepted_bytes)?;
    let mut output = serde_json::to_value(summary)?;
    output["resources"]["spool_available_bytes"] = spool_available_bytes.into();
    output["resources"]["spool_available_inodes"] = spool_available_inodes.into();
    output["resources"]["audit"] = serde_json::to_value(crate::audit::preflight()?)?;
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

impl SyncConfig {
    pub(crate) fn load() -> Result<Self> {
        Ok(Self::load_summary()?.config)
    }

    fn load_summary() -> Result<Summary> {
        for name in ["RFC_SYNC_HTTP_CONCURRENCY", "RFC_SYNC_PARSE_CONCURRENCY"] {
            if env::var_os(name).is_some() {
                return Err(
                    format!("{name} was removed; migrate to the schema v2 ingestion TOML").into(),
                );
            }
        }
        let (config, source) = match env::var_os(CONFIG_FILE) {
            Some(path) => (Self::from_toml(&config_text(Path::new(&path))?)?, "file"),
            None => (Self::default(), "defaults"),
        };
        config.validate()?;
        let scratch = scratch_stats().ok_or("cannot inspect ingestion scratch filesystem")?;
        let scratch_available = scratch
            .f_bavail
            .checked_mul(scratch.f_frsize)
            .ok_or("ingestion scratch capacity overflows")?;
        let resources =
            config.resources(scratch_available, physical_memory()?, cgroup_memory()?)?;
        Ok(Summary {
            source,
            config,
            phase: "audit",
            publication_gate: crate::discovery::PUBLICATION_GATE,
            pending_consumers: [],
            informational: ["ingestion.target_bootstrap_seconds"],
            resources,
        })
    }

    fn from_toml(input: &str) -> Result<Self> {
        // The bounded file may contain secrets in unsolicited keys. Never quote it.
        let table: toml::Table = toml::from_str(input).map_err(toml_error)?;
        match table
            .get("schema_version")
            .and_then(toml::Value::as_integer)
        {
            Some(1) => return Err(migration_hint()),
            Some(2) => (),
            _ => return Err("ingestion schema_version must be 2".into()),
        }
        if let Some(ingestion) = table.get("ingestion").and_then(toml::Value::as_table) {
            if ["txt_provenance", "rsync"]
                .iter()
                .any(|key| ingestion.contains_key(*key))
            {
                return Err(migration_hint());
            }
            for (section, replaced) in [
                ("https", "rfcxml_max_attempts"),
                ("pipeline", "max_pending_documents"),
            ] {
                if ingestion
                    .get(section)
                    .and_then(toml::Value::as_table)
                    .is_some_and(|section| section.contains_key(replaced))
                {
                    return Err(migration_hint());
                }
            }
        }
        table.try_into().map_err(toml_error)
    }

    fn validate(&self) -> Result<()> {
        let i = &self.ingestion;
        let p = &i.pipeline;
        let r = &i.revalidation;
        for (key, value, min, max) in [
            (
                "target_bootstrap_seconds",
                i.target_bootstrap_seconds,
                1,
                43_200,
            ),
            ("max_accepted_bytes", i.max_accepted_bytes, 1, 4096 * MIB),
            (
                "inventory_timeout_seconds",
                i.discovery.inventory_timeout_seconds,
                5,
                120,
            ),
            (
                "full_reconcile_interval_seconds",
                i.discovery.full_reconcile_interval_seconds,
                3600,
                604_800,
            ),
            (
                "max_in_flight_requests",
                i.https.max_in_flight_requests as u64,
                1,
                2,
            ),
            (
                "min_request_start_interval_ms",
                i.https.min_request_start_interval_ms,
                MIN_HTTPS_INTERVAL_MS,
                60_000,
            ),
            (
                "request_timeout_seconds",
                i.https.request_timeout_seconds,
                5,
                30,
            ),
            ("https.max_attempts", u64::from(i.https.max_attempts), 1, 3),
            ("max_active_documents", p.max_active_documents, 1, 3),
            ("parser_workers", p.parser_workers, 1, 2),
            ("txt_queue_items", p.txt_queue_items, 1, 64),
            ("xml_queue_items", p.xml_queue_items, 1, 64),
            ("result_queue_items", p.result_queue_items, 1, 32),
            (
                "raw_payload_budget_bytes",
                p.raw_payload_budget_bytes,
                1,
                48 * MIB,
            ),
            ("parse_budget_bytes", p.parse_budget_bytes, 1, 384 * MIB),
            ("max_rsync_sessions", r.max_rsync_sessions, 1, 1),
            ("revalidation.batch_size", r.batch_size, 1, 512),
            ("session_timeout_seconds", r.session_timeout_seconds, 5, 120),
            (
                "revalidation.interval_seconds",
                r.interval_seconds,
                3600,
                604_800,
            ),
            ("revalidation.max_attempts", r.max_attempts as u64, 1, 3),
            ("database.batch_size", i.database.batch_size, 1, 32),
        ] {
            if !(min..=max).contains(&value) {
                return Err(format!("{key} must be {min} through {max}").into());
            }
        }
        if (i.https.max_in_flight_requests as u64).max(p.parser_workers) > p.max_active_documents
            || p.txt_queue_items
                .min(p.xml_queue_items)
                .min(p.result_queue_items)
                < p.max_active_documents
        {
            return Err(
                "HTTPS/parser concurrency and queue capacity must fit the active document window"
                    .into(),
            );
        }
        if p.max_active_documents
            .checked_mul(RAW_PER_DOCUMENT)
            .is_none_or(|bytes| bytes > p.raw_payload_budget_bytes)
            || p.max_active_documents
                .checked_mul(DOCUMENT_BYTES as u64)
                .is_none_or(|bytes| bytes > p.parse_budget_bytes)
        {
            return Err("raw and parse budgets must cover the active document window".into());
        }
        if r.retry_backoff_seconds.len() != r.max_attempts - 1
            || r.retry_backoff_seconds
                .iter()
                .any(|value| !(1..=120).contains(value))
            || !r.retry_backoff_seconds.is_sorted_by(|a, b| a < b)
        {
            return Err(
                "revalidation requires one increasing backoff of 1 through 120 seconds per retry"
                    .into(),
            );
        }
        Ok(())
    }

    fn resources(&self, scratch: u64, physical: u64, cgroup: Option<u64>) -> Result<Resources> {
        let p = &self.ingestion.pipeline;
        // Include the existing base working reserve, two catalog bodies, inventory
        // output, and compact descriptors outside the active payload window.
        let descriptors = p
            .txt_queue_items
            .checked_add(p.xml_queue_items)
            .and_then(|count| count.checked_add(p.result_queue_items))
            .and_then(|count| count.checked_mul(512));
        let memory_required = descriptors
            .and_then(|bytes| {
                bytes.checked_add(512 * MIB + SCRATCH_REQUIRED + 2 * CATALOG_LIMIT as u64)
            })
            .and_then(|bytes| bytes.checked_add(crate::inventory::OUTPUT_RESERVATION))
            .and_then(|bytes| bytes.checked_add(crate::audit::MEMORY_RESERVATION))
            .and_then(|bytes| bytes.checked_add(p.raw_payload_budget_bytes))
            .and_then(|bytes| bytes.checked_add(p.parse_budget_bytes))
            .ok_or("ingestion memory reservation overflows")?;
        let scratch_available = scratch.min(SCRATCH_LIMIT);
        let memory_available = physical
            .min(MEMORY_LIMIT)
            .min(cgroup.unwrap_or(MEMORY_LIMIT));
        if SCRATCH_REQUIRED > scratch_available {
            return Err(format!(
                "ingestion scratch needs {SCRATCH_REQUIRED} bytes, available {scratch_available}"
            )
            .into());
        }
        if memory_required > memory_available {
            return Err(format!(
                "ingestion memory needs {memory_required} bytes, available {memory_available}"
            )
            .into());
        }
        Ok(Resources {
            scratch_required_bytes: SCRATCH_REQUIRED,
            scratch_available_bytes: scratch_available,
            memory_required_bytes: memory_required,
            memory_available_bytes: memory_available,
            cgroup_memory_limit_bytes: cgroup,
            physical_memory_bytes: physical,
        })
    }
}

fn migration_hint() -> Box<dyn std::error::Error + Send + Sync> {
    "legacy ingestion configuration; migrate to schema v2 and remove TXT provenance and rsync acquisition settings (see deploy/ingestion.toml.example)".into()
}

fn toml_error(error: toml::de::Error) -> Box<dyn std::error::Error + Send + Sync> {
    match error.span() {
        Some(span) => format!("invalid ingestion TOML at byte {}", span.start).into(),
        None => "invalid ingestion TOML".into(),
    }
}

pub(crate) fn config_text(path: &Path) -> Result<String> {
    if path.as_os_str().is_empty() {
        return Err("RFC_SYNC_CONFIG_FILE must be nonempty".into());
    }
    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| "cannot open ingestion config file")?;
    let metadata = file
        .metadata()
        .map_err(|_| "cannot inspect ingestion config file")?;
    if !metadata.is_file() {
        return Err("ingestion config must be a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| "cannot read ingestion config file")?;
    if bytes.len() as u64 > CONFIG_BYTES {
        return Err("ingestion config exceeds 16 KiB".into());
    }
    String::from_utf8(bytes).map_err(|_| "ingestion config must be UTF-8".into())
}

pub(crate) fn scratch_stats() -> Option<libc::statvfs> {
    let mut stats = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(c"/tmp".as_ptr(), stats.as_mut_ptr()) } != 0 {
        return None;
    }
    Some(unsafe { stats.assume_init() })
}

pub(crate) fn physical_memory() -> Result<u64> {
    let info = fs::read_to_string("/proc/meminfo").map_err(|_| "cannot inspect physical memory")?;
    let kib = info
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|line| line.split_ascii_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or("invalid MemTotal in /proc/meminfo")?;
    kib.checked_mul(1024)
        .ok_or_else(|| "physical memory size overflows".into())
}

pub(crate) fn cgroup_memory() -> Result<Option<u64>> {
    let membership =
        fs::read_to_string("/proc/self/cgroup").map_err(|_| "cannot inspect cgroup membership")?;
    let Some(cgroup) = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::/"))
    else {
        return Ok(None);
    };
    cgroup_limit(Path::new("/sys/fs/cgroup"), Path::new(cgroup))
}

pub(crate) fn cgroup_limit(root: &Path, cgroup: &Path) -> Result<Option<u64>> {
    if cgroup
        .components()
        .any(|part| !matches!(part, Component::Normal(_)))
    {
        return Err("invalid cgroup membership path".into());
    }
    let mut limit = None;
    for group in cgroup.ancestors() {
        match fs::read_to_string(root.join(group).join("memory.max")) {
            Ok(value) if value.trim() == "max" => (),
            Ok(value) => {
                let bytes = value
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| "invalid cgroup memory limit")?;
                limit = Some(limit.map_or(bytes, |previous: u64| previous.min(bytes)));
            }
            // Root cgroups and groups without the memory controller may lack this file.
            Err(error) if error.kind() == io::ErrorKind::NotFound => (),
            Err(_) => return Err("cannot read cgroup memory limit".into()),
        }
    }
    Ok(limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_example_matches_defaults() {
        let config =
            SyncConfig::from_toml(include_str!("../../../deploy/ingestion.toml.example")).unwrap();
        assert_eq!(config, SyncConfig::default());
        assert_eq!(config, SyncConfig::from_toml("schema_version = 2").unwrap());
        config.validate().unwrap();
        assert!(
            config
                .resources(SCRATCH_LIMIT, MEMORY_LIMIT, Some(MEMORY_LIMIT))
                .is_ok()
        );
        assert!(
            config
                .resources(SCRATCH_REQUIRED - 1, MEMORY_LIMIT, None)
                .is_err()
        );
        assert!(
            config
                .resources(SCRATCH_LIMIT, MEMORY_LIMIT, Some(768 * MIB))
                .is_err()
        );
    }

    #[test]
    fn recognizes_migration_errors_without_echoing_configuration() {
        let legacy = include_str!("../../../fixtures/synthetic/ingestion-v1.toml");
        for input in [
            legacy,
            "schema_version = 2\n[ingestion]\ntxt_provenance = 'secret'",
            "schema_version = 2\n[ingestion.rsync]\nmode = 'secret'",
        ] {
            let error = SyncConfig::from_toml(input).unwrap_err().to_string();
            assert!(error.contains("migrate to schema v2"));
            assert!(!error.contains("secret"));
        }
        for invalid in [
            "schema_version = 3",
            "schema_version = 2\n[ingestion.discovery]\nfeed_format = 'atom'",
            "schema_version = 2\n[ingestion.revalidation]\nchecksum_choice = 'none'",
            "schema_version = 2\n[ingestion.https]\nurl = 'secret'",
            "schema_version = 2\n[ingestion.https]\nmax_in_flight_requests = -1",
        ] {
            assert!(
                !SyncConfig::from_toml(invalid)
                    .unwrap_err()
                    .to_string()
                    .contains("secret")
            );
        }
    }

    #[test]
    fn enforces_capacity_coupling_and_retry_budget() {
        for invalid in [
            "[ingestion.https]\nmin_request_start_interval_ms = 999",
            "[ingestion.pipeline]\nmax_active_documents = 1",
            "[ingestion.pipeline]\nresult_queue_items = 2",
            "[ingestion.pipeline]\nraw_payload_budget_bytes = 50331647",
            "[ingestion.pipeline]\nparse_budget_bytes = 402653183",
            "[ingestion.revalidation]\nmax_rsync_sessions = 2",
            "[ingestion.revalidation]\nmax_attempts = 2",
            "[ingestion.revalidation]\nretry_backoff_seconds = [30,5]",
            "[ingestion.database]\nbatch_size = 33",
        ] {
            assert!(
                SyncConfig::from_toml(&format!("schema_version = 2\n{invalid}"))
                    .unwrap()
                    .validate()
                    .is_err()
            );
        }
        let small = SyncConfig::from_toml("schema_version = 2\n[ingestion.https]\nmax_in_flight_requests = 1\n[ingestion.pipeline]\nmax_active_documents = 1\nparser_workers = 1\nraw_payload_budget_bytes = 16777216\nparse_budget_bytes = 134217728\n[ingestion.revalidation]\nmax_attempts = 1\nretry_backoff_seconds = []").unwrap();
        small.validate().unwrap();
        assert!(
            small
                .resources(16 * MIB, MEMORY_LIMIT, Some(800 * MIB))
                .is_ok()
        );
    }
}
