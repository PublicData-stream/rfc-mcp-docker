//! Versioned ingestion settings and startup resource checks.

use crate::{
    bulk,
    config::{cgroup_memory, config_text, physical_memory, scratch_stats},
    parse::DOCUMENT_BYTES,
    upstream::{TXT_LIMIT, XML_LIMIT},
};
use rfc_storage::postgres::Result;
use serde::{Deserialize, Serialize};
#[cfg(test)]
use std::fs;
use std::{env, path::Path};

const CONFIG_FILE: &str = "RFC_SYNC_CONFIG_FILE";
const LEGACY_HTTP: &str = "RFC_SYNC_HTTP_CONCURRENCY";
const LEGACY_PARSE: &str = "RFC_SYNC_PARSE_CONCURRENCY";
const MIB: u64 = 1024 * 1024;
const SCRATCH_LIMIT: u64 = 512 * MIB;
const MEMORY_LIMIT: u64 = 2 * 1024 * MIB;
const ACCEPTED_LIMIT: u64 = 4 * 1024 * MIB;
const MIN_HTTPS_INTERVAL_MS: u64 = 1000;
const MAX_RSYNC_BATCH: usize = 32;
const MAX_PENDING_DOCUMENTS: usize = 3;
pub(crate) const RAW_PER_DOCUMENT: usize = 2 * TXT_LIMIT + XML_LIMIT;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SyncConfig {
    schema_version: u32,
    #[serde(default)]
    pub(crate) ingestion: Ingestion,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Ingestion {
    pub(crate) txt_provenance: TxtProvenance,
    pub(crate) https: Https,
    pub(crate) rsync: Rsync,
    pub(crate) pipeline: Pipeline,
    pub(crate) max_accepted_bytes: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TxtProvenance {
    DualSource,
    RsyncBootstrap,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Https {
    pub(crate) max_in_flight_requests: usize,
    pub(crate) min_request_start_interval_ms: u64,
    pub(crate) request_timeout_seconds: u64,
    pub(crate) rfcxml_max_attempts: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Rsync {
    pub(crate) mode: RsyncMode,
    pub(crate) batch_size: usize,
    pub(crate) transfer_timeout_seconds: u64,
    pub(crate) bulk_transfer_timeout_seconds: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RsyncMode {
    Batched,
    PersistentBulk,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Pipeline {
    pub(crate) max_pending_documents: usize,
    pub(crate) parser_workers: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            schema_version: 1,
            ingestion: Ingestion::default(),
        }
    }
}

impl Default for Ingestion {
    fn default() -> Self {
        Self {
            txt_provenance: TxtProvenance::DualSource,
            https: Https::default(),
            rsync: Rsync::default(),
            pipeline: Pipeline::default(),
            max_accepted_bytes: ACCEPTED_LIMIT,
        }
    }
}

impl Default for Https {
    fn default() -> Self {
        Self {
            max_in_flight_requests: 2,
            min_request_start_interval_ms: MIN_HTTPS_INTERVAL_MS,
            request_timeout_seconds: 30,
            rfcxml_max_attempts: 3,
        }
    }
}

impl Default for Rsync {
    fn default() -> Self {
        Self {
            mode: RsyncMode::Batched,
            batch_size: MAX_RSYNC_BATCH,
            transfer_timeout_seconds: 120,
            bulk_transfer_timeout_seconds: 900,
        }
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self {
            max_pending_documents: MAX_PENDING_DOCUMENTS,
            parser_workers: 1,
        }
    }
}

#[derive(Serialize)]
struct Summary {
    source: &'static str,
    #[serde(flatten)]
    config: SyncConfig,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    bulk_filesystem: Option<bulk::Capacity>,
}

/// Validates the same settings the daemon uses without opening its runtime services.
pub fn check_config() -> Result<()> {
    println!("{}", serde_json::to_string(&SyncConfig::load_summary()?)?);
    Ok(())
}

impl SyncConfig {
    pub(crate) fn load() -> Result<Self> {
        Ok(Self::load_summary()?.config)
    }

    fn load_summary() -> Result<Summary> {
        let path = env::var_os(CONFIG_FILE);
        let http = env::var_os(LEGACY_HTTP);
        let parse = env::var_os(LEGACY_PARSE);
        let (config, source) = if let Some(path) = path {
            if http.is_some() || parse.is_some() {
                return Err(
                    "ingestion config file conflicts with legacy concurrency variables".into(),
                );
            }
            (Self::from_file(Path::new(&path))?, "file")
        } else {
            let mut config = Self::default();
            let from_env = http.is_some() || parse.is_some();
            if let Some(value) = http {
                config.ingestion.https.max_in_flight_requests = legacy(LEGACY_HTTP, &value)?;
            }
            if let Some(value) = parse {
                config.ingestion.pipeline.parser_workers = legacy(LEGACY_PARSE, &value)?;
            }
            (config, if from_env { "legacy_env" } else { "defaults" })
        };
        config.validate()?;
        let scratch = scratch_stats().ok_or("cannot inspect ingestion scratch filesystem")?;
        let scratch_available = scratch
            .f_bavail
            .checked_mul(scratch.f_frsize)
            .ok_or("ingestion scratch capacity overflows")?;
        let mut resources =
            config.resources(scratch_available, physical_memory()?, cgroup_memory()?)?;
        if config.ingestion.rsync.mode == RsyncMode::PersistentBulk {
            resources.bulk_filesystem = Some(bulk::preflight()?);
        }
        Ok(Summary {
            source,
            config,
            resources,
        })
    }

    fn from_file(path: &Path) -> Result<Self> {
        Self::from_toml(&config_text(path)?)
    }

    fn from_toml(input: &str) -> Result<Self> {
        // TOML diagnostics can quote operator data, including unknown keys.
        toml::from_str(input).map_err(|error| match error.span() {
            Some(span) => format!("invalid ingestion TOML at byte {}", span.start).into(),
            None => "invalid ingestion TOML".into(),
        })
    }

    fn validate(&self) -> Result<()> {
        let Ingestion {
            txt_provenance,
            https,
            rsync,
            pipeline,
            max_accepted_bytes,
        } = self.ingestion;
        if self.schema_version != 1 {
            return Err("ingestion schema_version must be 1".into());
        }
        if !(1..=2).contains(&https.max_in_flight_requests) {
            return Err("max_in_flight_requests must be 1 or 2".into());
        }
        if !(MIN_HTTPS_INTERVAL_MS..=60_000).contains(&https.min_request_start_interval_ms) {
            return Err("min_request_start_interval_ms must be 1000 through 60000".into());
        }
        if !(5..=30).contains(&https.request_timeout_seconds) {
            return Err("request_timeout_seconds must be 5 through 30".into());
        }
        if !(1..=3).contains(&https.rfcxml_max_attempts) {
            return Err("rfcxml_max_attempts must be 1 through 3".into());
        }
        if !(1..=MAX_RSYNC_BATCH).contains(&rsync.batch_size) {
            return Err("batch_size must be 1 through 32".into());
        }
        if !(30..=120).contains(&rsync.transfer_timeout_seconds) {
            return Err("transfer_timeout_seconds must be 30 through 120".into());
        }
        if !(30..=900).contains(&rsync.bulk_transfer_timeout_seconds) {
            return Err("bulk_transfer_timeout_seconds must be 30 through 900".into());
        }
        if txt_provenance == TxtProvenance::RsyncBootstrap
            && rsync.mode != RsyncMode::PersistentBulk
        {
            return Err("rsync_bootstrap requires persistent_bulk rsync mode".into());
        }
        if !(1..=MAX_PENDING_DOCUMENTS).contains(&pipeline.max_pending_documents) {
            return Err("max_pending_documents must be 1 through 3".into());
        }
        if !(1..=2).contains(&pipeline.parser_workers) {
            return Err("parser_workers must be 1 or 2".into());
        }
        if https.max_in_flight_requests > pipeline.max_pending_documents
            || pipeline.parser_workers > pipeline.max_pending_documents
        {
            return Err("HTTPS and parser concurrency must fit max_pending_documents".into());
        }
        if !(1..=ACCEPTED_LIMIT).contains(&max_accepted_bytes) {
            return Err("max_accepted_bytes must be 1 through 4294967296".into());
        }
        Ok(())
    }

    fn resources(&self, scratch: u64, physical: u64, cgroup: Option<u64>) -> Result<Resources> {
        let batch_size = match self.ingestion.rsync.mode {
            RsyncMode::Batched => self.ingestion.rsync.batch_size as u64,
            RsyncMode::PersistentBulk => 0,
        };
        let scratch_required = batch_size
            .checked_mul(TXT_LIMIT as u64)
            .and_then(|bytes| bytes.checked_add(8 * MIB))
            .ok_or("ingestion scratch reservation overflows")?;
        let memory_required = (self.ingestion.pipeline.max_pending_documents as u64)
            .checked_mul((RAW_PER_DOCUMENT + DOCUMENT_BYTES) as u64)
            .and_then(|bytes| bytes.checked_add(scratch_required))
            .and_then(|bytes| bytes.checked_add(512 * MIB))
            .ok_or("ingestion memory reservation overflows")?;
        let scratch_available = scratch.min(SCRATCH_LIMIT);
        let memory_available = physical
            .min(MEMORY_LIMIT)
            .min(cgroup.unwrap_or(MEMORY_LIMIT));
        if scratch_required > scratch_available {
            return Err(format!(
                "ingestion scratch needs {scratch_required} bytes, available {scratch_available}"
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
            scratch_required_bytes: scratch_required,
            scratch_available_bytes: scratch_available,
            memory_required_bytes: memory_required,
            memory_available_bytes: memory_available,
            cgroup_memory_limit_bytes: cgroup,
            physical_memory_bytes: physical,
            bulk_filesystem: None,
        })
    }
}

fn legacy(name: &str, value: &std::ffi::OsStr) -> Result<usize> {
    match value.to_str() {
        Some("1") => Ok(1),
        Some("2") => Ok(2),
        _ => Err(format!("{name} must be 1 or 2").into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CONFIG_BYTES, cgroup_limit};

    #[test]
    fn example_and_omitted_settings_match_defaults() {
        for input in [
            include_str!("../../../fixtures/synthetic/ingestion-v1.toml"),
            "schema_version = 1",
            "schema_version = 1\n[ingestion.https]\n[ingestion.rsync]\n[ingestion.pipeline]\n",
        ] {
            let config = SyncConfig::from_toml(input).unwrap();
            config.validate().unwrap();
            assert_eq!(config, SyncConfig::default());
        }
    }

    #[test]
    fn rejects_invalid_documents_and_settings() {
        for input in [
            "",
            "schema_version =",
            "schema_version = 2",
            "schema_version = 1\nschema_version = 1",
            "schema_version = 1\nurl = 'sentinel'",
            "schema_version = 1\n[ingestion]\ntxt_provenance = 'rsync_authoritative'",
            "schema_version = 1\n[ingestion.rsync]\ncommand = 'sentinel'",
            "schema_version = 1\n[ingestion.pipeline]\nmax_pending_documents = 1",
            "schema_version = 1\n[ingestion.pipeline]\nparser_workers = 0",
            "schema_version = 1\n[ingestion.https]\nmax_in_flight_requests = -1",
            "schema_version = 1\n[ingestion.https]\nmin_request_start_interval_ms = 999",
            "schema_version = 1\n[ingestion.https]\nrequest_timeout_seconds = 31",
            "schema_version = 1\n[ingestion.https]\nrfcxml_max_attempts = 0",
            "schema_version = 1\n[ingestion.https]\nrfcxml_max_attempts = 4",
            "schema_version = 1\n[ingestion.rsync]\nbatch_size = 33",
            "schema_version = 1\n[ingestion.rsync]\ntransfer_timeout_seconds = 29",
            "schema_version = 1\n[ingestion]\nmax_accepted_bytes = 4294967297",
            "schema_version = 1\n[ingestion]\nmax_accepted_bytes = 18446744073709551616",
        ] {
            let error = SyncConfig::from_toml(input)
                .and_then(|config| config.validate())
                .unwrap_err();
            assert!(!error.to_string().contains("sentinel"));
        }
    }

    #[test]
    fn rejects_nonregular_and_oversized_files() {
        let dir = tempfile::tempdir_in("/tmp").unwrap();
        assert!(SyncConfig::from_file(dir.path()).is_err());
        let file = dir.path().join("config.toml");
        fs::write(&file, vec![b' '; CONFIG_BYTES as usize + 1]).unwrap();
        assert!(
            SyncConfig::from_file(&file)
                .unwrap_err()
                .to_string()
                .contains("16 KiB")
        );
        fs::write(&file, [0xff]).unwrap();
        assert!(
            SyncConfig::from_file(&file)
                .unwrap_err()
                .to_string()
                .contains("UTF-8")
        );
    }

    #[test]
    fn validates_scratch_and_cgroup_capacity() {
        let default = SyncConfig::default();
        let resources = default
            .resources(SCRATCH_LIMIT, MEMORY_LIMIT, None)
            .unwrap();
        assert_eq!(resources.scratch_required_bytes, 136 * MIB);
        assert_eq!(resources.memory_required_bytes, 1080 * MIB);
        assert!(default.resources(135 * MIB, MEMORY_LIMIT, None).is_err());
        assert!(
            default
                .resources(SCRATCH_LIMIT, MEMORY_LIMIT, Some(1024 * MIB))
                .is_err()
        );

        let small = SyncConfig::from_toml("schema_version = 1\n[ingestion.https]\nmax_in_flight_requests = 1\n[ingestion.rsync]\nbatch_size = 1\n[ingestion.pipeline]\nmax_pending_documents = 1").unwrap();
        small.validate().unwrap();
        let resources = small
            .resources(16 * MIB, MEMORY_LIMIT, Some(768 * MIB))
            .unwrap();
        assert_eq!(resources.memory_available_bytes, 768 * MIB);
        assert_eq!(resources.memory_required_bytes, 668 * MIB);
    }

    #[test]
    fn uses_tightest_visible_cgroup_ancestor() {
        let dir = tempfile::tempdir_in("/tmp").unwrap();
        fs::create_dir_all(dir.path().join("slice/container")).unwrap();
        fs::write(dir.path().join("slice/memory.max"), "805306368\n").unwrap();
        let leaf = dir.path().join("slice/container/memory.max");
        fs::write(&leaf, "max\n").unwrap();
        assert_eq!(
            cgroup_limit(dir.path(), Path::new("slice/container")).unwrap(),
            Some(768 * MIB)
        );
        fs::write(&leaf, "536870912\n").unwrap();
        assert_eq!(
            cgroup_limit(dir.path(), Path::new("slice/container")).unwrap(),
            Some(512 * MIB)
        );
        fs::write(leaf, "invalid").unwrap();
        assert!(cgroup_limit(dir.path(), Path::new("slice/container")).is_err());
    }
}
