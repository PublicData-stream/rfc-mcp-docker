//! Safe diagnostics shared by structured logs, job records, and operators.

use crate::{Scope, inventory};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fmt,
    io::{self, Write},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type Error = dyn std::error::Error + Send + Sync;

#[derive(Debug)]
pub(crate) struct CapacityPressure(pub String);

impl fmt::Display for CapacityPressure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for CapacityPressure {}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Preflight,
    Recovery,
    Catalog,
    Inventory,
    Acquisition,
    CatalogFence,
    CandidateValidation,
    Publication,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Code {
    Cancelled,
    DeadlineExceeded,
    StartupPrerequisite,
    RecoveryFailed,
    CatalogFailed,
    CatalogChanged,
    InventoryFailed,
    InvalidInventory,
    AwaitingCatalog,
    AwaitingMirror,
    HttpsCooldown,
    HttpsTimeout,
    HttpsTransport,
    HttpsStatus,
    AcquisitionFailed,
    AuditFailed,
    RepairFailed,
    CandidateFailed,
    PublicationFailed,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Diagnostic {
    pub code: Code,
    phase: Phase,
    #[serde(skip_serializing_if = "Option::is_none")]
    http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inventory_reason: Option<inventory::Rejection>,
    #[serde(skip_serializing_if = "Option::is_none")]
    inventory_line: Option<usize>,
}

impl Diagnostic {
    pub fn new(code: Code, phase: Phase) -> Self {
        Self {
            code,
            phase,
            http_status: None,
            inventory_reason: None,
            inventory_line: None,
        }
    }

    pub fn failure(phase: Phase, error: &Error) -> Self {
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error);
        let mut context = None;
        while let Some(error) = cause {
            if let Some(error) = error.downcast_ref::<inventory::ParseError>() {
                return Self {
                    inventory_reason: Some(error.reason),
                    inventory_line: Some(error.line),
                    ..Self::new(Code::InvalidInventory, phase)
                };
            }
            if let Some(error) = error.downcast_ref::<Failure>() {
                match error.code {
                    Code::AuditFailed => {
                        context.get_or_insert(error.code);
                    }
                    Code::RepairFailed => context = Some(error.code),
                    code => {
                        return Self {
                            http_status: error.http_status,
                            ..Self::new(code, phase)
                        };
                    }
                }
            }
            cause = error.source();
        }
        Self::new(
            context.unwrap_or(match phase {
                Phase::Preflight => Code::StartupPrerequisite,
                Phase::Recovery => Code::RecoveryFailed,
                Phase::Catalog | Phase::CatalogFence => Code::CatalogFailed,
                Phase::Inventory => Code::InventoryFailed,
                Phase::Acquisition => Code::AcquisitionFailed,
                Phase::CandidateValidation => Code::CandidateFailed,
                Phase::Publication => Code::PublicationFailed,
            }),
            phase,
        )
    }
}

/// Carries a safe classification without changing legacy CLI error messages.
#[derive(Debug)]
pub(crate) struct Failure {
    code: Code,
    http_status: Option<u16>,
    source: Box<Error>,
}

impl Failure {
    pub fn new(code: Code, source: impl Into<Box<Error>>) -> Self {
        Self {
            code,
            http_status: None,
            source: source.into(),
        }
    }

    pub fn http(code: Code, status: u16, source: impl Into<Box<Error>>) -> Self {
        Self {
            http_status: Some(status),
            ..Self::new(code, source)
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for Failure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.source)
    }
}

#[derive(Clone, Copy, PartialEq, PartialOrd)]
pub(crate) enum Level {
    Error,
    Warn,
    Info,
    Debug,
}

impl Level {
    fn name(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
        }
    }

    pub fn enabled(self) -> bool {
        static LEVEL: OnceLock<Level> = OnceLock::new();
        self <= *LEVEL.get_or_init(|| match std::env::var("RFC_SYNC_LOG_LEVEL").as_deref() {
            Ok("error") => Level::Error,
            Ok("warn") => Level::Warn,
            Ok("debug") => Level::Debug,
            _ => Level::Info,
        })
    }
}

pub(crate) fn epoch_ms() -> u64 {
    millis(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default(),
    )
}

pub(crate) fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn event(level: Level, name: &'static str, fields: impl FnOnce() -> Value) {
    if !level.enabled() {
        return;
    }
    let mut record = fields();
    record["timestamp_ms"] = epoch_ms().into();
    record["level"] = level.name().into();
    record["event"] = name.into();
    if let Ok(mut line) = serde_json::to_vec(&record) {
        line.push(b'\n');
        let _ = io::stderr().lock().write_all(&line);
    }
}

#[derive(Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Progress {
    pub expected: usize,
    pub inventory: usize,
    pub txt_sealed: usize,
    pub xml_classified: usize,
    pub audit_matched: usize,
    pub audit_repaired: usize,
    pub checkpoint_batches: usize,
}

struct State {
    phase: Phase,
    progress: Progress,
    last_progress_log: Instant,
    finished: Option<(Instant, RunResult)>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RunResult {
    pub outcome: String,
    pub diagnostic: Option<Diagnostic>,
    pub generation_id: Option<String>,
    pub publication_sequence: Option<i64>,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RunSnapshot {
    pub run_id: String,
    pub scope: Scope,
    pub job_id: Option<i64>,
    pub phase: Phase,
    pub progress: Progress,
    pub started_at_ms: u64,
    pub elapsed_ms: u64,
    pub result: Option<RunResult>,
}

#[derive(Clone)]
pub(crate) struct Run {
    id: Arc<str>,
    scope: Scope,
    job: Option<i64>,
    started_at_ms: u64,
    started: Instant,
    state: Arc<Mutex<State>>,
}

impl Run {
    pub fn start(scope: Scope, job: Option<i64>) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let started = Instant::now();
        let started_at_ms = epoch_ms();
        let run = Self {
            id: format!(
                "{}-{}-{}",
                started_at_ms,
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            )
            .into(),
            scope,
            job,
            started_at_ms,
            started,
            state: Arc::new(Mutex::new(State {
                phase: Phase::Preflight,
                progress: Progress::default(),
                last_progress_log: started,
                finished: None,
            })),
        };
        run.event(
            Level::Info,
            "run_started",
            || json!({"phase": Phase::Preflight}),
        );
        run
    }

    pub fn scope(&self) -> Scope {
        self.scope
    }

    pub fn job(&self) -> Option<i64> {
        self.job
    }

    pub fn snapshot(&self) -> RunSnapshot {
        let state = self.state.lock().unwrap();
        let (elapsed, result) = match &state.finished {
            Some((end, result)) => (end.duration_since(self.started), Some(result.clone())),
            None => (self.started.elapsed(), None),
        };
        RunSnapshot {
            run_id: self.id.to_string(),
            scope: self.scope,
            job_id: self.job,
            phase: state.phase,
            progress: state.progress,
            started_at_ms: self.started_at_ms,
            elapsed_ms: millis(elapsed),
            result,
        }
    }

    pub fn phase(&self, phase: Phase) {
        self.state.lock().unwrap().phase = phase;
        self.event(Level::Info, "run_phase", || json!({"phase": phase}));
    }

    pub fn progress(&self, update: impl FnOnce(&mut Progress)) {
        let summary = {
            let mut state = self.state.lock().unwrap();
            update(&mut state.progress);
            if state.last_progress_log.elapsed() < Duration::from_secs(30) {
                return;
            }
            state.last_progress_log = Instant::now();
            (state.phase, state.progress)
        };
        self.event(Level::Info, "run_progress", || {
            json!({
                "phase": summary.0, "progress": summary.1,
            })
        });
    }

    pub fn diagnose(&self, error: &Error) -> Diagnostic {
        Diagnostic::failure(self.state.lock().unwrap().phase, error)
    }

    pub fn finish(
        &self,
        outcome: &str,
        diagnostic: Option<Diagnostic>,
        generation: Option<&str>,
        sequence: Option<i64>,
    ) {
        let progress = {
            let mut state = self.state.lock().unwrap();
            state.finished = Some((
                Instant::now(),
                RunResult {
                    outcome: outcome.to_owned(),
                    diagnostic,
                    generation_id: generation.map(str::to_owned),
                    publication_sequence: sequence,
                },
            ));
            state.progress
        };
        self.event(
            if diagnostic.is_some() {
                Level::Warn
            } else {
                Level::Info
            },
            "run_finished",
            || {
                json!({
                    "outcome": outcome, "diagnostic": diagnostic, "progress": progress,
                    "generation_id": generation, "publication_sequence": sequence,
                })
            },
        );
    }

    pub fn event(&self, level: Level, name: &'static str, fields: impl FnOnce() -> Value) {
        event(level, name, || {
            let mut record = fields();
            record["run_id"] = json!(self.id.as_ref());
            record["scope"] = self.scope.as_str().into();
            record["job_id"] = self.job.into();
            record["elapsed_ms"] = millis(self.started.elapsed()).into();
            record
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_uses_typed_evidence_and_excludes_error_text() {
        let error = Failure::new(
            Code::RepairFailed,
            Failure::http(Code::HttpsCooldown, 429, "untrusted error detail"),
        );
        let diagnosis = Diagnostic::failure(Phase::Acquisition, &error);
        assert_eq!(diagnosis.code, Code::HttpsCooldown);
        let stored = serde_json::to_string(&diagnosis).unwrap();
        assert_eq!(
            serde_json::from_str::<Diagnostic>(&stored).unwrap(),
            diagnosis
        );
        assert!(stored.contains("429"));
        assert!(!stored.contains("untrusted"));
        assert!(
            serde_json::from_str::<Diagnostic>(
                r#"{"code":"https_cooldown","phase":"acquisition","raw":"untrusted"}"#
            )
            .is_err()
        );
    }

    #[test]
    fn repair_failure_keeps_its_reason_inside_the_audit_task() {
        let error = Failure::new(
            Code::AuditFailed,
            Failure::new(Code::RepairFailed, "quarantine full"),
        );
        assert_eq!(
            Diagnostic::failure(Phase::Acquisition, &error).code,
            Code::RepairFailed
        );
    }
}
