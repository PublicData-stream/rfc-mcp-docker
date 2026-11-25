//! Interpret observations and inspect startup prerequisites without repairing them.

use super::{
    Availability, Snapshot, Source,
    database::{self, JobState},
};
use crate::{
    audit,
    config::SyncConfig,
    daemon,
    diagnostics::{self, CapacityPressure, Code, Diagnostic},
    egress, rsync,
};
use rfc_storage::{
    blob::BlobWriter,
    postgres::{Result, mirror_root},
};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Severity {
    Info,
    Warning,
    Error,
}

impl Severity {
    pub fn label(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

#[derive(Serialize)]
pub(super) struct Finding {
    pub code: &'static str,
    pub severity: Severity,
    pub message: &'static str,
    pub action: &'static str,
    pub evidence: Value,
}

fn finding(
    code: &'static str,
    severity: Severity,
    message: &'static str,
    action: &'static str,
    evidence: Value,
) -> Finding {
    Finding {
        code,
        severity,
        message,
        action,
        evidence,
    }
}

pub(super) fn diagnose(live: &Source<Snapshot>, db: &Source<database::Snapshot>) -> Vec<Finding> {
    let mut findings = Vec::new();
    let live_data = live.data.as_ref();
    let active = live_data.is_some_and(|data| data.active.is_some());
    if live_data.is_none() {
        let severity = match live.state {
            Availability::AccessDenied | Availability::Incompatible => Severity::Error,
            _ => Severity::Warning,
        };
        findings.push(finding(
            "daemon_monitor_unavailable", severity, "The live Sync snapshot is unavailable.",
            "Check that Sync is running and execute the command inside its container as the Sync UID.",
            json!({"state": live.state}),
        ));
    }
    if let Some(snapshot) = &db.data {
        durable_findings(snapshot, &mut findings);
    } else {
        findings.push(finding(
            "database_unavailable", Severity::Error, "The verified PostgreSQL snapshot is unavailable.",
            "Check PostgreSQL reachability, storage schema, Sync role grants, and the mounted database credentials and CA.",
            json!({"state": db.state}),
        ));
    }
    if let Some(last) = live_data.and_then(|data| data.last.as_ref())
        && let Some(diagnostic) = last.result.as_ref().and_then(|result| result.diagnostic)
        && (last.job_id.is_none() || db.data.is_none())
    {
        let severity = match diagnostic.code {
            Code::AwaitingCatalog | Code::AwaitingMirror | Code::Cancelled => Severity::Info,
            _ if active => Severity::Warning,
            _ => Severity::Error,
        };
        findings.push(run_finding(
            diagnostic,
            severity,
            json!({"run_id": last.run_id, "job_id": last.job_id}),
        ));
    }
    local_findings(active, &mut findings);
    findings
}

fn durable_findings(db: &database::Snapshot, findings: &mut Vec<Finding>) {
    if db.active_generation_id.is_none() {
        findings.push(finding(
            "awaiting_first_publication",
            Severity::Info,
            "Sync has no active generation yet.",
            "Follow the first reconciliation with watch and check the job outcome.",
            json!({"publication_sequence": db.publication_sequence}),
        ));
    }
    let waiting = db
        .running_job
        .as_ref()
        .or(db.pending_job.as_ref())
        .filter(|job| {
            job.next_attempt_ms
                .is_some_and(|next| next > db.sampled_at_ms)
        });
    if let Some(job) = waiting {
        let evidence = json!({"job_id": job.id, "retries": job.retries, "next_attempt_ms": job.next_attempt_ms});
        findings.push(finding(
            "job_waiting",
            Severity::Info,
            "The scheduler is waiting for the next eligible attempt.",
            "Wait for the recorded deadline and follow the next attempt with watch.",
            evidence.clone(),
        ));
        if let Some(diagnostic) = job.diagnostic {
            findings.push(run_finding(diagnostic, Severity::Warning, evidence));
        }
    }
    if let Some(job) = &db.last_terminal_job
        && job.state == JobState::Failed
    {
        let superseded = db
            .running_job
            .iter()
            .chain(&db.pending_job)
            .any(|newer| newer.id > job.id);
        let severity = if superseded {
            Severity::Warning
        } else {
            Severity::Error
        };
        let evidence = json!({"job_id": job.id, "outcome": job.outcome, "retries": job.retries});
        if let Some(diagnostic) = job.diagnostic {
            findings.push(run_finding(diagnostic, severity, evidence));
        } else {
            findings.push(finding(
                "job_failed",
                severity,
                "The last finished job failed. Specific diagnostic evidence is unavailable.",
                "Find the job ID in the Sync container logs and review its last attempt.",
                evidence,
            ));
        }
    }
    if let Some(notification) = &db.oldest_pending_notification
        && notification.attempts > 0
    {
        findings.push(finding(
            "notification_pending", Severity::Warning, "A committed publication is waiting for an authenticated notification acknowledgement.",
            "Check Serve availability, the private notification address, and the corresponding key configuration.",
            json!(notification),
        ));
    }
}

fn local_findings(active: bool, findings: &mut Vec<Finding>) {
    if let Some(config) = check(
        findings,
        active,
        "ingestion_preflight",
        "The local ingestion configuration or scratch and memory budget check failed.",
        "Validate the mounted ingestion TOML and compare local resource limits with --check-config.",
        SyncConfig::load(),
    ) {
        check(
            findings,
            active,
            "spool_preflight",
            "The HTTPS spool failed its ownership or capacity check.",
            "Check the private disk-backed spool mount, Sync UID ownership, free bytes and free inodes.",
            egress::preflight(config.ingestion.max_accepted_bytes),
        );
    }
    check(
        findings,
        active,
        "landlock_unavailable",
        "The required Landlock ABI is unavailable.",
        "Check the host kernel and the Sync container's seccomp policy.",
        rsync::landlock_abi().map_err(Into::into),
    );
    check(
        findings,
        active,
        "audit_preflight",
        "The rsync audit mount failed preflight.",
        "Check the separate disk-backed audit mount, Sync UID ownership, free bytes and free inodes.",
        audit::preflight(),
    );
    check(
        findings,
        active,
        "mirror_preflight",
        "Sync cannot access its writable mirror mount.",
        "Check that the mirror volume is mounted for the Sync UID with write and search permission.",
        BlobWriter::check_mount(&mirror_root()).map_err(Into::into),
    );
    check(
        findings,
        active,
        "control_config",
        "The private Sync control settings failed validation.",
        "Check the configured private addresses, key mounts, and key rotation settings.",
        daemon::control_settings(),
    );
    if egress::private_root().is_ok()
        && let Some(remaining) = check(
            findings,
            active,
            "cooldown_state",
            "The persisted HTTPS cooldown record could not be read.",
            "Check the spool's cooldown record and the Sync container logs before restarting acquisition.",
            egress::cooldown_remaining(),
        )
        && !remaining.is_zero()
    {
        findings.push(finding(
            "upstream_cooldown",
            Severity::Info,
            "The spool has a recorded upstream cooldown.",
            "Let the cooldown expire before expecting the next upstream request.",
            json!({"remaining_ms": diagnostics::millis(remaining)}),
        ));
    }
}

fn check<T>(
    findings: &mut Vec<Finding>,
    active: bool,
    code: &'static str,
    message: &'static str,
    action: &'static str,
    result: Result<T>,
) -> Option<T> {
    match result {
        Ok(value) => Some(value),
        Err(error) => {
            let pressure = error.is::<CapacityPressure>();
            let severity = if active && pressure {
                Severity::Warning
            } else {
                Severity::Error
            };
            findings.push(finding(
                code,
                severity,
                message,
                action,
                json!({"capacity_pressure": pressure}),
            ));
            None
        }
    }
}

fn run_finding(diagnostic: Diagnostic, severity: Severity, mut evidence: Value) -> Finding {
    let (code, message, action) = match diagnostic.code {
        Code::AwaitingCatalog => (
            "awaiting_catalog",
            "A Feed hint has yet to appear in the catalog.",
            "Wait for source convergence and check the next scheduled attempt.",
        ),
        Code::AwaitingMirror => (
            "awaiting_mirror",
            "The rsync inventory does not yet contain expected TXT.",
            "Wait for mirror convergence and review the next inventory check.",
        ),
        Code::InvalidInventory => (
            "invalid_inventory",
            "Sync rejected the upstream rsync listing.",
            "Compare the reported reason and line number with the expected inventory format.",
        ),
        Code::InventoryFailed => (
            "inventory_failed",
            "Sync could not validate the upstream rsync inventory.",
            "Check rsync availability and confirm that previously accepted entries are still present.",
        ),
        Code::HttpsCooldown => (
            "https_cooldown",
            "Upstream throttling delayed HTTPS acquisition.",
            "Wait for the recorded cooldown and inspect HTTP status in the Sync logs.",
        ),
        Code::HttpsTimeout => (
            "https_timeout",
            "An upstream HTTPS request timed out.",
            "Check upstream reachability and review the affected source and retry timing in the logs.",
        ),
        Code::HttpsTransport | Code::HttpsStatus => (
            "https_failed",
            "An upstream HTTPS request failed.",
            "Check the HTTPS attempt outcome and HTTP status where available.",
        ),
        Code::AuditFailed => (
            "audit_failed",
            "Required TXT audit did not finish successfully.",
            "Check the rsync audit mount and the audit batch outcomes in the logs.",
        ),
        Code::RepairFailed => (
            "repair_failed",
            "A required TXT repair could not be sealed.",
            "Check rsync and HTTPS repair outcomes and available audit quarantine space.",
        ),
        Code::CatalogFailed => (
            "catalog_failed",
            "Sync could not acquire or validate the HTTPS catalog.",
            "Check the catalog response and confirm that previously accepted RFCs remain present.",
        ),
        Code::CatalogChanged => (
            "catalog_changed",
            "The opening and closing catalogs differed.",
            "Allow a new attempt against a stable catalog.",
        ),
        Code::CandidateFailed => (
            "candidate_failed",
            "Sync could not validate and seal the candidate.",
            "Review candidate validation, storage and database events for this run.",
        ),
        Code::PublicationFailed => (
            "publication_failed",
            "Sync could not promote the candidate.",
            "Check the active sequence, audit validity, and publication events before retrying.",
        ),
        Code::StartupPrerequisite => (
            "startup_prerequisite",
            "The run failed during startup checks.",
            "Review the local preflight findings and daemon logs.",
        ),
        Code::RecoveryFailed => (
            "recovery_failed",
            "Sync could not recover its acquisition or candidate state.",
            "Review recovery events and check the mirror and database.",
        ),
        Code::Cancelled => (
            "cancelled",
            "Sync cancelled the attempt.",
            "Check whether Sync was stopped or restarted before the attempt completed.",
        ),
        Code::DeadlineExceeded => (
            "deadline_exceeded",
            "The observed phase exceeded its deadline.",
            "Review the phase and the most recent progress and attempt events.",
        ),
        Code::AcquisitionFailed => (
            "acquisition_failed",
            "Acquisition failed during processing.",
            "Review HTTPS, checkpoint and audit events for the affected run.",
        ),
    };
    evidence["diagnostic"] = json!(diagnostic);
    finding(code, severity, message, action, evidence)
}
