//! Person-readable and machine-readable presentations of operator snapshots.

use super::{Availability, Snapshot, Source, checks, database, read_live};
use crate::diagnostics::{Phase, RunSnapshot, epoch_ms};
use serde::Serialize;
use std::{
    io::{self, Write},
    time::{Duration, UNIX_EPOCH},
};
use tokio::time::{Instant, MissedTickBehavior, interval};

#[derive(Clone, Copy)]
pub enum Command {
    Status,
    Watch,
    Diagnose,
}

#[derive(Serialize)]
struct Report {
    schema_version: u8,
    sampled_at_ms: u64,
    live: Source<Snapshot>,
    database: Source<database::Snapshot>,
    findings: Vec<checks::Finding>,
}

impl Report {
    fn new(live: Source<Snapshot>, database: Source<database::Snapshot>) -> Self {
        Self {
            schema_version: 1,
            sampled_at_ms: epoch_ms(),
            live,
            database,
            findings: Vec::new(),
        }
    }

    fn write(&self, json: bool) -> io::Result<()> {
        let mut output = io::stdout().lock();
        if json {
            let mut bytes = serde_json::to_vec(self)?;
            bytes.push(b'\n');
            output.write_all(&bytes)?;
        } else {
            self.render(&mut output)?;
        }
        output.flush()
    }

    fn render(&self, output: &mut impl Write) -> io::Result<()> {
        writeln!(output, "Sync daemon {}", self.live.state.label())?;
        if let Some(live) = &self.live.data {
            if let Some(run) = &live.active {
                write_run(output, "Current run", run)?;
            } else {
                writeln!(output, "No active reconciliation")?;
            }
            if let Some(wait) = &live.retry_wait {
                writeln!(
                    output,
                    "Job {} next attempt {}",
                    wait.job_id,
                    date(wait.next_attempt_ms)
                )?;
            }
            if let Some(run) = &live.last {
                write_run(output, "Last run", run)?;
            }
        }
        if self.database.state != Availability::NotRequested {
            writeln!(output, "PostgreSQL {}", self.database.state.label())?;
        }
        if let Some(db) = &self.database.data {
            match db.active_generation_id {
                Some(id) => writeln!(
                    output,
                    "Active generation {id}, publication sequence {}",
                    db.publication_sequence
                )?,
                None => writeln!(
                    output,
                    "No active generation, publication sequence {}",
                    db.publication_sequence
                )?,
            }
            writeln!(output, "Source rights epoch {}", db.rights_epoch)?;
            for (decision, count) in &db.policy_documents {
                writeln!(output, "Source policy {decision} {count} documents")?;
            }
            for job in [&db.running_job, &db.pending_job, &db.last_terminal_job]
                .into_iter()
                .flatten()
            {
                write!(
                    output,
                    "Job {} {}, {} retries",
                    job.id,
                    job.state.label(),
                    job.retries
                )?;
                if let Some(next_attempt) =
                    job.next_attempt_ms.filter(|next| *next > db.sampled_at_ms)
                {
                    write!(output, ", next attempt {}", date(next_attempt))?;
                }
                if let Some(outcome) = job.outcome {
                    write!(output, ", {}", outcome.replace('_', " "))?;
                }
                writeln!(output)?;
            }
            if let Some(notification) = &db.oldest_pending_notification {
                writeln!(
                    output,
                    "Publication {} notification pending, {} attempts, next attempt {}",
                    notification.publication_sequence,
                    notification.attempts,
                    date(notification.next_attempt_ms)
                )?;
            }
        }
        for finding in &self.findings {
            writeln!(
                output,
                "{} [{}] {}",
                finding.severity.label(),
                finding.code,
                finding.message
            )?;
            if let Some(line) = finding
                .evidence
                .pointer("/diagnostic/inventory_line")
                .and_then(|value| value.as_u64())
            {
                writeln!(output, "  Inventory line {line}")?;
            }
            if let Some(status) = finding
                .evidence
                .pointer("/diagnostic/http_status")
                .and_then(|value| value.as_u64())
            {
                writeln!(output, "  HTTP status {status}")?;
            }
            writeln!(output, "  {}", finding.action)?;
        }
        Ok(())
    }
}

fn write_run(output: &mut impl Write, label: &str, run: &RunSnapshot) -> io::Result<()> {
    let phase = match run.phase {
        Phase::Preflight => "preflight",
        Phase::Recovery => "recovery",
        Phase::Catalog => "HTTPS catalog",
        Phase::Inventory => "rsync inventory",
        Phase::Acquisition => "acquisition and audit",
        Phase::CatalogFence => "closing catalog validation",
        Phase::CandidateValidation => "candidate validation",
        Phase::Publication => "publication",
    };
    write!(output, "{label} {} ({})", run.run_id, run.scope.as_str())?;
    if let Some(job) = run.job_id {
        write!(output, ", job {job}")?;
    }
    write!(output, ", {phase}, {:.1}s", run.elapsed_ms as f64 / 1000.0)?;
    if let Some(result) = &run.result {
        write!(output, ", {}", result.outcome.replace('_', " "))?;
        if let Some(id) = &result.generation_id {
            write!(output, ", generation {id}")?;
        }
        if let Some(sequence) = result.publication_sequence {
            write!(output, ", sequence {sequence}")?;
        }
    }
    writeln!(output)?;
    let progress = &run.progress;
    if progress.expected != 0 {
        writeln!(
            output,
            "  {} expected RFCs, {} inventory entries, {} TXT sealed, {} RFCXML classified, {} audit matches, {} repairs, {} checkpoint batches",
            progress.expected,
            progress.inventory,
            progress.txt_sealed,
            progress.xml_classified,
            progress.audit_matched,
            progress.audit_repaired,
            progress.checkpoint_batches
        )?;
    }
    Ok(())
}

fn date(epoch_ms: i64) -> String {
    match u64::try_from(epoch_ms) {
        Ok(ms) if ms < 253_402_300_800_000 => {
            httpdate::fmt_http_date(UNIX_EPOCH + Duration::from_millis(ms))
        }
        _ => format!("{epoch_ms} ms since the Unix epoch"),
    }
}

pub async fn inspect(command: Command, json: bool) -> u8 {
    let result = match command {
        Command::Watch => watch(json).await,
        Command::Status | Command::Diagnose => once(command, json).await,
    };
    match result {
        Ok(exit) => exit,
        Err(error) if error.kind() == io::ErrorKind::BrokenPipe => 0,
        Err(_) => {
            eprintln!("Sync monitoring could not write its output or register a signal handler");
            1
        }
    }
}

async fn once(command: Command, json: bool) -> io::Result<u8> {
    let (live, database) = tokio::join!(read_live(), database::read());
    let mut report = Report::new(live.into(), database.into());
    let failed = match command {
        Command::Diagnose => {
            report.findings = checks::diagnose(&report.live, &report.database);
            report
                .findings
                .iter()
                .any(|finding| finding.severity == checks::Severity::Error)
        }
        _ => report.live.data.is_none() && report.database.data.is_none(),
    };
    report.write(json)?;
    Ok(u8::from(failed))
}

async fn watch(json: bool) -> io::Result<u8> {
    let mut tick = interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut last = None;
    let mut last_output = Instant::now();
    loop {
        let live: Source<Snapshot> = tokio::select! {
            _ = interrupt.recv() => return Ok(0),
            _ = terminate.recv() => return Ok(0),
            result = async {
                tick.tick().await;
                read_live().await
            } => result.into(),
        };
        let mut key = live.clone();
        if let Some(active) = key.data.as_mut().and_then(|data| data.active.as_mut()) {
            active.elapsed_ms = 0;
        }
        if last.as_ref() != Some(&key) || last_output.elapsed() >= Duration::from_secs(30) {
            let terminal = matches!(
                live.state,
                Availability::AccessDenied | Availability::Incompatible
            );
            Report::new(live, Err(Availability::NotRequested).into()).write(json)?;
            if terminal {
                return Ok(1);
            }
            last = Some(key);
            last_output = Instant::now();
        }
    }
}
