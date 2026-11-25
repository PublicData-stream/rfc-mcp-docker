//! A bounded, consistent view of the durable operator records.

use super::Availability;
use crate::{Writer, diagnostics::Diagnostic};
use rfc_storage::postgres::Result;
use serde::Serialize;
use std::time::Duration;
use tokio::time::timeout;
use tokio_postgres::{IsolationLevel, Row};

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum JobState {
    Pending,
    Running,
    Succeeded,
    Failed,
}

impl JobState {
    pub fn label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

#[derive(Serialize)]
pub(super) struct Job {
    pub id: i64,
    pub state: JobState,
    pub generation_id: Option<i64>,
    pub retries: i32,
    pub next_attempt_ms: Option<i64>,
    pub started_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub outcome: Option<&'static str>,
    pub diagnostic: Option<Diagnostic>,
}

impl Job {
    fn read(row: Row) -> Result<Self> {
        let state = match row.try_get::<_, &str>("state")? {
            "pending" => JobState::Pending,
            "running" => JobState::Running,
            "succeeded" => JobState::Succeeded,
            "failed" => JobState::Failed,
            _ => return Err("unexpected Sync job state".into()),
        };
        let outcome = match row.try_get::<_, Option<&str>>("outcome")? {
            Some("published") => Some("published"),
            Some("acquisition_deferred") => Some("acquisition_deferred"),
            Some("reconciliation_failed") => Some("reconciliation_failed"),
            _ => None,
        };
        Ok(Self {
            id: row.try_get("id")?,
            state,
            generation_id: row.try_get("generation_id")?,
            retries: row.try_get("retries")?,
            next_attempt_ms: matches!(state, JobState::Pending | JobState::Running)
                .then(|| row.try_get("next_attempt_ms"))
                .transpose()?,
            started_at_ms: row.try_get("started_at_ms")?,
            finished_at_ms: row.try_get("finished_at_ms")?,
            outcome,
            diagnostic: row
                .try_get::<_, Option<&str>>("diagnostic")?
                .and_then(|raw| serde_json::from_str(raw).ok()),
        })
    }
}

#[derive(Serialize)]
pub(super) struct Notification {
    pub publication_sequence: i64,
    pub generation_id: i64,
    pub attempts: i32,
    pub next_attempt_ms: i64,
}

#[derive(Serialize)]
pub(super) struct Snapshot {
    pub sampled_at_ms: i64,
    pub active_generation_id: Option<i64>,
    pub publication_sequence: i64,
    pub running_job: Option<Job>,
    pub pending_job: Option<Job>,
    pub last_terminal_job: Option<Job>,
    pub oldest_pending_notification: Option<Notification>,
}

pub(super) async fn read() -> std::result::Result<Snapshot, Availability> {
    match timeout(Duration::from_secs(12), snapshot()).await {
        Ok(Ok(snapshot)) => Ok(snapshot),
        Ok(Err(_)) => Err(Availability::Unavailable),
        Err(_) => Err(Availability::TimedOut),
    }
}

async fn snapshot() -> Result<Snapshot> {
    let mut writer = Writer::connect().await?;
    let tx = writer
        .client
        .build_transaction()
        .read_only(true)
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let pointer = tx.query_one(
        "SELECT generation_id, publication_sequence, (extract(epoch FROM transaction_timestamp()) * 1000)::bigint
         FROM rfc.active_generation WHERE singleton", &[],
    ).await?;
    let rows = tx.query(
        "WITH selected AS (
            SELECT id FROM rfc.sync_jobs WHERE state IN ('pending', 'running')
            UNION ALL
            (SELECT id FROM rfc.sync_jobs WHERE state IN ('succeeded', 'failed') ORDER BY id DESC LIMIT 1)
         )
         SELECT j.id, j.state, j.generation_id, j.retries, j.outcome, j.diagnostic,
            (extract(epoch FROM j.next_attempt_at) * 1000)::bigint AS next_attempt_ms,
            (extract(epoch FROM j.started_at) * 1000)::bigint AS started_at_ms,
            (extract(epoch FROM j.finished_at) * 1000)::bigint AS finished_at_ms
         FROM selected JOIN rfc.sync_jobs j USING (id)", &[],
    ).await?;
    let notification = tx.query_opt(
        "SELECT publication_sequence, generation_id, attempts, (extract(epoch FROM next_attempt_at) * 1000)::bigint
         FROM rfc.notification_outbox WHERE delivery_state = 'pending' ORDER BY publication_sequence LIMIT 1", &[],
    ).await?;
    tx.commit().await?;

    let mut snapshot = Snapshot {
        sampled_at_ms: pointer.try_get(2)?,
        active_generation_id: pointer.try_get(0)?,
        publication_sequence: pointer.try_get(1)?,
        running_job: None,
        pending_job: None,
        last_terminal_job: None,
        oldest_pending_notification: None,
    };
    for row in rows {
        let job = Job::read(row)?;
        match job.state {
            JobState::Pending => snapshot.pending_job = Some(job),
            JobState::Running => snapshot.running_job = Some(job),
            JobState::Succeeded | JobState::Failed => snapshot.last_terminal_job = Some(job),
        }
    }
    if let Some(notification) = notification {
        snapshot.oldest_pending_notification = Some(Notification {
            publication_sequence: notification.try_get(0)?,
            generation_id: notification.try_get(1)?,
            attempts: notification.try_get(2)?,
            next_attempt_ms: notification.try_get(3)?,
        });
    }
    Ok(snapshot)
}
