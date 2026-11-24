use crate::{
    Scope, Writer,
    config::SyncConfig,
    ingest::ingest_locked,
    pipeline::Shutdown,
    policy::Registry,
    publication::promote_locked,
    upstream::{Download, Source, Upstream},
    xml,
};
use rfc_core::GenerationId;
use rfc_mcp_docker::control::{
    self, Client, Command, GenerationCommitted, Keys, Link, Outcome, Rejection, SyncRequested,
    jitter,
};
use rfc_storage::{fixture_boundary, postgres::Result};
use std::{env, time::Duration};
use tokio::{
    net::TcpListener,
    time::{Instant, sleep, timeout},
};
use tokio_postgres::Transaction;

const HEARTBEAT: Duration = Duration::from_secs(2);
const LOOP_TICK: Duration = Duration::from_secs(1);
const DAILY: Duration = Duration::from_secs(86400);
const HOURLY: Duration = Duration::from_secs(3600);

struct Schedule {
    daily: Duration,
    hourly: Duration,
    outbox_retry: Duration,
}

impl Schedule {
    fn new() -> Result<Self> {
        #[cfg(feature = "phase6-fixtures")]
        if fixtures() {
            let setting = |name, default| -> Result<Duration> {
                let seconds = env::var(name)
                    .ok()
                    .map(|value| value.parse())
                    .transpose()?
                    .unwrap_or(default);
                if seconds == 0 {
                    return Err("fixture duration must be positive".into());
                }
                Ok(Duration::from_secs(seconds))
            };
            return Ok(Self {
                daily: setting("RFC_PHASE6_DAILY_SECS", 3600)?,
                hourly: setting("RFC_PHASE6_HOURLY_SECS", 3600)?,
                outbox_retry: Duration::from_secs(2),
            });
        }
        Ok(Self {
            daily: DAILY,
            hourly: HOURLY,
            outbox_retry: Duration::from_secs(30),
        })
    }
}

#[cfg(feature = "phase6-fixtures")]
fn fixtures() -> bool {
    env::var("RFC_PHASE6_FIXTURES").as_deref() == Ok("1")
}

fn upstream(config: SyncConfig) -> Result<Upstream> {
    #[cfg(feature = "phase6-fixtures")]
    if fixtures() {
        return Upstream::fixture(config);
    }
    Upstream::official(config)
}

pub async fn run_daemon() -> Result<()> {
    let config = SyncConfig::load()?;
    let listen = control::address(&env::var("RFC_SYNC_CONTROL_BIND")?)?;
    let notify = control::address(&env::var("RFC_SYNC_SERVE_ADDR")?)?;
    let feed_keys = Keys::load(Link::FeedSync)?;
    let notify_keys = Keys::load(Link::SyncServe)?;
    if feed_keys.overlaps(&notify_keys) {
        return Err("feed and notification key sets overlap".into());
    }
    let mut leader = Writer::connect().await?;
    let held: bool = leader
        .client
        .query_one("SELECT pg_catalog.pg_try_advisory_lock(-3::bigint)", &[])
        .await?
        .try_get(0)?;
    if !held {
        return Err("another synchronization daemon owns the scheduler".into());
    }
    enqueue(&mut leader, None, true).await?;
    let listener = TcpListener::bind(listen).await?;
    let schedule = Schedule::new()?;
    let notify = Client::new(notify, Link::SyncServe, notify_keys);
    let outbox_retry = schedule.outbox_retry;
    let shutdown = Shutdown::new();
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let worker_shutdown = shutdown.clone();
    let mut worker = tokio::spawn(async move { worker(config, &schedule, &worker_shutdown).await });
    let services = async {
        tokio::try_join!(
            control::serve(listener, Link::FeedSync, feed_keys, accept),
            dispatch(notify, outbox_retry),
            watch_leader(leader),
        )?;
        Result::Ok(())
    };
    tokio::pin!(services);
    tokio::select! {
        result = &mut worker => result?,
        result = &mut services => {
            shutdown.stop();
            result.and(worker.await?)
        }
        _ = term.recv() => {
            shutdown.stop();
            worker.await?
        }
    }
}

async fn watch_leader(leader: Writer) -> Result<()> {
    loop {
        sleep(HEARTBEAT).await;
        timeout(HEARTBEAT, leader.client.simple_query("SELECT 1")).await??;
    }
}

async fn accept(command: Command) -> std::result::Result<Outcome, Rejection> {
    let Command::Sync(event) = command else {
        return Err(Rejection::Conflict);
    };
    let mut writer = Writer::connect()
        .await
        .map_err(|_| Rejection::Unavailable)?;
    match enqueue(&mut writer, Some(&event), false).await {
        Ok(true) => Ok(Outcome::Accepted),
        Ok(false) => Err(Rejection::Conflict),
        Err(_) => Err(Rejection::Unavailable),
    }
}

async fn lock_queue(tx: &Transaction<'_>) -> Result<()> {
    let held: bool = tx
        .query_one(
            "SELECT pg_catalog.pg_try_advisory_xact_lock(-4::bigint)",
            &[],
        )
        .await?
        .try_get(0)?;
    if !held {
        return Err("synchronization queue is busy".into());
    }
    Ok(())
}

async fn enqueue(
    writer: &mut Writer,
    event: Option<&SyncRequested>,
    startup: bool,
) -> Result<bool> {
    let tx = writer.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit = on")
        .await?;
    lock_queue(&tx).await?;
    let mut trigger = None;
    if let Some(event) = event {
        let id = event.event_id.to_string();
        let time = event.observed_at as i64;
        let inserted = tx.execute(
            "INSERT INTO rfc.control_events (id, observation_digest, observed_at, status)
             VALUES ($1::text::uuid, decode($2, 'hex'), timestamptz 'epoch' + $3::bigint * interval '1 second', 'accepted')
             ON CONFLICT (id) DO NOTHING",
            &[&id, &event.observation_digest, &time],
        ).await?;
        if inserted == 0 {
            let same: bool = tx
                .query_one(
                    "SELECT observation_digest = decode($2, 'hex') AND job_id IS NOT NULL
                 FROM rfc.control_events WHERE id = $1::text::uuid",
                    &[&id, &event.observation_digest],
                )
                .await?
                .try_get::<_, Option<bool>>(0)?
                .unwrap_or(false);
            tx.commit().await?;
            return Ok(same);
        }
        trigger = Some(id);
    }
    let existing = tx
        .query_opt(
            "SELECT id FROM rfc.sync_jobs WHERE state = 'pending' OR ($1 AND state = 'running')
         ORDER BY CASE state WHEN 'running' THEN 0 ELSE 1 END LIMIT 1",
            &[&startup],
        )
        .await?;
    let coalesced = existing.is_some();
    let job: i64 = match existing {
        Some(row) => row.try_get(0)?,
        None => tx
            .query_one(
                "INSERT INTO rfc.sync_jobs (trigger_id) VALUES ($1::text::uuid) RETURNING id",
                &[&trigger],
            )
            .await?
            .try_get(0)?,
    };
    if let Some(id) = trigger {
        let status = if coalesced { "coalesced" } else { "accepted" };
        tx.execute(
            "UPDATE rfc.control_events SET status = $2, job_id = $3 WHERE id = $1::text::uuid",
            &[&id, &status, &job],
        )
        .await?;
        fixture_boundary("before_event_commit");
    }
    tx.commit().await?;
    if event.is_some() {
        fixture_boundary("after_event_commit");
    }
    Ok(true)
}

enum NextJob {
    Ready(i64),
    Waiting,
    Idle,
}

async fn claim(writer: &mut Writer) -> Result<NextJob> {
    let tx = writer.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit = on")
        .await?;
    lock_queue(&tx).await?;
    let running = tx.query_opt(
        "SELECT id, next_attempt_at <= clock_timestamp() FROM rfc.sync_jobs WHERE state = 'running'", &[],
    ).await?;
    let job = if let Some(row) = running {
        if row.try_get::<_, bool>(1)? {
            NextJob::Ready(row.try_get(0)?)
        } else {
            NextJob::Waiting
        }
    } else if let Some(row) = tx
        .query_opt("SELECT id FROM rfc.sync_jobs WHERE state = 'pending'", &[])
        .await?
    {
        let id: i64 = row.try_get(0)?;
        tx.execute("UPDATE rfc.sync_jobs SET state = 'running', started_at = clock_timestamp() WHERE id = $1", &[&id]).await?;
        NextJob::Ready(id)
    } else {
        NextJob::Idle
    };
    tx.commit().await?;
    Ok(job)
}

pub(crate) async fn finish_job(tx: &Transaction<'_>, job: i64, success: bool) -> Result<()> {
    lock_queue(tx).await?;
    complete(tx, job, success).await
}

async fn complete(tx: &Transaction<'_>, job: i64, success: bool) -> Result<()> {
    let (state, outcome, event_status) = if success {
        ("succeeded", "published", "completed")
    } else {
        ("failed", "reconciliation_failed", "failed")
    };
    if tx
        .execute(
            "UPDATE rfc.sync_jobs SET state = $2, outcome = $3, finished_at = clock_timestamp()
         WHERE id = $1 AND state = 'running'",
            &[&job, &state, &outcome],
        )
        .await?
        != 1
    {
        return Err("synchronization job is no longer running".into());
    }
    tx.execute(
        "UPDATE rfc.control_events SET status = $2 WHERE job_id = $1 AND status IN ('accepted', 'coalesced')",
        &[&job, &event_status],
    ).await?;
    Ok(())
}

async fn schedule_retry(writer: &mut Writer, job: i64) -> Result<()> {
    let tx = writer.client.transaction().await?;
    tx.batch_execute("SET LOCAL synchronous_commit = on")
        .await?;
    lock_queue(&tx).await?;
    let retries: i32 = tx
        .query_one(
            "SELECT retries FROM rfc.sync_jobs WHERE id = $1 AND state = 'running'",
            &[&job],
        )
        .await?
        .try_get(0)?;
    if retries >= 2 {
        complete(&tx, job, false).await?;
    } else {
        let delay = if retries == 0 { 30_i32 } else { 120_i32 };
        tx.execute(
            "UPDATE rfc.sync_jobs SET retries = retries + 1, next_attempt_at = clock_timestamp() + make_interval(secs => $2::integer)
             WHERE id = $1",
            &[&job, &delay],
        ).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn reconcile(job: i64, config: SyncConfig, shutdown: &Shutdown) -> Result<()> {
    let mut writer = Writer::connect().await?;
    let _corpus = writer.acquire_coordinator().await?;
    let row = writer.client.query_one(
        "SELECT j.generation_id, j.expected_publication_sequence, g.state
         FROM rfc.sync_jobs j LEFT JOIN rfc.generations g ON j.generation_id = g.id WHERE j.id = $1 AND j.state = 'running'",
        &[&job],
    ).await?;
    let state: Option<&str> = row.try_get(2)?;
    let (candidate, sequence) = match state {
        Some("ready") => {
            let id =
                GenerationId::new(row.try_get::<_, i64>(0)?).ok_or("invalid ready candidate ID")?;
            (id, row.try_get(1)?)
        }
        Some("published" | "retained") => {
            let tx = writer.client.transaction().await?;
            tx.batch_execute("SET LOCAL synchronous_commit = on")
                .await?;
            finish_job(&tx, job, true).await?;
            tx.commit().await?;
            return Ok(());
        }
        _ => {
            let (id, _) = ingest_locked(
                &mut writer,
                upstream(config),
                Registry::load(),
                Scope::Full,
                Some(job),
                shutdown,
            )
            .await?;
            let sequence = writer
                .client
                .query_one(
                    "SELECT expected_publication_sequence FROM rfc.sync_jobs WHERE id = $1",
                    &[&job],
                )
                .await?
                .try_get(0)?;
            (id, sequence)
        }
    };
    shutdown.check()?;
    promote_locked(&mut writer, candidate, sequence, false, Some(job)).await?;
    Ok(())
}

async fn catalog_changed(config: SyncConfig) -> Result<bool> {
    // Use a short-lived session so the advisory lock is released on return.
    let writer = Writer::connect().await?;
    let _corpus = writer.acquire_coordinator().await?;
    let upstream = upstream(config)?;
    upstream.after_fence().await;
    let stored: Option<String> = writer.client.query_opt(
        "SELECT encode(o.raw_digest, 'hex')
         FROM rfc.active_generation a JOIN rfc.generation_ingestion i ON i.generation_id = a.generation_id
         JOIN rfc.source_observations o ON o.identity = i.catalog_after WHERE a.singleton",
        &[],
    ).await?.map(|row| row.try_get(0)).transpose()?;
    let Download::Captured(capture) = upstream.fetch(Source::Catalog).await? else {
        return Err("catalog is unavailable".into());
    };
    let digest = rfc_core::BlobDigest::hash(&capture.bytes).to_string();
    if stored.as_ref() == Some(&digest) {
        return Ok(false);
    }
    xml::catalog(&capture.bytes)?;
    Ok(true)
}

async fn worker(config: SyncConfig, schedule: &Schedule, shutdown: &Shutdown) -> Result<()> {
    let mut writer = Writer::connect().await?;
    let mut daily = Instant::now() + jitter(schedule.daily);
    let mut hourly = Instant::now() + jitter(schedule.hourly);
    while shutdown.check().is_ok() {
        if Instant::now() >= daily && enqueue(&mut writer, None, false).await.is_ok() {
            daily = Instant::now() + jitter(schedule.daily);
        }
        match claim(&mut writer).await {
            Ok(NextJob::Ready(job)) => {
                if reconcile(job, config, shutdown).await.is_err() && shutdown.check().is_ok() {
                    eprintln!("reconciliation job {job} encountered an error");
                    if schedule_retry(&mut writer, job).await.is_err() {
                        sleep(LOOP_TICK).await;
                    }
                }
            }
            Ok(NextJob::Idle) if Instant::now() >= hourly => {
                hourly = Instant::now() + jitter(schedule.hourly);
                let changed = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    result = catalog_changed(config) => result,
                };
                match changed {
                    Ok(true) if enqueue(&mut writer, None, false).await.is_err() => {
                        eprintln!("catalog change could not be queued");
                        hourly = Instant::now() + LOOP_TICK;
                    }
                    Ok(_) => {}
                    Err(_) => eprintln!("hourly catalog check failed"),
                }
            }
            _ => tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = sleep(LOOP_TICK) => {}
            },
        }
        if writer.client.is_closed() {
            return Err("synchronization worker lost its database connection".into());
        }
    }
    Ok(())
}

async fn dispatch(mut client: Client, base_retry: Duration) -> Result<()> {
    let writer = Writer::connect().await?;
    loop {
        let row = writer.client.query_opt(
            "SELECT publication_sequence, generation_id, event_id::text, attempts
             FROM rfc.notification_outbox WHERE delivery_state = 'pending' AND next_attempt_at <= clock_timestamp()
             ORDER BY next_attempt_at, publication_sequence LIMIT 1",
            &[],
        ).await?;
        let Some(row) = row else {
            sleep(LOOP_TICK).await;
            continue;
        };
        let event = GenerationCommitted {
            event_id: row.try_get::<_, &str>(2)?.parse()?,
            generation_id: row.try_get(1)?,
            publication_sequence: row.try_get(0)?,
            manifest_digest: None,
        };
        let attempts: i32 = row.try_get(3)?;
        let retry = jitter((base_retry * (1 << attempts.min(10))).min(Duration::from_secs(3600)))
            .as_secs() as i64;
        // Reserve a retry time before waiting on the network. No transaction or row lock crosses HTTP.
        let claim = writer.client.query_opt(
            "UPDATE rfc.notification_outbox SET attempts = CASE WHEN attempts < 2147483647 THEN attempts + 1 ELSE attempts END,
                next_attempt_at = clock_timestamp() + make_interval(secs => $3::bigint),
                last_error = 'awaiting_authenticated_ack'
             WHERE publication_sequence = $1 AND attempts = $2 AND delivery_state = 'pending'
               AND next_attempt_at <= clock_timestamp()
             RETURNING attempts, next_attempt_at::text",
            &[&event.publication_sequence, &attempts, &retry],
        ).await?;
        let Some(claim) = claim else { continue };
        let attempt: i32 = claim.try_get(0)?;
        let reserved_until: &str = claim.try_get(1)?;
        if client.notify_generation(&event).await.is_ok() {
            fixture_boundary("before_outbox_ack");
            writer.client.execute(
                "UPDATE rfc.notification_outbox SET delivery_state = 'delivered', delivered_at = clock_timestamp(), last_error = NULL
                 WHERE publication_sequence = $1 AND attempts = $2 AND next_attempt_at = $3::text::timestamptz AND delivery_state = 'pending'",
                &[&event.publication_sequence, &attempt, &reserved_until],
            ).await?;
            fixture_boundary("after_outbox_ack");
        } else {
            eprintln!(
                "notification for publication {} will retry",
                event.publication_sequence
            );
        }
    }
}
