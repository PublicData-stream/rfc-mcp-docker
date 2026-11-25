use crate::{
    Scope, Writer,
    acquisition::Spool,
    config::SyncConfig,
    discovery,
    egress::Lease,
    feed_broker::Broker,
    manual::{Manual, Reply},
    shutdown::Shutdown,
    upstream::Upstream,
};
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
const HOURLY: Duration = Duration::from_secs(3600);

struct Schedule {
    daily: Duration,
    hourly: Duration,
    outbox_retry: Duration,
}

impl Schedule {
    fn new(daily: Duration) -> Result<Self> {
        #[cfg(feature = "phase6-fixtures")]
        if acquisition_fixture() {
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
                daily: setting("RFC_PHASE6_DAILY_SECS", daily.as_secs())?,
                hourly: setting("RFC_PHASE6_HOURLY_SECS", 3600)?,
                outbox_retry: Duration::from_secs(2),
            });
        }
        Ok(Self {
            daily,
            hourly: HOURLY,
            outbox_retry: Duration::from_secs(30),
        })
    }
}

#[cfg(feature = "phase6-fixtures")]
pub(crate) fn acquisition_fixture() -> bool {
    env::var("RFC_DISCOVERY_FIXTURES").as_deref() == Ok("1")
}

pub async fn run_daemon() -> Result<()> {
    let config = SyncConfig::load()?;
    let lease = Lease::acquire()?;
    Spool::sweep().await?;
    config.preflight()?;
    let upstream = discovery::upstream(&config)?.with_lease(&lease)?;
    let settings = &config.ingestion;
    let schedule = Schedule::new(Duration::from_secs(
        settings
            .discovery
            .full_reconcile_interval_seconds
            .min(settings.revalidation.interval_seconds),
    ))?;
    let listen = control::address(&env::var("RFC_SYNC_CONTROL_BIND")?)?;
    let notify = control::address(&env::var("RFC_SYNC_SERVE_ADDR")?)?;
    let feed_keys = Keys::load(Link::FeedSync)?;
    let notify_keys = Keys::load(Link::SyncServe)?;
    if feed_keys.overlaps(&notify_keys) {
        return Err("feed and notification key sets overlap".into());
    }
    let mut leader = Writer::connect().await?;
    leader.acquire_egress_leader().await?;
    enqueue(&mut leader, None, true).await?;
    let listener = TcpListener::bind(listen).await?;
    let notify = Client::new(notify, Link::SyncServe, notify_keys);
    let outbox_retry = schedule.outbox_retry;
    let shutdown = Shutdown::new();
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let stopping = async {
        tokio::select! {
            _ = term.recv() => (),
            _ = interrupt.recv() => (),
        }
    };
    let manual = Manual::default();
    let operator = Manual::bind()?;
    let (broker, feed_requests) = Broker::new();
    let broker_control = broker.clone();
    let broker_upstream = upstream.clone();
    let broker_shutdown = shutdown.clone();
    let mut feed_worker = tokio::spawn(async move {
        broker
            .run(feed_requests, &broker_upstream, &broker_shutdown)
            .await
    });
    let worker_shutdown = shutdown.clone();
    let worker_manual = manual.clone();
    let mut worker = tokio::spawn(async move {
        worker(
            config,
            &upstream,
            &schedule,
            &worker_shutdown,
            &worker_manual,
        )
        .await
    });
    let operator_shutdown = shutdown.clone();
    let mut operator_worker =
        tokio::spawn(async move { manual.serve(operator, &operator_shutdown).await });
    let services = async {
        tokio::try_join!(
            control::serve(listener, Link::FeedSync, feed_keys, move |command| {
                let broker = broker_control.clone();
                async move { accept(command, &broker).await }
            }),
            dispatch(notify, outbox_retry),
            watch_leader(&leader),
        )?;
        Result::Ok(())
    };
    tokio::pin!(services);
    let (result, finished_worker) = tokio::select! {
        result = &mut worker => (result, 1),
        result = &mut feed_worker => (result, 2),
        result = &mut operator_worker => (result, 3),
        result = &mut services => (Ok(result), 0),
        _ = stopping => (Ok(Ok(())), 0),
    };
    shutdown.stop();
    let worker_result = if finished_worker == 1 {
        Ok(Ok(()))
    } else {
        worker.await
    };
    let feed_result = if finished_worker == 2 {
        Ok(Ok(()))
    } else {
        feed_worker.await
    };
    let operator_result = if finished_worker == 3 {
        Ok(Ok(()))
    } else {
        operator_worker.await
    };
    drop(lease);
    result?
        .and(worker_result?)
        .and(feed_result?)
        .and(operator_result?)
}

pub(crate) async fn watch_leader(leader: &Writer) -> Result<()> {
    loop {
        sleep(HEARTBEAT).await;
        timeout(HEARTBEAT, leader.client.simple_query("SELECT 1")).await??;
    }
}

async fn accept(command: Command, broker: &Broker) -> std::result::Result<Outcome, Rejection> {
    let event = match command {
        Command::FeedPoll(poll) => return Ok(Outcome::Feed(broker.poll(&poll))),
        Command::Sync(event) => event,
        Command::Generation(_) => return Err(Rejection::Conflict),
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
        if !coalesced {
            tx.execute(
                "UPDATE rfc.sync_jobs SET next_attempt_at = clock_timestamp() + interval '1 second' WHERE id = $1",
                &[&job],
            ).await?;
        }
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
        .query_opt("SELECT id, next_attempt_at <= clock_timestamp() FROM rfc.sync_jobs WHERE state = 'pending'", &[])
        .await?
    {
        if row.try_get::<_, bool>(1)? {
            let id: i64 = row.try_get(0)?;
            tx.execute("UPDATE rfc.sync_jobs SET state = 'running', started_at = clock_timestamp() WHERE id = $1", &[&id]).await?;
            NextJob::Ready(id)
        } else {
            NextJob::Waiting
        }
    } else {
        NextJob::Idle
    };
    tx.commit().await?;
    Ok(job)
}

pub(crate) async fn finish_published_job(tx: &Transaction<'_>, job: i64) -> Result<()> {
    lock_queue(tx).await?;
    complete(tx, job, JobStatus::Published).await
}

#[derive(Clone, Copy)]
enum JobStatus {
    Deferred,
    Failed,
    Published,
}

async fn complete(tx: &Transaction<'_>, job: i64, status: JobStatus) -> Result<()> {
    let (state, outcome, event_status) = match status {
        JobStatus::Deferred => ("failed", "acquisition_deferred", "failed"),
        JobStatus::Failed => ("failed", "reconciliation_failed", "failed"),
        JobStatus::Published => ("succeeded", "published", "completed"),
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

async fn schedule_retry(writer: &mut Writer, job: i64, status: JobStatus) -> Result<()> {
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
        complete(&tx, job, status).await?;
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

async fn catalog_changed(
    upstream: &Upstream,
    accepted_bytes: u64,
    shutdown: &Shutdown,
) -> Result<bool> {
    // Use a short-lived session so the advisory lock is released on return.
    let deadline = Instant::now() + Duration::from_secs(600);
    let writer = shutdown.until(deadline, Writer::connect()).await?;
    let _corpus = shutdown
        .until(deadline, writer.acquire_coordinator())
        .await?;
    upstream.after_fence().await;
    let stored: Option<String> = shutdown.until(deadline, async {
        Ok(writer.client.query_opt(
            "SELECT encode(o.raw_digest, 'hex')
             FROM rfc.active_generation a JOIN rfc.generation_ingestion i ON i.generation_id = a.generation_id
             JOIN rfc.source_observations o ON o.identity = i.catalog_after WHERE a.singleton",
            &[],
        ).await?.map(|row| row.try_get(0)).transpose()?)
    }).await?;
    let capture = shutdown.until(deadline, upstream.catalog()).await?;
    if capture.bytes.len() as u64 > accepted_bytes {
        return Err("catalog probe exceeds accepted byte limit".into());
    }
    let _permit = upstream.parse_slot(shutdown).await?;
    let changed = tokio::task::spawn_blocking(move || {
        let digest = rfc_core::BlobDigest::hash(&capture.bytes).to_string();
        if stored.as_ref() == Some(&digest) {
            return Ok(false);
        }
        discovery::parse_catalog(&capture.bytes)?;
        Result::Ok(true)
    })
    .await??;
    shutdown.check()?;
    Ok(changed)
}

async fn worker(
    config: SyncConfig,
    upstream: &Upstream,
    schedule: &Schedule,
    shutdown: &Shutdown,
    manual: &Manual,
) -> Result<()> {
    let mut writer = Writer::connect().await?;
    let mut daily = Instant::now() + jitter(schedule.daily);
    let mut hourly = Instant::now() + jitter(schedule.hourly);
    while shutdown.check().is_ok() {
        if let Some(scope) = manual.claim() {
            let _ = report_execution(
                discovery::execute(scope, None, &config, upstream, shutdown).await,
                manual,
            );
            continue;
        }
        if Instant::now() >= daily && enqueue(&mut writer, None, false).await.is_ok() {
            daily = Instant::now() + jitter(schedule.daily);
        }
        match claim(&mut writer).await {
            Ok(NextJob::Ready(job)) => {
                let result = process_job(&config, upstream, job, shutdown, manual).await;
                if let Some(status) = result
                    && shutdown.check().is_ok()
                    && schedule_retry(&mut writer, job, status).await.is_err()
                {
                    sleep(LOOP_TICK).await;
                }
            }
            Ok(NextJob::Idle) if Instant::now() >= hourly => {
                hourly = Instant::now() + jitter(schedule.hourly);
                let changed =
                    catalog_changed(upstream, config.ingestion.max_accepted_bytes, shutdown).await;
                if shutdown.check().is_err() {
                    break;
                }
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

async fn process_job(
    config: &SyncConfig,
    upstream: &Upstream,
    job: i64,
    shutdown: &Shutdown,
    manual: &Manual,
) -> Option<JobStatus> {
    manual.start_automatic();
    let result = discovery::execute(Scope::Full, Some(job), config, upstream, shutdown).await;
    match report_execution(result, manual) {
        Ok(discovery::Outcome::Complete) => None,
        Ok(discovery::Outcome::Deferred) => Some(JobStatus::Deferred),
        Err(_) => Some(JobStatus::Failed),
    }
}

fn report_execution(
    result: Result<discovery::Execution>,
    manual: &Manual,
) -> Result<discovery::Outcome> {
    match result {
        Ok(execution) => {
            println!("{}", execution.report);
            manual.finish(execution.operator());
            execution.outcome
        }
        Err(error) => {
            manual.finish(Reply {
                success: false,
                report: r#"{"state":"acquisition_failed","error":"could not encode acquisition report"}"#.to_owned(),
            });
            Err(error)
        }
    }
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
