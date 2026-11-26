//! Exclusive Serve sessions. Physical slots outlive all driver cleanup.

use super::{
    Client, DatabaseFiles, GenerationReadGuard, GenerationSelection, Result, RuntimeRole, active,
    connect_transport, read_generation, verify,
};
use rfc_core::GenerationId;
use std::{
    fmt::{self, Write},
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    },
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    task::{AbortHandle, JoinHandle, JoinSet},
    time::{Instant, timeout_at},
};

const CONNECT_LIMIT: Duration = Duration::from_secs(10);
const MAINTENANCE: Duration = Duration::from_millis(100);
const WAIT_BUCKETS_MS: [u64; 7] = [1, 5, 10, 25, 100, 500, 1000];

/// Capacity reserved for a class is unavailable to the other classes.
#[derive(Clone, Copy)]
pub enum PoolClass {
    Public = 0,
    Readiness = 1,
    Notification = 2,
}

#[derive(Clone, Copy, Debug)]
pub struct PoolConfig {
    pub min_idle: usize,
    pub max_connections: usize,
    pub acquire_timeout_ms: u64,
    pub idle_timeout_seconds: u64,
    pub max_lifetime_seconds: u64,
    pub shutdown_timeout_ms: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_idle: 2,
            max_connections: 8,
            acquire_timeout_ms: 1000,
            idle_timeout_seconds: 300,
            max_lifetime_seconds: 1800,
            shutdown_timeout_ms: 2000,
        }
    }
}

impl PoolConfig {
    pub fn validate(self) -> io::Result<Self> {
        for (valid, key) in [
            ((3..=18).contains(&self.max_connections), "max_connections"),
            (self.min_idle <= self.max_connections, "min_idle"),
            (
                (1..=15_000).contains(&self.acquire_timeout_ms),
                "acquire_timeout_ms",
            ),
            (
                (1..=86_400).contains(&self.idle_timeout_seconds),
                "idle_timeout_seconds",
            ),
            (
                (self.idle_timeout_seconds..=86_400).contains(&self.max_lifetime_seconds)
                    && self.max_lifetime_seconds > 0,
                "max_lifetime_seconds",
            ),
            (
                (1..=5000).contains(&self.shutdown_timeout_ms),
                "shutdown_timeout_ms",
            ),
        ] {
            if !valid {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid database.pool.{key}"),
                ));
            }
        }
        Ok(self)
    }

    fn shutdown_timeout(self) -> Duration {
        Duration::from_millis(self.shutdown_timeout_ms)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum PoolError {
    Exhausted,
    Deadline,
    Unavailable,
}

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Exhausted => "database pool capacity exhausted",
            Self::Deadline => "database operation deadline elapsed",
            Self::Unavailable => "database pool unavailable",
        })
    }
}

impl std::error::Error for PoolError {}

/// Explicit transaction disposition. An interrupted page is already buffered and
/// can be returned even though the session must be discarded.
pub enum ReadOutcome<T> {
    Complete(T),
    Rejected(Box<dyn std::error::Error + Send + Sync>),
    Interrupted(T),
}

impl<T> ReadOutcome<T> {
    pub(super) fn into_result(self) -> Result<T> {
        match self {
            Self::Complete(value) | Self::Interrupted(value) => Ok(value),
            Self::Rejected(error) => Err(error),
        }
    }
}

#[derive(Clone)]
pub struct ServePool(Arc<Inner>);

struct Inner {
    config: PoolConfig,
    lanes: [Arc<Lane>; 3],
    material_key: [u8; 32],
    accepting: AtomicBool,
    warming: AtomicBool,
    stop_connecting: watch::Sender<bool>,
    stop_drivers: watch::Sender<Option<Instant>>,
    registrations: mpsc::Sender<Registration>,
    supervisor: Mutex<Option<JoinHandle<()>>>,
    maintenance: Mutex<Option<JoinHandle<()>>>,
    metrics: Arc<Metrics>,
}

struct Lane {
    slots: Arc<Semaphore>,
    capacity: usize,
    warm_target: usize,
    state: Mutex<LaneState>,
    changed: watch::Sender<()>,
}

#[derive(Default)]
struct LaneState {
    idle: Vec<Session>,
    connecting: bool,
    failures: u32,
    retry_at: Option<Instant>,
}

impl Lane {
    fn new(capacity: usize, warm_target: usize) -> Arc<Self> {
        Arc::new(Self {
            slots: Arc::new(Semaphore::new(capacity)),
            capacity,
            warm_target,
            state: Mutex::default(),
            changed: watch::channel(()).0,
        })
    }

    fn start(
        self: &Arc<Self>,
        accepting: &AtomicBool,
    ) -> std::result::Result<Option<Connecting>, PoolError> {
        let mut state = self.state.lock().expect("pool lane");
        if !accepting.load(Relaxed) {
            return Err(PoolError::Unavailable);
        }
        if state.connecting {
            return Ok(None);
        }
        if state
            .retry_at
            .is_some_and(|retry_at| retry_at > Instant::now())
        {
            return Err(PoolError::Unavailable);
        }
        let Ok(slot) = self.slots.clone().try_acquire_owned() else {
            return Ok(None);
        };
        state.connecting = true;
        Ok(Some(Connecting {
            lane: self.clone(),
            slot: Some(slot),
        }))
    }

    fn wake(&self) {
        self.changed.send_modify(|()| {});
    }
}

struct Connecting {
    lane: Arc<Lane>,
    slot: Option<OwnedSemaphorePermit>,
}

impl Connecting {
    fn completed(&self, success: bool, jitter: u64) {
        let mut state = self.lane.state.lock().expect("pool lane");
        if success {
            state.failures = 0;
            state.retry_at = None;
        } else {
            state.failures = state.failures.saturating_add(1);
            let delay = 100u64 << state.failures.min(5);
            state.retry_at =
                Some(Instant::now() + Duration::from_millis(delay + jitter % (delay / 4)));
        }
    }
}

impl Drop for Connecting {
    fn drop(&mut self) {
        drop(self.slot.take());
        self.lane.state.lock().expect("pool lane").connecting = false;
        self.lane.wake();
    }
}

type Material = blake3::Hash;

struct Session {
    client: Option<Client>,
    driver: AbortHandle,
    retire: Option<oneshot::Sender<Retirement>>,
    material: Material,
    born: Instant,
    idle_since: Instant,
    metrics: Arc<Metrics>,
}

impl Session {
    fn healthy(&self) -> bool {
        !self.driver.is_finished()
            && self
                .client
                .as_ref()
                .is_some_and(|client| !client.is_closed())
    }

    fn retire(mut self, deadline: Instant) {
        if let Some(sender) = self.retire.take() {
            let _ = sender.send(Retirement {
                deadline,
                _gauge: Gauge::new(self.metrics.clone(), MetricGauge::Retiring),
            });
        }
        drop(self.client.take());
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        if self.retire.is_some() {
            self.metrics.discarded.fetch_add(1, Relaxed);
            self.driver.abort();
        }
    }
}

struct Retirement {
    deadline: Instant,
    _gauge: Gauge,
}

struct Lease {
    session: Session,
    _gauge: Gauge,
}

struct Registration {
    driver: JoinHandle<Closure>,
    lane: Arc<Lane>,
    slot: Option<OwnedSemaphorePermit>,
}

impl Drop for Registration {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

#[derive(Clone, Copy)]
enum Closure {
    Graceful,
    Forced,
    Failed,
}

#[derive(Default)]
struct Metrics {
    leased: AtomicU64,
    retiring: AtomicU64,
    verifying: AtomicU64,
    waiters: AtomicU64,
    acquisitions: AtomicU64,
    reused: AtomicU64,
    established: AtomicU64,
    acquisition_timeouts: AtomicU64,
    verification_failures: AtomicU64,
    discarded: AtomicU64,
    graceful: AtomicU64,
    forced: AtomicU64,
    driver_failures: AtomicU64,
    wait_count: AtomicU64,
    wait_us: AtomicU64,
    wait_buckets: [AtomicU64; WAIT_BUCKETS_MS.len()],
}

enum MetricGauge {
    Leased,
    Retiring,
    Verifying,
    Waiters,
}

struct Gauge(Arc<Metrics>, MetricGauge);

impl Gauge {
    fn new(metrics: Arc<Metrics>, kind: MetricGauge) -> Self {
        let gauge = Self(metrics, kind);
        gauge.counter().fetch_add(1, Relaxed);
        gauge
    }

    fn counter(&self) -> &AtomicU64 {
        match self.1 {
            MetricGauge::Leased => &self.0.leased,
            MetricGauge::Retiring => &self.0.retiring,
            MetricGauge::Verifying => &self.0.verifying,
            MetricGauge::Waiters => &self.0.waiters,
        }
    }
}

impl Drop for Gauge {
    fn drop(&mut self) {
        self.counter().fetch_sub(1, Relaxed);
    }
}

impl ServePool {
    pub fn new(config: PoolConfig, notifications: bool) -> Result<Self> {
        let config = config.validate()?;
        let mut material_key = [0; 32];
        getrandom::fill(&mut material_key).map_err(io::Error::other)?;
        let public = config.max_connections - 1 - usize::from(notifications);
        let ready_warm = usize::from(config.min_idle >= 2);
        let notify_warm = usize::from(notifications && config.min_idle >= 3);
        let lanes = [
            Lane::new(public, config.min_idle - ready_warm - notify_warm),
            Lane::new(1, ready_warm),
            Lane::new(usize::from(notifications), notify_warm),
        ];
        let metrics = Arc::new(Metrics::default());
        let (registrations, receiver) = mpsc::channel(config.max_connections);
        let (stop_drivers, _) = watch::channel(None);
        let supervisor = tokio::spawn(supervise(
            receiver,
            stop_drivers.subscribe(),
            metrics.clone(),
        ));
        let pool = Self(Arc::new(Inner {
            config,
            lanes,
            material_key,
            accepting: AtomicBool::new(true),
            warming: AtomicBool::new(true),
            stop_connecting: watch::channel(false).0,
            stop_drivers,
            registrations,
            supervisor: Mutex::new(Some(supervisor)),
            maintenance: Mutex::default(),
            metrics,
        }));
        let weak = Arc::downgrade(&pool.0);
        let maintenance = tokio::spawn(async move {
            while let Some(inner) = weak.upgrade() {
                if !inner.warming.load(Relaxed) {
                    return;
                }
                let pool = Self(inner);
                tokio::join!(
                    pool.maintain(PoolClass::Public),
                    pool.maintain(PoolClass::Readiness),
                    pool.maintain(PoolClass::Notification),
                );
                drop(pool);
                tokio::time::sleep(MAINTENANCE).await;
            }
        });
        *pool.0.maintenance.lock().expect("pool maintenance") = Some(maintenance);
        Ok(pool)
    }

    /// Stop background connection creation while admitted requests drain.
    pub fn stop_warming(&self) {
        self.0.warming.store(false, Relaxed);
        if let Some(worker) = self
            .0
            .maintenance
            .lock()
            .expect("pool maintenance")
            .as_ref()
        {
            worker.abort();
        }
    }

    /// Close all sessions in parallel, retaining their physical slots until reaped.
    pub async fn shutdown(&self) {
        self.0.accepting.store(false, Relaxed);
        self.0.stop_connecting.send_replace(true);
        self.stop_warming();
        let maintenance = self.0.maintenance.lock().expect("pool maintenance").take();
        if let Some(worker) = maintenance {
            let _ = worker.await;
        }
        let deadline = Instant::now() + self.0.config.shutdown_timeout();
        for lane in &self.0.lanes {
            let mut changed = lane.changed.subscribe();
            while lane.state.lock().expect("pool lane").connecting {
                let _ = changed.changed().await;
            }
            let idle = std::mem::take(&mut lane.state.lock().expect("pool lane").idle);
            for session in idle {
                session.retire(deadline);
            }
            lane.wake();
        }
        self.0.stop_drivers.send_replace(Some(deadline));
        let supervisor = self.0.supervisor.lock().expect("pool supervisor").take();
        if let Some(worker) = supervisor {
            let _ = worker.await;
        }
    }

    pub async fn with_generation<T>(
        &self,
        class: PoolClass,
        deadline: Instant,
        selection: GenerationSelection,
        operation: impl for<'a, 'b> AsyncFnOnce(&'a GenerationReadGuard<'b>) -> Result<ReadOutcome<T>>,
    ) -> Result<T> {
        let mut lease = self.acquire(class, deadline).await?;
        let client = lease.session.client.as_mut().expect("leased client");
        let result = timeout_at(deadline, read_generation(client, selection, operation))
            .await
            .map_err(|_| PoolError::Deadline)??;
        if !matches!(result, ReadOutcome::Interrupted(_)) {
            self.release(class, lease.session);
        }
        result.into_result()
    }

    /// Authorize handoff of a buffered response under the database rights fence.
    pub async fn authorize_response<T>(
        &self,
        expected: i64,
        deadline: Instant,
        response: impl FnOnce() -> T,
    ) -> Result<Option<T>> {
        let class = PoolClass::Public;
        let mut lease = self.acquire(class, deadline).await?;
        let client = lease.session.client.as_mut().expect("leased client");
        let result = timeout_at(deadline, async {
            let tx = client.build_transaction().read_only(true).start().await?;
            tx.query_one(
                "SELECT pg_catalog.pg_advisory_xact_lock_shared($1)",
                &[&crate::policy::RIGHTS_LOCK],
            )
            .await?;
            let epoch: i64 = tx
                .query_one(
                    "SELECT rights_epoch FROM rfc.policy_state WHERE singleton",
                    &[],
                )
                .await?
                .try_get(0)?;
            let output = (epoch == expected).then(response);
            tx.commit().await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(output)
        })
        .await
        .map_err(|_| PoolError::Deadline)??;
        self.release(class, lease.session);
        Ok(result)
    }

    pub async fn active(&self, deadline: Instant) -> Result<(Option<GenerationId>, i64)> {
        let class = PoolClass::Notification;
        let mut lease = self.acquire(class, deadline).await?;
        let client = lease.session.client.as_mut().expect("leased client");
        let result = timeout_at(deadline, async {
            let tx = client.build_transaction().read_only(true).start().await?;
            let pointer = active(&tx).await?;
            tx.commit().await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(pointer)
        })
        .await
        .map_err(|_| PoolError::Deadline)??;
        self.release(class, lease.session);
        Ok(result)
    }

    fn read_material(&self) -> Result<(DatabaseFiles, Material)> {
        let files = DatabaseFiles::read()?;
        let mut hash = blake3::Hasher::new_keyed(&self.0.material_key);
        hash.update(&(files.password.len() as u64).to_le_bytes());
        hash.update(&files.password);
        hash.update(&files.ca);
        Ok((files, hash.finalize()))
    }

    fn retire_mismatched(&self, material: Option<Material>) {
        let deadline = Instant::now() + self.0.config.shutdown_timeout();
        for lane in &self.0.lanes {
            let mut state = lane.state.lock().expect("pool lane");
            let mut index = 0;
            while index < state.idle.len() {
                if Some(state.idle[index].material) == material {
                    index += 1;
                } else {
                    state.idle.remove(index).retire(deadline);
                }
            }
        }
    }

    async fn acquire(&self, class: PoolClass, deadline: Instant) -> Result<Lease> {
        let now = Instant::now();
        let wait_deadline = now + Duration::from_millis(self.0.config.acquire_timeout_ms);
        let lane = &self.0.lanes[class as usize];
        if lane.capacity == 0 {
            return Err(PoolError::Unavailable.into());
        }
        let mut changed = lane.changed.subscribe();
        let mut waiter = Some(Gauge::new(self.0.metrics.clone(), MetricGauge::Waiters));
        loop {
            if !self.0.accepting.load(Relaxed) {
                return Err(PoolError::Unavailable.into());
            }
            if Instant::now() >= deadline {
                return Err(PoolError::Deadline.into());
            }
            if Instant::now() >= wait_deadline {
                self.record_wait(now);
                self.0.metrics.acquisition_timeouts.fetch_add(1, Relaxed);
                return Err(PoolError::Exhausted.into());
            }
            let (files, material) = match self.read_material() {
                Ok(material) => material,
                Err(_) => {
                    self.retire_mismatched(None);
                    return Err(PoolError::Unavailable.into());
                }
            };
            self.retire_mismatched(Some(material));
            let idle = lane.state.lock().expect("pool lane").idle.pop();
            let (session, reused) = if let Some(mut session) = idle {
                if !session.healthy() {
                    continue;
                }
                if self.expired(&session) {
                    session.retire(Instant::now() + self.0.config.shutdown_timeout());
                    continue;
                }
                drop(waiter.take());
                self.record_wait(now);
                self.check(&mut session, deadline).await?;
                (session, true)
            } else if let Some(connecting) = lane.start(&self.0.accepting)? {
                drop(waiter.take());
                self.record_wait(now);
                (
                    self.connect(connecting, files, material, deadline).await?,
                    false,
                )
            } else {
                let _ = timeout_at(deadline.min(wait_deadline), changed.changed()).await;
                continue;
            };
            if !self.0.accepting.load(Relaxed) {
                return Err(PoolError::Unavailable.into());
            }
            let current = self.read_material().map(|(_, current)| current);
            if current.ok() != Some(session.material) {
                session.retire(Instant::now() + self.0.config.shutdown_timeout());
                return Err(PoolError::Unavailable.into());
            }
            self.0.metrics.acquisitions.fetch_add(1, Relaxed);
            if reused {
                self.0.metrics.reused.fetch_add(1, Relaxed);
            }
            return Ok(Lease {
                session,
                _gauge: Gauge::new(self.0.metrics.clone(), MetricGauge::Leased),
            });
        }
    }

    async fn connect(
        &self,
        mut connecting: Connecting,
        files: DatabaseFiles,
        material: Material,
        deadline: Instant,
    ) -> Result<Session> {
        let born = Instant::now();
        let deadline = deadline.min(born + CONNECT_LIMIT);
        let mut stopping = self.0.stop_connecting.subscribe();
        let establish = async {
            let (client, mut connection) = connect_transport(RuntimeRole::Serve, files)
                .await
                .map_err(|_| PoolError::Unavailable)?;
            let (retire, retirement) = oneshot::channel::<Retirement>();
            let mut stop = self.0.stop_drivers.subscribe();
            let driver = tokio::spawn(async move {
                tokio::select! {
                    biased;
                    Ok(retirement) = retirement => {
                        match timeout_at(retirement.deadline, &mut connection).await {
                            Ok(Ok(())) => Closure::Graceful,
                            Ok(Err(_)) => Closure::Failed,
                            Err(_) => Closure::Forced,
                        }
                    }
                    _ = async { let _ = stop.wait_for(Option::is_some).await; } => Closure::Forced,
                    _ = &mut connection => Closure::Failed,
                }
            });
            let mut session = Session {
                client: Some(client),
                driver: driver.abort_handle(),
                retire: Some(retire),
                material,
                born,
                idle_since: born,
                metrics: self.0.metrics.clone(),
            };
            self.0
                .registrations
                .try_send(Registration {
                    driver,
                    lane: connecting.lane.clone(),
                    slot: connecting.slot.take(),
                })
                .map_err(|_| PoolError::Unavailable)?;
            self.0.metrics.established.fetch_add(1, Relaxed);
            self.check(&mut session, deadline).await?;
            Ok::<_, PoolError>(session)
        };
        let result = tokio::select! {
            biased;
            _ = async { let _ = stopping.wait_for(|stop| *stop).await; } => Err(PoolError::Unavailable),
            result = timeout_at(deadline, establish) => result.unwrap_or(Err(PoolError::Deadline)),
        };
        let mut jitter = [0; 8];
        let _ = getrandom::fill(&mut jitter);
        connecting.completed(result.is_ok(), u64::from_le_bytes(jitter));
        Ok(result?)
    }

    async fn check(
        &self,
        session: &mut Session,
        deadline: Instant,
    ) -> std::result::Result<(), PoolError> {
        let _verifying = Gauge::new(self.0.metrics.clone(), MetricGauge::Verifying);
        let client = session.client.as_mut().expect("session client");
        let result = timeout_at(deadline, async {
            let tx = client.build_transaction().read_only(true).start().await?;
            verify(&tx, RuntimeRole::Serve).await?;
            tx.commit().await?;
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .await;
        if matches!(result, Ok(Ok(()))) && session.healthy() {
            return Ok(());
        }
        self.0.metrics.verification_failures.fetch_add(1, Relaxed);
        match result {
            Err(_) => Err(PoolError::Deadline),
            Ok(Err(error))
                if error
                    .downcast_ref::<tokio_postgres::Error>()
                    .and_then(tokio_postgres::Error::code)
                    .is_some_and(|code| {
                        use tokio_postgres::error::SqlState;
                        *code == SqlState::QUERY_CANCELED || *code == SqlState::LOCK_NOT_AVAILABLE
                    }) =>
            {
                Err(PoolError::Deadline)
            }
            Ok(_) => Err(PoolError::Unavailable),
        }
    }

    fn expired(&self, session: &Session) -> bool {
        session.born.elapsed() >= Duration::from_secs(self.0.config.max_lifetime_seconds)
    }

    fn release(&self, class: PoolClass, mut session: Session) {
        let lane = &self.0.lanes[class as usize];
        let mut state = lane.state.lock().expect("pool lane");
        if session.healthy() {
            if self.0.accepting.load(Relaxed) && !self.expired(&session) {
                session.idle_since = Instant::now();
                state.idle.push(session);
            } else {
                session.retire(Instant::now() + self.0.config.shutdown_timeout());
            }
        }
        drop(state);
        lane.wake();
    }

    async fn maintain(&self, class: PoolClass) {
        let lane = &self.0.lanes[class as usize];
        if lane.capacity == 0 {
            return;
        }
        {
            let mut state = lane.state.lock().expect("pool lane");
            let mut excess = state.idle.len().saturating_sub(lane.warm_target);
            let mut index = 0;
            while index < state.idle.len() {
                let session = &state.idle[index];
                let idle_expired = excess > 0
                    && session.idle_since.elapsed()
                        >= Duration::from_secs(self.0.config.idle_timeout_seconds);
                if !session.healthy() {
                    drop(state.idle.remove(index));
                } else if self.expired(session) || idle_expired {
                    state
                        .idle
                        .remove(index)
                        .retire(Instant::now() + self.0.config.shutdown_timeout());
                } else {
                    index += 1;
                    continue;
                }
                excess = excess.saturating_sub(1);
            }
            if state.idle.len() >= lane.warm_target {
                return;
            }
        }
        if let Ok(Some(connecting)) = lane.start(&self.0.accepting) {
            if let Ok((files, material)) = self.read_material() {
                if let Ok(session) = self
                    .connect(connecting, files, material, Instant::now() + CONNECT_LIMIT)
                    .await
                {
                    self.release(class, session);
                }
            } else {
                connecting.completed(false, 0);
                self.retire_mismatched(None);
            }
        }
    }

    fn record_wait(&self, start: Instant) {
        let metrics = &self.0.metrics;
        let elapsed = start.elapsed();
        metrics.wait_count.fetch_add(1, Relaxed);
        metrics
            .wait_us
            .fetch_add(elapsed.as_micros() as u64, Relaxed);
        for (index, bound) in WAIT_BUCKETS_MS.iter().enumerate() {
            if elapsed <= Duration::from_millis(*bound) {
                metrics.wait_buckets[index].fetch_add(1, Relaxed);
            }
        }
    }

    pub fn metrics(&self, output: &mut String) {
        let mut physical = 0;
        let mut idle = 0;
        let mut connecting = 0;
        for lane in &self.0.lanes {
            let state = lane.state.lock().expect("pool lane");
            physical += lane.capacity - lane.slots.available_permits();
            idle += state.idle.len();
            connecting += usize::from(state.connecting);
        }
        let m = &self.0.metrics;
        for (name, value) in [
            ("capacity", self.0.config.max_connections as u64),
            ("physical", physical as u64),
            ("idle", idle as u64),
            ("connecting", connecting as u64),
            ("leased", m.leased.load(Relaxed)),
            ("retiring", m.retiring.load(Relaxed)),
            ("verifying", m.verifying.load(Relaxed)),
            ("waiters", m.waiters.load(Relaxed)),
            ("acquisitions_total", m.acquisitions.load(Relaxed)),
            ("reused_total", m.reused.load(Relaxed)),
            ("established_total", m.established.load(Relaxed)),
            (
                "acquisition_timeouts_total",
                m.acquisition_timeouts.load(Relaxed),
            ),
            (
                "verification_failures_total",
                m.verification_failures.load(Relaxed),
            ),
            ("discarded_total", m.discarded.load(Relaxed)),
            ("graceful_closes_total", m.graceful.load(Relaxed)),
            ("forced_closes_total", m.forced.load(Relaxed)),
            ("driver_failures_total", m.driver_failures.load(Relaxed)),
        ] {
            let _ = writeln!(output, "rfc_mcp_db_pool_{name} {value}");
        }
        for (index, bound) in WAIT_BUCKETS_MS.iter().enumerate() {
            let count = m.wait_buckets[index].load(Relaxed);
            let _ = writeln!(
                output,
                "rfc_mcp_db_pool_wait_seconds_bucket{{le=\"{}\"}} {count}",
                *bound as f64 / 1000.0
            );
        }
        let count = m.wait_count.load(Relaxed);
        let sum = m.wait_us.load(Relaxed) as f64 / 1_000_000.0;
        let _ = writeln!(
            output,
            "rfc_mcp_db_pool_wait_seconds_bucket{{le=\"+Inf\"}} {count}\nrfc_mcp_db_pool_wait_seconds_count {count}\nrfc_mcp_db_pool_wait_seconds_sum {sum}"
        );
    }
}

async fn supervise(
    mut registrations: mpsc::Receiver<Registration>,
    mut stopping: watch::Receiver<Option<Instant>>,
    metrics: Arc<Metrics>,
) {
    let mut drivers = JoinSet::new();
    let mut closed = false;
    let mut last_diagnostic = None;
    loop {
        tokio::select! {
            registering = registrations.recv(), if !closed => {
                match registering {
                    Some(mut registered) => {
                        drivers.spawn(async move {
                            let outcome = (&mut registered.driver).await.unwrap_or(Closure::Forced);
                            drop(registered.slot.take());
                            registered.lane.wake();
                            outcome
                        });
                    }
                    None => closed = true,
                }
            }
            _ = stopping.changed(), if !closed && !registrations.is_closed() => {
                registrations.close();
            }
            Some(result) = drivers.join_next(), if !drivers.is_empty() => {
                match result.unwrap_or(Closure::Forced) {
                    Closure::Graceful => &metrics.graceful,
                    Closure::Forced => &metrics.forced,
                    Closure::Failed => {
                        let now = Instant::now();
                        if last_diagnostic.is_none_or(|last| now - last >= Duration::from_secs(5)) {
                            eprintln!("serve_database_driver outcome=unexpected_exit");
                            last_diagnostic = Some(now);
                        }
                        &metrics.driver_failures
                    }
                }.fetch_add(1, Relaxed);
            }
            else => break,
        }
        if closed && drivers.is_empty() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_bounds_leave_capacity_for_all_classes() {
        let defaults = PoolConfig::default();
        assert!(defaults.validate().is_ok());
        for config in [
            PoolConfig {
                max_connections: 2,
                ..defaults
            },
            PoolConfig {
                min_idle: 9,
                ..defaults
            },
            PoolConfig {
                acquire_timeout_ms: 0,
                ..defaults
            },
            PoolConfig {
                idle_timeout_seconds: 0,
                ..defaults
            },
            PoolConfig {
                max_lifetime_seconds: 299,
                ..defaults
            },
            PoolConfig {
                shutdown_timeout_ms: 5001,
                ..defaults
            },
        ] {
            assert!(config.validate().is_err(), "{config:?}");
        }
    }

    #[tokio::test]
    async fn cancelled_connection_attempt_releases_only_its_own_slot() {
        let accepting = AtomicBool::new(true);
        let lane = Lane::new(2, 0);
        let reserved = Lane::new(1, 0);
        let first = lane.start(&accepting).unwrap().unwrap();
        assert!(lane.start(&accepting).unwrap().is_none());
        let private = reserved.start(&accepting).unwrap().unwrap();
        drop(first);
        assert_eq!(lane.slots.available_permits(), 2);
        assert_eq!(reserved.slots.available_permits(), 0);
        assert!(lane.start(&accepting).unwrap().is_some());
        drop(private);
    }

    #[tokio::test]
    async fn physical_slot_is_released_after_driver_exit() {
        let lane = Lane::new(1, 0);
        let mut registered = lane.start(&AtomicBool::new(true)).unwrap().unwrap();
        let (registrations, receiver) = mpsc::channel(1);
        let (stop, stopping) = watch::channel(None);
        let metrics = Arc::new(Metrics::default());
        let supervisor = tokio::spawn(supervise(receiver, stopping, metrics.clone()));
        let (exit, closed) = oneshot::channel();
        let driver = tokio::spawn(async move {
            closed.await.unwrap();
            Closure::Graceful
        });
        registrations
            .try_send(Registration {
                driver,
                lane: lane.clone(),
                slot: registered.slot.take(),
            })
            .ok()
            .unwrap();
        drop(registered);
        assert_eq!(lane.slots.available_permits(), 0);
        stop.send_replace(Some(Instant::now() + Duration::from_secs(1)));
        exit.send(()).unwrap();
        supervisor.await.unwrap();
        assert_eq!(lane.slots.available_permits(), 1);
        assert_eq!(metrics.graceful.load(Relaxed), 1);
    }
}
