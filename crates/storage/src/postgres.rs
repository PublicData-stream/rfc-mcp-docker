//! PostgreSQL connection checks and the read-only corpus interface.

use crate::{
    blob::BlobReader,
    manifest::{Audit, Coverage, Edge, Ingestion, Manifest, Member},
};
use rfc_core::{BlobDigest, ByteRange, GenerationId, RfcNumber};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::env;
use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::{Instant, timeout, timeout_at};
use tokio_postgres::{
    Client, Connection, GenericClient, IsolationLevel, Socket, Transaction, config::SslMode,
};
use tokio_postgres_rustls::MakeRustlsConnect;

mod pool;
pub use pool::{PoolClass, PoolConfig, PoolError, ReadOutcome, ServePool};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SCHEMA_VERSIONS: [i32; 13] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];
/// The latest migration required by this build.
pub const STORAGE_SCHEMA_VERSION: i32 = SCHEMA_VERSIONS[SCHEMA_VERSIONS.len() - 1];
const READ_DEADLINE: Duration = Duration::from_secs(15);
const PUBLIC_READS: &[&str] = &[
    "schema_migrations",
    "blobs",
    "source_observations",
    "rfc_identities",
    "document_versions",
    "generations",
    "active_generation",
    "generation_documents",
    "sections",
    "text_chunks",
    "notice_ranges",
    "relation_evidence",
    "rfc_relations",
    "edge_evidence",
    "relation_coverage",
    "document_lexemes",
    "generation_ingestion",
    "generation_audit",
    "generation_source_evidence",
    "generation_policy",
    "policy_receipts",
    "source_rights",
    "source_restrictions",
];

/// Expected role for the executable opening a storage repository.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum RuntimeRole {
    Serve,
    Sync,
}

impl RuntimeRole {
    fn name(self) -> &'static str {
        match self {
            Self::Serve => "rfc_serve",
            Self::Sync => "rfc_sync",
        }
    }

    fn privileges(self) -> Vec<String> {
        let mut grants: Vec<_> = PUBLIC_READS
            .iter()
            .map(|name| format!("{name}:SELECT"))
            .collect();
        match self {
            Self::Serve => grants.extend(
                [
                    "pg_advisory_xact_lock_shared(bigint):EXECUTE",
                    "policy_state.singleton:SELECT",
                    "policy_state.rights_epoch:SELECT",
                    "policy_restrictions.rfc_id:SELECT",
                    "policy_restrictions.ceiling:SELECT",
                    "policy_restrictions.epoch:SELECT",
                    "policy_restrictions.denied:SELECT",
                ]
                .map(str::to_owned),
            ),
            Self::Sync => {
                for name in [
                    "sync_jobs",
                    "control_events",
                    "notification_outbox",
                    "blob_deletion_queue",
                    "ingestion_checkpoints",
                    "policy_state",
                    "policy_restrictions",
                ] {
                    grants.push(format!("{name}:SELECT"));
                }
                for name in [
                    "source_observations",
                    "rfc_identities",
                    "generation_documents",
                    "sections",
                    "notice_ranges",
                    "relation_evidence",
                    "rfc_relations",
                    "edge_evidence",
                    "relation_coverage",
                    "control_events",
                    "document_lexemes",
                    "generation_ingestion",
                    "generation_audit",
                    "generation_source_evidence",
                    "source_rights",
                ] {
                    grants.push(format!("{name}:INSERT"));
                }
                for (name, privilege, columns) in [
                    ("blobs", "INSERT", "digest byte_length"),
                    (
                        "document_versions",
                        "INSERT",
                        "identity rfc_id txt_digest byte_length parser_version index_version encoding indexing_state notice_state notice_policy index_identity section_state line_starts",
                    ),
                    ("document_versions", "UPDATE", "sealed"),
                    ("source_rights", "UPDATE", "identity evidence"),
                    (
                        "text_chunks",
                        "INSERT",
                        "document_version blob_length start_byte end_byte search_text",
                    ),
                    ("generations", "INSERT", "parser_version index_version"),
                    (
                        "generations",
                        "UPDATE",
                        "state manifest_format manifest_bytes manifest_digest",
                    ),
                    (
                        "sync_jobs",
                        "INSERT",
                        "generation_id state trigger_id outcome diagnostic started_at finished_at",
                    ),
                    (
                        "sync_jobs",
                        "UPDATE",
                        "generation_id state trigger_id outcome diagnostic started_at finished_at expected_publication_sequence retries next_attempt_at",
                    ),
                    ("control_events", "UPDATE", "status job_id"),
                    (
                        "notification_outbox",
                        "UPDATE",
                        "delivery_state attempts next_attempt_at delivered_at last_error",
                    ),
                ] {
                    grants.extend(
                        columns
                            .split_ascii_whitespace()
                            .map(|column| format!("{name}.{column}:{privilege}")),
                    );
                }
                grants.extend(
                    [
                        "generations_id_seq:USAGE",
                        "sync_jobs_id_seq:USAGE",
                        "pg_try_advisory_xact_lock(bigint):EXECUTE",
                        "pg_try_advisory_lock(bigint):EXECUTE",
                        "generation_blobs(bigint):EXECUTE",
                        "promote_generation(bigint,bigint,boolean):EXECUTE",
                        "retire_generation(bigint,integer,integer,boolean):EXECUTE",
                        "mark_unreachable_blobs():EXECUTE",
                        "prune_ingestion_checkpoints(text,bytea):EXECUTE",
                        "remove_ingestion_checkpoints(text,bytea,integer,text):EXECUTE",
                        "store_ingestion_checkpoint(text,bytea,integer,text,bytea):EXECUTE",
                        "store_source_checkpoint(text,bytea,integer,text,bytea,text,bytea):EXECUTE",
                        "queue_orphan_blobs(bytea[]):EXECUTE",
                        "finish_blob_deletion(bytea):EXECUTE",
                        "deny_source_rights(integer,integer,text,text,bigint):EXECUTE",
                        "restrict_policy(integer,text,text,text,bigint):EXECUTE",
                    ]
                    .map(str::to_owned),
                );
            }
        }
        grants
    }
}

/// Opens and verifies a dedicated PostgreSQL session. Writer construction lives in sync.
pub async fn connect(role: RuntimeRole) -> Result<Client> {
    Ok(open_session(role).await?.0)
}

async fn open_session(role: RuntimeRole) -> Result<(Client, tokio::task::JoinHandle<()>)> {
    let deadline = Instant::now() + Duration::from_secs(10);
    timeout_at(deadline, connect_and_verify(role, deadline)).await?
}

async fn connect_and_verify(
    role: RuntimeRole,
    deadline: Instant,
) -> Result<(Client, tokio::task::JoinHandle<()>)> {
    let (client, connection) = connect_transport(role, DatabaseFiles::read()?).await?;
    struct AbortOnDrop(Option<tokio::task::JoinHandle<()>>);
    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            if let Some(driver) = &self.0 {
                driver.abort();
            }
        }
    }
    let mut driver = AbortOnDrop(Some(tokio::spawn(async move {
        let _ = connection.await;
    })));
    if let Err(error) = verify(&client, role).await {
        drop(client);
        let connection = driver.0.as_mut().expect("driver is present");
        if timeout_at(deadline, connection).await.is_ok() {
            driver.0.take();
        }
        return Err(error);
    }
    Ok((client, driver.0.take().expect("driver is present")))
}

/// Check storage as the runtime role without opening the mirror or starting services.
pub async fn check_storage_schema(role: RuntimeRole) -> Result<()> {
    let (client, mut driver) = open_session(role).await.map_err(|error| {
        format!(
            "{} database preflight failed: {error}. Verify the image and database, then follow the initialization or upgrade procedure in docs/deployment.md.",
            role.name()
        )
    })?;
    drop(client);
    match timeout(Duration::from_secs(1), &mut driver).await {
        Ok(result) => result?,
        Err(_) => {
            driver.abort();
            return Err("database preflight connection did not close".into());
        }
    }
    println!(
        "{} storage schema {STORAGE_SCHEMA_VERSION} verified",
        role.name()
    );
    Ok(())
}

struct DatabaseFiles {
    password: Vec<u8>,
    ca: Vec<u8>,
}

impl DatabaseFiles {
    fn read() -> Result<Self> {
        let password = PathBuf::from(
            env::var_os("RFC_DB_PASSWORD_FILE").ok_or("RFC_DB_PASSWORD_FILE is required")?,
        );
        let ca = PathBuf::from(env::var_os("RFC_DB_CA_FILE").ok_or("RFC_DB_CA_FILE is required")?);
        Ok(Self {
            password: read_database_file(password, 4098)?,
            ca: read_database_file(ca, 1024 * 1024)?,
        })
    }
}

fn read_database_file(path: PathBuf, limit: u64) -> Result<Vec<u8>> {
    use rustix::fs::{Mode, OFlags, open};
    let file = File::from(open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
    )?);
    if !file.metadata()?.is_file() {
        return Err("database credential path is not a regular file".into());
    }
    let mut bytes = Vec::new();
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err("database credential file exceeds its limit".into());
    }
    Ok(bytes)
}

async fn connect_transport(
    role: RuntimeRole,
    files: DatabaseFiles,
) -> Result<(
    Client,
    Connection<Socket, <MakeRustlsConnect as tokio_postgres::tls::MakeTlsConnect<Socket>>::Stream>,
)> {
    let host = env::var("RFC_DB_HOST")?;
    let port = env::var("RFC_DB_PORT")
        .ok()
        .map(|port| port.parse())
        .transpose()?
        .unwrap_or(5432);
    let user = env::var("RFC_DB_USER").unwrap_or_else(|_| role.name().to_owned());
    let DatabaseFiles { password, ca } = files;
    let password = match password.strip_suffix(b"\n") {
        Some(line) => line.strip_suffix(b"\r").unwrap_or(line),
        None => password.as_slice(),
    };
    if password.is_empty() || password.len() > 4096 {
        return Err("invalid database password file length".into());
    }
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(&ca) {
        roots.add(cert?)?;
    }
    if roots.is_empty() {
        return Err("database CA file contains no certificates".into());
    }
    let tls = rustls::ClientConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    let mut options = String::from(
        "-csearch_path=pg_catalog,rfc -cstatement_timeout=5000 -clock_timeout=3000 -cidle_in_transaction_session_timeout=10000",
    );
    if role == RuntimeRole::Serve {
        // PostgreSQL must check the socket even while a read is blocked on a lock.
        options.push_str(" -cclient_connection_check_interval=100ms");
    }
    Ok(tokio_postgres::Config::new()
        .host(&host)
        .port(port)
        .user(&user)
        .password(password)
        .dbname("rfc")
        .ssl_mode(SslMode::Require)
        .connect_timeout(Duration::from_secs(5))
        .options(&options)
        .connect(MakeRustlsConnect::new(tls))
        .await?)
}

async fn verify(client: &impl GenericClient, role: RuntimeRole) -> Result<()> {
    let valid: bool = client.query_one(
        "SELECT current_user::text = $1 AND session_user::text = $1 AND current_database() = 'rfc'
          AND current_setting('server_version_num')::integer / 10000 = 19
          AND current_setting('default_transaction_read_only')::boolean = $2
          AND NOT (r.rolsuper OR r.rolinherit OR r.rolcreaterole OR r.rolcreatedb
                   OR r.rolreplication OR r.rolbypassrls) AND r.rolcanlogin
          AND NOT EXISTS (SELECT FROM pg_auth_members WHERE member = r.oid)
          AND NOT has_database_privilege(current_database(), 'CREATE,TEMPORARY')
          AND has_database_privilege(current_database(), 'CONNECT')
          AND NOT has_database_privilege(current_database(), 'CONNECT WITH GRANT OPTION')
          AND has_schema_privilege('rfc', 'USAGE')
          AND NOT has_schema_privilege('rfc', 'USAGE WITH GRANT OPTION')
          AND NOT EXISTS (SELECT FROM pg_namespace WHERE has_schema_privilege(oid, 'CREATE'))
          AND NOT EXISTS (
              SELECT FROM pg_namespace
              WHERE nspname !~ '^pg_' AND nspname NOT IN ('rfc', 'information_schema')
                AND has_schema_privilege(oid, 'USAGE')
          )
          AND coalesce((SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()), false)
         FROM pg_roles r WHERE r.rolname = current_user",
        &[&role.name(), &(role == RuntimeRole::Serve)],
    ).await?.get(0);
    if !valid {
        return Err(
            "database identity, role attributes, TLS, or database/schema privileges are incorrect"
                .into(),
        );
    }
    let versions: Vec<i32> = client
        .query(
            "SELECT version FROM rfc.schema_migrations ORDER BY version",
            &[],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    if versions != SCHEMA_VERSIONS {
        return Err(format!(
            "unsupported storage schema version for {}: expected {:?}, observed {:?}",
            role.name(),
            SCHEMA_VERSIONS,
            versions
        )
        .into());
    }
    let differences = client.query(PRIVILEGE_CHECK, &[&role.privileges()]).await?;
    if !differences.is_empty() {
        let differences: Vec<&str> = differences.iter().map(|row| row.get(0)).collect();
        return Err(format!(
            "unexpected effective privileges: {}",
            differences.join(", ")
        )
        .into());
    }
    Ok(())
}

const PRIVILEGE_CHECK: &str = "
WITH relations AS (
    SELECT c.oid, c.relname, c.relkind
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'rfc' AND c.relkind IN ('r','p','v','m','f','S')
), actual AS (
    SELECT relname || ':' || privilege FROM relations
    CROSS JOIN unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER','MAINTAIN']) privilege
    WHERE relkind <> 'S' AND has_table_privilege(oid, privilege)
    UNION ALL
    SELECT relname || '.' || attname || ':' || privilege FROM relations
    JOIN pg_attribute a ON attrelid = oid AND attnum > 0 AND NOT attisdropped
    CROSS JOIN unnest(ARRAY['SELECT','INSERT','UPDATE','REFERENCES']) privilege
    WHERE relkind <> 'S' AND has_column_privilege(oid, attnum, privilege)
      AND NOT has_table_privilege(oid, privilege)
    UNION ALL
    SELECT relname || ':' || privilege FROM relations
    CROSS JOIN unnest(ARRAY['SELECT','UPDATE','USAGE']) privilege
    WHERE relkind = 'S' AND has_sequence_privilege(oid, privilege)
    UNION ALL
    SELECT p.oid::regprocedure::text || ':EXECUTE'
    FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE has_function_privilege(p.oid, 'EXECUTE') AND (
        n.nspname = 'rfc' OR (n.nspname = 'pg_catalog' AND
            (p.proname LIKE 'pg%advisory%lock%' OR p.proname IN
             ('pg_notify','lo_create','lo_creat','lo_from_bytea','lo_import','lo_export','lo_open','lo_put',
              'lowrite','lo_truncate','lo_truncate64','lo_unlink','pg_read_file','pg_read_binary_file',
              'pg_ls_dir','pg_stat_file','pg_reload_conf','pg_rotate_logfile'))))
), disallowed_grant_options AS (
    SELECT relname || ':GRANT OPTION' FROM relations WHERE
        CASE WHEN relkind = 'S' THEN has_sequence_privilege(oid, 'SELECT WITH GRANT OPTION,UPDATE WITH GRANT OPTION,USAGE WITH GRANT OPTION')
        ELSE has_table_privilege(oid, 'SELECT WITH GRANT OPTION,INSERT WITH GRANT OPTION,UPDATE WITH GRANT OPTION,DELETE WITH GRANT OPTION,
                                       TRUNCATE WITH GRANT OPTION,TRIGGER WITH GRANT OPTION,MAINTAIN WITH GRANT OPTION')
          OR has_any_column_privilege(oid, 'SELECT WITH GRANT OPTION,INSERT WITH GRANT OPTION,UPDATE WITH GRANT OPTION,REFERENCES WITH GRANT OPTION')
        END
    UNION ALL
    SELECT p.oid::regprocedure::text || ':GRANT OPTION' FROM pg_proc p
    WHERE has_function_privilege(p.oid, 'EXECUTE WITH GRANT OPTION')
), expected AS (SELECT unnest($1::text[]))
SELECT * FROM ((SELECT * FROM actual EXCEPT SELECT * FROM expected)
               UNION (SELECT * FROM expected EXCEPT SELECT * FROM actual)
               UNION SELECT * FROM disallowed_grant_options) diff
ORDER BY 1";

/// A reader connection that has verified its SELECT-only privilege boundary.
pub struct Reader {
    client: Client,
    driver: tokio::task::JoinHandle<()>,
}

impl Drop for Reader {
    fn drop(&mut self) {
        // PostgreSQL's connection checks observe closure even during a blocked read.
        self.driver.abort();
    }
}

#[derive(Clone, Copy)]
pub enum GenerationSelection {
    Active,
    Pinned(GenerationId),
}

#[derive(Debug)]
pub struct GenerationUnavailable;

impl std::fmt::Display for GenerationUnavailable {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("generation_unavailable")
    }
}

impl std::error::Error for GenerationUnavailable {}

tokio::task_local! {
    static OBSERVED_RIGHTS: std::cell::Cell<Option<i64>>;
}

/// Capture the rights epoch of the generation read by a buffered MCP call.
pub async fn observe_rights<T>(
    operation: impl std::future::Future<Output = T>,
) -> (T, Option<i64>) {
    OBSERVED_RIGHTS
        .scope(std::cell::Cell::new(None), async {
            let result = operation.await;
            (result, OBSERVED_RIGHTS.with(std::cell::Cell::get))
        })
        .await
}

/// Keeps both the database snapshot selection and the mirror fence alive.
pub struct GenerationReadGuard<'a> {
    tx: Transaction<'a>,
    mirror: &'a BlobReader,
    _fence: File,
    generation: GenerationId,
    sequence: i64,
    rights_epoch: i64,
}

/// An original file selected through generation membership. The mirror fence
/// remains held if a bounded blocking read outlives its requesting task.
pub struct GuardedOriginal {
    file: File,
    digest: BlobDigest,
    _fence: File,
}

impl GuardedOriginal {
    pub fn read_verified(self, expected_length: usize) -> io::Result<Vec<u8>> {
        let length = u64::try_from(expected_length).map_err(io::Error::other)?;
        if self.file.metadata()?.len() != length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Original length differs",
            ));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(expected_length)
            .map_err(io::Error::other)?;
        let read_limit = length.checked_add(1).ok_or(io::ErrorKind::InvalidInput)?;
        self.file.take(read_limit).read_to_end(&mut bytes)?;
        if bytes.len() != expected_length || BlobDigest::hash(&bytes) != self.digest {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Original content differs",
            ));
        }
        Ok(bytes)
    }
}

impl GenerationReadGuard<'_> {
    pub fn generation(&self) -> GenerationId {
        self.generation
    }

    pub fn publication_sequence(&self) -> i64 {
        self.sequence
    }

    pub fn rights_epoch(&self) -> i64 {
        self.rights_epoch
    }

    /// Bind generation-scoped metadata queries to `generation()`.
    pub fn transaction(&self) -> &Transaction<'_> {
        &self.tx
    }

    async fn document_digest(&self, rfc: RfcNumber) -> Result<BlobDigest> {
        let generation = i64::try_from(self.generation.get())?;
        let number = i32::try_from(rfc.get())?;
        let row = self
            .tx
            .query_opt(
                "SELECT d.txt_digest::bytea FROM rfc.generation_documents m
             JOIN rfc.document_versions d ON d.identity = m.document_version
             WHERE m.generation_id = $1 AND m.rfc_id = $2::integer",
                &[&generation, &number],
            )
            .await?
            .ok_or("RFC is absent from the guarded generation")?;
        Ok(BlobDigest::from_bytes(
            row.try_get::<_, &[u8]>(0)?.try_into()?,
        ))
    }

    pub async fn read_document(
        &self,
        rfc: RfcNumber,
        range: ByteRange,
        limit: usize,
    ) -> Result<Vec<u8>> {
        let digest = self.document_digest(rfc).await?;
        Ok(self.mirror.read_range(digest, range, limit)?)
    }

    pub async fn open_original(&self, rfc: RfcNumber) -> Result<GuardedOriginal> {
        let digest = self.document_digest(rfc).await?;
        Ok(GuardedOriginal {
            file: self.mirror.open_blob(digest)?,
            digest,
            _fence: self._fence.try_clone()?,
        })
    }

    /// Plan a worker's allowlist through membership and pre-open the original inodes.
    /// The caller keeps the guard through completion and reaping of the worker.
    pub async fn open_worker_files(&self, rfcs: &[RfcNumber], limit: usize) -> Result<Vec<File>> {
        if rfcs.len() > limit {
            return Err("worker file count exceeds limit".into());
        }
        let mut files = Vec::with_capacity(rfcs.len());
        for &rfc in rfcs {
            let digest = self.document_digest(rfc).await?;
            files.push(self.mirror.open_blob(digest)?);
        }
        Ok(files)
    }

    /// Start a trusted launcher. The supervisor retains the filesystem fence
    /// through kill and reap even when the caller is cancelled or times out.
    pub async fn run_worker(
        &self,
        command: &mut tokio::process::Command,
    ) -> Result<std::process::ExitStatus> {
        let fence = self.worker_fence()?;
        let mut child = command.process_group(0).kill_on_drop(true).spawn()?;
        let group = child
            .id()
            .and_then(|id| i32::try_from(id).ok())
            .and_then(rustix::process::Pid::from_raw)
            .ok_or("worker has no process group ID")?;
        let (caller, cancelled) = tokio::sync::oneshot::channel::<()>();
        let worker = tokio::spawn(async move {
            let _fence = fence;
            tokio::select! {
                status = child.wait() => status,
                _ = cancelled => {
                    let _ = rustix::process::kill_process_group(group, rustix::process::Signal::KILL);
                    child.wait().await
                }
            }
        });
        let status = worker.await??;
        drop(caller);
        Ok(status)
    }

    /// Keep the guarded original alive if the database session dies before a
    /// supervised child has been killed and reaped.
    pub fn worker_fence(&self) -> std::io::Result<File> {
        self._fence.try_clone()
    }
}

impl Reader {
    pub async fn connect() -> Result<Self> {
        let (client, driver) = open_session(RuntimeRole::Serve).await?;
        Ok(Self { client, driver })
    }

    /// Returns the current pointer and its independent monotonic sequence.
    pub async fn active(&self) -> Result<(Option<GenerationId>, i64)> {
        active(&self.client).await
    }

    /// Run one bounded operation on a dedicated, disposable read session.
    pub async fn with_generation<T>(
        mut self,
        selection: GenerationSelection,
        operation: impl for<'a, 'b> AsyncFnOnce(&'a GenerationReadGuard<'b>) -> Result<T>,
    ) -> Result<T> {
        let operation = read_generation(&mut self.client, selection, async |guard| {
            operation(guard).await.map(ReadOutcome::Complete)
        });
        tokio::select! {
            result = timeout(READ_DEADLINE, operation) => result??.into_result(),
            _ = &mut self.driver => Err("generation read session closed".into()),
        }
    }

    /// Checks stored manifest bytes and the manifest reconstructed from rows.
    /// Used by the offline restore probe for immutable fixture generations.
    pub async fn verify_manifest(&mut self, generation: GenerationId) -> Result<BlobDigest> {
        let id = i64::try_from(generation.get())?;
        let tx = self
            .client
            .build_transaction()
            .read_only(true)
            .start()
            .await?;
        tx.query_one("SELECT pg_advisory_xact_lock_shared($1)", &[&id])
            .await?;
        tx.query_one(
            "SELECT id FROM rfc.generations WHERE id = $1 AND state = 'ready'",
            &[&id],
        )
        .await?;
        let digest = verify_stored_manifest(&tx, generation).await?;
        tx.commit().await?;
        Ok(digest)
    }
}

async fn active(db: &impl GenericClient) -> Result<(Option<GenerationId>, i64)> {
    let row = db
        .query_one(
            "SELECT generation_id, publication_sequence FROM rfc.active_generation WHERE singleton",
            &[],
        )
        .await?;
    let generation = row
        .try_get::<_, Option<i64>>(0)?
        .map(|id| GenerationId::new(id).ok_or("invalid generation ID"))
        .transpose()?;
    Ok((generation, row.try_get(1)?))
}

async fn read_generation<T>(
    client: &mut Client,
    selection: GenerationSelection,
    operation: impl for<'a, 'b> AsyncFnOnce(&'a GenerationReadGuard<'b>) -> Result<ReadOutcome<T>>,
) -> Result<ReadOutcome<T>> {
    for attempt in 0..2 {
        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .read_only(true)
            .start()
            .await?;
        let (id, resolved_sequence) = match selection {
            GenerationSelection::Active => {
                let row = tx.query_one("SELECT generation_id, publication_sequence FROM rfc.active_generation WHERE singleton", &[]).await?;
                let Some(id) = row.try_get::<_, Option<i64>>(0)? else {
                    tx.rollback().await?;
                    return Ok(ReadOutcome::Rejected(GenerationUnavailable.into()));
                };
                (id, Some(row.try_get(1)?))
            }
            GenerationSelection::Pinned(generation) => (i64::try_from(generation.get())?, None),
        };
        let generation = GenerationId::new(id).ok_or("invalid generation ID")?;
        crate::fixture_boundary("reader_resolved");
        tx.query_one("SELECT pg_advisory_xact_lock_shared($1)", &[&id])
            .await?;
        let mirror = loop {
            let result = BlobReader::open(&mirror_root(), true).and_then(|mirror| {
                let fence = mirror.generation_fence(generation)?;
                Ok((mirror, fence))
            });
            if result
                .as_ref()
                .is_err_and(|error| error.kind() == std::io::ErrorKind::WouldBlock)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            } else {
                break result;
            }
        };
        let mirror = match mirror {
            Ok(mirror) => Some(mirror),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        // This statement starts after taking the locks, including when the
        // generation was retired while the advisory lock was pending.
        let row = tx
            .query_opt(
                "SELECT last_publication_sequence,
                        (SELECT rights_epoch FROM rfc.policy_state WHERE singleton)
                 FROM rfc.generations WHERE id = $1 AND state IN ('published', 'retained')",
                &[&id],
            )
            .await?;
        if let Some(row) = row {
            let (mirror, fence) =
                mirror.ok_or("readable generation is missing its mirror fence")?;
            let sequence = resolved_sequence.unwrap_or(row.try_get(0)?);
            let rights_epoch = row.try_get(1)?;
            let _ = OBSERVED_RIGHTS.try_with(|observed| observed.set(Some(rights_epoch)));
            let guard = GenerationReadGuard {
                tx,
                mirror: &mirror,
                _fence: fence,
                generation,
                sequence,
                rights_epoch,
            };
            return match operation(&guard).await? {
                ReadOutcome::Complete(result) => {
                    guard.tx.commit().await?;
                    Ok(ReadOutcome::Complete(result))
                }
                ReadOutcome::Rejected(error) => {
                    guard.tx.rollback().await?;
                    Ok(ReadOutcome::Rejected(error))
                }
                ReadOutcome::Interrupted(result) => Ok(ReadOutcome::Interrupted(result)),
            };
        }
        tx.rollback().await?;
        if attempt == 1 || matches!(selection, GenerationSelection::Pinned(_)) {
            return Ok(ReadOutcome::Rejected(GenerationUnavailable.into()));
        }
    }
    unreachable!("generation attempts are bounded")
}

pub async fn verify_stored_manifest(
    db: &impl GenericClient,
    generation: GenerationId,
) -> Result<BlobDigest> {
    let id = i64::try_from(generation.get())?;
    let row = db.query_one(
        "SELECT manifest_format, manifest_bytes, manifest_digest::bytea FROM rfc.generations WHERE id = $1", &[&id],
    ).await?;
    let manifest = load_manifest(db, generation).await?;
    let format = manifest.version();
    let (actual, digest) = manifest.encode();
    if row.try_get::<_, i32>(0)? != format
        || row.try_get::<_, Vec<u8>>(1)? != actual
        || row.try_get::<_, &[u8]>(2)? != digest.as_bytes()
    {
        return Err("stored generation manifest differs from its inputs".into());
    }
    Ok(digest)
}

/// Loads a manifest for a generation whose state is protected by the caller.
pub async fn load_manifest(db: &impl GenericClient, generation: GenerationId) -> Result<Manifest> {
    let id = i64::try_from(generation.get())?;
    let versions = db
        .query_one(
            "SELECT parser_version, index_version FROM rfc.generations WHERE id = $1",
            &[&id],
        )
        .await?;
    let mut members = Vec::new();
    for row in db.query(
        "SELECT gd.rfc_id::integer, encode(d.txt_digest, 'hex'), encode(gd.document_version, 'hex'),
                encode(gd.metadata_identity, 'hex')
         FROM rfc.generation_documents gd JOIN rfc.document_versions d ON d.identity = gd.document_version
         WHERE gd.generation_id = $1", &[&id],
    ).await? {
        members.push(Member(row.try_get(0)?, row.try_get(1)?, row.try_get(2)?, row.try_get(3)?));
    }
    let mut edges = Vec::new();
    for row in db
        .query(
            "SELECT r.source_rfc::integer, r.kind, r.target_rfc::integer,
                array_agg(encode(e.evidence, 'hex')) FILTER (WHERE e.evidence IS NOT NULL)
         FROM rfc.rfc_relations r LEFT JOIN rfc.edge_evidence e
         USING (generation_id, source_rfc, kind, target_rfc)
         WHERE r.generation_id = $1 GROUP BY r.source_rfc, r.kind, r.target_rfc",
            &[&id],
        )
        .await?
    {
        let evidence = row
            .try_get::<_, Option<Vec<String>>>(3)?
            .ok_or("forward edge has no evidence")?;
        edges.push(Edge(
            row.try_get(0)?,
            row.try_get(1)?,
            row.try_get(2)?,
            evidence,
        ));
    }
    let mut coverage = Vec::new();
    for row in db.query(
        "SELECT scope, rfc_id::integer, family, state::text FROM rfc.relation_coverage WHERE generation_id = $1", &[&id],
    ).await? {
        coverage.push(Coverage(row.try_get(0)?, row.try_get(1)?, row.try_get(2)?, row.try_get(3)?));
    }
    let ingestion = db.query_opt(
        "SELECT scope, encode(catalog_before, 'hex'), encode(catalog_after, 'hex'), policy_registry, catalog_only::text
         FROM rfc.generation_ingestion WHERE generation_id = $1", &[&id],
    ).await?.map(|row| -> Result<Ingestion> {
        Ok(Ingestion(row.try_get(0)?, row.try_get(1)?, row.try_get(2)?, row.try_get(3)?,
            serde_json::from_str(row.try_get::<_, &str>(4)?)?))
    }).transpose()?;
    let audit = if let Some(header) = db
        .query_opt(
            "SELECT jsonb_build_array(encode(selection, 'hex'), encode(inventory, 'hex'), policy,
            expected_rfc_ids, expected_sequence, started_us, expires_us)::text, fixture
         FROM rfc.generation_audit WHERE generation_id = $1",
            &[&id],
        )
        .await?
    {
        let mut proofs = Vec::new();
        for row in db.query(
            "SELECT jsonb_build_array(rfc_id, encode(txt_observation, 'hex'), encode(document_version, 'hex'),
                encode(txt_digest, 'hex'), byte_length, method, encode(receipt, 'hex'), rsync_url,
                started_us, observed_us, checksum_choice, algorithm, encode(rsync_observation, 'hex'),
                rfcxml_outcome, encode(rfcxml_observation, 'hex'), rfcxml_audit)::text
             FROM rfc.generation_source_evidence WHERE generation_id = $1 ORDER BY rfc_id", &[&id],
        ).await? {
            proofs.push(serde_json::from_str(row.try_get::<_, &str>(0)?)?);
        }
        let mut header_fields: Vec<serde_json::Value> =
            serde_json::from_str(header.try_get::<_, &str>(0)?)?;
        if ingestion
            .as_ref()
            .is_some_and(|ingestion| ingestion.3.is_none())
        {
            header_fields.push(serde_json::json!(header.try_get::<_, bool>(1)?));
        }
        Some(Audit(serde_json::Value::Array(header_fields), proofs))
    } else {
        None
    };
    let policy = db
        .query_opt(
            "SELECT encode(registry_digest, 'hex'), classifier, fixture
         FROM rfc.generation_policy WHERE generation_id = $1",
            &[&id],
        )
        .await?
        .map(|row| -> Result<_> { Ok((row.try_get(0)?, row.try_get(1)?, row.try_get(2)?)) })
        .transpose()?;
    if let Some((registry, classifier, _)) = &policy {
        for row in db.query(
            "SELECT m.rfc_id::integer, d.txt_digest::bytea, d.notice_policy,
                    d.policy_receipt::bytea, p.receipt::text
             FROM rfc.generation_documents m JOIN rfc.document_versions d ON d.identity = m.document_version
             LEFT JOIN rfc.policy_receipts p ON p.identity = d.policy_receipt
             WHERE m.generation_id = $1", &[&id],
        ).await? {
            let receipt: crate::policy::Receipt = serde_json::from_str(row.try_get::<_, &str>(4)?)?;
            let original = BlobDigest::from_bytes(row.try_get::<_, &[u8]>(1)?.try_into()?);
            if receipt.rfc != u32::try_from(row.try_get::<_, i32>(0)?)?
                || receipt.source_digest != original.to_string()
                || receipt.decision.name() != row.try_get::<_, &str>(2)?
                || receipt.registry_digest != *registry || receipt.classifier != *classifier
                || receipt.digest()?.as_bytes().as_slice() != row.try_get::<_, &[u8]>(3)?
                || (receipt.decision == crate::policy::Policy::ExcerptAllowed && !receipt.eligible_for_excerpt())
            {
                return Err("generation contains invalid policy decision evidence".into());
            }
        }
    }
    Ok(Manifest {
        parser_version: versions.try_get(0)?,
        index_version: versions.try_get(1)?,
        members,
        edges,
        coverage,
        ingestion,
        audit,
        policy,
    })
}

/// The mirror mount path in the local Compose runtime.
pub fn mirror_root() -> PathBuf {
    env::var_os("RFC_MIRROR_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| "/mirror".into())
}
