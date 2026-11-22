//! PostgreSQL connection checks and the read-only corpus interface.

use crate::manifest::{Coverage, Edge, Ingestion, Manifest, Member};
use rfc_core::{BlobDigest, GenerationId};
use rustls::pki_types::{CertificateDer, pem::PemObject};
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::timeout;
use tokio_postgres::{Client, GenericClient, config::SslMode};
use tokio_postgres_rustls::MakeRustlsConnect;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

const SCHEMA_VERSIONS: [i32; 2] = [1, 2];
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
            Self::Serve => grants.push("pg_advisory_xact_lock_shared(bigint):EXECUTE".to_owned()),
            Self::Sync => {
                for name in ["sync_jobs", "control_events", "notification_outbox"] {
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
                        "generation_id state trigger_id outcome diagnostic started_at finished_at",
                    ),
                    ("control_events", "UPDATE", "status job_id"),
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
    timeout(Duration::from_secs(10), connect_and_verify(role)).await?
}

async fn connect_and_verify(role: RuntimeRole) -> Result<Client> {
    let host = env::var("RFC_DB_HOST")?;
    let port = env::var("RFC_DB_PORT")
        .ok()
        .map(|port| port.parse())
        .transpose()?
        .unwrap_or(5432);
    let user = env::var("RFC_DB_USER").unwrap_or_else(|_| role.name().to_owned());
    let password_path = PathBuf::from(
        env::var_os("RFC_DB_PASSWORD_FILE").ok_or("RFC_DB_PASSWORD_FILE is required")?,
    );
    let ca_path = PathBuf::from(env::var_os("RFC_DB_CA_FILE").ok_or("RFC_DB_CA_FILE is required")?);
    let password = fs::read(password_path)?;
    let password = match password.strip_suffix(b"\n") {
        Some(line) => line.strip_suffix(b"\r").unwrap_or(line),
        None => password.as_slice(),
    };
    if password.is_empty() || password.len() > 4096 {
        return Err("invalid database password file length".into());
    }
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(ca_path)? {
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
    let (client, connection) = tokio_postgres::Config::new()
        .host(&host)
        .port(port)
        .user(&user)
        .password(password)
        .dbname("rfc")
        .ssl_mode(SslMode::Require)
        .connect_timeout(Duration::from_secs(5))
        .options("-csearch_path=pg_catalog,rfc -cstatement_timeout=5000 -clock_timeout=3000 -cidle_in_transaction_session_timeout=10000")
        .connect(MakeRustlsConnect::new(tls))
        .await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    verify(&client, role).await?;
    Ok(client)
}

async fn verify(client: &Client, role: RuntimeRole) -> Result<()> {
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
        return Err("unsupported storage schema version".into());
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
}

impl Reader {
    pub async fn connect() -> Result<Self> {
        Ok(Self {
            client: connect(RuntimeRole::Serve).await?,
        })
    }

    /// Returns the current pointer and its independent monotonic sequence.
    pub async fn active(&self) -> Result<(Option<GenerationId>, i64)> {
        let row = self.client.query_one("SELECT generation_id, publication_sequence FROM rfc.active_generation WHERE singleton", &[]).await?;
        let generation = row
            .try_get::<_, Option<i64>>(0)?
            .map(|id| GenerationId::new(id).ok_or("invalid generation ID"))
            .transpose()?;
        Ok((generation, row.try_get(1)?))
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
        let row = tx.query_one(
            "SELECT manifest_format, manifest_bytes, manifest_digest::bytea FROM rfc.generations
             WHERE id = $1 AND state IN ('ready', 'published', 'retained')", &[&id],
        ).await?;
        let manifest = load_manifest(&tx, generation).await?;
        let format = manifest.version();
        let (actual, digest) = manifest.encode();
        if row.try_get::<_, i32>(0)? != format
            || row.try_get::<_, Vec<u8>>(1)? != actual
            || row.try_get::<_, &[u8]>(2)? != digest.as_bytes()
        {
            return Err("stored generation manifest differs from its inputs".into());
        }
        tx.commit().await?;
        Ok(digest)
    }
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
    Ok(Manifest {
        parser_version: versions.try_get(0)?,
        index_version: versions.try_get(1)?,
        members,
        edges,
        coverage,
        ingestion,
    })
}

/// The mirror mount path in the local Compose runtime.
pub fn mirror_root() -> PathBuf {
    env::var_os("RFC_MIRROR_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| "/mirror".into())
}
