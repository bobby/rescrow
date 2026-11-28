//! DSQL authority. Domain decisions and retry policy remain in the Application.
//!
//! The synchronous port runs on a dedicated runtime. Async hosts must construct
//! and call it from their blocking boundary (as rescrow-server does).
//! `postgres_simulation` is development evidence only, never a regional profile.
mod metrics;
mod schema;
mod topology;
mod wire;

pub use metrics::Measurements;
use rescrow_application::{StoreError, authority::*, capabilities::*};
use serde::{Deserialize, Serialize};
use sqlx::{
    PgConnection, PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions, PgRow},
};
use std::sync::atomic::Ordering;
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tokio::runtime::Runtime;
use wire::{Key, Publication, StoredValue, corrupt, decode, encode};

// Below DSQL's 1 MiB column, 2 MiB row and 10 MiB transaction ceilings,
// leaving room for key encoding and database metadata. No column is chunked.
pub const MAX_COLUMN_BYTES: usize = 900 * 1024;
pub const MAX_PARTICIPANTS: usize = 128;
const MAX_MUTATED_BYTES: usize = 8 * 1024 * 1024;
const MAX_MUTATED_ROWS: usize = 3000;

fn default_user() -> String {
    "admin".into()
}
fn default_pool() -> u32 {
    16
}
fn default_timeout() -> u64 {
    30
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub cluster_arn: String,
    pub authority: String,
    #[serde(default = "default_user")]
    pub user: String,
    pub profile: Option<String>,
    #[serde(default = "default_pool")]
    pub max_connections: u32,
    #[serde(default = "default_timeout")]
    pub operation_timeout_seconds: u64,
}
impl Config {
    fn validate(&self) -> Result<(), StoreError> {
        TenantId::new(&self.authority).map_err(corrupt)?;
        topology::Cluster::parse(&self.cluster_arn)?;
        if self.user.is_empty()
            || self.user.len() > 63
            || !self
                .user
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
            || !(1..=256).contains(&self.max_connections)
            || !(1..=120).contains(&self.operation_timeout_seconds)
        {
            return Err(corrupt("invalid DSQL user, pool size, or timeout"));
        }
        Ok(())
    }
}

struct Executor(Option<Runtime>);
impl Executor {
    fn new() -> Result<Self, StoreError> {
        Ok(Self(Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|_| io("create DSQL runtime"))?,
        )))
    }
    fn run<T>(
        &self,
        timeout: Duration,
        future: impl Future<Output = Result<T, StoreError>>,
        publication: bool,
    ) -> Result<T, StoreError> {
        self.0.as_ref().expect("live runtime").block_on(async {
            tokio::time::timeout(timeout, future).await.map_err(|_| {
                if publication {
                    StoreError::Unknown("DSQL publication deadline exceeded".into())
                } else {
                    io("DSQL operation deadline exceeded")
                }
            })?
        })
    }
}
impl Drop for Executor {
    fn drop(&mut self) {
        // Safe when the final Arc is released by an async composition root.
        // Server drain retains active work before dropping its authority.
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}
struct Inner {
    pool: PgPool,
    executor: Executor,
    authority: String,
    timeout: Duration,
    config: Option<Config>,
    counters: metrics::Counters,
}
#[derive(Clone)]
pub struct DsqlStore(Arc<Inner>);

pub(crate) fn io(message: impl Into<String>) -> StoreError {
    StoreError::Io(message.into())
}
fn sql_error(error: sqlx::Error) -> StoreError {
    if let sqlx::Error::Database(error) = &error {
        match error.code().as_deref() {
            Some("40001" | "40P01" | "23505") => return StoreError::Contention,
            Some("54000" | "54011") => return StoreError::TooLarge,
            Some(code) => return io(format!("DSQL statement failed (SQLSTATE {code})")),
            None => {}
        }
    }
    // Database diagnostics can contain stored values. Do not expose them, URLs,
    // connection options or IAM credentials in logs / public problem details.
    io("DSQL connection or protocol failure")
}
fn commit_error(error: sqlx::Error) -> StoreError {
    if let sqlx::Error::Database(ref database) = error {
        if matches!(
            database.code().as_deref(),
            Some("40001" | "40P01" | "23505")
        ) {
            return StoreError::Contention;
        }
        // A server-reported transaction abort is known; transport errors aren't.
        if database.code().as_deref().is_some_and(|code| {
            code.starts_with("23") || code.starts_with("40") || code.starts_with("54")
        }) {
            return sql_error(error);
        }
    }
    StoreError::Unknown("DSQL COMMIT response was not conclusive".into())
}
fn pool_options(size: u32, timeout: Duration) -> PgPoolOptions {
    PgPoolOptions::new()
        .max_connections(size)
        .min_connections(0)
        .acquire_timeout(timeout)
        .max_lifetime(Duration::from_secs(50 * 60))
        .idle_timeout(Duration::from_secs(5 * 60))
}
impl DsqlStore {
    pub fn connect(config: Config) -> Result<Self, StoreError> {
        config.validate()?;
        let cluster = topology::Cluster::parse(&config.cluster_arn)?;
        let executor = Executor::new()?;
        let timeout = Duration::from_secs(config.operation_timeout_seconds);
        let pool = executor.run(
            timeout,
            async {
                topology::inspect(&config).await?;
                let options = aurora_dsql_sqlx_connector::DsqlConnectOptionsBuilder::default()
                    .pg_connect_options(
                        PgConnectOptions::new()
                            .host(&cluster.endpoint())
                            .username(&config.user)
                            .database("postgres")
                            .ssl_mode(sqlx::postgres::PgSslMode::VerifyFull),
                    )
                    .profile(config.profile.clone())
                    .build()
                    .map_err(|_| io("invalid DSQL IAM connection configuration"))?;
                aurora_dsql_sqlx_connector::pool::connect_with(
                    &options,
                    pool_options(config.max_connections, timeout),
                )
                .await
                .map_err(|_| io("DSQL IAM/TLS connection failed"))
            },
            false,
        )?;
        Ok(Self(Arc::new(Inner {
            pool,
            executor,
            authority: config.authority.clone(),
            timeout,
            config: Some(config),
            counters: metrics::Counters::default(),
        })))
    }
    /// A loopback PostgreSQL test target. It cannot report durable capabilities.
    /// Use the same schema and port implementation; PostgreSQL is not DSQL proof.
    pub fn postgres_simulation(url: &str, authority: &str) -> Result<Self, StoreError> {
        TenantId::new(authority).map_err(corrupt)?;
        let options =
            PgConnectOptions::from_str(url).map_err(|_| io("invalid PostgreSQL simulation URL"))?;
        if !options
            .get_host()
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
        {
            return Err(io(
                "PostgreSQL simulation requires a literal loopback address",
            ));
        }
        let executor = Executor::new()?;
        let timeout = Duration::from_secs(30);
        let pool = executor.run(
            timeout,
            async {
                pool_options(16, timeout)
                    .connect_with(options)
                    .await
                    .map_err(sql_error)
            },
            false,
        )?;
        Ok(Self(Arc::new(Inner {
            pool,
            executor,
            authority: authority.into(),
            timeout,
            config: None,
            counters: metrics::Counters::default(),
        })))
    }
    fn run<T>(
        &self,
        future: impl Future<Output = Result<T, StoreError>>,
        publication: bool,
    ) -> Result<T, StoreError> {
        self.0.executor.run(self.0.timeout, future, publication)
    }
    /// Explicit administrative DDL; never called by HTTP startup. Each statement
    /// has its own implicit transaction. Repeating an interrupted install is safe.
    pub fn install_schema(&self) -> Result<(), StoreError> {
        self.run(
            async {
                for statement in schema::STATEMENTS {
                    sqlx::query(*statement)
                        .execute(&self.0.pool)
                        .await
                        .map_err(sql_error)?;
                }
                Ok(())
            },
            false,
        )
    }
    pub fn measurements(&self) -> Measurements {
        self.0.counters.snapshot()
    }
    async fn begin(&self) -> Result<sqlx::Transaction<'_, sqlx::Postgres>, StoreError> {
        self.0
            .pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ")
            .await
            .map_err(sql_error)
    }
    async fn identity_on(&self, connection: &mut PgConnection) -> Result<String, StoreError> {
        self.0.counters.statement(0);
        let row = sqlx::query(
            "SELECT generation, schema_version FROM public.rescrow_authorities WHERE authority=$1",
        )
        .bind(&self.0.authority)
        .fetch_optional(connection)
        .await
        .map_err(sql_error)?
        .ok_or(StoreError::Missing)?;
        let version: i32 = row.try_get("schema_version").map_err(sql_error)?;
        if version != schema::VERSION {
            return Err(StoreError::Unsupported(
                version.try_into().map_err(corrupt)?,
            ));
        }
        let generation: String = row.try_get("generation").map_err(sql_error)?;
        TransitionId::new(&generation)?;
        Ok(generation)
    }
    async fn record_on(
        &self,
        connection: &mut PgConnection,
        tenant: &TenantId,
        key: &Key,
    ) -> Result<Option<PgRow>, StoreError> {
        self.0.counters.statement(0);
        let row = sqlx::query("SELECT publication, version, recorded_at_ms, payload FROM public.rescrow_records WHERE authority=$1 AND tenant=$2 AND kind=$3 AND namespace=$4 AND id=$5")
            .bind(&self.0.authority).bind(tenant.as_str()).bind(&key.kind).bind(&key.namespace).bind(&key.id)
            .fetch_optional(connection).await.map_err(sql_error)?;
        if let Some(row) = &row {
            self.0.counters.read(
                row.try_get::<Option<&str>, _>("payload")
                    .map_err(sql_error)?
                    .map_or(0, str::len),
            );
        }
        Ok(row)
    }
    async fn publication_on(
        &self,
        connection: &mut PgConnection,
        tenant: &TenantId,
        id: &TransitionId,
    ) -> Result<Option<Option<Publication>>, StoreError> {
        self.0.counters.statement(0);
        let row = sqlx::query(
            "SELECT payload FROM public.rescrow_publications WHERE authority=$1 AND tenant=$2 AND id=$3",
        )
        .bind(&self.0.authority)
        .bind(tenant.as_str())
        .bind(id.as_str())
        .fetch_optional(connection)
        .await
        .map_err(sql_error)?;
        row.map(|row| {
            let value: Option<String> = row.try_get("payload").map_err(sql_error)?;
            self.0.counters.read(value.as_ref().map_or(0, String::len));
            value
                .map(|json| {
                    let publication: Publication = decode(&json)?;
                    publication.validate(tenant, id)?;
                    Ok(publication)
                })
                .transpose()
        })
        .transpose()
    }
}

fn decoded_record(row: &PgRow, key: &RecordKey) -> Result<Option<Versioned>, StoreError> {
    let publication: String = row.try_get("publication").map_err(sql_error)?;
    TransitionId::new(publication)?;
    let version: Option<String> = row.try_get("version").map_err(sql_error)?;
    let time: Option<String> = row.try_get("recorded_at_ms").map_err(sql_error)?;
    let value: Option<String> = row.try_get("payload").map_err(sql_error)?;
    match (version, time, value) {
        (None, None, None) => Ok(None),
        (Some(version), Some(time), Some(value)) => {
            TransitionId::new(&version)?;
            let recorded_at_ms: u64 = time.parse().map_err(corrupt)?;
            if recorded_at_ms.to_string() != time {
                return Err(corrupt("noncanonical recording time"));
            }
            if rescrow_application::uuid_v7_millis(&version)
                .is_some_and(|time| time > recorded_at_ms)
            {
                return Err(corrupt("logical version postdates its recording time"));
            }
            let value = decode::<StoredValue>(&value)?.validated(key)?;
            Ok(Some(Versioned {
                version: Version(version.into_bytes()),
                recorded_at_ms,
                value,
            }))
        }
        _ => Err(corrupt("partially populated record or tombstone")),
    }
}

impl TransactionalStore for DsqlStore {
    fn identity(&self) -> Result<String, StoreError> {
        self.run(
            async {
                let mut connection = self.0.pool.acquire().await.map_err(sql_error)?;
                self.identity_on(&mut connection).await
            },
            false,
        )
    }
    fn initialize(&self) -> Result<(), StoreError> {
        let generation = rescrow_application::new_id().map_err(corrupt)?;
        self.run(async {
            self.0.counters.statement(0);
            let result = sqlx::query("INSERT INTO public.rescrow_authorities (authority,generation,schema_version) VALUES ($1,$2,$3)")
                .bind(&self.0.authority).bind(generation).bind(schema::VERSION).execute(&self.0.pool).await;
            match result {
                Ok(_) => Ok(()),
                Err(sqlx::Error::Database(ref e)) if e.is_unique_violation() => {
                    let mut connection = self.0.pool.acquire().await.map_err(sql_error)?;
                    self.identity_on(&mut connection).await?;
                    Err(StoreError::Exists)
                },
                Err(e) => Err(commit_error(e)),
            }
        }, true)
    }
    fn read(
        &self,
        tenant: &TenantId,
        keys: &[RecordKey],
        terminal_since_ms: u64,
    ) -> Result<Read, StoreError> {
        self.run(async {
            let mut tx = self.begin().await?;
            self.identity_on(&mut tx).await?;
            let mut records = BTreeMap::new();
            for key in keys {
                let row = self.record_on(&mut tx, tenant, &Key::from(key)).await?;
                let value = row.as_ref().map(|row| decoded_record(row,key)).transpose()?.flatten();
                let value = value.filter(|v| !matches!(&v.value, Value::Reservation(reservation) if !matches!(reservation.state, rescrow_application::dto::State::Prepared {}) && v.recorded_at_ms < terminal_since_ms));
                records.insert(key.clone(),value);
            }
            tx.commit().await.map_err(sql_error)?;
            Ok(read_from(records))
        }, false)
    }
    fn scan(&self, tenant: &TenantId) -> Result<Inspection, StoreError> {
        self.run(async {
            let mut tx = self.begin().await?;
            self.identity_on(&mut tx).await?;
            self.0.counters.statement(0);
            let rows = sqlx::query("SELECT kind,namespace,id,publication,version,recorded_at_ms,payload FROM public.rescrow_records WHERE authority=$1 AND tenant=$2")
                .bind(&self.0.authority).bind(tenant.as_str()).fetch_all(&mut *tx).await.map_err(sql_error)?;
            let mut records = BTreeMap::new();
            let mut markers = BTreeSet::new();
            for row in rows {
                markers.insert(TransitionId::new(row.try_get::<String,_>("publication").map_err(sql_error)?)?);
                self.0.counters.read(row.try_get::<Option<&str>,_>("payload").map_err(sql_error)?.map_or(0,str::len));
                let key = Key {kind:row.try_get("kind").map_err(sql_error)?, namespace:row.try_get("namespace").map_err(sql_error)?, id:row.try_get("id").map_err(sql_error)?}.record_key()?;
                // Tombstone guards are physical dependencies, not logical records.
                if let Some(value) = decoded_record(&row,&key)? { records.insert(key,Some(value)); }
            }
            let history = self.history_on(&mut tx,tenant).await?;
            let published: BTreeSet<_> = history.iter().map(|h| &h.transition_id).collect();
            if markers.iter().any(|id| !published.contains(id)) {
                return Err(corrupt("record marker has no durable publication"));
            }
            tx.commit().await.map_err(sql_error)?;
            Ok(Inspection {read:read_from(records),history})
        }, false)
    }
    fn commit(&self, commit: &Commit) -> Result<CommitOutcome, StoreError> {
        let publication = Publication::from_commit(commit)?;
        let payload = encode(&publication)?;
        if payload.len() > MAX_COLUMN_BYTES
            || commit.conditions().len() + 1 > MAX_MUTATED_ROWS
            || commit
                .conditions()
                .keys()
                .filter(|key| matches!(key, RecordKey::Resource(_)))
                .count()
                > MAX_PARTICIPANTS
        {
            return Err(StoreError::TooLarge);
        }
        self.0
            .counters
            .publication_attempts
            .fetch_add(1, Ordering::Relaxed);
        let result = match self.run(self.publish(commit, &publication, &payload), true) {
            Err(StoreError::Contention) => Ok(CommitOutcome::Conflict),
            other => other,
        };
        match &result {
            Ok(CommitOutcome::Applied(_)) => {
                self.0
                    .counters
                    .applied_responses
                    .fetch_add(1, Ordering::Relaxed);
            }
            Ok(CommitOutcome::Conflict) => {
                self.0.counters.conflicts.fetch_add(1, Ordering::Relaxed);
            }
            Err(StoreError::Unknown(_)) => {
                self.0
                    .counters
                    .unknown_publications
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        result
    }
    fn resolve(&self, tenant: &TenantId, id: &TransitionId) -> Result<Resolution, StoreError> {
        let result = self.run(async {
            let mut tx = self.begin().await?;
            self.identity_on(&mut tx).await?;
            if let Some(publication) = self.publication_on(&mut tx,tenant,id).await? {
                tx.commit().await.map_err(sql_error)?;
                return match publication {Some(p) => Ok(Resolution::Applied {history:p.history(),receipt:p.receipt()?}),None=>Ok(Resolution::NotApplied)};
            }
            // The original publication must insert this same primary key in its
            // transaction. A durable NULL row fences even a delayed COMMIT.
            self.0.counters.statement(2);
            sqlx::query("INSERT INTO public.rescrow_publications (authority,tenant,id,payload,predecessors) VALUES ($1,$2,$3,NULL,'[]')")
                .bind(&self.0.authority).bind(tenant.as_str()).bind(id.as_str()).execute(&mut *tx).await.map_err(sql_error)?;
            tx.commit().await.map_err(commit_error)?;
            Ok(Resolution::NotApplied)
        }, true);
        match result {
            Err(StoreError::Contention | StoreError::Unknown(_)) => Ok(Resolution::Unknown),
            other => other,
        }
    }
    fn history(&self, tenant: &TenantId) -> Result<Vec<HistoryRecord>, StoreError> {
        self.run(
            async {
                let mut tx = self.begin().await?;
                self.identity_on(&mut tx).await?;
                let history = self.history_on(&mut tx, tenant).await?;
                tx.commit().await.map_err(sql_error)?;
                Ok(history)
            },
            false,
        )
    }
}

fn read_from(records: ReadSet) -> Read {
    let recorded_at_ms = records
        .values()
        .flatten()
        .map(|v| v.recorded_at_ms)
        .max()
        .unwrap_or(0);
    Read {
        records,
        recorded_at_ms,
        fence: None,
    }
}

impl DsqlStore {
    async fn publish(
        &self,
        commit: &Commit,
        publication: &Publication,
        payload: &str,
    ) -> Result<CommitOutcome, StoreError> {
        let mut tx = self.begin().await?;
        self.identity_on(&mut tx).await?;
        let tenant = commit.tenant();
        let id = &commit.history().transition_id;
        if let Some(existing) = self.publication_on(&mut tx, tenant, id).await? {
            return match existing {
                Some(existing) if existing == *publication => {
                    Ok(CommitOutcome::Applied(existing.receipt()?))
                }
                Some(_) => Err(corrupt(
                    "publication identity reused with different content",
                )),
                None => Ok(CommitOutcome::Conflict),
            };
        }
        let mut predecessors = BTreeSet::new();
        let mut bytes = payload.len() + 1024;
        for (key, expected) in commit.conditions() {
            let sql_key = Key::from(key);
            let row = self.record_on(&mut tx, tenant, &sql_key).await?;
            let current = row
                .as_ref()
                .map(|row| decoded_record(row, key))
                .transpose()?
                .flatten();
            if current.as_ref().map(|v| &v.version) != expected.as_ref() {
                return Ok(CommitOutcome::Conflict);
            }
            if current
                .as_ref()
                .is_some_and(|v| v.recorded_at_ms > commit.history().records[0].recorded_at_ms)
            {
                return Err(corrupt("publication precedes a dependency"));
            }
            if let Some(row) = &row {
                predecessors.insert(row.try_get::<String, _>("publication").map_err(sql_error)?);
            }
            let replacement = commit.replacements().get(key);
            let version = if replacement.is_some() {
                Some(id.as_str().to_owned())
            } else {
                current
                    .as_ref()
                    .map(|v| String::from_utf8(v.version.0.clone()).map_err(corrupt))
                    .transpose()?
            };
            let time = if replacement.is_some() {
                Some(
                    commit
                        .history()
                        .records
                        .last()
                        .ok_or_else(|| corrupt("empty history"))?
                        .recorded_at_ms
                        .to_string(),
                )
            } else {
                current.as_ref().map(|v| v.recorded_at_ms.to_string())
            };
            let value = replacement
                .or_else(|| current.as_ref().map(|v| &v.value))
                .map(|v| encode(&StoredValue::from(v)))
                .transpose()?;
            if value.as_ref().is_some_and(|v| v.len() > MAX_COLUMN_BYTES) {
                return Err(StoreError::TooLarge);
            }
            bytes += value.as_ref().map_or(0, String::len) + 1024;
            if bytes > MAX_MUTATED_BYTES {
                return Err(StoreError::TooLarge);
            }
            // Write EVERY dependency, retaining logical versions on guard-only
            // updates. Absence becomes a tombstone in this publication, never
            // from GET. Snapshot isolation then cannot admit write skew.
            let query = if row.is_some() {
                "UPDATE public.rescrow_records SET publication=$6,version=$7,recorded_at_ms=$8,payload=$9 WHERE authority=$1 AND tenant=$2 AND kind=$3 AND namespace=$4 AND id=$5"
            } else {
                "INSERT INTO public.rescrow_records (authority,tenant,kind,namespace,id,publication,version,recorded_at_ms,payload) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)"
            };
            self.0
                .counters
                .statement(value.as_ref().map_or(0, String::len));
            sqlx::query(query)
                .bind(&self.0.authority)
                .bind(tenant.as_str())
                .bind(&sql_key.kind)
                .bind(&sql_key.namespace)
                .bind(&sql_key.id)
                .bind(id.as_str())
                .bind(version)
                .bind(time)
                .bind(value)
                .execute(&mut *tx)
                .await
                .map_err(sql_error)?;
        }
        let predecessors = encode(&predecessors)?;
        if bytes + predecessors.len() > MAX_MUTATED_BYTES {
            return Err(StoreError::TooLarge);
        }
        // A resolver may have inserted a permanent no-publication fence. The
        // primary-key conflict aborts this whole transaction, including guards.
        self.0
            .counters
            .statement(payload.len() + predecessors.len());
        sqlx::query("INSERT INTO public.rescrow_publications (authority,tenant,id,payload,predecessors) VALUES ($1,$2,$3,$4,$5)")
            .bind(&self.0.authority).bind(tenant.as_str()).bind(id.as_str()).bind(payload).bind(predecessors).execute(&mut *tx).await.map_err(sql_error)?;
        tx.commit().await.map_err(commit_error)?;
        Ok(CommitOutcome::Applied(publication.receipt()?))
    }
    async fn history_on(
        &self,
        connection: &mut PgConnection,
        tenant: &TenantId,
    ) -> Result<Vec<HistoryRecord>, StoreError> {
        self.0.counters.statement(0);
        let rows = sqlx::query("SELECT id,payload,predecessors FROM public.rescrow_publications WHERE authority=$1 AND tenant=$2")
            .bind(&self.0.authority).bind(tenant.as_str()).fetch_all(connection).await.map_err(sql_error)?;
        let mut publications = BTreeMap::new();
        let mut children = BTreeMap::<TransitionId, Vec<TransitionId>>::new();
        let mut pending = BTreeMap::new();
        for row in rows {
            self.0.counters.read(
                row.try_get::<Option<&str>, _>("payload")
                    .map_err(sql_error)?
                    .map_or(0, str::len)
                    + row
                        .try_get::<&str, _>("predecessors")
                        .map_err(sql_error)?
                        .len(),
            );
            let id = TransitionId::new(row.try_get::<String, _>("id").map_err(sql_error)?)?;
            let predecessors: Vec<TransitionId> = decode(
                &row.try_get::<String, _>("predecessors")
                    .map_err(sql_error)?,
            )?;
            let payload: Option<String> = row.try_get("payload").map_err(sql_error)?;
            let Some(payload) = payload else {
                if !predecessors.is_empty() {
                    return Err(corrupt("resolution fence has predecessors"));
                }
                continue;
            };
            let publication: Publication = decode(&payload)?;
            publication.validate(tenant, &id)?;
            let unique: BTreeSet<_> = predecessors.iter().collect();
            if unique.len() != predecessors.len() {
                return Err(corrupt("duplicate history predecessor"));
            }
            pending.insert(id.clone(), predecessors.len());
            for parent in predecessors {
                children.entry(parent).or_default().push(id.clone());
            }
            if publications.insert(id, publication).is_some() {
                return Err(corrupt("duplicate publication"));
            }
        }
        if children.keys().any(|id| !publications.contains_key(id)) {
            return Err(corrupt("missing causal predecessor"));
        }
        let mut ready: BTreeSet<_> = pending
            .iter()
            .filter(|(_, n)| **n == 0)
            .map(|(id, _)| id.clone())
            .collect();
        let mut result = Vec::with_capacity(publications.len());
        while let Some(id) = ready.pop_first() {
            result.push(
                publications
                    .remove(&id)
                    .ok_or_else(|| corrupt("duplicate causal traversal"))?
                    .history(),
            );
            for child in children.get(&id).into_iter().flatten() {
                let n = pending
                    .get_mut(child)
                    .ok_or_else(|| corrupt("missing history child"))?;
                *n = n
                    .checked_sub(1)
                    .ok_or_else(|| corrupt("history predecessor underflow"))?;
                if *n == 0 {
                    ready.insert(child.clone());
                }
            }
        }
        if !publications.is_empty() {
            return Err(corrupt("cyclic publication history"));
        }
        Ok(result)
    }
}

impl Readiness for DsqlStore {
    fn readiness(&self) -> Result<Capabilities, StoreError> {
        self.identity()?;
        match &self.0.config {
            Some(config) => self.run(topology::inspect(config), false),
            None => Ok(Capabilities {
                multi_tenant: true,
                max_record_bytes: Some(MAX_COLUMN_BYTES as u64),
                max_participants: Some(MAX_PARTICIPANTS),
                ..Capabilities::simulation()
            }),
        }
    }
}
