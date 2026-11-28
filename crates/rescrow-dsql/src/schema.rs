//! Explicit installation only. DSQL requires each DDL statement in its own
//! transaction, separate from DML. Ordinary startup never installs or migrates.
pub(crate) const VERSION: i32 = 1;
pub(crate) const STATEMENTS: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS public.rescrow_authorities (
        authority VARCHAR(128) PRIMARY KEY,
        generation VARCHAR(36) NOT NULL,
        schema_version INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS public.rescrow_records (
        authority VARCHAR(128) NOT NULL,
        tenant VARCHAR(128) NOT NULL,
        kind VARCHAR(16) NOT NULL,
        namespace VARCHAR(128) NOT NULL,
        id VARCHAR(128) NOT NULL,
        publication VARCHAR(36) NOT NULL,
        version VARCHAR(36),
        recorded_at_ms VARCHAR(20),
        payload TEXT,
        PRIMARY KEY (authority, tenant, kind, namespace, id)
    )",
    "CREATE TABLE IF NOT EXISTS public.rescrow_publications (
        authority VARCHAR(128) NOT NULL,
        tenant VARCHAR(128) NOT NULL,
        id VARCHAR(36) NOT NULL,
        payload TEXT,
        predecessors TEXT NOT NULL,
        PRIMARY KEY (authority, tenant, id)
    )",
];
