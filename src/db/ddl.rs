//! DDL generation and constraint application for finalized table schemas.
//!
//! Pure generation functions (`generate_*`) return owned `String` values with no I/O.
//! [`add_constraints`] is the async entry point that executes `ALTER TABLE` statements
//! against `PostgreSQL` after the COPY phase of Pass 2 completes.
//!
//! [`generate_ddl_preview`] is used by the IHM to show the user what SQL will be executed
//! before they confirm Pass 2.
//!
//! Fonctions :
//! - fn `emit` — envoie un `ProgressEvent` si un canal est fourni.
//!
//! Génération DDL (pure) :
//! - fn `build_col_defs` — définitions de colonnes communes.
//! - fn `generate_create_table` — CREATE TABLE avec PK inline.
//! - fn `generate_create_table_no_constraints` — CREATE TABLE colonnes seules (IF NOT EXISTS).
//! - fn `generate_add_pk_sql` — ALTER TABLE ADD CONSTRAINT PRIMARY KEY.
//! - fn `generate_add_fk_sql` — ALTER TABLE ADD CONSTRAINT FOREIGN KEY … NOT VALID (None pour une table racine).
//! - fn `generate_validate_fk_sql` — ALTER TABLE VALIDATE CONSTRAINT de la FK parent (None pour une table racine).
//! - fn `generate_ddl_preview` — aperçu lisible pour l'IHM (CREATE TABLE + FK inline, non exécuté tel quel).
//! - fn `quote_ident` — échappe un identifiant PG entre guillemets doubles.
//! - fn `generate_autovacuum_sql` — ALTER TABLE SET/RESET (`autovacuum_enabled`).
//! - fn `generate_vacuum_analyze_sql` — `VACUUM (ANALYZE)` d'une table.
//!
//! Création des tables :
//! - fn `build_batch_drop_sql` — construit le SQL de DROP batché.
//! - fn `build_batch_create_sql` — construit le SQL de CREATE batché.
//! - fn `create_tables_no_constraints` — DROP (si demandé) + CREATE par lots, émet la progression.
//! - struct `ConstraintWarning` — contrainte n'ayant pas pu être appliquée après le chargement.
//! - enum `ConstraintKind` — type de contrainte concernée par un `ConstraintWarning` (PK ou FK).
//!
//! Contraintes post-chargement :
//! - fn `calc_maintenance_work_mem_mb` — calcule `maintenance_work_mem` depuis la RAM disponible.
//! - fn `table_sizes` — lignes (`reltuples`) et octets de chaque table, lus dans le catalogue.
//! - fn `largest_first` — tables triées de la plus grosse à la plus petite (taille catalogue).
//! - fn `pk_mem_mb` — mémoire d'une construction de PK : ce que son tri demande, dans le budget commun.
//! - fn `try_configure_constraints_conn` — configure `maintenance_work_mem`/`wal_compression`/index en série/`synchronous_commit` (best-effort).
//! - fn `open_constraints_conn` — ouvre une connexion dédiée aux contraintes et la configure.
//! - fn `add_pk` — pose la PK d'une table (fatal si échec).
//! - fn `validate_fk` — valide la FK d'une table ; si échec, warning + DROP de la contrainte.
//! - fn `set_tables_autovacuum` — coupe / rétablit l'autovacuum de toutes les tables en parallèle (chargement Pass 2).
//! - fn `vacuum_analyze_tables` — `VACUUM (ANALYZE)` non bridé de toutes les tables en parallèle, avant les PK.
//! - fn `add_constraints` — toutes les PK, puis FK `NOT VALID`, puis `VALIDATE` en parallèle ; PK fatale / FK en warning.
//! - fn `constraint_progress` — émet un événement de progression.
//! - fn `to_fk_warning` — convertit une erreur `PostgreSQL` en `ConstraintWarning`.

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::time::Instant;

use futures_util::{StreamExt, TryStreamExt};
use tokio_postgres::Client;

use crate::db::error::pg_err;
use crate::error::{J2sError, Result};
use crate::io::progress_event::{ProgressEvent, ProgressTx};
use crate::schema::table_schema::TableSchema;

fn emit(tx: Option<&ProgressTx>, event: ProgressEvent) {
    if let Some(t) = tx {
        let _ = t.send(event);
    }
}



/// Build the column definition strings for a schema (used by all CREATE TABLE variants).
/// Only generated infrastructure columns get NOT NULL; data columns are always nullable.
fn build_col_defs(schema: &TableSchema) -> Vec<String> {
    schema.columns.iter().map(|col| {
        let null_constraint = if col.not_null && col.is_generated { " NOT NULL" } else { "" };
        format!("    {} {}{}", quote_ident(&col.name), col.pg_type.as_sql(), null_constraint)
    }).collect()
}

/// Generate the CREATE TABLE SQL for a single schema.
/// Uses `IF NOT EXISTS` when `drop_existing` is false (append / rerun mode).
#[must_use]
pub fn generate_create_table(schema: &TableSchema, pg_schema: &str, drop_existing: bool) -> String {
    let if_not_exists = if drop_existing { "" } else { "IF NOT EXISTS " };
    let mut col_defs = build_col_defs(schema);

    // Primary key constraint — name is guaranteed ≤ 63 chars because NamingRegistry
    // caps table names at PG_TABLE_MAX_IDENT (53): "pk_" (3) + 53 = 56 ≤ 63.
    col_defs.push(format!(
        "    CONSTRAINT {} PRIMARY KEY (j2s_id)",
        quote_ident(&format!("pk_{}", schema.name))
    ));

    format!(
        "CREATE TABLE {}{}.{} (\n{}\n)",
        if_not_exists,
        quote_ident(pg_schema),
        quote_ident(&schema.name),
        col_defs.join(",\n")
    )
}

/// Generate CREATE TABLE SQL with columns only — no PRIMARY KEY or FOREIGN KEY constraints.
/// Always uses IF NOT EXISTS so it is safe to rerun.
/// Constraints are added separately via `add_constraints()` after data is loaded.
#[must_use]
pub fn generate_create_table_no_constraints(schema: &TableSchema, pg_schema: &str) -> String {
    let col_defs = build_col_defs(schema);
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (\n{}\n)",
        quote_ident(pg_schema),
        quote_ident(&schema.name),
        col_defs.join(",\n")
    )
}

/// Generate ALTER TABLE … ADD CONSTRAINT … PRIMARY KEY for a single schema.
/// `fillfactor = 100`: `UUIDv7` keys only ever append at the right end, so the 10 % free space of
/// the default would never be used — it would only make the index (and its WAL) bigger (#68).
#[must_use]
pub fn generate_add_pk_sql(schema: &TableSchema, pg_schema: &str) -> String {
    format!(
        "ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY (j2s_id) WITH (fillfactor = 100)",
        quote_ident(pg_schema),
        quote_ident(&schema.name),
        quote_ident(&format!("pk_{}", schema.name)),
    )
}

/// Generate ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY … NOT VALID for a child schema: instant,
/// existing rows are checked later by [`generate_validate_fk_sql`] (#68).
/// Returns None for root tables (no parent).
#[must_use]
pub fn generate_add_fk_sql(schema: &TableSchema, pg_schema: &str) -> Option<String> {
    let parent_name = schema.parent_table.as_ref()?;
    let fk_col = schema
        .columns
        .iter()
        .find(|c| c.is_parent_fk)
        .map_or("j2s_parent_id", |c| c.name.as_str());
    Some(format!(
        "ALTER TABLE {schema_q}.{table_q} \
         ADD CONSTRAINT {constraint_q} \
         FOREIGN KEY ({fk_col_q}) \
         REFERENCES {schema_q}.{parent_q} (j2s_id) NOT VALID",
        schema_q = quote_ident(pg_schema),
        table_q = quote_ident(&schema.name),
        constraint_q = quote_ident(&format!("fk_{}_parent", schema.name)),
        fk_col_q = quote_ident(fk_col),
        parent_q = quote_ident(parent_name),
    ))
}

/// Generate ALTER TABLE … VALIDATE CONSTRAINT for a child's parent FK. Returns None for root tables.
/// Takes only `ROW SHARE` on the parent, so validations against one parent run concurrently (#68).
#[must_use]
pub fn generate_validate_fk_sql(schema: &TableSchema, pg_schema: &str) -> Option<String> {
    schema.parent_table.as_ref()?;
    Some(format!(
        "ALTER TABLE {}.{} VALIDATE CONSTRAINT {}",
        quote_ident(pg_schema),
        quote_ident(&schema.name),
        quote_ident(&format!("fk_{}_parent", schema.name)),
    ))
}

const DDL_BATCH_SIZE: usize = 50;

/// Build a single SQL string that DROPs all schemas in one `batch_execute` call.
fn build_batch_drop_sql(schemas: &[TableSchema], pg_schema: &str) -> String {
    schemas.iter()
        .map(|s| format!(
            "DROP TABLE IF EXISTS {}.{} CASCADE",
            quote_ident(pg_schema),
            quote_ident(&s.name)
        ))
        .collect::<Vec<_>>()
        .join(";\n")
}

/// Build a single SQL string that CREATEs all schemas in one `batch_execute` call.
fn build_batch_create_sql(schemas: &[TableSchema], pg_schema: &str) -> String {
    schemas.iter()
        .map(|s| generate_create_table_no_constraints(s, pg_schema))
        .collect::<Vec<_>>()
        .join(";\n")
}

/// Create all tables without constraints (columns only).
/// Drops existing tables first when `drop_existing = true` (single batch).
/// Creates tables in batches of `DDL_BATCH_SIZE` to reduce round-trips.
pub async fn create_tables_no_constraints(
    client: &Client,
    schemas: &[TableSchema],
    pg_schema: &str,
    drop_existing: bool,
    progress_tx: Option<&ProgressTx>,
) -> Result<()> {
    let total = schemas.len();
    emit(progress_tx, ProgressEvent::DdlStart { table_count: total });

    if drop_existing && !schemas.is_empty() {
        let drop_sql = build_batch_drop_sql(schemas, pg_schema);
        client.batch_execute(&drop_sql).await
            .map_err(|e| pg_err("DROP TABLE batch", &e))?;
    }

    let mut done = 0usize;
    for chunk in schemas.chunks(DDL_BATCH_SIZE) {
        let create_sql = build_batch_create_sql(chunk, pg_schema);
        client.batch_execute(&create_sql).await
            .map_err(|e| pg_err("CREATE TABLE batch", &e))?;
        done += chunk.len();
        emit(progress_tx, ProgressEvent::DdlProgress { done, total });
    }

    emit(progress_tx, ProgressEvent::DdlDone);
    Ok(())
}

/// A constraint that could not be applied after loading data.
#[derive(Debug, Clone)]
pub struct ConstraintWarning {
    pub table: String,
    #[allow(dead_code)]
    pub constraint_type: ConstraintKind,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum ConstraintKind {
    #[allow(dead_code)]
    PrimaryKey,
    ForeignKey,
}

/// Compute per-connection `maintenance_work_mem` in MB: half the available RAM, shared by `conns`
/// concurrent connections (#68). Floor: 256 MB. Cap: `cap_mb` (default 4096 = 4 GB).
/// shortcut: the 256 MB floor is per connection, so `conns` × 256 MB can still exceed RAM on a
/// small machine; lower the floor or `parallel` if that shows up.
fn calc_maintenance_work_mem_mb(available_bytes: u64, cap_mb: u64, conns: usize) -> u64 {
    (available_bytes / 1024 / 1024 / 2 / conns.max(1) as u64).max(256).min(cap_mb)
}

/// `maintenance_work_mem` one row needs for a PK sort to stay in memory: ~24 B index tuple
/// (uuid key) + 24 B `SortTuple`, plus slack for the tuple array growing by doubling (#68).
const PK_SORT_BYTES_PER_ROW: u64 = 72;

/// `maintenance_work_mem` (MB) for one PK build: what its sort needs to stay in memory (a spilled
/// sort writes and re-reads its data), at least 64 MB, at most the whole `budget_mb`.
/// `rows == 0` means unknown (table never vacuumed or analyzed): falls back to `unknown_mb`.
fn pk_mem_mb(rows: u64, budget_mb: u64, unknown_mb: u64) -> u64 {
    if rows == 0 {
        return unknown_mb.min(budget_mb);
    }
    (rows.saturating_mul(PK_SORT_BYTES_PER_ROW) / 1024 / 1024).clamp(64, budget_mb.max(64))
}

/// Rows and bytes (TOAST included) of every table of `pg_schema`, from the catalog. Rows come from
/// `reltuples`: exact right after `VACUUM`, 0 when unknown. The Pass 1 `row_count` is no substitute:
/// it counts JSON objects, not the rows an array or a pivot expands into (#68: a table with
/// 53.6 M rows was counted 4.3 M).
async fn table_sizes(client: &Client, pg_schema: &str) -> Result<HashMap<String, (u64, u64)>> {
    let rows = client.query(
        "SELECT c.relname::text, greatest(c.reltuples, 0)::bigint, pg_table_size(c.oid) \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relkind = 'r'",
        &[&pg_schema],
    ).await.map_err(|e| pg_err("table sizes", &e))?;
    Ok(rows.iter().map(|r| {
        let n = |i: usize| u64::try_from(r.get::<_, i64>(i)).unwrap_or(0);
        (r.get::<_, String>(0), (n(1), n(2)))
    }).collect())
}

/// `schemas` largest first by catalog size, so a late giant does not stall the end of a phase.
fn largest_first<'a>(schemas: &'a [TableSchema], sizes: &HashMap<String, (u64, u64)>) -> Vec<&'a TableSchema> {
    let mut by_size: Vec<&TableSchema> = schemas.iter().collect();
    by_size.sort_by_key(|s| std::cmp::Reverse(sizes.get(&s.name).map_or(0, |&(_, bytes)| bytes)));
    by_size
}

/// Configure a fresh constraints connection: set `maintenance_work_mem`, `wal_compression = lz4`,
/// serial index builds (all non-fatal) and `synchronous_commit = off` (non-fatal on
/// `INSUFFICIENT_PRIVILEGE`, fatal otherwise).
async fn try_configure_constraints_conn(client: &Client, maintenance_work_mem_mb: u64) -> Result<()> {
    if let Err(e) = client.execute(
        &format!("SET maintenance_work_mem = '{maintenance_work_mem_mb}MB'"),
        &[],
    ).await {
        eprintln!("WARNING: SET maintenance_work_mem failed — {e}");
    }
    // Index builds WAL-log every page they write: compress those images (#68). Superuser only,
    // like --tune-wal: silently skipped without the privilege; other failures (PG < 15, server
    // built without lz4) are reported.
    if let Err(e) = client.execute("SET wal_compression = lz4", &[]).await {
        if !e.as_db_error().is_some_and(|db| db.code() == &tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE) {
            eprintln!("WARNING: SET wal_compression = lz4 failed — {e}");
        }
    }
    // Serial index builds: a parallel build always writes its workers' sorted runs to temp files
    // for the leader to merge (#68: ~1 GB of temp for a 1.1 GB index, even with 4 GB of memory),
    // which costs more than the CPU it saves on a write-bound disk. With `pk_mem_mb` sized for the
    // sort, a serial build writes no temp file at all.
    // shortcut: on fast storage with idle CPU a parallel build may win back; make it configurable then.
    if let Err(e) = client.execute("SET max_parallel_maintenance_workers = 0", &[]).await {
        eprintln!("WARNING: SET max_parallel_maintenance_workers = 0 failed — {e}");
    }
    match client.execute("SET synchronous_commit = off", &[]).await {
        Ok(_) => Ok(()),
        Err(e) if e.as_db_error().is_some_and(|db| {
            db.code() == &tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE
        }) => {
            eprintln!("WARNING: SET synchronous_commit = off not permitted — continuing without it");
            Ok(())
        }
        Err(e) => Err(J2sError::Db(e)),
    }
}

/// Opens a dedicated constraints connection configured with `maintenance_work_mem_mb`.
async fn open_constraints_conn(db_url: &str, maintenance_work_mem_mb: u64) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(db_url, tokio_postgres::NoTls)
        .await
        .map_err(|e| J2sError::DbContext(format!("constraints connection: {e}")))?;
    tokio::spawn(async move { let _ = connection.await; });
    try_configure_constraints_conn(&client, maintenance_work_mem_mb).await?;
    Ok(client)
}

/// Adds one table's PRIMARY KEY on its own connection. Failure is fatal.
async fn add_pk(
    schema: &TableSchema,
    pg_schema: &str,
    db_url: &str,
    maintenance_work_mem_mb: u64,
    done: &AtomicUsize,
    total: usize,
    ptx: Option<&ProgressTx>,
) -> Result<()> {
    let client = open_constraints_conn(db_url, maintenance_work_mem_mb).await?;
    let start = Instant::now();
    client.execute(generate_add_pk_sql(schema, pg_schema).as_str(), &[]).await
        .map_err(|e| pg_err(&format!("ADD PRIMARY KEY {}", schema.name), &e))?;
    eprintln!("[CONSTRAINTS] PK {}: {}ms ({maintenance_work_mem_mb} MB)", schema.name, start.elapsed().as_millis());
    constraint_progress(done, total, ptx);
    Ok(())
}

/// Validates one table's `NOT VALID` parent FK on its own connection. On failure the constraint is
/// dropped and a warning returned, so violating data ends up without FK, as before #68.
async fn validate_fk(
    schema: &TableSchema,
    pg_schema: &str,
    db_url: &str,
    maintenance_work_mem_mb: u64,
    done: &AtomicUsize,
    total: usize,
    ptx: Option<&ProgressTx>,
) -> Result<Option<ConstraintWarning>> {
    let Some(sql) = generate_validate_fk_sql(schema, pg_schema) else { return Ok(None) };
    let client = open_constraints_conn(db_url, maintenance_work_mem_mb).await?;
    let start = Instant::now();
    let warning = match client.execute(sql.as_str(), &[]).await {
        Ok(_) => None,
        Err(e) => {
            let drop_sql = format!(
                "ALTER TABLE {}.{} DROP CONSTRAINT IF EXISTS {}",
                quote_ident(pg_schema),
                quote_ident(&schema.name),
                quote_ident(&format!("fk_{}_parent", schema.name)),
            );
            if let Err(drop_e) = client.execute(drop_sql.as_str(), &[]).await {
                eprintln!("WARNING: could not drop unvalidated FK on {} — {drop_e}", schema.name);
            }
            Some(to_fk_warning(&e, &schema.name))
        }
    };
    eprintln!("[CONSTRAINTS] FK {}: {}ms", schema.name, start.elapsed().as_millis());
    constraint_progress(done, total, ptx);
    Ok(warning)
}

/// Add PRIMARY KEY + FOREIGN KEY constraints after data has been loaded (#68).
///
/// 1. Every PK, largest tables first, `parallel.min(4)` connections. Each build gets the
///    `maintenance_work_mem` its sort needs (catalog row count, exact after the import's `VACUUM`)
///    out of a shared budget of half the available RAM, so big sorts stay in memory.
/// 2. Every FK added `NOT VALID` on one connection: no scan, so the self-conflicting
///    `SHARE ROW EXCLUSIVE` lock it takes on the parent is held only for an instant.
/// 3. Every FK validated, `parallel` connections, smaller memory each: `VALIDATE` takes only
///    `ROW SHARE` on the parent, so checks against the same parent run concurrently.
///
/// PK failures are fatal.  FK failures become warnings (and leave no FK behind).
#[allow(clippy::too_many_lines)] // three sequential phases sharing setup: PK → FK NOT VALID → VALIDATE
pub async fn add_constraints(
    db_url: &str,
    schemas: &[TableSchema],
    pg_schema: &str,
    parallel: usize,
    progress_tx: Option<&ProgressTx>,
) -> Result<Vec<ConstraintWarning>> {
    if schemas.is_empty() {
        emit(progress_tx, ProgressEvent::ConstraintsStart { table_count: 0 });
        emit(progress_tx, ProgressEvent::ConstraintsDone);
        return Ok(Vec::new());
    }

    let fk_count = schemas.iter().filter(|s| s.parent_table.is_some()).count();
    let total = schemas.len() + fk_count;
    let done = AtomicUsize::new(0);

    emit(progress_tx, ProgressEvent::ConstraintsStart { table_count: schemas.len() });

    let mut sys = sysinfo::System::new();
    sys.refresh_memory();
    let available = sys.available_memory();
    let parallel = parallel.max(1);
    let pk_conns = parallel.min(4);
    let pk_budget_mb = calc_maintenance_work_mem_mb(available, u64::MAX, 1);
    let pk_unknown_mb = pk_budget_mb / pk_conns as u64;
    let fk_mem_mb = calc_maintenance_work_mem_mb(available, 1024, parallel);

    // One connection for the catalog lookup now and the FK NOT VALID phase later.
    let client = open_constraints_conn(db_url, fk_mem_mb).await?;
    let sizes = table_sizes(&client, pg_schema).await?;
    let by_size = largest_first(schemas, &sizes);

    // Phase 1 — PK. Dynamic queue; first error aborts. Each build holds its memory (in MB permits)
    // while it runs. shortcut: the semaphore is FIFO, so a big build waiting for memory holds back
    // the small ones queued behind it; fine while the disk is the bottleneck, revisit if a lone
    // big build leaves the disk idle.
    let pk_memory = tokio::sync::Semaphore::new(
        usize::try_from(pk_budget_mb).unwrap_or(tokio::sync::Semaphore::MAX_PERMITS),
    );
    futures_util::stream::iter(by_size.iter().map(|s| {
        let mem_mb = pk_mem_mb(sizes.get(&s.name).map_or(0, |&(rows, _)| rows), pk_budget_mb, pk_unknown_mb);
        let (pk_memory, done) = (&pk_memory, &done);
        async move {
            let _permit = pk_memory.acquire_many(u32::try_from(mem_mb).unwrap_or(u32::MAX)).await
                .map_err(|e| J2sError::DbContext(format!("PK memory budget: {e}")))?;
            add_pk(s, pg_schema, db_url, mem_mb, done, total, progress_tx).await
        }
    }))
    .buffer_unordered(pk_conns)
    .try_collect::<Vec<()>>()
    .await?;

    // Phase 2 — FK NOT VALID, sequential on one connection (instant each).
    let mut all_warnings: Vec<ConstraintWarning> = Vec::new();
    let mut to_validate: Vec<&TableSchema> = Vec::new();
    for s in &by_size {
        let Some(sql) = generate_add_fk_sql(s, pg_schema) else { continue };
        match client.execute(sql.as_str(), &[]).await {
            Ok(_) => to_validate.push(s),
            Err(e) => {
                all_warnings.push(to_fk_warning(&e, &s.name));
                constraint_progress(&done, total, progress_tx);
            }
        }
    }
    drop(client);

    // Phase 3 — VALIDATE, in parallel.
    let validated: Vec<Option<ConstraintWarning>> = futures_util::stream::iter(to_validate.iter().map(|s| {
        validate_fk(s, pg_schema, db_url, fk_mem_mb, &done, total, progress_tx)
    }))
    .buffer_unordered(parallel)
    .try_collect()
    .await?;
    all_warnings.extend(validated.into_iter().flatten());

    emit(progress_tx, ProgressEvent::ConstraintsDone);
    Ok(all_warnings)
}

fn constraint_progress(done: &std::sync::atomic::AtomicUsize, total: usize, ptx: Option<&ProgressTx>) {
    let d = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    if let Some(t) = ptx {
        let _ = t.send(ProgressEvent::ConstraintsProgress { done: d, total });
    }
}

fn to_fk_warning(e: &tokio_postgres::Error, table: &str) -> ConstraintWarning {
    let detail = e.as_db_error().map_or_else(
        || e.to_string(),
        |db| format!("{} (code: {})", db.message(), db.code().code()),
    );
    ConstraintWarning { table: table.to_string(), constraint_type: ConstraintKind::ForeignKey, message: detail }
}

/// Generate a human-readable DDL preview for a single schema, including the FK constraint inline.
/// This is for display only — execution uses `generate_create_table` + a separate ALTER TABLE.
#[allow(dead_code)]
#[must_use]
pub fn generate_ddl_preview(schema: &TableSchema, pg_schema: &str) -> String {
    let mut col_defs = build_col_defs(schema);

    col_defs.push(format!(
        "    CONSTRAINT {} PRIMARY KEY (j2s_id)",
        quote_ident(&format!("pk_{}", schema.name))
    ));

    if let Some(ref parent_name) = schema.parent_table {
        let fk_col = schema
            .columns
            .iter()
            .find(|c| c.is_parent_fk)
            .map_or("j2s_parent_id", |c| c.name.as_str());
        col_defs.push(format!(
            "    CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}.{} (j2s_id)",
            quote_ident(&format!("fk_{}_parent", schema.name)),
            quote_ident(fk_col),
            quote_ident(pg_schema),
            quote_ident(parent_name),
        ));
    }

    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (\n{}\n)",
        quote_ident(pg_schema),
        quote_ident(&schema.name),
        col_defs.join(",\n")
    )
}

/// Quote a `PostgreSQL` identifier with double quotes, escaping internal quotes.
#[must_use]
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Autovacuum off for the bulk load and the constraints (#65, #68), or back on.
/// Storage-parameter change only: no table rewrite, even on a populated table.
/// shortcut: `RESET` drops any per-table autovacuum setting a user put on a pre-existing table;
/// remember and restore the previous reloptions if that case shows up.
#[must_use]
pub fn generate_autovacuum_sql(schema: &TableSchema, pg_schema: &str, enabled: bool) -> String {
    let table = format!("{}.{}", quote_ident(pg_schema), quote_ident(&schema.name));
    if enabled {
        format!("ALTER TABLE {table} RESET (autovacuum_enabled)")
    } else {
        format!("ALTER TABLE {table} SET (autovacuum_enabled = false)")
    }
}

/// Runs `generate_autovacuum_sql` on every table, `parallel` tables at a time, one connection each.
pub async fn set_tables_autovacuum(
    db_url: &str,
    schemas: &[TableSchema],
    pg_schema: &str,
    enabled: bool,
    parallel: usize,
) -> Result<()> {
    let results: Vec<Result<()>> = futures_util::stream::iter(schemas.iter().map(|s| {
        let sql = generate_autovacuum_sql(s, pg_schema, enabled);
        async move {
            let client = crate::db::connection::connect(db_url).await?;
            client.batch_execute(&sql).await
                .map_err(|e| pg_err(&format!("autovacuum_enabled={enabled} {}", s.name), &e))
        }
    }))
    .buffer_unordered(parallel.max(1))
    .collect()
    .await;
    results.into_iter().collect()
}

/// Generate `VACUUM (ANALYZE)` for one table.
#[must_use]
pub fn generate_vacuum_analyze_sql(schema: &TableSchema, pg_schema: &str) -> String {
    format!("VACUUM (ANALYZE) {}.{}", quote_ident(pg_schema), quote_ident(&schema.name))
}

/// `VACUUM (ANALYZE)` every table right after the load, before the PKs (#68), largest first,
/// `parallel` at a time, one connection each. The first full read of freshly loaded rows rewrites
/// every page (hint bits, then "all visible"): done here once, unthrottled (`vacuum_cost_delay = 0`),
/// the PK builds no longer rewrite the pages they read, and autovacuum has nothing left to do once
/// the import reports it is done (measured: PK + VACUUM 10.4 GB written vs 6.8 GB the other way).
pub async fn vacuum_analyze_tables(
    db_url: &str,
    schemas: &[TableSchema],
    pg_schema: &str,
    parallel: usize,
) -> Result<()> {
    let sizes = table_sizes(&crate::db::connection::connect(db_url).await?, pg_schema).await?;
    futures_util::stream::iter(largest_first(schemas, &sizes).into_iter().map(|s| async move {
        let client = crate::db::connection::connect(db_url).await?;
        // Two round trips: VACUUM cannot run inside a multi-statement (implicit transaction) string.
        client.batch_execute("SET vacuum_cost_delay = 0").await
            .map_err(|e| pg_err(&format!("VACUUM {}", s.name), &e))?;
        let start = Instant::now();
        client.batch_execute(&generate_vacuum_analyze_sql(s, pg_schema)).await
            .map_err(|e| pg_err(&format!("VACUUM (ANALYZE) {}", s.name), &e))?;
        eprintln!("[VACUUM] {}: {}ms", s.name, start.elapsed().as_millis());
        Ok::<(), J2sError>(())
    }))
    .buffer_unordered(parallel.max(1))
    .try_collect::<Vec<()>>()
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;
    use crate::schema::table_schema::{ColumnSchema, TableSchema};
    use crate::schema::type_tracker::PgType;

    #[test]
    fn autovacuum_sql_off_quotes_identifiers() {
        let schema = TableSchema::new("my\"t".to_string(), vec!["t".to_string()], 0);
        let sql = generate_autovacuum_sql(&schema, "s", false);
        assert_eq!(sql, r#"ALTER TABLE "s"."my""t" SET (autovacuum_enabled = false)"#);
    }

    #[test]
    fn autovacuum_sql_on_resets() {
        let schema = TableSchema::new("t".to_string(), vec!["t".to_string()], 0);
        assert_eq!(generate_autovacuum_sql(&schema, "s", true), r#"ALTER TABLE "s"."t" RESET (autovacuum_enabled)"#);
    }

    #[test]
    fn vacuum_analyze_sql_quotes_identifiers() {
        let schema = TableSchema::new("my\"t".to_string(), vec!["t".to_string()], 0);
        assert_eq!(generate_vacuum_analyze_sql(&schema, "s"), r#"VACUUM (ANALYZE) "s"."my""t""#);
    }

    #[test]
    fn test_generate_create_table() {
        let mut schema = TableSchema::new("users".to_string(), vec!["users".to_string()], 0);
        schema.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        schema.columns.push(ColumnSchema {
            name: "name".to_string(),
            original_name: "name".to_string(),
            pg_type: PgType::VarChar(60),
            not_null: true,
            is_generated: false,
            is_parent_fk: false,
        });
        schema.columns.push(ColumnSchema {
            name: "age".to_string(),
            original_name: "age".to_string(),
            pg_type: PgType::Integer,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });

        let sql = generate_create_table(&schema, "public", true);
        assert!(sql.contains("CREATE TABLE \"public\".\"users\""));
        assert!(sql.contains("\"j2s_id\" UUID NOT NULL"));
        assert!(sql.contains("\"name\" VARCHAR(60)"));
        assert!(sql.contains("\"age\" INTEGER"));
        assert!(sql.contains("PRIMARY KEY (j2s_id)"));
    }

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident("my\"table"), "\"my\"\"table\"");
    }

    fn make_root_schema() -> TableSchema {
        let mut s = TableSchema::new("products".to_string(), vec!["products".to_string()], 0);
        s.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        s.columns.push(ColumnSchema {
            name: "title".to_string(),
            original_name: "title".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        s
    }

    fn make_child_schema() -> TableSchema {
        let mut s = TableSchema::new("tags".to_string(), vec!["products".to_string(), "tags".to_string()], 1);
        s.parent_table = Some("products".to_string());
        s.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        s.columns.push(ColumnSchema {
            name: "j2s_parent_id".to_string(),
            original_name: "j2s_parent_id".to_string(),
            pg_type: PgType::Uuid,
            not_null: true,
            is_generated: true,
            is_parent_fk: true,
        });
        s.columns.push(ColumnSchema {
            name: "value".to_string(),
            original_name: "value".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        s
    }

    #[test]
    fn no_constraints_create_excludes_pk_and_fk() {
        let root = make_root_schema();
        let sql = generate_create_table_no_constraints(&root, "public");
        assert!(sql.contains("CREATE TABLE"), "should be a CREATE TABLE");
        assert!(!sql.contains("PRIMARY KEY"), "no PK inline");
        assert!(!sql.contains("FOREIGN KEY"), "no FK inline");
        assert!(sql.contains("\"j2s_id\""), "j2s_id column present");
        assert!(sql.contains("\"title\""), "title column present");
    }

    #[test]
    fn no_constraints_create_uses_if_not_exists() {
        let root = make_root_schema();
        let sql = generate_create_table_no_constraints(&root, "public");
        assert!(sql.contains("IF NOT EXISTS"), "always idempotent");
    }

    #[test]
    fn add_pk_sql_correct() {
        let root = make_root_schema();
        let sql = generate_add_pk_sql(&root, "public");
        assert!(sql.contains("ALTER TABLE"), "ALTER TABLE");
        assert!(sql.contains("ADD CONSTRAINT"), "ADD CONSTRAINT");
        assert!(sql.contains("PRIMARY KEY"), "PRIMARY KEY");
        assert!(sql.contains("\"pk_products\""), "constraint name");
        assert!(sql.contains("\"products\""), "table name");
        assert!(sql.ends_with("WITH (fillfactor = 100)"), "UUIDv7 keys append only (#68)");
    }

    #[test]
    fn add_fk_sql_root_returns_none() {
        let root = make_root_schema();
        assert!(generate_add_fk_sql(&root, "public").is_none(), "root has no parent");
    }

    #[test]
    fn add_fk_sql_child_correct() {
        let child = make_child_schema();
        let sql = generate_add_fk_sql(&child, "public").expect("child should have FK sql");
        assert!(sql.contains("ALTER TABLE"), "ALTER TABLE");
        assert!(sql.contains("FOREIGN KEY"), "FOREIGN KEY");
        assert!(sql.contains("REFERENCES"), "REFERENCES");
        assert!(sql.contains("\"products\""), "references parent");
        assert!(sql.contains("\"tags\""), "on child table");
        assert!(sql.ends_with("NOT VALID"), "validated later, in parallel (#68)");
    }

    #[test]
    fn validate_fk_sql_child_and_root() {
        assert!(generate_validate_fk_sql(&make_root_schema(), "public").is_none(), "root has no parent");
        assert_eq!(
            generate_validate_fk_sql(&make_child_schema(), "public").as_deref(),
            Some(r#"ALTER TABLE "public"."tags" VALIDATE CONSTRAINT "fk_tags_parent""#),
        );
    }

    #[test]
    fn emit_helper_sends_to_channel_and_ignores_none() {
        use crate::io::progress_event::ProgressEvent;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        emit(Some(&tx), ProgressEvent::DdlStart { table_count: 5 });
        emit(Some(&tx), ProgressEvent::DdlProgress { done: 1, total: 5 });
        emit(Some(&tx), ProgressEvent::DdlDone);
        emit(None, ProgressEvent::DdlDone); // None must not panic
        drop(tx);
        assert!(matches!(rx.try_recv().unwrap(), ProgressEvent::DdlStart { table_count: 5 }));
        assert!(matches!(rx.try_recv().unwrap(), ProgressEvent::DdlProgress { done: 1, total: 5 }));
        assert!(matches!(rx.try_recv().unwrap(), ProgressEvent::DdlDone));
        assert!(rx.try_recv().is_err(), "None emit produces no event");
    }

    #[test]
    fn constraints_total_counts_pk_plus_fk_only() {
        // 1 root (no FK) + 2 children (each has FK) → total = 3 PKs + 2 FKs = 5
        let root = make_root_schema();
        let child1 = make_child_schema();
        let mut child2 = make_child_schema();
        child2.name = "tags2".to_string();
        let schemas = [root, child1, child2];
        let fk_count = schemas.iter().filter(|s| s.parent_table.is_some()).count();
        let total = schemas.len() + fk_count;
        assert_eq!(fk_count, 2);
        assert_eq!(total, 5);
    }

    #[test]
    fn build_batch_create_sql_contains_all_tables() {
        let root = make_root_schema();
        let child = make_child_schema();
        let sql = build_batch_create_sql(&[root, child], "public");
        assert_eq!(sql.matches("CREATE TABLE").count(), 2, "one CREATE TABLE per schema");
        assert!(sql.contains("\"products\""), "root table present");
        assert!(sql.contains("\"tags\""), "child table present");
        assert!(sql.contains(";\n"), "statements separated by semicolon");
        // No PK or FK constraints (columns only)
        assert!(!sql.contains("PRIMARY KEY"), "no PK in no-constraints create");
        assert!(!sql.contains("FOREIGN KEY"), "no FK in no-constraints create");
    }

    #[test]
    fn build_batch_drop_sql_contains_all_tables_with_cascade() {
        let root = make_root_schema();
        let child = make_child_schema();
        let sql = build_batch_drop_sql(&[root, child], "public");
        assert_eq!(sql.matches("DROP TABLE IF EXISTS").count(), 2, "one DROP per schema");
        assert!(sql.contains("CASCADE"), "DROP includes CASCADE");
        assert!(sql.contains("\"products\""), "root table present");
        assert!(sql.contains("\"tags\""), "child table present");
    }

    #[test]
    fn build_batch_create_sql_empty_slice_returns_empty() {
        assert_eq!(build_batch_create_sql(&[], "public"), "");
        assert_eq!(build_batch_drop_sql(&[], "public"), "");
    }

    #[test]
    fn pk_mem_mb_sized_from_row_count_within_budget() {
        assert_eq!(pk_mem_mb(1_000, 4096, 1024), 64, "small table: 64 MB floor");
        // 53.6 M rows × 72 B = 3679 MB: the biggest OFF table sorts in memory under a 4 GB budget
        assert_eq!(pk_mem_mb(53_588_448, 4096, 1024), 3679);
        assert_eq!(pk_mem_mb(1_000_000_000, 4096, 1024), 4096, "capped at the whole budget");
        assert_eq!(pk_mem_mb(0, 4096, 1024), 1024, "unknown row count: fallback share");
        assert_eq!(pk_mem_mb(0, 512, 1024), 512, "fallback never exceeds the budget");
    }

    #[test]
    fn maintenance_work_mem_mb_floor_on_zero_available() {
        assert_eq!(calc_maintenance_work_mem_mb(0, 4096, 1), 256);
    }

    #[test]
    fn maintenance_work_mem_mb_cap_on_large_ram() {
        // 32 GB available → raw = 16384 MB → capped at 4096
        assert_eq!(calc_maintenance_work_mem_mb(32 * 1024 * 1024 * 1024, 4096, 1), 4096);
    }

    #[test]
    fn maintenance_work_mem_mb_nominal_2gb() {
        // 2 GB available → raw = 1024 MB → no floor, no cap
        assert_eq!(calc_maintenance_work_mem_mb(2 * 1024 * 1024 * 1024, 4096, 1), 1024);
    }

    #[test]
    fn maintenance_work_mem_mb_split_across_connections() {
        // 8 GB available, 8 connections → 4096 MB budget / 8 = 512 MB each
        assert_eq!(calc_maintenance_work_mem_mb(8 * 1024 * 1024 * 1024, 4096, 8), 512);
    }

}
