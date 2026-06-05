//! Pass 2 — data insertion: stream the JSON file a second time and write rows to PostgreSQL.
//!
//! Execution is split into two phases:
//! - **Phase A** (streaming): workers read the JSON in parallel and write COPY-format rows
//!   to per-table temp files. Large sinks are snapshotted to disk to bound memory usage.
//! - **Phase B** (COPY): all temp files are bulk-loaded into PostgreSQL via `COPY FROM STDIN`,
//!   then PK and FK constraints are applied.
//!
//! The main entry point is [`run`], which requires a Tokio runtime and an open
//! [`tokio_postgres::Client`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::Value;
use simd_json;
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::anomaly::collector::{AnomalyCollector, AnomalyEvent, AnomalyProxy};
use crate::db::copy_sink::{copy_snapshot_to_pg, merge_copy_to_db, TempFileSink};
use crate::schema::PATH_SEP;
use crate::db::ddl::{add_constraints, ConstraintWarning};
use crate::error::{J2sError, Result};
use crate::io::progress::ProgressTracker;
use crate::io::progress_event::{ProgressEvent, ProgressTx};
use crate::io::reader::{file_size, JsonReader};
use crate::pass2::insert::{insert_object, InsertCtx};
use crate::schema::table_schema::TableSchema;

/// Phase B batch copy handle: one task processes a batch of tables, returns all (table, rows).
type CopyHandle = tokio::task::JoinHandle<Result<Vec<(String, u64)>>>;
/// Interim copy handle: one background task copies a single sink snapshot.
type InterimCopyHandle = tokio::task::JoinHandle<Result<(String, u64)>>;
/// Phase A worker handle: returns the interim copy handles it spawned.
type WorkerHandle = tokio::task::JoinHandle<Result<Vec<InterimCopyHandle>>>;

/// Wall-clock breakdown of the two main phases of Pass 2.
#[allow(dead_code)]
pub struct Pass2Timing {
    /// Time spent streaming the JSON file and writing rows to temp files.
    pub streaming_ms: u64,
    /// Time spent in the COPY-to-PostgreSQL phase (including constraints).
    pub copy_ms: u64,
}

impl Pass2Timing {
    #[allow(dead_code)]
    #[must_use]
    pub fn total_ms(&self) -> u64 {
        self.streaming_ms + self.copy_ms
    }
}

/// Pass 2 result summary.
#[allow(dead_code)]
pub struct Pass2Result {
    pub rows_per_table: HashMap<String, u64>,
    pub anomaly_collector: AnomalyCollector,
    /// FK constraints that could not be applied after loading data.
    /// PK failures are fatal (returned as Err); only FK failures appear here.
    pub constraint_warnings: Vec<ConstraintWarning>,
    pub timing: Pass2Timing,
}

const PROGRESS_INTERVAL: u64 = 1_000;

/// Per-worker budget for in-memory pending data during Phase A.
/// When the sum of all sinks' `bytes_buffered` exceeds this value, all sinks
/// are force-spilled to disk, freeing the pending allocations. The budget
/// then advances by the same amount so spills are evenly spaced throughout
/// the streaming phase regardless of how many tables the schema has.
pub const PER_WORKER_FLUSH_THRESHOLD: u64 = 256 * 1024 * 1024; // 256 MiB

/// Minimum bytes a sink must hold before it gets an interim COPY to PG.
/// Sinks below this are force-spilled to disk (RAM freed) and COPYed in Phase B.
/// Prevents COPY overhead (~5 ms/table) on tables with very few rows.
const MIN_SINK_COPY_BYTES: u64 = 16 * 1024 * 1024; // 16 MiB

/// All parameters controlling a Pass 2 run.
pub struct Pass2Config {
    pub root_table: String,
    pub pg_schema: String,
    pub parallel: usize,
    pub anomaly_dir: Option<PathBuf>,
    pub temp_dir: Option<PathBuf>,
    pub per_worker_budget: Option<u64>,
    pub min_interim_copy_bytes: Option<u64>,
    /// Stop pass 2 after inserting this many root objects. None = full import.
    /// Some(0) = create tables with no rows.
    pub limit: Option<u64>,
}

fn validate_run_params(parallel: usize) -> Result<()> {
    if parallel == 0 {
        return Err(J2sError::InvalidInput(
            "parallel must be >= 1 (0 would produce an empty connection pool)".to_string(),
        ));
    }
    Ok(())
}

/// Shared config injected into each streaming worker.
#[derive(Clone)]
struct WorkerConfig {
    pg_url: String,
    progress_tx: Option<ProgressTx>,
    copy_sem: Arc<tokio::sync::Semaphore>,
    worker_budget: u64,
    interim_copy_threshold: u64,
}

impl WorkerConfig {
    fn new(
        pg_url: String,
        progress_tx: Option<ProgressTx>,
        copy_sem: Arc<tokio::sync::Semaphore>,
        worker_budget: u64,
        interim_copy_threshold: u64,
    ) -> Self {
        WorkerConfig { pg_url, progress_tx, copy_sem, worker_budget, interim_copy_threshold }
    }
}

/// When the worker budget is reached: snapshot large sinks → interim COPY task,
/// spill small sinks to disk. Called from within the worker loop.
#[allow(clippy::too_many_lines)] // per-sink decision + spawn tightly coupled around sink_arc
fn trigger_budget_flush(
    sinks: &HashMap<String, Arc<Mutex<TempFileSink>>>,
    copy_handles: &mut Vec<InterimCopyHandle>,
    cfg: &WorkerConfig,
) {
    for (table_name, sink_arc) in sinks {
        let snap = {
            let mut s = sink_arc.lock().expect("sink mutex is not poisoned");
            if s.bytes_buffered >= cfg.interim_copy_threshold {
                s.take_flush_snapshot()
            } else {
                let _ = s.force_spill();
                None
            }
        };
        if let Some(snap) = snap {
            let sem = cfg.copy_sem.clone();
            let url = cfg.pg_url.clone();
            let ptx = cfg.progress_tx.clone();
            let sink_arc2 = Arc::clone(sink_arc);
            let tname = table_name.clone();
            copy_handles.push(tokio::spawn(async move {
                let _permit = sem.acquire_owned().await.expect("semaphore closed");
                let conn = crate::db::connection::connect(&url).await?;
                conn.execute("SET synchronous_commit = off", &[]).await
                    .map_err(crate::error::J2sError::Db)?;
                let rows = copy_snapshot_to_pg(snap, &conn).await?;
                sink_arc2.lock().expect("sink mutex is not poisoned").apply_flush(rows);
                if rows > 0 {
                    if let Some(tx) = ptx {
                        let _ = tx.send(ProgressEvent::Pass2Flush {
                            table_name: tname.clone(),
                            rows_flushed: rows,
                        });
                    }
                }
                Ok::<(String, u64), J2sError>((tname, rows))
            }));
        }
    }
}

/// Process one worker's stream of JSON byte chunks: parse, insert into sinks,
/// and fire interim COPY snapshots when the per-worker budget is reached.
/// Returns background COPY handles spawned during streaming.
#[allow(clippy::too_many_lines)] // async event loop: receive → parse → insert → budget check
async fn run_worker(
    mut rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    mut sinks: HashMap<String, Arc<Mutex<TempFileSink>>>,
    anomaly_tx: tokio::sync::mpsc::UnboundedSender<AnomalyEvent>,
    path_map: Arc<HashMap<String, TableSchema>>,
    root_schema: Arc<TableSchema>,
    cancel: CancellationToken,
    cfg: WorkerConfig,
) -> Result<Vec<tokio::task::JoinHandle<Result<(String, u64)>>>> {
    let mut proxy = AnomalyProxy::new(anomaly_tx);
    let mut copy_handles: Vec<InterimCopyHandle> = Vec::new();
    let mut my_bytes: u64 = 0;

    loop {
        let mut bytes = tokio::select! {
            _ = cancel.cancelled() => break,
            msg = rx.recv() => match msg { Some(b) => b, None => break },
        };
        let obj_len = bytes.len() as u64;
        let obj = match simd_json::from_slice::<serde_json::Value>(&mut bytes) {
            Ok(Value::Object(o)) => o,
            Ok(other) => return Err(J2sError::InvalidInput(format!(
                "Expected JSON object at root level, found: {other}"
            ))),
            Err(e) => return Err(J2sError::InvalidInput(format!(
                "JSON parse error in worker: {e}"
            ))),
        };

        insert_object(&path_map, &mut InsertCtx { sinks: &mut sinks, anomalies: &mut proxy }, &root_schema, &obj, Uuid::now_v7(), None, None)?;

        my_bytes += obj_len;
        if my_bytes >= cfg.worker_budget {
            my_bytes = 0;
            trigger_budget_flush(&sinks, &mut copy_handles, &cfg);
        }
    }

    for sink_arc in sinks.values() {
        let _ = sink_arc.lock().expect("sink mutex is not poisoned").force_spill();
    }
    Ok(copy_handles)
}

fn unwrap_and_sort_sinks(
    shared_sinks: HashMap<String, Arc<Mutex<TempFileSink>>>,
) -> Vec<(String, TempFileSink)> {
    let mut sinks: Vec<(String, TempFileSink)> = shared_sinks
        .into_iter()
        .filter_map(|(name, arc)| {
            let sink = Arc::try_unwrap(arc).ok()?.into_inner().ok()?;
            if sink.row_count > 0 || sink.total_flushed > 0 { Some((name, sink)) } else { None }
        })
        .collect();
    sinks.sort_by(|a, b| a.0.cmp(&b.0));
    sinks
}

async fn collect_copy_results(
    copy_handles: Vec<CopyHandle>,
    interim_rows: HashMap<String, u64>,
) -> Result<HashMap<String, u64>> {
    let mut rows_per_table: HashMap<String, u64> = HashMap::new();
    for handle in copy_handles {
        match handle.await {
            Ok(Ok(results)) => {
                for (name, rows) in results {
                    *rows_per_table.entry(name).or_insert(0) += rows;
                }
            }
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(J2sError::InvalidInput(format!("copy task panic: {e}"))),
        }
    }
    for (name, rows) in interim_rows {
        *rows_per_table.entry(name).or_insert(0) += rows;
    }
    Ok(rows_per_table)
}

/// Phase B: unwrap shared sinks after streaming, distribute round-robin across
/// `parallel` PG connections, COPY each table, then merge with interim rows.
async fn copy_batch(batch: Vec<(String, Vec<TempFileSink>)>, url: String, ptx: Option<ProgressTx>) -> Result<Vec<(String, u64)>> {
    use crate::db::connection::connect;
    let conn = connect(&url).await?;
    conn.execute("SET synchronous_commit = off", &[]).await
        .map_err(crate::error::J2sError::Db)?;
    let mut results = Vec::new();
    for (table_name, sinks) in batch {
        let rows = merge_copy_to_db(sinks, &conn).await?;
        if let Some(ref tx) = ptx {
            let _ = tx.send(ProgressEvent::Pass2Flush { table_name: table_name.clone(), rows_flushed: rows });
        }
        results.push((table_name, rows));
    }
    Ok(results)
}

async fn phase_copy(
    shared_sinks: HashMap<String, Arc<Mutex<TempFileSink>>>,
    parallel: usize,
    pg_url: &str,
    progress_tx: &Option<ProgressTx>,
    interim_rows: HashMap<String, u64>,
) -> Result<HashMap<String, u64>> {
    let mut all_sinks = unwrap_and_sort_sinks(shared_sinks);

    let mut table_batches: Vec<Vec<(String, Vec<TempFileSink>)>> =
        (0..parallel).map(|_| Vec::new()).collect();
    for (i, (table_name, sink)) in all_sinks.drain(..).enumerate() {
        table_batches[i % parallel].push((table_name, vec![sink]));
    }

    let mut copy_handles: Vec<CopyHandle> = Vec::with_capacity(parallel);
    for batch in table_batches {
        if batch.is_empty() { continue; }
        copy_handles.push(tokio::task::spawn(copy_batch(batch, pg_url.to_string(), progress_tx.clone())));
    }

    collect_copy_results(copy_handles, interim_rows).await
}

/// Spawn the blocking anomaly writer task. Returns `(sender, handle)`.
fn spawn_anomaly_writer(
    anomaly_dir: Option<std::path::PathBuf>,
) -> (tokio::sync::mpsc::UnboundedSender<AnomalyEvent>, tokio::task::JoinHandle<Result<AnomalyCollector>>) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<AnomalyEvent>();
    let handle = tokio::task::spawn_blocking(move || {
        let mut collector = AnomalyCollector::new(anomaly_dir);
        while let Some(event) = rx.blocking_recv() {
            match event {
                AnomalyEvent::Record { table, column, row_id, expected_type, actual_value, actual_type } => {
                    collector.record(&table, &column, &row_id, &expected_type, &actual_value, &actual_type)?;
                }
                AnomalyEvent::IncTotal { table } => collector.inc_total(&table),
            }
        }
        collector.finish()?;
        Ok(collector)
    });
    (tx, handle)
}

/// Warn (via stderr + progress channel) if any root tables already contain rows.
async fn preflight_warn_nonempty(
    schemas: &[crate::schema::table_schema::TableSchema],
    client: &Client,
    pg_schema: &str,
    progress_tx: &Option<ProgressTx>,
) {
    let mut nonempty: Vec<String> = Vec::new();
    for s in schemas.iter().filter(|s| s.is_root()) {
        let sql = format!(
            r#"SELECT 1 FROM "{}"."{}" LIMIT 1"#,
            pg_schema.replace('"', "\"\""),
            s.name.replace('"', "\"\"")
        );
        if let Ok(Some(_)) = client.query_opt(&sql, &[]).await {
            nonempty.push(s.name.clone());
        }
    }
    if !nonempty.is_empty() {
        let msg = format!(
            "WARNING: {} root table(s) are non-empty before import: {}. \
             Rows will be appended. Drop the schema first if this is unintended.",
            nonempty.len(),
            nonempty.join(", ")
        );
        eprintln!("{msg}");
        if let Some(ref tx) = progress_tx {
            let _ = tx.send(ProgressEvent::Pass2Log(msg));
        }
    }
}

/// Send the final progress update and finish the progress bar after the dispatch loop.
fn finalize_dispatch(
    progress_tx: &Option<ProgressTx>,
    progress: &Option<ProgressTracker>,
    rows_processed: u64,
    bytes_read: u64,
    total_bytes: u64,
) {
    if let Some(ref tx) = progress_tx {
        if rows_processed > 0 && !rows_processed.is_multiple_of(PROGRESS_INTERVAL) {
            let _ = tx.send(ProgressEvent::Pass2Progress { rows_processed, bytes_read, total_bytes });
        }
    }
    if let Some(ref bar) = progress { bar.finish(); }
}

/// Emit per-table anomaly updates and the final Pass2Done event.
fn emit_completion_events(
    tx: &ProgressTx,
    anomalies: &AnomalyCollector,
    rows_per_table: &HashMap<String, u64>,
    constraint_warnings: &[crate::db::ddl::ConstraintWarning],
) {
    for (table_name, count) in anomalies.per_table_anomaly_counts() {
        let _ = tx.send(ProgressEvent::Pass2AnomalyUpdate { table_name, count });
    }
    let _ = tx.send(ProgressEvent::Pass2Done {
        total_rows: rows_per_table.values().sum(),
        anomaly_count: anomalies.total_anomalies(),
        constraint_warning_count: constraint_warnings.len() as u64,
    });
}

/// Log FK constraint warnings to stderr and the progress channel.
fn log_constraint_warnings(warnings: &[crate::db::ddl::ConstraintWarning], progress_tx: &Option<ProgressTx>) {
    if warnings.is_empty() { return; }
    eprintln!("WARNING: {} FK constraint(s) could not be applied after import:", warnings.len());
    for w in warnings {
        let msg = format!("FK warning — {} : {}", w.table, w.message);
        eprintln!("  {msg}");
        if let Some(ref tx) = progress_tx {
            let _ = tx.send(ProgressEvent::Pass2Log(msg));
        }
    }
}

/// Dispatch raw JSON bytes from `reader` round-robin to workers.
/// Returns `(rows_processed, worker_died)`.
#[allow(clippy::too_many_lines)] // dense event loop: dispatch + limit + progress bar + progress channel
async fn dispatch_loop(
    reader: &mut JsonReader,
    senders: &[tokio::sync::mpsc::Sender<Vec<u8>>],
    progress_tx: &Option<ProgressTx>,
    progress: &Option<ProgressTracker>,
    limit: Option<u64>,
    total_bytes: u64,
) -> Result<(u64, bool)> {
    let parallel = senders.len();
    let mut rows_processed = 0u64;
    let mut robin = 0usize;
    let mut worker_died = false;
    if limit != Some(0) {
        'dispatch: while let Some(item) = reader.next_raw() {
            let bytes = item?;
            if senders[robin].send(bytes).await.is_err() {
                worker_died = true;
                break 'dispatch;
            }
            rows_processed += 1;
            if limit.is_some_and(|n| rows_processed >= n) {
                break 'dispatch;
            }
            robin = (robin + 1) % parallel;
            if let Some(ref bar) = progress {
                bar.inc_rows(1);
            }
            if rows_processed.is_multiple_of(PROGRESS_INTERVAL) {
                if let Some(ref tx) = progress_tx {
                    let _ = tx.send(ProgressEvent::Pass2Progress {
                        rows_processed,
                        bytes_read: reader.bytes_read(),
                        total_bytes,
                    });
                }
            }
        }
    }
    Ok((rows_processed, worker_died))
}

/// Join Phase A workers, await the anomaly writer, and collect interim COPY results.
/// Returns `(merged_anomalies, interim_rows)`.
#[allow(clippy::too_many_lines)] // sequential join pipeline, error accumulation pattern non-factorisable
async fn join_phase_a(
    worker_handles: Vec<WorkerHandle>,
    anomaly_tx: tokio::sync::mpsc::UnboundedSender<AnomalyEvent>,
    anomaly_writer_handle: tokio::task::JoinHandle<Result<AnomalyCollector>>,
    worker_died: bool,
) -> Result<(AnomalyCollector, HashMap<String, u64>)> {
    let mut all_copy_handles: Vec<InterimCopyHandle> = Vec::new();
    let mut first_error: Option<J2sError> = None;
    for handle in worker_handles {
        match handle.await {
            Ok(Ok(copy_handles)) => all_copy_handles.extend(copy_handles),
            Ok(Err(e)) => { if first_error.is_none() { first_error = Some(e); } }
            Err(e) => { if first_error.is_none() {
                first_error = Some(J2sError::InvalidInput(format!("worker panic: {e}")));
            }}
        }
    }
    // Drop anomaly_tx so the writer task's channel closes and it can finish.
    drop(anomaly_tx);
    let merged_anomalies = match anomaly_writer_handle.await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(J2sError::InvalidInput(format!("anomaly writer task panicked: {e}"))),
    };
    if let Some(err) = first_error { return Err(err); }
    if worker_died {
        return Err(J2sError::InvalidInput("worker channel closed unexpectedly".into()));
    }
    let mut interim_rows: HashMap<String, u64> = HashMap::new();
    for handle in all_copy_handles {
        match handle.await {
            Ok(Ok((table_name, rows))) => { *interim_rows.entry(table_name).or_insert(0) += rows; }
            Ok(Err(e)) => { if first_error.is_none() { first_error = Some(e); } }
            Err(e) => { if first_error.is_none() {
                first_error = Some(J2sError::InvalidInput(format!("copy task panic: {e}")));
            }}
        }
    }
    if let Some(err) = first_error { return Err(err); }
    Ok((merged_anomalies, interim_rows))
}

/// Run Pass 2: stream the file into per-worker temp-file buffers, COPY to
/// PostgreSQL, then add PRIMARY KEY and FOREIGN KEY constraints.
///
/// **The caller is responsible for creating tables** (without constraints)
/// via `db::ddl::create_tables_no_constraints()` before calling this function.
///
fn find_root_schema(schemas: &[TableSchema], root_table: &str, sep: &str) -> Result<TableSchema> {
    schemas
        .iter()
        .find(|s| s.path.join(sep) == root_table)
        .cloned()
        .ok_or_else(|| J2sError::Schema(format!("Root table '{root_table}' not found")))
}

fn build_shared_sinks(
    schemas: &[TableSchema],
    pg_schema: &str,
    temp_dir: Option<&Path>,
) -> Result<HashMap<String, Arc<Mutex<TempFileSink>>>> {
    schemas
        .iter()
        .map(|s| Ok((
            s.name.clone(),
            Arc::new(Mutex::new(TempFileSink::new(s, pg_schema, temp_dir)?)),
        )))
        .collect()
}

/// Internal phases:
///   B — N workers (parallel ≥ 1) stream root objects round-robin into
///       per-table `TempFileSink` buffers. A dedicated flush task runs
///       concurrently, COPYing sinks to PG (up to `parallel` simultaneous
///       connections) as they fill up and when workers finish.
///   D — `add_constraints()` adds PRIMARY KEY (fatal on error) then
///       FOREIGN KEY (failures become `constraint_warnings`).
#[allow(clippy::too_many_lines)] // top-level orchestrator: delegates to phase functions, not splittable further
pub async fn run(
    path: &Path,
    schemas: &[TableSchema],
    client: &Client,
    pg_url: &str,
    config: &Pass2Config,
    progress_tx: Option<ProgressTx>,
) -> Result<Pass2Result> {
    let worker_budget = config.per_worker_budget.unwrap_or(PER_WORKER_FLUSH_THRESHOLD);
    let interim_copy_threshold = config.min_interim_copy_bytes.unwrap_or(MIN_SINK_COPY_BYTES);

    validate_run_params(config.parallel)?;
    let total_bytes = file_size(path)?;
    let progress = progress_tx.is_none().then(|| ProgressTracker::new(total_bytes, "Pass 2"));

    let sep = PATH_SEP.to_string();
    let path_map: HashMap<String, TableSchema> = schemas.iter().map(|s| (s.path.join(&sep), s.clone())).collect();
    let root_schema = find_root_schema(schemas, &config.root_table, &sep)?;

    if let Some(ref dir) = config.anomaly_dir {
        std::fs::create_dir_all(dir).map_err(J2sError::Io)?;
    }
    let (anomaly_tx, anomaly_writer_handle) = spawn_anomaly_writer(config.anomaly_dir.clone());

    let parallel = config.parallel.max(1);
    preflight_warn_nonempty(schemas, client, &config.pg_schema, &progress_tx).await;

    let cancel = CancellationToken::new();
    let _cancel_guard = cancel.clone().drop_guard();

    // Phase A — Parallel streaming: shared sinks, N workers, round-robin dispatch.
    let (mut reader, _format) = JsonReader::open(path)?;
    let path_map_arc: Arc<HashMap<String, TableSchema>> = Arc::new(path_map);
    let root_schema_arc: Arc<TableSchema> = Arc::new(root_schema.clone());
    let copy_sem: Arc<tokio::sync::Semaphore> = Arc::new(tokio::sync::Semaphore::new(parallel));
    let shared_sinks = build_shared_sinks(schemas, &config.pg_schema, config.temp_dir.as_deref())?;
    let worker_cfg = WorkerConfig::new(pg_url.to_string(), progress_tx.clone(), copy_sem.clone(), worker_budget, interim_copy_threshold);
    let (senders, worker_handles) = spawn_pass2_workers(
        parallel, &shared_sinks, &anomaly_tx, path_map_arc, root_schema_arc, cancel, worker_cfg,
    );

    let stream_start = Instant::now();
    let (rows_processed, worker_died) = dispatch_loop(&mut reader, &senders, &progress_tx, &progress, config.limit, total_bytes).await?;
    drop(senders);

    finalize_dispatch(&progress_tx, &progress, rows_processed, reader.bytes_read(), total_bytes);
    let streaming_ms = stream_start.elapsed().as_millis() as u64;
    eprintln!("Pass 2 streaming done ({parallel} workers). Flushing remaining rows to PostgreSQL...");

    let (merged_anomalies, interim_rows) =
        join_phase_a(worker_handles, anomaly_tx, anomaly_writer_handle, worker_died).await?;

    // Phase B — COPY remaining data to PG.
    let copy_start = Instant::now();
    let rows_per_table = phase_copy(shared_sinks, parallel, pg_url, &progress_tx, interim_rows).await?;

    // Phase D — Constraints: PK (fatal), FK (warnings).
    let constraint_warnings = add_constraints(pg_url, schemas, &config.pg_schema, parallel, progress_tx.as_ref()).await?;
    log_constraint_warnings(&constraint_warnings, &progress_tx);

    build_pass2_result(merged_anomalies, rows_per_table, constraint_warnings, progress_tx, copy_start, streaming_ms)
}

fn build_pass2_result(
    mut merged_anomalies: AnomalyCollector,
    rows_per_table: HashMap<String, u64>,
    constraint_warnings: Vec<ConstraintWarning>,
    progress_tx: Option<ProgressTx>,
    copy_start: Instant,
    streaming_ms: u64,
) -> Result<Pass2Result> {
    if let Some(ref tx) = progress_tx {
        emit_completion_events(tx, &merged_anomalies, &rows_per_table, &constraint_warnings);
    }
    merged_anomalies.finish()?;
    let copy_ms = copy_start.elapsed().as_millis() as u64;
    eprintln!("Pass 2 timing: streaming={streaming_ms}ms, copy={copy_ms}ms, total={}ms", streaming_ms + copy_ms);
    Ok(Pass2Result {
        rows_per_table,
        anomaly_collector: merged_anomalies,
        constraint_warnings,
        timing: Pass2Timing { streaming_ms, copy_ms },
    })
}

/// Spawn `parallel` async worker tasks, each receiving JSON bytes over an independent channel.
/// Workers share access to `shared_sinks` via `Arc<Mutex<...>>`.
fn spawn_pass2_workers(
    parallel: usize,
    shared_sinks: &HashMap<String, Arc<Mutex<TempFileSink>>>,
    anomaly_tx: &tokio::sync::mpsc::UnboundedSender<AnomalyEvent>,
    path_map_arc: Arc<HashMap<String, TableSchema>>,
    root_schema_arc: Arc<TableSchema>,
    cancel: CancellationToken,
    worker_cfg: WorkerConfig,
) -> (Vec<tokio::sync::mpsc::Sender<Vec<u8>>>, Vec<WorkerHandle>) {
    const CHANNEL_CAP: usize = 256;
    let mut senders = Vec::with_capacity(parallel);
    let mut handles = Vec::with_capacity(parallel);
    for _ in 0..parallel {
        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(CHANNEL_CAP);
        senders.push(tx);
        let worker_sinks = shared_sinks.iter().map(|(k, v)| (k.clone(), Arc::clone(v))).collect();
        let handle = tokio::task::spawn(run_worker(
            rx, worker_sinks, anomaly_tx.clone(),
            path_map_arc.clone(), root_schema_arc.clone(), cancel.clone(),
            worker_cfg.clone(),
        ));
        handles.push(handle);
    }
    (senders, handles)
}

#[cfg(test)]
mod tests {
    use super::{Pass2Config, Pass2Timing, validate_run_params};
    use crate::db::copy_sink::TempFileSink;
    use crate::schema::table_schema::{ColumnSchema, TableSchema};
    use crate::schema::type_tracker::PgType;

    #[test]
    fn pass2_timing_total_ms_is_sum() {
        let t = Pass2Timing { streaming_ms: 3000, copy_ms: 500 };
        assert_eq!(t.total_ms(), 3500);
    }

    #[test]
    fn pass2_timing_zero() {
        let t = Pass2Timing { streaming_ms: 0, copy_ms: 0 };
        assert_eq!(t.total_ms(), 0);
    }

    #[test]
    fn run_params_parallel_zero_is_invalid() {
        assert!(matches!(
            validate_run_params(0),
            Err(crate::error::J2sError::InvalidInput(_))
        ));
    }

    #[test]
    fn run_params_parallel_one_is_valid() {
        assert!(validate_run_params(1).is_ok());
    }

    #[test]
    fn pass2_config_limit_none_means_full_import() {
        let cfg = Pass2Config {
            root_table: "root".to_string(),
            pg_schema: "public".to_string(),
            parallel: 1,
            anomaly_dir: None,
            temp_dir: None,
            per_worker_budget: None,
            min_interim_copy_bytes: None,
            limit: None,
        };
        assert!(cfg.limit.is_none());
    }

    #[test]
    fn pass2_config_limit_zero_means_ddl_only() {
        let cfg = Pass2Config {
            root_table: "root".to_string(),
            pg_schema: "public".to_string(),
            parallel: 1,
            anomaly_dir: None,
            temp_dir: None,
            per_worker_budget: None,
            min_interim_copy_bytes: None,
            limit: Some(0),
        };
        assert_eq!(cfg.limit, Some(0));
    }

    /// Validates the writer task pattern without a database:
    /// events sent via AnomalyProxy reach the AnomalyCollector in the writer task,
    /// and NDJSON files are created on disk.
    #[tokio::test]
    async fn anomaly_writer_task_creates_ndjson_files() {
        use crate::anomaly::collector::{AnomalyCollect, AnomalyCollector, AnomalyEvent, AnomalyProxy};
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let (anomaly_tx, mut anomaly_rx) = tokio::sync::mpsc::unbounded_channel::<AnomalyEvent>();

        let anomaly_dir = Some(dir.path().to_path_buf());
        let handle: tokio::task::JoinHandle<crate::error::Result<AnomalyCollector>> =
            tokio::task::spawn_blocking(move || {
                let mut collector = AnomalyCollector::new(anomaly_dir);
                while let Some(event) = anomaly_rx.blocking_recv() {
                    match event {
                        AnomalyEvent::Record {
                            table, column, row_id, expected_type, actual_value, actual_type,
                        } => {
                            collector.record(
                                &table, &column, &row_id, &expected_type, &actual_value, &actual_type,
                            )?;
                        }
                        AnomalyEvent::IncTotal { table } => {
                            collector.inc_total(&table);
                        }
                    }
                }
                Ok(collector)
            });

        // Simulate two workers sending events
        let mut proxy1 = AnomalyProxy::new(anomaly_tx.clone());
        let mut proxy2 = AnomalyProxy::new(anomaly_tx.clone());

        proxy1.inc_total("products");
        proxy1.inc_total("products");
        proxy1.record("products", "price", "r1", "double precision", "gratuit", "string").unwrap();

        proxy2.inc_total("products");
        proxy2.record("products", "price", "r2", "double precision", "N/A", "string").unwrap();

        // Drop proxies + main sender to close the channel
        drop(proxy1);
        drop(proxy2);
        drop(anomaly_tx);

        let mut collector = handle.await.unwrap().unwrap();
        collector.finish().unwrap();

        assert_eq!(collector.total_anomalies(), 2);
        assert!((collector.overall_anomaly_rate() - 2.0 / 3.0).abs() < 1e-9);

        let paths = collector.written_paths();
        assert!(paths.contains_key("products"), "NDJSON file must exist for products");
        let content = std::fs::read_to_string(&paths["products"]).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 2, "both anomaly rows must be in the file");
        for line in &lines {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(v["table"], "products");
            assert_eq!(v["column"], "price");
        }
    }

}
