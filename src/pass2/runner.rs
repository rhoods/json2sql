//! Pass 2 — data insertion: stream the JSON file a second time and write rows to `PostgreSQL`.
//!
//! Execution is split into two phases:
//! - **Phase A** (streaming): workers read the JSON in parallel and write COPY-format rows
//!   to per-table temp files. Large sinks are snapshotted to disk to bound memory usage.
//! - **Phase B** (COPY): all temp files are bulk-loaded into `PostgreSQL` via `COPY FROM STDIN`,
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

use futures_util::SinkExt;
use crate::anomaly::collector::{AnomalyCollector, AnomalyEvent, AnomalyProxy};
use crate::db::copy_sink::{copy_snapshot_to_pg, merge_copy_to_db, stream_snapshot_to_open_copy, FlushSnapshot, TempFileSink};
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
/// COPY-direct task handle: one persistent task streams all snapshots for one large table.
type CopyDirectHandle = tokio::task::JoinHandle<Result<(String, u64)>>;

/// Channel capacity for COPY-direct snapshot senders: bounds in-flight snapshot memory
/// (max COPY_DIRECT_CHANNEL_CAP × PER_WORKER_FLUSH_THRESHOLD bytes per large table).
pub(crate) const COPY_DIRECT_CHANNEL_CAP: usize = 4;

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
    pub const fn total_ms(&self) -> u64 {
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
/// Sinks below this are force-spilled to disk (RAM freed) and `COPYed` in Phase B.
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
    /// Tables with `total_observed >= large_table_threshold` bypass temp files and stream
    /// directly to PostgreSQL via a persistent COPY STDIN task. None = disabled (all tables
    /// use the temp file path).
    pub large_table_threshold: Option<u64>,
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
    /// Senders to persistent COPY-direct tasks for large tables.
    /// Workers call `try_send` instead of spilling to disk for these tables.
    copy_direct_senders: Arc<HashMap<String, tokio::sync::mpsc::Sender<FlushSnapshot>>>,
}

impl WorkerConfig {
    fn new(
        pg_url: String,
        progress_tx: Option<ProgressTx>,
        copy_sem: Arc<tokio::sync::Semaphore>,
        worker_budget: u64,
        interim_copy_threshold: u64,
        copy_direct_senders: Arc<HashMap<String, tokio::sync::mpsc::Sender<FlushSnapshot>>>,
    ) -> Self {
        Self { pg_url, progress_tx, copy_sem, worker_budget, interim_copy_threshold, copy_direct_senders }
    }
}

/// Attempt to disable synchronous commit for faster bulk-load. Non-fatal if the server
/// denies the privilege (RDS, Supabase, restricted PG) — any other error is propagated.
async fn try_set_synchronous_commit_off(conn: &Client) -> Result<()> {
    match conn.execute("SET synchronous_commit = off", &[]).await {
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

/// When the worker budget is reached: snapshot large sinks → interim COPY task,
/// spill small sinks to disk. Called from within the worker loop.
#[allow(clippy::too_many_lines)] // per-sink decision + spawn tightly coupled around sink_arc
fn trigger_budget_flush(
    sinks: &HashMap<String, Arc<Mutex<TempFileSink>>>,
    copy_handles: &mut Vec<InterimCopyHandle>,
    cfg: &WorkerConfig,
) -> Result<()> {
    for (table_name, sink_arc) in sinks {
        // Large-table path: stream snapshots directly to the persistent COPY task.
        if let Some(direct_tx) = cfg.copy_direct_senders.get(table_name) {
            let mut s = sink_arc.lock().expect("sink mutex is not poisoned");
            if s.bytes_buffered == 0 && s.row_count == 0 { continue; }
            match direct_tx.try_reserve() {
                Ok(permit) => {
                    if let Some(snap) = s.take_flush_snapshot() {
                        permit.send(snap);
                    }
                }
                Err(_) => {
                    // Channel full: backpressure — spill to disk as fallback.
                    s.force_spill()?;
                }
            }
            continue;
        }

        // Small-table path: existing interim-COPY logic.
        let snap = {
            let mut s = sink_arc.lock().expect("sink mutex is not poisoned");
            if s.bytes_buffered == 0 && s.row_count == 0 { continue; }
            if s.bytes_buffered >= cfg.interim_copy_threshold {
                s.take_flush_snapshot()
            } else {
                s.force_spill()?;
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
                try_set_synchronous_commit_off(&conn).await?;
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
    Ok(())
}

/// Process one worker's stream of JSON byte chunks: parse, insert into sinks,
fn parse_json_object(bytes: &mut [u8]) -> Result<serde_json::Map<String, Value>> {
    match simd_json::from_slice::<Value>(bytes) {
        Ok(Value::Object(o)) => Ok(o),
        Ok(other) => Err(J2sError::InvalidInput(format!(
            "Expected JSON object at root level, found: {other}"
        ))),
        Err(e) => Err(J2sError::InvalidInput(format!(
            "JSON parse error in worker: {e}"
        ))),
    }
}

#[allow(clippy::too_many_arguments)] // 8 mutable state refs from run_worker — no natural grouping
fn process_worker_item(
    mut bytes: Vec<u8>,
    sinks: &mut HashMap<String, Arc<Mutex<TempFileSink>>>,
    proxy: &mut AnomalyProxy,
    copy_handles: &mut Vec<InterimCopyHandle>,
    path_map: &HashMap<String, TableSchema>,
    root_schema: &TableSchema,
    cfg: &WorkerConfig,
    my_bytes: &mut u64,
) -> Result<()> {
    let obj_len = bytes.len() as u64;
    let obj = parse_json_object(&mut bytes)?;
    insert_object(path_map, &mut InsertCtx { sinks, anomalies: proxy }, root_schema, &obj, Uuid::now_v7(), None, None)?;
    *my_bytes += obj_len;
    if *my_bytes >= cfg.worker_budget {
        *my_bytes = 0;
        trigger_budget_flush(sinks, copy_handles, cfg)?;
    }
    Ok(())
}

/// and fire interim COPY snapshots when the per-worker budget is reached.
/// Returns background COPY handles spawned during streaming.
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
        let bytes = tokio::select! {
            () = cancel.cancelled() => break,
            msg = rx.recv() => match msg { Some(b) => b, None => break },
        };
        process_worker_item(bytes, &mut sinks, &mut proxy, &mut copy_handles, &path_map, &root_schema, &cfg, &mut my_bytes)?;
    }
    worker_teardown_flush(&sinks, &cfg).await?;
    Ok(copy_handles)
}

/// Worker teardown: flush each sink's remaining data after the streaming loop ends.
///
/// Large tables (in `cfg.copy_direct_senders`): take final snapshot and send to the
/// persistent COPY task via a guaranteed (blocking) send.
/// Small tables: spill remaining in-memory bytes to disk for Phase B.
async fn worker_teardown_flush(
    sinks: &HashMap<String, Arc<Mutex<TempFileSink>>>,
    cfg: &WorkerConfig,
) -> Result<()> {
    for (table_name, sink_arc) in sinks {
        if let Some(direct_tx) = cfg.copy_direct_senders.get(table_name) {
            let snap = {
                let mut s = sink_arc.lock().expect("sink mutex is not poisoned");
                if s.row_count == 0 { continue; }
                s.take_flush_snapshot()
            };
            if let Some(snap) = snap {
                direct_tx.send(snap).await.map_err(|_| {
                    J2sError::InvalidInput(format!(
                        "copy_direct task disconnected at teardown for {table_name}"
                    ))
                })?;
            }
        } else {
            let _ = sink_arc.lock().expect("sink mutex is not poisoned").force_spill();
        }
    }
    Ok(())
}

fn unwrap_and_sort_sinks(
    shared_sinks: HashMap<String, Arc<Mutex<TempFileSink>>>,
) -> Result<Vec<(String, TempFileSink)>> {
    let mut sinks: Vec<(String, TempFileSink)> = shared_sinks
        .into_iter()
        .map(|(name, arc)| -> Result<Option<(String, TempFileSink)>> {
            let mutex = Arc::try_unwrap(arc).map_err(|_| {
                J2sError::InvalidInput(format!(
                    "sink '{name}' still has multiple owners — possible data loss"
                ))
            })?;
            let sink = mutex.into_inner().map_err(|_| {
                J2sError::InvalidInput(format!("sink '{name}' mutex is poisoned"))
            })?;
            if sink.row_count > 0 {
                Ok(Some((name, sink)))
            } else {
                Ok(None)
            }
        })
        .collect::<Result<Vec<Option<_>>>>()?
        .into_iter()
        .flatten()
        .collect();
    sinks.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(sinks)
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
    try_set_synchronous_commit_off(&conn).await?;
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

/// Distribute sinks across `parallel` connections using greedy bin-packing.
/// Sinks are sorted by `bytes_buffered` DESC so the heaviest tables are assigned first,
/// minimising the maximum per-connection total (Phase B wall time).
/// Weight is `bytes_buffered` — bytes since the last Phase A snapshot, which reflects
/// the actual COPY data volume for Phase B regardless of row width.
fn distribute_sinks(
    mut sinks: Vec<(String, TempFileSink)>,
    parallel: usize,
) -> Vec<Vec<(String, Vec<TempFileSink>)>> {
    let mut batches: Vec<Vec<(String, Vec<TempFileSink>)>> =
        (0..parallel).map(|_| Vec::new()).collect();
    sinks.sort_by(|a, b| b.1.bytes_buffered.cmp(&a.1.bytes_buffered));
    let mut loads = vec![0u64; parallel];
    for (name, sink) in sinks {
        let min_idx = loads.iter().enumerate().min_by_key(|&(_, &v)| v).map(|(i, _)| i).expect("parallel >= 1 enforced by validate_run_params");
        loads[min_idx] += sink.bytes_buffered;
        batches[min_idx].push((name, vec![sink]));
    }
    batches
}

async fn phase_copy(
    shared_sinks: HashMap<String, Arc<Mutex<TempFileSink>>>,
    parallel: usize,
    pg_url: &str,
    progress_tx: Option<&ProgressTx>,
    interim_rows: HashMap<String, u64>,
) -> Result<HashMap<String, u64>> {
    let all_sinks = unwrap_and_sort_sinks(shared_sinks)?;
    let table_batches = distribute_sinks(all_sinks, parallel);

    let mut copy_handles: Vec<CopyHandle> = Vec::with_capacity(parallel);
    for batch in table_batches {
        if batch.is_empty() { continue; }
        copy_handles.push(tokio::task::spawn(copy_batch(batch, pg_url.to_string(), progress_tx.cloned())));
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
    progress_tx: Option<&ProgressTx>,
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
        if let Some(tx) = progress_tx {
            let _ = tx.send(ProgressEvent::Pass2Log(msg));
        }
    }
}

/// Send the final progress update and finish the progress bar after the dispatch loop.
fn finalize_dispatch(
    progress_tx: Option<&ProgressTx>,
    progress: Option<&ProgressTracker>,
    rows_processed: u64,
    bytes_read: u64,
    total_bytes: u64,
) {
    if let Some(tx) = progress_tx {
        if rows_processed > 0 && !rows_processed.is_multiple_of(PROGRESS_INTERVAL) {
            let _ = tx.send(ProgressEvent::Pass2Progress { rows_processed, bytes_read, total_bytes });
        }
    }
    if let Some(bar) = progress { bar.finish(); }
}

/// Emit per-table anomaly updates and the final `Pass2Done` event.
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
fn log_constraint_warnings(warnings: &[crate::db::ddl::ConstraintWarning], progress_tx: Option<&ProgressTx>) {
    if warnings.is_empty() { return; }
    eprintln!("WARNING: {} FK constraint(s) could not be applied after import:", warnings.len());
    for w in warnings {
        let msg = format!("FK warning — {} : {}", w.table, w.message);
        eprintln!("  {msg}");
        if let Some(tx) = progress_tx {
            let _ = tx.send(ProgressEvent::Pass2Log(msg));
        }
    }
}

fn update_row_progress(
    progress: Option<&ProgressTracker>,
    progress_tx: Option<&ProgressTx>,
    rows_processed: u64,
    bytes_read: u64,
    total_bytes: u64,
) {
    if let Some(bar) = progress { bar.inc_rows(1); }
    if rows_processed.is_multiple_of(PROGRESS_INTERVAL) {
        if let Some(tx) = progress_tx {
            let _ = tx.send(ProgressEvent::Pass2Progress { rows_processed, bytes_read, total_bytes });
        }
    }
}

/// Dispatch raw JSON bytes from `reader` round-robin to workers.
/// Returns `(rows_processed, worker_died)`.
async fn dispatch_loop(
    reader: &mut JsonReader,
    senders: &[tokio::sync::mpsc::Sender<Vec<u8>>],
    progress_tx: Option<&ProgressTx>,
    progress: Option<&ProgressTracker>,
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
            if limit.is_some_and(|n| rows_processed >= n) { break 'dispatch; }
            robin = (robin + 1) % parallel;
            update_row_progress(progress, progress_tx, rows_processed, reader.bytes_read(), total_bytes);
        }
    }
    Ok((rows_processed, worker_died))
}

/// Join Phase A workers, await the anomaly writer, and collect interim COPY results.
/// Returns `(merged_anomalies, interim_rows)`.
///
/// `copy_direct_handles` are awaited AFTER workers finish (workers drop their senders,
/// which causes the COPY-direct tasks to finish their COPY sessions).
#[allow(clippy::too_many_lines)] // sequential join pipeline, error accumulation pattern non-factorisable
async fn join_phase_a(
    worker_handles: Vec<WorkerHandle>,
    copy_direct_handles: Vec<CopyDirectHandle>,
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
    // Workers have dropped their senders → COPY-direct tasks see channel closed and finish.
    for handle in copy_direct_handles {
        match handle.await {
            Ok(Ok((table_name, rows))) => { *interim_rows.entry(table_name).or_insert(0) += rows; }
            Ok(Err(e)) => { if first_error.is_none() { first_error = Some(e); } }
            Err(e) => { if first_error.is_none() {
                first_error = Some(J2sError::InvalidInput(format!("copy_direct task panic: {e}")));
            }}
        }
    }
    if let Some(err) = first_error { return Err(err); }
    if worker_died {
        return Err(J2sError::InvalidInput("worker channel closed unexpectedly".into()));
    }
    Ok((merged_anomalies, interim_rows))
}

/// Run Pass 2: stream the file into per-worker temp-file buffers, COPY to
/// `PostgreSQL`, then add PRIMARY KEY and FOREIGN KEY constraints.
///
/// **The caller is responsible for creating tables** (without constraints)
/// via `db::ddl::create_tables_no_constraints()` before calling this function.
///
/// Returns the set of table names whose `row_count` (from Pass 1) is >= `threshold`.
/// Returns an empty set when `threshold` is `None`, disabling the COPY-direct path.
fn classify_tables(schemas: &[TableSchema], threshold: Option<u64>) -> std::collections::HashSet<String> {
    let Some(t) = threshold else { return std::collections::HashSet::new(); };
    schemas.iter().filter(|s| s.row_count >= t).map(|s| s.name.clone()).collect()
}

/// Inner loop for a COPY-direct task: receives `FlushSnapshot`s on `rx` and streams them
/// into a single open COPY STDIN session for `table_name`. Returns `(table_name, total_rows)`.
///
/// The task owns its own PG connection so it never contends with the interim-copy semaphore.
/// When the sender side is dropped, `rx.recv()` returns `None` and the COPY is finished.
async fn run_copy_direct_task(
    table_name: String,
    copy_sql: String,
    pg_url: String,
    mut rx: tokio::sync::mpsc::Receiver<FlushSnapshot>,
    _copy_sem: Arc<tokio::sync::Semaphore>,
    progress_tx: Option<ProgressTx>,
) -> Result<(String, u64)> {
    let (client, connection) = tokio_postgres::connect(&pg_url, tokio_postgres::NoTls)
        .await
        .map_err(J2sError::Db)?;
    tokio::spawn(async move { drop(connection.await) });

    let sink = client
        .copy_in::<_, bytes::Bytes>(&copy_sql)
        .await
        .map_err(J2sError::Db)?;
    let mut pinned = Box::pin(sink);

    let mut total_rows: u64 = 0;
    while let Some(snap) = rx.recv().await {
        let rows = snap.row_count;
        stream_snapshot_to_open_copy(snap, &mut pinned).await?;
        total_rows += rows;
        if let Some(ref tx) = progress_tx {
            let _ = tx.send(ProgressEvent::Pass2Flush {
                table_name: table_name.clone(),
                rows_flushed: rows,
            });
        }
    }

    pinned.close().await.map_err(J2sError::Db)?;

    Ok((table_name, total_rows))
}

/// Spawns a persistent COPY-direct task for `table_name`.
///
/// Returns the sender (workers use it to forward `FlushSnapshot`s) and the task handle.
/// The COPY session ends when the last sender is dropped.
fn spawn_copy_direct_task(
    table_name: String,
    copy_sql: String,
    pg_url: String,
    copy_sem: Arc<tokio::sync::Semaphore>,
    progress_tx: Option<ProgressTx>,
) -> (tokio::sync::mpsc::Sender<FlushSnapshot>, CopyDirectHandle) {
    let (tx, rx) = tokio::sync::mpsc::channel(COPY_DIRECT_CHANNEL_CAP);
    let handle = tokio::spawn(run_copy_direct_task(
        table_name,
        copy_sql,
        pg_url,
        rx,
        copy_sem,
        progress_tx,
    ));
    (tx, handle)
}

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
) -> HashMap<String, Arc<Mutex<TempFileSink>>> {
    schemas
        .iter()
        .map(|s| (
            s.name.clone(),
            Arc::new(Mutex::new(TempFileSink::new(s, pg_schema, temp_dir))),
        ))
        .collect()
}

/// Internal phases:
///   B — N workers (parallel ≥ 1) stream root objects round-robin into
///       per-table `TempFileSink` buffers. A dedicated flush task runs
///       concurrently, `COPYing` sinks to PG (up to `parallel` simultaneous
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
    let large_table_set = classify_tables(schemas, config.large_table_threshold);

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
    preflight_warn_nonempty(schemas, client, &config.pg_schema, progress_tx.as_ref()).await;

    let cancel = CancellationToken::new();
    let _cancel_guard = cancel.clone().drop_guard();

    // Phase A — Parallel streaming: shared sinks, N workers, round-robin dispatch.
    let (mut reader, _format) = JsonReader::open(path)?;
    let path_map_arc: Arc<HashMap<String, TableSchema>> = Arc::new(path_map);
    let root_schema_arc: Arc<TableSchema> = Arc::new(root_schema.clone());
    let copy_sem: Arc<tokio::sync::Semaphore> = Arc::new(tokio::sync::Semaphore::new(parallel));
    let shared_sinks = build_shared_sinks(schemas, &config.pg_schema, config.temp_dir.as_deref());

    // Spawn persistent COPY-direct tasks for large tables; collect their senders for workers.
    let mut copy_direct_handles: Vec<CopyDirectHandle> = Vec::new();
    let mut direct_senders_map: HashMap<String, tokio::sync::mpsc::Sender<FlushSnapshot>> = HashMap::new();
    for table_name in &large_table_set {
        if let Some(sink_arc) = shared_sinks.get(table_name) {
            let copy_sql = sink_arc.lock().expect("sink mutex is not poisoned").copy_sql().to_string();
            let (tx, handle) = spawn_copy_direct_task(
                table_name.clone(),
                copy_sql,
                pg_url.to_string(),
                copy_sem.clone(),
                progress_tx.clone(),
            );
            direct_senders_map.insert(table_name.clone(), tx);
            copy_direct_handles.push(handle);
        }
    }

    let worker_cfg = WorkerConfig::new(
        pg_url.to_string(),
        progress_tx.clone(),
        copy_sem.clone(),
        worker_budget,
        interim_copy_threshold,
        Arc::new(direct_senders_map),
    );
    let (senders, worker_handles) = spawn_pass2_workers(
        parallel, &shared_sinks, &anomaly_tx, &path_map_arc, &root_schema_arc, &cancel, &worker_cfg,
    );
    // Drop run()'s ref to copy_direct senders: only workers hold refs now.
    // When workers finish (in join_phase_a), all senders drop → COPY-direct tasks complete.
    drop(worker_cfg);

    let stream_start = Instant::now();
    let (rows_processed, worker_died) = dispatch_loop(&mut reader, &senders, progress_tx.as_ref(), progress.as_ref(), config.limit, total_bytes).await?;
    drop(senders);

    finalize_dispatch(progress_tx.as_ref(), progress.as_ref(), rows_processed, reader.bytes_read(), total_bytes);
    #[allow(clippy::cast_possible_truncation)]
    let streaming_ms = stream_start.elapsed().as_millis() as u64;
    eprintln!("Pass 2 streaming done ({parallel} workers). Flushing remaining rows to PostgreSQL...");

    let (merged_anomalies, interim_rows) =
        join_phase_a(worker_handles, copy_direct_handles, anomaly_tx, anomaly_writer_handle, worker_died).await?;

    // Phase B — COPY remaining data to PG.
    let copy_start = Instant::now();
    let rows_per_table = phase_copy(shared_sinks, parallel, pg_url, progress_tx.as_ref(), interim_rows).await?;

    // Phase D — Constraints: PK (fatal), FK (warnings).
    let constraint_warnings = add_constraints(pg_url, schemas, &config.pg_schema, parallel, progress_tx.as_ref()).await?;
    log_constraint_warnings(&constraint_warnings, progress_tx.as_ref());

    build_pass2_result(merged_anomalies, rows_per_table, constraint_warnings, progress_tx.as_ref(), copy_start, streaming_ms)
}

fn build_pass2_result(
    mut merged_anomalies: AnomalyCollector,
    rows_per_table: HashMap<String, u64>,
    constraint_warnings: Vec<ConstraintWarning>,
    progress_tx: Option<&ProgressTx>,
    copy_start: Instant,
    streaming_ms: u64,
) -> Result<Pass2Result> {
    if let Some(tx) = progress_tx {
        emit_completion_events(tx, &merged_anomalies, &rows_per_table, &constraint_warnings);
    }
    merged_anomalies.finish()?;
    #[allow(clippy::cast_possible_truncation)]
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
    path_map_arc: &Arc<HashMap<String, TableSchema>>,
    root_schema_arc: &Arc<TableSchema>,
    cancel: &CancellationToken,
    worker_cfg: &WorkerConfig,
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
    use std::collections::HashMap;
    use super::{classify_tables, spawn_copy_direct_task, Pass2Config, Pass2Timing, validate_run_params};
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
            large_table_threshold: None,
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
            large_table_threshold: None,
        };
        assert_eq!(cfg.limit, Some(0));
    }

    #[test]
    fn pass2_config_large_table_threshold_none_disables_direct() {
        let cfg = Pass2Config {
            root_table: "root".to_string(),
            pg_schema: "public".to_string(),
            parallel: 1,
            anomaly_dir: None,
            temp_dir: None,
            per_worker_budget: None,
            min_interim_copy_bytes: None,
            limit: None,
            large_table_threshold: None,
        };
        assert!(cfg.large_table_threshold.is_none());
    }

    #[test]
    fn pass2_config_large_table_threshold_some_is_preserved() {
        let cfg = Pass2Config {
            root_table: "root".to_string(),
            pg_schema: "public".to_string(),
            parallel: 1,
            anomaly_dir: None,
            temp_dir: None,
            per_worker_budget: None,
            min_interim_copy_bytes: None,
            limit: None,
            large_table_threshold: Some(500_000),
        };
        assert_eq!(cfg.large_table_threshold, Some(500_000));
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

    #[test]
    fn test_update_row_progress_sends_at_interval() {
        use crate::io::progress_event::ProgressEvent;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();
        // rows_processed == PROGRESS_INTERVAL → must send
        super::update_row_progress(None, Some(&tx), super::PROGRESS_INTERVAL, 0, 0);
        assert!(rx.try_recv().is_ok(), "must send at interval boundary");
    }

    #[test]
    fn test_update_row_progress_silent_between_intervals() {
        use crate::io::progress_event::ProgressEvent;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<ProgressEvent>();
        // rows_processed == 1 → not a multiple of PROGRESS_INTERVAL (which is > 1)
        super::update_row_progress(None, Some(&tx), 1, 0, 0);
        assert!(rx.try_recv().is_err(), "must not send between interval boundaries");
    }

    #[test]
    fn test_parse_json_object_valid() {
        let mut bytes = b"{\"k\":1}".to_vec();
        let result = super::parse_json_object(&mut bytes);
        assert!(result.is_ok());
        assert!(result.unwrap().contains_key("k"));
    }

    #[test]
    fn test_parse_json_object_scalar_is_error() {
        let mut bytes = b"42".to_vec();
        let result = super::parse_json_object(&mut bytes);
        assert!(result.is_err(), "scalar at root level must be an error");
    }

    #[test]
    fn test_parse_json_object_invalid_json_is_error() {
        let mut bytes = b"{broken".to_vec();
        let result = super::parse_json_object(&mut bytes);
        assert!(result.is_err(), "invalid JSON must be an error");
    }

    #[tokio::test]
    async fn join_phase_a_awaits_copy_handles_before_returning_worker_error() {
        use crate::anomaly::collector::{AnomalyCollector, AnomalyEvent};
        use crate::error::J2sError;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let flag = Arc::new(AtomicBool::new(false));
        let flag2 = Arc::clone(&flag);

        // Copy handle: sleeps 50 ms then sets the flag — proves it was awaited.
        let copy_handle: super::InterimCopyHandle = tokio::task::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            flag2.store(true, Ordering::SeqCst);
            Ok::<(String, u64), J2sError>(("t".to_string(), 0u64))
        });

        // Worker 1: succeeds and hands over the copy handle.
        let worker1: super::WorkerHandle = tokio::task::spawn(async move {
            Ok::<Vec<super::InterimCopyHandle>, J2sError>(vec![copy_handle])
        });

        // Worker 2: returns an error → sets first_error in join_phase_a.
        let worker2: super::WorkerHandle = tokio::task::spawn(async {
            Err::<Vec<super::InterimCopyHandle>, J2sError>(
                J2sError::InvalidInput("worker failed".into()),
            )
        });

        let (anomaly_tx, anomaly_rx) =
            tokio::sync::mpsc::unbounded_channel::<AnomalyEvent>();
        let anomaly_writer = tokio::task::spawn_blocking(move || {
            let mut rx = anomaly_rx;
            let collector = AnomalyCollector::new(None);
            while rx.blocking_recv().is_some() {}
            Ok::<AnomalyCollector, J2sError>(collector)
        });

        let result = super::join_phase_a(
            vec![worker1, worker2],
            vec![], // no copy_direct_handles for this test
            anomaly_tx,
            anomaly_writer,
            false,
        )
        .await;

        assert!(result.is_err(), "must propagate worker error");
        assert!(
            flag.load(Ordering::SeqCst),
            "copy handle must be awaited to completion before returning Err"
        );
    }

    #[tokio::test]
    async fn join_phase_a_awaits_copy_direct_handles_and_collects_rows() {
        use crate::anomaly::collector::{AnomalyCollector, AnomalyEvent};
        use crate::error::J2sError;

        let worker: super::WorkerHandle = tokio::task::spawn(async {
            Ok::<Vec<super::InterimCopyHandle>, J2sError>(vec![])
        });
        let copy_direct_handle: super::CopyDirectHandle = tokio::task::spawn(async {
            Ok::<(String, u64), J2sError>(("large_table".to_string(), 42u64))
        });

        let (anomaly_tx, anomaly_rx) = tokio::sync::mpsc::unbounded_channel::<AnomalyEvent>();
        let anomaly_writer = tokio::task::spawn_blocking(move || {
            let mut rx = anomaly_rx;
            let collector = AnomalyCollector::new(None);
            while rx.blocking_recv().is_some() {}
            Ok::<AnomalyCollector, J2sError>(collector)
        });

        let result = super::join_phase_a(
            vec![worker],
            vec![copy_direct_handle],
            anomaly_tx,
            anomaly_writer,
            false,
        ).await.unwrap();

        let (_anomalies, interim_rows) = result;
        assert_eq!(
            interim_rows.get("large_table").copied().unwrap_or(0),
            42,
            "rows from CopyDirect handle must be collected in interim_rows"
        );
    }

    #[test]
    fn unwrap_and_sort_sinks_shared_arc_is_error() {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        let schema = crate::schema::table_schema::TableSchema::new(
            "t".to_string(), vec!["t".to_string()], 0,
        );
        let sink = TempFileSink::new(&schema, "public", None);
        let arc = Arc::new(Mutex::new(sink));
        let _clone = Arc::clone(&arc); // strong count = 2 → must fail
        let mut map: HashMap<String, Arc<Mutex<TempFileSink>>> = HashMap::new();
        map.insert("t".to_string(), arc);
        assert!(super::unwrap_and_sort_sinks(map).is_err());
    }

    #[test]
    fn unwrap_and_sort_sinks_single_owner_succeeds() {
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        let mut schema = crate::schema::table_schema::TableSchema::new(
            "t".to_string(), vec!["t".to_string()], 0,
        );
        schema.columns.push(ColumnSchema {
            name: "col".to_string(),
            original_name: "col".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        let mut sink = TempFileSink::new(&schema, "public", None);
        sink.row_count = 1;
        let arc = Arc::new(Mutex::new(sink));
        let mut map: HashMap<String, Arc<Mutex<TempFileSink>>> = HashMap::new();
        map.insert("t".to_string(), arc);
        let result = super::unwrap_and_sort_sinks(map);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn unwrap_and_sort_sinks_excludes_interim_only_sinks() {
        // Sinks with total_flushed > 0 but row_count == 0 are fully handled by Phase A.
        // They must not consume a Phase B connection slot.
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        let make = |name: &str, row_count: u64, total_flushed: u64| {
            let schema = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
            let mut sink = TempFileSink::new(&schema, "public", None);
            sink.row_count = row_count;
            sink.total_flushed = total_flushed;
            (name.to_string(), Arc::new(Mutex::new(sink)))
        };
        let mut map: HashMap<String, Arc<Mutex<TempFileSink>>> = HashMap::new();
        let (name_a, arc_a) = make("phase_b_table", 5, 0);   // has Phase B rows → keep
        let (name_b, arc_b) = make("interim_only", 0, 10);   // Phase A only → exclude
        let (name_c, arc_c) = make("empty", 0, 0);            // never wrote → exclude
        map.insert(name_a, arc_a);
        map.insert(name_b, arc_b);
        map.insert(name_c, arc_c);
        let result = super::unwrap_and_sort_sinks(map).unwrap();
        assert_eq!(result.len(), 1, "only the Phase B sink should be retained");
        assert_eq!(result[0].0, "phase_b_table");
    }

    fn make_sink_with_bytes(name: &str, bytes_buffered: u64) -> TempFileSink {
        let schema = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        let mut sink = TempFileSink::new(&schema, "public", None);
        sink.bytes_buffered = bytes_buffered;
        sink.row_count = 1;
        sink
    }

    #[test]
    fn distribute_sinks_single_connection_all_to_conn0() {
        let sinks = vec![
            ("a".to_string(), make_sink_with_bytes("a", 10)),
            ("b".to_string(), make_sink_with_bytes("b", 5)),
            ("c".to_string(), make_sink_with_bytes("c", 3)),
        ];
        let batches = super::distribute_sinks(sinks, 1);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 3, "all tables assigned to the single connection");
    }

    #[test]
    fn distribute_sinks_greedy_isolates_heaviest_table() {
        // 2 connections, bytes=[50, 25, 10, 5]
        // greedy: conn0=[50], conn1=[25,10,5] → loads 50 vs 40
        let sinks = vec![
            ("big".to_string(), make_sink_with_bytes("big", 50)),
            ("med".to_string(), make_sink_with_bytes("med", 25)),
            ("sm1".to_string(), make_sink_with_bytes("sm1", 10)),
            ("sm2".to_string(), make_sink_with_bytes("sm2", 5)),
        ];
        let batches = super::distribute_sinks(sinks, 2);
        assert_eq!(batches.len(), 2);
        let total: u64 = batches.iter().flat_map(|b| b.iter()).map(|(_, sinks)| sinks[0].bytes_buffered).sum();
        assert_eq!(total, 90, "all bytes accounted for");
        let isolated = batches.iter().any(|b| b.len() == 1 && b[0].1[0].bytes_buffered == 50);
        assert!(isolated, "50-byte table should be isolated on its own connection");
    }

    #[test]
    fn distribute_sinks_empty_returns_empty_batches() {
        let batches = super::distribute_sinks(vec![], 3);
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|b| b.is_empty()), "all batches empty when no sinks");
    }

    #[test]
    fn distribute_sinks_uses_bytes_buffered_not_row_count_as_weight() {
        // bytes_buffered is the Phase B load signal; row_count and total_flushed must not influence distribution
        let mut sink_heavy = make_sink_with_bytes("heavy", 1000);
        sink_heavy.total_flushed = 5; // Phase A history — irrelevant for Phase B distribution
        let mut sink_light = make_sink_with_bytes("light", 10);
        sink_light.row_count = 500; // many rows but small bytes — must be treated as light
        let sinks = vec![
            ("heavy".to_string(), sink_heavy),
            ("light".to_string(), sink_light),
        ];
        let batches = super::distribute_sinks(sinks, 2);
        // "heavy" (bytes_buffered=1000) must be on conn0 (heaviest first)
        assert_eq!(batches[0][0].0, "heavy", "largest bytes_buffered assigned first");
        assert_eq!(batches[1][0].0, "light");
    }

    // -------------------------------------------------------------------------
    // spawn_copy_direct_task tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn copy_direct_task_propagates_pg_connection_error() {
        use std::sync::Arc;
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let (tx, handle) = spawn_copy_direct_task(
            "t".to_string(),
            "COPY public.t FROM STDIN".to_string(),
            "postgres://invalid-host-xyz:9999/db".to_string(),
            sem,
            None,
        );
        drop(tx);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            handle,
        ).await.expect("task must complete within timeout")
         .expect("task must not panic");
        assert!(result.is_err(), "must fail when PG is unreachable");
    }

    #[tokio::test]
    async fn copy_direct_task_sender_is_bounded() {
        use std::sync::Arc;
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let (tx, _handle) = spawn_copy_direct_task(
            "t".to_string(),
            "COPY public.t FROM STDIN".to_string(),
            "postgres://invalid:9999/db".to_string(),
            sem,
            None,
        );
        assert_eq!(tx.max_capacity(), super::COPY_DIRECT_CHANNEL_CAP);
    }

    // -------------------------------------------------------------------------
    // classify_tables tests
    // -------------------------------------------------------------------------

    fn make_schema_with_rows(name: &str, row_count: u64) -> crate::schema::table_schema::TableSchema {
        let mut s = crate::schema::table_schema::TableSchema::new(
            name.to_string(), vec![name.to_string()], 0,
        );
        s.row_count = row_count;
        s
    }

    #[test]
    fn classify_tables_none_threshold_returns_empty() {
        let schemas = vec![
            make_schema_with_rows("big", 1_000_000),
            make_schema_with_rows("small", 100),
        ];
        let result = classify_tables(&schemas, None);
        assert!(result.is_empty(), "None threshold must classify no tables as large");
    }

    #[test]
    fn classify_tables_threshold_zero_classifies_all() {
        let schemas = vec![
            make_schema_with_rows("a", 0),
            make_schema_with_rows("b", 1),
        ];
        let result = classify_tables(&schemas, Some(0));
        assert_eq!(result.len(), 2, "threshold=0 must classify all tables");
        assert!(result.contains("a"));
        assert!(result.contains("b"));
    }

    #[test]
    fn classify_tables_filters_by_row_count() {
        let schemas = vec![
            make_schema_with_rows("big", 500_000),
            make_schema_with_rows("medium", 499_999),
            make_schema_with_rows("small", 1_000),
        ];
        let result = classify_tables(&schemas, Some(500_000));
        assert_eq!(result.len(), 1);
        assert!(result.contains("big"), "only 'big' meets the threshold");
        assert!(!result.contains("medium"));
    }

    #[test]
    fn classify_tables_max_threshold_returns_empty() {
        let schemas = vec![make_schema_with_rows("huge", u64::MAX)];
        // Only u64::MAX itself would match u64::MAX threshold
        let result = classify_tables(&schemas, Some(u64::MAX));
        assert_eq!(result.len(), 1);
        assert!(result.contains("huge"));
    }

    #[test]
    fn classify_tables_empty_schemas_returns_empty() {
        let result = classify_tables(&[], Some(100));
        assert!(result.is_empty());
    }

    // -------------------------------------------------------------------------
    // trigger_budget_flush tests — large-table direct path
    // -------------------------------------------------------------------------

    fn make_worker_cfg_with_direct_sender(
        table: &str,
        direct_tx: tokio::sync::mpsc::Sender<crate::db::copy_sink::FlushSnapshot>,
    ) -> super::WorkerConfig {
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let mut senders = HashMap::new();
        senders.insert(table.to_string(), direct_tx);
        super::WorkerConfig::new(
            "postgres://x".to_string(),
            None,
            sem,
            1_000_000,
            512 * 1024,
            std::sync::Arc::new(senders),
        )
    }

    fn make_sink_arc_with_row(table: &str) -> std::sync::Arc<std::sync::Mutex<crate::db::copy_sink::TempFileSink>> {
        let schema = TableSchema::new(table.to_string(), vec![table.to_string()], 0);
        let mut sink = crate::db::copy_sink::TempFileSink::new(&schema, "public", None);
        sink.write_row(b"1\t2\n").unwrap();
        std::sync::Arc::new(std::sync::Mutex::new(sink))
    }

    #[test]
    fn trigger_budget_flush_large_table_sends_snapshot_when_channel_has_room() {
        let table = "orders";
        let (direct_tx, mut direct_rx) = tokio::sync::mpsc::channel(4);
        let cfg = make_worker_cfg_with_direct_sender(table, direct_tx);
        let sink_arc = make_sink_arc_with_row(table);
        let sinks = [(table.to_string(), std::sync::Arc::clone(&sink_arc))].into_iter().collect();
        let mut copy_handles = Vec::new();

        super::trigger_budget_flush(&sinks, &mut copy_handles, &cfg).unwrap();

        assert!(direct_rx.try_recv().is_ok(), "snapshot must be sent to direct channel");
        assert_eq!(sink_arc.lock().unwrap().row_count, 0, "sink must be reset after snapshot");
        assert!(copy_handles.is_empty(), "no interim copy handle for large table");
    }

    #[test]
    fn trigger_budget_flush_large_table_spills_to_disk_when_channel_full() {
        let table = "orders";
        let (direct_tx, _direct_rx) = tokio::sync::mpsc::channel(1);
        // Fill the channel so it's full
        let schema = TableSchema::new(table.to_string(), vec![table.to_string()], 0);
        let dummy_sink = crate::db::copy_sink::TempFileSink::new(&schema, "public", None);
        let dummy_snap = {
            let mut s2 = dummy_sink;
            s2.write_row(b"x\n").unwrap();
            s2.take_flush_snapshot().unwrap()
        };
        direct_tx.try_send(dummy_snap).unwrap(); // fills the 1-capacity channel
        let cfg = make_worker_cfg_with_direct_sender(table, direct_tx);

        let sink_arc = make_sink_arc_with_row(table);
        let sinks = [(table.to_string(), std::sync::Arc::clone(&sink_arc))].into_iter().collect();
        let mut copy_handles = Vec::new();

        super::trigger_budget_flush(&sinks, &mut copy_handles, &cfg).unwrap();

        // Sink should have been spilled to disk (row_count unchanged — force_spill keeps row_count)
        assert!(sink_arc.lock().unwrap().row_count > 0, "sink row_count preserved after disk spill");
        assert!(copy_handles.is_empty(), "no interim copy handle for large table");
    }

    // -------------------------------------------------------------------------
    // run_worker teardown tests — large-table final snapshot
    // -------------------------------------------------------------------------

    /// Build a minimal WorkerConfig suitable for teardown tests.
    fn make_teardown_worker_cfg(
        table: &str,
        direct_tx: tokio::sync::mpsc::Sender<crate::db::copy_sink::FlushSnapshot>,
    ) -> super::WorkerConfig {
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let mut senders = HashMap::new();
        senders.insert(table.to_string(), direct_tx);
        super::WorkerConfig::new(
            "postgres://x".to_string(),
            None,
            sem,
            u64::MAX, // never trigger budget flush during test
            u64::MAX,
            std::sync::Arc::new(senders),
        )
    }

    #[tokio::test]
    async fn run_worker_teardown_sends_final_snapshot_for_large_table() {
        let table = "big_orders";
        let (direct_tx, mut direct_rx) = tokio::sync::mpsc::channel(4);
        let cfg = make_teardown_worker_cfg(table, direct_tx);

        let sink_arc = make_sink_arc_with_row(table);
        let sinks = [(table.to_string(), std::sync::Arc::clone(&sink_arc))].into_iter().collect();
        super::worker_teardown_flush(&sinks, &cfg).await.unwrap();

        assert!(direct_rx.try_recv().is_ok(), "final snapshot sent to direct channel at teardown");
        assert_eq!(sink_arc.lock().unwrap().row_count, 0, "sink reset after teardown snapshot");
    }

    #[test]
    fn run_worker_teardown_spills_to_disk_for_small_table() {
        let table = "small_orders";
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let cfg = super::WorkerConfig::new(
            "postgres://x".to_string(),
            None,
            sem,
            u64::MAX,
            u64::MAX,
            std::sync::Arc::new(HashMap::new()), // no large-table senders
        );

        let sink_arc = make_sink_arc_with_row(table);
        let sinks = [(table.to_string(), std::sync::Arc::clone(&sink_arc))].into_iter().collect();
        // For small tables, teardown is sync (force_spill only) — test can be sync via blocking call
        // Use try_reserve path: no channel, so force_spill is called synchronously.
        // Since worker_teardown_flush is async, use tokio::runtime::Handle::current in a sync context
        // OR convert this test to tokio::test as well:
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(super::worker_teardown_flush(&sinks, &cfg))
            .unwrap();

        assert!(sink_arc.lock().unwrap().row_count > 0, "small table row_count preserved after spill");
    }

    #[tokio::test]
    async fn trigger_budget_flush_small_table_uses_interim_copy_when_above_threshold() {
        let table = "small";
        // No entry in copy_direct_senders for this table
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
        let cfg = super::WorkerConfig::new(
            "postgres://x".to_string(),
            None,
            sem,
            1_000_000,
            1, // very low threshold so any data triggers snapshot
            std::sync::Arc::new(HashMap::new()),
        );
        let sink_arc = make_sink_arc_with_row(table);
        let sinks = [(table.to_string(), std::sync::Arc::clone(&sink_arc))].into_iter().collect();
        let mut copy_handles: Vec<super::InterimCopyHandle> = Vec::new();

        super::trigger_budget_flush(&sinks, &mut copy_handles, &cfg).unwrap();

        assert!(!copy_handles.is_empty(), "interim copy handle spawned for small table above threshold");
    }

}
