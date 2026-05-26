use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
use simd_json;
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::anomaly::collector::AnomalyCollector;
use crate::db::copy_sink::{merge_copy_to_db, TempFileSink, INTERIM_FLUSH_THRESHOLD, MAX_OPEN_TEMP_FILES};
use crate::schema::PATH_SEP;
use crate::db::ddl::{add_constraints, ConstraintWarning};
use crate::error::{J2sError, Result};
use crate::io::mem::ram_pressure_exceeded;
use crate::io::progress::ProgressTracker;
use crate::io::progress_event::{ProgressEvent, ProgressTx};
use crate::io::reader::{file_size, JsonReader};
use crate::pass2::insert::insert_object;
use crate::schema::table_schema::TableSchema;

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

/// Fraction of `MAX_OPEN_TEMP_FILES` that all workers combined may hold open
/// simultaneously. Conservative to leave headroom for PG connection, reader,
/// and process descriptors.
const FD_GLOBAL_THRESHOLD: usize = MAX_OPEN_TEMP_FILES * 9 / 10; // 855 with default 950

/// Minimum bytes physically on disk in a worker sink before it is handed off to
/// the flush task during a drain cycle. Keyed on `bytes_on_disk` (not
/// `bytes_buffered`) so a table that only accumulates a small amount per drain
/// cycle still gets handed off once its temp file reaches this size, bounding
/// per-table disk usage across the full streaming run.
/// Sinks below this threshold are force_spill'd in-place; they accumulate
/// until they cross the threshold on a future drain cycle.
/// Peak worker disk ≈ parallel × N_tables × MIN_SINK_HANDOFF_BYTES.
const MIN_SINK_HANDOFF_BYTES: u64 = 1024 * 1024; // 1 MiB

/// Returns true if a sink should be handed off to the flush task during a drain cycle.
/// Keyed on bytes_on_disk (not bytes_buffered) — see MIN_SINK_HANDOFF_BYTES.
fn sink_eligible_for_handoff(sink: &crate::db::copy_sink::TempFileSink) -> bool {
    sink.bytes_on_disk >= MIN_SINK_HANDOFF_BYTES
}

fn validate_run_params(parallel: usize) -> Result<()> {
    if parallel == 0 {
        return Err(J2sError::InvalidInput(
            "parallel must be >= 1 (0 would produce an empty connection pool)".to_string(),
        ));
    }
    Ok(())
}

/// Saturating subtraction on an `AtomicUsize`.
///
/// Needed because `fetch_sub` wraps on underflow (undefined in usize arithmetic).
fn global_sub(global: &AtomicUsize, n: usize) {
    let mut cur = global.load(Ordering::Relaxed);
    loop {
        let next = cur.saturating_sub(n);
        match global.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(actual) => cur = actual,
        }
    }
}

/// Default RAM usage percentage above which workers force-spill all sinks in-place.
pub const DEFAULT_RAM_PRESSURE_PCT: u8 = 70;

/// Run Pass 2: stream the file into per-worker temp-file buffers, COPY to
/// PostgreSQL, then add PRIMARY KEY and FOREIGN KEY constraints.
///
/// **The caller is responsible for creating tables** (without constraints)
/// via `db::ddl::create_tables_no_constraints()` before calling this function.
///
/// Internal phases:
///   B — N workers (parallel ≥ 1) stream root objects round-robin into
///       per-table `TempFileSink` buffers. A dedicated flush task runs
///       concurrently, COPYing sinks to PG (up to `parallel` simultaneous
///       connections) as they fill up and when workers finish.
///   D — `add_constraints()` adds PRIMARY KEY (fatal on error) then
///       FOREIGN KEY (failures become `constraint_warnings`).

pub async fn run(
    path: &Path,
    root_table: &str,
    schemas: &[TableSchema],
    client: &Client,
    pg_url: &str,
    pg_schema: &str,
    parallel: usize,
    anomaly_dir: Option<PathBuf>,
    progress_tx: Option<ProgressTx>,
    ram_pressure_pct: Option<u8>,
) -> Result<Pass2Result> {
    validate_run_params(parallel)?;
    let total_bytes = file_size(path)?;
    let progress = if progress_tx.is_none() {
        Some(ProgressTracker::new(total_bytes, "Pass 2"))
    } else {
        None
    };
    let mut rows_processed = 0u64;
    const PROGRESS_INTERVAL: u64 = 1_000;
    let ram_pct = ram_pressure_pct.unwrap_or(DEFAULT_RAM_PRESSURE_PCT);
    // Shared flag: set by the dispatch loop every PROGRESS_INTERVAL rows when
    // RSS exceeds ram_pct% of total RAM. Workers drain all sinks when true.
    let memory_pressure = Arc::new(AtomicBool::new(false));

    let sep = PATH_SEP.to_string();
    let path_map: HashMap<String, TableSchema> =
        schemas.iter().map(|s| (s.path.join(&sep), s.clone())).collect();

    let root_schema = schemas
        .iter()
        .find(|s| s.path.join(&sep) == root_table)
        .ok_or_else(|| J2sError::Schema(format!("Root table '{}' not found", root_table)))?;

    // Keyed by table name so workers can create replacement sinks after interim flushes.
    let schema_by_name: Arc<HashMap<String, TableSchema>> = Arc::new(
        schemas.iter().map(|s| (s.name.clone(), s.clone())).collect(),
    );

    if let Some(ref dir) = anomaly_dir {
        std::fs::create_dir_all(dir).map_err(J2sError::Io)?;
    }

    let parallel = parallel.max(1);

    // Pre-flight check: warn if any root tables are already non-empty.
    // Append is allowed (two files with the same schema), but the user should know.
    {
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

    // Per-worker interim flush threshold: divide the global budget evenly so that
    // total temp-file disk usage stays bounded at ~INTERIM_FLUSH_THRESHOLD regardless
    // of the number of workers. Without this, 32 workers × 512 MiB = 16 GiB on disk.
    let per_worker_flush_threshold = INTERIM_FLUSH_THRESHOLD / parallel as u64;

    // Cancellation token: a DropGuard is held for the lifetime of this async fn.
    // If the caller aborts the task (e.g. UI cancel), the guard is dropped, the
    // token is cancelled, and all workers / flush / conn tasks exit their loops.
    let cancel = CancellationToken::new();
    let _cancel_guard = cancel.clone().drop_guard();

    // -------------------------------------------------------------------------
    // Phase B — Parallel streaming
    // N workers each hold their own HashMap<table_name, TempFileSink>.
    // Root objects are dispatched round-robin from the main task.
    // -------------------------------------------------------------------------
    let (mut reader, _format) = JsonReader::open(path)?;
    let path_map_arc: Arc<HashMap<String, TableSchema>> = Arc::new(path_map);
    let root_schema_arc: Arc<TableSchema> = Arc::new(root_schema.clone());

    const CHANNEL_CAP: usize = 256;
    let mut senders: Vec<tokio::sync::mpsc::Sender<Vec<u8>>> =
        Vec::with_capacity(parallel);
    let mut worker_handles = Vec::with_capacity(parallel);

    // Per-worker FD ceiling: each worker hibernates when it holds this many
    // open sinks. Divided across workers with 10% global headroom.
    let fd_budget_per_worker = (FD_GLOBAL_THRESHOLD / parallel).max(64);

    // Global FD counter shared across all workers.
    // Workers update this after every insert so every worker has visibility into
    // total process FD pressure, not just its own slice.
    let global_open_fds = Arc::new(AtomicUsize::new(0));

    // Flush channel: workers send over-threshold sinks here and replace them
    // with a fresh empty sink. A dedicated flush task (spawned below) drains
    // this channel and COPYs sinks to PG concurrently with Phase B streaming.
    let (flush_tx, flush_rx) =
        tokio::sync::mpsc::unbounded_channel::<(String, TempFileSink)>();

    for _ in 0..parallel {
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<Vec<u8>>(CHANNEL_CAP);
        senders.push(tx);

        let worker_sinks: HashMap<String, TempFileSink> = schemas
            .iter()
            .map(|s| Ok((s.name.clone(), TempFileSink::new(s, pg_schema)?)))
            .collect::<Result<_>>()?;
        let mut worker_anomalies = AnomalyCollector::new(None);
        let pm = path_map_arc.clone();
        let rs = root_schema_arc.clone();
        let global = Arc::clone(&global_open_fds);
        let ftx = flush_tx.clone();
        let sbn = schema_by_name.clone();
        let pg_schema_owned = pg_schema.to_string();
        let cancel_token = cancel.clone();
        let mem_pressure = Arc::clone(&memory_pressure);

        let handle = tokio::task::spawn(async move {
            let mut sinks = worker_sinks;
            // FDs this worker currently holds open, reflected in `global`.
            let mut my_open: usize = 0;

            loop {
            let mut bytes = tokio::select! {
                _ = cancel_token.cancelled() => break,
                msg = rx.recv() => match msg { Some(b) => b, None => break },
            };
                let obj = match simd_json::from_slice::<serde_json::Value>(&mut bytes) {
                    Ok(Value::Object(o)) => o,
                    Ok(other) => return Err(J2sError::InvalidInput(format!(
                        "Expected JSON object at root level, found: {other}"
                    ))),
                    Err(e) => return Err(J2sError::InvalidInput(format!(
                        "JSON parse error in worker: {e}"
                    ))),
                };

                // Global pressure check: if the process-wide FD count is at or
                // above threshold, release our share before adding more.
                if global.load(Ordering::Relaxed) >= FD_GLOBAL_THRESHOLD && my_open > 0 {
                    for sink in sinks.values_mut() {
                        sink.hibernate()?;
                    }
                    global_sub(&global, my_open);
                    my_open = 0;
                }
                // Per-worker budget check before insert.
                if my_open >= fd_budget_per_worker {
                    for sink in sinks.values_mut() {
                        sink.hibernate()?;
                    }
                    global_sub(&global, my_open);
                    my_open = 0;
                }

                insert_object(
                    &pm,
                    &mut sinks,
                    &mut worker_anomalies,
                    &rs,
                    &obj,
                    Uuid::now_v7(),
                    None,
                    None,
                )?;

                // Recount after every insert and update the global counter.
                let new_open = sinks.values().filter(|s| s.is_open()).count();
                if new_open > my_open {
                    global.fetch_add(new_open - my_open, Ordering::Relaxed);
                } else if new_open < my_open {
                    global_sub(&global, my_open - new_open);
                }
                my_open = new_open;

                // Per-worker budget check after insert.
                if my_open >= fd_budget_per_worker {
                    for sink in sinks.values_mut() {
                        sink.hibernate()?;
                    }
                    global_sub(&global, my_open);
                    my_open = 0;
                }

                let total_bytes: u64 = sinks.values().map(|s| s.bytes_buffered).sum();
                let ram_flag = mem_pressure.load(Ordering::Relaxed);

                if ram_flag {
                    // RAM pressure: force_spill all sinks in-place — no handoff.
                    // Pending Vecs are written to each sink's existing temp file
                    // (append), freeing RAM. Sinks stay in the worker and continue
                    // accumulating new rows into the same file — no fragmentation.
                    for sink in sinks.values_mut() {
                        let was_open = sink.is_open();
                        sink.force_spill()?;
                        sink.hibernate()?;
                        if was_open {
                            global_sub(&global, 1);
                            my_open = my_open.saturating_sub(1);
                        }
                    }
                } else if total_bytes > per_worker_flush_threshold {
                    // Byte budget exceeded.
                    // Sinks ≥ MIN_SINK_HANDOFF_BYTES are handed off to the flush
                    // task for a streaming COPY (paces large tables during import).
                    // All other non-empty sinks are force_spill'd in-place: their
                    // pending Vec is written to their existing temp file, freeing
                    // RAM without creating a new fragmented file per drain cycle.
                    let handoff_names: Vec<String> = sinks
                        .iter()
                        .filter(|(_, s)| sink_eligible_for_handoff(s))
                        .map(|(k, _)| k.clone())
                        .collect();

                    for name in handoff_names {
                        if let Some(mut old_sink) = sinks.remove(&name) {
                            let was_open = old_sink.is_open();
                            old_sink.force_spill()?;
                            old_sink.hibernate()?;
                            if was_open {
                                global_sub(&global, 1);
                                my_open = my_open.saturating_sub(1);
                            }
                            if let Some(schema) = sbn.get(&name) {
                                match TempFileSink::new(schema, &pg_schema_owned) {
                                    Ok(new_sink) => {
                                        let _ = ftx.send((name.clone(), old_sink));
                                        sinks.insert(name, new_sink);
                                    }
                                    Err(_) => {
                                        sinks.insert(name, old_sink);
                                    }
                                }
                            } else {
                                sinks.insert(name, old_sink);
                            }
                        }
                    }

                    // force_spill all remaining non-empty sinks in-place.
                    for sink in sinks.values_mut() {
                        if sink.bytes_buffered > 0 {
                            let was_open = sink.is_open();
                            sink.force_spill()?;
                            sink.hibernate()?;
                            if was_open {
                                global_sub(&global, 1);
                                my_open = my_open.saturating_sub(1);
                            }
                        }
                    }
                }
            }

            // Hibernate all open sinks to release FDs, then send remaining
            // non-empty sinks to the flush task before exiting.
            for sink in sinks.values_mut() {
                sink.hibernate()?;
            }
            global_sub(&global, my_open);
            for (name, sink) in sinks {
                if sink.row_count > 0 || sink.total_flushed > 0 {
                    let _ = ftx.send((name, sink));
                }
            }
            Ok::<_, J2sError>(worker_anomalies)
        });
        worker_handles.push(handle);
    }

    // Drop the main-task's sender clone; only the per-worker clones remain.
    drop(flush_tx);

    // -------------------------------------------------------------------------
    // Flush pool — `parallel` persistent PG connections + accumulating dispatcher.
    //
    // The dispatcher (flush_task) groups sinks by table name. Small tables wait
    // until flush_rx closes and are dispatched as one merged COPY per table
    // (N worker sinks → 1 COPY). Large tables are dispatched early when their
    // accumulated bytes_on_disk exceeds flush_dispatch_threshold (= parallel × MIN_SINK_HANDOFF_BYTES).
    // -------------------------------------------------------------------------

    // Dispatch a table once it accumulates parallel × MIN_SINK_HANDOFF_BYTES.
    // This merges one sink per worker into a single COPY, reducing transaction
    // count while bounding flush-task queue depth to ~1 sink per table per worker.
    let flush_dispatch_threshold: u64 = parallel as u64 * MIN_SINK_HANDOFF_BYTES;

    let (result_tx, mut result_rx) =
        tokio::sync::mpsc::unbounded_channel::<Result<(String, u64)>>();
    let mut conn_senders: Vec<tokio::sync::mpsc::Sender<Vec<TempFileSink>>> =
        Vec::with_capacity(parallel);
    let mut conn_handles: Vec<tokio::task::JoinHandle<()>> = Vec::with_capacity(parallel);

    for _ in 0..parallel {
        let (ctx, mut crx) = tokio::sync::mpsc::channel::<Vec<TempFileSink>>(64);
        conn_senders.push(ctx);
        let url = pg_url.to_string();
        let rtx = result_tx.clone();
        let cancel_conn = cancel.clone();
        let ptx_conn = progress_tx.clone();
        conn_handles.push(tokio::task::spawn(async move {
            use crate::db::connection::connect;
            let conn = match connect(&url).await {
                Ok(c) => c,
                Err(e) => { let _ = rtx.send(Err(e)); return; }
            };
            loop {
                let sinks = tokio::select! {
                    _ = cancel_conn.cancelled() => break,
                    msg = crx.recv() => match msg { Some(s) => s, None => break },
                };
                if sinks.is_empty() { continue; }
                let name = sinks[0].table_name.clone();
                let result = merge_copy_to_db(sinks, &conn).await;
                match &result {
                    Ok(rows) => {
                        eprintln!("  COPY {} ({} rows) done.", name, rows);
                        if let Some(ref tx) = ptx_conn {
                            let _ = tx.send(ProgressEvent::Pass2Log(format!(
                                "COPY {} ({} rows)", name, rows
                            )));
                            let _ = tx.send(ProgressEvent::Pass2Flush {
                                table_name: name.clone(),
                                rows_flushed: *rows,
                            });
                        }
                    }
                    Err(e) => {
                        eprintln!("  COPY {} FAILED: {}", name, e);
                        if let Some(ref tx) = ptx_conn {
                            let _ = tx.send(ProgressEvent::Pass2Error {
                                table_name: name.clone(),
                                message: e.to_string(),
                            });
                        }
                    }
                }
                let _ = rtx.send(result.map(|rows| (name, rows)));
            }
        }));
    }
    drop(result_tx);

    let cancel_flush = cancel.clone();
    let flush_task: tokio::task::JoinHandle<Result<HashMap<String, u64>>> =
        tokio::task::spawn(async move {
            let mut flush_rx = flush_rx;
            let mut robin = 0usize;
            let mut table_pending: HashMap<String, Vec<TempFileSink>> = HashMap::new();

            loop {
                let (table_name, sink) = tokio::select! {
                    _ = cancel_flush.cancelled() => break,
                    msg = flush_rx.recv() => match msg { Some(v) => v, None => break },
                };
                if sink.row_count == 0 && sink.total_flushed == 0 { continue; }

                let entry = table_pending.entry(table_name.clone()).or_default();
                entry.push(sink);

                // Early dispatch for large tables: don't wait for end-of-streaming.
                let total_bytes: u64 = entry.iter().map(|s| s.bytes_buffered).sum();
                if total_bytes >= flush_dispatch_threshold {
                    let sinks = table_pending.remove(&table_name).unwrap();
                    if conn_senders[robin].send(sinks).await.is_err() { break; }
                    robin = (robin + 1) % conn_senders.len();
                }
            }

            // Dispatch remaining accumulated sinks — one merged COPY per table.
            // Replaces up to N per-worker COPYs with a single COPY for each table.
            for (_table_name, sinks) in table_pending {
                let total_rows: u64 = sinks.iter().map(|s| s.total_flushed + s.row_count).sum();
                if total_rows == 0 { continue; }
                if conn_senders[robin].send(sinks).await.is_err() { break; }
                robin = (robin + 1) % conn_senders.len();
            }

            // Signal conn workers to drain and exit.
            drop(conn_senders);

            // Collect all COPY results (channel closes when all conn workers exit).
            let mut rows_per_table: HashMap<String, u64> = HashMap::new();
            let mut first_error: Option<J2sError> = None;
            while let Some(result) = result_rx.recv().await {
                match result {
                    Ok((name, count)) => { *rows_per_table.entry(name).or_insert(0) += count; }
                    Err(e) => { if first_error.is_none() { first_error = Some(e); } }
                }
            }
            for handle in conn_handles {
                if let Err(e) = handle.await {
                    if first_error.is_none() {
                        first_error = Some(J2sError::InvalidInput(format!("conn worker panic: {e}")));
                    }
                }
            }
            if let Some(e) = first_error { return Err(e); }
            Ok(rows_per_table)
        });

    let stream_start = Instant::now();
    let mut robin = 0usize;
    let mut worker_died = false;
    // Initial check before any objects are dispatched so workers see the flag
    // from their very first iteration (important for small files / low threshold).
    memory_pressure.store(ram_pressure_exceeded(ram_pct), Ordering::Relaxed);
    'dispatch: while let Some(item) = reader.next_raw() {
        let bytes = item?;
        if senders[robin].send(bytes).await.is_err() {
            // A worker exited early; collect the real error below.
            worker_died = true;
            break 'dispatch;
        }
        rows_processed += 1;
        robin = (robin + 1) % parallel;
        if let Some(ref bar) = progress {
            bar.inc_rows(1);
        }
        if rows_processed % PROGRESS_INTERVAL == 0 {
            if let Some(ref tx) = progress_tx {
                let _ = tx.send(ProgressEvent::Pass2Progress {
                    rows_processed,
                    bytes_read: reader.bytes_read(),
                    total_bytes,
                });
            }
            memory_pressure.store(ram_pressure_exceeded(ram_pct), Ordering::Relaxed);
        }
    }
    drop(senders);

    if let Some(ref tx) = progress_tx {
        if rows_processed > 0 && rows_processed % PROGRESS_INTERVAL != 0 {
            let _ = tx.send(ProgressEvent::Pass2Progress {
                rows_processed,
                bytes_read: reader.bytes_read(),
                total_bytes,
            });
        }
    }
    if let Some(ref bar) = progress {
        bar.finish();
    }
    let streaming_ms = stream_start.elapsed().as_millis() as u64;
    eprintln!("Pass 2 streaming done ({parallel} workers). Flushing remaining rows to PostgreSQL...");

    // Drain all worker handles — always collect real errors rather than
    // swallowing them behind "worker channel closed unexpectedly".
    let mut merged_anomalies = AnomalyCollector::new(anomaly_dir);
    let mut first_worker_error: Option<J2sError> = None;
    for handle in worker_handles {
        match handle.await {
            Ok(Ok(w_anomalies)) => {
                merged_anomalies.merge(w_anomalies);
            }
            Ok(Err(e)) => {
                if first_worker_error.is_none() {
                    first_worker_error = Some(e);
                }
            }
            Err(e) => {
                if first_worker_error.is_none() {
                    first_worker_error =
                        Some(J2sError::InvalidInput(format!("worker panic: {}", e)));
                }
            }
        }
    }
    if let Some(err) = first_worker_error {
        return Err(err);
    }
    if worker_died {
        return Err(J2sError::InvalidInput(
            "worker channel closed unexpectedly".into(),
        ));
    }

    // -------------------------------------------------------------------------
    // Join flush task — all workers have sent their remaining sinks via flush_rx.
    // The flush task terminates once flush_rx is exhausted and all in-flight
    // COPYs complete. rows_per_table accumulates counts from every COPY.
    // -------------------------------------------------------------------------
    let copy_start = Instant::now();
    let rows_per_table = match flush_task.await {
        Ok(Ok(rpt)) => rpt,
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(J2sError::InvalidInput(format!("flush task panic: {e}"))),
    };

    // -------------------------------------------------------------------------
    // Phase D — Constraints: PK (fatal on error), FK (failures → warnings)
    // -------------------------------------------------------------------------
    let constraint_warnings = add_constraints(client, schemas, pg_schema).await?;
    if !constraint_warnings.is_empty() {
        eprintln!(
            "WARNING: {} FK constraint(s) could not be applied after import:",
            constraint_warnings.len()
        );
        for w in &constraint_warnings {
            let msg = format!("FK warning — {} : {}", w.table, w.message);
            eprintln!("  {msg}");
            if let Some(ref tx) = progress_tx {
                let _ = tx.send(ProgressEvent::Pass2Log(msg));
            }
        }
    }

    if let Some(ref tx) = progress_tx {
        for (table_name, count) in merged_anomalies.per_table_anomaly_counts() {
            let _ = tx.send(ProgressEvent::Pass2AnomalyUpdate { table_name, count });
        }
        let total_rows: u64 = rows_per_table.values().sum();
        let _ = tx.send(ProgressEvent::Pass2Done {
            total_rows,
            anomaly_count: merged_anomalies.total_anomalies(),
            constraint_warning_count: constraint_warnings.len() as u64,
        });
    }
    merged_anomalies.finish()?;
    let copy_ms = copy_start.elapsed().as_millis() as u64;
    eprintln!(
        "Pass 2 timing: streaming={streaming_ms}ms, copy={copy_ms}ms, total={}ms",
        streaming_ms + copy_ms
    );
    Ok(Pass2Result {
        rows_per_table,
        anomaly_collector: merged_anomalies,
        constraint_warnings,
        timing: Pass2Timing {
            streaming_ms,
            copy_ms,
        },
    })
}


#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{global_sub, Pass2Timing, MIN_SINK_HANDOFF_BYTES, sink_eligible_for_handoff, validate_run_params};
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
    fn global_sub_does_not_underflow() {
        let g = AtomicUsize::new(10);
        global_sub(&g, 20); // subtract more than current value
        assert_eq!(g.load(Ordering::Relaxed), 0); // must saturate to 0, not wrap
    }

    #[test]
    fn global_sub_normal_case() {
        let g = AtomicUsize::new(100);
        global_sub(&g, 30);
        assert_eq!(g.load(Ordering::Relaxed), 70);
    }

    #[test]
    fn global_sub_exact_to_zero() {
        let g = AtomicUsize::new(50);
        global_sub(&g, 50);
        assert_eq!(g.load(Ordering::Relaxed), 0);
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

    // INTERIM_FLUSH_THRESHOLD divided by parallel must never be zero,
    // so the per-worker flush threshold remains a meaningful bound.
    #[test]
    fn per_worker_flush_threshold_never_zero() {
        use crate::db::copy_sink::INTERIM_FLUSH_THRESHOLD;
        for parallel in [1usize, 2, 4, 8, 16, 32, 64, 128, 256] {
            let per_worker = INTERIM_FLUSH_THRESHOLD / parallel as u64;
            assert!(per_worker > 0, "threshold must be > 0 for parallel={parallel}");
        }
    }

    // -------------------------------------------------------------------------
    // Drain cycle handoff filter — bytes_on_disk >= MIN_SINK_HANDOFF_BYTES
    //
    // These tests guard the critical semantic change in the drain cycle filter:
    // handoff is keyed on bytes_on_disk (bytes physically on disk since last PG
    // flush), NOT bytes_buffered (total bytes written since last PG flush). A
    // regression back to bytes_buffered would let sinks with large pending buffers
    // get handed off prematurely, before they have meaningful data on disk.
    // -------------------------------------------------------------------------

    // TempFileSink auto-spills when pending.len() >= 256 KiB (copy_sink internal).
    const TEST_SPILL_SIZE: usize = 256 * 1024;

    fn make_test_sink() -> TempFileSink {
        let mut schema = TableSchema::new("t".to_string(), vec!["t".to_string()], 0);
        schema.columns.push(ColumnSchema {
            name: "col".to_string(),
            original_name: "col".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        TempFileSink::new(&schema, "public").unwrap()
    }

    /// A sink with pending data but no spill must not pass the handoff filter.
    /// bytes_buffered > 0 is insufficient — bytes_on_disk must reach the threshold.
    #[test]
    fn drain_filter_not_triggered_before_spill() {
        let mut sink = make_test_sink();
        sink.write_row(vec![b'x'; TEST_SPILL_SIZE - 1]).unwrap();
        assert!(sink.bytes_buffered > 0, "precondition: bytes_buffered must be non-zero");
        assert_eq!(sink.bytes_on_disk, 0, "no spill yet → bytes_on_disk must be 0");
        assert!(!sink_eligible_for_handoff(&sink), "must not pass handoff filter");
    }

    /// A sink that has spilled but accumulated less than MIN_SINK_HANDOFF_BYTES on
    /// disk must not pass the handoff filter.
    #[test]
    fn drain_filter_not_triggered_below_threshold() {
        let mut sink = make_test_sink();
        // 3 × 256 KiB = 768 KiB < 1 MiB = MIN_SINK_HANDOFF_BYTES
        for _ in 0..3 {
            sink.write_row(vec![b'x'; TEST_SPILL_SIZE]).unwrap();
        }
        assert!(sink.bytes_on_disk > 0, "precondition: some bytes must be on disk");
        assert!(!sink_eligible_for_handoff(&sink), "must not pass handoff filter");
    }

    /// A sink that has accumulated >= MIN_SINK_HANDOFF_BYTES on disk must pass the filter.
    #[test]
    fn drain_filter_triggered_at_threshold() {
        let mut sink = make_test_sink();
        // 4 × 256 KiB = 1 MiB = MIN_SINK_HANDOFF_BYTES
        for _ in 0..4 {
            sink.write_row(vec![b'x'; TEST_SPILL_SIZE]).unwrap();
        }
        assert!(sink_eligible_for_handoff(&sink), "must pass handoff filter at 4 × SPILL_SIZE");
    }

    /// Regression guard: a large bytes_buffered must not trigger handoff when
    /// bytes_on_disk is still below the threshold. This is the exact failure mode
    /// of the old bytes_buffered-based filter.
    ///
    /// Uses chunks just below SPILL_THRESHOLD so auto-spill is never triggered;
    /// force_spill is called manually to accumulate bytes_on_disk in a controlled way.
    /// After 4 force_spill cycles: bytes_on_disk = 4×(SPILL_SIZE-1) = 1 048 572 < threshold.
    /// A 5th write (no spill) brings bytes_buffered to 5×(SPILL_SIZE-1) = 1 310 715 > threshold.
    #[test]
    fn drain_filter_large_bytes_buffered_insufficient_without_enough_spill() {
        let mut sink = make_test_sink();
        let chunk = vec![b'x'; TEST_SPILL_SIZE - 1]; // below auto-spill trigger
        for _ in 0..4 {
            sink.write_row(chunk.clone()).unwrap();
            sink.force_spill().unwrap();
        }
        // One more write (no spill) pushes bytes_buffered past the threshold.
        sink.write_row(chunk.clone()).unwrap();
        assert!(sink.bytes_buffered >= MIN_SINK_HANDOFF_BYTES,
            "precondition: bytes_buffered ({}) must exceed threshold ({})",
            sink.bytes_buffered, MIN_SINK_HANDOFF_BYTES);
        assert!(sink.bytes_on_disk < MIN_SINK_HANDOFF_BYTES,
            "bytes_on_disk ({}) must still be below threshold ({})",
            sink.bytes_on_disk, MIN_SINK_HANDOFF_BYTES);
        // sink_eligible_for_handoff uses bytes_on_disk — the old bytes_buffered filter
        // would have returned true here, this is the exact regression being guarded.
        assert!(!sink_eligible_for_handoff(&sink), "must not pass handoff filter");
    }

}
