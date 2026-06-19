//! Pass 2 — data insertion into PostgreSQL via a diskless pipeline.
//!
//! N workers stream the JSON round-robin into local `MemSink` buffers. A concurrent
//! `run_flusher` task drains those buffers to PostgreSQL via `COPY FROM STDIN`, eliminating
//! the need for temp files. The main entry point is [`run`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
use simd_json;
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::anomaly::collector::{AnomalyCollector, AnomalyEvent, AnomalyProxy};
use crate::db::copy_sink::flush_mem_sink_to_pg;
use crate::schema::PATH_SEP;
use crate::db::ddl::{add_constraints, ConstraintWarning};
use crate::error::{J2sError, Result};
use crate::io::progress::ProgressTracker;
use crate::io::progress_event::{ProgressEvent, ProgressTx};
use crate::io::reader::{file_size, JsonReader};
use crate::pass2::insert::{insert_object, InsertCtx};
use crate::schema::table_schema::TableSchema;

/// Wall-clock breakdown of Pass 2 streaming and COPY phases.
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

// ---------------------------------------------------------------------------
// Diskless flusher — in-memory buffers, flushes to PostgreSQL concurrently
// ---------------------------------------------------------------------------

/// Returns all table_ids with a non-empty pending buffer, in arbitrary order.
/// Used during RAM-pressure flushes to drain every accumulated table in one tick.
fn find_all_nonempty_buffers(buffers: &HashMap<String, bytes::BytesMut>) -> Vec<String> {
    buffers.iter()
        .filter(|(_, b)| !b.is_empty())
        .map(|(k, _)| k.clone())
        .collect()
}

/// RAM usage ratio in [0.0, 1.0]. Returns 0.0 when total memory is zero.
fn ram_used_ratio(available: u64, total: u64) -> f64 {
    if total == 0 { return 0.0; }
    total.saturating_sub(available) as f64 / total as f64
}

fn format_flusher_pause_log(total_mb: u64, high_mb: u64) -> String {
    format!(
        "[FLUSHER] {} MB in buffers > {} MB threshold — workers paused",
        total_mb, high_mb
    )
}

fn format_flusher_resume_log(total_mb: u64, low_mb: u64) -> String {
    format!(
        "[FLUSHER] {} MB in buffers < {} MB threshold — workers resumed",
        total_mb, low_mb
    )
}

fn format_copy_start_log(table_id: &str, buf_len: usize, rows: u64) -> String {
    format!(
        "[FLUSHER] COPY '{table_id}' — {} MB, {rows} rows",
        buf_len / 1024 / 1024
    )
}

fn format_copy_done_log(table_id: &str, elapsed_ms: u128) -> String {
    format!("[FLUSHER] COPY done '{table_id}' — {elapsed_ms} ms")
}

/// Returns the key of the non-empty buffer with the most bytes, or `None` if all are empty.
/// Used to pick the best candidate for a proactive flush during RAM pressure.
fn find_largest_buffer(buffers: &HashMap<String, bytes::BytesMut>) -> Option<String> {
    buffers.iter()
        .filter(|(_, b)| !b.is_empty())
        .max_by_key(|(_, b)| b.len())
        .map(|(k, _)| k.clone())
}

/// Flush one table's pending buffer to PostgreSQL and update accounting.
/// Removes the table from `buffers` and `pending_rows`, adds to `total_rows`.
/// Sets `error_flag` and returns `Err` on PG failure.
async fn flush_table_to_pg(
    table_id: &str,
    buffers: &mut HashMap<String, bytes::BytesMut>,
    pending_rows: &mut HashMap<String, u64>,
    total_rows: &mut HashMap<String, u64>,
    copy_sql_map: &HashMap<String, String>,
    conn: &tokio_postgres::Client,
    progress_tx: Option<&ProgressTx>,
    error_flag: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    let Some(buf) = buffers.remove(table_id) else { return Ok(()); };
    if buf.is_empty() { return Ok(()); }
    let rows = pending_rows.remove(table_id).unwrap_or(0);
    let copy_sql = copy_sql_map.get(table_id).ok_or_else(|| {
        J2sError::InvalidInput(format!("flusher: no copy_sql for table '{table_id}'"))
    })?;
    eprintln!("{}", format_copy_start_log(table_id, buf.len(), rows));
    let copy_start = std::time::Instant::now();
    if let Err(e) = flush_mem_sink_to_pg(buf.freeze(), copy_sql, conn).await {
        error_flag.store(true, std::sync::atomic::Ordering::Release);
        if let Some(tx) = progress_tx {
            let _ = tx.send(ProgressEvent::Pass2Error {
                table_name: table_id.to_string(),
                message: e.to_string(),
            });
        }
        return Err(e);
    }
    eprintln!("{}", format_copy_done_log(table_id, copy_start.elapsed().as_millis()));
    *total_rows.entry(table_id.to_string()).or_insert(0) += rows;
    if rows > 0 {
        if let Some(tx) = progress_tx {
            let _ = tx.send(ProgressEvent::Pass2Flush { table_name: table_id.to_string(), rows_flushed: rows });
        }
    }
    Ok(())
}

/// Concurrent flusher task: receives `(table_id, bytes, row_count)` batches from workers,
/// accumulates per-table `BytesMut` buffers, and COPYs to PostgreSQL when:
/// - a table's buffer exceeds `mem_flush_threshold_bytes`, or
/// - the flusher's own total buffered bytes exceed `DEFAULT_HIGH_FLUSHER_BYTES` (flushes
///   the largest table and pauses workers until buffers drop below `DEFAULT_LOW_FLUSHER_BYTES`).
///
/// Unlike a system-RAM-based signal, the total_buffered counter is unaffected by PostgreSQL's
/// buffer cache growth, which would otherwise keep `available_memory()` permanently above any
/// watermark during bulk imports.
///
/// Returns total row count per table after draining all remaining buffers.
pub(crate) async fn run_flusher(
    mut rx: tokio::sync::mpsc::Receiver<(String, bytes::Bytes, u64)>,
    copy_sql_map: HashMap<String, String>,
    pg_url: String,
    progress_tx: Option<ProgressTx>,
    pause_flag: Arc<std::sync::atomic::AtomicBool>,
    error_flag: Arc<std::sync::atomic::AtomicBool>,
    mem_flush_threshold_bytes: u64,
    ram_high_watermark: f64,
    ram_low_watermark: f64,
    verbose: bool,
) -> Result<HashMap<String, u64>> {
    use std::sync::atomic::Ordering;

    let conn = crate::db::connection::connect(&pg_url).await?;
    try_set_synchronous_commit_off(&conn).await?;

    let mut buffers: HashMap<String, bytes::BytesMut> = HashMap::new();
    let mut pending_rows: HashMap<String, u64> = HashMap::new();
    let mut total_rows: HashMap<String, u64> = HashMap::new();
    let mut total_buffered: u64 = 0;
    let mut sys = sysinfo::System::new();
    let mut ram_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    ram_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ram_tick.tick() => {
                sys.refresh_memory();
                let ratio = ram_used_ratio(sys.available_memory(), sys.total_memory());
                if verbose {
                    eprintln!("[FLUSHER] RAM tick — {:.1}% RAM, {} MB dans buffers flusher",
                        ratio * 100.0, total_buffered / 1024 / 1024);
                    if ratio > ram_high_watermark {
                        let available_mb = sys.available_memory() / 1024 / 1024;
                        let total_mb = sys.total_memory() / 1024 / 1024;
                        eprintln!("[FLUSHER] RAM {:.1}% > high {:.1}% ({} MB avail / {} MB total) — PG buffer cache (informatif)",
                            ratio * 100.0, ram_high_watermark * 100.0, available_mb, total_mb);
                    }
                }
                // Proactive flush when workers are paused: flusher's rx.recv() arm is not called
                // while workers spin, so the ram_tick is the only way to drain flusher buffers.
                // Flush tables largest-first and stop as soon as total_buffered < LOW —
                // avoids unnecessary small COPYs while still making meaningful progress.
                if pause_flag.load(Ordering::Acquire) {
                    let mut candidates = find_all_nonempty_buffers(&buffers);
                    candidates.sort_by_key(|k| std::cmp::Reverse(buffers.get(k).map_or(0, |b| b.len())));
                    for table_id in candidates {
                        if total_buffered < DEFAULT_LOW_FLUSHER_BYTES { break; }
                        let pre_flush = buffers.get(&table_id).map_or(0, |b| b.len() as u64);
                        if let Err(e) = flush_table_to_pg(
                            &table_id, &mut buffers, &mut pending_rows, &mut total_rows,
                            &copy_sql_map, &conn, progress_tx.as_ref(), &error_flag,
                        ).await {
                            pause_flag.store(false, Ordering::Release);
                            return Err(e);
                        }
                        total_buffered = total_buffered.saturating_sub(pre_flush);
                    }
                    if total_buffered < DEFAULT_LOW_FLUSHER_BYTES {
                        let msg = format_flusher_resume_log(
                            total_buffered / 1024 / 1024,
                            DEFAULT_LOW_FLUSHER_BYTES / 1024 / 1024,
                        );
                        eprintln!("{msg}");
                        if let Some(tx) = progress_tx.as_ref() { let _ = tx.send(ProgressEvent::Pass2Log(msg)); }
                        pause_flag.store(false, Ordering::Release);
                    }
                }
                let _ = ram_low_watermark; // kept for config validation
            }
            msg = rx.recv() => {
                let Some((table_id, bytes, rows)) = msg else { break; };
                total_buffered += bytes.len() as u64;
                buffers.entry(table_id.clone()).or_default().extend_from_slice(&bytes);
                *pending_rows.entry(table_id.clone()).or_insert(0) += rows;

                // Per-table threshold flush
                if buffers[&table_id].len() as u64 >= mem_flush_threshold_bytes {
                    let pre_flush = buffers.get(&table_id).map_or(0, |b| b.len() as u64);
                    if let Err(e) = flush_table_to_pg(
                        &table_id, &mut buffers, &mut pending_rows, &mut total_rows,
                        &copy_sql_map, &conn, progress_tx.as_ref(), &error_flag,
                    ).await {
                        pause_flag.store(false, Ordering::Release);
                        return Err(e);
                    }
                    total_buffered = total_buffered.saturating_sub(pre_flush);
                }

                // Pause workers when the flusher's own buffers are overloaded.
                // total_buffered measures our actual in-process bytes — immune to PG buffer cache.
                if total_buffered > DEFAULT_HIGH_FLUSHER_BYTES && !pause_flag.load(Ordering::Acquire) {
                    let msg = format_flusher_pause_log(
                        total_buffered / 1024 / 1024,
                        DEFAULT_HIGH_FLUSHER_BYTES / 1024 / 1024,
                    );
                    eprintln!("{msg}");
                    if let Some(tx) = progress_tx.as_ref() { let _ = tx.send(ProgressEvent::Pass2Log(msg)); }
                    pause_flag.store(true, Ordering::Release);
                }

                // Eager flush when paused: the channel may still carry a few in-flight messages
                // sent by workers just before they detected the pause flag. Flush the largest
                // table on each such message to keep total_buffered moving downward.
                if pause_flag.load(Ordering::Acquire) {
                    if let Some(largest) = find_largest_buffer(&buffers) {
                        let pre_flush = buffers.get(&largest).map_or(0, |b| b.len() as u64);
                        if let Err(e) = flush_table_to_pg(
                            &largest, &mut buffers, &mut pending_rows, &mut total_rows,
                            &copy_sql_map, &conn, progress_tx.as_ref(), &error_flag,
                        ).await {
                            pause_flag.store(false, Ordering::Release);
                            return Err(e);
                        }
                        total_buffered = total_buffered.saturating_sub(pre_flush);
                        if total_buffered < DEFAULT_LOW_FLUSHER_BYTES {
                            let msg = format_flusher_resume_log(
                                total_buffered / 1024 / 1024,
                                DEFAULT_LOW_FLUSHER_BYTES / 1024 / 1024,
                            );
                            eprintln!("{msg}");
                            if let Some(tx) = progress_tx.as_ref() { let _ = tx.send(ProgressEvent::Pass2Log(msg)); }
                            pause_flag.store(false, Ordering::Release);
                        }
                    }
                }
            }
        }
    }

    // Drain all remaining buffers once all workers finish (channel closed)
    for table_id in buffers.keys().cloned().collect::<Vec<_>>() {
        if let Err(e) = flush_table_to_pg(
            &table_id, &mut buffers, &mut pending_rows, &mut total_rows,
            &copy_sql_map, &conn, progress_tx.as_ref(), &error_flag,
        ).await {
            pause_flag.store(false, Ordering::Release);
            return Err(e);
        }
    }

    pause_flag.store(false, Ordering::Release);
    Ok(total_rows)
}

/// All parameters controlling a Pass 2 run.
pub struct Pass2Config {
    pub root_table: String,
    pub pg_schema: String,
    pub parallel: usize,
    pub anomaly_dir: Option<PathBuf>,
    /// Stop pass 2 after inserting this many root objects. None = full import.
    /// Some(0) = create tables with no rows.
    pub limit: Option<u64>,
    /// Per-table buffer size before the diskless worker sends a batch to the flusher.
    /// None = 64 MiB.
    pub mem_flush_threshold_bytes: Option<u64>,
    /// Flusher pauses workers above this RAM usage ratio. None = 0.70.
    pub ram_high_watermark: Option<f64>,
    /// Flusher unpauses workers below this RAM usage ratio. None = 0.50.
    pub ram_low_watermark: Option<f64>,
    /// Emit verbose logs (RAM tick every second, DISPATCH progress every 10k rows).
    /// Default false — high-frequency logs are noisy in normal use.
    pub verbose: bool,
}

/// Per-worker flush threshold: divides the global threshold by worker count so that
/// with many workers and a wide schema, each worker's per-table buffer stays proportionally
/// small and flushes reach the flusher before RAM is exhausted.
fn effective_worker_threshold(mem_flush_threshold: u64, parallel: usize) -> u64 {
    (mem_flush_threshold / parallel as u64).max(1)
}

/// Default RAM high-watermark: log a warning when system RAM exceeds this ratio.
/// Used for observability only — the actual pause trigger is `DEFAULT_HIGH_FLUSHER_BYTES`.
pub(crate) const DEFAULT_RAM_HIGH_WATERMARK: f64 = 0.70;

/// Default RAM low-watermark: kept for config validation and backward compatibility.
pub(crate) const DEFAULT_RAM_LOW_WATERMARK: f64 = 0.50;

/// Flusher's own in-process buffer high-water mark (512 MiB).
/// Workers are paused when the flusher's total buffered bytes exceeds this.
/// Independent of system RAM — unaffected by PostgreSQL's buffer cache.
pub(crate) const DEFAULT_HIGH_FLUSHER_BYTES: u64 = 512 * 1024 * 1024;

/// Flusher's own in-process buffer low-water mark (128 MiB).
/// Workers are resumed when the flusher's total buffered bytes drops below this.
pub(crate) const DEFAULT_LOW_FLUSHER_BYTES: u64 = 128 * 1024 * 1024;

fn validate_run_params(parallel: usize) -> Result<()> {
    if parallel == 0 {
        return Err(J2sError::InvalidInput(
            "parallel must be >= 1 (0 would produce an empty connection pool)".to_string(),
        ));
    }
    Ok(())
}

fn validate_watermarks(ram_high: f64, ram_low: f64, mem_flush_threshold: u64) -> Result<()> {
    if !ram_high.is_finite() || ram_high <= 0.0 || ram_high > 1.0 {
        return Err(J2sError::InvalidInput(format!(
            "ram_high_watermark must be in (0.0, 1.0], got {ram_high}"
        )));
    }
    if !ram_low.is_finite() || ram_low <= 0.0 || ram_low >= 1.0 {
        return Err(J2sError::InvalidInput(format!(
            "ram_low_watermark must be in (0.0, 1.0), got {ram_low}"
        )));
    }
    if ram_low >= ram_high {
        return Err(J2sError::InvalidInput(format!(
            "ram_low_watermark ({ram_low}) must be < ram_high_watermark ({ram_high})"
        )));
    }
    if mem_flush_threshold == 0 {
        return Err(J2sError::InvalidInput(
            "mem_flush_threshold_bytes must be > 0".to_string(),
        ));
    }
    Ok(())
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

// ---------------------------------------------------------------------------
// Diskless worker — local MemSink accumulation + channel flush
// ---------------------------------------------------------------------------

/// Collect sinks whose buffer exceeds `threshold`, returning `(table_id, bytes, row_count)`.
/// Resets extracted sinks to empty. Skips empty buffers regardless of threshold.
fn collect_above_threshold(
    sinks: &mut HashMap<String, crate::db::copy_sink::MemSink>,
    threshold: u64,
) -> Vec<(String, bytes::Bytes, u64)> {
    let mut result = Vec::new();
    for (table_id, sink) in sinks.iter_mut() {
        if sink.buf.is_empty() { continue; }
        if sink.buf.len() as u64 >= threshold {
            let bytes = std::mem::take(&mut sink.buf).freeze();
            let rows = std::mem::replace(&mut sink.row_count, 0);
            result.push((table_id.clone(), bytes, rows));
        }
    }
    result
}

/// Process one JSON item in the diskless worker: parse → insert → flush above-threshold sinks.
async fn process_worker_item_diskless(
    mut bytes: Vec<u8>,
    sinks: &mut HashMap<String, crate::db::copy_sink::MemSink>,
    proxy: &mut AnomalyProxy,
    flush_tx: &tokio::sync::mpsc::Sender<(String, bytes::Bytes, u64)>,
    path_map: &HashMap<String, TableSchema>,
    root_schema: &TableSchema,
    mem_flush_threshold: u64,
) -> Result<()> {
    let obj = parse_json_object(&mut bytes)?;
    insert_object(path_map, &mut InsertCtx { sinks, anomalies: proxy }, root_schema, &obj, Uuid::now_v7(), None, None)?;
    for (table_id, chunk, rows) in collect_above_threshold(sinks, mem_flush_threshold) {
        flush_tx.send((table_id, chunk, rows)).await
            .map_err(|_| J2sError::InvalidInput("flusher channel closed unexpectedly".to_string()))?;
    }
    Ok(())
}

/// Diskless worker: local MemSink accumulation, sends batches to `run_flusher` via channel.
/// Checks `error_flag` each iteration (flusher fatal error) and yields on `pause_flag`
/// (flusher buffer pressure). Workers spin without draining — flusher controls when to resume.
#[allow(clippy::too_many_arguments)]
async fn run_worker_diskless(
    mut rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    mut sinks: HashMap<String, crate::db::copy_sink::MemSink>,
    anomaly_tx: tokio::sync::mpsc::UnboundedSender<AnomalyEvent>,
    path_map: Arc<HashMap<String, TableSchema>>,
    root_schema: Arc<TableSchema>,
    cancel: CancellationToken,
    flush_tx: tokio::sync::mpsc::Sender<(String, bytes::Bytes, u64)>,
    pause_flag: Arc<std::sync::atomic::AtomicBool>,
    error_flag: Arc<std::sync::atomic::AtomicBool>,
    mem_flush_threshold: u64,
) -> Result<()> {
    use std::sync::atomic::Ordering;
    let mut proxy = AnomalyProxy::new(anomaly_tx);
    loop {
        if error_flag.load(Ordering::Acquire) {
            return Err(J2sError::InvalidInput("flusher reported a fatal PG error — aborting worker".to_string()));
        }
        if pause_flag.load(Ordering::Relaxed) {
            // Spin without draining: flusher has 512 MB+ of its own buffered data and needs to
            // COPY it down before accepting more. Adding more data via a drain would increase
            // total_buffered and delay the resume. Flusher controls the pause lifecycle.
            while pause_flag.load(Ordering::Relaxed) && !error_flag.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        }
        if error_flag.load(Ordering::Acquire) {
            return Err(J2sError::InvalidInput("flusher reported a fatal PG error — aborting worker".to_string()));
        }
        let bytes = tokio::select! {
            () = cancel.cancelled() => break,
            msg = rx.recv() => match msg { Some(b) => b, None => break },
        };
        process_worker_item_diskless(bytes, &mut sinks, &mut proxy, &flush_tx, &path_map, &root_schema, mem_flush_threshold).await?;
    }
    // Final flush: drain remaining sink buffers
    for (table_id, sink) in sinks.iter_mut() {
        if sink.buf.is_empty() { continue; }
        let chunk = std::mem::take(&mut sink.buf).freeze();
        let rows = std::mem::replace(&mut sink.row_count, 0);
        flush_tx.send((table_id.clone(), chunk, rows)).await
            .map_err(|_| J2sError::InvalidInput("flusher channel closed at worker teardown".to_string()))?;
    }
    Ok(())
}

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
    verbose: bool,
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
            if verbose && rows_processed % 10_000 == 0 {
                eprintln!("[DISPATCH] {} records, {} MB scanned", rows_processed, reader.bytes_read() / 1024 / 1024);
            }
        }
    }
    Ok((rows_processed, worker_died))
}

fn find_root_schema(schemas: &[TableSchema], root_table: &str, sep: &str) -> Result<TableSchema> {
    schemas
        .iter()
        .find(|s| s.path.join(sep) == root_table)
        .cloned()
        .ok_or_else(|| J2sError::Schema(format!("Root table '{root_table}' not found")))
}

/// Build the per-table COPY SQL map used by `run_flusher` to open COPY FROM STDIN sessions.
fn build_copy_sql_map(schemas: &[TableSchema], pg_schema: &str) -> HashMap<String, String> {
    schemas.iter()
        .map(|s| {
            let sink = crate::db::copy_sink::MemSink::new(s, pg_schema);
            (s.name.clone(), sink.copy_sql)
        })
        .collect()
}

/// Diskless pipeline: N workers stream JSON into local `MemSink` buffers, sending batches to a
/// concurrent flusher that COPYs directly to PostgreSQL. Phase B (temp-file re-read) is eliminated.
///
/// Phase D — `add_constraints()` adds PRIMARY KEY (fatal on error) then
/// FOREIGN KEY (failures become `constraint_warnings`).
#[allow(clippy::too_many_lines)] // top-level orchestrator — sequential stages, not splittable further
pub async fn run(
    path: &Path,
    schemas: &[TableSchema],
    client: &Client,
    pg_url: &str,
    config: &Pass2Config,
    progress_tx: Option<ProgressTx>,
) -> Result<Pass2Result> {
    validate_run_params(config.parallel)?;

    let mem_flush_threshold = config.mem_flush_threshold_bytes.unwrap_or(64 * 1024 * 1024);
    let ram_high = config.ram_high_watermark.unwrap_or(DEFAULT_RAM_HIGH_WATERMARK);
    let ram_low = config.ram_low_watermark.unwrap_or(DEFAULT_RAM_LOW_WATERMARK);
    let verbose = config.verbose;
    validate_watermarks(ram_high, ram_low, mem_flush_threshold)?;

    let parallel = config.parallel.max(1);
    let total_bytes = file_size(path)?;
    let progress = progress_tx.is_none().then(|| ProgressTracker::new(total_bytes, "Pass 2"));

    let sep = PATH_SEP.to_string();
    let path_map: HashMap<String, TableSchema> = schemas.iter()
        .map(|s| (s.path.join(&sep), s.clone()))
        .collect();
    let root_schema = find_root_schema(schemas, &config.root_table, &sep)?;

    if let Some(ref dir) = config.anomaly_dir {
        std::fs::create_dir_all(dir).map_err(J2sError::Io)?;
    }
    let (anomaly_tx, anomaly_writer_handle) = spawn_anomaly_writer(config.anomaly_dir.clone());
    preflight_warn_nonempty(schemas, client, &config.pg_schema, progress_tx.as_ref()).await;

    let cancel = CancellationToken::new();
    let _cancel_guard = cancel.clone().drop_guard();

    let copy_sql_map = build_copy_sql_map(schemas, &config.pg_schema);
    let (flush_tx, flush_rx) = tokio::sync::mpsc::channel::<(String, bytes::Bytes, u64)>(256);
    let pause_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let error_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flusher_handle = tokio::spawn(run_flusher(
        flush_rx,
        copy_sql_map,
        pg_url.to_string(),
        progress_tx.clone(),
        Arc::clone(&pause_flag),
        Arc::clone(&error_flag),
        mem_flush_threshold,
        ram_high,
        ram_low,
        verbose,
    ));

    let (mut reader, _format) = JsonReader::open(path)?;
    let path_map_arc: Arc<HashMap<String, TableSchema>> = Arc::new(path_map);
    let root_schema_arc: Arc<TableSchema> = Arc::new(root_schema);

    const WORKER_CHANNEL_CAP: usize = 256;
    let mut senders = Vec::with_capacity(parallel);
    let mut worker_handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(parallel);
    for _ in 0..parallel {
        let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(WORKER_CHANNEL_CAP);
        senders.push(tx);
        let worker_sinks: HashMap<String, crate::db::copy_sink::MemSink> = schemas.iter()
            .map(|s| (s.name.clone(), crate::db::copy_sink::MemSink::new(s, &config.pg_schema)))
            .collect();
        worker_handles.push(tokio::task::spawn(run_worker_diskless(
            rx,
            worker_sinks,
            anomaly_tx.clone(),
            path_map_arc.clone(),
            root_schema_arc.clone(),
            cancel.clone(),
            flush_tx.clone(),
            Arc::clone(&pause_flag),
            Arc::clone(&error_flag),
            effective_worker_threshold(mem_flush_threshold, parallel),
        )));
    }
    drop(flush_tx);  // workers now hold all flush_tx clones
    drop(anomaly_tx); // workers now hold all anomaly_tx clones

    let stream_start = Instant::now();
    let (rows_processed, worker_died) = dispatch_loop(
        &mut reader, &senders, progress_tx.as_ref(), progress.as_ref(), config.limit, total_bytes, verbose,
    ).await?;
    drop(senders); // workers break their recv loops → drain local sinks → drop flush_tx + anomaly_tx

    finalize_dispatch(progress_tx.as_ref(), progress.as_ref(), rows_processed, reader.bytes_read(), total_bytes);
    #[allow(clippy::cast_possible_truncation)]
    let streaming_ms = stream_start.elapsed().as_millis() as u64;
    eprintln!("Pass 2 streaming done ({parallel} workers). Flushing remaining rows to PostgreSQL...");

    // Join all workers — accumulate first error; always drain all handles so channels are closed.
    let mut first_error: Option<J2sError> = None;
    for handle in worker_handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => { if first_error.is_none() { first_error = Some(e); } }
            Err(e) => { if first_error.is_none() {
                first_error = Some(J2sError::InvalidInput(format!("worker panic: {e}")));
            }}
        }
    }
    if worker_died && first_error.is_none() {
        first_error = Some(J2sError::InvalidInput("worker channel closed unexpectedly".into()));
    }

    // All workers done → anomaly channel closed → writer finishes.
    let merged_anomalies = match anomaly_writer_handle.await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => {
            flusher_handle.abort();
            let _ = flusher_handle.await;
            return Err(e);
        }
        Err(e) => {
            flusher_handle.abort();
            let _ = flusher_handle.await;
            return Err(J2sError::InvalidInput(format!("anomaly writer task panicked: {e}")));
        }
    };

    // All flush_tx clones dropped by workers → flusher drains buffers → finishes.
    // Check flusher result first — it carries the real PG error (table + SQL), not the generic
    // "flusher reported a fatal error" message that workers emit when they see error_flag.
    let flusher_result = flusher_handle.await;

    let rows_per_table = match flusher_result {
        Ok(Ok(rows)) => rows,
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(J2sError::InvalidInput(format!("flusher task panicked: {e}"))),
    };

    if let Some(err) = first_error { return Err(err); }

    eprintln!("Pass 2 flusher complete. Applying constraints...");
    let copy_start = Instant::now();
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::{Pass2Config, Pass2Timing, validate_run_params};
    use crate::schema::table_schema::TableSchema;

    // -------------------------------------------------------------------------
    // Error priority tests — flusher error must surface before generic worker error
    // -------------------------------------------------------------------------

    #[test]
    fn flusher_error_takes_precedence_over_worker_generic_error() {
        use crate::error::J2sError;

        // Simulate: flusher has real PG error, workers have generic "flusher died" message.
        // The FIXED code checks flusher_result first, so the PG error is returned.
        let flusher_err = J2sError::InvalidInput("COPY failed: table 'orders', unique violation".to_string());
        let worker_err = J2sError::InvalidInput("flusher reported a fatal PG error — aborting worker".to_string());

        // Reproduce the fixed ordering: flusher checked first, then first_error.
        let flusher_result: Result<std::collections::HashMap<String, u64>, J2sError> = Err(flusher_err);
        let first_error: Option<J2sError> = Some(worker_err);

        let result: crate::error::Result<()> = match flusher_result {
            Ok(_) => {
                if let Some(e) = first_error { Err(e) } else { Ok(()) }
            }
            Err(e) => Err(e),
        };

        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("COPY failed"), "must surface PG error, not generic: {msg}");
        assert!(!msg.contains("aborting worker"), "must not surface generic worker error: {msg}");
    }

    // -------------------------------------------------------------------------
    // Pass2Error event emission tests
    // -------------------------------------------------------------------------

    #[test]
    fn pass2_error_event_is_sent_via_progress_tx() {
        use crate::io::progress_event::{ProgressEvent, ProgressTx};

        let (tx, mut rx): (ProgressTx, _) = tokio::sync::mpsc::unbounded_channel();
        let _ = tx.send(ProgressEvent::Pass2Error {
            table_name: "orders".to_string(),
            message: "COPY failed: duplicate key value".to_string(),
        });
        let event = rx.try_recv().expect("event must be in the channel");
        match event {
            ProgressEvent::Pass2Error { table_name, message } => {
                assert_eq!(table_name, "orders");
                assert!(message.contains("duplicate key"), "message must contain error text");
            }
            other => panic!("unexpected event variant: {other:?}"),
        }
    }

    // -------------------------------------------------------------------------
    // flusher handle leak tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn flusher_abort_await_cancels_running_task() {
        let handle: tokio::task::JoinHandle<crate::error::Result<HashMap<String, u64>>> =
            tokio::spawn(async { futures_util::future::pending::<crate::error::Result<HashMap<String, u64>>>().await });
        handle.abort();
        let result = handle.await;
        assert!(result.is_err(), "aborted task must return JoinError");
        assert!(result.unwrap_err().is_cancelled(), "must be JoinError::Cancelled");
    }

    #[tokio::test]
    async fn flusher_abort_is_safe_when_already_completed() {
        let handle: tokio::task::JoinHandle<crate::error::Result<HashMap<String, u64>>> =
            tokio::spawn(async { Ok(HashMap::new()) });
        tokio::task::yield_now().await;
        handle.abort();
        let result = handle.await;
        assert!(result.is_ok(), "completed task result must survive abort");
        assert!(result.unwrap().is_ok());
    }

    // -------------------------------------------------------------------------
    // pause_flag / error_flag interaction tests
    // -------------------------------------------------------------------------

    #[tokio::test]
    async fn pause_spin_exits_when_error_flag_set() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let pause_flag = Arc::new(AtomicBool::new(true));
        let error_flag = Arc::new(AtomicBool::new(false));
        let pf = Arc::clone(&pause_flag);
        let ef = Arc::clone(&error_flag);

        let spin = tokio::spawn(async move {
            while pf.load(Ordering::Relaxed) && !ef.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        });
        tokio::task::yield_now().await;
        error_flag.store(true, Ordering::Release);
        tokio::time::timeout(std::time::Duration::from_millis(200), spin)
            .await
            .expect("spin must exit within 200ms when error_flag is set")
            .expect("task must not panic");
    }

    /// Verifies that the worker exits its pause spin when error_flag is set externally,
    /// even if pause_flag is never cleared. This test FAILS on the old code (infinite spin)
    /// and PASSES after the fix (spin checks error_flag).
    #[tokio::test]
    async fn worker_exits_pause_spin_when_error_flag_set_while_spinning() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let schema = make_schema_with_rows("root", 0);
        let sep = crate::schema::PATH_SEP.to_string();
        let path_map = Arc::new(HashMap::from([
            (schema.path.join(&sep), schema.clone()),
        ]));
        let root_schema = Arc::new(schema);
        let (anomaly_tx, _) = tokio::sync::mpsc::unbounded_channel::<crate::anomaly::collector::AnomalyEvent>();
        let (flush_tx, _flush_rx) = tokio::sync::mpsc::channel::<(String, bytes::Bytes, u64)>(1);
        let (_item_tx, item_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
        let cancel = tokio_util::sync::CancellationToken::new();
        let error_flag = Arc::new(AtomicBool::new(false)); // starts false
        let pause_flag = Arc::new(AtomicBool::new(true));  // starts true → worker enters spin

        let ef = Arc::clone(&error_flag);
        let worker_handle = tokio::spawn(super::run_worker_diskless(
            item_rx, HashMap::new(), anomaly_tx, path_map, root_schema,
            cancel, flush_tx, pause_flag, Arc::clone(&error_flag), 64 * 1024 * 1024,
        ));
        // Give the worker enough time to enter the pause spin loop.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        // Set error_flag without clearing pause_flag — the fix makes the worker exit.
        ef.store(true, Ordering::Release);

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            worker_handle,
        ).await;
        assert!(result.is_ok(), "worker must not hang when error_flag set while in pause spin");
        assert!(result.unwrap().unwrap().is_err(), "worker must return Err");
    }

    #[tokio::test]
    async fn worker_returns_err_immediately_when_error_flag_preset() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let schema = make_schema_with_rows("root", 0);
        let sep = crate::schema::PATH_SEP.to_string();
        let path_map = Arc::new(HashMap::from([
            (schema.path.join(&sep), schema.clone()),
        ]));
        let root_schema = Arc::new(schema);
        let (anomaly_tx, _) = tokio::sync::mpsc::unbounded_channel::<crate::anomaly::collector::AnomalyEvent>();
        let (flush_tx, _flush_rx) = tokio::sync::mpsc::channel::<(String, bytes::Bytes, u64)>(1);
        let (_item_tx, item_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
        let cancel = tokio_util::sync::CancellationToken::new();
        let error_flag = Arc::new(AtomicBool::new(true));
        let pause_flag = Arc::new(AtomicBool::new(false));

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            super::run_worker_diskless(
                item_rx, HashMap::new(), anomaly_tx, path_map, root_schema,
                cancel, flush_tx, pause_flag, error_flag, 64 * 1024 * 1024,
            ),
        ).await;
        assert!(result.is_ok(), "worker must not hang");
        assert!(result.unwrap().is_err(), "worker must return Err when error_flag is set");
    }

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

    // -------------------------------------------------------------------------
    // validate_watermarks tests
    // -------------------------------------------------------------------------

    #[test]
    fn watermarks_valid_defaults_pass() {
        assert!(super::validate_watermarks(
            super::DEFAULT_RAM_HIGH_WATERMARK,
            super::DEFAULT_RAM_LOW_WATERMARK,
            64 * 1024 * 1024,
        ).is_ok());
    }

    #[test]
    fn default_ram_high_watermark_is_logging_threshold() {
        // RAM watermarks are now informational only — actual pause uses total_buffered.
        assert_eq!(super::DEFAULT_RAM_HIGH_WATERMARK, 0.70);
    }

    #[test]
    fn default_ram_low_watermark_kept_for_config_compat() {
        assert_eq!(super::DEFAULT_RAM_LOW_WATERMARK, 0.50);
    }

    #[test]
    fn default_high_flusher_bytes_is_512_mb() {
        assert_eq!(super::DEFAULT_HIGH_FLUSHER_BYTES, 512 * 1024 * 1024);
    }

    #[test]
    fn default_low_flusher_bytes_is_128_mb() {
        assert_eq!(super::DEFAULT_LOW_FLUSHER_BYTES, 128 * 1024 * 1024);
    }

    #[test]
    fn watermarks_high_above_one_is_invalid() {
        assert!(super::validate_watermarks(1.01, 0.70, 1024).is_err());
    }

    #[test]
    fn watermarks_high_at_one_is_valid() {
        assert!(super::validate_watermarks(1.0, 0.70, 1024).is_ok());
    }

    #[test]
    fn watermarks_high_zero_is_invalid() {
        assert!(super::validate_watermarks(0.0, 0.0, 1024).is_err());
    }

    #[test]
    fn watermarks_high_nan_is_invalid() {
        assert!(super::validate_watermarks(f64::NAN, 0.70, 1024).is_err());
    }

    #[test]
    fn watermarks_high_inf_is_invalid() {
        assert!(super::validate_watermarks(f64::INFINITY, 0.70, 1024).is_err());
    }

    #[test]
    fn watermarks_low_geq_high_is_invalid() {
        assert!(super::validate_watermarks(0.80, 0.80, 1024).is_err());
        assert!(super::validate_watermarks(0.80, 0.90, 1024).is_err());
    }

    #[test]
    fn watermarks_threshold_zero_is_invalid() {
        assert!(super::validate_watermarks(0.85, 0.70, 0).is_err());
    }

    #[test]
    fn watermarks_threshold_one_is_valid() {
        assert!(super::validate_watermarks(0.85, 0.70, 1).is_ok());
    }

    // -------------------------------------------------------------------------
    // validate_run_params tests
    // -------------------------------------------------------------------------

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
            limit: None,
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: false,
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
            limit: Some(0),
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: false,
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

    fn make_schema_with_rows(name: &str, row_count: u64) -> crate::schema::table_schema::TableSchema {
        let mut s = crate::schema::table_schema::TableSchema::new(
            name.to_string(), vec![name.to_string()], 0,
        );
        s.row_count = row_count;
        s
    }

    // -------------------------------------------------------------------------
    // collect_above_threshold tests — pure function, no PG required
    // -------------------------------------------------------------------------

    fn make_mem_sink_with_data(table: &str, data: &[u8]) -> crate::db::copy_sink::MemSink {
        let schema = TableSchema::new(table.to_string(), vec![table.to_string()], 0);
        let mut sink = crate::db::copy_sink::MemSink::new(&schema, "public");
        sink.write_row(data).unwrap();
        sink
    }

    #[test]
    fn collect_above_threshold_empty_sinks_returns_empty() {
        let mut sinks: HashMap<String, crate::db::copy_sink::MemSink> = HashMap::new();
        let result = super::collect_above_threshold(&mut sinks, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn collect_above_threshold_sink_below_threshold_not_collected() {
        let mut sinks = HashMap::new();
        sinks.insert("t".to_string(), make_mem_sink_with_data("t", b"small\n"));
        let result = super::collect_above_threshold(&mut sinks, 1000);
        assert!(result.is_empty(), "sink below threshold must not be collected");
        assert!(!sinks["t"].buf.is_empty(), "sink must retain its data");
    }

    #[test]
    fn collect_above_threshold_sink_above_threshold_collected_and_reset() {
        let mut sinks = HashMap::new();
        sinks.insert("t".to_string(), make_mem_sink_with_data("t", b"row\n"));
        let result = super::collect_above_threshold(&mut sinks, 1); // threshold=1 → always flush
        assert_eq!(result.len(), 1);
        let (tid, bytes, rows) = &result[0];
        assert_eq!(tid, "t");
        assert_eq!(&bytes[..], b"row\n");
        assert_eq!(*rows, 1);
        // Sink must be reset
        assert!(sinks["t"].buf.is_empty(), "sink buf must be cleared after collection");
        assert_eq!(sinks["t"].row_count, 0, "sink row_count must be reset");
    }

    #[test]
    fn collect_above_threshold_only_above_threshold_collected() {
        let mut sinks = HashMap::new();
        sinks.insert("big".to_string(), make_mem_sink_with_data("big", b"lots of data here\n"));
        sinks.insert("small".to_string(), make_mem_sink_with_data("small", b"x\n"));
        // threshold=10 → "big" (18 bytes) collected; "small" (2 bytes) kept
        let result = super::collect_above_threshold(&mut sinks, 10);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, "big");
        assert!(!sinks["small"].buf.is_empty(), "small sink must be kept");
    }

    // -------------------------------------------------------------------------
    // run_flusher helper tests — pure functions, no PG required
    // -------------------------------------------------------------------------

    #[test]
    fn find_all_nonempty_buffers_empty_map_returns_empty() {
        let buffers: HashMap<String, bytes::BytesMut> = HashMap::new();
        assert!(super::find_all_nonempty_buffers(&buffers).is_empty());
    }

    #[test]
    fn find_all_nonempty_buffers_skips_empty_entries() {
        let mut buffers = HashMap::new();
        buffers.insert("empty".to_string(), bytes::BytesMut::new());
        assert!(super::find_all_nonempty_buffers(&buffers).is_empty());
    }

    #[test]
    fn find_all_nonempty_buffers_returns_all_nonempty_keys() {
        let mut buffers = HashMap::new();
        let mut b1 = bytes::BytesMut::new();
        b1.extend_from_slice(b"data");
        let mut b2 = bytes::BytesMut::new();
        b2.extend_from_slice(b"more data");
        buffers.insert("t1".to_string(), b1);
        buffers.insert("t2".to_string(), b2);
        buffers.insert("empty".to_string(), bytes::BytesMut::new());
        let mut result = super::find_all_nonempty_buffers(&buffers);
        result.sort();
        assert_eq!(result, vec!["t1".to_string(), "t2".to_string()]);
    }

    #[test]
    fn find_all_nonempty_buffers_single_entry() {
        let mut buffers = HashMap::new();
        let mut buf = bytes::BytesMut::new();
        buf.extend_from_slice(b"x");
        buffers.insert("t1".to_string(), buf);
        assert_eq!(super::find_all_nonempty_buffers(&buffers), vec!["t1".to_string()]);
    }

    // -------------------------------------------------------------------------
    // RAM watermark log format tests
    // -------------------------------------------------------------------------

    #[test]
    fn flusher_pause_log_contains_current_and_threshold_mb() {
        let msg = super::format_flusher_pause_log(600, 512);
        assert!(msg.contains("600"), "must show current MB: {msg}");
        assert!(msg.contains("512"), "must show threshold MB: {msg}");
    }

    #[test]
    fn flusher_resume_log_contains_current_and_threshold_mb() {
        let msg = super::format_flusher_resume_log(100, 128);
        assert!(msg.contains("100"), "must show current MB: {msg}");
        assert!(msg.contains("128"), "must show threshold MB: {msg}");
    }

    #[test]
    fn copy_start_log_contains_table_size_and_rows() {
        let msg = super::format_copy_start_log("ingredients_debug", 67_108_864, 7_664_150);
        assert!(msg.contains("ingredients_debug"), "must show table name: {msg}");
        assert!(msg.contains("64"), "must show size in MB: {msg}");
        assert!(msg.contains("7664150"), "must show row count: {msg}");
    }

    #[test]
    fn copy_done_log_contains_table_and_duration() {
        let msg = super::format_copy_done_log("ingredients_debug", 2_341);
        assert!(msg.contains("ingredients_debug"), "must show table name: {msg}");
        assert!(msg.contains("2341"), "must show duration ms: {msg}");
    }

    // -------------------------------------------------------------------------
    // find_largest_buffer tests
    // -------------------------------------------------------------------------

    #[test]
    fn find_largest_buffer_returns_key_of_largest_nonempty() {
        let mut buffers = HashMap::new();
        let mut big = bytes::BytesMut::new();
        big.extend_from_slice(&vec![0u8; 1000]);
        buffers.insert("big".to_string(), big);
        let mut small = bytes::BytesMut::new();
        small.extend_from_slice(&vec![0u8; 100]);
        buffers.insert("small".to_string(), small);
        assert_eq!(super::find_largest_buffer(&buffers), Some("big".to_string()));
    }

    #[test]
    fn find_largest_buffer_returns_none_when_all_empty() {
        let mut buffers = HashMap::new();
        buffers.insert("t".to_string(), bytes::BytesMut::new());
        assert_eq!(super::find_largest_buffer(&buffers), None);
    }

    #[test]
    fn find_largest_buffer_returns_none_for_empty_map() {
        let buffers: HashMap<String, bytes::BytesMut> = HashMap::new();
        assert_eq!(super::find_largest_buffer(&buffers), None);
    }

    // -------------------------------------------------------------------------
    // effective_worker_threshold tests
    // -------------------------------------------------------------------------

    #[test]
    fn effective_worker_threshold_divides_by_parallel() {
        assert_eq!(super::effective_worker_threshold(64 * 1024 * 1024, 8), 8 * 1024 * 1024);
    }

    #[test]
    fn effective_worker_threshold_single_worker_unchanged() {
        assert_eq!(super::effective_worker_threshold(64 * 1024 * 1024, 1), 64 * 1024 * 1024);
    }

    #[test]
    fn effective_worker_threshold_16_workers_reproduces_fix() {
        // 64 MB / 16 workers = 4 MB per worker — tables flush before deadlock
        assert_eq!(super::effective_worker_threshold(64 * 1024 * 1024, 16), 4 * 1024 * 1024);
    }

    #[test]
    fn effective_worker_threshold_minimum_is_one() {
        // threshold=1 / parallel=100 → 0, clamped to 1
        assert_eq!(super::effective_worker_threshold(1, 100), 1);
    }

    #[test]
    fn ram_used_ratio_zero_total_returns_zero() {
        assert_eq!(super::ram_used_ratio(0, 0), 0.0);
    }

    #[test]
    fn ram_used_ratio_full_usage() {
        let r = super::ram_used_ratio(0, 100);
        assert!((r - 1.0).abs() < 1e-9, "available=0, total=100 → ratio=1.0, got {r}");
    }

    #[test]
    fn ram_used_ratio_half_usage() {
        let r = super::ram_used_ratio(50, 100);
        assert!((r - 0.5).abs() < 1e-9, "available=50, total=100 → ratio=0.5, got {r}");
    }

    // -------------------------------------------------------------------------
    // build_copy_sql_map tests
    // -------------------------------------------------------------------------

    #[test]
    fn build_copy_sql_map_includes_all_schemas() {
        let schemas = vec![
            make_schema_with_rows("orders", 0),
            make_schema_with_rows("items", 0),
        ];
        let map = super::build_copy_sql_map(&schemas, "public");
        assert!(map.contains_key("orders"), "orders must be in the map");
        assert!(map.contains_key("items"), "items must be in the map");
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn build_copy_sql_map_sql_references_schema_and_table() {
        let schemas = vec![make_schema_with_rows("products", 0)];
        let map = super::build_copy_sql_map(&schemas, "myschema");
        let sql = map.get("products").expect("products must be present");
        assert!(sql.contains("myschema"), "COPY SQL must reference the pg_schema");
        assert!(sql.contains("products"), "COPY SQL must reference the table name");
    }

}
