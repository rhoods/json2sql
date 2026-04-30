use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
use tokio_postgres::Client;
use uuid::Uuid;

use crate::anomaly::collector::AnomalyCollector;
use crate::db::copy_sink::{TempFileSink, MAX_OPEN_TEMP_FILES};
use crate::error::{J2sError, Result};
use crate::io::progress::ProgressTracker;
use crate::io::progress_event::{ProgressEvent, ProgressTx};
use crate::io::reader::{file_size, JsonReader};
use crate::pass2::insert::insert_object;
use crate::schema::table_schema::TableSchema;

/// Wall-clock breakdown of the two main phases of Pass 2.
pub struct Pass2Timing {
    /// Time spent streaming the JSON file and writing rows to temp files.
    pub streaming_ms: u64,
    /// Time spent in the COPY-to-PostgreSQL phase.
    pub copy_ms: u64,
}

impl Pass2Timing {
    pub fn total_ms(&self) -> u64 {
        self.streaming_ms + self.copy_ms
    }
}

/// Pass 2 result summary.
pub struct Pass2Result {
    pub rows_per_table: HashMap<String, u64>,
    pub anomaly_collector: AnomalyCollector,
    pub timing: Pass2Timing,
}

/// Run Pass 2: stream through the file again, buffer rows to temp files,
/// then COPY each table into PostgreSQL.
///
/// When `parallel > 1`, the streaming phase is parallelised: root JSON objects
/// are distributed round-robin to N worker tasks each with their own
/// `TempFileSink` set. After all workers finish, sinks are COPYed to PostgreSQL
/// in topological order (parents before children) using the shared `client`.
///
/// `db_url` is accepted for API compatibility but is not used in the current
/// implementation (parallel streaming uses the shared `client` for COPY).
/// `anomaly_dir` is the directory where per-table NDJSON anomaly files are streamed;
/// `None` disables file streaming (counters and examples are still collected in RAM).
/// `progress_tx` — optional channel for streaming progress to the IHM.
pub async fn run(
    path: &Path,
    root_table: &str,
    schemas: &[TableSchema],
    client: &Client,
    pg_schema: &str,
    flush_threshold: usize,
    use_transaction: bool,
    _db_url: Option<&str>,
    parallel: usize,
    anomaly_dir: Option<PathBuf>,
    progress_tx: Option<ProgressTx>,
) -> Result<Pass2Result> {
    let total_bytes = file_size(path)?;
    let progress = if progress_tx.is_none() {
        Some(ProgressTracker::new(total_bytes, "Pass 2"))
    } else {
        None
    };
    let mut rows_processed = 0u64;
    const PROGRESS_INTERVAL: u64 = 1_000;

    // Build path → owned schema lookup (owned so it can be Arc-shared across workers)
    let path_map: HashMap<String, TableSchema> =
        schemas.iter().map(|s| (s.path.join("."), s.clone())).collect();

    let root_schema = schemas
        .iter()
        .find(|s| s.path.join(".") == root_table)
        .ok_or_else(|| J2sError::Schema(format!("Root table '{}' not found", root_table)))?;

    // Pre-computed topological order: schemas is already sorted parents-before-children
    // by Pass 1. We use this order for periodic flushes and the final COPY phase to
    // avoid FK constraint violations.
    let topo_order: Vec<String> = schemas.iter().map(|s| s.name.clone()).collect();

    // Create anomaly_dir if specified and not yet existing
    if let Some(ref dir) = anomaly_dir {
        std::fs::create_dir_all(dir).map_err(crate::error::J2sError::Io)?;
    }

    // ---------------------------------------------------------------------------
    // Parallel streaming path
    // ---------------------------------------------------------------------------
    if parallel > 1 {
        let (mut reader, _format) = JsonReader::open(path)?;
        let path_map_arc: Arc<HashMap<String, TableSchema>> = Arc::new(path_map);
        let root_schema_arc: Arc<TableSchema> = Arc::new(root_schema.clone());

        // N channels: main thread → workers (bounded to limit peak memory)
        const CHANNEL_CAP: usize = 256;
        let mut senders: Vec<tokio::sync::mpsc::Sender<serde_json::Map<String, Value>>> =
            Vec::with_capacity(parallel);
        let mut worker_handles = Vec::with_capacity(parallel);

        // FD guard: limit how many workers may run insert_object simultaneously.
        // Each insert opens at most schemas.len() temp-file FDs. Capping concurrent
        // inserts ensures peak FDs stay within the OS ulimit regardless of schema size.
        let n_tables = schemas.len();
        let max_concurrent = max_concurrent_inserts(n_tables, parallel);
        let insert_sem = Arc::new(tokio::sync::Semaphore::new(max_concurrent));

        for _ in 0..parallel {
            let (tx, mut rx) =
                tokio::sync::mpsc::channel::<serde_json::Map<String, Value>>(CHANNEL_CAP);
            senders.push(tx);

            let worker_sinks: HashMap<String, TempFileSink> = schemas
                .iter()
                .map(|s| Ok((s.name.clone(), TempFileSink::new(s, pg_schema)?)))
                .collect::<Result<_>>()?;
            let mut worker_anomalies = AnomalyCollector::new(None);
            let pm = path_map_arc.clone();
            let rs = root_schema_arc.clone();
            let sem = insert_sem.clone();

            let handle = tokio::task::spawn(async move {
                let mut sinks = worker_sinks;
                while let Some(obj) = rx.recv().await {
                    // Try to grab an insert slot without blocking.
                    // If all slots are taken, hibernate open FDs first (cheap: just
                    // close(), no write — pending stays in memory) then wait.
                    let _permit = match sem.try_acquire() {
                        Ok(p) => p,
                        Err(_) => {
                            for sink in sinks.values_mut() {
                                sink.hibernate()?;
                            }
                            sem.acquire().await
                                .map_err(|_| J2sError::InvalidInput("insert semaphore closed".into()))?
                        }
                    };
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
                    // Permit released here. Open FDs (from spills) persist until
                    // the next time this worker must wait for a slot.
                }
                Ok::<_, J2sError>((sinks, worker_anomalies))
            });
            worker_handles.push(handle);
        }

        // Distribute root objects round-robin to workers
        let stream_start = Instant::now();
        let mut robin = 0usize;
        let mut worker_died = false;
        'dispatch: while let Some(item) = reader.next() {
            let value = item?;
            if let Value::Object(obj) = value {
                if senders[robin].send(obj).await.is_err() {
                    // A worker exited early (likely with an error). Stop dispatching;
                    // the real error will be collected from the JoinHandle below.
                    worker_died = true;
                    break 'dispatch;
                }
                rows_processed += 1;
                robin = (robin + 1) % parallel;
                if let Some(ref bar) = progress {
                    bar.inc_rows(1);
                }
                if let Some(ref tx) = progress_tx {
                    if rows_processed % PROGRESS_INTERVAL == 0 {
                        let _ = tx.send(ProgressEvent::Pass2Progress {
                            rows_processed,
                            bytes_read: reader.bytes_read(),
                            total_bytes,
                        });
                    }
                }
            }
        }
        // Signal all workers that streaming is done
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

        // Collect worker results — always drain all handles so we surface the real error.
        let mut all_worker_sinks: Vec<HashMap<String, TempFileSink>> =
            Vec::with_capacity(parallel);
        let mut merged_anomalies = AnomalyCollector::new(anomaly_dir);
        let mut first_worker_error: Option<J2sError> = None;
        for handle in worker_handles {
            match handle.await {
                Ok(Ok((w_sinks, w_anomalies))) => {
                    all_worker_sinks.push(w_sinks);
                    merged_anomalies.merge(w_anomalies);
                }
                Ok(Err(e)) => {
                    if first_worker_error.is_none() {
                        first_worker_error = Some(e);
                    }
                }
                Err(e) => {
                    if first_worker_error.is_none() {
                        first_worker_error = Some(J2sError::InvalidInput(
                            format!("worker panic: {}", e),
                        ));
                    }
                }
            }
        }
        if let Some(err) = first_worker_error {
            return Err(err);
        }
        if worker_died {
            return Err(J2sError::InvalidInput("worker channel closed unexpectedly".into()));
        }

        // COPY in topological order: ALL workers' sinks for each table before moving to
        // the next depth level, so FK constraints (parent rows present before children)
        // are satisfied.
        let copy_start = Instant::now();

        if use_transaction {
            client.execute("BEGIN", &[]).await.map_err(J2sError::Db)?;
        }

        let copy_result = async {
            let mut rows_per_table: HashMap<String, u64> = HashMap::new();
            for name in &topo_order {
                let mut table_total = 0u64;
                for worker_sinks in &mut all_worker_sinks {
                    if let Some(sink) = worker_sinks.remove(name) {
                        let count = sink.row_count;
                        if count > 0 {
                            if let Some(ref tx) = progress_tx {
                                let _ = tx.send(ProgressEvent::Pass2Log(
                                    format!("COPY {} ({} rows, worker)", name, count),
                                ));
                            }
                            let inserted = sink.copy_to_db(client).await?;
                            table_total += inserted;
                        }
                    }
                }
                rows_per_table.insert(name.clone(), table_total);
                if table_total > 0 {
                    if let Some(ref tx) = progress_tx {
                        let _ = tx.send(ProgressEvent::Pass2Flush {
                            table_name: name.clone(),
                            rows_flushed: table_total,
                        });
                    }
                }
            }
            Ok::<_, J2sError>(rows_per_table)
        }
        .await;

        if use_transaction {
            match copy_result {
                Ok(rows_per_table) => {
                    client.execute("COMMIT", &[]).await.map_err(J2sError::Db)?;
                    if let Some(ref tx) = progress_tx {
                        let total_rows: u64 = rows_per_table.values().sum();
                        let _ = tx.send(ProgressEvent::Pass2Done {
                            total_rows,
                            anomaly_count: merged_anomalies.total_anomalies(),
                        });
                    }
                    merged_anomalies.finish()?;
                    let copy_ms = copy_start.elapsed().as_millis() as u64;
                    eprintln!("Pass 2 timing: streaming={streaming_ms}ms, copy={copy_ms}ms, total={}ms", streaming_ms + copy_ms);
                    return Ok(Pass2Result {
                        rows_per_table,
                        anomaly_collector: merged_anomalies,
                        timing: Pass2Timing { streaming_ms, copy_ms },
                    });
                }
                Err(e) => {
                    if let Err(rb_err) = client.execute("ROLLBACK", &[]).await {
                        eprintln!("WARNING: ROLLBACK failed after import error: {rb_err}");
                        eprintln!("         The database may be in an inconsistent state.");
                    }
                    return Err(e);
                }
            }
        }

        let rows_per_table = copy_result?;
        if let Some(ref tx) = progress_tx {
            let total_rows: u64 = rows_per_table.values().sum();
            let _ = tx.send(ProgressEvent::Pass2Done {
                total_rows,
                anomaly_count: merged_anomalies.total_anomalies(),
            });
        }
        merged_anomalies.finish()?;
        let copy_ms = copy_start.elapsed().as_millis() as u64;
        eprintln!("Pass 2 timing: streaming={streaming_ms}ms, copy={copy_ms}ms, total={}ms", streaming_ms + copy_ms);
        return Ok(Pass2Result {
            rows_per_table,
            anomaly_collector: merged_anomalies,
            timing: Pass2Timing { streaming_ms, copy_ms },
        });
    }

    // ---------------------------------------------------------------------------
    // Sequential path (parallel == 1)
    // ---------------------------------------------------------------------------

    // Open a TempFileSink for each table
    let mut sinks: HashMap<String, TempFileSink> = HashMap::new();
    for schema in schemas {
        sinks.insert(schema.name.clone(), TempFileSink::new(schema, pg_schema)?);
    }

    let mut anomalies = AnomalyCollector::new(anomaly_dir);
    let (mut reader, _format) = JsonReader::open(path)?;

    let flush_threshold = flush_threshold as u64;

    let flush_check_interval = if flush_threshold > 0 { (flush_threshold / 100).max(1) } else { 0 };
    let mut flush_check_counter = 0u64;

    const FD_CHECK_INTERVAL: u64 = 1_000;
    let mut fd_check_counter = 0u64;

    let stream_start = Instant::now();
    while let Some(item) = reader.next() {
        let value = item?;
        if let Value::Object(ref obj) = value {
            let row_id = Uuid::now_v7();
            insert_object(
                &path_map,
                &mut sinks,
                &mut anomalies,
                root_schema,
                obj,
                row_id,
                None,
                None,
            )?;
            rows_processed += 1;
            if let Some(ref bar) = progress {
                bar.inc_rows(1);
            }
            if let Some(ref tx) = progress_tx {
                if rows_processed % PROGRESS_INTERVAL == 0 {
                    let _ = tx.send(ProgressEvent::Pass2Progress {
                        rows_processed,
                        bytes_read: reader.bytes_read(),
                        total_bytes,
                    });
                }
            }

            // Periodic flush: when any sink reaches the threshold, flush ALL sinks
            // in topological order (parents before children). This keeps temp-file
            // disk usage bounded while respecting FK constraints.
            if flush_check_interval > 0 {
                flush_check_counter += 1;
                if flush_check_counter >= flush_check_interval {
                    flush_check_counter = 0;
                    if sinks.values().any(|s| s.row_count >= flush_threshold) {
                        for name in &topo_order {
                            if let Some(sink) = sinks.get_mut(name.as_str()) {
                                if sink.row_count > 0 {
                                    let flushed = sink.row_count;
                                    sink.flush_to_db(client).await?;
                                    if let Some(ref tx) = progress_tx {
                                        let _ = tx.send(ProgressEvent::Pass2Flush {
                                            table_name: name.clone(),
                                            rows_flushed: flushed,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // FD guard: flush all open sinks when approaching the OS fd limit.
            fd_check_counter += 1;
            if fd_check_counter >= FD_CHECK_INTERVAL {
                fd_check_counter = 0;
                let open_count = sinks.values().filter(|s| s.is_open()).count();
                if open_count >= MAX_OPEN_TEMP_FILES {
                    let msg = format!(
                        "fd guard: {} temp files open — flushing all sinks to stay within limit",
                        open_count
                    );
                    eprintln!("WARNING: {msg}");
                    if let Some(ref tx) = progress_tx {
                        let _ = tx.send(ProgressEvent::Pass2Log(msg));
                    }
                    for name in &topo_order {
                        if let Some(sink) = sinks.get_mut(name.as_str()) {
                            if sink.row_count > 0 {
                                sink.flush_to_db(client).await?;
                            }
                        }
                    }
                }
            }
        }
    }

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
    eprintln!("Pass 2 streaming done. Flushing remaining rows to PostgreSQL...");

    // Group sinks by depth level (topological order: parents before children).
    // Within each level, tables can be COPYed independently.
    let name_to_depth: HashMap<&str, usize> =
        schemas.iter().map(|s| (s.name.as_str(), s.depth)).collect();
    let mut by_depth: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    for (name, _) in &sinks {
        let depth = name_to_depth.get(name.as_str()).copied().unwrap_or(0);
        by_depth.entry(depth).or_default().push(name.clone());
    }

    let table_names: Vec<String> = by_depth.into_values().flatten().collect();

    if use_transaction {
        client.execute("BEGIN", &[]).await.map_err(J2sError::Db)?;
    }

    let copy_start = Instant::now();

    let copy_result = async {
        let mut rows_per_table = HashMap::new();
        for name in &table_names {
            if let Some(sink) = sinks.remove(name) {
                let count = sink.row_count;
                eprintln!("  COPY {} ({} rows)...", name, count);
                if let Some(ref tx) = progress_tx {
                    let _ = tx.send(ProgressEvent::Pass2Log(
                        format!("COPY {} ({} rows)", name, count)
                    ));
                }
                let inserted = sink.copy_to_db(client).await?;
                rows_per_table.insert(name.clone(), inserted);
                if let Some(ref tx) = progress_tx {
                    let _ = tx.send(ProgressEvent::Pass2Flush {
                        table_name: name.clone(),
                        rows_flushed: count,
                    });
                }
            }
        }
        Ok::<_, J2sError>(rows_per_table)
    }
    .await;

    if use_transaction {
        match copy_result {
            Ok(rows_per_table) => {
                client.execute("COMMIT", &[]).await.map_err(J2sError::Db)?;
                if let Some(ref tx) = progress_tx {
                    let total_rows: u64 = rows_per_table.values().sum();
                    let _ = tx.send(ProgressEvent::Pass2Done {
                        total_rows,
                        anomaly_count: anomalies.total_anomalies(),
                    });
                }
                anomalies.finish()?;
                let copy_ms = copy_start.elapsed().as_millis() as u64;
                eprintln!("Pass 2 timing: streaming={streaming_ms}ms, copy={copy_ms}ms, total={}ms", streaming_ms + copy_ms);
                return Ok(Pass2Result { rows_per_table, anomaly_collector: anomalies, timing: Pass2Timing { streaming_ms, copy_ms } });
            }
            Err(e) => {
                if let Err(rb_err) = client.execute("ROLLBACK", &[]).await {
                    eprintln!("WARNING: ROLLBACK failed after import error: {rb_err}");
                    eprintln!("         The database may be in an inconsistent state.");
                }
                return Err(e);
            }
        }
    }

    let rows_per_table = copy_result?;
    if let Some(ref tx) = progress_tx {
        let total_rows: u64 = rows_per_table.values().sum();
        let _ = tx.send(ProgressEvent::Pass2Done {
            total_rows,
            anomaly_count: anomalies.total_anomalies(),
        });
    }
    anomalies.finish()?;
    let copy_ms = copy_start.elapsed().as_millis() as u64;
    eprintln!("Pass 2 timing: streaming={streaming_ms}ms, copy={copy_ms}ms, total={}ms", streaming_ms + copy_ms);
    Ok(Pass2Result { rows_per_table, anomaly_collector: anomalies, timing: Pass2Timing { streaming_ms, copy_ms } })
}

/// Compute how many workers may execute `insert_object` simultaneously so that
/// peak open temp-file FDs stay within the OS soft limit.
///
/// Each insert can open at most `n_tables` FDs. With `k` concurrent inserts,
/// the peak is `k × n_tables`. We bound this to `MAX_OPEN_TEMP_FILES - BASELINE_FDS`
/// to leave headroom for the PG connection, the JSON reader, stdin/stdout/stderr,
/// and tokio internals.
fn max_concurrent_inserts(n_tables: usize, parallel: usize) -> usize {
    const BASELINE_FDS: usize = 32;
    let safe = MAX_OPEN_TEMP_FILES.saturating_sub(BASELINE_FDS);
    (safe / n_tables.max(1)).max(1).min(parallel)
}

#[cfg(test)]
mod tests {
    use super::*;

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

    /// The peak FD count (max_concurrent × n_tables) must stay within the safe limit.
    #[test]
    fn max_concurrent_peak_fds_within_safe_limit() {
        for &n_tables in &[1usize, 50, 150, 300, 500] {
            for &parallel in &[1usize, 4, 8] {
                let k = max_concurrent_inserts(n_tables, parallel);
                let peak = k * n_tables;
                let safe = MAX_OPEN_TEMP_FILES - 32;
                assert!(
                    peak <= safe,
                    "n_tables={n_tables} parallel={parallel}: peak={peak} > safe={safe}"
                );
            }
        }
    }

    /// At least 1 worker must always be allowed to insert, even with a huge schema.
    #[test]
    fn max_concurrent_at_least_one() {
        assert_eq!(max_concurrent_inserts(10_000, 8), 1);
        assert_eq!(max_concurrent_inserts(usize::MAX / 2, 4), 1);
    }

    /// max_concurrent must never exceed the actual number of parallel workers.
    #[test]
    fn max_concurrent_never_exceeds_parallel() {
        assert_eq!(max_concurrent_inserts(1, 8), 8);
        assert_eq!(max_concurrent_inserts(10, 4), 4);
    }
}
