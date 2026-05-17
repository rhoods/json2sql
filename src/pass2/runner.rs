use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use serde_json::Value;
use tokio_postgres::Client;
use uuid::Uuid;

use crate::anomaly::collector::AnomalyCollector;
use crate::db::copy_sink::{TempFileSink, INTERIM_FLUSH_THRESHOLD, MAX_OPEN_TEMP_FILES};
use crate::schema::PATH_SEP;
use crate::db::ddl::{add_constraints, ConstraintWarning};
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
    /// Time spent in the COPY-to-PostgreSQL phase (including constraints).
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
    /// FK constraints that could not be applied after loading data.
    /// PK failures are fatal (returned as Err); only FK failures appear here.
    pub constraint_warnings: Vec<ConstraintWarning>,
    pub timing: Pass2Timing,
}

/// Fraction of `MAX_OPEN_TEMP_FILES` that all workers combined may hold open
/// simultaneously. Conservative to leave headroom for PG connection, reader,
/// and process descriptors.
const FD_GLOBAL_THRESHOLD: usize = MAX_OPEN_TEMP_FILES * 9 / 10; // 855 with default 950

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

/// Run Pass 2: stream the file into per-worker temp-file buffers, COPY to
/// PostgreSQL, then add PRIMARY KEY and FOREIGN KEY constraints.
///
/// **The caller is responsible for creating tables** (without constraints)
/// via `db::ddl::create_tables_no_constraints()` before calling this function.
///
/// Internal phases:
///   B — N workers (parallel ≥ 1) stream root objects round-robin into
///       per-table `TempFileSink` buffers. File descriptors are managed via a
///       per-worker budget; sinks are hibernated when the budget is exceeded.
///   C — All temp files are COPYed to PostgreSQL in schema order.
///       No FK constraints are active yet, so any table order is safe.
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
) -> Result<Pass2Result> {
    let total_bytes = file_size(path)?;
    let progress = if progress_tx.is_none() {
        Some(ProgressTracker::new(total_bytes, "Pass 2"))
    } else {
        None
    };
    let mut rows_processed = 0u64;
    const PROGRESS_INTERVAL: u64 = 1_000;

    let sep = PATH_SEP.to_string();
    let path_map: HashMap<String, TableSchema> =
        schemas.iter().map(|s| (s.path.join(&sep), s.clone())).collect();

    let root_schema = schemas
        .iter()
        .find(|s| s.path.join(&sep) == root_table)
        .ok_or_else(|| J2sError::Schema(format!("Root table '{}' not found", root_table)))?;

    let table_names: Vec<String> = schemas.iter().map(|s| s.name.clone()).collect();

    // Keyed by table name so workers can create replacement sinks after interim flushes.
    let schema_by_name: Arc<HashMap<String, TableSchema>> = Arc::new(
        schemas.iter().map(|s| (s.name.clone(), s.clone())).collect(),
    );

    if let Some(ref dir) = anomaly_dir {
        std::fs::create_dir_all(dir).map_err(J2sError::Io)?;
    }

    let parallel = parallel.max(1);

    // -------------------------------------------------------------------------
    // Phase B — Parallel streaming
    // N workers each hold their own HashMap<table_name, TempFileSink>.
    // Root objects are dispatched round-robin from the main task.
    // -------------------------------------------------------------------------
    let (mut reader, _format) = JsonReader::open(path)?;
    let path_map_arc: Arc<HashMap<String, TableSchema>> = Arc::new(path_map);
    let root_schema_arc: Arc<TableSchema> = Arc::new(root_schema.clone());

    const CHANNEL_CAP: usize = 256;
    let mut senders: Vec<tokio::sync::mpsc::Sender<serde_json::Map<String, Value>>> =
        Vec::with_capacity(parallel);
    let mut worker_handles = Vec::with_capacity(parallel);

    // Per-worker FD ceiling: each worker hibernates when it holds this many
    // open sinks. Divided across workers with 10% global headroom.
    let fd_budget_per_worker = (FD_GLOBAL_THRESHOLD / parallel).max(64);

    // Global FD counter shared across all workers.
    // Workers update this after every insert so every worker has visibility into
    // total process FD pressure, not just its own slice.
    let global_open_fds = Arc::new(AtomicUsize::new(0));

    // Interim-flush channel: workers send over-threshold sinks to the main task
    // for COPY without blocking. Main task drains this with try_recv between
    // dispatches. Workers replace the sent sink with a fresh empty one.
    let (flush_tx, mut flush_rx) =
        tokio::sync::mpsc::unbounded_channel::<(String, TempFileSink)>();

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
        let global = Arc::clone(&global_open_fds);
        let ftx = flush_tx.clone();
        let sbn = schema_by_name.clone();
        let pg_schema_owned = pg_schema.to_string();

        let handle = tokio::task::spawn(async move {
            let mut sinks = worker_sinks;
            // FDs this worker currently holds open, reflected in `global`.
            let mut my_open: usize = 0;

            while let Some(obj) = rx.recv().await {
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

                // Interim-flush check: if this worker's total buffered bytes exceed
                // the threshold, hand off the largest sink to the main task for a
                // COPY and replace it with a fresh empty sink. Non-blocking — the
                // main task drains flush_rx between dispatches.
                let total_bytes: u64 = sinks.values().map(|s| s.bytes_buffered).sum();
                if total_bytes > INTERIM_FLUSH_THRESHOLD {
                    if let Some(name) = sinks
                        .iter()
                        .filter(|(_, s)| s.bytes_buffered > 0)
                        .max_by_key(|(_, s)| s.bytes_buffered)
                        .map(|(k, _)| k.clone())
                    {
                        if let Some(old_sink) = sinks.remove(&name) {
                            // Adjust FD tracking before removing the sink.
                            if old_sink.is_open() {
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
                                        // Can't create replacement; put old sink back.
                                        sinks.insert(name, old_sink);
                                    }
                                }
                            } else {
                                sinks.insert(name, old_sink);
                            }
                        }
                    }
                }
            }

            // Release this worker's FDs from the global counter on exit.
            global_sub(&global, my_open);
            Ok::<_, J2sError>((sinks, worker_anomalies))
        });
        worker_handles.push(handle);
    }

    // Drop the main-task's sender clone; only the per-worker clones remain.
    drop(flush_tx);

    let stream_start = Instant::now();
    let mut robin = 0usize;
    let mut worker_died = false;
    'dispatch: while let Some(item) = reader.next() {
        let value = item?;
        if let Value::Object(obj) = value {
            if senders[robin].send(obj).await.is_err() {
                // A worker exited early; collect the real error below.
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
        // Drain any interim flush requests sent by workers.
        while let Ok((table_name, mut sink)) = flush_rx.try_recv() {
            eprintln!("  Interim flush: {} ({} rows)...", table_name, sink.row_count);
            if let Some(ref tx) = progress_tx {
                let _ = tx.send(ProgressEvent::Pass2Log(format!(
                    "Interim flush {} ({} rows)",
                    table_name, sink.row_count
                )));
            }
            sink.flush_to_db(client).await?;
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
    let mut all_worker_sinks: Vec<HashMap<String, TempFileSink>> = Vec::with_capacity(parallel);
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

    // Drain any flush requests that workers sent near end-of-stream but that
    // the dispatch loop didn't get a chance to process.
    while let Ok((table_name, mut sink)) = flush_rx.try_recv() {
        eprintln!("  Post-stream flush: {} ({} rows)...", table_name, sink.row_count);
        sink.flush_to_db(client).await?;
    }

    // -------------------------------------------------------------------------
    // Phase C — Parallel COPY to PostgreSQL
    // No FK constraints are active yet so tables are fully independent.
    // Distribute tables round-robin across `parallel` connections; each
    // connection runs its COPYs sequentially within its own batch.
    // -------------------------------------------------------------------------
    let copy_start = Instant::now();

    // Build per-connection batches: Vec<(table_name, Vec<TempFileSink>)>
    let n_conns = parallel.min(table_names.len().max(1));
    let name_batches = batch_table_names(&table_names, n_conns);

    let mut conn_batches: Vec<Vec<(String, Vec<TempFileSink>)>> =
        name_batches.into_iter().map(|names| {
            names.into_iter().map(|name| {
                let sinks: Vec<TempFileSink> = all_worker_sinks
                    .iter_mut()
                    .filter_map(|ws| ws.remove(&name))
                    .collect();
                (name, sinks)
            }).collect()
        }).collect();

    let mut copy_handles = Vec::with_capacity(n_conns);
    for batch in conn_batches.drain(..) {
        let pg_url_owned = pg_url.to_string();
        let pg_schema_owned = pg_schema.to_string();
        let ptx = progress_tx.clone();
        let handle = tokio::task::spawn(async move {
            use crate::db::connection::connect;
            let conn = connect(&pg_url_owned).await?;
            let mut batch_rows: HashMap<String, u64> = HashMap::new();
            for (table_name, sinks) in batch {
                let mut table_total = 0u64;
                for sink in sinks {
                    if sink.row_count > 0 || sink.total_flushed > 0 {
                        if let Some(ref tx) = ptx {
                            let _ = tx.send(ProgressEvent::Pass2Log(format!(
                                "COPY {} ({} rows)",
                                table_name, sink.row_count
                            )));
                        }
                        eprintln!("  COPY {} ({} rows)...", table_name, sink.row_count);
                        let inserted = sink.copy_to_db(&conn).await?;
                        table_total += inserted;
                    }
                }
                batch_rows.insert(table_name, table_total);
            }
            Ok::<_, J2sError>(batch_rows)
        });
        copy_handles.push(handle);
    }

    let mut rows_per_table: HashMap<String, u64> = HashMap::new();
    for handle in copy_handles {
        match handle.await {
            Ok(Ok(batch_rows)) => {
                for (name, count) in batch_rows {
                    if count > 0 {
                        if let Some(ref tx) = progress_tx {
                            let _ = tx.send(ProgressEvent::Pass2Flush {
                                table_name: name.clone(),
                                rows_flushed: count,
                            });
                        }
                    }
                    rows_per_table.insert(name, count);
                }
            }
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(J2sError::InvalidInput(format!("COPY task panic: {e}"))),
        }
    }

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

/// Distribute `names` across `n` buckets round-robin.
/// Used to assign tables to parallel COPY connections in Phase C.
fn batch_table_names(names: &[String], n: usize) -> Vec<Vec<String>> {
    let n = n.max(1);
    let mut batches = vec![Vec::new(); n];
    for (i, name) in names.iter().enumerate() {
        batches[i % n].push(name.clone());
    }
    batches
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{batch_table_names, global_sub, Pass2Timing};

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
    fn batch_table_names_distributes_round_robin() {
        let names: Vec<String> = (0..8).map(|i| format!("t{i}")).collect();
        let batches = batch_table_names(&names, 3);
        assert_eq!(batches.len(), 3);
        // Round-robin: bucket 0 gets t0,t3,t6 ; bucket 1 gets t1,t4,t7 ; bucket 2 gets t2,t5
        assert_eq!(batches[0], ["t0", "t3", "t6"]);
        assert_eq!(batches[1], ["t1", "t4", "t7"]);
        assert_eq!(batches[2], ["t2", "t5"]);
        let total: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(total, 8);
    }

    #[test]
    fn batch_table_names_fewer_tables_than_connections() {
        let names: Vec<String> = vec!["t0".into(), "t1".into()];
        let batches = batch_table_names(&names, 8);
        assert_eq!(batches.len(), 8);
        let total: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(total, 2, "no tables must be lost");
        // Each of the first 2 buckets has 1 table; remaining 6 are empty
        assert_eq!(batches[0], ["t0"]);
        assert_eq!(batches[1], ["t1"]);
        for empty in &batches[2..] {
            assert!(empty.is_empty());
        }
    }

    #[test]
    fn batch_table_names_n_zero_treated_as_one() {
        let names: Vec<String> = vec!["t0".into(), "t1".into()];
        let batches = batch_table_names(&names, 0);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0], ["t0", "t1"]);
    }
}
