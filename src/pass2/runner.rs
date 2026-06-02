use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use serde_json::Value;
use simd_json;
use tokio_postgres::Client;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::anomaly::collector::AnomalyCollector;
use crate::db::copy_sink::{copy_snapshot_to_pg, merge_copy_to_db, TempFileSink};
use crate::schema::PATH_SEP;
use crate::db::ddl::{add_constraints, ConstraintWarning};
use crate::error::{J2sError, Result};
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

fn validate_run_params(parallel: usize) -> Result<()> {
    if parallel == 0 {
        return Err(J2sError::InvalidInput(
            "parallel must be >= 1 (0 would produce an empty connection pool)".to_string(),
        ));
    }
    Ok(())
}


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
    temp_dir: Option<PathBuf>,
    progress_tx: Option<ProgressTx>,
    per_worker_budget: Option<u64>,
    min_interim_copy_bytes: Option<u64>,
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
    let worker_budget = per_worker_budget.unwrap_or(PER_WORKER_FLUSH_THRESHOLD);
    let interim_copy_threshold = min_interim_copy_bytes.unwrap_or(MIN_SINK_COPY_BYTES);

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

    // Cancellation token: a DropGuard is held for the lifetime of this async fn.
    let cancel = CancellationToken::new();
    let _cancel_guard = cancel.clone().drop_guard();

    // -------------------------------------------------------------------------
    // Phase A — Parallel streaming.
    // One shared TempFileSink per table (Arc<Mutex>) — 245 files instead of
    // N*245. Workers write to shared sinks under a brief sync lock.
    // When a worker's budget fires, large sinks emit a FlushSnapshot handed to
    // a background COPY task; the sink immediately resets for new data.
    // Workers return anomalies + COPY handles; sinks are unwrapped in Phase B.
    // -------------------------------------------------------------------------
    let (mut reader, _format) = JsonReader::open(path)?;
    let path_map_arc: Arc<HashMap<String, TableSchema>> = Arc::new(path_map);
    let root_schema_arc: Arc<TableSchema> = Arc::new(root_schema.clone());
    // Limits concurrent interim COPYs — HDD benefits from low values (1-2).
    let copy_sem: Arc<tokio::sync::Semaphore> = Arc::new(tokio::sync::Semaphore::new(parallel));

    // One shared sink per table.
    let shared_sinks: HashMap<String, Arc<Mutex<TempFileSink>>> = schemas
        .iter()
        .map(|s| Ok((
            s.name.clone(),
            Arc::new(Mutex::new(TempFileSink::new(s, pg_schema, temp_dir.as_deref())?)),
        )))
        .collect::<Result<_>>()?;

    const CHANNEL_CAP: usize = 256;
    let mut senders: Vec<tokio::sync::mpsc::Sender<Vec<u8>>> =
        Vec::with_capacity(parallel);
    let mut worker_handles = Vec::with_capacity(parallel);

    for _ in 0..parallel {
        let (tx, mut rx) =
            tokio::sync::mpsc::channel::<Vec<u8>>(CHANNEL_CAP);
        senders.push(tx);

        // Each worker gets its own HashMap of Arc clones (cheap — no TempFileSink copy).
        let worker_sinks: HashMap<String, Arc<Mutex<TempFileSink>>> =
            shared_sinks.iter().map(|(k, v)| (k.clone(), Arc::clone(v))).collect();
        let mut worker_anomalies = AnomalyCollector::new(None);
        let pm = path_map_arc.clone();
        let rs = root_schema_arc.clone();
        let cancel_token = cancel.clone();
        let worker_pg_url = pg_url.to_string();
        let worker_ptx = progress_tx.clone();
        let worker_copy_sem = copy_sem.clone();

        let handle = tokio::task::spawn(async move {
            let mut sinks = worker_sinks;
            let mut copy_handles: Vec<tokio::task::JoinHandle<Result<(String, u64)>>> = Vec::new();
            // Budget: proxy via raw JSON bytes (avoids per-object lock on all 245 sinks).
            let mut my_bytes: u64 = 0;

            loop {
                let mut bytes = tokio::select! {
                    _ = cancel_token.cancelled() => break,
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

                my_bytes += obj_len;
                if my_bytes >= worker_budget {
                    my_bytes = 0;
                    for (table_name, sink_arc) in &sinks {
                        let snap = {
                            let mut s = sink_arc.lock().unwrap();
                            if s.bytes_buffered >= interim_copy_threshold {
                                s.take_flush_snapshot()
                            } else {
                                let _ = s.force_spill();
                                None
                            }
                        };
                        if let Some(snap) = snap {
                            let sem = worker_copy_sem.clone();
                            let url = worker_pg_url.clone();
                            let ptx = worker_ptx.clone();
                            let sink_arc2 = Arc::clone(sink_arc);
                            let tname = table_name.clone();
                            copy_handles.push(tokio::spawn(async move {
                                let _permit = sem.acquire_owned().await.expect("semaphore closed");
                                let conn = crate::db::connection::connect(&url).await?;
                                conn.execute("SET synchronous_commit = off", &[]).await
                                    .map_err(crate::error::J2sError::Db)?;
                                let rows = copy_snapshot_to_pg(snap, &conn).await?;
                                sink_arc2.lock().unwrap().apply_flush(rows);
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
            }

            // Flush remaining in-memory pending to disk — Phase B will COPY it.
            for (_, sink_arc) in &sinks {
                let _ = sink_arc.lock().unwrap().force_spill();
            }
            Ok::<_, J2sError>((worker_anomalies, copy_handles))
        });
        worker_handles.push(handle);
    }

    // -------------------------------------------------------------------------
    // (nothing here — flush pool removed, Phase B COPY happens after workers join)
    // -------------------------------------------------------------------------

    let stream_start = Instant::now();
    let mut robin = 0usize;
    let mut worker_died = false;
    'dispatch: while let Some(item) = reader.next_raw() {
        let bytes = item?;
        if senders[robin].send(bytes).await.is_err() {
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

    // Join all workers and collect background COPY handles.
    let mut merged_anomalies = AnomalyCollector::new(anomaly_dir);
    let mut all_copy_handles: Vec<tokio::task::JoinHandle<Result<(String, u64)>>> = Vec::new();
    let mut first_worker_error: Option<J2sError> = None;
    for handle in worker_handles {
        match handle.await {
            Ok(Ok((w_anomalies, copy_handles))) => {
                merged_anomalies.merge(w_anomalies);
                all_copy_handles.extend(copy_handles);
            }
            Ok(Err(e)) => {
                if first_worker_error.is_none() { first_worker_error = Some(e); }
            }
            Err(e) => {
                if first_worker_error.is_none() {
                    first_worker_error = Some(J2sError::InvalidInput(format!("worker panic: {}", e)));
                }
            }
        }
    }
    if let Some(err) = first_worker_error { return Err(err); }
    if worker_died {
        return Err(J2sError::InvalidInput("worker channel closed unexpectedly".into()));
    }

    // Await all background interim COPY tasks spawned during Phase A.
    let mut interim_rows: HashMap<String, u64> = HashMap::new();
    for handle in all_copy_handles {
        match handle.await {
            Ok(Ok((table_name, rows))) => {
                *interim_rows.entry(table_name).or_insert(0) += rows;
            }
            Ok(Err(e)) => {
                if first_worker_error.is_none() { first_worker_error = Some(e); }
            }
            Err(e) => {
                if first_worker_error.is_none() {
                    first_worker_error = Some(J2sError::InvalidInput(format!("copy task panic: {e}")));
                }
            }
        }
    }
    if let Some(err) = first_worker_error { return Err(err); }

    // -------------------------------------------------------------------------
    // Phase B — COPY post-streaming: parallel on `parallel` PG connections.
    // Workers and all background COPYs are done — unwrap shared sinks (one per
    // table) and distribute round-robin across connection tasks.
    // -------------------------------------------------------------------------
    let copy_start = Instant::now();

    // Unwrap Arc<Mutex<TempFileSink>> — workers are done, no other references.
    let mut all_sinks: Vec<(String, TempFileSink)> = shared_sinks
        .into_iter()
        .filter_map(|(name, arc)| {
            let sink = Arc::try_unwrap(arc).ok()?.into_inner().ok()?;
            if sink.row_count > 0 || sink.total_flushed > 0 { Some((name, sink)) } else { None }
        })
        .collect();
    all_sinks.sort_by(|a, b| a.0.cmp(&b.0));

    let mut table_batches: Vec<Vec<(String, Vec<TempFileSink>)>> =
        (0..parallel).map(|_| Vec::new()).collect();
    for (i, (table_name, sink)) in all_sinks.drain(..).enumerate() {
        table_batches[i % parallel].push((table_name, vec![sink]));
    }

    // Spawn one task per PG connection, each processing its batch of tables.
    let mut copy_handles: Vec<tokio::task::JoinHandle<Result<Vec<(String, u64)>>>> =
        Vec::with_capacity(parallel);
    for batch in table_batches {
        if batch.is_empty() { continue; }
        let url = pg_url.to_string();
        let ptx = progress_tx.clone();
        copy_handles.push(tokio::task::spawn(async move {
            use crate::db::connection::connect;
            let conn = connect(&url).await?;
            conn.execute("SET synchronous_commit = off", &[]).await
                .map_err(crate::error::J2sError::Db)?;
            let mut results = Vec::new();
            for (table_name, sinks) in batch {
                let rows = merge_copy_to_db(sinks, &conn).await?;
                if let Some(ref tx) = ptx {
                    let _ = tx.send(ProgressEvent::Pass2Flush {
                        table_name: table_name.clone(),
                        rows_flushed: rows,
                    });
                }
                results.push((table_name, rows));
            }
            Ok(results)
        }));
    }

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
    // Add rows sent via background interim COPYs during Phase A.
    for (name, rows) in interim_rows {
        *rows_per_table.entry(name).or_insert(0) += rows;
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
    use super::{Pass2Timing, validate_run_params};
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

}
