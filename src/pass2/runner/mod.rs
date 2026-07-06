//! Pass 2 — data insertion into `PostgreSQL` via a diskless pipeline.
//!
//! N workers stream the JSON round-robin into local `MemSink` buffers. A concurrent
//! flusher task drains those buffers to `PostgreSQL` via `COPY FROM STDIN`, eliminating
//! the need for temp files. The main entry point is `run`.
//!
//! Fonctions :
//! - struct `Pass2Timing` — répartition du temps d'exécution entre streaming et phase COPY.
//! - fn `Pass2Timing::total_ms` — somme `streaming_ms` + `copy_ms`.
//! - struct `Pass2Result` — résumé du résultat de Pass 2 (lignes par table, anomalies, warnings, timing).
//!
//! Flusher (draine les buffers vers PG) :
//! - fn `find_all_nonempty_buffers` — sélectionne tous les buffers non vides à drainer.
//! - fn `topological_drain_order` — ordonne les buffers à drainer selon l'ordre topologique.
//! - fn `ram_used_ratio` — ratio RAM utilisée / RAM totale.
//! - fn `format_flusher_pause_log` — formate le log de pause du flusher (pression RAM haute).
//! - fn `format_flusher_resume_log` — formate le log de reprise du flusher (pression RAM basse).
//! - fn `format_copy_start_log` — formate le log de début de COPY pour une table.
//! - fn `format_copy_done_log` — formate le log de fin de COPY pour une table.
//! - fn `find_largest_buffer` — sélectionne le plus gros buffer non vide.
//! - fn `flush_table_to_pg` — flush une table vers `PostgreSQL` via COPY.
//! - fn `run_flusher` — boucle principale : draine les buffers workers vers PG selon la pression RAM.
//!
//! Config & validation :
//! - struct `Pass2Config` — tous les paramètres contrôlant un run de Pass 2.
//! - fn `effective_worker_threshold` — répartit le seuil de flush entre workers.
//! - fn `validate_run_params` — valide les paramètres d'entrée (ex. parallel > 0).
//! - fn `validate_watermarks` — valide les seuils RAM haut/bas et le seuil de flush.
//! - fn `try_set_synchronous_commit_off` — désactive `synchronous_commit` (best-effort).
//!
//! Worker (ingestion diskless) :
//! - fn `collect_above_threshold` — sinks prêts à flush (au-dessus du seuil par worker).
//! - fn `process_worker_item_diskless` — traite un item JSON dans un worker.
//! - struct `WorkerDisklessConfig` — configuration passée à `run_worker_diskless`.
//! - fn `run_worker_diskless` — boucle d'un worker (parse, insert, flush si seuil dépassé).
//! - fn `parse_json_object` — parse un objet JSON racine.
//!
//! Dispatch & progression :
//! - fn `spawn_anomaly_writer` — tâche d'écriture des anomalies.
//! - fn `preflight_warn_nonempty` — avertit si des tables racines contiennent déjà des lignes.
//! - fn `finalize_dispatch` — mise à jour finale de la progress bar en fin de dispatch.
//! - fn `emit_completion_events` — envoie les événements de fin de run.
//! - fn `log_constraint_warnings` — logue les warnings de contraintes FK non appliquées.
//! - fn `update_row_progress` — mise à jour périodique de la progress bar.
//! - fn `dispatch_loop` — répartit le flux JSON vers les workers.
//!
//! Orchestration :
//! - fn `find_root_schema` — résout le schéma racine.
//! - fn `schema_topo_order` — ordre topologique parents→enfants.
//! - fn `build_copy_sql_map` — construit les requêtes COPY par table.
//! - fn `run` — pipeline complet (dispatch → workers → flusher → contraintes).
//! - fn `build_pass2_result` — assemble le résultat final.

use std::collections::HashMap;
use std::path::Path;
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

mod config;
pub use config::Pass2Config;
use config::{DEFAULT_RAM_HIGH_WATERMARK, DEFAULT_RAM_LOW_WATERMARK};

mod flusher;

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

struct WorkerDisklessConfig {
    anomaly_tx: tokio::sync::mpsc::UnboundedSender<AnomalyEvent>,
    path_map: Arc<HashMap<String, TableSchema>>,
    root_schema: Arc<TableSchema>,
    cancel: CancellationToken,
    flush_tx: tokio::sync::mpsc::Sender<(String, bytes::Bytes, u64)>,
    pause_flag: Arc<std::sync::atomic::AtomicBool>,
    error_flag: Arc<std::sync::atomic::AtomicBool>,
    mem_flush_threshold: u64,
}

/// Diskless worker: local MemSink accumulation, sends batches to `run_flusher` via channel.
/// Checks `error_flag` each iteration (flusher fatal error) and yields on `pause_flag`
/// (flusher buffer pressure). Workers spin without draining — flusher controls when to resume.
/// For wrapper-format files, `key` carries the raw wrapper key and the worker resolves the
/// correct root schema via `path_map`; for regular formats `key` is `None` and `root_schema`
/// is used directly.
async fn run_worker_diskless(
    mut rx: tokio::sync::mpsc::Receiver<(Option<String>, Vec<u8>)>,
    mut sinks: HashMap<String, crate::db::copy_sink::MemSink>,
    config: WorkerDisklessConfig,
) -> Result<()> {
    use std::sync::atomic::Ordering;
    let WorkerDisklessConfig { anomaly_tx, path_map, root_schema, cancel, flush_tx, pause_flag, error_flag, mem_flush_threshold } = config;
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
        let (key, bytes) = tokio::select! {
            () = cancel.cancelled() => break,
            msg = rx.recv() => match msg { Some(b) => b, None => break },
        };
        let effective_root: &TableSchema = key.as_deref()
            .and_then(|k| path_map.get(k))
            .unwrap_or(&root_schema);
        process_worker_item_diskless(bytes, &mut sinks, &mut proxy, &flush_tx, &path_map, effective_root, mem_flush_threshold).await?;
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
/// Each payload carries the current wrapper key (None for non-wrapper formats).
/// Returns `(rows_processed, worker_died)`.
async fn dispatch_loop(
    reader: &mut JsonReader,
    senders: &[tokio::sync::mpsc::Sender<(Option<String>, Vec<u8>)>],
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
            let key = reader.current_key().map(str::to_string);
            if senders[robin].send((key, bytes)).await.is_err() {
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
/// Returns schema names in topological order (parents before children), using BFS from roots.
/// Tables absent from the schemas list but present in buffers are appended after in arbitrary order.
fn schema_topo_order(schemas: &[TableSchema]) -> Vec<String> {
    let mut children_of: HashMap<String, Vec<String>> = HashMap::new();
    let mut roots: Vec<String> = vec![];
    for s in schemas {
        match &s.parent_table {
            None => roots.push(s.name.clone()),
            Some(parent) if parent == &s.name => roots.push(s.name.clone()),
            Some(parent) => children_of.entry(parent.clone()).or_default().push(s.name.clone()),
        }
    }
    let mut result = Vec::with_capacity(schemas.len());
    let mut queue = std::collections::VecDeque::from(roots);
    let mut seen = std::collections::HashSet::new();
    while let Some(name) = queue.pop_front() {
        if seen.insert(name.clone()) {
            result.push(name.clone());
            if let Some(children) = children_of.get(&name) {
                for child in children { queue.push_back(child.clone()); }
            }
        }
    }
    result
}

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
    config::validate_run_params(config.parallel)?;

    let mem_flush_threshold = config.mem_flush_threshold_bytes.unwrap_or(64 * 1024 * 1024);
    let ram_high = config.ram_high_watermark.unwrap_or(DEFAULT_RAM_HIGH_WATERMARK);
    let ram_low = config.ram_low_watermark.unwrap_or(DEFAULT_RAM_LOW_WATERMARK);
    let verbose = config.verbose;
    config::validate_watermarks(ram_high, ram_low, mem_flush_threshold)?;

    let parallel = config.parallel.max(1);
    let total_bytes = file_size(path)?;
    let progress = progress_tx.is_none().then(|| ProgressTracker::new(total_bytes, "Pass 2"));

    let sep = PATH_SEP.to_string();
    let path_map: HashMap<String, TableSchema> = schemas.iter()
        .map(|s| (s.path.join(&sep), s.clone()))
        .collect();

    let (mut reader, format) = if let Some(known) = config.hint_format.clone() {
        let reader = JsonReader::open_with_format(path, known.clone())?;
        (reader, known)
    } else {
        if let Some(ref tx) = progress_tx {
            let _ = tx.send(ProgressEvent::Pass2Log("Detecting JSON format…".to_string()));
        }
        JsonReader::open(path)?
    };
    // For wrapper format, root table names come from wrapper keys (raw, as stored in s.path),
    // not from config.root_table (the filename-derived fallback).
    let effective_root_table = if let crate::io::reader::JsonFormat::RootWrapper(ref keys) = format {
        keys.first().cloned().unwrap_or_else(|| config.root_table.clone())
    } else {
        config.root_table.clone()
    };
    let root_schema = find_root_schema(schemas, &effective_root_table, &sep)?;

    if let Some(ref dir) = config.anomaly_dir {
        std::fs::create_dir_all(dir).map_err(J2sError::Io)?;
    }
    let (anomaly_tx, anomaly_writer_handle) = spawn_anomaly_writer(config.anomaly_dir.clone());
    preflight_warn_nonempty(schemas, client, &config.pg_schema, progress_tx.as_ref()).await;

    let cancel = CancellationToken::new();
    let _cancel_guard = cancel.clone().drop_guard();

    let copy_sql_map = build_copy_sql_map(schemas, &config.pg_schema);
    let topo_order = schema_topo_order(schemas);
    let (flush_tx, flush_rx) = tokio::sync::mpsc::channel::<(String, bytes::Bytes, u64)>(256);
    let pause_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let error_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flusher_handle = tokio::spawn(flusher::run_flusher(
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
        topo_order,
    ));

    let path_map_arc: Arc<HashMap<String, TableSchema>> = Arc::new(path_map);
    let root_schema_arc: Arc<TableSchema> = Arc::new(root_schema);

    const WORKER_CHANNEL_CAP: usize = 256;
    let mut senders = Vec::with_capacity(parallel);
    let mut worker_handles: Vec<tokio::task::JoinHandle<Result<()>>> = Vec::with_capacity(parallel);
    for _ in 0..parallel {
        let (tx, rx) = tokio::sync::mpsc::channel::<(Option<String>, Vec<u8>)>(WORKER_CHANNEL_CAP);
        senders.push(tx);
        let worker_sinks: HashMap<String, crate::db::copy_sink::MemSink> = schemas.iter()
            .map(|s| (s.name.clone(), crate::db::copy_sink::MemSink::new(s, &config.pg_schema)))
            .collect();
        worker_handles.push(tokio::task::spawn(run_worker_diskless(
            rx,
            worker_sinks,
            WorkerDisklessConfig {
                anomaly_tx: anomaly_tx.clone(),
                path_map: path_map_arc.clone(),
                root_schema: root_schema_arc.clone(),
                cancel: cancel.clone(),
                flush_tx: flush_tx.clone(),
                pause_flag: Arc::clone(&pause_flag),
                error_flag: Arc::clone(&error_flag),
                mem_flush_threshold: config::effective_worker_threshold(mem_flush_threshold, parallel),
            },
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
    let constraint_warnings = if config.skip_constraints {
        eprintln!("Constraints skipped (skip_constraints = true).");
        vec![]
    } else {
        let warnings = add_constraints(pg_url, schemas, &config.pg_schema, parallel, progress_tx.as_ref()).await?;
        log_constraint_warnings(&warnings, progress_tx.as_ref());
        warnings
    };

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
    use super::Pass2Timing;
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
        let (_item_tx, item_rx) = tokio::sync::mpsc::channel::<(Option<String>, Vec<u8>)>(1);
        let cancel = tokio_util::sync::CancellationToken::new();
        let error_flag = Arc::new(AtomicBool::new(false)); // starts false
        let pause_flag = Arc::new(AtomicBool::new(true));  // starts true → worker enters spin

        let ef = Arc::clone(&error_flag);
        let worker_handle = tokio::spawn(super::run_worker_diskless(
            item_rx,
            HashMap::new(),
            super::WorkerDisklessConfig {
                anomaly_tx,
                path_map,
                root_schema,
                cancel,
                flush_tx,
                pause_flag,
                error_flag: Arc::clone(&error_flag),
                mem_flush_threshold: 64 * 1024 * 1024,
            },
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
        let (_item_tx, item_rx) = tokio::sync::mpsc::channel::<(Option<String>, Vec<u8>)>(1);
        let cancel = tokio_util::sync::CancellationToken::new();
        let error_flag = Arc::new(AtomicBool::new(true));
        let pause_flag = Arc::new(AtomicBool::new(false));

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            super::run_worker_diskless(
                item_rx,
                HashMap::new(),
                super::WorkerDisklessConfig {
                    anomaly_tx,
                    path_map,
                    root_schema,
                    cancel,
                    flush_tx,
                    pause_flag,
                    error_flag,
                    mem_flush_threshold: 64 * 1024 * 1024,
                },
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

    // -------------------------------------------------------------------------
    // Pass2Log detection event test
    // -------------------------------------------------------------------------

    #[test]
    fn pass2_log_detecting_format_event_reaches_channel() {
        use crate::io::progress_event::{ProgressEvent, ProgressTx};

        let (tx, mut rx): (ProgressTx, _) = tokio::sync::mpsc::unbounded_channel();
        let _ = tx.send(ProgressEvent::Pass2Log("Detecting JSON format…".to_string()));
        let event = rx.try_recv().expect("Pass2Log event must be in the channel");
        match event {
            ProgressEvent::Pass2Log(msg) => {
                assert!(msg.contains("Detecting"), "message must mention detection");
            }
            other => panic!("unexpected event variant: {other:?}"),
        }
    }

}
