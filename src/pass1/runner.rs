use std::path::Path;
use std::sync::{Arc, Mutex};

use serde_json::Value;
use simd_json;

use crate::error::Result;
use crate::io::progress::ProgressTracker;
use crate::io::progress_event::{ProgressEvent, ProgressTx};
use crate::io::reader::{file_size, JsonReader};
use crate::schema::naming::{ColumnCollision, TruncatedName};
use crate::schema::registry::{apply_column_limit_guard, OverflowWarning, SchemaRegistry};
use crate::schema::stats::ColumnStats;
use crate::schema::table_schema::TableSchema;

/// Result of Pass 1.
pub struct Pass1Result {
    pub schemas: Vec<TableSchema>,
    pub total_rows: u64,
    pub stats: Vec<ColumnStats>,
    /// Table names that were truncated to fit the 63-byte PostgreSQL limit.
    pub truncated_names: Vec<TruncatedName>,
    /// Column name collisions resolved by hash suffix (multiple JSON fields → same SQL identifier).
    pub column_collisions: Vec<ColumnCollision>,
    /// Tables auto-converted to JSONB because they exceeded PostgreSQL's 1600-column limit.
    pub overflow_warnings: Vec<OverflowWarning>,
}

/// Run Pass 1: stream through the entire file and build the schema.
/// Returns finalized table schemas sorted topologically.
///
/// `progress_tx` — optional channel for streaming progress to the IHM.
/// Pass `None` for CLI / headless mode (terminal progress bar is used instead).
pub fn run(
    path: &Path,
    root_table: &str,
    text_threshold: u32,
    array_as_pg_array: bool,
    wide_column_threshold: usize,
    sibling_threshold: usize,
    sibling_jaccard: f64,
    stable_threshold: f64,
    rare_threshold: f64,
    progress_tx: Option<ProgressTx>,
) -> Result<Pass1Result> {
    let total_bytes = file_size(path)?;
    // Terminal progress bar: used only in CLI mode (when no IHM channel provided).
    let progress = if progress_tx.is_none() {
        Some(ProgressTracker::new(total_bytes, "Pass 1"))
    } else {
        None
    };

    let mut registry = SchemaRegistry::new(text_threshold, array_as_pg_array, wide_column_threshold, sibling_threshold, sibling_jaccard, stable_threshold, rare_threshold);
    let (mut reader, _format) = JsonReader::open(path)?;

    let mut total_rows = 0u64;
    // Emit a progress event every 1000 rows to keep the channel lean.
    const PROGRESS_INTERVAL: u64 = 1_000;

    while let Some(item) = reader.next() {
        let value = item?;
        match value {
            Value::Object(ref obj) => {
                registry.observe_root(root_table, obj);
                total_rows += 1;
            }
            other => {
                return Err(crate::error::J2sError::InvalidInput(format!(
                    "Expected JSON object at root level, found: {}",
                    other
                )));
            }
        }

        if let Some(ref bar) = progress {
            bar.inc_rows(1);
            bar.set_bytes(reader.bytes_read());
        }

        if let Some(ref tx) = progress_tx {
            if total_rows % PROGRESS_INTERVAL == 0 {
                let _ = tx.send(ProgressEvent::Pass1Progress {
                    rows_scanned: total_rows,
                    bytes_read: reader.bytes_read(),
                    total_bytes,
                });
            }
        }
    }

    if let Some(ref tx) = progress_tx {
        if total_rows > 0 && total_rows % PROGRESS_INTERVAL != 0 {
            let _ = tx.send(ProgressEvent::Pass1Progress {
                rows_scanned: total_rows,
                bytes_read: reader.bytes_read(),
                total_bytes,
            });
        }
    }

    if let Some(ref bar) = progress {
        bar.finish();
    }
    eprintln!("Pass 1 complete: {} rows, building schema...", total_rows);

    let mut schemas = registry.finalize();
    let overflow_warnings = apply_column_limit_guard(&mut schemas);
    let stats = registry.collect_stats();
    let truncated_names = registry.truncated_names().to_vec();
    let column_collisions = registry.column_collisions().to_vec();

    let tables_count = schemas.len();
    let columns_count = schemas.iter().map(|s| s.columns.len()).sum::<usize>();
    eprintln!("Schema: {} tables, {} total columns", tables_count, columns_count);

    if let Some(ref tx) = progress_tx {
        let _ = tx.send(ProgressEvent::Pass1Done { total_rows, tables_count, columns_count });
    }

    Ok(Pass1Result { schemas, total_rows, stats, truncated_names, column_collisions, overflow_warnings })
}

/// Result of an inspect run (raw schema, no strategies or guards applied).
pub struct InspectResult {
    pub schemas: Vec<TableSchema>,
    pub rows_scanned: u64,
    pub anomaly_count: usize,
    /// The raw JSON objects that were scanned (up to `limit`), in order.
    pub sampled_objects: Vec<Value>,
}

/// Run a lightweight schema inspection on the first `limit` objects of a JSON file.
///
/// Unlike `run`, this function:
/// - Stops after `limit` root objects (no full-file scan required)
/// - Does NOT apply `apply_column_limit_guard`
/// - Does NOT apply wide-table strategies, sibling merging, or any overrides
/// - Disables sibling detection and wide-table heuristics (thresholds set to usize::MAX / 0)
///
/// Useful for quickly understanding the structure of a large file before a full import.
pub fn run_inspect(
    path: &std::path::Path,
    root_table: &str,
    text_threshold: u32,
    limit: usize,
) -> Result<InspectResult> {
    let mut registry = SchemaRegistry::new(
        text_threshold,
        false,        // array_as_pg_array
        usize::MAX,   // wide_column_threshold: disable wide detection
        usize::MAX,   // sibling_threshold: disable sibling merging
        1.0,          // sibling_jaccard: disable sibling merging
        0.0,          // stable_threshold: irrelevant (wide detection disabled)
        0.0,          // rare_threshold: irrelevant (wide detection disabled)
    );

    let (mut reader, _format) = JsonReader::open(path)?;
    let mut rows_scanned = 0u64;
    let mut sampled_objects: Vec<Value> = Vec::new();

    while let Some(item) = reader.next() {
        if rows_scanned >= limit as u64 {
            break;
        }
        let value = item?;
        match value {
            Value::Object(ref obj) => {
                registry.observe_root(root_table, obj);
                rows_scanned += 1;
                sampled_objects.push(Value::Object(obj.clone()));
            }
            other => {
                return Err(crate::error::J2sError::InvalidInput(format!(
                    "Expected JSON object at root level, found: {}",
                    other
                )));
            }
        }
    }

    let anomaly_count = registry.anomaly_iter().count();
    let schemas = registry.finalize();

    Ok(InspectResult { schemas, rows_scanned, anomaly_count, sampled_objects })
}

/// Clamp `requested` workers to the number of logical CPUs available.
///
/// Returns `(effective, Some(cap))` when clamping occurred, `(requested, None)` otherwise.
/// A `requested` value of 0 is treated as 1 (sequential).
/// Callers are expected to emit a warning when `Some(cap)` is returned.
pub fn effective_workers(requested: usize) -> (usize, Option<usize>) {
    let requested = requested.max(1);
    let cap = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(usize::MAX); // if detection fails, don't cap
    if requested > cap {
        (cap, Some(cap))
    } else {
        (requested, None)
    }
}

/// Run Pass 1 with `num_workers` parallel schema-inference threads.
///
/// One reader thread streams and parses the file sequentially (preserving I/O order),
/// distributing each parsed object to `num_workers` worker threads via a bounded channel.
/// Each worker maintains its own `SchemaRegistry`; they are merged and finalized once
/// the reader is done.
///
/// Using `num_workers = 1` is equivalent to sequential processing with extra overhead;
/// prefer `run()` for single-threaded use.
pub fn run_parallel(
    path: &Path,
    root_table: &str,
    text_threshold: u32,
    array_as_pg_array: bool,
    wide_column_threshold: usize,
    sibling_threshold: usize,
    sibling_jaccard: f64,
    stable_threshold: f64,
    rare_threshold: f64,
    progress_tx: Option<ProgressTx>,
    num_workers: usize,
) -> Result<Pass1Result> {
    let num_workers = num_workers.max(1);
    let total_bytes = file_size(path)?;

    let progress = if progress_tx.is_none() {
        Some(ProgressTracker::new(total_bytes, "Pass 1 (parallel)"))
    } else {
        None
    };

    // Bounded channel — backpressure prevents unbounded RAM growth on fast readers.
    // Sends raw JSON bytes; workers parse in parallel (no single-threaded serde bottleneck).
    let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(num_workers * 4);
    let rx = Arc::new(Mutex::new(rx));

    // Spawn worker threads, each with its own SchemaRegistry.
    let root_table_owned = root_table.to_string();
    let worker_handles: Vec<std::thread::JoinHandle<crate::error::Result<SchemaRegistry>>> = (0..num_workers)
        .map(|_| {
            let rx = Arc::clone(&rx);
            let root = root_table_owned.clone();
            let mut reg = SchemaRegistry::new(
                text_threshold, array_as_pg_array, wide_column_threshold,
                sibling_threshold, sibling_jaccard, stable_threshold, rare_threshold,
            );
            std::thread::spawn(move || {
                loop {
                    let mut bytes = match rx.lock().unwrap().recv() {
                        Ok(b) => b,
                        Err(_) => break, // channel closed — reader finished
                    };
                    match simd_json::from_slice::<serde_json::Value>(&mut bytes) {
                        Ok(serde_json::Value::Object(obj)) => reg.observe_root(&root, &obj),
                        Ok(other) => return Err(crate::error::J2sError::InvalidInput(format!(
                            "Expected JSON object at root level, found: {}", other
                        ))),
                        Err(e) => return Err(crate::error::J2sError::InvalidInput(format!(
                            "JSON parse error in worker: {}", e
                        ))),
                    }
                }
                Ok(reg)
            })
        })
        .collect();

    // Reader: current thread finds object boundaries and sends raw bytes to workers.
    let mut total_rows = 0u64;
    let (mut reader, _format) = JsonReader::open(path)?;
    let mut reader_err: Option<crate::error::J2sError> = None;
    const PROGRESS_INTERVAL: u64 = 1_000;

    while let Some(item) = reader.next_raw() {
        match item {
            Ok(bytes) => {
                // sync_channel::send blocks when the channel is full (backpressure).
                if tx.send(bytes).is_err() {
                    break; // all workers died — stop reading
                }
                total_rows += 1;
            }
            Err(e) => {
                reader_err = Some(e);
                break;
            }
        }

        if let Some(ref bar) = progress {
            bar.inc_rows(1);
            bar.set_bytes(reader.bytes_read());
        }
        if let Some(ref tx_prog) = progress_tx {
            if total_rows % PROGRESS_INTERVAL == 0 {
                let _ = tx_prog.send(ProgressEvent::Pass1Progress {
                    rows_scanned: total_rows,
                    bytes_read: reader.bytes_read(),
                    total_bytes,
                });
            }
        }
    }

    // Signal workers that reading is done.
    drop(tx);

    // Collect and merge all worker registries — propagate the first worker error if any.
    let mut merged = SchemaRegistry::new(
        text_threshold, array_as_pg_array, wide_column_threshold,
        sibling_threshold, sibling_jaccard, stable_threshold, rare_threshold,
    );
    let mut worker_err: Option<crate::error::J2sError> = None;
    for handle in worker_handles {
        match handle.join().expect("Pass 1 worker thread panicked") {
            Ok(reg) => { if worker_err.is_none() { merged.merge(reg); } }
            Err(e)  => { if worker_err.is_none() { worker_err = Some(e); } }
        }
    }

    if let Some(ref bar) = progress {
        bar.finish();
    }

    if let Some(e) = reader_err { return Err(e); }
    if let Some(e) = worker_err { return Err(e); }

    if let Some(ref tx_prog) = progress_tx {
        if total_rows > 0 && total_rows % PROGRESS_INTERVAL != 0 {
            let _ = tx_prog.send(ProgressEvent::Pass1Progress {
                rows_scanned: total_rows,
                bytes_read: total_bytes, // file fully read at this point
                total_bytes,
            });
        }
    }

    eprintln!("Pass 1 complete (parallel, {} workers): {} rows, building schema...", num_workers, total_rows);

    let mut schemas = merged.finalize();
    let overflow_warnings = apply_column_limit_guard(&mut schemas);
    let stats = merged.collect_stats();
    let truncated_names = merged.truncated_names().to_vec();
    let column_collisions = merged.column_collisions().to_vec();

    let tables_count = schemas.len();
    let columns_count = schemas.iter().map(|s| s.columns.len()).sum::<usize>();
    eprintln!("Schema: {} tables, {} total columns", tables_count, columns_count);

    if let Some(ref tx_prog) = progress_tx {
        let _ = tx_prog.send(ProgressEvent::Pass1Done { total_rows, tables_count, columns_count });
    }

    Ok(Pass1Result { schemas, total_rows, stats, truncated_names, column_collisions, overflow_warnings })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn fixture(name: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
    }

    #[test]
    fn test_inspect_respects_limit() {
        let path = fixture("users.jsonl"); // 3 rows
        let result = run_inspect(&path, "users", 256, 2).unwrap();
        assert_eq!(result.rows_scanned, 2, "should stop at limit");
    }

    #[test]
    fn test_inspect_reads_all_when_limit_exceeds_file() {
        let path = fixture("users.jsonl"); // 3 rows
        let result = run_inspect(&path, "users", 256, 1000).unwrap();
        assert_eq!(result.rows_scanned, 3, "should read all rows when limit > file size");
    }

    #[test]
    fn test_inspect_returns_schemas() {
        let path = fixture("users.jsonl");
        let result = run_inspect(&path, "users", 256, 10).unwrap();
        assert!(!result.schemas.is_empty(), "should infer at least one table");
        assert!(result.schemas.iter().any(|s| s.name == "users"), "root table must be present");
    }

    #[test]
    fn test_inspect_no_column_limit_guard() {
        let path = fixture("users.jsonl");
        let result = run_inspect(&path, "users", 256, 10).unwrap();
        use crate::schema::table_schema::WideStrategy;
        assert!(
            result.schemas.iter().all(|s| !matches!(s.wide_strategy, WideStrategy::Jsonb)),
            "column limit guard must not be applied in inspect mode"
        );
    }

    #[test]
    fn test_inspect_sampled_objects_count_matches_rows_scanned() {
        let path = fixture("users.jsonl"); // 3 rows
        let result = run_inspect(&path, "users", 256, 2).unwrap();
        assert_eq!(result.sampled_objects.len(), result.rows_scanned as usize);
        assert_eq!(result.sampled_objects.len(), 2);
    }

    #[test]
    fn test_inspect_sampled_objects_are_json_objects() {
        let path = fixture("users.jsonl");
        let result = run_inspect(&path, "users", 256, 3).unwrap();
        for obj in &result.sampled_objects {
            assert!(obj.is_object(), "each sampled item must be a JSON object");
        }
    }

    #[test]
    fn test_inspect_sampled_objects_all_when_limit_exceeds_file() {
        let path = fixture("users.jsonl"); // 3 rows
        let result = run_inspect(&path, "users", 256, 1000).unwrap();
        assert_eq!(result.sampled_objects.len(), 3);
    }

    use super::effective_workers;

    #[test]
    fn test_effective_workers_zero_becomes_one() {
        let (n, warn) = effective_workers(0);
        assert_eq!(n, 1);
        assert!(warn.is_none());
    }

    #[test]
    fn test_effective_workers_one_no_warning() {
        let (n, warn) = effective_workers(1);
        assert_eq!(n, 1);
        assert!(warn.is_none());
    }

    #[test]
    fn test_effective_workers_over_cap_is_clamped() {
        let cap = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        let (n, warn) = effective_workers(cap + 1000);
        assert_eq!(n, cap, "must be clamped to cap");
        assert_eq!(warn, Some(cap), "must report the cap when clamping");
    }

    #[test]
    fn test_effective_workers_exactly_at_cap_no_warning() {
        let cap = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
        let (n, warn) = effective_workers(cap);
        assert_eq!(n, cap);
        assert!(warn.is_none(), "exactly at cap must not warn");
    }
}
