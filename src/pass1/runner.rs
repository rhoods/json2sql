//! Pass 1 — schema inference: stream the JSON file once and build a finalized table schema.
//!
//! Entry points:
//! - [`run`] — single-threaded, produces a [`Pass1Result`] with finalized schemas.
//! - [`run_inspect`] — like `run` but also collects per-column value statistics.
//! - [`run_parallel`] — multi-worker variant; merges per-worker registries at the end.
//!
//! Wide-table strategies ([`crate::schema::table_schema::InferredStrategy`]) are selected
//! during `finalize()` at the end of the pass — not row-by-row.
//! The resulting [`Pass1Result::schemas`] are sorted topologically (parents before children)
//! and are ready to be serialized or handed directly to Pass 2.
//!
//! Fonctions :
//! - fn `emit_root_wrapper_warning` — log si format wrapper détecté.
//! - struct `Pass1Config` — tous les paramètres contrôlant un run de Pass 1.
//! - struct `Pass1Result` — résultat de Pass 1 (schémas, stats, warnings, format détecté).
//!
//! Séquentiel :
//! - fn `run` — orchestre l'ouverture, le scan, la finalisation.
//! - fn `report_progress` — émet la progression (barre CLI et/ou canal IHM).
//! - fn `flush_final_progress` — émet un dernier événement de progression si nécessaire.
//! - fn `scan_json_rows` — boucle principale (observe chaque objet racine).
//! - fn `build_pass1_result` — finalise le registre et construit `Pass1Result`.
//!
//! Inspection (`run_inspect`, aperçu sans stratégies) :
//! - struct `InspectResult` — résultat d'un run d'inspection (schéma brut, pas de guard ni stratégies).
//! - fn `run_inspect` — scanne les N premiers objets sans guard ni stratégies wide.
//! - fn `build_inspect_registry` — registre aux seuils désactivés.
//! - fn `scan_objects_with_limit` — boucle bornée par `limit`, conserve les objets échantillonnés.
//!
//! Parallèle (`run_parallel`, N threads + merge) :
//! - fn `effective_workers` — plafonne au nombre de CPU logiques.
//! - fn `run_parallel` — orchestre lecteur + workers + merge.
//! - fn `spawn_worker_threads` — lance les threads consommant le channel MPMC.
//! - fn `read_and_dispatch` — thread lecteur, distribue les objets bruts round-robin.
//! - fn `join_and_merge_workers` — joint tous les threads et fusionne leurs `SchemaRegistry`
//!   (agrège les erreurs de tous les workers, n'en perd aucune).

use std::path::Path;

use crossbeam_channel;
use serde_json::Value;
use simd_json;

use crate::error::Result;
use crate::io::progress::ProgressTracker;
use crate::io::progress_event::{ProgressEvent, ProgressTx};
use crate::io::reader::{file_size, JsonFormat, JsonReader};
use crate::schema::naming::{ColumnCollision, TruncatedName};
use crate::schema::config::SkipCascadeWarning;
use crate::schema::finalizer::OverflowWarning;
use crate::schema::registry::{RegistryConfig, SchemaRegistry};
use crate::schema::stats::ColumnStats;
use crate::schema::table_schema::TableSchema;

const PROGRESS_INTERVAL: u64 = 1_000;

fn emit_root_wrapper_warning(format: &JsonFormat, progress_tx: Option<&ProgressTx>) {
    if let JsonFormat::RootWrapper(keys) = format {
        let msg = format!("Root wrapper detected — streaming keys: [{}]", keys.join(", "));
        eprintln!("{msg}");
        if let Some(tx) = progress_tx {
            let _ = tx.send(ProgressEvent::Pass1Log(msg));
        }
    }
}

/// All parameters controlling a Pass 1 run.
pub struct Pass1Config {
    pub root_table: String,
    pub registry: RegistryConfig,
    /// Used by `run_parallel` only; ignored by run and `run_inspect`.
    pub num_workers: Option<usize>,
}

/// Result of Pass 1.
pub struct Pass1Result {
    pub schemas: Vec<TableSchema>,
    pub total_rows: u64,
    pub stats: Vec<ColumnStats>,
    /// Table names that were truncated to fit the 63-byte `PostgreSQL` limit.
    pub truncated_names: Vec<TruncatedName>,
    /// Column name collisions resolved by hash suffix (multiple JSON fields → same SQL identifier).
    pub column_collisions: Vec<ColumnCollision>,
    /// Tables auto-converted to JSONB because they exceeded `PostgreSQL`'s 1600-column limit.
    pub overflow_warnings: Vec<OverflowWarning>,
    /// Real children cascaded away by a `Skip` override. Always empty on a fresh Pass 1 run —
    /// only populated when restoring a snapshot whose `strategy_overrides` include a `Skip`.
    pub skip_cascade_warnings: Vec<SkipCascadeWarning>,
    /// JSON format detected during pass1. `None` when restored from an old snapshot that
    /// predates this field — pass2 will re-detect in that case.
    pub detected_format: Option<JsonFormat>,
}

/// Run Pass 1: stream through the entire file and build the schema.
/// Returns finalized table schemas sorted topologically.
///
/// `progress_tx` — optional channel for streaming progress to the IHM.
/// Pass `None` for CLI / headless mode (terminal progress bar is used instead).
#[allow(clippy::needless_pass_by_value)] // public API: callers pass owned Option<ProgressTx>
pub fn run(
    path: &Path,
    config: &Pass1Config,
    progress_tx: Option<ProgressTx>,
) -> Result<Pass1Result> {
    let total_bytes = file_size(path)?;
    // Terminal progress bar: used only in CLI mode (when no IHM channel provided).
    let progress = if progress_tx.is_none() {
        Some(ProgressTracker::new(total_bytes, "Pass 1"))
    } else {
        None
    };
    let mut registry = SchemaRegistry::new(config.registry.clone());
    if let Some(ref tx) = progress_tx {
        let _ = tx.send(ProgressEvent::Pass1Log("Detecting JSON format…".to_string()));
    }
    let (mut reader, format) = JsonReader::open(path)?;
    emit_root_wrapper_warning(&format, progress_tx.as_ref());
    let total_rows = scan_json_rows(&mut reader, &mut registry, config, progress.as_ref(), progress_tx.as_ref(), total_bytes)?;
    if let Some(ref bar) = progress { bar.finish(); }
    eprintln!("Pass 1 complete: {total_rows} rows, building schema...");
    Ok(build_pass1_result(registry, total_rows, format, progress_tx.as_ref()))
}

fn report_progress(
    progress: Option<&ProgressTracker>,
    progress_tx: Option<&ProgressTx>,
    total_rows: u64,
    bytes_read: u64,
    total_bytes: u64,
) {
    if let Some(bar) = progress {
        bar.inc_rows(1);
        bar.set_bytes(bytes_read);
    }
    if let Some(tx) = progress_tx {
        if total_rows.is_multiple_of(PROGRESS_INTERVAL) {
            let _ = tx.send(ProgressEvent::Pass1Progress { rows_scanned: total_rows, bytes_read, total_bytes });
        }
    }
}

fn flush_final_progress(progress_tx: Option<&ProgressTx>, total_rows: u64, bytes_read: u64, total_bytes: u64) {
    if let Some(tx) = progress_tx {
        if total_rows > 0 && !total_rows.is_multiple_of(PROGRESS_INTERVAL) {
            let _ = tx.send(ProgressEvent::Pass1Progress { rows_scanned: total_rows, bytes_read, total_bytes });
        }
    }
}

fn scan_json_rows(
    reader: &mut JsonReader,
    registry: &mut SchemaRegistry,
    config: &Pass1Config,
    progress: Option<&ProgressTracker>,
    progress_tx: Option<&ProgressTx>,
    total_bytes: u64,
) -> Result<u64> {
    let mut total_rows = 0u64;
    while let Some(item) = reader.next() {
        let value = item?;
        match value {
            Value::Object(ref obj) => {
                let root = reader.current_key().unwrap_or(&config.root_table);
                registry.observe_root(root, obj);
                total_rows += 1;
            }
            other => {
                return Err(crate::error::J2sError::InvalidInput(format!(
                    "Expected JSON object at root level, found: {other}"
                )));
            }
        }
        report_progress(progress, progress_tx, total_rows, reader.bytes_read(), total_bytes);
    }
    flush_final_progress(progress_tx, total_rows, reader.bytes_read(), total_bytes);
    Ok(total_rows)
}

fn build_pass1_result(
    mut registry: SchemaRegistry,
    total_rows: u64,
    format: JsonFormat,
    progress_tx: Option<&ProgressTx>,
) -> Pass1Result {
    let (schemas, overflow_warnings) = registry.finalize_with_pg_guard();
    let stats = registry.collect_stats();
    let truncated_names = registry.truncated_names().to_vec();
    let column_collisions = registry.column_collisions().to_vec();
    let tables_count = schemas.len();
    let columns_count = schemas.iter().map(|s| s.columns.len()).sum::<usize>();
    eprintln!("Schema: {tables_count} tables, {columns_count} total columns");
    if let Some(tx) = progress_tx {
        let _ = tx.send(ProgressEvent::Pass1Done { total_rows, tables_count, columns_count });
    }
    Pass1Result {
        schemas, total_rows, stats, truncated_names, column_collisions, overflow_warnings,
        skip_cascade_warnings: Vec::new(),
        detected_format: Some(format),
    }
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
/// - Disables sibling detection and wide-table heuristics (thresholds set to `usize::MAX` / 0)
///
/// Useful for quickly understanding the structure of a large file before a full import.
pub fn run_inspect(
    path: &std::path::Path,
    config: &Pass1Config,
    limit: usize,
) -> Result<InspectResult> {
    let mut registry = build_inspect_registry(config);
    let (reader, format) = JsonReader::open(path)?;
    emit_root_wrapper_warning(&format, None);
    if let JsonFormat::RootWrapper(keys) = &format {
        if keys.len() > 1 {
            eprintln!(
                "  Note: inspect limit applies to '{}' only (multi-key wrapper)",
                keys[0]
            );
        }
    }
    let (rows_scanned, sampled_objects) =
        scan_objects_with_limit(reader, &mut registry, &config.root_table, limit)?;
    let anomaly_count = registry.anomaly_iter().count();
    let schemas = registry.finalize();
    Ok(InspectResult { schemas, rows_scanned, anomaly_count, sampled_objects })
}

fn build_inspect_registry(config: &Pass1Config) -> SchemaRegistry {
    use std::collections::HashSet;
    use crate::schema::strategies::StrategyName;
    SchemaRegistry::new(RegistryConfig {
        text_threshold: config.registry.text_threshold,
        array_as_pg_array: false,
        wide_column_threshold: usize::MAX,
        sibling_threshold: usize::MAX,
        sibling_jaccard: 1.0,
        stable_threshold: 0.0,
        rare_threshold: 0.0,
        disabled_strategies: HashSet::from([StrategyName::Sibling]),
    })
}

fn scan_objects_with_limit(
    mut reader: JsonReader,
    registry: &mut SchemaRegistry,
    root_table: &str,
    limit: usize,
) -> Result<(u64, Vec<Value>)> {
    let mut rows_scanned = 0u64;
    let mut sampled_objects: Vec<Value> = Vec::new();
    while rows_scanned < limit as u64 {
        let value = match reader.next() {
            None => break,
            Some(item) => item?,
        };
        let effective_root = reader.current_key().unwrap_or(root_table);
        match value {
            Value::Object(ref obj) => {
                registry.observe_root(effective_root, obj);
                rows_scanned += 1;
                sampled_objects.push(Value::Object(obj.clone()));
            }
            other => return Err(crate::error::J2sError::InvalidInput(format!(
                "Expected JSON object at root level, found: {other}"
            ))),
        }
    }
    Ok((rows_scanned, sampled_objects))
}

/// Clamp `requested` workers to the number of logical CPUs available.
///
/// Returns `(effective, Some(cap))` when clamping occurred, `(requested, None)` otherwise.
/// A `requested` value of 0 is treated as 1 (sequential).
/// Callers are expected to emit a warning when `Some(cap)` is returned.
#[must_use]
pub fn effective_workers(requested: usize) -> (usize, Option<usize>) {
    let requested = requested.max(1);
    let cap = std::thread::available_parallelism()
        .map_or(usize::MAX, std::num::NonZero::get); // if detection fails, don't cap
    if requested > cap {
        (cap, Some(cap))
    } else {
        (requested, None)
    }
}

/// Run Pass 1 with `config.num_workers` parallel schema-inference threads.
///
/// One reader thread streams and parses the file sequentially (preserving I/O order),
/// distributing each parsed object to the worker threads via a bounded channel.
/// Each worker maintains its own `SchemaRegistry`; they are merged and finalized once
/// the reader is done.
///
/// `config.num_workers = None` or `Some(1)` is equivalent to sequential processing with extra
/// overhead; prefer `run()` for single-threaded use.
#[allow(clippy::needless_pass_by_value)] // public API: callers pass owned Option<ProgressTx>
pub fn run_parallel(
    path: &Path,
    config: &Pass1Config,
    progress_tx: Option<ProgressTx>,
) -> Result<Pass1Result> {
    let num_workers = config.num_workers.unwrap_or(1).max(1);
    let total_bytes = file_size(path)?;

    let progress = if progress_tx.is_none() {
        Some(ProgressTracker::new(total_bytes, "Pass 1 (parallel)"))
    } else {
        None
    };

    // Bounded MPMC channel — capacity: 4 slots per worker gives the reader a small lead.
    // At typical JSON object sizes (~1–100 KB), peak buffer ≈ num_workers × 4 × object_size.
    let (tx, rx) = crossbeam_channel::bounded::<(Option<String>, Vec<u8>)>(num_workers * 4);
    let worker_handles = spawn_worker_threads(rx, config, num_workers);

    let (total_rows, reader_err, format) =
        read_and_dispatch(path, tx, progress.as_ref(), progress_tx.as_ref(), total_bytes)?;

    let (merged, worker_err) = join_and_merge_workers(worker_handles, config);

    if let Some(ref bar) = progress { bar.finish(); }

    // Reader errors take priority: an I/O failure is the most actionable signal.
    if let Some(e) = reader_err { return Err(e); }
    if let Some(e) = worker_err { return Err(e); }

    flush_final_progress(progress_tx.as_ref(), total_rows, total_bytes, total_bytes);
    eprintln!("Pass 1 complete (parallel, {num_workers} workers): {total_rows} rows, building schema...");
    Ok(build_pass1_result(merged, total_rows, format, progress_tx.as_ref()))
}

/// Spawn `num_workers` threads, each consuming JSON object bytes from `rx` and building
/// an independent `SchemaRegistry`. The original `rx` is dropped at the end of this function;
/// each worker holds its own clone.
#[allow(clippy::needless_pass_by_value)] // rx is dropped here to signal workers that reading is done
fn spawn_worker_threads(
    rx: crossbeam_channel::Receiver<(Option<String>, Vec<u8>)>,
    config: &Pass1Config,
    num_workers: usize,
) -> Vec<std::thread::JoinHandle<crate::error::Result<SchemaRegistry>>> {
    let fallback_root = config.root_table.clone();
    let registry_cfg = config.registry.clone();

    let handles = (0..num_workers)
        .map(|_| {
            let rx = rx.clone();
            let fallback_root = fallback_root.clone();
            let registry_cfg = registry_cfg.clone();
            let mut reg = SchemaRegistry::new(registry_cfg);
            std::thread::spawn(move || {
                while let Ok((key, mut bytes)) = rx.recv() {
                    let root = key.as_deref().unwrap_or(&fallback_root);
                    // simd_json mutates the slice in-place (zero-copy parsing); bytes is owned here.
                    match simd_json::from_slice::<serde_json::Value>(&mut bytes) {
                        Ok(serde_json::Value::Object(obj)) => reg.observe_root(root, &obj),
                        Ok(other) => return Err(crate::error::J2sError::InvalidInput(format!(
                            "Expected JSON object at root level, found: {other}"
                        ))),
                        Err(e) => return Err(crate::error::J2sError::InvalidInput(format!(
                            "JSON parse error in worker: {e}"
                        ))),
                    }
                }
                Ok(reg)
            })
        })
        .collect();
    // rx is dropped here — workers keep their clones; the reader uses only tx.
    handles
}

/// Stream JSON objects from `path`, sending raw bytes to workers via `tx`.
///
/// Returns `(total_rows, reader_err, format)`. Drops `tx` on return to signal workers that
/// reading is done. Returns `Err` only for I/O errors opening the file; parse errors are
/// returned as the `Option<J2sError>`.
#[allow(clippy::needless_pass_by_value)] // tx is dropped at return to signal workers that reading is done
fn read_and_dispatch(
    path: &Path,
    tx: crossbeam_channel::Sender<(Option<String>, Vec<u8>)>,
    progress: Option<&ProgressTracker>,
    progress_tx: Option<&ProgressTx>,
    total_bytes: u64,
) -> Result<(u64, Option<crate::error::J2sError>, JsonFormat)> {
    const PROGRESS_INTERVAL: u64 = 1_000;
    if let Some(tx_prog) = progress_tx {
        let _ = tx_prog.send(ProgressEvent::Pass1Log("Detecting JSON format…".to_string()));
    }
    let (mut reader, format) = JsonReader::open(path)?;
    emit_root_wrapper_warning(&format, progress_tx);
    let mut total_rows = 0u64;
    let mut reader_err: Option<crate::error::J2sError> = None;

    while let Some(item) = reader.next_raw() {
        match item {
            Ok(bytes) => {
                let key = reader.current_key().map(str::to_string);
                if tx.send((key, bytes)).is_err() { break; } // all workers died
                total_rows += 1;
            }
            Err(e) => { reader_err = Some(e); break; }
        }
        if let Some(bar) = progress {
            bar.inc_rows(1);
            bar.set_bytes(reader.bytes_read());
        }
        if let Some(tx_prog) = progress_tx {
            if total_rows.is_multiple_of(PROGRESS_INTERVAL) {
                let _ = tx_prog.send(ProgressEvent::Pass1Progress {
                    rows_scanned: total_rows,
                    bytes_read: reader.bytes_read(),
                    total_bytes,
                });
            }
        }
    }
    // tx dropped here — signals workers that reading is done.
    Ok((total_rows, reader_err, format))
}

/// Join all worker threads and merge their `SchemaRegistry` results.
///
/// Joins ALL handles before propagating any error so no thread is left detached.
/// Returns `(merged_registry, aggregated_worker_error)`.
fn join_and_merge_workers(
    handles: Vec<std::thread::JoinHandle<crate::error::Result<SchemaRegistry>>>,
    config: &Pass1Config,
) -> (SchemaRegistry, Option<crate::error::J2sError>) {
    let join_results: Vec<_> = handles.into_iter().map(std::thread::JoinHandle::join).collect();

    let mut merged = SchemaRegistry::new(config.registry.clone());
    let mut worker_errors: Vec<String> = Vec::new();
    for result in join_results {
        match result.map_err(|_| crate::error::J2sError::Schema(
            "Pass 1 worker thread panicked unexpectedly".to_string()
        )) {
            Ok(Ok(reg)) => { if worker_errors.is_empty() { merged.merge(reg); } }
            Err(e) | Ok(Err(e)) => {
                let msg = match e {
                    crate::error::J2sError::InvalidInput(m) => m,
                    other => other.to_string(),
                };
                worker_errors.push(msg);
            }
        }
    }
    let worker_err = if worker_errors.is_empty() {
        None
    } else {
        Some(crate::error::J2sError::InvalidInput(worker_errors.join(" | ")))
    };
    (merged, worker_err)
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::path::Path;

    fn fixture(name: &str) -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
    }

    fn inspect_config(root: &str) -> Pass1Config {
        Pass1Config {
            root_table: root.to_string(),
            registry: RegistryConfig {
                text_threshold: 256,
                array_as_pg_array: false,
                wide_column_threshold: usize::MAX,
                sibling_threshold: usize::MAX,
                sibling_jaccard: 1.0,
                stable_threshold: 0.0,
                rare_threshold: 0.0,
                disabled_strategies: HashSet::new(),
            },
            num_workers: None,
        }
    }

    #[test]
    fn test_inspect_respects_limit() {
        let path = fixture("users.jsonl"); // 3 rows
        let result = run_inspect(&path, &inspect_config("users"), 2).unwrap();
        assert_eq!(result.rows_scanned, 2, "should stop at limit");
    }

    #[test]
    fn test_inspect_reads_all_when_limit_exceeds_file() {
        let path = fixture("users.jsonl"); // 3 rows
        let result = run_inspect(&path, &inspect_config("users"), 1000).unwrap();
        assert_eq!(result.rows_scanned, 3, "should read all rows when limit > file size");
    }

    #[test]
    fn test_inspect_returns_schemas() {
        let path = fixture("users.jsonl");
        let result = run_inspect(&path, &inspect_config("users"), 10).unwrap();
        assert!(!result.schemas.is_empty(), "should infer at least one table");
        assert!(result.schemas.iter().any(|s| s.name == "users"), "root table must be present");
    }

    #[test]
    fn test_inspect_no_column_limit_guard() {
        let path = fixture("users.jsonl");
        let result = run_inspect(&path, &inspect_config("users"), 10).unwrap();
        use crate::schema::table_schema::InferredStrategy;
        assert!(
            result.schemas.iter().all(|s| !matches!(s.inferred_strategy, InferredStrategy::Jsonb)),
            "column limit guard must not be applied in inspect mode"
        );
    }

    #[test]
    fn test_inspect_sampled_objects_count_matches_rows_scanned() {
        let path = fixture("users.jsonl"); // 3 rows
        let result = run_inspect(&path, &inspect_config("users"), 2).unwrap();
        assert_eq!(result.sampled_objects.len(), result.rows_scanned as usize);
        assert_eq!(result.sampled_objects.len(), 2);
    }

    #[test]
    fn test_inspect_sampled_objects_are_json_objects() {
        let path = fixture("users.jsonl");
        let result = run_inspect(&path, &inspect_config("users"), 3).unwrap();
        for obj in &result.sampled_objects {
            assert!(obj.is_object(), "each sampled item must be a JSON object");
        }
    }

    #[test]
    fn test_inspect_sampled_objects_all_when_limit_exceeds_file() {
        let path = fixture("users.jsonl"); // 3 rows
        let result = run_inspect(&path, &inspect_config("users"), 1000).unwrap();
        assert_eq!(result.sampled_objects.len(), 3);
    }

    #[test]
    fn test_inspect_errors_on_non_object_root() {
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "42").unwrap(); // scalar, not object
        let result = run_inspect(tmp.path(), &inspect_config("t"), 10);
        assert!(result.is_err(), "scalar at root level must be an error");
    }

    // ── run_parallel tests ──────────────────────────────────────────────────

    fn run_parallel_default(path: &std::path::PathBuf, workers: usize) -> crate::error::Result<Pass1Result> {
        run_parallel(
            path,
            &Pass1Config {
                root_table: "users".to_string(),
                registry: RegistryConfig {
                    text_threshold: 256,
                    array_as_pg_array: false,
                    wide_column_threshold: usize::MAX,
                    sibling_threshold: usize::MAX,
                    sibling_jaccard: 1.0,
                    stable_threshold: 0.0,
                    rare_threshold: 0.0,
                    disabled_strategies: HashSet::new(),
                },
                num_workers: Some(workers),
            },
            None,
        )
    }

    #[test]
    fn test_parallel_single_worker_correct_row_count() {
        let path = fixture("users.jsonl");
        let result = run_parallel_default(&path, 1).unwrap();
        assert_eq!(result.total_rows, 3);
    }

    #[test]
    fn test_parallel_multi_worker_correct_row_count() {
        let path = fixture("users.jsonl");
        let result = run_parallel_default(&path, 4).unwrap();
        assert_eq!(result.total_rows, 3);
    }

    #[test]
    fn test_parallel_produces_root_table() {
        let path = fixture("users.jsonl");
        let result = run_parallel_default(&path, 2).unwrap();
        assert!(!result.schemas.is_empty());
        assert!(result.schemas.iter().any(|s| s.name == "users"), "root table must be present");
    }

    #[test]
    fn test_parallel_worker_count_zero_treated_as_one() {
        let path = fixture("users.jsonl");
        let result = run_parallel_default(&path, 0).unwrap();
        assert_eq!(result.total_rows, 3);
    }

    #[test]
    fn test_parallel_error_with_many_workers_returns_err_cleanly() {
        // All worker handles must be joined before propagating — no detached threads.
        // Uses many workers to stress the join-before-propagate path.
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"[{\"a\": 1}, 42, {\"b\": 2}]").unwrap();
        f.flush().unwrap();
        let result = run_parallel(
            f.path(),
            &Pass1Config {
                root_table: "root".to_string(),
                registry: RegistryConfig {
                    text_threshold: 256,
                    array_as_pg_array: false,
                    wide_column_threshold: usize::MAX,
                    sibling_threshold: usize::MAX,
                    sibling_jaccard: 1.0,
                    stable_threshold: 0.0,
                    rare_threshold: 0.0,
                    disabled_strategies: HashSet::new(),
                },
                num_workers: Some(8),
            },
            None,
        );
        assert!(result.is_err(), "must return Err for non-object root element");
    }

    #[test]
    fn test_parallel_multiple_worker_errors_aggregated_in_message() {
        // When multiple workers encounter errors, all error messages must appear
        // in the final Err — not just the first worker's error.
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        // 8 non-object root elements — with 4 workers each will see at least one error.
        f.write_all(b"[42, 43, 44, 45, 46, 47, 48, 49]").unwrap();
        f.flush().unwrap();
        let result = run_parallel(
            f.path(),
            &Pass1Config {
                root_table: "root".to_string(),
                registry: RegistryConfig {
                    text_threshold: 256,
                    array_as_pg_array: false,
                    wide_column_threshold: usize::MAX,
                    sibling_threshold: usize::MAX,
                    sibling_jaccard: 1.0,
                    stable_threshold: 0.0,
                    rare_threshold: 0.0,
                    disabled_strategies: HashSet::new(),
                },
                num_workers: Some(4),
            },
            None,
        );
        match result {
            Err(crate::error::J2sError::InvalidInput(msg)) => {
                // With 4 workers and 8 bad elements, each worker sees ≥1 error.
                // Aggregation must produce a message with ≥2 "root level" occurrences.
                let count = msg.matches("root level").count();
                assert!(count >= 2, "expected ≥2 errors aggregated in message, got {count}: {msg}");
            }
            Err(e) => panic!("expected InvalidInput, got: {e}"),
            Ok(_)  => panic!("expected Err"),
        }
    }

    #[test]
    fn test_parallel_non_object_root_returns_invalid_input_error() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"[{\"a\": 1}, 42]").unwrap();
        f.flush().unwrap();
        let result = run_parallel(
            f.path(),
            &Pass1Config {
                root_table: "root".to_string(),
                registry: RegistryConfig {
                    text_threshold: 256,
                    array_as_pg_array: false,
                    wide_column_threshold: usize::MAX,
                    sibling_threshold: usize::MAX,
                    sibling_jaccard: 1.0,
                    stable_threshold: 0.0,
                    rare_threshold: 0.0,
                    disabled_strategies: HashSet::new(),
                },
                num_workers: Some(1),
            },
            None,
        );
        match result {
            Err(crate::error::J2sError::InvalidInput(msg)) =>
                assert!(msg.contains("root level"), "error must mention root level: {msg}"),
            Err(e) => panic!("expected InvalidInput, got: {e}"),
            Ok(_)  => panic!("expected Err for non-object root element"),
        }
    }

    #[test]
    fn test_parallel_worker_error_no_double_invalid_input_prefix() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"[42]").unwrap();
        f.flush().unwrap();
        let result = run_parallel(
            f.path(),
            &Pass1Config {
                root_table: "root".to_string(),
                registry: RegistryConfig {
                    text_threshold: 256,
                    array_as_pg_array: false,
                    wide_column_threshold: usize::MAX,
                    sibling_threshold: usize::MAX,
                    sibling_jaccard: 1.0,
                    stable_threshold: 0.0,
                    rare_threshold: 0.0,
                    disabled_strategies: HashSet::new(),
                },
                num_workers: Some(1),
            },
            None,
        );
        match result {
            Err(crate::error::J2sError::InvalidInput(msg)) => {
                assert!(
                    !msg.contains("Invalid input:"),
                    "aggregated error must not contain double 'Invalid input:' prefix, got: {msg}"
                );
            }
            Err(e) => panic!("expected InvalidInput, got: {e}"),
            Ok(_) => panic!("expected Err"),
        }
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
        let cap = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let (n, warn) = effective_workers(cap + 1000);
        assert_eq!(n, cap, "must be clamped to cap");
        assert_eq!(warn, Some(cap), "must report the cap when clamping");
    }

    // ── wrapper format pipeline tests ───────────────────────────────────────

    fn wrapper_config(root: &str) -> Pass1Config {
        Pass1Config {
            root_table: root.to_string(),
            registry: RegistryConfig {
                text_threshold: 256,
                array_as_pg_array: false,
                wide_column_threshold: usize::MAX,
                sibling_threshold: usize::MAX,
                sibling_jaccard: 1.0,
                stable_threshold: 0.0,
                rare_threshold: 0.0,
                disabled_strategies: HashSet::new(),
            },
            num_workers: None,
        }
    }

    #[test]
    fn test_run_on_wrapper_mono_key_correct_row_count() {
        let path = fixture("wrapper_mono.json");
        let result = run(&path, &wrapper_config("foods"), None).unwrap();
        assert_eq!(result.total_rows, 2);
    }

    #[test]
    fn test_run_on_wrapper_multi_key_correct_row_count() {
        let path = fixture("wrapper_multi.json");
        let result = run(&path, &wrapper_config("foods"), None).unwrap();
        assert_eq!(result.total_rows, 4); // 2 Foods + 2 Nutrients
    }

    #[test]
    fn test_run_on_wrapper_table_named_after_key_not_config() {
        let path = fixture("wrapper_mono.json");
        let result = run(&path, &wrapper_config("wrong_name"), None).unwrap();
        let names: Vec<&str> = result.schemas.iter().map(|s| s.name.as_str()).collect();
        // SchemaRegistry normalises names; "Foods" → "foods"
        assert!(names.contains(&"foods"), "table should be 'foods' (normalised wrapper key 'Foods'), got: {names:?}");
        assert!(!names.contains(&"wrong_name"), "table must NOT be named after config.root_table");
    }

    #[test]
    fn test_run_on_wrapper_multi_key_produces_separate_tables() {
        let path = fixture("wrapper_multi.json");
        let result = run(&path, &wrapper_config("wrong_name"), None).unwrap();
        let names: Vec<&str> = result.schemas.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"foods"), "should have 'foods' table, got: {names:?}");
        assert!(names.contains(&"nutrients"), "should have 'nutrients' table, got: {names:?}");
    }

    #[test]
    fn test_run_inspect_on_wrapper_table_named_after_key() {
        let path = fixture("wrapper_mono.json");
        let result = run_inspect(&path, &inspect_config("wrong_name"), 100).unwrap();
        let names: Vec<&str> = result.schemas.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"foods"), "inspect table should be 'foods' (normalised 'Foods'), got: {names:?}");
    }

    #[test]
    fn test_run_parallel_on_wrapper_table_named_after_key() {
        let path = fixture("wrapper_mono.json");
        let result = run_parallel(
            &path,
            &Pass1Config {
                root_table: "wrong_name".to_string(),
                registry: RegistryConfig {
                    text_threshold: 256,
                    array_as_pg_array: false,
                    wide_column_threshold: usize::MAX,
                    sibling_threshold: usize::MAX,
                    sibling_jaccard: 1.0,
                    stable_threshold: 0.0,
                    rare_threshold: 0.0,
                    disabled_strategies: HashSet::new(),
                },
                num_workers: Some(1),
            },
            None,
        ).unwrap();
        let names: Vec<&str> = result.schemas.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"foods"), "parallel: table should be 'foods' (normalised 'Foods'), got: {names:?}");
    }

    #[test]
    fn test_run_inspect_on_wrapper_succeeds() {
        let path = fixture("wrapper_mono.json");
        let result = run_inspect(&path, &inspect_config("foods"), 100).unwrap();
        assert_eq!(result.rows_scanned, 2);
        assert!(!result.schemas.is_empty());
    }

    #[test]
    fn test_run_on_wrapper_sends_pass1log_to_ihm_channel() {
        use crate::io::progress_event::ProgressEvent;
        use tokio::sync::mpsc;
        let path = fixture("wrapper_mono.json");
        let (tx, mut rx) = mpsc::unbounded_channel();
        run(&path, &wrapper_config("foods"), Some(tx)).unwrap();
        let events: Vec<ProgressEvent> = {
            let mut v = Vec::new();
            while let Ok(e) = rx.try_recv() { v.push(e); }
            v
        };
        let log_msgs: Vec<&str> = events.iter().filter_map(|e| {
            if let ProgressEvent::Pass1Log(msg) = e { Some(msg.as_str()) } else { None }
        }).collect();
        assert!(
            log_msgs.iter().any(|m| m.contains("Root wrapper") && m.contains("Foods")),
            "expected Pass1Log with wrapper warning, got: {log_msgs:?}"
        );
    }

    #[test]
    fn test_run_parallel_on_wrapper_correct_row_count() {
        let path = fixture("wrapper_mono.json");
        let result = run_parallel(
            &path,
            &Pass1Config {
                root_table: "foods".to_string(),
                registry: RegistryConfig {
                    text_threshold: 256,
                    array_as_pg_array: false,
                    wide_column_threshold: usize::MAX,
                    sibling_threshold: usize::MAX,
                    sibling_jaccard: 1.0,
                    stable_threshold: 0.0,
                    rare_threshold: 0.0,
                    disabled_strategies: HashSet::new(),
                },
                num_workers: Some(2),
            },
            None,
        ).unwrap();
        assert_eq!(result.total_rows, 2);
    }

    #[test]
    fn test_effective_workers_exactly_at_cap_no_warning() {
        let cap = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        let (n, warn) = effective_workers(cap);
        assert_eq!(n, cap);
        assert!(warn.is_none(), "exactly at cap must not warn");
    }

    // --- detected_format in Pass1Result ---

    #[test]
    fn test_run_sets_detected_format_array() {
        let path = fixture("users.json");
        let result = run(&path, &wrapper_config("users"), None).unwrap();
        assert_eq!(result.detected_format, Some(crate::io::reader::JsonFormat::Array));
    }

    #[test]
    fn test_run_sets_detected_format_lines() {
        let path = fixture("users.jsonl");
        let result = run(&path, &wrapper_config("users"), None).unwrap();
        assert_eq!(result.detected_format, Some(crate::io::reader::JsonFormat::Lines));
    }

    #[test]
    fn test_run_sets_detected_format_root_wrapper() {
        let path = fixture("wrapper_mono.json");
        let result = run(&path, &wrapper_config("foods"), None).unwrap();
        assert_eq!(
            result.detected_format,
            Some(crate::io::reader::JsonFormat::RootWrapper(vec!["Foods".to_string()]))
        );
    }

    #[test]
    fn test_run_parallel_sets_detected_format() {
        let path = fixture("users.json");
        let result = run_parallel(&path, &Pass1Config { num_workers: Some(2), ..wrapper_config("users") }, None).unwrap();
        assert_eq!(result.detected_format, Some(crate::io::reader::JsonFormat::Array));
    }
}
