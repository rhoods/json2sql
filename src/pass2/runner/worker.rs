//! Pass 2 — diskless worker: local `MemSink` accumulation, sends batches to `run_flusher`.
//!
//! Fonctions :
//! - fn `collect_above_threshold` — sinks ready to flush (above the per-worker threshold).
//! - fn `process_worker_item_diskless` — processes one JSON item in a worker.
//! - struct `WorkerDisklessConfig` — configuration passed to `run_worker_diskless`.
//! - fn `run_worker_diskless` — worker loop (parse, insert, flush above threshold).
//! - fn `parse_json_object` — parses a root JSON object.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::anomaly::collector::{AnomalyEvent, AnomalyProxy};
use crate::error::{J2sError, Result};
use crate::pass2::insert::{insert_object, InsertCtx};
use crate::schema::table_schema::TableSchema;

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

pub(super) struct WorkerDisklessConfig {
    pub(super) anomaly_tx: tokio::sync::mpsc::UnboundedSender<AnomalyEvent>,
    pub(super) path_map: Arc<HashMap<String, TableSchema>>,
    pub(super) root_schema: Arc<TableSchema>,
    pub(super) cancel: CancellationToken,
    pub(super) flush_tx: tokio::sync::mpsc::Sender<(String, bytes::Bytes, u64)>,
    pub(super) pause_flag: Arc<std::sync::atomic::AtomicBool>,
    pub(super) error_flag: Arc<std::sync::atomic::AtomicBool>,
    pub(super) mem_flush_threshold: u64,
}

/// Diskless worker: local MemSink accumulation, sends batches to `run_flusher` via channel.
/// Checks `error_flag` each iteration (flusher fatal error) and yields on `pause_flag`
/// (flusher buffer pressure). Workers spin without draining — flusher controls when to resume.
/// For wrapper-format files, `key` carries the raw wrapper key and the worker resolves the
/// correct root schema via `path_map`; for regular formats `key` is `None` and `root_schema`
/// is used directly.
pub(super) async fn run_worker_diskless(
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::pass2::runner::test_support::make_schema_with_rows;
    use crate::schema::table_schema::TableSchema;

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
        use std::sync::atomic::AtomicBool;

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
}
