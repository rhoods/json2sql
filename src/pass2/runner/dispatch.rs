//! Pass 2 — dispatch loop and progress/anomaly reporting.
//!
//! - fn `spawn_anomaly_writer` — spawns the anomaly-writing task.
//! - fn `preflight_warn_nonempty` — warns if root tables already contain rows.
//! - fn `finalize_dispatch` — final progress bar update at the end of dispatch.
//! - fn `emit_completion_events` — sends the end-of-run events.
//! - fn `log_constraint_warnings` — logs unapplied FK constraint warnings.
//! - fn `update_row_progress` — periodic progress bar update.
//! - fn `dispatch_loop` — dispatches the JSON stream to workers.

use std::collections::HashMap;

use tokio_postgres::Client;

use crate::anomaly::collector::{AnomalyCollector, AnomalyEvent};
use crate::error::Result;
use crate::io::progress::ProgressTracker;
use crate::io::progress_event::{ProgressEvent, ProgressTx};
use crate::io::reader::JsonReader;

const PROGRESS_INTERVAL: u64 = 1_000;

/// Spawn the blocking anomaly writer task. Returns `(sender, handle)`.
pub(super) fn spawn_anomaly_writer(
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
pub(super) async fn preflight_warn_nonempty(
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
pub(super) fn finalize_dispatch(
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
pub(super) fn emit_completion_events(
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
pub(super) fn log_constraint_warnings(warnings: &[crate::db::ddl::ConstraintWarning], progress_tx: Option<&ProgressTx>) {
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
pub(super) async fn dispatch_loop(
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

#[cfg(test)]
mod tests {
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
}
