//! Pass 2 — diskless flusher: drains worker buffers to `PostgreSQL` via `COPY FROM STDIN`.
//!
//! Fonctions :
//! - fn `find_all_nonempty_buffers` — selects all non-empty buffers to drain.
//! - fn `topological_drain_order` — orders buffers to drain in topological order.
//! - fn `ram_used_ratio` — used RAM / total RAM ratio.
//! - fn `format_flusher_pause_log` — formats the flusher pause log (high RAM pressure).
//! - fn `format_flusher_resume_log` — formats the flusher resume log (low RAM pressure).
//! - fn `format_copy_start_log` — formats the COPY start log for a table.
//! - fn `format_copy_done_log` — formats the COPY done log for a table.
//! - fn `find_largest_buffer` — selects the largest non-empty buffer.
//! - fn `flush_table_to_pg` — flushes one table to `PostgreSQL` via COPY.
//! - fn `run_flusher` — main loop: drains worker buffers to PG under RAM pressure.

use std::collections::HashMap;
use std::sync::Arc;

use crate::db::copy_sink::flush_mem_sink_to_pg;
use crate::error::{J2sError, Result};
use crate::io::progress_event::{ProgressEvent, ProgressTx};

use super::config;

/// Returns all `table_ids` with a non-empty pending buffer, in arbitrary order.
/// Used during RAM-pressure flushes to drain every accumulated table in one tick.
fn find_all_nonempty_buffers(buffers: &HashMap<String, bytes::BytesMut>) -> Vec<String> {
    buffers.iter()
        .filter(|(_, b)| !b.is_empty())
        .map(|(k, _)| k.clone())
        .collect()
}

/// Returns the keys of `buffers` in `topo_order` (parents first), followed by any
/// remaining keys not covered by the topo order. Empty buffers are excluded.
/// Used in the drain phase so FK constraints already in the DB are not violated
/// when flushing child tables before their parents.
fn topological_drain_order(topo_order: &[String], buffers: &HashMap<String, bytes::BytesMut>) -> Vec<String> {
    let mut result: Vec<String> = topo_order.iter()
        .filter(|t| buffers.get(*t).is_some_and(|b| !b.is_empty()))
        .cloned()
        .collect();
    let in_topo: std::collections::HashSet<&str> = topo_order.iter().map(std::string::String::as_str).collect();
    for (k, b) in buffers {
        if !b.is_empty() && !in_topo.contains(k.as_str()) {
            result.push(k.clone());
        }
    }
    result
}

/// RAM usage ratio in [0.0, 1.0]. Returns 0.0 when total memory is zero.
#[allow(clippy::cast_precision_loss)] // byte counts fit well within f64's 52-bit mantissa for any realistic RAM size
fn ram_used_ratio(available: u64, total: u64) -> f64 {
    if total == 0 { return 0.0; }
    total.saturating_sub(available) as f64 / total as f64
}

fn format_flusher_pause_log(total_mb: u64, high_mb: u64) -> String {
    format!(
        "[FLUSHER] {total_mb} MB in buffers > {high_mb} MB threshold — workers paused"
    )
}

fn format_flusher_resume_log(total_mb: u64, low_mb: u64) -> String {
    format!(
        "[FLUSHER] {total_mb} MB in buffers < {low_mb} MB threshold — workers resumed"
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

/// Flush one table's pending buffer to `PostgreSQL` and update accounting.
/// Removes the table from `buffers` and `pending_rows`, adds to `total_rows`.
/// Sets `error_flag` and returns `Err` on PG failure.
#[allow(clippy::too_many_arguments)] // each param is distinct flush/accounting state, not groupable without an artificial struct
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
/// accumulates per-table `BytesMut` buffers, and COPYs to `PostgreSQL` when:
/// - a table's buffer exceeds `mem_flush_threshold_bytes`, or
/// - the flusher's own total buffered bytes exceed `DEFAULT_HIGH_FLUSHER_BYTES` (flushes
///   the largest table and pauses workers until buffers drop below `DEFAULT_LOW_FLUSHER_BYTES`).
///
/// Unlike a system-RAM-based signal, the `total_buffered` counter is unaffected by `PostgreSQL`'s
/// buffer cache growth, which would otherwise keep `available_memory()` permanently above any
/// watermark during bulk imports.
///
/// Returns total row count per table after draining all remaining buffers.
#[allow(clippy::too_many_lines)] // tokio::select! event loop over rx; do not split — would move rx across fn boundaries
#[allow(clippy::too_many_arguments)] // each param is distinct flusher config/state, not groupable without an artificial struct
pub(super) async fn run_flusher(
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
    topo_order: Vec<String>,
) -> Result<HashMap<String, u64>> {
    use std::sync::atomic::Ordering;

    let conn = crate::db::connection::connect(&pg_url).await?;
    config::try_set_synchronous_commit_off(&conn).await?;

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
                    candidates.sort_by_key(|k| std::cmp::Reverse(buffers.get(k).map_or(0, bytes::BytesMut::len)));
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

    // Drain all remaining buffers once all workers finish (channel closed).
    // Flush in topological order (parents before children) so FK constraints already
    // in the DB (from a previous import run) are not violated.
    for table_id in topological_drain_order(&topo_order, &buffers) {
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

/// Flusher's own in-process buffer high-water mark (512 MiB).
/// Workers are paused when the flusher's total buffered bytes exceeds this.
/// Independent of system RAM — unaffected by `PostgreSQL`'s buffer cache.
const DEFAULT_HIGH_FLUSHER_BYTES: u64 = 512 * 1024 * 1024;

/// Flusher's own in-process buffer low-water mark (128 MiB).
/// Workers are resumed when the flusher's total buffered bytes drops below this.
const DEFAULT_LOW_FLUSHER_BYTES: u64 = 128 * 1024 * 1024;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    #[test]
    fn default_high_flusher_bytes_is_512_mb() {
        assert_eq!(super::DEFAULT_HIGH_FLUSHER_BYTES, 512 * 1024 * 1024);
    }

    #[test]
    fn default_low_flusher_bytes_is_128_mb() {
        assert_eq!(super::DEFAULT_LOW_FLUSHER_BYTES, 128 * 1024 * 1024);
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
        small.extend_from_slice(&[0u8; 100]);
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
}
