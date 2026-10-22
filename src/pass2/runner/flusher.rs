//! Pass 2 — diskless flusher: drains worker buffers to `PostgreSQL` via `COPY FROM STDIN`
//! over a pool of connections (several COPYs may run on the same table concurrently).
//!
//! Fonctions :
//! - enum `FlushMode` — scheduling mode: normal, paused (RAM pressure) or final drain.
//! - struct `CopyOutcome` — result of one COPY task, carrying its connection back to the pool.
//! - fn `next_flush_candidate` — picks the next table to COPY for the current mode.
//! - fn `ram_used_ratio` — used RAM / total RAM ratio.
//! - fn `format_flusher_pause_log` — formats the flusher pause log (high RAM pressure).
//! - fn `format_flusher_resume_log` — formats the flusher resume log (low RAM pressure).
//! - fn `format_copy_start_log` — formats the COPY start log for a table.
//! - fn `format_copy_done_log` — formats the COPY done log for a table.
//! - fn `open_pool` — opens the flusher's PG connections (`synchronous_commit = off`).
//! - fn `run_flusher` — main loop: buffers worker batches and runs COPYs in parallel.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio_postgres::Client;

use crate::db::copy_sink::flush_mem_sink_to_pg;
use crate::error::{J2sError, Result};
use crate::io::progress_event::{ProgressEvent, ProgressTx};

use super::config;

/// How `next_flush_candidate` picks tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FlushMode {
    /// Only tables whose buffer reached the per-table threshold.
    Normal,
    /// Workers paused: also the largest tables while enough idle bytes remain.
    Paused,
    /// Channel closed: every non-empty table whose parent is fully flushed.
    Drain,
}

/// Result of one COPY task. The client goes back to the idle pool once joined.
struct CopyOutcome {
    table_id: String,
    rows: u64,
    bytes: u64,
    elapsed_ms: u128,
    client: Client,
    result: Result<()>,
}

/// Returns the largest eligible table, or `None`. A table may be picked while one of its COPYs
/// is still running: the dominant table (often the root) must not be capped to one connection.
/// - `Normal`: buffer `>= threshold`.
/// - `Paused`: as `Normal`, or any table while `idle_bytes >= DEFAULT_LOW_FLUSHER_BYTES`
///   (avoids tiny COPYs once RAM pressure is relieved).
/// - `Drain`: any table whose parent has nothing buffered or in flight, so FK constraints
///   already in the DB (import into an existing schema) are not violated.
///   shortcut: `Normal`/`Paused` flushes ignore FK order (as before #65), so pre-existing FKs
///   can still trip mid-stream; gate those modes on `parent_of` too if that case matters.
fn next_flush_candidate(
    buffers: &HashMap<String, bytes::BytesMut>,
    in_flight: &HashMap<String, usize>,
    mode: FlushMode,
    threshold: u64,
    idle_bytes: u64,
    parent_of: &HashMap<String, String>,
) -> Option<String> {
    let parent_done = |table: &str| parent_of.get(table).is_none_or(|p| {
        !in_flight.contains_key(p) && buffers.get(p).is_none_or(bytes::BytesMut::is_empty)
    });
    buffers.iter()
        .filter(|(_, b)| !b.is_empty())
        .filter(|(k, b)| match mode {
            FlushMode::Normal => b.len() as u64 >= threshold,
            FlushMode::Paused => b.len() as u64 >= threshold || idle_bytes >= DEFAULT_LOW_FLUSHER_BYTES,
            FlushMode::Drain => parent_done(k),
        })
        .max_by_key(|(_, b)| b.len())
        .map(|(k, _)| k.clone())
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

/// Opens `size` connections, each with `synchronous_commit = off` (best-effort).
async fn open_pool(pg_url: &str, size: usize) -> Result<Vec<Client>> {
    futures_util::future::try_join_all((0..size).map(|_| async {
        let conn = crate::db::connection::connect(pg_url).await?;
        config::try_set_synchronous_commit_off(&conn).await?;
        Ok::<_, J2sError>(conn)
    })).await
}

/// Concurrent flusher task: receives `(table_id, bytes, row_count)` batches from workers,
/// accumulates per-table `BytesMut` buffers, and COPYs them to `PostgreSQL` on a pool of
/// `parallel` connections (a table may have several COPYs running) when:
/// - a table's buffer exceeds `mem_flush_threshold_bytes`, or
/// - the flusher's total buffered bytes exceed `DEFAULT_HIGH_FLUSHER_BYTES` (workers are
///   paused and the largest tables flushed until buffers drop below `DEFAULT_LOW_FLUSHER_BYTES`).
///
/// `total_buffered` includes bytes of COPYs still in flight: it is only decremented once a COPY
/// completes, so workers are not resumed before the RAM is actually released. Unlike a
/// system-RAM-based signal, it is unaffected by `PostgreSQL`'s buffer cache growth.
///
/// On the first COPY failure: sets `error_flag`, sends `Pass2Error`, aborts the other COPYs
/// and returns `Err`. Returns total row count per table after draining all remaining buffers.
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
    parent_of: HashMap<String, String>,
    parallel: usize,
) -> Result<HashMap<String, u64>> {
    let mut idle = open_pool(&pg_url, parallel.max(1)).await?;
    let copy_sql_map = Arc::new(copy_sql_map);

    let mut buffers: HashMap<String, bytes::BytesMut> = HashMap::new();
    let mut pending_rows: HashMap<String, u64> = HashMap::new();
    let mut total_rows: HashMap<String, u64> = HashMap::new();
    let mut total_buffered: u64 = 0;
    // COPYs in flight per table; a table can have several at once.
    let mut in_flight: HashMap<String, usize> = HashMap::new();
    let mut in_flight_bytes: u64 = 0;
    let mut tasks: tokio::task::JoinSet<CopyOutcome> = tokio::task::JoinSet::new();
    let mut rx_open = true;
    let mut sys = sysinfo::System::new();
    let mut ram_tick = tokio::time::interval(std::time::Duration::from_secs(1));
    ram_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let fail = |table_name: &str, e: J2sError| {
        error_flag.store(true, Ordering::Release);
        if let Some(tx) = progress_tx.as_ref() {
            let _ = tx.send(ProgressEvent::Pass2Error { table_name: table_name.to_string(), message: e.to_string() });
        }
        pause_flag.store(false, Ordering::Release);
        e
    };

    loop {
        // Resume workers once enough bytes have actually been written to PG.
        if pause_flag.load(Ordering::Acquire) && total_buffered < DEFAULT_LOW_FLUSHER_BYTES {
            let msg = format_flusher_resume_log(total_buffered / 1024 / 1024, DEFAULT_LOW_FLUSHER_BYTES / 1024 / 1024);
            eprintln!("{msg}");
            if let Some(tx) = progress_tx.as_ref() { let _ = tx.send(ProgressEvent::Pass2Log(msg)); }
            pause_flag.store(false, Ordering::Release);
        }

        // Start as many COPYs as there are idle connections and eligible tables.
        let mode = if !rx_open {
            FlushMode::Drain
        } else if pause_flag.load(Ordering::Acquire) {
            FlushMode::Paused
        } else {
            FlushMode::Normal
        };
        while !idle.is_empty() {
            let Some(table_id) = next_flush_candidate(
                &buffers, &in_flight, mode, mem_flush_threshold_bytes,
                total_buffered - in_flight_bytes, &parent_of,
            ) else { break; };
            let Some(copy_sql) = copy_sql_map.get(&table_id).cloned() else {
                tasks.abort_all();
                return Err(fail(&table_id, J2sError::InvalidInput(format!("flusher: no copy_sql for table '{table_id}'"))));
            };
            let buf = buffers.remove(&table_id).unwrap_or_default();
            let rows = pending_rows.remove(&table_id).unwrap_or(0);
            let bytes = buf.len() as u64;
            eprintln!("{}", format_copy_start_log(&table_id, buf.len(), rows));
            let client = idle.pop().expect("loop guard: pool not empty");
            *in_flight.entry(table_id.clone()).or_insert(0) += 1;
            in_flight_bytes += bytes;
            tasks.spawn(async move {
                let start = std::time::Instant::now();
                let result = flush_mem_sink_to_pg(buf.freeze(), &copy_sql, &client).await;
                CopyOutcome { table_id, rows, bytes, elapsed_ms: start.elapsed().as_millis(), client, result }
            });
        }

        if !rx_open && tasks.is_empty() {
            if let Some(left) = buffers.iter().find(|(_, b)| !b.is_empty()).map(|(k, _)| k.clone()) {
                return Err(fail(&left, J2sError::InvalidInput(format!("flusher: drain left rows unflushed for '{left}'"))));
            }
            break;
        }

        tokio::select! {
            joined = tasks.join_next(), if !tasks.is_empty() => {
                let outcome = match joined {
                    Some(Ok(o)) => o,
                    Some(Err(e)) => {
                        tasks.abort_all();
                        while tasks.join_next().await.is_some() {}
                        return Err(fail("flusher", J2sError::InvalidInput(format!("flusher COPY task panicked: {e}"))));
                    }
                    None => continue,
                };
                if let Err(e) = outcome.result {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return Err(fail(&outcome.table_id, e));
                }
                eprintln!("{}", format_copy_done_log(&outcome.table_id, outcome.elapsed_ms));
                idle.push(outcome.client);
                if let Some(n) = in_flight.get_mut(&outcome.table_id) {
                    *n -= 1;
                    if *n == 0 { in_flight.remove(&outcome.table_id); }
                }
                in_flight_bytes -= outcome.bytes;
                total_buffered -= outcome.bytes;
                *total_rows.entry(outcome.table_id.clone()).or_insert(0) += outcome.rows;
                if outcome.rows > 0 {
                    if let Some(tx) = progress_tx.as_ref() {
                        let _ = tx.send(ProgressEvent::Pass2Flush { table_name: outcome.table_id, rows_flushed: outcome.rows });
                    }
                }
            }
            _ = ram_tick.tick(), if rx_open => {
                // Wakes the loop while workers are paused (rx idle) so scheduling keeps going.
                if verbose {
                    sys.refresh_memory();
                    let ratio = ram_used_ratio(sys.available_memory(), sys.total_memory());
                    eprintln!("[FLUSHER] RAM tick — {:.1}% RAM, {} MB dans buffers flusher ({} COPY en cours)",
                        ratio * 100.0, total_buffered / 1024 / 1024, in_flight.values().sum::<usize>());
                    if ratio > ram_high_watermark {
                        let available_mb = sys.available_memory() / 1024 / 1024;
                        let total_mb = sys.total_memory() / 1024 / 1024;
                        eprintln!("[FLUSHER] RAM {:.1}% > high {:.1}% ({} MB avail / {} MB total) — PG buffer cache (informatif)",
                            ratio * 100.0, ram_high_watermark * 100.0, available_mb, total_mb);
                    }
                }
                let _ = ram_low_watermark; // kept for config validation
            }
            msg = rx.recv(), if rx_open => {
                let Some((table_id, bytes, rows)) = msg else {
                    rx_open = false;
                    continue;
                };
                total_buffered += bytes.len() as u64;
                buffers.entry(table_id.clone()).or_default().extend_from_slice(&bytes);
                *pending_rows.entry(table_id).or_insert(0) += rows;

                // Pause workers when the flusher's own buffers (incl. in-flight COPYs) are overloaded.
                if total_buffered > DEFAULT_HIGH_FLUSHER_BYTES && !pause_flag.load(Ordering::Acquire) {
                    let msg = format_flusher_pause_log(total_buffered / 1024 / 1024, DEFAULT_HIGH_FLUSHER_BYTES / 1024 / 1024);
                    eprintln!("{msg}");
                    if let Some(tx) = progress_tx.as_ref() { let _ = tx.send(ProgressEvent::Pass2Log(msg)); }
                    pause_flag.store(true, Ordering::Release);
                }
            }
        }
    }

    pause_flag.store(false, Ordering::Release);
    Ok(total_rows)
}

/// Workers are paused when the flusher's total buffered bytes exceeds this.
/// Independent of system RAM — unaffected by `PostgreSQL`'s buffer cache.
const DEFAULT_HIGH_FLUSHER_BYTES: u64 = 512 * 1024 * 1024;

/// Flusher's own in-process buffer low-water mark (128 MiB).
/// Workers are resumed when the flusher's total buffered bytes drops below this.
const DEFAULT_LOW_FLUSHER_BYTES: u64 = 128 * 1024 * 1024;

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
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
    // next_flush_candidate tests — pure scheduling, no PG required
    // -------------------------------------------------------------------------

    use super::{next_flush_candidate, FlushMode};

    fn bufs(entries: &[(&str, usize)]) -> HashMap<String, bytes::BytesMut> {
        entries.iter().map(|(k, n)| (k.to_string(), bytes::BytesMut::from(&vec![0u8; *n][..]))).collect()
    }

    fn set(items: &[&str]) -> HashMap<String, usize> {
        items.iter().map(|s| (s.to_string(), 1)).collect()
    }

    #[test]
    fn normal_mode_only_picks_tables_over_threshold() {
        let b = bufs(&[("small", 10), ("big", 100)]);
        let none = HashMap::new();
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Normal, 50, 110, &none), Some("big".into()));
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Normal, 500, 110, &none), None);
    }

    #[test]
    fn table_with_copy_in_flight_stays_eligible_and_empty_is_skipped() {
        // A dominant table must be able to use several connections at once.
        let b = bufs(&[("a", 100), ("b", 60), ("empty", 0)]);
        let none = HashMap::new();
        assert_eq!(next_flush_candidate(&b, &set(&["a"]), FlushMode::Normal, 50, 160, &none), Some("a".into()));
        let b = bufs(&[("empty", 0)]);
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Drain, 50, 0, &none), None);
    }

    #[test]
    fn paused_mode_picks_largest_only_while_idle_bytes_above_low() {
        let b = bufs(&[("a", 10), ("b", 20)]);
        let none = HashMap::new();
        let low = super::DEFAULT_LOW_FLUSHER_BYTES;
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Paused, 1000, low, &none), Some("b".into()));
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Paused, 1000, low - 1, &none), None);
        // threshold still applies below LOW
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Paused, 15, low - 1, &none), Some("b".into()));
    }

    #[test]
    fn drain_waits_for_parent_buffered_or_in_flight() {
        let parent_of: HashMap<String, String> = [("child".to_string(), "root".to_string())].into();
        // child is bigger, but root still buffered → root first
        let b = bufs(&[("root", 10), ("child", 100)]);
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Drain, 1000, 110, &parent_of), Some("root".into()));
        // root in flight → child blocked
        let b = bufs(&[("child", 100)]);
        assert_eq!(next_flush_candidate(&b, &set(&["root"]), FlushMode::Drain, 1000, 100, &parent_of), None);
        // root done → child allowed, even under threshold
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Drain, 1000, 100, &parent_of), Some("child".into()));
    }

    // -------------------------------------------------------------------------
    // run_flusher against PostgreSQL — skipped without TEST_DATABASE_URL
    // -------------------------------------------------------------------------

    /// Creates a scratch schema with tables `a`, `b` (a = parent of b), runs `run_flusher` with
    /// 3 connections over `batches`, then drops the schema. `bad` COPYs into a missing table.
    #[allow(clippy::too_many_lines)] // linear setup → run → count → teardown
    async fn run_flusher_on_scratch_schema(
        batches: &[(&str, &str)],
        threshold: u64,
    ) -> Option<(crate::error::Result<HashMap<String, u64>>, bool, bool, i64)> {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;
        let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("[SKIP] TEST_DATABASE_URL not set");
            return None;
        };
        let schema = format!("j2s_flusher_{}", &uuid::Uuid::now_v7().simple().to_string()[20..]);
        let admin = crate::db::connection::connect(&url).await.expect("connect");
        admin.batch_execute(&format!(
            "CREATE SCHEMA {schema}; CREATE TABLE {schema}.a (n int); CREATE TABLE {schema}.b (n int)"
        )).await.expect("create scratch schema");

        let copy_sql_map: HashMap<String, String> = ["a", "b", "bad"].iter()
            .map(|t| (t.to_string(), format!("COPY {schema}.{} (n) FROM STDIN", if *t == "bad" { "missing" } else { t })))
            .collect();
        let parent_of: HashMap<String, String> = [("b".to_string(), "a".to_string())].into();
        let (tx, rx) = tokio::sync::mpsc::channel(batches.len().max(1));
        for (table, line) in batches {
            tx.send((table.to_string(), bytes::Bytes::from(line.to_string()), 1)).await.expect("send");
        }
        drop(tx);
        let pause_flag = Arc::new(AtomicBool::new(true));
        let error_flag = Arc::new(AtomicBool::new(false));
        let result = tokio::time::timeout(std::time::Duration::from_secs(20), super::run_flusher(
            rx, copy_sql_map, url, None, Arc::clone(&pause_flag), Arc::clone(&error_flag),
            threshold, 0.7, 0.5, false, parent_of, 3,
        )).await.expect("run_flusher must not hang");
        let rows: i64 = admin.query_one(&format!("SELECT (SELECT count(*) FROM {schema}.a) + (SELECT count(*) FROM {schema}.b)"), &[])
            .await.expect("count").get(0);
        admin.batch_execute(&format!("DROP SCHEMA {schema} CASCADE")).await.expect("drop scratch schema");
        Some((result, error_flag.load(Ordering::Acquire), pause_flag.load(Ordering::Acquire), rows))
    }

    #[tokio::test]
    async fn run_flusher_parallel_drain_copies_every_table() {
        let Some((result, errored, paused, rows)) =
            run_flusher_on_scratch_schema(&[("a", "1\n"), ("b", "2\n"), ("a", "3\n")], 1 << 30).await else { return };
        let counts = result.expect("drain must succeed");
        assert_eq!(counts.get("a"), Some(&2));
        assert_eq!(counts.get("b"), Some(&1));
        assert_eq!(rows, 3, "every buffered row must reach PG");
        assert!(!errored && !paused, "flags must be clear after a clean drain");
    }

    #[tokio::test]
    async fn run_flusher_streaming_flushes_keep_counts_exact() {
        // threshold = 1: every batch triggers a Normal-mode COPY while rx is still open, so a
        // table regularly receives data while its own COPY is in flight.
        let batches: Vec<(&str, &str)> = (0..20).map(|i| (if i % 3 == 0 { "b" } else { "a" }, "1\n")).collect();
        let Some((result, errored, paused, rows)) = run_flusher_on_scratch_schema(&batches, 1).await else { return };
        let counts = result.expect("streaming flushes must succeed");
        assert_eq!(counts.get("a"), Some(&13));
        assert_eq!(counts.get("b"), Some(&7));
        assert_eq!(rows, 20, "every row must reach PG exactly once");
        assert!(!errored && !paused);
    }

    #[tokio::test]
    async fn run_flusher_copy_error_returns_err_and_releases_workers() {
        let Some((result, errored, paused, _)) =
            run_flusher_on_scratch_schema(&[("a", "1\n"), ("bad", "2\n"), ("b", "3\n")], 1 << 30).await else { return };
        let err = result.expect_err("COPY into a missing table must fail the flusher");
        assert!(err.to_string().contains("missing"), "error must carry the PG cause: {err}");
        assert!(errored, "error_flag must be set so workers stop");
        assert!(!paused, "pause_flag must be cleared so paused workers are released");
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

    #[test]
    #[allow(clippy::float_cmp)] // exact literal return value from an early-return guard, not an approximation
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
