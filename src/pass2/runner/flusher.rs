//! Pass 2 — diskless flusher: drains worker buffers to `PostgreSQL` via `COPY FROM STDIN`
//! over a pool of connections (several COPYs may run on the same table concurrently).
//!
//! Fonctions :
//! - enum `FlushMode` — scheduling mode: normal, paused (RAM pressure) or final drain.
//! - struct `CopyOutcome` — result of one COPY task, carrying its connection (and pool slot) back to the pool.
//! - struct `FreezeTarget` — table loadable with `COPY FREEZE`: quoted name + `COPY … FREEZE` statement.
//! - fn `next_flush_candidate` — picks the next table to COPY for the current mode and connection.
//! - fn `ram_used_ratio` — used RAM / total RAM ratio.
//! - fn `format_flusher_pause_log` — formats the flusher pause log (high RAM pressure).
//! - fn `format_flusher_resume_log` — formats the flusher resume log (low RAM pressure).
//! - fn `format_copy_start_log` — formats the COPY start log for a table.
//! - fn `format_copy_done_log` — formats the COPY done log for a table.
//! - fn `claim_for_freeze` — locks a table in the load transaction and truncates it if empty.
//! - fn `copy_task` — one COPY, claiming the table first on its first FREEZE COPY.
//! - fn `open_pool` — opens the flusher's PG connections (`synchronous_commit = off`, load transaction for a FREEZE load).
//! - fn `run_flusher` — main loop: buffers worker batches and runs COPYs in parallel.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio_postgres::Client;

use crate::db::copy_sink::flush_mem_sink_to_pg;
use crate::db::error::pg_err;
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

/// Result of one COPY task. The client goes back to the idle pool (slot `conn_id`) once joined.
/// `result` carries the claim outcome of a FREEZE table's first COPY (`Some(truncated)`).
struct CopyOutcome {
    table_id: String,
    conn_id: usize,
    rows: u64,
    bytes: u64,
    elapsed_ms: u128,
    client: Client,
    result: Result<Option<bool>>,
}

/// A table loadable with `COPY FREEZE` (#68, #77): `table` is its quoted qualified name (to lock,
/// check and truncate it), `copy_sql` its `COPY … FREEZE` statement.
pub(super) struct FreezeTarget {
    pub(super) table: String,
    pub(super) copy_sql: String,
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
///
/// `rank(table)` says whether the connection asking may take it: `None` = pinned to another
/// connection (FREEZE load), `Some(true)` = pinned to this one (served before anything else),
/// `Some(false)` = any connection.
fn next_flush_candidate(
    buffers: &HashMap<String, bytes::BytesMut>,
    in_flight: &HashMap<String, usize>,
    mode: FlushMode,
    threshold: u64,
    idle_bytes: u64,
    parent_of: &HashMap<String, String>,
    rank: impl Fn(&str) -> Option<bool>,
) -> Option<String> {
    let parent_done = |table: &str| parent_of.get(table).is_none_or(|p| {
        !in_flight.contains_key(p) && buffers.get(p).is_none_or(bytes::BytesMut::is_empty)
    });
    buffers.iter()
        .filter(|(_, b)| !b.is_empty())
        .filter_map(|(k, b)| rank(k).map(|own| (k, b, own)))
        .filter(|(k, b, _)| match mode {
            FlushMode::Normal => b.len() as u64 >= threshold,
            FlushMode::Paused => b.len() as u64 >= threshold || idle_bytes >= DEFAULT_LOW_FLUSHER_BYTES,
            FlushMode::Drain => parent_done(k),
        })
        .max_by_key(|(_, b, own)| (*own, b.len()))
        .map(|(k, _, _)| k.clone())
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

/// Opens `size` connections (pool slots `0..size`), each with `synchronous_commit = off`
/// (best-effort). For a FREEZE load each one also opens the transaction it keeps for the whole load.
async fn open_pool(pg_url: &str, size: usize, freeze: bool) -> Result<Vec<(usize, Client)>> {
    futures_util::future::try_join_all((0..size).map(|id| async move {
        let conn = crate::db::connection::connect(pg_url).await?;
        config::try_set_synchronous_commit_off(&conn).await?;
        if freeze {
            // The transaction idles between COPYs: no idle-in-transaction timeout. READ COMMITTED
            // explicitly: a repeatable-read default keeps a snapshot, and COPY FREEZE is refused.
            conn.batch_execute("SET idle_in_transaction_session_timeout = 0").await
                .map_err(|e| pg_err("open load transaction", &e))?;
            conn.batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED").await
                .map_err(|e| pg_err("open load transaction", &e))?;
        }
        Ok::<_, J2sError>((id, conn))
    })).await
}

/// First COPY of a table on its owner connection, inside the load transaction: locks the
/// table, then truncates it if it is empty, which makes `COPY FREEZE` valid in this transaction.
/// A table that already has rows is never truncated: returns `false`, and it gets a plain COPY.
async fn claim_for_freeze(client: &Client, table: &str) -> Result<bool> {
    client.batch_execute(&format!("LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE")).await
        .map_err(|e| pg_err(&format!("LOCK {table}"), &e))?;
    let empty: bool = client.query_one(&format!("SELECT NOT EXISTS (SELECT 1 FROM {table})"), &[]).await
        .map_err(|e| pg_err(&format!("emptiness check {table}"), &e))?
        .get(0);
    if empty {
        client.batch_execute(&format!("TRUNCATE {table}")).await
            .map_err(|e| pg_err(&format!("TRUNCATE {table}"), &e))?;
    }
    Ok(empty)
}

/// One COPY task. `claim` (`FreezeTarget` of a table's first COPY in a FREEZE load) claims
/// the table first and switches to `COPY FREEZE` when it could be truncated. Returns the claim
/// outcome, if any.
async fn copy_task(client: &Client, buf: bytes::Bytes, copy_sql: &str, claim: Option<(String, String)>) -> Result<Option<bool>> {
    let Some((table, freeze_sql)) = claim else {
        flush_mem_sink_to_pg(buf, copy_sql, client).await?;
        return Ok(None);
    };
    let truncated = claim_for_freeze(client, &table).await?;
    flush_mem_sink_to_pg(buf, if truncated { &freeze_sql } else { copy_sql }, client).await?;
    Ok(Some(truncated))
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
/// and returns `Err`. Returns total row count per table after draining all remaining buffers,
/// and the pool's connections.
///
/// FREEZE load (`freeze_sql` is `Some`, #68, #77): every connection keeps one transaction for the
/// whole load. A table (key of `freeze_sql`) belongs to the connection that COPYs it first: that
/// connection claims it (`claim_for_freeze`) and loads every batch of it, with `COPY FREEZE` once
/// truncated. Other tables (the root, unless `wal_level = minimal`) keep using any connection.
/// Nothing is committed here: the caller commits the returned connections once it knows no worker
/// failed; dropping them rolls the load back.
#[allow(clippy::too_many_lines)] // tokio::select! event loop over rx; do not split — would move rx across fn boundaries
#[allow(clippy::too_many_arguments)] // each param is distinct flusher config/state, not groupable without an artificial struct
pub(super) async fn run_flusher(
    mut rx: tokio::sync::mpsc::Receiver<(String, bytes::Bytes, u64)>,
    copy_sql_map: HashMap<String, String>,
    freeze_sql: Option<HashMap<String, FreezeTarget>>,
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
) -> Result<(HashMap<String, u64>, Vec<Client>)> {
    let mut idle = open_pool(&pg_url, parallel.max(1), freeze_sql.is_some()).await?;
    let copy_sql_map = Arc::new(copy_sql_map);
    // FREEZE load: table → owner slot, bytes COPYed into its pinned tables per slot (new
    // tables go to the lightest idle slot), and the tables their owner managed to truncate.
    let mut owner: HashMap<String, usize> = HashMap::new();
    let mut owned_bytes: Vec<u64> = vec![0; idle.len()];
    let mut frozen: HashSet<String> = HashSet::new();

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
        // Lightest slots pick first (`pop` takes the last), so new tables spread over the pool.
        idle.sort_by_key(|(id, _)| std::cmp::Reverse(owned_bytes[*id]));
        let mut no_work: Vec<(usize, Client)> = Vec::new();
        while let Some((conn_id, client)) = idle.pop() {
            let target_of = |t: &str| freeze_sql.as_ref().and_then(|f| f.get(t));
            let rank = |t: &str| match (target_of(t), owner.get(t)) {
                (Some(_), Some(&o)) if o != conn_id => None,
                (Some(_), Some(_)) => Some(true),
                _ => Some(false),
            };
            let Some(table_id) = next_flush_candidate(
                &buffers, &in_flight, mode, mem_flush_threshold_bytes,
                total_buffered - in_flight_bytes, &parent_of, rank,
            ) else {
                no_work.push((conn_id, client));
                continue;
            };
            let Some(plain_sql) = copy_sql_map.get(&table_id).cloned() else {
                tasks.abort_all();
                return Err(fail(&table_id, J2sError::InvalidInput(format!("flusher: no copy_sql for table '{table_id}'"))));
            };
            let buf = buffers.remove(&table_id).unwrap_or_default();
            let rows = pending_rows.remove(&table_id).unwrap_or(0);
            let bytes = buf.len() as u64;
            eprintln!("{}", format_copy_start_log(&table_id, buf.len(), rows));
            let (copy_sql, claim) = match target_of(&table_id) {
                None => (plain_sql, None),
                Some(t) => {
                    owned_bytes[conn_id] += bytes;
                    let claim = owner.insert(table_id.clone(), conn_id).is_none()
                        .then(|| (t.table.clone(), t.copy_sql.clone()));
                    (if frozen.contains(&table_id) { t.copy_sql.clone() } else { plain_sql }, claim)
                }
            };
            *in_flight.entry(table_id.clone()).or_insert(0) += 1;
            in_flight_bytes += bytes;
            tasks.spawn(async move {
                let start = std::time::Instant::now();
                let result = copy_task(&client, buf.freeze(), &copy_sql, claim).await;
                CopyOutcome { table_id, conn_id, rows, bytes, elapsed_ms: start.elapsed().as_millis(), client, result }
            });
        }
        idle.extend(no_work);

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
                let claimed = match outcome.result {
                    Ok(claimed) => claimed,
                    Err(e) => {
                        tasks.abort_all();
                        while tasks.join_next().await.is_some() {}
                        return Err(fail(&outcome.table_id, e));
                    }
                };
                if let Some(truncated) = claimed {
                    if truncated { frozen.insert(outcome.table_id.clone()); }
                    eprintln!("[FLUSHER] '{}' → connection {} ({})", outcome.table_id, outcome.conn_id,
                        if truncated { "COPY FREEZE" } else { "plain COPY: table not empty" });
                }
                eprintln!("{}", format_copy_done_log(&outcome.table_id, outcome.elapsed_ms));
                idle.push((outcome.conn_id, outcome.client));
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
    Ok((total_rows, idle.into_iter().map(|(_, c)| c).collect()))
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

    /// `rank` of a plain load: every table goes to any connection.
    #[allow(clippy::unnecessary_wraps)] // same signature as the `rank` of a FREEZE load
    fn any(_: &str) -> Option<bool> { Some(false) }

    #[test]
    fn freeze_load_serves_own_tables_first_and_never_another_connections() {
        let none = HashMap::new();
        let rank = |t: &str| match t { "theirs" => None, "mine" => Some(true), _ => Some(false) };
        let b = bufs(&[("root", 100), ("mine", 60), ("theirs", 200)]);
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Normal, 50, 360, &none, rank), Some("mine".into()));
        let b = bufs(&[("root", 100), ("theirs", 200)]);
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Normal, 50, 300, &none, rank), Some("root".into()));
        let b = bufs(&[("theirs", 200)]);
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Drain, 50, 200, &none, rank), None);
    }

    #[test]
    fn normal_mode_only_picks_tables_over_threshold() {
        let b = bufs(&[("small", 10), ("big", 100)]);
        let none = HashMap::new();
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Normal, 50, 110, &none, any), Some("big".into()));
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Normal, 500, 110, &none, any), None);
    }

    #[test]
    fn table_with_copy_in_flight_stays_eligible_and_empty_is_skipped() {
        // A dominant table must be able to use several connections at once.
        let b = bufs(&[("a", 100), ("b", 60), ("empty", 0)]);
        let none = HashMap::new();
        assert_eq!(next_flush_candidate(&b, &set(&["a"]), FlushMode::Normal, 50, 160, &none, any), Some("a".into()));
        let b = bufs(&[("empty", 0)]);
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Drain, 50, 0, &none, any), None);
    }

    #[test]
    fn paused_mode_picks_largest_only_while_idle_bytes_above_low() {
        let b = bufs(&[("a", 10), ("b", 20)]);
        let none = HashMap::new();
        let low = super::DEFAULT_LOW_FLUSHER_BYTES;
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Paused, 1000, low, &none, any), Some("b".into()));
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Paused, 1000, low - 1, &none, any), None);
        // threshold still applies below LOW
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Paused, 15, low - 1, &none, any), Some("b".into()));
    }

    #[test]
    fn drain_waits_for_parent_buffered_or_in_flight() {
        let parent_of: HashMap<String, String> = [("child".to_string(), "root".to_string())].into();
        // child is bigger, but root still buffered → root first
        let b = bufs(&[("root", 10), ("child", 100)]);
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Drain, 1000, 110, &parent_of, any), Some("root".into()));
        // root in flight → child blocked
        let b = bufs(&[("child", 100)]);
        assert_eq!(next_flush_candidate(&b, &set(&["root"]), FlushMode::Drain, 1000, 100, &parent_of, any), None);
        // root done → child allowed, even under threshold
        assert_eq!(next_flush_candidate(&b, &set(&[]), FlushMode::Drain, 1000, 100, &parent_of, any), Some("child".into()));
    }

    // -------------------------------------------------------------------------
    // run_flusher against PostgreSQL — skipped without TEST_DATABASE_URL
    // -------------------------------------------------------------------------

    /// Creates a scratch schema with tables `a`, `b` (a = parent of b), runs `run_flusher` with
    /// 3 connections over `batches`, then drops the schema. `bad` COPYs into a missing table.
    /// `freeze`: FREEZE targets of a FREEZE load, committed on success like the runner does —
    /// `["b"]` as under `replica` (children only), `["a", "b"]` as under `wal_level = minimal`.
    /// Empty: plain load.
    /// `prefill_b`: `b` already holds one row before the load.
    #[allow(clippy::too_many_lines)] // linear setup → run → count → teardown
    async fn run_flusher_on_scratch_schema(
        batches: &[(&str, &str)],
        threshold: u64,
        freeze: &[&str],
        prefill_b: bool,
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
        if prefill_b {
            admin.batch_execute(&format!("INSERT INTO {schema}.b VALUES (42)")).await.expect("prefill b");
        }
        let freeze_sql = (!freeze.is_empty()).then(|| freeze.iter().map(|t| (t.to_string(), super::FreezeTarget {
            table: format!("{schema}.{t}"),
            copy_sql: format!("COPY {schema}.{t} (n) FROM STDIN (FREEZE)"),
        })).collect::<HashMap<_, _>>());

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
            rx, copy_sql_map, freeze_sql, url, None, Arc::clone(&pause_flag), Arc::clone(&error_flag),
            threshold, 0.7, 0.5, false, parent_of, 3,
        )).await.expect("run_flusher must not hang");
        let result = match result {
            Ok((counts, clients)) => {
                for c in clients.iter().filter(|_| !freeze.is_empty()) {
                    c.batch_execute("COMMIT").await.expect("commit the FREEZE load");
                }
                Ok(counts)
            }
            Err(e) => Err(e), // connections dropped: a FREEZE load is rolled back
        };
        let rows: i64 = admin.query_one(&format!("SELECT (SELECT count(*) FROM {schema}.a) + (SELECT count(*) FROM {schema}.b)"), &[])
            .await.expect("count").get(0);
        admin.batch_execute(&format!("DROP SCHEMA {schema} CASCADE")).await.expect("drop scratch schema");
        Some((result, error_flag.load(Ordering::Acquire), pause_flag.load(Ordering::Acquire), rows))
    }

    #[tokio::test]
    async fn run_flusher_parallel_drain_copies_every_table() {
        let Some((result, errored, paused, rows)) =
            run_flusher_on_scratch_schema(&[("a", "1\n"), ("b", "2\n"), ("a", "3\n")], 1 << 30, &[], false).await else { return };
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
        let Some((result, errored, paused, rows)) = run_flusher_on_scratch_schema(&batches, 1, &[], false).await else { return };
        let counts = result.expect("streaming flushes must succeed");
        assert_eq!(counts.get("a"), Some(&13));
        assert_eq!(counts.get("b"), Some(&7));
        assert_eq!(rows, 20, "every row must reach PG exactly once");
        assert!(!errored && !paused);
    }

    #[tokio::test]
    async fn run_flusher_copy_error_returns_err_and_releases_workers() {
        let Some((result, errored, paused, _)) =
            run_flusher_on_scratch_schema(&[("a", "1\n"), ("bad", "2\n"), ("b", "3\n")], 1 << 30, &[], false).await else { return };
        let err = result.expect_err("COPY into a missing table must fail the flusher");
        assert!(err.to_string().contains("missing"), "error must carry the PG cause: {err}");
        assert!(errored, "error_flag must be set so workers stop");
        assert!(!paused, "pause_flag must be cleared so paused workers are released");
    }

    #[tokio::test]
    async fn run_flusher_freeze_load_commits_every_row() {
        // threshold = 1: many COPYs per table. `["b"]`: the root `a` spreads over the connections
        // (replica); `["a", "b"]`: each table stays on the connection that claimed it (minimal).
        let batches: Vec<(&str, &str)> = (0..20).map(|i| (if i % 3 == 0 { "b" } else { "a" }, "1\n")).collect();
        for targets in [&["b"][..], &["a", "b"]] {
            let Some((result, errored, _, rows)) = run_flusher_on_scratch_schema(&batches, 1, targets, false).await else { return };
            let counts = result.expect("FREEZE load must succeed");
            assert_eq!(counts.get("a"), Some(&13), "{targets:?}");
            assert_eq!(counts.get("b"), Some(&7), "{targets:?}");
            assert_eq!(rows, 20, "every row must be committed exactly once ({targets:?})");
            assert!(!errored);
        }
    }

    #[tokio::test]
    async fn run_flusher_freeze_never_truncates_a_table_with_rows() {
        let Some((result, _, _, rows)) =
            run_flusher_on_scratch_schema(&[("a", "1\n"), ("b", "2\n"), ("b", "3\n")], 1 << 30, &["a", "b"], true).await else { return };
        result.expect("a non-empty child falls back to a plain COPY");
        assert_eq!(rows, 4, "the row already in `b` must survive next to the 3 loaded ones");
    }

    #[tokio::test]
    async fn run_flusher_freeze_error_rolls_the_whole_load_back() {
        let Some((result, errored, _, rows)) =
            run_flusher_on_scratch_schema(&[("a", "1\n"), ("b", "2\n"), ("bad", "3\n")], 1, &["a", "b"], false).await else { return };
        result.expect_err("COPY into a missing table must fail the flusher");
        assert!(errored);
        assert_eq!(rows, 0, "a failed FREEZE load must leave nothing behind");
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
