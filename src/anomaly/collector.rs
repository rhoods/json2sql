//! Anomaly collection during Pass 2: gathers type coercion failures and dropped keys.
#![allow(clippy::cast_precision_loss)]
//!
//! [`AnomalyCollector`] is the central aggregator; workers communicate with it via
//! [`AnomalyProxy`] (a channel-based handle). Per-table NDJSON anomaly files are written
//! as events arrive — the full set is never buffered in memory, so large anomaly volumes
//! don't cause OOM. Both types implement the `AnomalyCollect` trait (`record`/`inc_total`).
//!
//! Fonctions :
//! - enum `AnomalyEvent` — événement envoyé d'un worker vers la tâche writer (Record/IncTotal).
//! - struct `AnomalyProxy` — poignée channel-based vers le collecteur central.
//! - fn `AnomalyProxy::new` — crée le proxy channel-based.
//! - fn `AnomalyProxy::record` — implémente `AnomalyCollect` en envoyant un `AnomalyEvent::Record`.
//! - fn `AnomalyProxy::inc_total` — implémente `AnomalyCollect` en envoyant un `AnomalyEvent::IncTotal`.
//! - struct `AnomalyExample` — une anomalie exemple conservée en mémoire pour affichage IHM.
//! - struct `ColAnomalyStat` — statistiques par (table, colonne), tenues entièrement en RAM.
//! - struct `AnomalySummary` — résumé sérialisable par (table, colonne) pour les rapports.
//! - struct `AnomalyEntry` — entrée interne empaquetant les champs d'une ligne NDJSON à écrire.
//! - struct `AnomalyCollector` — collecteur en mémoire + streaming NDJSON optionnel par table.
//! - fn `AnomalyCollector::fmt` — implémente `std::fmt::Debug` sans exposer les writers ouverts.
//! - fn `AnomalyCollector::drop` — flush best-effort si `finish()` n'a pas été appelé explicitement.
//! - fn `AnomalyCollector::new` — crée le collecteur (streaming NDJSON optionnel si `anomaly_dir` est fourni).
//! - fn `AnomalyCollector::record` — enregistre une anomalie (stats en mémoire + ligne NDJSON si activé).
//! - fn `AnomalyCollector::stream_to_file` — écrit une ligne NDJSON, crée le fichier per-table au premier appel.
//! - fn `AnomalyCollector::inc_total` — incrémente le compteur de lignes total par table (dénominateur du taux).
//! - fn `AnomalyCollector::total_anomalies` — nombre total d'anomalies (O(1)).
//! - fn `AnomalyCollector::per_table_anomaly_counts` — total d'anomalies par table.
//! - fn `AnomalyCollector::summaries` — résumés par (table, colonne) avec exemples plafonnés.
//! - fn `AnomalyCollector::overall_anomaly_rate` — taux d'anomalie global (pour `--max-anomaly-rate`).
//! - fn `AnomalyCollector::finish` — flush tous les writers NDJSON (idempotent).
//! - fn `AnomalyCollector::written_paths` — chemins des fichiers NDJSON réellement écrits.
//! - fn `AnomalyCollector::record` — implémente `AnomalyCollect` en délégant à la méthode inhérente `record`.
//! - fn `AnomalyCollector::inc_total` — implémente `AnomalyCollect` en délégant à la méthode inhérente `inc_total`.
//! - fn `sanitize_table_name` — nom de table sûr comme composant de chemin de fichier.
//! - fn `truncate_value` — tronque une valeur à N caractères (affichage/NDJSON).

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use serde::Serialize;
use tokio::sync::mpsc::UnboundedSender;

use crate::error::{J2sError, Result};

// ---------------------------------------------------------------------------
// Protocol — trait, event enum, and channel-based proxy
// ---------------------------------------------------------------------------

/// One anomaly event sent from a worker to the central writer task.
#[derive(Debug)]
pub enum AnomalyEvent {
    Record {
        table: String,
        column: String,
        row_id: String,
        expected_type: String,
        actual_value: String,
        actual_type: String,
    },
    IncTotal {
        table: String,
    },
}

/// Abstraction over anomaly collection — either in-process (`AnomalyCollector`)
/// or cross-task via channel (`AnomalyProxy`).
pub trait AnomalyCollect {
    fn record(
        &mut self,
        table: &str,
        column: &str,
        row_id: &str,
        expected_type: &str,
        actual_value: &str,
        actual_type: &str,
    ) -> Result<()>;

    fn inc_total(&mut self, table: &str);
}

/// Sends anomaly events to a writer task via an unbounded channel.
/// Used by parallel workers — no blocking, no file I/O in worker threads.
pub struct AnomalyProxy {
    tx: UnboundedSender<AnomalyEvent>,
}

impl AnomalyProxy {
    #[must_use]
    pub const fn new(tx: UnboundedSender<AnomalyEvent>) -> Self {
        Self { tx }
    }
}

impl AnomalyCollect for AnomalyProxy {
    fn record(
        &mut self,
        table: &str,
        column: &str,
        row_id: &str,
        expected_type: &str,
        actual_value: &str,
        actual_type: &str,
    ) -> Result<()> {
        self.tx
            .send(AnomalyEvent::Record {
                table: table.to_string(),
                column: column.to_string(),
                row_id: row_id.to_string(),
                expected_type: expected_type.to_string(),
                actual_value: actual_value.to_string(),
                actual_type: actual_type.to_string(),
            })
            .map_err(|e| J2sError::AnomalyReport(e.to_string()))
    }

    fn inc_total(&mut self, table: &str) {
        let _ = self.tx.send(AnomalyEvent::IncTotal {
            table: table.to_string(),
        });
    }
}

// ---------------------------------------------------------------------------
// Collector — in-process implementation
// ---------------------------------------------------------------------------

/// Maximum number of examples stored per (table, column) pair.
/// Beyond this cap, anomalies are still counted and streamed to file
/// but not kept in memory.
const MAX_EXAMPLES: usize = 5;

/// One example anomaly kept in memory for IHM display.
#[derive(Debug, Clone, Serialize)]
pub struct AnomalyExample {
    pub row_id: String,
    /// String representation of the actual value, truncated to 200 chars.
    pub actual_value: String,
    /// Original character length before truncation.
    pub actual_value_len: usize,
    pub actual_type: String,
}

/// Per-(table, column) statistics, held entirely in RAM.
#[derive(Debug, Clone)]
struct ColAnomalyStat {
    expected_type: String,
    count: u64,
    examples: Vec<AnomalyExample>,
}

/// Summary statistics per (table, column) pair — serialisable for reports.
#[derive(Debug, Clone, Serialize)]
pub struct AnomalySummary {
    pub table: String,
    pub column: String,
    pub expected_type: String,
    pub anomaly_count: u64,
    pub total_rows: u64,
    pub anomaly_rate: f64,
    /// Up to `MAX_EXAMPLES` representative anomaly entries.
    pub examples: Vec<AnomalyExample>,
}

struct AnomalyEntry<'a> {
    table: &'a str,
    column: &'a str,
    row_id: &'a str,
    expected_type: &'a str,
    truncated_value: &'a str,
    char_len: usize,
    actual_type: &'a str,
}

/// Collects anomaly statistics during Pass 2 and optionally streams each
/// rejected row to a per-table NDJSON file for post-import investigation.
///
/// # File layout
///
/// When `anomaly_dir` is set, one file is created per table that has at
/// least one anomaly: `<anomaly_dir>/<table_name>_anomalies.ndjson`.
/// Each line is a JSON object with the fields below:
///
/// ```json
/// {"table":"products","column":"price","row_id":"019...","expected_type":"double precision","actual_value":"\"gratuit\"","actual_value_len":8,"actual_type":"string"}
/// ```
///
/// Tables with zero anomalies produce no file.
pub struct AnomalyCollector {
    /// Per-(table, col) stats: count + capped examples + `expected_type`.
    stats: HashMap<(String, String), ColAnomalyStat>,
    /// Per-table total row counts (denominator for anomaly rate).
    totals: HashMap<String, u64>,
    /// Fast total anomaly counter (avoids summing stats values each time).
    total_count: u64,
    /// Lazy-created streaming writers: `table_name` → writer.
    writers: HashMap<String, BufWriter<File>>,
    /// Directory for per-table NDJSON files. None = no file streaming.
    anomaly_dir: Option<PathBuf>,
    /// Paths of files actually written (populated as files are created).
    written_files: HashMap<String, PathBuf>,
    /// Set to true after `finish()` to prevent double-flush.
    finished: bool,
}

impl std::fmt::Debug for AnomalyCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnomalyCollector")
            .field("total_count", &self.total_count)
            .field("tables", &self.totals.len())
            .field("columns_with_anomalies", &self.stats.len())
            .field("anomaly_dir", &self.anomaly_dir)
            .field("open_writers", &self.writers.len())
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl Drop for AnomalyCollector {
    /// Best-effort flush on drop (e.g. when an error causes early return from Pass 2).
    /// Skipped if `finish()` already completed successfully.
    /// Errors are silently ignored — the caller already has a more important error to handle.
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        for writer in self.writers.values_mut() {
            let _ = writer.flush();
        }
    }
}

impl AnomalyCollector {
    /// Create a collector. Pass `anomaly_dir = Some(path)` to enable
    /// per-table NDJSON streaming; `None` keeps anomalies in-memory only
    /// (counters + examples, no unbounded Vec).
    #[must_use]
    pub fn new(anomaly_dir: Option<PathBuf>) -> Self {
        Self {
            stats: HashMap::new(),
            totals: HashMap::new(),
            total_count: 0,
            writers: HashMap::new(),
            anomaly_dir,
            written_files: HashMap::new(),
            finished: false,
        }
    }

    /// Record one anomaly. Updates in-memory counters and (if enabled)
    /// appends a line to the per-table NDJSON file.
    ///
    /// Returns `Err` only if file I/O fails. Callers must propagate the
    /// error — silently continuing would leave the anomaly file incomplete
    /// and give the user a false sense of completeness.
    pub fn record(
        &mut self,
        table: &str,
        column: &str,
        row_id: &str,
        expected_type: &str,
        actual_value: &str,
        actual_type: &str,
    ) -> Result<()> {
        // Update in-memory stats (O(1) per call)
        let col_stats = self
            .stats
            .entry((table.to_string(), column.to_string()))
            .or_insert_with(|| ColAnomalyStat {
                expected_type: expected_type.to_string(),
                count: 0,
                examples: Vec::new(),
            });
        col_stats.count += 1;

        // Capture an in-memory example if still below the cap.
        let want_example = col_stats.examples.len() < MAX_EXAMPLES;
        // Stream to file regardless of the example cap.
        let want_file = self.anomaly_dir.is_some();

        // Avoid the O(n) truncation scan when neither path needs the value.
        if want_example || want_file {
            let char_len = actual_value.chars().count();
            let truncated = truncate_value(actual_value, 200);

            if want_example {
                col_stats.examples.push(AnomalyExample {
                    row_id: row_id.to_string(),
                    actual_value: truncated.clone(),
                    actual_value_len: char_len,
                    actual_type: actual_type.to_string(),
                });
            }

            if want_file {
                self.stream_to_file(&AnomalyEntry {
                    table,
                    column,
                    row_id,
                    expected_type,
                    truncated_value: &truncated,
                    char_len,
                    actual_type,
                })?;
            }
        }
        self.total_count += 1;
        Ok(())
    }

    /// Append one NDJSON line to the per-table anomaly file, creating it if necessary.
    fn stream_to_file(&mut self, entry: &AnomalyEntry<'_>) -> Result<()> {
        let dir = self.anomaly_dir.as_ref().expect("called only when anomaly_dir is Some");
        if !self.writers.contains_key(entry.table) {
            let safe_name = sanitize_table_name(entry.table);
            let path = dir.join(format!("{safe_name}_anomalies.ndjson"));
            let file = File::create(&path).map_err(J2sError::Io)?;
            self.writers.insert(entry.table.to_string(), BufWriter::new(file));
            self.written_files.insert(entry.table.to_string(), path);
        }
        let writer = self.writers.get_mut(entry.table)
            .expect("writer was inserted above in this same block");
        // serde_json::Map is infallible — avoids json! macro's internal unwrap.
        let mut obj = serde_json::Map::new();
        obj.insert("table".into(),            entry.table.into());
        obj.insert("column".into(),           entry.column.into());
        obj.insert("row_id".into(),           entry.row_id.into());
        obj.insert("expected_type".into(),    entry.expected_type.into());
        obj.insert("actual_value".into(),     entry.truncated_value.into());
        obj.insert("actual_value_len".into(), entry.char_len.into());
        obj.insert("actual_type".into(),      entry.actual_type.into());
        writeln!(writer, "{}", serde_json::Value::Object(obj)).map_err(J2sError::Io)
    }

    /// Increment the total-row counter for a table (used as anomaly-rate denominator).
    pub fn inc_total(&mut self, table: &str) {
        *self.totals.entry(table.to_string()).or_insert(0) += 1;
    }

    /// Total anomaly count across all tables (O(1)).
    #[must_use]
    pub const fn total_anomalies(&self) -> u64 {
        self.total_count
    }

    /// Per-table anomaly totals (sum across all columns). `O(n_columns)`.
    #[must_use]
    pub fn per_table_anomaly_counts(&self) -> HashMap<String, u64> {
        let mut out: HashMap<String, u64> = HashMap::new();
        for ((table, _col), cs) in &self.stats {
            *out.entry(table.clone()).or_default() += cs.count;
        }
        out
    }

    /// Per-(table, column) summaries including capped examples.
    /// `O(n_columns)` — not `O(n_anomalies)`.
    #[must_use]
    pub fn summaries(&self) -> Vec<AnomalySummary> {
        self.stats
            .iter()
            .map(|((table, col), cs)| {
                let total = *self.totals.get(table).unwrap_or(&0);
                let rate = if total > 0 {
                    cs.count as f64 / total as f64
                } else {
                    0.0
                };
                AnomalySummary {
                    table: table.clone(),
                    column: col.clone(),
                    expected_type: cs.expected_type.clone(),
                    anomaly_count: cs.count,
                    total_rows: total,
                    anomaly_rate: rate,
                    examples: cs.examples.clone(),
                }
            })
            .collect()
    }

    /// Overall anomaly rate across all tables.
    /// Used for `--max-anomaly-rate` threshold checks and the JSON anomaly report.
    #[must_use]
    pub fn overall_anomaly_rate(&self) -> f64 {
        let total: u64 = self.totals.values().sum();
        if total == 0 {
            return 0.0;
        }
        self.total_count as f64 / total as f64
    }

    /// Flush all open NDJSON writers. Idempotent — safe to call multiple times.
    /// Call explicitly after Pass 2 completes; `Drop` provides a best-effort
    /// flush on error paths.
    ///
    /// # Partial failure
    /// If this returns `Err`, some writers may have been flushed and others not.
    /// `finished` remains `false`, so `Drop` will attempt a best-effort re-flush.
    /// Files listed in `written_paths()` may be truncated in this case.
    pub fn finish(&mut self) -> Result<()> {
        if self.finished {
            return Ok(());
        }
        for writer in self.writers.values_mut() {
            writer.flush().map_err(J2sError::Io)?;
        }
        self.finished = true;
        Ok(())
    }

    /// Paths of NDJSON files produced so far (one per table with anomalies).
    #[allow(dead_code)]
    #[must_use]
    pub const fn written_paths(&self) -> &HashMap<String, PathBuf> {
        &self.written_files
    }
}

impl AnomalyCollect for AnomalyCollector {
    fn record(
        &mut self,
        table: &str,
        column: &str,
        row_id: &str,
        expected_type: &str,
        actual_value: &str,
        actual_type: &str,
    ) -> Result<()> {
        self.record(table, column, row_id, expected_type, actual_value, actual_type)
    }

    fn inc_total(&mut self, table: &str) {
        self.inc_total(table);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Replace any character that is not ASCII alphanumeric or `_` with `_` so
/// the table name is safe as a file-system component on all platforms.
///
/// In practice, table names produced by json2sql (`sanitize_pg_name`) are
/// already `[a-z0-9_]`, so this is a defensive last-resort guard — not a
/// primary sanitization layer. Using `is_ascii_alphanumeric` (not
/// `is_alphanumeric`) avoids Unicode characters that may be invalid or
/// platform-dependent on Windows/FAT32 file systems.
///
/// Collision note: if two distinct table names map to the same sanitized
/// name, the second `File::create` would overwrite the first. This cannot
/// happen with j2s-generated names (already ASCII-safe and unique), but
/// callers should not rely on this for externally-supplied table names.
fn sanitize_table_name(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect()
}

fn truncate_value(s: &str, max: usize) -> String {
    let mut chars = s.char_indices();
    match chars.nth(max) {
        None => s.to_string(),
        Some((i, _)) => format!("{}…", &s[..i]),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_counters_no_dir() {
        let mut c = AnomalyCollector::new(None);
        c.inc_total("products");
        c.inc_total("products");
        c.inc_total("products");
        c.record("products", "price", "row1", "double precision", "\"gratuit\"", "string").unwrap();

        assert_eq!(c.total_anomalies(), 1);
        let sums = c.summaries();
        assert_eq!(sums.len(), 1);
        assert_eq!(sums[0].anomaly_count, 1);
        assert_eq!(sums[0].total_rows, 3);
        assert!((sums[0].anomaly_rate - 1.0 / 3.0).abs() < 1e-9);
        assert_eq!(sums[0].expected_type, "double precision");
        assert_eq!(sums[0].examples.len(), 1);
    }

    #[test]
    fn test_example_cap() {
        let mut c = AnomalyCollector::new(None);
        for i in 0..10 {
            c.record("t", "col", &format!("row{}", i), "integer", "bad", "string").unwrap();
        }
        assert_eq!(c.total_anomalies(), 10);
        let sums = c.summaries();
        // Count is full
        assert_eq!(sums[0].anomaly_count, 10);
        // But examples are capped
        assert_eq!(sums[0].examples.len(), MAX_EXAMPLES);
    }

    #[test]
    fn test_streaming_to_file() {
        let dir = TempDir::new().unwrap();
        let mut c = AnomalyCollector::new(Some(dir.path().to_path_buf()));
        c.inc_total("products");
        c.record("products", "price", "row1", "double precision", "gratuit", "string").unwrap();
        c.record("products", "price", "row2", "double precision", "N/A", "string").unwrap();
        c.finish().unwrap();
        let files = c.written_paths();

        assert!(files.contains_key("products"));
        let path = &files["products"];
        let content = std::fs::read_to_string(path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);

        // Each line must be valid JSON
        for line in &lines {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(v["table"], "products");
            assert_eq!(v["column"], "price");
        }
    }

    #[test]
    fn test_no_file_for_clean_table() {
        let dir = TempDir::new().unwrap();
        let mut c = AnomalyCollector::new(Some(dir.path().to_path_buf()));
        c.inc_total("products");
        // No anomalies recorded
        c.finish().unwrap();
        assert!(c.written_paths().is_empty(), "no file should be created for a clean table");
    }

    #[test]
    fn test_overall_rate() {
        let mut c = AnomalyCollector::new(None);
        c.inc_total("t");
        c.inc_total("t");
        c.inc_total("t");
        c.inc_total("t");
        c.record("t", "x", "r1", "integer", "bad", "string").unwrap();
        c.record("t", "x", "r2", "integer", "bad", "string").unwrap();
        assert!((c.overall_anomaly_rate() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn per_table_anomaly_counts_aggregates_columns() {
        let mut c = AnomalyCollector::new(None);
        c.record("orders", "price", "r1", "float8", "bad", "string").unwrap();
        c.record("orders", "qty",   "r2", "int4",   "bad", "string").unwrap();
        c.record("users",  "age",   "r3", "int4",   "bad", "string").unwrap();
        let counts = c.per_table_anomaly_counts();
        assert_eq!(counts.get("orders").copied().unwrap_or(0), 2);
        assert_eq!(counts.get("users").copied().unwrap_or(0),  1);
        assert_eq!(counts.len(), 2);
    }

    #[test]
    fn collector_implements_anomaly_collect_record() {
        let mut c = AnomalyCollector::new(None);
        let result = AnomalyCollect::record(
            &mut c, "products", "price", "row1", "double precision", "gratuit", "string",
        );
        assert!(result.is_ok());
        assert_eq!(c.total_anomalies(), 1);
    }

    #[test]
    fn collector_implements_anomaly_collect_inc_total() {
        let mut c = AnomalyCollector::new(None);
        AnomalyCollect::inc_total(&mut c, "products");
        AnomalyCollect::inc_total(&mut c, "products");
        assert!((c.overall_anomaly_rate() - 0.0).abs() < 1e-9);
    }

    #[test]
    fn proxy_sends_record_event() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut proxy = AnomalyProxy::new(tx);
        proxy
            .record("orders", "qty", "r1", "integer", "bad", "string")
            .unwrap();
        let event = rx.try_recv().expect("event must be in channel");
        match event {
            AnomalyEvent::Record { table, column, row_id, expected_type, actual_value, actual_type } => {
                assert_eq!(table, "orders");
                assert_eq!(column, "qty");
                assert_eq!(row_id, "r1");
                assert_eq!(expected_type, "integer");
                assert_eq!(actual_value, "bad");
                assert_eq!(actual_type, "string");
            }
            _ => panic!("expected AnomalyEvent::Record"),
        }
    }

    #[test]
    fn proxy_sends_inc_total_event() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut proxy = AnomalyProxy::new(tx);
        proxy.inc_total("users");
        let event = rx.try_recv().expect("event must be in channel");
        match event {
            AnomalyEvent::IncTotal { table } => assert_eq!(table, "users"),
            _ => panic!("expected AnomalyEvent::IncTotal"),
        }
    }

    #[test]
    fn proxy_record_errors_when_channel_closed() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        drop(rx);
        let mut proxy = AnomalyProxy::new(tx);
        let result = proxy.record("t", "c", "r", "int4", "bad", "string");
        assert!(result.is_err(), "send on closed channel must return Err");
    }

    #[test]
    fn streaming_writes_all_ndjson_fields() {
        let dir = TempDir::new().unwrap();
        let mut c = AnomalyCollector::new(Some(dir.path().to_path_buf()));
        c.record("t1", "col_a", "row42", "integer", "hello!", "string").unwrap();
        c.finish().unwrap();
        let files = c.written_paths();
        let content = std::fs::read_to_string(&files["t1"]).unwrap();
        let v: serde_json::Value = serde_json::from_str(content.trim()).unwrap();
        assert_eq!(v["table"], "t1");
        assert_eq!(v["column"], "col_a");
        assert_eq!(v["row_id"], "row42");
        assert_eq!(v["expected_type"], "integer");
        assert_eq!(v["actual_value"], "hello!");
        assert_eq!(v["actual_value_len"], 6);
        assert_eq!(v["actual_type"], "string");
    }
}
