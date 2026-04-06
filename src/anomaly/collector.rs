use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use serde::Serialize;

use crate::error::{J2sError, Result};

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
struct ColumnStats {
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
    /// Up to MAX_EXAMPLES representative anomaly entries.
    pub examples: Vec<AnomalyExample>,
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
    /// Per-(table, col) stats: count + capped examples + expected_type.
    stats: HashMap<(String, String), ColumnStats>,
    /// Per-table total row counts (denominator for anomaly rate).
    totals: HashMap<String, u64>,
    /// Fast total anomaly counter (avoids summing stats values each time).
    total_count: u64,
    /// Lazy-created streaming writers: table_name → writer.
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
            .finish()
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
            .or_insert_with(|| ColumnStats {
                expected_type: expected_type.to_string(),
                count: 0,
                examples: Vec::new(),
            });
        col_stats.count += 1;

        // Only pay the truncation cost when we still need an example or will stream to file.
        let needs_value = col_stats.examples.len() < MAX_EXAMPLES || self.anomaly_dir.is_some();
        let (truncated, char_len) = if needs_value {
            let len = actual_value.chars().count();
            (truncate_value(actual_value, 200), len)
        } else {
            (String::new(), 0)
        };

        if col_stats.examples.len() < MAX_EXAMPLES {
            col_stats.examples.push(AnomalyExample {
                row_id: row_id.to_string(),
                actual_value: truncated.clone(),
                actual_value_len: char_len,
                actual_type: actual_type.to_string(),
            });
        }
        self.total_count += 1;

        // Stream to NDJSON file if a directory was configured
        if let Some(ref dir) = self.anomaly_dir {
            if !self.writers.contains_key(table) {
                let safe_name = sanitize_table_name(table);
                let path = dir.join(format!("{}_anomalies.ndjson", safe_name));
                let file = File::create(&path).map_err(J2sError::Io)?;
                self.writers
                    .insert(table.to_string(), BufWriter::new(file));
                self.written_files.insert(table.to_string(), path);
            }
            let writer = self.writers.get_mut(table).unwrap();

            // One JSON object per line — serde_json escapes all control chars
            // including null bytes (\u0000), so the output is always valid UTF-8.
            let line = serde_json::json!({
                "table": table,
                "column": column,
                "row_id": row_id,
                "expected_type": expected_type,
                "actual_value": truncated,
                "actual_value_len": char_len,
                "actual_type": actual_type,
            });
            writeln!(writer, "{}", line).map_err(J2sError::Io)?;
        }

        Ok(())
    }

    /// Increment the total-row counter for a table (used as anomaly-rate denominator).
    pub fn inc_total(&mut self, table: &str) {
        *self.totals.entry(table.to_string()).or_insert(0) += 1;
    }

    /// Total anomaly count across all tables (O(1)).
    pub fn total_anomalies(&self) -> u64 {
        self.total_count
    }

    /// Per-(table, column) summaries including capped examples.
    /// O(n_columns) — not O(n_anomalies).
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
    /// Reserved for future IHM use; not currently displayed in the CLI summary.
    #[allow(dead_code)]
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
    pub fn written_paths(&self) -> &HashMap<String, PathBuf> {
        &self.written_files
    }
}

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
}
