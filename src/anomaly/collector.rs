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
        let char_len = actual_value.chars().count();
        let truncated = truncate_value(actual_value, 200);

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
                let path = dir.join(format!("{}_anomalies.ndjson", table));
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
    pub fn overall_anomaly_rate(&self) -> f64 {
        let total: u64 = self.totals.values().sum();
        if total == 0 {
            return 0.0;
        }
        self.total_count as f64 / total as f64
    }

    /// Flush all open NDJSON writers and return the paths of files produced.
    /// Call once after Pass 2 is complete.
    pub fn finish(&mut self) -> Result<HashMap<String, PathBuf>> {
        for writer in self.writers.values_mut() {
            writer.flush().map_err(J2sError::Io)?;
        }
        Ok(self.written_files.clone())
    }
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
        let files = c.finish().unwrap();

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
        let files = c.finish().unwrap();
        assert!(files.is_empty(), "no file should be created for a clean table");
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
