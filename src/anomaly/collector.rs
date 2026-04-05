use serde::Serialize;
use std::collections::HashMap;

/// A single anomaly: a value that couldn't be coerced to the column's dominant type.
#[derive(Debug, Clone, Serialize)]
pub struct AnomalyEntry {
    pub table: String,
    pub column: String,
    /// j2s_id of the row that contained the anomaly (as a string for JSON serialization)
    pub row_id: String,
    /// The dominant/expected type for this column
    pub expected_type: String,
    /// String representation of the actual value, truncated to 200 chars if longer.
    pub actual_value: String,
    /// Original byte length of `actual_value` before truncation.
    /// If equal to `actual_value.len()`, no truncation occurred.
    pub actual_value_len: usize,
    /// Detected type of the actual value
    pub actual_type: String,
}

/// Summary statistics per (table, column) pair.
#[derive(Debug, Clone, Serialize)]
pub struct AnomalySummary {
    pub table: String,
    pub column: String,
    pub expected_type: String,
    pub anomaly_count: u64,
    pub total_rows: u64,
    pub anomaly_rate: f64,
}

/// Collects anomaly entries during Pass 2.
#[derive(Debug, Default)]
pub struct AnomalyCollector {
    entries: Vec<AnomalyEntry>,
    /// (table, column) → count
    counts: HashMap<(String, String), u64>,
    /// (table, column) → total rows
    totals: HashMap<String, u64>,
}

impl AnomalyCollector {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(
        &mut self,
        table: &str,
        column: &str,
        row_id: &str,
        expected_type: &str,
        actual_value: &str,
        actual_type: &str,
    ) {
        self.entries.push(AnomalyEntry {
            table: table.to_string(),
            column: column.to_string(),
            row_id: row_id.to_string(),
            expected_type: expected_type.to_string(),
            actual_value_len: actual_value.len(),
            actual_value: truncate_value(actual_value, 200),
            actual_type: actual_type.to_string(),
        });
        *self
            .counts
            .entry((table.to_string(), column.to_string()))
            .or_insert(0) += 1;
    }

    pub fn inc_total(&mut self, table: &str) {
        *self.totals.entry(table.to_string()).or_insert(0) += 1;
    }

    pub fn entries(&self) -> &[AnomalyEntry] {
        &self.entries
    }

    pub fn total_anomalies(&self) -> u64 {
        self.entries.len() as u64
    }

    /// Compute per-(table, column) summaries.
    pub fn summaries(&self) -> Vec<AnomalySummary> {
        self.counts
            .iter()
            .map(|((table, col), &count)| {
                let total = *self.totals.get(table).unwrap_or(&0);
                let rate = if total > 0 {
                    count as f64 / total as f64
                } else {
                    0.0
                };
                // Retrieve expected type from first matching entry
                let expected_type = self
                    .entries
                    .iter()
                    .find(|e| &e.table == table && &e.column == col)
                    .map(|e| e.expected_type.clone())
                    .unwrap_or_default();
                AnomalySummary {
                    table: table.clone(),
                    column: col.clone(),
                    expected_type,
                    anomaly_count: count,
                    total_rows: total,
                    anomaly_rate: rate,
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
        self.entries.len() as f64 / total as f64
    }
}

fn truncate_value(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        format!("{}…", &s[..max])
    }
}
