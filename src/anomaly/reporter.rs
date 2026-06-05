//! Anomaly summary report: serializes aggregated anomaly counts to JSON or CSV.
//!
//! Individual anomaly rows live in the per-table NDJSON files written by
//! [`crate::anomaly::collector`]. This module produces the summary (table → total count)
//! written to `--anomaly-report`.

use std::path::Path;

use crate::error::{J2sError, Result};

/// Output format for the anomaly summary report.
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum AnomalyFormat {
    Json,
    Csv,
}

use super::collector::AnomalyCollector;

/// Write the anomaly summary report to a file or stdout.
/// Individual anomaly rows are in the per-table NDJSON files produced
/// during Pass 2 (see `--anomaly-dir`).
pub fn write_report(
    collector: &AnomalyCollector,
    format: &AnomalyFormat,
    output: Option<&Path>,
) -> Result<()> {
    let content = match format {
        AnomalyFormat::Json => to_json(collector)?,
        AnomalyFormat::Csv => to_csv(collector)?,
    };

    match output {
        Some(path) => {
            std::fs::write(path, content.as_bytes()).map_err(J2sError::Io)?;
            eprintln!("Anomaly summary report written to: {}", path.display());
        }
        None => {
            print!("{}", content);
        }
    }

    Ok(())
}

fn sort_summaries(summaries: &mut Vec<super::collector::AnomalySummary>) {
    summaries.sort_by(|a, b| {
        b.anomaly_count
            .cmp(&a.anomaly_count)
            .then(a.table.cmp(&b.table))
            .then(a.column.cmp(&b.column))
    });
}

fn write_summary_row(
    wtr: &mut csv::Writer<Vec<u8>>,
    s: &super::collector::AnomalySummary,
) -> Result<()> {
    let example_val = s.examples.first().map(|e| e.actual_value.as_str()).unwrap_or("");
    let example_type = s.examples.first().map(|e| e.actual_type.as_str()).unwrap_or("");
    wtr.write_record([
        &s.table, &s.column, &s.expected_type,
        &s.anomaly_count.to_string(), &s.total_rows.to_string(),
        &format!("{:.4}", s.anomaly_rate * 100.0),
        example_val, example_type,
    ]).map_err(|e| J2sError::AnomalyReport(e.to_string()))
}

fn to_json(collector: &AnomalyCollector) -> Result<String> {
    #[derive(serde::Serialize)]
    struct Report {
        summaries: Vec<super::collector::AnomalySummary>,
        total_anomalies: u64,
        overall_anomaly_rate: f64,
    }

    let mut summaries = collector.summaries();
    sort_summaries(&mut summaries);

    let report = Report {
        summaries,
        total_anomalies: collector.total_anomalies(),
        overall_anomaly_rate: collector.overall_anomaly_rate(),
    };

    serde_json::to_string_pretty(&report)
        .map_err(|e| J2sError::AnomalyReport(e.to_string()))
}

fn to_csv(collector: &AnomalyCollector) -> Result<String> {
    let mut wtr = csv::Writer::from_writer(vec![]);
    wtr.write_record([
        "table", "column", "expected_type", "anomaly_count",
        "total_rows", "anomaly_rate_pct", "example_value", "example_type",
    ]).map_err(|e| J2sError::AnomalyReport(e.to_string()))?;

    let mut summaries = collector.summaries();
    sort_summaries(&mut summaries);
    for s in &summaries {
        write_summary_row(&mut wtr, s)?;
    }

    let bytes = wtr.into_inner().map_err(|e| J2sError::AnomalyReport(e.to_string()))?;
    String::from_utf8(bytes).map_err(|e| J2sError::AnomalyReport(e.to_string()))
}
