use std::path::Path;

use crate::cli::AnomalyFormat;
use crate::error::{J2sError, Result};

use super::collector::AnomalyCollector;

/// Write the anomaly report to a file or stdout.
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
            std::fs::write(path, content.as_bytes())
                .map_err(J2sError::Io)?;
            eprintln!("Anomaly report written to: {}", path.display());
        }
        None => {
            print!("{}", content);
        }
    }

    Ok(())
}

fn to_json(collector: &AnomalyCollector) -> Result<String> {
    #[derive(serde::Serialize)]
    struct Report<'a> {
        summaries: Vec<super::collector::AnomalySummary>,
        entries: &'a [super::collector::AnomalyEntry],
        total_anomalies: u64,
        overall_anomaly_rate: f64,
    }

    let report = Report {
        summaries: collector.summaries(),
        entries: collector.entries(),
        total_anomalies: collector.total_anomalies(),
        overall_anomaly_rate: collector.overall_anomaly_rate(),
    };

    serde_json::to_string_pretty(&report)
        .map_err(|e| J2sError::AnomalyReport(e.to_string()))
}

fn to_csv(collector: &AnomalyCollector) -> Result<String> {
    let mut wtr = csv::Writer::from_writer(vec![]);

    wtr.write_record(&[
        "table",
        "column",
        "row_id",
        "expected_type",
        "actual_value",
        "actual_type",
    ])
    .map_err(|e| J2sError::AnomalyReport(e.to_string()))?;

    for entry in collector.entries() {
        wtr.write_record(&[
            &entry.table,
            &entry.column,
            &entry.row_id,
            &entry.expected_type,
            &entry.actual_value,
            &entry.actual_type,
        ])
        .map_err(|e| J2sError::AnomalyReport(e.to_string()))?;
    }

    let bytes = wtr
        .into_inner()
        .map_err(|e| J2sError::AnomalyReport(e.to_string()))?;

    String::from_utf8(bytes).map_err(|e| J2sError::AnomalyReport(e.to_string()))
}
