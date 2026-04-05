use crate::schema::type_tracker::PgType;

/// Type distribution for one column observed during Pass 1.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnStats {
    pub table_name: String,
    pub column_name: String,
    pub pg_type: PgType,
    pub total_count: u64,
    pub null_count: u64,
    /// Histogram of observed JSON types: [(type_label, count), ...]
    /// Empty for generated j2s_ columns.
    pub type_histogram: Vec<(String, u64)>,
}

impl ColumnStats {
    /// True if more than one JSON type was observed (potential anomalies).
    pub fn is_mixed(&self) -> bool {
        self.type_histogram.len() > 1
    }

    /// Non-null count.
    pub fn non_null_count(&self) -> u64 {
        self.total_count.saturating_sub(self.null_count)
    }
}

/// Write a human-readable schema statistics report to `writer`.
pub fn write_text_report(
    stats: &[ColumnStats],
    total_rows: u64,
    writer: &mut dyn std::io::Write,
) -> std::io::Result<()> {
    writeln!(writer, "=== Pass 1 Schema Statistics ===")?;
    writeln!(writer, "Total root rows: {}", total_rows)?;

    // Group by table
    let mut current_table = "";
    for col in stats {
        if col.table_name != current_table {
            writeln!(writer, "\nTable: {}", col.table_name)?;
            current_table = &col.table_name;
        }

        if col.is_mixed() {
            let histogram: Vec<String> = col
                .type_histogram
                .iter()
                .map(|(t, n)| format!("{} {}", n, t))
                .collect();
            writeln!(
                writer,
                "  {:<30} {:<22} {} non-null  ** MIXED: {} **",
                col.column_name,
                col.pg_type.as_sql(),
                col.non_null_count(),
                histogram.join(", ")
            )?;
        } else {
            writeln!(
                writer,
                "  {:<30} {:<22} {} non-null",
                col.column_name,
                col.pg_type.as_sql(),
                col.non_null_count(),
            )?;
        }
    }
    writeln!(writer)
}
