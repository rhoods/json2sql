use std::path::Path;

use serde_json::Value;

use crate::error::Result;
use crate::io::progress::ProgressTracker;
use crate::io::reader::{file_size, JsonReader};
use crate::schema::naming::{ColumnCollision, TruncatedName};
use crate::schema::registry::SchemaRegistry;
use crate::schema::stats::ColumnStats;
use crate::schema::table_schema::TableSchema;

/// Result of Pass 1.
pub struct Pass1Result {
    pub schemas: Vec<TableSchema>,
    pub total_rows: u64,
    pub stats: Vec<ColumnStats>,
    /// Table names that were truncated to fit the 63-byte PostgreSQL limit.
    pub truncated_names: Vec<TruncatedName>,
    /// Column name collisions resolved by hash suffix (multiple JSON fields → same SQL identifier).
    pub column_collisions: Vec<ColumnCollision>,
}

/// Run Pass 1: stream through the entire file and build the schema.
/// Returns finalized table schemas sorted topologically.
pub fn run(
    path: &Path,
    root_table: &str,
    text_threshold: u32,
    array_as_pg_array: bool,
    wide_column_threshold: usize,
    sibling_threshold: usize,
    sibling_jaccard: f64,
    stable_threshold: f64,
    rare_threshold: f64,
) -> Result<Pass1Result> {
    let total_bytes = file_size(path)?;
    let progress = ProgressTracker::new(total_bytes, "Pass 1");

    let mut registry = SchemaRegistry::new(text_threshold, array_as_pg_array, wide_column_threshold, sibling_threshold, sibling_jaccard, stable_threshold, rare_threshold);
    let (mut reader, _format) = JsonReader::open(path)?;

    let mut total_rows = 0u64;

    while let Some(item) = reader.next() {
        let value = item?;
        match value {
            Value::Object(ref obj) => {
                registry.observe_root(root_table, obj);
                total_rows += 1;
            }
            other => {
                return Err(crate::error::J2sError::InvalidInput(format!(
                    "Expected JSON object at root level, found: {}",
                    other
                )));
            }
        }
        progress.inc_rows(1);
        progress.set_bytes(reader.bytes_read());
    }

    progress.finish();
    eprintln!("Pass 1 complete: {} rows, building schema...", total_rows);

    let schemas = registry.finalize();
    let stats = registry.collect_stats();
    let truncated_names = registry.truncated_names().to_vec();
    let column_collisions = registry.column_collisions().to_vec();

    eprintln!(
        "Schema: {} tables, {} total columns",
        schemas.len(),
        schemas.iter().map(|s| s.columns.len()).sum::<usize>()
    );

    Ok(Pass1Result { schemas, total_rows, stats, truncated_names, column_collisions })
}
