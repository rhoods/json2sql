use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{J2sError, Result};
use crate::schema::naming::{ColumnCollision, TruncatedName};
use crate::schema::stats::ColumnStats;
use crate::schema::table_schema::TableSchema;

const SCHEMA_FORMAT_VERSION: u32 = 1;

/// Serializable snapshot of a Pass 1 result.
#[derive(Serialize, Deserialize)]
pub struct SchemaSnapshot {
    pub version: u32,
    pub total_rows: u64,
    pub schemas: Vec<TableSchema>,
    pub truncated_names: Vec<TruncatedName>,
    pub column_collisions: Vec<ColumnCollision>,
    pub stats: Vec<ColumnStats>,
}

/// Save a Pass 1 result to a JSON file.
pub fn save(
    schemas: &[TableSchema],
    total_rows: u64,
    truncated_names: &[TruncatedName],
    column_collisions: &[ColumnCollision],
    stats: &[ColumnStats],
    path: &Path,
) -> Result<()> {
    let snapshot = SchemaSnapshot {
        version: SCHEMA_FORMAT_VERSION,
        total_rows,
        schemas: schemas.to_vec(),
        truncated_names: truncated_names.to_vec(),
        column_collisions: column_collisions.to_vec(),
        stats: stats.to_vec(),
    };
    let json = serde_json::to_string_pretty(&snapshot)
        .map_err(|e| J2sError::InvalidInput(format!("Schema serialization failed: {}", e)))?;
    std::fs::write(path, json).map_err(J2sError::Io)?;
    Ok(())
}

/// Load a Pass 1 result from a previously saved JSON snapshot.
pub fn load(path: &Path) -> Result<SchemaSnapshot> {
    let data = std::fs::read(path).map_err(J2sError::Io)?;
    let snapshot: SchemaSnapshot = serde_json::from_slice(&data)
        .map_err(|e| J2sError::InvalidInput(format!("Schema deserialization failed: {}", e)))?;
    if snapshot.version != SCHEMA_FORMAT_VERSION {
        return Err(J2sError::InvalidInput(format!(
            "Schema snapshot version {} is not supported (expected {})",
            snapshot.version, SCHEMA_FORMAT_VERSION
        )));
    }
    Ok(snapshot)
}
