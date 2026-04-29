use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{J2sError, Result};
use crate::schema::naming::{ColumnCollision, TruncatedName};
use crate::schema::stats::ColumnStats;
use crate::schema::table_schema::{TableSchema, WideStrategy};

const SCHEMA_FORMAT_VERSION: u32 = 1;

/// Serializable snapshot of a Pass 1 result, optionally including user strategy overrides.
#[derive(Serialize, Deserialize)]
pub struct SchemaSnapshot {
    pub version: u32,
    pub total_rows: u64,
    pub schemas: Vec<TableSchema>,
    pub truncated_names: Vec<TruncatedName>,
    pub column_collisions: Vec<ColumnCollision>,
    pub stats: Vec<ColumnStats>,
    /// User strategy overrides applied via the IHM. Absent in snapshots saved by older
    /// versions (or by the CLI) — deserialized as an empty map via `serde(default)`.
    #[serde(default, skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub strategy_overrides: std::collections::HashMap<String, WideStrategy>,
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
        strategy_overrides: std::collections::HashMap::new(),
    };
    let json = serde_json::to_string_pretty(&snapshot)
        .map_err(|e| J2sError::InvalidInput(format!("Schema serialization failed: {}", e)))?;
    std::fs::write(path, json).map_err(J2sError::Io)?;
    Ok(())
}

/// Save a schema snapshot including user strategy overrides.
pub fn save_with_overrides(
    schemas: &[TableSchema],
    total_rows: u64,
    truncated_names: &[TruncatedName],
    column_collisions: &[ColumnCollision],
    stats: &[ColumnStats],
    strategy_overrides: &std::collections::HashMap<String, crate::schema::table_schema::WideStrategy>,
    path: &Path,
) -> Result<()> {
    let snapshot = SchemaSnapshot {
        version: SCHEMA_FORMAT_VERSION,
        total_rows,
        schemas: schemas.to_vec(),
        truncated_names: truncated_names.to_vec(),
        column_collisions: column_collisions.to_vec(),
        stats: stats.to_vec(),
        strategy_overrides: strategy_overrides.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::table_schema::WideStrategy;
    use std::collections::HashMap;

    fn empty_snapshot() -> SchemaSnapshot {
        SchemaSnapshot {
            version: 1,
            total_rows: 0,
            schemas: vec![],
            truncated_names: vec![],
            column_collisions: vec![],
            stats: vec![],
            strategy_overrides: HashMap::new(),
        }
    }

    #[test]
    fn strategy_overrides_round_trips() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut overrides = HashMap::new();
        overrides.insert("my_table".to_string(), WideStrategy::Jsonb);

        save_with_overrides(&[], 0, &[], &[], &[], &overrides, tmp.path()).unwrap();
        let loaded = load(tmp.path()).unwrap();

        assert!(matches!(
            loaded.strategy_overrides.get("my_table"),
            Some(WideStrategy::Jsonb)
        ));
    }

    #[test]
    fn old_snapshot_without_overrides_deserialises_as_empty() {
        // Simulate a snapshot saved before strategy_overrides was added.
        let json = r#"{"version":1,"total_rows":0,"schemas":[],"truncated_names":[],"column_collisions":[],"stats":[]}"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();

        let loaded = load(tmp.path()).unwrap();
        assert!(loaded.strategy_overrides.is_empty());
    }

    #[test]
    fn strategy_overrides_default_empty_on_new_snapshot() {
        let s = empty_snapshot();
        assert!(s.strategy_overrides.is_empty());
    }
}
