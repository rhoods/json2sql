//! Serialization of Pass 1 results to/from a JSON snapshot file (the "inspect" output).
//!
//! [`SchemaSnapshot`] wraps schemas, column stats, and user strategy overrides.
//! `SCHEMA_FORMAT_VERSION` is bumped when the on-disk format changes in a
//! backwards-incompatible way — callers must discard and regenerate old snapshots.

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{J2sError, Result};
use crate::schema::finalizer::OverflowWarning;
use crate::schema::naming::{ColumnCollision, TruncatedName};
use crate::schema::stats::ColumnStats;
use crate::schema::table_schema::{TableSchema, WideStrategy};

/// v2: table names are now capped at 53 chars (`PG_TABLE_MAX_IDENT`) instead of 63,
/// ensuring all derived identifiers (pk_, fk_..._parent, j2s_..._id) fit within
/// `PostgreSQL`'s 63-byte NAMEDATALEN limit. Snapshots from v1 must be regenerated.
const SCHEMA_FORMAT_VERSION: u32 = 2;

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
    /// Tables converted to JSONB due to column overflow. Absent in pre-fix snapshots
    /// — deserialized as empty vec via `serde(default)`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overflow_warnings: Vec<OverflowWarning>,
}

/// Save a Pass 1 result to a JSON file.
pub fn save(
    schemas: &[TableSchema],
    total_rows: u64,
    truncated_names: &[TruncatedName],
    column_collisions: &[ColumnCollision],
    stats: &[ColumnStats],
    overflow_warnings: &[OverflowWarning],
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
        overflow_warnings: overflow_warnings.to_vec(),
    };
    let json = serde_json::to_string_pretty(&snapshot)
        .map_err(|e| J2sError::InvalidInput(format!("Schema serialization failed: {e}")))?;
    std::fs::write(path, json).map_err(J2sError::Io)?;
    Ok(())
}

/// Save a schema snapshot including user strategy overrides.
#[allow(dead_code)]
pub fn save_with_overrides(
    schemas: &[TableSchema],
    total_rows: u64,
    truncated_names: &[TruncatedName],
    column_collisions: &[ColumnCollision],
    stats: &[ColumnStats],
    overflow_warnings: &[OverflowWarning],
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
        overflow_warnings: overflow_warnings.to_vec(),
    };
    let json = serde_json::to_string_pretty(&snapshot)
        .map_err(|e| J2sError::InvalidInput(format!("Schema serialization failed: {e}")))?;
    std::fs::write(path, json).map_err(J2sError::Io)?;
    Ok(())
}

/// Load a Pass 1 result from a previously saved JSON snapshot.
pub fn load(path: &Path) -> Result<SchemaSnapshot> {
    let data = std::fs::read(path).map_err(J2sError::Io)?;
    let snapshot: SchemaSnapshot = serde_json::from_slice(&data)
        .map_err(|e| J2sError::InvalidInput(format!("Schema deserialization failed: {e}")))?;
    if snapshot.version != SCHEMA_FORMAT_VERSION {
        if snapshot.version == 1 {
            return Err(J2sError::InvalidInput(
                "Schema snapshot v1 is no longer supported — table names used a wider budget \
                 that could cause PostgreSQL constraint name collisions. \
                 Please re-run Pass 1 to regenerate a v2 snapshot.".to_string()
            ));
        }
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
            version: SCHEMA_FORMAT_VERSION,
            total_rows: 0,
            schemas: vec![],
            truncated_names: vec![],
            column_collisions: vec![],
            stats: vec![],
            strategy_overrides: HashMap::new(),
            overflow_warnings: vec![],
        }
    }

    #[test]
    fn strategy_overrides_round_trips() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut overrides = HashMap::new();
        overrides.insert("my_table".to_string(), WideStrategy::Jsonb);

        save_with_overrides(&[], 0, &[], &[], &[], &[], &overrides, tmp.path()).unwrap();
        let loaded = load(tmp.path()).unwrap();

        assert!(matches!(
            loaded.strategy_overrides.get("my_table"),
            Some(WideStrategy::Jsonb)
        ));
    }

    #[test]
    fn v1_snapshot_returns_clear_error() {
        // v1 snapshots used a 63-char table name budget; v2 uses 53. They are incompatible.
        let json = r#"{"version":1,"total_rows":0,"schemas":[],"truncated_names":[],"column_collisions":[],"stats":[]}"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();

        let err = load(tmp.path()).err().expect("expected an error for v1 snapshot");
        let msg = err.to_string();
        assert!(msg.contains("v1"), "error should mention v1, got: {}", msg);
        assert!(msg.contains("Pass 1") || msg.contains("re-run") || msg.contains("regenerate"),
            "error should tell user to re-run Pass 1, got: {}", msg);
    }

    #[test]
    fn v2_snapshot_round_trips() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        save(&[], 42, &[], &[], &[], &[], tmp.path()).unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert_eq!(loaded.version, SCHEMA_FORMAT_VERSION);
        assert_eq!(loaded.total_rows, 42);
    }

    #[test]
    fn strategy_overrides_default_empty_on_new_snapshot() {
        let s = empty_snapshot();
        assert!(s.strategy_overrides.is_empty());
    }

    #[test]
    fn overflow_warnings_round_trip() {
        use crate::schema::finalizer::OverflowWarning;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let warnings = vec![
            OverflowWarning { table_name: "wide_table".to_string(), original_column_count: 1700 },
            OverflowWarning { table_name: "another_wide".to_string(), original_column_count: 1800 },
        ];
        save(&[], 0, &[], &[], &[], &warnings, tmp.path()).unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert_eq!(loaded.overflow_warnings.len(), 2);
        assert_eq!(loaded.overflow_warnings[0].table_name, "wide_table");
        assert_eq!(loaded.overflow_warnings[0].original_column_count, 1700);
        assert_eq!(loaded.overflow_warnings[1].table_name, "another_wide");
    }

    #[test]
    fn overflow_warnings_default_empty_on_old_snapshot() {
        // Snapshots written before this fix have no overflow_warnings field → default to empty.
        let json = r#"{"version":2,"total_rows":0,"schemas":[],"truncated_names":[],"column_collisions":[],"stats":[]}"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert!(loaded.overflow_warnings.is_empty(), "old snapshots must deserialize with empty overflow_warnings");
    }
}
