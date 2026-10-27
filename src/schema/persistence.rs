//! Serialization of Pass 1 results to/from a JSON snapshot file (the "inspect" output).
//!
//! [`SchemaSnapshot`] wraps schemas, column stats, and user strategy overrides.
//! `SCHEMA_FORMAT_VERSION` is bumped when the on-disk format changes in a
//! backwards-incompatible way — callers must discard and regenerate old snapshots.
//!
//! Fonctions :
//! - fn `format_version` — version à écrire pour des schémas : 2, ou 3 si une table est découpée en groupes de colonnes.
//! - struct `SchemaSnapshot` — snapshot sérialisable d'un résultat Pass 1 (schémas, stats, overrides).
//! - fn `write_snapshot` — sérialise et écrit sur disque (écriture atomique via `.tmp` + rename si demandé).
//! - fn `save` — construit un `SchemaSnapshot` sans overrides et l'écrit (non atomique).
//! - fn `save_with_overrides` — idem, avec les overrides de stratégie utilisateur (écriture atomique).
//! - fn `parse_snapshot` — désérialise un snapshot ; une table de jonction `ScalarArray` d'avant #84 donne
//!   un message dédié (relancer la Pass 1) au lieu de l'erreur serde brute.
//! - fn `load` — lit et désérialise un snapshot ; rejette les versions incompatibles (v1) ; migre
//!   les `strategy_overrides` racine legacy vers `ui_override` par table (sans écraser l'existant).

use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::error::{J2sError, Result};
use crate::io::reader::JsonFormat;
use crate::schema::finalizer::OverflowWarning;
use crate::schema::naming::{ColumnCollision, TruncatedName};
use crate::schema::stats::ColumnStats;
use crate::schema::table_schema::{TableSchema, UserOverride};

/// v2: table names are now capped at 53 chars (`PG_TABLE_MAX_IDENT`) instead of 63,
/// ensuring all derived identifiers (pk_, fk_..._parent, j2s_..._id) fit within
/// `PostgreSQL`'s 63-byte NAMEDATALEN limit. Snapshots from v1 must be regenerated.
pub const SCHEMA_FORMAT_VERSION: u32 = 2;

/// v3: a table was split into column groups (#78). Written only for such a schema, so that older programs refuse
/// the file instead of reading a parent without its moved columns and group tables that are never filled.
pub const COLUMN_GROUPS_FORMAT_VERSION: u32 = 3;

/// The format version to write for `schemas`: [`SCHEMA_FORMAT_VERSION`] unless a table has column groups.
#[must_use]
pub fn format_version(schemas: &[TableSchema]) -> u32 {
    if schemas.iter().any(|s| !s.column_groups.is_empty()) {
        COLUMN_GROUPS_FORMAT_VERSION
    } else {
        SCHEMA_FORMAT_VERSION
    }
}

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
    pub strategy_overrides: std::collections::HashMap<String, UserOverride>,
    /// Tables converted to JSONB due to column overflow. Absent in pre-fix snapshots
    /// — deserialized as empty vec via `serde(default)`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub overflow_warnings: Vec<OverflowWarning>,
    /// JSON format detected during Pass 1. Absent in older snapshots — deserialized as
    /// `None`, in which case pass2 re-detects the format by scanning the source file.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detected_format: Option<JsonFormat>,
    /// Tables the user ticked for column groups in the UI (#91): the groups are computed again at each load, they are
    /// never saved. Absent in older snapshots, which an older program reads as having no switch (no data is lost).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_columns: Vec<String>,
    /// Presence and values of the eligible wide tables in a bounded sample of the rows, taken during Pass 1 (#91,
    /// phase 2): the data signals of the column groups without reading the file again. An optional field: an older
    /// program ignores it and loses nothing, so the format version does not change.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub samples: Vec<crate::schema::grouping::sample::StoredSample>,
}

/// Serialize and write a snapshot to disk (atomic for `save_with_overrides`, direct for `save`).
pub fn write_snapshot(snapshot: &SchemaSnapshot, path: &Path, atomic: bool) -> Result<()> {
    let json = serde_json::to_string_pretty(snapshot)
        .map_err(|e| J2sError::InvalidInput(format!("Schema serialization failed: {e}")))?;
    if atomic {
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, &json).map_err(J2sError::Io)?;
        std::fs::rename(&tmp, path).map_err(J2sError::Io)?;
    } else {
        std::fs::write(path, json).map_err(J2sError::Io)?;
    }
    Ok(())
}

/// Save a Pass 1 result to a JSON file. `detected_format` must be the format
/// returned by `JsonReader::open` during pass1 — it is always known after a fresh run.
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)] // each param is a distinct snapshot field, mirrors SchemaSnapshot's own shape
pub fn save(
    schemas: &[TableSchema],
    total_rows: u64,
    truncated_names: &[TruncatedName],
    column_collisions: &[ColumnCollision],
    stats: &[ColumnStats],
    overflow_warnings: &[OverflowWarning],
    detected_format: JsonFormat,
    path: &Path,
) -> Result<()> {
    let snapshot = SchemaSnapshot {
        version: format_version(schemas),
        total_rows,
        schemas: schemas.to_vec(),
        truncated_names: truncated_names.to_vec(),
        column_collisions: column_collisions.to_vec(),
        stats: stats.to_vec(),
        strategy_overrides: std::collections::HashMap::new(),
        overflow_warnings: overflow_warnings.to_vec(),
        detected_format: Some(detected_format),
        group_columns: Vec::new(),
        samples: Vec::new(),
    };
    write_snapshot(&snapshot, path, false)
}

/// Save a schema snapshot including user strategy overrides.
/// `detected_format` must be the format detected during pass1 — always known at IHM save time.
#[allow(dead_code)]
#[allow(clippy::too_many_arguments)] // each param is a distinct snapshot field, mirrors SchemaSnapshot's own shape
pub fn save_with_overrides(
    schemas: &[TableSchema],
    total_rows: u64,
    truncated_names: &[TruncatedName],
    column_collisions: &[ColumnCollision],
    stats: &[ColumnStats],
    overflow_warnings: &[OverflowWarning],
    strategy_overrides: &std::collections::HashMap<String, UserOverride>,
    detected_format: JsonFormat,
    path: &Path,
) -> Result<()> {
    let snapshot = SchemaSnapshot {
        version: format_version(schemas),
        total_rows,
        schemas: schemas.to_vec(),
        truncated_names: truncated_names.to_vec(),
        column_collisions: column_collisions.to_vec(),
        stats: stats.to_vec(),
        strategy_overrides: strategy_overrides.clone(),
        overflow_warnings: overflow_warnings.to_vec(),
        detected_format: Some(detected_format),
        group_columns: Vec::new(),
        samples: Vec::new(),
    };
    write_snapshot(&snapshot, path, true)
}

/// Deserialize a snapshot. One made before #84 can hold junction tables for arrays of scalars
/// (`child_kind` = `ScalarArray`), a variant that no longer exists: it gets its own message.
/// Recognised from the serde error itself — grepping the file would also hit a column that
/// happens to be named like that.
fn parse_snapshot(data: &[u8]) -> Result<SchemaSnapshot> {
    serde_json::from_slice(data).map_err(|e| {
        if e.to_string().contains("unknown variant `ScalarArray`") {
            return J2sError::InvalidInput(
                "Schema snapshot uses junction tables for arrays of scalars, which json2sql no longer \
                 generates (they are now array columns). Please re-run Pass 1 to regenerate it.".to_string()
            );
        }
        J2sError::InvalidInput(format!("Schema deserialization failed: {e}"))
    })
}

/// Load a Pass 1 result from a previously saved JSON snapshot.
pub fn load(path: &Path) -> Result<SchemaSnapshot> {
    let data = std::fs::read(path).map_err(J2sError::Io)?;
    let mut snapshot = parse_snapshot(&data)?;
    if snapshot.version != SCHEMA_FORMAT_VERSION && snapshot.version != COLUMN_GROUPS_FORMAT_VERSION
    {
        if snapshot.version == 1 {
            return Err(J2sError::InvalidInput(
                "Schema snapshot v1 is no longer supported — table names used a wider budget \
                 that could cause PostgreSQL constraint name collisions. \
                 Please re-run Pass 1 to regenerate a v2 snapshot."
                    .to_string(),
            ));
        }
        return Err(J2sError::InvalidInput(format!(
            "Schema snapshot version {} is not supported (expected {} or {})",
            snapshot.version, SCHEMA_FORMAT_VERSION, COLUMN_GROUPS_FORMAT_VERSION
        )));
    }
    // Migrate legacy root-level strategy_overrides → ui_override per TableSchema.
    // ui_override already set on a schema (newer snapshots) takes precedence.
    if !snapshot.strategy_overrides.is_empty() {
        for (table_name, ov) in &snapshot.strategy_overrides {
            if let Some(s) = snapshot.schemas.iter_mut().find(|s| &s.name == table_name) {
                if s.ui_override().is_none() {
                    s.set_ui_override(Some(ov.clone()));
                }
            }
        }
    }
    Ok(snapshot)
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;
    use crate::io::reader::JsonFormat;
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
            detected_format: None,
            group_columns: vec![],
            samples: vec![],
        }
    }

    #[test]
    fn strategy_overrides_round_trips() {
        use crate::schema::table_schema::UserOverride;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut overrides: HashMap<String, UserOverride> = HashMap::new();
        overrides.insert("my_table".to_string(), UserOverride::Jsonb);

        save_with_overrides(
            &[],
            0,
            &[],
            &[],
            &[],
            &[],
            &overrides,
            JsonFormat::Lines,
            tmp.path(),
        )
        .unwrap();
        let loaded = load(tmp.path()).unwrap();

        assert!(matches!(
            loaded.strategy_overrides.get("my_table"),
            Some(UserOverride::Jsonb)
        ));
    }

    #[test]
    fn strategy_overrides_ignore_alias_deserializes_as_skip() {
        // Old snapshots persist "Ignore" — must deserialize as UserOverride::Skip.
        use crate::schema::table_schema::UserOverride;
        let json = r#"{"version":2,"total_rows":0,"schemas":[],"truncated_names":[],
            "column_collisions":[],"stats":[],"strategy_overrides":{"products":"Ignore"}}"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert!(
            matches!(
                loaded.strategy_overrides.get("products"),
                Some(UserOverride::Skip)
            ),
            "\"Ignore\" must deserialize as UserOverride::Skip"
        );
    }

    #[test]
    fn v1_snapshot_returns_clear_error() {
        // v1 snapshots used a 63-char table name budget; v2 uses 53. They are incompatible.
        let json = r#"{"version":1,"total_rows":0,"schemas":[],"truncated_names":[],"column_collisions":[],"stats":[]}"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();

        let err = load(tmp.path())
            .err()
            .expect("expected an error for v1 snapshot");
        let msg = err.to_string();
        assert!(msg.contains("v1"), "error should mention v1, got: {msg}");
        assert!(
            msg.contains("Pass 1") || msg.contains("re-run") || msg.contains("regenerate"),
            "error should tell user to re-run Pass 1, got: {msg}"
        );
    }

    /// JSON of a snapshot holding one `users_tags` child table of the given kind.
    fn snapshot_json_with_child(kind: crate::schema::table_schema::ChildKind) -> String {
        let mut child = TableSchema::new(
            "users_tags".to_string(),
            vec!["users".to_string(), "tags".to_string()],
            1,
        );
        child.parent_table = Some("users".to_string());
        child.child_kind = Some(kind);
        serde_json::to_string(&SchemaSnapshot {
            schemas: vec![child],
            ..empty_snapshot()
        })
        .unwrap()
    }

    #[test]
    fn load_refuses_a_snapshot_with_a_scalar_array_junction_table() {
        use crate::schema::table_schema::ChildKind;
        // Snapshots made before #84 can hold junction tables (`child_kind: "ScalarArray"`), a variant that no
        // longer exists: the user must be told to re-run Pass 1, not shown a raw serde error.
        let json = snapshot_json_with_child(ChildKind::ObjectArray)
            .replace("\"ObjectArray\"", "\"ScalarArray\"");
        assert!(
            json.contains("\"ScalarArray\""),
            "test setup: the snapshot must hold a ScalarArray child"
        );
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();

        let msg = load(tmp.path())
            .err()
            .expect("an old junction snapshot must be refused")
            .to_string();
        assert!(
            msg.contains("re-run Pass 1") && msg.contains("junction"),
            "got: {msg}"
        );
        assert!(
            !msg.contains("unknown variant"),
            "the raw serde error must not leak: {msg}"
        );
    }

    #[test]
    fn load_accepts_a_snapshot_with_object_array_children() {
        use crate::schema::table_schema::ChildKind;
        // regression guard: the new refusal must not catch snapshots made with array columns
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), snapshot_json_with_child(ChildKind::ObjectArray)).unwrap();
        let loaded = load(tmp.path()).expect("a snapshot without junction tables still loads");
        assert_eq!(
            (loaded.version, loaded.schemas.len()),
            (SCHEMA_FORMAT_VERSION, 1)
        );
    }

    #[test]
    fn v2_snapshot_round_trips() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        save(&[], 42, &[], &[], &[], &[], JsonFormat::Lines, tmp.path()).unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert_eq!(loaded.version, SCHEMA_FORMAT_VERSION);
        assert_eq!(loaded.total_rows, 42);
    }

    #[test]
    fn a_snapshot_is_version_3_only_when_a_table_was_split_into_column_groups() {
        use crate::schema::table_schema::TableSchema;
        let plain = TableSchema::new("items".to_string(), vec!["items".to_string()], 0);
        let mut split = plain.clone();
        split.column_groups.push("items_alpha".to_string());
        let (a, b) = (
            tempfile::NamedTempFile::new().unwrap(),
            tempfile::NamedTempFile::new().unwrap(),
        );
        save(
            std::slice::from_ref(&plain),
            0,
            &[],
            &[],
            &[],
            &[],
            JsonFormat::Lines,
            a.path(),
        )
        .unwrap();
        save(&[split], 0, &[], &[], &[], &[], JsonFormat::Lines, b.path()).unwrap();
        assert_eq!(
            load(a.path()).unwrap().version,
            2,
            "snapshots without groups stay what they were"
        );
        let loaded = load(b.path()).unwrap();
        assert_eq!(loaded.version, 3);
        assert_eq!(loaded.schemas[0].column_groups, ["items_alpha"]);
        assert!(!std::fs::read_to_string(a.path())
            .unwrap()
            .contains("column_groups"));
    }

    #[test]
    fn versions_2_and_3_load_and_the_others_do_not() {
        for (version, loads) in [(1, false), (2, true), (3, true), (4, false)] {
            let json = format!(
                r#"{{"version":{version},"total_rows":0,"schemas":[],"truncated_names":[],"column_collisions":[],"stats":[]}}"#
            );
            let tmp = tempfile::NamedTempFile::new().unwrap();
            std::fs::write(tmp.path(), json).unwrap();
            assert_eq!(load(tmp.path()).is_ok(), loads, "version {version}");
        }
    }

    #[test]
    fn the_tables_ticked_for_column_groups_round_trip_and_are_absent_when_empty() {
        // #91 D3: the per-table switches of the UI; the automatic groups themselves are never saved
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let ticked = SchemaSnapshot {
            group_columns: vec!["products".to_string()],
            ..empty_snapshot()
        };
        write_snapshot(&ticked, tmp.path(), false).unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert_eq!(
            (loaded.version, loaded.group_columns),
            (SCHEMA_FORMAT_VERSION, vec!["products".to_string()])
        );
        write_snapshot(&empty_snapshot(), tmp.path(), false).unwrap();
        assert!(!std::fs::read_to_string(tmp.path())
            .unwrap()
            .contains("group_columns"));
        let old = r#"{"version":2,"total_rows":0,"schemas":[],"truncated_names":[],"column_collisions":[],"stats":[]}"#;
        std::fs::write(tmp.path(), old).unwrap();
        assert_eq!(
            load(tmp.path()).unwrap().group_columns.len(),
            0,
            "older snapshots have no switch"
        );
    }

    /// The sample of a table of three columns: a vocabulary, numbers with gaps (NaN in the rank sample), presence.
    fn stored_sample() -> crate::schema::grouping::sample::StoredSample {
        use crate::schema::grouping::presence::Presence;
        use crate::schema::grouping::sample::{StoredSample, TableSample};
        use crate::schema::grouping::values::ValueProfile;
        let mut presence = Presence::new(3);
        let mut values = ValueProfile::new(3, 1);
        for i in 0..40_i64 {
            let cells = [
                serde_json::json!(format!("w{}", i % 6)),
                if i % 3 == 0 {
                    serde_json::Value::Null
                } else {
                    serde_json::json!(i)
                },
                serde_json::json!(i * 2),
            ];
            presence.push_row(
                cells
                    .iter()
                    .map(crate::schema::grouping::sample::is_present),
            );
            values.observe_row(cells.iter().map(Some));
        }
        StoredSample {
            table: "items".to_string(),
            columns: vec!["a".into(), "b".into(), "c".into()],
            sample: TableSample { presence, values },
        }
    }

    #[test]
    fn the_samples_of_pass_1_round_trip_and_older_snapshots_have_none() {
        // #91 phase 2: the data signals come back from the snapshot, without reading the file again
        use crate::schema::grouping::proposal::DataSignals;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let sample = stored_sample();
        write_snapshot(
            &SchemaSnapshot {
                samples: vec![sample.clone()],
                ..empty_snapshot()
            },
            tmp.path(),
            false,
        )
        .unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert_eq!(
            loaded.version, SCHEMA_FORMAT_VERSION,
            "an optional field, no new version"
        );
        let got = &loaded.samples[0];
        assert_eq!(
            (got.table.as_str(), got.columns.clone()),
            ("items", sample.columns.clone())
        );
        let (a, b) = (&sample.sample, &got.sample);
        assert_eq!(a.presence.rows(), b.presence.rows());
        for c in 0..3 {
            assert_eq!(a.presence.present_count(c), b.presence.present_count(c));
            assert_eq!(a.values.vocabulary(c), b.values.vocabulary(c));
            assert_eq!(
                format!("{:?}", a.values.ranked(c)),
                format!("{:?}", b.values.ranked(c)),
                "NaN gaps survive"
            );
        }
        assert_eq!(
            format!("{:?}", DataSignals::new(a).rows()),
            format!("{:?}", DataSignals::new(b).rows())
        );
        write_snapshot(&empty_snapshot(), tmp.path(), false).unwrap();
        assert!(!std::fs::read_to_string(tmp.path())
            .unwrap()
            .contains("samples"));
        assert_eq!(load(tmp.path()).unwrap().samples.len(), 0);
    }

    #[test]
    fn strategy_overrides_default_empty_on_new_snapshot() {
        let s = empty_snapshot();
        assert!(s.strategy_overrides.is_empty());
    }

    #[test]
    fn load_migrates_strategy_overrides_to_ui_override() {
        use crate::schema::table_schema::{InferredStrategy, UserOverride};
        // Old snapshot with strategy_overrides at root level, no ui_override on schemas
        let json = r#"{
            "version": 2,
            "total_rows": 0,
            "schemas": [
                {"name":"products","path":["products"],"columns":[],"depth":0,"inferred_strategy":"Columns","flatten_sources":{}}
            ],
            "truncated_names": [],
            "column_collisions": [],
            "stats": [],
            "strategy_overrides": {"products": "Skip"}
        }"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();
        let loaded = load(tmp.path()).unwrap();
        let products = loaded
            .schemas
            .iter()
            .find(|s| s.name == "products")
            .unwrap();
        assert_eq!(
            products.ui_override(),
            Some(&UserOverride::Skip),
            "strategy_overrides from root must be migrated into ui_override"
        );
        assert_eq!(
            products.inferred_strategy,
            InferredStrategy::Columns,
            "inferred_strategy must not be mutated during migration"
        );
    }

    #[test]
    fn load_does_not_overwrite_existing_ui_override_with_strategy_overrides() {
        use crate::schema::table_schema::UserOverride;
        // New snapshot with ui_override already on the table AND strategy_overrides at root
        // ui_override wins (was set explicitly)
        let json = r#"{
            "version": 2,
            "total_rows": 0,
            "schemas": [
                {"name":"products","path":["products"],"columns":[],"depth":0,
                 "inferred_strategy":"Columns","flatten_sources":{},"ui_override":"Pivot"}
            ],
            "truncated_names": [],
            "column_collisions": [],
            "stats": [],
            "strategy_overrides": {"products": "Jsonb"}
        }"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();
        let loaded = load(tmp.path()).unwrap();
        let products = loaded
            .schemas
            .iter()
            .find(|s| s.name == "products")
            .unwrap();
        assert_eq!(products.ui_override(), Some(&UserOverride::Pivot),
            "existing ui_override on schema must not be overwritten by strategy_overrides migration");
    }

    /// A modern snapshot carries `ui_override` directly on the schema (no legacy
    /// `strategy_overrides` at root) — the most common "resume from snapshot" path.
    #[test]
    fn load_reflects_ui_override_for_modern_snapshot_without_legacy_overrides() {
        use crate::schema::table_schema::InferredStrategy;
        let json = r#"{
            "version": 2,
            "total_rows": 0,
            "schemas": [
                {"name":"products","path":["products"],"columns":[],"depth":0,
                 "inferred_strategy":"Columns","flatten_sources":{},"ui_override":"Pivot"}
            ],
            "truncated_names": [],
            "column_collisions": [],
            "stats": []
        }"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();
        let loaded = load(tmp.path()).unwrap();
        let products = loaded
            .schemas
            .iter()
            .find(|s| s.name == "products")
            .unwrap();
        assert_eq!(*products.effective_strategy(), InferredStrategy::Pivot);
    }

    #[test]
    fn load_reflects_ui_override_after_legacy_migration() {
        use crate::schema::table_schema::InferredStrategy;
        let json = r#"{
            "version": 2,
            "total_rows": 0,
            "schemas": [
                {"name":"products","path":["products"],"columns":[],"depth":0,"inferred_strategy":"Columns","flatten_sources":{}}
            ],
            "truncated_names": [],
            "column_collisions": [],
            "stats": [],
            "strategy_overrides": {"products": "Skip"}
        }"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();
        let loaded = load(tmp.path()).unwrap();
        let products = loaded
            .schemas
            .iter()
            .find(|s| s.name == "products")
            .unwrap();
        assert_eq!(
            *products.effective_strategy(),
            InferredStrategy::Ignore,
            "effective_strategy must reflect the migrated ui_override (Skip -> Ignore)"
        );
    }

    #[test]
    fn overflow_warnings_round_trip() {
        use crate::schema::finalizer::OverflowWarning;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let warnings = vec![
            OverflowWarning {
                table_name: "wide_table".to_string(),
                original_column_count: 1700,
            },
            OverflowWarning {
                table_name: "another_wide".to_string(),
                original_column_count: 1800,
            },
        ];
        save(
            &[],
            0,
            &[],
            &[],
            &[],
            &warnings,
            JsonFormat::Array,
            tmp.path(),
        )
        .unwrap();
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
        assert!(
            loaded.overflow_warnings.is_empty(),
            "old snapshots must deserialize with empty overflow_warnings"
        );
    }

    #[test]
    fn save_with_overrides_leaves_no_tmp_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema.json");
        save_with_overrides(
            &[],
            0,
            &[],
            &[],
            &[],
            &[],
            &std::collections::HashMap::new(),
            JsonFormat::Lines,
            &path,
        )
        .unwrap();
        assert!(path.exists(), "target file must exist after save");
        let tmp = path.with_extension("json.tmp");
        assert!(
            !tmp.exists(),
            ".tmp file must be cleaned up after atomic save"
        );
    }

    #[test]
    fn save_with_overrides_does_not_corrupt_existing_on_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema.json");
        // Write an initial valid snapshot
        save_with_overrides(
            &[],
            10,
            &[],
            &[],
            &[],
            &[],
            &std::collections::HashMap::new(),
            JsonFormat::Lines,
            &path,
        )
        .unwrap();
        // Overwrite with a different value
        save_with_overrides(
            &[],
            42,
            &[],
            &[],
            &[],
            &[],
            &std::collections::HashMap::new(),
            JsonFormat::Lines,
            &path,
        )
        .unwrap();
        // The file must be readable and contain the new value
        let loaded = load(&path).unwrap();
        assert_eq!(
            loaded.total_rows, 42,
            "overwrite must produce a complete, readable file"
        );
    }

    // --- detected_format field tests ---

    #[test]
    fn save_stores_detected_format() {
        use crate::io::reader::JsonFormat;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        save(&[], 0, &[], &[], &[], &[], JsonFormat::Lines, tmp.path()).unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert_eq!(loaded.detected_format, Some(JsonFormat::Lines));
    }

    #[test]
    fn save_with_overrides_stores_detected_format() {
        use crate::io::reader::JsonFormat;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        save_with_overrides(
            &[],
            0,
            &[],
            &[],
            &[],
            &[],
            &std::collections::HashMap::new(),
            JsonFormat::RootWrapper(vec!["K".to_string()]),
            tmp.path(),
        )
        .unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert_eq!(
            loaded.detected_format,
            Some(JsonFormat::RootWrapper(vec!["K".to_string()]))
        );
    }

    #[test]
    fn detected_format_defaults_none_on_old_snapshot() {
        // Old snapshots without detected_format must deserialize with None.
        let json = r#"{"version":2,"total_rows":0,"schemas":[],"truncated_names":[],"column_collisions":[],"stats":[]}"#;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), json).unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert!(
            loaded.detected_format.is_none(),
            "old snapshots must deserialize detected_format as None"
        );
    }

    #[test]
    fn detected_format_round_trips_array() {
        use crate::io::reader::JsonFormat;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut snap = empty_snapshot();
        snap.detected_format = Some(JsonFormat::Array);
        let json = serde_json::to_string_pretty(&snap).unwrap();
        std::fs::write(tmp.path(), &json).unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert_eq!(loaded.detected_format, Some(JsonFormat::Array));
    }

    #[test]
    fn detected_format_round_trips_root_wrapper() {
        use crate::io::reader::JsonFormat;
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut snap = empty_snapshot();
        snap.detected_format = Some(JsonFormat::RootWrapper(vec![
            "K1".to_string(),
            "K2".to_string(),
        ]));
        let json = serde_json::to_string_pretty(&snap).unwrap();
        std::fs::write(tmp.path(), &json).unwrap();
        let loaded = load(tmp.path()).unwrap();
        assert_eq!(
            loaded.detected_format,
            Some(JsonFormat::RootWrapper(vec![
                "K1".to_string(),
                "K2".to_string()
            ]))
        );
    }
}
