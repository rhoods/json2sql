//! TOML configuration file: user-defined type and strategy overrides applied before Pass 2.
//!
//! The config is optional. When present, `apply_overrides` validates each override against
//! the finalized schema and mutates the relevant [`TableSchema`] entries in place.
//! Unknown table or column names produce a hard error rather than silently doing nothing.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use crate::error::{J2sError, Result};
use crate::schema::wide_strategies::{
    apply_flatten, apply_normalize_dynamic_keys, apply_structured_pivot_columns,
    apply_wide_strategy_columns, build_union_columns,
};
use crate::schema::suffix_detector::build_suffix_schema_from_list;
use crate::schema::table_schema::{ColumnSchema, KeyShape, SiblingSchema, TableSchema, InferredStrategy, UserOverride};
use crate::schema::type_tracker::PgType;

/// TOML config file for manual type overrides.
///
/// ```toml
/// [users]
/// age = "INTEGER"
/// created_at = "TIMESTAMP"
///
/// [nutrients]
/// strategy = "structured_pivot"
/// suffix_columns = ["_100g", "_unit", "_label"]
///
/// [users_orders]
/// amount = "DOUBLE PRECISION"
/// ```
///
/// Keys are the `PostgreSQL` column names (sanitized). Values are SQL type strings.
/// Special keys: `strategy`, `suffix_columns`.
/// Définition d'un groupe de fusion (`SiblingCollapse` manuel).
#[derive(Debug, Deserialize)]
pub struct GroupConfig {
    pub strategy: String,
    pub members: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SchemaConfig {
    /// Groupes de fusion : `nom_résultant` → { strategy, members }
    #[serde(default)]
    pub group: HashMap<String, GroupConfig>,
    /// Overrides par table : `table_name` → { `colonne_ou_strategy` → valeur }
    #[serde(flatten)]
    pub tables: HashMap<String, HashMap<String, toml::Value>>,
}

impl SchemaConfig {
    pub fn from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(J2sError::Io)?;
        toml::from_str(&content).map_err(|e| {
            J2sError::InvalidInput(format!(
                "Failed to parse schema config '{}': {}",
                path.display(),
                e
            ))
        })
    }
}

/// Non-fatal warning emitted when a TOML config override references an unknown table,
/// column, type, strategy, or group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigWarning {
    UnknownTable(String),
    UnknownColumn { table: String, column: String },
    UnknownType    { table: String, column: String, type_str: String },
    UnknownStrategy { table: String, strategy: String },
    UnknownGroupStrategy { group: String, strategy: String },
    GroupMergeFailed { group: String, found: usize, expected: usize },
    /// Override to `to_strategy` is incompatible with the table's current column layout
    /// (inferred as `from_strategy`). Column names were lost during finalization and cannot
    /// be reconstructed.
    InvalidOverride { table: String, from_strategy: String, to_strategy: String },
}

impl ConfigWarning {
    pub fn to_message(&self) -> String {
        match self {
            Self::UnknownTable(t) =>
                format!("schema-config: table '{t}' not found in inferred schema"),
            Self::UnknownColumn { table, column } =>
                format!("schema-config: column '{table}.{column}' not found"),
            Self::UnknownType { table, column, type_str } =>
                format!("schema-config: unknown type '{type_str}' for '{table}.{column}', ignored"),
            Self::UnknownStrategy { table, strategy } =>
                format!("schema-config: unknown strategy '{strategy}' for '{table}', ignored"),
            Self::UnknownGroupStrategy { group, strategy } =>
                format!("schema-config: unknown group strategy '{strategy}' for group '{group}', ignored"),
            Self::GroupMergeFailed { group, found, expected } =>
                format!("group '{group}': {found}/{expected} member(s) found, merge ignored"),
            Self::InvalidOverride { table, from_strategy, to_strategy } =>
                format!("schema-config: cannot override '{table}' from {from_strategy} to {to_strategy} — column names are lost after finalization, ignored"),
        }
    }
}

struct DeferredNormalize { table_name: String, id_column: String }
struct DeferredFlatten  { table_name: String, prefix: String, max_depth: u8 }

/// Apply type overrides from `config` to the finalized schemas.
/// Matches by table name and column name (both sanitized `PostgreSQL` identifiers).
/// Unknown tables, columns, types, or strategies are returned as `ConfigWarning` values
/// rather than written to stderr — the caller decides how to display them.
pub fn apply_overrides(schemas: &mut Vec<TableSchema>, config: &SchemaConfig) -> crate::error::Result<Vec<ConfigWarning>> {
    let mut warnings: Vec<ConfigWarning> = Vec::new();
    let mut deferred_normalize: Vec<DeferredNormalize> = Vec::new();
    let mut deferred_flatten:   Vec<DeferredFlatten>   = Vec::new();

    for (table_name, col_overrides) in &config.tables {
        match schemas.iter_mut().find(|s| &s.name == table_name) {
            None => warnings.push(ConfigWarning::UnknownTable(table_name.clone())),
            Some(schema) => {
                // Strategy must run before suffix_columns so the column layout is correct.
                // NormalizeDynamicKeys and Flatten are deferred (need full schemas slice).
                warnings.extend(apply_strategy_override(schema, table_name, col_overrides, &mut deferred_normalize, &mut deferred_flatten));
                apply_suffix_columns_override(schema, table_name, col_overrides);
                warnings.extend(apply_column_type_overrides(schema, table_name, col_overrides));
            }
        }
    }

    for op in deferred_normalize {
        let original = schemas.iter().find(|s| s.name == op.table_name).map(|s| s.inferred_strategy.clone());
        apply_normalize_dynamic_keys(schemas, &op.table_name, op.id_column.clone())?;
        if let Some(s) = schemas.iter_mut().find(|s| s.name == op.table_name) {
            if let Some(orig) = original { s.inferred_strategy = orig; }
            s.toml_override = Some(UserOverride::NormalizeDynamicKeys { id_column: op.id_column });
        }
    }
    for op in deferred_flatten {
        apply_flatten(schemas, &op.table_name, &op.prefix, op.max_depth)?;
    }
    Ok(warnings)
}

fn toml_str(map: &HashMap<String, toml::Value>, key: &str) -> Option<String> {
    map.get(key).and_then(|v| if let toml::Value::String(s) = v { Some(s.clone()) } else { None })
}

#[allow(clippy::too_many_lines)] // exhaustive dispatch over all override strategy variants
fn apply_strategy_override(
    schema: &mut TableSchema,
    table_name: &str,
    col_overrides: &HashMap<String, toml::Value>,
    deferred_normalize: &mut Vec<DeferredNormalize>,
    deferred_flatten: &mut Vec<DeferredFlatten>,
) -> Vec<ConfigWarning> {
    let Some(toml::Value::String(strategy_str)) = col_overrides.get("strategy") else { return vec![] };
    match strategy_str.to_lowercase().as_str() {
        "pivot" => {
            let original = schema.inferred_strategy.clone();
            eprintln!("  Override strategy: {table_name} → Pivot");
            apply_wide_strategy_columns(schema, InferredStrategy::Pivot);
            schema.inferred_strategy = original;
            schema.toml_override = Some(UserOverride::Pivot);
        }
        "jsonb" => {
            let original = schema.inferred_strategy.clone();
            eprintln!("  Override strategy: {table_name} → Jsonb");
            apply_wide_strategy_columns(schema, InferredStrategy::Jsonb);
            schema.inferred_strategy = original;
            schema.toml_override = Some(UserOverride::Jsonb);
        }
        "columns" => {
            if !matches!(schema.inferred_strategy, InferredStrategy::Columns) {
                return vec![ConfigWarning::InvalidOverride {
                    table: table_name.to_string(),
                    from_strategy: format!("{:?}", schema.inferred_strategy)
                        .split('(').next().unwrap_or("Unknown").to_string(),
                    to_strategy: "columns".to_string(),
                }];
            }
            eprintln!("  Override strategy: {table_name} → Columns (no-op, already inferred)");
            schema.toml_override = Some(UserOverride::Columns);
        }
        "structured_pivot" => {} // handled via suffix_columns below
        "normalize_dynamic_keys" => {
            let id_col = toml_str(col_overrides, "id_column").unwrap_or_else(|| "key_id".to_string());
            deferred_normalize.push(DeferredNormalize { table_name: table_name.to_string(), id_column: id_col });
        }
        "flatten" => {
            let prefix = toml_str(col_overrides, "prefix").unwrap_or_else(|| format!("{table_name}_"));
            let max_depth = col_overrides.get("max_depth")
                .and_then(|v| if let toml::Value::Integer(n) = v { u8::try_from(*n).ok() } else { None })
                .unwrap_or(1);
            deferred_flatten.push(DeferredFlatten { table_name: table_name.to_string(), prefix, max_depth });
        }
        other => return vec![ConfigWarning::UnknownStrategy {
            table: table_name.to_string(),
            strategy: other.to_string(),
        }],
    }
    vec![]
}

fn apply_suffix_columns_override(
    schema: &mut TableSchema,
    table_name: &str,
    col_overrides: &HashMap<String, toml::Value>,
) {
    let Some(toml::Value::Array(arr)) = col_overrides.get("suffix_columns") else { return };
    let suffix_list: Vec<String> = arr
        .iter()
        .filter_map(|v| if let toml::Value::String(s) = v { Some(s.clone()) } else { None })
        .collect();
    if suffix_list.is_empty() { return; }

    // At config-apply time the schema is already finalized (columns are resolved).
    // Build a dummy TypeTracker map from existing column types so
    // build_suffix_schema_from_list can widen types correctly.
    let mut type_map: indexmap::IndexMap<String, crate::schema::type_tracker::TypeTracker> =
        indexmap::IndexMap::new();
    for col in schema.data_columns() {
        let mut tracker = crate::schema::type_tracker::TypeTracker::new(256);
        prime_tracker_from_pg_type(&mut tracker, &col.pg_type);
        type_map.insert(col.original_name.clone(), tracker);
    }
    let suffix_schema = build_suffix_schema_from_list(&suffix_list, &type_map);
    eprintln!("  Override strategy: {table_name} → StructuredPivot (suffixes: {suffix_list:?})");
    apply_structured_pivot_columns(schema, suffix_schema);
}

fn apply_column_type_overrides(
    schema: &mut TableSchema,
    table_name: &str,
    col_overrides: &HashMap<String, toml::Value>,
) -> Vec<ConfigWarning> {
    let mut warnings = Vec::new();
    for (col_name, value) in col_overrides {
        if matches!(col_name.as_str(), "strategy" | "suffix_columns" | "id_column" | "prefix" | "max_depth") {
            continue;
        }
        let toml::Value::String(type_str) = value else { continue };
        match schema.columns.iter_mut().find(|c| &c.name == col_name) {
            None => warnings.push(ConfigWarning::UnknownColumn {
                table: table_name.to_string(),
                column: col_name.clone(),
            }),
            Some(col) => match parse_pg_type(type_str) {
                None => warnings.push(ConfigWarning::UnknownType {
                    table: table_name.to_string(),
                    column: col_name.clone(),
                    type_str: type_str.clone(),
                }),
                Some(pg_type) => {
                    eprintln!("  Override: {}.{} {} → {}", table_name, col_name, col.pg_type.as_sql(), pg_type.as_sql());
                    col.pg_type = pg_type;
                }
            },
        }
    }
    warnings
}

/// Appliquer les groupes de fusion définis dans la config.
/// Doit être appelé APRÈS `apply_overrides` et AVANT la sauvegarde du snapshot.
pub fn apply_group_overrides(schemas: &mut Vec<TableSchema>, config: &SchemaConfig) -> Vec<ConfigWarning> {
    let mut warnings = Vec::new();
    for (group_name, group_cfg) in &config.group {
        match group_cfg.strategy.to_lowercase().as_str() {
            "keyed_pivot" => {
                if let Some(w) = apply_keyed_pivot_merge(schemas, group_name, &group_cfg.members) {
                    warnings.push(w);
                }
            }
            other => warnings.push(ConfigWarning::UnknownGroupStrategy {
                group: group_name.to_string(),
                strategy: other.to_string(),
            }),
        }
    }
    warnings
}

/// Apply all config overrides in sequence, then re-run child exclusion.
///
/// Strategy overrides may change a parent table from `Columns` to `Jsonb`/`Pivot`, which
/// means its former child tables would receive no data. `exclude_absorbed_children` removes
/// them so Pass 2 never tries to insert into non-existent tables.
pub fn apply_overrides_complete(
    schemas: &mut Vec<TableSchema>,
    config: &SchemaConfig,
) -> crate::error::Result<Vec<ConfigWarning>> {
    let mut warnings = apply_overrides(schemas, config)?;
    warnings.extend(apply_group_overrides(schemas, config));
    // Must run BEFORE exclude_absorbed_children: that call reads absorbs_children() →
    // effective_strategy(), which would otherwise see the stale pre-override cache and miss
    // children that the overrides just applied above made absorbable (issue #22 regression risk #1).
    for schema in schemas.iter_mut() {
        schema.recompute_cached_strategy();
    }
    crate::schema::finalizer::exclude_absorbed_children(schemas);
    Ok(warnings)
}

#[allow(clippy::too_many_lines)] // struct construction pipeline: generated cols → key col → union cols → strategy
fn build_merged_keyed_pivot_schema(group_name: &str, cloned: &[TableSchema]) -> TableSchema {
    let refs: Vec<&TableSchema> = cloned.iter().collect();
    let first = &cloned[0];
    let mut merged =
        TableSchema::new(group_name.to_string(), vec![group_name.to_string()], first.depth);
    merged.parent_table.clone_from(&first.parent_table);
    merged.child_kind.clone_from(&first.child_kind);
    merged.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
    if let Some(ref parent) = first.parent_table {
        merged.columns.push(ColumnSchema::parent_fk(parent));
    }
    if first.has_order_column() {
        merged.columns.push(ColumnSchema::generated("j2s_order", PgType::BigInt));
    }
    merged.columns.push(ColumnSchema {
        name: "key_id".to_string(),
        original_name: "key_id".to_string(),
        pg_type: PgType::Text,
        not_null: true,
        is_generated: false,
        is_parent_fk: false,
    });
    for col in build_union_columns(&refs) {
        merged.columns.push(col);
    }
    merged.inferred_strategy = InferredStrategy::SiblingCollapse(SiblingSchema {
        key_col_name: "key_id".to_string(),
        key_shape: KeyShape::Mixed,
        array_children: false,
    });
    merged
}

fn apply_keyed_pivot_merge(schemas: &mut Vec<TableSchema>, group_name: &str, members: &[String]) -> Option<ConfigWarning> {
    let mut indices: Vec<usize> = members
        .iter()
        .filter_map(|name| schemas.iter().position(|s| &s.name == name))
        .collect();

    if indices.len() < 2 {
        return Some(ConfigWarning::GroupMergeFailed {
            group: group_name.to_string(),
            found: indices.len(),
            expected: members.len(),
        });
    }
    indices.sort_unstable();
    let insert_pos = indices[0];

    // Cloner les membres avant toute mutation
    let cloned: Vec<TableSchema> = indices.iter().map(|&i| schemas[i].clone()).collect();
    let merged = build_merged_keyed_pivot_schema(group_name, &cloned);

    // Retirer les membres du plus grand index au plus petit pour éviter le décalage
    for &i in indices.iter().rev() {
        schemas.remove(i);
    }
    schemas.insert(insert_pos, merged);

    eprintln!(
        "  Groupe '{}' : {} tables → 1 (SiblingCollapse)",
        group_name,
        indices.len()
    );
    None
}

/// Apply IHM strategy overrides (`Pivot | Jsonb | Skip`) to a mutable schema slice.
///
/// Called by the CLI after loading a snapshot that includes `strategy_overrides`.
/// `Skip` removes the table — and if it had `AutoSplit`, also removes the companion
/// `_wide` table. `Pivot` and `Jsonb` mutate `inferred_strategy` in place.
pub fn apply_user_overrides(
    schemas: &mut Vec<TableSchema>,
    overrides: &HashMap<String, UserOverride>,
) {
    // Collect companion _wide table names for AutoSplit tables being skipped.
    let wide_to_remove: Vec<String> = overrides.iter()
        .filter_map(|(name, ov)| {
            if !matches!(ov, UserOverride::Skip) { return None; }
            schemas.iter().find(|s| &s.name == name).and_then(|s| {
                if let InferredStrategy::AutoSplit { wide_table_name, .. } = &s.inferred_strategy {
                    Some(wide_table_name.clone())
                } else {
                    None
                }
            })
        })
        .collect();

    for (table_name, ov) in overrides {
        if let Some(s) = schemas.iter_mut().find(|s| &s.name == table_name) {
            match ov {
                UserOverride::Pivot | UserOverride::Jsonb => {
                    let strategy = if matches!(ov, UserOverride::Pivot) {
                        InferredStrategy::Pivot
                    } else {
                        InferredStrategy::Jsonb
                    };
                    let original = s.inferred_strategy.clone();
                    apply_wide_strategy_columns(s, strategy);
                    s.inferred_strategy = original;
                    if s.ui_override.is_none() {
                        s.ui_override = Some(ov.clone());
                    }
                }
                UserOverride::Columns => {
                    if matches!(s.inferred_strategy, InferredStrategy::Columns)
                        && s.ui_override.is_none()
                    {
                        s.ui_override = Some(UserOverride::Columns);
                    }
                }
                UserOverride::Skip
                | UserOverride::JsonbFlatten
                | UserOverride::Flatten { .. }
                | UserOverride::NormalizeDynamicKeys { .. } => {}
            }
        }
    }

    schemas.retain(|s| {
        !matches!(overrides.get(&s.name), Some(UserOverride::Skip))
            && !wide_to_remove.contains(&s.name)
    });
}

/// Prime a `TypeTracker` with a representative observation so `to_pg_type()` returns
/// a type consistent with the given `PgType`.  Used when rebuilding type maps from
/// already-resolved column schemas.
const fn prime_tracker_from_pg_type(
    tracker: &mut crate::schema::type_tracker::TypeTracker,
    pg_type: &PgType,
) {
    use crate::schema::type_tracker::InferredType;
    let inferred = match pg_type {
        PgType::Integer => InferredType::Integer,
        PgType::BigInt => InferredType::BigInt,
        PgType::DoublePrecision => InferredType::Float,
        PgType::Boolean => InferredType::Boolean,
        PgType::Uuid => InferredType::Uuid,
        PgType::Date => InferredType::Date,
        PgType::Timestamp => InferredType::Timestamp,
        PgType::VarChar(_) | PgType::Text | PgType::Jsonb | PgType::Array(_) => {
            InferredType::Varchar
        }
    };
    tracker.type_counts[inferred as usize] += 1;
    tracker.total_count += 1;
}

/// Parse a SQL type string into a `PgType`.
/// Supports common aliases (case-insensitive).
fn parse_pg_type(s: &str) -> Option<PgType> {
    match s.trim().to_uppercase().as_str() {
        "INTEGER" | "INT" | "INT4" => Some(PgType::Integer),
        "BIGINT" | "INT8" => Some(PgType::BigInt),
        "DOUBLE PRECISION" | "FLOAT" | "FLOAT8" | "REAL" | "FLOAT4" => {
            Some(PgType::DoublePrecision)
        }
        "BOOLEAN" | "BOOL" => Some(PgType::Boolean),
        "UUID" => Some(PgType::Uuid),
        "DATE" => Some(PgType::Date),
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => Some(PgType::Timestamp),
        "TEXT" => Some(PgType::Text),
        other => {
            // VARCHAR(N)
            if let Some(inner) = other.strip_prefix("VARCHAR(").and_then(|s| s.strip_suffix(')')) {
                if let Ok(n) = inner.trim().parse::<u32>() {
                    return Some(PgType::VarChar(n));
                }
            }
            // CHARACTER VARYING(N)
            if let Some(inner) = other
                .strip_prefix("CHARACTER VARYING(")
                .and_then(|s| s.strip_suffix(')'))
            {
                if let Ok(n) = inner.trim().parse::<u32>() {
                    return Some(PgType::VarChar(n));
                }
            }
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pg_type() {
        assert_eq!(parse_pg_type("INTEGER"), Some(PgType::Integer));
        assert_eq!(parse_pg_type("int"), Some(PgType::Integer));
        assert_eq!(parse_pg_type("BIGINT"), Some(PgType::BigInt));
        assert_eq!(parse_pg_type("double precision"), Some(PgType::DoublePrecision));
        assert_eq!(parse_pg_type("float"), Some(PgType::DoublePrecision));
        assert_eq!(parse_pg_type("BOOLEAN"), Some(PgType::Boolean));
        assert_eq!(parse_pg_type("bool"), Some(PgType::Boolean));
        assert_eq!(parse_pg_type("UUID"), Some(PgType::Uuid));
        assert_eq!(parse_pg_type("DATE"), Some(PgType::Date));
        assert_eq!(parse_pg_type("TIMESTAMP"), Some(PgType::Timestamp));
        assert_eq!(parse_pg_type("TEXT"), Some(PgType::Text));
        assert_eq!(parse_pg_type("VARCHAR(128)"), Some(PgType::VarChar(128)));
        assert_eq!(parse_pg_type("CHARACTER VARYING(64)"), Some(PgType::VarChar(64)));
        assert_eq!(parse_pg_type("NONSENSE"), None);
    }

    // --- ConfigWarning ---

    #[test]
    fn config_warning_to_message_unknown_table() {
        let w = ConfigWarning::UnknownTable("orders".to_string());
        assert!(w.to_message().contains("orders"));
        assert!(w.to_message().contains("not found"));
    }

    #[test]
    fn config_warning_to_message_unknown_column() {
        let w = ConfigWarning::UnknownColumn { table: "users".to_string(), column: "age".to_string() };
        let msg = w.to_message();
        assert!(msg.contains("users"));
        assert!(msg.contains("age"));
    }

    #[test]
    fn config_warning_to_message_unknown_type() {
        let w = ConfigWarning::UnknownType {
            table: "users".to_string(),
            column: "age".to_string(),
            type_str: "BADTYPE".to_string(),
        };
        let msg = w.to_message();
        assert!(msg.contains("BADTYPE"));
        assert!(msg.contains("users"));
        assert!(msg.contains("age"));
    }

    #[test]
    fn config_warning_to_message_unknown_strategy() {
        let w = ConfigWarning::UnknownStrategy { table: "tags".to_string(), strategy: "magic".to_string() };
        let msg = w.to_message();
        assert!(msg.contains("magic"));
        assert!(msg.contains("tags"));
    }

    #[test]
    fn config_warning_to_message_group_merge_failed() {
        let w = ConfigWarning::GroupMergeFailed { group: "merged".to_string(), found: 1, expected: 3 };
        let msg = w.to_message();
        assert!(msg.contains("merged"));
        assert!(msg.contains("1"));
        assert!(msg.contains("3"));
    }

    #[test]
    fn config_warning_clone_and_eq() {
        let w = ConfigWarning::UnknownTable("t".to_string());
        assert_eq!(w.clone(), ConfigWarning::UnknownTable("t".to_string()));
        assert_ne!(w, ConfigWarning::UnknownTable("other".to_string()));
    }

    // --- apply_overrides + helpers (return Vec<ConfigWarning>) ---

    #[test]
    fn apply_overrides_unknown_table_returns_warning() {
        let mut schemas = vec![simple_table("users")];
        let mut tables = HashMap::new();
        tables.insert("ghost".to_string(), HashMap::new());
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings, vec![ConfigWarning::UnknownTable("ghost".to_string())]);
    }

    #[test]
    fn apply_overrides_unknown_column_returns_warning() {
        use crate::schema::table_schema::{ColumnSchema, TableSchema};
        let mut schemas = vec![{
            let mut s = TableSchema::new("users".to_string(), vec!["users".to_string()], 0);
            s.columns.push(ColumnSchema {
                name: "name".to_string(), original_name: "name".to_string(),
                pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
            });
            s
        }];
        let mut tables = HashMap::new();
        let mut cols = HashMap::new();
        cols.insert("ghost_col".to_string(), toml::Value::String("TEXT".to_string()));
        tables.insert("users".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings, vec![ConfigWarning::UnknownColumn {
            table: "users".to_string(), column: "ghost_col".to_string(),
        }]);
    }

    #[test]
    fn apply_overrides_unknown_type_returns_warning() {
        use crate::schema::table_schema::{ColumnSchema, TableSchema};
        let mut schemas = vec![{
            let mut s = TableSchema::new("users".to_string(), vec!["users".to_string()], 0);
            s.columns.push(ColumnSchema {
                name: "age".to_string(), original_name: "age".to_string(),
                pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
            });
            s
        }];
        let mut tables = HashMap::new();
        let mut cols = HashMap::new();
        cols.insert("age".to_string(), toml::Value::String("NONSENSE".to_string()));
        tables.insert("users".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings, vec![ConfigWarning::UnknownType {
            table: "users".to_string(), column: "age".to_string(), type_str: "NONSENSE".to_string(),
        }]);
    }

    #[test]
    fn apply_overrides_unknown_strategy_returns_warning() {
        let mut schemas = vec![simple_table("tags")];
        let mut tables = HashMap::new();
        let mut cols = HashMap::new();
        cols.insert("strategy".to_string(), toml::Value::String("magic".to_string()));
        tables.insert("tags".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings, vec![ConfigWarning::UnknownStrategy {
            table: "tags".to_string(), strategy: "magic".to_string(),
        }]);
    }

    #[test]
    fn apply_overrides_valid_override_no_warnings() {
        use crate::schema::table_schema::{ColumnSchema, TableSchema};
        let mut schemas = vec![{
            let mut s = TableSchema::new("users".to_string(), vec!["users".to_string()], 0);
            s.columns.push(ColumnSchema {
                name: "age".to_string(), original_name: "age".to_string(),
                pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
            });
            s
        }];
        let mut tables = HashMap::new();
        let mut cols = HashMap::new();
        cols.insert("age".to_string(), toml::Value::String("INTEGER".to_string()));
        tables.insert("users".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_apply_overrides() {
        use crate::schema::table_schema::{ColumnSchema, TableSchema};

        let mut schemas = vec![{
            let mut s = TableSchema::new("users".to_string(), vec!["users".to_string()], 0);
            s.columns.push(ColumnSchema {
                name: "age".to_string(),
                original_name: "age".to_string(),
                pg_type: PgType::Text,
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
            s
        }];

        let mut tables = HashMap::new();
        let mut cols = HashMap::new();
        cols.insert("age".to_string(), toml::Value::String("INTEGER".to_string()));
        tables.insert("users".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };

        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert!(warnings.is_empty(), "valid override must produce no warnings");

        let col = schemas[0].columns.iter().find(|c| c.name == "age").unwrap();
        assert_eq!(col.pg_type, PgType::Integer);
    }

    fn toml_strategy(strategy: &str) -> HashMap<String, toml::Value> {
        let mut m = HashMap::new();
        m.insert("strategy".to_string(), toml::Value::String(strategy.to_string()));
        m
    }

    #[test]
    fn toml_columns_on_columns_table_sets_toml_override() {
        let mut schemas = vec![simple_table("flat")];
        // inferred as Columns (default for simple_table)
        let mut tables = HashMap::new();
        tables.insert("flat".to_string(), toml_strategy("columns"));
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert!(warnings.is_empty(), "no warning when inferred is already Columns");
        assert_eq!(schemas[0].toml_override, Some(UserOverride::Columns));
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns);
    }

    #[test]
    fn toml_columns_on_pivot_table_returns_invalid_override_warning() {
        use crate::schema::wide_strategies::apply_wide_strategy_columns;
        let mut schemas = vec![simple_table("metrics")];
        apply_wide_strategy_columns(&mut schemas[0], InferredStrategy::Pivot);
        schemas[0].inferred_strategy = InferredStrategy::Pivot;
        let mut tables = HashMap::new();
        tables.insert("metrics".to_string(), toml_strategy("columns"));
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(matches!(&warnings[0], ConfigWarning::InvalidOverride { table, .. } if table == "metrics"));
        assert_eq!(schemas[0].toml_override, None, "toml_override must not be set on invalid override");
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Pivot, "inferred_strategy must be unchanged");
    }

    #[test]
    fn toml_columns_on_jsonb_table_returns_invalid_override_warning() {
        use crate::schema::wide_strategies::apply_wide_strategy_columns;
        let mut schemas = vec![simple_table("events")];
        apply_wide_strategy_columns(&mut schemas[0], InferredStrategy::Jsonb);
        schemas[0].inferred_strategy = InferredStrategy::Jsonb;
        let mut tables = HashMap::new();
        tables.insert("events".to_string(), toml_strategy("columns"));
        let config = SchemaConfig { tables, group: HashMap::new() };
        let warnings = apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(matches!(&warnings[0], ConfigWarning::InvalidOverride { table, .. } if table == "events"));
    }

    #[test]
    fn toml_pivot_sets_toml_override_and_preserves_inferred() {
        let mut schemas = vec![simple_table("tags")];
        schemas[0].inferred_strategy = InferredStrategy::Columns;
        let mut tables = HashMap::new();
        tables.insert("tags".to_string(), toml_strategy("pivot"));
        let config = SchemaConfig { tables, group: HashMap::new() };
        apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns, "inferred_strategy must not be mutated");
        assert_eq!(schemas[0].toml_override, Some(UserOverride::Pivot));
        assert_eq!(*schemas[0].effective_strategy(), InferredStrategy::Pivot);
    }

    #[test]
    fn toml_jsonb_sets_toml_override_and_preserves_inferred() {
        let mut schemas = vec![simple_table("blob")];
        schemas[0].inferred_strategy = InferredStrategy::Columns;
        let mut tables = HashMap::new();
        tables.insert("blob".to_string(), toml_strategy("jsonb"));
        let config = SchemaConfig { tables, group: HashMap::new() };
        apply_overrides(&mut schemas, &config).unwrap();
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns, "inferred_strategy must not be mutated");
        assert_eq!(schemas[0].toml_override, Some(UserOverride::Jsonb));
        assert_eq!(*schemas[0].effective_strategy(), InferredStrategy::Jsonb);
    }

    #[test]
    fn toml_normalize_dynamic_keys_sets_toml_override_and_preserves_inferred() {
        use crate::schema::table_schema::{ChildKind, ColumnSchema};
        let mut parent = simple_table("images");
        parent.inferred_strategy = InferredStrategy::Columns;
        parent.columns.push(ColumnSchema::generated("j2s_id", PgType::BigInt));
        // NormalizeDynamicKeys requires at least one Object child
        let mut child = TableSchema::new("images_12584".to_string(), vec!["images".to_string(), "12584".to_string()], 1);
        child.parent_table = Some("images".to_string());
        child.child_kind = Some(ChildKind::Object);
        let mut schemas = vec![parent, child];
        let mut cols = HashMap::new();
        cols.insert("strategy".to_string(), toml::Value::String("normalize_dynamic_keys".to_string()));
        cols.insert("id_column".to_string(), toml::Value::String("image_id".to_string()));
        let mut tables = HashMap::new();
        tables.insert("images".to_string(), cols);
        let config = SchemaConfig { tables, group: HashMap::new() };
        apply_overrides(&mut schemas, &config).unwrap();
        let images = schemas.iter().find(|s| s.name == "images").unwrap();
        assert_eq!(images.inferred_strategy, InferredStrategy::Columns, "inferred_strategy must not be mutated");
        assert_eq!(
            images.toml_override,
            Some(UserOverride::NormalizeDynamicKeys { id_column: "image_id".to_string() })
        );
        assert_eq!(
            *images.effective_strategy(),
            InferredStrategy::NormalizeDynamicKeys { id_column: "image_id".to_string() }
        );
    }

    fn simple_table(name: &str) -> TableSchema {
        TableSchema::new(name.to_string(), vec![name.to_string()], 0)
    }

    // --- apply_group_overrides (return Vec<ConfigWarning>) ---

    #[test]
    fn apply_group_overrides_merge_failure_returns_warning() {
        let mut schemas = vec![simple_table("a")];
        let mut group = HashMap::new();
        group.insert("merged".to_string(), GroupConfig {
            strategy: "keyed_pivot".to_string(),
            members: vec!["a".to_string(), "b".to_string()],
        });
        let config = SchemaConfig { tables: HashMap::new(), group };
        let warnings = apply_group_overrides(&mut schemas, &config);
        assert_eq!(warnings, vec![ConfigWarning::GroupMergeFailed {
            group: "merged".to_string(),
            found: 1,
            expected: 2,
        }]);
    }

    #[test]
    fn apply_group_overrides_unknown_strategy_returns_warning() {
        let mut schemas = vec![simple_table("a"), simple_table("b")];
        let mut group = HashMap::new();
        group.insert("merged".to_string(), GroupConfig {
            strategy: "magic".to_string(),
            members: vec!["a".to_string(), "b".to_string()],
        });
        let config = SchemaConfig { tables: HashMap::new(), group };
        let warnings = apply_group_overrides(&mut schemas, &config);
        assert_eq!(warnings, vec![ConfigWarning::UnknownGroupStrategy {
            group: "merged".to_string(),
            strategy: "magic".to_string(),
        }]);
    }

    #[test]
    fn apply_group_overrides_valid_merge_no_warnings() {
        let mut schemas = vec![simple_table("a"), simple_table("b")];
        let mut group = HashMap::new();
        group.insert("merged".to_string(), GroupConfig {
            strategy: "keyed_pivot".to_string(),
            members: vec!["a".to_string(), "b".to_string()],
        });
        let config = SchemaConfig { tables: HashMap::new(), group };
        let warnings = apply_group_overrides(&mut schemas, &config);
        assert!(warnings.is_empty());
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "merged");
    }

    #[test]
    fn config_warning_to_message_unknown_group_strategy() {
        let w = ConfigWarning::UnknownGroupStrategy {
            group: "g1".to_string(),
            strategy: "magic".to_string(),
        };
        let msg = w.to_message();
        assert!(msg.contains("magic"));
        assert!(msg.contains("g1"));
    }

    #[test]
    fn config_warning_invalid_override_to_message() {
        let w = ConfigWarning::InvalidOverride {
            table: "metrics".to_string(),
            from_strategy: "Pivot".to_string(),
            to_strategy: "columns".to_string(),
        };
        let msg = w.to_message();
        assert!(msg.contains("metrics"));
        assert!(msg.contains("Pivot"));
        assert!(msg.contains("columns"));
    }

    #[test]
    fn config_warning_invalid_override_clone_and_eq() {
        let w = ConfigWarning::InvalidOverride {
            table: "t".to_string(),
            from_strategy: "Jsonb".to_string(),
            to_strategy: "columns".to_string(),
        };
        assert_eq!(w.clone(), w);
        let w2 = ConfigWarning::InvalidOverride {
            table: "other".to_string(),
            from_strategy: "Jsonb".to_string(),
            to_strategy: "columns".to_string(),
        };
        assert_ne!(w, w2);
    }

    // --- apply_overrides_complete (aggregates both warning sources) ---

    #[test]
    fn apply_overrides_complete_aggregates_both_sources() {
        let mut schemas = vec![simple_table("users")];
        let mut tables = HashMap::new();
        tables.insert("ghost".to_string(), HashMap::new());
        let mut group = HashMap::new();
        group.insert("g".to_string(), GroupConfig {
            strategy: "keyed_pivot".to_string(),
            members: vec!["a".to_string(), "b".to_string()],
        });
        let config = SchemaConfig { tables, group };
        let warnings = apply_overrides_complete(&mut schemas, &config).unwrap();
        assert!(warnings.iter().any(|w| matches!(w, ConfigWarning::UnknownTable(_))));
        assert!(warnings.iter().any(|w| matches!(w, ConfigWarning::GroupMergeFailed { .. })));
    }

    #[test]
    fn apply_overrides_complete_no_warnings_clean_config() {
        let mut schemas = vec![simple_table("users")];
        let config = SchemaConfig { tables: HashMap::new(), group: HashMap::new() };
        let warnings = apply_overrides_complete(&mut schemas, &config).unwrap();
        assert!(warnings.is_empty());
    }

    /// Regression test for issue #22 risk #1: `exclude_absorbed_children()` (called inside
    /// `apply_overrides_complete`) reads `absorbs_children()` → `effective_strategy()`. If the
    /// cache recompute happens too late (or not before this call), a toml_override that flips
    /// `absorbs_children()` to true is invisible to this same function call, and the child that
    /// should now be absorbed survives incorrectly.
    #[test]
    fn apply_overrides_complete_recomputes_cache_before_excluding_absorbed_children() {
        let mut parent = simple_table("images");
        parent.inferred_strategy = InferredStrategy::Columns;
        // Simulate the baseline cache written by finalize() *before* any override exists.
        parent.cached_strategy = Some(InferredStrategy::Columns);
        let mut child = simple_table("images_items");
        child.parent_table = Some("images".to_string());
        child.cached_strategy = Some(InferredStrategy::Columns);
        let mut schemas = vec![parent, child];

        let mut tables = HashMap::new();
        tables.insert("images".to_string(), toml_strategy("jsonb"));
        let config = SchemaConfig { tables, group: HashMap::new() };

        apply_overrides_complete(&mut schemas, &config).unwrap();

        assert!(
            schemas.iter().all(|s| s.name != "images_items"),
            "images_items must be excluded once images' toml_override=Jsonb makes it absorb children"
        );
    }

    #[test]
    fn apply_user_overrides_skip_removes_table() {
        let mut schemas = vec![simple_table("a"), simple_table("b")];
        let mut overrides = HashMap::new();
        overrides.insert("a".to_string(), UserOverride::Skip);
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].name, "b");
    }

    #[test]
    fn apply_user_overrides_skip_removes_autosplit_companion() {
        let mut t = simple_table("products");
        t.inferred_strategy = InferredStrategy::AutoSplit {
            stable_threshold: 0.8,
            rare_threshold: 0.01,
            medium_keys: Default::default(),
            wide_table_name: "products_wide".to_string(),
        };
        let mut schemas = vec![t, simple_table("products_wide"), simple_table("orders")];
        let mut overrides = HashMap::new();
        overrides.insert("products".to_string(), UserOverride::Skip);
        apply_user_overrides(&mut schemas, &overrides);
        assert!(!schemas.iter().any(|s| s.name == "products"),       "main table removed");
        assert!(!schemas.iter().any(|s| s.name == "products_wide"),  "companion _wide removed");
        assert!(schemas.iter().any(|s| s.name == "orders"),          "unrelated table kept");
    }

    #[test]
    fn apply_user_overrides_pivot_sets_ui_override_and_preserves_inferred() {
        let mut schemas = vec![simple_table("tags")];
        let mut overrides = HashMap::new();
        overrides.insert("tags".to_string(), UserOverride::Pivot);
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].ui_override, Some(UserOverride::Pivot), "ui_override must be set");
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns, "inferred_strategy must not be mutated");
        assert_eq!(*schemas[0].effective_strategy(), InferredStrategy::Pivot);
        // Column layout must have been restructured (EAV: key + value)
        assert!(schemas[0].columns.iter().any(|c| c.name == "key"), "Pivot layout: key column expected");
        assert!(schemas[0].columns.iter().any(|c| c.name == "value"), "Pivot layout: value column expected");
    }

    #[test]
    fn apply_user_overrides_jsonb_sets_ui_override_and_preserves_inferred() {
        let mut schemas = vec![simple_table("blob")];
        let mut overrides = HashMap::new();
        overrides.insert("blob".to_string(), UserOverride::Jsonb);
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas[0].ui_override, Some(UserOverride::Jsonb), "ui_override must be set");
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns, "inferred_strategy must not be mutated");
        assert_eq!(*schemas[0].effective_strategy(), InferredStrategy::Jsonb);
        assert!(schemas[0].columns.iter().any(|c| c.name == "data"), "Jsonb layout: data column expected");
    }

    #[test]
    fn apply_user_overrides_pivot_does_not_overwrite_existing_ui_override() {
        let mut schemas = vec![simple_table("tags")];
        schemas[0].ui_override = Some(UserOverride::Jsonb); // already set from snapshot
        let mut overrides = HashMap::new();
        overrides.insert("tags".to_string(), UserOverride::Pivot); // strategy_overrides says Pivot
        apply_user_overrides(&mut schemas, &overrides);
        // Guard: ui_override.is_none() — existing Jsonb override must be preserved
        assert_eq!(schemas[0].ui_override, Some(UserOverride::Jsonb), "existing ui_override must not be overwritten");
    }

    #[test]
    fn apply_user_overrides_columns_on_columns_table_sets_ui_override() {
        let mut schemas = vec![simple_table("flat")];
        // simple_table infers Columns by default
        let mut overrides = HashMap::new();
        overrides.insert("flat".to_string(), UserOverride::Columns);
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas[0].ui_override, Some(UserOverride::Columns));
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Columns);
    }

    #[test]
    fn apply_user_overrides_columns_on_pivot_table_is_no_op() {
        use crate::schema::wide_strategies::apply_wide_strategy_columns;
        let mut schemas = vec![simple_table("metrics")];
        apply_wide_strategy_columns(&mut schemas[0], InferredStrategy::Pivot);
        schemas[0].inferred_strategy = InferredStrategy::Pivot;
        let mut overrides = HashMap::new();
        overrides.insert("metrics".to_string(), UserOverride::Columns);
        apply_user_overrides(&mut schemas, &overrides);
        // No ui_override should be set — incompatible layout
        assert_eq!(schemas[0].ui_override, None, "Columns override on Pivot must not set ui_override");
        assert_eq!(schemas[0].inferred_strategy, InferredStrategy::Pivot, "inferred_strategy unchanged");
    }

    #[test]
    fn apply_user_overrides_columns_does_not_overwrite_existing_ui_override() {
        let mut schemas = vec![simple_table("flat")];
        schemas[0].ui_override = Some(UserOverride::Jsonb);
        let mut overrides = HashMap::new();
        overrides.insert("flat".to_string(), UserOverride::Columns);
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas[0].ui_override, Some(UserOverride::Jsonb), "existing ui_override preserved");
    }

    #[test]
    fn apply_user_overrides_no_override_unchanged() {
        let mut schemas = vec![simple_table("unchanged")];
        let overrides: HashMap<String, UserOverride> = HashMap::new();
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas.len(), 1);
        assert!(matches!(schemas[0].inferred_strategy, InferredStrategy::Columns));
    }
}
