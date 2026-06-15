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

struct DeferredNormalize { table_name: String, id_column: String }
struct DeferredFlatten  { table_name: String, prefix: String, max_depth: u8 }

/// Apply type overrides from `config` to the finalized schemas.
/// Matches by table name and column name (both sanitized `PostgreSQL` identifiers).
/// Unknown tables or columns are silently ignored but reported via eprintln.
pub fn apply_overrides(schemas: &mut Vec<TableSchema>, config: &SchemaConfig) -> crate::error::Result<()> {
    let mut deferred_normalize: Vec<DeferredNormalize> = Vec::new();
    let mut deferred_flatten:   Vec<DeferredFlatten>   = Vec::new();

    for (table_name, col_overrides) in &config.tables {
        match schemas.iter_mut().find(|s| &s.name == table_name) {
            None => eprintln!("WARNING: schema-config: table '{table_name}' not found in inferred schema"),
            Some(schema) => {
                // Strategy must run before suffix_columns so the column layout is correct.
                // NormalizeDynamicKeys and Flatten are deferred (need full schemas slice).
                apply_strategy_override(schema, table_name, col_overrides, &mut deferred_normalize, &mut deferred_flatten);
                apply_suffix_columns_override(schema, table_name, col_overrides);
                apply_column_type_overrides(schema, table_name, col_overrides);
            }
        }
    }

    for op in deferred_normalize {
        apply_normalize_dynamic_keys(schemas, &op.table_name, op.id_column)?;
    }
    for op in deferred_flatten {
        apply_flatten(schemas, &op.table_name, &op.prefix, op.max_depth)?;
    }
    Ok(())
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
) {
    let Some(toml::Value::String(strategy_str)) = col_overrides.get("strategy") else { return };
    match strategy_str.to_lowercase().as_str() {
        "pivot" => {
            if schema.inferred_strategy != InferredStrategy::Pivot {
                eprintln!("  Override strategy: {table_name} → Pivot");
                apply_wide_strategy_columns(schema, InferredStrategy::Pivot);
            }
        }
        "jsonb" => {
            if schema.inferred_strategy != InferredStrategy::Jsonb {
                eprintln!("  Override strategy: {table_name} → Jsonb");
                apply_wide_strategy_columns(schema, InferredStrategy::Jsonb);
            }
        }
        "columns" => {
            if schema.inferred_strategy != InferredStrategy::Columns {
                eprintln!("  Override strategy: {table_name} → Columns");
                apply_wide_strategy_columns(schema, InferredStrategy::Columns);
            }
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
        other => eprintln!("WARNING: schema-config: unknown strategy '{other}' for '{table_name}', ignored"),
    }
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
) {
    for (col_name, value) in col_overrides {
        if matches!(col_name.as_str(), "strategy" | "suffix_columns" | "id_column" | "prefix" | "max_depth") {
            continue;
        }
        let toml::Value::String(type_str) = value else { continue };
        match schema.columns.iter_mut().find(|c| &c.name == col_name) {
            None => eprintln!("WARNING: schema-config: column '{table_name}.{col_name}' not found"),
            Some(col) => match parse_pg_type(type_str) {
                None => eprintln!("WARNING: schema-config: unknown type '{type_str}' for '{table_name}.{col_name}', ignored"),
                Some(pg_type) => {
                    eprintln!("  Override: {}.{} {} → {}", table_name, col_name, col.pg_type.as_sql(), pg_type.as_sql());
                    col.pg_type = pg_type;
                }
            },
        }
    }
}

/// Appliquer les groupes de fusion définis dans la config.
/// Doit être appelé APRÈS `apply_overrides` et AVANT la sauvegarde du snapshot.
pub fn apply_group_overrides(schemas: &mut Vec<TableSchema>, config: &SchemaConfig) {
    for (group_name, group_cfg) in &config.group {
        match group_cfg.strategy.to_lowercase().as_str() {
            "keyed_pivot" => apply_keyed_pivot_merge(schemas, group_name, &group_cfg.members),
            other => eprintln!(
                "WARNING: group '{group_name}': stratégie '{other}' non supportée, ignoré"
            ),
        }
    }
}

/// Apply all config overrides in sequence, then re-run child exclusion.
///
/// Strategy overrides may change a parent table from `Columns` to `Jsonb`/`Pivot`, which
/// means its former child tables would receive no data. `exclude_absorbed_children` removes
/// them so Pass 2 never tries to insert into non-existent tables.
pub fn apply_overrides_complete(
    schemas: &mut Vec<TableSchema>,
    config: &SchemaConfig,
) -> crate::error::Result<()> {
    apply_overrides(schemas, config)?;
    apply_group_overrides(schemas, config);
    crate::schema::finalizer::exclude_absorbed_children(schemas);
    Ok(())
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
        data_col_name: String::new(),
    });
    merged
}

fn apply_keyed_pivot_merge(schemas: &mut Vec<TableSchema>, group_name: &str, members: &[String]) {
    let mut indices: Vec<usize> = members
        .iter()
        .filter_map(|name| schemas.iter().position(|s| &s.name == name))
        .collect();

    if indices.len() < 2 {
        eprintln!(
            "WARNING: group '{}': {}/{} membre(s) trouvé(s), fusion ignorée",
            group_name, indices.len(), members.len()
        );
        return;
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
                UserOverride::Pivot => apply_wide_strategy_columns(s, InferredStrategy::Pivot),
                UserOverride::Jsonb => apply_wide_strategy_columns(s, InferredStrategy::Jsonb),
                UserOverride::Skip  => {}
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

        apply_overrides(&mut schemas, &config);

        let col = schemas[0].columns.iter().find(|c| c.name == "age").unwrap();
        assert_eq!(col.pg_type, PgType::Integer);
    }

    fn simple_table(name: &str) -> TableSchema {
        TableSchema::new(name.to_string(), vec![name.to_string()], 0)
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
    fn apply_user_overrides_pivot_sets_strategy() {
        let mut schemas = vec![simple_table("tags")];
        let mut overrides = HashMap::new();
        overrides.insert("tags".to_string(), UserOverride::Pivot);
        apply_user_overrides(&mut schemas, &overrides);
        assert_eq!(schemas.len(), 1);
        assert!(matches!(schemas[0].inferred_strategy, InferredStrategy::Pivot));
    }

    #[test]
    fn apply_user_overrides_jsonb_sets_strategy() {
        let mut schemas = vec![simple_table("blob")];
        let mut overrides = HashMap::new();
        overrides.insert("blob".to_string(), UserOverride::Jsonb);
        apply_user_overrides(&mut schemas, &overrides);
        assert!(matches!(schemas[0].inferred_strategy, InferredStrategy::Jsonb));
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
