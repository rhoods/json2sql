use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use crate::error::{J2sError, Result};
use crate::schema::registry::{
    apply_flatten, apply_normalize_dynamic_keys, apply_structured_pivot_columns,
    apply_wide_strategy_columns, build_union_columns,
};
use crate::schema::suffix_detector::build_suffix_schema_from_list;
use crate::schema::table_schema::{ColumnSchema, KeyShape, SiblingSchema, TableSchema, WideStrategy};
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
/// Keys are the PostgreSQL column names (sanitized). Values are SQL type strings.
/// Special keys: `strategy`, `suffix_columns`.
/// Définition d'un groupe de fusion (KeyedPivot manuel).
#[derive(Debug, Deserialize)]
pub struct GroupConfig {
    pub strategy: String,
    pub members: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SchemaConfig {
    /// Groupes de fusion : nom_résultant → { strategy, members }
    #[serde(default)]
    pub group: HashMap<String, GroupConfig>,
    /// Overrides par table : table_name → { colonne_ou_strategy → valeur }
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

/// Apply type overrides from `config` to the finalized schemas.
/// Matches by table name and column name (both sanitized PostgreSQL identifiers).
/// Unknown tables or columns are silently ignored but reported via eprintln.
pub fn apply_overrides(schemas: &mut Vec<TableSchema>, config: &SchemaConfig) {
    // Collect deferred operations that require the full schemas slice.
    // These cannot be applied inside the single-schema iteration below.
    struct DeferredNormalize { table_name: String, id_column: String }
    struct DeferredFlatten  { table_name: String, prefix: String, max_depth: u8 }
    let mut deferred_normalize: Vec<DeferredNormalize> = Vec::new();
    let mut deferred_flatten:   Vec<DeferredFlatten>   = Vec::new();

    for (table_name, col_overrides) in &config.tables {
        match schemas.iter_mut().find(|s| &s.name == table_name) {
            None => {
                eprintln!(
                    "WARNING: schema-config: table '{}' not found in inferred schema",
                    table_name
                );
            }
            Some(schema) => {
                // --- strategy override ---
                // Must run before suffix_columns so the column layout is correct.
                // NormalizeDynamicKeys and Flatten are deferred (need full schemas slice).
                if let Some(toml::Value::String(strategy_str)) = col_overrides.get("strategy") {
                    match strategy_str.to_lowercase().as_str() {
                        "pivot" => {
                            if schema.wide_strategy != WideStrategy::Pivot {
                                eprintln!("  Override strategy: {} → Pivot", table_name);
                                apply_wide_strategy_columns(schema, WideStrategy::Pivot);
                            }
                        }
                        "jsonb" => {
                            if schema.wide_strategy != WideStrategy::Jsonb {
                                eprintln!("  Override strategy: {} → Jsonb", table_name);
                                apply_wide_strategy_columns(schema, WideStrategy::Jsonb);
                            }
                        }
                        "columns" => {
                            if schema.wide_strategy != WideStrategy::Columns {
                                eprintln!("  Override strategy: {} → Columns", table_name);
                                apply_wide_strategy_columns(schema, WideStrategy::Columns);
                            }
                        }
                        // "structured_pivot" is handled via suffix_columns below
                        "structured_pivot" => {}
                        "normalize_dynamic_keys" => {
                            let id_col = col_overrides
                                .get("id_column")
                                .and_then(|v| if let toml::Value::String(s) = v { Some(s.clone()) } else { None })
                                .unwrap_or_else(|| "key_id".to_string());
                            deferred_normalize.push(DeferredNormalize {
                                table_name: table_name.clone(),
                                id_column: id_col,
                            });
                        }
                        "flatten" => {
                            let prefix = col_overrides
                                .get("prefix")
                                .and_then(|v| if let toml::Value::String(s) = v { Some(s.clone()) } else { None })
                                .unwrap_or_else(|| format!("{}_", table_name));
                            let max_depth = col_overrides
                                .get("max_depth")
                                .and_then(|v| if let toml::Value::Integer(n) = v { Some(*n as u8) } else { None })
                                .unwrap_or(1);
                            deferred_flatten.push(DeferredFlatten {
                                table_name: table_name.clone(),
                                prefix,
                                max_depth,
                            });
                        }
                        other => {
                            eprintln!(
                                "WARNING: schema-config: unknown strategy '{}' for '{}', ignored",
                                other, table_name
                            );
                        }
                    }
                }

                // --- suffix_columns override → StructuredPivot ---
                if let Some(toml::Value::Array(arr)) = col_overrides.get("suffix_columns") {
                    let suffix_list: Vec<String> = arr
                        .iter()
                        .filter_map(|v| {
                            if let toml::Value::String(s) = v {
                                Some(s.clone())
                            } else {
                                None
                            }
                        })
                        .collect();

                    if !suffix_list.is_empty() {
                        // We need the TypeTracker map to infer types, but at config-apply time
                        // the schema is already finalized (columns are resolved).
                        // Build a dummy IndexMap from existing column types so
                        // build_suffix_schema_from_list can widen types correctly.
                        let mut type_map: indexmap::IndexMap<
                            String,
                            crate::schema::type_tracker::TypeTracker,
                        > = indexmap::IndexMap::new();
                        for col in schema.data_columns() {
                            let mut tracker = crate::schema::type_tracker::TypeTracker::new(256);
                            // Inject a fake value matching the resolved type so the tracker
                            // returns the same type.  We prime the tracker's type_counts directly
                            // by observing a representative value.
                            prime_tracker_from_pg_type(&mut tracker, &col.pg_type);
                            type_map.insert(col.original_name.clone(), tracker);
                        }
                        let suffix_schema =
                            build_suffix_schema_from_list(&suffix_list, &type_map);
                        eprintln!(
                            "  Override strategy: {} → StructuredPivot (suffixes: {:?})",
                            table_name, suffix_list
                        );
                        apply_structured_pivot_columns(schema, suffix_schema);
                    }
                }

                // --- column-level type overrides ---
                for (col_name, value) in col_overrides {
                    if matches!(
                        col_name.as_str(),
                        "strategy" | "suffix_columns" | "id_column" | "prefix" | "max_depth"
                    ) {
                        continue;
                    }
                    let type_str = match value {
                        toml::Value::String(s) => s.as_str(),
                        _ => continue, // non-string values are not type overrides
                    };
                    match schema.columns.iter_mut().find(|c| &c.name == col_name) {
                        None => {
                            eprintln!(
                                "WARNING: schema-config: column '{}.{}' not found",
                                table_name, col_name
                            );
                        }
                        Some(col) => match parse_pg_type(type_str) {
                            None => {
                                eprintln!(
                                    "WARNING: schema-config: unknown type '{}' for '{}.{}', ignored",
                                    type_str, table_name, col_name
                                );
                            }
                            Some(pg_type) => {
                                eprintln!(
                                    "  Override: {}.{} {} → {}",
                                    table_name,
                                    col_name,
                                    col.pg_type.as_sql(),
                                    pg_type.as_sql()
                                );
                                col.pg_type = pg_type;
                            }
                        },
                    }
                }
            }
        }
    }

    // Apply deferred operations that need the full schemas slice.
    for op in deferred_normalize {
        apply_normalize_dynamic_keys(schemas, &op.table_name, op.id_column);
    }
    for op in deferred_flatten {
        apply_flatten(schemas, &op.table_name, &op.prefix, op.max_depth);
    }
}

/// Appliquer les groupes de fusion définis dans la config.
/// Doit être appelé APRÈS `apply_overrides` et AVANT la sauvegarde du snapshot.
pub fn apply_group_overrides(schemas: &mut Vec<TableSchema>, config: &SchemaConfig) {
    for (group_name, group_cfg) in &config.group {
        match group_cfg.strategy.to_lowercase().as_str() {
            "keyed_pivot" => apply_keyed_pivot_merge(schemas, group_name, &group_cfg.members),
            other => eprintln!(
                "WARNING: group '{}': stratégie '{}' non supportée, ignoré",
                group_name, other
            ),
        }
    }
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
    let refs: Vec<&TableSchema> = cloned.iter().collect();
    let first = &cloned[0];

    let mut merged =
        TableSchema::new(group_name.to_string(), vec![group_name.to_string()], first.depth);
    merged.parent_table = first.parent_table.clone();
    merged.child_kind = first.child_kind.clone();

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
    merged.wide_strategy = WideStrategy::KeyedPivot(SiblingSchema {
        key_col_name: "key_id".to_string(),
        key_shape: KeyShape::Mixed,
    });

    // Retirer les membres du plus grand index au plus petit pour éviter le décalage
    for &i in indices.iter().rev() {
        schemas.remove(i);
    }
    schemas.insert(insert_pos, merged);

    eprintln!(
        "  Groupe '{}' : {} tables → 1 (KeyedPivot)",
        group_name,
        indices.len()
    );
}

/// Prime a TypeTracker with a representative observation so `to_pg_type()` returns
/// a type consistent with the given `PgType`.  Used when rebuilding type maps from
/// already-resolved column schemas.
fn prime_tracker_from_pg_type(
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
    *tracker.type_counts.entry(inferred).or_insert(0) += 1;
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
}
