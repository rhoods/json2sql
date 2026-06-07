//! Génération des colonnes pour les stratégies wide-table.
#![allow(clippy::cast_precision_loss)]
//!
//! **Pivot** : chaque clé JSON distincte devient une colonne (`key_fr`, `key_en`…).
//! **Structured pivot** : variante où les colonnes sont groupées par suffixe détecté.
//! **Keyed pivot** : pivot où la clé pivot est une colonne explicite (ex: code langue).
//! **Flatten / `JsonbFlatten`** : les colonnes de la table enfant remontent dans le parent.
//! **`NormalizeDynamicKeys`** : les clés dynamiques sont normalisées en table EAV.
//!
//! Frontière avec `finalizer.rs` : ce module génère les colonnes résultantes d'une
//! stratégie donnée. `finalizer.rs` décide *quelle* stratégie appliquer à chaque table.

use indexmap::IndexMap;

use crate::error::{J2sError, Result};
use super::finalizer::exclude_absorbed_children;
use super::observer::TableEntry;
use super::table_schema::{ChildKind, ColumnSchema, KeyShape, SuffixSchema, TableSchema, InferredStrategy};
use super::type_tracker::{widen_pg_types, PgType};

/// Fraction of keys that must be numeric (or ISO-language codes) to classify a key shape as
/// `KeyShape::Numeric` / `KeyShape::IsoLang` rather than Slug or Mixed.
const KEY_SHAPE_DOMINANT_RATIO: f64 = 0.8;

/// Determine whether a wide table's values are type-homogeneous (→ Pivot) or not (→ Jsonb).
pub fn suggest_wide_strategy(entry: &TableEntry) -> InferredStrategy {
    let mut has_string = false;
    let mut has_numeric = false;
    let mut has_boolean = false;
    let mut has_date_like = false;

    for tracker in entry.columns.values() {
        if tracker.is_object_field() || tracker.is_array_field() {
            continue;
        }
        match tracker.to_pg_type() {
            PgType::Text | PgType::VarChar(_) | PgType::Array(_) | PgType::Jsonb => has_string = true,
            PgType::Integer | PgType::BigInt | PgType::DoublePrecision => has_numeric = true,
            PgType::Boolean => has_boolean = true,
            PgType::Uuid | PgType::Date | PgType::Timestamp => has_date_like = true,
        }
    }

    let type_categories = [has_string, has_numeric, has_boolean, has_date_like]
        .iter()
        .filter(|&&x| x)
        .count();

    // Only one type category across all value columns → safe to pivot
    if type_categories <= 1 {
        InferredStrategy::Pivot
    } else {
        InferredStrategy::Jsonb
    }
}

/// Restructure a schema's data columns to match the given `InferredStrategy`.
fn apply_pivot_columns(schema: &mut TableSchema) {
    let value_type = schema
        .data_columns()
        .fold(None::<PgType>, |acc, col| {
            Some(acc.map_or_else(|| col.pg_type.clone(), |a| widen_pg_types(a, &col.pg_type)))
        })
        .unwrap_or(PgType::Text);
    schema.columns.retain(|c| c.is_generated);
    schema.columns.push(ColumnSchema {
        name: "key".to_string(),
        original_name: "key".to_string(),
        pg_type: PgType::Text,
        not_null: true,
        is_generated: false,
        is_parent_fk: false,
    });
    schema.columns.push(ColumnSchema {
        name: "value".to_string(),
        original_name: "value".to_string(),
        pg_type: value_type,
        not_null: false,
        is_generated: false,
        is_parent_fk: false,
    });
    schema.inferred_strategy = InferredStrategy::Pivot;
}

/// Replaces all non-generated columns with either (key, value) for Pivot
/// or (data JSONB) for Jsonb.
#[allow(clippy::too_many_lines)] // exhaustive match over all InferredStrategy variants
pub fn apply_wide_strategy_columns(schema: &mut TableSchema, strategy: InferredStrategy) {
    match strategy {
        InferredStrategy::Columns
        | InferredStrategy::SiblingCollapse(_)
        | InferredStrategy::AutoSplit { .. }
        | InferredStrategy::Ignore
        | InferredStrategy::NormalizeDynamicKeys { .. }
        | InferredStrategy::Flatten { .. }
        | InferredStrategy::JsonbFlatten => {
            // Applied elsewhere: SiblingCollapse via finalize_siblings(), AutoSplit/Ignore inline
            // in finalize(), NormalizeDynamicKeys/Flatten/JsonbFlatten via dedicated apply_* fns.
        }
        InferredStrategy::Pivot => {
            apply_pivot_columns(schema);
        }
        InferredStrategy::Jsonb => {
            schema.columns.retain(|c| c.is_generated);
            schema.columns.push(ColumnSchema {
                name: "data".to_string(),
                original_name: "data".to_string(),
                pg_type: PgType::Jsonb,
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
            schema.inferred_strategy = InferredStrategy::Jsonb;
        }
        InferredStrategy::StructuredPivot(suffix_schema) => {
            apply_structured_pivot_columns(schema, suffix_schema);
        }
        InferredStrategy::SiblingCollapseMulti(_) => {
            // SiblingCollapseMulti: parent keeps only its generated columns (no data columns).
            // The synthetic child pivot tables are created by finalize_siblings().
            schema.columns.retain(|c| c.is_generated);
        }
    }
}

/// Restructure a wide table's columns for `StructuredPivot`:
/// (`j2s_id`, `j2s_parent_id`, name TEXT, value <type>, <`suffix_col`>...)
pub fn apply_structured_pivot_columns(schema: &mut TableSchema, suffix_schema: SuffixSchema) {
    schema.columns.retain(|c| c.is_generated);
    schema.columns.push(ColumnSchema {
        name: "name".to_string(),
        original_name: "name".to_string(),
        pg_type: PgType::Text,
        not_null: true,
        is_generated: false,
        is_parent_fk: false,
    });
    schema.columns.push(ColumnSchema {
        name: "value".to_string(),
        original_name: "value".to_string(),
        pg_type: suffix_schema.value_type.clone(),
        not_null: false,
        is_generated: false,
        is_parent_fk: false,
    });
    for sc in &suffix_schema.suffix_cols {
        schema.columns.push(ColumnSchema {
            name: sc.col_name.clone(),
            // original_name = the suffix string so pass2 can look it up
            original_name: sc.suffix.clone(),
            pg_type: sc.pg_type.clone(),
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
    }
    schema.inferred_strategy = InferredStrategy::StructuredPivot(suffix_schema);
}

#[must_use]
pub fn build_union_columns(children: &[&TableSchema]) -> Vec<ColumnSchema> {
    let mut col_map: IndexMap<String, (String, PgType)> = IndexMap::new();
    for child in children {
        for col in child.data_columns() {
            col_map
                .entry(col.original_name.clone())
                .and_modify(|(_, t)| *t = widen_pg_types(t.clone(), &col.pg_type))
                .or_insert((col.name.clone(), col.pg_type.clone()));
        }
    }
    col_map
        .into_iter()
        .map(|(original_name, (name, pg_type))| ColumnSchema {
            name,
            original_name,
            pg_type,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        })
        .collect()
}

/// Classify the shape of sibling keys to produce a semantic column name.
#[must_use]
pub fn classify_key_shape(keys: &[&str]) -> KeyShape {
    let total = keys.len();
    if total == 0 {
        return KeyShape::Slug;
    }
    let numeric = keys
        .iter()
        .filter(|k| !k.is_empty() && k.chars().all(|c| c.is_ascii_digit()))
        .count();
    let isolang = keys
        .iter()
        .filter(|k| {
            (k.len() == 2 || k.len() == 3) && k.chars().all(|c| c.is_ascii_alphabetic())
        })
        .count();

    let numeric_ratio = numeric as f64 / total as f64;
    let isolang_ratio = isolang as f64 / total as f64;

    if numeric_ratio >= KEY_SHAPE_DOMINANT_RATIO {
        KeyShape::Numeric
    } else if isolang_ratio >= KEY_SHAPE_DOMINANT_RATIO {
        KeyShape::IsoLang
    } else if numeric > 0 && isolang > 0 {
        KeyShape::Mixed
    } else {
        KeyShape::Slug
    }
}

/// Apply `NormalizeDynamicKeys` strategy to a table: collapse all its direct Object children
/// into a single normalized table with `id_column` TEXT + union of value columns.
///
/// Equivalent to a user-triggered `SiblingCollapse` with a custom ID column name.
/// Call `exclude_absorbed_children` after to remove the now-absorbed child tables.
fn find_object_child_indices(schemas: &[TableSchema], table_name: &str) -> Result<Vec<usize>> {
    let indices: Vec<usize> = schemas.iter().enumerate()
        .filter(|(_, s)| {
            s.parent_table.as_deref() == Some(table_name)
                && matches!(s.child_kind, Some(ChildKind::Object))
        })
        .map(|(i, _)| i)
        .collect();
    if indices.is_empty() {
        return Err(J2sError::Schema(format!(
            "apply_normalize_dynamic_keys: no Object children found for '{table_name}'; strategy not applied"
        )));
    }
    Ok(indices)
}

fn rebuild_normalize_columns(target: &mut TableSchema, union_cols: Vec<ColumnSchema>, id_column: &str) {
    target.columns.retain(|c| c.is_generated);
    target.columns.push(ColumnSchema {
        name: id_column.to_string(), original_name: id_column.to_string(),
        pg_type: PgType::Text, not_null: true, is_generated: false, is_parent_fk: false,
    });
    target.columns.extend(union_cols);
}

pub fn apply_normalize_dynamic_keys(
    schemas: &mut Vec<TableSchema>,
    table_name: &str,
    id_column: String,
) -> Result<()> {
    let target_idx = schemas.iter().position(|s| s.name == table_name)
        .ok_or_else(|| J2sError::Schema(format!("apply_normalize_dynamic_keys: table '{table_name}' not found")))?;
    let child_indices = find_object_child_indices(schemas, table_name)?;
    let children: Vec<&TableSchema> = child_indices.iter().map(|&i| &schemas[i]).collect();
    let union_cols = build_union_columns(&children);
    let keys: Vec<String> = child_indices.iter()
        .map(|&i| schemas[i].path.last().cloned().unwrap_or_default()).collect();
    let key_shape = classify_key_shape(&keys.iter().map(std::string::String::as_str).collect::<Vec<_>>());
    rebuild_normalize_columns(&mut schemas[target_idx], union_cols, &id_column);
    eprintln!(
        "  NormalizeDynamicKeys: {} ({} child tables → 1, id_col: {} [{}])",
        table_name, child_indices.len(), id_column, key_shape,
    );
    schemas[target_idx].inferred_strategy = InferredStrategy::NormalizeDynamicKeys { id_column };
    exclude_absorbed_children(schemas);
    Ok(())
}

/// Apply Flatten strategy to a child table: inline its scalar columns into the parent table
/// with the given prefix. The child table is removed from the schema after inlining.
///
fn resolve_child_info(schemas: &[TableSchema], child_table_name: &str, caller: &str) -> Result<(usize, String, String)> {
    let (idx, child) = schemas.iter().enumerate()
        .find(|(_, s)| s.name == child_table_name)
        .ok_or_else(|| J2sError::Schema(format!("{caller}: table '{child_table_name}' not found")))?;
    let parent_name = child.parent_table.clone()
        .ok_or_else(|| J2sError::Schema(format!("{caller}: '{child_table_name}' is a root table, cannot flatten into parent")))?;
    let field_name = child.path.last().cloned().unwrap_or_else(|| child_table_name.to_string());
    Ok((idx, parent_name, field_name))
}

fn collect_flatten_info(
    schemas: &[TableSchema],
    child_table_name: &str,
    prefix: &str,
) -> Result<(String, String, Vec<ColumnSchema>)> {
    let (child_idx, parent_name, field_name) = resolve_child_info(schemas, child_table_name, "apply_flatten")?;
    // Build prefixed copies of all data columns (max_depth=1: scalars only)
    let new_cols: Vec<ColumnSchema> = schemas[child_idx]
        .data_columns()
        .map(|col| ColumnSchema {
            name: format!("{prefix}{}", col.name),
            original_name: col.original_name.clone(),
            pg_type: col.pg_type.clone(),
            not_null: false, // flattened columns are always nullable in parent
            is_generated: false,
            is_parent_fk: false,
        })
        .collect();
    Ok((parent_name, field_name, new_cols))
}

/// After this call, `schemas` no longer contains `child_table_name`. The parent table gains
/// new data columns and a populated `flatten_sources` map for Pass 2 lookups.
pub fn apply_flatten(
    schemas: &mut Vec<TableSchema>,
    child_table_name: &str,
    prefix: &str,
    max_depth: u8,
) -> Result<()> {
    // Collect info before any mutations (avoids borrow conflicts)
    let (parent_name, field_name, new_cols) = collect_flatten_info(schemas, child_table_name, prefix)?;

    // Mark child as Flatten so absorbs_children() returns true for its descendants
    if let Some(child) = schemas.iter_mut().find(|s| s.name == child_table_name) {
        child.inferred_strategy = InferredStrategy::Flatten { prefix: prefix.to_string(), max_depth };
    }

    // Remove descendants of the child (e.g. nutrients.sub_items)
    exclude_absorbed_children(schemas);

    // Add flattened columns + flatten_sources to parent
    if let Some(parent) = schemas.iter_mut().find(|s| s.name == parent_name) {
        for col in &new_cols {
            if !parent.columns.iter().any(|c| c.name == col.name) {
                parent.flatten_sources.insert(col.name.clone(), field_name.clone());
                parent.columns.push(col.clone());
            }
        }
        eprintln!(
            "  Flatten: {}.{} → {} columns inlined into {} (prefix: {:?})",
            parent_name,
            field_name,
            new_cols.len(),
            parent_name,
            prefix,
        );
    } else {
        return Err(J2sError::Schema(format!(
            "apply_flatten: parent table '{parent_name}' not found for '{child_table_name}'"
        )));
    }

    // Remove the flattened child table from the schema
    schemas.retain(|s| !matches!(s.inferred_strategy, InferredStrategy::Flatten { .. }));
    Ok(())
}

/// Inline a child table as a single JSONB column on the parent table.
/// The child table is removed from the schema; the parent gains `{child_table_name} JSONB`.
/// Used for `InferredStrategy::JsonbFlatten` (IHM override "JSONB inline").
#[allow(dead_code)] // pub API consumed by json2sql-ui (separate crate, invisible to binary dead_code lint)
pub fn apply_jsonb_flatten(schemas: &mut Vec<TableSchema>, child_table_name: &str) -> Result<()> {
    let (_, parent_name, field_name) = resolve_child_info(schemas, child_table_name, "apply_jsonb_flatten")?;

    // Mark child as JsonbFlatten so absorbs_children() returns true for its descendants
    if let Some(child) = schemas.iter_mut().find(|s| s.name == child_table_name) {
        child.inferred_strategy = InferredStrategy::JsonbFlatten;
    }

    // Remove any nested children of the child table
    exclude_absorbed_children(schemas);

    // Add JSONB column to parent (SQL name = child table name, original = JSON field name).
    if let Some(parent) = schemas.iter_mut().find(|s| s.name == parent_name) {
        if !parent.columns.iter().any(|c| c.name == child_table_name) {
            parent.columns.push(ColumnSchema {
                name: child_table_name.to_string(),
                original_name: field_name,
                pg_type: PgType::Jsonb,
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
        }
    } else {
        return Err(J2sError::Schema(format!(
            "apply_jsonb_flatten: parent table '{parent_name}' not found for '{child_table_name}'"
        )));
    }

    // Remove the child table and its absorbed descendants
    schemas.retain(|s| !matches!(s.inferred_strategy, InferredStrategy::JsonbFlatten));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::type_tracker::PgType;

    fn gen_col(name: &str, is_parent_fk: bool) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(), original_name: name.to_string(),
            pg_type: PgType::Text, not_null: false,
            is_generated: true, is_parent_fk,
        }
    }

    fn data_col(name: &str) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(), original_name: name.to_string(),
            pg_type: PgType::Text, not_null: false,
            is_generated: false, is_parent_fk: false,
        }
    }

    #[test]
    fn test_rebuild_normalize_columns_retains_generated_and_adds_id_and_union() {
        let mut target = TableSchema::new("t".to_string(), vec!["t".to_string()], 0);
        target.columns.push(gen_col("j2s_id", false));
        target.columns.push(gen_col("j2s_parent_id", true));
        target.columns.push(data_col("old_data")); // non-generated, must be dropped

        let union = vec![data_col("col_a"), data_col("col_b")];
        rebuild_normalize_columns(&mut target, union, "image_id");

        let names: Vec<&str> = target.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"j2s_id"),       "generated col must be kept");
        assert!(names.contains(&"j2s_parent_id"), "generated col must be kept");
        assert!(names.contains(&"image_id"),      "id_column must be added");
        assert!(names.contains(&"col_a"),         "union col must be added");
        assert!(names.contains(&"col_b"),         "union col must be added");
        assert!(!names.contains(&"old_data"),     "non-generated col must be dropped");
    }
}
