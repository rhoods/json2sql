use indexmap::IndexMap;

use crate::error::{J2sError, Result};
use super::finalizer::exclude_absorbed_children;
use super::observer::TableEntry;
use super::table_schema::{ChildKind, ColumnSchema, KeyShape, SuffixSchema, TableSchema, WideStrategy};
use super::type_tracker::{widen_pg_types, PgType};

/// Fraction of keys that must be numeric (or ISO-language codes) to classify a key shape as
/// KeyShape::Numeric / KeyShape::IsoLang rather than Slug or Mixed.
const KEY_SHAPE_DOMINANT_RATIO: f64 = 0.8;

/// Determine whether a wide table's values are type-homogeneous (→ Pivot) or not (→ Jsonb).
pub(crate) fn suggest_wide_strategy(entry: &TableEntry) -> WideStrategy {
    let mut has_string = false;
    let mut has_numeric = false;
    let mut has_boolean = false;
    let mut has_date_like = false;

    for tracker in entry.columns.values() {
        if tracker.is_object_field() || tracker.is_array_field() {
            continue;
        }
        match tracker.to_pg_type() {
            PgType::Text | PgType::VarChar(_) => has_string = true,
            PgType::Integer | PgType::BigInt | PgType::DoublePrecision => has_numeric = true,
            PgType::Boolean => has_boolean = true,
            PgType::Uuid | PgType::Date | PgType::Timestamp => has_date_like = true,
            PgType::Array(_) | PgType::Jsonb => has_string = true,
        }
    }

    let type_categories = [has_string, has_numeric, has_boolean, has_date_like]
        .iter()
        .filter(|&&x| x)
        .count();

    // Only one type category across all value columns → safe to pivot
    if type_categories <= 1 {
        WideStrategy::Pivot
    } else {
        WideStrategy::Jsonb
    }
}

/// Restructure a schema's data columns to match the given WideStrategy.
/// Replaces all non-generated columns with either (key, value) for Pivot
/// or (data JSONB) for Jsonb.
pub fn apply_wide_strategy_columns(schema: &mut TableSchema, strategy: WideStrategy) {
    match strategy {
        WideStrategy::Columns => {} // nothing to restructure
        WideStrategy::Pivot => {
            // Compute widest value type from existing data columns before clearing
            let value_type = schema
                .data_columns()
                .fold(None::<PgType>, |acc, col| {
                    Some(match acc {
                        None => col.pg_type.clone(),
                        Some(a) => widen_pg_types(a, &col.pg_type),
                    })
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
            schema.wide_strategy = WideStrategy::Pivot;
        }
        WideStrategy::Jsonb => {
            schema.columns.retain(|c| c.is_generated);
            schema.columns.push(ColumnSchema {
                name: "data".to_string(),
                original_name: "data".to_string(),
                pg_type: PgType::Jsonb,
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
            schema.wide_strategy = WideStrategy::Jsonb;
        }
        WideStrategy::StructuredPivot(suffix_schema) => {
            apply_structured_pivot_columns(schema, suffix_schema);
        }
        WideStrategy::KeyedPivot(_) => {
            // KeyedPivot is applied by finalize_siblings(), not through this path.
        }
        WideStrategy::AutoSplit { .. } | WideStrategy::Ignore => {
            // AutoSplit is handled inline in finalize(); Ignore is per-key, not per-table.
            // Neither reaches this function.
        }
        WideStrategy::NormalizeDynamicKeys { .. } | WideStrategy::Flatten { .. } | WideStrategy::JsonbFlatten => {
            // These strategies require the full schemas slice.
            // Use apply_normalize_dynamic_keys(), apply_flatten(), or apply_jsonb_flatten() instead.
        }
        WideStrategy::MultiKeyedPivot(_) => {
            // MultiKeyedPivot: parent keeps only its generated columns (no data columns).
            // The synthetic child pivot tables are created by finalize_siblings().
            schema.columns.retain(|c| c.is_generated);
        }
    }
}

/// Restructure a wide table's columns for StructuredPivot:
/// (j2s_id, j2s_parent_id, name TEXT, value <type>, <suffix_col>...)
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
    schema.wide_strategy = WideStrategy::StructuredPivot(suffix_schema);
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

/// Apply NormalizeDynamicKeys strategy to a table: collapse all its direct Object children
/// into a single normalized table with `id_column` TEXT + union of value columns.
///
/// Equivalent to a user-triggered KeyedPivot with a custom ID column name.
/// Call `exclude_absorbed_children` after to remove the now-absorbed child tables.
pub fn apply_normalize_dynamic_keys(
    schemas: &mut Vec<TableSchema>,
    table_name: &str,
    id_column: String,
) -> Result<()> {
    let target_idx = schemas.iter().position(|s| s.name == table_name)
        .ok_or_else(|| J2sError::Schema(format!("apply_normalize_dynamic_keys: table '{}' not found", table_name)))?;

    let child_indices: Vec<usize> = schemas
        .iter()
        .enumerate()
        .filter(|(_, s)| {
            s.parent_table.as_deref() == Some(table_name)
                && matches!(s.child_kind, Some(ChildKind::Object))
        })
        .map(|(i, _)| i)
        .collect();

    if child_indices.is_empty() {
        return Err(J2sError::Schema(format!(
            "apply_normalize_dynamic_keys: no Object children found for '{}'; strategy not applied",
            table_name
        )));
    }

    let children: Vec<&TableSchema> = child_indices.iter().map(|&i| &schemas[i]).collect();
    let union_cols = build_union_columns(&children);

    let keys: Vec<String> = child_indices
        .iter()
        .map(|&i| schemas[i].path.last().cloned().unwrap_or_default())
        .collect();
    let key_shape = classify_key_shape(&keys.iter().map(|s| s.as_str()).collect::<Vec<_>>());

    let target = &mut schemas[target_idx];
    target.columns.retain(|c| c.is_generated);
    target.columns.push(ColumnSchema {
        name: id_column.clone(),
        original_name: id_column.clone(),
        pg_type: PgType::Text,
        not_null: true,
        is_generated: false,
        is_parent_fk: false,
    });
    for col in union_cols {
        target.columns.push(col);
    }
    eprintln!(
        "  NormalizeDynamicKeys: {} ({} child tables → 1, id_col: {} [{}])",
        table_name,
        child_indices.len(),
        id_column,
        key_shape,
    );
    target.wide_strategy = WideStrategy::NormalizeDynamicKeys { id_column };

    exclude_absorbed_children(schemas);
    Ok(())
}

/// Apply Flatten strategy to a child table: inline its scalar columns into the parent table
/// with the given prefix. The child table is removed from the schema after inlining.
///
/// After this call, `schemas` no longer contains `child_table_name`. The parent table gains
/// new data columns and a populated `flatten_sources` map for Pass 2 lookups.
pub fn apply_flatten(
    schemas: &mut Vec<TableSchema>,
    child_table_name: &str,
    prefix: &str,
    max_depth: u8,
) -> Result<()> {
    // Collect info before any mutations (avoids borrow conflicts)
    let (parent_name, field_name, new_cols) = {
        let child = schemas.iter().find(|s| s.name == child_table_name)
            .ok_or_else(|| J2sError::Schema(format!("apply_flatten: table '{}' not found", child_table_name)))?;

        let parent_name = child.parent_table.clone()
            .ok_or_else(|| J2sError::Schema(format!(
                "apply_flatten: '{}' is a root table, cannot flatten into parent",
                child_table_name
            )))?;

        // The JSON field name is the last path segment of the child table
        let field_name = child.path.last()
            .cloned()
            .unwrap_or_else(|| child_table_name.to_string());

        // Build prefixed copies of all data columns (max_depth=1: scalars only)
        let new_cols: Vec<ColumnSchema> = child
            .data_columns()
            .map(|col| ColumnSchema {
                name: format!("{}{}", prefix, col.name),
                original_name: col.original_name.clone(),
                pg_type: col.pg_type.clone(),
                not_null: false, // flattened columns are always nullable in parent
                is_generated: false,
                is_parent_fk: false,
            })
            .collect();

        (parent_name, field_name, new_cols)
    };

    // Mark child as Flatten so absorbs_children() returns true for its descendants
    if let Some(child) = schemas.iter_mut().find(|s| s.name == child_table_name) {
        child.wide_strategy = WideStrategy::Flatten { prefix: prefix.to_string(), max_depth };
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
            "apply_flatten: parent table '{}' not found for '{}'",
            parent_name, child_table_name
        )));
    }

    // Remove the flattened child table from the schema
    schemas.retain(|s| !matches!(s.wide_strategy, WideStrategy::Flatten { .. }));
    Ok(())
}

/// Inline a child table as a single JSONB column on the parent table.
/// The child table is removed from the schema; the parent gains `{child_table_name} JSONB`.
/// Used for WideStrategy::JsonbFlatten (IHM override "JSONB inline").
#[allow(dead_code)]
pub fn apply_jsonb_flatten(schemas: &mut Vec<TableSchema>, child_table_name: &str) -> Result<()> {
    let (parent_name, field_name) = {
        let child = schemas.iter().find(|s| s.name == child_table_name)
            .ok_or_else(|| J2sError::Schema(format!("apply_jsonb_flatten: table '{}' not found", child_table_name)))?;
        let parent = child.parent_table.clone()
            .ok_or_else(|| J2sError::Schema(format!(
                "apply_jsonb_flatten: '{}' is a root table, cannot inline into parent",
                child_table_name
            )))?;
        // The JSON field name is the last path segment of the child table.
        let field = child.path.last()
            .cloned()
            .unwrap_or_else(|| child_table_name.to_string());
        (parent, field)
    };

    // Mark child as JsonbFlatten so absorbs_children() returns true for its descendants
    if let Some(child) = schemas.iter_mut().find(|s| s.name == child_table_name) {
        child.wide_strategy = WideStrategy::JsonbFlatten;
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
            "apply_jsonb_flatten: parent table '{}' not found for '{}'",
            parent_name, child_table_name
        )));
    }

    // Remove the child table and its absorbed descendants
    schemas.retain(|s| !matches!(s.wide_strategy, WideStrategy::JsonbFlatten));
    Ok(())
}
