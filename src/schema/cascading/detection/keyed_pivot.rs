//! Post-pass keyed-pivot — fusion des orphelins `Columns` sous un parent `SiblingCollapse`.
//!
//! Extrait de `detection.rs` (voir `super` pour l'orchestration et les types partagés).

use super::super::super::wide_strategies::{build_union_columns, classify_key_shape};
use super::super::super::table_schema::{ChildKind, ColumnSchema, InferredStrategy, KeyShape, SiblingSchema, TableSchema};
use super::super::super::type_tracker::PgType;
use super::super::scoring::{pairwise_jaccard_min, unique_cluster_suffix};
use super::{cascade, CoSiblingGroup};

/// Post-pass: merge `Columns` children of `SiblingCollapse` parents into a synthetic sub-pivot.
///
/// After the main BFS cascade, some `Columns` tables survive as direct children of a
/// `SiblingCollapse` parent — for example, the lang-code T tables produced by cascade wave 1
/// (one per shared language across image types).  These tables are similar to each other
/// (same schema) but the main `run_sibling_wave` skips them because their parent is no
/// longer `InferredStrategy::Columns`.
///
/// This function detects groups of ≥ `threshold` such orphans with Jaccard ≥ `min_jaccard`,
/// creates a `{parent}_key` sub-pivot, re-parents the orphans under it, and updates the
fn resolve_pivot_key_info(
    schemas: &[TableSchema],
    child_indices: &[usize],
    parent_name: &str,
) -> (String, KeyShape, Vec<ColumnSchema>, String) {
    let keys: Vec<String> = child_indices
        .iter()
        .map(|&i| schemas[i].path.last().cloned().unwrap_or_default())
        .collect();
    let key_shape = classify_key_shape(&keys.iter().map(std::string::String::as_str).collect::<Vec<_>>());
    let key_col_name = match &key_shape {
        KeyShape::Numeric => "key_id".to_string(),
        KeyShape::IsoLang => "lang_code".to_string(),
        _ => "key".to_string(),
    };
    let children_refs: Vec<&TableSchema> = child_indices.iter().map(|&i| &schemas[i]).collect();
    let union_cols = build_union_columns(&children_refs);
    let suffix = unique_cluster_suffix(parent_name, "key", schemas);
    let sub_pivot_name = format!("{parent_name}_{suffix}");
    (key_col_name, key_shape, union_cols, sub_pivot_name)
}


fn build_sub_pivot_schema(
    sub_pivot_name: String,
    parent_name: String,
    sub_path: Vec<String>,
    parent_depth: usize,
    cols: Vec<ColumnSchema>,
    sibling_schema: SiblingSchema,
    // Sum of source children's row_count — approximation for classify_tables.
    source_row_count: u64,
) -> TableSchema {
    let mut schema = TableSchema::new(sub_pivot_name, sub_path, parent_depth + 1);
    schema.parent_table = Some(parent_name);
    schema.columns = cols;
    schema.child_kind = Some(ChildKind::Object);
    schema.inferred_strategy = InferredStrategy::SiblingCollapse(sibling_schema);
    schema.row_count = source_row_count;
    schema
}


fn process_keyed_pivot_work_item(
    schemas: &mut [TableSchema],
    parent_idx: usize,
    child_indices: &[usize],
    min_jaccard: f64,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Option<(TableSchema, Vec<CoSiblingGroup>)> {
    let jaccard = pairwise_jaccard_min(schemas, child_indices);
    if jaccard < min_jaccard { return None; }
    let parent_name = schemas[parent_idx].name.clone();
    let parent_depth = schemas[parent_idx].depth;
    let mut sub_path = schemas[parent_idx].path.clone();
    sub_path.push("key".to_string());
    let (key_col_name, key_shape, union_cols, sub_pivot_name) =
        resolve_pivot_key_info(schemas, child_indices, &parent_name);
    let fk_col = format!("j2s_{parent_name}_id");
    let sibling_schema = SiblingSchema { key_col_name: key_col_name.clone(), key_shape, array_children: false };
    let cols = build_sub_pivot_columns(&fk_col, &key_col_name, &union_cols);
    let co_sibs = collect_pivot_co_siblings(schemas, child_indices, &sub_pivot_name, obj_map, arr_map);
    reparent_and_update_routes(schemas, parent_idx, child_indices, &sub_pivot_name);
    let source_row_count: u64 = child_indices.iter().map(|&i| schemas[i].row_count).sum();
    eprintln!("  SiblingCollapse post-pass: {} ({} orphan tables → sub-pivot {})", parent_name, child_indices.len(), sub_pivot_name);
    Some((build_sub_pivot_schema(sub_pivot_name, parent_name, sub_path, parent_depth, cols, sibling_schema, source_row_count), co_sibs))
}


/// parent's `child_routes` to point to the sub-pivot.  Their own children are returned as
/// `CoSiblingGroup`s for an additional cascade wave.
pub(super) fn run_keyed_pivot_children_wave(
    schemas: &mut Vec<TableSchema>,
    threshold: usize,
    min_jaccard: f64,
) -> Vec<CoSiblingGroup> {
    let (obj_map, arr_map) = super::build_parent_child_maps(schemas);
    let work = collect_keyed_pivot_work_items(schemas, &obj_map, threshold);

    let mut new_schemas: Vec<TableSchema> = Vec::new();
    let mut co_siblings: Vec<CoSiblingGroup> = Vec::new();

    for (parent_idx, child_indices) in work {
        if let Some((t_schema, co_sibs)) =
            process_keyed_pivot_work_item(schemas, parent_idx, &child_indices, min_jaccard, &obj_map, &arr_map)
        {
            new_schemas.push(t_schema);
            co_siblings.extend(co_sibs);
        }
    }

    schemas.append(&mut new_schemas);
    co_siblings
}


fn collect_pivot_co_siblings(
    schemas: &mut [TableSchema],
    child_indices: &[usize],
    sub_pivot_name: &str,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Vec<CoSiblingGroup> {
    let children_by_key = cascade::collect_children_by_key(schemas, child_indices, obj_map, arr_map);
    let mut result = Vec::new();
    for (json_key, siblings, arr) in children_by_key {
        if siblings.len() >= 2 {
            result.push(CoSiblingGroup {
                synthetic_parent_name: sub_pivot_name.to_string(),
                json_key,
                sibling_indices: siblings,
                array_children: arr,
            });
        } else if let Some(&sole_idx) = siblings.first() {
            schemas[sole_idx].parent_table = Some(sub_pivot_name.to_string());
        }
    }
    result
}


/// Collect (`parent_idx`, `sorted_child_indices`) for `SiblingCollapse` parents with enough Columns children.
///
/// Only children registered in `child_routes` are considered — this excludes original absorbed
/// siblings (still in `schemas` at this point) from diluting the Jaccard score.
fn collect_keyed_pivot_work_items(
    schemas: &[TableSchema],
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    threshold: usize,
) -> Vec<(usize, Vec<usize>)> {
    schemas
        .iter()
        .enumerate()
        .filter_map(|(parent_idx, s)| {
            if !matches!(s.inferred_strategy, InferredStrategy::SiblingCollapse(_)) {
                return None;
            }
            let routed: std::collections::HashSet<&str> =
                s.child_routes.values().map(std::string::String::as_str).collect();
            let mut children: Vec<usize> = obj_map
                .get(&s.name)
                .into_iter()
                .flatten()
                .copied()
                .filter(|&i| matches!(schemas[i].inferred_strategy, InferredStrategy::Columns))
                .filter(|&i| routed.contains(schemas[i].name.as_str()))
                .collect();
            if children.len() < threshold { return None; }
            children.sort_unstable_by_key(|&i| &schemas[i].name);
            Some((parent_idx, children))
        })
        .collect()
}


/// Build the column list for a sub-pivot `SiblingCollapse` table.
fn build_sub_pivot_columns(
    fk_col: &str,
    key_col_name: &str,
    union_cols: &[ColumnSchema],
) -> Vec<ColumnSchema> {
    let mut cols = vec![
        ColumnSchema::generated("j2s_id", PgType::Uuid),
        ColumnSchema {
            name: fk_col.to_string(),
            original_name: fk_col.to_string(),
            pg_type: PgType::Uuid,
            not_null: true,
            is_generated: true,
            is_parent_fk: true,
        },
        ColumnSchema {
            name: key_col_name.to_string(),
            original_name: key_col_name.to_string(),
            pg_type: PgType::Text,
            not_null: true,
            is_generated: false,
            is_parent_fk: false,
        },
    ];
    cols.extend(union_cols.iter().cloned());
    cols
}


/// Re-parent absorbed children to `sub_pivot_name` and update `parent`'s `child_routes`.
fn reparent_and_update_routes(
    schemas: &mut [TableSchema],
    parent_idx: usize,
    child_indices: &[usize],
    sub_pivot_name: &str,
) {
    let absorbed_names: std::collections::HashSet<String> =
        child_indices.iter().map(|&i| schemas[i].name.clone()).collect();
    for &i in child_indices {
        schemas[i].parent_table = Some(sub_pivot_name.to_string());
    }
    for val in schemas[parent_idx].child_routes.values_mut() {
        if absorbed_names.contains(val.as_str()) {
            *val = sub_pivot_name.to_string();
        }
    }
}




#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::super::table_schema::{InferredStrategy, KeyShape, SiblingSchema};

    #[test]
    fn test_build_sub_pivot_schema_wires_fields() {
        use crate::schema::table_schema::{SiblingSchema, KeyShape};
        let sib = SiblingSchema {
            key_col_name: "key".to_string(),
            key_shape: KeyShape::Slug,
            array_children: false,
        };
        let schema = build_sub_pivot_schema(
            "p_pivot".to_string(),
            "p".to_string(),
            vec!["p".to_string(), "key".to_string()],
            1,
            vec![],
            sib,
            0,
        );
        assert_eq!(schema.name, "p_pivot");
        assert_eq!(schema.parent_table.as_deref(), Some("p"));
        assert_eq!(schema.depth, 2);
        assert!(matches!(schema.inferred_strategy, InferredStrategy::SiblingCollapse(_)));
    }

    #[test]
    fn build_sub_pivot_schema_propagates_row_count() {
        use crate::schema::table_schema::{SiblingSchema, KeyShape};
        let sib = SiblingSchema {
            key_col_name: "key".to_string(),
            key_shape: KeyShape::Slug,
            array_children: false,
        };
        let schema = build_sub_pivot_schema(
            "p_pivot".to_string(),
            "p".to_string(),
            vec!["p".to_string(), "key".to_string()],
            1,
            vec![],
            sib,
            42_000,
        );
        assert_eq!(schema.row_count, 42_000, "sub_pivot row_count must equal source_row_count");
    }
}
