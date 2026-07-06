//! Application des collapses — transformation des schémas détectés en tables pivot.
//!
//! Extrait de `detection.rs` (voir `super` pour l'orchestration et les types partagés).

use super::super::super::table_schema::{ChildKind, ColumnSchema, InferredStrategy, SiblingSchema, TableSchema};
use super::super::super::type_tracker::PgType;
use super::{cascade, Collapse, CollapseKind, CoSiblingGroup, SubgroupData};

fn apply_single_collapse(
    schemas: &mut [TableSchema],
    collapse: &Collapse,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Vec<CoSiblingGroup> {
    let CollapseKind::Single { key_col_name, key_shape, union_cols } = &collapse.kind else { return Vec::new(); };
    let sibling_schema = SiblingSchema {
        key_col_name: key_col_name.clone(),
        key_shape: key_shape.clone(),
        array_children: collapse.array_children,
    };
    let parent = &mut schemas[collapse.parent_idx];
    parent.columns.retain(|c| c.is_generated);
    if collapse.array_children {
        parent.columns.push(ColumnSchema::generated("j2s_order", PgType::BigInt));
    }
    parent.columns.push(ColumnSchema { name: key_col_name.clone(), original_name: key_col_name.clone(), pg_type: PgType::Text, not_null: true, is_generated: false, is_parent_fk: false });
    for col in union_cols { parent.columns.push(col.clone()); }
    parent.inferred_strategy = InferredStrategy::SiblingCollapse(sibling_schema);
    let synthetic_parent_name = schemas[collapse.parent_idx].name.clone();
    cascade::collect_children_by_key(schemas, &collapse.absorbed_indices, obj_map, arr_map)
        .into_iter()
        .filter(|(_, siblings, _)| siblings.len() >= 2)
        .map(|(json_key, sibling_indices, array_children)| CoSiblingGroup { synthetic_parent_name: synthetic_parent_name.clone(), json_key, sibling_indices, array_children })
        .collect()
}


struct ParentCtx<'a> {
    name: &'a str,
    path: &'a [String],
    depth: usize,
    array_children: bool,
}


pub(super) fn make_pivot_preamble(parent_name: &str, array_children: bool) -> Vec<ColumnSchema> {
    let fk_col = format!("j2s_{parent_name}_id");
    let mut cols = vec![
        ColumnSchema::generated("j2s_id", PgType::Uuid),
        ColumnSchema {
            name: fk_col.clone(),
            original_name: fk_col,
            pg_type: PgType::Uuid,
            not_null: true,
            is_generated: true,
            is_parent_fk: true,
        },
    ];
    if array_children {
        cols.push(ColumnSchema::generated("j2s_order", PgType::BigInt));
    }
    cols
}


fn build_multi_group_entry(
    g: &SubgroupData,
    parent: &ParentCtx<'_>,
    schemas: &[TableSchema],
    name_to_idx: &std::collections::HashMap<String, usize>,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> (TableSchema, Vec<CoSiblingGroup>) {
    let mut cols = make_pivot_preamble(parent.name, parent.array_children);
    cols.push(ColumnSchema { name: g.key_col_name.clone(), original_name: g.key_col_name.clone(), pg_type: PgType::Text, not_null: true, is_generated: false, is_parent_fk: false });
    for col in &g.union_cols { cols.push(col.clone()); }
    let mut path = parent.path.to_vec();
    path.push(g.path_segment.clone());
    let sibling_schema = SiblingSchema { key_col_name: g.key_col_name.clone(), key_shape: g.key_shape.clone(), array_children: parent.array_children };
    let pivot_name = g.pivot_table_name.clone();
    let absorbed_idx: Vec<usize> = g.absorbed_names.iter().filter_map(|n| name_to_idx.get(n.as_str()).copied()).collect();
    // Sum absorbed tables' row_count as an approximation for classify_tables.
    let source_row_count: u64 = absorbed_idx.iter().map(|&i| schemas[i].row_count).sum();
    let mut co_siblings: Vec<CoSiblingGroup> = Vec::new();
    for (json_key, siblings, arr) in cascade::collect_children_by_key(schemas, &absorbed_idx, obj_map, arr_map) {
        if siblings.len() >= 2 { co_siblings.push(CoSiblingGroup { synthetic_parent_name: pivot_name.clone(), json_key, sibling_indices: siblings, array_children: arr }); }
    }
    let mut schema = TableSchema::new(pivot_name, path, parent.depth + 1);
    schema.parent_table = Some(parent.name.to_string());
    schema.columns = cols;
    schema.child_kind = Some(ChildKind::Object);
    schema.inferred_strategy = InferredStrategy::SiblingCollapse(sibling_schema);
    schema.row_count = source_row_count;
    (schema, co_siblings)
}


pub(super) fn apply_multi_collapse(
    schemas: &mut [TableSchema],
    collapse: &Collapse,
    name_to_idx: &std::collections::HashMap<String, usize>,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> (Vec<TableSchema>, Vec<CoSiblingGroup>) {
    let CollapseKind::Multi { groups } = &collapse.kind else { return (Vec::new(), Vec::new()); };
    let sibling_groups = groups.iter().map(|g| crate::schema::table_schema::SiblingGroup {
        pivot_table: g.pivot_table_name.clone(),
        key_is_numeric: g.key_is_numeric,
        sibling_schema: SiblingSchema { key_col_name: g.key_col_name.clone(), key_shape: g.key_shape.clone(), array_children: collapse.array_children },
        absorbed_names: g.absorbed_names.clone(),
        path_segment: g.path_segment.clone(),
        absorbed_path_segments: g.absorbed_path_segments.clone(),
    }).collect();
    schemas[collapse.parent_idx].inferred_strategy = InferredStrategy::SiblingCollapseMulti(sibling_groups);
    let parent_ctx = ParentCtx {
        name: &schemas[collapse.parent_idx].name.clone(),
        path: &schemas[collapse.parent_idx].path.clone(),
        depth: schemas[collapse.parent_idx].depth,
        array_children: collapse.array_children,
    };
    let mut new_schemas: Vec<TableSchema> = Vec::new();
    let mut co_siblings: Vec<CoSiblingGroup> = Vec::new();
    for g in groups {
        let (schema, new_co) = build_multi_group_entry(
            g, &parent_ctx, schemas, name_to_idx, obj_map, arr_map,
        );
        new_schemas.push(schema);
        co_siblings.extend(new_co);
    }
    (new_schemas, co_siblings)
}


pub(super) fn apply_collapses(
    schemas: &mut Vec<TableSchema>,
    collapses: &[Collapse],
    name_to_idx: &std::collections::HashMap<String, usize>,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Vec<CoSiblingGroup> {
    let mut new_schemas: Vec<TableSchema> = Vec::new();
    let mut co_siblings: Vec<CoSiblingGroup> = Vec::new();
    for collapse in collapses {
        eprintln!("{}", collapse.log_msg);
        match &collapse.kind {
            CollapseKind::Single { .. } => {
                co_siblings.extend(apply_single_collapse(schemas, collapse, obj_map, arr_map));
            }
            CollapseKind::Multi { .. } => {
                let (pivot_schemas, pivot_co) = apply_multi_collapse(schemas, collapse, name_to_idx, obj_map, arr_map);
                new_schemas.extend(pivot_schemas);
                co_siblings.extend(pivot_co);
            }
        }
    }
    schemas.extend(new_schemas);
    co_siblings
}



#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::super::table_schema::{ColumnSchema, TableSchema};
    use super::super::super::super::type_tracker::PgType;

    fn make_parent(name: &str) -> TableSchema {
        TableSchema::new(name.to_string(), vec![name.to_string()], 0)
    }

    fn make_sibling(name: &str, parent: &str, data_keys: &[&str]) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 1);
        t.parent_table = Some(parent.to_string());
        t.columns.push(ColumnSchema {
            name: "j2s_id".to_string(), original_name: "j2s_id".to_string(),
            pg_type: PgType::BigInt, not_null: true, is_generated: true, is_parent_fk: false,
        });
        t.columns.push(ColumnSchema {
            name: "j2s_parent_id".to_string(), original_name: "j2s_parent_id".to_string(),
            pg_type: PgType::BigInt, not_null: true, is_generated: true, is_parent_fk: true,
        });
        for &k in data_keys {
            t.columns.push(ColumnSchema {
                name: k.to_string(), original_name: k.to_string(),
                pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
            });
        }
        t
    }

    fn make_child_with_key(name: &str, parent: &str, key: &str, data_keys: &[&str]) -> TableSchema {
        let mut t = make_sibling(name, parent, data_keys);
        t.path = vec![parent.to_string(), key.to_string()];
        t
    }



    #[test]
    fn build_multi_group_entry_propagates_sum_of_absorbed_row_counts() {
        let mut parent = make_parent("p");
        parent.row_count = 0;
        let mut child_a = make_child_with_key("p_a", "p", "a", &["v"]);
        child_a.row_count = 30_000;
        let mut child_b = make_child_with_key("p_b", "p", "b", &["v"]);
        child_b.row_count = 20_000;
        let schemas = vec![parent, child_a, child_b];

        let mut name_to_idx = std::collections::HashMap::new();
        for (i, s) in schemas.iter().enumerate() { name_to_idx.insert(s.name.clone(), i); }

        let mut obj_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        obj_map.insert("p".to_string(), vec![1, 2]);
        let arr_map = std::collections::HashMap::new();

        let g = SubgroupData {
            pivot_table_name: "p_pivot".to_string(),
            key_col_name: "key".to_string(),
            key_shape: crate::schema::table_schema::KeyShape::Slug,
            key_is_numeric: false,
            path_segment: "pivot".to_string(),
            union_cols: vec![],
            absorbed_names: vec!["p_a".to_string(), "p_b".to_string()],
            absorbed_path_segments: vec![],
        };
        let parent_ctx = ParentCtx {
            name: "p",
            path: &schemas[0].path.clone(),
            depth: 0,
            array_children: false,
        };
        let (schema, _) = build_multi_group_entry(
            &g, &parent_ctx, &schemas, &name_to_idx, &obj_map, &arr_map,
        );
        assert_eq!(schema.row_count, 50_000, "multi_group row_count must be sum of absorbed row_counts");
    }
}
