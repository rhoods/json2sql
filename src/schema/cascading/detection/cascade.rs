//! Cascade des co-siblings (waves 1+) — fusion des tables enfants partageant une clé JSON.
//!
//! Extrait de `detection.rs` (voir `super` pour l'orchestration et les types partagés).
//!
//! Fonctions :
//! - fn `collect_children_by_key` — regroupe les enfants par clé JSON partagée.
//! - fn `process_co_sibling_group` — dispatch fusion ou reparentage seul.
//! - fn `handle_single_co_sibling` — reparente un co-sibling isolé.
//! - fn `merge_co_sibling_group` — fusionne un groupe de co-siblings en une nouvelle table T.
//! - fn `build_co_sibling_schema` — construit le schéma de la nouvelle table T fusionnée.
//! - fn `cascade_grandchildren_to_next_wave` — propage les enfants de T à la vague suivante.

use super::super::super::wide_strategies::build_union_columns;
use super::super::super::table_schema::{ChildKind, ColumnSchema, InferredStrategy, TableSchema};
use super::super::scoring::pg_truncate_name;
use super::{apply, CoSiblingGroup};

/// Collect the children of `sibling_indices` grouped by their last JSON path segment.
/// Returns `(json_key, child_indices, array_children)` triples sorted by key.
/// `array_children` is `true` when at least one sibling exposes the key as an `ObjectArray`.
///
/// Example input:
/// ```json
/// { "sibling_a": { "info": { "x": "foo" } },
///   "sibling_b": { "info": [{ "x": "bar" }, { "x": "baz" }] } }
/// ```
/// Produces key "info" with `array_children = true`:
/// ```sql
/// CREATE TABLE parent_info (
///     j2s_id         UUID,
///     j2s_parent_id  UUID NOT NULL,
///     j2s_order      BIGINT,
///     x              TEXT
/// );
/// ```
pub(super) fn collect_children_by_key(
    schemas: &[TableSchema],
    sibling_indices: &[usize],
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Vec<(String, Vec<usize>, bool)> {
    // json_key → (child_indices, array_children)
    let mut key_map: std::collections::HashMap<String, (Vec<usize>, bool)> =
        std::collections::HashMap::new();
    for &i in sibling_indices {
        let name = schemas[i].name.as_str();
        for (child_map, is_arr) in [(&obj_map, false), (&arr_map, true)] {
            if let Some(children) = child_map.get(name) {
                for &ci in children {
                    if let Some(key) = schemas[ci].path.last() {
                        let entry = key_map.entry(key.clone()).or_insert_with(|| (Vec::new(), false));
                        entry.1 |= is_arr;
                        entry.0.push(ci);
                    }
                }
            }
        }
    }
    let mut out: Vec<(String, Vec<usize>, bool)> = key_map
        .into_iter()
        .map(|(k, (mut v, a))| {
            // Sort children by name so sibling[0] is always deterministic.
            v.sort_by_key(|&i| &schemas[i].name);
            (k, v, a)
        })
        .collect();
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}


/// Process one `CoSiblingGroup` from a cascade wave.
/// Returns new `CoSiblingGroup`s for the next wave (grandchildren level).
///
/// Co-siblings share the same `json_key` and therefore MUST be routed to a single table at
/// Pass 2. Merging is unconditional: a low Jaccard score produces a wider table with nullable
/// columns, which is correct. The previous Jaccard gate caused `child_routes.insert` to
/// overwrite for each sibling, leaving only the last sibling routable.
pub(super) fn process_co_sibling_group(
    schemas: &mut Vec<TableSchema>,
    group: &CoSiblingGroup,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
    name_to_idx: &std::collections::HashMap<String, usize>,
) -> Vec<CoSiblingGroup> {
    if group.sibling_indices.len() < 2 {
        handle_single_co_sibling(schemas, group, name_to_idx);
        return Vec::new();
    }

    let Some(&parent_idx) = name_to_idx.get(&group.synthetic_parent_name) else { return Vec::new() };
    merge_co_sibling_group(schemas, group, parent_idx, obj_map, arr_map)
}


/// Re-parent a sole co-sibling to its synthetic pivot and register it in `child_routes`.
fn handle_single_co_sibling(
    schemas: &mut [TableSchema],
    group: &CoSiblingGroup,
    name_to_idx: &std::collections::HashMap<String, usize>,
) {
    let Some(&idx) = group.sibling_indices.first() else { return };
    let child_name = schemas[idx].name.clone();
    schemas[idx].parent_table = Some(group.synthetic_parent_name.clone());
    if let Some(&pi) = name_to_idx.get(&group.synthetic_parent_name) {
        schemas[pi].child_routes.insert(group.json_key.clone(), child_name);
    }
}


pub(super) fn build_co_sibling_schema(
    group: &CoSiblingGroup,
    parent_depth: usize,
    parent_path: Vec<String>,
    parent_name: String,
    union_cols: Vec<ColumnSchema>,
    // Sum of sibling children's row_count — approximation for classify_tables.
    source_row_count: u64,
) -> (String, TableSchema) {
    let t_name = pg_truncate_name(&format!("{}_{}", group.synthetic_parent_name, group.json_key));
    let mut cols = apply::make_pivot_preamble(&parent_name, group.array_children);
    cols.extend(union_cols);
    let mut t_path = parent_path;
    t_path.push(group.json_key.clone());
    let mut t_schema = TableSchema::new(t_name.clone(), t_path, parent_depth + 1);
    t_schema.parent_table = Some(parent_name);
    t_schema.columns = cols;
    t_schema.child_kind = Some(if group.array_children { ChildKind::ObjectArray } else { ChildKind::Object });
    t_schema.inferred_strategy = InferredStrategy::Columns;
    t_schema.row_count = source_row_count;
    (t_name, t_schema)
}


fn cascade_grandchildren_to_next_wave(
    schemas: &mut [TableSchema],
    group: &CoSiblingGroup,
    t_name: &str,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Vec<CoSiblingGroup> {
    let grandchildren = collect_children_by_key(schemas, &group.sibling_indices, obj_map, arr_map);
    let mut next: Vec<CoSiblingGroup> = Vec::new();
    for (json_key, siblings, arr) in grandchildren {
        if siblings.len() >= 2 {
            next.push(CoSiblingGroup {
                synthetic_parent_name: t_name.to_string(),
                json_key,
                sibling_indices: siblings,
                array_children: arr,
            });
        } else if let Some(&sole_idx) = siblings.first() {
            let child_name = schemas[sole_idx].name.clone();
            schemas[sole_idx].parent_table = Some(t_name.to_string());
            // t_name was pushed to schemas immediately before this function was called.
            let t_pos = schemas.len() - 1;
            schemas[t_pos].child_routes.insert(json_key, child_name);
        }
    }
    next
}


/// Merge co-siblings into a new synthetic table T and cascade their grandchildren.
fn merge_co_sibling_group(
    schemas: &mut Vec<TableSchema>,
    group: &CoSiblingGroup,
    parent_idx: usize,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Vec<CoSiblingGroup> {
    let children_refs: Vec<&TableSchema> = group.sibling_indices.iter().map(|&i| &schemas[i]).collect();
    let union_cols = build_union_columns(&children_refs);
    let source_row_count: u64 = children_refs.iter().map(|s| s.row_count).sum();

    let parent_depth = schemas[parent_idx].depth;
    let parent_path = schemas[parent_idx].path.clone();
    let parent_name = schemas[parent_idx].name.clone();

    let (t_name, t_schema) = build_co_sibling_schema(group, parent_depth, parent_path, parent_name, union_cols, source_row_count);
    schemas[parent_idx].child_routes.insert(group.json_key.clone(), t_name.clone());
    schemas.push(t_schema);

    cascade_grandchildren_to_next_wave(schemas, group, &t_name, obj_map, arr_map)
}




#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;
    use super::super::super::super::table_schema::{ColumnSchema, InferredStrategy, TableSchema};
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



    // --- Finding 3: child_routes.insert écrasé pour co-siblings avec le même json_key ---

    #[test]
    #[allow(clippy::too_many_lines)] // single test case with verbose setup
    fn test_collect_children_by_key_object_and_array_conflict_sets_array_children() {
        // Input JSON scenario:
        //   { "sibling_a": { "info": { "x": 1 } },          -- Object child
        //     "sibling_b": { "info": [{ "x": 1 }, { "x": 2 }] } }  -- ObjectArray child
        //
        // Bug (or_insert_with always locks flag from first insertion, obj_map iterated first):
        //   CREATE TABLE parent_info (   -- child_kind = Object (WRONG)
        //     j2s_id UUID,
        //     j2s_parent_id UUID NOT NULL,
        //     x TEXT
        //     -- j2s_order absent, array elements lost
        //   );
        //
        // After fix (entry.1 |= is_arr):
        //   CREATE TABLE parent_info (   -- child_kind = ObjectArray (correct)
        //     j2s_id UUID,
        //     j2s_parent_id UUID NOT NULL,
        //     j2s_order BIGINT,          -- preserved
        //     x TEXT
        //   );
        let sibling_a = {
            let mut t = make_sibling("sibling_a", "parent", &["v"]);
            t.path = vec!["parent".to_string(), "sibling_a".to_string()];
            t
        };
        let sibling_b = {
            let mut t = make_sibling("sibling_b", "parent", &["v"]);
            t.path = vec!["parent".to_string(), "sibling_b".to_string()];
            t
        };
        let child_obj = {
            let mut t = make_sibling("parent_info", "sibling_a", &["x"]);
            t.path = vec!["parent".to_string(), "sibling_a".to_string(), "info".to_string()];
            t
        };
        let child_arr = {
            let mut t = make_sibling("sibling_b_info", "sibling_b", &["x"]);
            t.path = vec!["parent".to_string(), "sibling_b".to_string(), "info".to_string()];
            t
        };
        let schemas = vec![sibling_a, sibling_b, child_obj, child_arr];
        // sibling_a (idx 0) → obj child at idx 2; sibling_b (idx 1) → arr child at idx 3
        let mut obj_map: std::collections::HashMap<String, Vec<usize>> =
            std::collections::HashMap::new();
        obj_map.insert("sibling_a".to_string(), vec![2]);
        let mut arr_map: std::collections::HashMap<String, Vec<usize>> =
            std::collections::HashMap::new();
        arr_map.insert("sibling_b".to_string(), vec![3]);

        let result = collect_children_by_key(&schemas, &[0, 1], &obj_map, &arr_map);

        // Key "info" should appear once (merged from both siblings)
        let info = result.iter().find(|(k, _, _)| k == "info")
            .expect("key 'info' must appear in result");
        assert_eq!(info.0, "info");
        assert!(info.2, "array_children must be true when any co-sibling exposes key as ObjectArray");
        assert_eq!(info.1.len(), 2, "both child indices must be included");
    }

    #[test]
    #[allow(clippy::too_many_lines)] // single test case with verbose setup
    fn test_process_co_sibling_low_jaccard_still_merges_and_routes() {
        // Co-siblings share the same json_key → they MUST produce a single routable table
        // even when their schemas are disjoint (Jaccard=0). The routing table must have a
        // child_routes entry for "desc" pointing to an existing schema.
        use crate::schema::table_schema::{KeyShape, SiblingSchema};
        let synthetic = {
            let mut t = make_parent("pivot");
            t.inferred_strategy = InferredStrategy::SiblingCollapse(SiblingSchema {
                key_col_name: "key".to_string(),
                key_shape: KeyShape::Slug,
                array_children: false,
            });
            t
        };
        let mut schemas = vec![
            synthetic,
            make_child_with_key("pivot_desc_v1", "pivot", "desc", &["color"]),
            make_child_with_key("pivot_desc_v2", "pivot", "desc", &["length"]),
        ];
        let group = CoSiblingGroup {
            synthetic_parent_name: "pivot".to_string(),
            json_key: "desc".to_string(),
            sibling_indices: vec![1, 2],
            array_children: false,
        };
        // Co-siblings with disjoint schemas must still produce a single routable table.
        let (obj_map, arr_map) = super::super::build_parent_child_maps(&schemas);
        let name_to_idx: std::collections::HashMap<String, usize> =
            schemas.iter().enumerate().map(|(i, s)| (s.name.clone(), i)).collect();
        process_co_sibling_group(&mut schemas, &group, &obj_map, &arr_map, &name_to_idx);
        let pivot_idx = schemas.iter().position(|s| s.name == "pivot").unwrap();
        assert!(
            schemas[pivot_idx].child_routes.contains_key("desc"),
            "pivot must have a child_routes entry for 'desc' after merge"
        );
        let target_name = schemas[pivot_idx].child_routes["desc"].clone();
        assert!(
            schemas.iter().any(|s| s.name == target_name),
            "target table '{target_name}' must exist in schemas"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)] // single test case with verbose setup
    fn test_two_co_sibling_groups_same_wave_both_merged() {
        // Regression test for #8/#9: two CoSiblingGroups processed in the same BFS wave
        // must both produce routable child_routes entries on their respective parents,
        // even when maps and name_to_idx are built once for the whole wave.
        //
        // Setup: two independent synthetic pivots each with 2 co-siblings sharing a key.
        //   pivot_a has co-siblings [a_desc_v1, a_desc_v2] for key "desc"
        //   pivot_b has co-siblings [b_info_v1, b_info_v2] for key "info"
        use crate::schema::table_schema::{KeyShape, SiblingSchema};
        let make_pivot = |name: &str| {
            let mut t = make_parent(name);
            t.inferred_strategy = InferredStrategy::SiblingCollapse(SiblingSchema {
                key_col_name: "key".to_string(),
                key_shape: KeyShape::Slug,
                array_children: false,
            });
            t
        };
        let mut schemas = vec![
            make_pivot("pivot_a"),                                          // 0
            make_child_with_key("a_desc_v1", "pivot_a", "desc", &["x"]),    // 1
            make_child_with_key("a_desc_v2", "pivot_a", "desc", &["y"]),    // 2
            make_pivot("pivot_b"),                                          // 3
            make_child_with_key("b_info_v1", "pivot_b", "info", &["p"]),    // 4
            make_child_with_key("b_info_v2", "pivot_b", "info", &["q"]),    // 5
        ];
        let groups = vec![
            CoSiblingGroup { synthetic_parent_name: "pivot_a".to_string(), json_key: "desc".to_string(), sibling_indices: vec![1, 2], array_children: false },
            CoSiblingGroup { synthetic_parent_name: "pivot_b".to_string(), json_key: "info".to_string(), sibling_indices: vec![4, 5], array_children: false },
        ];
        // Build maps once for the wave (simulating the perf fix)
        let (obj_map, arr_map) = super::super::build_parent_child_maps(&schemas);
        let name_to_idx: std::collections::HashMap<String, usize> =
            schemas.iter().enumerate().map(|(i, s)| (s.name.clone(), i)).collect();
        for group in &groups {
            process_co_sibling_group(&mut schemas, group, &obj_map, &arr_map, &name_to_idx);
        }
        let pa = schemas.iter().find(|s| s.name == "pivot_a").unwrap();
        assert!(pa.child_routes.contains_key("desc"), "pivot_a must have child_routes['desc']");
        let pb = schemas.iter().find(|s| s.name == "pivot_b").unwrap();
        assert!(pb.child_routes.contains_key("info"), "pivot_b must have child_routes['info']");
        let ta_name = &pa.child_routes["desc"];
        let tb_name = &pb.child_routes["info"];
        assert!(schemas.iter().any(|s| &s.name == ta_name), "T table for pivot_a/desc must exist");
        assert!(schemas.iter().any(|s| &s.name == tb_name), "T table for pivot_b/info must exist");
    }

    #[test]
    fn build_co_sibling_schema_propagates_row_count() {
        let group = CoSiblingGroup {
            synthetic_parent_name: "pivot".to_string(),
            json_key: "en".to_string(),
            sibling_indices: vec![],
            array_children: false,
        };
        let (_name, schema) = build_co_sibling_schema(
            &group, 1,
            vec!["parent".to_string(), "en".to_string()],
            "parent".to_string(),
            vec![],
            75_000,
        );
        assert_eq!(schema.row_count, 75_000, "co_sibling row_count must equal source_row_count");
    }
}
