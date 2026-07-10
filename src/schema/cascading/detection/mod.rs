//! BFS cascade — détection des groupes de siblings et collapse en tables canoniques.
//!
//! Décomposé en sous-modules par phase de traitement (issue #41) : ce fichier garde les types
//! partagés et l'orchestration ; [`wave0`] contient la détection wave 0, [`apply`] l'application
//! des collapses, [`cascade`] la cascade des co-siblings (waves 1+), [`keyed_pivot`] le post-pass
//! keyed-pivot.
//!
//! Fonctions :
//! - struct `SubgroupData` — un sous-groupe pivot synthétique (nom, clé, colonnes union, absorbés).
//! - enum `CollapseKind` — forme du collapse détecté (Single une table, Multi plusieurs sous-groupes).
//! - struct `Collapse` — collapse à appliquer sur un parent (indices absorbés, kind, log).
//! - struct `SiblingDetectCtx` — contexte de détection pour un parent (enfants, seuils, index numériques).
//! - struct `CoSiblingGroup` — groupe de tables co-sibling produit par une vague de cascade.
//!
//! Orchestration :
//! - fn `finalize_cascading` — enchaîne wave 0, cascade waves 1+, wave 0-bis (tables
//!   T créées par le cascade), et le post-pass keyed-pivot.
//! - fn `run_sibling_wave` — une passe complète (maps → work items → détection → application).
//! - fn `build_parent_child_maps` — index `parent_name → [enfants]` (Object/ObjectArray séparés).

use super::super::table_schema::{ChildKind, ColumnSchema, KeyShape, TableSchema};

mod wave0;
mod apply;
mod cascade;
mod keyed_pivot;

struct SubgroupData {
    pivot_table_name: String,
    key_is_numeric: bool,
    key_col_name: String,
    key_shape: KeyShape,
    union_cols: Vec<ColumnSchema>,
    absorbed_names: Vec<String>,
    path_segment: String,
    absorbed_path_segments: Vec<String>,
}


enum CollapseKind {
    Single {
        key_col_name: String,
        key_shape: KeyShape,
        union_cols: Vec<ColumnSchema>,
    },
    Multi { groups: Vec<SubgroupData> },
}


struct Collapse {
    parent_idx: usize,
    array_children: bool,
    log_msg: String,
    kind: CollapseKind,
    absorbed_indices: Vec<usize>,
}


struct SiblingDetectCtx {
    parent_name: String,
    parent_idx: usize,
    child_indices: Vec<usize>,
    array_children: bool,
    threshold: usize,
    min_jaccard: f64,
    parent_has_data: bool,
    numeric_idx: Vec<usize>,
    non_numeric_idx: Vec<usize>,
}

// ---------------------------------------------------------------------------
// Cascading sibling detection
// ---------------------------------------------------------------------------


/// BFS top-down sibling fusion with child-compatibility gate and cascaded merging.
///
/// Wave 0 — sibling detection with two additions over the original algorithm:
///   1. **Child-compatibility gate**: a sibling group is only merged when the pairwise Jaccard
///      of their *shared* child tables (same JSON key across siblings) is ≥ `min_jaccard`.
///      This prevents merging structurally similar parents whose children are incompatible.
///   2. **Co-sibling collection**: children of merged siblings that share the same JSON key
///      are collected as candidates for the next cascade wave.
///
/// Waves 1+ — co-sibling groups from the previous wave are analysed in turn:
///   - Similar co-siblings (Jaccard ≥ `min_jaccard`) → merged into a new synthetic pivot T,
///     registered in the parent pivot's `child_routes`.
///   - Dissimilar or sole-occurrence children → re-parented to the synthetic pivot S so they
///     survive `exclude_absorbed_children` and are routed by Pass 2 via `child_routes`.
pub fn finalize_cascading(schemas: &mut Vec<TableSchema>, threshold: usize, min_jaccard: f64) {
    // ── Wave 0: standard sibling detection + child-compat gate ──────────────
    let co_siblings_0 = run_sibling_wave(schemas, threshold, min_jaccard);

    // ── Waves 1+: cascade ────────────────────────────────────────────────────
    drain_cascade_waves(schemas, co_siblings_0);

    // ── Wave 0 bis: sibling detection on T tables created by the BFS cascade ──
    // Tables produced by process_co_sibling_group (e.g. cluster_0_sizes_100/200/400/full)
    // share a Columns parent but did not exist during wave 0. This pass fuses them.
    // Parents already converted to SiblingCollapse/SiblingCollapseMulti are skipped automatically.
    let co_siblings_bis = run_sibling_wave(schemas, threshold, min_jaccard);
    drain_cascade_waves(schemas, co_siblings_bis);

    // ── Post-pass: merge Columns orphans under SiblingCollapse parents ───────────
    // After the BFS cascade, some Columns tables survive as children of a SiblingCollapse
    // parent (e.g. lang-code T tables produced by cascade wave 1 that themselves
    // are numerous and similar).  A second sibling-detection pass fuses them into
    // a synthetic sub-pivot and cascades their own children.
    let post_co_siblings = keyed_pivot::run_keyed_pivot_children_wave(schemas, threshold, min_jaccard);
    drain_cascade_waves(schemas, post_co_siblings);
}

/// Repeatedly processes co-sibling groups until no further groups are produced.
/// Maps and `name_to_idx` are rebuilt once per wave (not per group — #8/#9).
fn drain_cascade_waves(schemas: &mut Vec<TableSchema>, initial: Vec<CoSiblingGroup>) {
    let mut pending = initial;
    while !pending.is_empty() {
        let (obj_map, arr_map) = build_parent_child_maps(schemas);
        let name_to_idx: std::collections::HashMap<String, usize> =
            schemas.iter().enumerate().map(|(i, s)| (s.name.clone(), i)).collect();
        let mut next_pending: Vec<CoSiblingGroup> = Vec::new();
        for group in pending {
            let produced = cascade::process_co_sibling_group(schemas, &group, &obj_map, &arr_map, &name_to_idx);
            next_pending.extend(produced);
        }
        pending = next_pending;
    }
}


/// A group of co-sibling tables produced by a cascade wave.
/// These are children of tables that were merged in the previous wave,
/// grouped by their JSON key so they can be evaluated for further merging.
struct CoSiblingGroup {
    /// Name of the synthetic pivot table that is now their logical parent.
    synthetic_parent_name: String,
    /// The JSON key that these tables share relative to their original JSON path.
    json_key: String,
    /// Indices into `schemas` of the co-sibling tables.
    sibling_indices: Vec<usize>,
    /// True when the co-siblings are `ObjectArray` children.
    array_children: bool,
}


/// Build `parent_name` → [`child_index`] maps for Object and `ObjectArray` children.
fn build_parent_child_maps(
    schemas: &[TableSchema],
) -> (
    std::collections::HashMap<String, Vec<usize>>,
    std::collections::HashMap<String, Vec<usize>>,
) {
    let mut obj_map: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    let mut arr_map: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    for (i, schema) in schemas.iter().enumerate() {
        if let Some(ref parent) = schema.parent_table {
            match schema.child_kind {
                Some(ChildKind::Object) => {
                    obj_map.entry(parent.clone()).or_default().push(i);
                }
                Some(ChildKind::ObjectArray | ChildKind::ScalarArray) => {
                    arr_map.entry(parent.clone()).or_default().push(i);
                }
                _ => {}
            }
        }
    }
    (obj_map, arr_map)
}


fn run_sibling_wave(
    schemas: &mut Vec<TableSchema>,
    threshold: usize,
    min_jaccard: f64,
) -> Vec<CoSiblingGroup> {
    let (parent_to_object_children, parent_to_array_children) = build_parent_child_maps(schemas);
    let name_to_idx: std::collections::HashMap<String, usize> = schemas
        .iter().enumerate().map(|(i, s)| (s.name.clone(), i)).collect();
    let work = wave0::build_work_items(schemas, threshold, &parent_to_object_children, &parent_to_array_children);
    let collapses = wave0::collect_sibling_collapses(
        schemas, &work, threshold, min_jaccard,
        &name_to_idx, &parent_to_object_children, &parent_to_array_children,
    );
    apply::apply_collapses(schemas, &collapses, &name_to_idx, &parent_to_object_children, &parent_to_array_children)
}



#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::table_schema::{ColumnSchema, TableSchema};
    use super::super::super::type_tracker::PgType;

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
    fn test_apply_multi_collapse_pivot_schemas_have_distinct_paths() {
        // After apply_multi_collapse, each group's TableSchema.path.last() must be distinct
        // so that path_map (keyed on path.join(SEP)) stores both pivots without collision.
        let parent = make_parent("p");
        let mut schemas = vec![
            parent,
            make_child_with_key("p_a1", "p", "a1", &["col"]),
            make_child_with_key("p_a2", "p", "a2", &["col"]),
            make_child_with_key("p_a3", "p", "a3", &["col"]),
            make_child_with_key("p_b1", "p", "b1", &["size"]),
            make_child_with_key("p_b2", "p", "b2", &["size"]),
            make_child_with_key("p_b3", "p", "b3", &["size"]),
        ];
        let g0 = wave0::make_subgroup(&schemas, "p", &[1, 2, 3], false, "cluster_0");
        let g1 = wave0::make_subgroup(&schemas, "p", &[4, 5, 6], false, "cluster_1");
        let collapse = Collapse {
            parent_idx: 0,
            array_children: false,
            log_msg: String::new(),
            kind: CollapseKind::Multi { groups: vec![g0, g1] },
            absorbed_indices: vec![1, 2, 3, 4, 5, 6],
        };
        let name_to_idx: std::collections::HashMap<String, usize> =
            schemas.iter().enumerate().map(|(i, s)| (s.name.clone(), i)).collect();
        let empty = std::collections::HashMap::new();
        let (pivot_schemas, _) = apply::apply_multi_collapse(&mut schemas, &collapse, &name_to_idx, &empty, &empty);
        assert_eq!(pivot_schemas.len(), 2, "expected 2 pivot schemas");
        let last_segs: Vec<Option<&String>> = pivot_schemas.iter().map(|s| s.path.last()).collect();
        assert_ne!(
            last_segs[0], last_segs[1],
            "pivot schemas must have distinct path last-segments; got {last_segs:?}",
        );
    }
}
