//! BFS cascade — détection des groupes de siblings et collapse en tables canoniques.
//!
//! Ce module contient : les types internes du cascade (Collapse, `SiblingDetectCtx`…),
//! `finalize_cascading` (point d'entrée crate), et toutes les fonctions auxiliaires du BFS.
//!
//! Décomposé en sous-modules par phase de traitement (issue #41) : ce fichier garde les types
//! partagés et l'orchestration ; [`wave0`] contient la détection wave 0.
//!
//! Fonctions :
//! - struct `SubgroupData` — un sous-groupe pivot synthétique (nom, clé, colonnes union, absorbés).
//! - enum `CollapseKind` — forme du collapse détecté (Single une table, Multi plusieurs sous-groupes).
//! - struct `Collapse` — collapse à appliquer sur un parent (indices absorbés, kind, log).
//! - struct `SiblingDetectCtx` — contexte de détection pour un parent (enfants, seuils, index numériques).
//! - struct `CoSiblingGroup` — groupe de tables co-sibling produit par une vague de cascade.
//! - struct `ParentCtx` — nom/chemin/profondeur/`array_children` du parent en cours de traitement.
//!
//! Orchestration :
//! - fn `finalize_cascading` — enchaîne wave 0, cascade waves 1+, wave 0-bis (tables
//!   T créées par le cascade), et le post-pass keyed-pivot.
//! - fn `run_sibling_wave` — une passe complète (maps → work items → détection → application).
//! - fn `build_parent_child_maps` — index `parent_name → [enfants]` (Object/ObjectArray séparés).
//!
//! Application des collapses :
//! - fn `apply_collapses` — dispatch Single/Multi et journalise.
//! - fn `apply_single_collapse` — transforme le parent en table pivot unique.
//! - fn `apply_multi_collapse` — crée les tables pivot synthétiques (une par sous-groupe).
//! - fn `build_multi_group_entry` — construit l'entrée de schéma d'un sous-groupe pivot.
//! - fn `make_pivot_preamble` — colonnes générées communes (`j2s_id`, FK, order).
//!
//! Cascade des co-siblings (waves 1+) :
//! - fn `collect_children_by_key` — regroupe les enfants par clé JSON partagée.
//! - fn `process_co_sibling_group` — dispatch fusion ou reparentage seul.
//! - fn `handle_single_co_sibling` — reparente un co-sibling isolé.
//! - fn `merge_co_sibling_group` — fusionne un groupe de co-siblings en une nouvelle table T.
//! - fn `build_co_sibling_schema` — construit le schéma de la nouvelle table T fusionnée.
//! - fn `cascade_grandchildren_to_next_wave` — propage les enfants de T à la vague suivante.
//!
//! Post-pass keyed-pivot (orphelins `Columns` sous un parent `SiblingCollapse`) :
//! - fn `run_keyed_pivot_children_wave` — détecte et traite ces orphelins.
//! - fn `collect_keyed_pivot_work_items` — collecte les items de travail keyed-pivot.
//! - fn `process_keyed_pivot_work_item` — traite un item de travail keyed-pivot.
//! - fn `resolve_pivot_key_info` — résout la clé/forme du sous-pivot.
//! - fn `build_sub_pivot_columns` — construit les colonnes union du sous-pivot.
//! - fn `build_sub_pivot_schema` — construit le schéma du sous-pivot.
//! - fn `collect_pivot_co_siblings` — reparente les enfants absorbés.
//! - fn `reparent_and_update_routes` — met à jour `child_routes` du parent.

use super::super::wide_strategies::{build_union_columns, classify_key_shape};
use super::super::table_schema::{ChildKind, ColumnSchema, KeyShape, SiblingSchema, TableSchema, InferredStrategy};
use super::super::type_tracker::PgType;
use super::scoring::{pairwise_jaccard_min, pg_truncate_name, unique_cluster_suffix};

mod wave0;


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
    // Maps and name_to_idx are rebuilt once per wave (not per group — #8/#9).
    let mut pending: Vec<CoSiblingGroup> = co_siblings_0;
    while !pending.is_empty() {
        let (obj_map, arr_map) = build_parent_child_maps(schemas);
        let name_to_idx: std::collections::HashMap<String, usize> =
            schemas.iter().enumerate().map(|(i, s)| (s.name.clone(), i)).collect();
        let mut next_pending: Vec<CoSiblingGroup> = Vec::new();
        for group in pending {
            let produced = process_co_sibling_group(schemas, &group, &obj_map, &arr_map, &name_to_idx);
            next_pending.extend(produced);
        }
        pending = next_pending;
    }

    // ── Wave 0 bis: sibling detection on T tables created by the BFS cascade ──
    // Tables produced by process_co_sibling_group (e.g. cluster_0_sizes_100/200/400/full)
    // share a Columns parent but did not exist during wave 0. This pass fuses them.
    // Parents already converted to SiblingCollapse/SiblingCollapseMulti are skipped automatically.
    let co_siblings_bis = run_sibling_wave(schemas, threshold, min_jaccard);
    let mut pending_bis: Vec<CoSiblingGroup> = co_siblings_bis;
    while !pending_bis.is_empty() {
        let (obj_map, arr_map) = build_parent_child_maps(schemas);
        let name_to_idx: std::collections::HashMap<String, usize> =
            schemas.iter().enumerate().map(|(i, s)| (s.name.clone(), i)).collect();
        let mut next_pending: Vec<CoSiblingGroup> = Vec::new();
        for group in pending_bis {
            let produced = process_co_sibling_group(schemas, &group, &obj_map, &arr_map, &name_to_idx);
            next_pending.extend(produced);
        }
        pending_bis = next_pending;
    }

    // ── Post-pass: merge Columns orphans under SiblingCollapse parents ───────────
    // After the BFS cascade, some Columns tables survive as children of a SiblingCollapse
    // parent (e.g. lang-code T tables produced by cascade wave 1 that themselves
    // are numerous and similar).  A second sibling-detection pass fuses them into
    // a synthetic sub-pivot and cascades their own children.
    let post_co_siblings = run_keyed_pivot_children_wave(schemas, threshold, min_jaccard);
    let mut pending: Vec<CoSiblingGroup> = post_co_siblings;
    while !pending.is_empty() {
        let (obj_map, arr_map) = build_parent_child_maps(schemas);
        let name_to_idx: std::collections::HashMap<String, usize> =
            schemas.iter().enumerate().map(|(i, s)| (s.name.clone(), i)).collect();
        let mut next_pending: Vec<CoSiblingGroup> = Vec::new();
        for group in pending {
            let produced = process_co_sibling_group(schemas, &group, &obj_map, &arr_map, &name_to_idx);
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

/// Collect the children of `sibling_indices` grouped by their last JSON path segment.
/// Returns `(json_key, child_indices, array_children)` triples sorted by key.
/// `array_children` is `true` when at least one sibling exposes the key as an ObjectArray.
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
fn collect_children_by_key(
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
    collect_children_by_key(schemas, &collapse.absorbed_indices, obj_map, arr_map)
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

fn make_pivot_preamble(parent_name: &str, array_children: bool) -> Vec<ColumnSchema> {
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
    for (json_key, siblings, arr) in collect_children_by_key(schemas, &absorbed_idx, obj_map, arr_map) {
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

fn apply_multi_collapse(
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

fn apply_collapses(
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
    apply_collapses(schemas, &collapses, &name_to_idx, &parent_to_object_children, &parent_to_array_children)
}

/// Process one `CoSiblingGroup` from a cascade wave.
/// Returns new `CoSiblingGroup`s for the next wave (grandchildren level).
///
/// Co-siblings share the same `json_key` and therefore MUST be routed to a single table at
/// Pass 2. Merging is unconditional: a low Jaccard score produces a wider table with nullable
/// columns, which is correct. The previous Jaccard gate caused `child_routes.insert` to
/// overwrite for each sibling, leaving only the last sibling routable.
fn process_co_sibling_group(
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

fn build_co_sibling_schema(
    group: &CoSiblingGroup,
    parent_depth: usize,
    parent_path: Vec<String>,
    parent_name: String,
    union_cols: Vec<ColumnSchema>,
    // Sum of sibling children's row_count — approximation for classify_tables.
    source_row_count: u64,
) -> (String, TableSchema) {
    let t_name = pg_truncate_name(&format!("{}_{}", group.synthetic_parent_name, group.json_key));
    let mut cols = make_pivot_preamble(&parent_name, group.array_children);
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
fn run_keyed_pivot_children_wave(
    schemas: &mut Vec<TableSchema>,
    threshold: usize,
    min_jaccard: f64,
) -> Vec<CoSiblingGroup> {
    let (obj_map, arr_map) = build_parent_child_maps(schemas);
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
    let children_by_key = collect_children_by_key(schemas, child_indices, obj_map, arr_map);
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
    use super::super::super::table_schema::{ChildKind, ColumnSchema, KeyShape, SiblingSchema, TableSchema, InferredStrategy};
    use super::super::super::type_tracker::PgType;
    use super::super::scoring;

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
        let (pivot_schemas, _) = apply_multi_collapse(&mut schemas, &collapse, &name_to_idx, &empty, &empty);
        assert_eq!(pivot_schemas.len(), 2, "expected 2 pivot schemas");
        let last_segs: Vec<Option<&String>> = pivot_schemas.iter().map(|s| s.path.last()).collect();
        assert_ne!(
            last_segs[0], last_segs[1],
            "pivot schemas must have distinct path last-segments; got {:?}",
            last_segs,
        );
    }

    // --- Finding 3: child_routes.insert écrasé pour co-siblings avec le même json_key ---

    #[test]
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
        let (obj_map, arr_map) = build_parent_child_maps(&schemas);
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
            "target table '{}' must exist in schemas", target_name
        );
    }

    #[test]
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
        let (obj_map, arr_map) = build_parent_child_maps(&schemas);
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

    #[test]
    fn build_co_sibling_schema_propagates_row_count() {
        use crate::schema::table_schema::ChildKind;
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
