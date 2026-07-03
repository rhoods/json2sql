//! BFS cascade — détection des groupes de siblings et collapse en tables canoniques.
//!
//! Ce module contient : les types internes du cascade (Collapse, `SiblingDetectCtx`…),
//! `finalize_cascading` (point d'entrée crate), et toutes les fonctions auxiliaires du BFS.

use super::super::wide_strategies::{build_union_columns, classify_key_shape};
use super::super::table_schema::{ChildKind, ColumnSchema, KeyShape, SiblingSchema, TableSchema, InferredStrategy};
use super::super::type_tracker::PgType;
use super::scoring::{child_compatibility_score, greedy_schema_clusters, pairwise_jaccard_min, pg_truncate_name, siblings_key_prefix, unique_cluster_suffix};


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

/// Run wave 0 of sibling detection — with
/// the child-compatibility gate added. Returns co-sibling groups for cascade waves 1+.
/// Build the sorted work list for `run_sibling_wave`.
/// Each item is `(parent_name, child_indices_sorted_by_name, array_children)`.
/// Child indices are sorted alphabetically so the Jaccard large-group fast path
/// always uses the same reference sibling (deterministic).
fn build_work_items(
    schemas: &[TableSchema],
    threshold: usize,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Vec<(String, Vec<usize>, bool)> {
    let mut work: Vec<(String, Vec<usize>, bool)> = Vec::new();
    for (parent_map, array_children) in [(obj_map, false), (arr_map, true)] {
        for (parent_name, child_indices) in parent_map {
            if child_indices.len() >= threshold {
                let mut sorted = child_indices.clone();
                sorted.sort_by_key(|&i| &schemas[i].name);
                work.push((parent_name.clone(), sorted, array_children));
            }
        }
    }
    work.sort_by(|a, b| a.0.cmp(&b.0));
    work
}

fn try_unified_fallback(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Option<Collapse> {
    if ctx.parent_has_data || ctx.child_indices.len() < ctx.threshold {
        return None;
    }
    let unified_jaccard = {
        let data_bearing: Vec<usize> = ctx.child_indices.iter().copied()
            .filter(|&i| schemas[i].data_columns().next().is_some()).collect();
        if data_bearing.len() >= ctx.threshold { pairwise_jaccard_min(schemas, &data_bearing) }
        else { pairwise_jaccard_min(schemas, &ctx.child_indices) }
    };
    if unified_jaccard < ctx.min_jaccard { return None; }
    if child_compatibility_score(schemas, &ctx.child_indices, obj_map, arr_map) < ctx.min_jaccard { return None; }
    let keys: Vec<String> = ctx.child_indices.iter().map(|&i| schemas[i].path.last().cloned().unwrap_or_default()).collect();
    let key_shape = classify_key_shape(&keys.iter().map(std::string::String::as_str).collect::<Vec<_>>());
    let union_cols = build_union_columns(&ctx.child_indices.iter().map(|&i| &schemas[i]).collect::<Vec<_>>());
    let kind_label = if ctx.array_children { "ObjectArray" } else { "Object" };
    Some(Collapse {
        parent_idx: ctx.parent_idx,
        array_children: ctx.array_children,
        log_msg: format!("  Unified-fallback SiblingCollapse {}: {} ({} tables → 1, Jaccard {:.2})", kind_label, ctx.parent_name, ctx.child_indices.len(), unified_jaccard),
        kind: CollapseKind::Single { key_col_name: "key".to_string(), key_shape, union_cols },
        absorbed_indices: ctx.child_indices.clone(),
    })
}

/// Returns a suffix not colliding with existing `{parent}_{suffix}` schemas nor with
/// suffixes already chosen in the current call (tracked via `used`).
fn pick_unique_suffix(
    parent: &str,
    desired: &str,
    schemas: &[TableSchema],
    used: &std::collections::HashSet<String>,
) -> String {
    let taken = |s: &str| {
        used.contains(s) || schemas.iter().any(|t| t.name == format!("{parent}_{s}"))
    };
    if !taken(desired) {
        return desired.to_string();
    }
    let mut n = 2usize;
    loop {
        let candidate = format!("{desired}_{n}");
        if !taken(&candidate) { return candidate; }
        n += 1;
    }
}

/// Split `regular` into schema-compatible clusters when global Jaccard is too low.
/// Returns `None` if fewer than 2 valid clusters are found.
///
/// Example input:
/// ```json
/// { "img_back":  {"col_a": "…"}, "img_front": {"col_a": "…"},
///   "img_side":  {"col_b": "…"}, "img_top":   {"col_b": "…"} }
/// ```
/// Two incompatible clusters despite the same "img_" prefix → two distinct pivot tables:
/// ```sql
/// CREATE TABLE p_img_key   (j2s_id uuid, j2s_p_id uuid, key text, col_a text);
/// CREATE TABLE p_img_key_2 (j2s_id uuid, j2s_p_id uuid, key text, col_b text);
/// ```
fn try_cluster_fallback(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    regular: &[usize],
) -> Option<Collapse> {
    let has_sig = regular.len() < ctx.child_indices.len();
    let is_autosplit = matches!(schemas[ctx.parent_idx].inferred_strategy, InferredStrategy::AutoSplit { .. });
    if (ctx.parent_has_data || has_sig) && !is_autosplit {
        return None;
    }
    let valid_clusters: Vec<Vec<usize>> = greedy_schema_clusters(schemas, regular, ctx.min_jaccard, ctx.threshold)
        .into_iter()
        .filter(|c| pairwise_jaccard_min(schemas, c) >= ctx.min_jaccard)
        .collect();
    if valid_clusters.len() < 2 { return None; }
    let mut groups: Vec<SubgroupData> = Vec::new();
    let mut all_absorbed = Vec::new();
    let mut used_suffixes: std::collections::HashSet<String> = std::collections::HashSet::new();
    for cluster in &valid_clusters {
        let prefix = siblings_key_prefix(schemas, cluster);
        let desired = if prefix.is_empty() { format!("cluster_{}", groups.len()) } else { format!("{prefix}_key") };
        let suffix = pick_unique_suffix(&ctx.parent_name, &desired, schemas, &used_suffixes);
        used_suffixes.insert(suffix.clone());
        all_absorbed.extend_from_slice(cluster);
        groups.push(make_subgroup(schemas, &ctx.parent_name, cluster, false, &suffix));
    }
    Some(Collapse {
        parent_idx: ctx.parent_idx,
        array_children: ctx.array_children,
        log_msg: format!("  Schema-cluster SiblingCollapseMulti: {} ({} tables → {} clusters)", ctx.parent_name, regular.len(), groups.len()),
        kind: CollapseKind::Multi { groups },
        absorbed_indices: all_absorbed,
    })
}

fn filter_routing_tables(
    schemas: &[TableSchema],
    non_numeric_idx: &[usize],
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
    threshold: usize,
) -> Vec<usize> {
    non_numeric_idx.iter().copied().filter(|&i| {
        let name = &schemas[i].name;
        let is_pure = schemas[i].data_columns().next().is_none();
        let child_count = obj_map.get(name).map_or(0, std::vec::Vec::len)
            + arr_map.get(name).map_or(0, std::vec::Vec::len);
        !(is_pure && child_count >= threshold)
    }).collect()
}

fn build_non_num_clusters(
    schemas: &[TableSchema],
    non_num_regular: &[usize],
    ctx: &SiblingDetectCtx,
) -> Vec<Vec<usize>> {
    greedy_schema_clusters(schemas, non_num_regular, ctx.min_jaccard, ctx.threshold)
        .into_iter()
        .filter(|c| pairwise_jaccard_min(schemas, c) >= ctx.min_jaccard)
        .collect()
}

fn detect_mixed_collapse(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Option<Collapse> {
    let non_num_regular = filter_routing_tables(schemas, &ctx.non_numeric_idx, obj_map, arr_map, ctx.threshold);
    let num_ok = ctx.numeric_idx.len() >= ctx.threshold
        && pairwise_jaccard_min(schemas, &ctx.numeric_idx) >= ctx.min_jaccard;
    let non_ok = non_num_regular.len() >= ctx.threshold
        && pairwise_jaccard_min(schemas, &non_num_regular) >= ctx.min_jaccard;
    if !num_ok && !non_ok {
        return try_unified_fallback(schemas, ctx, obj_map, arr_map);
    }
    let num_ok = num_ok
        && child_compatibility_score(schemas, &ctx.numeric_idx, obj_map, arr_map) >= ctx.min_jaccard;
    let non_ok = non_ok
        && child_compatibility_score(schemas, &non_num_regular, obj_map, arr_map) >= ctx.min_jaccard;
    let non_num_clusters = if !non_ok && !non_num_regular.is_empty() {
        build_non_num_clusters(schemas, &non_num_regular, ctx)
    } else {
        Vec::new()
    };
    if !num_ok && !non_ok && non_num_clusters.is_empty() { return None; }
    Some(assemble_mixed_collapse(schemas, ctx, num_ok, non_ok, &non_num_regular, &non_num_clusters))
}

/// Assemble a `SiblingCollapseMulti` collapse from pre-classified numeric/non-numeric groups.
/// Each group gets a unique suffix via `pick_unique_suffix` to avoid name collisions.
///
/// **Scenario 1 — numeric + two incompatible non-numeric clusters:**
/// ```json
/// { "1":      {"val":   "a"}, "2":      {"val":   "b"},
///   "tag_fr": {"label": "x"}, "tag_en": {"label": "y"},
///   "tag_de": {"size":  "m"}, "tag_it": {"size":  "n"} }
/// ```
/// ```sql
/// CREATE TABLE p_num       (j2s_id uuid, j2s_p_id uuid, key_id text, val   text);
/// CREATE TABLE p_tag_key   (j2s_id uuid, j2s_p_id uuid, key   text, label text);
/// CREATE TABLE p_tag_key_2 (j2s_id uuid, j2s_p_id uuid, key   text, size  text);
/// ```
///
/// **Scenario 2 — `p_num` already exists in schemas (collision avoidance):**
/// ```json
/// { "1": {"val": "a"}, "2": {"val": "b"} }
/// ```
/// ```sql
/// -- p_num already exists, new pivot gets a distinct name:
/// CREATE TABLE p_num_2 (j2s_id uuid, j2s_p_id uuid, key_id text, val text);
/// ```
fn assemble_mixed_collapse(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    num_ok: bool,
    non_ok: bool,
    non_num_regular: &[usize],
    non_num_clusters: &[Vec<usize>],
) -> Collapse {
    let mut groups: Vec<SubgroupData> = Vec::new();
    let mut all_absorbed: Vec<usize> = Vec::new();
    let mut used_suffixes: std::collections::HashSet<String> = std::collections::HashSet::new();
    if num_ok {
        let suffix = pick_unique_suffix(&ctx.parent_name, "num", schemas, &used_suffixes);
        used_suffixes.insert(suffix.clone());
        all_absorbed.extend_from_slice(&ctx.numeric_idx);
        groups.push(make_subgroup(schemas, &ctx.parent_name, &ctx.numeric_idx, true, &suffix));
    }
    if non_ok {
        let suffix = pick_unique_suffix(&ctx.parent_name, "key", schemas, &used_suffixes);
        used_suffixes.insert(suffix.clone());
        all_absorbed.extend_from_slice(non_num_regular);
        groups.push(make_subgroup(schemas, &ctx.parent_name, non_num_regular, false, &suffix));
    }
    for (i, cluster) in non_num_clusters.iter().enumerate() {
        let prefix = siblings_key_prefix(schemas, cluster);
        let desired = if prefix.is_empty() { format!("cluster_{i}") } else { format!("{prefix}_key") };
        let suffix = pick_unique_suffix(&ctx.parent_name, &desired, schemas, &used_suffixes);
        used_suffixes.insert(suffix.clone());
        all_absorbed.extend_from_slice(cluster);
        groups.push(make_subgroup(schemas, &ctx.parent_name, cluster, false, &suffix));
    }
    let kind_label = if ctx.array_children { "ObjectArray" } else { "Object" };
    Collapse {
        parent_idx: ctx.parent_idx,
        array_children: ctx.array_children,
        log_msg: format!(
            "  SiblingCollapseMulti {} tables detected: {} ({} tables → {} pivot tables)",
            kind_label, ctx.parent_name, ctx.child_indices.len(), groups.len(),
        ),
        kind: CollapseKind::Multi { groups },
        absorbed_indices: all_absorbed,
    }
}

fn filter_significant_siblings(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Vec<usize> {
    let all_pure = ctx.child_indices.iter().all(|&i| schemas[i].data_columns().next().is_none());
    if all_pure {
        return ctx.child_indices.clone();
    }
    ctx.child_indices
        .iter()
        .copied()
        .filter(|&i| {
            let name = &schemas[i].name;
            let is_pure = schemas[i].data_columns().next().is_none();
            let child_count = obj_map.get(name).map_or(0, std::vec::Vec::len)
                + arr_map.get(name).map_or(0, std::vec::Vec::len);
            !(is_pure && child_count >= ctx.threshold)
        })
        .collect()
}

fn effective_jaccard_for_regular(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    regular: &[usize],
) -> f64 {
    let data_bearing: Vec<usize> = regular
        .iter()
        .copied()
        .filter(|&i| schemas[i].data_columns().next().is_some())
        .collect();
    if data_bearing.len() >= ctx.threshold {
        pairwise_jaccard_min(schemas, &data_bearing)
    } else {
        pairwise_jaccard_min(schemas, regular)
    }
}

/// Child-compatibility gate bypass threshold: skip the gate when Jaccard similarity is very high.
const HIGH_JACCARD: f64 = 0.9;

/// Build a synthetic `SiblingCollapseMulti` when the parent carries its own data or has
/// significant containers — the parent cannot be repurposed as a pivot table itself.
/// Returns `None` when the parent has data and the keys are non-numeric.
///
/// Input — a JSON object where the parent has its own data alongside numeric children:
/// ```json
/// {
///   "own_col": "x",
///   "1": {"v": "a"},
///   "2": {"v": "b"},
///   "3": {"v": "c"}
/// }
/// ```
/// Output — the parent keeps its own columns; a separate pivot table is created for the children:
/// ```sql
/// CREATE TABLE p     (j2s_id uuid, own_col text);
/// CREATE TABLE p_num (j2s_id uuid, j2s_p_id uuid, key_id text, v text);
/// ```
/// If `p_num` already exists in schemas, the pivot is named `p_num_2` (and so on).
fn build_synthetic_pivot_collapse(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    regular: Vec<usize>,
) -> Option<Collapse> {
    let key_is_numeric = !ctx.numeric_idx.is_empty();
    if ctx.parent_has_data && !key_is_numeric { return None; }
    let raw_suffix = if key_is_numeric { "num" } else { "key" };
    let suffix = unique_cluster_suffix(&ctx.parent_name, raw_suffix, schemas);
    let groups = vec![make_subgroup(schemas, &ctx.parent_name, &regular, key_is_numeric, &suffix)];
    Some(Collapse {
        parent_idx: ctx.parent_idx,
        array_children: ctx.array_children,
        log_msg: format!(
            "  Synthetic pivot for parent with data/sig-containers: {} ({} tables → 1)",
            ctx.parent_name, regular.len(),
        ),
        kind: CollapseKind::Multi { groups },
        absorbed_indices: regular,
    })
}

fn detect_homogeneous_collapse(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Option<Collapse> {
    let regular = filter_significant_siblings(schemas, ctx, obj_map, arr_map);
    if regular.len() < ctx.threshold { return None; }
    let actual_jaccard = effective_jaccard_for_regular(schemas, ctx, &regular);
    if actual_jaccard < ctx.min_jaccard { return try_cluster_fallback(schemas, ctx, &regular); }
    if actual_jaccard < HIGH_JACCARD
        && child_compatibility_score(schemas, &regular, obj_map, arr_map) < ctx.min_jaccard
    {
        return None;
    }
    let has_sig_containers = regular.len() < ctx.child_indices.len();
    if ctx.parent_has_data || has_sig_containers {
        return build_synthetic_pivot_collapse(schemas, ctx, regular);
    }
    Some(build_classic_keyed_pivot_collapse(schemas, ctx, regular))
}

/// Build a classic `SiblingCollapse` collapse: the pure-container parent absorbs all `regular`
/// siblings into a single keyed pivot table.
#[allow(clippy::too_many_lines)] // inline Collapse construction with log assembly
fn build_classic_keyed_pivot_collapse(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    regular: Vec<usize>,
) -> Collapse {
    let keys: Vec<String> = regular
        .iter()
        .map(|&i| schemas[i].path.last().cloned().unwrap_or_default())
        .collect();
    let key_shape = classify_key_shape(&keys.iter().map(std::string::String::as_str).collect::<Vec<_>>());
    let key_col_name = match &key_shape {
        KeyShape::Numeric => "key_id".to_string(),
        KeyShape::IsoLang => "lang_code".to_string(),
        _ => "key".to_string(),
    };
    let children: Vec<&TableSchema> = regular.iter().map(|&i| &schemas[i]).collect();
    let union_cols = build_union_columns(&children);
    let key_examples = keys.iter().take(5).map(std::string::String::as_str).collect::<Vec<_>>().join("\", \"");
    let more = if keys.len() > 5 { format!("\" (+{} more)", keys.len() - 5) } else { "\"".to_string() };
    let kind_label = if ctx.array_children { "ObjectArray" } else { "Object" };
    Collapse {
        parent_idx: ctx.parent_idx,
        array_children: ctx.array_children,
        log_msg: format!(
            "  Sibling {} tables detected: {} ({} tables → 1)\n  Keys: \"{}{}\n  Jaccard min: {:.2} → strategy: SiblingCollapse (col: {} {})",
            kind_label, ctx.parent_name, regular.len(), key_examples, more,
            ctx.min_jaccard, key_col_name, key_shape,
        ),
        kind: CollapseKind::Single {
            key_col_name,
            key_shape,
            union_cols,
        },
        absorbed_indices: regular,
    }
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
    let schema = TableSchema {
        name: pivot_name,
        path,
        parent_table: Some(parent.name.to_string()),
        depth: parent.depth + 1,
        columns: cols,
        child_kind: Some(ChildKind::Object),
        inferred_strategy: InferredStrategy::SiblingCollapse(sibling_schema),
        toml_override: None,
        ui_override: None,
        flatten_sources: std::collections::HashMap::new(),
        child_routes: std::collections::HashMap::new(),
        row_count: source_row_count,
        cached_strategy: None,
    };
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

fn make_subgroup(
    schemas: &[TableSchema],
    parent_name: &str,
    indices: &[usize],
    key_is_numeric: bool,
    suffix: &str,
) -> SubgroupData {
    let pivot_name = format!("{parent_name}_{suffix}");
    let sub_keys: Vec<String> = indices
        .iter()
        .map(|&i| schemas[i].path.last().cloned().unwrap_or_default())
        .collect();
    let shape = classify_key_shape(&sub_keys.iter().map(std::string::String::as_str).collect::<Vec<_>>());
    let key_col = if key_is_numeric {
        "key_id".to_string()
    } else {
        match shape {
            KeyShape::IsoLang => "lang_code".to_string(),
            _ => "key".to_string(),
        }
    };
    let children: Vec<&TableSchema> = indices.iter().map(|&i| &schemas[i]).collect();
    let union_cols = build_union_columns(&children);
    let absorbed = indices.iter().map(|&i| schemas[i].name.clone()).collect();
    SubgroupData {
        pivot_table_name: pivot_name,
        key_is_numeric,
        key_col_name: key_col,
        key_shape: shape,
        union_cols,
        absorbed_names: absorbed,
        path_segment: suffix.to_string(),
        absorbed_path_segments: sub_keys,
    }
}

/// Build the `SiblingDetectCtx` for one work item, filtering `AutoSplit` companions.
/// Returns `None` if effective children fall below `threshold` after filtering.
fn build_sibling_ctx(
    schemas: &[TableSchema],
    parent_name: String,
    parent_idx: usize,
    child_indices: &[usize],
    array_children: bool,
    threshold: usize,
    min_jaccard: f64,
) -> Option<(SiblingDetectCtx, bool)> {
    let filtered: Vec<usize> = child_indices.iter().copied()
        .filter(|&i| !matches!(schemas[i].inferred_strategy, InferredStrategy::Pivot))
        .collect();
    if filtered.len() < threshold { return None; }
    let effective: Vec<usize> = filtered;
    let (numeric_idx, non_numeric_idx): (Vec<usize>, Vec<usize>) =
        effective.iter().partition(|&&i| {
            schemas[i].path.last()
                .is_some_and(|k| !k.is_empty() && k.chars().all(|c| c.is_ascii_digit()))
        });
    let is_mixed = !numeric_idx.is_empty() && !non_numeric_idx.is_empty();
    let ctx = SiblingDetectCtx {
        parent_name,
        parent_idx,
        child_indices: effective,
        array_children,
        threshold,
        min_jaccard,
        parent_has_data: schemas[parent_idx].data_columns().next().is_some(),
        numeric_idx,
        non_numeric_idx,
    };
    Some((ctx, is_mixed))
}

fn should_skip_parent(schemas: &[TableSchema], parent_idx: usize) -> bool {
    !matches!(schemas[parent_idx].inferred_strategy, InferredStrategy::Columns | InferredStrategy::AutoSplit { .. })
}

fn collect_sibling_collapses(
    schemas: &[TableSchema],
    work: &[(String, Vec<usize>, bool)],
    threshold: usize,
    min_jaccard: f64,
    name_to_idx: &std::collections::HashMap<String, usize>,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Vec<Collapse> {
    let mut collapses: Vec<Collapse> = Vec::new();
    for (parent_name, child_indices, array_children) in work {
        if child_indices.len() < threshold { continue; }
        let Some(&parent_idx) = name_to_idx.get(parent_name.as_str()) else { continue };
        if should_skip_parent(schemas, parent_idx) { continue; }
        let Some((ctx, is_mixed)) = build_sibling_ctx(
            schemas, parent_name.clone(), parent_idx, child_indices, *array_children, threshold, min_jaccard,
        ) else { continue; };
        let collapse = if is_mixed {
            detect_mixed_collapse(schemas, &ctx, obj_map, arr_map)
        } else {
            detect_homogeneous_collapse(schemas, &ctx, obj_map, arr_map)
        };
        if let Some(c) = collapse { collapses.push(c); }
    }
    collapses
}

fn run_sibling_wave(
    schemas: &mut Vec<TableSchema>,
    threshold: usize,
    min_jaccard: f64,
) -> Vec<CoSiblingGroup> {
    let (parent_to_object_children, parent_to_array_children) = build_parent_child_maps(schemas);
    let name_to_idx: std::collections::HashMap<String, usize> = schemas
        .iter().enumerate().map(|(i, s)| (s.name.clone(), i)).collect();
    let work = build_work_items(schemas, threshold, &parent_to_object_children, &parent_to_array_children);
    let collapses = collect_sibling_collapses(
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
    let t_schema = TableSchema {
        name: t_name.clone(),
        path: t_path,
        parent_table: Some(parent_name),
        depth: parent_depth + 1,
        columns: cols,
        child_kind: Some(if group.array_children { ChildKind::ObjectArray } else { ChildKind::Object }),
        inferred_strategy: InferredStrategy::Columns,
        toml_override: None,
        ui_override: None,
        flatten_sources: std::collections::HashMap::new(),
        child_routes: std::collections::HashMap::new(),
        row_count: source_row_count,
        cached_strategy: None,
    };
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
    TableSchema {
        name: sub_pivot_name,
        path: sub_path,
        parent_table: Some(parent_name),
        depth: parent_depth + 1,
        columns: cols,
        child_kind: Some(ChildKind::Object),
        inferred_strategy: InferredStrategy::SiblingCollapse(sibling_schema),
        toml_override: None,
        ui_override: None,
        flatten_sources: std::collections::HashMap::new(),
        child_routes: std::collections::HashMap::new(),
        row_count: source_row_count,
        cached_strategy: None,
    }
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

    fn make_ctx(
        schemas: &[TableSchema],
        parent_idx: usize,
        child_indices: Vec<usize>,
        threshold: usize,
        min_jaccard: f64,
    ) -> SiblingDetectCtx {
        let (numeric_idx, non_numeric_idx) = child_indices.iter().partition(|&&i| {
            schemas[i].path.last().map(|k| k.chars().all(|c| c.is_ascii_digit())).unwrap_or(false)
        });
        SiblingDetectCtx {
            parent_name: schemas[parent_idx].name.clone(),
            parent_idx,
            child_indices: child_indices.clone(),
            array_children: false,
            threshold,
            min_jaccard,
            parent_has_data: schemas[parent_idx].data_columns().next().is_some(),
            numeric_idx,
            non_numeric_idx,
        }
    }

    #[test]
    fn test_detect_homogeneous_classic_keyed_pivot() {
        let mut parent = make_parent("p");
        parent.inferred_strategy = InferredStrategy::Columns;
        let schemas = vec![
            parent,
            make_child_with_key("p_fr", "p", "fr", &["label"]),
            make_child_with_key("p_en", "p", "en", &["label"]),
            make_child_with_key("p_de", "p", "de", &["label"]),
        ];
        let ctx = make_ctx(&schemas, 0, vec![1, 2, 3], 3, 0.5);
        let empty = std::collections::HashMap::new();
        let result = detect_homogeneous_collapse(&schemas, &ctx, &empty, &empty);
        assert!(result.is_some());
        if let Some(Collapse { kind: CollapseKind::Single { key_col_name, .. }, .. }) = result {
            assert_eq!(key_col_name, "lang_code");
        } else {
            panic!("expected Single collapse");
        }
    }

    #[test]
    fn test_detect_homogeneous_below_threshold_returns_none() {
        let mut parent = make_parent("p");
        parent.inferred_strategy = InferredStrategy::Columns;
        let schemas = vec![
            parent,
            make_child_with_key("p_fr", "p", "fr", &["label"]),
            make_child_with_key("p_en", "p", "en", &["label"]),
        ];
        let ctx = make_ctx(&schemas, 0, vec![1, 2], 3, 0.5);
        let empty = std::collections::HashMap::new();
        assert!(detect_homogeneous_collapse(&schemas, &ctx, &empty, &empty).is_none());
    }

    #[test]
    fn test_detect_mixed_both_groups_qualify() {
        let mut parent = make_parent("p");
        parent.inferred_strategy = InferredStrategy::Columns;
        let schemas = vec![
            parent,
            make_child_with_key("p_1",  "p", "1",  &["val"]),
            make_child_with_key("p_2",  "p", "2",  &["val"]),
            make_child_with_key("p_3",  "p", "3",  &["val"]),
            make_child_with_key("p_fr", "p", "fr", &["label"]),
            make_child_with_key("p_en", "p", "en", &["label"]),
            make_child_with_key("p_de", "p", "de", &["label"]),
        ];
        let ctx = make_ctx(&schemas, 0, vec![1, 2, 3, 4, 5, 6], 3, 0.5);
        let empty = std::collections::HashMap::new();
        let result = detect_mixed_collapse(&schemas, &ctx, &empty, &empty);
        assert!(result.is_some());
        if let Some(Collapse { kind: CollapseKind::Multi { groups }, .. }) = result {
            assert_eq!(groups.len(), 2);
            assert!(groups.iter().any(|g| g.key_is_numeric));
            assert!(groups.iter().any(|g| !g.key_is_numeric));
        } else {
            panic!("expected Multi collapse");
        }
    }

    #[test]
    fn test_detect_mixed_both_below_threshold_returns_none() {
        let mut parent = make_parent("p");
        parent.inferred_strategy = InferredStrategy::Columns;
        let schemas = vec![
            parent,
            make_child_with_key("p_1",  "p", "1",  &["v"]),
            make_child_with_key("p_2",  "p", "2",  &["v"]),
            make_child_with_key("p_fr", "p", "fr", &["label"]),
            make_child_with_key("p_en", "p", "en", &["label"]),
        ];
        let ctx = make_ctx(&schemas, 0, vec![1, 2, 3, 4], 3, 0.5);
        let empty = std::collections::HashMap::new();
        let _ = detect_mixed_collapse(&schemas, &ctx, &empty, &empty);
    }

    #[test]
    fn test_build_work_items_filters_below_threshold() {
        let schemas = vec![
            make_sibling("p_a", "p", &["v"]),
            make_sibling("p_b", "p", &["v"]),
        ];
        let mut obj_map = std::collections::HashMap::new();
        obj_map.insert("p".to_string(), vec![0usize, 1]);
        let arr_map = std::collections::HashMap::new();
        let work = build_work_items(&schemas, 3, &obj_map, &arr_map);
        assert!(work.is_empty());
        let work2 = build_work_items(&schemas, 2, &obj_map, &arr_map);
        assert_eq!(work2.len(), 1);
        assert_eq!(work2[0].0, "p");
        assert!(!work2[0].2);
    }

    #[test]
    fn test_build_work_items_child_indices_sorted_by_name() {
        let schemas = vec![
            make_sibling("p_c", "p", &["v"]),
            make_sibling("p_a", "p", &["v"]),
            make_sibling("p_b", "p", &["v"]),
        ];
        let mut obj_map = std::collections::HashMap::new();
        obj_map.insert("p".to_string(), vec![0usize, 1, 2]);
        let arr_map = std::collections::HashMap::new();
        let work = build_work_items(&schemas, 3, &obj_map, &arr_map);
        assert_eq!(work.len(), 1);
        assert_eq!(work[0].1, vec![1, 2, 0]);
    }

    #[test]
    fn test_build_work_items_sorted_by_parent_name() {
        let schemas = vec![
            make_sibling("z_a", "z", &["v"]),
            make_sibling("z_b", "z", &["v"]),
            make_sibling("z_c", "z", &["v"]),
            make_sibling("a_a", "a", &["v"]),
            make_sibling("a_b", "a", &["v"]),
            make_sibling("a_c", "a", &["v"]),
        ];
        let mut obj_map = std::collections::HashMap::new();
        obj_map.insert("z".to_string(), vec![0, 1, 2]);
        obj_map.insert("a".to_string(), vec![3, 4, 5]);
        let arr_map = std::collections::HashMap::new();
        let work = build_work_items(&schemas, 3, &obj_map, &arr_map);
        assert_eq!(work.len(), 2);
        assert_eq!(work[0].0, "a");
        assert_eq!(work[1].0, "z");
    }

    #[test]
    fn test_make_subgroup_pivot_name_and_absorbed() {
        let schemas = vec![
            make_child_with_key("p_front", "p", "front", &["url"]),
            make_child_with_key("p_back",  "p", "back",  &["url"]),
            make_child_with_key("p_top",   "p", "top",   &["url"]),
        ];
        let g = make_subgroup(&schemas, "p", &[0, 1, 2], false, "key");
        assert_eq!(g.pivot_table_name, "p_key");
        let mut absorbed = g.absorbed_names.clone();
        absorbed.sort();
        assert_eq!(absorbed, vec!["p_back", "p_front", "p_top"]);
        assert!(!g.key_is_numeric);
    }

    #[test]
    fn test_make_subgroup_numeric_key_col() {
        let schemas = vec![
            make_child_with_key("p_1", "p", "1", &["val"]),
            make_child_with_key("p_2", "p", "2", &["val"]),
            make_child_with_key("p_3", "p", "3", &["val"]),
        ];
        let g = make_subgroup(&schemas, "p", &[0, 1, 2], true, "num");
        assert_eq!(g.pivot_table_name, "p_num");
        assert_eq!(g.key_col_name, "key_id");
        assert!(g.key_is_numeric);
        assert!(matches!(g.key_shape, KeyShape::Numeric));
    }

    #[test]
    fn test_make_subgroup_isolang_key_col() {
        let schemas = vec![
            make_child_with_key("p_fr", "p", "fr", &["label"]),
            make_child_with_key("p_en", "p", "en", &["label"]),
            make_child_with_key("p_de", "p", "de", &["label"]),
        ];
        let g = make_subgroup(&schemas, "p", &[0, 1, 2], false, "key");
        assert_eq!(g.key_col_name, "lang_code");
        assert!(matches!(g.key_shape, KeyShape::IsoLang));
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
    fn test_build_synthetic_pivot_numeric_keys_produces_multi() {
        let parent = make_parent("p");
        let schemas = vec![
            parent,
            make_child_with_key("p_1", "p", "1", &["v"]),
            make_child_with_key("p_2", "p", "2", &["v"]),
            make_child_with_key("p_3", "p", "3", &["v"]),
        ];
        let ctx = make_ctx(&schemas, 0, vec![1, 2, 3], 3, 0.5);
        let result = build_synthetic_pivot_collapse(&schemas, &ctx, vec![1, 2, 3]);
        assert!(result.is_some(), "numeric keys must produce a synthetic pivot");
        if let Some(Collapse { kind: CollapseKind::Multi { groups }, .. }) = result {
            assert_eq!(groups.len(), 1);
            assert!(groups[0].key_is_numeric);
        } else {
            panic!("expected Multi collapse");
        }
    }

    #[test]
    fn test_build_synthetic_pivot_parent_data_non_numeric_returns_none() {
        let mut parent = make_parent("p");
        parent.columns.push(ColumnSchema {
            name: "extra".to_string(), original_name: "extra".to_string(),
            pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
        });
        let schemas = vec![
            parent,
            make_child_with_key("p_fr", "p", "fr", &["label"]),
            make_child_with_key("p_en", "p", "en", &["label"]),
            make_child_with_key("p_de", "p", "de", &["label"]),
        ];
        let ctx = make_ctx(&schemas, 0, vec![1, 2, 3], 3, 0.5);
        // parent has data + non-numeric keys → must return None
        let result = build_synthetic_pivot_collapse(&schemas, &ctx, vec![1, 2, 3]);
        assert!(result.is_none(), "parent with data and non-numeric keys must return None");
    }

    #[test]
    fn test_filter_routing_tables_excludes_pure_with_enough_children() {
        // A pure schema (no data columns) with >= threshold child routes is a "routing table"
        // and must be excluded from non_num_regular.
        let mut routing = make_sibling("p_x", "p", &[]); // pure: no data cols
        routing.child_routes.insert("key".to_string(), "child".to_string());
        routing.child_routes.insert("key2".to_string(), "child2".to_string());
        let normal = make_sibling("p_y", "p", &["label"]); // has data
        let schemas = vec![make_parent("p"), routing, normal];
        // indices 1 (routing) and 2 (normal) are non-numeric siblings
        let non_numeric_idx = vec![1usize, 2];
        let mut obj_map = std::collections::HashMap::new();
        obj_map.insert("p_x".to_string(), vec![0usize, 1]); // 2 children >= threshold 2
        let arr_map = std::collections::HashMap::new();
        let result = filter_routing_tables(&schemas, &non_numeric_idx, &obj_map, &arr_map, 2);
        assert!(!result.contains(&1), "routing table must be excluded");
        assert!(result.contains(&2), "table with data columns must be kept");
    }

    #[test]
    fn test_filter_routing_tables_keeps_pure_with_few_children() {
        let mut routing = make_sibling("p_x", "p", &[]); // pure
        routing.child_routes.insert("key".to_string(), "child".to_string());
        let schemas = vec![make_parent("p"), routing];
        let non_numeric_idx = vec![1usize];
        let obj_map = std::collections::HashMap::new(); // 0 children → below threshold
        let arr_map = std::collections::HashMap::new();
        let result = filter_routing_tables(&schemas, &non_numeric_idx, &obj_map, &arr_map, 2);
        assert!(result.contains(&1), "pure table with few children is not a routing table");
    }

    // --- Finding 2: path collision dans path_map pour clusters non-numériques multiples ---

    #[test]
    fn test_two_non_numeric_clusters_have_distinct_path_segments() {
        // 6 non-numeric siblings split into 2 incompatible clusters (Jaccard=0 between groups,
        // Jaccard=1.0 within each group). try_cluster_fallback must produce SubgroupData with
        // distinct path_segment values so path_map can store both without collision.
        let mut parent = make_parent("p");
        parent.inferred_strategy = InferredStrategy::Columns;
        let schemas = vec![
            parent,
            make_child_with_key("p_a1", "p", "a1", &["col"]),
            make_child_with_key("p_a2", "p", "a2", &["col"]),
            make_child_with_key("p_a3", "p", "a3", &["col"]),
            make_child_with_key("p_b1", "p", "b1", &["size"]),
            make_child_with_key("p_b2", "p", "b2", &["size"]),
            make_child_with_key("p_b3", "p", "b3", &["size"]),
        ];
        let ctx = make_ctx(&schemas, 0, vec![1, 2, 3, 4, 5, 6], 3, 0.5);
        let regular = vec![1, 2, 3, 4, 5, 6];
        let result = try_cluster_fallback(&schemas, &ctx, &regular);
        let Some(Collapse { kind: CollapseKind::Multi { groups }, .. }) = result else {
            panic!("expected Multi collapse with 2 non-numeric groups");
        };
        assert_eq!(groups.len(), 2, "expected exactly 2 clusters");
        assert_ne!(
            groups[0].path_segment, groups[1].path_segment,
            "path_segments must be distinct; got {:?} and {:?}",
            groups[0].path_segment, groups[1].path_segment,
        );
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
        let g0 = make_subgroup(&schemas, "p", &[1, 2, 3], false, "cluster_0");
        let g1 = make_subgroup(&schemas, "p", &[4, 5, 6], false, "cluster_1");
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

    #[test]
    fn test_make_subgroup_stores_path_segment_and_absorbed_path_segments() {
        let schemas = vec![
            make_child_with_key("p_front", "p", "front", &["url"]),
            make_child_with_key("p_back",  "p", "back",  &["url"]),
            make_child_with_key("p_top",   "p", "top",   &["url"]),
        ];
        let g = make_subgroup(&schemas, "p", &[0, 1, 2], false, "cluster_0");
        assert_eq!(g.path_segment, "cluster_0");
        let mut segs = g.absorbed_path_segments.clone();
        segs.sort();
        assert_eq!(segs, vec!["back", "front", "top"]);
    }

    // --- Bugs #1/#2/#4/#6: collisions de suffixes dans les boucles de clustering ---

    #[test]
    fn test_try_cluster_fallback_same_key_prefix_distinct_pivot_names() {
        // JSON en entrée :
        //   { "img_back": {"col_a":"…"}, "img_front": {"col_a":"…"},
        //     "img_side": {"col_b":"…"}, "img_top":   {"col_b":"…"} }
        //
        // Avec le bug — les deux clusters reçoivent pivot_table_name = "p_img_key".
        // apply_multi_collapse pousse deux TableSchema "p_img_key" dans schemas ;
        // Pass 2 écrase le premier en path_map → cluster 1 silencieusement perdu :
        //   CREATE TABLE p_img_key (j2s_id uuid, j2s_p_id uuid, key text, col_b text);
        //   -- col_a / img_back / img_front jamais importés
        //
        // Après fix — deux tables distinctes, toutes les données importées :
        //   CREATE TABLE p_img_key   (j2s_id uuid, j2s_p_id uuid, key text, col_a text);
        //   CREATE TABLE p_img_key_2 (j2s_id uuid, j2s_p_id uuid, key text, col_b text);
        let parent = make_parent("p");
        let schemas = vec![
            parent,
            make_child_with_key("p_img_back",  "p", "img_back",  &["col_a"]),
            make_child_with_key("p_img_front", "p", "img_front", &["col_a"]),
            make_child_with_key("p_img_side",  "p", "img_side",  &["col_b"]),
            make_child_with_key("p_img_top",   "p", "img_top",   &["col_b"]),
        ];
        let ctx = make_ctx(&schemas, 0, vec![1, 2, 3, 4], 2, 0.5);
        let result = try_cluster_fallback(&schemas, &ctx, &[1, 2, 3, 4]);
        let Some(Collapse { kind: CollapseKind::Multi { groups }, .. }) = result else {
            panic!("expected Multi collapse with 2 clusters");
        };
        assert_eq!(groups.len(), 2, "expected exactly 2 clusters");
        assert_ne!(
            groups[0].pivot_table_name, groups[1].pivot_table_name,
            "same key prefix must produce distinct pivot_table_names; got {:?} and {:?}",
            groups[0].pivot_table_name, groups[1].pivot_table_name,
        );
        assert_ne!(
            groups[0].path_segment, groups[1].path_segment,
            "same key prefix must produce distinct path_segments; got {:?} and {:?}",
            groups[0].path_segment, groups[1].path_segment,
        );
    }

    #[test]
    fn test_assemble_mixed_collapse_same_prefix_clusters_distinct_pivot_names() {
        // JSON en entrée :
        //   { "1": {"val":"…"}, "2": {"val":"…"},
        //     "tag_fr": {"label":"…"}, "tag_en": {"label":"…"},
        //     "tag_de": {"size":"…"},  "tag_it":  {"size":"…"} }
        //
        // Avec le bug — les deux clusters non-numériques reçoivent pivot_table_name = "p_tag_key" :
        //   CREATE TABLE p_num     (j2s_id uuid, j2s_p_id uuid, key_id text, val text);
        //   CREATE TABLE p_tag_key (j2s_id uuid, j2s_p_id uuid, key   text, size text);
        //   -- label / tag_fr / tag_en jamais importés (cluster 0 écrasé par cluster 1)
        //
        // Après fix :
        //   CREATE TABLE p_num       (j2s_id uuid, j2s_p_id uuid, key_id text, val   text);
        //   CREATE TABLE p_tag_key   (j2s_id uuid, j2s_p_id uuid, key   text, label text);
        //   CREATE TABLE p_tag_key_2 (j2s_id uuid, j2s_p_id uuid, key   text, size  text);
        let parent = make_parent("p");
        let schemas = vec![
            parent,
            make_child_with_key("p_1",      "p", "1",      &["val"]),
            make_child_with_key("p_2",      "p", "2",      &["val"]),
            make_child_with_key("p_tag_fr", "p", "tag_fr", &["label"]),
            make_child_with_key("p_tag_en", "p", "tag_en", &["label"]),
            make_child_with_key("p_tag_de", "p", "tag_de", &["size"]),
            make_child_with_key("p_tag_it", "p", "tag_it", &["size"]),
        ];
        let ctx = make_ctx(&schemas, 0, vec![1, 2, 3, 4, 5, 6], 2, 0.5);
        let non_num_clusters = vec![vec![3usize, 4], vec![5usize, 6]];
        let collapse = assemble_mixed_collapse(&schemas, &ctx, true, false, &[], &non_num_clusters);
        let CollapseKind::Multi { groups } = collapse.kind else { panic!("expected Multi") };
        let cluster_groups: Vec<_> = groups.iter().filter(|g| !g.key_is_numeric).collect();
        assert_eq!(cluster_groups.len(), 2, "expected 2 non-numeric cluster groups");
        assert_ne!(
            cluster_groups[0].pivot_table_name, cluster_groups[1].pivot_table_name,
            "clusters with same key prefix must get distinct pivot names; got {:?} and {:?}",
            cluster_groups[0].pivot_table_name, cluster_groups[1].pivot_table_name,
        );
    }

    #[test]
    fn test_assemble_mixed_collapse_existing_num_no_collision() {
        // JSON en entrée : clés numériques "1", "2" sous p.
        // Précondition : la table p_num existe déjà dans schemas (autre chemin JSON).
        //
        // Avec le bug — "num" est hardcodé sans vérifier schemas :
        //   CREATE TABLE p_num (j2s_id uuid, j2s_p_id uuid, key_id text, val text);
        //   -- deux TableSchema "p_num" dans le vecteur ; path_map n'en garde qu'un
        //   -- → les données de l'un des deux sont silencieusement perdues
        //
        // Après fix :
        //   CREATE TABLE p_num   (...);  -- ancienne table inchangée
        //   CREATE TABLE p_num_2 (j2s_id uuid, j2s_p_id uuid, key_id text, val text);
        let parent = make_parent("p");
        let mut existing_num = make_parent("p_num");
        existing_num.parent_table = Some("p".to_string());
        let schemas = vec![
            parent,
            existing_num,
            make_child_with_key("p_1", "p", "1", &["val"]),
            make_child_with_key("p_2", "p", "2", &["val"]),
        ];
        let ctx = make_ctx(&schemas, 0, vec![2, 3], 2, 0.5);
        let collapse = assemble_mixed_collapse(&schemas, &ctx, true, false, &[], &[]);
        let CollapseKind::Multi { groups } = collapse.kind else { panic!("expected Multi") };
        let num_group = groups.iter().find(|g| g.key_is_numeric).unwrap();
        assert_ne!(
            num_group.pivot_table_name, "p_num",
            "pivot_table_name must not collide with existing p_num; got {:?}",
            num_group.pivot_table_name,
        );
    }

    #[test]
    fn test_build_synthetic_pivot_existing_num_no_collision() {
        // JSON en entrée : parent p avec données propres + clés numériques "1", "2", "3".
        // Précondition : la table p_num existe déjà dans schemas.
        //
        // Avec le bug — "num" hardcodé dans build_synthetic_pivot_collapse :
        //   CREATE TABLE p_num (j2s_id uuid, j2s_p_id uuid, key_id text, v text);
        //   -- deux TableSchema "p_num" → path_map écrase l'un ; données perdues
        //
        // Après fix :
        //   CREATE TABLE p_num   (...);  -- ancienne table inchangée
        //   CREATE TABLE p_num_2 (j2s_id uuid, j2s_p_id uuid, key_id text, v text);
        let parent = make_parent("p");
        let mut existing_num = make_parent("p_num");
        existing_num.parent_table = Some("p".to_string());
        let schemas = vec![
            parent,
            existing_num,
            make_child_with_key("p_1", "p", "1", &["v"]),
            make_child_with_key("p_2", "p", "2", &["v"]),
            make_child_with_key("p_3", "p", "3", &["v"]),
        ];
        let ctx = make_ctx(&schemas, 0, vec![2, 3, 4], 3, 0.5);
        let result = build_synthetic_pivot_collapse(&schemas, &ctx, vec![2, 3, 4]);
        let Some(Collapse { kind: CollapseKind::Multi { groups }, .. }) = result else {
            panic!("expected Some(Multi)");
        };
        assert_eq!(groups.len(), 1);
        assert_ne!(
            groups[0].pivot_table_name, "p_num",
            "pivot_table_name must not collide with existing p_num; got {:?}",
            groups[0].pivot_table_name,
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
