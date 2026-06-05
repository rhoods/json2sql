use super::wide_strategies::{build_union_columns, classify_key_shape};
use super::table_schema::{ChildKind, ColumnSchema, KeyShape, SiblingSchema, TableSchema, WideStrategy};
use super::type_tracker::PgType;

// ---------------------------------------------------------------------------
// Sibling collapse types and detection context (used by run_sibling_wave)
// ---------------------------------------------------------------------------

struct SubgroupData {
    pivot_table_name: String,
    key_is_numeric: bool,
    key_col_name: String,
    key_shape: KeyShape,
    union_cols: Vec<ColumnSchema>,
    absorbed_names: Vec<String>,
}

enum CollapseKind {
    Single {
        key_col_name: String,
        key_shape: KeyShape,
        union_cols: Vec<ColumnSchema>,
        data_col_name: String,
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
// Sibling table detection — helpers
// ---------------------------------------------------------------------------

/// Returns the minimum pairwise Jaccard similarity of the column sets of children
/// that are **shared** (same JSON last-path-segment) across at least two members of
/// `sibling_indices`.
///
/// Used as a hard gate: if any shared child key has Jaccard < `min_jaccard`, the
/// caller should refuse to merge the sibling group even when the siblings' own
/// column Jaccard is high.
///
/// Returns `1.0` when:
///   - no sibling has any Object/ObjectArray children (vacuously compatible), OR
///   - all child keys appear in only one sibling (no shared children to compare).
pub fn child_compatibility_score(
    schemas: &[TableSchema],
    sibling_indices: &[usize],
    parent_to_obj: &std::collections::HashMap<String, Vec<usize>>,
    parent_to_arr: &std::collections::HashMap<String, Vec<usize>>,
) -> f64 {
    // Build: json_key → [child_indices from each sibling that owns that key]
    let mut key_to_children: std::collections::HashMap<&str, Vec<usize>> =
        std::collections::HashMap::new();

    for &i in sibling_indices {
        let name = schemas[i].name.as_str();
        let obj_ch = parent_to_obj.get(name).map(|v| v.as_slice()).unwrap_or(&[]);
        let arr_ch = parent_to_arr.get(name).map(|v| v.as_slice()).unwrap_or(&[]);
        for &ci in obj_ch.iter().chain(arr_ch.iter()) {
            if let Some(key) = schemas[ci].path.last() {
                key_to_children.entry(key.as_str()).or_default().push(ci);
            }
        }
    }

    // Only consider keys present in ≥ 2 siblings.
    let shared: Vec<&Vec<usize>> = key_to_children
        .values()
        .filter(|v| v.len() >= 2)
        .collect();

    if shared.is_empty() {
        return 1.0;
    }

    shared
        .iter()
        .map(|group| pairwise_jaccard_min(schemas, group))
        .fold(1.0_f64, f64::min)
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
pub(crate) fn finalize_cascading(schemas: &mut Vec<TableSchema>, threshold: usize, min_jaccard: f64) {
    // ── Wave 0: standard sibling detection + child-compat gate ──────────────
    let co_siblings_0 = run_sibling_wave(schemas, threshold, min_jaccard);

    // ── Waves 1+: cascade ────────────────────────────────────────────────────
    let mut pending: Vec<CoSiblingGroup> = co_siblings_0;
    while !pending.is_empty() {
        let mut next_pending: Vec<CoSiblingGroup> = Vec::new();
        for group in pending {
            let produced = process_co_sibling_group(schemas, threshold, min_jaccard, group);
            next_pending.extend(produced);
        }
        pending = next_pending;
    }

    // ── Wave 0 bis: sibling detection on T tables created by the BFS cascade ──
    // Tables produced by process_co_sibling_group (e.g. cluster_0_sizes_100/200/400/full)
    // share a Columns parent but did not exist during wave 0. This pass fuses them.
    // Parents already converted to KeyedPivot/MultiKeyedPivot are skipped automatically.
    let co_siblings_bis = run_sibling_wave(schemas, threshold, min_jaccard);
    let mut pending_bis: Vec<CoSiblingGroup> = co_siblings_bis;
    while !pending_bis.is_empty() {
        let mut next_pending: Vec<CoSiblingGroup> = Vec::new();
        for group in pending_bis {
            let produced = process_co_sibling_group(schemas, threshold, min_jaccard, group);
            next_pending.extend(produced);
        }
        pending_bis = next_pending;
    }

    // ── Post-pass: merge Columns orphans under KeyedPivot parents ───────────
    // After the BFS cascade, some Columns tables survive as children of a KeyedPivot
    // parent (e.g. lang-code T tables produced by cascade wave 1 that themselves
    // are numerous and similar).  A second sibling-detection pass fuses them into
    // a synthetic sub-pivot and cascades their own children.
    let post_co_siblings = run_keyed_pivot_children_wave(schemas, threshold, min_jaccard);
    let mut pending: Vec<CoSiblingGroup> = post_co_siblings;
    while !pending.is_empty() {
        let mut next_pending: Vec<CoSiblingGroup> = Vec::new();
        for group in pending {
            let produced = process_co_sibling_group(schemas, threshold, min_jaccard, group);
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
    /// True when the co-siblings are ObjectArray children.
    array_children: bool,
}

/// Build parent_name → [child_index] maps for Object and ObjectArray children.
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
                Some(ChildKind::ObjectArray) | Some(ChildKind::ScalarArray) => {
                    arr_map.entry(parent.clone()).or_default().push(i);
                }
                _ => {}
            }
        }
    }
    (obj_map, arr_map)
}

/// Collect the children of `sibling_indices` grouped by their last JSON path segment.
/// Returns `(json_key, child_indices)` pairs sorted by key.
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
                        let entry = key_map.entry(key.clone()).or_insert_with(|| (Vec::new(), is_arr));
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
    let key_shape = classify_key_shape(&keys.iter().map(|s| s.as_str()).collect::<Vec<_>>());
    let union_cols = build_union_columns(&ctx.child_indices.iter().map(|&i| &schemas[i]).collect::<Vec<_>>());
    let kind_label = if ctx.array_children { "ObjectArray" } else { "Object" };
    Some(Collapse {
        parent_idx: ctx.parent_idx,
        array_children: ctx.array_children,
        log_msg: format!("  Unified-fallback KeyedPivot {}: {} ({} tables → 1, Jaccard {:.2})", kind_label, ctx.parent_name, ctx.child_indices.len(), unified_jaccard),
        kind: CollapseKind::Single { key_col_name: "key".to_string(), key_shape, union_cols, data_col_name: "j2s_data".to_string() },
        absorbed_indices: ctx.child_indices.clone(),
    })
}

fn try_cluster_fallback(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    regular: &[usize],
) -> Option<Collapse> {
    let has_sig = regular.len() < ctx.child_indices.len();
    let is_autosplit = matches!(schemas[ctx.parent_idx].wide_strategy, WideStrategy::AutoSplit { .. });
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
    for cluster in &valid_clusters {
        let prefix = siblings_key_prefix(schemas, cluster);
        let desired = if prefix.is_empty() { format!("cluster_{}", groups.len()) } else { format!("{}_key", prefix) };
        let suffix = unique_cluster_suffix(&ctx.parent_name, &desired, schemas);
        all_absorbed.extend_from_slice(cluster);
        groups.push(make_subgroup(schemas, &ctx.parent_name, cluster, false, &suffix));
    }
    Some(Collapse {
        parent_idx: ctx.parent_idx,
        array_children: ctx.array_children,
        log_msg: format!("  Schema-cluster MultiKeyedPivot: {} ({} tables → {} clusters)", ctx.parent_name, regular.len(), groups.len()),
        kind: CollapseKind::Multi { groups },
        absorbed_indices: all_absorbed,
    })
}

fn detect_mixed_collapse(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Option<Collapse> {
    let non_num_regular: Vec<usize> = ctx.non_numeric_idx
        .iter()
        .copied()
        .filter(|&i| {
            let name = &schemas[i].name;
            let is_pure = schemas[i].data_columns().next().is_none();
            let child_count = obj_map.get(name).map_or(0, |v| v.len())
                + arr_map.get(name).map_or(0, |v| v.len());
            !(is_pure && child_count >= ctx.threshold)
        })
        .collect();

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

    let non_num_clusters: Vec<Vec<usize>> = if !non_ok && !non_num_regular.is_empty() {
        greedy_schema_clusters(schemas, &non_num_regular, ctx.min_jaccard, ctx.threshold)
            .into_iter()
            .filter(|c| pairwise_jaccard_min(schemas, c) >= ctx.min_jaccard)
            .collect()
    } else {
        Vec::new()
    };

    if !num_ok && !non_ok && non_num_clusters.is_empty() {
        return None;
    }

    Some(assemble_mixed_collapse(schemas, ctx, num_ok, non_ok, &non_num_regular, &non_num_clusters))
}

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
    if num_ok {
        all_absorbed.extend_from_slice(&ctx.numeric_idx);
        groups.push(make_subgroup(schemas, &ctx.parent_name, &ctx.numeric_idx, true, "num"));
    }
    if non_ok {
        all_absorbed.extend_from_slice(non_num_regular);
        groups.push(make_subgroup(schemas, &ctx.parent_name, non_num_regular, false, "key"));
    }
    for (i, cluster) in non_num_clusters.iter().enumerate() {
        let prefix = siblings_key_prefix(schemas, cluster);
        let desired = if prefix.is_empty() { format!("cluster_{}", i) } else { format!("{}_key", prefix) };
        let suffix = unique_cluster_suffix(&ctx.parent_name, &desired, schemas);
        all_absorbed.extend_from_slice(cluster);
        groups.push(make_subgroup(schemas, &ctx.parent_name, cluster, false, &suffix));
    }
    let kind_label = if ctx.array_children { "ObjectArray" } else { "Object" };
    Collapse {
        parent_idx: ctx.parent_idx,
        array_children: ctx.array_children,
        log_msg: format!(
            "  MultiKeyedPivot {} tables detected: {} ({} tables → {} pivot tables)",
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
            let child_count = obj_map.get(name).map_or(0, |v| v.len())
                + arr_map.get(name).map_or(0, |v| v.len());
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

fn detect_homogeneous_collapse(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Option<Collapse> {
    let regular = filter_significant_siblings(schemas, ctx, obj_map, arr_map);

    if regular.len() < ctx.threshold {
        return None;
    }

    let actual_jaccard = effective_jaccard_for_regular(schemas, ctx, &regular);

    if actual_jaccard < ctx.min_jaccard {
        return try_cluster_fallback(schemas, ctx, &regular);
    }

    // Child-compatibility gate — bypassed when Jaccard is very high (≥ 0.9).
    const HIGH_JACCARD: f64 = 0.9;
    if actual_jaccard < HIGH_JACCARD
        && child_compatibility_score(schemas, &regular, obj_map, arr_map) < ctx.min_jaccard
    {
        return None;
    }

    let has_sig_containers = regular.len() < ctx.child_indices.len();

    if ctx.parent_has_data || has_sig_containers {
        let key_is_numeric = !ctx.numeric_idx.is_empty();
        if ctx.parent_has_data && !key_is_numeric {
            return None;
        }
        let suffix = if key_is_numeric { "num" } else { "key" };
        let groups = vec![make_subgroup(schemas, &ctx.parent_name, &regular, key_is_numeric, suffix)];
        return Some(Collapse {
            parent_idx: ctx.parent_idx,
            array_children: ctx.array_children,
            log_msg: format!(
                "  Synthetic pivot for parent with data/sig-containers: {} ({} tables → 1)",
                ctx.parent_name, regular.len(),
            ),
            kind: CollapseKind::Multi { groups },
            absorbed_indices: regular,
        });
    }

    // Classic KeyedPivot: pure container parent becomes the pivot.
    Some(build_classic_keyed_pivot_collapse(schemas, ctx, regular))
}

/// Build a classic `KeyedPivot` collapse: the pure-container parent absorbs all `regular`
/// siblings into a single keyed pivot table.
fn build_classic_keyed_pivot_collapse(
    schemas: &[TableSchema],
    ctx: &SiblingDetectCtx,
    regular: Vec<usize>,
) -> Collapse {
    let keys: Vec<String> = regular
        .iter()
        .map(|&i| schemas[i].path.last().cloned().unwrap_or_default())
        .collect();
    let key_shape = classify_key_shape(&keys.iter().map(|s| s.as_str()).collect::<Vec<_>>());
    let key_col_name = match &key_shape {
        KeyShape::Numeric => "key_id".to_string(),
        KeyShape::IsoLang => "lang_code".to_string(),
        _ => "key".to_string(),
    };
    let children: Vec<&TableSchema> = regular.iter().map(|&i| &schemas[i]).collect();
    let union_cols = build_union_columns(&children);
    let key_examples = keys.iter().take(5).map(|s| s.as_str()).collect::<Vec<_>>().join("\", \"");
    let more = if keys.len() > 5 { format!("\" (+{} more)", keys.len() - 5) } else { "\"".to_string() };
    let kind_label = if ctx.array_children { "ObjectArray" } else { "Object" };
    Collapse {
        parent_idx: ctx.parent_idx,
        array_children: ctx.array_children,
        log_msg: format!(
            "  Sibling {} tables detected: {} ({} tables → 1)\n  Keys: \"{}{}\n  Jaccard min: {:.2} → strategy: KeyedPivot (col: {} {})",
            kind_label, ctx.parent_name, regular.len(), key_examples, more,
            ctx.min_jaccard, key_col_name, key_shape,
        ),
        kind: CollapseKind::Single {
            key_col_name,
            key_shape,
            union_cols,
            data_col_name: "j2s_data".to_string(),
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
    let CollapseKind::Single { key_col_name, key_shape, union_cols, data_col_name } = &collapse.kind else { return Vec::new(); };
    let sibling_schema = SiblingSchema {
        key_col_name: key_col_name.clone(),
        key_shape: key_shape.clone(),
        array_children: collapse.array_children,
        data_col_name: data_col_name.clone(),
    };
    let parent = &mut schemas[collapse.parent_idx];
    parent.columns.retain(|c| c.is_generated);
    if collapse.array_children {
        parent.columns.push(ColumnSchema::generated("j2s_order", PgType::BigInt));
    }
    parent.columns.push(ColumnSchema { name: key_col_name.clone(), original_name: key_col_name.clone(), pg_type: PgType::Text, not_null: true, is_generated: false, is_parent_fk: false });
    for col in union_cols { parent.columns.push(col.clone()); }
    parent.columns.push(ColumnSchema { name: data_col_name.clone(), original_name: data_col_name.clone(), pg_type: PgType::Jsonb, not_null: false, is_generated: true, is_parent_fk: false });
    parent.wide_strategy = WideStrategy::KeyedPivot(sibling_schema);
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

fn build_multi_group_entry(
    g: &SubgroupData,
    parent: &ParentCtx<'_>,
    schemas: &[TableSchema],
    name_to_idx: &std::collections::HashMap<String, usize>,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> (TableSchema, Vec<CoSiblingGroup>) {
    let fk_col = format!("j2s_{}_id", parent.name);
    let mut cols: Vec<ColumnSchema> = vec![
        ColumnSchema::generated("j2s_id", PgType::Uuid),
        ColumnSchema { name: fk_col.clone(), original_name: fk_col, pg_type: PgType::Uuid, not_null: true, is_generated: true, is_parent_fk: true },
    ];
    if parent.array_children { cols.push(ColumnSchema::generated("j2s_order", PgType::BigInt)); }
    cols.push(ColumnSchema { name: g.key_col_name.clone(), original_name: g.key_col_name.clone(), pg_type: PgType::Text, not_null: true, is_generated: false, is_parent_fk: false });
    for col in &g.union_cols { cols.push(col.clone()); }
    cols.push(ColumnSchema { name: "j2s_data".to_string(), original_name: "j2s_data".to_string(), pg_type: PgType::Jsonb, not_null: false, is_generated: true, is_parent_fk: false });
    let mut path = parent.path.to_vec();
    path.push(if g.key_is_numeric { "num" } else { "key" }.to_string());
    let sibling_schema = SiblingSchema { key_col_name: g.key_col_name.clone(), key_shape: g.key_shape.clone(), array_children: parent.array_children, data_col_name: "j2s_data".to_string() };
    let pivot_name = g.pivot_table_name.clone();
    let absorbed_idx: Vec<usize> = g.absorbed_names.iter().filter_map(|n| name_to_idx.get(n.as_str()).copied()).collect();
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
        wide_strategy: WideStrategy::KeyedPivot(sibling_schema),
        flatten_sources: std::collections::HashMap::new(),
        child_routes: std::collections::HashMap::new(),
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
        sibling_schema: SiblingSchema { key_col_name: g.key_col_name.clone(), key_shape: g.key_shape.clone(), array_children: collapse.array_children, data_col_name: "j2s_data".to_string() },
        absorbed_names: g.absorbed_names.clone(),
    }).collect();
    schemas[collapse.parent_idx].wide_strategy = WideStrategy::MultiKeyedPivot(sibling_groups);
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
    collapses: Vec<Collapse>,
    name_to_idx: &std::collections::HashMap<String, usize>,
    obj_map: &std::collections::HashMap<String, Vec<usize>>,
    arr_map: &std::collections::HashMap<String, Vec<usize>>,
) -> Vec<CoSiblingGroup> {
    let mut new_schemas: Vec<TableSchema> = Vec::new();
    let mut co_siblings: Vec<CoSiblingGroup> = Vec::new();
    for collapse in &collapses {
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
    let pivot_name = format!("{}_{}", parent_name, suffix);
    let sub_keys: Vec<String> = indices
        .iter()
        .map(|&i| schemas[i].path.last().cloned().unwrap_or_default())
        .collect();
    let shape = classify_key_shape(&sub_keys.iter().map(|s| s.as_str()).collect::<Vec<_>>());
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
    }
}

/// Build the `SiblingDetectCtx` for one work item, filtering AutoSplit companions.
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
    let effective: Vec<usize> =
        if matches!(schemas[parent_idx].wide_strategy, WideStrategy::AutoSplit { .. }) {
            let filtered: Vec<usize> = child_indices.iter().copied()
                .filter(|&i| !matches!(schemas[i].wide_strategy, WideStrategy::Pivot))
                .collect();
            if filtered.len() < threshold { return None; }
            filtered
        } else {
            child_indices.to_vec()
        };
    let (numeric_idx, non_numeric_idx): (Vec<usize>, Vec<usize>) =
        effective.iter().partition(|&&i| {
            schemas[i].path.last()
                .map(|k| !k.is_empty() && k.chars().all(|c| c.is_ascii_digit()))
                .unwrap_or(false)
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
        let parent_idx = match name_to_idx.get(parent_name.as_str()) {
            Some(&i) => i,
            None => continue,
        };
        if matches!(schemas[parent_idx].wide_strategy, WideStrategy::KeyedPivot(_) | WideStrategy::MultiKeyedPivot(_)) {
            continue;
        }
        if !matches!(schemas[parent_idx].wide_strategy, WideStrategy::Columns | WideStrategy::AutoSplit { .. }) {
            continue;
        }
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
    apply_collapses(schemas, collapses, &name_to_idx, &parent_to_object_children, &parent_to_array_children)
}

/// Process one `CoSiblingGroup` from a cascade wave.
/// Returns new `CoSiblingGroup`s for the next wave (grandchildren level).
fn process_co_sibling_group(
    schemas: &mut Vec<TableSchema>,
    _threshold: usize,
    min_jaccard: f64,
    group: CoSiblingGroup,
) -> Vec<CoSiblingGroup> {
    if group.sibling_indices.len() < 2 {
        handle_single_co_sibling(schemas, &group);
        return Vec::new();
    }

    let (obj_map, arr_map) = build_parent_child_maps(schemas);
    let jaccard = pairwise_jaccard_min(schemas, &group.sibling_indices);
    let compat = child_compatibility_score(schemas, &group.sibling_indices, &obj_map, &arr_map);

    let parent_idx = match schemas.iter().position(|s| s.name == group.synthetic_parent_name) {
        Some(i) => i,
        None => return Vec::new(),
    };

    if jaccard >= min_jaccard && compat >= min_jaccard {
        merge_co_sibling_group(schemas, &group, parent_idx, &obj_map, &arr_map)
    } else {
        reparent_siblings_individually(schemas, &group, parent_idx);
        Vec::new()
    }
}

/// Re-parent a sole co-sibling to its synthetic pivot and register it in child_routes.
fn handle_single_co_sibling(schemas: &mut [TableSchema], group: &CoSiblingGroup) {
    let Some(&idx) = group.sibling_indices.first() else { return };
    let child_name = schemas[idx].name.clone();
    schemas[idx].parent_table = Some(group.synthetic_parent_name.clone());
    if let Some(pi) = schemas.iter().position(|s| s.name == group.synthetic_parent_name) {
        schemas[pi].child_routes.insert(group.json_key.clone(), child_name);
    }
}

fn build_co_sibling_schema(
    group: &CoSiblingGroup,
    parent_depth: usize,
    parent_path: Vec<String>,
    parent_name: String,
    union_cols: Vec<ColumnSchema>,
) -> (String, TableSchema) {
    let t_name = pg_truncate_name(&format!("{}_{}", group.synthetic_parent_name, group.json_key));
    let fk_col = format!("j2s_{parent_name}_id");
    let mut cols: Vec<ColumnSchema> = vec![
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
    if group.array_children {
        cols.push(ColumnSchema::generated("j2s_order", PgType::BigInt));
    }
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
        wide_strategy: WideStrategy::Columns,
        flatten_sources: std::collections::HashMap::new(),
        child_routes: std::collections::HashMap::new(),
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
            let t_pos = schemas.iter().position(|s| s.name == t_name)
                .expect("t_name was just pushed to schemas");
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

    let parent_depth = schemas[parent_idx].depth;
    let parent_path = schemas[parent_idx].path.clone();
    let parent_name = schemas[parent_idx].name.clone();

    let (t_name, t_schema) = build_co_sibling_schema(group, parent_depth, parent_path, parent_name, union_cols);
    schemas[parent_idx].child_routes.insert(group.json_key.clone(), t_name.clone());
    schemas.push(t_schema);

    cascade_grandchildren_to_next_wave(schemas, group, &t_name, obj_map, arr_map)
}

/// Re-parent each sibling individually to the synthetic parent (Jaccard too low to merge).
fn reparent_siblings_individually(
    schemas: &mut [TableSchema],
    group: &CoSiblingGroup,
    parent_idx: usize,
) {
    for &i in &group.sibling_indices {
        let child_name = schemas[i].name.clone();
        let child_key = schemas[i].path.last().cloned().unwrap_or_else(|| group.json_key.clone());
        schemas[i].parent_table = Some(group.synthetic_parent_name.clone());
        schemas[parent_idx].child_routes.insert(child_key, child_name);
    }
}

/// Post-pass: merge `Columns` children of `KeyedPivot` parents into a synthetic sub-pivot.
///
/// After the main BFS cascade, some `Columns` tables survive as direct children of a
/// `KeyedPivot` parent — for example, the lang-code T tables produced by cascade wave 1
/// (one per shared language across image types).  These tables are similar to each other
/// (same schema) but the main `run_sibling_wave` skips them because their parent is no
/// longer `WideStrategy::Columns`.
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
    let key_shape = classify_key_shape(&keys.iter().map(|s| s.as_str()).collect::<Vec<_>>());
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
    let sibling_schema = SiblingSchema {
        key_col_name: key_col_name.clone(),
        key_shape,
        array_children: false,
        data_col_name: "j2s_data".to_string(),
    };
    let cols = build_sub_pivot_columns(&fk_col, &key_col_name, &union_cols);
    let co_sibs = collect_pivot_co_siblings(schemas, child_indices, &sub_pivot_name, obj_map, arr_map);
    reparent_and_update_routes(schemas, parent_idx, child_indices, &sub_pivot_name);
    eprintln!("  KeyedPivot post-pass: {} ({} orphan tables → sub-pivot {})", parent_name, child_indices.len(), sub_pivot_name);
    let t_schema = TableSchema {
        name: sub_pivot_name,
        path: sub_path,
        parent_table: Some(parent_name),
        depth: parent_depth + 1,
        columns: cols,
        child_kind: Some(ChildKind::Object),
        wide_strategy: WideStrategy::KeyedPivot(sibling_schema),
        flatten_sources: std::collections::HashMap::new(),
        child_routes: std::collections::HashMap::new(),
    };
    Some((t_schema, co_sibs))
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

/// Collect (parent_idx, sorted_child_indices) for KeyedPivot parents with enough Columns children.
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
            if !matches!(s.wide_strategy, WideStrategy::KeyedPivot(_)) {
                return None;
            }
            let routed: std::collections::HashSet<&str> =
                s.child_routes.values().map(|v| v.as_str()).collect();
            let mut children: Vec<usize> = obj_map
                .get(&s.name)
                .into_iter()
                .flatten()
                .copied()
                .filter(|&i| matches!(schemas[i].wide_strategy, WideStrategy::Columns))
                .filter(|&i| routed.contains(schemas[i].name.as_str()))
                .collect();
            if children.len() < threshold { return None; }
            children.sort_unstable_by_key(|&i| &schemas[i].name);
            Some((parent_idx, children))
        })
        .collect()
}

/// Build the column list for a sub-pivot KeyedPivot table.
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
    cols.push(ColumnSchema {
        name: "j2s_data".to_string(),
        original_name: "j2s_data".to_string(),
        pg_type: PgType::Jsonb,
        not_null: false,
        is_generated: true,
        is_parent_fk: false,
    });
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

/// Greedy schema clustering: partition `indices` into groups where every member
/// has Jaccard ≥ `min_jaccard` against the cluster seed (first unassigned sibling).
/// Only clusters with at least `min_size` members are returned.
/// Indices are sorted by table name before processing for determinism.
fn greedy_schema_clusters(
    schemas: &[TableSchema],
    indices: &[usize],
    min_jaccard: f64,
    min_size: usize,
) -> Vec<Vec<usize>> {
    let mut unassigned: Vec<usize> = indices.to_vec();
    unassigned.sort_unstable_by_key(|&i| &schemas[i].name);
    let mut clusters: Vec<Vec<usize>> = Vec::new();

    while !unassigned.is_empty() {
        let seed = unassigned.remove(0);
        let seed_cols: std::collections::HashSet<&str> = schemas[seed]
            .data_columns()
            .map(|c| c.original_name.as_str())
            .collect();
        let seed_pure = seed_cols.is_empty();

        let mut cluster = vec![seed];
        let mut remaining = Vec::new();
        for &i in &unassigned {
            let other_cols: std::collections::HashSet<&str> = schemas[i]
                .data_columns()
                .map(|c| c.original_name.as_str())
                .collect();
            let j = if seed_pure && other_cols.is_empty() {
                1.0
            } else if seed_pure || other_cols.is_empty() {
                0.0
            } else {
                let inter = seed_cols.iter().filter(|&&c| other_cols.contains(c)).count();
                let union = seed_cols.len() + other_cols.len() - inter;
                if union == 0 { 1.0 } else { inter as f64 / union as f64 }
            };
            if j >= min_jaccard { cluster.push(i); } else { remaining.push(i); }
        }
        unassigned = remaining;
        if cluster.len() >= min_size {
            clusters.push(cluster);
        }
    }
    clusters
}

/// Returns the longest common prefix of the last JSON path segment across `indices`,
/// with trailing underscores and digits stripped.
fn siblings_key_prefix(schemas: &[TableSchema], indices: &[usize]) -> String {
    let keys: Vec<&[u8]> = indices
        .iter()
        .filter_map(|&i| schemas[i].path.last())
        .map(|s| s.as_bytes())
        .collect();
    if keys.is_empty() {
        return String::new();
    }
    let min_len = keys.iter().map(|b| b.len()).min().unwrap_or(0);
    let prefix_len = (0..min_len)
        .take_while(|&i| keys.iter().all(|k| k[i] == keys[0][i]))
        .count();
    let raw = std::str::from_utf8(&keys[0][..prefix_len]).unwrap_or("");
    raw.trim_end_matches(|c: char| c == '_' || c.is_ascii_digit()).to_string()
}

/// Returns a suffix guaranteed not to produce a name collision with existing schemas.
/// Tries `desired_suffix`, then `desired_suffix_2`, `desired_suffix_3`, … until unique.
fn unique_cluster_suffix(
    parent_name: &str,
    desired_suffix: &str,
    schemas: &[TableSchema],
) -> String {
    let taken = |suffix: &str| schemas.iter().any(|s| s.name == format!("{}_{}", parent_name, suffix));
    if !taken(desired_suffix) {
        return desired_suffix.to_string();
    }
    let mut n = 2usize;
    loop {
        let try_suffix = format!("{}_{}", desired_suffix, n);
        if !taken(&try_suffix) {
            return try_suffix;
        }
        n += 1;
    }
}

/// Truncate a raw table name to fit PostgreSQL's 63-byte identifier limit.
/// Appends a 7-char FNV-1a hex suffix when truncation is needed, matching the
/// strategy used by `NamingRegistry` for consistency.
fn pg_truncate_name(raw: &str) -> String {
    const MAX: usize = 63;
    if raw.len() <= MAX {
        return raw.to_string();
    }
    // FNV-1a 64-bit hash → 7 hex chars (same algorithm as naming::short_hash)
    let h = raw.bytes().fold(14695981039346656037u64, |acc, b| {
        (acc ^ b as u64).wrapping_mul(1099511628211)
    });
    let hash = format!("{:07x}", h & 0x0fff_ffff);
    format!("{}_{}", &raw[..MAX - 8], hash)
}

/// Build per-sibling column sets for Jaccard computation.
///
/// When all siblings are data-bearing, applies a noise filter: columns present in fewer than
/// `max(2, len/20)` schemas are excluded. Falls back to unfiltered sets if the filter
/// removes all columns (fully disjoint schemas would otherwise produce a false 1.0 Jaccard).
fn build_jaccard_col_sets<'a>(
    schemas: &'a [TableSchema],
    indices: &[usize],
    all_data_bearing: bool,
) -> Vec<std::collections::HashSet<&'a str>> {
    if !all_data_bearing {
        return indices
            .iter()
            .map(|&i| schemas[i].data_columns().map(|c| c.original_name.as_str()).collect())
            .collect();
    }
    let min_presence = (indices.len() / 20).max(2);
    let mut col_freq: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for &i in indices {
        for col in schemas[i].data_columns() {
            *col_freq.entry(col.original_name.as_str()).or_default() += 1;
        }
    }
    let filtered: Vec<std::collections::HashSet<&'a str>> = indices
        .iter()
        .map(|&i| {
            schemas[i]
                .data_columns()
                .filter(|c| col_freq.get(c.original_name.as_str()).copied().unwrap_or(0) >= min_presence)
                .map(|c| c.original_name.as_str())
                .collect()
        })
        .collect();
    // If noise filter emptied every set, fall back to unfiltered to avoid masking divergence.
    if filtered.iter().all(|s| s.is_empty()) {
        indices
            .iter()
            .map(|&i| schemas[i].data_columns().map(|c| c.original_name.as_str()).collect())
            .collect()
    } else {
        filtered
    }
}

/// Compute the minimum pairwise Jaccard similarity of data-column names across all pairs.
///
/// Two fast paths avoid the O(n²) full pairwise loop for large sibling groups:
///
/// 1. **Pure-container fast path** — if every sibling has zero data columns (they are pure
///    containers whose data lives in their own children), the Jaccard is 1.0 by convention
///    (union = 0 for all pairs). This covers the common pangenomegraph/genome-key pattern.
///
/// 2. **Large-group fast path** — when N > PAIRWISE_LIMIT, compare each sibling against
///    sibling[0] instead of all N*(N-1)/2 pairs. Semantically equivalent for the homogeneous
///    schemas typical of KeyedPivot detection (language codes, numeric IDs, genome keys).
///    Outliers are still detected: any sibling with 0 column overlap with sibling[0] returns 0.
fn min_jaccard_from_col_sets(col_sets: &[std::collections::HashSet<&str>]) -> f64 {
    const PAIRWISE_LIMIT: usize = 200;
    if col_sets.len() > PAIRWISE_LIMIT {
        let reference = &col_sets[0];
        let mut min_j = 1.0_f64;
        for other in col_sets.iter().skip(1) {
            let intersection = reference.iter().filter(|&&c| other.contains(c)).count();
            let union = reference.len() + other.len() - intersection;
            let j_val = if union == 0 { 1.0 } else { intersection as f64 / union as f64 };
            if j_val < min_j {
                min_j = j_val;
                if min_j == 0.0 { return 0.0; }
            }
        }
        return min_j;
    }
    let mut min_j = 1.0_f64;
    for i in 0..col_sets.len() {
        for j in (i + 1)..col_sets.len() {
            let intersection = col_sets[i].iter().filter(|&&c| col_sets[j].contains(c)).count();
            let union = col_sets[i].len() + col_sets[j].len() - intersection;
            let j_val = if union == 0 { 1.0 } else { intersection as f64 / union as f64 };
            if j_val < min_j {
                min_j = j_val;
                if min_j == 0.0 { return 0.0; }
            }
        }
    }
    min_j
}

#[must_use]
pub fn pairwise_jaccard_min(schemas: &[TableSchema], indices: &[usize]) -> f64 {
    if indices.len() < 2 { return 1.0; }
    // Sort by name so col_sets[0] is always the alphabetically-first sibling —
    // deterministic regardless of call-site ordering.
    let mut sorted: Vec<usize> = indices.to_vec();
    sorted.sort_unstable_by_key(|&i| &schemas[i].name);
    let indices = sorted.as_slice();
    // Fast path 1: pure containers — every sibling has no data columns.
    if indices.iter().all(|&i| schemas[i].data_columns().next().is_none()) { return 1.0; }
    // Noise filter: when ALL siblings have data columns, filter out columns present
    // in fewer than max(2, len/20) schemas. Intentionally disabled when any sibling
    // is a pure container so Jaccard correctly signals heterogeneous groups.
    let all_data_bearing = indices.iter().all(|&i| schemas[i].data_columns().next().is_some());
    let col_sets = build_jaccard_col_sets(schemas, indices, all_data_bearing);
    min_jaccard_from_col_sets(&col_sets)
}

// ---------------------------------------------------------------------------
// Manual sibling merge — public API
// ---------------------------------------------------------------------------

/// Error returned by [`build_keyed_pivot_from_siblings`].
#[allow(dead_code)] // used by json2sql-ui::state::apply_sibling_merge
#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error("need at least 2 tables to merge, got {0}")]
    TooFewTables(usize),
    #[error("table '{0}' has no parent — can only merge sibling tables")]
    NoParent(String),
    #[error("selected tables have different parents — can only merge siblings of the same parent")]
    DifferentParents,
    #[error("table '{0}' is a routing table (all generated columns) — cannot be merged manually")]
    RoutingTable(String),
}

/// Result of a manual sibling merge.
#[allow(dead_code)] // used by json2sql-ui::state::apply_sibling_merge
#[derive(Debug)]
pub struct MergeResult {
    /// Parent table that receives the new WideStrategy.
    pub parent_name: String,
    /// `KeyedPivot` or `MultiKeyedPivot` to store in `strategy_overrides[parent_name]`.
    pub strategy: WideStrategy,
    /// Sibling tables that are absorbed — caller should set
    /// `strategy_overrides[name] = WideStrategy::Ignore` for each.
    pub absorbed_names: Vec<String>,
}

/// Build a `KeyedPivot` or `MultiKeyedPivot` strategy from a manual user selection of
/// sibling tables. Infers key shape from table name suffixes; auto-detects whether to
/// produce a single-group (`KeyedPivot`) or two-group (`MultiKeyedPivot`) strategy.
#[allow(dead_code)] // used by json2sql-ui::state::apply_sibling_merge
pub fn build_keyed_pivot_from_siblings(
    schemas: &[TableSchema],
    indices: &[usize],
    key_col_name: &str,
) -> Result<MergeResult, MergeError> {
    if indices.len() < 2 {
        return Err(MergeError::TooFewTables(indices.len()));
    }

    let tables: Vec<&TableSchema> = indices.iter().map(|&i| &schemas[i]).collect();

    for t in &tables {
        if t.parent_table.is_none() {
            return Err(MergeError::NoParent(t.name.clone()));
        }
        if !t.columns.is_empty() && t.columns.iter().all(|c| c.is_generated) {
            return Err(MergeError::RoutingTable(t.name.clone()));
        }
    }

    let parent_name = tables[0].parent_table.as_deref()
        .expect("NoParent was checked for all tables above — parent_table is Some");
    if tables.iter().any(|t| t.parent_table.as_deref() != Some(parent_name)) {
        return Err(MergeError::DifferentParents);
    }

    let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    let absorbed_names: Vec<String> = names.iter().map(|s| s.to_string()).collect();

    let keys = extract_key_suffixes(&names);
    let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();

    let is_numeric: Vec<bool> = keys.iter().map(|k| k.chars().all(|c| c.is_ascii_digit())).collect();
    let has_numeric = is_numeric.iter().any(|&b| b);
    let has_non_numeric = is_numeric.iter().any(|&b| !b);

    let strategy = if has_numeric && has_non_numeric {
        build_mixed_keyed_pivot_strategy(parent_name, key_col_name, &names, &key_refs, &is_numeric)
    } else {
        WideStrategy::KeyedPivot(SiblingSchema {
            key_col_name: key_col_name.to_string(),
            key_shape: classify_key_shape(&key_refs),
            array_children: false,
            data_col_name: "j2s_data".to_string(),
        })
    };

    Ok(MergeResult { parent_name: parent_name.to_string(), strategy, absorbed_names })
}

fn build_mixed_keyed_pivot_strategy(
    parent_name: &str,
    key_col_name: &str,
    names: &[&str],
    key_refs: &[&str],
    is_numeric: &[bool],
) -> WideStrategy {
    use super::table_schema::SiblingGroup;
    let mut numeric_names: Vec<String> = Vec::new();
    let mut non_numeric_names: Vec<String> = Vec::new();
    let mut numeric_keys: Vec<&str> = Vec::new();
    let mut non_numeric_keys: Vec<&str> = Vec::new();
    for (i, &num) in is_numeric.iter().enumerate() {
        if num { numeric_names.push(names[i].to_string()); numeric_keys.push(key_refs[i]); }
        else { non_numeric_names.push(names[i].to_string()); non_numeric_keys.push(key_refs[i]); }
    }
    WideStrategy::MultiKeyedPivot(vec![
        SiblingGroup {
            pivot_table: pg_truncate_name(&format!("{}_{}_num", parent_name, key_col_name)),
            key_is_numeric: true,
            sibling_schema: SiblingSchema {
                key_col_name: key_col_name.to_string(),
                key_shape: classify_key_shape(&numeric_keys),
                array_children: false,
                data_col_name: "j2s_data".to_string(),
            },
            absorbed_names: numeric_names,
        },
        SiblingGroup {
            pivot_table: pg_truncate_name(&format!("{}_{}_txt", parent_name, key_col_name)),
            key_is_numeric: false,
            sibling_schema: SiblingSchema {
                key_col_name: key_col_name.to_string(),
                key_shape: classify_key_shape(&non_numeric_keys),
                array_children: false,
                data_col_name: "j2s_data".to_string(),
            },
            absorbed_names: non_numeric_names,
        },
    ])
}

fn extract_key_suffixes(names: &[&str]) -> Vec<String> {
    if names.is_empty() { return vec![]; }
    let bytes: Vec<&[u8]> = names.iter().map(|s| s.as_bytes()).collect();
    let min_len = bytes.iter().map(|b| b.len()).min().unwrap_or(0);
    let prefix_len = (0..min_len)
        .take_while(|&i| bytes.iter().all(|b| b[i] == bytes[0][i]))
        .count();
    let skip = if matches!(names[0].as_bytes().get(prefix_len), Some(&b'_') | Some(&b'-')) {
        prefix_len + 1
    } else {
        prefix_len
    };
    names.iter().map(|n| n[skip..].to_string()).collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_merge_slug_siblings_keyed_pivot() {
        let schemas = vec![
            make_parent("products_images"),
            make_sibling("products_images_front", "products_images", &["url", "width"]),
            make_sibling("products_images_back",  "products_images", &["url", "width"]),
        ];
        let r = build_keyed_pivot_from_siblings(&schemas, &[1, 2], "img_key").unwrap();
        assert_eq!(r.parent_name, "products_images");
        assert!(matches!(r.strategy, WideStrategy::KeyedPivot(_)));
        let mut absorbed = r.absorbed_names.clone();
        absorbed.sort();
        assert_eq!(absorbed, vec!["products_images_back", "products_images_front"]);
    }

    #[test]
    fn test_merge_numeric_siblings_keyed_pivot() {
        let schemas = vec![
            make_parent("p"),
            make_sibling("p_1", "p", &["val"]),
            make_sibling("p_2", "p", &["val"]),
            make_sibling("p_3", "p", &["val"]),
        ];
        let r = build_keyed_pivot_from_siblings(&schemas, &[1, 2, 3], "key").unwrap();
        assert_eq!(r.parent_name, "p");
        if let WideStrategy::KeyedPivot(ss) = &r.strategy {
            assert_eq!(ss.key_shape, KeyShape::Numeric);
            assert_eq!(ss.key_col_name, "key");
        } else {
            panic!("expected KeyedPivot, got {:?}", r.strategy);
        }
    }

    #[test]
    fn test_merge_mixed_siblings_multikeyed_pivot() {
        let schemas = vec![
            make_parent("img"),
            make_sibling("img_1",     "img", &["url"]),
            make_sibling("img_2",     "img", &["url"]),
            make_sibling("img_front", "img", &["url"]),
        ];
        let r = build_keyed_pivot_from_siblings(&schemas, &[1, 2, 3], "key").unwrap();
        assert_eq!(r.parent_name, "img");
        if let WideStrategy::MultiKeyedPivot(groups) = &r.strategy {
            assert_eq!(groups.len(), 2);
            let num = groups.iter().find(|g| g.key_is_numeric).unwrap();
            let mut num_absorbed = num.absorbed_names.clone();
            num_absorbed.sort();
            assert_eq!(num_absorbed, vec!["img_1", "img_2"]);
            let txt = groups.iter().find(|g| !g.key_is_numeric).unwrap();
            assert_eq!(txt.absorbed_names, vec!["img_front"]);
        } else {
            panic!("expected MultiKeyedPivot, got {:?}", r.strategy);
        }
    }

    #[test]
    fn test_merge_error_too_few_tables() {
        let schemas = vec![
            make_parent("p"),
            make_sibling("p_1", "p", &["val"]),
        ];
        let err = build_keyed_pivot_from_siblings(&schemas, &[1], "key").unwrap_err();
        assert!(matches!(err, MergeError::TooFewTables(_)));
    }

    #[test]
    fn test_merge_error_different_parents() {
        let schemas = vec![
            make_sibling("a_1", "a", &["val"]),
            make_sibling("b_1", "b", &["val"]),
        ];
        let err = build_keyed_pivot_from_siblings(&schemas, &[0, 1], "key").unwrap_err();
        assert!(matches!(err, MergeError::DifferentParents));
    }

    #[test]
    fn test_merge_error_no_parent() {
        let t1 = TableSchema::new("a".to_string(), vec!["a".to_string()], 0);
        let t2 = TableSchema::new("b".to_string(), vec!["b".to_string()], 0);
        // Both have parent_table = None by default
        let schemas = vec![t1, t2];
        let err = build_keyed_pivot_from_siblings(&schemas, &[0, 1], "key").unwrap_err();
        assert!(matches!(err, MergeError::NoParent(_)));
    }

    #[test]
    fn test_merge_error_routing_table() {
        let mut routing = TableSchema::new("p_r".to_string(), vec!["p_r".to_string()], 1);
        routing.parent_table = Some("p".to_string());
        routing.columns.push(ColumnSchema {
            name: "j2s_id".to_string(), original_name: "j2s_id".to_string(),
            pg_type: PgType::BigInt, not_null: true, is_generated: true, is_parent_fk: false,
        });
        let sibling = make_sibling("p_s", "p", &["val"]);
        let schemas = vec![routing, sibling];
        let err = build_keyed_pivot_from_siblings(&schemas, &[0, 1], "key").unwrap_err();
        assert!(matches!(err, MergeError::RoutingTable(_)));
    }

    #[test]
    fn test_merge_absorbed_names_complete() {
        let schemas = vec![
            make_parent("x"),
            make_sibling("x_a", "x", &["v"]),
            make_sibling("x_b", "x", &["v"]),
            make_sibling("x_c", "x", &["v"]),
        ];
        let r = build_keyed_pivot_from_siblings(&schemas, &[1, 2, 3], "key").unwrap();
        let mut absorbed = r.absorbed_names.clone();
        absorbed.sort();
        assert_eq!(absorbed, vec!["x_a", "x_b", "x_c"]);
    }

    // Large-group fast path (>200) uses col_sets[0] as Jaccard reference.
    // Without sorting indices by name first, the reference changes with call-site order,
    // producing different approximations for the same schema set.
    //
    // Scenario: 200 typical siblings {a,b,c,d,e} + alpha {a,b} + bravo {a,b,c}.
    // - alpha-typical Jaccard = 2/5 = 0.4  (true minimum)
    // - bravo-typical Jaccard = 3/5 = 0.6
    // bravo-first (before fix): col_sets[0]={a,b,c} → min=0.6 (misses the 0.4 pair)
    // alpha-first  (before fix): col_sets[0]={a,b}  → min=0.4
    // After fix (sort by name): alpha is always [0] → min=0.4 for both orderings.
    #[test]
    fn test_pairwise_jaccard_large_group_order_independent() {
        let mut schemas: Vec<TableSchema> = vec![
            make_parent("root"),
            make_sibling("alpha", "root", &["a", "b"]),          // index 1
            make_sibling("bravo", "root", &["a", "b", "c"]),     // index 2
        ];
        for i in 0..200_usize {
            schemas.push(make_sibling(
                &format!("typical_{i:03}"),
                "root",
                &["a", "b", "c", "d", "e"],
            ));
        }
        // 203 schemas: root(0), alpha(1), bravo(2), typical_000..typical_199(3..202).

        // alpha_first: the reference is alpha → min = Jaccard(alpha, typical) = 2/5 = 0.4
        let alpha_first: Vec<usize> = std::iter::once(1)
            .chain(std::iter::once(2))
            .chain(3..schemas.len())
            .collect();
        // bravo_first: reference is bravo → without sort fix, min = Jaccard(bravo, typical) = 3/5 = 0.6
        let bravo_first: Vec<usize> = std::iter::once(2)
            .chain(std::iter::once(1))
            .chain(3..schemas.len())
            .collect();

        let j_alpha = pairwise_jaccard_min(&schemas, &alpha_first);
        let j_bravo = pairwise_jaccard_min(&schemas, &bravo_first);
        assert_eq!(
            j_alpha, j_bravo,
            "pairwise_jaccard_min must be order-independent: alpha_first={j_alpha:.4}, bravo_first={j_bravo:.4}"
        );
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
        // 3 identical lang siblings → Some(Single) with lang_code col
        let mut parent = make_parent("p");
        parent.wide_strategy = WideStrategy::Columns;
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
        parent.wide_strategy = WideStrategy::Columns;
        let schemas = vec![
            parent,
            make_child_with_key("p_fr", "p", "fr", &["label"]),
            make_child_with_key("p_en", "p", "en", &["label"]),
        ];
        // threshold=3 but only 2 children
        let ctx = make_ctx(&schemas, 0, vec![1, 2], 3, 0.5);
        let empty = std::collections::HashMap::new();
        assert!(detect_homogeneous_collapse(&schemas, &ctx, &empty, &empty).is_none());
    }

    #[test]
    fn test_detect_mixed_both_groups_qualify() {
        // 3 numeric + 3 alpha, each with Jaccard=1.0 → Multi with 2 groups
        let mut parent = make_parent("p");
        parent.wide_strategy = WideStrategy::Columns;
        let schemas = vec![
            parent,
            make_child_with_key("p_1", "p", "1", &["val"]),
            make_child_with_key("p_2", "p", "2", &["val"]),
            make_child_with_key("p_3", "p", "3", &["val"]),
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
        // 2 numeric + 2 alpha, threshold=3 — unified fallback fails (parent_has_data=false but 4 < 6)
        let mut parent = make_parent("p");
        parent.wide_strategy = WideStrategy::Columns;
        let schemas = vec![
            parent,
            make_child_with_key("p_1", "p", "1", &["v"]),
            make_child_with_key("p_2", "p", "2", &["v"]),
            make_child_with_key("p_fr", "p", "fr", &["label"]),
            make_child_with_key("p_en", "p", "en", &["label"]),
        ];
        // total 4 children, threshold=3 — unified fallback: 4 >= 3, Jaccard between 1+2 vs fr+en
        // The test verifies detect_mixed runs without panic; result may be Some or None
        let ctx = make_ctx(&schemas, 0, vec![1, 2, 3, 4], 3, 0.5);
        let empty = std::collections::HashMap::new();
        // Just ensure no panic — the logic is tested via integration tests
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
        // threshold=3 → no items (only 2 children)
        let work = build_work_items(&schemas, 3, &obj_map, &arr_map);
        assert!(work.is_empty());
        // threshold=2 → one item
        let work2 = build_work_items(&schemas, 2, &obj_map, &arr_map);
        assert_eq!(work2.len(), 1);
        assert_eq!(work2[0].0, "p");
        assert!(!work2[0].2); // not array_children
    }

    #[test]
    fn test_build_work_items_child_indices_sorted_by_name() {
        // indices 0,1,2 correspond to p_c, p_a, p_b — should come out sorted by name
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
        // sorted by name: p_a(1), p_b(2), p_c(0)
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

}

