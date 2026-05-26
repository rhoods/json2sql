use super::wide_strategies::{build_union_columns, classify_key_shape};
use super::table_schema::{ChildKind, ColumnSchema, KeyShape, SiblingSchema, TableSchema, WideStrategy};
use super::type_tracker::PgType;

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
                Some(ChildKind::ObjectArray) => {
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
fn run_sibling_wave(
    schemas: &mut Vec<TableSchema>,
    threshold: usize,
    min_jaccard: f64,
) -> Vec<CoSiblingGroup> {
    let (parent_to_object_children, parent_to_array_children) = build_parent_child_maps(schemas);

    // ── Local collapse types (same as archived function) ─────────────────────
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
        // Indices of all siblings absorbed by this collapse (for co-sibling collection).
        absorbed_indices: Vec<usize>,
    }

    let mut collapses: Vec<Collapse> = Vec::new();
    let mut co_siblings: Vec<CoSiblingGroup> = Vec::new();

    let name_to_idx: std::collections::HashMap<String, usize> = schemas
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.clone(), i))
        .collect();

    // Wave 0: all parents with enough children.
    // Each work item carries (parent_name, child_indices, array_children).
    // Sort child_indices by schema name so sibling[0] (used as Jaccard reference
    // in the large-group fast path) is always the alphabetically first child → deterministic.
    let mut work: Vec<(String, Vec<usize>, bool)> = Vec::new();
    for (parent_map, array_children) in [
        (&parent_to_object_children, false),
        (&parent_to_array_children, true),
    ] {
        for (parent_name, child_indices) in parent_map {
            if child_indices.len() >= threshold {
                let mut sorted = child_indices.clone();
                sorted.sort_by_key(|&i| &schemas[i].name);
                work.push((parent_name.clone(), sorted, array_children));
            }
        }
    }
    work.sort_by(|a, b| a.0.cmp(&b.0));

    // Helper: build one SubgroupData from a slice of child indices.
    let make_subgroup = |parent_name: &str, indices: &[usize], key_is_numeric: bool, suffix: &str| -> SubgroupData {
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
    };

    for (parent_name, child_indices, array_children) in &work {

        if child_indices.len() < threshold {
            continue;
        }

        let parent_idx = match name_to_idx.get(parent_name.as_str()) {
            Some(&i) => i,
            None => continue,
        };

        // Skip parents managed by the post-pass (KeyedPivot / MultiKeyedPivot).
        if matches!(
            schemas[parent_idx].wide_strategy,
            WideStrategy::KeyedPivot(_) | WideStrategy::MultiKeyedPivot(_)
        ) {
            continue;
        }
        // Only Columns and AutoSplit parents produce dynamic-key children worth evaluating.
        if !matches!(
            schemas[parent_idx].wide_strategy,
            WideStrategy::Columns | WideStrategy::AutoSplit { .. }
        ) {
            continue;
        }
        // For AutoSplit parents, filter out the companion _wide table (WideStrategy::Pivot)
        // — it is a synthetic artifact, not a dynamic-key sibling.
        let wide_filtered: Vec<usize>;
        let child_indices: &Vec<usize> =
            if matches!(schemas[parent_idx].wide_strategy, WideStrategy::AutoSplit { .. }) {
                wide_filtered = child_indices
                    .iter()
                    .copied()
                    .filter(|&i| !matches!(schemas[i].wide_strategy, WideStrategy::Pivot))
                    .collect();
                if wide_filtered.len() < threshold {
                    continue;
                }
                &wide_filtered
            } else {
                child_indices
            };

        let parent_has_data = schemas[parent_idx].data_columns().next().is_some();

        let (numeric_idx, non_numeric_idx): (Vec<usize>, Vec<usize>) =
            child_indices.iter().partition(|&&i| {
                schemas[i]
                    .path
                    .last()
                    .map(|k| !k.is_empty() && k.chars().all(|c| c.is_ascii_digit()))
                    .unwrap_or(false)
            });

        let is_mixed = !numeric_idx.is_empty() && !non_numeric_idx.is_empty();

        if is_mixed {
            let non_num_regular: Vec<usize> = non_numeric_idx
                .iter()
                .copied()
                .filter(|&i| {
                    let name = &schemas[i].name;
                    let is_pure = schemas[i].data_columns().next().is_none();
                    let child_count =
                        parent_to_object_children.get(name).map_or(0, |v| v.len())
                            + parent_to_array_children.get(name).map_or(0, |v| v.len());
                    !(is_pure && child_count >= threshold)
                })
                .collect();

            let num_ok = numeric_idx.len() >= threshold
                && pairwise_jaccard_min(schemas, &numeric_idx) >= min_jaccard;
            let non_ok = non_num_regular.len() >= threshold
                && pairwise_jaccard_min(schemas, &non_num_regular) >= min_jaccard;

            if !num_ok && !non_ok {
                // Unified fallback (T3).
                if parent_has_data {
                    continue;
                }
                let all_mixed_len = child_indices.len();
                if all_mixed_len < threshold {
                    continue;
                }
                let unified_jaccard = {
                    let data_bearing: Vec<usize> = child_indices
                        .iter()
                        .copied()
                        .filter(|&i| schemas[i].data_columns().next().is_some())
                        .collect();
                    if data_bearing.len() >= threshold {
                        pairwise_jaccard_min(schemas, &data_bearing)
                    } else {
                        pairwise_jaccard_min(schemas, child_indices)
                    }
                };
                if unified_jaccard < min_jaccard {
                    continue;
                }
                // Child-compat gate (T3 fallback).
                if child_compatibility_score(schemas, child_indices, &parent_to_object_children, &parent_to_array_children) < min_jaccard {
                    continue;
                }
                let keys: Vec<String> = child_indices
                    .iter()
                    .map(|&i| schemas[i].path.last().cloned().unwrap_or_default())
                    .collect();
                let key_shape = classify_key_shape(&keys.iter().map(|s| s.as_str()).collect::<Vec<_>>());
                let children: Vec<&TableSchema> =
                    child_indices.iter().map(|&i| &schemas[i]).collect();
                let union_cols = build_union_columns(&children);
                let kind_label = if *array_children { "ObjectArray" } else { "Object" };
                let log_msg = format!(
                    "  Unified-fallback KeyedPivot {}: {} ({} tables → 1, Jaccard {:.2})",
                    kind_label, parent_name, all_mixed_len, unified_jaccard,
                );
                collapses.push(Collapse {
                    parent_idx,
                    array_children: *array_children,
                    log_msg,
                    kind: CollapseKind::Single {
                        key_col_name: "key".to_string(),
                        key_shape,
                        union_cols,
                        data_col_name: "j2s_data".to_string(),
                    },
                    absorbed_indices: child_indices.clone(),
                });
                continue;
            }

            // Apply child-compat gate to each qualifying subgroup.
            let num_ok = num_ok && child_compatibility_score(schemas, &numeric_idx, &parent_to_object_children, &parent_to_array_children) >= min_jaccard;
            let non_ok = non_ok && child_compatibility_score(schemas, &non_num_regular, &parent_to_object_children, &parent_to_array_children) >= min_jaccard;

            // ── Schema-based clustering for non-numeric group when Jaccard fails ──
            let non_num_clusters: Vec<Vec<usize>> = if !non_ok && !non_num_regular.is_empty() {
                greedy_schema_clusters(schemas, &non_num_regular, min_jaccard, threshold)
                    .into_iter()
                    .filter(|c| pairwise_jaccard_min(schemas, c) >= min_jaccard)
                    .collect()
            } else {
                Vec::new()
            };

            if !num_ok && !non_ok && non_num_clusters.is_empty() {
                continue;
            }

            let mut groups = Vec::new();
            let mut all_absorbed: Vec<usize> = Vec::new();
            if num_ok {
                all_absorbed.extend_from_slice(&numeric_idx);
                groups.push(make_subgroup(&parent_name, &numeric_idx, true, "num"));
            }
            if non_ok {
                all_absorbed.extend_from_slice(&non_num_regular);
                groups.push(make_subgroup(&parent_name, &non_num_regular, false, "key"));
            }
            for (i, cluster) in non_num_clusters.iter().enumerate() {
                let prefix = siblings_key_prefix(schemas, cluster);
                let desired = if prefix.is_empty() {
                    format!("cluster_{}", i)
                } else {
                    format!("{}_key", prefix)
                };
                let suffix = unique_cluster_suffix(&parent_name, &desired, schemas);
                all_absorbed.extend_from_slice(cluster);
                groups.push(make_subgroup(&parent_name, cluster, false, &suffix));
            }

            let kind_label = if *array_children { "ObjectArray" } else { "Object" };
            let log_msg = format!(
                "  MultiKeyedPivot {} tables detected: {} ({} tables → {} pivot tables)",
                kind_label, parent_name, child_indices.len(), groups.len(),
            );
            collapses.push(Collapse {
                parent_idx,
                array_children: *array_children,
                log_msg,
                kind: CollapseKind::Multi { groups },
                absorbed_indices: all_absorbed,
            });
        } else {
            // Homogeneous key shape.
            let all_pure =
                child_indices.iter().all(|&i| schemas[i].data_columns().next().is_none());
            let regular: Vec<usize> = if all_pure {
                child_indices.to_vec()
            } else {
                child_indices
                    .iter()
                    .copied()
                    .filter(|&i| {
                        let name = &schemas[i].name;
                        let is_pure = schemas[i].data_columns().next().is_none();
                        let child_count =
                            parent_to_object_children.get(name).map_or(0, |v| v.len())
                                + parent_to_array_children.get(name).map_or(0, |v| v.len());
                        !(is_pure && child_count >= threshold)
                    })
                    .collect()
            };

            if regular.len() < threshold {
                continue;
            }

            let actual_jaccard = {
                let data_bearing: Vec<usize> = regular
                    .iter()
                    .copied()
                    .filter(|&i| schemas[i].data_columns().next().is_some())
                    .collect();
                if data_bearing.len() >= threshold {
                    pairwise_jaccard_min(schemas, &data_bearing)
                } else {
                    pairwise_jaccard_min(schemas, &regular)
                }
            };
            if actual_jaccard < min_jaccard {
                // ── Schema-based greedy clustering fallback ─────────────────
                // When the global Jaccard fails, try to find homogeneous sub-groups
                // (e.g. front_* vs ingredients_* in an images dict).
                let has_sig = regular.len() < child_indices.len();
                let is_autosplit = matches!(
                    schemas[parent_idx].wide_strategy,
                    WideStrategy::AutoSplit { .. }
                );
                if (!parent_has_data && !has_sig) || is_autosplit {
                    let raw_clusters =
                        greedy_schema_clusters(schemas, &regular, min_jaccard, threshold);
                    let valid_clusters: Vec<Vec<usize>> = raw_clusters
                        .into_iter()
                        .filter(|c| pairwise_jaccard_min(schemas, c) >= min_jaccard)
                        .collect();
                    if valid_clusters.len() >= 2 {
                        let mut groups = Vec::new();
                        let mut all_absorbed = Vec::new();
                        for cluster in &valid_clusters {
                            let prefix = siblings_key_prefix(schemas, cluster);
                            let desired = if prefix.is_empty() {
                                format!("cluster_{}", groups.len())
                            } else {
                                format!("{}_key", prefix)
                            };
                            let suffix = unique_cluster_suffix(&parent_name, &desired, schemas);
                            all_absorbed.extend_from_slice(cluster);
                            groups.push(make_subgroup(&parent_name, cluster, false, &suffix));
                        }
                        collapses.push(Collapse {
                            parent_idx,
                            array_children: *array_children,
                            log_msg: format!(
                                "  Schema-cluster MultiKeyedPivot: {} ({} tables → {} clusters)",
                                parent_name,
                                regular.len(),
                                groups.len()
                            ),
                            kind: CollapseKind::Multi { groups },
                            absorbed_indices: all_absorbed,
                        });
                    }
                }
                continue;
            }

            // ── Child-compatibility gate ────────────────────────────────────
            if child_compatibility_score(schemas, &regular, &parent_to_object_children, &parent_to_array_children) < min_jaccard {
                continue;
            }

            let has_sig_containers = regular.len() < child_indices.len();

            if parent_has_data || has_sig_containers {
                let key_is_numeric = !numeric_idx.is_empty();
                if parent_has_data && !key_is_numeric {
                    continue;
                }
                let suffix = if key_is_numeric { "num" } else { "key" };
                let groups = vec![make_subgroup(&parent_name, &regular, key_is_numeric, suffix)];
                let log_msg = format!(
                    "  Synthetic pivot for parent with data/sig-containers: {} ({} tables → 1)",
                    parent_name, regular.len(),
                );
                collapses.push(Collapse {
                    parent_idx,
                    array_children: *array_children,
                    log_msg,
                    kind: CollapseKind::Multi { groups },
                    absorbed_indices: regular.clone(),
                });
            } else {
                // Classic KeyedPivot: pure container parent becomes the pivot.
                let keys: Vec<String> = regular
                    .iter()
                    .map(|&i| schemas[i].path.last().cloned().unwrap_or_default())
                    .collect();
                let key_shape =
                    classify_key_shape(&keys.iter().map(|s| s.as_str()).collect::<Vec<_>>());
                let key_col_name = match &key_shape {
                    KeyShape::Numeric => "key_id".to_string(),
                    KeyShape::IsoLang => "lang_code".to_string(),
                    _ => "key".to_string(),
                };
                let children: Vec<&TableSchema> =
                    regular.iter().map(|&i| &schemas[i]).collect();
                let union_cols = build_union_columns(&children);
                let key_examples = keys.iter().take(5).map(|s| s.as_str()).collect::<Vec<_>>().join("\", \"");
                let more = if keys.len() > 5 { format!("\" (+{} more)", keys.len() - 5) } else { "\"".to_string() };
                let kind_label = if *array_children { "ObjectArray" } else { "Object" };
                let log_msg = format!(
                    "  Sibling {} tables detected: {} ({} tables → 1)\n  Keys: \"{}{}\n  Jaccard min: {:.2} → strategy: KeyedPivot (col: {} {})",
                    kind_label, parent_name, regular.len(), key_examples, more,
                    min_jaccard, key_col_name, key_shape,
                );
                collapses.push(Collapse {
                    parent_idx,
                    array_children: *array_children,
                    log_msg,
                    kind: CollapseKind::Single {
                        key_col_name,
                        key_shape,
                        union_cols,
                        data_col_name: "j2s_data".to_string(),
                    },
                    absorbed_indices: regular.clone(),
                });
            }
        }
    }

    // ── Apply collapses ───────────────────────────────────────────────────────
    let mut new_schemas: Vec<TableSchema> = Vec::new();

    for collapse in &collapses {
        eprintln!("{}", collapse.log_msg);

        match &collapse.kind {
            CollapseKind::Single { key_col_name, key_shape, union_cols, data_col_name } => {
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
                parent.columns.push(ColumnSchema {
                    name: key_col_name.clone(),
                    original_name: key_col_name.clone(),
                    pg_type: PgType::Text,
                    not_null: true,
                    is_generated: false,
                    is_parent_fk: false,
                });
                for col in union_cols {
                    parent.columns.push(col.clone());
                }
                parent.columns.push(ColumnSchema {
                    name: data_col_name.clone(),
                    original_name: data_col_name.clone(),
                    pg_type: PgType::Jsonb,
                    not_null: false,
                    is_generated: true,
                    is_parent_fk: false,
                });
                parent.wide_strategy = WideStrategy::KeyedPivot(sibling_schema);

                // Co-sibling collection: collect children of absorbed siblings grouped by JSON key.
                let synthetic_parent_name = schemas[collapse.parent_idx].name.clone();
                let children_by_key = collect_children_by_key(
                    schemas,
                    &collapse.absorbed_indices,
                    &parent_to_object_children,
                    &parent_to_array_children,
                );
                for (json_key, siblings, arr) in children_by_key {
                    if siblings.len() >= 2 {
                        co_siblings.push(CoSiblingGroup {
                            synthetic_parent_name: synthetic_parent_name.clone(),
                            json_key,
                            sibling_indices: siblings,
                            array_children: arr,
                        });
                    }
                }
            }

            CollapseKind::Multi { groups } => {
                let sibling_groups: Vec<crate::schema::table_schema::SiblingGroup> = groups
                    .iter()
                    .map(|g| crate::schema::table_schema::SiblingGroup {
                        pivot_table: g.pivot_table_name.clone(),
                        key_is_numeric: g.key_is_numeric,
                        sibling_schema: SiblingSchema {
                            key_col_name: g.key_col_name.clone(),
                            key_shape: g.key_shape.clone(),
                            array_children: collapse.array_children,
                            data_col_name: "j2s_data".to_string(),
                        },
                        absorbed_names: g.absorbed_names.clone(),
                    })
                    .collect();

                schemas[collapse.parent_idx].wide_strategy =
                    WideStrategy::MultiKeyedPivot(sibling_groups);

                let parent_name = schemas[collapse.parent_idx].name.clone();
                let parent_path = schemas[collapse.parent_idx].path.clone();
                let parent_depth = schemas[collapse.parent_idx].depth;

                for g in groups {
                    let fk_col = format!("j2s_{}_id", parent_name);
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
                    if collapse.array_children {
                        cols.push(ColumnSchema::generated("j2s_order", PgType::BigInt));
                    }
                    cols.push(ColumnSchema {
                        name: g.key_col_name.clone(),
                        original_name: g.key_col_name.clone(),
                        pg_type: PgType::Text,
                        not_null: true,
                        is_generated: false,
                        is_parent_fk: false,
                    });
                    for col in &g.union_cols {
                        cols.push(col.clone());
                    }
                    cols.push(ColumnSchema {
                        name: "j2s_data".to_string(),
                        original_name: "j2s_data".to_string(),
                        pg_type: PgType::Jsonb,
                        not_null: false,
                        is_generated: true,
                        is_parent_fk: false,
                    });

                    let mut path = parent_path.clone();
                    path.push(if g.key_is_numeric { "num" } else { "key" }.to_string());

                    let sibling_schema = SiblingSchema {
                        key_col_name: g.key_col_name.clone(),
                        key_shape: g.key_shape.clone(),
                        array_children: collapse.array_children,
                        data_col_name: "j2s_data".to_string(),
                    };
                    let pivot_name = g.pivot_table_name.clone();

                    // Co-sibling collection: children of absorbed siblings → next cascade.
                    let absorbed_idx: Vec<usize> = g.absorbed_names.iter()
                        .filter_map(|n| name_to_idx.get(n.as_str()).copied())
                        .collect();
                    let children_by_key = collect_children_by_key(
                        schemas,
                        &absorbed_idx,
                        &parent_to_object_children,
                        &parent_to_array_children,
                    );
                    for (json_key, siblings, arr) in children_by_key {
                        if siblings.len() >= 2 {
                            co_siblings.push(CoSiblingGroup {
                                synthetic_parent_name: pivot_name.clone(),
                                json_key,
                                sibling_indices: siblings,
                                array_children: arr,
                            });
                        }
                    }

                    new_schemas.push(TableSchema {
                        name: pivot_name,
                        path,
                        parent_table: Some(parent_name.clone()),
                        depth: parent_depth + 1,
                        columns: cols,
                        child_kind: Some(ChildKind::Object),
                        wide_strategy: WideStrategy::KeyedPivot(sibling_schema),
                        flatten_sources: std::collections::HashMap::new(),
                        child_routes: std::collections::HashMap::new(),
                    });
                }
            }
        }
    }

    schemas.append(&mut new_schemas);
    co_siblings
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
        // Single occurrence: re-parent to synthetic pivot and register in child_routes.
        if let Some(&idx) = group.sibling_indices.first() {
            let child_name = schemas[idx].name.clone();
            // Re-parent
            schemas[idx].parent_table = Some(group.synthetic_parent_name.clone());
            // Register in parent's child_routes
            let parent_idx = schemas
                .iter()
                .position(|s| s.name == group.synthetic_parent_name);
            if let Some(pi) = parent_idx {
                schemas[pi].child_routes.insert(group.json_key, child_name);
            }
        }
        return Vec::new();
    }

    let (obj_map, arr_map) = build_parent_child_maps(schemas);

    // Check Jaccard similarity between the co-siblings.
    let jaccard = pairwise_jaccard_min(schemas, &group.sibling_indices);
    // Check child compatibility of the co-siblings.
    let compat = child_compatibility_score(schemas, &group.sibling_indices, &obj_map, &arr_map);

    if jaccard >= min_jaccard && compat >= min_jaccard {
        // ── Merge co-siblings into a new synthetic table T ───────────────────
        let parent_idx_opt = schemas
            .iter()
            .position(|s| s.name == group.synthetic_parent_name);
        let parent_idx = match parent_idx_opt {
            Some(i) => i,
            None => return Vec::new(),
        };

        // Build the union of co-sibling columns.
        let children_refs: Vec<&TableSchema> =
            group.sibling_indices.iter().map(|&i| &schemas[i]).collect();
        let union_cols = build_union_columns(&children_refs);

        // Derive a stable name for T from the synthetic parent + json_key, truncated to 63 chars.
        let t_name = pg_truncate_name(&format!("{}_{}", group.synthetic_parent_name, group.json_key));

        let parent_depth = schemas[parent_idx].depth;
        let parent_path = schemas[parent_idx].path.clone();
        let parent_name = schemas[parent_idx].name.clone();
        let fk_col = format!("j2s_{}_id", parent_name);

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
        for col in union_cols {
            cols.push(col);
        }

        let mut t_path = parent_path;
        t_path.push(group.json_key.clone());

        let t_schema = TableSchema {
            name: t_name.clone(),
            path: t_path,
            parent_table: Some(parent_name.clone()),
            depth: parent_depth + 1,
            columns: cols,
            child_kind: Some(if group.array_children {
                ChildKind::ObjectArray
            } else {
                ChildKind::Object
            }),
            wide_strategy: WideStrategy::Columns,
            flatten_sources: std::collections::HashMap::new(),
            child_routes: std::collections::HashMap::new(),
        };

        // Register T in synthetic parent S's child_routes.
        schemas[parent_idx]
            .child_routes
            .insert(group.json_key.clone(), t_name.clone());

        schemas.push(t_schema);

        // Collect grandchildren of the co-siblings for the next cascade wave.
        let grandchildren = collect_children_by_key(schemas, &group.sibling_indices, &obj_map, &arr_map);
        let mut next: Vec<CoSiblingGroup> = Vec::new();
        for (json_key, siblings, arr) in grandchildren {
            if siblings.len() >= 2 {
                next.push(CoSiblingGroup {
                    synthetic_parent_name: t_name.clone(),
                    json_key,
                    sibling_indices: siblings,
                    array_children: arr,
                });
            } else if let Some(&sole_idx) = siblings.first() {
                // Sole occurrence: re-parent to T.
                let child_name = schemas[sole_idx].name.clone();
                schemas[sole_idx].parent_table = Some(t_name.clone());
                let t_pos = schemas.iter().position(|s| s.name == t_name).unwrap();
                schemas[t_pos].child_routes.insert(json_key, child_name);
            }
        }
        next
    } else {
        // ── Re-parent each sibling individually to the synthetic parent ───────
        let parent_idx_opt = schemas
            .iter()
            .position(|s| s.name == group.synthetic_parent_name);
        let parent_idx = match parent_idx_opt {
            Some(i) => i,
            None => return Vec::new(),
        };

        for &i in &group.sibling_indices {
            let child_name = schemas[i].name.clone();
            let child_key = schemas[i].path.last().cloned().unwrap_or_else(|| group.json_key.clone());
            schemas[i].parent_table = Some(group.synthetic_parent_name.clone());
            schemas[parent_idx]
                .child_routes
                .insert(child_key, child_name);
        }
        Vec::new()
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
/// parent's `child_routes` to point to the sub-pivot.  Their own children are returned as
/// `CoSiblingGroup`s for an additional cascade wave.
fn run_keyed_pivot_children_wave(
    schemas: &mut Vec<TableSchema>,
    threshold: usize,
    min_jaccard: f64,
) -> Vec<CoSiblingGroup> {
    let (obj_map, arr_map) = build_parent_child_maps(schemas);

    // Collect (parent_idx, sorted_child_indices) for KeyedPivot parents with enough Columns children.
    let work: Vec<(usize, Vec<usize>)> = schemas
        .iter()
        .enumerate()
        .filter_map(|(parent_idx, s)| {
            if !matches!(s.wide_strategy, WideStrategy::KeyedPivot(_)) {
                return None;
            }
            // Only consider children explicitly registered in child_routes — this excludes
            // the original absorbed siblings (which are still in `schemas` at this point but
            // will be removed by exclude_absorbed_children) from diluting the Jaccard score.
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
            if children.len() < threshold {
                return None;
            }
            children.sort_unstable_by_key(|&i| &schemas[i].name);
            Some((parent_idx, children))
        })
        .collect();

    let mut new_schemas: Vec<TableSchema> = Vec::new();
    let mut co_siblings: Vec<CoSiblingGroup> = Vec::new();

    for (parent_idx, child_indices) in work {
        let jaccard = pairwise_jaccard_min(schemas, &child_indices);
        if jaccard < min_jaccard {
            continue;
        }

        let parent_name = schemas[parent_idx].name.clone();
        let parent_path = schemas[parent_idx].path.clone();
        let parent_depth = schemas[parent_idx].depth;

        let keys: Vec<String> = child_indices
            .iter()
            .map(|&i| schemas[i].path.last().cloned().unwrap_or_default())
            .collect();
        let key_shape =
            classify_key_shape(&keys.iter().map(|s| s.as_str()).collect::<Vec<_>>());
        let key_col_name = match &key_shape {
            KeyShape::Numeric => "key_id".to_string(),
            KeyShape::IsoLang => "lang_code".to_string(),
            _ => "key".to_string(),
        };

        let children_refs: Vec<&TableSchema> =
            child_indices.iter().map(|&i| &schemas[i]).collect();
        let union_cols = build_union_columns(&children_refs);

        let suffix = unique_cluster_suffix(&parent_name, "key", schemas);
        let sub_pivot_name = format!("{}_{}", parent_name, suffix);

        let fk_col = format!("j2s_{}_id", parent_name);
        let sibling_schema = SiblingSchema {
            key_col_name: key_col_name.clone(),
            key_shape,
            array_children: false,
            data_col_name: "j2s_data".to_string(),
        };

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
            ColumnSchema {
                name: key_col_name.clone(),
                original_name: key_col_name,
                pg_type: PgType::Text,
                not_null: true,
                is_generated: false,
                is_parent_fk: false,
            },
        ];
        for col in &union_cols {
            cols.push(col.clone());
        }
        cols.push(ColumnSchema {
            name: "j2s_data".to_string(),
            original_name: "j2s_data".to_string(),
            pg_type: PgType::Jsonb,
            not_null: false,
            is_generated: true,
            is_parent_fk: false,
        });

        let mut sub_path = parent_path;
        sub_path.push("key".to_string());

        // Collect co-siblings from absorbed children for the next cascade wave.
        let children_by_key =
            collect_children_by_key(schemas, &child_indices, &obj_map, &arr_map);
        for (json_key, siblings, arr) in children_by_key {
            if siblings.len() >= 2 {
                co_siblings.push(CoSiblingGroup {
                    synthetic_parent_name: sub_pivot_name.clone(),
                    json_key,
                    sibling_indices: siblings,
                    array_children: arr,
                });
            } else if let Some(&sole_idx) = siblings.first() {
                // Single child: re-parent immediately.
                schemas[sole_idx].parent_table = Some(sub_pivot_name.clone());
            }
        }

        new_schemas.push(TableSchema {
            name: sub_pivot_name.clone(),
            path: sub_path,
            parent_table: Some(parent_name.clone()),
            depth: parent_depth + 1,
            columns: cols,
            child_kind: Some(ChildKind::Object),
            wide_strategy: WideStrategy::KeyedPivot(sibling_schema),
            flatten_sources: std::collections::HashMap::new(),
            child_routes: std::collections::HashMap::new(),
        });

        // Re-parent absorbed children and update parent's child_routes.
        let absorbed_names: std::collections::HashSet<String> =
            child_indices.iter().map(|&i| schemas[i].name.clone()).collect();
        for &i in &child_indices {
            schemas[i].parent_table = Some(sub_pivot_name.clone());
        }
        for val in schemas[parent_idx].child_routes.values_mut() {
            if absorbed_names.contains(val.as_str()) {
                *val = sub_pivot_name.clone();
            }
        }

        eprintln!(
            "  KeyedPivot post-pass: {} ({} orphan tables → sub-pivot {})",
            parent_name,
            child_indices.len(),
            sub_pivot_name
        );
    }

    schemas.append(&mut new_schemas);
    co_siblings
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
pub fn pairwise_jaccard_min(schemas: &[TableSchema], indices: &[usize]) -> f64 {
    if indices.len() < 2 {
        return 1.0;
    }

    // Sort by name so col_sets[0] is always the alphabetically-first sibling —
    // deterministic regardless of call-site ordering.
    let mut sorted: Vec<usize> = indices.to_vec();
    sorted.sort_unstable_by_key(|&i| &schemas[i].name);
    let indices = sorted.as_slice();

    // Fast path 1: pure containers — every sibling has no data columns.
    // Check before allocating col_sets to skip HashSet construction entirely.
    if indices.iter().all(|&i| schemas[i].data_columns().next().is_none()) {
        return 1.0;
    }

    // Noise filter: when ALL siblings have data columns, filter out columns present
    // in fewer than max(2, len/20) schemas. These are data-quality artefacts (e.g.
    // a handful of records with extra fields from a different data pattern) whose
    // presence in a minority of schemas would drag pairwise Jaccard near zero and
    // block an otherwise valid sibling group from being detected.
    //
    // The filter is intentionally disabled when ANY sibling is a pure container
    // (0 data cols): in that case the Jaccard correctly returns 0, signalling that
    // the group is heterogeneous. T1 in finalize_siblings then handles exclusion
    // of significant containers; the Jaccard must not mask that signal.
    let all_data_bearing = indices.iter().all(|&i| schemas[i].data_columns().next().is_some());

    let col_sets: Vec<std::collections::HashSet<&str>> = if all_data_bearing {
        let min_presence = (indices.len() / 20).max(2);
        let mut col_freq: std::collections::HashMap<&str, usize> =
            std::collections::HashMap::new();
        for &i in indices {
            for col in schemas[i].data_columns() {
                *col_freq.entry(col.original_name.as_str()).or_default() += 1;
            }
        }
        let filtered: Vec<std::collections::HashSet<&str>> = indices
            .iter()
            .map(|&i| {
                schemas[i]
                    .data_columns()
                    .filter(|c| {
                        col_freq
                            .get(c.original_name.as_str())
                            .copied()
                            .unwrap_or(0)
                            >= min_presence
                    })
                    .map(|c| c.original_name.as_str())
                    .collect()
            })
            .collect();
        // If the noise filter removed *all* columns from every sibling (schemas with fully
        // disjoint column sets each having fewer occurrences than min_presence), the filtered
        // sets are empty and union = 0 → Jaccard = 1.0, masking the true incompatibility.
        // Fall back to unfiltered sets so the Jaccard correctly reflects the divergence.
        if filtered.iter().all(|s| s.is_empty()) {
            indices
                .iter()
                .map(|&i| schemas[i].data_columns().map(|c| c.original_name.as_str()).collect())
                .collect()
        } else {
            filtered
        }
    } else {
        // Build one HashSet per sibling — O(n·m).
        indices
            .iter()
            .map(|&i| schemas[i].data_columns().map(|c| c.original_name.as_str()).collect())
            .collect()
    };

    // Fast path 2: large groups — compare each sibling against sibling[0] in O(n·m).
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
                if min_j == 0.0 {
                    return 0.0;
                }
            }
        }
        return min_j;
    }

    // Full pairwise for small groups — exact result.
    let mut min_j = 1.0_f64;
    for i in 0..col_sets.len() {
        for j in (i + 1)..col_sets.len() {
            let intersection = col_sets[i].iter().filter(|&&c| col_sets[j].contains(c)).count();
            let union = col_sets[i].len() + col_sets[j].len() - intersection;
            let j_val = if union == 0 { 1.0 } else { intersection as f64 / union as f64 };
            if j_val < min_j {
                min_j = j_val;
                if min_j == 0.0 {
                    return 0.0; // Early exit — can't get lower
                }
            }
        }
    }
    min_j
}

// ---------------------------------------------------------------------------
// Manual sibling merge — public API
// ---------------------------------------------------------------------------

/// Error returned by [`build_keyed_pivot_from_siblings`].
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
pub fn build_keyed_pivot_from_siblings(
    schemas: &[TableSchema],
    indices: &[usize],
    key_col_name: &str,
) -> Result<MergeResult, MergeError> {
    use super::table_schema::SiblingGroup;

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

    let parent_name = tables[0].parent_table.as_deref().unwrap();
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
        let mut numeric_names: Vec<String> = Vec::new();
        let mut non_numeric_names: Vec<String> = Vec::new();
        let mut numeric_keys: Vec<&str> = Vec::new();
        let mut non_numeric_keys: Vec<&str> = Vec::new();
        for (i, &num) in is_numeric.iter().enumerate() {
            if num {
                numeric_names.push(names[i].to_string());
                numeric_keys.push(key_refs[i]);
            } else {
                non_numeric_names.push(names[i].to_string());
                non_numeric_keys.push(key_refs[i]);
            }
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
}

