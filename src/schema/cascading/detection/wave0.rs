//! Wave 0 — détection des groupes de siblings (heuristiques Jaccard, fallback, clustering).
//!
//! Extrait de `detection.rs` (voir `super` pour l'orchestration et les types partagés
//! `Collapse`, `CollapseKind`, `SiblingDetectCtx`, `SubgroupData`).
//!
//! Fonctions :
//! - fn `build_work_items` — construit les items de travail à partir des maps parent/enfants.
//! - fn `build_sibling_ctx` — construit le contexte de détection pour un parent.
//! - fn `should_skip_parent` — filtre les parents non éligibles.
//! - fn `collect_sibling_collapses` — collecte les collapses détectés sur tous les parents éligibles.
//! - fn `detect_homogeneous_collapse` — dispatch pour un groupe de clés homogènes.
//! - fn `detect_mixed_collapse` — dispatch pour un groupe mixte numérique/texte.
//! - fn `filter_significant_siblings` — exclut les tables de routing pures.
//! - fn `filter_routing_tables` — exclut les tables de routing du calcul de Jaccard.
//! - fn `effective_jaccard_for_regular` — calcule le Jaccard effectif hors tables de routing.
//! - fn `try_unified_fallback` — repli quand le Jaccard global est insuffisant (fusion unifiée).
//! - fn `try_cluster_fallback` — repli par clustering glouton.
//! - fn `build_non_num_clusters` — construit les clusters non-numériques pour le repli clustering.
//! - fn `build_synthetic_pivot_collapse` — construit la structure `Collapse` finale (cas Multi).
//! - fn `build_classic_keyed_pivot_collapse` — construit la structure `Collapse` finale (cas Single classique).
//! - fn `assemble_mixed_collapse` — assemble le `Collapse` final pour un groupe mixte.
//! - fn `make_subgroup` — construit un `SubgroupData` (nom, clé, colonnes union) pour un sous-groupe.
//! - fn `pick_unique_suffix` — suffixe de table sans collision.

use super::super::super::wide_strategies::{build_union_columns, classify_key_shape};
use super::super::super::table_schema::{ColumnSchema, KeyShape, TableSchema, InferredStrategy};
use super::super::scoring::{child_compatibility_score, greedy_schema_clusters, pairwise_jaccard_min, siblings_key_prefix, unique_cluster_suffix};
use super::{Collapse, CollapseKind, SiblingDetectCtx, SubgroupData};

/// Run wave 0 of sibling detection — with
/// the child-compatibility gate added. Returns co-sibling groups for cascade waves 1+.
/// Build the sorted work list for `run_sibling_wave`.
/// Each item is `(parent_name, child_indices_sorted_by_name, array_children)`.
/// Child indices are sorted alphabetically so the Jaccard large-group fast path
/// always uses the same reference sibling (deterministic).
pub(super) fn build_work_items(
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


pub(super) fn make_subgroup(
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


pub(super) fn collect_sibling_collapses(
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



#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::super::table_schema::{ColumnSchema, KeyShape, TableSchema, InferredStrategy};
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
}
