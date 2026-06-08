//! Jaccard similarity scoring et clustering glouton pour la détection de siblings.
#![allow(clippy::cast_precision_loss)]
//!
//! Toutes les fonctions de ce module opèrent exclusivement sur `&[TableSchema]`
//! et des indices — pas d'effets de bord, pas d'état mutable.

use super::super::table_schema::TableSchema;

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
        let obj_ch: &[usize] = parent_to_obj.get(name).map_or(&[], Vec::as_slice);
        let arr_ch: &[usize] = parent_to_arr.get(name).map_or(&[], Vec::as_slice);
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

/// Greedy schema clustering: partition `indices` into groups where every member
/// has Jaccard ≥ `min_jaccard` against the cluster seed (first unassigned sibling).
/// Only clusters with at least `min_size` members are returned.
/// Indices are sorted by table name before processing for determinism.
#[allow(clippy::too_many_lines)] // greedy O(n²) clustering algorithm, self-contained loop
pub(super) fn greedy_schema_clusters(
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
pub(super) fn siblings_key_prefix(schemas: &[TableSchema], indices: &[usize]) -> String {
    let keys: Vec<&[u8]> = indices
        .iter()
        .filter_map(|&i| schemas[i].path.last())
        .map(std::string::String::as_bytes)
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
pub(super) fn unique_cluster_suffix(
    parent_name: &str,
    desired_suffix: &str,
    schemas: &[TableSchema],
) -> String {
    let taken = |suffix: &str| schemas.iter().any(|s| s.name == format!("{parent_name}_{suffix}"));
    if !taken(desired_suffix) {
        return desired_suffix.to_string();
    }
    let mut n = 2usize;
    loop {
        let try_suffix = format!("{desired_suffix}_{n}");
        if !taken(&try_suffix) {
            return try_suffix;
        }
        n += 1;
    }
}

/// Truncate a raw pivot table name to fit `PostgreSQL`'s 63-byte identifier limit.
/// Uses a hash suffix strategy (FNV-1a, 7 hex chars) — intentionally different from
/// `NamingRegistry::truncate_table_name` which strips leading path segments first.
/// Pivot names are synthetic (not hierarchical paths), so segment-stripping is not applicable.
pub(super) fn pg_truncate_name(raw: &str) -> String {
    const MAX: usize = 63;
    if raw.len() <= MAX {
        return raw.to_string();
    }
    // FNV-1a 64-bit hash → 7 hex chars (same algorithm as naming::short_hash)
    let h = raw.bytes().fold(14_695_981_039_346_656_037_u64, |acc, b| {
        (acc ^ u64::from(b)).wrapping_mul(1_099_511_628_211)
    });
    let hash = format!("{:07x}", h & 0x0fff_ffff);
    format!("{}_{}", &raw[..MAX - 8], hash)
}

/// Build per-sibling column sets for Jaccard computation.
///
/// When all siblings are data-bearing, applies a noise filter: columns present in fewer than
/// `max(2, len/20)` schemas are excluded. Falls back to unfiltered sets if the filter
/// removes all columns (fully disjoint schemas would otherwise produce a false 1.0 Jaccard).
/// Apply a frequency-based noise filter to column sets: columns present in fewer than
/// `max(2, len/20)` siblings are excluded. Falls back to unfiltered if the filter
/// would empty every set (avoids masking genuine divergence).
fn noise_filtered_col_sets<'a>(
    schemas: &'a [TableSchema],
    indices: &[usize],
) -> Vec<std::collections::HashSet<&'a str>> {
    let min_presence = (indices.len() / 20).max(2);
    let mut col_freq: std::collections::HashMap<&'a str, usize> = std::collections::HashMap::new();
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
    if filtered.iter().all(std::collections::HashSet::is_empty) {
        indices
            .iter()
            .map(|&i| schemas[i].data_columns().map(|c| c.original_name.as_str()).collect())
            .collect()
    } else {
        filtered
    }
}

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
    noise_filtered_col_sets(schemas, indices)
}

/// Compute the minimum pairwise Jaccard similarity of data-column names across all pairs.
///
/// Two fast paths avoid the O(n²) full pairwise loop for large sibling groups:
///
/// 1. **Pure-container fast path** — if every sibling has zero data columns (they are pure
///    containers whose data lives in their own children), the Jaccard is 1.0 by convention
///    (union = 0 for all pairs). This covers the common pangenomegraph/genome-key pattern.
///
/// 2. **Large-group fast path** — when N > `PAIRWISE_LIMIT`, compare each sibling against
///    sibling[0] instead of all N*(N-1)/2 pairs. Semantically equivalent for the homogeneous
///    schemas typical of `SiblingCollapse` detection (language codes, numeric IDs, genome keys).
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

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::table_schema::{ColumnSchema, TableSchema};
    use super::super::super::type_tracker::PgType;

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
            TableSchema::new("root".to_string(), vec!["root".to_string()], 0),
            make_sibling("alpha", "root", &["a", "b"]),
            make_sibling("bravo", "root", &["a", "b", "c"]),
        ];
        for i in 0..200_usize {
            schemas.push(make_sibling(
                &format!("typical_{i:03}"),
                "root",
                &["a", "b", "c", "d", "e"],
            ));
        }
        let alpha_first: Vec<usize> = std::iter::once(1)
            .chain(std::iter::once(2))
            .chain(3..schemas.len())
            .collect();
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
