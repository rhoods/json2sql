//! Phase 4 — garde-fou de limite de colonnes PostgreSQL, exclusion des enfants absorbés.
//!
//! - struct `OverflowWarning` — enregistré quand une table est auto-convertie en JSONB (> 1600 colonnes).
//! - fn `apply_column_limit_guard` — convertit en Jsonb les tables dépassant 1600 colonnes.
//! - fn `collect_surviving_route_targets` — calcule les cibles de routing protégées.
//! - fn `exclude_absorbed_children` — retire les tables absorbées par une stratégie parente
//!   (sauf cibles de routing survivantes).

use super::super::table_schema::{InferredStrategy, TableSchema};
use super::super::wide_strategies::apply_wide_strategy_columns;

/// `PostgreSQL` hard limit on columns per table.
pub const PG_MAX_COLUMNS: usize = 1600;

/// Recorded when a table is auto-converted to JSONB by `apply_column_limit_guard`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OverflowWarning {
    pub table_name: String,
    pub original_column_count: usize,
}

/// Convert any table with more than [`PG_MAX_COLUMNS`] data columns to `InferredStrategy::Jsonb`.
///
/// Only the table's own columns are affected; child tables are preserved in the schema
/// and continue to function normally with their parent FK intact.
///
/// Must be called after `finalize()` (schemas in topological order). Each table is
/// evaluated independently — a child can be converted without its parent being converted.
pub(crate) fn apply_column_limit_guard(schemas: &mut [TableSchema]) -> Vec<OverflowWarning> {
    let mut warnings = Vec::new();
    for schema in schemas.iter_mut() {
        let count = schema.data_columns().count();
        let generated = schema.columns.iter().filter(|c| c.is_generated).count();
        // PostgreSQL enforces a hard limit of PG_MAX_COLUMNS total columns per table,
        // which includes both data and generated columns (j2s_id, j2s_parent_id, j2s_order).
        if count + generated > PG_MAX_COLUMNS {
            warnings.push(OverflowWarning {
                table_name: schema.name.clone(),
                original_column_count: count,
            });
            apply_wide_strategy_columns(schema, InferredStrategy::Jsonb);
        }
    }
    warnings
}

/// (Pivot, Jsonb, `StructuredPivot`, `SiblingCollapse`). `AutoSplit` does NOT absorb children.
///
/// The schemas must be topologically sorted (parents before children) for the single-pass
/// transitive exclusion to work correctly. Safe to call multiple times (idempotent).
fn collect_surviving_route_targets<'a>(
    schemas: &'a [TableSchema],
    absorbers: &std::collections::HashSet<&str>,
    partial_absorbed: &std::collections::HashSet<&'a str>,
) -> std::collections::HashSet<&'a str> {
    // Pass 1: preliminary exclusion WITHOUT route_targets protection.
    let mut preliminary_excluded: std::collections::HashSet<&str> = partial_absorbed.clone();
    for schema in schemas {
        if let Some(ref parent) = schema.parent_table {
            if absorbers.contains(parent.as_str()) || preliminary_excluded.contains(parent.as_str()) {
                preliminary_excluded.insert(schema.name.as_str());
            }
        }
    }
    // Route targets from excluded tables are stale — only surviving tables' routes count.
    schemas.iter()
        .filter(|s| !preliminary_excluded.contains(s.name.as_str()))
        .flat_map(|s| s.child_routes.values().map(std::string::String::as_str))
        .collect()
}

pub fn exclude_absorbed_children(schemas: &mut Vec<TableSchema>) {
    let absorbers: std::collections::HashSet<&str> = schemas
        .iter().filter(|s| s.absorbs_children()).map(|s| s.name.as_str()).collect();
    let partial_absorbed: std::collections::HashSet<&str> = schemas
        .iter().flat_map(|s| s.inferred_strategy.absorbed_names()).collect();
    if absorbers.is_empty() && partial_absorbed.is_empty() { return; }
    let route_targets = collect_surviving_route_targets(schemas, &absorbers, &partial_absorbed);
    // Pass 2: final exclusion, protecting only valid route targets.
    let mut excluded: std::collections::HashSet<String> =
        partial_absorbed.into_iter().map(std::string::ToString::to_string).collect();
    for schema in schemas.iter() {
        if route_targets.contains(schema.name.as_str()) { continue; }
        if let Some(ref parent) = schema.parent_table {
            if absorbers.contains(parent.as_str()) || excluded.contains(parent) {
                excluded.insert(schema.name.clone());
            }
        }
    }
    if !excluded.is_empty() {
        schemas.retain(|s| !excluded.contains(&s.name));
    }
}
