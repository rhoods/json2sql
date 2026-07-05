//! Phase 3 — décision de la stratégie wide-table, par table.
//!
//! - fn `apply_wide_table_strategies` — applique la stratégie choisie à chaque table éligible.
//! - fn `apply_wide_strategy` — décide Columns/`StructuredPivot`/Pivot/Jsonb/`AutoSplit` selon le
//!   ratio de colonnes stables.
//! - fn `apply_non_autosplit_strategy` — branche `StructuredPivot` ou Pivot/Jsonb.
//! - fn `apply_autosplit_strategy` — construit la table EAV compagnon `_wide`.
//! - fn `build_wide_pivot_schema` — construit la table EAV compagnon `_wide`.
//! - fn `collect_medium_keys` — sélectionne les clés de fréquence moyenne.
//! - fn `infer_medium_value_type` — détermine leur type de valeur commun.
//! - fn `build_finalizer_config` — assemble la config figée passée à ces fonctions.
//! - struct `FinalizerConfig` — config figée (seuils + stratégies désactivées) pour cette phase.

use indexmap::IndexMap;

use super::super::naming::NamingRegistry;
use super::super::observer::TableEntry;
use super::super::strategies::StrategyName;
use super::super::suffix_detector::detect_suffix_schema;
use super::super::table_schema::{ChildKind, ColumnSchema, TableSchema, InferredStrategy};
use super::super::type_tracker::{widen_pg_types, PgType};
use super::super::wide_strategies::{apply_structured_pivot_columns, apply_wide_strategy_columns, suggest_wide_strategy};
use super::SchemaFinalizer;

/// Phase 3: apply per-table wide strategies (Pivot, Jsonb, StructuredPivot, AutoSplit)
/// to all remaining tables after fusion is complete.
pub(super) fn apply_wide_table_strategies(
    schemas: &mut Vec<TableSchema>,
    tables: &IndexMap<String, TableEntry>,
    naming: &NamingRegistry,
    config: &FinalizerConfig,
    tables_with_object_children: &std::collections::HashSet<String>,
) {
    let mut extra_schemas: Vec<TableSchema> = Vec::new();
    let schema_map: std::collections::HashMap<String, usize> =
        schemas.iter().enumerate().map(|(i, s)| (s.name.clone(), i)).collect();

    for (path_key, entry) in tables {
        let pg_name = naming.table_name_lookup_from_dot_key(path_key);
        if let Some(&idx) = schema_map.get(&pg_name) {
            if let Some(extra) = apply_wide_strategy(&mut schemas[idx], entry, config, tables_with_object_children) {
                extra_schemas.push(extra);
            }
        }
    }
    schemas.extend(extra_schemas);
}

pub(super) fn build_finalizer_config(finalizer: &SchemaFinalizer, text_threshold: u32) -> FinalizerConfig {
    FinalizerConfig {
        wide_column_threshold: finalizer.wide_column_threshold,
        stable_threshold: finalizer.stable_threshold,
        rare_threshold: finalizer.rare_threshold,
        text_threshold,
        disable_pivot: finalizer.disabled_strategies.contains(&StrategyName::Pivot),
        disable_structured_pivot: finalizer.disabled_strategies.contains(&StrategyName::StructuredPivot),
    }
}

pub(super) struct FinalizerConfig {
    wide_column_threshold: usize,
    stable_threshold: f64,
    rare_threshold: f64,
    text_threshold: u32,
    disable_pivot: bool,
    disable_structured_pivot: bool,
}

/// Apply a wide-table strategy to `schema` if the column count exceeds the threshold.
///
/// Only eligible for direct Object children (not ObjectArray/ScalarArray).
/// Returns a companion `_wide` table if the `AutoSplit` strategy is chosen.
fn apply_wide_strategy(
    schema: &mut TableSchema,
    entry: &TableEntry,
    config: &FinalizerConfig,
    tables_with_object_children: &std::collections::HashSet<String>,
) -> Option<TableSchema> {
    let is_wide_eligible = matches!(entry.child_kind, Some(ChildKind::Object) | None);
    let data_col_count = schema.data_columns().count();

    if !is_wide_eligible || data_col_count <= config.wide_column_threshold {
        return None;
    }

    let row_count = entry.row_count.max(1) as f64;
    let stable_count = entry
        .columns
        .values()
        .filter(|t| !t.is_object_field() && !t.is_array_field())
        .filter(|t| t.total_count as f64 / row_count >= config.stable_threshold)
        .count();
    let ratio_stable = stable_count as f64 / data_col_count as f64;

    let is_root = entry.parent_key.is_empty();
    let has_object_children = tables_with_object_children.contains(&entry.path_key);

    if ratio_stable > WIDE_TABLE_HIGH_STABLE_RATIO && entry.row_count >= 10 {
        eprintln!(
            "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: Columns \
            (high stable ratio — legitimate schema, not key explosion)",
            schema.name, data_col_count, ratio_stable * 100.0
        );
        return None;
    }
    if is_root && has_object_children {
        return Some(apply_autosplit_strategy(schema, entry, config, row_count, data_col_count, ratio_stable));
    }

    apply_non_autosplit_strategy(schema, entry, config, data_col_count, ratio_stable);
    None
}

fn apply_non_autosplit_strategy(
    schema: &mut TableSchema,
    entry: &TableEntry,
    config: &FinalizerConfig,
    data_col_count: usize,
    ratio_stable: f64,
) {
    let suffix_schema = if config.disable_structured_pivot {
        None
    } else {
        detect_suffix_schema(&entry.columns, SUFFIX_MIN_COVERAGE, config.text_threshold)
    };
    if let Some(suffix_schema) = suffix_schema {
        eprintln!(
            "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: StructuredPivot ({} suffixes)",
            schema.name, data_col_count, ratio_stable * 100.0, suffix_schema.suffix_cols.len()
        );
        apply_structured_pivot_columns(schema, suffix_schema);
    } else {
        let strategy = if config.disable_pivot { InferredStrategy::Jsonb } else { suggest_wide_strategy(entry) };
        eprintln!(
            "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: {:?}",
            schema.name, data_col_count, ratio_stable * 100.0, strategy
        );
        apply_wide_strategy_columns(schema, strategy);
    }
}

/// Apply the P5 `AutoSplit` strategy: retain stable columns on the main table,
/// build a companion `_wide` EAV table for medium-frequency keys.
fn collect_medium_keys(
    entry: &TableEntry,
    row_count: f64,
    rare_threshold: f64,
    stable_threshold: f64,
) -> std::collections::HashSet<String> {
    entry.columns.iter()
        .filter(|(_, t)| !t.is_object_field() && !t.is_array_field())
        .filter(|(_, t)| {
            let freq = t.total_count as f64 / row_count;
            freq >= rare_threshold && freq < stable_threshold
        })
        .map(|(k, _)| k.clone())
        .collect()
}

fn infer_medium_value_type(
    entry: &TableEntry,
    medium_keys: &std::collections::HashSet<String>,
) -> PgType {
    medium_keys.iter()
        .filter_map(|k| entry.columns.get(k))
        .fold(None::<PgType>, |acc, t| {
            Some(acc.map_or_else(|| t.to_pg_type(), |a| widen_pg_types(a, &t.to_pg_type())))
        })
        .unwrap_or(PgType::Text)
}

fn apply_autosplit_strategy(
    schema: &mut TableSchema,
    entry: &TableEntry,
    config: &FinalizerConfig,
    row_count: f64,
    data_col_count: usize,
    ratio_stable: f64,
) -> TableSchema {
    let medium_keys = collect_medium_keys(entry, row_count, config.rare_threshold, config.stable_threshold);
    schema.columns.retain(|c| {
        c.is_generated || entry.columns.get(&c.original_name)
            .is_some_and(|t| t.total_count as f64 / row_count >= config.stable_threshold)
    });
    let stable_col_count = schema.data_columns().count();
    let rare_count = data_col_count.saturating_sub(stable_col_count).saturating_sub(medium_keys.len());
    // Strip any existing `_wide` suffix to avoid `foo_wide_wide`; fall back to `_eav` on collision.
    let base_name = schema.name.strip_suffix("_wide").unwrap_or(&schema.name);
    let wide_candidate = format!("{base_name}_wide");
    let wide_name = if wide_candidate == schema.name { format!("{base_name}_eav") } else { wide_candidate };
    eprintln!(
        "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: AutoSplit \
        ({} stable cols, {} medium → {}, {} rare dropped)",
        schema.name, data_col_count, ratio_stable * 100.0,
        stable_col_count, medium_keys.len(), wide_name, rare_count,
    );
    let value_type = infer_medium_value_type(entry, &medium_keys);
    build_wide_pivot_schema(schema, wide_name, value_type, medium_keys, config)
}

fn build_wide_pivot_schema(
    schema: &mut TableSchema,
    wide_name: String,
    value_type: PgType,
    medium_keys: std::collections::HashSet<String>,
    config: &FinalizerConfig,
) -> TableSchema {
    let mut wide_schema = TableSchema::new(wide_name.clone(), vec![wide_name.clone()], schema.depth + 1);
    wide_schema.parent_table = Some(schema.name.clone());
    wide_schema.child_kind = Some(ChildKind::Object);
    wide_schema.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
    wide_schema.columns.push(ColumnSchema::parent_fk(&schema.name));
    wide_schema.columns.push(ColumnSchema {
        name: "key".to_string(), original_name: "key".to_string(),
        pg_type: PgType::Text, not_null: true, is_generated: false, is_parent_fk: false,
    });
    wide_schema.columns.push(ColumnSchema {
        name: "value".to_string(), original_name: "value".to_string(),
        pg_type: value_type, not_null: false, is_generated: false, is_parent_fk: false,
    });
    wide_schema.inferred_strategy = InferredStrategy::Pivot;
    schema.inferred_strategy = InferredStrategy::AutoSplit {
        stable_threshold: config.stable_threshold,
        rare_threshold: config.rare_threshold,
        medium_keys,
        wide_table_name: wide_name,
    };
    wide_schema
}

/// A wide table with this fraction or more of stable columns is kept as-is (Columns strategy).
/// Below this, the table is split or pivoted.
const WIDE_TABLE_HIGH_STABLE_RATIO: f64 = 0.5;

/// Minimum fraction of columns that must share a common suffix pattern to trigger `StructuredPivot`.
const SUFFIX_MIN_COVERAGE: f64 = 0.3;
