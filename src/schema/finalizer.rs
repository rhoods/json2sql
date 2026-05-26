use indexmap::IndexMap;
use rayon::prelude::*;

use super::naming::{ColumnCollision, ColumnNameRegistry, NamingRegistry};
use super::observer::TableEntry;
use super::suffix_detector::detect_suffix_schema;
use super::table_schema::{ChildKind, ColumnSchema, TableSchema, WideStrategy};
use super::type_tracker::{widen_pg_types, PgType};
use super::cascading::finalize_cascading;
use super::wide_strategies::{apply_structured_pivot_columns, apply_wide_strategy_columns, suggest_wide_strategy};

/// Transforms raw observations (from SchemaObserver) into finalized TableSchema objects.
///
/// Responsible for: schema construction, wide-table strategies, sibling detection,
/// naming, topological sorting. Stateless with respect to the observation phase.
pub struct SchemaFinalizer {
    pub(crate) wide_column_threshold: usize,
    pub(crate) sibling_threshold: usize,
    pub(crate) sibling_jaccard: f64,
    pub(crate) stable_threshold: f64,
    pub(crate) rare_threshold: f64,
}

impl SchemaFinalizer {
    pub fn new(
        wide_column_threshold: usize,
        sibling_threshold: usize,
        sibling_jaccard: f64,
        stable_threshold: f64,
        rare_threshold: f64,
    ) -> Self {
        Self { wide_column_threshold, sibling_threshold, sibling_jaccard, stable_threshold, rare_threshold }
    }

    /// Convert all accumulated observations into finalized `TableSchema` objects,
    /// sorted topologically (parents before children).
    /// Returns `(schemas, column_collisions)`.
    pub(crate) fn run(
        &self,
        tables: &IndexMap<String, TableEntry>,
        text_threshold: u32,
        naming: &mut NamingRegistry,
    ) -> (Vec<TableSchema>, Vec<ColumnCollision>) {
        // Pre-register all table names so that table_name_lookup() (read-only) works in parallel.
        let dot_keys: Vec<String> = tables.keys().cloned().collect();
        for key in &dot_keys {
            naming.table_name_from_dot_key(key);
        }

        // Pre-compute set of path_keys that have at least one Object/ObjectArray child.
        let tables_with_object_children: std::collections::HashSet<String> = tables
            .values()
            .filter(|e| matches!(e.child_kind, Some(ChildKind::Object) | Some(ChildKind::ObjectArray)))
            .map(|e| e.parent_key.clone())
            .collect();

        // Build schemas in parallel — each entry is independent after pre-registration.
        let entries: Vec<&TableEntry> = tables.values().collect();
        let results: Vec<(TableSchema, Option<TableSchema>, Vec<ColumnCollision>)> = entries
            .par_iter()
            .map(|entry| {
                build_entry_schema(
                    entry,
                    naming,
                    &tables_with_object_children,
                    self.wide_column_threshold,
                    self.stable_threshold,
                    self.rare_threshold,
                    text_threshold,
                )
            })
            .collect();

        let mut schemas: Vec<TableSchema> = Vec::with_capacity(results.len());
        let mut extra_schemas: Vec<TableSchema> = Vec::new();
        let mut all_collisions: Vec<ColumnCollision> = Vec::new();
        for (schema, extra, collisions) in results {
            schemas.push(schema);
            if let Some(e) = extra {
                extra_schemas.push(e);
            }
            all_collisions.extend(collisions);
        }

        schemas.extend(extra_schemas);
        // Secondary sort by name within each depth level ensures schema ordering
        // is fully deterministic regardless of par_iter() task scheduling order.
        schemas.sort_by(|a, b| a.depth.cmp(&b.depth).then_with(|| a.name.cmp(&b.name)));

        {
            let mut seen = std::collections::HashSet::new();
            schemas.retain(|s| seen.insert(s.name.clone()));
        }

        finalize_cascading(&mut schemas, self.sibling_threshold, self.sibling_jaccard);
        exclude_absorbed_children(&mut schemas);
        // Re-sort after cascade: finalize_cascading appends synthetic tables at the end of the
        // Vec without inserting them at their correct depth position. A second sort restores
        // topological (depth-then-name) order so the UI tree and pass2 flush order are correct.
        schemas.sort_by(|a, b| a.depth.cmp(&b.depth).then_with(|| a.name.cmp(&b.name)));

        (schemas, all_collisions)
    }
}

/// Build the `TableSchema` for a single `TableEntry`.
///
/// Pure function — no access to `SchemaRegistry` state. Called in parallel via rayon.
fn build_entry_schema(
    entry: &TableEntry,
    naming: &NamingRegistry,
    tables_with_object_children: &std::collections::HashSet<String>,
    wide_column_threshold: usize,
    stable_threshold: f64,
    rare_threshold: f64,
    text_threshold: u32,
) -> (TableSchema, Option<TableSchema>, Vec<ColumnCollision>) {
    let pg_name = naming.table_name_lookup_from_dot_key(&entry.path_key);
    let depth = entry.path.len().saturating_sub(1);
    let parent_table: Option<String> = if entry.parent_key.is_empty() {
        None
    } else {
        Some(naming.table_name_lookup_from_dot_key(&entry.parent_key))
    };

    let mut schema = TableSchema::new(pg_name.clone(), entry.path.clone(), depth);
    schema.parent_table = parent_table;
    schema.child_kind = entry.child_kind.clone();

    // Generated columns first
    schema.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
    if let Some(ref p) = schema.parent_table {
        schema.columns.push(ColumnSchema::parent_fk(p));
    }
    if schema.has_order_column() {
        schema.columns.push(ColumnSchema::generated("j2s_order", PgType::BigInt));
    }

    let mut extra_schema: Option<TableSchema> = None;
    let mut local_collisions: Vec<ColumnCollision> = Vec::new();

    // Junction tables have a single `value` column
    if schema.is_junction() {
        if let Some(ref tracker) = entry.scalar_tracker {
            let pg_type = tracker.to_pg_type();
            schema.columns.push(ColumnSchema {
                name: "value".to_string(),
                original_name: "value".to_string(),
                pg_type,
                not_null: tracker.is_not_null(),
                is_generated: false,
                is_parent_fk: false,
            });
        }
    } else {
        let row_count = entry.row_count.max(1) as f64;

        // Build per-table column name registry to detect and resolve collisions
        let mut col_registry = ColumnNameRegistry::new();
        for (original_field, tracker) in &entry.columns {
            if !tracker.is_object_field() && !tracker.is_array_field() {
                col_registry.register(original_field);
            }
        }
        for original_field in entry.array_columns.keys() {
            col_registry.register(original_field);
        }
        col_registry.build(&pg_name);
        local_collisions.extend_from_slice(col_registry.collisions());

        // Regular data columns
        for (original_field, tracker) in &entry.columns {
            if tracker.is_object_field() || tracker.is_array_field() {
                continue;
            }
            let col_name = col_registry.resolve(original_field);
            schema.columns.push(ColumnSchema {
                name: col_name,
                original_name: original_field.clone(),
                pg_type: tracker.to_pg_type(),
                not_null: tracker.is_not_null(),
                is_generated: false,
                is_parent_fk: false,
            });
        }

        // Array-as-column fields (array_as_pg_array mode)
        for (original_field, elem_tracker) in &entry.array_columns {
            let elem_type = elem_tracker.to_pg_type();
            let col_name = col_registry.resolve(original_field);
            schema.columns.push(ColumnSchema {
                name: col_name,
                original_name: original_field.clone(),
                pg_type: PgType::Array(Box::new(elem_type)),
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
        }

        // Apply wide strategy if data column count exceeds threshold.
        // Only eligible for direct Object children (not ObjectArray/ScalarArray)
        // whose keys are dynamic and variable.
        let is_wide_eligible = matches!(entry.child_kind, Some(ChildKind::Object) | None);
        let data_col_count = schema.data_columns().count();
        if is_wide_eligible && data_col_count > wide_column_threshold {
            let is_root = entry.parent_key.is_empty();
            let has_object_children = tables_with_object_children.contains(&entry.path_key);

            // Compute ratio of "stable" keys (present in >= stable_threshold of rows).
            let stable_count = entry
                .columns
                .values()
                .filter(|t| !t.is_object_field() && !t.is_array_field())
                .filter(|t| t.total_count as f64 / row_count >= stable_threshold)
                .count();
            let ratio_stable = stable_count as f64 / data_col_count as f64;

            if ratio_stable > WIDE_TABLE_HIGH_STABLE_RATIO && entry.row_count >= 10 {
                eprintln!(
                    "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: Columns \
                    (high stable ratio — legitimate schema, not key explosion)",
                    schema.name, data_col_count, ratio_stable * 100.0
                );
            } else if is_root && has_object_children {
                // P5: classify keys by frequency.
                let medium_keys: std::collections::HashSet<String> = entry
                    .columns
                    .iter()
                    .filter(|(_, t)| !t.is_object_field() && !t.is_array_field())
                    .filter(|(_, t)| {
                        let freq = t.total_count as f64 / row_count;
                        freq >= rare_threshold && freq < stable_threshold
                    })
                    .map(|(k, _)| k.clone())
                    .collect();

                // Drop medium and rare columns from main schema.
                schema.columns.retain(|c| {
                    if c.is_generated {
                        return true;
                    }
                    entry
                        .columns
                        .get(&c.original_name)
                        .map(|t| t.total_count as f64 / row_count >= stable_threshold)
                        .unwrap_or(false)
                });

                let stable_col_count = schema.data_columns().count();
                let rare_count = data_col_count
                    .saturating_sub(stable_col_count)
                    .saturating_sub(medium_keys.len());
                // Build the companion table name. Strip any existing `_wide` suffix first
                // to avoid `foo_wide_wide`. If the result still collides with the main
                // table name (e.g. main table is itself named `foo_wide`), fall back to `_eav`.
                let base_name = schema.name.strip_suffix("_wide").unwrap_or(&schema.name);
                let wide_candidate = format!("{}_wide", base_name);
                let wide_name = if wide_candidate == schema.name {
                    format!("{}_eav", base_name)
                } else {
                    wide_candidate
                };

                eprintln!(
                    "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: AutoSplit \
                    ({} stable cols, {} medium → {}, {} rare dropped)",
                    schema.name, data_col_count, ratio_stable * 100.0,
                    stable_col_count, medium_keys.len(), wide_name, rare_count,
                );

                // Compute widened value type from medium keys for the _wide table.
                let value_type = medium_keys
                    .iter()
                    .filter_map(|k| entry.columns.get(k))
                    .fold(None::<PgType>, |acc, t| {
                        Some(match acc {
                            None => t.to_pg_type(),
                            Some(a) => widen_pg_types(a, &t.to_pg_type()),
                        })
                    })
                    .unwrap_or(PgType::Text);

                // Build the synthetic _wide companion table (EAV Pivot, child of main).
                let mut wide_schema = TableSchema::new(
                    wide_name.clone(),
                    vec![wide_name.clone()],
                    depth + 1,
                );
                wide_schema.parent_table = Some(schema.name.clone());
                wide_schema.child_kind = Some(ChildKind::Object);
                wide_schema.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
                wide_schema.columns.push(ColumnSchema::parent_fk(&schema.name));
                wide_schema.columns.push(ColumnSchema {
                    name: "key".to_string(),
                    original_name: "key".to_string(),
                    pg_type: PgType::Text,
                    not_null: true,
                    is_generated: false,
                    is_parent_fk: false,
                });
                wide_schema.columns.push(ColumnSchema {
                    name: "value".to_string(),
                    original_name: "value".to_string(),
                    pg_type: value_type,
                    not_null: false,
                    is_generated: false,
                    is_parent_fk: false,
                });
                wide_schema.wide_strategy = WideStrategy::Pivot;
                extra_schema = Some(wide_schema);

                schema.wide_strategy = WideStrategy::AutoSplit {
                    stable_threshold,
                    rare_threshold,
                    medium_keys,
                    wide_table_name: wide_name,
                };
            } else {
                if let Some(suffix_schema) =
                    detect_suffix_schema(&entry.columns, SUFFIX_MIN_COVERAGE, text_threshold)
                {
                    eprintln!(
                        "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: StructuredPivot ({} suffixes)",
                        schema.name, data_col_count, ratio_stable * 100.0, suffix_schema.suffix_cols.len()
                    );
                    apply_structured_pivot_columns(&mut schema, suffix_schema);
                } else {
                    let strategy = suggest_wide_strategy(entry);
                    eprintln!(
                        "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: {:?}",
                        schema.name, data_col_count, ratio_stable * 100.0, strategy
                    );
                    apply_wide_strategy_columns(&mut schema, strategy);
                }
            }
        }
    }

    (schema, extra_schema, local_collisions)
}

/// PostgreSQL hard limit on columns per table.
pub const PG_MAX_COLUMNS: usize = 1600;

/// A wide table with this fraction or more of stable columns is kept as-is (Columns strategy).
/// Below this, the table is split or pivoted.
const WIDE_TABLE_HIGH_STABLE_RATIO: f64 = 0.5;

/// Minimum fraction of columns that must share a common suffix pattern to trigger StructuredPivot.
const SUFFIX_MIN_COVERAGE: f64 = 0.3;

/// Fraction of keys that must be numeric (or ISO-language codes) to classify a key shape as
/// KeyShape::Numeric / KeyShape::IsoLang rather than Slug or Mixed.
/// Recorded when a table is auto-converted to JSONB by `apply_column_limit_guard`.
#[derive(Debug, Clone)]
pub struct OverflowWarning {
    pub table_name: String,
    pub original_column_count: usize,
}

/// Convert any table with more than [`PG_MAX_COLUMNS`] data columns to `WideStrategy::Jsonb`.
///
/// Only the table's own columns are affected; child tables are preserved in the schema
/// and continue to function normally with their parent FK intact.
///
/// Must be called after `finalize()` (schemas in topological order). Each table is
/// evaluated independently — a child can be converted without its parent being converted.
pub fn apply_column_limit_guard(schemas: &mut Vec<TableSchema>) -> Vec<OverflowWarning> {
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
            apply_wide_strategy_columns(schema, WideStrategy::Jsonb);
        }
    }
    warnings
}

/// (Pivot, Jsonb, StructuredPivot, KeyedPivot). AutoSplit does NOT absorb children.
///
/// The schemas must be topologically sorted (parents before children) for the single-pass
/// transitive exclusion to work correctly. Safe to call multiple times (idempotent).
pub fn exclude_absorbed_children(schemas: &mut Vec<TableSchema>) {
    // O(n): pre-build set of table names whose wide_strategy absorbs ALL children.
    let absorbers: std::collections::HashSet<&str> = schemas
        .iter()
        .filter(|s| s.wide_strategy.absorbs_children())
        .map(|s| s.name.as_str())
        .collect();

    // MultiKeyedPivot absorbs specific children by name (not all children of the parent).
    let partial_absorbed: std::collections::HashSet<&str> = schemas
        .iter()
        .flat_map(|s| s.wide_strategy.absorbed_names())
        .collect();

    if absorbers.is_empty() && partial_absorbed.is_empty() {
        return;
    }

    // Pass 1: build a preliminary exclusion set WITHOUT route_targets protection,
    // so we know which tables are going away.
    let mut preliminary_excluded: std::collections::HashSet<&str> = partial_absorbed.clone();
    for schema in schemas.iter() {
        if let Some(ref parent) = schema.parent_table {
            if absorbers.contains(parent.as_str()) || preliminary_excluded.contains(parent.as_str()) {
                preliminary_excluded.insert(schema.name.as_str());
            }
        }
    }

    // Route targets that count are only those registered by tables that will SURVIVE.
    // child_routes entries from excluded tables are stale — they point to subtrees that
    // have been superseded by cascade-produced replacements and must not block exclusion.
    let route_targets: std::collections::HashSet<&str> = schemas
        .iter()
        .filter(|s| !preliminary_excluded.contains(s.name.as_str()))
        .flat_map(|s| s.child_routes.values().map(|v| v.as_str()))
        .collect();

    // Pass 2: final exclusion pass, protecting only valid route targets.
    let mut excluded: std::collections::HashSet<String> = partial_absorbed
        .into_iter()
        .map(|s| s.to_string())
        .collect();

    for schema in schemas.iter() {
        if route_targets.contains(schema.name.as_str()) {
            continue; // Protected by a surviving parent's child_routes.
        }
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

