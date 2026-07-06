//! `SchemaFinalizer` — orchestrateur des 4 phases (base → cascade → wide strategies → guard).
//!
//! - struct `SchemaFinalizer` — transforme les observations brutes en `TableSchema` finalisés.
//! - fn `SchemaFinalizer::new` — construit le finaliseur avec ses seuils.
//! - fn `SchemaFinalizer::run` — orchestre les 4 phases (base → cascade → wide strategies → guard).

use std::collections::HashSet;
use indexmap::IndexMap;

use super::super::naming::{ColumnCollision, NamingRegistry};
use super::super::observer::TableEntry;
use super::super::strategies::StrategyName;
use super::super::table_schema::{ChildKind, TableSchema};
use super::super::cascading::finalize_cascading;
use super::base::build_base_schemas;
use super::wide_strategy::{apply_wide_table_strategies, build_finalizer_config};
use super::{OverflowWarning, apply_column_limit_guard, exclude_absorbed_children};

/// Transforms raw observations (from `SchemaObserver`) into finalized `TableSchema` objects.
///
/// Responsible for: schema construction, wide-table strategies, sibling detection,
/// naming, topological sorting. Stateless with respect to the observation phase.
pub struct SchemaFinalizer {
    pub(crate) wide_column_threshold: usize,
    pub(crate) sibling_threshold: usize,
    pub(crate) sibling_jaccard: f64,
    pub(crate) stable_threshold: f64,
    pub(crate) rare_threshold: f64,
    pub(crate) disabled_strategies: HashSet<StrategyName>,
    /// When `true`, `run()` applies the PG column-limit guard and returns `OverflowWarning`s.
    /// Set to `false` for inspect mode (no guard, raw schema).
    pub(crate) apply_pg_guard: bool,
}

impl SchemaFinalizer {
    #[must_use]
    pub fn new(
        wide_column_threshold: usize,
        sibling_threshold: usize,
        sibling_jaccard: f64,
        stable_threshold: f64,
        rare_threshold: f64,
        disabled_strategies: HashSet<StrategyName>,
        apply_pg_guard: bool,
    ) -> Self {
        Self { wide_column_threshold, sibling_threshold, sibling_jaccard, stable_threshold, rare_threshold, disabled_strategies, apply_pg_guard }
    }

    /// Convert all accumulated observations into finalized `TableSchema` objects,
    /// sorted topologically (parents before children).
    /// Returns `(schemas, column_collisions, overflow_warnings)`.
    /// `overflow_warnings` is non-empty only when `apply_pg_guard` is `true`.
    /// Build the final schema from the collected `TableEntry` map.
    ///
    /// Ordering is constrained by two structural phases that cannot be interleaved:
    ///
    /// **Phase A — per-table (parallelisable):** Phases 1 and 3 process each table
    /// independently. Phase 1 builds base schemas; Phase 3 applies wide-table strategies
    /// (Pivot, Jsonb, AutoSplit…). These are safe to parallelise because each table is
    /// self-contained at this point.
    ///
    /// **Phase B — cross-table (sequential):** Phase 2 runs sibling detection and
    /// cascade fusion (SiblingCollapse, KeyedPivot). It *must* execute between Phase 1
    /// and Phase 3: fusible tables must be merged into their final form before any
    /// wide-table strategy is applied, otherwise the strategy targets a pre-fusion table
    /// that no longer exists, producing either a missing table or a double-split.
    pub(crate) fn run(
        &self,
        tables: &IndexMap<String, TableEntry>,
        text_threshold: u32,
        naming: &mut NamingRegistry,
    ) -> (Vec<TableSchema>, Vec<ColumnCollision>, Vec<OverflowWarning>) {
        // Pre-register all table names so that table_name_lookup() (read-only) works in parallel.
        let dot_keys: Vec<String> = tables.keys().cloned().collect();
        for key in &dot_keys {
            naming.table_name_from_dot_key(key);
        }

        // Pre-compute set of path_keys that have at least one Object/ObjectArray child.
        let tables_with_object_children: std::collections::HashSet<String> = tables
            .values()
            .filter(|e| matches!(e.child_kind, Some(ChildKind::Object | ChildKind::ObjectArray)))
            .map(|e| e.parent_key.clone())
            .collect();

        let config = build_finalizer_config(self, text_threshold);

        // Phase 1 — per-table: schema construction WITHOUT wide-table strategies yet.
        // Each table is processed independently and in parallel. Defers InferredStrategy
        // application until after cross-table fusion detection.
        let (mut schemas, all_collisions) =
            build_base_schemas(tables, naming, &tables_with_object_children);

        // Phase 2 — cross-table: sibling detection and fusion strategies (SiblingCollapse, KeyedPivot).
        // Must run before per-table wide strategies so that fusible tables are merged
        // before being split/pivoted individually.
        if !self.disabled_strategies.contains(&StrategyName::Sibling) {
            finalize_cascading(&mut schemas, self.sibling_threshold, self.sibling_jaccard);
        }

        // Post-cascade dedup: cascading can produce synthetic table names that collide
        // with already-registered names. Keeps the first (pre-cascade) occurrence.
        {
            let mut seen = std::collections::HashSet::new();
            schemas.retain(|s| seen.insert(s.name.clone()));
        }

        // Re-sort after cascade: finalize_cascading appends synthetic tables without
        // maintaining topological (depth-then-name) order.
        schemas.sort_by(|a, b| a.depth.cmp(&b.depth).then_with(|| a.name.cmp(&b.name)));

        // Phase 3 — per-table: apply wide-table strategies (Pivot, Jsonb, StructuredPivot, AutoSplit)
        // to remaining tables after fusion is complete.
        apply_wide_table_strategies(&mut schemas, tables, naming, &config, &tables_with_object_children);

        // Phase 4 — cleanup: column limit guard and absorbed child exclusion.
        let overflow_warnings = if self.apply_pg_guard {
            apply_column_limit_guard(&mut schemas)
        } else {
            Vec::new()
        };
        exclude_absorbed_children(&mut schemas);

        (schemas, all_collisions, overflow_warnings)
    }
}
