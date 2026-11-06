//! `SchemaFinalizer` — orchestrateur des 4 phases (base → cascade, puis wide strategies → guard).
//!
//! - struct `SchemaFinalizer` — transforme les observations brutes en `TableSchema` finalisés.
//! - fn `SchemaFinalizer::new` — construit le finaliseur avec ses seuils.
//! - fn `SchemaFinalizer::run` — phases 1 et 2 (base → cascade) : le schéma avant le split des tables larges.
//! - fn `SchemaFinalizer::split` — phases 3 et 4 (wide strategies → guard), d'après les stats de la Pass 1 (#200).
//! - fn `parents_of_object_children` — tables qui ont au moins un enfant objet.
//! - fn `dedup_and_sort` — après la cascade : retire les noms en double (`WARNING` si deux chemins, #180), rétablit l'ordre profondeur puis chemin.

use indexmap::IndexMap;
use std::collections::HashSet;

use super::super::cascading::finalize_cascading;
use super::super::naming::{ColumnCollision, NamingRegistry};
use super::super::observer::TableEntry;
use super::super::stats::ColumnStats;
use super::super::strategies::StrategyName;
use super::super::table_schema::{ChildKind, TableSchema};
use super::base::build_base_schemas;
use super::wide_strategy::{apply_wide_table_strategies, build_finalizer_config};
use super::{apply_column_limit_guard, exclude_absorbed_children, OverflowWarning};

/// Transforms raw observations (from `SchemaObserver`) into finalized `TableSchema` objects.
///
/// Responsible for: schema construction, wide-table strategies, sibling detection,
/// naming, topological sorting. Stateless with respect to the observation phase.
pub struct SchemaFinalizer {
    pub(crate) wide_column_threshold: usize,
    pub(crate) sibling_threshold: usize,
    pub(crate) sibling_jaccard: f64,
    pub(crate) stable_threshold: f64,
    pub(crate) disabled_strategies: HashSet<StrategyName>,
    /// When `true`, `run()` applies the PG column-limit guard and returns `OverflowWarning`s.
    /// Set to `false` for inspect mode (no guard, raw schema).
    pub(crate) apply_pg_guard: bool,
}

impl SchemaFinalizer {
    #[must_use]
    pub const fn new(
        wide_column_threshold: usize,
        sibling_threshold: usize,
        sibling_jaccard: f64,
        stable_threshold: f64,
        disabled_strategies: HashSet<StrategyName>,
        apply_pg_guard: bool,
    ) -> Self {
        Self {
            wide_column_threshold,
            sibling_threshold,
            sibling_jaccard,
            stable_threshold,
            disabled_strategies,
            apply_pg_guard,
        }
    }

    /// Build the schema from the collected `TableEntry` map, sorted topologically (parents before
    /// children), before the split of the wide tables: `split` does it, from the Pass 1 statistics.
    /// Returns `(schemas, column_collisions)`.
    ///
    /// Ordering is constrained by two structural phases that cannot be interleaved:
    ///
    /// **Phase A — per-table (parallelisable):** Phases 1 and 3 process each table
    /// independently. Phase 1 builds base schemas; Phase 3 applies wide-table strategies
    /// (Pivot, Jsonb, `AutoSplit`…). These are safe to parallelise because each table is
    /// self-contained at this point.
    ///
    /// **Phase B — cross-table (sequential):** Phase 2 runs sibling detection and
    /// cascade fusion (`SiblingCollapse`, `KeyedPivot`). It *must* execute between Phase 1
    /// and Phase 3: fusible tables must be merged into their final form before any
    /// wide-table strategy is applied, otherwise the strategy targets a pre-fusion table
    /// that no longer exists, producing either a missing table or a double-split.
    pub(crate) fn run(
        &self,
        tables: &IndexMap<String, TableEntry>,
        naming: &mut NamingRegistry,
    ) -> (Vec<TableSchema>, Vec<ColumnCollision>) {
        // Pre-register all table names so that table_name_lookup() (read-only) works in parallel.
        for key in tables.keys() {
            naming.table_name_from_dot_key(key);
        }

        let tables_with_object_children = parents_of_object_children(tables);

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

        dedup_and_sort(&mut schemas);
        (schemas, all_collisions)
    }

    /// Phases 3 and 4 on the schemas `run` built: split the wide tables after reading their keys
    /// in `stats` (a snapshot keeps them, #200), then the column limit guard and the exclusion of
    /// the absorbed children. Returns the overflow warnings, empty unless `apply_pg_guard` is `true`.
    pub(crate) fn split(
        &self,
        schemas: &mut Vec<TableSchema>,
        stats: &[ColumnStats],
    ) -> Vec<OverflowWarning> {
        // Phase 3 — per-table: apply wide-table strategies (Pivot, Jsonb, StructuredPivot, AutoSplit)
        // to remaining tables after fusion is complete.
        apply_wide_table_strategies(schemas, stats, &build_finalizer_config(self));

        // Phase 4 — cleanup: column limit guard and absorbed child exclusion.
        let overflow_warnings = if self.apply_pg_guard {
            apply_column_limit_guard(schemas)
        } else {
            Vec::new()
        };
        exclude_absorbed_children(schemas);
        overflow_warnings
    }
}

/// Path keys of the tables that have at least one Object/ObjectArray child.
fn parents_of_object_children(
    tables: &IndexMap<String, TableEntry>,
) -> std::collections::HashSet<String> {
    tables
        .values()
        .filter(|e| {
            matches!(
                e.child_kind,
                Some(ChildKind::Object | ChildKind::ObjectArray)
            )
        })
        .map(|e| e.parent_key.clone())
        .collect()
}

/// After the cascade: drop the synthetic tables whose name collides with an already-registered
/// one (the first, pre-cascade occurrence is kept), then restore the depth-then-path order (#180)
/// that `finalize_cascading` does not maintain. Names come unique from their creation
/// (`unique_derived_table_name`); a duplicate of another path is a bug, reported.
fn dedup_and_sort(schemas: &mut Vec<TableSchema>) {
    let mut seen: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
    schemas.retain(|s| match seen.entry(s.name.clone()) {
        std::collections::hash_map::Entry::Vacant(first) => {
            first.insert(s.path.clone());
            true
        }
        std::collections::hash_map::Entry::Occupied(first) => {
            // Same path: the same table built twice. Another path: a name collision — the routes to the dropped
            // table would write into the kept one (#180).
            if *first.get() != s.path {
                eprintln!(
                    "WARNING: '{}' and '{}' both get the table name '{}': the second is dropped",
                    first.get().join("."),
                    s.path.join("."),
                    s.name
                );
                debug_assert!(false, "table name '{}' used by two paths", s.name);
            }
            false
        }
    });
    schemas.sort_by(|a, b| (a.depth, &a.path, &a.name).cmp(&(b.depth, &b.path, &b.name)));
}
