//! Câblage du module `finalizer` — transformation des observations brutes en schémas SQL définitifs.
#![allow(clippy::cast_precision_loss)]
//!
//! Reçoit les `TableEntry` accumulés par `SchemaObserver` et produit des `TableSchema`
//! prêts pour le DDL et le Pass 2. Responsabilités : construction des colonnes, sélection
//! de la stratégie wide-table (*pivot*, *autosplit*, *jsonb*…), détection et collapse des
//! siblings, tri topologique (parents avant enfants). L'orchestration de ces 4 phases est
//! implémentée dans [`orchestrator`] (struct `SchemaFinalizer`, réexportée ici).
//!
//! Frontière avec `wide_strategies.rs` : ce module *décide* quelle stratégie appliquer.
//! `wide_strategies.rs` *génère* les colonnes résultantes de cette stratégie.
//!
//! Sous-modules :
//! - [`orchestrator`]  — orchestration des 4 phases (base → cascade → wide strategies → guard), struct `SchemaFinalizer`
//! - [`base`]         — Phase 1, construction des schémas de base (par table, parallèle)
//! - [`wide_strategy`] — Phase 3, décision de la stratégie wide-table (par table)
//! - [`guard`]         — Phase 4, garde-fou 1600 colonnes + exclusion des enfants absorbés

mod orchestrator;
pub use orchestrator::SchemaFinalizer;

mod base;

mod wide_strategy;

mod guard;
pub(crate) use guard::apply_column_limit_guard;
pub use guard::{OverflowWarning, exclude_absorbed_children};
#[cfg(test)]
pub use guard::PG_MAX_COLUMNS;

