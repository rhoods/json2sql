//! Schema domain: type inference, naming, strategy selection, and the finalized table model.
//!
//! Sub-modules follow the pipeline order: `observer` → `inspector` → `finalizer` →
//! `table_schema`. The `cascading`, `naming`, `config`, and `persistence` modules
//! handle cross-cutting concerns.
//!
//! The crate-internal constant `PATH_SEP` (null byte `\x00`) is used throughout
//! to join JSON path segments into collision-free lookup keys.

/// Internal separator used to build path keys (e.g. `"root\x00foo.bar\x00child"`).
/// Null byte is guaranteed not to appear in JSON field names (serde_json rejects them),
/// so it is a collision-free separator even for field names that contain dots or underscores.
pub(crate) const PATH_SEP: char = '\x00';

pub mod cascading;
pub mod strategies;
pub mod config;
pub mod finalizer;
pub mod wide_strategies;
pub mod naming;
pub mod observer;
pub mod persistence;
pub mod registry;
pub mod inspector;
pub mod stats;
pub mod suffix_detector;
pub mod table_schema;
pub mod type_tracker;
