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
pub mod reporter;
pub mod stats;
pub mod suffix_detector;
pub mod table_schema;
pub mod type_tracker;
