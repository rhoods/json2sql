//! Pass 1 — JSON schema inference.
//!
//! Streams the input file once and builds finalized [`crate::schema::table_schema::TableSchema`]
//! entries sorted topologically. See [`runner`] for the entry points.
pub mod runner;
