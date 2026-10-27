//! Pass 1 — JSON schema inference.
//!
//! Streams the input file once and builds finalized [`crate::schema::table_schema::TableSchema`]
//! entries sorted topologically. See [`runner`] for the entry points; [`row_sample`] keeps a bounded sample of the
//! root rows for the data signals of the column groups (#91).
pub mod row_sample;
pub mod runner;
