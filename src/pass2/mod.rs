//! Pass 2 — data insertion into PostgreSQL.
//!
//! N workers stream the JSON round-robin into local `MemSink` buffers. A concurrent
//! `run_flusher` task drains those buffers to PostgreSQL via `COPY FROM STDIN` as they fill,
//! eliminating the need for temp files. See [`runner`] for the main entry point.
pub mod coercer;
pub mod insert;
pub mod runner;
pub use runner::Pass2Config;
pub mod sink;
pub mod traversal;
#[cfg(test)]
pub(crate) mod test_utils;
