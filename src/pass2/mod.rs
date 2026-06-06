//! Pass 2 — data insertion into `PostgreSQL`.
//!
//! Workers stream the JSON a second time, coerce values via [`coercer`], and bulk-load
//! rows via `COPY FROM STDIN`. See [`runner`] for the main entry points.
pub mod coercer;
pub mod insert;
pub mod runner;
pub use runner::Pass2Config;
pub mod sink;
pub mod traversal;
