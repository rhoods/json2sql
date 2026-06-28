//! json2sql — stream a JSON file into a normalized PostgreSQL schema.
//!
//! Two-pass pipeline: Pass 1 infers a relational schema from the JSON structure;
//! Pass 2 loads data via `COPY FROM STDIN`. The `schema` module owns the domain model;
//! `pass1` and `pass2` own the pipeline runners.
#![forbid(unsafe_code)]
#![deny(dead_code)]
pub mod anomaly;
pub mod ipc;
pub mod cli;
pub mod db;
pub mod error;
pub mod io;
pub mod pass1;
pub mod pass2;
pub mod pipeline;
pub mod schema;
