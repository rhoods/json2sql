//! Database layer: `PostgreSQL` connection ([`connection`]), DDL generation ([`ddl`]),
//! COPY-format row encoding ([`copy_text`]), per-table file sinks ([`copy_sink`]), and server
//! tuning around Pass 2 ([`pg_tuning`]).
pub mod connection;
pub mod copy_sink;
pub mod copy_text;
pub mod ddl;
pub mod error;
pub mod pg_tuning;
