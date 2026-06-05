//! Database layer: PostgreSQL connection ([`connection`]), DDL generation ([`ddl`]),
//! COPY-format row encoding ([`copy_text`]), and per-table file sinks ([`copy_sink`]).
pub mod connection;
pub mod copy_sink;
pub mod copy_text;
pub mod ddl;
