//! Test helpers shared across `pass2::runner` submodules (used by both `mod.rs` and
//! `worker.rs` test modules — kept here once to avoid silent divergence between copies).

#![cfg(test)]

pub(super) fn make_schema_with_rows(name: &str, row_count: u64) -> crate::schema::table_schema::TableSchema {
    let mut s = crate::schema::table_schema::TableSchema::new(
        name.to_string(), vec![name.to_string()], 0,
    );
    s.row_count = row_count;
    s
}
