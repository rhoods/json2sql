//! Trait `RowSink` — abstraction over the row-writing destination for COPY text rows.
//!
//! Decouples insert logic (`pass2/insert.rs`) from the underlying buffer so that
//! `MemSink` can be used in both production and tests without code changes.

use std::sync::{Arc, Mutex};

use crate::db::copy_sink::MemSink;
use crate::error::Result;

/// Abstraction over row writing, decoupling insert logic from the buffer implementation.
pub trait RowSink {
    fn write_row(&mut self, row: &[u8]) -> Result<()>;
}

impl RowSink for MemSink {
    fn write_row(&mut self, row: &[u8]) -> Result<()> {
        Self::write_row(self, row)
    }
}

/// Shared sink: multiple workers write to the same `MemSink` under a Mutex.
/// Lock is held only for the duration of `write_row` (nanoseconds — pure memory append).
impl RowSink for Arc<Mutex<MemSink>> {
    fn write_row(&mut self, row: &[u8]) -> Result<()> {
        self.lock().expect("sink mutex is not poisoned").write_row(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::copy_sink::MemSink;
    use crate::schema::table_schema::{ColumnSchema, TableSchema};
    use crate::schema::type_tracker::PgType;

    fn make_mem_sink() -> MemSink {
        let mut schema = TableSchema::new("t".to_string(), vec!["t".to_string()], 0);
        schema.columns.push(ColumnSchema {
            name: "col".to_string(),
            original_name: "col".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        MemSink::new(&schema, "public")
    }

    #[test]
    fn row_sink_mem_sink_write_row_delegates_correctly() {
        let mut sink = make_mem_sink();
        <MemSink as RowSink>::write_row(&mut sink, b"row1\n").unwrap();
        <MemSink as RowSink>::write_row(&mut sink, b"row2\n").unwrap();
        assert_eq!(sink.row_count, 2);
        assert_eq!(&sink.buf[..], b"row1\nrow2\n");
    }

    #[test]
    fn row_sink_arc_mutex_mem_sink_delegates_correctly() {
        let sink = Arc::new(Mutex::new(make_mem_sink()));
        let mut shared = Arc::clone(&sink);
        <Arc<Mutex<MemSink>> as RowSink>::write_row(&mut shared, b"row\n").unwrap();
        let inner = sink.lock().unwrap();
        assert_eq!(inner.row_count, 1);
        assert_eq!(&inner.buf[..], b"row\n");
    }
}
