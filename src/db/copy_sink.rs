//! Sink `PostgreSQL` COPY — in-memory row buffer and COPY text construction.
//!
//! `MemSink` accumulates COPY-format rows in a `BytesMut` buffer and hands them
//! to `flush_mem_sink_to_pg` for bulk-loading via `COPY FROM STDIN`.
//! `RowBuilder` constructs one tab-separated COPY text row field by field.

use bytes::{Bytes, BytesMut};
use futures_util::SinkExt;
use tokio_postgres::Client;
use uuid::Uuid;

use crate::db::copy_text::CopyEscaped;
use crate::db::error::pg_err;
use crate::schema::table_schema::TableSchema;

/// NULL representation in `PostgreSQL` COPY text format.
pub const COPY_NULL: &str = "\\N";
/// Column delimiter in COPY text format.
pub const COPY_DELIMITER: u8 = b'\t';

// ---------------------------------------------------------------------------
// Row builder
// ---------------------------------------------------------------------------

/// Builds a tab-separated row for COPY text format.
pub struct RowBuilder {
    buf: Vec<u8>,
    first: bool,
}

impl RowBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256),
            first: true,
        }
    }

    pub fn push_value(&mut self, value: &CopyEscaped) {
        if !self.first {
            self.buf.push(COPY_DELIMITER);
        }
        self.first = false;
        self.buf.extend_from_slice(value.as_str().as_bytes());
    }

    pub fn push_null(&mut self) {
        if !self.first {
            self.buf.push(COPY_DELIMITER);
        }
        self.first = false;
        self.buf.extend_from_slice(COPY_NULL.as_bytes());
    }

    /// Write a UUID column directly into the COPY buffer without a heap allocation.
    #[inline]
    pub fn push_uuid(&mut self, uuid: Uuid) {
        if !self.first {
            self.buf.push(COPY_DELIMITER);
        }
        self.first = false;
        let mut tmp = [0u8; 36];
        let s = uuid.hyphenated().encode_lower(&mut tmp);
        self.buf.extend_from_slice(s.as_bytes());
    }

    /// Finish the row, appending a newline.
    #[must_use]
    pub fn finish(mut self) -> Vec<u8> {
        self.buf.push(b'\n');
        self.buf
    }
}

impl Default for RowBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// MemSink — in-memory buffer for COPY text (diskless pipeline)
// ---------------------------------------------------------------------------

/// In-memory row buffer for one table. Rows are accumulated via `write_row`,
/// then handed to `flush_mem_sink_to_pg` as a single `Bytes` chunk.
/// No spill to disk — the flusher thread controls when to flush based on RAM pressure.
pub struct MemSink {
    pub buf: BytesMut,
    pub row_count: u64,
    pub copy_sql: String,
}

impl MemSink {
    pub fn new(schema: &TableSchema, pg_schema: &str) -> Self {
        let col_names: Vec<String> = schema
            .columns
            .iter()
            .map(|c| format!("\"{}\"", c.name.replace('"', "\"\"")))
            .collect();
        let copy_sql = format!(
            "COPY \"{}\".\"{}\" ({}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')",
            pg_schema.replace('"', "\"\""),
            schema.name.replace('"', "\"\""),
            col_names.join(", ")
        );
        Self { buf: BytesMut::new(), row_count: 0, copy_sql }
    }

    pub fn write_row(&mut self, row: &[u8]) -> crate::error::Result<()> {
        self.buf.extend_from_slice(row);
        self.row_count += 1;
        Ok(())
    }
}

/// Flush `buf` to PostgreSQL via `COPY FROM STDIN`.
/// No-op when `buf` is empty — avoids opening a COPY session for zero data.
/// Row count tracking is the caller's responsibility.
pub async fn flush_mem_sink_to_pg(buf: Bytes, copy_sql: &str, client: &Client) -> crate::error::Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let sink = client.copy_in::<_, Bytes>(copy_sql).await
        .map_err(|e| pg_err("COPY FROM STDIN (MemSink)", &e))?;
    let mut pinned = Box::pin(sink);
    pinned.send(buf).await
        .map_err(|e| pg_err("COPY send (MemSink)", &e))?;
    pinned.close().await
        .map_err(|e| pg_err("COPY close (MemSink)", &e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn mem_sink_new_generates_correct_copy_sql() {
        let sink = make_mem_sink();
        assert_eq!(
            sink.copy_sql,
            r#"COPY "public"."t" ("col") FROM STDIN WITH (FORMAT text, DELIMITER E'\t', NULL '\N')"#
        );
        assert_eq!(sink.row_count, 0);
        assert!(sink.buf.is_empty());
    }

    #[test]
    fn mem_sink_write_row_appends_bytes_and_tracks_count() {
        let mut sink = make_mem_sink();
        sink.write_row(b"val1\n").unwrap();
        sink.write_row(b"val2\n").unwrap();
        assert_eq!(sink.row_count, 2);
        assert_eq!(&sink.buf[..], b"val1\nval2\n");
    }

    #[test]
    fn mem_sink_write_row_empty_slice_increments_row_count() {
        let mut sink = make_mem_sink();
        sink.write_row(b"").unwrap();
        assert_eq!(sink.row_count, 1);
        assert!(sink.buf.is_empty());
    }
}
