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

const COPY_CHUNK_SIZE: usize = 4 * 1024 * 1024; // 4 MiB per COPY feed call

/// Flush `buf` to PostgreSQL via `COPY FROM STDIN`, streaming in 4 MiB chunks.
/// No-op when `buf` is empty — avoids opening a COPY session for zero data.
/// Row count tracking is the caller's responsibility.
pub async fn flush_mem_sink_to_pg(buf: Bytes, copy_sql: &str, client: &Client) -> crate::error::Result<()> {
    if buf.is_empty() {
        return Ok(());
    }
    let sink = client.copy_in::<_, Bytes>(copy_sql).await
        .map_err(|e| pg_err("COPY FROM STDIN (MemSink)", &e))?;
    let mut pinned = Box::pin(sink);
    let mut offset = 0;
    while offset < buf.len() {
        let end = (offset + COPY_CHUNK_SIZE).min(buf.len());
        pinned.feed(buf.slice(offset..end)).await
            .map_err(|e| pg_err("COPY feed chunk (MemSink)", &e))?;
        offset = end;
    }
    pinned.flush().await
        .map_err(|e| pg_err("COPY flush (MemSink)", &e))?;
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

    // -------------------------------------------------------------------------
    // flush_mem_sink_to_pg chunking logic tests (no PG required)
    // -------------------------------------------------------------------------

    fn collect_chunks(buf: &Bytes, chunk_size: usize) -> Vec<Bytes> {
        let mut chunks = Vec::new();
        let mut offset = 0;
        while offset < buf.len() {
            let end = (offset + chunk_size).min(buf.len());
            chunks.push(buf.slice(offset..end));
            offset = end;
        }
        chunks
    }

    #[test]
    fn chunking_buf_smaller_than_chunk_yields_one_chunk() {
        let buf = Bytes::from(vec![1u8; 1024]);
        let chunks = collect_chunks(&buf, 4 * 1024 * 1024);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), 1024);
    }

    #[test]
    fn chunking_buf_exactly_one_chunk_size() {
        let chunk_size = 4 * 1024 * 1024;
        let buf = Bytes::from(vec![0u8; chunk_size]);
        let chunks = collect_chunks(&buf, chunk_size);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].len(), chunk_size);
    }

    #[test]
    fn chunking_buf_one_chunk_plus_remainder() {
        let chunk_size = 4 * 1024 * 1024;
        let buf = Bytes::from(vec![0u8; chunk_size + 100]);
        let chunks = collect_chunks(&buf, chunk_size);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), chunk_size);
        assert_eq!(chunks[1].len(), 100);
    }

    #[test]
    fn chunking_buf_multiple_full_chunks() {
        let chunk_size = 4 * 1024 * 1024;
        let buf = Bytes::from(vec![0u8; chunk_size * 3]);
        let chunks = collect_chunks(&buf, chunk_size);
        assert_eq!(chunks.len(), 3);
        for c in &chunks { assert_eq!(c.len(), chunk_size); }
    }

    #[test]
    fn chunking_preserves_all_data() {
        let data: Vec<u8> = (0u8..=255).cycle().take(10_000).collect();
        let buf = Bytes::from(data.clone());
        let chunk_size = 1000;
        let chunks = collect_chunks(&buf, chunk_size);
        let reassembled: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        assert_eq!(reassembled, data);
    }
}
