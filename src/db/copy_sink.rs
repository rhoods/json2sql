use std::io::{BufWriter, Write};

use bytes::Bytes;
use futures_util::SinkExt;
use tempfile::NamedTempFile;
use tokio_postgres::Client;
use uuid::Uuid;

use crate::error::{J2sError, Result};
use crate::schema::table_schema::TableSchema;

fn pg_err(context: &str, e: tokio_postgres::Error) -> J2sError {
    let detail = if let Some(db) = e.as_db_error() {
        format!("{} (code: {})", db.message(), db.code().code())
    } else {
        e.to_string()
    };
    J2sError::DbContext(format!("{}: {}", context, detail))
}

/// NULL representation in PostgreSQL COPY text format.
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
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(256),
            first: true,
        }
    }

    pub fn push_value(&mut self, value: &str) {
        if !self.first {
            self.buf.push(COPY_DELIMITER);
        }
        self.first = false;
        self.buf.extend_from_slice(value.as_bytes());
    }

    pub fn push_null(&mut self) {
        if !self.first {
            self.buf.push(COPY_DELIMITER);
        }
        self.first = false;
        self.buf.extend_from_slice(COPY_NULL.as_bytes());
    }

    /// Write a UUID column directly into the COPY buffer without a heap allocation.
    /// UUIDs are always 36 bytes; we format into a stack array then extend the buffer.
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
// TempFileSink
// ---------------------------------------------------------------------------

/// Buffers rows for one table to a temp file during pass 2.
/// After streaming is done, `copy_to_db` sends the file to PostgreSQL via COPY.
/// `flush_to_db` can be called periodically to stream data incrementally and
/// keep temp file sizes bounded.
pub struct TempFileSink {
    pub table_name: String,
    /// Rows buffered since the last flush (or since creation).
    pub row_count: u64,
    /// Total rows sent to PG across all periodic flushes (not counting unflushed rows).
    pub total_flushed: u64,
    writer: BufWriter<NamedTempFile>,
    copy_sql: String,
}

impl TempFileSink {
    pub fn new(schema: &TableSchema, pg_schema: &str) -> Result<Self> {
        let temp = NamedTempFile::new().map_err(J2sError::Io)?;

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

        Ok(Self {
            table_name: schema.name.clone(),
            row_count: 0,
            total_flushed: 0,
            writer: BufWriter::with_capacity(256 * 1024, temp),
            copy_sql,
        })
    }

    pub fn write_row(&mut self, row: Vec<u8>) -> Result<()> {
        self.writer.write_all(&row).map_err(J2sError::Io)?;
        self.row_count += 1;
        Ok(())
    }

    /// Flush currently buffered rows to PostgreSQL, then reset the temp file for
    /// continued use. The `row_count` is added to `total_flushed` and reset to 0.
    /// This bounds temp-file disk usage when called periodically during Pass 2.
    pub async fn flush_to_db(&mut self, client: &Client) -> Result<u64> {
        if self.row_count == 0 {
            return Ok(0);
        }

        // Drain BufWriter's internal buffer to the underlying file.
        self.writer.flush().map_err(J2sError::Io)?;

        let path = self.writer.get_ref().path().to_path_buf();
        let data = tokio::fs::read(&path).await.map_err(J2sError::Io)?;

        if !data.is_empty() {
            let sink = client
                .copy_in::<_, Bytes>(&self.copy_sql)
                .await
                .map_err(|e| pg_err(&format!("COPY INTO {}", self.table_name), e))?;
            let mut pinned = Box::pin(sink);
            for chunk in data.chunks(1024 * 1024) {
                pinned
                    .send(Bytes::copy_from_slice(chunk))
                    .await
                    .map_err(|e| pg_err(&format!("COPY send {}", self.table_name), e))?;
            }
            pinned
                .close()
                .await
                .map_err(|e| pg_err(&format!("COPY close {}", self.table_name), e))?;
        }

        // Truncate the temp file and seek back to the start so it can be reused.
        {
            use std::io::Seek;
            let f = self.writer.get_mut();
            f.as_file().set_len(0).map_err(J2sError::Io)?;
            f.seek(std::io::SeekFrom::Start(0)).map_err(J2sError::Io)?;
        }

        let flushed = self.row_count;
        self.total_flushed += flushed;
        self.row_count = 0;
        Ok(flushed)
    }

    /// Flush the remaining temp file rows and stream them into PostgreSQL via COPY FROM STDIN.
    /// Returns the total number of rows sent across all flushes (periodic + this final call).
    pub async fn copy_to_db(self, client: &Client) -> Result<u64> {
        let remaining = self.row_count;
        let total_flushed = self.total_flushed;

        let mut writer = self.writer;
        writer.flush().map_err(J2sError::Io)?;
        let temp_file = writer
            .into_inner()
            .map_err(|e| J2sError::Io(e.into_error()))?;

        let data = tokio::fs::read(temp_file.path())
            .await
            .map_err(J2sError::Io)?;

        // NamedTempFile deletes itself on drop
        drop(temp_file);

        if !data.is_empty() {
            // Open the COPY session.
            let sink = client
                .copy_in::<_, Bytes>(&self.copy_sql)
                .await
                .map_err(|e| pg_err(&format!("COPY INTO {}", self.table_name), e))?;

            // CopyInSink<T> is !Unpin (uses PhantomPinned as an API marker).
            // Pin<Box<T>>: Unpin because Box<T>: Unpin regardless of T, so
            // SinkExt::send (which requires Self: Unpin) is callable on it.
            let mut pinned = Box::pin(sink);

            for chunk in data.chunks(1024 * 1024) {
                pinned
                    .send(Bytes::copy_from_slice(chunk))
                    .await
                    .map_err(|e| pg_err(&format!("COPY send {}", self.table_name), e))?;
            }

            // `finish()` on CopyInSink takes `self` and returns the server row count.
            // Since we track row_count ourselves, we use `close()` (via SinkExt)
            // which sends the COPY terminator and waits for CommandComplete.
            // Pin<Box<T>>: Unpin (Box: Unpin regardless of T), so close() is callable.
            pinned
                .close()
                .await
                .map_err(|e| pg_err(&format!("COPY close {}", self.table_name), e))?;
        }

        Ok(total_flushed + remaining)
    }
}
