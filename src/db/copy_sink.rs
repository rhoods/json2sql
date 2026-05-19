use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use bytes::Bytes;
use futures_util::SinkExt;
use tempfile::NamedTempFile;
use tokio_postgres::Client;
use uuid::Uuid;

use crate::db::copy_text::CopyEscaped;
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
/// Maximum number of temp files open simultaneously before the Pass 2 runner
/// triggers a preventive flush of all active sinks to recycle file descriptors.
/// Chosen conservatively below the typical ulimit of 1024.
pub const MAX_OPEN_TEMP_FILES: usize = 950;

/// Total bytes written to a single worker's sinks that trigger an interim
/// COPY-to-PostgreSQL flush during Phase B streaming. Keeps per-worker peak
/// temp-file disk usage bounded instead of accumulating the full source file.
pub const INTERIM_FLUSH_THRESHOLD: u64 = 512 * 1024 * 1024; // 512 MiB per worker

/// Row data accumulates in memory up to this size before being spilled to disk.
/// Large batches amortize the syscall overhead of write() and open()/close().
const SPILL_THRESHOLD: usize = 256 * 1024;

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
// TempFilePath — owns the temp file path and deletes it on drop
// ---------------------------------------------------------------------------

struct TempFilePath(PathBuf);

impl Drop for TempFilePath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

// ---------------------------------------------------------------------------
// TempFileSink
// ---------------------------------------------------------------------------

/// Buffers rows for one table during Pass 2, then COPYs them to PostgreSQL.
///
/// # Memory vs. disk buffering
///
/// Rows accumulate in an in-memory `pending` buffer. When `pending` exceeds
/// [`SPILL_THRESHOLD`], the data is flushed to a temp file (a "spill") and the
/// buffer is cleared. This batches disk writes to large, efficient chunks.
///
/// # Hibernation
///
/// `hibernate()` closes the open file descriptor (if any) **without flushing
/// `pending`**. In-memory data survives the hibernation intact. The FD is
/// reopened on the next spill. This makes hibernation cheap — just a
/// `close()` syscall — regardless of how many rows are buffered.
///
/// This design avoids flushing small BufWriter buffers on every insert, which
/// was the bottleneck in the previous implementation.
pub struct TempFileSink {
    pub table_name: String,
    /// Rows buffered since the last flush (or since creation).
    pub row_count: u64,
    /// Total rows sent to PG across all periodic flushes.
    pub total_flushed: u64,
    /// Raw bytes written since the last flush. Used by the runner to decide
    /// when to trigger an interim COPY to bound per-worker disk usage.
    pub bytes_buffered: u64,
    /// In-memory row data. Survives hibernation. Spilled to disk when it grows
    /// past SPILL_THRESHOLD.
    pending: Vec<u8>,
    /// Raw file descriptor. Open only while a spill is in progress.
    /// None = FD released (hibernated or no spill yet).
    writer: Option<File>,
    /// Keeps the temp file alive between spills. None until the first spill.
    temp_file: Option<TempFilePath>,
    copy_sql: String,
}

impl TempFileSink {
    pub fn new(schema: &TableSchema, pg_schema: &str) -> Result<Self> {
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
            bytes_buffered: 0,
            pending: Vec::new(),
            writer: None,
            temp_file: None,
            copy_sql,
        })
    }

    /// Returns true when a temp-file FD is currently held open (i.e. a spill
    /// is in progress or has just completed and hibernate has not been called).
    pub fn is_open(&self) -> bool {
        self.writer.is_some()
    }

    /// Close the open file descriptor without touching the in-memory `pending`
    /// buffer. This is cheap: a single `close()` syscall regardless of how
    /// many rows are buffered. Data in `pending` is preserved for the next write.
    ///
    /// No-op when no FD is held.
    pub fn hibernate(&mut self) -> Result<()> {
        self.writer = None; // drop File → close FD; pending stays in memory
        Ok(())
    }

    /// Open (or reopen) the temp file for appending.
    fn ensure_file(&mut self) -> Result<&mut File> {
        if self.writer.is_none() {
            let file = if let Some(ref guard) = self.temp_file {
                OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(&guard.0)
                    .map_err(J2sError::Io)?
            } else {
                let tmp = NamedTempFile::new().map_err(J2sError::Io)?;
                let (file, path) = tmp
                    .keep()
                    .map_err(|e| J2sError::Io(std::io::Error::other(e)))?;
                self.temp_file = Some(TempFilePath(path));
                file
            };
            self.writer = Some(file);
        }
        Ok(self.writer.as_mut().unwrap())
    }

    /// Write all of `pending` to the temp file and clear the buffer.
    /// The file FD stays open after the spill for the next spill.
    fn spill(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        // mem::take satisfies the borrow checker: ensure_file borrows &mut self,
        // so we extract pending first. On write failure the data is lost, but
        // an I/O error on a temp file is already fatal to the import.
        let data = std::mem::take(&mut self.pending);
        let file = self.ensure_file()?;
        file.write_all(&data).map_err(J2sError::Io)?;
        Ok(())
    }

    pub fn write_row(&mut self, row: Vec<u8>) -> Result<()> {
        self.bytes_buffered += row.len() as u64;
        self.pending.extend_from_slice(&row);
        self.row_count += 1;
        if self.pending.len() >= SPILL_THRESHOLD {
            self.spill()?;
        }
        Ok(())
    }

    /// Send all buffered rows to PostgreSQL, then reset the sink for reuse.
    /// Data may be split between the temp file (previous spills) and `pending`
    /// (rows accumulated since the last spill).
    pub async fn flush_to_db(&mut self, client: &Client) -> Result<u64> {
        if self.row_count == 0 {
            return Ok(0);
        }

        // Close the FD before reading (no flush needed — pending is separate).
        self.writer = None;

        // Collect on-disk data.
        let file_data = if let Some(ref guard) = self.temp_file {
            tokio::fs::read(&guard.0).await.map_err(J2sError::Io)?
        } else {
            Vec::new()
        };

        let has_data = !file_data.is_empty() || !self.pending.is_empty();
        if has_data {
            let sink = client
                .copy_in::<_, Bytes>(&self.copy_sql)
                .await
                .map_err(|e| pg_err(&format!("COPY INTO {}", self.table_name), e))?;
            let mut pinned = Box::pin(sink);
            for chunk in file_data.chunks(1024 * 1024) {
                pinned
                    .send(Bytes::copy_from_slice(chunk))
                    .await
                    .map_err(|e| pg_err(&format!("COPY send {}", self.table_name), e))?;
            }
            for chunk in self.pending.chunks(1024 * 1024) {
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

        // Truncate the on-disk file for reuse; clear the in-memory buffer.
        if let Some(ref guard) = self.temp_file {
            OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&guard.0)
                .map_err(J2sError::Io)?;
        }
        self.pending.clear();

        let flushed = self.row_count;
        self.total_flushed += flushed;
        self.row_count = 0;
        self.bytes_buffered = 0;
        Ok(flushed)
    }

    /// Flush all remaining rows to PostgreSQL.
    /// Returns total rows sent (periodic flushes + this final call).
    pub async fn copy_to_db(self, client: &Client) -> Result<u64> {
        // Destructure — TempFileSink has no Drop; TempFilePath (in temp_file) does.
        let TempFileSink {
            row_count,
            total_flushed,
            bytes_buffered: _,
            pending,
            writer,
            temp_file,
            copy_sql,
            table_name,
        } = self;

        if row_count == 0 {
            return Ok(total_flushed);
        }

        // Close FD if open (no flush needed — pending is the source of truth).
        drop(writer);

        // Read on-disk data (from previous spills).
        let file_data = if let Some(ref guard) = temp_file {
            tokio::fs::read(&guard.0).await.map_err(J2sError::Io)?
        } else {
            Vec::new()
        };

        // Delete the temp file before starting the COPY.
        drop(temp_file);

        let has_data = !file_data.is_empty() || !pending.is_empty();
        if has_data {
            let sink = client
                .copy_in::<_, Bytes>(&copy_sql)
                .await
                .map_err(|e| pg_err(&format!("COPY INTO {}", table_name), e))?;
            let mut pinned = Box::pin(sink);
            for chunk in file_data.chunks(1024 * 1024) {
                pinned
                    .send(Bytes::copy_from_slice(chunk))
                    .await
                    .map_err(|e| pg_err(&format!("COPY send {}", table_name), e))?;
            }
            for chunk in pending.chunks(1024 * 1024) {
                pinned
                    .send(Bytes::copy_from_slice(chunk))
                    .await
                    .map_err(|e| pg_err(&format!("COPY send {}", table_name), e))?;
            }
            pinned
                .close()
                .await
                .map_err(|e| pg_err(&format!("COPY close {}", table_name), e))?;
        }

        Ok(total_flushed + row_count)
    }
}

/// Send all buffered rows from multiple sinks to PostgreSQL in a single COPY
/// session. All sinks must target the same table (same copy_sql / schema).
///
/// Reduces COPY overhead for tables whose rows are split across many workers:
/// instead of N small COPYs, one COPY streams data from all N sinks in sequence.
pub async fn merge_copy_to_db(sinks: Vec<TempFileSink>, client: &Client) -> Result<u64> {
    let total_rows: u64 = sinks.iter().map(|s| s.total_flushed + s.row_count).sum();
    if total_rows == 0 {
        return Ok(0);
    }

    // Invariant: merge_copy_to_db must only be called on sinks that have never been
    // interim-flushed to PG. flush_to_db() increments total_flushed and truncates the
    // temp file — so if total_flushed > 0, the data for those rows is gone and cannot
    // be re-streamed, causing an inflated Ok() count with a silent data gap.
    debug_assert!(
        sinks.iter().all(|s| s.total_flushed == 0),
        "merge_copy_to_db: called on sink(s) with total_flushed > 0 — \
         flush_to_db() must not be called before merging"
    );

    // Precondition: all sinks target the same table (same copy_sql). Guaranteed by
    // flush_task which groups sinks under table_pending[table_name]. total_rows > 0
    // ensures sinks is non-empty, so first() always succeeds here.
    let first = sinks.first().expect("sinks non-empty (total_rows > 0)");
    debug_assert!(
        sinks.iter().all(|s| s.table_name == first.table_name),
        "merge_copy_to_db: sinks target different tables — caller must group by table_name"
    );
    let copy_sql = first.copy_sql.clone();
    let table_name = first.table_name.clone();

    let sink = client
        .copy_in::<_, Bytes>(&copy_sql)
        .await
        .map_err(|e| pg_err(&format!("COPY INTO {}", table_name), e))?;
    let mut pinned = Box::pin(sink);

    for s in sinks {
        let TempFileSink { row_count, total_flushed, pending, writer, temp_file, .. } = s;
        if row_count == 0 && total_flushed == 0 {
            continue;
        }
        drop(writer);
        let file_data = if let Some(ref guard) = temp_file {
            tokio::fs::read(&guard.0).await.map_err(J2sError::Io)?
        } else {
            Vec::new()
        };
        drop(temp_file);
        for chunk in file_data.chunks(1024 * 1024) {
            pinned.send(Bytes::copy_from_slice(chunk)).await
                .map_err(|e| pg_err(&format!("COPY send {}", table_name), e))?;
        }
        for chunk in pending.chunks(1024 * 1024) {
            pinned.send(Bytes::copy_from_slice(chunk)).await
                .map_err(|e| pg_err(&format!("COPY send {}", table_name), e))?;
        }
    }

    pinned.close().await
        .map_err(|e| pg_err(&format!("COPY close {}", table_name), e))?;

    Ok(total_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::table_schema::{ColumnSchema, TableSchema};
    use crate::schema::type_tracker::PgType;

    fn make_sink() -> TempFileSink {
        let mut schema = TableSchema::new("t".to_string(), vec!["t".to_string()], 0);
        schema.columns.push(ColumnSchema {
            name: "col".to_string(),
            original_name: "col".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        TempFileSink::new(&schema, "public").unwrap()
    }

    /// Returns true if any /proc/self/fd/N symlink resolves to `path`.
    #[cfg(target_os = "linux")]
    fn fd_points_to(path: &std::path::Path) -> bool {
        let Ok(fds) = std::fs::read_dir("/proc/self/fd") else {
            return false;
        };
        fds.flatten()
            .any(|e| std::fs::read_link(e.path()).map_or(false, |t| t == path))
    }

    /// Small writes (below SPILL_THRESHOLD) must not open any file descriptor.
    #[test]
    fn no_fd_below_spill_threshold() {
        let mut sink = make_sink();
        sink.write_row(b"row\n".to_vec()).unwrap();
        assert!(!sink.is_open(), "no FD should be opened below spill threshold");
        assert!(sink.temp_file.is_none(), "no temp file should be created below threshold");
    }

    /// Data written across multiple writes and hibernations must all end up in
    /// `pending` (when no spill has occurred).
    #[test]
    fn write_after_hibernate_appends() {
        let mut sink = make_sink();

        sink.write_row(b"row1\n".to_vec()).unwrap();
        sink.hibernate().unwrap();
        sink.write_row(b"row2\n".to_vec()).unwrap();
        sink.hibernate().unwrap();

        assert_eq!(sink.row_count, 2);
        assert_eq!(&sink.pending, b"row1\nrow2\n");
        assert!(!sink.is_open(), "no FD after hibernate");
    }

    /// After a spill, hibernate must close the file descriptor.
    #[cfg(target_os = "linux")]
    #[test]
    fn hibernate_releases_fd_after_spill() {
        let mut sink = make_sink();
        // Write enough to trigger a spill.
        let row = vec![b'x'; SPILL_THRESHOLD + 1];
        sink.write_row(row).unwrap();

        let path = sink.temp_file.as_ref().unwrap().0.clone();
        assert!(fd_points_to(&path), "FD should be open after spill");

        sink.hibernate().unwrap();
        assert!(!fd_points_to(&path), "FD must be closed after hibernate");
        // pending is empty (was spilled), but temp_file still exists.
        assert!(sink.pending.is_empty());
    }

    #[test]
    fn bytes_buffered_tracks_written_data() {
        let mut sink = make_sink();
        assert_eq!(sink.bytes_buffered, 0);

        let row1 = b"hello\n".to_vec();
        let len1 = row1.len() as u64;
        sink.write_row(row1).unwrap();
        assert_eq!(sink.bytes_buffered, len1);

        let row2 = b"world\n".to_vec();
        let len2 = row2.len() as u64;
        sink.write_row(row2).unwrap();
        assert_eq!(sink.bytes_buffered, len1 + len2);
    }

    #[test]
    fn bytes_buffered_survives_spill() {
        let mut sink = make_sink();
        let large_row = vec![b'x'; SPILL_THRESHOLD + 1];
        let expected = large_row.len() as u64;
        sink.write_row(large_row).unwrap();
        assert!(sink.is_open(), "spill should have opened a file");
        assert_eq!(sink.bytes_buffered, expected, "bytes_buffered must reflect spilled data");
    }

    // -------------------------------------------------------------------------
    // merge_copy_to_db tests (logic only — no PG connection required)
    // -------------------------------------------------------------------------

    /// Total rows reported across sinks equals the sum of row_count from each.
    #[test]
    fn merge_total_rows_sum_is_correct() {
        let mut s1 = make_sink();
        s1.write_row(b"row1\n".to_vec()).unwrap();
        s1.write_row(b"row2\n".to_vec()).unwrap();

        let mut s2 = make_sink();
        s2.write_row(b"row3\n".to_vec()).unwrap();

        let total: u64 = [&s1, &s2].iter().map(|s| s.total_flushed + s.row_count).sum();
        assert_eq!(total, 3);
    }

    /// Empty sinks (row_count == 0) must not count toward the total.
    #[test]
    fn merge_empty_sinks_contribute_zero_rows() {
        let s_empty = make_sink();
        assert_eq!(s_empty.row_count, 0);
        assert_eq!(s_empty.total_flushed, 0);
        let total: u64 = [&s_empty].iter().map(|s| s.total_flushed + s.row_count).sum();
        assert_eq!(total, 0);
    }

    /// Pending bytes from multiple sinks must be individually accessible
    /// (verifies the data structure is correct for the merge streaming loop).
    #[test]
    fn merge_pending_bytes_preserved_across_sinks() {
        let mut s1 = make_sink();
        s1.write_row(b"abc\n".to_vec()).unwrap();

        let mut s2 = make_sink();
        s2.write_row(b"def\n".to_vec()).unwrap();
        s2.write_row(b"ghi\n".to_vec()).unwrap();

        assert_eq!(&s1.pending, b"abc\n");
        assert_eq!(&s2.pending, b"def\nghi\n");
        assert_eq!(s1.row_count, 1);
        assert_eq!(s2.row_count, 2);
    }

    /// is_open() must be false after hibernation.
    #[test]
    fn is_open_false_after_hibernate() {
        let mut sink = make_sink();

        // Force a spill to open a FD.
        let row = vec![b'y'; SPILL_THRESHOLD + 1];
        sink.write_row(row).unwrap();
        assert!(sink.is_open());

        sink.hibernate().unwrap();
        assert!(!sink.is_open());
    }
}
