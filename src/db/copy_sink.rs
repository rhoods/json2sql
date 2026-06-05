use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use tokio::io::AsyncReadExt;

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

async fn send_copy_data(
    client: &Client,
    copy_sql: &str,
    table_name: &str,
    file_data: &[u8],
    pending: &[u8],
) -> Result<()> {
    let sink = client
        .copy_in::<_, Bytes>(copy_sql)
        .await
        .map_err(|e| pg_err(&format!("COPY INTO {}", table_name), e))?;
    let mut pinned = Box::pin(sink);
    for chunk in file_data.chunks(1024 * 1024) {
        pinned.send(Bytes::copy_from_slice(chunk))
            .await
            .map_err(|e| pg_err(&format!("COPY send {}", table_name), e))?;
    }
    for chunk in pending.chunks(1024 * 1024) {
        pinned.send(Bytes::copy_from_slice(chunk))
            .await
            .map_err(|e| pg_err(&format!("COPY send {}", table_name), e))?;
    }
    pinned.close().await.map_err(|e| pg_err(&format!("COPY close {}", table_name), e))
}

/// NULL representation in PostgreSQL COPY text format.
pub const COPY_NULL: &str = "\\N";
/// Column delimiter in COPY text format.
pub const COPY_DELIMITER: u8 = b'\t';
/// Row data accumulates in memory up to this size before being spilled to disk.
/// Large batches amortize the syscall overhead of write() and open()/close().
const SPILL_THRESHOLD: usize = 4 * 1024 * 1024;

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
// TempFilePath — owns the temp file path and deletes it on drop
// ---------------------------------------------------------------------------

struct TempFilePath(PathBuf);

impl Drop for TempFilePath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

impl TempFilePath {
    /// Consume self and return the path without triggering file deletion.
    /// Caller is responsible for deleting the file when done.
    fn into_path_no_delete(self) -> PathBuf {
        let p = self.0.clone();
        std::mem::forget(self);
        p
    }
}

/// Data extracted from a [`TempFileSink`] for an async background COPY.
/// The caller reads `file_path` + `pending`, COPYs to PG, then deletes
/// `file_path`. The sink is already reset and ready for new writes.
pub struct FlushSnapshot {
    pub copy_sql: String,
    /// Path of the spill file to read. Caller must delete it after COPY.
    pub file_path: Option<PathBuf>,
    pub pending: Vec<u8>,
    pub row_count: u64,
    pub table_name: String,
}

/// Copy the data in `snap` to PostgreSQL, then delete the spill file.
/// Returns the number of rows sent (= `snap.row_count`).
pub async fn copy_snapshot_to_pg(snap: FlushSnapshot, client: &Client) -> Result<u64> {
    let FlushSnapshot { copy_sql, file_path, pending, row_count, table_name } = snap;
    if row_count == 0 {
        if let Some(ref p) = file_path { let _ = tokio::fs::remove_file(p).await; }
        return Ok(0);
    }
    let file_data = if let Some(ref p) = file_path {
        tokio::fs::read(p).await.map_err(J2sError::Io)?
    } else {
        Vec::new()
    };
    if let Some(ref p) = file_path { let _ = tokio::fs::remove_file(p).await; }
    if !file_data.is_empty() || !pending.is_empty() {
        send_copy_data(client, &copy_sql, &table_name, &file_data, &pending).await?;
    }
    Ok(row_count)
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
    /// Directory for the spill temp file. None = system default (std::env::temp_dir()).
    temp_dir: Option<PathBuf>,
    copy_sql: String,
}

impl TempFileSink {
    pub fn new(schema: &TableSchema, pg_schema: &str, temp_dir: Option<&Path>) -> Result<Self> {
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
            temp_dir: temp_dir.map(|p| p.to_path_buf()),
            copy_sql,
        })
    }

    /// Returns true when a temp-file FD is currently held open (i.e. a spill
    /// is in progress or has just completed and hibernate has not been called).
    #[allow(dead_code)] // public API — not yet used in binary, available for future callers
    #[must_use]
    pub fn is_open(&self) -> bool {
        self.writer.is_some()
    }

    /// Close the open file descriptor without touching the in-memory `pending`
    /// buffer. This is cheap: a single `close()` syscall regardless of how
    /// many rows are buffered. Data in `pending` is preserved for the next write.
    ///
    /// No-op when no FD is held.
    #[allow(dead_code)] // public API — not yet used in binary, available for future callers
    pub fn hibernate(&mut self) -> Result<()> {
        self.writer = None; // drop File → close FD; pending stays in memory
        Ok(())
    }

    /// Open (or reopen) the temp file for appending.
    fn ensure_file(&mut self) -> Result<&mut File> {
        if self.writer.is_none() {
            let file = if let Some(ref guard) = self.temp_file {
                OpenOptions::new()
                    
                    .append(true)
                    .open(&guard.0)
                    .map_err(J2sError::Io)?
            } else {
                let tmp = match &self.temp_dir {
                    Some(dir) => NamedTempFile::new_in(dir).map_err(J2sError::Io)?,
                    None => NamedTempFile::new().map_err(J2sError::Io)?,
                };
                let (file, path) = tmp
                    .keep()
                    .map_err(|e| J2sError::Io(std::io::Error::other(e)))?;
                self.temp_file = Some(TempFilePath(path));
                file
            };
            self.writer = Some(file);
        }
        Ok(self.writer.as_mut().expect("ensure_file sets self.writer just above"))
    }

    /// Write all of `pending` to the temp file, clear the buffer, and immediately
    /// close the FD (auto-hibernate). This keeps FD count at 0 across all workers,
    /// eliminating the need for a global FD counter or per-worker budget checks.
    fn spill(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let data = std::mem::take(&mut self.pending);
        let file = self.ensure_file()?;
        file.write_all(&data).map_err(J2sError::Io)?;
        self.writer = None; // close FD immediately after write
        Ok(())
    }

    /// Flush all in-memory `pending` data to the temp file unconditionally,
    /// even when below [`SPILL_THRESHOLD`]. Frees the `pending` allocation so
    /// the caller can reclaim that memory. No-op if `pending` is already empty.
    pub fn force_spill(&mut self) -> Result<()> {
        self.spill()
    }

    /// Atomically extract all buffered data for a background async COPY, and reset
    /// this sink so new writes immediately go to a fresh file.
    ///
    /// After this call: `row_count == 0`, `bytes_buffered == 0`, `pending` is empty,
    /// and `temp_file` is `None` (next spill creates a new file).
    /// The returned snapshot owns the old file path; the caller must delete it after COPY.
    /// Call [`apply_flush`] with the returned `row_count` once the COPY succeeds.
    pub fn take_flush_snapshot(&mut self) -> Option<FlushSnapshot> {
        if self.row_count == 0 {
            return None;
        }
        self.writer = None; // close FD if open (auto-hibernate may have done this already)
        let snap = FlushSnapshot {
            copy_sql: self.copy_sql.clone(),
            file_path: self.temp_file.take().map(|p| p.into_path_no_delete()),
            pending: std::mem::take(&mut self.pending),
            row_count: self.row_count,
            table_name: self.table_name.clone(),
        };
        self.row_count = 0;
        self.bytes_buffered = 0;
        Some(snap)
    }

    /// Record that `rows` were successfully COPYed to PG by a background task.
    /// Must be called after [`copy_snapshot_to_pg`] succeeds.
    pub fn apply_flush(&mut self, rows: u64) {
        self.total_flushed += rows;
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
    #[allow(dead_code)]
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

        if !file_data.is_empty() || !self.pending.is_empty() {
            send_copy_data(client, &self.copy_sql, &self.table_name, &file_data, &self.pending).await?;
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
    #[allow(dead_code)]
    pub async fn copy_to_db(self, client: &Client) -> Result<u64> {
        // Destructure — TempFileSink has no Drop; TempFilePath (in temp_file) does.
        let TempFileSink {
            row_count,
            total_flushed,
            bytes_buffered: _,
            pending,
            writer,
            temp_file,
            temp_dir: _,
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

        if !file_data.is_empty() || !pending.is_empty() {
            send_copy_data(client, &copy_sql, &table_name, &file_data, &pending).await?;
        }

        Ok(total_flushed + row_count)
    }
}

/// Send all buffered rows from multiple sinks to PostgreSQL in a single COPY
/// session. All sinks must target the same table (same copy_sql / schema).
///
/// Reduces COPY overhead for tables whose rows are split across many workers:
/// instead of N small COPYs, one COPY streams data from all N sinks in sequence.
async fn stream_sink_to_copy(
    pinned: &mut std::pin::Pin<Box<tokio_postgres::CopyInSink<Bytes>>>,
    sink: TempFileSink,
    table_name: &str,
) -> Result<()> {
    let TempFileSink { row_count, total_flushed, pending, writer, temp_file, .. } = sink;
    if row_count == 0 && total_flushed == 0 { return Ok(()); }
    drop(writer);
    if let Some(ref guard) = temp_file {
        let mut file = tokio::fs::File::open(&guard.0).await.map_err(J2sError::Io)?;
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        loop {
            let n = file.read(&mut buf).await.map_err(J2sError::Io)?;
            if n == 0 { break; }
            pinned.send(Bytes::copy_from_slice(&buf[..n])).await
                .map_err(|e| pg_err(&format!("COPY send {}", table_name), e))?;
        }
    }
    drop(temp_file);
    for chunk in pending.chunks(1024 * 1024) {
        pinned.send(Bytes::copy_from_slice(chunk)).await
            .map_err(|e| pg_err(&format!("COPY send {}", table_name), e))?;
    }
    Ok(())
}

pub async fn merge_copy_to_db(sinks: Vec<TempFileSink>, client: &Client) -> Result<u64> {
    let total_rows: u64 = sinks.iter().map(|s| s.total_flushed + s.row_count).sum();
    if total_rows == 0 { return Ok(0); }
    // Precondition: all sinks target the same table. Guaranteed by flush_task grouping.
    let first = sinks.first().expect("sinks non-empty (total_rows > 0)");
    debug_assert!(
        sinks.iter().all(|s| s.table_name == first.table_name),
        "merge_copy_to_db: sinks target different tables — caller must group by table_name"
    );
    let copy_sql = first.copy_sql.clone();
    let table_name = first.table_name.clone();
    let sink = client.copy_in::<_, Bytes>(&copy_sql).await
        .map_err(|e| pg_err(&format!("COPY INTO {}", table_name), e))?;
    let mut pinned = Box::pin(sink);
    for s in sinks {
        stream_sink_to_copy(&mut pinned, s, &table_name).await?;
    }
    pinned.close().await.map_err(|e| pg_err(&format!("COPY close {}", table_name), e))?;
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
        TempFileSink::new(&schema, "public", None).unwrap()
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

    /// spill() must auto-hibernate: FD is closed immediately after write_all,
    /// without a separate hibernate() call.
    #[cfg(target_os = "linux")]
    #[test]
    fn spill_auto_hibernates_fd() {
        let mut sink = make_sink();
        let row = vec![b'x'; SPILL_THRESHOLD + 1];
        sink.write_row(row).unwrap();

        let path = sink.temp_file.as_ref().unwrap().0.clone();
        // After spill, FD must already be closed — no hibernate() needed.
        assert!(!sink.is_open(), "is_open() must be false immediately after spill");
        assert!(!fd_points_to(&path), "FD must be closed immediately after spill");
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
        assert!(!sink.is_open(), "FD must be closed immediately after auto-hibernate spill");
        assert!(sink.temp_file.is_some(), "temp file must exist after spill");
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

    /// is_open() must be false after a spill (auto-hibernate) and remains false
    /// after an explicit hibernate() call (which is now a no-op in this path).
    #[test]
    fn is_open_false_after_spill() {
        let mut sink = make_sink();

        let row = vec![b'y'; SPILL_THRESHOLD + 1];
        sink.write_row(row).unwrap();
        assert!(!sink.is_open(), "spill must auto-hibernate — no explicit call needed");

        sink.hibernate().unwrap();
        assert!(!sink.is_open(), "hibernate after auto-hibernate must remain false");
    }

    // -------------------------------------------------------------------------
    // force_spill tests
    // -------------------------------------------------------------------------

    /// force_spill on an empty sink must not create a temp file.
    #[test]
    fn force_spill_on_empty_sink_is_noop() {
        let mut sink = make_sink();
        sink.force_spill().unwrap();
        assert!(sink.pending.is_empty());
        assert!(sink.temp_file.is_none(), "no temp file must be created for empty sink");
        assert_eq!(sink.row_count, 0);
    }

    /// force_spill must flush pending to disk even when below SPILL_THRESHOLD.
    #[test]
    fn force_spill_below_threshold_clears_pending() {
        let mut sink = make_sink();
        sink.write_row(b"row\n".to_vec()).unwrap();
        assert!(!sink.pending.is_empty(), "pending must hold data before force_spill");

        sink.force_spill().unwrap();

        assert!(sink.pending.is_empty(), "pending must be empty after force_spill");
        assert!(sink.temp_file.is_some(), "temp file must exist after force_spill");
        assert_eq!(sink.row_count, 1, "row_count must be preserved");
    }

    /// force_spill must not alter bytes_buffered.
    #[test]
    fn force_spill_preserves_bytes_buffered() {
        let mut sink = make_sink();
        let row = b"hello\n".to_vec();
        let expected = row.len() as u64;
        sink.write_row(row).unwrap();
        sink.force_spill().unwrap();
        assert_eq!(sink.bytes_buffered, expected);
    }

    /// force_spill must auto-hibernate: FD is closed immediately after write_all.
    /// A subsequent hibernate() is a no-op (already closed).
    #[test]
    fn force_spill_then_hibernate_releases_fd() {
        let mut sink = make_sink();
        sink.write_row(b"row\n".to_vec()).unwrap();
        sink.force_spill().unwrap();
        assert!(!sink.is_open(), "force_spill must auto-hibernate the FD");
        sink.hibernate().unwrap();
        assert!(!sink.is_open());
        assert!(sink.pending.is_empty());
    }

    /// force_spill must propagate IO errors — deleting the temp file between
    /// spills simulates a real failure (disk full, removed by another process).
    ///
    /// Hibernate is required before deletion: on Linux, an open FD to an unlinked
    /// file remains valid. Closing the FD first forces ensure_file() to reopen
    /// the (now missing) path, which returns NotFound.
    #[test]
    fn force_spill_propagates_io_error_on_deleted_temp_file() {
        let mut sink = make_sink();
        sink.write_row(b"row\n".to_vec()).unwrap();
        sink.force_spill().unwrap(); // creates temp file, FD open
        sink.hibernate().unwrap();   // close FD so the next open attempt hits the path

        // Delete the temp file externally to simulate an IO failure.
        let path = sink.temp_file.as_ref().unwrap().0.clone();
        std::fs::remove_file(&path).unwrap();

        // Write a second row so pending is non-empty again.
        sink.write_row(b"row2\n".to_vec()).unwrap();

        // force_spill must fail: FD is closed, file is gone → OpenOptions::open → NotFound.
        let result = sink.force_spill();
        assert!(result.is_err(), "force_spill must propagate IO error when temp file is deleted");
    }

    /// Chunked read of a file larger than 4 MiB must accumulate all bytes without
    /// loading the full content at once. This validates the streaming read pattern
    /// used by merge_copy_to_db independently of a PostgreSQL connection.
    #[tokio::test]
    async fn stream_file_reads_all_bytes_in_chunks() {
        use tokio::io::AsyncReadExt;

        let file_size = 5 * 1024 * 1024; // 5 MiB
        let content: Vec<u8> = (0..file_size).map(|i| (i % 256) as u8).collect();

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut tmp, &content).unwrap();
        let path = tmp.path().to_path_buf();

        let mut file = tokio::fs::File::open(&path).await.unwrap();
        let mut buf = vec![0u8; 4 * 1024 * 1024];
        let mut received: Vec<u8> = Vec::new();
        loop {
            let n = file.read(&mut buf).await.unwrap();
            if n == 0 { break; }
            received.extend_from_slice(&buf[..n]);
        }

        assert_eq!(received.len(), file_size, "all bytes must be read across chunks");
        assert_eq!(received, content, "byte content must be identical");
    }

    /// With temp_dir=None, spilled file lands in the system temp directory.
    #[test]
    fn temp_dir_none_uses_system_temp() {
        let mut sink = make_sink();
        sink.write_row(vec![b'x'; SPILL_THRESHOLD + 1]).unwrap();
        let path = sink.temp_file.as_ref().unwrap().0.clone();
        assert!(
            path.starts_with(std::env::temp_dir()),
            "expected system temp dir {:?}, got {:?}",
            std::env::temp_dir(),
            path
        );
    }

    /// With a custom temp_dir, spilled file lands in that directory.
    #[test]
    fn temp_dir_custom_creates_file_in_dir() {
        let dir = tempfile::tempdir().unwrap();
        let mut schema = TableSchema::new("t".to_string(), vec!["t".to_string()], 0);
        schema.columns.push(ColumnSchema {
            name: "col".to_string(),
            original_name: "col".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        let mut sink = TempFileSink::new(&schema, "public", Some(dir.path())).unwrap();
        sink.write_row(vec![b'x'; SPILL_THRESHOLD + 1]).unwrap();
        let path = sink.temp_file.as_ref().unwrap().0.clone();
        assert!(
            path.starts_with(dir.path()),
            "expected custom temp dir {:?}, got {:?}",
            dir.path(),
            path
        );
    }

    /// take_flush_snapshot resets sink state and returns extractable data.
    /// apply_flush updates total_flushed. Next writes go to a fresh file.
    #[test]
    fn take_flush_snapshot_resets_sink_and_preserves_data() {
        let mut sink = make_sink();
        sink.write_row(b"row1\n".to_vec()).unwrap();
        sink.write_row(b"row2\n".to_vec()).unwrap();
        let expected_bytes = b"row1\nrow2\n".len();

        let snap = sink.take_flush_snapshot().unwrap();
        assert_eq!(snap.row_count, 2);
        assert_eq!(snap.pending, b"row1\nrow2\n");
        assert!(snap.file_path.is_none(), "no spill happened so no file");

        // Sink is reset: ready for new data
        assert_eq!(sink.row_count, 0);
        assert_eq!(sink.bytes_buffered, 0);
        assert!(sink.pending.is_empty());
        assert_eq!(sink.total_flushed, 0, "apply_flush not called yet");

        sink.apply_flush(snap.row_count);
        assert_eq!(sink.total_flushed, 2);

        // New writes go to a fresh file
        sink.write_row(b"row3\n".to_vec()).unwrap();
        assert_eq!(sink.row_count, 1);
        assert_eq!(sink.bytes_buffered, b"row3\n".len() as u64);
        let _ = expected_bytes; // suppress unused warning
    }

    /// take_flush_snapshot with spilled data: file_path is set, next spill creates new file.
    #[test]
    fn take_flush_snapshot_with_spilled_data_uses_new_file() {
        let mut sink = make_sink();
        // Force a spill
        sink.write_row(vec![b'x'; SPILL_THRESHOLD + 1]).unwrap();
        assert!(sink.temp_file.is_some(), "spill must have created temp file");
        let old_path = sink.temp_file.as_ref().unwrap().0.clone();

        // Write one more small row (stays in pending)
        sink.write_row(b"small\n".to_vec()).unwrap();

        let snap = sink.take_flush_snapshot().unwrap();
        assert_eq!(snap.file_path, Some(old_path.clone()), "snapshot gets old file path");
        assert_eq!(&snap.pending, b"small\n");
        assert!(sink.temp_file.is_none(), "sink no longer owns the file");

        // New spill creates a different file
        sink.write_row(vec![b'y'; SPILL_THRESHOLD + 1]).unwrap();
        let new_path = sink.temp_file.as_ref().unwrap().0.clone();
        assert_ne!(new_path, old_path, "new writes use a different temp file");

        // Old file still exists (not deleted yet — snapshot owns it)
        assert!(old_path.exists(), "old file must still exist while snapshot is alive");
        // Simulate flusher deleting it
        std::fs::remove_file(&old_path).unwrap();
    }

    /// take_flush_snapshot on empty sink returns None.
    #[test]
    fn take_flush_snapshot_on_empty_sink_is_none() {
        let mut sink = make_sink();
        assert!(sink.take_flush_snapshot().is_none());
    }
}
