//! Sink `PostgreSQL` COPY — buffer disque et construction de lignes CSV.
//!
//! Un *sink* est la destination finale dans le pipeline d'import (source → transform → sink).
//! `TempFileSink` accumule les lignes CSV dans un fichier temporaire (spill à 4 MiB),
//! puis les envoie en bloc via `COPY … FROM STDIN` à la fin du Pass 2.
//! `RowBuilder` construit une ligne CSV COPY texte champ par champ (valeurs séparées par `\t`).

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
use crate::db::error::pg_err;
use crate::error::{J2sError, Result};
use crate::schema::table_schema::TableSchema;


/// NULL representation in `PostgreSQL` COPY text format.
pub const COPY_NULL: &str = "\\N";
/// Column delimiter in COPY text format.
pub const COPY_DELIMITER: u8 = b'\t';
/// Row data accumulates in memory up to this size before being spilled to disk.
/// Large batches amortize the syscall overhead of `write()` and `open()/close()`.
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

/// Fails with `Io(NotFound)` if `file_path` is `Some` but the file no longer exists,
/// preventing an orphaned COPY session that would otherwise be opened first.
async fn verify_spill_file_exists(file_path: &Option<PathBuf>) -> Result<()> {
    if let Some(ref p) = file_path {
        if !tokio::fs::try_exists(p).await.map_err(J2sError::Io)? {
            return Err(J2sError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("spill file missing before COPY: {}", p.display()),
            )));
        }
    }
    Ok(())
}

/// Best-effort deletion of the spill file. Logs I/O errors to stderr.
async fn cleanup_spill_file(file_path: &Option<PathBuf>) {
    if let Some(ref p) = file_path {
        if let Err(e) = tokio::fs::remove_file(p).await {
            eprintln!("WARNING: failed to remove spill file {}: {e}", p.display());
        }
    }
}

/// Copy the data in `snap` to `PostgreSQL`, then delete the spill file.
/// Returns the number of rows sent (= `snap.row_count`).
pub async fn copy_snapshot_to_pg(snap: FlushSnapshot, client: &Client) -> Result<u64> {
    let FlushSnapshot { copy_sql, file_path, pending, row_count, table_name } = snap;
    if row_count == 0 {
        cleanup_spill_file(&file_path).await;
        return Ok(0);
    }
    if let Err(e) = verify_spill_file_exists(&file_path).await {
        cleanup_spill_file(&file_path).await;
        return Err(e);
    }
    let sink = client.copy_in::<_, Bytes>(&copy_sql).await
        .map_err(|e| pg_err(&format!("COPY INTO {table_name}"), &e))?;
    let mut pinned = Box::pin(sink);
    if let Some(ref p) = file_path {
        let result = stream_file_chunks(p, &table_name, &mut pinned).await;
        cleanup_spill_file(&file_path).await;
        result?;
    }
    for chunk in pending.chunks(1024 * 1024) {
        pinned.send(Bytes::copy_from_slice(chunk)).await
            .map_err(|e| pg_err(&format!("COPY send {table_name}"), &e))?;
    }
    pinned.close().await.map_err(|e| pg_err(&format!("COPY close {table_name}"), &e))?;
    Ok(row_count)
}

// ---------------------------------------------------------------------------
// TempFileSink
// ---------------------------------------------------------------------------

/// Buffers rows for one table during Pass 2, then COPYs them to `PostgreSQL`.
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
/// This design avoids flushing small `BufWriter` buffers on every insert, which
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
    /// Total bytes sent directly to PG via the copy_direct channel (Phase A, no disk).
    pub bytes_sent_direct: u64,
    /// Total bytes written to disk across all spills (auto + force). Phase B path.
    pub bytes_spilled: u64,
    /// In-memory row data. Survives hibernation. Spilled to disk when it grows
    /// past `SPILL_THRESHOLD`.
    pending: Vec<u8>,
    /// Raw file descriptor. Open only while a spill is in progress.
    /// None = FD released (hibernated or no spill yet).
    writer: Option<File>,
    /// Keeps the temp file alive between spills. None until the first spill.
    temp_file: Option<TempFilePath>,
    /// Directory for the spill temp file. None = system default (`std::env::temp_dir()`).
    temp_dir: Option<PathBuf>,
    copy_sql: String,
}

impl TempFileSink {
    pub fn new(schema: &TableSchema, pg_schema: &str, temp_dir: Option<&Path>) -> Self {
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

        Self {
            table_name: schema.name.clone(),
            row_count: 0,
            total_flushed: 0,
            bytes_buffered: 0,
            bytes_sent_direct: 0,
            bytes_spilled: 0,
            pending: Vec::new(),
            writer: None,
            temp_file: None,
            temp_dir: temp_dir.map(std::path::Path::to_path_buf),
            copy_sql,
        }
    }

    /// Returns true when a temp-file FD is currently held open (i.e. a spill
    /// is in progress or has just completed and hibernate has not been called).
    #[allow(dead_code)] // public API — not yet used in binary, available for future callers
    #[must_use]
    pub const fn is_open(&self) -> bool {
        self.writer.is_some()
    }

    /// Close the open file descriptor without touching the in-memory `pending`
    /// buffer. This is cheap: a single `close()` syscall regardless of how
    /// many rows are buffered. Data in `pending` is preserved for the next write.
    ///
    /// No-op when no FD is held.
    #[allow(dead_code)] // public API — not yet used in binary, available for future callers
    pub fn hibernate(&mut self) {
        self.writer = None; // drop File → close FD; pending stays in memory
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
        let spill_len = self.pending.len() as u64;
        self.ensure_file()?;
        // Split-borrow: self.writer (via file) and self.pending are distinct fields.
        // pending is cleared only after a successful write to avoid data loss on I/O error.
        if let Some(ref mut file) = self.writer {
            file.write_all(&self.pending).map_err(J2sError::Io)?;
        }
        std::mem::take(&mut self.pending);
        self.writer = None; // close FD immediately after write
        self.bytes_spilled += spill_len;
        Ok(())
    }

    /// Flush all in-memory `pending` data to the temp file unconditionally,
    /// even when below [`SPILL_THRESHOLD`]. Frees the `pending` allocation so
    /// the caller can reclaim that memory. No-op if `pending` is already empty.
    pub fn force_spill(&mut self) -> Result<()> {
        self.spill()
    }

    /// Returns true if there is in-memory data not yet spilled to disk.
    /// Only used in tests to verify MIN_SPILL_BYTES behavior.
    #[cfg(test)]
    pub fn has_pending_data(&self) -> bool {
        !self.pending.is_empty()
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
            file_path: self.temp_file.take().map(TempFilePath::into_path_no_delete),
            pending: std::mem::take(&mut self.pending),
            row_count: self.row_count,
            table_name: self.table_name.clone(),
        };
        self.row_count = 0;
        self.bytes_buffered = 0;
        Some(snap)
    }

    /// Record that `rows` were successfully `COPYed` to PG by a background task.
    /// Must be called after [`copy_snapshot_to_pg`] succeeds.
    pub const fn apply_flush(&mut self, rows: u64) {
        self.total_flushed += rows;
    }

    pub(crate) fn copy_sql(&self) -> &str {
        &self.copy_sql
    }

    pub fn write_row(&mut self, row: &[u8]) -> Result<()> {
        self.bytes_buffered += row.len() as u64;
        self.pending.extend_from_slice(row);
        self.row_count += 1;
        if self.pending.len() >= SPILL_THRESHOLD {
            self.spill()?;
        }
        Ok(())
    }

}

/// Stream a file to an open COPY sink in 4 MiB chunks, avoiding a full in-memory load.
async fn stream_file_chunks(
    path: &Path,
    table_name: &str,
    pinned: &mut std::pin::Pin<Box<tokio_postgres::CopyInSink<Bytes>>>,
) -> Result<()> {
    let mut file = tokio::fs::File::open(path).await.map_err(|e| {
        if e.kind() == std::io::ErrorKind::NotFound {
            J2sError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("spill file missing during COPY {table_name}: {}", path.display()),
            ))
        } else {
            J2sError::Io(e)
        }
    })?;
    let mut buf = bytes::BytesMut::with_capacity(4 * 1024 * 1024);
    loop {
        tokio::io::AsyncReadExt::read_buf(&mut file, &mut buf).await.map_err(J2sError::Io)?;
        if buf.is_empty() { break; }
        pinned.send(buf.split().freeze()).await
            .map_err(|e| pg_err(&format!("COPY send {table_name}"), &e))?;
    }
    Ok(())
}

/// Send all buffered rows from multiple sinks to `PostgreSQL` in a single COPY
/// session. All sinks must target the same table (same `copy_sql` / schema).
///
/// Reduces COPY overhead for tables whose rows are split across many workers:
/// instead of N small COPYs, one COPY streams data from all N sinks in sequence.
async fn stream_sink_to_copy(
    pinned: &mut std::pin::Pin<Box<tokio_postgres::CopyInSink<Bytes>>>,
    sink: TempFileSink,
    table_name: &str,
) -> Result<()> {
    let TempFileSink { row_count, total_flushed: _, pending, writer, temp_file, .. } = sink;
    if row_count == 0 { return Ok(()); }
    drop(writer);
    if let Some(ref guard) = temp_file {
        stream_file_chunks(&guard.0, table_name, pinned).await?;
    }
    drop(temp_file);
    for chunk in pending.chunks(1024 * 1024) {
        pinned.send(Bytes::copy_from_slice(chunk)).await
            .map_err(|e| pg_err(&format!("COPY send {table_name}"), &e))?;
    }
    Ok(())
}

/// Stream one `FlushSnapshot` into an already-open COPY STDIN session.
/// Cleans up the spill file (if any) after streaming. Does NOT close the session.
/// Called by the COPY-direct task to feed multiple snapshots into a single connection.
pub(crate) async fn stream_snapshot_to_open_copy(
    snap: FlushSnapshot,
    pinned: &mut std::pin::Pin<Box<tokio_postgres::CopyInSink<Bytes>>>,
) -> Result<()> {
    let FlushSnapshot { file_path, pending, table_name, .. } = snap;
    if let Err(e) = verify_spill_file_exists(&file_path).await {
        cleanup_spill_file(&file_path).await;
        return Err(e);
    }
    if let Some(ref p) = file_path {
        let result = stream_file_chunks(p, &table_name, pinned).await;
        cleanup_spill_file(&file_path).await;
        result?;
    }
    for chunk in pending.chunks(1024 * 1024) {
        pinned.send(Bytes::copy_from_slice(chunk)).await
            .map_err(|e| pg_err(&format!("COPY send {table_name}"), &e))?;
    }
    Ok(())
}

pub async fn merge_copy_to_db(sinks: Vec<TempFileSink>, client: &Client) -> Result<u64> {
    let phase_b_rows: u64 = sinks.iter().map(|s| s.row_count).sum();
    if phase_b_rows == 0 { return Ok(0); }
    // Precondition: all sinks target the same table. Guaranteed by flush_task grouping.
    let first = sinks.first().expect("sinks non-empty (phase_b_rows > 0)");
    debug_assert!(
        sinks.iter().all(|s| s.table_name == first.table_name),
        "merge_copy_to_db: sinks target different tables — caller must group by table_name"
    );
    let copy_sql = first.copy_sql.clone();
    let table_name = first.table_name.clone();
    let sink = client.copy_in::<_, Bytes>(&copy_sql).await
        .map_err(|e| pg_err(&format!("COPY INTO {table_name}"), &e))?;
    let mut pinned = Box::pin(sink);
    for s in sinks {
        stream_sink_to_copy(&mut pinned, s, &table_name).await?;
    }
    pinned.close().await.map_err(|e| pg_err(&format!("COPY close {table_name}"), &e))?;
    Ok(phase_b_rows)
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
        TempFileSink::new(&schema, "public", None)
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
        sink.write_row(b"row\n").unwrap();
        assert!(!sink.is_open(), "no FD should be opened below spill threshold");
        assert!(sink.temp_file.is_none(), "no temp file should be created below threshold");
    }

    /// Data written across multiple writes and hibernations must all end up in
    /// `pending` (when no spill has occurred).
    #[test]
    fn write_after_hibernate_appends() {
        let mut sink = make_sink();

        sink.write_row(b"row1\n").unwrap();
        sink.hibernate();
        sink.write_row(b"row2\n").unwrap();
        sink.hibernate();

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
        sink.write_row(&row).unwrap();

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
        sink.write_row(&row1).unwrap();
        assert_eq!(sink.bytes_buffered, len1);

        let row2 = b"world\n".to_vec();
        let len2 = row2.len() as u64;
        sink.write_row(&row2).unwrap();
        assert_eq!(sink.bytes_buffered, len1 + len2);
    }

    #[test]
    fn bytes_buffered_survives_spill() {
        let mut sink = make_sink();
        let large_row = vec![b'x'; SPILL_THRESHOLD + 1];
        let expected = large_row.len() as u64;
        sink.write_row(&large_row).unwrap();
        assert!(!sink.is_open(), "FD must be closed immediately after auto-hibernate spill");
        assert!(sink.temp_file.is_some(), "temp file must exist after spill");
        assert_eq!(sink.bytes_buffered, expected, "bytes_buffered must reflect spilled data");
    }

    // -------------------------------------------------------------------------
    // merge_copy_to_db tests (logic only — no PG connection required)
    // -------------------------------------------------------------------------

    /// merge_copy_to_db must return only Phase B rows (row_count), not total_flushed + row_count.
    /// total_flushed is already tracked in interim_rows by collect_copy_results.
    #[test]
    fn merge_total_rows_sum_is_correct() {
        let mut s1 = make_sink();
        s1.write_row(b"row1\n").unwrap();
        s1.write_row(b"row2\n").unwrap();

        let mut s2 = make_sink();
        s2.write_row(b"row3\n").unwrap();

        let phase_b: u64 = [&s1, &s2].iter().map(|s| s.row_count).sum();
        assert_eq!(phase_b, 3);
    }

    /// An interim-only sink (total_flushed > 0, row_count == 0) contributes 0 to Phase B.
    #[test]
    fn interim_sink_contributes_zero_to_phase_b() {
        let mut sink = make_sink();
        sink.write_row(b"row\n").unwrap();
        let snap = sink.take_flush_snapshot().unwrap();
        sink.apply_flush(snap.row_count);
        assert_eq!(sink.row_count, 0, "no Phase B rows for interim-only sink");
        assert_eq!(sink.total_flushed, 1, "Phase A row tracked separately");
        let phase_b: u64 = sink.row_count;
        assert_eq!(phase_b, 0);
    }

    /// Empty sinks (row_count == 0) must not count toward the total.
    #[test]
    fn merge_empty_sinks_contribute_zero_rows() {
        let s_empty = make_sink();
        assert_eq!(s_empty.row_count, 0);
        assert_eq!(s_empty.total_flushed, 0);
        let phase_b: u64 = [&s_empty].iter().map(|s| s.row_count).sum();
        assert_eq!(phase_b, 0);
    }

    /// Pending bytes from multiple sinks must be individually accessible
    /// (verifies the data structure is correct for the merge streaming loop).
    #[test]
    fn merge_pending_bytes_preserved_across_sinks() {
        let mut s1 = make_sink();
        s1.write_row(b"abc\n").unwrap();

        let mut s2 = make_sink();
        s2.write_row(b"def\n").unwrap();
        s2.write_row(b"ghi\n").unwrap();

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
        sink.write_row(&row).unwrap();
        assert!(!sink.is_open(), "spill must auto-hibernate — no explicit call needed");

        sink.hibernate();
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
        sink.write_row(b"row\n").unwrap();
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
        sink.write_row(&row).unwrap();
        sink.force_spill().unwrap();
        assert_eq!(sink.bytes_buffered, expected);
    }

    /// force_spill must auto-hibernate: FD is closed immediately after write_all.
    /// A subsequent hibernate() is a no-op (already closed).
    #[test]
    fn force_spill_then_hibernate_releases_fd() {
        let mut sink = make_sink();
        sink.write_row(b"row\n").unwrap();
        sink.force_spill().unwrap();
        assert!(!sink.is_open(), "force_spill must auto-hibernate the FD");
        sink.hibernate();
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
        sink.write_row(b"row\n").unwrap();
        sink.force_spill().unwrap(); // creates temp file, FD open
        sink.hibernate();   // close FD so the next open attempt hits the path

        // Delete the temp file externally to simulate an IO failure.
        let path = sink.temp_file.as_ref().unwrap().0.clone();
        std::fs::remove_file(&path).unwrap();

        // Write a second row so pending is non-empty again.
        sink.write_row(b"row2\n").unwrap();

        // force_spill must fail: FD is closed, file is gone → OpenOptions::open → NotFound.
        let result = sink.force_spill();
        assert!(result.is_err(), "force_spill must propagate IO error when temp file is deleted");
    }

    /// After a failed force_spill, `pending` must still contain the original data.
    /// Data must never be discarded before the write succeeds.
    #[test]
    fn spill_pending_preserved_on_io_error() {
        let mut sink = make_sink();
        sink.write_row(b"row\n").unwrap();
        sink.force_spill().unwrap();
        sink.hibernate();

        let path = sink.temp_file.as_ref().unwrap().0.clone();
        std::fs::remove_file(&path).unwrap();

        sink.write_row(b"row2\n").unwrap();
        assert_eq!(&sink.pending, b"row2\n", "pending must contain row2 before failed spill");

        let result = sink.force_spill();
        assert!(result.is_err(), "force_spill must propagate IO error");
        assert_eq!(&sink.pending, b"row2\n", "pending must be preserved after failed spill");
    }


    /// With temp_dir=None, spilled file lands in the system temp directory.
    #[test]
    fn temp_dir_none_uses_system_temp() {
        let mut sink = make_sink();
        sink.write_row(&vec![b'x'; SPILL_THRESHOLD + 1]).unwrap();
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
        let mut sink = TempFileSink::new(&schema, "public", Some(dir.path()));
        sink.write_row(&vec![b'x'; SPILL_THRESHOLD + 1]).unwrap();
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
        sink.write_row(b"row1\n").unwrap();
        sink.write_row(b"row2\n").unwrap();
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
        sink.write_row(b"row3\n").unwrap();
        assert_eq!(sink.row_count, 1);
        assert_eq!(sink.bytes_buffered, b"row3\n".len() as u64);
        let _ = expected_bytes; // suppress unused warning
    }

    /// take_flush_snapshot with spilled data: file_path is set, next spill creates new file.
    #[test]
    fn take_flush_snapshot_with_spilled_data_uses_new_file() {
        let mut sink = make_sink();
        // Force a spill
        sink.write_row(&vec![b'x'; SPILL_THRESHOLD + 1]).unwrap();
        assert!(sink.temp_file.is_some(), "spill must have created temp file");
        let old_path = sink.temp_file.as_ref().unwrap().0.clone();

        // Write one more small row (stays in pending)
        sink.write_row(b"small\n").unwrap();

        let snap = sink.take_flush_snapshot().unwrap();
        assert_eq!(snap.file_path, Some(old_path.clone()), "snapshot gets old file path");
        assert_eq!(&snap.pending, b"small\n");
        assert!(sink.temp_file.is_none(), "sink no longer owns the file");

        // New spill creates a different file
        sink.write_row(&vec![b'y'; SPILL_THRESHOLD + 1]).unwrap();
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

    /// An interim-only sink (all rows already sent via Phase A) has row_count == 0.
    /// stream_sink_to_copy must skip it to avoid opening an empty COPY session.
    #[test]
    fn interim_only_sink_has_zero_row_count() {
        let mut sink = make_sink();
        sink.write_row(b"row\n").unwrap();
        let snap = sink.take_flush_snapshot().unwrap();
        sink.apply_flush(snap.row_count);
        assert_eq!(sink.row_count, 0, "no rows to write in Phase B");
        assert!(sink.total_flushed > 0, "rows already counted from Phase A");
    }

    /// After take_flush_snapshot, both guard fields are zero — the sink is skippable.
    /// Guards trigger_budget_flush's skip condition: bytes_buffered == 0 && row_count == 0.
    #[test]
    fn sink_after_snapshot_satisfies_empty_guard() {
        let mut sink = make_sink();
        sink.write_row(b"row\n").unwrap();
        assert!(sink.take_flush_snapshot().is_some());

        assert_eq!(sink.bytes_buffered, 0, "bytes_buffered must be zero after snapshot");
        assert_eq!(sink.row_count, 0, "row_count must be zero after snapshot");

        // force_spill on this sink must be a no-op (no temp file created).
        sink.force_spill().unwrap();
        assert!(sink.temp_file.is_none(), "no temp file must exist for a post-snapshot empty sink");
    }

    #[tokio::test]
    async fn spill_file_removed_after_stream_error() {
        // Validates the cleanup-before-propagate pattern: remove_file runs even on stream error.
        // Tests `cleanup_spill_file` directly since the full path through copy_snapshot_to_pg
        // requires a live PG connection.
        let f = tempfile::NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();
        drop(f); // disarm RAII guard; we manage the file ourselves
        std::fs::write(&path, b"fake spill data").unwrap();
        assert!(path.exists(), "precondition: file exists before cleanup");
        super::cleanup_spill_file(&Some(path.clone())).await;
        assert!(!path.exists(), "file must be removed even when called after an error");
    }

    #[tokio::test]
    async fn spill_file_cleanup_noop_for_none() {
        // No panic or error when file_path is None.
        super::cleanup_spill_file(&None).await;
    }

    #[test]
    fn force_spill_frees_pending_heap_allocation() {
        // clear() retains Vec capacity; mem::take must drop it to 0 so RSS can be reclaimed.
        let mut sink = make_sink();
        let large_row = vec![b'x'; SPILL_THRESHOLD + 1];
        sink.write_row(&large_row).unwrap(); // triggers spill internally
        // After spill, pending was cleared. With mem::take, capacity must be 0.
        assert_eq!(sink.pending.capacity(), 0, "spill() must free pending Vec capacity");
    }

    #[test]
    fn force_spill_explicit_call_frees_pending_heap() {
        // force_spill() is the public API; same invariant must hold.
        let mut sink = make_sink();
        sink.write_row(b"some data\n").unwrap();
        // manually trigger spill below threshold via force_spill
        sink.force_spill().unwrap();
        assert_eq!(sink.pending.capacity(), 0, "force_spill() must free pending Vec capacity");
    }

    #[tokio::test]
    async fn stream_file_chunks_reads_all_bytes_via_bytesmut() {
        // Validates that the BytesMut::read_buf + split() pattern (used by stream_file_chunks)
        // reads the full file correctly without data loss across multiple iterations.
        // The send-to-CopyInSink path requires PG and is tested via integration tests.
        use bytes::BytesMut;
        use tokio::io::AsyncReadExt;

        let content = b"line1\nline2\nline3\n";
        let mut f = tempfile::NamedTempFile::new().unwrap();
        std::io::Write::write_all(&mut f, content).unwrap();
        let path = f.path().to_path_buf();

        let mut file = tokio::fs::File::open(&path).await.unwrap();
        let mut collected = Vec::new();
        let mut buf = BytesMut::with_capacity(8); // small cap to force multiple iterations; allocated once
        loop {
            file.read_buf(&mut buf).await.unwrap();
            if buf.is_empty() { break; }
            collected.extend_from_slice(&buf.split().freeze()); // split() leaves buf empty with capacity
        }
        assert_eq!(collected, content, "BytesMut::read_buf + split() must read all bytes without data loss");
    }

    #[tokio::test]
    async fn stream_file_chunks_empty_file_produces_no_chunks() {
        use bytes::BytesMut;
        use tokio::io::AsyncReadExt;

        let f = tempfile::NamedTempFile::new().unwrap();
        let mut file = tokio::fs::File::open(f.path()).await.unwrap();
        let mut buf = BytesMut::with_capacity(4 * 1024 * 1024);
        file.read_buf(&mut buf).await.unwrap();
        assert!(buf.is_empty(), "empty file must produce zero bytes on first read_buf");
    }

    #[tokio::test]
    async fn verify_spill_file_exists_ok_for_none() {
        assert!(super::verify_spill_file_exists(&None).await.is_ok());
    }

    #[tokio::test]
    async fn verify_spill_file_exists_ok_when_file_present() {
        let f = tempfile::NamedTempFile::new().unwrap();
        assert!(super::verify_spill_file_exists(&Some(f.path().to_path_buf())).await.is_ok());
    }

    #[tokio::test]
    async fn verify_spill_file_exists_err_when_file_missing() {
        let path = std::path::PathBuf::from("/nonexistent/spill_file_xyz_test.bin");
        let result = super::verify_spill_file_exists(&Some(path)).await;
        assert!(result.is_err(), "missing spill file must return Err before opening COPY session");
        let err_str = result.unwrap_err().to_string();
        assert!(err_str.contains("spill file missing"), "error must identify the cause: {err_str}");
    }

    #[test]
    fn bytes_spilled_increments_on_force_spill() {
        let mut sink = make_sink();
        let row = b"hello\n";
        sink.write_row(row).unwrap();
        assert_eq!(sink.bytes_spilled, 0, "no spill yet");
        sink.force_spill().unwrap();
        assert_eq!(sink.bytes_spilled, row.len() as u64, "bytes_spilled must reflect force_spill bytes");
    }

    #[test]
    fn bytes_spilled_accumulates_across_multiple_force_spills() {
        let mut sink = make_sink();
        let row = b"row\n";
        sink.write_row(row).unwrap();
        sink.force_spill().unwrap();
        sink.write_row(row).unwrap();
        sink.force_spill().unwrap();
        assert_eq!(sink.bytes_spilled, row.len() as u64 * 2, "bytes_spilled accumulates");
    }

    #[test]
    fn bytes_spilled_increments_on_auto_spill_in_write_row() {
        let mut sink = make_sink();
        let large_row = vec![b'x'; SPILL_THRESHOLD + 1];
        sink.write_row(&large_row).unwrap();
        assert_eq!(sink.bytes_spilled, large_row.len() as u64, "auto-spill in write_row must increment bytes_spilled");
    }

    #[test]
    fn bytes_sent_direct_starts_at_zero() {
        let sink = make_sink();
        assert_eq!(sink.bytes_sent_direct, 0);
    }
}
