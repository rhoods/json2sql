use std::collections::VecDeque;
use std::path::PathBuf;

use urlencoding::encode;
use zeroize::{Zeroize, Zeroizing};
use json2sql::io::progress_event::ProgressEvent;
use json2sql::schema::table_schema::TableSchema;

// ---------------------------------------------------------------------------
// Screen navigation
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Debug, Default)]
pub enum AppScreen {
    #[default]
    Setup,
    Analysis,
    Strategy,
    Preview,
    Import,
}

// ---------------------------------------------------------------------------
// PostgreSQL connection config
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct PgConfig {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
}

impl Drop for PgConfig {
    fn drop(&mut self) {
        self.password.zeroize();
    }
}

impl Default for PgConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            database: String::new(),
            username: String::new(),
            password: String::new(),
        }
    }
}

impl PgConfig {
    /// Build a postgres connection URL from the config fields.
    /// All user-provided components are percent-encoded to handle special characters.
    ///
    /// Returns `Zeroizing<String>` so the heap allocation containing the password
    /// is overwritten when the value is dropped, not only when `PgConfig` itself
    /// is dropped (which would be too late for URL copies passed to connect()).
    pub fn to_url(&self) -> Zeroizing<String> {
        // IPv6 addresses must be bracketed; encode host for all other special chars.
        let host = if self.host.contains(':') && !self.host.starts_with('[') {
            format!("[{}]", encode(&self.host))
        } else {
            encode(&self.host).into_owned()
        };

        Zeroizing::new(format!(
            "postgres://{}:{}@{}:{}/{}",
            encode(&self.username), encode(&self.password),
            host, self.port, encode(&self.database)
        ))
    }

    pub fn is_complete(&self) -> bool {
        !self.host.is_empty()
            && !self.database.is_empty()
            && !self.username.is_empty()
            && self.port != 0
    }
}

/// Maximum log lines kept in memory per pass (ring-buffer via VecDeque).
const LOG_MAX: usize = 500;

/// Format a byte count as a human-readable string using SI units (powers of 1 000).
/// Shows KB for < 1 MB so sub-megabyte values are never displayed as "0 MB".
pub fn format_bytes(b: u64) -> String {
    if b >= 1_000_000_000 {
        format!("{:.1} GB", b as f64 / 1_000_000_000.0)
    } else if b >= 1_000_000 {
        format!("{} MB", b / 1_000_000)
    } else {
        format!("{} KB", b / 1_000)
    }
}

// ---------------------------------------------------------------------------
// Pass 1 progress (fed by ProgressEvent stream)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct Pass1Progress {
    pub rows_scanned: u64,
    pub bytes_read: u64,
    pub total_bytes: u64,
    pub tables_count: usize,
    pub columns_count: usize,
    pub log_lines: VecDeque<String>,
    pub done: bool,
}

impl Pass1Progress {
    pub fn push_log(&mut self, line: String) {
        if self.log_lines.len() >= LOG_MAX {
            self.log_lines.pop_front();
        }
        self.log_lines.push_back(line);
    }
}

// ---------------------------------------------------------------------------
// Pass 2 progress
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct Pass2Progress {
    pub rows_processed: u64,
    pub bytes_read: u64,
    pub total_bytes: u64,
    /// Per-table row counts for the right panel.
    pub rows_per_table: std::collections::HashMap<String, u64>,
    pub log_lines: VecDeque<String>,
    pub done: bool,
    pub total_anomalies: u64,
}

impl Pass2Progress {
    pub fn push_log(&mut self, line: String) {
        if self.log_lines.len() >= LOG_MAX {
            self.log_lines.pop_front();
        }
        self.log_lines.push_back(line);
    }
}

// ---------------------------------------------------------------------------
// Root application state
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct AppState {
    pub screen: AppScreen,

    // — Screen 1 —
    pub source_file: Option<PathBuf>,
    pub pg: PgConfig,
    /// Target PostgreSQL schema (default: "public").
    pub pg_schema: String,
    /// Drop and recreate tables before import (destructive — clean slate).
    /// False = CREATE IF NOT EXISTS (safe for reruns, may accumulate data).
    pub drop_existing: bool,
    /// Optional directory where anomaly NDJSON files are streamed during Pass 2.
    pub anomaly_dir: Option<PathBuf>,
    /// True while the "Test connection" check is in flight.
    pub pg_testing: bool,
    /// Some(true/false) after the test completes.
    pub pg_ok: Option<bool>,
    /// Connection error details when the PG health check fails.
    pub pg_error: Option<String>,

    // — Screen 2 —
    pub pass1_progress: Pass1Progress,

    // — Screen 3 / 4 —
    /// Working copy of schemas; mutated by strategy editor (Screen 3).
    pub schemas: Vec<TableSchema>,
    /// Index of the table currently selected in Strategy / Preview panels.
    /// Persisted in AppState so the selection survives navigation between the two screens.
    pub selected_table_idx: usize,

    // — Screen 5 —
    pub pass2_progress: Pass2Progress,

    /// Handle to the currently running Pass 1 or Pass 2 task.
    /// Set by the screen that spawns the task; cleared by `cancel()`.
    pub abort_handle: Option<tokio::task::AbortHandle>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            screen: AppScreen::default(),
            source_file: None,
            pg: PgConfig::default(),
            pg_schema: "public".to_string(),
            drop_existing: false,
            anomaly_dir: None,
            pg_testing: false,
            pg_ok: None,
            pg_error: None,
            pass1_progress: Pass1Progress::default(),
            schemas: Vec::new(),
            selected_table_idx: 0,
            pass2_progress: Pass2Progress::default(),
            abort_handle: None,
        }
    }
}

impl AppState {
    /// Convenience: true when both source file and PG config are ready.
    pub fn ready_to_start(&self) -> bool {
        self.source_file.is_some()
            && self.pg.is_complete()
            && !self.pg_schema.is_empty()
            && self.pg_schema.chars().all(|c| c.is_alphanumeric() || c == '_')
    }

    /// Abort the running task (if any), reset all transient state, and return to Setup.
    /// Preserves `source_file` and `pg` (user preferences).
    /// `drop_existing` is intentionally reset — it is a destructive flag that must be
    /// re-enabled explicitly on each import.
    pub fn cancel(&mut self) {
        if let Some(handle) = self.abort_handle.take() {
            handle.abort();
        }
        self.pass1_progress = Pass1Progress::default();
        self.pass2_progress = Pass2Progress::default();
        self.schemas = Vec::new();
        self.pg_testing = false;
        self.pg_ok = None;
        self.pg_error = None;
        self.drop_existing = false;
        self.screen = AppScreen::Setup;
    }

    /// Apply a `ProgressEvent` coming from a Pass 1 / Pass 2 runner.
    pub fn apply_progress_event(&mut self, event: ProgressEvent) {
        use ProgressEvent::*;
        match event {
            Pass1Progress { rows_scanned, bytes_read, total_bytes } => {
                self.pass1_progress.rows_scanned = rows_scanned;
                self.pass1_progress.bytes_read = bytes_read;
                self.pass1_progress.total_bytes = total_bytes;
                self.pass1_progress.push_log(format!(
                    "Scanned {} records ({} / {})",
                    rows_scanned,
                    format_bytes(bytes_read),
                    format_bytes(total_bytes),
                ));
            }
            Pass1Done { total_rows, tables_count, columns_count } => {
                self.pass1_progress.rows_scanned = total_rows;
                self.pass1_progress.tables_count = tables_count;
                self.pass1_progress.columns_count = columns_count;
                self.pass1_progress.done = true;
                self.pass1_progress.push_log(format!(
                    "Schema complete: {} tables, {} columns",
                    tables_count, columns_count
                ));
            }
            Pass2Progress { rows_processed, bytes_read, total_bytes } => {
                self.pass2_progress.rows_processed = rows_processed;
                self.pass2_progress.bytes_read = bytes_read;
                self.pass2_progress.total_bytes = total_bytes;
            }
            Pass2Flush { table_name, rows_flushed } => {
                *self.pass2_progress.rows_per_table.entry(table_name.clone()).or_default() += rows_flushed;
                self.pass2_progress.push_log(format!(
                    "flush {} ({} rows)",
                    table_name, rows_flushed
                ));
            }
            Pass2Log(msg) => {
                self.pass2_progress.push_log(msg);
            }
            Pass2Done { total_rows, anomaly_count } => {
                self.pass2_progress.rows_processed = total_rows;
                self.pass2_progress.total_anomalies = anomaly_count;
                self.pass2_progress.done = true;
                self.pass2_progress.push_log(format!(
                    "Import complete: {} rows, {} anomalies",
                    total_rows, anomaly_count
                ));
            }
        }
    }
}
