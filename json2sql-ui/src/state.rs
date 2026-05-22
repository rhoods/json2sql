use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;

use urlencoding::encode;
use zeroize::{Zeroize, Zeroizing};
use json2sql::io::progress_event::ProgressEvent;
use json2sql::schema::naming::{ColumnCollision, TruncatedName};
use json2sql::schema::finalizer::OverflowWarning;
use json2sql::schema::stats::ColumnStats;
use json2sql::schema::table_schema::{TableSchema, WideStrategy};

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
    /// FK constraints that failed after import (non-fatal; PK failures are errors).
    pub constraint_warning_count: u64,
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
    /// Original schemas from pass1 — never mutated after being set.
    pub schemas: Vec<TableSchema>,
    /// Tables that were auto-converted to JSONB because they exceeded the 1600-column PG limit.
    pub overflow_warnings: Vec<OverflowWarning>,
    /// User-chosen strategy overrides: table_name → WideStrategy.
    /// Applied on top of `schemas` at DDL generation and import time.
    pub strategy_overrides: HashMap<String, WideStrategy>,
    /// Set of table indices currently selected in the Strategy panel (Ctrl+click multi-select).
    /// The Preview panel and right-panel configurator always operate on `last_selected_idx`.
    pub selected_table_indices: HashSet<usize>,
    /// Index of the last table clicked — drives the center/right panels in Strategy and Preview.
    pub last_selected_idx: usize,

    // — Pass 1 metadata (also persisted in SchemaSnapshot) —
    pub truncated_names: Vec<TruncatedName>,
    pub column_collisions: Vec<ColumnCollision>,
    pub pass1_stats: Vec<ColumnStats>,
    /// True when schemas were loaded from a saved snapshot rather than a live Pass 1 run.
    pub schema_snapshot_loaded: bool,

    // — Screen 5 —
    pub pass2_progress: Pass2Progress,

    /// Handle to the currently running Pass 1 or Pass 2 task.
    /// Set by the screen that spawns the task; cleared by `cancel()`.
    pub abort_handle: Option<tokio::task::AbortHandle>,

    /// Number of worker threads for Pass 1 schema inference (1 = sequential).
    pub workers: usize,
    /// Number of parallel PostgreSQL connections for Pass 2 COPY (default: min(cpus, 8)).
    pub pass2_parallel: usize,
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
            overflow_warnings: Vec::new(),
            strategy_overrides: HashMap::new(),
            selected_table_indices: HashSet::from([0]),
            last_selected_idx: 0,
            truncated_names: Vec::new(),
            column_collisions: Vec::new(),
            pass1_stats: Vec::new(),
            schema_snapshot_loaded: false,
            pass2_progress: Pass2Progress::default(),
            abort_handle: None,
            workers: 1,
            pass2_parallel: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .min(8),
        }
    }
}

impl AppState {

    /// Populate AppState from a loaded SchemaSnapshot.
    /// Sets schema_snapshot_loaded = true so Setup screen can adapt its UI.
    pub fn load_snapshot(&mut self, snapshot: json2sql::schema::persistence::SchemaSnapshot) {
        self.schemas = snapshot.schemas;
        self.truncated_names = snapshot.truncated_names;
        self.column_collisions = snapshot.column_collisions;
        self.pass1_stats = snapshot.stats;
        self.strategy_overrides = snapshot.strategy_overrides;
        self.pass1_progress.rows_scanned = snapshot.total_rows;
        self.pass1_progress.done = true;
        self.schema_snapshot_loaded = true;
        self.selected_table_indices = HashSet::from([0]);
        self.last_selected_idx = 0;
    }

    /// Remove a loaded snapshot and restore the default Pass 1 flow.
    /// Preserves source_file, pg config, and pg_schema — only clears schema data.
    pub fn clear_snapshot(&mut self) {
        self.schemas = Vec::new();
        self.overflow_warnings = Vec::new();
        self.strategy_overrides = std::collections::HashMap::new();
        self.truncated_names = Vec::new();
        self.column_collisions = Vec::new();
        self.pass1_stats = Vec::new();
        self.pass1_progress = Pass1Progress::default();
        self.selected_table_indices = HashSet::from([0]);
        self.last_selected_idx = 0;
        self.schema_snapshot_loaded = false;
    }

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
        self.overflow_warnings = Vec::new();
        self.strategy_overrides = HashMap::new();
        self.truncated_names = Vec::new();
        self.column_collisions = Vec::new();
        self.pass1_stats = Vec::new();
        self.schema_snapshot_loaded = false;
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
            Pass2Done { total_rows, anomaly_count, constraint_warning_count } => {
                self.pass2_progress.rows_processed = total_rows;
                self.pass2_progress.total_anomalies = anomaly_count;
                self.pass2_progress.constraint_warning_count = constraint_warning_count;
                self.pass2_progress.done = true;
                self.pass2_progress.push_log(format!(
                    "Import complete: {} rows, {} anomalies, {} FK warnings",
                    total_rows, anomaly_count, constraint_warning_count
                ));
            }
            Pass2Error { table_name, message } => {
                self.pass2_progress.push_log(format!(
                    "Error in {}: {}",
                    table_name, message
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clear_snapshot_resets_schema_fields_but_keeps_pg_and_source() {
        use json2sql::schema::persistence::SchemaSnapshot;
        use std::collections::HashMap;

        let mut s = AppState::default();
        s.source_file = Some(std::path::PathBuf::from("/tmp/data.json"));
        s.pg.host = "myhost".to_string();
        s.load_snapshot(SchemaSnapshot {
            version: 1,
            total_rows: 10,
            schemas: vec![],
            truncated_names: vec![],
            column_collisions: vec![],
            stats: vec![],
            strategy_overrides: HashMap::new(),
        });

        s.clear_snapshot();

        assert!(!s.schema_snapshot_loaded);
        assert!(s.schemas.is_empty());
        assert!(s.strategy_overrides.is_empty());
        assert!(!s.pass1_progress.done);
        // source_file and pg are preserved
        assert!(s.source_file.is_some());
        assert_eq!(s.pg.host, "myhost");
    }

    #[test]
    fn load_snapshot_populates_state_and_sets_flag() {
        use json2sql::schema::persistence::SchemaSnapshot;
        use std::collections::HashMap;

        let snapshot = SchemaSnapshot {
            version: 1,
            total_rows: 42,
            schemas: vec![],
            truncated_names: vec![],
            column_collisions: vec![],
            stats: vec![],
            strategy_overrides: {
                let mut m = HashMap::new();
                m.insert("t".to_string(), json2sql::schema::table_schema::WideStrategy::Jsonb);
                m
            },
        };

        let mut s = AppState::default();
        s.load_snapshot(snapshot);

        assert!(s.schema_snapshot_loaded);
        assert_eq!(s.pass1_progress.rows_scanned, 42);
        assert!(matches!(
            s.strategy_overrides.get("t"),
            Some(json2sql::schema::table_schema::WideStrategy::Jsonb)
        ));
    }

    #[test]
    fn snapshot_fields_default_to_empty_and_false() {
        let s = AppState::default();
        assert!(!s.schema_snapshot_loaded);
        assert!(s.truncated_names.is_empty());
        assert!(s.column_collisions.is_empty());
        assert!(s.pass1_stats.is_empty());
    }

    #[test]
    fn cancel_resets_snapshot_fields() {
        let mut s = AppState::default();
        s.schema_snapshot_loaded = true;
        s.truncated_names.push(json2sql::schema::naming::TruncatedName {
            original_path: "a.b".to_string(),
            full_name: "a_b_long".to_string(),
            pg_name: "a_b".to_string(),
        });
        s.cancel();
        assert!(!s.schema_snapshot_loaded);
        assert!(s.truncated_names.is_empty());
        assert!(s.column_collisions.is_empty());
        assert!(s.pass1_stats.is_empty());
    }

    #[test]
    fn pass2_parallel_default_is_in_valid_range() {
        let s = AppState::default();
        assert!(s.pass2_parallel >= 1, "pass2_parallel must be at least 1");
        assert!(s.pass2_parallel <= 32, "pass2_parallel should not exceed 32");
    }

    #[test]
    fn pass2_parallel_matches_available_cpus_capped_at_8() {
        let s = AppState::default();
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        assert_eq!(s.pass2_parallel, cpus.min(8));
    }

    // --- format_bytes ---

    #[test]
    fn format_bytes_gigabytes() {
        assert_eq!(format_bytes(2_000_000_000), "2.0 GB");
    }

    #[test]
    fn format_bytes_megabytes() {
        assert_eq!(format_bytes(500_000_000), "500 MB");
    }

    #[test]
    fn format_bytes_kilobytes() {
        assert_eq!(format_bytes(500_000), "500 KB");
    }

    #[test]
    fn format_bytes_sub_mb_shows_kb() {
        assert_eq!(format_bytes(50_000), "50 KB");
    }

    #[test]
    fn format_bytes_gb_boundary() {
        assert_eq!(format_bytes(1_000_000_000), "1.0 GB");
    }
}
