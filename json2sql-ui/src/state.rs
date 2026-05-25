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
// Pass 1 runner defaults
// ---------------------------------------------------------------------------

/// Maximum byte length before a JSON string value is inferred as TEXT (vs VARCHAR).
pub const PASS1_TEXT_THRESHOLD: u32 = 256;
/// Column count above which a table is flagged as "wide" and triggers overflow warnings.
pub const PASS1_WIDE_COLUMN_THRESHOLD: usize = 100;
/// Minimum number of sibling tables required to attempt a sibling-group collapse.
pub const PASS1_SIBLING_THRESHOLD: usize = 3;
/// Minimum pairwise Jaccard similarity for a sibling group to be collapsed.
pub const PASS1_SIBLING_JACCARD: f64 = 0.5;
/// Frequency threshold above which a key is considered "stable" (kept as a column).
pub const PASS1_STABLE_THRESHOLD: f64 = 0.10;
/// Frequency threshold below which a key is considered "rare" (excluded entirely).
pub const PASS1_RARE_THRESHOLD: f64 = 0.001;

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
// ProjectState — Screen 1 (Setup) + global project config
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct ProjectState {
    pub source_file: Option<PathBuf>,
    pub pg: PgConfig,
    /// Target PostgreSQL schema (default: "public").
    pub pg_schema: String,
    /// Drop and recreate tables before import (destructive — clean slate).
    pub drop_existing: bool,
    /// Optional directory where anomaly NDJSON files are streamed during Pass 2.
    pub anomaly_dir: Option<PathBuf>,
    /// True while the "Test connection" check is in flight.
    pub pg_testing: bool,
    /// Some(true/false) after the test completes.
    pub pg_ok: Option<bool>,
    /// Connection error details when the PG health check fails.
    pub pg_error: Option<String>,
    /// Number of worker threads for Pass 1 schema inference (1 = sequential).
    pub workers: usize,
    /// Number of parallel PostgreSQL connections for Pass 2 COPY.
    pub pass2_parallel: usize,
}

impl Default for ProjectState {
    fn default() -> Self {
        Self {
            source_file: None,
            pg: PgConfig::default(),
            pg_schema: "public".to_string(),
            drop_existing: false,
            anomaly_dir: None,
            pg_testing: false,
            pg_ok: None,
            pg_error: None,
            workers: 1,
            pass2_parallel: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .min(8),
        }
    }
}

impl ProjectState {
    pub fn is_complete(&self) -> bool {
        self.source_file.is_some()
            && self.pg.is_complete()
            && !self.pg_schema.is_empty()
            && self.pg_schema.chars().all(|c| c.is_alphanumeric() || c == '_')
    }
}

// ---------------------------------------------------------------------------
// SchemaState — Screens 2, 3, 4 (Analysis, Strategy, Preview)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct SchemaState {
    pub pass1_progress: Pass1Progress,
    /// Original schemas from pass1 — never mutated after being set.
    pub schemas: Vec<TableSchema>,
    /// Tables auto-converted to JSONB (exceeded PostgreSQL 1600-column limit).
    pub overflow_warnings: Vec<OverflowWarning>,
    /// User-chosen strategy overrides: table_name → WideStrategy.
    pub strategy_overrides: HashMap<String, WideStrategy>,
    /// Set of table indices currently selected in the Strategy panel.
    pub selected_table_indices: HashSet<usize>,
    /// Index of the last table clicked — drives center/right panels.
    pub last_selected_idx: usize,
    pub truncated_names: Vec<TruncatedName>,
    pub column_collisions: Vec<ColumnCollision>,
    pub pass1_stats: Vec<ColumnStats>,
    /// True when schemas were loaded from a saved snapshot rather than a live Pass 1 run.
    pub schema_snapshot_loaded: bool,
}

impl Default for SchemaState {
    fn default() -> Self {
        Self {
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
        }
    }
}

impl SchemaState {
    fn clear(&mut self) {
        self.schemas = Vec::new();
        self.overflow_warnings = Vec::new();
        self.strategy_overrides = HashMap::new();
        self.truncated_names = Vec::new();
        self.column_collisions = Vec::new();
        self.pass1_stats = Vec::new();
        self.pass1_progress = Pass1Progress::default();
        self.selected_table_indices = HashSet::from([0]);
        self.last_selected_idx = 0;
        self.schema_snapshot_loaded = false;
    }

    /// Apply a Shift+click range-select.
    ///
    /// Selects all entries in `visible_indices` whose index falls in
    /// `[min(anchor, i), max(anchor, i)]`, skipping rows not in `visible_indices`.
    pub fn apply_shift_click(&mut self, i: usize, anchor: usize, visible_indices: &[usize]) {
        let lo = anchor.min(i);
        let hi = anchor.max(i);
        let range: HashSet<usize> = visible_indices.iter()
            .copied()
            .filter(|&idx| idx >= lo && idx <= hi)
            .collect();
        if !range.is_empty() {
            self.selected_table_indices = range;
            self.last_selected_idx = i;
        }
    }

    /// Apply a table-row click to the selection.
    ///
    /// `ctrl` = Ctrl or Meta modifier held.
    /// Plain click replaces the selection; Ctrl+click toggles the row in/out,
    /// refusing to deselect when it is the last selected item.
    pub fn apply_click(&mut self, i: usize, ctrl: bool) {
        if ctrl {
            if self.selected_table_indices.contains(&i) {
                if self.selected_table_indices.len() > 1 {
                    self.selected_table_indices.remove(&i);
                }
            } else {
                self.selected_table_indices.insert(i);
                self.last_selected_idx = i;
            }
        } else {
            self.selected_table_indices = HashSet::from([i]);
            self.last_selected_idx = i;
        }
    }
}

// ---------------------------------------------------------------------------
// ImportState — Screen 5 (Import / Pass 2)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct ImportState {
    pub pass2_progress: Pass2Progress,
}

// ---------------------------------------------------------------------------
// UiState — persisted pane widths + collapse states (populated in F3)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct UiState {
    // Future: pane_widths: HashMap<ScreenId, SplitPaneConfig>
}

// ---------------------------------------------------------------------------
// Root application state
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct AppState {
    pub screen: AppScreen,
    pub project: ProjectState,
    pub schema: SchemaState,
    pub import: ImportState,
    pub ui: UiState,
    /// Handle to the currently running Pass 1 or Pass 2 task.
    pub abort_handle: Option<tokio::task::AbortHandle>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            screen: AppScreen::default(),
            project: ProjectState::default(),
            schema: SchemaState::default(),
            import: ImportState::default(),
            ui: UiState::default(),
            abort_handle: None,
        }
    }
}

impl AppState {
    /// Populate AppState from a loaded SchemaSnapshot.
    pub fn load_snapshot(&mut self, snapshot: json2sql::schema::persistence::SchemaSnapshot) {
        self.schema.schemas = snapshot.schemas;
        self.schema.truncated_names = snapshot.truncated_names;
        self.schema.column_collisions = snapshot.column_collisions;
        self.schema.pass1_stats = snapshot.stats;
        self.schema.strategy_overrides = snapshot.strategy_overrides;
        self.schema.pass1_progress.rows_scanned = snapshot.total_rows;
        self.schema.pass1_progress.done = true;
        self.schema.schema_snapshot_loaded = true;
        self.schema.selected_table_indices = HashSet::from([0]);
        self.schema.last_selected_idx = 0;
    }

    /// Remove a loaded snapshot and restore the default Pass 1 flow.
    /// Preserves source_file, pg config, and pg_schema — only clears schema data.
    pub fn clear_snapshot(&mut self) {
        self.schema.clear();
    }

    /// Convenience: true when both source file and PG config are ready.
    pub fn ready_to_start(&self) -> bool {
        self.project.is_complete()
    }

    /// Abort the running task (if any), reset all transient state, and return to Setup.
    /// Preserves `project.source_file` and `project.pg` (user preferences).
    pub fn cancel(&mut self) {
        if let Some(handle) = self.abort_handle.take() {
            handle.abort();
        }
        self.schema.clear();
        self.import.pass2_progress = Pass2Progress::default();
        self.project.pg_testing = false;
        self.project.pg_ok = None;
        self.project.pg_error = None;
        // drop_existing is reset intentionally — it is destructive and must be re-enabled explicitly.
        self.project.drop_existing = false;
        self.screen = AppScreen::Setup;
    }

    /// Apply a `ProgressEvent` coming from a Pass 1 / Pass 2 runner.
    pub fn apply_progress_event(&mut self, event: ProgressEvent) {
        use ProgressEvent::*;
        match event {
            Pass1Progress { rows_scanned, bytes_read, total_bytes } => {
                self.schema.pass1_progress.rows_scanned = rows_scanned;
                self.schema.pass1_progress.bytes_read = bytes_read;
                self.schema.pass1_progress.total_bytes = total_bytes;
                self.schema.pass1_progress.push_log(format!(
                    "Scanned {} records ({} / {})",
                    rows_scanned,
                    format_bytes(bytes_read),
                    format_bytes(total_bytes),
                ));
            }
            Pass1Done { total_rows, tables_count, columns_count } => {
                self.schema.pass1_progress.rows_scanned = total_rows;
                self.schema.pass1_progress.tables_count = tables_count;
                self.schema.pass1_progress.columns_count = columns_count;
                self.schema.pass1_progress.done = true;
                self.schema.pass1_progress.push_log(format!(
                    "Schema complete: {} tables, {} columns",
                    tables_count, columns_count
                ));
            }
            Pass2Progress { rows_processed, bytes_read, total_bytes } => {
                self.import.pass2_progress.rows_processed = rows_processed;
                self.import.pass2_progress.bytes_read = bytes_read;
                self.import.pass2_progress.total_bytes = total_bytes;
            }
            Pass2Flush { table_name, rows_flushed } => {
                *self.import.pass2_progress.rows_per_table.entry(table_name.clone()).or_default() += rows_flushed;
                self.import.pass2_progress.push_log(format!(
                    "flush {} ({} rows)",
                    table_name, rows_flushed
                ));
            }
            Pass2Log(msg) => {
                self.import.pass2_progress.push_log(msg);
            }
            Pass2Done { total_rows, anomaly_count, constraint_warning_count } => {
                self.import.pass2_progress.rows_processed = total_rows;
                self.import.pass2_progress.total_anomalies = anomaly_count;
                self.import.pass2_progress.constraint_warning_count = constraint_warning_count;
                self.import.pass2_progress.done = true;
                self.import.pass2_progress.push_log(format!(
                    "Import complete: {} rows, {} anomalies, {} FK warnings",
                    total_rows, anomaly_count, constraint_warning_count
                ));
            }
            Pass2Error { table_name, message } => {
                self.import.pass2_progress.push_log(format!(
                    "Error in {}: {}",
                    table_name, message
                ));
            }
        }
    }
}

/// Return the indices of direct children of `schemas[parent_idx]` that are in `visible_indices`.
pub fn select_children_visible(
    schemas: &[TableSchema],
    parent_idx: usize,
    visible_indices: &[usize],
) -> HashSet<usize> {
    let Some(parent) = schemas.get(parent_idx) else { return HashSet::new() };
    visible_indices.iter()
        .copied()
        .filter(|&i| {
            schemas.get(i)
                .and_then(|t| t.parent_table.as_ref())
                .map(|p| p == &parent.name)
                .unwrap_or(false)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- apply_click ---

    fn schema_with_selection(selected: impl IntoIterator<Item = usize>, last: usize) -> SchemaState {
        let mut s = SchemaState::default();
        s.selected_table_indices = selected.into_iter().collect();
        s.last_selected_idx = last;
        s
    }

    #[test]
    fn plain_click_replaces_selection() {
        let mut s = schema_with_selection([0, 1, 2], 2);
        s.apply_click(5, false);
        assert_eq!(s.selected_table_indices, HashSet::from([5]));
        assert_eq!(s.last_selected_idx, 5);
    }

    #[test]
    fn ctrl_click_adds_new_item() {
        let mut s = schema_with_selection([0], 0);
        s.apply_click(3, true);
        assert!(s.selected_table_indices.contains(&0));
        assert!(s.selected_table_indices.contains(&3));
        assert_eq!(s.last_selected_idx, 3);
    }

    #[test]
    fn ctrl_click_removes_selected_item() {
        let mut s = schema_with_selection([0, 3], 3);
        s.apply_click(3, true);
        assert_eq!(s.selected_table_indices, HashSet::from([0]));
    }

    #[test]
    fn ctrl_click_cannot_deselect_last_item() {
        let mut s = schema_with_selection([0], 0);
        s.apply_click(0, true);
        assert_eq!(s.selected_table_indices, HashSet::from([0]), "last item must stay selected");
    }

    // --- apply_shift_click ---

    #[test]
    fn shift_click_selects_visible_range_forward() {
        // visible: 0, 1, 3, 5 (2 and 4 filtered out)
        let visible = vec![0, 1, 3, 5];
        let mut s = schema_with_selection([0], 0);
        s.apply_shift_click(3, 0, &visible);
        assert_eq!(s.selected_table_indices, HashSet::from([0, 1, 3]));
        assert_eq!(s.last_selected_idx, 3);
    }

    #[test]
    fn shift_click_selects_visible_range_backward() {
        let visible = vec![0, 1, 3, 5];
        let mut s = schema_with_selection([5], 5);
        s.apply_shift_click(1, 5, &visible);
        assert_eq!(s.selected_table_indices, HashSet::from([1, 3, 5]));
        assert_eq!(s.last_selected_idx, 1);
    }

    #[test]
    fn shift_click_skips_invisible_indices() {
        // indices 2 and 4 are not in visible list → not selected
        let visible = vec![0, 1, 3, 5];
        let mut s = schema_with_selection([0], 0);
        s.apply_shift_click(5, 0, &visible);
        assert!(!s.selected_table_indices.contains(&2));
        assert!(!s.selected_table_indices.contains(&4));
        assert_eq!(s.selected_table_indices, HashSet::from([0, 1, 3, 5]));
    }

    #[test]
    fn shift_click_on_same_item_keeps_selection() {
        let visible = vec![0, 1, 2];
        let mut s = schema_with_selection([0], 0);
        s.apply_shift_click(0, 0, &visible);
        assert_eq!(s.selected_table_indices, HashSet::from([0]));
    }

    // --- select_children_visible ---

    fn make_schema(name: &str, parent: Option<&str>) -> json2sql::schema::table_schema::TableSchema {
        let mut s = json2sql::schema::table_schema::TableSchema::new(
            name.to_string(), vec![name.to_string()], if parent.is_some() { 1 } else { 0 },
        );
        s.parent_table = parent.map(|p| p.to_string());
        s
    }

    #[test]
    fn select_children_returns_direct_children() {
        let schemas = vec![
            make_schema("product", None),          // 0
            make_schema("product_image", Some("product")),  // 1
            make_schema("product_tag", Some("product")),    // 2
            make_schema("order", None),             // 3
        ];
        let visible = vec![0, 1, 2, 3];
        let result = select_children_visible(&schemas, 0, &visible);
        assert_eq!(result, HashSet::from([1, 2]));
    }

    #[test]
    fn select_children_skips_invisible() {
        let schemas = vec![
            make_schema("product", None),
            make_schema("product_image", Some("product")),  // 1
            make_schema("product_tag", Some("product")),    // 2 — filtered out
        ];
        let visible = vec![0, 1]; // 2 not visible
        let result = select_children_visible(&schemas, 0, &visible);
        assert_eq!(result, HashSet::from([1]));
    }

    #[test]
    fn select_children_returns_empty_for_leaf() {
        let schemas = vec![
            make_schema("product", None),
            make_schema("product_image", Some("product")),
        ];
        let visible = vec![0, 1];
        let result = select_children_visible(&schemas, 1, &visible);
        assert!(result.is_empty(), "leaf node has no children");
    }

    // --- Sub-struct defaults ---

    #[test]
    fn project_state_default_has_expected_values() {
        let p = ProjectState::default();
        assert_eq!(p.pg.host, "localhost");
        assert_eq!(p.pg.port, 5432);
        assert!(!p.drop_existing);
        assert_eq!(p.workers, 1);
        assert!(p.source_file.is_none());
        assert!(p.anomaly_dir.is_none());
        assert!(!p.pg_testing);
        assert!(p.pg_ok.is_none());
        assert!(p.pg_error.is_none());
    }

    #[test]
    fn schema_state_is_empty_by_default() {
        let s = SchemaState::default();
        assert!(s.schemas.is_empty());
        assert!(!s.schema_snapshot_loaded);
        assert_eq!(s.last_selected_idx, 0);
        assert_eq!(s.selected_table_indices.len(), 1);
        assert!(s.selected_table_indices.contains(&0));
        assert!(s.truncated_names.is_empty());
        assert!(s.column_collisions.is_empty());
        assert!(s.pass1_stats.is_empty());
        assert!(!s.pass1_progress.done);
    }

    #[test]
    fn import_state_is_empty_by_default() {
        let i = ImportState::default();
        assert!(!i.pass2_progress.done);
        assert_eq!(i.pass2_progress.rows_processed, 0);
        assert_eq!(i.pass2_progress.total_anomalies, 0);
    }

    #[test]
    fn app_state_delegates_to_sub_states() {
        let s = AppState::default();
        assert_eq!(s.project.pg.host, "localhost");
        assert!(s.schema.schemas.is_empty());
        assert!(!s.import.pass2_progress.done);
        assert!(s.abort_handle.is_none());
    }

    // --- AppState methods with updated field paths ---

    #[test]
    fn clear_snapshot_resets_schema_fields_but_keeps_project() {
        use json2sql::schema::persistence::SchemaSnapshot;

        let mut s = AppState::default();
        s.project.source_file = Some(std::path::PathBuf::from("/tmp/data.json"));
        s.project.pg.host = "myhost".to_string();
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

        assert!(!s.schema.schema_snapshot_loaded);
        assert!(s.schema.schemas.is_empty());
        assert!(s.schema.strategy_overrides.is_empty());
        assert!(!s.schema.pass1_progress.done);
        // project fields preserved
        assert!(s.project.source_file.is_some());
        assert_eq!(s.project.pg.host, "myhost");
    }

    #[test]
    fn load_snapshot_populates_schema_state_and_sets_flag() {
        use json2sql::schema::persistence::SchemaSnapshot;

        let snapshot = SchemaSnapshot {
            version: 1,
            total_rows: 42,
            schemas: vec![],
            truncated_names: vec![],
            column_collisions: vec![],
            stats: vec![],
            strategy_overrides: {
                let mut m = HashMap::new();
                m.insert("t".to_string(), WideStrategy::Jsonb);
                m
            },
        };

        let mut s = AppState::default();
        s.load_snapshot(snapshot);

        assert!(s.schema.schema_snapshot_loaded);
        assert_eq!(s.schema.pass1_progress.rows_scanned, 42);
        assert!(matches!(
            s.schema.strategy_overrides.get("t"),
            Some(WideStrategy::Jsonb)
        ));
    }

    #[test]
    fn schema_state_fields_default_to_empty_and_false() {
        let s = AppState::default();
        assert!(!s.schema.schema_snapshot_loaded);
        assert!(s.schema.truncated_names.is_empty());
        assert!(s.schema.column_collisions.is_empty());
        assert!(s.schema.pass1_stats.is_empty());
    }

    #[test]
    fn cancel_resets_schema_and_import_but_keeps_project_source() {
        let mut s = AppState::default();
        s.project.source_file = Some(PathBuf::from("/tmp/data.json"));
        s.project.pg.host = "myhost".to_string();
        s.schema.schema_snapshot_loaded = true;
        s.schema.truncated_names.push(json2sql::schema::naming::TruncatedName {
            original_path: "a.b".to_string(),
            full_name: "a_b_long".to_string(),
            pg_name: "a_b".to_string(),
        });
        s.import.pass2_progress.rows_processed = 42;
        s.cancel();

        assert!(!s.schema.schema_snapshot_loaded);
        assert!(s.schema.truncated_names.is_empty());
        assert_eq!(s.import.pass2_progress.rows_processed, 0);
        assert!(!s.project.drop_existing, "drop_existing reset on cancel");
        // source_file and pg preserved
        assert!(s.project.source_file.is_some());
        assert_eq!(s.project.pg.host, "myhost");
    }

    #[test]
    fn ready_to_start_delegates_to_project_is_complete() {
        let mut s = AppState::default();
        assert!(!s.ready_to_start(), "incomplete project should not be ready");
        s.project.source_file = Some(PathBuf::from("/tmp/data.json"));
        s.project.pg.database = "mydb".to_string();
        s.project.pg.username = "user".to_string();
        assert!(s.ready_to_start());
    }

    #[test]
    fn pass2_parallel_default_is_in_valid_range() {
        let s = AppState::default();
        assert!(s.project.pass2_parallel >= 1, "pass2_parallel must be at least 1");
        assert!(s.project.pass2_parallel <= 32, "pass2_parallel should not exceed 32");
    }

    #[test]
    fn pass2_parallel_matches_available_cpus_capped_at_8() {
        let s = AppState::default();
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        assert_eq!(s.project.pass2_parallel, cpus.min(8));
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

    #[test]
    fn pass2_flush_accumulates_once_per_table() {
        // Each Pass2Flush event adds to the per-table count.
        // Runner must emit exactly one Pass2Flush per table per run —
        // a second emit (duplicate) would double the displayed count.
        let mut s = AppState::default();
        s.apply_progress_event(ProgressEvent::Pass2Flush {
            table_name: "orders".to_string(),
            rows_flushed: 120,
        });
        assert_eq!(s.import.pass2_progress.rows_per_table["orders"], 120);
    }

    #[test]
    fn pass2_flush_duplicate_emit_doubles_count() {
        // Regression test: if runner.rs emits Pass2Flush twice for the same table,
        // the UI count doubles. This test documents the bug — runner.rs must not
        // emit the final batch (lines 598-607) alongside the per-COPY events (line 429).
        let mut s = AppState::default();
        s.apply_progress_event(ProgressEvent::Pass2Flush {
            table_name: "orders".to_string(),
            rows_flushed: 120,
        });
        s.apply_progress_event(ProgressEvent::Pass2Flush {
            table_name: "orders".to_string(),
            rows_flushed: 120,
        });
        assert_eq!(
            s.import.pass2_progress.rows_per_table["orders"],
            240,
            "two identical Pass2Flush events double the count — runner must not emit both"
        );
    }
}
