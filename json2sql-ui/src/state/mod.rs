//! Application state — persisted UI state, per-pass progress, and `PostgreSQL` connection config.
//!
//! `AppState` is the root state tree threaded through every screen via `Signal<AppState>`.
//! Sub-states: `ProjectState` (Setup + PG config), `SchemaState` (Analysis/Strategy/Preview),
//! `ImportState` (Pass 2 progress), `UiState` (reserved). Table-selection logic
//! (`apply_click`/`apply_shift_click`/`select_children_visible`) lives in [`selection`];
//! Jaccard display info for the Strategy screen lives in `crate::screens::strategy`.
//!
//! Fonctions :
//! - enum `AppScreen` — écran courant de l'IHM (routing)
//! - struct `PgConfig` — paramètres de connexion `PostgreSQL` (password zeroïsé au drop)
//! - fn `PgConfig::drop` — zeroïse le password en mémoire à la destruction
//! - fn `PgConfig::default` — valeurs par défaut (host localhost, port 5432)
//! - fn `PgConfig::to_url` — construit l'URL de connexion Postgres (retour `Zeroizing<String>`)
//! - fn `PgConfig::is_complete` — vérifie que les champs de connexion sont renseignés
//! - fn `format_bytes` — formate un nombre d'octets en unités SI lisibles
//! - struct `Pass1Progress` — état de progression de la Pass 1 (compteurs + logs)
//! - fn `Pass1Progress::push_log` — ajoute une ligne au ring-buffer de logs
//! - struct `Pass2Progress` — état de progression de la Pass 2 (compteurs, phases DDL/streaming/contraintes)
//! - fn `Pass2Progress::push_log` — ajoute une ligne au ring-buffer de logs
//! - struct `ProjectState` — état de l'écran Setup (source, config PG, options avancées)
//! - fn `ProjectState::default` — valeurs par défaut (host localhost, `pass2_parallel` selon CPU)
//! - fn `ProjectState::is_complete` — vérifie que la source + la config PG sont prêtes pour un import
//! - struct `SchemaState` — état des écrans Analysis/Strategy/Preview (schémas, overrides, sélection)
//! - fn `SchemaState::default` — valeurs par défaut (sélection initiale sur la table 0)
//! - fn `SchemaState::clear` — réinitialise l'état schéma (nouveau fichier / annulation)
//! - struct `ImportState` — état de l'écran Import (progression Pass 2)
//! - struct `UiState` — état de layout réservé (largeurs de panneaux, à venir)
//! - struct `AppState` — état racine partagé entre tous les écrans via `Signal<AppState>`
//! - fn `AppState::load_snapshot` — recharge un `SchemaSnapshot` sauvegardé (dédoublonne les tables)
//! - fn `AppState::clear_snapshot` — retire un snapshot chargé, revient au flux Pass 1 par défaut
//! - fn `AppState::ready_to_start` — vrai si le projet est prêt à lancer un import
//! - fn `AppState::cancel` — annule la tâche en cours, tue le worker, réinitialise l'état transitoire
//! - fn `AppState::apply_worker_result` — applique le résultat lu depuis le fichier JSON du worker
//! - fn `AppState::apply_progress_event` — dispatch un `ProgressEvent` vers l'état correspondant

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;

use urlencoding::encode;
use zeroize::{Zeroize, Zeroizing};
use json2sql::io::progress_event::ProgressEvent;
use json2sql::schema::naming::{ColumnCollision, TruncatedName};
use json2sql::schema::finalizer::OverflowWarning;
use json2sql::schema::stats::ColumnStats;
use json2sql::schema::strategies::StrategyName;
use json2sql::schema::config::ConfigWarning;
use json2sql::schema::table_schema::{TableSchema, UserOverride};

use crate::worker_client::WorkerKillHandle;

mod selection;
pub use selection::select_children_visible;

// ---------------------------------------------------------------------------
// Screen navigation
// ---------------------------------------------------------------------------

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub enum AppScreen {
    #[default]
    Setup,
    Analysis,
    Strategy,
    Preview,
    Import,
    /// Active worker subprocess detected at startup — show "reprendre ?" screen.
    Resume,
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
    /// is dropped (which would be too late for URL copies passed to `connect()`).
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

    pub const fn is_complete(&self) -> bool {
        !self.host.is_empty()
            && !self.database.is_empty()
            && !self.username.is_empty()
            && self.port != 0
    }
}

/// Maximum log lines kept in memory per pass (ring-buffer via `VecDeque`).
const LOG_MAX: usize = 500;

/// Format a byte count as a human-readable string using SI units (powers of 1 000).
/// Shows KB for < 1 MB so sub-megabyte values are never displayed as "0 MB".
#[allow(clippy::cast_precision_loss)]
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
pub const PASS1_WIDE_COLUMN_THRESHOLD: usize = 10;
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
#[allow(clippy::struct_excessive_bools)]
pub struct Pass2Progress {
    pub rows_processed: u64,
    pub bytes_read: u64,
    pub total_bytes: u64,
    /// Per-table row counts for the right panel.
    pub rows_per_table: std::collections::HashMap<String, u64>,
    pub log_lines: VecDeque<String>,
    pub done: bool,
    pub total_anomalies: u64,
    /// Per-table anomaly counts populated from `Pass2AnomalyUpdate` events.
    pub anomaly_counts_per_table: std::collections::HashMap<String, u64>,
    /// FK constraints that failed after import (non-fatal; PK failures are errors).
    pub constraint_warning_count: u64,
    // DDL phase (CREATE TABLE before data load)
    pub ddl_table_count: usize,
    pub ddl_done: usize,
    pub ddl_complete: bool,
    /// True once all workers have finished COPY — set on `ConstraintsStart`.
    /// Used to show Phase A and Phase B as complete independently of constraints.
    pub copy_complete: bool,
    // Constraints phase (PK + FK after data load)
    pub constraints_total: usize,
    pub constraints_done: usize,
    pub constraints_complete: bool,
    /// True when the import was run with `skip_constraints = true` — Phase D shows "Skipped".
    pub constraints_skipped: bool,
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
    /// Target `PostgreSQL` schema (default: "public").
    pub pg_schema: String,
    /// Drop and recreate tables before import (destructive — clean slate).
    pub drop_existing: bool,
    /// Optional directory where anomaly NDJSON files are streamed during Pass 2.
    pub anomaly_dir: Option<PathBuf>,
    /// Optional directory for Pass 2 temporary files (default: system temp dir).
    pub temp_dir: Option<PathBuf>,
    /// True while the "Test connection" check is in flight.
    pub pg_testing: bool,
    /// Some(true/false) after the test completes.
    pub pg_ok: Option<bool>,
    /// Connection error details when the PG health check fails.
    pub pg_error: Option<String>,
    /// Number of worker threads for Pass 1 schema inference (1 = sequential).
    pub workers: usize,
    /// Number of parallel `PostgreSQL` connections for Pass 2 COPY.
    pub pass2_parallel: usize,
    /// Optional strategies disabled before analysis. Empty set = all strategies active (default).
    pub disabled_strategies: HashSet<StrategyName>,
    /// Stop pass 2 after this many root objects. None = full import (default).
    pub import_limit: Option<u64>,
    /// Emit verbose pass 2 logs (RAM tick every second, DISPATCH every 10k rows). Default false.
    pub verbose_logs: bool,
    /// Skip the constraint phase (PK + FK) at the end of Pass 2. Default false.
    pub skip_constraints: bool,
}

impl Default for ProjectState {
    fn default() -> Self {
        Self {
            source_file: None,
            pg: PgConfig::default(),
            pg_schema: "public".to_string(),
            drop_existing: false,
            anomaly_dir: None,
            temp_dir: None,
            pg_testing: false,
            pg_ok: None,
            pg_error: None,
            workers: 1,
            pass2_parallel: std::thread::available_parallelism()
                .map_or(4, std::num::NonZero::get)
                .min(8),
            disabled_strategies: HashSet::new(),
            import_limit: None,
            verbose_logs: false,
            skip_constraints: false,
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
    /// Tables auto-converted to JSONB (exceeded `PostgreSQL` 1600-column limit).
    pub overflow_warnings: Vec<OverflowWarning>,
    /// User-chosen strategy overrides: `table_name` → `UserOverride`.
    pub strategy_overrides: HashMap<String, UserOverride>,
    /// Set of table indices currently selected in the Strategy panel.
    pub selected_table_indices: HashSet<usize>,
    /// Index of the last table clicked — drives center/right panels.
    pub last_selected_idx: usize,
    pub truncated_names: Vec<TruncatedName>,
    pub column_collisions: Vec<ColumnCollision>,
    pub pass1_stats: Vec<ColumnStats>,
    /// True when schemas were loaded from a saved snapshot rather than a live Pass 1 run.
    pub schema_snapshot_loaded: bool,
    /// Tables absorbed by a manual sibling merge — hidden from the table list.
    pub absorbed_names: HashSet<String>,
    /// Warnings from TOML config overrides (unknown table, column, type, strategy).
    pub config_warnings: Vec<ConfigWarning>,
    /// JSON format detected during Pass 1, or loaded from snapshot. Used to skip
    /// re-detection in Pass 2 and to persist the format when saving the snapshot.
    pub detected_format: Option<json2sql::io::reader::JsonFormat>,
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
            absorbed_names: HashSet::new(),
            config_warnings: Vec::new(),
            detected_format: None,
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
        self.absorbed_names = HashSet::new();
        self.config_warnings = Vec::new();
        self.detected_format = None;
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

#[derive(Clone, Debug, Default)]
pub struct AppState {
    pub screen: AppScreen,
    pub project: ProjectState,
    pub schema: SchemaState,
    pub import: ImportState,
    #[allow(dead_code)]
    pub ui: UiState,
    /// Handle to the currently running Pass 1 (analysis) task.
    pub abort_handle: Option<tokio::task::AbortHandle>,
    /// Handle to the running worker subprocess (Pass 2 import).
    /// Replaces the old in-process abort path for the import coroutine.
    pub worker_kill: WorkerKillHandle,
    /// Socket path of an existing worker found at startup (Resume screen).
    pub resume_socket: Option<PathBuf>,
}

impl AppState {
    /// Populate `AppState` from a loaded `SchemaSnapshot`.
    pub fn load_snapshot(&mut self, snapshot: json2sql::schema::persistence::SchemaSnapshot) {
        // Dedup defensively: snapshots saved before the finalizer fix may contain
        // duplicate table names → add_constraints() would fail with 42P16.
        let mut seen = std::collections::HashSet::new();
        self.schema.schemas = snapshot.schemas.into_iter()
            .filter(|s| seen.insert(s.name.clone()))
            .collect();
        self.schema.truncated_names = snapshot.truncated_names;
        self.schema.column_collisions = snapshot.column_collisions;
        self.schema.pass1_stats = snapshot.stats;
        self.schema.strategy_overrides = snapshot.strategy_overrides;
        self.schema.pass1_progress.rows_scanned = snapshot.total_rows;
        self.schema.pass1_progress.done = true;
        self.schema.schema_snapshot_loaded = true;
        self.schema.selected_table_indices = HashSet::from([0]);
        self.schema.last_selected_idx = 0;
        self.schema.detected_format = snapshot.detected_format;
    }

    /// Remove a loaded snapshot and restore the default Pass 1 flow.
    /// Preserves `source_file`, pg config, and `pg_schema` — only clears schema data.
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
        // Kill subprocess worker if one is active (Pass 2 / import)
        self.worker_kill.kill();
        self.worker_kill = WorkerKillHandle::default();
        self.resume_socket = None;
        self.schema.clear();
        self.import.pass2_progress = Pass2Progress::default();
        self.project.pg_testing = false;
        self.project.pg_ok = None;
        self.project.pg_error = None;
        // drop_existing is reset intentionally — it is destructive and must be re-enabled explicitly.
        self.project.drop_existing = false;
        self.screen = AppScreen::Setup;
    }

    /// Apply a `WorkerResult` read from the result file after an unexpected EOF.
    ///
    /// If status is `"success"`, synthesises a `Pass2Done` event so the UI shows the
    /// success banner. Otherwise, pushes an error log line.
    pub fn apply_worker_result(&mut self, result: json2sql::ipc::WorkerResult) {
        match result.status.as_str() {
            "success" => {
                self.apply_progress_event(ProgressEvent::Pass2Done {
                    total_rows: result.total_rows,
                    anomaly_count: result.anomaly_count,
                    constraint_warning_count: result.constraint_warning_count,
                });
            }
            _ => {
                let msg = result
                    .message
                    .unwrap_or_else(|| format!("status: {}", result.status));
                self.import
                    .pass2_progress
                    .push_log(format!("Import terminé (hors-connexion) : {msg}"));
            }
        }
    }

    /// Apply a `ProgressEvent` coming from a Pass 1 / Pass 2 runner.
    #[allow(clippy::too_many_lines)]
    pub fn apply_progress_event(&mut self, event: ProgressEvent) {
        use ProgressEvent::{Pass1Log, Pass1Progress, Pass1Done, Pass2Progress, Pass2Flush, Pass2AnomalyUpdate, Pass2Log, Pass2Done, Pass2Error, DdlStart, DdlProgress, DdlDone, ConstraintsStart, ConstraintsProgress, ConstraintsDone};
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
                    "Schema complete: {tables_count} tables, {columns_count} columns"
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
                    "flush {table_name} ({rows_flushed} rows)"
                ));
            }
            Pass2AnomalyUpdate { table_name, count } => {
                self.import.pass2_progress.anomaly_counts_per_table
                    .insert(table_name, count);
            }
            Pass1Log(msg) => {
                self.schema.pass1_progress.push_log(msg);
            }
            Pass2Log(msg) => {
                self.import.pass2_progress.push_log(msg);
            }
            Pass2Done { total_rows, anomaly_count, constraint_warning_count } => {
                self.import.pass2_progress.rows_processed = total_rows;
                self.import.pass2_progress.total_anomalies = anomaly_count;
                self.import.pass2_progress.constraint_warning_count = constraint_warning_count;
                self.import.pass2_progress.done = true;
                if self.import.pass2_progress.constraints_skipped {
                    self.import.pass2_progress.constraints_complete = true;
                }
                self.import.pass2_progress.push_log(format!(
                    "Import complete: {total_rows} rows, {anomaly_count} anomalies, {constraint_warning_count} FK warnings"
                ));
            }
            Pass2Error { table_name, message } => {
                self.import.pass2_progress.push_log(format!(
                    "Error in {table_name}: {message}"
                ));
            }
            DdlStart { table_count } => {
                self.import.pass2_progress.ddl_table_count = table_count;
                self.import.pass2_progress.ddl_done = 0;
                self.import.pass2_progress.ddl_complete = false;
                self.import.pass2_progress.push_log(format!(
                    "Creating {table_count} tables…"
                ));
            }
            DdlProgress { done, total } => {
                self.import.pass2_progress.ddl_done = done;
                self.import.pass2_progress.ddl_table_count = total;
            }
            DdlDone => {
                self.import.pass2_progress.ddl_complete = true;
                self.import.pass2_progress.push_log(format!(
                    "Tables created ({} total)",
                    self.import.pass2_progress.ddl_table_count
                ));
            }
            ConstraintsStart { table_count } => {
                self.import.pass2_progress.copy_complete = true;
                self.import.pass2_progress.constraints_done = 0;
                self.import.pass2_progress.constraints_total = table_count * 2;
                self.import.pass2_progress.constraints_complete = false;
                self.import.pass2_progress.push_log(format!(
                    "Applying PK + FK constraints ({table_count} tables)…"
                ));
            }
            ConstraintsProgress { done, total } => {
                self.import.pass2_progress.constraints_done = done;
                self.import.pass2_progress.constraints_total = total;
            }
            ConstraintsDone => {
                self.import.pass2_progress.constraints_complete = true;
                self.import.pass2_progress.push_log(
                    "Constraints applied".to_string()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(s.config_warnings.is_empty());
    }

    #[test]
    fn schema_state_clear_resets_config_warnings() {
        use json2sql::schema::config::ConfigWarning;
        let mut s = SchemaState::default();
        s.config_warnings = vec![ConfigWarning::UnknownTable("t".to_string())];
        s.clear();
        assert!(s.config_warnings.is_empty());
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
            overflow_warnings: vec![],
            detected_format: None,
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
            overflow_warnings: vec![],
            detected_format: None,
            strategy_overrides: {
                let mut m = HashMap::new();
                m.insert("t".to_string(), UserOverride::Jsonb);
                m
            },
        };

        let mut s = AppState::default();
        s.load_snapshot(snapshot);

        assert!(s.schema.schema_snapshot_loaded);
        assert_eq!(s.schema.pass1_progress.rows_scanned, 42);
        assert!(matches!(
            s.schema.strategy_overrides.get("t"),
            Some(UserOverride::Jsonb)
        ));
    }

    #[test]
    fn load_snapshot_deduplicates_schemas_by_name() {
        use json2sql::schema::persistence::SchemaSnapshot;
        use json2sql::schema::table_schema::TableSchema;

        let dup = TableSchema::new("dup".to_string(), vec!["dup".to_string()], 0);
        let other = TableSchema::new("other".to_string(), vec!["other".to_string()], 0);
        let snapshot = SchemaSnapshot {
            version: 1,
            total_rows: 3,
            schemas: vec![dup.clone(), other.clone(), dup.clone()], // duplicate "dup"
            truncated_names: vec![],
            column_collisions: vec![],
            stats: vec![],
            overflow_warnings: vec![],
            detected_format: None,
            strategy_overrides: HashMap::new(),
        };
        let mut s = AppState::default();
        s.load_snapshot(snapshot);

        assert_eq!(s.schema.schemas.len(), 2, "duplicate table names must be removed on load");
        let names: Vec<&str> = s.schema.schemas.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"dup"));
        assert!(names.contains(&"other"));
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
            .map_or(4, std::num::NonZeroUsize::get);
        assert_eq!(s.project.pass2_parallel, cpus.min(8));
    }

    #[test]
    fn temp_dir_defaults_to_none() {
        assert!(ProjectState::default().temp_dir.is_none());
    }

    #[test]
    fn import_limit_defaults_to_none() {
        assert!(ProjectState::default().import_limit.is_none());
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

    #[test]
    fn apply_pass2_anomaly_update_stores_per_table_count() {
        let mut s = AppState::default();
        s.apply_progress_event(ProgressEvent::Pass2AnomalyUpdate {
            table_name: "orders".to_string(),
            count: 5,
        });
        s.apply_progress_event(ProgressEvent::Pass2AnomalyUpdate {
            table_name: "users".to_string(),
            count: 2,
        });
        assert_eq!(s.import.pass2_progress.anomaly_counts_per_table["orders"], 5);
        assert_eq!(s.import.pass2_progress.anomaly_counts_per_table["users"],  2);
        assert_eq!(s.import.pass2_progress.anomaly_counts_per_table.len(),     2);
    }

    #[test]
    fn copy_complete_set_on_constraints_start() {
        let mut s = AppState::default();
        assert!(!s.import.pass2_progress.copy_complete, "false before ConstraintsStart");

        // Streaming + inserting events should not set it
        s.apply_progress_event(ProgressEvent::Pass2Progress {
            rows_processed: 1000, bytes_read: 1_000_000, total_bytes: 100_000_000,
        });
        s.apply_progress_event(ProgressEvent::Pass2Flush {
            table_name: "users".to_string(), rows_flushed: 500,
        });
        assert!(!s.import.pass2_progress.copy_complete, "still false after streaming/flush");

        s.apply_progress_event(ProgressEvent::ConstraintsStart { table_count: 3 });
        assert!(s.import.pass2_progress.copy_complete, "true once ConstraintsStart fires");
    }

    #[test]
    fn apply_ddl_events_updates_state_correctly() {
        let mut s = AppState::default();
        s.apply_progress_event(ProgressEvent::DdlStart { table_count: 4 });
        assert_eq!(s.import.pass2_progress.ddl_table_count, 4);
        assert!(!s.import.pass2_progress.ddl_complete);

        s.apply_progress_event(ProgressEvent::DdlProgress { done: 2, total: 4 });
        assert_eq!(s.import.pass2_progress.ddl_done, 2);

        s.apply_progress_event(ProgressEvent::DdlDone);
        assert!(s.import.pass2_progress.ddl_complete);
        // DdlDone should have appended a log line
        assert!(s.import.pass2_progress.log_lines.iter().any(|l| l.contains("Tables created")));
    }

    #[test]
    fn apply_constraints_events_updates_state_correctly() {
        let mut s = AppState::default();
        // 2 tables → total = 2 PKs + 1 FK (1 child) = 3 ops in real life, but
        // ConstraintsStart uses table_count for display, ConstraintsProgress carries total.
        s.apply_progress_event(ProgressEvent::ConstraintsStart { table_count: 3 });
        assert!(!s.import.pass2_progress.constraints_complete);

        s.apply_progress_event(ProgressEvent::ConstraintsProgress { done: 3, total: 5 });
        assert_eq!(s.import.pass2_progress.constraints_done, 3);
        assert_eq!(s.import.pass2_progress.constraints_total, 5);

        s.apply_progress_event(ProgressEvent::ConstraintsDone);
        assert!(s.import.pass2_progress.constraints_complete);
        assert!(s.import.pass2_progress.log_lines.iter().any(|l| l.contains("Constraints applied")));
    }

    #[test]
    fn pct_b_source_reflects_empty_tables_correctly() {
        // Reproduces the 84% bug: 189 tables have rows, 36 are empty → rows_per_table.len() = 189
        // copy_complete must be the signal for 100%, not rows_per_table.len() / total
        let mut s = AppState::default();
        for i in 0..189u64 {
            s.apply_progress_event(ProgressEvent::Pass2Flush {
                table_name: format!("t{i}"),
                rows_flushed: 10,
            });
        }
        assert_eq!(s.import.pass2_progress.rows_per_table.len(), 189);
        assert!(!s.import.pass2_progress.copy_complete);

        // ConstraintsStart fires → copy_complete becomes true → pct_b must show 100%
        s.apply_progress_event(ProgressEvent::ConstraintsStart { table_count: 225 });
        assert!(s.import.pass2_progress.copy_complete);
    }

    #[test]
    fn throttled_batch_with_done_sets_done_flag() {
        // Simulates a batch of events applied together (as the throttled coroutine would do)
        // including Pass2Done at the end. The resulting state must be identical to
        // applying them one-by-one — and done must be true.
        let mut s = AppState::default();
        let batch = vec![
            ProgressEvent::Pass2Progress { rows_processed: 5_000, bytes_read: 1024, total_bytes: 2048 },
            ProgressEvent::Pass2Flush { table_name: "orders".to_string(), rows_flushed: 5_000 },
            ProgressEvent::Pass2Done { total_rows: 5_000, anomaly_count: 0, constraint_warning_count: 0 },
        ];
        for e in batch { s.apply_progress_event(e); }
        assert!(s.import.pass2_progress.done, "Pass2Done in batch must set done=true");
        assert_eq!(s.import.pass2_progress.rows_processed, 5_000);
        assert_eq!(s.import.pass2_progress.rows_per_table["orders"], 5_000);
    }

    #[test]
    fn throttled_batch_residual_flush_preserves_last_log_line() {
        // After the channel closes (None), any pending events must be flushed.
        // This test verifies that Pass2Log in a residual batch appears in log_lines.
        let mut s = AppState::default();
        let residual = vec![
            ProgressEvent::Pass2Log("Import complete".to_string()),
        ];
        for e in residual { s.apply_progress_event(e); }
        assert!(
            s.import.pass2_progress.log_lines.iter().any(|l| l.contains("Import complete")),
            "residual log event must appear after flush"
        );
    }

    #[test]
    fn pass2_progress_constraints_skipped_default_false() {
        let p = Pass2Progress::default();
        assert!(!p.constraints_skipped, "constraints_skipped must default to false");
    }

    #[test]
    fn project_state_skip_constraints_default_false() {
        let p = ProjectState::default();
        assert!(!p.skip_constraints, "skip_constraints must default to false");
    }

    #[test]
    fn pass2_done_with_constraints_skipped_forces_constraints_complete() {
        let mut s = AppState::default();
        s.import.pass2_progress.constraints_skipped = true;
        s.apply_progress_event(ProgressEvent::Pass2Done {
            total_rows: 100,
            anomaly_count: 0,
            constraint_warning_count: 0,
        });
        assert!(s.import.pass2_progress.constraints_complete,
            "constraints_complete must be forced true when constraints_skipped");
    }

    #[test]
    fn pass2_done_without_constraints_skipped_leaves_constraints_incomplete() {
        let mut s = AppState::default();
        s.import.pass2_progress.constraints_skipped = false;
        s.apply_progress_event(ProgressEvent::Pass2Done {
            total_rows: 100,
            anomaly_count: 0,
            constraint_warning_count: 0,
        });
        assert!(!s.import.pass2_progress.constraints_complete,
            "constraints_complete must not be forced when constraints_skipped is false");
    }
}
