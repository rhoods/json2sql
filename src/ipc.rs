//! IPC entre l'IHM (`json2sql-ui`) et le processus worker (`json2sql-worker`) :
//! config envoyée en JSON sur stdin, commandes sur socket Unix, résultat final sur disque.
//!
//! Fonctions :
//! - struct `WorkerConfig` — config envoyée par l'IHM au worker via stdin (JSON, sans mot de passe PG).
//! - fn `WorkerConfig::into_pass2_config` — projette la config IPC vers `Pass2Config` (le mot de passe PG est résolu séparément, via `J2S_PG_PASSWORD`).
//! - struct `WorkerCommand` — commande envoyée par l'IHM au worker sur le socket (une ligne JSON).
//! - struct `WorkerResult` — résultat final écrit atomiquement dans `result_file` avant fermeture du socket.
//! - fn `new_socket_path` — génère un chemin de socket Unix temporaire unique (UUID v7).
//! - fn `lockfile_path` — chemin du lockfile empêchant plusieurs workers concurrents.

use std::path::PathBuf;

use crate::io::reader::JsonFormat;
use crate::pass2::Pass2Config;
use crate::schema::table_schema::TableSchema;

/// Configuration sent by the UI to the worker via stdin (JSON).
/// PostgreSQL password is NOT included — passed via `J2S_PG_PASSWORD` env var.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerConfig {
    pub source_file: PathBuf,
    pub root_table: String,
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_database: String,
    pub pg_user: String,
    pub pg_schema: String,
    pub schemas: Vec<TableSchema>,
    pub drop_existing: bool,
    pub anomaly_dir: Option<PathBuf>,
    pub pass2_parallel: usize,
    pub import_limit: Option<u64>,
    pub verbose_logs: bool,
    pub hint_format: Option<JsonFormat>,
    pub skip_constraints: bool,
    /// Full path to the Unix socket the worker will bind.
    pub socket_path: PathBuf,
    /// Full path where the worker writes the JSON result file.
    pub result_file: PathBuf,
}

impl WorkerConfig {
    /// Build a `Pass2Config` from this config (password resolved separately).
    #[must_use]
    pub fn into_pass2_config(self) -> Pass2Config {
        Pass2Config {
            root_table: self.root_table,
            pg_schema: self.pg_schema,
            parallel: self.pass2_parallel,
            anomaly_dir: self.anomaly_dir,
            limit: self.import_limit,
            mem_flush_threshold_bytes: None,
            ram_high_watermark: None,
            ram_low_watermark: None,
            verbose: self.verbose_logs,
            hint_format: self.hint_format,
            skip_constraints: self.skip_constraints,
        }
    }
}

/// Command sent by the UI to the worker on the socket (one JSON line).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerCommand {
    pub cmd: String,
}

/// Final result written atomically to `result_file` before the socket is closed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkerResult {
    /// `"success"`, `"error"`, or `"cancelled"`
    pub status: String,
    pub total_rows: u64,
    pub anomaly_count: u64,
    pub constraint_warning_count: u64,
    pub message: Option<String>,
}

/// Generate a fresh socket path under `temp_dir()` using a UUID v7 session ID.
#[must_use]
pub fn new_socket_path() -> PathBuf {
    let id = uuid::Uuid::now_v7();
    std::env::temp_dir().join(format!("json2sql-{id}.sock"))
}

/// Path to the exclusive lockfile preventing concurrent workers.
#[must_use]
pub fn lockfile_path() -> PathBuf {
    std::env::temp_dir().join("json2sql-worker.lock")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;

    fn minimal_config(socket_path: PathBuf, result_file: PathBuf) -> WorkerConfig {
        WorkerConfig {
            source_file: PathBuf::from("/tmp/data.json"),
            root_table: "root".to_string(),
            pg_host: "localhost".to_string(),
            pg_port: 5432,
            pg_database: "mydb".to_string(),
            pg_user: "user".to_string(),
            pg_schema: "public".to_string(),
            schemas: vec![],
            drop_existing: false,
            anomaly_dir: None,
            pass2_parallel: 4,
            import_limit: None,
            verbose_logs: false,
            hint_format: None,
            skip_constraints: false,
            socket_path,
            result_file,
        }
    }

    #[test]
    fn worker_config_serde_round_trip() {
        let socket = new_socket_path();
        let result = std::env::temp_dir().join("result.json");
        let cfg = minimal_config(socket.clone(), result.clone());
        let json = serde_json::to_string(&cfg).expect("serialize");
        let back: WorkerConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.root_table, "root");
        assert_eq!(back.socket_path, socket);
        assert_eq!(back.result_file, result);
    }

    #[test]
    fn worker_result_serde_round_trip() {
        let r = WorkerResult {
            status: "success".to_string(),
            total_rows: 42,
            anomaly_count: 1,
            constraint_warning_count: 0,
            message: None,
        };
        let json = serde_json::to_string(&r).expect("serialize");
        let back: WorkerResult = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.status, "success");
        assert_eq!(back.total_rows, 42);
    }

    #[test]
    fn worker_command_serde_round_trip() {
        let cmd = WorkerCommand { cmd: "cancel".to_string() };
        let json = serde_json::to_string(&cmd).expect("serialize");
        let back: WorkerCommand = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.cmd, "cancel");
    }

    #[test]
    fn new_socket_path_uses_temp_dir() {
        let p = new_socket_path();
        assert!(p.starts_with(std::env::temp_dir()));
        let name = p.file_name().unwrap().to_string_lossy();
        assert!(name.starts_with("json2sql-"));
        assert!(name.ends_with(".sock"));
    }

    #[test]
    fn into_pass2_config_maps_fields() {
        let socket = new_socket_path();
        let result = std::env::temp_dir().join("result.json");
        let cfg = WorkerConfig {
            pass2_parallel: 8,
            import_limit: Some(500),
            skip_constraints: true,
            pg_schema: "myschema".to_string(),
            root_table: "myroot".to_string(),
            ..minimal_config(socket, result)
        };
        let p2 = cfg.into_pass2_config();
        assert_eq!(p2.parallel, 8);
        assert_eq!(p2.limit, Some(500));
        assert!(p2.skip_constraints);
        assert_eq!(p2.pg_schema, "myschema");
    }
}
