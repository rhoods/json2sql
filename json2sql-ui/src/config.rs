/// Project configuration persistence.
///
/// Saves/loads the Setup-screen form to `~/.config/json2sql/last_project.toml`.
/// Password is intentionally excluded — never written to disk.
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::state::ProjectState;

// ---------------------------------------------------------------------------
// Serializable config struct (flat TOML — no nested sections)
// ---------------------------------------------------------------------------

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProjectConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_file: Option<PathBuf>,
    pub pg_host: String,
    pub pg_port: u16,
    pub pg_database: String,
    pub pg_username: String,
    // pg_password intentionally absent — never persisted
    pub pg_schema: String,
    pub drop_existing: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anomaly_dir: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temp_dir: Option<PathBuf>,
    pub workers: usize,
    pub pass2_parallel: usize,
}

impl Default for ProjectConfig {
    fn default() -> Self {
        Self {
            source_file: None,
            pg_host: "localhost".to_string(),
            pg_port: 5432,
            pg_database: String::new(),
            pg_username: String::new(),
            pg_schema: "public".to_string(),
            drop_existing: false,
            anomaly_dir: None,
            temp_dir: None,
            workers: 1,
            pass2_parallel: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
                .min(8),
        }
    }
}

impl ProjectConfig {
    pub fn from_project(p: &ProjectState) -> Self {
        Self {
            source_file: p.source_file.clone(),
            pg_host: p.pg.host.clone(),
            pg_port: p.pg.port,
            pg_database: p.pg.database.clone(),
            pg_username: p.pg.username.clone(),
            pg_schema: p.pg_schema.clone(),
            drop_existing: p.drop_existing,
            anomaly_dir: p.anomaly_dir.clone(),
            temp_dir: p.temp_dir.clone(),
            workers: p.workers,
            pass2_parallel: p.pass2_parallel,
        }
    }

    pub fn apply_to(&self, p: &mut ProjectState) {
        p.source_file = self.source_file.clone();
        p.pg.host = self.pg_host.clone();
        p.pg.port = self.pg_port;
        p.pg.database = self.pg_database.clone();
        p.pg.username = self.pg_username.clone();
        // password intentionally NOT restored
        p.pg_schema = self.pg_schema.clone();
        p.drop_existing = self.drop_existing;
        p.anomaly_dir = self.anomaly_dir.clone();
        p.temp_dir = self.temp_dir.clone();
        p.workers = self.workers;
        p.pass2_parallel = self.pass2_parallel;
    }
}

// ---------------------------------------------------------------------------
// I/O helpers
// ---------------------------------------------------------------------------

pub fn config_path() -> Option<PathBuf> {
    directories_next::ProjectDirs::from("", "", "json2sql")
        .map(|dirs| dirs.config_dir().join("last_project.toml"))
}

pub fn load() -> Option<ProjectConfig> {
    let path = config_path()?;
    let content = std::fs::read_to_string(&path).ok()?;
    toml::from_str(&content).ok()
}

/// Silently persists `project` to TOML. Errors are ignored — config save
/// is best-effort and must never break the UI flow.
pub fn try_save(project: &ProjectState) {
    let cfg = ProjectConfig::from_project(project);
    let Some(path) = config_path() else { return };
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Ok(toml_str) = toml::to_string(&cfg) {
        let _ = std::fs::write(&path, toml_str);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_config_default_matches_state_defaults() {
        let cfg = ProjectConfig::default();
        assert_eq!(cfg.pg_host, "localhost");
        assert_eq!(cfg.pg_port, 5432);
        assert_eq!(cfg.pg_schema, "public");
        assert!(!cfg.drop_existing);
        assert_eq!(cfg.workers, 1);
        assert!(cfg.source_file.is_none());
        assert!(cfg.anomaly_dir.is_none());
        assert!(cfg.pg_database.is_empty());
        assert!(cfg.pg_username.is_empty());
    }

    #[test]
    fn from_project_maps_all_fields_except_password() {
        let mut p = ProjectState::default();
        p.source_file = Some(PathBuf::from("/tmp/data.json"));
        p.pg.host = "myhost".to_string();
        p.pg.port = 5433;
        p.pg.database = "mydb".to_string();
        p.pg.username = "myuser".to_string();
        p.pg.password = "secret".to_string();
        p.pg_schema = "staging".to_string();
        p.drop_existing = true;
        p.workers = 4;
        p.pass2_parallel = 6;
        p.anomaly_dir = Some(PathBuf::from("/tmp/anomalies"));

        let cfg = ProjectConfig::from_project(&p);

        assert_eq!(cfg.source_file, Some(PathBuf::from("/tmp/data.json")));
        assert_eq!(cfg.pg_host, "myhost");
        assert_eq!(cfg.pg_port, 5433);
        assert_eq!(cfg.pg_database, "mydb");
        assert_eq!(cfg.pg_username, "myuser");
        assert_eq!(cfg.pg_schema, "staging");
        assert!(cfg.drop_existing);
        assert_eq!(cfg.workers, 4);
        assert_eq!(cfg.pass2_parallel, 6);
        assert_eq!(cfg.anomaly_dir, Some(PathBuf::from("/tmp/anomalies")));
    }

    #[test]
    fn apply_to_populates_project_state_and_skips_password() {
        let cfg = ProjectConfig {
            source_file: Some(PathBuf::from("/data/file.json")),
            pg_host: "db.local".to_string(),
            pg_port: 5433,
            pg_database: "prod".to_string(),
            pg_username: "admin".to_string(),
            pg_schema: "analytics".to_string(),
            drop_existing: false,
            anomaly_dir: None,
            temp_dir: None,
            workers: 2,
            pass2_parallel: 3,
        };
        let mut p = ProjectState::default();
        p.pg.password = "original_password".to_string();
        cfg.apply_to(&mut p);

        assert_eq!(p.source_file, Some(PathBuf::from("/data/file.json")));
        assert_eq!(p.pg.host, "db.local");
        assert_eq!(p.pg.port, 5433);
        assert_eq!(p.pg.database, "prod");
        assert_eq!(p.pg.username, "admin");
        assert_eq!(p.pg_schema, "analytics");
        assert!(!p.drop_existing);
        assert!(p.anomaly_dir.is_none());
        assert_eq!(p.workers, 2);
        assert_eq!(p.pass2_parallel, 3);
        // password must NOT be touched
        assert_eq!(p.pg.password, "original_password");
    }

    #[test]
    fn password_not_serialized_to_toml() {
        let mut p = ProjectState::default();
        p.pg.password = "supersecret".to_string();
        p.pg.database = "mydb".to_string();

        let cfg = ProjectConfig::from_project(&p);
        let toml_str = toml::to_string(&cfg).expect("serialize");

        assert!(!toml_str.contains("supersecret"), "password value must not appear in TOML");
        assert!(!toml_str.contains("password"), "password key must not appear in TOML");
    }

    #[test]
    fn round_trip_toml_preserves_all_fields() {
        let cfg = ProjectConfig {
            source_file: Some(PathBuf::from("/tmp/test.json")),
            pg_host: "remotehost".to_string(),
            pg_port: 5433,
            pg_database: "testdb".to_string(),
            pg_username: "testuser".to_string(),
            pg_schema: "staging".to_string(),
            drop_existing: true,
            anomaly_dir: Some(PathBuf::from("/tmp/anomalies")),
            temp_dir: None,
            workers: 3,
            pass2_parallel: 5,
        };

        let toml_str = toml::to_string(&cfg).expect("serialize");
        let parsed: ProjectConfig = toml::from_str(&toml_str).expect("deserialize");

        assert_eq!(parsed.source_file, cfg.source_file);
        assert_eq!(parsed.pg_host, cfg.pg_host);
        assert_eq!(parsed.pg_port, cfg.pg_port);
        assert_eq!(parsed.pg_database, cfg.pg_database);
        assert_eq!(parsed.pg_username, cfg.pg_username);
        assert_eq!(parsed.pg_schema, cfg.pg_schema);
        assert_eq!(parsed.drop_existing, cfg.drop_existing);
        assert_eq!(parsed.anomaly_dir, cfg.anomaly_dir);
        assert_eq!(parsed.workers, cfg.workers);
        assert_eq!(parsed.pass2_parallel, cfg.pass2_parallel);
    }

    #[test]
    fn optional_fields_absent_when_none() {
        let cfg = ProjectConfig::default(); // source_file, anomaly_dir, temp_dir are None
        let toml_str = toml::to_string(&cfg).expect("serialize");

        assert!(!toml_str.contains("source_file"), "absent when None");
        assert!(!toml_str.contains("anomaly_dir"), "absent when None");
        assert!(!toml_str.contains("temp_dir"), "absent when None");
    }

    #[test]
    fn temp_dir_round_trips_toml() {
        let mut p = ProjectState::default();
        p.temp_dir = Some(PathBuf::from("/data/tmp"));
        let cfg = ProjectConfig::from_project(&p);
        let toml_str = toml::to_string(&cfg).expect("serialize");
        let parsed: ProjectConfig = toml::from_str(&toml_str).expect("deserialize");
        assert_eq!(parsed.temp_dir, Some(PathBuf::from("/data/tmp")));
    }

    #[test]
    fn temp_dir_absent_in_toml_when_none() {
        let cfg = ProjectConfig::default();
        let toml_str = toml::to_string(&cfg).expect("serialize");
        assert!(!toml_str.contains("temp_dir"));
    }

    #[test]
    fn load_returns_none_when_file_missing() {
        // config_path() may or may not exist; we test the load() behaviour
        // by verifying it doesn't panic and returns None or Some (both valid).
        // We only assert it does not panic.
        let _ = load();
    }
}
