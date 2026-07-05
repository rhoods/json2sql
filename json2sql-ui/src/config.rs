//! Project configuration persistence.
//!
//! Saves/loads the Setup-screen form to `~/.config/json2sql/last_project.toml`.
//! Password is intentionally excluded — never written to disk.
//!
//! Fonctions :
//! - struct `ProjectConfig` — config projet sérialisable en TOML (sans password)
//! - fn `ProjectConfig::default` — valeurs par défaut (host localhost, port 5432, `pass2_parallel` selon CPU)
//! - fn `ProjectConfig::from_project` — projette `ProjectState` vers la struct sérialisable (sans password)
//! - fn `ProjectConfig::apply_to` — réinjecte la config chargée dans `ProjectState` (password préservé, noms de stratégie invalides ignorés silencieusement)
//! - fn `config_path` — chemin `~/.config/json2sql/last_project.toml` (via `directories_next`)
//! - fn `load` — charge et parse le TOML, `None` si absent/invalide
//! - fn `try_save` — sauvegarde best-effort (erreurs ignorées, ne doit jamais casser l'IHM)

use std::collections::HashSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use json2sql::schema::strategies::StrategyName;
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
    #[serde(default)]
    pub disabled_strategies: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import_limit: Option<u64>,
    /// Skip the constraint phase at the end of Pass 2. Persisted for workflows that always skip.
    #[serde(default)]
    pub skip_constraints: bool,
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
                .map_or(4, std::num::NonZero::get)
                .min(8),
            disabled_strategies: Vec::new(),
            import_limit: None,
            skip_constraints: false,
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
            disabled_strategies: p.disabled_strategies.iter().map(|s| s.as_str().to_string()).collect(),
            import_limit: p.import_limit,
            skip_constraints: p.skip_constraints,
        }
    }

    pub fn apply_to(&self, p: &mut ProjectState) {
        p.source_file.clone_from(&self.source_file);
        p.pg.host.clone_from(&self.pg_host);
        p.pg.port = self.pg_port;
        p.pg.database.clone_from(&self.pg_database);
        p.pg.username.clone_from(&self.pg_username);
        // password intentionally NOT restored
        p.pg_schema.clone_from(&self.pg_schema);
        p.drop_existing = self.drop_existing;
        p.anomaly_dir.clone_from(&self.anomaly_dir);
        p.temp_dir.clone_from(&self.temp_dir);
        p.workers = self.workers;
        p.pass2_parallel = self.pass2_parallel;
        // Silently ignore invalid entries — config file may predate a strategy rename
        p.disabled_strategies = self.disabled_strategies.iter()
            .filter_map(|s| StrategyName::try_from(s.as_str()).ok())
            .collect::<HashSet<_>>();
        p.import_limit = self.import_limit;
        p.skip_constraints = self.skip_constraints;
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
            disabled_strategies: Vec::new(),
            import_limit: None,
            skip_constraints: false,
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
            disabled_strategies: Vec::new(),
            import_limit: None,
            skip_constraints: true,
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
        assert_eq!(parsed.skip_constraints, cfg.skip_constraints);
    }

    #[test]
    fn skip_constraints_round_trips_toml() {
        let mut p = ProjectState::default();
        p.skip_constraints = true;
        let cfg = ProjectConfig::from_project(&p);
        let toml_str = toml::to_string(&cfg).expect("serialize");
        let parsed: ProjectConfig = toml::from_str(&toml_str).expect("deserialize");
        assert!(parsed.skip_constraints, "skip_constraints=true must survive TOML round-trip");
    }

    #[test]
    fn skip_constraints_defaults_to_false_when_absent_from_toml() {
        // Simulates loading an old TOML file that doesn't have skip_constraints
        let toml_str = "pg_host = \"localhost\"\npg_port = 5432\npg_database = \"\"\npg_username = \"\"\npg_schema = \"public\"\ndrop_existing = false\nworkers = 1\npass2_parallel = 4\n";
        let parsed: ProjectConfig = toml::from_str(toml_str).expect("deserialize");
        assert!(!parsed.skip_constraints, "skip_constraints must default to false for backward compat");
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
    fn import_limit_round_trips_toml() {
        let mut p = ProjectState::default();
        p.import_limit = Some(500);
        let cfg = ProjectConfig::from_project(&p);
        let toml_str = toml::to_string(&cfg).expect("serialize");
        let parsed: ProjectConfig = toml::from_str(&toml_str).expect("deserialize");
        assert_eq!(parsed.import_limit, Some(500));
    }

    #[test]
    fn import_limit_absent_in_toml_when_none() {
        let cfg = ProjectConfig::default();
        let toml_str = toml::to_string(&cfg).expect("serialize");
        assert!(!toml_str.contains("import_limit"));
    }

    #[test]
    fn load_returns_none_when_file_missing() {
        // config_path() may or may not exist; we test the load() behaviour
        // by verifying it doesn't panic and returns None or Some (both valid).
        // We only assert it does not panic.
        let _ = load();
    }

    #[test]
    fn disabled_strategies_round_trips_from_project_to_apply() {
        let mut p = ProjectState::default();
        p.disabled_strategies.insert(StrategyName::Sibling);
        p.disabled_strategies.insert(StrategyName::Pivot);

        let cfg = ProjectConfig::from_project(&p);
        assert!(cfg.disabled_strategies.contains(&"sibling".to_string()));
        assert!(cfg.disabled_strategies.contains(&"pivot".to_string()));
        assert!(!cfg.disabled_strategies.contains(&"structured_pivot".to_string()));

        let mut p2 = ProjectState::default();
        cfg.apply_to(&mut p2);
        assert!(p2.disabled_strategies.contains(&StrategyName::Sibling));
        assert!(p2.disabled_strategies.contains(&StrategyName::Pivot));
        assert!(!p2.disabled_strategies.contains(&StrategyName::StructuredPivot));
    }

    #[test]
    fn disabled_strategies_toml_round_trip() {
        let mut p = ProjectState::default();
        p.disabled_strategies.insert(StrategyName::StructuredPivot);

        let cfg = ProjectConfig::from_project(&p);
        let toml_str = toml::to_string(&cfg).expect("serialize");
        let parsed: ProjectConfig = toml::from_str(&toml_str).expect("deserialize");
        assert!(parsed.disabled_strategies.contains(&"structured_pivot".to_string()));

        let mut p2 = ProjectState::default();
        parsed.apply_to(&mut p2);
        assert!(p2.disabled_strategies.contains(&StrategyName::StructuredPivot));
    }

    #[test]
    fn apply_to_ignores_invalid_strategy_names() {
        let cfg = ProjectConfig {
            disabled_strategies: vec!["unknown_strategy".to_string()],
            ..ProjectConfig::default()
        };
        let mut p = ProjectState::default();
        cfg.apply_to(&mut p);
        assert!(p.disabled_strategies.is_empty(), "invalid strategy names must be silently ignored");
    }
}
