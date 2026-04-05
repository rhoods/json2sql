use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

use json2sql::schema::persistence::SchemaSnapshot;
use json2sql::schema::table_schema::{TableSchema, WideStrategy};

use crate::migration::MigrationState;

/// Un groupe nommé — Table Group ou Column Group.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Group {
    pub id: String,
    pub name: String,
    pub kind: GroupKind,
    pub table_members: Vec<String>,
    pub strategy: Option<String>,
    pub origin: GroupOrigin,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum GroupKind {
    Table,
    Column,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum GroupOrigin {
    AutoDetected,
    Modified,
    Manual,
}

pub struct AppState {
    pub snapshot: SchemaSnapshot,
    /// Fichier JSON source (optionnel — requis pour lancer Pass 2)
    pub input_file: RwLock<Option<PathBuf>>,
    /// URL de connexion PostgreSQL (optionnel — requis pour lancer Pass 2)
    pub db_url: RwLock<Option<String>>,
    /// Schéma PostgreSQL cible
    pub pg_schema: String,
    pub groups: RwLock<HashMap<String, Group>>,
    pub strategy_overrides: RwLock<HashMap<String, String>>,
    pub migration: MigrationState,
}

impl AppState {
    pub fn new(
        snapshot: SchemaSnapshot,
        input_file: Option<PathBuf>,
        db_url: Option<String>,
        pg_schema: String,
    ) -> Self {
        let auto_groups = build_auto_groups(&snapshot.schemas);
        Self {
            snapshot,
            input_file: RwLock::new(input_file),
            db_url: RwLock::new(db_url),
            pg_schema,
            groups: RwLock::new(auto_groups),
            strategy_overrides: RwLock::new(HashMap::new()),
            migration: MigrationState::new(),
        }
    }

    pub fn tables(&self) -> &[TableSchema] {
        &self.snapshot.schemas
    }

    pub fn table_by_name(&self, name: &str) -> Option<&TableSchema> {
        self.snapshot.schemas.iter().find(|t| t.name == name)
    }

    /// Reads db_url without panicking on a poisoned lock.
    pub fn read_db_url(&self) -> Option<String> {
        self.db_url.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Reads input_file without panicking on a poisoned lock.
    pub fn read_input_file(&self) -> Option<PathBuf> {
        self.input_file.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    pub fn can_migrate(&self) -> bool {
        self.read_input_file().is_some() && self.read_db_url().is_some()
    }

    /// Retourne l'ensemble des noms de tables absorbées par un override Jsonb (transitif).
    /// Utilise un BFS depuis chaque table avec override Jsonb pour propager correctement
    /// quelle que soit l'ordre des schemas dans la liste.
    pub fn absorbed_table_names(&self) -> std::collections::HashSet<String> {
        let overrides = self.strategy_overrides.read().unwrap();
        let absorbers: std::collections::HashSet<&str> = overrides
            .iter()
            .filter(|(_, v)| v.as_str() == "Jsonb")
            .map(|(k, _)| k.as_str())
            .collect();
        if absorbers.is_empty() {
            return std::collections::HashSet::new();
        }
        // Construire la map parent → enfants une seule fois
        let mut children: std::collections::HashMap<&str, Vec<&str>> =
            std::collections::HashMap::new();
        for schema in &self.snapshot.schemas {
            if let Some(ref parent) = schema.parent_table {
                children.entry(parent.as_str()).or_default().push(schema.name.as_str());
            }
        }
        // BFS depuis toutes les tables absorbantes
        let mut absorbed: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut queue: std::collections::VecDeque<&str> = std::collections::VecDeque::new();
        for &name in &absorbers {
            if let Some(kids) = children.get(name) {
                queue.extend(kids.iter().copied());
            }
        }
        while let Some(name) = queue.pop_front() {
            if absorbed.insert(name.to_string()) {
                if let Some(kids) = children.get(name) {
                    queue.extend(kids.iter().copied());
                }
            }
        }
        absorbed
    }
}

fn build_auto_groups(schemas: &[TableSchema]) -> HashMap<String, Group> {
    let mut groups = HashMap::new();

    // ── 1. Tables KeyedPivot sœurs (même parent) ─────────────────────────────
    // Un groupe KeyedPivot n'a de sens qu'avec ≥ 2 membres : on regroupe les tables
    // qui ont déjà WideStrategy::KeyedPivot ET le même parent_table.
    // Les tables KeyedPivot isolées (enfant unique) n'ont pas besoin de groupe —
    // leur stratégie est déjà portée par le champ wide_strategy.
    let mut kp_by_parent: HashMap<Option<String>, Vec<&TableSchema>> = HashMap::new();
    for schema in schemas {
        if matches!(schema.wide_strategy, WideStrategy::KeyedPivot(_)) {
            kp_by_parent
                .entry(schema.parent_table.clone())
                .or_default()
                .push(schema);
        }
    }
    for (parent, members) in &kp_by_parent {
        if members.len() < 2 {
            continue; // singleton = inutile
        }
        let parent_name = parent.as_deref().unwrap_or("root");
        let id = format!("auto_kp_{}", parent_name);
        groups.insert(
            id.clone(),
            Group {
                id,
                name: format!("{}_kp", parent_name),
                kind: GroupKind::Table,
                table_members: members.iter().map(|t| t.name.clone()).collect(),
                strategy: Some("KeyedPivot".to_string()),
                origin: GroupOrigin::AutoDetected,
            },
        );
    }

    // ── 2. Tables à clé numérique non détectées par la Pass 1 ────────────────
    // Quand un objet JSON a des clés numériques ("0", "1", "42"…), la Pass 1 peut
    // rater le regroupement KeyedPivot si la similarité Jaccard est trop faible.
    // On détecte ici les tables dont le dernier composant du path est purement
    // numérique, on les regroupe par parent et on propose un groupe KeyedPivot.
    const NUMERIC_SIBLING_MIN: usize = 3;
    let mut num_by_parent: HashMap<&str, Vec<&TableSchema>> = HashMap::new();
    for schema in schemas {
        if !matches!(schema.wide_strategy, WideStrategy::Columns) {
            continue; // déjà géré (KeyedPivot, Jsonb, etc.)
        }
        let last = schema.path.last().map(|s| s.as_str()).unwrap_or("");
        if last.chars().all(|c| c.is_ascii_digit()) {
            if let Some(ref parent) = schema.parent_table {
                num_by_parent.entry(parent.as_str()).or_default().push(schema);
            }
        }
    }
    for (parent_name, members) in &num_by_parent {
        if members.len() < NUMERIC_SIBLING_MIN {
            continue;
        }
        let id = format!("auto_num_{}", parent_name);
        if groups.contains_key(&id) {
            continue;
        }
        groups.insert(
            id.clone(),
            Group {
                id,
                name: format!("{}_merged", parent_name),
                kind: GroupKind::Table,
                table_members: members.iter().map(|t| t.name.clone()).collect(),
                strategy: Some("KeyedPivot".to_string()),
                origin: GroupOrigin::AutoDetected,
            },
        );
    }

    groups
}
