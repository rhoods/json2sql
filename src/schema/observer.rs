//! Pass 1 observation: accumulates JSON structure into `TableEntry` records as the
//! input is streamed.
//!
//! [`SchemaObserver`] is the per-worker accumulator; each JSON object updates the relevant
//! `TableEntry` via its `observe` method. Multi-worker results are merged via
//! `TableEntry::merge` before finalization.
//!
//! Fonctions :
//! - `TableEntry::new` — crée une entrée de table vide pour un chemin donné.
//! - `TableEntry::merge` — fusionne les colonnes/compteurs d'une autre entrée (multi-worker).
//! - `TableEntry::observe_field` — met à jour le `TypeTracker` d'un champ scalaire.
//! - `SchemaObserver::new` — crée l'observateur (seuil texte, mode array-as-pg-array).
//! - `SchemaObserver::observe_root` — observe un objet racine via une pile explicite (pas de
//!   récursion, évite tout stack overflow sur imbrication profonde).
//! - `SchemaObserver::observe_array_field` — route un champ tableau (objets → table enfant,
//!   scalaires → colonne PG array ou table de jonction selon `array_as_pg_array`).
//! - `SchemaObserver::merge` — fusionne un observateur d'un autre worker (Pass 1 parallèle).
//! - `SchemaObserver::anomaly_iter` — itère les colonnes ayant des anomalies de type.
//! - `SchemaObserver::ensure_table_key` — crée l'entrée de table si absente (idempotent).

use indexmap::IndexMap;
use serde_json::Value;

use super::PATH_SEP;
use super::table_schema::ChildKind;
use super::type_tracker::TypeTracker;

/// One entry per table discovered during Pass 1 observation, keyed by PATH_SEP-joined path.
#[derive(Debug)]
pub struct TableEntry {
    pub(crate) path_key: String,
    pub(crate) path: Vec<String>,
    pub(crate) parent_key: String,
    pub(crate) columns: IndexMap<String, TypeTracker>,
    pub(crate) child_kind: Option<ChildKind>,
    /// For scalar junction tables, the element type tracker.
    pub(crate) scalar_tracker: Option<TypeTracker>,
    /// Element type trackers for scalar arrays stored as PG array columns (`array_as_pg_array` mode).
    pub(crate) array_columns: IndexMap<String, TypeTracker>,
    pub(crate) row_count: u64,
}

impl TableEntry {
    fn new(path: Vec<String>, parent_key: String, child_kind: Option<ChildKind>) -> Self {
        let path_key = path.join(&PATH_SEP.to_string());
        Self {
            path_key,
            path,
            parent_key,
            columns: IndexMap::new(),
            child_kind,
            scalar_tracker: None,
            array_columns: IndexMap::new(),
            row_count: 0,
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.row_count += other.row_count;
        for (name, tracker) in other.columns {
            if let Some(existing) = self.columns.get_mut(&name) {
                existing.merge(&tracker);
            } else {
                self.columns.insert(name, tracker);
            }
        }
        for (name, tracker) in other.array_columns {
            if let Some(existing) = self.array_columns.get_mut(&name) {
                existing.merge(&tracker);
            } else {
                self.array_columns.insert(name, tracker);
            }
        }
        match (&mut self.scalar_tracker, other.scalar_tracker) {
            (Some(a), Some(b)) => a.merge(&b),
            (None, Some(b))    => self.scalar_tracker = Some(b),
            _                  => {}
        }
    }

    fn observe_field(&mut self, field: &str, value: &Value, text_threshold: u32) {
        if let Some(tracker) = self.columns.get_mut(field) {
            tracker.observe(value);
        } else {
            self.columns
                .entry(field.to_string())
                .or_insert_with(|| TypeTracker::new(text_threshold))
                .observe(value);
        }
    }
}

/// Accumulates raw JSON observations row-by-row during Pass 1 streaming.
///
/// Responsible for: table discovery, column type tracking, scalar/array routing.
/// Does not finalize, name, or transform schemas — that is `SchemaFinalizer`'s job (T2).
pub struct SchemaObserver {
    pub(crate) tables: IndexMap<String, TableEntry>,
    pub(crate) text_threshold: u32,
    array_as_pg_array: bool,
}

impl SchemaObserver {
    #[must_use]
    pub fn new(text_threshold: u32, array_as_pg_array: bool) -> Self {
        Self {
            tables: IndexMap::new(),
            text_threshold,
            array_as_pg_array,
        }
    }

    /// Observe a single root-level JSON object.
    ///
    /// Uses an explicit heap stack instead of recursion to handle arbitrarily
    /// deep nesting without risk of stack overflow.
    pub fn observe_root(&mut self, root_name: &str, obj: &serde_json::Map<String, Value>) {
        self.ensure_table_key(root_name, "", None);

        let mut stack: Vec<(String, &serde_json::Map<String, Value>)> =
            vec![(root_name.to_string(), obj)];

        while let Some((path_key, map)) = stack.pop() {
            if let Some(entry) = self.tables.get_mut(&path_key) {
                entry.row_count += 1;
            }

            for (field, value) in map {
                match value {
                    Value::Object(nested) => {
                        let child_key = format!("{path_key}{PATH_SEP}{field}");
                        self.ensure_table_key(&child_key, &path_key, Some(ChildKind::Object));
                        stack.push((child_key, nested));
                    }
                    Value::Array(arr) => {
                        if !arr.is_empty() {
                            let child_key = format!("{path_key}{PATH_SEP}{field}");
                            self.observe_array_field(&mut stack, &path_key, field, arr, &child_key);
                        }
                    }
                    scalar => {
                        let threshold = self.text_threshold;
                        self.tables
                            .get_mut(&path_key)
                            .expect("invariant: path_key was popped from stack, must exist in tables")
                            .observe_field(field, scalar, threshold);
                    }
                }
            }
        }
    }

    fn observe_array_field<'s>(
        &mut self,
        stack: &mut Vec<(String, &'s serde_json::Map<String, Value>)>,
        path_key: &str,
        field: &str,
        arr: &'s [Value],
        child_key: &str,
    ) {
        let first_is_object = arr.iter().any(|v| matches!(v, Value::Object(_)));
        if first_is_object {
            self.ensure_table_key(child_key, path_key, Some(ChildKind::ObjectArray));
            let mut objs: Vec<&serde_json::Map<String, Value>> = arr
                .iter()
                .filter_map(|v| if let Value::Object(o) = v { Some(o) } else { None })
                .collect();
            if let Some(last) = objs.pop() {
                for obj in objs { stack.push((child_key.to_string(), obj)); }
                stack.push((child_key.to_string(), last));
            }
        } else if self.array_as_pg_array {
            let threshold = self.text_threshold;
            let entry = self.tables.get_mut(path_key).expect("invariant: path_key was popped from stack, must exist in tables");
            let tracker = entry.array_columns.entry(field.to_string()).or_insert_with(|| TypeTracker::new(threshold));
            for item in arr { tracker.observe(item); }
        } else {
            self.ensure_table_key(child_key, path_key, Some(ChildKind::ScalarArray));
            let threshold = self.text_threshold;
            let entry = self.tables.get_mut(child_key).expect("invariant: child_key just inserted by ensure_table_key");
            let tracker = entry.scalar_tracker.get_or_insert_with(|| TypeTracker::new(threshold));
            for item in arr { tracker.observe(item); }
        }
    }

    /// Merge all observations from `other` into `self`.
    /// Used after parallel Pass 1: each worker builds its own observer, then they are merged.
    pub fn merge(&mut self, other: Self) {
        for (key, other_entry) in other.tables {
            if let Some(entry) = self.tables.get_mut(&key) {
                entry.merge(other_entry);
            } else {
                self.tables.insert(key, other_entry);
            }
        }
    }

    /// Iterate over columns that have anomalies: (`table_path_key`, `col_original_name`, `TypeTracker`).
    /// Used after Pass 1 to feed the anomaly report.
    pub fn anomaly_iter(&self) -> impl Iterator<Item = (&str, &str, &TypeTracker)> {
        self.tables.values().flat_map(|entry| {
            entry.columns.iter().filter_map(|(field, tracker)| {
                if tracker.has_anomalies() {
                    Some((entry.path_key.as_str(), field.as_str(), tracker))
                } else {
                    None
                }
            })
        })
    }

    fn ensure_table_key(&mut self, key: &str, parent_key: &str, child_kind: Option<ChildKind>) {
        self.tables.entry(key.to_string()).or_insert_with(|| {
            let path: Vec<String> = key.split(PATH_SEP).map(std::string::ToString::to_string).collect();
            TableEntry::new(path, parent_key.to_string(), child_kind)
        });
    }
}
