//! Application des overrides utilisateur aux schémas inférés.
//!
//! Fonctions :
//! - fn `build_effective_schemas` — applique les overrides utilisateur à une copie des schémas (déduplique défensivement, retire aussi la table `_wide` compagnon si le parent est skip)
//! - fn `skip_cascade_names` — noms des vrais enfants retirés par cascade d'un `Skip` (#47),
//!   pour affichage (badges, récapitulatif) sans muter les schémas.
use std::collections::{HashMap, HashSet};
use json2sql::schema::table_schema::{TableSchema, UserOverride};

/// Apply user strategy overrides (`Pivot | Jsonb | Skip`) to a copy of `schemas`.
///
/// Delegates to `json2sql::schema::config::apply_user_overrides` so both the GUI
/// and CLI paths produce identical output for the same overrides. `Skip` removes the
/// table and — for `AutoSplit` tables — also removes the companion `_wide` table.
/// The original slice is never mutated.
pub fn build_effective_schemas(
    schemas: &[TableSchema],
    strategy_overrides: &HashMap<String, UserOverride>,
) -> Vec<TableSchema> {
    let mut result = schemas.to_vec();
    json2sql::schema::config::apply_user_overrides(&mut result, strategy_overrides);
    // Defensive dedup: stale snapshots saved before the finalizer fix may contain
    // duplicate table names, which would cause add_constraints() to fail with 42P16.
    let mut seen = std::collections::HashSet::new();
    result.retain(|s| seen.insert(s.name.clone()));
    result
}

/// Names of real children that will be removed by cascade from a `Skip` override on an
/// ancestor (direct root or its `AutoSplit` `_wide` companion) — never set by the user
/// directly. A pure query: `schemas` is never mutated, so this is safe to call on the
/// full, un-reduced table list (e.g. the Strategy screen, before `build_effective_schemas`
/// has removed anything).
pub fn skip_cascade_names(
    schemas: &[TableSchema],
    strategy_overrides: &HashMap<String, UserOverride>,
) -> HashSet<String> {
    let (_removed, warnings) = json2sql::schema::config::compute_skip_cascade(schemas, strategy_overrides);
    warnings.into_iter().flat_map(|w| w.cascaded_children).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use json2sql::schema::table_schema::InferredStrategy;

    fn make_table(name: &str, parent: Option<&str>) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        t.parent_table = parent.map(str::to_string);
        t
    }

    #[test]
    fn ignore_removes_table() {
        let schemas = vec![make_table("a", None), make_table("b", None)];
        let mut overrides = HashMap::new();
        overrides.insert("a".to_string(), UserOverride::Skip);
        let result = build_effective_schemas(&schemas, &overrides);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "b");
    }

    #[test]
    fn no_overrides_returns_full_clone() {
        let schemas = vec![make_table("x", None), make_table("y", None)];
        let result = build_effective_schemas(&schemas, &HashMap::new());
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "x");
    }

    #[test]
    fn jsonb_override_applied() {
        use json2sql::schema::table_schema::ColumnSchema;
        use json2sql::schema::type_tracker::PgType;

        let mut t = make_table("wide", None);
        // Add a data column so apply_wide_strategy_columns has something to restructure.
        t.columns.push(ColumnSchema {
            name: "val".to_string(),
            original_name: "val".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        let schemas = vec![t];
        let mut overrides = HashMap::new();
        overrides.insert("wide".to_string(), UserOverride::Jsonb);
        let result = build_effective_schemas(&schemas, &overrides);
        assert_eq!(result[0].ui_override(), Some(&UserOverride::Jsonb), "ui_override must be set");
        assert_eq!(result[0].inferred_strategy, InferredStrategy::Columns, "inferred_strategy must be preserved");
        assert_eq!(*result[0].effective_strategy(), InferredStrategy::Jsonb, "effective_strategy must reflect override");
    }

    #[test]
    fn original_schemas_not_mutated() {
        let schemas = vec![make_table("a", None), make_table("b", None)];
        let original_len = schemas.len();
        let mut overrides = HashMap::new();
        overrides.insert("a".to_string(), UserOverride::Skip);
        let _ = build_effective_schemas(&schemas, &overrides);
        assert_eq!(schemas.len(), original_len, "original slice must not be mutated");
    }

    #[test]
    fn duplicate_names_in_input_are_deduped() {
        // Regression: stale snapshots saved before the finalizer dedup fix can
        // contain duplicate table names → add_constraints() would fail with 42P16.
        // build_effective_schemas must dedup defensively.
        let schemas = vec![
            make_table("a", None),
            make_table("b", None),
            make_table("a", None), // duplicate
        ];
        let result = build_effective_schemas(&schemas, &HashMap::new());
        assert_eq!(result.len(), 2, "duplicate table names must be removed");
        assert!(result.iter().any(|s| s.name == "a"));
        assert!(result.iter().any(|s| s.name == "b"));
    }

    #[test]
    fn skip_autosplit_removes_companion_wide_table() {
        // AutoSplit produces two tables: the main (e.g. "events") and a companion
        // "events_wide". Applying Skip on the main must remove BOTH so the GUI
        // Preview shows the same schema as the CLI snapshot-restore path.
        let mut main = make_table("events", None);
        main.inferred_strategy = InferredStrategy::AutoSplit {
            stable_threshold: 0.8,
            rare_threshold: 0.1,
            medium_keys: std::collections::HashSet::new(),
            wide_table_name: "events_wide".to_string(),
        };
        let companion = make_table("events_wide", None);
        let other = make_table("users", None);

        let schemas = vec![main, companion, other];
        let mut overrides = HashMap::new();
        overrides.insert("events".to_string(), UserOverride::Skip);
        let result = build_effective_schemas(&schemas, &overrides);

        let names: Vec<&str> = result.iter().map(|s| s.name.as_str()).collect();
        assert!(!names.contains(&"events"),      "main table must be removed");
        assert!(!names.contains(&"events_wide"), "companion _wide table must be removed");
        assert!(names.contains(&"users"),        "unrelated table must survive");
    }

    #[test]
    fn skip_cascade_names_reports_real_child_without_mutating_input() {
        let parent = make_table("parent", None);
        let child = make_table("child", Some("parent"));
        let schemas = vec![parent, child];
        let mut overrides = HashMap::new();
        overrides.insert("parent".to_string(), UserOverride::Skip);

        let names = skip_cascade_names(&schemas, &overrides);

        assert_eq!(names, std::collections::HashSet::from(["child".to_string()]));
        assert_eq!(schemas.len(), 2, "schemas must not be mutated by a display-only query");
    }

    #[test]
    fn skip_cascade_names_empty_when_no_real_children() {
        let schemas = vec![make_table("a", None), make_table("b", None)];
        let mut overrides = HashMap::new();
        overrides.insert("a".to_string(), UserOverride::Skip);

        let names = skip_cascade_names(&schemas, &overrides);

        assert!(names.is_empty(), "a plain Skip with no real children cascades nothing");
    }
}
