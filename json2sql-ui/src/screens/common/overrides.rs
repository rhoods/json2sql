//! Application des overrides utilisateur aux schémas inférés.
//!
//! Fonctions :
//! - fn `build_effective_schemas` — applique les overrides utilisateur à une copie des schémas (déduplique défensivement, retire aussi la table `_wide` compagnon si le parent est skip), puis découpe les tables cochées en groupes de colonnes (#91)
//! - fn `display_schemas` — tables de l'écran Strategy : les tables d'origine (indexées), puis les tables de groupe des tables cochées.
//! - fn `splittable_tables` — tables que les noms de colonnes permettent de découper : ce que coche l'option de Setup.
//! - enum `GroupPreview` — aperçu des groupes de colonnes d'une table : tables de groupe et leurs colonnes, ou raison de leur absence.
//! - struct `PreviewGroup` — une table de groupe d'un aperçu : nom, colonnes et colonnes candidates à confirmer.
//! - fn `group_preview` — aperçu des groupes de colonnes d'une table, sur le schéma effectif, avec les noms qu'aura l'import.
//! - fn `not_splittable` — raison pour laquelle une table du schéma effectif ne peut pas être découpée, s'il y en a une.
//! - fn `skip_cascade_names` — noms des vrais enfants retirés par cascade d'un `Skip` (#47),
//!   pour affichage (badges, récapitulatif) sans muter les schémas.
use std::collections::{BTreeSet, HashMap, HashSet};
use json2sql::schema::grouping::auto::{apply_name_groups, name_proposals};
use json2sql::schema::grouping::proposal::Status;
use json2sql::schema::grouping::sample::StoredSample;
use json2sql::schema::grouping::suggest::MIN_TABLE_COLUMNS;
use json2sql::schema::naming::group_table_name;
use json2sql::schema::table_schema::{TableSchema, UserOverride};

/// Apply user strategy overrides (`Pivot | Jsonb | Skip`) to a copy of `schemas`.
///
/// Delegates to `json2sql::schema::config::apply_user_overrides` so both the GUI
/// and CLI paths produce identical output for the same overrides. `Skip` removes the
/// table and — for `AutoSplit` tables — also removes the companion `_wide` table.
/// The original slice is never mutated. The tables in `group_columns` are then split into the column groups their
/// names confirm (#91), after the overrides, so that a `Pivot`, `Jsonb` or `Skip` wins: the CLI does the same with a
/// snapshot that holds these switches.
pub fn build_effective_schemas(
    schemas: &[TableSchema],
    strategy_overrides: &HashMap<String, UserOverride>,
    group_columns: &BTreeSet<String>,
    samples: &[StoredSample],
) -> Vec<TableSchema> {
    let mut result = schemas.to_vec();
    json2sql::schema::config::apply_user_overrides(&mut result, strategy_overrides);
    // Defensive dedup: stale snapshots saved before the finalizer fix may contain
    // duplicate table names, which would cause add_constraints() to fail with 42P16.
    let mut seen = std::collections::HashSet::new();
    result.retain(|s| seen.insert(s.name.clone()));
    if !group_columns.is_empty() {
        // the only error is a table of over 2000 columns, which the 1600-column guard already turned into JSONB
        if let Err(e) = apply_name_groups(&mut result, samples, |t| group_columns.contains(&t.name)) {
            eprintln!("column groups not applied: {e}");
        }
    }
    result
}

/// The tables the Strategy screen lists: the original tables, which its indices point into (a split table as the import
/// will have it, without the columns that moved), then the group tables of the ticked tables, which the tree shows
/// under their parent and which are read-only there.
pub fn display_schemas(
    schemas: &[TableSchema],
    strategy_overrides: &HashMap<String, UserOverride>,
    group_columns: &BTreeSet<String>,
    samples: &[StoredSample],
) -> Vec<TableSchema> {
    let effective = build_effective_schemas(schemas, strategy_overrides, group_columns, samples);
    let groups: HashSet<&str> = effective.iter().flat_map(|t| t.column_groups.iter().map(String::as_str)).collect();
    let mut shown: Vec<TableSchema> = schemas
        .iter()
        .map(|t| {
            effective
                .iter()
                .find(|e| e.name == t.name && !e.column_groups.is_empty())
                .unwrap_or(t)
                .clone()
        })
        .collect();
    shown.extend(
        effective
            .iter()
            .filter(|t| groups.contains(t.name.as_str()) && !schemas.iter().any(|s| s.name == t.name))
            .cloned(),
    );
    shown
}

/// The tables the column names can split on the schema the overrides give: what the Setup switch ticks, like
/// `--group-columns` does in the CLI.
pub fn splittable_tables(
    schemas: &[TableSchema],
    strategy_overrides: &HashMap<String, UserOverride>,
    samples: &[StoredSample],
) -> BTreeSet<String> {
    let effective = build_effective_schemas(schemas, strategy_overrides, &BTreeSet::new(), samples);
    name_proposals(&effective, samples, |_| true)
        .unwrap_or_default()
        .into_iter()
        .filter(|p| p.groups.iter().any(|g| g.status == Status::Confirmed))
        .map(|p| p.table)
        .collect()
}

/// One group table of a preview.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewGroup {
    pub table: String,
    pub columns: Vec<String>,
    /// Columns left in the table that the data tie to this group: candidates to confirm, never moved (#91, T8).
    pub candidates: Vec<String>,
    /// The group fuses several families of names, which the data of Pass 1 confirmed (#91, T8).
    pub fused: bool,
}

/// What splitting a table into column groups would give.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupPreview {
    /// Each group table, with its columns and its candidates.
    Groups(Vec<PreviewGroup>),
    /// Why the table gets no group.
    Unavailable(String),
}

/// The column groups `table` would get, on the schema the overrides give. The tables already in `group_columns` take
/// part, so that the names shown are those of the import even when two group names would collide.
pub fn group_preview(
    schemas: &[TableSchema],
    strategy_overrides: &HashMap<String, UserOverride>,
    group_columns: &BTreeSet<String>,
    samples: &[StoredSample],
    table: &str,
) -> GroupPreview {
    let effective = build_effective_schemas(schemas, strategy_overrides, &BTreeSet::new(), samples);
    if let Some(why) = not_splittable(&effective, table) {
        return GroupPreview::Unavailable(why);
    }
    let proposals = match name_proposals(&effective, samples, |s| s.name == table || group_columns.contains(&s.name)) {
        Ok(proposals) => proposals,
        Err(e) => return GroupPreview::Unavailable(e.to_string()),
    };
    let groups: Vec<PreviewGroup> = proposals
        .iter()
        .filter(|p| p.table == table)
        .flat_map(|p| {
            p.groups.iter().enumerate().filter(|(_, g)| g.status == Status::Confirmed).map(move |(at, g)| PreviewGroup {
                table: group_table_name(table, &g.name),
                columns: g.columns.iter().map(|&c| p.columns[c].name.clone()).collect(),
                candidates: p.attachments.iter().filter(|a| a.group == at).map(|a| p.columns[a.column].name.clone()).collect(),
                fused: g.families > 1,
            })
        })
        .collect();
    let moved: usize = groups.iter().map(|g| g.columns.len()).sum();
    let data_columns = effective.iter().find(|s| s.name == table).map_or(0, |t| t.data_columns().count());
    if groups.is_empty() {
        GroupPreview::Unavailable("the column names give no group".to_string())
    } else if moved == data_columns {
        GroupPreview::Unavailable("every data column would leave the table: it is not split".to_string())
    } else {
        GroupPreview::Groups(groups)
    }
}

/// Why `table` of the effective schema cannot be split at all, if it cannot: skipped, a strategy other than
/// `Columns` or `AutoSplit`, too few data columns, already split or a group table.
fn not_splittable(effective: &[TableSchema], table: &str) -> Option<String> {
    let Some(target) = effective.iter().find(|s| s.name == table) else {
        return Some("the table is skipped: it is not imported".to_string());
    };
    if !target.takes_column_groups() {
        let strategy = format!("{:?}", *target.effective_strategy());
        let short = strategy.split(['(', ' ', '{']).next().unwrap_or_default();
        return Some(format!("{short} strategy: only Columns and AutoSplit tables can be split"));
    }
    let data_columns = target.data_columns().count();
    if data_columns < MIN_TABLE_COLUMNS {
        return Some(format!("{data_columns} data columns: at least {MIN_TABLE_COLUMNS} data columns are needed"));
    }
    let split = !target.column_groups.is_empty() || effective.iter().any(|s| s.column_groups.iter().any(|g| g == table));
    split.then(|| "already split by the column groups of the snapshot".to_string())
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
    use std::collections::BTreeSet;

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
        let result = build_effective_schemas(&schemas, &overrides, &BTreeSet::new(), &[]);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "b");
    }

    #[test]
    fn no_overrides_returns_full_clone() {
        let schemas = vec![make_table("x", None), make_table("y", None)];
        let result = build_effective_schemas(&schemas, &HashMap::new(), &BTreeSet::new(), &[]);
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
        let result = build_effective_schemas(&schemas, &overrides, &BTreeSet::new(), &[]);
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
        let _ = build_effective_schemas(&schemas, &overrides, &BTreeSet::new(), &[]);
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
        let result = build_effective_schemas(&schemas, &HashMap::new(), &BTreeSet::new(), &[]);
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
        let result = build_effective_schemas(&schemas, &overrides, &BTreeSet::new(), &[]);

        let names: Vec<&str> = result.iter().map(|s| s.name.as_str()).collect();
        assert!(!names.contains(&"events"),      "main table must be removed");
        assert!(!names.contains(&"events_wide"), "companion _wide table must be removed");
        assert!(names.contains(&"users"),        "unrelated table must survive");
    }

    /// A root table `name` with six `kiwi_*`, six `zulu_*` and twelve unrelated columns: two groups by the names.
    fn wide_table(name: &str) -> TableSchema {
        use json2sql::schema::table_schema::ColumnSchema;
        use json2sql::schema::type_tracker::PgType;
        let mut t = make_table(name, None);
        t.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        let others = ["quartz", "bamboo", "violin", "ember", "glacier", "harbor", "lantern", "meadow", "oyster", "pebble", "saddle", "thimble"];
        let names = ('a'..='f').flat_map(|c| [format!("kiwi_{c}"), format!("zulu_{c}")]).chain(others.iter().map(|n| (*n).to_string()));
        t.columns.extend(names.map(|n| ColumnSchema {
            name: n.clone(),
            original_name: n,
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        }));
        t.row_count = 10;
        t
    }

    fn ticked(names: &[&str]) -> BTreeSet<String> {
        names.iter().map(|n| (*n).to_string()).collect()
    }

    fn names(schemas: &[TableSchema]) -> Vec<&str> {
        schemas.iter().map(|s| s.name.as_str()).collect()
    }

    #[test]
    fn a_ticked_table_is_split_in_the_effective_schema_only() {
        let schemas = vec![wide_table("a"), wide_table("b")];
        let result = build_effective_schemas(&schemas, &HashMap::new(), &ticked(&["b"]), &[]);
        assert_eq!(names(&result), ["a", "b", "b_kiwi", "b_zulu"]);
        assert_eq!(names(&schemas), ["a", "b"], "the schemas of the Strategy screen keep their indices");
    }

    #[test]
    fn a_pivot_or_skip_chosen_for_a_ticked_table_wins() {
        let schemas = vec![wide_table("a"), wide_table("b")];
        let overrides = HashMap::from([("a".to_string(), UserOverride::Pivot), ("b".to_string(), UserOverride::Skip)]);
        let result = build_effective_schemas(&schemas, &overrides, &ticked(&["a", "b"]), &[]);
        assert_eq!(names(&result), ["a"]);
    }

    #[test]
    fn the_strategy_screen_lists_the_group_tables_after_the_tables_it_indexes() {
        // the indices of the Strategy screen point into the original tables: the group tables come after them
        let schemas = vec![wide_table("a"), wide_table("b")];
        let shown = display_schemas(&schemas, &HashMap::new(), &ticked(&["a"]), &[]);
        assert_eq!(names(&shown), ["a", "b", "a_kiwi", "a_zulu"]);
        let kept: Vec<&str> = shown[0].data_columns().map(|c| c.name.as_str()).collect();
        assert!(!kept.contains(&"kiwi_a") && kept.contains(&"quartz"), "a split table shows the columns it keeps: {kept:?}");
        assert_eq!(shown[0].column_groups, ["a_kiwi", "a_zulu"]);
        assert_eq!(shown[1].columns.len(), schemas[1].columns.len(), "a table that is not split is shown as it is");
        assert_eq!(names(&display_schemas(&schemas, &HashMap::new(), &ticked(&[]), &[])), ["a", "b"]);
    }

    #[test]
    fn the_splittable_tables_are_those_that_get_groups() {
        // the Setup switch ticks them all, like --group-columns
        let schemas = vec![wide_table("a"), make_table("small", None), wide_table("b")];
        let overrides = HashMap::from([("b".to_string(), UserOverride::Jsonb)]);
        assert_eq!(splittable_tables(&schemas, &overrides, &[]), ticked(&["a"]));
    }

    #[test]
    fn the_preview_lists_the_group_tables_or_says_why_there_are_none() {
        let schemas = vec![wide_table("a"), make_table("small", None)];
        match group_preview(&schemas, &HashMap::new(), &ticked(&[]), &[], "a") {
            GroupPreview::Groups(groups) => {
                let tables: Vec<&str> = groups.iter().map(|g| g.table.as_str()).collect();
                assert_eq!(tables, ["a_kiwi", "a_zulu"]);
                assert_eq!(groups[0].columns.len(), 6);
                assert!(groups[0].candidates.is_empty(), "no data signal, no candidate");
                assert!(!groups[0].fused, "one family of names");
            }
            GroupPreview::Unavailable(why) => panic!("{why}"),
        }
        let why = |table: &str, overrides: &HashMap<String, UserOverride>| match group_preview(&schemas, overrides, &ticked(&[]), &[], table) {
            GroupPreview::Unavailable(why) => why,
            GroupPreview::Groups(g) => panic!("{g:?}"),
        };
        assert!(why("small", &HashMap::new()).contains("20 data columns"));
        assert!(why("a", &HashMap::from([("a".to_string(), UserOverride::Jsonb)])).contains("Jsonb"));
        assert!(why("a", &HashMap::from([("a".to_string(), UserOverride::Skip)])).contains("skipped"));
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
