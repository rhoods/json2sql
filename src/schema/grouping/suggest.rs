//! The `--suggest-groups` run on a finalized schema (#78, T1e): propose a split for every table that can take one.
//!
//! A table is eligible when its effective strategy is `Columns` or `AutoSplit` and it has at least
//! [`MIN_TABLE_COLUMNS`] data columns (a number of its own, not the wide-table threshold). All the eligible tables
//! are analysed in one run, from ONE reread of the input: the sampler reads every table from the same sampled
//! rows. The data signals need a table reachable through objects only from its root rows; under an array, or with
//! an unknown root row count, a table is proposed from its names alone and the report says why. Columns are read
//! like Pass 2 does (`push_data_col`): through `flatten_sources` when the column was inlined from a child, else by
//! its original name.
//!
//! Fonctions :
//! - struct `Suggestion` — les tables analysées, le TOML et le rapport.
//! - fn `suggest` — propose un découpage pour chaque table éligible du schéma finalisé : échantillon, propositions, sorties.
//! - fn `proposal_for` — proposition d'une table, à partir de ses colonnes et de ses signaux de données.
//! - struct `ReadPlan` — où lire une table : chemin vers son objet, s'il est atteignable, et chemins de ses colonnes.
//! - fn `ReadPlan::target` — la cible de l'échantillonneur pour cette table, si son objet est atteignable.
//! - fn `read_plans` — où lire chacune des tables.
//! - fn `eligible` — vrai pour une table de stratégie `Columns` ou `AutoSplit` avec assez de colonnes de données.
//! - fn `key_path` — chemin de clés d'une colonne dans l'objet de sa table, comme `push_data_col`.
//! - fn `object_path` — clés à suivre depuis une ligne racine jusqu'à la table, si aucun tableau n'est traversé.
//! - fn `samplable_root` — la table racine, si les signaux de données peuvent être calculés.
//! - fn `data_signals` — signaux de données de chaque table en un seul passage, ou la raison pour laquelle ils sont éteints.
//! - fn `signals_of` — signaux d'un échantillon, éteints s'il est vide.
//! - fn `left_out` — note sur les tableaux de scalaires restés des tables de jonction, donc hors de l'analyse.

use std::path::Path;

use super::embedding::StaticEmbedder;
use super::lexical::LexicalParams;
use super::placement::placements;
use super::proposal::TableProposal;
use super::proposal::{propose, ColumnName, DataSignals};
use super::render::{to_markdown, to_toml};
use super::sample::{sample_tables, SampleSource, SampleTarget, TableSample, DEFAULT_BUDGET};
use crate::error::{J2sError, Result};
use crate::schema::table_schema::{ChildKind, ColumnSchema, InferredStrategy, TableSchema};

/// A table needs at least this many data columns to be worth a proposal.
pub const MIN_TABLE_COLUMNS: usize = 20;

/// The data signals of a table, or why they are off.
type Signals = std::result::Result<DataSignals, String>;

/// Where to read one table in the input.
struct ReadPlan {
    /// Keys from a root row to the table's object; `None` when an array lies on the way.
    path: Option<Vec<String>>,
    /// One key path per data column.
    columns: Vec<Vec<String>>,
}

impl ReadPlan {
    /// What the sampler reads for this table; `None` when an array lies on the way.
    fn target(&self) -> Option<SampleTarget<'_>> {
        Some(SampleTarget {
            path: self.path.as_ref()?,
            columns: &self.columns,
        })
    }
}

/// The files of a suggestion.
#[derive(Debug)]
pub struct Suggestion {
    /// The tables that were analysed, in schema order.
    pub tables: Vec<String>,
    /// The `column_groups` TOML, one section per table that got a group.
    pub toml: String,
    /// The Markdown report.
    pub report: String,
}

/// A proposal for every eligible table of the finalized `schemas`, from the rows of `input`; `limit` is
/// `--limit`: only the first rows of the root table take part in the sample. With an `embedder`, the columns left
/// in a table also get the two groups nearest to their name.
///
/// # Errors
/// No table is eligible, or a table has more columns than the lexical grouping accepts. Problems with the data
/// signals are not errors: they turn them off, with a reason.
pub fn suggest(
    schemas: &[TableSchema],
    input: &Path,
    limit: Option<u64>,
    embedder: Option<&StaticEmbedder>,
) -> Result<Suggestion> {
    let tables: Vec<&TableSchema> = schemas.iter().filter(|s| eligible(s)).collect();
    if tables.is_empty() {
        return Err(J2sError::InvalidInput(format!(
            "no table to split: it needs a Columns or AutoSplit strategy and at least {MIN_TABLE_COLUMNS} data columns"
        )));
    }
    let signals = data_signals(schemas, &tables, input, limit);
    let proposals = tables
        .iter()
        .zip(&signals)
        .map(|(table, data)| proposal_for(table, data, embedder))
        .collect::<Result<Vec<_>>>()?;
    Ok(Suggestion {
        tables: tables.iter().map(|t| t.name.clone()).collect(),
        toml: to_toml(&proposals),
        report: format!("{}{}", to_markdown(&proposals), left_out(schemas, &tables)),
    })
}

/// The proposal for one table: its columns, its data signals or why they are off, and where its other columns
/// might go when there is an `embedder`.
fn proposal_for(
    table: &TableSchema,
    data: &Signals,
    embedder: Option<&StaticEmbedder>,
) -> Result<TableProposal> {
    let columns = table
        .data_columns()
        .map(|c| ColumnName {
            name: c.name.clone(),
            original: c.original_name.clone(),
        })
        .collect();
    let mut proposal = propose(
        &table.name,
        columns,
        data.as_ref().map_err(String::as_str),
        &LexicalParams::default(),
    )?;
    if let Some(embedder) = embedder {
        proposal.placements = placements(&proposal, embedder);
    }
    Ok(proposal)
}

/// A `Columns` or `AutoSplit` table with enough data columns.
fn eligible(schema: &TableSchema) -> bool {
    matches!(
        &*schema.effective_strategy(),
        InferredStrategy::Columns | InferredStrategy::AutoSplit { .. }
    ) && schema.data_columns().count() >= MIN_TABLE_COLUMNS
}

/// Keys leading to a column's value inside its table's object, as `push_data_col` reads it: through the field the
/// column was flattened from, if any, then its original name.
fn key_path(schema: &TableSchema, column: &ColumnSchema) -> Vec<String> {
    schema
        .flatten_sources
        .get(column.name.as_str())
        .map_or_else(
            || vec![column.original_name.clone()],
            |source| vec![source.clone(), column.original_name.clone()],
        )
}

/// Keys from a root row to the table's object, when every table on the way is an object child.
fn object_path(schemas: &[TableSchema], table: &TableSchema) -> Option<Vec<String>> {
    let mut current = table;
    for _ in 0..=schemas.len() {
        if current.is_root() {
            return Some(table.path[1..].to_vec());
        }
        if current.child_kind != Some(ChildKind::Object) {
            return None;
        }
        current = schemas
            .iter()
            .find(|s| Some(&s.name) == current.parent_table.as_ref())?;
    }
    None
}

/// The root table, when the data signals can be computed at all: its row count is known and `--limit` is not 0.
fn samplable_root(
    schemas: &[TableSchema],
    limit: Option<u64>,
) -> std::result::Result<&TableSchema, String> {
    let root = schemas
        .iter()
        .find(|s| s.is_root())
        .ok_or("the root table is not in the schema")?;
    if root.row_count == 0 {
        return Err("unknown root row count (schema snapshot without row counts)".to_string());
    }
    if limit == Some(0) {
        return Err("--limit 0 leaves no row to sample".to_string());
    }
    Ok(root)
}

/// The data signals of each table, in the order of `tables`, from ONE sample of `input`; or why they are off.
fn data_signals(
    schemas: &[TableSchema],
    tables: &[&TableSchema],
    input: &Path,
    limit: Option<u64>,
) -> Vec<Signals> {
    let root = match samplable_root(schemas, limit) {
        Ok(root) => root,
        Err(reason) => return tables.iter().map(|_| Err(reason.clone())).collect(),
    };
    let plans = read_plans(schemas, tables);
    let targets: Vec<SampleTarget<'_>> = plans.iter().filter_map(ReadPlan::target).collect();
    let source = SampleSource {
        input,
        root: &root.path[0],
        root_rows: root.row_count,
        limit,
        budget: DEFAULT_BUDGET,
    };
    // The samples come in the order of the targets, which are the readable tables in order.
    let mut samples = sample_tables(&source, &targets)
        .map(Vec::into_iter)
        .map_err(|e| e.to_string());
    plans
        .iter()
        .map(|plan| match (&plan.path, &mut samples) {
            (None, _) => {
                Err("the table lies under an array: its rows cannot be sampled".to_string())
            }
            (Some(_), Err(reason)) => Err(reason.clone()),
            (Some(_), Ok(next)) => signals_of(next.next()),
        })
        .collect()
}

/// Where to read each of `tables`.
fn read_plans(schemas: &[TableSchema], tables: &[&TableSchema]) -> Vec<ReadPlan> {
    tables
        .iter()
        .map(|table| ReadPlan {
            path: object_path(schemas, table),
            columns: table.data_columns().map(|c| key_path(table, c)).collect(),
        })
        .collect()
}

/// A note on the scalar arrays that stayed junction tables, so outside the analysed tables; empty when none did.
fn left_out(schemas: &[TableSchema], tables: &[&TableSchema]) -> String {
    let lines: Vec<String> = tables
        .iter()
        .filter_map(|table| {
            let arrays = schemas
                .iter()
                .filter(|s| s.is_junction() && s.parent_table.as_ref() == Some(&table.name))
                .count();
            (arrays > 0).then(|| format!("- `{}`: scalar arrays: {arrays}", table.name))
        })
        .collect();
    if lines.is_empty() {
        return String::new();
    }
    format!(
        "\n## Not analysed\n\nThese scalar arrays are junction tables, not columns, so they take no part in the proposal. \
         Import with `--array-as-pg-array` and propose again to include them:\n\n{}\n",
        lines.join("\n")
    )
}

/// The signals of a sample, off when no sampled row held the table's object.
fn signals_of(sample: Option<TableSample>) -> Signals {
    match sample {
        Some(sample) if sample.presence.rows() > 0 => Ok(DataSignals::new(&sample)),
        _ => Err("no sampled row holds the table's object".to_string()),
    }
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use std::io::Write;

    use serde_json::{json, Value};

    use super::*;
    use crate::schema::type_tracker::PgType;

    fn run(schemas: &[TableSchema], input: &Path, limit: Option<u64>) -> Result<Suggestion> {
        suggest(schemas, input, limit, None)
    }

    const KIWI: [&str; 6] = ["kiwi_a", "kiwi_b", "kiwi_c", "kiwi_d", "kiwi_e", "kiwi_f"];
    const ZULU: [&str; 6] = ["zulu_a", "zulu_b", "zulu_c", "zulu_d", "zulu_e", "zulu_f"];
    const OTHER: [&str; 12] = [
        "quartz", "bamboo", "violin", "ember", "glacier", "harbor", "lantern", "meadow", "oyster",
        "pebble", "saddle", "thimble",
    ];

    fn wide_names() -> Vec<String> {
        KIWI.iter()
            .chain(&ZULU)
            .chain(&OTHER)
            .map(|n| (*n).to_string())
            .collect()
    }

    fn table(
        name: &str,
        path: &[&str],
        child: Option<ChildKind>,
        columns: &[String],
        rows: u64,
    ) -> TableSchema {
        let mut t = TableSchema::new(
            name.to_string(),
            path.iter().map(|p| (*p).to_string()).collect(),
            path.len() - 1,
        );
        t.columns
            .push(ColumnSchema::generated("j2s_id", PgType::BigInt));
        t.columns.extend(columns.iter().map(|c| ColumnSchema {
            name: c.to_lowercase(),
            original_name: c.clone(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        }));
        t.child_kind = child;
        t.parent_table = (path.len() > 1).then(|| path[0].to_string());
        t.row_count = rows;
        t
    }

    /// `items` (root, few columns) with an object child `items_props` holding a wide table.
    fn schemas(root_rows: u64, child: ChildKind) -> Vec<TableSchema> {
        vec![
            table("items", &["items"], None, &["id".to_string()], root_rows),
            table(
                "items_props",
                &["items", "props"],
                Some(child),
                &wide_names(),
                root_rows,
            ),
        ]
    }

    /// A second wide table of `items`, `items_<key>`, under the given kind of child.
    fn wide_child(key: &str, kind: ChildKind) -> TableSchema {
        table(
            &format!("items_{key}"),
            &["items", key],
            Some(kind),
            &wide_names(),
            60,
        )
    }

    /// `rows` lines, each with the wide object under `props`; the first `more_rows` also have it under `more`.
    fn jsonl_with(rows: usize, more_rows: usize) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        for i in 0..rows {
            let wide: serde_json::Map<String, Value> = wide_names()
                .into_iter()
                .map(|c| (c, json!(format!("v{}", i % 5))))
                .collect();
            let more = (i < more_rows).then_some(&wide);
            writeln!(f, "{}", json!({"id": i, "props": wide, "more": more})).unwrap();
        }
        f
    }

    /// `rows` lines, each with the wide object under `props` and under `more`.
    fn jsonl(rows: usize) -> tempfile::NamedTempFile {
        jsonl_with(rows, rows)
    }

    fn parsed(toml: &str) -> toml::Table {
        toml::from_str(toml).unwrap_or_else(|e| panic!("{e}\n{toml}"))
    }

    #[test]
    fn every_eligible_table_is_proposed_from_one_read_of_the_file() {
        let mut schemas = schemas(60, ChildKind::Object);
        schemas.push(wide_child("more", ChildKind::Object));
        let f = jsonl(60);
        let s = run(&schemas, f.path(), None).unwrap();
        assert_eq!(s.tables, ["items_props", "items_more"]);
        let toml = parsed(&s.toml);
        for table in &s.tables {
            let groups = toml[table.as_str()]["column_groups"].as_table().unwrap();
            assert!(
                groups.contains_key("kiwi") && groups.contains_key("zulu"),
                "{table}: {:?}",
                groups.keys().collect::<Vec<_>>()
            );
        }
        assert_eq!(
            s.report
                .matches("Data signals: on, 60 rows sampled")
                .count(),
            2,
            "{}",
            s.report
        );
    }

    #[test]
    fn each_table_gets_the_signals_of_its_own_rows() {
        let mut schemas = schemas(60, ChildKind::Object);
        schemas.push(wide_child("more", ChildKind::Object));
        let f = jsonl_with(60, 30);
        let report = run(&schemas, f.path(), None).unwrap().report;
        let (props, more) = (
            report.find("Table `items_props`").unwrap(),
            report.find("Table `items_more`").unwrap(),
        );
        assert!(props < more, "{report}");
        assert!(
            report[props..more].contains("on, 60 rows sampled"),
            "{report}"
        );
        assert!(report[more..].contains("on, 30 rows sampled"), "{report}");
    }

    #[test]
    fn a_table_under_an_array_is_proposed_from_names_while_the_others_keep_their_data_signals() {
        let mut schemas = schemas(60, ChildKind::Object);
        schemas.insert(1, wide_child("more", ChildKind::ObjectArray));
        let f = jsonl(60);
        let s = run(&schemas, f.path(), None).unwrap();
        assert_eq!(s.tables, ["items_more", "items_props"]);
        assert_eq!(
            s.report
                .matches("Data signals: on, 60 rows sampled")
                .count(),
            1,
            "the readable table, though it comes after the array one: {}",
            s.report
        );
        assert_eq!(
            s.report.matches("Data signals: off").count(),
            1,
            "{}",
            s.report
        );
        assert!(s.report.contains("array"), "{}", s.report);
        assert!(
            parsed(&s.toml)["items_more"]["column_groups"]
                .as_table()
                .unwrap()
                .contains_key("kiwi"),
            "names alone still propose"
        );
    }

    /// An empty child table `<parent>_<key>` of `parent`, under the given kind.
    fn child_of(parent: &str, key: &str, kind: ChildKind) -> TableSchema {
        let name = format!("{parent}_{key}");
        let mut t = table(&name, &["items", key], Some(kind), &[], 60);
        t.parent_table = Some(parent.to_string());
        t
    }

    #[test]
    fn scalar_arrays_kept_as_junction_tables_are_reported_as_left_out() {
        let mut schemas = schemas(60, ChildKind::Object);
        let f = jsonl(60);
        let none = run(&schemas, f.path(), None).unwrap().report;
        assert!(
            !none.contains("array-as-pg-array"),
            "nothing to say: {none}"
        );
        schemas.push(child_of("items_props", "tags", ChildKind::ScalarArray));
        schemas.push(child_of("items_props", "labels", ChildKind::ScalarArray));
        schemas.push(child_of("items", "ids", ChildKind::ScalarArray)); // another table's
        schemas.push(child_of("items_props", "sub", ChildKind::Object)); // not an array
        let report = run(&schemas, f.path(), None).unwrap().report;
        assert!(
            report.contains("`items_props`: scalar arrays: 2")
                && report.contains("--array-as-pg-array"),
            "{report}"
        );
    }

    #[test]
    fn with_a_model_the_columns_left_in_the_table_get_their_nearest_groups() {
        let embedder = StaticEmbedder::from_rows(&[
            ("kiwi", &[1.0, 0.0]),
            ("zulu", &[0.0, 1.0]),
            ("quartz", &[1.0, 1.0]),
        ]);
        let f = jsonl(60);
        let schemas = schemas(60, ChildKind::Object);
        let plain = suggest(&schemas, f.path(), None, None).unwrap().report;
        assert!(!plain.contains("Where the other columns"), "{plain}");
        let report = suggest(&schemas, f.path(), None, Some(&embedder))
            .unwrap()
            .report;
        assert!(
            report.contains("- `quartz` → `kiwi` (0.71), `zulu` (0.71)"),
            "{report}"
        );
    }

    #[test]
    fn a_table_needs_exactly_twenty_data_columns() {
        let names = |n: usize| (0..n).map(|i| format!("c{i}")).collect::<Vec<_>>();
        assert!(eligible(&table("t", &["t"], None, &names(20), 1)));
        assert!(!eligible(&table("t", &["t"], None, &names(19), 1)));
    }

    #[test]
    fn no_eligible_table_is_an_error_that_says_what_is_needed() {
        let f = jsonl(5);
        let only_narrow = vec![table("items", &["items"], None, &["id".to_string()], 5)];
        let err = run(&only_narrow, f.path(), None).unwrap_err().to_string();
        assert!(err.contains("at least 20"), "{err}");
        let mut jsonb = schemas(5, ChildKind::Object);
        jsonb[1].inferred_strategy = InferredStrategy::Jsonb;
        assert!(
            run(&jsonb, f.path(), None).is_err(),
            "a Jsonb table has no columns to split"
        );
    }

    #[test]
    fn an_unknown_root_row_count_turns_every_table_data_signals_off_and_says_why() {
        let mut schemas = schemas(0, ChildKind::Object);
        schemas.push(wide_child("more", ChildKind::Object));
        let f = jsonl(60);
        let s = run(&schemas, f.path(), None).unwrap();
        assert_eq!(
            s.report.matches("snapshot without row counts").count(),
            2,
            "{}",
            s.report
        );
    }

    #[test]
    fn an_unreadable_input_turns_the_data_signals_off_with_the_reason_and_still_proposes() {
        let missing = Path::new("/nonexistent/dir/input.json");
        let s = run(&schemas(60, ChildKind::Object), missing, None).unwrap();
        assert!(s.report.contains("Data signals: off ("), "{}", s.report);
        assert!(
            !s.report.contains("Data signals: off ()"),
            "a reason is given: {}",
            s.report
        );
        assert!(s.toml.contains("kiwi"), "names alone still propose");
    }

    #[test]
    fn a_zero_limit_turns_the_data_signals_off_and_says_so() {
        let f = jsonl(60);
        let s = run(&schemas(60, ChildKind::Object), f.path(), Some(0)).unwrap();
        assert!(
            s.report.contains("Data signals: off") && s.report.contains("--limit 0"),
            "{}",
            s.report
        );
    }

    #[test]
    fn a_limit_bounds_the_rows_of_every_table() {
        let f = jsonl(60);
        let s = run(&schemas(60, ChildKind::Object), f.path(), Some(25)).unwrap();
        assert!(
            s.report.contains("Data signals: on, 25 rows sampled"),
            "{}",
            s.report
        );
    }

    #[test]
    fn a_table_whose_object_no_row_holds_has_its_data_signals_off() {
        let mut schemas = schemas(60, ChildKind::Object);
        schemas[1].path = vec!["items".to_string(), "absent".to_string()];
        let f = jsonl(60);
        let s = run(&schemas, f.path(), None).unwrap();
        assert!(
            s.report.contains("Data signals: off") && s.report.contains("no sampled row"),
            "{}",
            s.report
        );
    }

    #[test]
    fn a_flattened_column_is_read_from_its_source_field_like_pass_2() {
        let mut t = table(
            "items",
            &["items"],
            None,
            &["nutrients_fat".to_string(), "name".to_string()],
            1,
        );
        t.flatten_sources
            .insert("nutrients_fat".to_string(), "nutrients".to_string());
        t.columns[1].original_name = "fat".to_string(); // the child's own key, as apply_flatten keeps it
        assert_eq!(key_path(&t, &t.columns[1]), ["nutrients", "fat"]);
        assert_eq!(key_path(&t, &t.columns[2]), ["name"]);
    }
}
