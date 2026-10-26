//! Column groups found by the names alone, applied as a strategy (#91, phase 1): `--group-columns` and the per-table
//! switch of the UI.
//!
//! The tables are those `--suggest-groups` would analyse (`Columns` or `AutoSplit`, at least
//! [`super::suggest::MIN_TABLE_COLUMNS`] data columns), minus the tables that are already split and the group tables
//! themselves: a table is proposed once, from all its columns, so applying twice changes nothing, and Pass 2 only
//! fills the groups of a table it writes from its own object. Only the groups the names confirm are applied, never a
//! fusion to confirm. They go through [`apply_column_groups`], like a `column_groups` config, so they get the same
//! checks, names and order. The tables are taken by name and their columns sorted by name: the result does not depend
//! on the order of the schema.
//!
//! With the Pass 1 sample of a table (phase 2, kept in the snapshot), its data signals confirm the fusions of
//! families too, as in `--suggest-groups`; a table without a sample, or whose columns changed since Pass 1 (a TOML
//! `flatten`, an older snapshot), is grouped by its names alone.
//!
//! Fonctions :
//! - fn `name_proposals` — propositions par les noms des tables à découper, dans l'ordre de leurs noms.
//! - fn `apply_name_groups` — applique les groupes confirmés par les noms aux tables à découper ; avertissements de l'application.
//! - fn `confirmed_config` — la config `column_groups` des groupes confirmés d'une liste de propositions.
//! - fn `signals_of` — signaux de données d'une table depuis son échantillon du Pass 1, ou la raison de leur absence.

use std::collections::{HashMap, HashSet};

use super::proposal::{DataSignals, Status, TableProposal};
use super::sample::StoredSample;
use super::suggest::{data_columns_by_name, eligible, proposal_for};
use crate::error::Result;
use crate::schema::column_groups::apply_column_groups;
use crate::schema::config::{ConfigWarning, SchemaConfig};
use crate::schema::table_schema::TableSchema;

/// The proposals of the tables to split that `wanted` keeps, in the order of their names: an eligible table, not
/// split yet, that is not a group table. Each uses the data signals of its Pass 1 sample in `samples` when it has
/// one that fits, its names alone otherwise. The groups are named so that their tables do not exist yet.
///
/// # Errors
/// A table with more columns than the lexical grouping accepts.
pub fn name_proposals(
    schemas: &[TableSchema],
    samples: &[StoredSample],
    wanted: impl Fn(&TableSchema) -> bool,
) -> Result<Vec<TableProposal>> {
    let made: HashSet<&str> = schemas
        .iter()
        .flat_map(|s| s.column_groups.iter().map(String::as_str))
        .collect();
    let mut tables: Vec<&TableSchema> = schemas
        .iter()
        .filter(|s| {
            eligible(s) && s.column_groups.is_empty() && !made.contains(s.name.as_str()) && wanted(s)
        })
        .collect();
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    let mut taken: HashSet<String> = schemas.iter().map(|s| s.name.clone()).collect();
    tables
        .into_iter()
        .map(|table| proposal_for(table, &signals_of(table, samples), None, &mut taken))
        .collect()
}

/// The data signals of `table` from its Pass 1 sample, or why there are none: no sample, a sample whose columns are
/// not the table's (they changed since Pass 1), or no sampled row with the table's object.
fn signals_of(table: &TableSchema, samples: &[StoredSample]) -> std::result::Result<DataSignals, String> {
    let Some(stored) = samples.iter().find(|s| s.table == table.name) else {
        return Err("no Pass 1 sample of this table (an older snapshot?)".to_string());
    };
    if !stored.columns.iter().map(String::as_str).eq(data_columns_by_name(table).iter().map(|c| c.name.as_str())) {
        return Err("the columns of the table changed since Pass 1".to_string());
    }
    if stored.sample.presence.rows() == 0 {
        return Err("no sampled row holds the table's object".to_string());
    }
    Ok(DataSignals::new(&stored.sample))
}

/// Applies the confirmed groups of the tables of [`name_proposals`]; the warnings are those of
/// [`apply_column_groups`] (a table whose groups would take every data column is not split).
///
/// # Errors
/// A table with more columns than the lexical grouping accepts.
pub fn apply_name_groups(
    schemas: &mut Vec<TableSchema>,
    samples: &[StoredSample],
    wanted: impl Fn(&TableSchema) -> bool,
) -> Result<Vec<ConfigWarning>> {
    let proposals = name_proposals(schemas, samples, wanted)?;
    apply_column_groups(schemas, &confirmed_config(&proposals))
}

/// The `[table.column_groups]` config of the confirmed groups of `proposals`, in `PostgreSQL` column names.
fn confirmed_config(proposals: &[TableProposal]) -> SchemaConfig {
    let tables = proposals
        .iter()
        .map(|p| {
            let groups: toml::Table = p
                .groups
                .iter()
                .filter(|g| g.status == Status::Confirmed)
                .map(|g| {
                    let columns = g.columns.iter().map(|&c| toml::Value::String(p.columns[c].name.clone()));
                    (g.name.clone(), toml::Value::Array(columns.collect()))
                })
                .collect();
            (p.table.clone(), HashMap::from([("column_groups".to_string(), toml::Value::Table(groups))]))
        })
        .collect();
    SchemaConfig { group: HashMap::new(), tables }
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;
    use crate::schema::grouping::sample::StoredSample;
    use crate::schema::table_schema::{ChildKind, ColumnSchema, InferredStrategy};
    use crate::schema::type_tracker::PgType;

    const OTHER: [&str; 12] = [
        "quartz", "bamboo", "violin", "ember", "glacier", "harbor", "lantern", "meadow", "oyster", "pebble",
        "saddle", "thimble",
    ];

    /// Six `kiwi_*` and six `zulu_*` columns, then twelve unrelated ones.
    fn wide_names() -> Vec<String> {
        ('a'..='f')
            .flat_map(|c| [format!("kiwi_{c}"), format!("zulu_{c}")])
            .chain(OTHER.iter().map(|n| (*n).to_string()))
            .collect()
    }

    /// A root table `name` with `j2s_id` and the given data columns.
    fn table(name: &str, columns: &[String]) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        t.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        t.columns.extend(columns.iter().map(|c| ColumnSchema {
            name: c.clone(),
            original_name: c.clone(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        }));
        t.row_count = 60;
        t
    }

    fn apply(schemas: &mut Vec<TableSchema>) -> Vec<ConfigWarning> {
        apply_name_groups(schemas, &[], |_| true).unwrap()
    }

    fn names(schemas: &[TableSchema]) -> Vec<&str> {
        schemas.iter().map(|s| s.name.as_str()).collect()
    }

    fn data_names(t: &TableSchema) -> Vec<&str> {
        t.data_columns().map(|c| c.name.as_str()).collect()
    }

    #[test]
    fn a_wide_table_gets_the_groups_its_names_confirm() {
        let mut schemas = vec![table("t", &wide_names())];
        assert_eq!(apply(&mut schemas), []);
        assert_eq!(names(&schemas), ["t", "t_kiwi", "t_zulu"]);
        assert_eq!(schemas[0].column_groups, ["t_kiwi", "t_zulu"]);
        assert_eq!(data_names(&schemas[1]), ["kiwi_a", "kiwi_b", "kiwi_c", "kiwi_d", "kiwi_e", "kiwi_f"]);
        assert_eq!(data_names(&schemas[0]).len(), 12, "the unrelated columns stay");
    }

    #[test]
    fn a_fusion_to_confirm_is_never_applied() {
        let mut columns: Vec<String> = ["nutrition", "nutriscore"]
            .iter()
            .flat_map(|p| ["fat", "sugar", "salt", "fiber"].map(|s| format!("{p}_{s}")))
            .collect();
        columns.extend(OTHER.iter().map(|n| (*n).to_string()));
        let mut schemas = vec![table("t", &columns)];
        assert_eq!(apply(&mut schemas), [], "the fusion is not even tried");
        assert_eq!(names(&schemas), ["t", "t_nutriscore", "t_nutrition"], "the families, not their fusion");
    }

    #[test]
    fn applying_twice_changes_nothing() {
        let mut schemas = vec![table("t", &wide_names())];
        apply(&mut schemas);
        let once = serde_json::to_string(&schemas).unwrap();
        assert_eq!(apply(&mut schemas), []);
        assert_eq!(serde_json::to_string(&schemas).unwrap(), once);
    }

    /// `nutrition_*` and `nutriscore_*` (four each) and the twelve unrelated columns.
    fn fused_columns() -> Vec<String> {
        let mut columns: Vec<String> = ["nutrition", "nutriscore"]
            .iter()
            .flat_map(|p| ["fat", "sugar", "salt", "fiber"].map(|s| format!("{p}_{s}")))
            .collect();
        columns.extend(OTHER.iter().map(|n| (*n).to_string()));
        columns
    }

    /// The Pass 1 sample of `t` with the fused columns: `nutrition_x` and `nutriscore_x` hold the same six values,
    /// the unrelated columns are empty.
    fn fused_sample(columns: &[String]) -> StoredSample {
        use crate::schema::grouping::presence::Presence;
        use crate::schema::grouping::sample::TableSample;
        use crate::schema::grouping::values::ValueProfile;
        let mut sorted = columns.to_vec();
        sorted.sort();
        let mut presence = Presence::new(sorted.len());
        let mut values = ValueProfile::new(sorted.len(), 1);
        for row in 0..6 {
            let cells: Vec<Option<serde_json::Value>> = sorted
                .iter()
                .map(|c| c.starts_with("nutri").then(|| serde_json::json!(format!("{}{row}", c.rsplit('_').next().unwrap()))))
                .collect();
            presence.push_row(cells.iter().map(Option::is_some));
            values.observe_row(cells.iter().map(Option::as_ref));
        }
        StoredSample { table: "t".to_string(), columns: sorted, sample: TableSample { presence, values } }
    }

    #[test]
    fn the_pass_1_sample_confirms_a_fusion_the_names_alone_leave_to_confirm() {
        // #91 phase 2: the data signals come from the snapshot
        let mut schemas = vec![table("t", &fused_columns())];
        assert_eq!(apply_name_groups(&mut schemas, &[fused_sample(&fused_columns())], |_| true).unwrap(), []);
        assert_eq!(names(&schemas), ["t", "t_nutriscore_nutrition"]);
    }

    #[test]
    fn a_sample_whose_columns_changed_since_pass_1_is_not_used() {
        // a TOML flatten or a dropped column: the signals would land on other columns
        let mut stale = fused_sample(&fused_columns());
        stale.columns.pop();
        let mut schemas = vec![table("t", &fused_columns())];
        apply_name_groups(&mut schemas, &[stale], |_| true).unwrap();
        assert_eq!(names(&schemas), ["t", "t_nutriscore", "t_nutrition"], "the names alone");
    }

    #[test]
    fn a_table_already_split_is_left_as_it_is() {
        let mut schemas = vec![table("t", &wide_names())];
        let config: SchemaConfig =
            toml::from_str("[t.column_groups]\nmine = [\"quartz\", \"bamboo\", \"violin\"]\n").unwrap();
        apply_column_groups(&mut schemas, &config).unwrap();
        apply(&mut schemas);
        assert_eq!(names(&schemas), ["t", "t_mine"], "its other columns are not grouped on top");
    }

    #[test]
    fn a_group_table_is_never_split_again() {
        // Pass 2 fills the groups of a table it writes from its own object only
        let mut parent = table("t", &OTHER.iter().map(|n| (*n).to_string()).collect::<Vec<_>>());
        parent.column_groups.push("t_big".to_string());
        let mut group = table("t_big", &wide_names());
        group.parent_table = Some("t".to_string());
        group.child_kind = Some(ChildKind::Object);
        let schemas = vec![parent, group];
        assert!(name_proposals(&schemas, &[], |_| true).unwrap().is_empty());
    }

    #[test]
    fn a_pivot_or_jsonb_table_is_not_split() {
        for strategy in [InferredStrategy::Pivot, InferredStrategy::Jsonb] {
            let mut schemas = vec![table("t", &wide_names())];
            schemas[0].inferred_strategy = strategy;
            apply(&mut schemas);
            assert_eq!(schemas.len(), 1);
        }
    }

    #[test]
    fn only_the_tables_wanted_are_split() {
        let mut schemas = vec![table("a", &wide_names()), table("b", &wide_names())];
        apply_name_groups(&mut schemas, &[], |t| t.name == "b").unwrap();
        assert_eq!(names(&schemas), ["a", "b", "b_kiwi", "b_zulu"]);
    }

    #[test]
    fn a_group_named_like_a_nested_object_gets_the_cols_suffix() {
        let mut nested = table("t_kiwi", &["v".to_string()]);
        nested.parent_table = Some("t".to_string());
        nested.child_kind = Some(ChildKind::Object);
        let mut schemas = vec![table("t", &wide_names()), nested];
        assert_eq!(apply(&mut schemas), []);
        assert_eq!(schemas[0].column_groups, ["t_kiwi_cols", "t_zulu"]);
    }

    #[test]
    fn the_order_of_the_columns_does_not_change_the_groups() {
        let mut reversed = wide_names();
        reversed.reverse();
        let (mut a, mut b) = (vec![table("t", &wide_names())], vec![table("t", &reversed)]);
        apply(&mut a);
        apply(&mut b);
        assert_eq!(names(&a), names(&b));
        for (x, y) in a.iter().zip(&b) {
            let (mut cx, mut cy) = (data_names(x), data_names(y));
            cx.sort_unstable();
            cy.sort_unstable();
            assert_eq!(cx, cy, "{}", x.name);
        }
    }
}
