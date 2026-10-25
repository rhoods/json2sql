//! Application of the `column_groups` of a config to a finalized schema (#78, T3a).
//!
//! `[table.column_groups]` lists, for a table, groups of columns that become 1:1 child tables:
//!
//! ```toml
//! [openfoodfacts.column_groups]
//! nutrition = ["nutrition_data", "nutrition_data_per", "nutrition_grades"]
//! ```
//!
//! The groups are applied after the type, strategy and flatten overrides, in the alphabetical order of their names,
//! so that the same config always gives the same schema. A group table is `{table}_{group}` (53 bytes at most, a
//! hash when it is longer), an object child of `table` with a `j2s_id`, the foreign key to `table` and the moved
//! columns in the order `table` had them; `table` keeps the other columns and lists its group tables in
//! `column_groups`. A group that is not valid is ignored with a warning and the rest of the config still applies:
//! unknown or generated column, column in two groups, fewer than [`MIN_GROUP_COLUMNS`] columns, a table of that
//! name already there. A table whose strategy is not `Columns` or `AutoSplit` is not split, and neither is one
//! whose data columns would all leave.
//!
//! Applying the config again to a schema that already has the groups (a saved snapshot) changes nothing; a group
//! table that exists with other columns than the config says is an error.
//!
//! Fonctions :
//! - fn `apply_column_groups` — applique les `column_groups` d'une config ; avertissements pour les groupes invalides.
//! - fn `dissolve_column_groups` — remet les colonnes des groupes d'une table dans la table et retire les tables de groupe.
//! - fn `prune_missing_groups` — retire des tables les noms de groupes dont la table n'existe plus.
//! - fn `reindex_stats` — rattache les statistiques d'une colonne déplacée à la table de son groupe.
//! - struct `Wanted` — un groupe demandé par la config : nom et colonnes.
//! - struct `Plan` — les groupes à appliquer à une table et les avertissements pour ceux qu'on laisse.
//! - enum `Verdict` — ce qu'on fait d'un groupe de la config : déjà appliqué, laissé avec ses avertissements, accepté.
//! - fn `forbid_emptying` — écarte tous les groupes acceptés s'ils prendraient toutes les colonnes de données de la table.
//! - fn `judge` — le verdict pour un groupe, compte tenu des colonnes et des noms déjà pris.
//! - fn `moved_columns` — où est allée chaque colonne déplacée : table de groupe et rang du groupe.
//! - enum `Existing` — ce que le schéma contient déjà sous le nom d'une table de groupe.
//! - fn `apply_to_table` — applique les groupes d'une table du schéma.
//! - fn `read_groups` — lit les groupes d'une table dans la config, avec les avertissements de forme.
//! - fn `plan` — choisit les groupes valides d'une table, dans l'ordre alphabétique.
//! - enum `Fault` — ce qui ne va pas dans une colonne d'un groupe : inconnue, générée ou répétée.
//! - fn `column_fault` — le défaut éventuel d'une colonne d'un groupe.
//! - fn `fault_warning` — l'avertissement qui correspond au défaut d'une colonne.
//! - fn `check_group` — problèmes d'un groupe : colonne inconnue, répétée, générée, groupe trop petit.
//! - fn `existing_state` — état d'un nom de table de groupe dans le schéma : libre, déjà appliqué, pris.
//! - fn `split_off` — crée la table d'un groupe et en retire les colonnes de la table parente.
//! - fn `strategy_name` — nom court de la stratégie effective d'une table.

use std::collections::{HashMap, HashSet};

use crate::error::{J2sError, Result};
use crate::schema::config::{ConfigWarning, SchemaConfig};
use crate::schema::naming::group_table_name;
use crate::schema::stats::ColumnStats;
use crate::schema::table_schema::{ChildKind, ColumnSchema, TableSchema};
use crate::schema::type_tracker::PgType;

/// The smallest group worth its own table: the smallest the proposal ever makes.
pub const MIN_GROUP_COLUMNS: usize = 3;

/// One group the config asks for.
struct Wanted {
    name: String,
    columns: Vec<String>,
}

/// What the schema already holds under the name a group table would get.
enum Existing {
    /// Nothing: the name is free.
    Free,
    /// A group table of the same table with the columns the config lists: the group is already applied.
    Applied,
    /// A group table of the same table with other columns.
    Different,
    /// Another table.
    Taken,
}

/// Applies the `column_groups` of `config`; see the module documentation. A table the schema does not have is left
/// to `apply_overrides`, which already warns about it.
///
/// # Errors
/// A group table the schema already has for that group, with other columns than the config lists.
pub fn apply_column_groups(
    schemas: &mut Vec<TableSchema>,
    config: &SchemaConfig,
) -> Result<Vec<ConfigWarning>> {
    let mut tables: Vec<(&String, &toml::Value)> = config
        .tables
        .iter()
        .filter_map(|(table, keys)| keys.get("column_groups").map(|groups| (table, groups)))
        .collect();
    tables.sort_by(|a, b| a.0.cmp(b.0));
    let mut warnings = Vec::new();
    for (table, groups) in tables {
        warnings.extend(apply_to_table(schemas, table, groups)?);
    }
    Ok(warnings)
}

/// Puts the columns of the group tables of `parent` back into it and removes those tables.
pub fn dissolve_column_groups(schemas: &mut Vec<TableSchema>, parent: &str) {
    let Some(names) = schemas
        .iter_mut()
        .find(|s| s.name == parent)
        .map(|s| std::mem::take(&mut s.column_groups))
    else {
        return;
    };
    for name in names {
        let Some(at) = schemas
            .iter()
            .position(|s| s.name == name && s.parent_table.as_deref() == Some(parent))
        else {
            continue;
        };
        let group = schemas.remove(at);
        if let Some(table) = schemas.iter_mut().find(|s| s.name == parent) {
            table
                .columns
                .extend(group.columns.into_iter().filter(|c| !c.is_generated));
            table.flatten_sources.extend(group.flatten_sources);
        }
    }
}

/// Removes from every table the names of group tables that are no longer in the schema.
pub fn prune_missing_groups(schemas: &mut [TableSchema]) {
    let existing: HashSet<String> = schemas.iter().map(|s| s.name.clone()).collect();
    for table in schemas.iter_mut() {
        table.column_groups.retain(|group| existing.contains(group));
    }
}

/// Moves the statistics of a column that now lives in a group table to that table, right after the other
/// statistics of its former table (the rest of the order is kept, as the report prints a table's columns together).
pub fn reindex_stats(schemas: &[TableSchema], stats: &mut Vec<ColumnStats>) {
    let moved = moved_columns(schemas);
    if moved.is_empty() {
        return;
    }
    let mut first_seen: HashMap<&str, usize> = HashMap::new();
    for (at, stat) in stats.iter().enumerate() {
        first_seen.entry(stat.table_name.as_str()).or_insert(at);
    }
    // A statistic sorts with its former table (where that table first appears), after the table's own columns and
    // the earlier group tables.
    let keys: Vec<(usize, usize)> = stats
        .iter()
        .map(|stat| {
            let rank = moved
                .get(&(stat.table_name.as_str(), stat.column_name.as_str()))
                .map_or(0, |&(_, rank)| rank);
            (first_seen[stat.table_name.as_str()], rank)
        })
        .collect();
    let mut order: Vec<usize> = (0..stats.len()).collect();
    order.sort_by_key(|&at| keys[at]);
    let mut rows: Vec<Option<ColumnStats>> = std::mem::take(stats).into_iter().map(Some).collect();
    for row in rows.iter_mut().flatten() {
        let key = (row.table_name.as_str(), row.column_name.as_str());
        if let Some(&(group, _)) = moved.get(&key) {
            row.table_name = group.to_string();
        }
    }
    stats.extend(order.into_iter().filter_map(|at| rows[at].take()));
}

/// Where each moved column went: (table, column) to (group table, rank of the group among the table's groups, from 1).
fn moved_columns(schemas: &[TableSchema]) -> HashMap<(&str, &str), (&str, usize)> {
    let mut moved = HashMap::new();
    for parent in schemas {
        for (rank, name) in parent.column_groups.iter().enumerate() {
            for group in schemas.iter().filter(|s| &s.name == name) {
                for column in group.data_columns() {
                    moved.insert(
                        (parent.name.as_str(), column.name.as_str()),
                        (group.name.as_str(), rank + 1),
                    );
                }
            }
        }
    }
    moved
}

/// Applies the groups of one table of the schema.
fn apply_to_table(
    schemas: &mut Vec<TableSchema>,
    table: &str,
    value: &toml::Value,
) -> Result<Vec<ConfigWarning>> {
    let Some(parent_at) = schemas.iter().position(|s| s.name == table) else {
        return Ok(Vec::new());
    };
    let (wanted, mut warnings) = read_groups(table, value);
    let parent = &schemas[parent_at];
    if !parent.takes_column_groups() {
        if !wanted.is_empty() {
            warnings.push(ConfigWarning::ColumnGroupParentStrategy {
                table: table.to_string(),
                strategy: strategy_name(parent),
            });
        }
        return Ok(warnings);
    }
    let plan = plan(schemas, parent, &wanted)?;
    warnings.extend(plan.warnings);
    for (name, columns) in plan.accepted {
        split_off(schemas, parent_at, name, &columns);
    }
    Ok(warnings)
}

/// The groups of a table in the config, in the alphabetical order of their names, and a warning for each one (or
/// for the whole value) that is not a list of column names.
fn read_groups(table: &str, value: &toml::Value) -> (Vec<Wanted>, Vec<ConfigWarning>) {
    let Some(groups) = value.as_table() else {
        return (
            Vec::new(),
            vec![ConfigWarning::ColumnGroupsMalformed {
                table: table.to_string(),
            }],
        );
    };
    let (mut wanted, mut warnings) = (Vec::new(), Vec::new());
    for (name, list) in groups {
        let columns: Option<Vec<String>> = list.as_array().and_then(|items| {
            items
                .iter()
                .map(|v| v.as_str().map(str::to_string))
                .collect()
        });
        match columns {
            Some(columns) => wanted.push(Wanted {
                name: name.clone(),
                columns,
            }),
            None => warnings.push(ConfigWarning::ColumnGroupMalformed {
                table: table.to_string(),
                group: name.clone(),
            }),
        }
    }
    (wanted, warnings)
}

/// The groups to apply to a table, as (table name, columns), and the warnings for those left out.
struct Plan {
    accepted: Vec<(String, Vec<String>)>,
    warnings: Vec<ConfigWarning>,
}

/// What to do with one group of the config.
enum Verdict {
    /// It is in the schema already.
    Applied,
    /// It is not valid: the warnings say why.
    Left(Vec<ConfigWarning>),
    /// It is valid and its table gets this name.
    Accept(String),
}

/// The groups to apply to `parent`. A group already in the schema is skipped; the first valid group of a column
/// keeps it; all groups go if they would take every data column.
fn plan(schemas: &[TableSchema], parent: &TableSchema, wanted: &[Wanted]) -> Result<Plan> {
    let mut plan = Plan {
        accepted: Vec::new(),
        warnings: Vec::new(),
    };
    let (mut claimed, mut given): (HashMap<&str, &str>, HashSet<String>) =
        (HashMap::new(), HashSet::new());
    for group in wanted {
        match judge(schemas, parent, group, &claimed, &given)? {
            Verdict::Applied => {}
            Verdict::Left(problems) => plan.warnings.extend(problems),
            Verdict::Accept(name) => {
                claimed.extend(
                    group
                        .columns
                        .iter()
                        .map(|c| (c.as_str(), group.name.as_str())),
                );
                given.insert(name.clone());
                plan.accepted.push((name, group.columns.clone()));
            }
        }
    }
    forbid_emptying(parent, &claimed, &mut plan);
    Ok(plan)
}

/// All the accepted groups go if they would take every data column of the table.
fn forbid_emptying(parent: &TableSchema, claimed: &HashMap<&str, &str>, plan: &mut Plan) {
    let all_taken = parent
        .data_columns()
        .all(|c| claimed.contains_key(c.name.as_str()));
    if !plan.accepted.is_empty() && all_taken {
        plan.warnings.push(ConfigWarning::ColumnGroupEmptiesParent {
            table: parent.name.clone(),
        });
        plan.accepted.clear();
    }
}

/// What to do with `group`, given the columns the accepted groups claimed and the table names they were given.
fn judge(
    schemas: &[TableSchema],
    parent: &TableSchema,
    group: &Wanted,
    claimed: &HashMap<&str, &str>,
    given: &HashSet<String>,
) -> Result<Verdict> {
    let name = group_table_name(&parent.name, &group.name);
    let state = existing_state(schemas, parent, &name, &group.columns);
    match state {
        Existing::Applied => return Ok(Verdict::Applied),
        Existing::Different => {
            return Err(J2sError::Schema(format!(
                "column group '{}' of '{}': the table '{name}' already exists with other columns than the config lists",
                group.name, parent.name
            )));
        }
        Existing::Free | Existing::Taken => {}
    }
    if matches!(state, Existing::Taken) || given.contains(&name) {
        let (table, group) = (parent.name.clone(), group.name.clone());
        return Ok(Verdict::Left(vec![ConfigWarning::ColumnGroupNameTaken {
            table,
            group,
            name,
        }]));
    }
    let problems = check_group(parent, group, claimed);
    Ok(if problems.is_empty() {
        Verdict::Accept(name)
    } else {
        Verdict::Left(problems)
    })
}

/// What is wrong with one column of a group.
enum Fault {
    Unknown,
    Generated,
    /// Listed twice, or already in the group of that name.
    Repeated(String),
}

/// What is wrong with the `column` of `group`, if anything. `seen` holds the columns of the group already read and
/// `claimed` those of the groups already accepted.
fn column_fault(
    parent: &TableSchema,
    group: &Wanted,
    column: &str,
    seen: &mut HashSet<String>,
    claimed: &HashMap<&str, &str>,
) -> Option<Fault> {
    if !seen.insert(column.to_string()) {
        return Some(Fault::Repeated(group.name.clone()));
    }
    if let Some(other) = claimed.get(column) {
        return Some(Fault::Repeated((*other).to_string()));
    }
    match parent.columns.iter().find(|c| c.name == column) {
        None => Some(Fault::Unknown),
        Some(c) if c.is_generated => Some(Fault::Generated),
        Some(_) => None,
    }
}

/// The warning for a faulty column of a group.
fn fault_warning(
    parent: &TableSchema,
    group: &Wanted,
    column: &str,
    fault: Fault,
) -> ConfigWarning {
    let (table, group, column) = (parent.name.clone(), group.name.clone(), column.to_string());
    match fault {
        Fault::Unknown => ConfigWarning::ColumnGroupUnknownColumn {
            table,
            group,
            column,
        },
        Fault::Generated => ConfigWarning::ColumnGroupGeneratedColumn {
            table,
            group,
            column,
        },
        Fault::Repeated(other) => ConfigWarning::ColumnGroupColumnRepeated {
            table,
            group,
            column,
            other,
        },
    }
}

/// What is wrong with a group: a column that is unknown, generated, listed twice or already in an earlier group, or
/// too few columns. Nothing when it is valid.
fn check_group(
    parent: &TableSchema,
    group: &Wanted,
    claimed: &HashMap<&str, &str>,
) -> Vec<ConfigWarning> {
    let mut seen = HashSet::new();
    let mut problems: Vec<ConfigWarning> = group
        .columns
        .iter()
        .filter_map(|column| {
            column_fault(parent, group, column, &mut seen, claimed)
                .map(|fault| fault_warning(parent, group, column, fault))
        })
        .collect();
    if problems.is_empty() && seen.len() < MIN_GROUP_COLUMNS {
        problems.push(ConfigWarning::ColumnGroupTooSmall {
            table: parent.name.clone(),
            group: group.name.clone(),
            found: seen.len(),
            minimum: MIN_GROUP_COLUMNS,
        });
    }
    problems
}

/// What the schema holds under `name`: nothing, the group table the config describes, a group table of `parent`
/// with other columns, or some other table (by name, or by the one-segment path a group table would get).
fn existing_state(
    schemas: &[TableSchema],
    parent: &TableSchema,
    name: &str,
    columns: &[String],
) -> Existing {
    let Some(found) = schemas
        .iter()
        .find(|s| s.name == name || (s.path.len() == 1 && s.path[0] == name))
    else {
        return Existing::Free;
    };
    let ours = found.name == name
        && found.parent_table.as_deref() == Some(parent.name.as_str())
        && parent.column_groups.iter().any(|g| g == name);
    if !ours {
        return Existing::Taken;
    }
    let have: HashSet<&str> = found.data_columns().map(|c| c.name.as_str()).collect();
    let want: HashSet<&str> = columns.iter().map(String::as_str).collect();
    if have == want {
        Existing::Applied
    } else {
        Existing::Different
    }
}

/// Creates the table of a group from `columns` of the table at `parent_at` and inserts it after the tables of the
/// groups already split off. The columns keep the order they had; their flatten sources go with them.
fn split_off(schemas: &mut Vec<TableSchema>, parent_at: usize, name: String, columns: &[String]) {
    let parent = &mut schemas[parent_at];
    let mut group = TableSchema::new(name.clone(), vec![name.clone()], parent.depth + 1);
    group.parent_table = Some(parent.name.clone());
    group.child_kind = Some(ChildKind::Object);
    group.row_count = parent.row_count;
    group
        .columns
        .push(ColumnSchema::generated("j2s_id", PgType::Uuid));
    group.columns.push(ColumnSchema::parent_fk(&parent.name));
    let (moved, kept): (Vec<ColumnSchema>, Vec<ColumnSchema>) = std::mem::take(&mut parent.columns)
        .into_iter()
        .partition(|c| !c.is_generated && columns.contains(&c.name));
    parent.columns = kept;
    for column in &moved {
        if let Some(source) = parent.flatten_sources.remove(&column.name) {
            group.flatten_sources.insert(column.name.clone(), source);
        }
    }
    group.columns.extend(moved);
    let at = (parent_at + 1 + parent.column_groups.len()).min(schemas.len());
    schemas[parent_at].column_groups.push(name);
    schemas.insert(at, group);
}

/// The name of the strategy of a table without its details: `Pivot`, `SiblingCollapse`...
fn strategy_name(table: &TableSchema) -> String {
    let debug = format!("{:?}", *table.effective_strategy());
    debug
        .split(['(', ' ', '{'])
        .next()
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::schema::table_schema::{ChildKind, ColumnSchema, InferredStrategy};
    use crate::schema::type_tracker::PgType;

    fn column(name: &str) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            original_name: name.to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        }
    }

    /// A root table `name`: `j2s_id`, then the given data columns.
    fn table(name: &str, columns: &[&str]) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        t.columns
            .push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        t.columns.extend(columns.iter().map(|c| column(c)));
        t.row_count = 100;
        t
    }

    /// `items`: three columns for a group and four that stay.
    fn items() -> TableSchema {
        table("items", &["x1", "x2", "x3", "y1", "y2", "y3", "y4"])
    }

    fn config(text: &str) -> SchemaConfig {
        toml::from_str(text).unwrap()
    }

    fn names(t: &TableSchema) -> Vec<&str> {
        t.columns.iter().map(|c| c.name.as_str()).collect()
    }

    fn table_names(schemas: &[TableSchema]) -> Vec<&str> {
        schemas.iter().map(|s| s.name.as_str()).collect()
    }

    fn apply(schemas: &mut Vec<TableSchema>, text: &str) -> Vec<ConfigWarning> {
        apply_column_groups(schemas, &config(text)).unwrap()
    }

    fn json(schemas: &[TableSchema]) -> String {
        serde_json::to_string(schemas).unwrap()
    }

    const ALPHA: &str = "[items.column_groups]\nalpha = [\"x1\", \"x2\", \"x3\"]\n";

    #[test]
    fn a_group_becomes_a_one_to_one_child_table_that_takes_its_columns() {
        let mut schemas = vec![items()];
        assert_eq!(apply(&mut schemas, ALPHA), []);
        assert_eq!(table_names(&schemas), ["items", "items_alpha"]);
        let (parent, group) = (&schemas[0], &schemas[1]);
        assert_eq!(names(parent), ["j2s_id", "y1", "y2", "y3", "y4"]);
        assert_eq!(parent.column_groups, ["items_alpha"]);
        assert_eq!(group.path, ["items_alpha"]);
        assert_eq!(group.depth, 1);
        assert_eq!(group.parent_table.as_deref(), Some("items"));
        assert_eq!(group.child_kind, Some(ChildKind::Object));
        assert_eq!(group.row_count, 100);
        assert!(matches!(
            &*group.effective_strategy(),
            InferredStrategy::Columns
        ));
        assert_eq!(names(group), ["j2s_id", "j2s_items_id", "x1", "x2", "x3"]);
        assert!(group.columns[0].is_generated && group.columns[1].is_parent_fk);
    }

    #[test]
    fn the_moved_columns_keep_their_type_and_their_flatten_source() {
        let mut schemas = vec![items()];
        schemas[0].columns[1].pg_type = PgType::Integer;
        schemas[0].columns[1].not_null = true;
        schemas[0]
            .flatten_sources
            .insert("x2".to_string(), "nutrients".to_string());
        schemas[0]
            .flatten_sources
            .insert("y1".to_string(), "other".to_string());
        apply(&mut schemas, ALPHA);
        let (parent, group) = (&schemas[0], &schemas[1]);
        assert_eq!(
            (group.columns[2].pg_type.clone(), group.columns[2].not_null),
            (PgType::Integer, true)
        );
        assert_eq!(
            group.flatten_sources,
            HashMap::from([("x2".to_string(), "nutrients".to_string())])
        );
        assert_eq!(
            parent.flatten_sources,
            HashMap::from([("y1".to_string(), "other".to_string())])
        );
    }

    #[test]
    fn the_columns_come_in_the_order_of_the_table_whatever_the_order_of_the_list() {
        let mut schemas = vec![items()];
        apply(
            &mut schemas,
            "[items.column_groups]\nalpha = [\"x3\", \"x1\", \"x2\"]\n",
        );
        assert_eq!(
            names(&schemas[1]),
            ["j2s_id", "j2s_items_id", "x1", "x2", "x3"]
        );
    }

    #[test]
    fn groups_are_applied_in_alphabetical_order_right_after_their_table() {
        let mut schemas = vec![table("a_root", &["r"]), items(), table("zebra", &["z"])];
        let text = "[items.column_groups]\nbeta = [\"y1\", \"y2\", \"y3\"]\nalpha = [\"x1\", \"x2\", \"x3\"]\n";
        assert_eq!(apply(&mut schemas, text), []);
        assert_eq!(
            table_names(&schemas),
            ["a_root", "items", "items_alpha", "items_beta", "zebra"]
        );
        assert_eq!(schemas[1].column_groups, ["items_alpha", "items_beta"]);
        assert_eq!(names(&schemas[1]), ["j2s_id", "y4"]);
    }

    #[test]
    fn a_group_with_an_unknown_column_is_ignored_and_the_others_still_apply() {
        let mut schemas = vec![items()];
        let text = "[items.column_groups]\nalpha = [\"x1\", \"x2\", \"nope\"]\nbeta = [\"y1\", \"y2\", \"y3\"]\n";
        let warnings = apply(&mut schemas, text);
        assert_eq!(
            warnings,
            [ConfigWarning::ColumnGroupUnknownColumn {
                table: "items".to_string(),
                group: "alpha".to_string(),
                column: "nope".to_string()
            }]
        );
        assert_eq!(table_names(&schemas), ["items", "items_beta"]);
        assert!(
            names(&schemas[0]).contains(&"x1"),
            "nothing left the table for the ignored group"
        );
    }

    #[test]
    fn a_generated_column_cannot_be_moved() {
        let mut schemas = vec![items()];
        let warnings = apply(
            &mut schemas,
            "[items.column_groups]\nalpha = [\"x1\", \"x2\", \"j2s_id\"]\n",
        );
        assert_eq!(
            warnings,
            [ConfigWarning::ColumnGroupGeneratedColumn {
                table: "items".to_string(),
                group: "alpha".to_string(),
                column: "j2s_id".to_string()
            }]
        );
        assert_eq!(schemas.len(), 1);
    }

    #[test]
    fn a_group_below_the_minimum_is_ignored() {
        let mut schemas = vec![items()];
        let warnings = apply(
            &mut schemas,
            "[items.column_groups]\nalpha = [\"x1\", \"x2\"]\n",
        );
        assert_eq!(
            warnings,
            [ConfigWarning::ColumnGroupTooSmall {
                table: "items".to_string(),
                group: "alpha".to_string(),
                found: 2,
                minimum: MIN_GROUP_COLUMNS
            }]
        );
        assert_eq!(schemas.len(), 1);
    }

    #[test]
    fn a_column_listed_twice_or_in_two_groups_makes_the_later_group_invalid() {
        let mut schemas = vec![items()];
        let twice = apply(
            &mut schemas,
            "[items.column_groups]\nalpha = [\"x1\", \"x1\", \"x2\"]\n",
        );
        assert_eq!(
            twice,
            [ConfigWarning::ColumnGroupColumnRepeated {
                table: "items".to_string(),
                group: "alpha".to_string(),
                column: "x1".to_string(),
                other: "alpha".to_string()
            }]
        );
        let text = "[items.column_groups]\nalpha = [\"x1\", \"x2\", \"x3\"]\nbeta = [\"x3\", \"y1\", \"y2\"]\n";
        let two = apply(&mut schemas, text);
        assert_eq!(
            two,
            [ConfigWarning::ColumnGroupColumnRepeated {
                table: "items".to_string(),
                group: "beta".to_string(),
                column: "x3".to_string(),
                other: "alpha".to_string()
            }]
        );
        assert_eq!(table_names(&schemas), ["items", "items_alpha"]);
    }

    #[test]
    fn a_name_already_taken_by_a_table_or_by_another_group_ignores_the_group() {
        let mut real_child = table("items_alpha", &["v"]);
        real_child.parent_table = Some("items".to_string());
        let mut schemas = vec![items(), real_child];
        let taken = apply(&mut schemas, ALPHA);
        assert_eq!(
            taken,
            [ConfigWarning::ColumnGroupNameTaken {
                table: "items".to_string(),
                group: "alpha".to_string(),
                name: "items_alpha".to_string()
            }]
        );
        assert_eq!(schemas.len(), 2);
        let mut schemas = vec![items()];
        let text = "[items.column_groups]\nAlpha = [\"x1\", \"x2\", \"x3\"]\nalpha = [\"y1\", \"y2\", \"y3\"]\n";
        let same = apply(&mut schemas, text);
        assert_eq!(
            same,
            [ConfigWarning::ColumnGroupNameTaken {
                table: "items".to_string(),
                group: "alpha".to_string(),
                name: "items_alpha".to_string()
            }]
        );
        assert_eq!(table_names(&schemas), ["items", "items_alpha"]);
    }

    #[test]
    fn only_columns_and_autosplit_tables_can_be_split() {
        for strategy in [InferredStrategy::Pivot, InferredStrategy::Jsonb] {
            let mut schemas = vec![items()];
            let name = format!("{strategy:?}");
            schemas[0].inferred_strategy = strategy;
            let warnings = apply(&mut schemas, ALPHA);
            assert_eq!(
                warnings,
                [ConfigWarning::ColumnGroupParentStrategy {
                    table: "items".to_string(),
                    strategy: name
                }]
            );
            assert_eq!(schemas.len(), 1);
        }
        let mut schemas = vec![items()];
        schemas[0].inferred_strategy = InferredStrategy::AutoSplit {
            stable_threshold: 0.1,
            rare_threshold: 0.001,
            medium_keys: std::collections::HashSet::new(),
            wide_table_name: "items_wide".to_string(),
        };
        assert_eq!(apply(&mut schemas, ALPHA), []);
        assert_eq!(schemas.len(), 2);
    }

    #[test]
    fn groups_that_take_every_data_column_are_all_ignored() {
        let mut schemas = vec![table("items", &["x1", "x2", "x3", "y1", "y2", "y3"])];
        let text = "[items.column_groups]\nalpha = [\"x1\", \"x2\", \"x3\"]\nbeta = [\"y1\", \"y2\", \"y3\"]\n";
        let warnings = apply(&mut schemas, text);
        assert_eq!(
            warnings,
            [ConfigWarning::ColumnGroupEmptiesParent {
                table: "items".to_string()
            }]
        );
        assert_eq!(schemas.len(), 1);
        assert_eq!(schemas[0].column_groups.len(), 0);
    }

    #[test]
    fn a_group_name_that_makes_a_long_table_name_is_cut_with_a_hash() {
        let parent = "p".repeat(40);
        let mut schemas = vec![table(&parent, &["x1", "x2", "x3", "y1"])];
        let text = format!(
            "[{parent}.column_groups]\n{} = [\"x1\", \"x2\", \"x3\"]\n",
            "g".repeat(30)
        );
        assert_eq!(apply(&mut schemas, &text), []);
        let name = &schemas[1].name;
        assert!(name.len() <= 53 && name.starts_with(&parent), "{name}");
        let (_, hash) = name.rsplit_once('_').unwrap();
        assert_eq!(hash.len(), 7);
        let mut again = vec![table(&parent, &["x1", "x2", "x3", "y1"])];
        apply(&mut again, &text);
        assert_eq!(&again[1].name, name, "the same config gives the same name");
    }

    #[test]
    fn applying_the_config_again_changes_nothing() {
        let mut schemas = vec![items()];
        apply(&mut schemas, ALPHA);
        let before = json(&schemas);
        assert_eq!(apply(&mut schemas, ALPHA), []);
        assert_eq!(json(&schemas), before);
    }

    #[test]
    fn a_new_group_is_added_after_the_ones_already_there() {
        let mut schemas = vec![items(), table("zebra", &["z"])];
        apply(&mut schemas, ALPHA);
        let text = "[items.column_groups]\nalpha = [\"x1\", \"x2\", \"x3\"]\nbeta = [\"y1\", \"y2\", \"y3\"]\n";
        assert_eq!(apply(&mut schemas, text), []);
        assert_eq!(
            table_names(&schemas),
            ["items", "items_alpha", "items_beta", "zebra"]
        );
        assert_eq!(schemas[0].column_groups, ["items_alpha", "items_beta"]);
        assert_eq!(names(&schemas[0]), ["j2s_id", "y4"]);
    }

    #[test]
    fn a_group_table_that_exists_with_other_columns_is_an_error() {
        let mut schemas = vec![items()];
        apply(&mut schemas, ALPHA);
        for list in [
            "\"x1\", \"x2\", \"y1\"",
            "\"x1\", \"x2\", \"x3\", \"y1\"",
            "\"x1\", \"x2\", \"x3\", \"y1\", \"y2\"",
        ] {
            let other = config(&format!("[items.column_groups]\nalpha = [{list}]\n"));
            let message = apply_column_groups(&mut schemas, &other)
                .unwrap_err()
                .to_string();
            assert!(message.contains("items_alpha"), "{list}: {message}");
        }
    }

    #[test]
    fn a_malformed_list_is_a_warning_not_a_crash() {
        let mut schemas = vec![items()];
        let whole = apply(&mut schemas, "[items]\ncolumn_groups = \"oops\"\n");
        assert_eq!(
            whole,
            [ConfigWarning::ColumnGroupsMalformed {
                table: "items".to_string()
            }]
        );
        for list in ["\"x1\"", "[1, 2, 3]", "[\"x1\", 2, \"x3\"]"] {
            let text = format!("[items.column_groups]\nalpha = {list}\n");
            assert_eq!(
                apply(&mut schemas, &text),
                [ConfigWarning::ColumnGroupMalformed {
                    table: "items".to_string(),
                    group: "alpha".to_string()
                }],
                "{list}"
            );
        }
        assert_eq!(schemas.len(), 1);
    }

    /// The config of a too small group `alpha` for `table`.
    fn two_column_group_of(table: &str) -> String {
        format!("[{table}.column_groups]\nalpha = [\"x1\", \"x2\"]\n")
    }

    #[test]
    fn the_warnings_of_several_tables_come_in_the_alphabetical_order_of_the_tables() {
        let names = ["t_e", "t_a", "t_d", "t_b", "t_c"];
        let mut schemas: Vec<TableSchema> = names.iter().map(|n| table(n, &["x1", "x2"])).collect();
        let text: String = names.iter().map(|n| two_column_group_of(n)).collect();
        let warnings = apply(&mut schemas, &text);
        let tables: Vec<&str> = warnings
            .iter()
            .map(|w| match w {
                ConfigWarning::ColumnGroupTooSmall { table, .. } => table.as_str(),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(tables, ["t_a", "t_b", "t_c", "t_d", "t_e"]);
    }

    #[test]
    fn a_table_the_schema_does_not_have_is_left_to_apply_overrides() {
        let mut schemas = vec![items()];
        assert_eq!(
            apply(
                &mut schemas,
                "[ghost.column_groups]\nalpha = [\"a\", \"b\", \"c\"]\n"
            ),
            []
        );
        let warnings = crate::schema::config::apply_overrides_complete(
            &mut schemas,
            &config("[ghost.column_groups]\nalpha = [\"a\", \"b\", \"c\"]\n"),
        )
        .unwrap();
        assert_eq!(warnings, [ConfigWarning::UnknownTable("ghost".to_string())]);
    }

    #[test]
    fn dissolving_puts_the_columns_back_with_their_flatten_sources() {
        let mut schemas = vec![items()];
        schemas[0]
            .flatten_sources
            .insert("x2".to_string(), "nutrients".to_string());
        apply(&mut schemas, ALPHA);
        dissolve_column_groups(&mut schemas, "items");
        assert_eq!(table_names(&schemas), ["items"]);
        let mut restored = names(&schemas[0]);
        restored.sort_unstable();
        assert_eq!(
            restored,
            ["j2s_id", "x1", "x2", "x3", "y1", "y2", "y3", "y4"]
        );
        assert_eq!(schemas[0].column_groups.len(), 0);
        assert_eq!(
            schemas[0].flatten_sources.get("x2").map(String::as_str),
            Some("nutrients")
        );
        dissolve_column_groups(&mut schemas, "items");
        dissolve_column_groups(&mut schemas, "ghost");
        assert_eq!(
            schemas.len(),
            1,
            "nothing to do when there is no group or no table"
        );
    }

    #[test]
    fn names_of_group_tables_that_are_gone_are_dropped() {
        let mut schemas = vec![items()];
        apply(&mut schemas, "[items.column_groups]\nalpha = [\"x1\", \"x2\", \"x3\"]\nbeta = [\"y1\", \"y2\", \"y3\"]\n");
        schemas.retain(|s| s.name != "items_beta");
        prune_missing_groups(&mut schemas);
        assert_eq!(schemas[0].column_groups, ["items_alpha"]);
    }

    fn stat(table: &str, column: &str) -> ColumnStats {
        ColumnStats {
            table_name: table.to_string(),
            column_name: column.to_string(),
            pg_type: PgType::Text,
            total_count: 10,
            null_count: 0,
            type_histogram: Vec::new(),
        }
    }

    #[test]
    fn the_statistics_of_a_moved_column_follow_it_in_schema_order() {
        let mut schemas = vec![items(), table("other", &["z"])];
        apply(&mut schemas, ALPHA);
        let mut stats = vec![
            stat("items", "x1"),
            stat("items", "y1"),
            stat("other", "z"),
            stat("items", "x3"),
        ];
        reindex_stats(&schemas, &mut stats);
        let got: Vec<(&str, &str)> = stats
            .iter()
            .map(|s| (s.table_name.as_str(), s.column_name.as_str()))
            .collect();
        assert_eq!(
            got,
            [
                ("items", "y1"),
                ("items_alpha", "x1"),
                ("items_alpha", "x3"),
                ("other", "z")
            ]
        );
        let again = got.len();
        reindex_stats(&schemas, &mut stats);
        assert_eq!(stats.len(), again, "idempotent");
    }

    #[test]
    fn the_statistics_of_several_group_tables_come_in_the_order_of_their_groups() {
        let mut schemas = vec![items()];
        let text = "[items.column_groups]\nbeta = [\"y1\", \"y2\", \"y3\"]\nalpha = [\"x1\", \"x2\", \"x3\"]\n";
        apply(&mut schemas, text);
        let mut stats = vec![
            stat("items", "y1"),
            stat("items", "x1"),
            stat("items", "y4"),
            stat("items", "y2"),
        ];
        reindex_stats(&schemas, &mut stats);
        let got: Vec<(&str, &str)> = stats
            .iter()
            .map(|s| (s.table_name.as_str(), s.column_name.as_str()))
            .collect();
        assert_eq!(
            got,
            [
                ("items", "y4"),
                ("items_alpha", "x1"),
                ("items_beta", "y1"),
                ("items_beta", "y2")
            ]
        );
    }
}
