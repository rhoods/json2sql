//! Output of a proposal (#78, T1e): the `column_groups` TOML and the explanatory report.
//!
//! The TOML has one `[table.column_groups]` section per analysed table, in `PostgreSQL` column names, which is the
//! shape of `--schema-config` (a sub-table of the table's section). Confirmed groups are active lines. A fusion to
//! confirm is a commented block: it replaces the active groups it names, so uncommenting it means deleting those
//! lines. Each group has a comment line with its evidence. The report is Markdown and gives, per table, what the
//! data signals looked at, every group with its status and evidence, the columns left in the table, and the keyed or
//! numbered families that are reported and not transformed.
//!
//! Fonctions :
//! - fn `to_toml` — sections `column_groups` de toutes les tables : groupes actifs, fusions à confirmer en commentaire.
//! - fn `table_toml` — section TOML d'une table.
//! - fn `group_toml` — lignes TOML d'un groupe, avec son commentaire de preuves.
//! - fn `key` — clé TOML : nue si possible, entre guillemets sinon.
//! - fn `quoted` — chaîne TOML entre guillemets.
//! - fn `to_markdown` — rapport Markdown de toutes les tables.
//! - fn `table_markdown` — section du rapport pour une table.
//! - fn `table_summary` — titre, taille de la table et état des signaux de données.
//! - fn `families_markdown` — familles à clé ou numérotées que la proposition laisse telles quelles.
//! - fn `family_bullet` — une famille à clé ou numérotée dans le rapport.
//! - fn `group_markdown` — description d'un groupe dans le rapport.
//! - fn `status_text` — statut d'un groupe en toutes lettres.
//! - fn `evidence_text` — texte court d'une preuve.
//! - fn `column_list` — noms PG de colonnes entre apostrophes inverses, séparés par des virgules.

use super::proposal::{
    DataStatus, Evidence, FamilyKind, FamilyNote, Group, Signal, Status, TableProposal,
};

/// Widest line of the TOML.
const WIDTH: usize = 100;

const TOML_HEADER: &str = "# Suggested by `json2sql --suggest-groups`: review, edit, then apply with --schema-config.\n\
# Active lines are groups the names justify as they are. A commented block is a fusion that no\n\
# data signal confirms: it replaces the active groups it names, so delete their lines to use it.\n\n";

/// The `column_groups` sections of all the tables that have a group. Confirmed groups are active, fusions to
/// confirm are commented.
pub fn to_toml(proposals: &[TableProposal]) -> String {
    let sections: Vec<String> = proposals
        .iter()
        .filter(|p| !p.groups.is_empty())
        .map(table_toml)
        .collect();
    format!("{TOML_HEADER}{}", sections.join("\n"))
}

/// `[table.column_groups]` and its groups.
fn table_toml(proposal: &TableProposal) -> String {
    let mut out = format!("[{}.column_groups]\n", key(&proposal.table));
    for group in &proposal.groups {
        out.push_str(&group_toml(proposal, group));
    }
    out
}

/// A comment line with the status and evidence, then the group as a TOML array wrapped at [`WIDTH`]; the whole
/// group is commented when it is a fusion to confirm.
fn group_toml(proposal: &TableProposal, group: &Group) -> String {
    let comment = if group.status == Status::Confirmed {
        ""
    } else {
        "# "
    };
    let evidence: Vec<String> = group.evidence.iter().map(evidence_text).collect();
    let mut out = format!(
        "# {}: {}; {}\n{comment}{} = [\n",
        group.name,
        status_text(group),
        evidence.join(", "),
        key(&group.name)
    );
    let mut line = format!("{comment} ");
    for &column in &group.columns {
        let item = format!(" {},", quoted(&proposal.columns[column].name));
        if line.len() + item.len() > WIDTH && !line.trim_start_matches("# ").trim().is_empty() {
            out.push_str(&line);
            out.push('\n');
            line = format!("{comment} ");
        }
        line.push_str(&item);
    }
    out.push_str(&line);
    out.extend(["\n", comment, "]\n\n"]);
    out
}

/// A TOML key: bare when it only holds letters, digits, `_` and `-`, quoted otherwise.
fn key(name: &str) -> String {
    if !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        name.to_string()
    } else {
        quoted(name)
    }
}

/// A TOML basic string (JSON escaping is a subset of it).
fn quoted(text: &str) -> String {
    serde_json::to_string(text).unwrap_or_default()
}

/// The report of all the tables, in Markdown.
pub fn to_markdown(proposals: &[TableProposal]) -> String {
    let with_groups = proposals.iter().filter(|p| !p.groups.is_empty()).count();
    let tables: Vec<String> = proposals.iter().map(table_markdown).collect();
    format!(
        "# Column group proposal\n\nTables analysed: {}, with groups: {with_groups}.\n\n{}",
        proposals.len(),
        tables.join("\n")
    )
}

/// What the data signals saw, the groups, the columns left in the table, the families left alone; one line for a
/// table without any group.
fn table_markdown(proposal: &TableProposal) -> String {
    if proposal.groups.is_empty() {
        return format!(
            "## Table `{}`\n\n{} columns; no group found.\n",
            proposal.table,
            proposal.columns.len()
        );
    }
    format!(
        "{}### Groups\n\n{}\n### Columns that stay in the table ({})\n\n{}\n{}",
        table_summary(proposal),
        proposal
            .groups
            .iter()
            .map(|g| group_markdown(proposal, g))
            .collect::<String>(),
        proposal.ungrouped.len(),
        column_list(proposal, &proposal.ungrouped),
        families_markdown(proposal)
    )
}

/// Title, size of the table with what is confirmed, and the state of the data signals.
fn table_summary(proposal: &TableProposal) -> String {
    let (confirmed, moved) = proposal
        .groups
        .iter()
        .filter(|g| g.status == Status::Confirmed)
        .fold((0, 0), |(groups, columns), g| {
            (groups + 1, columns + g.columns.len())
        });
    let data = match &proposal.data {
        DataStatus::On { rows, informative } => {
            format!("on, {rows} rows sampled, {informative} informative columns")
        }
        DataStatus::Off(reason) => format!("off ({reason})"),
    };
    format!(
        "## Table `{}`\n\n{} columns; confirmed groups: {confirmed}, holding {moved} columns; fusions to confirm: {}.\nData signals: {data}.\n\n",
        proposal.table,
        proposal.columns.len(),
        proposal.groups.len() - confirmed
    )
}

/// The keyed and numbered families that are reported and left alone; nothing when there are none.
fn families_markdown(proposal: &TableProposal) -> String {
    if proposal.families.is_empty() {
        return String::new();
    }
    let bullets: String = proposal
        .families
        .iter()
        .map(|family| family_bullet(proposal, family))
        .collect();
    format!("\n### Families reported, not transformed\n\n{bullets}")
}

/// One line of the report for a keyed or numbered family: kind, label, size and columns.
fn family_bullet(proposal: &TableProposal, family: &FamilyNote) -> String {
    let kind = if family.kind == FamilyKind::Keyed {
        "keyed"
    } else {
        "numbered"
    };
    format!(
        "- {kind} `{}` ({} columns): {}\n",
        family.label,
        family.columns.len(),
        column_list(proposal, &family.columns)
    )
}

/// One bullet per group: name, status, size, what it replaces, its columns and its evidence.
fn group_markdown(proposal: &TableProposal, group: &Group) -> String {
    let evidence: Vec<String> = group.evidence.iter().map(evidence_text).collect();
    let families = if group.families > 1 {
        format!(", {} families", group.families)
    } else {
        String::new()
    };
    format!(
        "- **{}** — {}, {} columns{families}: {} — {}\n",
        group.name,
        status_text(group),
        group.columns.len(),
        column_list(proposal, &group.columns),
        evidence.join("; ")
    )
}

/// `confirmed`, or `to confirm` (with the groups the fusion would replace, if any).
fn status_text(group: &Group) -> String {
    match group.status {
        Status::Confirmed => "confirmed".to_string(),
        Status::ToConfirm if group.replaces.is_empty() => "to confirm".to_string(),
        Status::ToConfirm => format!("to confirm, replaces {}", group.replaces.join(", ")),
    }
}

/// `names 0.42`, `presence 3/20 pairs`.
fn evidence_text(evidence: &Evidence) -> String {
    match evidence {
        Evidence::Names { cohesion } => format!("names {cohesion:.2}"),
        Evidence::Data {
            signal,
            backed,
            defined,
        } => {
            let label = match signal {
                Signal::Presence => "presence",
                Signal::SharedValues => "shared values",
                Signal::Rank => "rank",
                Signal::Shape => "shape",
            };
            format!("{label} {backed}/{defined} pairs")
        }
    }
}

/// The `PostgreSQL` names of columns, in backticks, separated by commas.
fn column_list(proposal: &TableProposal, columns: &[usize]) -> String {
    columns
        .iter()
        .map(|&c| format!("`{}`", proposal.columns[c].name))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::super::proposal::ColumnName;
    use super::*;
    use crate::schema::config::SchemaConfig;

    #[allow(clippy::too_many_lines)] // flat fixture of one proposal
    fn proposal(table: &str) -> TableProposal {
        let columns: Vec<ColumnName> = (0..12)
            .map(|i| ColumnName {
                name: format!("col_{i}"),
                original: format!("Col{i}"),
            })
            .collect();
        let group = |name: &str, cols: &[usize], status, evidence, replaces: &[&str]| Group {
            name: name.to_string(),
            columns: cols.to_vec(),
            status,
            evidence,
            families: if replaces.is_empty() {
                1
            } else {
                replaces.len()
            },
            replaces: replaces.iter().map(|r| (*r).to_string()).collect(),
        };
        TableProposal {
            table: table.to_string(),
            columns,
            groups: vec![
                group(
                    "alpha",
                    &[0, 1, 2, 3],
                    Status::Confirmed,
                    vec![Evidence::Names { cohesion: 0.42 }],
                    &[],
                ),
                group(
                    "beta",
                    &[4, 5, 6],
                    Status::Confirmed,
                    vec![Evidence::Names { cohesion: 0.31 }],
                    &[],
                ),
                group(
                    "alpha_beta",
                    &[0, 1, 2, 3, 4, 5, 6],
                    Status::ToConfirm,
                    vec![
                        Evidence::Names { cohesion: 0.2 },
                        Evidence::Data {
                            signal: Signal::Presence,
                            backed: 1,
                            defined: 12,
                        },
                    ],
                    &["alpha", "beta"],
                ),
            ],
            ungrouped: vec![7, 8, 9, 10, 11],
            families: vec![FamilyNote {
                kind: FamilyKind::Numbered,
                label: "item".to_string(),
                columns: vec![8, 9, 10, 11],
            }],
            data: DataStatus::On {
                rows: 500,
                informative: 9,
            },
        }
    }

    fn parse(text: &str) -> toml::Table {
        toml::from_str(text).unwrap_or_else(|e| panic!("invalid TOML: {e}\n{text}"))
    }

    fn array(table: &toml::Table, section: &str, group: &str) -> Vec<String> {
        table[section]["column_groups"][group]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn confirmed_groups_are_active_lines_in_pg_column_names() {
        let parsed = parse(&to_toml(&[proposal("products")]));
        assert_eq!(
            array(&parsed, "products", "alpha"),
            ["col_0", "col_1", "col_2", "col_3"]
        );
        assert_eq!(
            array(&parsed, "products", "beta"),
            ["col_4", "col_5", "col_6"]
        );
        assert!(
            parsed["products"]["column_groups"]
                .get("alpha_beta")
                .is_none(),
            "a fusion to confirm is not active"
        );
    }

    #[test]
    fn a_fusion_to_confirm_is_a_commented_block_that_is_valid_once_uncommented() {
        let text = to_toml(&[proposal("products")]);
        assert!(text.contains("replaces alpha, beta"), "{text}");
        let lines: Vec<&str> = text.lines().collect();
        let start = lines
            .iter()
            .position(|l| *l == "# alpha_beta = [")
            .expect("the commented fusion");
        let end = start + lines[start..].iter().position(|l| *l == "# ]").unwrap();
        let uncommented = format!(
            "[products.column_groups]\n{}",
            lines[start..=end]
                .iter()
                .map(|l| &l[2..])
                .collect::<Vec<_>>()
                .join("\n")
        );
        assert_eq!(
            array(&parse(&uncommented), "products", "alpha_beta").len(),
            7
        );
    }

    #[test]
    fn a_fusion_that_replaces_nothing_does_not_claim_to_replace() {
        let mut p = proposal("products");
        p.groups[2].replaces.clear();
        let toml = to_toml(std::slice::from_ref(&p));
        assert!(toml.contains("# alpha_beta: to confirm; names"), "{toml}");
        assert!(!toml.contains("to confirm, replaces"), "{toml}");
        let md = to_markdown(std::slice::from_ref(&p));
        assert!(
            md.contains("**alpha_beta** — to confirm, 7 columns"),
            "{md}"
        );
        assert!(!md.contains("to confirm, replaces"), "{md}");
    }

    #[test]
    fn the_schema_config_loader_accepts_the_file() {
        let config: SchemaConfig =
            toml::from_str(&to_toml(&[proposal("products"), proposal("other")])).unwrap();
        for table in ["products", "other"] {
            assert!(config.tables[table]["column_groups"].is_table(), "{table}");
        }
    }

    #[test]
    fn long_groups_wrap_and_names_that_need_it_are_quoted() {
        let mut p = proposal("my table");
        p.columns = (0..80)
            .map(|i| ColumnName {
                name: format!("a_rather_long_column_name_{i}"),
                original: format!("c{i}"),
            })
            .collect();
        p.groups[0].columns = (0..80).collect();
        let text = to_toml(&[p]);
        assert!(text.lines().all(|l| l.len() <= 100), "{text}");
        assert_eq!(array(&parse(&text), "my table", "alpha").len(), 80);
    }

    #[test]
    fn a_table_without_any_group_has_no_section() {
        let mut p = proposal("products");
        p.groups.clear();
        assert!(!to_toml(&[p]).contains("column_groups"));
    }

    #[test]
    fn the_report_lists_groups_statuses_evidence_leftovers_and_families() {
        let md = to_markdown(&[proposal("products")]);
        for needle in [
            "products",
            "alpha",
            "beta",
            "alpha_beta",
            "to confirm",
            "confirmed",
            "names 0.42",
            "presence 1/12",
            "col_7",
            "item",
            "500 rows",
            "12 columns; confirmed groups: 2, holding 7 columns; fusions to confirm: 1.",
            "- numbered `item` (4 columns)",
        ] {
            assert!(md.contains(needle), "missing {needle:?}:\n{md}");
        }
    }

    #[test]
    fn the_report_counts_the_tables_and_gives_a_table_without_group_one_line() {
        let mut bare = proposal("bare");
        bare.groups.clear();
        let md = to_markdown(&[proposal("products"), bare]);
        assert!(md.contains("Tables analysed: 2, with groups: 1."), "{md}");
        assert!(
            md.contains("## Table `bare`\n\n12 columns; no group found.\n"),
            "{md}"
        );
        assert!(
            md.matches("### Groups").count() == 1
                && md.matches("### Columns that stay").count() == 1,
            "only the table with groups gets the long form:\n{md}"
        );
    }

    #[test]
    fn the_report_says_why_data_signals_are_off() {
        let mut p = proposal("products");
        p.data = DataStatus::Off("unknown row count".to_string());
        assert!(to_markdown(&[p]).contains("unknown row count"));
    }

    #[test]
    fn the_output_is_deterministic() {
        let p = proposal("products");
        assert_eq!(
            to_toml(std::slice::from_ref(&p)),
            to_toml(std::slice::from_ref(&p))
        );
        assert_eq!(
            to_markdown(std::slice::from_ref(&p)),
            to_markdown(std::slice::from_ref(&p))
        );
    }
}
