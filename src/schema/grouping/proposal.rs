//! Assembling a thematic split proposal for one wide table (#78, T1e): the rule of two signals.
//!
//! The lexical groups (names) are the candidates. A group made of ONE prefix family (columns sharing their first
//! stem word) is confirmed by the names alone. A group that fuses SEVERAL families is a bet of the names signal
//! only: it is confirmed when a data signal (presence, shared values, rank correlation, precise shape) backs the
//! links between its families, and otherwise left "to confirm". An unconfirmed fusion does not drop its parts:
//! each family of at least three columns stays an active group, and the fusion is offered as an alternative that
//! replaces them, so applying a proposal unreviewed only does what the names can justify. Columns in no active
//! group stay in the table. Keyed (`NAME_FR`) and numbered (`strIngredient1..20`) families are reported but never
//! transformed. All thresholds are starting points taken from the spikes, to recalibrate.
//!
//! Fonctions :
//! - struct `ColumnName` — nom PG et nom d'origine d'une colonne.
//! - enum `Signal` — signaux de données : présence, valeurs partagées, rang, forme.
//! - enum `Evidence` — ce qui soutient un groupe : cohésion des noms, ou paires soutenues par un signal de données.
//! - fn `Evidence::passes` — vrai quand un signal de données soutient assez de paires pour confirmer une fusion.
//! - enum `Status` — groupe confirmé ou à confirmer.
//! - struct `Group` — un groupe proposé : nom, colonnes, statut, preuves, groupes qu'il remplace.
//! - enum `FamilyKind` — famille à clé ou numérotée.
//! - struct `FamilyNote` — une famille à clé ou numérotée, signalée sans être transformée.
//! - enum `DataStatus` — ce que les signaux de données ont regardé, ou pourquoi ils sont éteints.
//! - struct `Placement` — une colonne laissée dans la table et les groupes les plus proches de son nom, avec leur score.
//! - struct `TableProposal` — la proposition pour une table.
//! - struct `DataSignals` — les signaux de données d'une table, prêts à être interrogés par paires.
//! - fn `DataSignals::new` — signaux de données à partir d'un échantillon.
//! - fn `DataSignals::rows` — lignes échantillonnées.
//! - fn `DataSignals::informative` — colonnes informatives pour la présence.
//! - fn `DataSignals::verdict` — un signal soutient-il une paire ? `None` s'il n'est pas défini pour elle.
//! - fn `DataSignals::backing` — paires soutenues et paires où le signal est défini, sur une liste de paires.
//! - fn `DataSignals::evidence` — preuves de données sur une liste de paires, pour les signaux définis.
//! - fn `background_rate` — part des paires informatives dont le φ atteint le seuil de lien, sans rien d'autre en commun.
//! - fn `propose` — proposition de groupes pour une table à partir des noms de ses colonnes et des signaux de données.
//! - fn `add_cohesion` — place la cohésion des noms en tête des preuves de chaque groupe.
//! - fn `ungrouped_columns` — colonnes qu'aucun groupe confirmé ne prend, dans l'ordre des colonnes.
//! - fn `data_status` — état des signaux de données à reporter : allumés avec leur échantillon, ou éteints avec la raison.
//! - fn `avoid_taken_names` — renomme les groupes dont la table existerait déjà (`_cols`, `_cols_2`…), et les fusions qui les remplacent.
//! - fn `families_of` — colonnes d'un groupe réparties par premier mot de radical.
//! - fn `cross_pairs` — paires de colonnes de familles différentes.
//! - fn `all_pairs` — toutes les paires de colonnes d'une liste.
//! - fn `split_or_fuse` — transforme un groupe lexical en groupes actifs, éventuellement avec une fusion à confirmer.
//! - fn `backing` — les liens entre les familles d'un groupe lexical sont-ils soutenus ? avec les preuves examinées.
//! - fn `family_groups` — un groupe actif par famille d'au moins trois colonnes, dans l'ordre des colonnes.
//! - fn `families_word` — mots d'un nom de groupe : ceux de ses familles les plus grosses.
//! - fn `unique_name` — nom de groupe libre : le mot demandé, suffixé de `_cols` (puis d'un numéro) en cas de collision.
//! - fn `nth_name` — n-ième nom essayé pour un groupe : le mot, puis `_cols`, puis `_cols_2`…
//! - fn `root_word` — mot d'origine d'un nom de groupe, sans son suffixe `_cols` ou `_cols_<n>`.
//! - fn `family_notes` — familles à clé et numérotées d'une table.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::lexical::{group_cohesion, lexical_groups, LexicalParams};
use super::names::{normalize, numbered_families, NameStems};
use super::numeric::{rank_correlations, shape, Shape};
use super::presence::PhiMatrix;
use super::sample::TableSample;
use super::shared::{shared_value_pairs, MIN_DISTINCT};
use super::values::Nature;
use crate::error::Result;
use crate::schema::column_groups::MIN_GROUP_COLUMNS;
use crate::schema::naming::group_table_name;

/// Phi from which two columns count as filled together.
const LINK_PHI: f64 = 0.4;
/// Shared-value similarity from which two columns count as sharing a vocabulary.
const VALUES_CUT: f64 = 0.5;
/// A data signal backs a fusion with at least this many pairs between its families...
const MIN_BACKED_PAIRS: usize = 3;
/// ...and at least this share (percent) of the pairs on which the signal is defined.
const MIN_BACKED_PERCENT: usize = 20;
/// Presence must also reach this multiple of the table's own background rate of phi >= [`LINK_PHI`].
const PRESENCE_OVER_BACKGROUND: f64 = 2.0;
/// Words of a group's name.
const NAME_WORDS: usize = 3;

/// A column of the analysed table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnName {
    /// `PostgreSQL` column name (what the TOML uses).
    pub name: String,
    /// JSON key (what the names signal reads).
    pub original: String,
}

/// The data signals that can back a link between two columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Presence,
    SharedValues,
    Rank,
    Shape,
}

/// What supports a group.
#[derive(Debug, Clone, PartialEq)]
pub enum Evidence {
    /// Mean similarity of the members' names.
    Names { cohesion: f64 },
    /// `backed` of the `defined` pairs the signal can judge are backed by it.
    Data {
        signal: Signal,
        backed: usize,
        defined: usize,
    },
}

impl Evidence {
    /// True for a data signal that backs enough pairs to confirm a fusion: at least `MIN_BACKED_PAIRS` of
    /// them and `MIN_BACKED_PERCENT` % of the pairs it can judge; presence must also clearly beat `background`.
    #[allow(clippy::cast_precision_loss)] // pair counts are far below 2^52
    pub fn passes(&self, background: f64) -> bool {
        let Self::Data {
            signal,
            backed,
            defined,
        } = self
        else {
            return false;
        };
        if *backed < MIN_BACKED_PAIRS || backed * 100 < MIN_BACKED_PERCENT * defined {
            return false;
        }
        *signal != Signal::Presence
            || *backed as f64 / *defined as f64 >= PRESENCE_OVER_BACKGROUND * background
    }
}

/// Whether a group can be applied as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Confirmed,
    /// A fusion of families that no data signal backs: offered, not applied.
    ToConfirm,
}

/// A proposed group of columns.
#[derive(Debug, Clone, PartialEq)]
pub struct Group {
    pub name: String,
    /// Indexes into the table's columns.
    pub columns: Vec<usize>,
    pub status: Status,
    pub evidence: Vec<Evidence>,
    /// Prefix families the group brings together: 1 for a plain family, more for a fusion.
    pub families: usize,
    /// For a fusion to confirm: the active groups it would replace.
    pub replaces: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FamilyKind {
    /// `NAME_FR`, `NAME_DE`: one key per country or language.
    Keyed,
    /// `strIngredient1..20`.
    Numbered,
}

/// A family that an unpivot would handle, reported and left alone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FamilyNote {
    pub kind: FamilyKind,
    pub label: String,
    pub columns: Vec<usize>,
}

/// What the data signals looked at, or why they are off.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataStatus {
    Off(String),
    On { rows: usize, informative: usize },
}

/// A column left in the table and the groups whose centre is nearest to its name in a static embedding. A hint:
/// nothing is moved or merged on this signal.
#[derive(Debug, Clone, PartialEq)]
pub struct Placement {
    /// Index of the column in the table.
    pub column: usize,
    /// `(index in the proposal's groups, cosine)`, best first.
    pub nearest: Vec<(usize, f64)>,
}

/// The proposal for one table.
#[derive(Debug, Clone)]
pub struct TableProposal {
    pub table: String,
    pub columns: Vec<ColumnName>,
    /// Name stems of the columns, as the names signal and the embeddings read them.
    pub stems: Vec<Vec<String>>,
    pub groups: Vec<Group>,
    /// Columns in no confirmed group: they stay in the table.
    pub ungrouped: Vec<usize>,
    pub families: Vec<FamilyNote>,
    pub data: DataStatus,
    /// Where the columns left in the table might go; empty without an embedding model.
    pub placements: Vec<Placement>,
}

/// The data signals of one table, ready to be asked about pairs of columns.
pub struct DataSignals {
    rows: usize,
    informative: usize,
    phi: PhiMatrix,
    /// Share of the informative pairs whose phi reaches [`LINK_PHI`] by chance of richness alone.
    background: f64,
    shared: HashMap<(usize, usize), f64>,
    rank: HashSet<(usize, usize)>,
    shapes: Vec<Option<Shape>>,
    /// Column has a vocabulary the shared-values signal can use.
    text: Vec<bool>,
    numeric: Vec<bool>,
}

impl DataSignals {
    /// The signals computed from one sample of the table.
    pub fn new(sample: &TableSample) -> Self {
        let values = &sample.values;
        let phi = sample.presence.phi_matrix();
        let columns = values.columns();
        Self {
            rows: sample.presence.rows(),
            informative: phi.informative().len(),
            background: background_rate(&phi),
            phi,
            shared: shared_value_pairs(values)
                .into_iter()
                .map(|p| ((p.a, p.b), p.score))
                .collect(),
            rank: rank_correlations(values)
                .into_iter()
                .map(|p| (p.a, p.b))
                .collect(),
            shapes: (0..columns).map(|c| shape(values, c)).collect(),
            text: (0..columns)
                .map(|c| {
                    values
                        .vocabulary(c)
                        .is_some_and(|v| v.len() >= MIN_DISTINCT)
                })
                .collect(),
            numeric: (0..columns)
                .map(|c| values.nature(c) == Nature::Numeric)
                .collect(),
        }
    }

    /// Rows of the sample.
    pub const fn rows(&self) -> usize {
        self.rows
    }

    /// Columns informative for the presence signal.
    pub const fn informative(&self) -> usize {
        self.informative
    }

    /// Does `signal` back the link between `a` and `b` (`a < b`)? `None` when it cannot judge this pair.
    fn verdict(&self, signal: Signal, a: usize, b: usize) -> Option<bool> {
        match signal {
            Signal::Presence => self.phi.get(a, b).map(|phi| phi >= LINK_PHI),
            Signal::SharedValues => (self.text[a] && self.text[b]).then(|| {
                self.shared
                    .get(&(a, b))
                    .is_some_and(|&score| score >= VALUES_CUT)
            }),
            Signal::Rank => {
                (self.numeric[a] && self.numeric[b]).then(|| self.rank.contains(&(a, b)))
            }
            Signal::Shape => (self.shapes[a].is_some() && self.shapes[b].is_some())
                .then(|| self.shapes[a] == self.shapes[b]),
        }
    }

    /// `(backed, defined)`: how many of `pairs` the signal backs, out of those it can judge.
    fn backing(&self, signal: Signal, pairs: &[(usize, usize)]) -> (usize, usize) {
        pairs
            .iter()
            .filter_map(|&(a, b)| self.verdict(signal, a, b))
            .fold((0, 0), |(backed, defined), yes| {
                (backed + usize::from(yes), defined + 1)
            })
    }

    /// One piece of evidence per signal that can judge at least one of `pairs`.
    fn evidence(&self, pairs: &[(usize, usize)]) -> Vec<Evidence> {
        [
            Signal::Presence,
            Signal::SharedValues,
            Signal::Rank,
            Signal::Shape,
        ]
        .into_iter()
        .filter_map(|signal| {
            let (backed, defined) = self.backing(signal, pairs);
            (defined > 0).then_some(Evidence::Data {
                signal,
                backed,
                defined,
            })
        })
        .collect()
    }
}

/// Share of the informative pairs of columns whose adjusted phi reaches [`LINK_PHI`].
#[allow(clippy::cast_precision_loss)] // pair counts are far below 2^52
fn background_rate(phi: &PhiMatrix) -> f64 {
    let informative = phi.informative();
    let values: Vec<f64> = informative
        .iter()
        .enumerate()
        .flat_map(|(i, &a)| informative[i + 1..].iter().map(move |&b| (a, b)))
        .filter_map(|(a, b)| phi.get(a, b))
        .collect();
    values.iter().filter(|&&v| v >= LINK_PHI).count() as f64 / values.len().max(1) as f64
}

/// The proposal for one table: lexical groups, the rule of two signals, names, ungrouped columns, reported
/// families. `data` is the table's data signals, or the reason they are off.
///
/// # Errors
/// More columns than the lexical grouping accepts.
pub fn propose(
    table: &str,
    columns: Vec<ColumnName>,
    data: std::result::Result<&DataSignals, &str>,
    params: &LexicalParams,
) -> Result<TableProposal> {
    let originals: Vec<&str> = columns.iter().map(|c| c.original.as_str()).collect();
    let names = normalize(&originals);
    let lexical = lexical_groups(&names.stems, params)?;
    let mut used = HashSet::new();
    let mut groups: Vec<Group> = lexical
        .iter()
        .flat_map(|members| split_or_fuse(members, &names.stems, data.ok(), &mut used))
        .collect();
    add_cohesion(&mut groups, &names.stems);
    let ungrouped = ungrouped_columns(&groups, columns.len());
    let families = family_notes(&names, &originals);
    Ok(TableProposal {
        table: table.to_string(),
        columns,
        stems: names.stems,
        groups,
        ungrouped,
        families,
        data: data_status(data),
        placements: Vec::new(),
    })
}

/// Puts the cohesion of the names first among the evidence of each group.
fn add_cohesion(groups: &mut [Group], stems: &[Vec<String>]) {
    let lists: Vec<Vec<usize>> = groups.iter().map(|g| g.columns.clone()).collect();
    for (group, cohesion) in groups.iter_mut().zip(group_cohesion(stems, &lists)) {
        group.evidence.insert(0, Evidence::Names { cohesion });
    }
}

/// The columns that no confirmed group takes, in column order.
fn ungrouped_columns(groups: &[Group], count: usize) -> Vec<usize> {
    let active: HashSet<usize> = groups
        .iter()
        .filter(|g| g.status == Status::Confirmed)
        .flat_map(|g| g.columns.iter().copied())
        .collect();
    (0..count)
        .filter(|column| !active.contains(column))
        .collect()
}

/// What the report says about the data signals.
fn data_status(data: std::result::Result<&DataSignals, &str>) -> DataStatus {
    match data {
        Ok(signals) => DataStatus::On {
            rows: signals.rows(),
            informative: signals.informative(),
        },
        Err(reason) => DataStatus::Off(reason.to_string()),
    }
}

/// The members of a group by prefix family: the first word of their stem (empty for a column without words).
fn families_of(members: &[usize], stems: &[Vec<String>]) -> BTreeMap<String, Vec<usize>> {
    let mut families: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for &member in members {
        families
            .entry(stems[member].first().cloned().unwrap_or_default())
            .or_default()
            .push(member);
    }
    families
}

/// Pairs `(a, b)`, `a < b`, of columns that belong to different families.
fn cross_pairs(families: &BTreeMap<String, Vec<usize>>) -> Vec<(usize, usize)> {
    let lists: Vec<&Vec<usize>> = families.values().collect();
    let mut pairs = Vec::new();
    for (i, left) in lists.iter().enumerate() {
        for right in &lists[i + 1..] {
            pairs.extend(
                left.iter()
                    .flat_map(|&a| right.iter().map(move |&b| (a.min(b), a.max(b)))),
            );
        }
    }
    pairs.sort_unstable();
    pairs
}

/// Every pair `(a, b)`, `a < b`, of a sorted list of columns.
fn all_pairs(members: &[usize]) -> Vec<(usize, usize)> {
    members
        .iter()
        .enumerate()
        .flat_map(|(i, &a)| members[i + 1..].iter().map(move |&b| (a, b)))
        .collect()
}

/// One lexical group as proposal groups. A single family is a confirmed group. Several families are one confirmed
/// group when a data signal backs the links between them; otherwise each family of enough columns is a confirmed
/// group and the fusion is an alternative to confirm that replaces them.
fn split_or_fuse(
    members: &[usize],
    stems: &[Vec<String>],
    data: Option<&DataSignals>,
    used: &mut HashSet<String>,
) -> Vec<Group> {
    let families = families_of(members, stems);
    let (backed, evidence) = backing(members, &families, data);
    if backed {
        let name = unique_name(used, &families_word(&families));
        return vec![Group {
            name,
            columns: members.to_vec(),
            status: Status::Confirmed,
            evidence,
            families: families.len().max(1),
            replaces: Vec::new(),
        }];
    }
    let mut groups = family_groups(&families, used);
    let replaces = groups.iter().map(|g| g.name.clone()).collect();
    let name = unique_name(used, &families_word(&families));
    groups.push(Group {
        name,
        columns: members.to_vec(),
        status: Status::ToConfirm,
        evidence,
        families: families.len(),
        replaces,
    });
    groups
}

/// Renames the groups whose table (`{table}_{group}`, as the config applies it) would take the name of a table in
/// `taken`, which holds the names of the existing tables and of the groups given so far: the group gets the next free
/// name of [`nth_name`], so that the proposal applies as it is (a nested `nutrition` object is already the table
/// `openfoodfacts_nutrition`, the group becomes `nutrition_cols`). A fusion to confirm follows the new names of the
/// families it replaces.
pub fn avoid_taken_names(proposal: &mut TableProposal, taken: &mut HashSet<String>) {
    let mut renamed: HashMap<String, String> = HashMap::new();
    let siblings: Vec<String> = proposal.groups.iter().map(|g| g.name.clone()).collect();
    for (at, group) in proposal.groups.iter_mut().enumerate() {
        let base = group.name.clone();
        // the next names of its word, past those of the other groups of the table: no `_cols_cols`
        let root = root_word(&base);
        let mut n = 1;
        while taken.contains(&group_table_name(&proposal.table, &group.name)) {
            group.name = nth_name(root, n);
            n += 1;
            if siblings.iter().enumerate().any(|(other, name)| other != at && *name == group.name) {
                group.name.clone_from(&base);
            }
        }
        taken.insert(group_table_name(&proposal.table, &group.name));
        if group.name != base {
            renamed.insert(base, group.name.clone());
        }
    }
    for replaced in proposal
        .groups
        .iter_mut()
        .flat_map(|g| g.replaces.iter_mut())
    {
        if let Some(new_name) = renamed.get(replaced.as_str()) {
            replaced.clone_from(new_name);
        }
    }
}

/// Whether the links between the families of a lexical group are backed, with the evidence that was looked at. A
/// group of one family needs no backing: its names are enough.
fn backing(
    members: &[usize],
    families: &BTreeMap<String, Vec<usize>>,
    data: Option<&DataSignals>,
) -> (bool, Vec<Evidence>) {
    let single = families.len() <= 1;
    let pairs = if single {
        all_pairs(members)
    } else {
        cross_pairs(families)
    };
    let evidence = data.map_or_else(Vec::new, |signals| signals.evidence(&pairs));
    let background = data.map_or(0.0, |signals| signals.background);
    (
        single || evidence.iter().any(|e| e.passes(background)),
        evidence,
    )
}

/// One confirmed group per family of at least [`MIN_GROUP_COLUMNS`] columns (a family cut out of an unconfirmed
/// fusion is an active group from the size a group table is accepted), in column order.
fn family_groups(
    families: &BTreeMap<String, Vec<usize>>,
    used: &mut HashSet<String>,
) -> Vec<Group> {
    let mut groups: Vec<Group> = families
        .iter()
        .filter(|(_, columns)| columns.len() >= MIN_GROUP_COLUMNS)
        .map(|(word, columns)| Group {
            name: unique_name(used, word),
            columns: columns.clone(),
            status: Status::Confirmed,
            evidence: Vec::new(),
            families: 1,
            replaces: Vec::new(),
        })
        .collect();
    groups.sort_by_key(|g| g.columns[0]);
    groups
}

/// What a group is named after: the words of its biggest families (up to [`NAME_WORDS`]), joined.
fn families_word(families: &BTreeMap<String, Vec<usize>>) -> String {
    let mut by_size: Vec<(&String, usize)> = families
        .iter()
        .filter(|(word, _)| !word.is_empty())
        .map(|(w, c)| (w, c.len()))
        .collect();
    by_size.sort_by(|x, y| y.1.cmp(&x.1).then(x.0.cmp(y.0)));
    by_size
        .iter()
        .take(NAME_WORDS)
        .map(|(word, _)| word.as_str())
        .collect::<Vec<_>>()
        .join("_")
}

/// `wanted`, or the next free name of [`nth_name`] if it is taken; `group` for an empty word.
fn unique_name(used: &mut HashSet<String>, wanted: &str) -> String {
    let base = if wanted.is_empty() { "group" } else { wanted };
    let mut n = 0;
    while !used.insert(nth_name(base, n)) {
        n += 1;
    }
    nth_name(base, n)
}

/// The word a group name was made from: the name without its `_cols` or `_cols_<n>` suffix of [`nth_name`].
fn root_word(name: &str) -> &str {
    let unnumbered = name
        .rsplit_once('_')
        .filter(|(head, n)| head.ends_with("_cols") && !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
        .map_or(name, |(head, _)| head);
    unnumbered.strip_suffix("_cols").unwrap_or(name)
}

/// The `n`-th name tried for a group named after `base`: `base`, then `base_cols`, `base_cols_2`, `base_cols_3`… A
/// number says nothing, `_cols` says these are columns of the table and not its nested `base` object (#91).
fn nth_name(base: &str, n: usize) -> String {
    match n {
        0 => base.to_string(),
        1 => format!("{base}_cols"),
        _ => format!("{base}_cols_{n}"),
    }
}

/// The keyed and numbered families of the table, in column order.
fn family_notes(names: &NameStems, originals: &[&str]) -> Vec<FamilyNote> {
    let mut keyed: BTreeMap<&[String], Vec<usize>> = BTreeMap::new();
    for (column, _) in names.keyed.iter().enumerate().filter(|(_, &k)| k) {
        keyed.entry(&names.stems[column]).or_default().push(column);
    }
    let keyed = keyed.into_iter().map(|(stem, columns)| FamilyNote {
        kind: FamilyKind::Keyed,
        label: format!("{}_*", stem.join("_")),
        columns,
    });
    let numbered = numbered_families(originals)
        .into_iter()
        .map(|columns| FamilyNote {
            kind: FamilyKind::Numbered,
            label: originals[columns[0]]
                .chars()
                .filter(|c| !c.is_ascii_digit())
                .collect(),
            columns,
        });
    let mut notes: Vec<FamilyNote> = keyed.chain(numbered).collect();
    notes.sort_by_key(|note| note.columns[0]);
    notes
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods, clippy::cast_precision_loss))]
mod tests {
    use serde_json::{json, Value};

    use super::super::presence::Presence;
    use super::super::values::ValueProfile;
    use super::*;

    fn columns(names: &[&str]) -> Vec<ColumnName> {
        names
            .iter()
            .map(|n| ColumnName {
                name: n.to_lowercase(),
                original: (*n).to_string(),
            })
            .collect()
    }

    fn names_only(names: &[&str]) -> TableProposal {
        propose("t", columns(names), Err("test"), &LexicalParams::default()).unwrap()
    }

    fn group<'a>(p: &'a TableProposal, name: &str) -> &'a Group {
        p.groups.iter().find(|g| g.name == name).unwrap_or_else(|| {
            panic!(
                "no group {name}: {:?}",
                p.groups.iter().map(|g| &g.name).collect::<Vec<_>>()
            )
        })
    }

    /// Two prefix families of four columns that the names merge into one lexical group.
    const FUSED: [&str; 8] = [
        "nutrition_fat",
        "nutrition_sugar",
        "nutrition_salt",
        "nutrition_fiber",
        "nutriscore_fat",
        "nutriscore_sugar",
        "nutriscore_salt",
        "nutriscore_fiber",
    ];

    /// The fused families plus twelve unrelated words: on eight columns each prefix would be 50 % of them, a noise word.
    fn fused_table() -> Vec<String> {
        const UNRELATED: [&str; 12] = [
            "quartz", "bamboo", "violin", "ember", "glacier", "harbor", "lantern", "meadow",
            "oyster", "pebble", "saddle", "thimble",
        ];
        FUSED
            .iter()
            .chain(&UNRELATED)
            .map(|n| (*n).to_string())
            .collect()
    }

    fn names_only_of(names: &[String]) -> TableProposal {
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        names_only(&refs)
    }

    /// Data signals for the fused table: columns `i` and `i + 4` hold the same six values (text), nothing else is shared.
    fn pairwise_vocabularies() -> DataSignals {
        let mut presence = Presence::new(20);
        let mut values = ValueProfile::new(20, 1);
        for row in 0..6 {
            let cells: Vec<Option<Value>> = (0..20_u8)
                .map(|c| (c < 8).then(|| json!(format!("{}{row}", char::from(b'a' + c % 4)))))
                .collect();
            presence.push_row(vec![true; 8]);
            values.observe_row(cells.iter().map(Option::as_ref));
        }
        DataSignals::new(&TableSample { presence, values })
    }

    /// The columns of the given prefix families (`prefix`, number of columns), each with the suffixes `fat`,
    /// `sugar`, `salt` and `fiber` in turn, then twelve unrelated fillers.
    fn families(spec: &[(&str, usize)]) -> Vec<String> {
        const SUFFIXES: [&str; 4] = ["fat", "sugar", "salt", "fiber"];
        const FILLERS: [&str; 12] = [
            "quartz", "bamboo", "violin", "ember", "glacier", "harbor", "lantern", "meadow",
            "oyster", "pebble", "saddle", "thimble",
        ];
        spec.iter()
            .flat_map(|&(prefix, count)| {
                SUFFIXES
                    .iter()
                    .take(count)
                    .map(move |s| format!("{prefix}_{s}"))
            })
            .chain(FILLERS.iter().map(|f| (*f).to_string()))
            .collect()
    }

    #[test]
    fn a_single_family_group_is_confirmed_by_names_alone() {
        let p = names_only(&[
            "kiwi_red",
            "kiwi_blue",
            "kiwi_green",
            "kiwi_gold",
            "zulu_aaa",
            "zulu_bbb",
            "zulu_ccc",
            "zulu_ddd",
            "qqq",
            "www",
        ]);
        assert_eq!(p.groups.len(), 2);
        let kiwi = group(&p, "kiwi");
        assert_eq!(
            (kiwi.columns.clone(), kiwi.status),
            (vec![0, 1, 2, 3], Status::Confirmed)
        );
        assert!(matches!(kiwi.evidence[0], Evidence::Names { cohesion } if cohesion >= 0.2));
        assert_eq!(group(&p, "zulu").columns, [4, 5, 6, 7]);
        assert_eq!(p.ungrouped, [8, 9]);
    }

    #[test]
    fn a_fusion_without_data_is_to_confirm_and_leaves_its_families_active() {
        let p = names_only_of(&fused_table());
        let fusion = p
            .groups
            .iter()
            .find(|g| g.status == Status::ToConfirm)
            .expect("a fusion to confirm");
        assert_eq!(fusion.columns, (0..8).collect::<Vec<_>>());
        assert_eq!(fusion.replaces.len(), 2);
        for family in ["nutrition", "nutriscore"] {
            let g = group(&p, family);
            assert_eq!(g.status, Status::Confirmed);
            assert!(fusion.replaces.contains(&g.name));
        }
        assert_eq!(
            p.ungrouped,
            (8..20).collect::<Vec<_>>(),
            "both families are active groups, only the fillers stay out"
        );
    }

    #[test]
    fn a_family_of_two_columns_is_not_cut_out_of_an_unconfirmed_fusion() {
        let p = names_only_of(&families(&[("nutrition", 4), ("nutriscore", 2)]));
        let fusion = p
            .groups
            .iter()
            .find(|g| g.status == Status::ToConfirm)
            .expect("a fusion to confirm");
        assert_eq!(fusion.columns, (0..6).collect::<Vec<_>>());
        assert_eq!(fusion.replaces, ["nutrition"]);
        assert!(
            p.groups.iter().all(|g| g.name != "nutriscore"),
            "{:?}",
            p.groups
        );
        assert_eq!(
            p.ungrouped,
            (4..18).collect::<Vec<_>>(),
            "the two nutriscore columns are in no active group"
        );
    }

    #[test]
    fn the_families_of_a_fusion_come_in_column_order_and_the_fusion_last() {
        let p = names_only_of(&families(&[("zeta", 4), ("alpha", 4)]));
        let names: Vec<&str> = p.groups.iter().map(|g| g.name.as_str()).collect();
        assert_eq!(names, ["zeta", "alpha", "alpha_zeta"]);
    }

    #[test]
    fn a_fusion_is_named_after_its_three_biggest_families() {
        let p = names_only_of(&families(&[
            ("nutrition", 4),
            ("nutriscore", 4),
            ("nutrients", 3),
            ("nutriment", 3),
        ]));
        let fusion = p
            .groups
            .iter()
            .find(|g| g.status == Status::ToConfirm)
            .expect("a fusion to confirm");
        assert_eq!(fusion.families, 4);
        assert_eq!(fusion.name, "nutriscore_nutrition_nutrients");
    }

    /// The fused table proposed with the pairwise vocabularies as data signals.
    fn fused_with_data() -> TableProposal {
        let data = pairwise_vocabularies();
        let names = fused_table();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        propose("t", columns(&refs), Ok(&data), &LexicalParams::default()).unwrap()
    }

    #[test]
    fn a_data_signal_that_backs_the_links_confirms_the_fusion() {
        let p = fused_with_data();
        assert_eq!(p.groups.len(), 1, "{:?}", p.groups);
        let g = &p.groups[0];
        assert_eq!((g.status, g.columns.len()), (Status::Confirmed, 8));
        assert_eq!(
            g.name, "nutriscore_nutrition",
            "a confirmed fusion is named after its families"
        );
        assert_eq!(
            p.data,
            DataStatus::On {
                rows: 6,
                informative: 0
            }
        );
    }

    #[test]
    fn the_evidence_of_a_group_starts_with_the_names_then_lists_the_data_signals() {
        let p = fused_with_data();
        let evidence = &p.groups[0].evidence;
        assert!(
            matches!(evidence[0], Evidence::Names { .. }),
            "{evidence:?}"
        );
        assert!(
            evidence.iter().any(|e| matches!(
                e,
                Evidence::Data {
                    signal: Signal::SharedValues,
                    backed: 4,
                    defined: 16
                }
            )),
            "{evidence:?}"
        );
    }

    #[test]
    fn a_data_signal_needs_enough_pairs_and_enough_share() {
        let strong = Evidence::Data {
            signal: Signal::SharedValues,
            backed: 4,
            defined: 16,
        };
        assert!(strong.passes(0.0));
        assert!(
            !Evidence::Data {
                signal: Signal::SharedValues,
                backed: 2,
                defined: 4
            }
            .passes(0.0),
            "two pairs are not enough"
        );
        assert!(
            !Evidence::Data {
                signal: Signal::SharedValues,
                backed: 4,
                defined: 40
            }
            .passes(0.0),
            "10 % is not enough"
        );
        assert!(
            !Evidence::Names { cohesion: 1.0 }.passes(0.0),
            "names alone never confirm a fusion"
        );
    }

    #[test]
    fn a_group_whose_table_name_is_taken_gets_the_next_free_name_and_its_fusion_follows() {
        let mut p = names_only_of(&fused_table());
        let names: Vec<String> = p.groups.iter().map(|g| g.name.clone()).collect();
        assert_eq!(names, ["nutrition", "nutriscore", "nutriscore_nutrition"]);
        let mut taken: HashSet<String> = ["t_nutrition", "t_nutrition_cols"]
            .iter()
            .map(|n| (*n).to_string())
            .collect();
        avoid_taken_names(&mut p, &mut taken);
        let names: Vec<&str> = p.groups.iter().map(|g| g.name.as_str()).collect();
        assert_eq!(names, ["nutrition_cols_2", "nutriscore", "nutriscore_nutrition"]);
        assert_eq!(
            p.groups[2].replaces,
            ["nutrition_cols_2", "nutriscore"],
            "the fusion names the renamed family"
        );
        assert!(
            taken.contains("t_nutrition_cols_2") && taken.contains("t_nutriscore"),
            "the names given are taken too"
        );
    }

    #[test]
    fn a_renamed_group_never_stacks_suffixes_on_a_sibling_name() {
        // OFF: two families named after `nutrition` in one table (`nutrition`, `nutrition_cols`) and a nested
        // `nutrition` object: the first must not become `nutrition_cols_cols`
        let mut p = names_only_of(&fused_table());
        p.groups[0].name = "nutrition".to_string();
        p.groups[1].name = "nutrition_cols".to_string();
        p.groups[2].replaces = vec!["nutrition".to_string(), "nutrition_cols".to_string()];
        let mut taken: HashSet<String> = HashSet::from(["t_nutrition".to_string()]);
        avoid_taken_names(&mut p, &mut taken);
        let names: Vec<&str> = p.groups.iter().map(|g| g.name.as_str()).collect();
        assert_eq!(names[..2], ["nutrition_cols_2", "nutrition_cols"]);
        assert_eq!(p.groups[2].replaces, ["nutrition_cols_2", "nutrition_cols"]);
    }

    #[test]
    fn a_long_table_name_still_gets_a_free_cols_name_once_hashed() {
        // the table names of 53 bytes and more end with a hash: the check is on that final name
        let mut p = names_only_of(&fused_table());
        p.table = "p".repeat(50);
        let mut taken: HashSet<String> = HashSet::from([group_table_name(&p.table, "nutrition")]);
        avoid_taken_names(&mut p, &mut taken);
        assert_eq!(p.groups[0].name, "nutrition_cols");
        let name = group_table_name(&p.table, "nutrition_cols");
        assert!(name.len() <= 53 && name != group_table_name(&p.table, "nutrition"), "{name}");
    }

    #[test]
    fn groups_of_two_tables_do_not_end_up_with_the_same_table_name() {
        // table `a` with a group `b_c` and table `a_b` with a group `c` would both make the table `a_b_c`
        let (mut first, mut second) =
            (names_only_of(&fused_table()), names_only_of(&fused_table()));
        (first.table, first.groups[0].name) = ("a".to_string(), "b_c".to_string());
        (second.table, second.groups[0].name) = ("a_b".to_string(), "c".to_string());
        assert_eq!(group_table_name("a", "b_c"), group_table_name("a_b", "c"));
        let mut taken = HashSet::new();
        avoid_taken_names(&mut first, &mut taken);
        avoid_taken_names(&mut second, &mut taken);
        assert_eq!(first.groups[0].name, "b_c");
        assert_eq!(second.groups[0].name, "c_cols");
        let tables: Vec<String> = [&first, &second]
            .iter()
            .flat_map(|p| p.groups.iter().map(|g| group_table_name(&p.table, &g.name)))
            .collect();
        assert_eq!(
            tables.len(),
            tables.iter().collect::<HashSet<_>>().len(),
            "{tables:?}"
        );
    }

    #[test]
    fn the_thresholds_are_inclusive_three_pairs_and_a_fifth_of_the_defined_ones() {
        let at = |backed, defined| Evidence::Data {
            signal: Signal::SharedValues,
            backed,
            defined,
        };
        assert!(at(3, 15).passes(0.0), "3 pairs, exactly 20 %");
        assert!(!at(3, 16).passes(0.0), "3 pairs, 18.75 %");
        assert!(!at(2, 2).passes(0.0), "all backed but only 2 pairs");
    }

    #[test]
    fn two_precise_shapes_back_a_pair_only_when_they_are_the_same_shape() {
        let mut presence = Presence::new(3);
        let mut values = ValueProfile::new(3, 1);
        for i in 0..30_i64 {
            let cells = [
                json!(1990 + i % 20),
                json!(2000 + (i * 7) % 10),
                json!(1_600_000_000 + i * 86_400),
            ];
            presence.push_row(vec![true; 3]);
            values.observe_row(cells.iter().map(Some));
        }
        let signals = DataSignals::new(&TableSample { presence, values });
        let shape = signals
            .evidence(&[(0, 1), (0, 2)])
            .into_iter()
            .find_map(|e| match e {
                Evidence::Data {
                    signal: Signal::Shape,
                    backed,
                    defined,
                } => Some((backed, defined)),
                _ => None,
            });
        assert_eq!(shape, Some((1, 2)), "year-year backs, year-epoch does not");
    }

    /// Six columns present half of the time over blocks of twenty rows: `a` (rows 0-9), `b` (rows 1-10: phi 0.8
    /// with `a`), `c` (rows 4-13: phi 0.2 with `a`), and the complement of each. Every row is equally rich, so the
    /// adjusted phi is the plain phi.
    fn half_present_columns() -> DataSignals {
        let present = |column: usize, row: usize| {
            let in_block = match column / 2 {
                0 => row < 10,
                1 => (1..=10).contains(&row),
                _ => (4..=13).contains(&row),
            };
            in_block == column.is_multiple_of(2)
        };
        let mut presence = Presence::new(6);
        let mut values = ValueProfile::new(6, 1);
        for row in (0..100).map(|r| r % 20) {
            presence.push_row((0..6).map(|c| present(c, row)));
            values.observe_row(std::iter::repeat_n(None, 6));
        }
        DataSignals::new(&TableSample { presence, values })
    }

    #[test]
    fn presence_backs_a_pair_from_a_phi_of_zero_point_four() {
        let signals = half_present_columns();
        assert_eq!(
            signals.verdict(Signal::Presence, 0, 2),
            Some(true),
            "phi 0.8"
        );
        assert_eq!(
            signals.verdict(Signal::Presence, 0, 4),
            Some(false),
            "phi 0.2"
        );
        assert_eq!(
            signals.verdict(Signal::Presence, 0, 1),
            Some(false),
            "complements: phi -1"
        );
    }

    /// Four text columns over 30 rows: 0 and 3 hold the same ten words, 1 has three of them and ten of its own, 2
    /// holds unrelated words.
    fn four_vocabularies() -> DataSignals {
        let mut presence = Presence::new(4);
        let mut values = ValueProfile::new(4, 1);
        for row in 0..30 {
            let own = if row < 3 {
                format!("v{row}")
            } else {
                format!("w{}", row % 10)
            };
            let cells = [
                json!(format!("v{}", row % 10)),
                json!(own),
                json!(format!("z{}", row % 10)),
                json!(format!("v{}", row % 10)),
            ];
            presence.push_row(vec![true; 4]);
            values.observe_row(cells.iter().map(Some));
        }
        DataSignals::new(&TableSample { presence, values })
    }

    #[test]
    fn shared_values_back_a_pair_only_from_a_weighted_containment_of_one_half() {
        let signals = four_vocabularies();
        assert_eq!(
            signals.verdict(Signal::SharedValues, 0, 3),
            Some(true),
            "same vocabulary"
        );
        assert_eq!(
            signals.verdict(Signal::SharedValues, 0, 1),
            Some(false),
            "3 common words, 0.15"
        );
        assert_eq!(
            signals.verdict(Signal::SharedValues, 0, 2),
            Some(false),
            "nothing in common"
        );
    }

    #[test]
    fn rank_backs_a_pair_only_when_the_columns_move_together() {
        let mut presence = Presence::new(3);
        let mut values = ValueProfile::new(3, 1);
        for i in 0..60_i64 {
            let cells = [json!(i), json!(3 * i + 1), json!(i * 23 % 61)];
            presence.push_row(vec![true; 3]);
            values.observe_row(cells.iter().map(Some));
        }
        let signals = DataSignals::new(&TableSample { presence, values });
        assert_eq!(signals.verdict(Signal::Rank, 0, 1), Some(true));
        assert_eq!(signals.verdict(Signal::Rank, 0, 2), Some(false));
    }

    #[test]
    fn presence_must_clearly_beat_its_own_background() {
        let half = Evidence::Data {
            signal: Signal::Presence,
            backed: 5,
            defined: 10,
        };
        assert!(half.passes(0.2), "50 % against a 20 % background");
        assert!(!half.passes(0.3), "50 % is not twice a 30 % background");
    }

    #[test]
    fn group_names_are_unique() {
        let mut used = HashSet::new();
        assert_eq!(unique_name(&mut used, "name"), "name");
        assert_eq!(unique_name(&mut used, "name"), "name_cols");
        assert_eq!(unique_name(&mut used, "name"), "name_cols_2");
        assert_eq!(unique_name(&mut used, ""), "group");
    }

    #[test]
    fn keyed_and_numbered_families_are_reported_not_transformed() {
        let mut names: Vec<String> = (1..=8).map(|i| format!("strIngredient{i}")).collect();
        names.extend(
            ["fr", "de", "es", "it", "pt", "ru", "zh", "ar"]
                .iter()
                .map(|k| format!("NAME_{k}")),
        );
        names.extend((0..12).map(|i| format!("filler{}", char::from(b'a' + i))));
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let p = names_only(&refs);
        let kinds: Vec<FamilyKind> = p.families.iter().map(|f| f.kind).collect();
        assert!(
            kinds.contains(&FamilyKind::Numbered) && kinds.contains(&FamilyKind::Keyed),
            "{:?}",
            p.families
        );
        let numbered = p
            .families
            .iter()
            .find(|f| f.kind == FamilyKind::Numbered)
            .unwrap();
        assert_eq!(numbered.columns, (0..8).collect::<Vec<_>>());
    }

    #[test]
    fn the_proposal_is_deterministic_and_off_data_says_why() {
        let a = names_only_of(&fused_table());
        let b = names_only_of(&fused_table());
        assert_eq!(a.groups, b.groups);
        assert_eq!(a.data, DataStatus::Off("test".to_string()));
    }

    #[test]
    fn too_many_columns_are_an_error_not_a_silent_empty_proposal() {
        let many: Vec<ColumnName> = (0..2001)
            .map(|i| ColumnName {
                name: format!("c{i}"),
                original: format!("c{i}"),
            })
            .collect();
        assert!(propose("t", many, Err("test"), &LexicalParams::default()).is_err());
    }
}
