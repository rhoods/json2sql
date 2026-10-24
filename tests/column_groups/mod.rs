//! Shared pieces of the column-grouping bench (#78): ground-truth fixtures, scoring, "first word" baseline.
//!
//! Scoring takes groups of column NAMES (the `original_name` the detection will receive), so every
//! method — baseline, lexical, data signals, embeddings — is judged by the same function.
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::path::PathBuf;

use serde::Deserialize;

/// The prefix baseline only keeps groups of at least this many columns.
const PREFIX_MIN_GROUP: usize = 4;
/// A group holding more than this share of the columns is no split at all: it is scored as ungrouped.
/// Without it a single `str*` group (52 of 54 columns in TheMealDB) beats any real split on coverage and recall.
/// The same cap bounds the ground truth: a `[[relation]]` over it could never be found, so the loader rejects it
/// (TO CONFIRM with the user: a blunt share also forbids a legitimately large theme, cf. #78 acceptance criteria).
const MAX_GROUP_SHARE_PERCENT: usize = 50;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// Thresholds may be tuned on it.
    Tuning,
    /// Never used to tune; read only by the final, explicit evaluation.
    HeldOut,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRelation {
    name: String,
    left: Vec<String>,
    /// Absent: every pair inside `left`. Present: every `(left, right)` pair.
    right: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDataset {
    name: String,
    role: Role,
    source: String,
    columns: Vec<String>,
    #[serde(default)]
    relation: Vec<RawRelation>,
    #[serde(default)]
    separate: Vec<RawRelation>,
}

/// Column pairs `(i, j)` with `i < j`, indexes into [`Dataset::columns`], without duplicates.
#[derive(Debug, Clone)]
pub struct Relation {
    pub name: String,
    pub pairs: Vec<(usize, usize)>,
}

#[derive(Debug, Clone)]
pub struct Dataset {
    pub name: String,
    pub role: Role,
    pub source: String,
    pub columns: Vec<String>,
    /// Pairs that must end up in the same group.
    pub linked: Vec<Relation>,
    /// Pairs that must never share a group.
    pub separated: Vec<Relation>,
}

impl Dataset {
    pub fn from_toml(text: &str) -> Result<Self, String> {
        let raw: RawDataset = toml::from_str(text).map_err(|e| e.to_string())?;
        let mut index = HashMap::new();
        for (i, c) in raw.columns.iter().enumerate() {
            if index.insert(c.as_str(), i).is_some() {
                return Err(format!("{}: duplicate column {c:?}", raw.name));
            }
        }
        if raw.relation.is_empty() {
            return Err(format!(
                "{}: no [[relation]], an unlabeled dataset scores nothing",
                raw.name
            ));
        }
        let linked = expand(&raw, &index, &raw.relation)?;
        let separated = expand(&raw, &index, &raw.separate)?;
        check_consistent(&raw, &linked, &separated)?;
        Ok(Self {
            name: raw.name,
            role: raw.role,
            source: raw.source,
            columns: raw.columns,
            linked,
            separated,
        })
    }
}

/// A truth that contradicts itself or the scoring rules would silently mislead every tuning round.
fn check_consistent(
    raw: &RawDataset,
    linked: &[Relation],
    separated: &[Relation],
) -> Result<(), String> {
    for r in linked {
        // all columns of a relation end up in one group: the group must be allowed to exist
        let members: BTreeSet<usize> = r.pairs.iter().flat_map(|&(i, j)| [i, j]).collect();
        if over_cap(members.len(), raw.columns.len()) {
            return Err(format!(
                "{}/{}: {} of {} columns exceed the {MAX_GROUP_SHARE_PERCENT}% cap on group size, label a finer theme",
                raw.name, r.name, members.len(), raw.columns.len()
            ));
        }
    }
    let together: BTreeSet<_> = linked
        .iter()
        .flat_map(|r| r.pairs.iter().copied())
        .collect();
    for r in separated {
        if let Some(&(i, j)) = r.pairs.iter().find(|p| together.contains(p)) {
            let (a, b) = (&raw.columns[i], &raw.columns[j]);
            return Err(format!(
                "{}/{}: ({a:?}, {b:?}) is both linked and separated",
                raw.name, r.name
            ));
        }
    }
    Ok(())
}

/// True when a group of `group_size` holds more than [`MAX_GROUP_SHARE_PERCENT`] of `columns`.
fn over_cap(group_size: usize, columns: usize) -> bool {
    group_size * 100 > MAX_GROUP_SHARE_PERCENT * columns
}

fn expand(
    raw: &RawDataset,
    index: &HashMap<&str, usize>,
    relations: &[RawRelation],
) -> Result<Vec<Relation>, String> {
    relations
        .iter()
        .map(|r| {
            let ctx = format!("{}/{}", raw.name, r.name);
            let left = resolve(&ctx, index, &r.left)?;
            let pairs: BTreeSet<(usize, usize)> = if let Some(right) = &r.right {
                let right = resolve(&ctx, index, right)?;
                if let Some(&x) = left.iter().find(|x| right.contains(x)) {
                    return Err(format!("{ctx}: {:?} is on both sides", raw.columns[x]));
                }
                left.iter()
                    .flat_map(|&a| right.iter().map(move |&b| (a.min(b), a.max(b))))
                    .collect()
            } else {
                if left.len() < 2 {
                    return Err(format!(
                        "{ctx}: a relation without `right` needs at least two columns"
                    ));
                }
                left.iter()
                    .enumerate()
                    .flat_map(|(k, &a)| left[k + 1..].iter().map(move |&b| (a.min(b), a.max(b))))
                    .collect()
            };
            Ok(Relation {
                name: r.name.clone(),
                pairs: pairs.into_iter().collect(),
            })
        })
        .collect()
}

fn resolve(
    ctx: &str,
    index: &HashMap<&str, usize>,
    names: &[String],
) -> Result<Vec<usize>, String> {
    if names.is_empty() {
        return Err(format!("{ctx}: empty column list"));
    }
    let mut out = Vec::with_capacity(names.len());
    for n in names {
        let i = *index
            .get(n.as_str())
            .ok_or_else(|| format!("{ctx}: unknown column {n:?}"))?;
        if out.contains(&i) {
            return Err(format!("{ctx}: column {n:?} listed twice"));
        }
        out.push(i);
    }
    Ok(out)
}

/// Every committed fixture, sorted by file name. Panics on an invalid fixture: the truth must be trusted.
pub fn fixtures() -> Vec<Dataset> {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/column_groups");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "toml"))
        .collect();
    paths.sort();
    paths
        .iter()
        .map(|p| {
            let text =
                std::fs::read_to_string(p).unwrap_or_else(|e| panic!("{}: {e}", p.display()));
            Dataset::from_toml(&text).unwrap_or_else(|e| panic!("{}: {e}", p.display()))
        })
        .collect()
}

/// The datasets thresholds may be tuned on.
pub fn only_tuning(datasets: Vec<Dataset>) -> Vec<Dataset> {
    datasets
        .into_iter()
        .filter(|d| d.role == Role::Tuning)
        .collect()
}

// ---------------------------------------------------------------- "first word" baseline

/// Lower-cased words of a column name: split on non-alphanumerics, camelCase and letter/digit boundaries.
fn words(name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut prev: Option<char> = None;
    for ch in name.chars() {
        let boundary = match prev {
            Some(p) if ch.is_alphanumeric() => {
                (p.is_lowercase() && ch.is_uppercase()) || p.is_alphabetic() != ch.is_alphabetic()
            }
            _ => !ch.is_alphanumeric(),
        };
        if boundary && !cur.is_empty() {
            out.push(std::mem::take(&mut cur));
        }
        if ch.is_alphanumeric() {
            cur.extend(ch.to_lowercase());
        }
        prev = ch.is_alphanumeric().then_some(ch);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// First word of a column name that is not a bare number (the whole lower-cased name if it has none).
pub fn first_word(name: &str) -> String {
    let all = words(name);
    all.iter()
        .find(|w| !w.chars().all(char::is_numeric))
        .or_else(|| all.first())
        .cloned()
        .unwrap_or_else(|| name.to_lowercase())
}

/// The reference to beat: columns sharing their first word, groups of [`PREFIX_MIN_GROUP`]+ only.
pub fn prefix_baseline(columns: &[String]) -> Vec<Vec<String>> {
    let mut by_word: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for c in columns {
        by_word.entry(first_word(c)).or_default().push(c.clone());
    }
    by_word
        .into_values()
        .filter(|g| g.len() >= PREFIX_MIN_GROUP)
        .collect()
}

// ---------------------------------------------------------------- scoring

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationScore {
    pub name: String,
    /// Pairs of the relation whose two columns share a group.
    pub hit: usize,
    pub total: usize,
}

#[derive(Debug, Clone)]
pub struct Score {
    pub columns: usize,
    /// Groups that count (not over [`MAX_GROUP_SHARE_PERCENT`] of the columns).
    pub groups: usize,
    /// Columns in a group that counts.
    pub coverage: usize,
    /// Size of the largest group proposed, capped or not.
    pub largest: usize,
    /// Groups dropped for holding more than [`MAX_GROUP_SHARE_PERCENT`] of the columns.
    pub capped: usize,
    /// `hit` = pairs found together: higher is better.
    pub linked: Vec<RelationScore>,
    /// `hit` = pairs wrongly together: lower is better.
    pub separated: Vec<RelationScore>,
}

/// Scores `groups` (column names, each column in at most one group) against the dataset's truth.
pub fn score(ds: &Dataset, groups: &[Vec<String>]) -> Result<Score, String> {
    let index: HashMap<&str, usize> = ds
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.as_str(), i))
        .collect();
    let mut seen = vec![false; ds.columns.len()];
    let mut group_of: Vec<Option<usize>> = vec![None; ds.columns.len()];
    let (mut kept, mut capped, mut coverage) = (0, 0, 0);
    for g in groups {
        let mut members = Vec::with_capacity(g.len());
        for c in g {
            let i = *index
                .get(c.as_str())
                .ok_or_else(|| format!("{}: unknown column {c:?}", ds.name))?;
            if std::mem::replace(&mut seen[i], true) {
                return Err(format!("{}: column {c:?} is in two groups", ds.name));
            }
            members.push(i);
        }
        if over_cap(members.len(), ds.columns.len()) {
            capped += 1;
            continue;
        }
        coverage += members.len();
        for &i in &members {
            group_of[i] = Some(kept);
        }
        kept += 1;
    }
    let tally = |relations: &[Relation]| -> Vec<RelationScore> {
        relations
            .iter()
            .map(|r| RelationScore {
                name: r.name.clone(),
                hit: r
                    .pairs
                    .iter()
                    .filter(|&&(i, j)| group_of[i].is_some() && group_of[i] == group_of[j])
                    .count(),
                total: r.pairs.len(),
            })
            .collect()
    };
    Ok(Score {
        columns: ds.columns.len(),
        groups: kept,
        coverage,
        largest: groups.iter().map(Vec::len).max().unwrap_or(0),
        capped,
        linked: tally(&ds.linked),
        separated: tally(&ds.separated),
    })
}

impl fmt::Display for Score {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let join = |rs: &[RelationScore]| {
            rs.iter()
                .map(|r| format!("{} {}/{}", r.name, r.hit, r.total))
                .collect::<Vec<_>>()
                .join(" | ")
        };
        writeln!(
            f,
            "{} groups, coverage {}/{}, largest {}, capped {}",
            self.groups, self.coverage, self.columns, self.largest, self.capped
        )?;
        writeln!(f, "   linked    : {}", join(&self.linked))?;
        write!(f, "   separated : {}", join(&self.separated))
    }
}
