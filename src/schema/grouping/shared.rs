//! Shared values between text columns (#78, T1d): "contenance pondérée par la rareté".
//!
//! Two text columns that draw their values from the same vocabulary (country codes in `ADM0_A3` and `GU_A3`,
//! the same names in `NAME` and `NAME_EN`) probably describe the same thing. The similarity of two columns is
//! the weighted share of the smaller vocabulary found in the other one: `sum(w(v), v in A and B) /
//! min(sum w(A), sum w(B))`, with `w(v) = ln(columns / columns holding v)`, so a value present in every column
//! weighs nothing and a rare one weighs a lot. Only text columns count (small integers collide by chance), with
//! at least five distinct values, and a pair needs at least three values in common. The signal is a hint, never
//! a decision on its own.
//!
//! Fonctions :
//! - struct `SharedPair` — deux colonnes texte, leurs valeurs communes et leur similarité pondérée.
//! - fn `shared_value_pairs` — toutes les paires de colonnes texte ayant au moins trois valeurs en commun.
//! - fn `columns_by_value` — pour chaque valeur, les colonnes éligibles qui la contiennent.
//! - fn `rarity` — poids d'une valeur : `ln(colonnes éligibles / colonnes qui la contiennent)`.
//! - fn `column_weights` — somme des poids des valeurs de chaque colonne.
//! - fn `pair_overlaps` — valeurs communes et poids commun de chaque paire de colonnes.

use std::collections::{BTreeMap, BTreeSet};

use super::values::ValueProfile;

/// A text column needs at least this many distinct values.
pub const MIN_DISTINCT: usize = 5;
/// A pair needs at least this many values in common.
pub const MIN_COMMON: usize = 3;
/// Weight sum given to a column whose values all weigh nothing, so that the division stays defined.
const MIN_WEIGHT: f64 = 1e-9;

/// Two text columns with enough values in common.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SharedPair {
    /// Index of the first column (smaller than `b`).
    pub a: usize,
    pub b: usize,
    /// Number of values both columns hold.
    pub common: usize,
    /// Weighted containment, from 0 to 1.
    pub score: f64,
}

/// Every pair of text columns sharing at least [`MIN_COMMON`] values, ordered by `(a, b)`.
pub fn shared_value_pairs(profile: &ValueProfile) -> Vec<SharedPair> {
    let vocabularies: Vec<(usize, &BTreeSet<u64>)> = (0..profile.columns())
        .filter_map(|column| {
            profile
                .vocabulary(column)
                .filter(|v| v.len() >= MIN_DISTINCT)
                .map(|v| (column, v))
        })
        .collect();
    let columns_of = columns_by_value(&vocabularies);
    let eligible = vocabularies.len();
    let weights = column_weights(&columns_of, eligible, profile.columns());
    pair_overlaps(&columns_of, eligible)
        .into_iter()
        .filter(|&(_, (common, _))| common >= MIN_COMMON)
        .map(|((a, b), (common, weight))| SharedPair {
            a,
            b,
            common,
            score: weight / weights[a].min(weights[b]),
        })
        .collect()
}

/// For each value, the eligible columns holding it, in column order.
fn columns_by_value(vocabularies: &[(usize, &BTreeSet<u64>)]) -> BTreeMap<u64, Vec<usize>> {
    let mut columns_of: BTreeMap<u64, Vec<usize>> = BTreeMap::new();
    for &(column, vocabulary) in vocabularies {
        for &value in vocabulary {
            columns_of.entry(value).or_default().push(column);
        }
    }
    columns_of
}

/// Weight of a value held by `holders` of the `eligible` columns: 0 when every column holds it.
#[allow(clippy::cast_precision_loss)] // column counts are far below 2^52
fn rarity(eligible: usize, holders: usize) -> f64 {
    (eligible as f64 / holders as f64).ln()
}

/// Sum of the weights of the values of each column; [`MIN_WEIGHT`] when it would be zero.
fn column_weights(
    columns_of: &BTreeMap<u64, Vec<usize>>,
    eligible: usize,
    columns: usize,
) -> Vec<f64> {
    let mut sums = vec![0.0; columns];
    for holders in columns_of.values() {
        let weight = rarity(eligible, holders.len());
        for &column in holders {
            sums[column] += weight;
        }
    }
    sums.into_iter()
        .map(|sum| if sum > 0.0 { sum } else { MIN_WEIGHT })
        .collect()
}

/// For every pair of columns holding a value together: how many values, and their total weight.
fn pair_overlaps(
    columns_of: &BTreeMap<u64, Vec<usize>>,
    eligible: usize,
) -> BTreeMap<(usize, usize), (usize, f64)> {
    let mut overlaps: BTreeMap<(usize, usize), (usize, f64)> = BTreeMap::new();
    for holders in columns_of.values().filter(|holders| holders.len() >= 2) {
        let weight = rarity(eligible, holders.len());
        for (i, &a) in holders.iter().enumerate() {
            for &b in &holders[i + 1..] {
                let overlap = overlaps.entry((a, b)).or_default();
                overlap.0 += 1;
                overlap.1 += weight;
            }
        }
    }
    overlaps
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use serde_json::{json, Value};

    use super::*;

    const EPS: f64 = 1e-12;

    /// A profile whose columns hold the given strings: row `i` has the `i`-th value of each column.
    fn texts(columns: &[Vec<String>]) -> ValueProfile {
        let rows = columns.iter().map(Vec::len).max().unwrap_or(0);
        let mut p = ValueProfile::new(columns.len(), 1);
        for i in 0..rows {
            let cells: Vec<Option<Value>> =
                columns.iter().map(|c| c.get(i).map(|s| json!(s))).collect();
            p.observe_row(cells.iter().map(Option::as_ref));
        }
        p
    }

    fn words(prefix: &str, n: usize) -> Vec<String> {
        (0..n).map(|i| format!("{prefix}{i}")).collect()
    }

    fn pair(pairs: &[SharedPair], a: usize, b: usize) -> Option<SharedPair> {
        pairs.iter().copied().find(|p| (p.a, p.b) == (a, b))
    }

    #[test]
    fn identical_vocabularies_score_one_and_unrelated_ones_make_no_pair() {
        let p = texts(&[words("a", 6), words("a", 6), words("c", 6)]);
        let pairs = shared_value_pairs(&p);
        assert_eq!(pairs.len(), 1);
        let ab = pair(&pairs, 0, 1).unwrap();
        assert_eq!(ab.common, 6);
        assert!((ab.score - 1.0).abs() < EPS, "{}", ab.score);
    }

    #[test]
    fn the_smaller_vocabulary_decides_so_a_contained_one_scores_one() {
        let big: Vec<String> = words("v", 5).into_iter().chain(words("w", 10)).collect();
        let p = texts(&[words("v", 5), big, words("z", 6)]);
        let ab = pair(&shared_value_pairs(&p), 0, 1).unwrap();
        assert!((ab.score - 1.0).abs() < EPS, "{}", ab.score);
    }

    #[test]
    fn a_value_held_by_every_column_weighs_nothing() {
        // x0..x4 are in all four columns (weight ln(4/4) = 0); a and b also share p0..p2 (weight ln(4/2))
        let shared = words("x", 5);
        let with = |extra: Vec<String>| shared.iter().cloned().chain(extra).collect::<Vec<_>>();
        let p = texts(&[
            with(words("p", 3)),
            with(words("p", 3)),
            with(words("c", 1)),
            with(words("d", 1)),
        ]);
        let pairs = shared_value_pairs(&p);
        let ab = pair(&pairs, 0, 1).unwrap();
        let ac = pair(&pairs, 0, 2).unwrap();
        assert_eq!((ab.common, ac.common), (8, 5));
        assert!(
            (ab.score - 1.0).abs() < EPS,
            "the rare values carry the whole weight of a: {}",
            ab.score
        );
        assert!(
            ac.score.abs() < EPS,
            "only values of weight zero in common: {}",
            ac.score
        );
    }

    #[test]
    fn two_common_values_are_not_a_pair() {
        let a = words("a", 8);
        let b: Vec<String> = a.iter().take(2).cloned().chain(words("b", 6)).collect();
        assert_eq!(shared_value_pairs(&texts(&[a, b])).len(), 0);
    }

    #[test]
    fn a_column_with_fewer_than_five_distinct_values_is_ignored() {
        let few = words("a", 4);
        let p = texts(&[few.clone(), few, words("a", 6)]);
        let pairs = shared_value_pairs(&p);
        assert!(
            pairs
                .iter()
                .all(|x| x.a != 0 && x.b != 0 && x.a != 1 && x.b != 1),
            "{pairs:?}"
        );
    }

    #[test]
    fn numbers_never_make_a_pair_even_when_they_collide() {
        let numeric: Vec<Vec<Value>> = (0..30)
            .map(|i| vec![json!(i % 10), json!(i % 10)])
            .collect();
        let mut p = ValueProfile::new(2, 1);
        for row in &numeric {
            p.observe_row(row.iter().map(Some));
        }
        assert_eq!(shared_value_pairs(&p).len(), 0);
    }

    #[test]
    fn pairs_are_ordered_deterministic_and_scores_stay_within_zero_and_one() {
        let p = texts(&[
            words("a", 9),
            words("a", 6),
            words("a", 7),
            words("b", 9),
            words("b", 5),
        ]);
        let pairs = shared_value_pairs(&p);
        assert_ne!(pairs.len(), 0);
        assert!(pairs
            .windows(2)
            .all(|w| (w[0].a, w[0].b) < (w[1].a, w[1].b)));
        assert!(pairs
            .iter()
            .all(|x| x.a < x.b && (0.0..=1.0 + EPS).contains(&x.score)));
        assert_eq!(shared_value_pairs(&p), pairs);
    }
}
