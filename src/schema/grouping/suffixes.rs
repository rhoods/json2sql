//! Suffixes alternatifs d'une grille proposée, repérés par la co-occurrence (#199, point 3).
//!
//! Les noms seuls trouvent une grille dès que des clés partagent un suffixe. Les suffixes d'un même nom remplis
//! ensemble dans un objet sont des aspects (`fat_100g`, `fat_serving`) : des colonnes de la grille. Ceux qui ne le sont
//! jamais sont des alternatives (`air_temp_c` / `air_temp_f`, `title_fr` / `title_en`) : une colonne par suffixe
//! serait presque toujours vide, et la table clé/valeur du split leur donne déjà une ligne par clé.
//!
//! La mesure part du suffixe le plus rempli. Pour chaque autre suffixe, sur les noms qu'ils ont tous les deux : les
//! lignes où les deux clés sont remplies, sur les lignes de la clé la plus rare (un `_label` rare reste un aspect).
//! Les présences brutes de l'échantillon servent, pas le φ : il ignore les colonnes de moins de 2 % des lignes.
//!
//! Fonctions :
//! - const `ASPECT_MIN_OVERLAP` — part des lignes de la clé la plus rare où deux suffixes d'un nom sont remplis ensemble.
//! - struct `CheckedGrid` — les suffixes gardés (aspects) et les suffixes alternatifs écartés.
//! - fn `split_alternatives` — sépare les aspects des alternatives d'une grille, d'après l'échantillon.
//! - fn `CheckedGrid::new` — une grille d'aspects et ses alternatives ; un seul aspect parmi des alternatives en est une.
//! - fn `sampled_bases` — base → colonne de l'échantillon, par suffixe.
//! - fn `overlap` — part des lignes de la clé la plus rare où deux suffixes sont remplis ensemble, sur leurs noms communs.

use std::collections::BTreeMap;

use super::presence::Presence;
use crate::schema::suffix_detector::assign_keys;

/// Two suffixes of a name filled together in at least this share of the rows of the rarer key are aspects. On OFF
/// `nutriments` the aspects reach 0.71 (`_modifier` with `_100g`) to 1.00; alternatives stay near 0.
pub const ASPECT_MIN_OVERLAP: f64 = 0.5;

/// A grid of the names, checked against the sample.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CheckedGrid {
    /// Suffixes kept in the grid, in the order of the grid.
    pub aspects: Vec<String>,
    /// Suffixes never filled together with the most filled one, in the order of the grid; all of them when no suffix is.
    pub alternatives: Vec<String>,
}

/// The aspects and the alternatives of the grid `suffixes` of `keys`: a suffix filled together with the most filled
/// one stays, the others are alternatives. A suffix without a name in common with it, or without a filled row, is
/// kept: nothing says it is an alternative. When no suffix stays with the most filled one, it is an alternative too
/// and the grid is gone. `column_of` gives the sample column of a key, if it was sampled.
pub fn split_alternatives(
    suffixes: &[String],
    keys: &[&str],
    column_of: impl Fn(&str) -> Option<usize>,
    presence: &Presence,
) -> CheckedGrid {
    let columns = sampled_bases(suffixes, keys, column_of);
    let filled = |s: &String| -> usize {
        columns[s]
            .values()
            .map(|&c| presence.present_count(c))
            .sum()
    };
    // The first of the most filled in suffix order.
    let main = columns.keys().copied().rev().max_by_key(|s| filled(s));
    let Some(main) = main.filter(|s| filled(s) > 0) else {
        return CheckedGrid::new(suffixes.to_vec(), Vec::new());
    };
    // `main` is filled with itself: it is always an aspect.
    let (aspects, alternatives) = suffixes.iter().cloned().partition(|s| {
        columns
            .get(s)
            .and_then(|other| overlap(&columns[main], other, presence))
            .is_none_or(|o| o >= ASPECT_MIN_OVERLAP)
    });
    CheckedGrid::new(aspects, alternatives)
}

impl CheckedGrid {
    /// The grid of `aspects`, without `alternatives`; a single aspect beside alternatives is one of them, the most
    /// filled: no grid then.
    fn new(aspects: Vec<String>, mut alternatives: Vec<String>) -> Self {
        if aspects.len() == 1 && !alternatives.is_empty() {
            alternatives.extend(aspects);
            alternatives.sort();
            return Self {
                aspects: Vec::new(),
                alternatives,
            };
        }
        Self {
            aspects,
            alternatives,
        }
    }
}

/// Base → sample column of its key, per suffix: each key under its longest suffix, as the grid assigns it.
fn sampled_bases<'s>(
    suffixes: &'s [String],
    keys: &[&'s str],
    column_of: impl Fn(&str) -> Option<usize>,
) -> BTreeMap<&'s String, BTreeMap<&'s str, usize>> {
    assign_keys(keys, suffixes)
        .into_iter()
        .filter_map(|(suffix, ks)| {
            let suffix = suffixes.iter().find(|s| **s == suffix)?;
            let bases = ks
                .iter()
                .filter_map(|k| Some((&k[..k.len() - suffix.len()], column_of(k)?)))
                .collect();
            Some((suffix, bases))
        })
        .collect()
}

/// Rows where both suffixes are filled over the rows of the rarer key, summed over the bases they share; `None`
/// without a shared base or a filled row.
fn overlap(
    a: &BTreeMap<&str, usize>,
    b: &BTreeMap<&str, usize>,
    presence: &Presence,
) -> Option<f64> {
    let (both, rarer) = a
        .iter()
        .filter_map(|(base, &ca)| Some((ca, *b.get(base)?)))
        .fold((0, 0), |(both, rarer), (ca, cb)| {
            (
                both + presence.together(ca, cb),
                rarer + presence.present_count(ca).min(presence.present_count(cb)),
            )
        });
    #[allow(clippy::cast_precision_loss)]
    (rarer > 0).then(|| both as f64 / rarer as f64)
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;

    /// The presence of `rows`, each the list of its filled keys among `keys` (one column per key, in order).
    fn presence_of(keys: &[&str], rows: &[&[&str]]) -> Presence {
        let mut presence = Presence::new(keys.len());
        for row in rows {
            presence.push_row(keys.iter().map(|k| row.contains(k)));
        }
        presence
    }

    fn check(suffixes: &[&str], keys: &[&str], rows: &[&[&str]]) -> CheckedGrid {
        let suffixes: Vec<String> = suffixes.iter().map(|s| (*s).to_string()).collect();
        let presence = presence_of(keys, rows);
        split_alternatives(
            &suffixes,
            keys,
            |k| keys.iter().position(|x| *x == k),
            &presence,
        )
    }

    fn strings(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn suffixes_filled_together_are_aspects() {
        let keys = ["a_min", "a_max", "b_min", "b_max"];
        let grid = check(
            &["_max", "_min"],
            &keys,
            &[&["a_min", "a_max"], &["b_min", "b_max"]],
        );
        assert_eq!(grid.aspects, strings(&["_max", "_min"]));
        assert_eq!(grid.alternatives, Vec::<String>::new());
    }

    #[test]
    fn suffixes_never_filled_together_leave_no_grid() {
        let keys = ["air_c", "air_f", "oil_c", "oil_f"];
        let rows: [&[&str]; 4] = [&["air_c"], &["air_f"], &["oil_c"], &["oil_f"]];
        let grid = check(&["_c", "_f"], &keys, &rows);
        assert_eq!(grid.aspects, Vec::<String>::new());
        assert_eq!(grid.alternatives, strings(&["_c", "_f"]));
    }

    #[test]
    fn a_rare_suffix_filled_with_the_main_one_stays_and_an_alternative_leaves() {
        // `_label` is in 1 row of 4 but always with `_100g`; `_prepared` never is.
        let keys = [
            "fat_100g",
            "fat_label",
            "fat_prepared",
            "salt_100g",
            "salt_prepared",
        ];
        let rows: [&[&str]; 4] = [
            &["fat_100g", "fat_label"],
            &["fat_100g"],
            &["salt_100g"],
            &["salt_prepared", "fat_prepared"],
        ];
        let grid = check(&["_100g", "_label", "_prepared"], &keys, &rows);
        assert_eq!(grid.aspects, strings(&["_100g", "_label"]));
        assert_eq!(grid.alternatives, strings(&["_prepared"]));
    }

    #[test]
    fn without_a_sampled_key_the_names_decide() {
        let keys = ["a_x", "a_y"];
        let suffixes = strings(&["_x", "_y"]);
        let presence = Presence::new(0);
        let grid = split_alternatives(&suffixes, &keys, |_| None, &presence);
        assert_eq!(grid.aspects, suffixes);
    }
}
