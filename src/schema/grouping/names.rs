//! Column-name normalization for the thematic grouping of wide tables (#78, T1a).
//!
//! Turns the `original_name` of every column of ONE table into a *stem*: the words that carry the theme.
//! Names are cut on `snake_case`, `camelCase` and letter/digit boundaries and numbering is dropped; then what
//! says nothing about the theme in THIS table is removed: format tokens (`tags`, `lc`: the same suffix
//! behind many different prefixes), the key of keyed families (`NAME_FR`, `FCLASS_IT`) and noise words
//! (a word in more than 40 % of the columns, e.g. `str` in `strMeal`). The thresholds are starting
//! points from the #78 spikes, to recalibrate on the T0 bench (`tests/bench_column_groups.rs`).
//!
//! Fonctions :
//! - struct `NameStems` — radicaux d'une table, avec les jetons de format, les mots de bruit et les colonnes à clé.
//! - fn `normalize` — radical de chaque colonne d'une table, à partir de leur `original_name`.
//! - fn `words` — mots d'un nom en minuscules, numérotation écartée.
//! - fn `split_words` — découpe brute d'un nom (`snake_case`, `camelCase`, lettre/chiffre), chiffres conservés.
//! - fn `starts_new_word` — vrai quand un caractère ouvre un nouveau mot (passage minuscule→majuscule ou lettre↔chiffre).
//! - fn `format_tokens` — mots qui suivent le premier mot derrière beaucoup de premiers mots différents.
//! - fn `noise_words` — mots présents dans plus de 40 % des colonnes.
//! - fn `keyed_columns` — colonnes membres d'une famille à clé (`NAME_FR`, `NAME_DE`…).
//! - fn `is_key` — vrai pour un mot de trois lettres au plus (code de langue ou de pays).
//! - fn `numbered_families` — familles de colonnes qui ne diffèrent que par leur numéro (`strIngredient1..20`).
//! - fn `stem` — radical d'une colonne : mots du thème une fois clé, jetons de format et bruit retirés.

use std::collections::{BTreeMap, BTreeSet};

/// A word is a format token when it follows the first word of columns with at least this many different first words.
const FORMAT_SPREAD: usize = 6;
/// A keyed family (`NAME_FR`, `NAME_DE`…: same words, one short key) has at least this many columns.
const KEYED_FAMILY_MIN: usize = 8;
/// A numbered family (`strIngredient1`, `strIngredient2`…) has at least this many columns.
const NUMBERED_FAMILY_MIN: usize = 8;
/// The key of a keyed family has at most this many letters.
const KEY_MAX_LEN: usize = 3;
/// A word in more than this percentage of the columns carries no theme.
const NOISE_SHARE_PERCENT: usize = 40;

/// Stems of one table's columns, with what was removed to get them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameStems {
    /// Stem of each input name, same order: the words that carry the theme (empty if the name has no word).
    pub stems: Vec<Vec<String>>,
    /// Words that follow the first word behind at least `FORMAT_SPREAD` different first words.
    pub format_tokens: BTreeSet<String>,
    /// Words present in more than `NOISE_SHARE_PERCENT` % of the columns.
    pub noise_words: BTreeSet<String>,
    /// `true` for the members of a keyed family, whose key was removed from the stem.
    pub keyed: Vec<bool>,
}

/// Stem of every column of a table, from their `original_name`s.
pub fn normalize<S: AsRef<str>>(names: &[S]) -> NameStems {
    let words: Vec<Vec<String>> = names.iter().map(|n| words(n.as_ref())).collect();
    let format_tokens = format_tokens(&words);
    let noise_words = noise_words(&words);
    let keyed = keyed_columns(&words);
    let stems = words
        .iter()
        .zip(&keyed)
        .map(|(w, &k)| stem(w, k, &format_tokens, &noise_words))
        .collect();
    NameStems {
        stems,
        format_tokens,
        noise_words,
        keyed,
    }
}

/// Lower-case words of a name, numbering dropped (`strIngredient12` gives `str`, `ingredient`).
/// A name made only of numbers keeps them.
fn words(name: &str) -> Vec<String> {
    let all = split_words(name);
    let themed: Vec<String> = all
        .iter()
        .filter(|w| !w.chars().all(char::is_numeric))
        .cloned()
        .collect();
    if themed.is_empty() {
        all
    } else {
        themed
    }
}

/// Raw cut of a name on non-alphanumerics, lower-to-upper `camelCase` and letter/digit boundaries.
fn split_words(name: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut prev: Option<char> = None;
    for ch in name.chars() {
        let boundary = match prev {
            Some(p) if ch.is_alphanumeric() => starts_new_word(p, ch),
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

/// True when `ch` starts a new word after `prev` (both alphanumeric): lower-to-upper `camelCase`, or letter/digit.
fn starts_new_word(prev: char, ch: char) -> bool {
    let camel = prev.is_lowercase() && ch.is_uppercase();
    let letter_digit = prev.is_alphabetic() != ch.is_alphabetic();
    camel || letter_digit
}

/// Words found after the first word behind at least `FORMAT_SPREAD` different first words.
fn format_tokens(words: &[Vec<String>]) -> BTreeSet<String> {
    let mut firsts_before: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for w in words {
        if let Some((first, rest)) = w.split_first() {
            for token in rest {
                firsts_before.entry(token).or_default().insert(first);
            }
        }
    }
    firsts_before
        .into_iter()
        .filter(|(_, firsts)| firsts.len() >= FORMAT_SPREAD)
        .map(|(token, _)| token.to_string())
        .collect()
}

/// Words that occur in more than `NOISE_SHARE_PERCENT` % of the columns.
fn noise_words(words: &[Vec<String>]) -> BTreeSet<String> {
    let mut columns_with: BTreeMap<&str, usize> = BTreeMap::new();
    for w in words {
        // a word counts once per column, however often the name repeats it
        let mut once: Vec<&str> = w.iter().map(String::as_str).collect();
        once.sort_unstable();
        once.dedup();
        for token in once {
            *columns_with.entry(token).or_default() += 1;
        }
    }
    columns_with
        .into_iter()
        .filter(|&(_, count)| count * 100 > NOISE_SHARE_PERCENT * words.len())
        .map(|(token, _)| token.to_string())
        .collect()
}

/// Columns of a keyed family: same words but a last word of at most [`KEY_MAX_LEN`] letters,
/// in a family of at least [`KEYED_FAMILY_MIN`] columns.
fn keyed_columns(words: &[Vec<String>]) -> Vec<bool> {
    let mut families: BTreeMap<&[String], Vec<usize>> = BTreeMap::new();
    for (i, w) in words.iter().enumerate() {
        if let Some((key, rest)) = w.split_last() {
            if !rest.is_empty() && is_key(key) {
                families.entry(rest).or_default().push(i);
            }
        }
    }
    let mut keyed = vec![false; words.len()];
    for &member in families
        .values()
        .filter(|m| m.len() >= KEYED_FAMILY_MIN)
        .flatten()
    {
        keyed[member] = true;
    }
    keyed
}

/// True for a word short enough, and alphabetic, to be a language or country code.
fn is_key(word: &str) -> bool {
    word.chars().count() <= KEY_MAX_LEN && word.chars().all(char::is_alphabetic)
}

/// Families of columns whose names differ only by a number (`strIngredient1..20`, `POP1950..POP2050`): the
/// indexes of each family of at least `NUMBERED_FAMILY_MIN` columns, families and members in column order.
pub fn numbered_families<S: AsRef<str>>(names: &[S]) -> Vec<Vec<usize>> {
    let mut families: BTreeMap<Vec<String>, Vec<usize>> = BTreeMap::new();
    for (index, name) in names.iter().enumerate() {
        let tokens = split_words(name.as_ref());
        let is_number = |token: &String| token.chars().all(char::is_numeric);
        if tokens.iter().any(is_number) && !tokens.iter().all(is_number) {
            families
                .entry(words(name.as_ref()))
                .or_default()
                .push(index);
        }
    }
    let mut kept: Vec<Vec<usize>> = families
        .into_values()
        .filter(|members| members.len() >= NUMBERED_FAMILY_MIN)
        .collect();
    kept.sort_by_key(|members| members[0]);
    kept
}

/// Stem of one column: its words without the key of its family, the format tokens (except the first
/// word) and the noise words; the words as they are if nothing would be left.
fn stem(
    words: &[String],
    keyed: bool,
    format: &BTreeSet<String>,
    noise: &BTreeSet<String>,
) -> Vec<String> {
    let body = if keyed {
        &words[..words.len().saturating_sub(1)]
    } else {
        words
    };
    let Some((first, rest)) = body.split_first() else {
        return Vec::new();
    };
    let base: Vec<String> = std::iter::once(first)
        .chain(rest.iter().filter(|t| !format.contains(*t)))
        .cloned()
        .collect();
    let themed: Vec<String> = base
        .iter()
        .filter(|t| !noise.contains(*t))
        .cloned()
        .collect();
    if themed.is_empty() {
        base
    } else {
        themed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `n` distinct one-word names (`qa`, `qb`…) that dilute the share of the words under test.
    fn fillers(n: usize) -> Vec<String> {
        (b'a'..=b'z')
            .take(n)
            .map(|c| format!("q{}", char::from(c)))
            .collect()
    }

    #[test]
    fn words_split_snake_camel_and_letter_digit_boundaries() {
        for (name, want) in [
            ("nutrition_score", vec!["nutrition", "score"]),
            ("strIngredient12", vec!["str", "ingredient"]),
            ("ADM0_A3_US", vec!["adm", "a", "us"]),
            ("POP1950", vec!["pop"]),
            ("_keywords", vec!["keywords"]),
            ("100g_energy", vec!["g", "energy"]),
            ("2024", vec!["2024"]),
            ("___", vec![]),
            ("", vec![]),
        ] {
            assert_eq!(words(name), want, "{name}");
        }
    }

    #[test]
    fn a_word_behind_six_different_first_words_is_a_format_token() {
        let mut names: Vec<String> = ["a", "b", "c", "d", "e", "f"]
            .iter()
            .map(|p| format!("{p}_tags"))
            .collect();
        names.extend(fillers(20));
        let got = normalize(&names);
        assert_eq!(got.format_tokens, BTreeSet::from(["tags".to_string()]));
        assert_eq!(got.stems[0], ["a"]);
        // five different first words are not enough
        assert!(normalize(&names[1..]).format_tokens.is_empty());
    }

    #[test]
    fn the_first_word_is_never_dropped_as_a_format_token() {
        let mut names: Vec<String> = ["a", "b", "c", "d", "e", "f"]
            .iter()
            .map(|p| format!("{p}_tags"))
            .collect();
        names.push("tags_other".into());
        names.extend(fillers(20));
        let got = normalize(&names);
        assert!(got.format_tokens.contains("tags"));
        assert_eq!(got.stems[6], ["tags", "other"]);
    }

    #[test]
    fn a_family_of_eight_columns_with_a_short_key_loses_its_key() {
        let mut names: Vec<String> = ["fr", "de", "es", "it", "pt", "ru", "zh", "zht"]
            .iter()
            .map(|k| format!("name_{k}"))
            .collect();
        names.push("name_long".into()); // four letters: a word, not a key
        names.extend(fillers(20));
        let got = normalize(&names);
        assert!(got.keyed[..8].iter().all(|&k| k));
        assert!(got.stems[..8].iter().all(|s| s == &["name"]));
        assert!(!got.keyed[8]);
        assert_eq!(got.stems[8], ["name", "long"]);
        // one member fewer and it is no family any more
        let seven = normalize(&names[1..]);
        assert!(seven.keyed.iter().all(|&k| !k));
        assert_eq!(seven.stems[0], ["name", "de"]);
    }

    #[test]
    fn a_word_in_more_than_forty_percent_of_the_columns_is_noise_unless_nothing_is_left() {
        // "str" is in 5 of 6 columns
        let got = normalize(&[
            "strMeal",
            "strArea",
            "strTags",
            "strCategory",
            "other",
            "str",
        ]);
        assert_eq!(got.noise_words, BTreeSet::from(["str".to_string()]));
        assert_eq!(got.stems[0], ["meal"]);
        assert_eq!(got.stems[5], ["str"], "a stem is never emptied");
        // exactly 40 % is not "more than 40 %"
        assert!(normalize(&["a_x", "a_y", "b", "c", "d"])
            .noise_words
            .is_empty());
    }

    #[test]
    fn a_word_repeated_inside_one_name_counts_once_toward_noise() {
        // "a" is in 1 of 4 columns (25 %), not 2 of 4
        assert!(normalize(&["a_a", "b", "c", "d"]).noise_words.is_empty());
    }

    #[test]
    fn columns_differing_only_by_numbering_share_their_stem() {
        let mut names = vec!["item1".to_string(), "item2".into(), "item10".into()];
        names.extend(fillers(20));
        let got = normalize(&names);
        assert_eq!(got.stems[0], ["item"]);
        assert_eq!(got.stems[0], got.stems[1]);
        assert_eq!(got.stems[1], got.stems[2]);
    }

    #[test]
    fn columns_that_differ_only_by_a_number_form_a_numbered_family() {
        let mut names: Vec<String> = (1..=8).map(|i| format!("strIngredient{i}")).collect();
        names.extend((1..=9).map(|i| format!("strMeasure{i}")));
        names.extend(["idMeal".to_string(), "strMeal".into(), "strTags".into()]);
        names.extend((1..=7).map(|i| format!("POP{}", 1950 + i * 5))); // seven only: below the minimum
        let got = numbered_families(&names);
        assert_eq!(
            got,
            [(0..8).collect::<Vec<_>>(), (8..17).collect::<Vec<_>>()]
        );
    }

    #[test]
    fn a_name_that_is_only_a_number_or_has_no_number_is_not_numbered() {
        let mut names: Vec<String> = (0..10).map(|i| format!("{}", 2000 + i)).collect();
        names.extend((0..10).map(|i| format!("plain{}", char::from(b'a' + i))));
        assert!(numbered_families(&names).is_empty());
    }

    #[test]
    fn stems_follow_the_input_order_and_a_nameless_column_has_an_empty_stem() {
        let names = ["zeta_b", "alpha", "___"];
        let got = normalize(&names);
        assert_eq!(got.stems, vec![vec!["zeta", "b"], vec!["alpha"], vec![]]);
        assert_eq!(got.keyed, [false; 3]);
        assert_eq!(normalize(&names), got, "same input, same output");
        let none = normalize::<&str>(&[]);
        assert!(
            none.stems.is_empty()
                && none.format_tokens.is_empty()
                && none.noise_words.is_empty()
                && none.keyed.is_empty()
        );
    }
}
