//! Detects suffix structure in wide-table key sets for the `StructuredPivot` strategy.
#![allow(clippy::cast_precision_loss)]
//!
//! Given keys like `energy`, `energy_100g`, `energy_unit`, identifies common suffixes
//! (`_100g`, `_unit`) shared by enough distinct bases, then assigns every scalar key to one
//! base to produce a [`SuffixSchema`] and the keys of the grid (#163).
//!
//! Fonctions :
//! - fn `build_suffix_to_bases` — énumère les découpages (base, suffixe) candidats sur `_`.
//! - fn `scalar_keys` — clés scalaires d'une table : ni objet, ni tableau, ni colonne PG array.
//! - fn `assign_keys` — attribue chaque clé au suffixe le plus long qui la termine.
//! - fn `detect_suffix_schema` — détecte automatiquement la grille bases × suffixes et ses clés.
//! - fn `grid_keys` — clés nues et clés de la grille d'une attribution.
//! - fn `retained_assignment` — attribution des clés aux suffixes qui gardent assez de bases.
//! - fn `build_suffix_cols_and_value_type` — construit les `SuffixColumn` + type de la colonne `value` de base.
//! - fn `widest_type` — type le plus large des clés observées (`TEXT` s'il n'y en a pas).
//! - fn `suffix_col_name` — nom de colonne d'un suffixe (`norm_value` pour `value`).
//! - fn `declared_suffixes` — les `suffix_columns` d'une config TOML, avec leur `_` de tête.
//! - fn `build_suffix_schema_from_list` — variante explicite (TOML `suffix_columns`) : suffixes déclarés et clés de la grille.

use std::collections::{BTreeMap, HashMap, HashSet};

use indexmap::IndexMap;

use crate::schema::naming::sanitize_identifier;
use crate::schema::observer::TableEntry;
use crate::schema::table_schema::{SuffixColumn, SuffixSchema};
use crate::schema::type_tracker::{widen_pg_types, PgType, TypeTracker};

/// Minimum number of distinct bases a suffix must appear with: two let a pair of dynamic keys
/// (`en:celery_seed`, `en:mustard_seed`) invent a `_seed` suffix (#163).
const MIN_BASES: usize = 3;

/// Every (base, suffix) split of the keys on `_`, as suffix → set of bases.
fn build_suffix_to_bases(all_keys: &[&str]) -> HashMap<String, HashSet<String>> {
    let mut suffix_to_bases: HashMap<String, HashSet<String>> = HashMap::new();
    for &key in all_keys {
        for (pos, _) in key.char_indices().filter(|(_, c)| *c == '_') {
            let base = &key[..pos];
            let suffix = &key[pos..]; // includes the leading `_`
            if !base.is_empty() && suffix.len() > 1 {
                suffix_to_bases
                    .entry(suffix.to_string())
                    .or_default()
                    .insert(base.to_string());
            }
        }
    }
    suffix_to_bases
}

/// Keys of the table that hold scalars: objects and arrays are child tables or PG array columns,
/// never part of a grid.
fn scalar_keys(entry: &TableEntry) -> Vec<&str> {
    entry
        .columns
        .iter()
        .filter(|(k, t)| {
            !t.is_object_field() && !t.is_array_field() && !entry.array_columns.contains_key(*k)
        })
        .map(|(k, _)| k.as_str())
        .collect()
}

/// Suffix → keys ending with it, each key under the longest suffix that ends it with a non-empty
/// base (`fat_prepared_100g` goes to `_prepared_100g`, not `_100g`). Pass 2 matches the same way.
fn assign_keys<'a>(keys: &[&'a str], suffixes: &[String]) -> BTreeMap<String, Vec<&'a str>> {
    let mut assigned: BTreeMap<String, Vec<&'a str>> = BTreeMap::new();
    for &key in keys {
        let longest = suffixes
            .iter()
            .filter(|s| key.len() > s.len() && key.ends_with(s.as_str()))
            .max_by_key(|s| s.len());
        if let Some(suffix) = longest {
            assigned.entry(suffix.clone()).or_default().push(key);
        }
    }
    assigned
}

/// The grid of a wide table: suffixes shared by at least `MIN_BASES` bases and by at least
/// `coverage_threshold` of all the bases, with the keys it covers (suffixed keys and bare bases).
/// The grid must also cover `coverage_threshold` of the table's scalar columns.
///
/// Returns `None` if no grid is found above threshold.
#[must_use]
pub fn detect_suffix_schema(
    entry: &TableEntry,
    coverage_threshold: f64,
) -> Option<(SuffixSchema, HashSet<String>)> {
    let keys = scalar_keys(entry);
    let assigned = retained_assignment(&keys, coverage_threshold);
    if assigned.is_empty() {
        return None;
    }
    let (bare, grid) = grid_keys(&keys, &assigned);
    if (grid.len() as f64 / keys.len() as f64) < coverage_threshold {
        return None;
    }
    let (suffix_cols, value_type) =
        build_suffix_cols_and_value_type(&assigned, &bare, &entry.columns);
    let schema = SuffixSchema {
        suffix_cols,
        value_type,
    };
    Some((schema, grid))
}

/// Bare keys of the grid (keys that are a base of an assigned key) and every key of the grid:
/// the assigned keys plus the bare ones.
fn grid_keys<'a>(
    keys: &[&'a str],
    assigned: &BTreeMap<String, Vec<&'a str>>,
) -> (Vec<&'a str>, HashSet<String>) {
    let bases: HashSet<&str> = assigned
        .iter()
        .flat_map(|(s, ks)| ks.iter().map(move |k| &k[..k.len() - s.len()]))
        .collect();
    let bare: Vec<&str> = keys.iter().copied().filter(|k| bases.contains(k)).collect();
    let grid = assigned
        .values()
        .flatten()
        .chain(&bare)
        .map(|k| (*k).to_string())
        .collect();
    (bare, grid)
}

/// Assignment of the keys to the suffixes that keep `MIN_BASES` bases and `coverage_threshold`
/// of all the bases. Assigning keys to their longest suffix can leave a shorter one with too few
/// bases: it is dropped and the keys are assigned again, until nothing changes (the set of
/// suffixes only shrinks).
fn retained_assignment<'a>(
    keys: &[&'a str],
    coverage_threshold: f64,
) -> BTreeMap<String, Vec<&'a str>> {
    let mut suffixes: Vec<String> = build_suffix_to_bases(keys)
        .into_iter()
        .filter(|(_, bases)| bases.len() >= MIN_BASES)
        .map(|(suffix, _)| suffix)
        .collect();
    loop {
        let assigned = assign_keys(keys, &suffixes);
        let all_bases: HashSet<&str> = assigned
            .iter()
            .flat_map(|(s, ks)| ks.iter().map(move |k| &k[..k.len() - s.len()]))
            .collect();
        let kept: Vec<String> = assigned
            .iter()
            .filter(|(_, ks)| {
                ks.len() >= MIN_BASES
                    && ks.len() as f64 / all_bases.len() as f64 >= coverage_threshold
            })
            .map(|(s, _)| s.clone())
            .collect();
        if kept.len() == suffixes.len() {
            return assigned;
        }
        suffixes = kept;
    }
}

/// Build `SuffixColumn` entries (in suffix order) and the base `value` column type, each typed
/// from the keys assigned to it.
fn build_suffix_cols_and_value_type(
    assigned: &BTreeMap<String, Vec<&str>>,
    bare: &[&str],
    columns: &IndexMap<String, TypeTracker>,
) -> (Vec<SuffixColumn>, PgType) {
    let suffix_cols = assigned
        .iter()
        .map(|(suffix, keys)| SuffixColumn {
            suffix: suffix.clone(),
            col_name: suffix_col_name(suffix),
            pg_type: widest_type(keys.iter().filter_map(|k| columns.get(*k))),
        })
        .collect();
    let value_type = widest_type(bare.iter().filter_map(|k| columns.get(*k)));
    (suffix_cols, value_type)
}

/// Widest type of the observed keys (`TEXT` when there is none).
fn widest_type<'a>(trackers: impl Iterator<Item = &'a TypeTracker>) -> PgType {
    trackers
        .fold(None::<PgType>, |acc, tracker| {
            let t = tracker.to_pg_type();
            Some(match acc {
                None => t,
                Some(a) => widen_pg_types(a, &t),
            })
        })
        .unwrap_or(PgType::Text)
}

/// Column name of a suffix: the suffix without its leading `_`, as an identifier. A suffix that
/// starts with a digit keeps its `_` (`_100g`, not the opaque `c_100g`). `value` and `name`
/// would clash with the pivot's own columns: `norm_value`, `norm_name`.
fn suffix_col_name(suffix: &str) -> String {
    let bare = suffix.trim_start_matches('_');
    let s = sanitize_identifier(bare);
    match s.as_str() {
        "value" => "norm_value".to_string(),
        "name" => "norm_name".to_string(),
        _ if bare.starts_with(|c: char| c.is_ascii_digit()) => {
            // sanitize_identifier prefixed `c_` to the digit.
            format!("_{}", s.strip_prefix("c_").unwrap_or(&s))
        }
        _ => s,
    }
}

/// The `suffix_columns` of a TOML config as suffixes: a leading `_` is added when missing.
#[must_use]
pub fn declared_suffixes(suffix_list: &[String]) -> Vec<String> {
    suffix_list
        .iter()
        .map(|s| {
            if s.starts_with('_') {
                s.clone()
            } else {
                format!("_{s}")
            }
        })
        .collect()
}

/// Used when the user declares `suffix_columns` in the TOML config: the declared suffixes (a
/// leading `_` is added when missing), in their order, with the keys of the grid. `columns` are
/// the table's scalar columns; keys go to their longest suffix, as in the detection, and only
/// the bare keys of the grid type `value` (#163).
#[must_use]
pub fn build_suffix_schema_from_list(
    suffix_list: &[String],
    columns: &IndexMap<String, TypeTracker>,
) -> (SuffixSchema, HashSet<String>) {
    let suffixes = declared_suffixes(suffix_list);
    let keys: Vec<&str> = columns.keys().map(String::as_str).collect();
    let assigned = assign_keys(&keys, &suffixes);
    let (bare, grid) = grid_keys(&keys, &assigned);
    let suffix_cols = suffixes
        .iter()
        .map(|suffix| SuffixColumn {
            suffix: suffix.clone(),
            col_name: suffix_col_name(suffix),
            pg_type: widest_type(
                assigned
                    .get(suffix)
                    .into_iter()
                    .flatten()
                    .filter_map(|k| columns.get(*k)),
            ),
        })
        .collect();
    let value_type = widest_type(bare.iter().filter_map(|k| columns.get(*k)));
    let schema = SuffixSchema {
        suffix_cols,
        value_type,
    };
    (schema, grid)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods))]
mod tests {
    use super::*;

    fn make_object_tracker() -> TypeTracker {
        let mut t = TypeTracker::new(256);
        t.observe(&serde_json::json!({"k": 1}));
        t
    }

    fn entry_of(columns: IndexMap<String, TypeTracker>) -> TableEntry {
        TableEntry {
            path_key: "t".to_string(),
            path: vec!["t".to_string()],
            parent_key: String::new(),
            columns,
            child_kind: None,
            array_columns: IndexMap::new(),
            row_count: 1,
        }
    }

    /// `{base}{suffix}` float columns for every base and suffix ("" = the bare key).
    fn grid(bases: &[&str], suffixes: &[&str]) -> IndexMap<String, TypeTracker> {
        let mut cols = IndexMap::new();
        for base in bases {
            for suffix in suffixes {
                cols.insert(format!("{base}{suffix}"), make_float_tracker());
            }
        }
        cols
    }

    #[test]
    fn suffix_columns_are_named_after_the_json_suffix() {
        // `100g` can't start an unquoted identifier: keep the `_` rather than the opaque `c_100g`.
        assert_eq!(suffix_col_name("_100g"), "_100g");
        assert_eq!(suffix_col_name("_100"), "_100");
        assert_eq!(suffix_col_name("_unit"), "unit");
        // Reserved by the pivot's own `name` / `value` columns.
        assert_eq!(suffix_col_name("_value"), "norm_value");
        assert_eq!(suffix_col_name("_name"), "norm_name");
    }

    #[test]
    fn two_suffixed_keys_among_thirty_unrelated_ones_are_not_a_grid() {
        let mut cols: IndexMap<String, TypeTracker> = (0..30)
            .map(|i| (format!("key{i}"), make_text_tracker()))
            .collect();
        cols.insert("a_t".to_string(), make_float_tracker());
        cols.insert("b_t".to_string(), make_float_tracker());
        assert!(detect_suffix_schema(&entry_of(cols), 0.3).is_none());
    }

    #[test]
    fn a_suffix_needs_three_bases() {
        // `en:celery_seed` + `en:mustard_seed` (OFF) made a `_seed` suffix out of two dynamic keys.
        let cols = grid(&["fat", "salt"], &["", "_100g"]);
        assert!(detect_suffix_schema(&entry_of(cols), 0.3).is_none());
    }

    #[test]
    fn coverage_counts_the_scalar_columns_of_the_table() {
        // Three `_t` keys out of 23 columns: the grid covers 13 % of the table, not 100 % of its bases.
        let mut cols = grid(&["a", "b", "c"], &["_t"]);
        cols.extend((0..20).map(|i| (format!("key{i}"), make_text_tracker())));
        assert!(detect_suffix_schema(&entry_of(cols), 0.3).is_none());
    }

    #[test]
    fn a_key_goes_to_the_longest_suffix() {
        let mut cols = grid(&["fat", "salt", "sugars"], &["", "_100g"]);
        for base in ["fat", "salt", "sugars"] {
            cols.insert(format!("{base}_prepared_100g"), make_text_tracker());
        }
        let (schema, keys) = detect_suffix_schema(&entry_of(cols), 0.3).expect("grid");
        let type_of = |suffix: &str| {
            schema
                .suffix_cols
                .iter()
                .find(|c| c.suffix == suffix)
                .map(|c| c.pg_type.clone())
        };
        // `fat_prepared_100g` (text) counted under `_100g` would widen it to text.
        assert_eq!(type_of("_100g"), Some(PgType::DoublePrecision));
        assert!(type_of("_prepared_100g").is_some());
        assert_eq!(keys.len(), 9);
    }

    #[test]
    fn objects_and_arrays_are_not_part_of_the_grid() {
        let mut cols = grid(&["a", "b", "c"], &["", "_100g"]);
        for base in ["p", "q", "r"] {
            cols.insert(format!("{base}_nutrients"), make_object_tracker());
        }
        cols.insert("d_100g".to_string(), make_float_tracker());
        let mut entry = entry_of(cols);
        // A field seen both as a scalar and as an array is a PG array column (#86).
        entry
            .array_columns
            .insert("d_100g".to_string(), make_float_tracker());
        let (schema, keys) = detect_suffix_schema(&entry, 0.3).expect("grid");
        let suffixes: Vec<&str> = schema
            .suffix_cols
            .iter()
            .map(|c| c.suffix.as_str())
            .collect();
        assert_eq!(suffixes, ["_100g"]);
        let mut keys: Vec<String> = keys.into_iter().collect();
        keys.sort();
        assert_eq!(keys, ["a", "a_100g", "b", "b_100g", "c", "c_100g"]);
    }

    fn make_float_tracker() -> TypeTracker {
        let mut t = TypeTracker::new(256);
        t.observe(&serde_json::Value::Number(
            serde_json::Number::from_f64(1.5).unwrap(),
        ));
        t
    }

    fn make_text_tracker() -> TypeTracker {
        let mut t = TypeTracker::new(256);
        t.observe(&serde_json::Value::String("g".to_string()));
        t
    }

    #[test]
    fn test_detect_nutrient_structure() {
        let mut cols: IndexMap<String, TypeTracker> = IndexMap::new();
        // Bases
        cols.insert("calcium".to_string(), make_float_tracker());
        cols.insert("iron".to_string(), make_float_tracker());
        cols.insert("sodium".to_string(), make_float_tracker());
        // _100g suffix
        cols.insert("calcium_100g".to_string(), make_float_tracker());
        cols.insert("iron_100g".to_string(), make_float_tracker());
        cols.insert("sodium_100g".to_string(), make_float_tracker());
        // _unit suffix
        cols.insert("calcium_unit".to_string(), make_text_tracker());
        cols.insert("iron_unit".to_string(), make_text_tracker());
        cols.insert("sodium_unit".to_string(), make_text_tracker());

        let result = detect_suffix_schema(&entry_of(cols), 0.3);
        assert!(result.is_some(), "should detect suffix structure");

        let (schema, _) = result.unwrap();
        let suffixes: Vec<&str> = schema
            .suffix_cols
            .iter()
            .map(|s| s.suffix.as_str())
            .collect();
        assert!(suffixes.contains(&"_100g"), "should detect _100g");
        assert!(suffixes.contains(&"_unit"), "should detect _unit");
    }

    #[test]
    fn test_no_detection_flat_keys() {
        // Keys with no common suffix pattern
        let mut cols: IndexMap<String, TypeTracker> = IndexMap::new();
        cols.insert("foo".to_string(), make_float_tracker());
        cols.insert("bar".to_string(), make_float_tracker());
        cols.insert("baz".to_string(), make_float_tracker());

        let result = detect_suffix_schema(&entry_of(cols), 0.3);
        assert!(
            result.is_none(),
            "flat keys should not be detected as structured"
        );
    }

    #[test]
    fn test_value_suffix_renamed_to_norm_value() {
        let mut cols: IndexMap<String, TypeTracker> = IndexMap::new();
        for base in ["calcium", "iron", "zinc"] {
            cols.insert(base.to_string(), make_float_tracker());
            cols.insert(format!("{base}_value"), make_float_tracker());
        }

        let result = detect_suffix_schema(&entry_of(cols), 0.3);
        assert!(result.is_some());
        let (schema, _) = result.unwrap();
        let col_names: Vec<&str> = schema
            .suffix_cols
            .iter()
            .map(|s| s.col_name.as_str())
            .collect();
        // `_value` suffix must be renamed to avoid collision with base `value` column
        assert!(
            !col_names.contains(&"value"),
            "should not have raw 'value' col name"
        );
        assert!(
            col_names.contains(&"norm_value"),
            "should have 'norm_value'"
        );
    }
}
