//! Detects suffix structure in wide-table key sets for the `StructuredPivot` strategy.
#![allow(clippy::cast_precision_loss)]
//!
//! Given keys like `energy`, `energy_100g`, `energy_unit`, identifies common suffixes
//! (`_100g`, `_unit`) shared by enough distinct bases, then assigns every scalar key to one
//! base to produce a [`SuffixSchema`] and the keys of the grid (#163).
//!
//! Fonctions :
//! - fn `build_suffix_to_bases` — énumère les découpages (base, suffixe) candidats sur `_`.
//! - fn `assign_keys` — attribue chaque clé à la partie la plus longue qui la termine (ou la commence, #199).
//! - fn `detect_suffix_schema` — détecte automatiquement la grille bases × suffixes et ses clés.
//! - fn `grid_keys` — clés nues et clés de la grille d'une attribution.
//! - fn `retained_assignment` — attribution des clés aux suffixes d'une seule grille, chacun avec assez de bases.
//! - fn `one_grid` — les suffixes d'une grille : ceux dont les bases sont celles de la grille (#199).
//! - fn `build_suffix_cols_and_value_type` — construit les `SuffixColumn` + type de la colonne `value` de base.
//! - fn `widest_type` — type le plus large des clés observées (`TEXT` s'il n'y en a pas).
//! - fn `suffix_col_name` — nom de colonne d'un suffixe ou d'un préfixe (`norm_name` pour `name`).
//! - fn `declared_suffixes` — les `suffix_columns` (ou `prefix_columns`) d'une config TOML, avec leur `_` côté base.
//! - fn `build_suffix_schema_from_list` — variante explicite (TOML `suffix_columns` ou `prefix_columns`) : parties déclarées et clés de la grille.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::schema::naming::sanitize_identifier;
use crate::schema::stats::{ColumnStats, ObservedKeys};
use crate::schema::table_schema::{GridSide, SuffixColumn, SuffixSchema};
use crate::schema::type_tracker::{widen_pg_types, PgType};

/// Minimum fraction of the keys a proposed grid must cover (#163); the grid is only proposed (#200).
pub const SUFFIX_MIN_COVERAGE: f64 = 0.3;

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

/// Part → keys that have it at the `side` end, each key under the longest part with a non-empty base
/// (`fat_prepared_100g` goes to `_prepared_100g`, not `_100g`). Pass 2 matches the same way.
pub fn assign_keys<'a>(
    keys: &[&'a str],
    suffixes: &[String],
    side: GridSide,
) -> BTreeMap<String, Vec<&'a str>> {
    let mut assigned: BTreeMap<String, Vec<&'a str>> = BTreeMap::new();
    for &key in keys {
        let longest = suffixes
            .iter()
            .filter(|s| side.base_of(key, s).is_some())
            .max_by_key(|s| s.len());
        if let Some(suffix) = longest {
            assigned.entry(suffix.clone()).or_default().push(key);
        }
    }
    assigned
}

/// The grid of a wide table, from its scalar keys (objects and arrays are child tables or PG array columns, never
/// part of a grid): suffixes shared by at least `MIN_BASES` bases and of the same bases (see [`one_grid`]), with the
/// keys it covers (suffixed keys and bare bases). The grid must cover `coverage_threshold` of the table's scalar
/// columns.
///
/// Returns `None` if no grid is found above threshold.
#[must_use]
pub fn detect_suffix_schema(
    observed: &ObservedKeys,
    coverage_threshold: f64,
) -> Option<(SuffixSchema, HashSet<String>)> {
    let keys: Vec<&str> = observed.keys().map(String::as_str).collect();
    let assigned = retained_assignment(&keys);
    if assigned.is_empty() {
        return None;
    }
    let (bare, grid) = grid_keys(&keys, &assigned, GridSide::Suffix);
    if (grid.len() as f64 / keys.len() as f64) < coverage_threshold {
        return None;
    }
    let (suffix_cols, value_type) = build_suffix_cols_and_value_type(&assigned, &bare, observed);
    let schema = SuffixSchema {
        suffix_cols,
        value_type,
        side: GridSide::Suffix,
    };
    Some((schema, grid))
}

/// Bare keys of the grid (keys that are a base of an assigned key) and every key of the grid:
/// the assigned keys plus the bare ones.
fn grid_keys<'a>(
    keys: &[&'a str],
    assigned: &BTreeMap<String, Vec<&'a str>>,
    side: GridSide,
) -> (Vec<&'a str>, HashSet<String>) {
    let bases: HashSet<&str> = assigned
        .iter()
        .flat_map(|(s, ks)| ks.iter().filter_map(move |k| side.base_of(k, s)))
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

/// Assignment of the keys to the suffixes of one grid (see [`one_grid`]), each suffix with `MIN_BASES` bases.
/// Assigning keys to their longest suffix can leave a shorter one with too few bases, and dropping a suffix moves
/// its keys: the keys are assigned again until nothing changes (the set of suffixes only shrinks).
fn retained_assignment<'a>(keys: &[&'a str]) -> BTreeMap<String, Vec<&'a str>> {
    let mut suffixes: Vec<String> = build_suffix_to_bases(keys)
        .into_iter()
        .filter(|(_, bases)| bases.len() >= MIN_BASES)
        .map(|(suffix, _)| suffix)
        .collect();
    loop {
        let mut assigned = assign_keys(keys, &suffixes, GridSide::Suffix);
        assigned.retain(|_, ks| ks.len() >= MIN_BASES);
        let kept = one_grid(&assigned);
        if kept.len() == suffixes.len() {
            return assigned;
        }
        suffixes = kept;
    }
}

/// The suffixes of one grid among the assigned ones (#199): from a seed, a suffix joins when most of its bases are
/// bases of the grid so far, so that suffixes of other names (`_at` of `created`, `_id` of `user`) stay out, rare
/// ones of the same names (`_label`) join. Every suffix is tried as the seed; the grid with the most keys wins.
fn one_grid(assigned: &BTreeMap<String, Vec<&str>>) -> Vec<String> {
    let bases: Vec<(&String, HashSet<&str>)> = assigned
        .iter()
        .map(|(s, ks)| (s, ks.iter().map(|k| &k[..k.len() - s.len()]).collect()))
        .collect();
    let grid_from = |seed: usize| {
        let mut grid_bases = bases[seed].1.clone();
        let mut grid = vec![seed];
        for (at, (_, b)) in bases.iter().enumerate() {
            if at != seed && 2 * b.intersection(&grid_bases).count() > b.len() {
                grid_bases.extend(b);
                grid.push(at);
            }
        }
        grid
    };
    let keys_of = |grid: &[usize]| grid.iter().map(|&at| bases[at].1.len()).sum::<usize>();
    // `max_by_key` keeps the last of equal grids: reversed, the first seed in suffix order wins.
    let best = (0..bases.len())
        .rev()
        .map(grid_from)
        .max_by_key(|g| keys_of(g))
        .unwrap_or_default();
    let mut suffixes: Vec<String> = best.into_iter().map(|at| bases[at].0.clone()).collect();
    suffixes.sort();
    suffixes
}

/// Build `SuffixColumn` entries (in suffix order) and the base `value` column type, each typed
/// from the keys assigned to it.
fn build_suffix_cols_and_value_type(
    assigned: &BTreeMap<String, Vec<&str>>,
    bare: &[&str],
    columns: &ObservedKeys,
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

/// Widest type of the observed keys (`TEXT` when there is none). A key only ever seen as `null`
/// says nothing about the type: its tracker falls back to `TEXT` and would widen the whole column.
fn widest_type<'a>(keys: impl Iterator<Item = &'a ColumnStats>) -> PgType {
    keys.filter(|k| k.total_count > k.null_count)
        .fold(None::<PgType>, |acc, key| {
            let t = key.pg_type.clone();
            Some(match acc {
                None => t,
                Some(a) => widen_pg_types(a, &t),
            })
        })
        .unwrap_or(PgType::Text)
}

/// Column name of a suffix or a prefix: the part without its `_` (`_unit`, `min_`), as an identifier. A part that
/// starts with a digit keeps a leading `_` (`_100g`, not the opaque `c_100g`). `name` would clash
/// with the pivot's own column: `norm_name`. `value` stays: the bare keys then take `bare_value` (#199).
fn suffix_col_name(suffix: &str) -> String {
    let bare = suffix.trim_matches('_');
    let s = sanitize_identifier(bare);
    match s.as_str() {
        "name" => "norm_name".to_string(),
        _ if bare.starts_with(|c: char| c.is_ascii_digit()) => {
            // sanitize_identifier prefixed `c_` to the digit.
            format!("_{}", s.strip_prefix("c_").unwrap_or(&s))
        }
        _ => s,
    }
}

/// The `suffix_columns` (or `prefix_columns`) of a TOML config as parts: the `_` on the side of the base is added
/// when missing (`unit` → `_unit`, `min` → `min_`).
#[must_use]
pub fn declared_suffixes(suffix_list: &[String], side: GridSide) -> Vec<String> {
    suffix_list
        .iter()
        .map(|s| match side {
            GridSide::Suffix if !s.starts_with('_') => format!("_{s}"),
            GridSide::Prefix if !s.ends_with('_') => format!("{s}_"),
            _ => s.clone(),
        })
        .collect()
}

/// Used when the user declares `suffix_columns` (or `prefix_columns`, `side`) in the TOML config: the declared
/// parts (see [`declared_suffixes`]), in their order, with the keys of the grid. `columns` are
/// the table's scalar columns; keys go to their longest part, as in the detection, and only
/// the bare keys of the grid type `value` (#163).
#[must_use]
pub fn build_suffix_schema_from_list(
    suffix_list: &[String],
    side: GridSide,
    columns: &ObservedKeys,
) -> (SuffixSchema, HashSet<String>) {
    let suffixes = declared_suffixes(suffix_list, side);
    let keys: Vec<&str> = columns.keys().map(String::as_str).collect();
    let assigned = assign_keys(&keys, &suffixes, side);
    let (bare, grid) = grid_keys(&keys, &assigned, side);
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
        side,
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
    use crate::schema::type_tracker::TypeTracker;
    use indexmap::IndexMap;

    fn make_object_tracker() -> TypeTracker {
        let mut t = TypeTracker::new(256);
        t.observe(&serde_json::json!({"k": 1}));
        t
    }

    fn stat_of(key: &str, tracker: &TypeTracker) -> ColumnStats {
        ColumnStats {
            table_name: "t".to_string(),
            column_name: key.to_string(),
            original_name: key.to_string(),
            pg_type: tracker.to_pg_type(),
            total_count: tracker.total_count,
            null_count: tracker.null_count,
            type_histogram: Vec::new(),
        }
    }

    /// The scalar keys of `columns`, as Pass 1 keeps them in its statistics: no object field.
    fn entry_of(columns: IndexMap<String, TypeTracker>) -> ObservedKeys {
        columns
            .into_iter()
            .filter(|(_, t)| !t.is_object_field() && !t.is_array_field())
            .map(|(k, t)| {
                let stat = stat_of(&k, &t);
                (k, stat)
            })
            .collect()
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

    /// The suffixes of the grid detected in `cols`, in order; empty without a grid.
    fn suffixes_of(cols: IndexMap<String, TypeTracker>) -> Vec<String> {
        detect_suffix_schema(&entry_of(cols), SUFFIX_MIN_COVERAGE)
            .map_or_else(Vec::new, |(s, _)| {
                s.suffix_cols.into_iter().map(|c| c.suffix).collect()
            })
    }

    #[test]
    fn a_suffix_whose_names_are_not_those_of_the_grid_stays_out() {
        // The #199 trap: `_at` dates other names than `_id` and `_name`, which share `user`, `product`…
        let mut cols = grid(&["user", "order", "product", "store", "shop"], &["_id"]);
        cols.extend(grid(&["user", "product", "store", "shop"], &["_name"]));
        cols.extend(grid(&["created", "updated", "shipped", "paid"], &["_at"]));
        cols.extend(["status", "amount", "note"].map(|k| (k.to_string(), make_text_tracker())));
        assert_eq!(suffixes_of(cols), ["_id", "_name"]);
    }

    #[test]
    fn a_rare_suffix_of_the_names_of_the_grid_joins_it() {
        // OFF `nutriments`: `_label` has 26 names of ~100, all of them names of the grid; 30 % of the names is too much.
        let bases: Vec<String> = (0..20).map(|i| format!("n{i}")).collect();
        let bases: Vec<&str> = bases.iter().map(String::as_str).collect();
        let mut cols = grid(&bases, &["", "_100g", "_unit"]);
        cols.extend(grid(&bases[..4], &["_label"]));
        assert_eq!(suffixes_of(cols), ["_100g", "_label", "_unit"]);
    }

    #[test]
    fn two_grids_of_other_names_give_the_one_with_more_keys() {
        let mut cols = grid(&["a", "b", "c", "d"], &["_min", "_max"]);
        cols.extend(grid(&["p", "q", "r"], &["_x", "_y"]));
        assert_eq!(suffixes_of(cols), ["_max", "_min"]);
    }

    #[test]
    fn prefix_columns_take_the_start_of_the_keys() {
        // `min_temperature`…: `min_` / `max_` are the columns, the measures the rows (#199).
        let mut cols = grid(&["min_", "max_"], &["temperature", "wind", "rain"]);
        cols.insert("temperature".to_string(), make_float_tracker());
        cols.insert("note".to_string(), make_text_tracker());
        let list = ["min".to_string(), "max_".to_string()];
        let (schema, keys) =
            build_suffix_schema_from_list(&list, GridSide::Prefix, &entry_of(cols));
        assert_eq!(schema.side, GridSide::Prefix);
        let parts: Vec<(&str, &str)> = schema
            .suffix_cols
            .iter()
            .map(|c| (c.suffix.as_str(), c.col_name.as_str()))
            .collect();
        assert_eq!(parts, [("min_", "min"), ("max_", "max")]);
        let mut keys: Vec<String> = keys.into_iter().collect();
        keys.sort();
        // `temperature` is the bare key of a base: it fills `value`; `note` stays out.
        assert_eq!(
            keys,
            [
                "max_rain",
                "max_temperature",
                "max_wind",
                "min_rain",
                "min_temperature",
                "min_wind",
                "temperature"
            ]
        );
    }

    #[test]
    fn a_key_seen_only_as_null_does_not_widen_its_suffix_column() {
        let mut cols = grid(&["fat", "salt", "sugars"], &["", "_100g"]);
        let mut null_only = TypeTracker::new(256);
        null_only.observe(&serde_json::Value::Null);
        cols.insert("fiber_100g".to_string(), null_only);
        let (ss, _) = detect_suffix_schema(&entry_of(cols), 0.3).expect("a 3-base grid");
        let col = ss
            .suffix_cols
            .iter()
            .find(|c| c.suffix == "_100g")
            .expect("_100g");
        assert_eq!(col.pg_type, PgType::DoublePrecision);
    }

    #[test]
    fn suffix_columns_are_named_after_the_json_suffix() {
        // `100g` can't start an unquoted identifier: keep the `_` rather than the opaque `c_100g`.
        assert_eq!(suffix_col_name("_100g"), "_100g");
        assert_eq!(suffix_col_name("_100"), "_100");
        assert_eq!(suffix_col_name("_unit"), "unit");
        // `value` is the suffix's (#199); `name` is the pivot's own column.
        assert_eq!(suffix_col_name("_value"), "value");
        assert_eq!(suffix_col_name("_name"), "norm_name");
        // A prefix (#199).
        assert_eq!(suffix_col_name("min_"), "min");
        assert_eq!(suffix_col_name("2021_"), "_2021");
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
    fn objects_are_not_part_of_the_grid() {
        let mut cols = grid(&["a", "b", "c"], &["", "_100g"]);
        for base in ["p", "q", "r"] {
            cols.insert(format!("{base}_nutrients"), make_object_tracker());
        }
        let (schema, keys) = detect_suffix_schema(&entry_of(cols), 0.3).expect("grid");
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
    fn the_value_suffix_is_the_value_column() {
        // #199: `fat_value` fills `value`; the bare `fat` gets `bare_value` (wide_strategies), not the reverse.
        let mut cols: IndexMap<String, TypeTracker> = IndexMap::new();
        for base in ["calcium", "iron", "zinc"] {
            cols.insert(base.to_string(), make_float_tracker());
            cols.insert(format!("{base}_value"), make_float_tracker());
        }
        let (schema, _) = detect_suffix_schema(&entry_of(cols), 0.3).expect("grid");
        let col_names: Vec<&str> = schema
            .suffix_cols
            .iter()
            .map(|s| s.col_name.as_str())
            .collect();
        assert_eq!(col_names, ["value"]);
    }

    /// Cas `OpenFoodFacts` : une chaîne rare dans une colonne de suffixe ne passe plus la colonne
    /// pivot en `TEXT`.
    #[test]
    fn test_widest_type_ignores_rare_string() {
        let mut rare = TypeTracker::new(256);
        for i in 0..10_000 {
            rare.observe(&serde_json::json!(f64::from(i) + 0.5));
        }
        rare.observe(&serde_json::json!("10000000000000000000"));
        let keys = [stat_of("a", &make_float_tracker()), stat_of("b", &rare)];
        assert_eq!(widest_type(keys.iter()), PgType::DoublePrecision);
    }
}
