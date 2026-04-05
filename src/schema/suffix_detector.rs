use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;

use crate::schema::naming::sanitize_identifier;
use crate::schema::table_schema::{SuffixColumn, SuffixSchema};
use crate::schema::type_tracker::{widen_pg_types, PgType, TypeTracker};

/// Minimum number of distinct bases a suffix must appear with to be a candidate.
const MIN_BASES: usize = 2;

/// Detect suffix structure in a wide table's columns.
///
/// Algorithm:
/// 1. For every key, enumerate all possible (base, suffix) splits on `_`.
/// 2. A suffix is candidate if it appears with >= MIN_BASES distinct bases.
/// 3. Determine the base set = all strings that appear as a base in any candidate decomposition.
/// 4. Compute coverage per suffix = |bases with this suffix| / |total base set|.
/// 5. Retain suffixes with coverage >= `coverage_threshold`.
///
/// Returns `None` if no structural pattern is found above threshold.
pub fn detect_suffix_schema(
    columns: &IndexMap<String, TypeTracker>,
    coverage_threshold: f64,
    text_threshold: u32,
) -> Option<SuffixSchema> {
    let all_keys: Vec<&str> = columns.keys().map(|s| s.as_str()).collect();
    let key_set: HashSet<&str> = all_keys.iter().copied().collect();

    // Phase 1: build suffix → set_of_bases map
    // We only split at `_` positions (the natural separator).
    let mut suffix_to_bases: HashMap<String, HashSet<String>> = HashMap::new();

    for &key in &all_keys {
        // Find all `_` positions in the key
        let positions: Vec<usize> = key
            .char_indices()
            .filter(|(_, c)| *c == '_')
            .map(|(i, _)| i)
            .collect();

        for &pos in &positions {
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

    // Phase 2: filter to candidate suffixes (>= MIN_BASES distinct bases)
    let candidates: Vec<(String, HashSet<String>)> = suffix_to_bases
        .into_iter()
        .filter(|(_, bases)| bases.len() >= MIN_BASES)
        .collect();

    if candidates.is_empty() {
        return None;
    }

    // Phase 3: determine the global base set
    // A key is a base if it appears as the "stem" in any candidate decomposition.
    let base_set: HashSet<String> = candidates
        .iter()
        .flat_map(|(_, bases)| bases.iter().cloned())
        .collect();

    let total_bases = base_set.len();
    if total_bases == 0 {
        return None;
    }

    // Phase 4: compute coverage and retain suffixes above threshold
    let mut retained: Vec<(String, HashSet<String>)> = candidates
        .into_iter()
        .filter(|(_, bases)| {
            let covered = bases.iter().filter(|b| base_set.contains(*b)).count();
            covered as f64 / total_bases as f64 >= coverage_threshold
        })
        .collect();

    if retained.is_empty() {
        return None;
    }

    // Sort by suffix string for deterministic output
    retained.sort_by(|a, b| a.0.cmp(&b.0));

    // Sanity check: the decomposition must cover a meaningful fraction of all keys.
    // If the entire key set is just noise (every key is unique), skip.
    let total_accounted: usize = retained.iter().map(|(_, bases)| bases.len()).sum::<usize>()
        + base_set
            .iter()
            .filter(|b| key_set.contains(b.as_str()))
            .count();
    if total_accounted < 2 {
        return None;
    }

    // Phase 5: build SuffixColumn list with inferred types
    let mut suffix_cols = Vec::new();
    for (suffix, bases) in &retained {
        // Aggregate PgType across all keys that end with this suffix
        let pg_type = bases
            .iter()
            .filter_map(|base| {
                let key = format!("{}{}", base, suffix);
                columns.get(key.as_str())
            })
            .fold(None::<PgType>, |acc, tracker| {
                let t = tracker.to_pg_type();
                Some(match acc {
                    None => t,
                    Some(a) => widen_pg_types(a, &t),
                })
            })
            .unwrap_or(PgType::Text);

        // Column name = sanitize(suffix without leading `_`)
        let stripped = suffix.trim_start_matches('_');
        let col_name = {
            let s = sanitize_identifier(stripped);
            // Avoid collision with the base "value" column
            if s == "value" {
                "norm_value".to_string()
            } else {
                s
            }
        };

        suffix_cols.push(SuffixColumn {
            suffix: suffix.clone(),
            col_name,
            pg_type,
        });
    }

    // Compute the type for the base ("value") column
    // = widen of all TypeTrackers for bare base keys (keys that exist without any suffix)
    let value_type = base_set
        .iter()
        .filter_map(|base| columns.get(base.as_str()))
        .fold(None::<PgType>, |acc, tracker| {
            let t = tracker.to_pg_type();
            Some(match acc {
                None => t,
                Some(a) => widen_pg_types(a, &t),
            })
        })
        .unwrap_or(PgType::Text);

    Some(SuffixSchema {
        suffix_cols,
        value_type,
    })
}

/// Build a SuffixSchema from an explicit list of suffix strings.
/// Used when the user declares `suffix_columns` in the TOML config.
pub fn build_suffix_schema_from_list(
    suffix_list: &[String],
    columns: &IndexMap<String, TypeTracker>,
) -> SuffixSchema {
    let suffix_cols = suffix_list
        .iter()
        .map(|suffix| {
            // Ensure the suffix starts with `_`
            let suffix = if suffix.starts_with('_') {
                suffix.clone()
            } else {
                format!("_{}", suffix)
            };

            // Collect all keys ending with this suffix and widen their types
            let pg_type = columns
                .iter()
                .filter(|(key, _)| key.ends_with(suffix.as_str()))
                .fold(None::<PgType>, |acc, (_, tracker)| {
                    let t = tracker.to_pg_type();
                    Some(match acc {
                        None => t,
                        Some(a) => widen_pg_types(a, &t),
                    })
                })
                .unwrap_or(PgType::Text);

            let stripped = suffix.trim_start_matches('_');
            let col_name = {
                let s = sanitize_identifier(stripped);
                if s == "value" {
                    "norm_value".to_string()
                } else {
                    s
                }
            };

            SuffixColumn {
                suffix,
                col_name,
                pg_type,
            }
        })
        .collect();

    // Base value type from bare keys (keys not ending with any declared suffix)
    let suffix_strs: HashSet<&str> = suffix_list
        .iter()
        .map(|s| {
            if s.starts_with('_') {
                s.as_str()
            } else {
                s.as_str()
            }
        })
        .collect();

    let value_type = columns
        .iter()
        .filter(|(key, _)| !suffix_strs.iter().any(|s| key.ends_with(*s)))
        .fold(None::<PgType>, |acc, (_, tracker)| {
            let t = tracker.to_pg_type();
            Some(match acc {
                None => t,
                Some(a) => widen_pg_types(a, &t),
            })
        })
        .unwrap_or(PgType::Text);

    SuffixSchema {
        suffix_cols,
        value_type,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

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

        let result = detect_suffix_schema(&cols, 0.3, 256);
        assert!(result.is_some(), "should detect suffix structure");

        let schema = result.unwrap();
        let suffixes: Vec<&str> = schema.suffix_cols.iter().map(|s| s.suffix.as_str()).collect();
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

        let result = detect_suffix_schema(&cols, 0.3, 256);
        assert!(result.is_none(), "flat keys should not be detected as structured");
    }

    #[test]
    fn test_value_suffix_renamed_to_norm_value() {
        let mut cols: IndexMap<String, TypeTracker> = IndexMap::new();
        cols.insert("calcium".to_string(), make_float_tracker());
        cols.insert("iron".to_string(), make_float_tracker());
        cols.insert("calcium_value".to_string(), make_float_tracker());
        cols.insert("iron_value".to_string(), make_float_tracker());

        let result = detect_suffix_schema(&cols, 0.3, 256);
        assert!(result.is_some());
        let schema = result.unwrap();
        let col_names: Vec<&str> = schema.suffix_cols.iter().map(|s| s.col_name.as_str()).collect();
        // `_value` suffix must be renamed to avoid collision with base `value` column
        assert!(!col_names.contains(&"value"), "should not have raw 'value' col name");
        assert!(col_names.contains(&"norm_value"), "should have 'norm_value'");
    }
}
