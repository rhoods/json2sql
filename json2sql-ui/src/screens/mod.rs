pub mod setup;
pub mod analysis;
pub mod strategy;
pub mod preview;
pub mod import;
pub mod table_list;
pub mod strategy_configurator;

pub use table_list::{TableListPanel, TableRowEntry};

use std::collections::HashMap;
use json2sql::schema::table_schema::{TableSchema, WideStrategy};
use crate::theme;

// ---------------------------------------------------------------------------
// Shared file picker helpers (rfd — native OS dialog, cross-platform)
// ---------------------------------------------------------------------------

/// Result of a file picker invocation.
pub enum PickResult {
    Selected(std::path::PathBuf),
    Cancelled,
    /// Never returned by rfd (compiled-in library); kept for exhaustive matches.
    NotAvailable,
}

fn option_to_pick_result(opt: Option<std::path::PathBuf>) -> PickResult {
    match opt {
        Some(path) => PickResult::Selected(path),
        None => PickResult::Cancelled,
    }
}

/// Parse a glob pattern string like `"*.json *.jsonl"` into rfd extension list `["json", "jsonl"]`.
fn parse_ext(pattern: &str) -> Vec<String> {
    pattern
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .map(|s| s.trim_start_matches("*.").to_string())
        .collect()
}

pub async fn pick_file(filters: &[(&str, &str)]) -> PickResult {
    let mut dialog = rfd::AsyncFileDialog::new().set_title("Select file");
    for (name, pattern) in filters {
        let exts = parse_ext(pattern);
        dialog = dialog.add_filter(*name, &exts);
    }
    let handle = dialog.pick_file().await;
    option_to_pick_result(handle.map(|h| h.path().to_path_buf()))
}

pub async fn pick_folder() -> PickResult {
    let handle = rfd::AsyncFileDialog::new()
        .set_title("Select folder")
        .pick_folder()
        .await;
    option_to_pick_result(handle.map(|h| h.path().to_path_buf()))
}

pub async fn pick_save_file(default_name: &str) -> PickResult {
    let handle = rfd::AsyncFileDialog::new()
        .set_title("Save schema as")
        .set_file_name(default_name)
        .add_filter("JSON", &["json"])
        .save_file()
        .await;
    option_to_pick_result(handle.map(|h| h.path().to_path_buf()))
}

pub fn strategy_label(s: &WideStrategy) -> &'static str {
    match s {
        WideStrategy::Columns                     => "DEFAULT",
        WideStrategy::Pivot                       => "PIVOT",
        WideStrategy::Jsonb                       => "JSONB SÉP.",
        WideStrategy::JsonbFlatten                => "JSONB INLINE",
        WideStrategy::StructuredPivot(_)          => "STRUCT PIVOT",
        WideStrategy::KeyedPivot(_)               => "KEYED PIVOT",
        WideStrategy::MultiKeyedPivot(_)          => "MULTI PIVOT",
        WideStrategy::AutoSplit { .. }            => "AUTO SPLIT",
        WideStrategy::Ignore                      => "SKIP",
        WideStrategy::NormalizeDynamicKeys { .. } => "NORMALIZE",
        WideStrategy::Flatten { .. }              => "FLATTEN",
    }
}

/// For each table in `schemas`, return true if no later table shares the same parent.
/// Used to pick tree connectors: └─ (last child) vs ├─ (has siblings below).
pub fn compute_last_child(schemas: &[json2sql::schema::table_schema::TableSchema]) -> Vec<bool> {
    let mut result = vec![true; schemas.len()];
    for i in 0..schemas.len() {
        if let Some(ref parent) = schemas[i].parent_table {
            for j in (i + 1)..schemas.len() {
                if schemas[j].parent_table.as_deref() == Some(parent.as_str()) {
                    result[i] = false;
                    break;
                }
            }
        }
    }
    result
}

pub fn strategy_color(s: &WideStrategy) -> &'static str {
    match s {
        WideStrategy::Columns                     => theme::BADGE_DEFAULT,
        WideStrategy::Pivot                       => theme::BADGE_NORMALIZE,
        WideStrategy::Jsonb                       => theme::BADGE_JSONB,
        WideStrategy::JsonbFlatten                => theme::BADGE_JSONB_INLINE,
        WideStrategy::StructuredPivot(_)          => theme::BADGE_FLATTEN,
        WideStrategy::KeyedPivot(_)               => theme::BADGE_FLATTEN,
        WideStrategy::MultiKeyedPivot(_)          => theme::BADGE_FLATTEN,
        WideStrategy::AutoSplit { .. }            => theme::BADGE_NORMALIZE,
        WideStrategy::Ignore                      => theme::BADGE_SKIP,
        WideStrategy::NormalizeDynamicKeys { .. } => theme::BADGE_NORMALIZE,
        WideStrategy::Flatten { .. }              => theme::BADGE_FLATTEN,
    }
}

/// Apply all user strategy overrides to a copy of `schemas`.
///
/// The original slice is never mutated — returns a fresh Vec ready for DDL
/// generation and pass2 import.  Three passes are needed because some strategies
/// (NormalizeDynamicKeys, JsonbFlatten) require the full schema slice and remove
/// child tables, so they must run after the single-table pass.
pub fn build_effective_schemas(
    schemas: &[TableSchema],
    strategy_overrides: &HashMap<String, WideStrategy>,
) -> Vec<TableSchema> {
    use json2sql::schema::wide_strategies::{
        apply_jsonb_flatten, apply_normalize_dynamic_keys, apply_wide_strategy_columns,
    };

    let mut result = schemas.to_vec();

    // Pass 1: single-table overrides (column layout change only).
    for (table_name, strategy) in strategy_overrides {
        if let Some(s) = result.iter_mut().find(|s| &s.name == table_name) {
            match strategy {
                WideStrategy::NormalizeDynamicKeys { .. } | WideStrategy::JsonbFlatten => {}
                other => apply_wide_strategy_columns(s, other.clone()),
            }
        }
    }

    // Pass 2: NormalizeDynamicKeys (removes child tables — needs full slice).
    let normalize_targets: Vec<(String, String)> = strategy_overrides
        .iter()
        .filter_map(|(name, strategy)| {
            if let WideStrategy::NormalizeDynamicKeys { id_column } = strategy {
                Some((name.clone(), id_column.clone()))
            } else {
                None
            }
        })
        .collect();
    for (table_name, id_column) in normalize_targets {
        if let Err(e) = apply_normalize_dynamic_keys(&mut result, &table_name, id_column) {
            eprintln!("WARNING: {e}");
        }
    }

    // Pass 3: JsonbFlatten (removes child table — needs full slice).
    let jsonb_flatten_targets: Vec<String> = strategy_overrides
        .iter()
        .filter_map(|(name, strategy)| {
            if matches!(strategy, WideStrategy::JsonbFlatten) {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect();
    for table_name in jsonb_flatten_targets {
        if let Err(e) = apply_jsonb_flatten(&mut result, &table_name) {
            eprintln!("WARNING: {e}");
        }
    }

    // Remove tables explicitly skipped by the user.
    result.retain(|s| !matches!(strategy_overrides.get(&s.name), Some(WideStrategy::Ignore)));

    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use json2sql::schema::table_schema::TableSchema;

    // --- picker helpers ---

    #[test]
    fn none_maps_to_cancelled() {
        assert!(matches!(option_to_pick_result(None), PickResult::Cancelled));
    }

    #[test]
    fn some_path_maps_to_selected() {
        let path = PathBuf::from("/tmp/test.json");
        assert!(matches!(
            option_to_pick_result(Some(path.clone())),
            PickResult::Selected(p) if p == path
        ));
    }

    #[test]
    fn parse_ext_single() {
        assert_eq!(parse_ext("*.json"), vec!["json"]);
    }

    #[test]
    fn parse_ext_multiple() {
        assert_eq!(parse_ext("*.json *.jsonl *.ndjson"), vec!["json", "jsonl", "ndjson"]);
    }

    #[test]
    fn parse_ext_no_star_prefix() {
        assert_eq!(parse_ext("json"), vec!["json"]);
    }

    #[test]
    fn parse_ext_empty_string() {
        let result = parse_ext("");
        assert!(result.is_empty());
    }

    fn make_table(name: &str, parent: Option<&str>) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        t.parent_table = parent.map(|s| s.to_string());
        t
    }

    #[test]
    fn no_children_all_true() {
        let schemas = vec![make_table("a", None), make_table("b", None)];
        assert_eq!(compute_last_child(&schemas), vec![true, true]);
    }

    #[test]
    fn single_child_is_last() {
        let schemas = vec![make_table("parent", None), make_table("child", Some("parent"))];
        assert_eq!(compute_last_child(&schemas), vec![true, true]);
    }

    #[test]
    fn two_children_first_is_not_last() {
        let schemas = vec![
            make_table("parent", None),
            make_table("child1", Some("parent")),
            make_table("child2", Some("parent")),
        ];
        assert_eq!(compute_last_child(&schemas), vec![true, false, true]);
    }

    #[test]
    fn three_children_only_last_is_true() {
        let schemas = vec![
            make_table("root", None),
            make_table("c1", Some("root")),
            make_table("c2", Some("root")),
            make_table("c3", Some("root")),
        ];
        assert_eq!(compute_last_child(&schemas), vec![true, false, false, true]);
    }

    #[test]
    fn empty_slice_returns_empty() {
        let schemas: Vec<TableSchema> = vec![];
        assert_eq!(compute_last_child(&schemas), Vec::<bool>::new());
    }

    // --- build_effective_schemas ---

    #[test]
    fn ignore_removes_table() {
        let schemas = vec![make_table("a", None), make_table("b", None)];
        let mut overrides = HashMap::new();
        overrides.insert("a".to_string(), WideStrategy::Ignore);
        let result = build_effective_schemas(&schemas, &overrides);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "b");
    }

    #[test]
    fn no_overrides_returns_full_clone() {
        let schemas = vec![make_table("x", None), make_table("y", None)];
        let result = build_effective_schemas(&schemas, &HashMap::new());
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].name, "x");
    }

    #[test]
    fn jsonb_override_applied() {
        use json2sql::schema::table_schema::ColumnSchema;
        use json2sql::schema::type_tracker::PgType;

        let mut t = make_table("wide", None);
        // Add a data column so apply_wide_strategy_columns has something to restructure.
        t.columns.push(ColumnSchema {
            name: "val".to_string(),
            original_name: "val".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        let schemas = vec![t];
        let mut overrides = HashMap::new();
        overrides.insert("wide".to_string(), WideStrategy::Jsonb);
        let result = build_effective_schemas(&schemas, &overrides);
        assert!(matches!(result[0].wide_strategy, WideStrategy::Jsonb));
    }

    #[test]
    fn original_schemas_not_mutated() {
        let schemas = vec![make_table("a", None), make_table("b", None)];
        let original_len = schemas.len();
        let mut overrides = HashMap::new();
        overrides.insert("a".to_string(), WideStrategy::Ignore);
        let _ = build_effective_schemas(&schemas, &overrides);
        assert_eq!(schemas.len(), original_len, "original slice must not be mutated");
    }
}
