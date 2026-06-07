//! API publique pour la fusion manuelle de siblings depuis l'IHM.
//!
//! Expose `build_sibling_collapse_from_siblings` : construit un `InferredStrategy::SiblingCollapse`
//! ou `SiblingCollapseMulti` à partir d'une sélection utilisateur de tables sœurs.

use super::super::table_schema::{SiblingGroup, SiblingSchema, TableSchema, InferredStrategy};
use super::super::wide_strategies::classify_key_shape;
use super::scoring::pg_truncate_name;


/// Error returned by [`build_sibling_collapse_from_siblings`].
#[allow(dead_code)] // used by json2sql-ui::state::apply_sibling_merge
#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error("need at least 2 tables to merge, got {0}")]
    TooFewTables(usize),
    #[error("table '{0}' has no parent — can only merge sibling tables")]
    NoParent(String),
    #[error("selected tables have different parents — can only merge siblings of the same parent")]
    DifferentParents,
    #[error("table '{0}' is a routing table (all generated columns) — cannot be merged manually")]
    RoutingTable(String),
}

/// Result of a manual sibling merge.
#[allow(dead_code)] // used by json2sql-ui::state::apply_sibling_merge
#[derive(Debug)]
pub struct MergeResult {
    /// Parent table that receives the new `InferredStrategy`.
    pub parent_name: String,
    /// `SiblingCollapse` or `SiblingCollapseMulti` to store in `strategy_overrides[parent_name]`.
    pub strategy: InferredStrategy,
    /// Sibling tables that are absorbed — caller should set
    /// `strategy_overrides[name] = InferredStrategy::Ignore` for each.
    pub absorbed_names: Vec<String>,
}

/// Validate preconditions for a manual sibling merge.
/// Returns the common parent name on success.
fn validate_merge_inputs(schemas: &[TableSchema], indices: &[usize]) -> Result<String, MergeError> {
    if indices.len() < 2 {
        return Err(MergeError::TooFewTables(indices.len()));
    }
    for &i in indices {
        let t = &schemas[i];
        if t.parent_table.is_none() {
            return Err(MergeError::NoParent(t.name.clone()));
        }
        if !t.columns.is_empty() && t.columns.iter().all(|c| c.is_generated) {
            return Err(MergeError::RoutingTable(t.name.clone()));
        }
    }
    let parent_name = schemas[indices[0]].parent_table.as_deref()
        .expect("NoParent was checked for all tables above — parent_table is Some");
    if indices.iter().any(|&i| schemas[i].parent_table.as_deref() != Some(parent_name)) {
        return Err(MergeError::DifferentParents);
    }
    Ok(parent_name.to_string())
}

/// Build a `SiblingCollapse` or `SiblingCollapseMulti` strategy from a manual user selection of
/// sibling tables. Infers key shape from table name suffixes; auto-detects whether to
/// produce a single-group (`SiblingCollapse`) or two-group (`SiblingCollapseMulti`) strategy.
#[allow(dead_code)] // used by json2sql-ui::state::apply_sibling_merge
pub fn build_sibling_collapse_from_siblings(
    schemas: &[TableSchema],
    indices: &[usize],
    key_col_name: &str,
) -> Result<MergeResult, MergeError> {
    let parent_name = validate_merge_inputs(schemas, indices)?;

    let names: Vec<&str> = indices.iter().map(|&i| schemas[i].name.as_str()).collect();
    let absorbed_names: Vec<String> = names.iter().map(std::string::ToString::to_string).collect();

    let keys = extract_key_suffixes(&names);
    let key_refs: Vec<&str> = keys.iter().map(std::string::String::as_str).collect();

    let is_numeric: Vec<bool> = keys.iter().map(|k| k.chars().all(|c| c.is_ascii_digit())).collect();
    let has_numeric = is_numeric.iter().any(|&b| b);
    let has_non_numeric = is_numeric.iter().any(|&b| !b);

    let strategy = if has_numeric && has_non_numeric {
        build_mixed_keyed_pivot_strategy(&parent_name, key_col_name, &names, &key_refs, &is_numeric)
    } else {
        InferredStrategy::SiblingCollapse(SiblingSchema {
            key_col_name: key_col_name.to_string(),
            key_shape: classify_key_shape(&key_refs),
            array_children: false,
            data_col_name: "j2s_data".to_string(),
        })
    };

    Ok(MergeResult { parent_name, strategy, absorbed_names })
}

#[allow(clippy::too_many_lines)] // symmetric two-group struct construction for SiblingCollapseMulti
fn build_mixed_keyed_pivot_strategy(
    parent_name: &str,
    key_col_name: &str,
    names: &[&str],
    key_refs: &[&str],
    is_numeric: &[bool],
) -> InferredStrategy {
    let mut numeric_names: Vec<String> = Vec::new();
    let mut non_numeric_names: Vec<String> = Vec::new();
    let mut numeric_keys: Vec<&str> = Vec::new();
    let mut non_numeric_keys: Vec<&str> = Vec::new();
    for (i, &num) in is_numeric.iter().enumerate() {
        if num { numeric_names.push(names[i].to_string()); numeric_keys.push(key_refs[i]); }
        else { non_numeric_names.push(names[i].to_string()); non_numeric_keys.push(key_refs[i]); }
    }
    InferredStrategy::SiblingCollapseMulti(vec![
        SiblingGroup {
            pivot_table: pg_truncate_name(&format!("{parent_name}_{key_col_name}_num")),
            key_is_numeric: true,
            sibling_schema: SiblingSchema {
                key_col_name: key_col_name.to_string(),
                key_shape: classify_key_shape(&numeric_keys),
                array_children: false,
                data_col_name: "j2s_data".to_string(),
            },
            absorbed_names: numeric_names,
            path_segment: format!("{key_col_name}_num"),
            absorbed_path_segments: numeric_keys.iter().map(ToString::to_string).collect(),
        },
        SiblingGroup {
            pivot_table: pg_truncate_name(&format!("{parent_name}_{key_col_name}_txt")),
            key_is_numeric: false,
            sibling_schema: SiblingSchema {
                key_col_name: key_col_name.to_string(),
                key_shape: classify_key_shape(&non_numeric_keys),
                array_children: false,
                data_col_name: "j2s_data".to_string(),
            },
            absorbed_names: non_numeric_names,
            path_segment: format!("{key_col_name}_txt"),
            absorbed_path_segments: non_numeric_keys.iter().map(ToString::to_string).collect(),
        },
    ])
}

fn extract_key_suffixes(names: &[&str]) -> Vec<String> {
    if names.is_empty() { return vec![]; }
    let bytes: Vec<&[u8]> = names.iter().map(|s| s.as_bytes()).collect();
    let min_len = bytes.iter().map(|b| b.len()).min().unwrap_or(0);
    let prefix_len = (0..min_len)
        .take_while(|&i| bytes.iter().all(|b| b[i] == bytes[0][i]))
        .count();
    let skip = if matches!(names[0].as_bytes().get(prefix_len), Some(&b'_' | &b'-')) {
        prefix_len + 1
    } else {
        prefix_len
    };
    names.iter().map(|n| n[skip..].to_string()).collect()
}


#[cfg(test)]
mod tests {
    use super::*;
    use super::super::super::table_schema::{ColumnSchema, KeyShape, TableSchema, InferredStrategy};
    use super::super::super::type_tracker::PgType;

    fn make_parent(name: &str) -> TableSchema {
        TableSchema::new(name.to_string(), vec![name.to_string()], 0)
    }

    fn make_sibling(name: &str, parent: &str, data_keys: &[&str]) -> TableSchema {
        let mut t = TableSchema::new(name.to_string(), vec![name.to_string()], 1);
        t.parent_table = Some(parent.to_string());
        t.columns.push(ColumnSchema {
            name: "j2s_id".to_string(), original_name: "j2s_id".to_string(),
            pg_type: PgType::BigInt, not_null: true, is_generated: true, is_parent_fk: false,
        });
        t.columns.push(ColumnSchema {
            name: "j2s_parent_id".to_string(), original_name: "j2s_parent_id".to_string(),
            pg_type: PgType::BigInt, not_null: true, is_generated: true, is_parent_fk: true,
        });
        for &k in data_keys {
            t.columns.push(ColumnSchema {
                name: k.to_string(), original_name: k.to_string(),
                pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
            });
        }
        t
    }

    #[test]
    fn test_merge_slug_siblings_keyed_pivot() {
        let schemas = vec![
            make_parent("products_images"),
            make_sibling("products_images_front", "products_images", &["url", "width"]),
            make_sibling("products_images_back",  "products_images", &["url", "width"]),
        ];
        let r = build_sibling_collapse_from_siblings(&schemas, &[1, 2], "img_key").unwrap();
        assert_eq!(r.parent_name, "products_images");
        assert!(matches!(r.strategy, InferredStrategy::SiblingCollapse(_)));
        let mut absorbed = r.absorbed_names.clone();
        absorbed.sort();
        assert_eq!(absorbed, vec!["products_images_back", "products_images_front"]);
    }

    #[test]
    fn test_merge_numeric_siblings_keyed_pivot() {
        let schemas = vec![
            make_parent("p"),
            make_sibling("p_1", "p", &["val"]),
            make_sibling("p_2", "p", &["val"]),
            make_sibling("p_3", "p", &["val"]),
        ];
        let r = build_sibling_collapse_from_siblings(&schemas, &[1, 2, 3], "key").unwrap();
        assert_eq!(r.parent_name, "p");
        if let InferredStrategy::SiblingCollapse(ss) = &r.strategy {
            assert_eq!(ss.key_shape, KeyShape::Numeric);
            assert_eq!(ss.key_col_name, "key");
        } else {
            panic!("expected SiblingCollapse, got {:?}", r.strategy);
        }
    }

    #[test]
    fn test_merge_mixed_siblings_multikeyed_pivot() {
        let schemas = vec![
            make_parent("img"),
            make_sibling("img_1",     "img", &["url"]),
            make_sibling("img_2",     "img", &["url"]),
            make_sibling("img_front", "img", &["url"]),
        ];
        let r = build_sibling_collapse_from_siblings(&schemas, &[1, 2, 3], "key").unwrap();
        assert_eq!(r.parent_name, "img");
        if let InferredStrategy::SiblingCollapseMulti(groups) = &r.strategy {
            assert_eq!(groups.len(), 2);
            let num = groups.iter().find(|g| g.key_is_numeric).unwrap();
            let mut num_absorbed = num.absorbed_names.clone();
            num_absorbed.sort();
            assert_eq!(num_absorbed, vec!["img_1", "img_2"]);
            let txt = groups.iter().find(|g| !g.key_is_numeric).unwrap();
            assert_eq!(txt.absorbed_names, vec!["img_front"]);
        } else {
            panic!("expected SiblingCollapseMulti, got {:?}", r.strategy);
        }
    }

    #[test]
    fn test_merge_error_too_few_tables() {
        let schemas = vec![
            make_parent("p"),
            make_sibling("p_1", "p", &["val"]),
        ];
        let err = build_sibling_collapse_from_siblings(&schemas, &[1], "key").unwrap_err();
        assert!(matches!(err, MergeError::TooFewTables(_)));
    }

    #[test]
    fn test_merge_error_different_parents() {
        let schemas = vec![
            make_sibling("a_1", "a", &["val"]),
            make_sibling("b_1", "b", &["val"]),
        ];
        let err = build_sibling_collapse_from_siblings(&schemas, &[0, 1], "key").unwrap_err();
        assert!(matches!(err, MergeError::DifferentParents));
    }

    #[test]
    fn test_merge_error_no_parent() {
        let t1 = TableSchema::new("a".to_string(), vec!["a".to_string()], 0);
        let t2 = TableSchema::new("b".to_string(), vec!["b".to_string()], 0);
        let schemas = vec![t1, t2];
        let err = build_sibling_collapse_from_siblings(&schemas, &[0, 1], "key").unwrap_err();
        assert!(matches!(err, MergeError::NoParent(_)));
    }

    #[test]
    fn test_merge_error_routing_table() {
        let mut routing = TableSchema::new("p_r".to_string(), vec!["p_r".to_string()], 1);
        routing.parent_table = Some("p".to_string());
        routing.columns.push(ColumnSchema {
            name: "j2s_id".to_string(), original_name: "j2s_id".to_string(),
            pg_type: PgType::BigInt, not_null: true, is_generated: true, is_parent_fk: false,
        });
        let sibling = make_sibling("p_s", "p", &["val"]);
        let schemas = vec![routing, sibling];
        let err = build_sibling_collapse_from_siblings(&schemas, &[0, 1], "key").unwrap_err();
        assert!(matches!(err, MergeError::RoutingTable(_)));
    }

    #[test]
    fn test_merge_absorbed_names_complete() {
        let schemas = vec![
            make_parent("x"),
            make_sibling("x_a", "x", &["v"]),
            make_sibling("x_b", "x", &["v"]),
            make_sibling("x_c", "x", &["v"]),
        ];
        let r = build_sibling_collapse_from_siblings(&schemas, &[1, 2, 3], "key").unwrap();
        let mut absorbed = r.absorbed_names.clone();
        absorbed.sort();
        assert_eq!(absorbed, vec!["x_a", "x_b", "x_c"]);
    }
}
