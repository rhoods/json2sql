use serde_json::Value;

use super::finalizer::SchemaFinalizer;
use super::naming::{ColumnCollision, NamingRegistry, TruncatedName};
use super::observer::SchemaObserver;
use super::reporter;
use super::stats::ColumnStats;
use super::type_tracker::TypeTracker;

/// Façade: ties together SchemaObserver (observation) and SchemaFinalizer (finalization).
/// Keeps the existing public API intact for callers that use SchemaRegistry directly.
pub struct SchemaRegistry {
    observer: SchemaObserver,
    naming: NamingRegistry,
    /// Tables with more data columns than this threshold get a WideStrategy applied automatically.
    wide_column_threshold: usize,
    /// Minimum number of sibling child tables to trigger KeyedPivot merging.
    sibling_threshold: usize,
    /// Minimum Jaccard similarity required between sibling table column sets.
    sibling_jaccard: f64,
    /// Fraction of rows a key must appear in to be considered "stable" (stays in main table).
    stable_threshold: f64,
    /// Fraction of rows below which a key is ignored entirely (P5 Ignore).
    rare_threshold: f64,
    /// Column name collisions detected during finalize() (populated after finalize() is called).
    column_collisions: Vec<ColumnCollision>,
}

impl SchemaRegistry {
    pub fn new(
        text_threshold: u32,
        array_as_pg_array: bool,
        wide_column_threshold: usize,
        sibling_threshold: usize,
        sibling_jaccard: f64,
        stable_threshold: f64,
        rare_threshold: f64,
    ) -> Self {
        Self {
            observer: SchemaObserver::new(text_threshold, array_as_pg_array),
            naming: NamingRegistry::new(),
            wide_column_threshold,
            sibling_threshold,
            sibling_jaccard,
            stable_threshold,
            rare_threshold,
            column_collisions: Vec::new(),
        }
    }

    pub fn observe_root(&mut self, root_name: &str, obj: &serde_json::Map<String, Value>) {
        self.observer.observe_root(root_name, obj);
    }

    /// Convert all accumulated observations into finalized `TableSchema` objects,
    /// sorted topologically (parents before children).
    pub fn finalize(&mut self) -> Vec<crate::schema::table_schema::TableSchema> {
        let finalizer = SchemaFinalizer::new(
            self.wide_column_threshold,
            self.sibling_threshold,
            self.sibling_jaccard,
            self.stable_threshold,
            self.rare_threshold,
        );
        let (schemas, collisions) = finalizer.run(
            &self.observer.tables,
            self.observer.text_threshold,
            &mut self.naming,
        );
        self.column_collisions = collisions;
        schemas
    }

    /// Collect type distribution statistics for every data column (excluding j2s_ generated columns).
    /// Call after `finalize()` — uses the same naming registry for consistent table/column names.
    pub fn collect_stats(&mut self) -> Vec<ColumnStats> {
        reporter::collect_stats(&self.observer, &mut self.naming)
    }

    pub fn truncated_names(&self) -> &[TruncatedName] {
        self.naming.truncated_names()
    }

    pub fn column_collisions(&self) -> &[ColumnCollision] {
        &self.column_collisions
    }

    /// Merge all observations from `other` into `self`.
    /// Used after parallel Pass 1: each worker builds its own registry, then
    /// they are all merged into one before calling `finalize()`.
    /// The `NamingRegistry` is NOT merged — it is recomputed by `finalize()`.
    pub fn merge(&mut self, other: SchemaRegistry) {
        self.observer.merge(other.observer);
    }

    pub fn anomaly_iter(&self) -> impl Iterator<Item = (&str, &str, &TypeTracker)> {
        reporter::anomaly_iter(&self.observer)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::cascading::{child_compatibility_score, pairwise_jaccard_min};
    use crate::schema::finalizer::*;
    use crate::schema::table_schema::{ChildKind, ColumnSchema, TableSchema, WideStrategy};
    use crate::schema::type_tracker::PgType;
    use crate::schema::wide_strategies::{apply_flatten, apply_jsonb_flatten, apply_normalize_dynamic_keys};
    use serde_json::json;

    fn make_root(value: &Value) -> &serde_json::Map<String, Value> {
        value.as_object().unwrap()
    }

    #[test]
    fn test_flat_object() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        let obj = json!({"name": "Alice", "age": 30});
        reg.observe_root("users", make_root(&obj));
        let schemas = reg.finalize();
        assert_eq!(schemas.len(), 1);
        let s = &schemas[0];
        assert_eq!(s.name, "users");
        assert!(s.find_by_original("name").is_some());
        assert!(s.find_by_original("age").is_some());
    }

    #[test]
    fn test_nested_object_creates_child_table() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        let obj = json!({"name": "Alice", "address": {"city": "Paris"}});
        reg.observe_root("users", make_root(&obj));
        let schemas = reg.finalize();
        assert_eq!(schemas.len(), 2);
        let names: Vec<&str> = schemas.iter().map(|s| s.name.as_str()).collect();
        assert!(names.contains(&"users"));
        assert!(names.contains(&"users_address"));
    }

    #[test]
    fn test_scalar_array_creates_junction_table() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        let obj = json!({"id": 1, "tags": ["rust", "sql"]});
        reg.observe_root("users", make_root(&obj));
        let schemas = reg.finalize();
        assert_eq!(schemas.len(), 2);
        let junction = schemas.iter().find(|s| s.name == "users_tags").unwrap();
        assert!(junction.is_junction());
        assert!(junction.find_by_original("value").is_some());
    }

    #[test]
    fn test_array_of_objects() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        let obj = json!({"id": 1, "orders": [{"amount": 100}, {"amount": 200}]});
        reg.observe_root("users", make_root(&obj));
        let schemas = reg.finalize();
        assert_eq!(schemas.len(), 2);
        let orders = schemas.iter().find(|s| s.name == "users_orders").unwrap();
        assert_eq!(orders.parent_table, Some("users".to_string()));
        assert!(orders.has_order_column());
    }

    #[test]
    fn test_topological_order() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        let obj = json!({"a": {"b": {"c": 1}}});
        reg.observe_root("root", make_root(&obj));
        let schemas = reg.finalize();
        // root must come before root_a, root_a before root_a_b
        let pos = |name: &str| schemas.iter().position(|s| s.name == name).unwrap();
        assert!(pos("root") < pos("root_a"));
        assert!(pos("root_a") < pos("root_a_b"));
    }

    #[test]
    fn test_wide_object_pivot_homogeneous() {
        // 3 numeric keys → threshold=2 → should get WideStrategy::Pivot
        let mut reg = SchemaRegistry::new(256, false, 2, 3, 0.5, 0.10, 0.001);
        let obj = json!({
            "id": 1,
            "nutrients": {
                "vitamin_c": 10.5,
                "iron": 2.3,
                "calcium": 50.0
            }
        });
        reg.observe_root("ingredient", make_root(&obj));
        let schemas = reg.finalize();

        // Child tables of pivot table should be removed → only ingredient + ingredient_nutrients
        let nutrients = schemas.iter().find(|s| s.name == "ingredient_nutrients");
        assert!(nutrients.is_some(), "nutrients table should exist");
        let n = nutrients.unwrap();
        assert_eq!(n.wide_strategy, WideStrategy::Pivot);
        // Should have j2s_id, j2s_parent_id, key, value
        assert!(n.find_by_original("key").is_some());
        assert!(n.find_by_original("value").is_some());
        assert_eq!(n.data_columns().count(), 2);
    }

    #[test]
    fn test_wide_object_jsonb_heterogeneous() {
        // Mixed types (string + numeric) → should get WideStrategy::Jsonb
        let mut reg = SchemaRegistry::new(256, false, 2, 3, 0.5, 0.10, 0.001);
        let obj = json!({
            "id": 1,
            "meta": {
                "color": "red",
                "weight": 1.5,
                "active": true
            }
        });
        reg.observe_root("item", make_root(&obj));
        let schemas = reg.finalize();

        let meta = schemas.iter().find(|s| s.name == "item_meta");
        assert!(meta.is_some(), "meta table should exist");
        let m = meta.unwrap();
        assert_eq!(m.wide_strategy, WideStrategy::Jsonb);
        // Should have j2s_id, j2s_parent_id, data
        assert!(m.find_by_original("data").is_some());
        assert_eq!(m.data_columns().count(), 1);
    }

    #[test]
    fn test_wide_children_excluded() {
        // Sub-tables of a pivot table must be filtered out
        let mut reg = SchemaRegistry::new(256, false, 2, 3, 0.5, 0.10, 0.001);
        // nutrients has 3 numeric keys → pivot
        // each nutrient value is a nested object → would create child tables, but should be dropped
        let obj = json!({
            "id": 1,
            "nutrients": {
                "vitamin_c": 10.5,
                "iron": 2.3,
                "calcium": 50.0
            }
        });
        reg.observe_root("ingredient", make_root(&obj));
        let schemas = reg.finalize();

        // No table should have ingredient_nutrients as parent
        let orphans: Vec<_> = schemas
            .iter()
            .filter(|s| s.parent_table.as_deref() == Some("ingredient_nutrients"))
            .collect();
        assert!(orphans.is_empty(), "no orphan children of pivot table");
    }

    // -----------------------------------------------------------------------
    // pairwise_jaccard_min performance + correctness
    // -----------------------------------------------------------------------

    /// 10 000 pure-container siblings (no data columns) must short-circuit immediately.
    /// With O(N²) pairwise this would be ~50M iterations (~2s in debug) — must finish <500ms.
    #[test]
    fn test_jaccard_large_pure_containers_fast() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.0, 0.10, 0.001);

        // Build a single JSON root where "genomes" contains 10 000 pure-container children.
        // Each genome child has one contig sub-object (making the genome a pure container).
        let mut genomes = serde_json::Map::new();
        for i in 0..10_000usize {
            let mut genome = serde_json::Map::new();
            genome.insert(
                format!("nc_{:05}", i),
                json!({ "is_circular": false }),
            );
            genomes.insert(format!("gcf_{:05}", i), Value::Object(genome));
        }
        let root = json!({ "id": 1, "genomes": Value::Object(genomes) });

        reg.observe_root("root", make_root(&root));

        let start = std::time::Instant::now();
        let schemas = reg.finalize();
        let elapsed = start.elapsed();

        // O(N²) on 10k items ≈ 2 000ms in debug / 200ms release. O(N log N) is ≈ 800ms debug / 30ms release.
        // Threshold is generous for debug builds (unoptimized code is ~10–20× slower).
        let limit_ms: u128 = if cfg!(debug_assertions) { 2000 } else { 500 };
        assert!(
            elapsed.as_millis() < limit_ms,
            "finalize() with 10k pure-container siblings took {}ms — likely O(N²)",
            elapsed.as_millis()
        );

        // The genomes table must have become a KeyedPivot
        let genomes_schema = schemas.iter().find(|s| s.name == "root_genomes");
        assert!(genomes_schema.is_some(), "root_genomes table must exist");
        assert!(
            matches!(
                genomes_schema.unwrap().wide_strategy,
                WideStrategy::KeyedPivot(_)
            ),
            "root_genomes must be KeyedPivot"
        );
    }

    /// 500 homogeneous siblings (identical schemas) → all similar → collapsed into KeyedPivot.
    #[test]
    fn test_jaccard_large_homogeneous_collapses() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.0, 0.10, 0.001);

        let mut langs = serde_json::Map::new();
        for i in 0..500usize {
            langs.insert(
                format!("lang_{:03}", i),
                json!({ "name": "foo", "value": 42 }),
            );
        }
        let root = json!({ "id": 1, "translations": Value::Object(langs) });
        reg.observe_root("root", make_root(&root));

        let schemas = reg.finalize();
        let translations = schemas.iter().find(|s| s.name == "root_translations");
        assert!(
            matches!(
                translations.unwrap().wide_strategy,
                WideStrategy::KeyedPivot(_)
            ),
            "500 identical siblings must collapse into KeyedPivot"
        );
    }

    /// Large group where one sibling has a completely different schema → must NOT collapse.
    #[test]
    fn test_jaccard_outlier_in_large_group_rejected() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);

        let mut items = serde_json::Map::new();
        // 299 siblings with {a, b, c}
        for i in 0..299usize {
            items.insert(format!("item_{:03}", i), json!({ "a": 1, "b": 2, "c": 3 }));
        }
        // 1 outlier with completely different columns {x, y, z}
        items.insert("item_outlier".to_string(), json!({ "x": 10, "y": 20, "z": 30 }));

        let root = json!({ "id": 1, "items": Value::Object(items) });
        reg.observe_root("root", make_root(&root));

        let schemas = reg.finalize();
        let items_schema = schemas.iter().find(|s| s.name == "root_items");
        assert!(
            items_schema.is_some(),
            "root_items table must exist"
        );
        assert!(
            !matches!(
                items_schema.unwrap().wide_strategy,
                WideStrategy::KeyedPivot(_)
            ),
            "group with outlier (0 column overlap) must not collapse into KeyedPivot"
        );
    }

    /// Pure-container check must short-circuit BEFORE HashSet construction.
    /// Verified via direct call to pairwise_jaccard_min with schemas that have
    /// only generated columns (data_columns() yields nothing).
    #[test]
    fn test_jaccard_pure_containers_early_exit() {
        use crate::schema::table_schema::ColumnSchema;

        // All siblings are pure containers → must return 1.0.
        let schemas: Vec<TableSchema> = (0..5)
            .map(|i| {
                let mut s = TableSchema::new(
                    format!("s{}", i),
                    vec![format!("s{}", i)],
                    1,
                );
                s.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
                s
            })
            .collect();
        let indices: Vec<usize> = (0..5).collect();
        assert_eq!(
            pairwise_jaccard_min(&schemas, &indices),
            1.0,
            "all pure-container siblings must return 1.0"
        );

        // Mixed: 4 pure containers + 1 with a real data column → must NOT return 1.0
        // (Jaccard between the data sibling and each pure container = 0).
        let mut schemas_mixed = schemas.clone();
        schemas_mixed[2].columns.push(ColumnSchema {
            name: "val".to_string(),
            original_name: "val".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        assert_eq!(
            pairwise_jaccard_min(&schemas_mixed, &indices),
            0.0,
            "one data sibling among pure containers must give Jaccard 0.0"
        );
    }

    /// j2s_data must be marked is_generated so it does not appear in data_columns().
    /// If is_generated is false, data_columns() leaks j2s_data into type overrides,
    /// stats collection, and Jaccard comparisons on already-finalized schemas.
    #[test]
    fn test_keyed_pivot_j2s_data_is_generated() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.0, 0.10, 0.001);

        let mut langs = serde_json::Map::new();
        for i in 0..5usize {
            langs.insert(format!("lang_{}", i), json!({ "name": "foo", "value": 42 }));
        }
        let root = json!({ "id": 1, "translations": Value::Object(langs) });
        reg.observe_root("root", make_root(&root));

        let schemas = reg.finalize();
        let translations = schemas.iter().find(|s| s.name == "root_translations").unwrap();

        assert!(
            matches!(translations.wide_strategy, WideStrategy::KeyedPivot(_)),
            "expected KeyedPivot strategy"
        );

        let data_col_names: Vec<&str> =
            translations.data_columns().map(|c| c.name.as_str()).collect();
        assert!(
            !data_col_names.contains(&"j2s_data"),
            "j2s_data must not appear in data_columns() — got: {:?}",
            data_col_names
        );
    }

    /// A JSON field name containing '.' must produce a child table at depth 1, not depth 2.
    /// Without normalization, "root.v1.0" splits into path ["root","v1","0"] → depth 2,
    /// breaking topological sort and Pass 2 flush order.
    #[test]
    fn test_dotted_field_name_correct_depth() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        // "v1.0" is a direct child of root — should produce depth 1
        let obj = json!({ "v1.0": { "count": 42 } });
        reg.observe_root("root", make_root(&obj));
        let schemas = reg.finalize();

        let child = schemas.iter().find(|s| s.name.contains("v1_0")).unwrap_or_else(|| {
            panic!("table with v1_0 not found — got: {:?}", schemas.iter().map(|s| &s.name).collect::<Vec<_>>())
        });
        assert_eq!(
            child.depth, 1,
            "direct child with dotted name must be at depth 1, got depth {}", child.depth
        );
        assert_eq!(
            child.parent_table.as_deref(), Some("root"),
            "parent must be root, got {:?}", child.parent_table
        );
    }

    /// ObjectArray field with '.' in name must also produce correct depth.
    #[test]
    fn test_dotted_field_name_array_correct_depth() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        let obj = json!({ "v1.0": [{"x": 1}, {"x": 2}] });
        reg.observe_root("root", make_root(&obj));
        let schemas = reg.finalize();

        let child = schemas.iter().find(|s| s.name.contains("v1_0")).unwrap_or_else(|| {
            panic!("table with v1_0 not found — got: {:?}", schemas.iter().map(|s| &s.name).collect::<Vec<_>>())
        });
        assert_eq!(
            child.depth, 1,
            "direct array child with dotted name must be at depth 1, got depth {}", child.depth
        );
    }

    /// Parity: observing rows split across two registries then merging must equal
    /// observing all rows on a single registry — for a flat table.
    #[test]
    fn test_registry_merge_parity_flat() {
        let rows = vec![
            json!({"name": "Alice", "age": 30}),
            json!({"name": "Bob",   "age": 25}),
            json!({"name": "Carol", "age": 40, "email": "carol@example.com"}),
            json!({"name": "Dave",  "age": null}),
        ];

        // Single registry
        let mut single = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        for row in &rows { single.observe_root("users", make_root(row)); }
        let schemas_single = single.finalize();

        // Split across two registries, then merge
        let mut reg_a = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        let mut reg_b = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        for row in &rows[..2] { reg_a.observe_root("users", make_root(row)); }
        for row in &rows[2..] { reg_b.observe_root("users", make_root(row)); }
        reg_a.merge(reg_b);
        let schemas_merged = reg_a.finalize();

        assert_eq!(schemas_single.len(), schemas_merged.len(), "table count must match");
        let s = schemas_single.iter().find(|s| s.name == "users").unwrap();
        let m = schemas_merged.iter().find(|s| s.name == "users").unwrap();
        assert_eq!(s.columns.len(), m.columns.len(), "column count must match");
        for col in &s.columns {
            let mc = m.columns.iter().find(|c| c.name == col.name)
                .unwrap_or_else(|| panic!("column {} missing from merged schema", col.name));
            assert_eq!(col.pg_type, mc.pg_type, "pg_type mismatch for {}", col.name);
            assert_eq!(col.not_null, mc.not_null, "not_null mismatch for {}", col.name);
        }
    }

    /// Parity: multi-table schema (nested object) split across two registries.
    #[test]
    fn test_registry_merge_parity_nested() {
        let rows = vec![
            json!({"id": 1, "address": {"city": "Paris", "zip": "75001"}}),
            json!({"id": 2, "address": {"city": "Lyon"}}),
            json!({"id": 3, "address": {"city": "Nice", "zip": "06000", "country": "FR"}}),
        ];

        let mut single = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        for row in &rows { single.observe_root("users", make_root(row)); }
        let schemas_single = single.finalize();

        let mut reg_a = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        let mut reg_b = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        for row in &rows[..2] { reg_a.observe_root("users", make_root(row)); }
        for row in &rows[2..] { reg_b.observe_root("users", make_root(row)); }
        reg_a.merge(reg_b);
        let schemas_merged = reg_a.finalize();

        assert_eq!(
            schemas_single.len(), schemas_merged.len(),
            "table count: single={}, merged={}",
            schemas_single.len(), schemas_merged.len()
        );
        for s in &schemas_single {
            let m = schemas_merged.iter().find(|ms| ms.name == s.name)
                .unwrap_or_else(|| panic!("table {} missing from merged schema", s.name));
            assert_eq!(s.columns.len(), m.columns.len(),
                "column count mismatch for table {}", s.name);
        }
    }

    // -------------------------------------------------------------------------
    // apply_normalize_dynamic_keys / apply_flatten / apply_jsonb_flatten
    // -------------------------------------------------------------------------

    fn make_parent_child(parent_name: &str, child_name: &str) -> Vec<TableSchema> {
        let mut parent = TableSchema::new(parent_name.to_string(), vec![parent_name.to_string()], 0);
        parent.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        parent.columns.push(ColumnSchema {
            name: "name".to_string(), original_name: "name".to_string(),
            pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
        });

        let mut child = TableSchema::new(child_name.to_string(), vec![parent_name.to_string(), child_name.to_string()], 1);
        child.parent_table = Some(parent_name.to_string());
        child.child_kind = Some(ChildKind::Object);
        child.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        child.columns.push(ColumnSchema::parent_fk(parent_name));
        child.columns.push(ColumnSchema {
            name: "val".to_string(), original_name: "val".to_string(),
            pg_type: PgType::Text, not_null: false, is_generated: false, is_parent_fk: false,
        });

        vec![parent, child]
    }

    #[test]
    fn test_apply_normalize_table_not_found_returns_err() {
        let mut schemas = make_parent_child("users", "en");
        let result = apply_normalize_dynamic_keys(&mut schemas, "nonexistent", "lang".to_string());
        assert!(result.is_err(), "should return Err when table not found");
    }

    #[test]
    fn test_apply_normalize_no_children_returns_err() {
        let mut schemas = make_parent_child("users", "en");
        // Remove the child — users has no Object children
        schemas.retain(|s| s.name != "en");
        let result = apply_normalize_dynamic_keys(&mut schemas, "users", "lang".to_string());
        assert!(result.is_err(), "should return Err when no Object children found");
    }

    #[test]
    fn test_apply_normalize_success() {
        let mut schemas = make_parent_child("users", "en");
        let result = apply_normalize_dynamic_keys(&mut schemas, "users", "lang".to_string());
        assert!(result.is_ok());
    }

    #[test]
    fn test_apply_flatten_table_not_found_returns_err() {
        let mut schemas = make_parent_child("users", "addr");
        let result = apply_flatten(&mut schemas, "nonexistent", "", 1);
        assert!(result.is_err(), "should return Err when child table not found");
    }

    #[test]
    fn test_apply_flatten_root_table_returns_err() {
        let mut schemas = make_parent_child("users", "addr");
        // Try to flatten a root table (no parent)
        let result = apply_flatten(&mut schemas, "users", "", 1);
        assert!(result.is_err(), "should return Err when flattening a root table");
    }

    #[test]
    fn test_apply_flatten_success() {
        let mut schemas = make_parent_child("users", "addr");
        let result = apply_flatten(&mut schemas, "addr", "", 1);
        assert!(result.is_ok());
        assert!(!schemas.iter().any(|s| s.name == "addr"), "child should be removed after flatten");
    }

    #[test]
    fn test_apply_jsonb_flatten_table_not_found_returns_err() {
        let mut schemas = make_parent_child("users", "meta");
        let result = apply_jsonb_flatten(&mut schemas, "nonexistent");
        assert!(result.is_err(), "should return Err when child table not found");
    }

    #[test]
    fn test_apply_jsonb_flatten_root_table_returns_err() {
        let mut schemas = make_parent_child("users", "meta");
        let result = apply_jsonb_flatten(&mut schemas, "users");
        assert!(result.is_err(), "should return Err when inlining a root table");
    }

    #[test]
    fn test_apply_jsonb_flatten_success() {
        let mut schemas = make_parent_child("users", "meta");
        let result = apply_jsonb_flatten(&mut schemas, "meta");
        assert!(result.is_ok());
        assert!(!schemas.iter().any(|s| s.name == "meta"), "child should be removed after jsonb flatten");
    }

    // -------------------------------------------------------------------------
    // apply_column_limit_guard
    // -------------------------------------------------------------------------

    fn make_wide_schema(name: &str, parent: Option<&str>, data_col_count: usize) -> TableSchema {
        let mut s = TableSchema::new(name.to_string(), vec![name.to_string()], 0);
        s.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        if let Some(p) = parent {
            s.columns.push(ColumnSchema::parent_fk(p));
            s.parent_table = Some(p.to_string());
        }
        for i in 0..data_col_count {
            s.columns.push(ColumnSchema {
                name: format!("col_{}", i),
                original_name: format!("col_{}", i),
                pg_type: PgType::Text,
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
        }
        s
    }

    #[test]
    fn test_column_limit_guard_converts_overflow() {
        let mut schemas = vec![make_wide_schema("t", None, 1601)];
        let warnings = apply_column_limit_guard(&mut schemas);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].table_name, "t");
        assert_eq!(warnings[0].original_column_count, 1601);

        // Only generated columns + one JSONB data column remain
        let data: Vec<_> = schemas[0].data_columns().collect();
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].name, "data");
        assert!(matches!(data[0].pg_type, PgType::Jsonb));
        assert!(matches!(schemas[0].wide_strategy, WideStrategy::Jsonb));
    }

    #[test]
    fn test_column_limit_guard_boundary() {
        // Root table has 1 generated col (j2s_id).
        // Max safe data cols = PG_MAX_COLUMNS - 1 = 1599.
        // 1599 data + 1 generated = 1600 total → must NOT be converted.
        let mut schemas = vec![make_wide_schema("t", None, PG_MAX_COLUMNS - 1)];
        let warnings = apply_column_limit_guard(&mut schemas);
        assert!(warnings.is_empty(), "{} data cols (root) must not trigger the guard", PG_MAX_COLUMNS - 1);
        assert_eq!(schemas[0].data_columns().count(), PG_MAX_COLUMNS - 1);

        // 1600 data + 1 generated = 1601 total → must be converted.
        let mut schemas2 = vec![make_wide_schema("t", None, PG_MAX_COLUMNS)];
        let warnings2 = apply_column_limit_guard(&mut schemas2);
        assert_eq!(warnings2.len(), 1, "{} data cols (root) must trigger the guard", PG_MAX_COLUMNS);

        // Child table has 2 generated cols (j2s_id + j2s_parent_id).
        // Max safe data cols = PG_MAX_COLUMNS - 2 = 1598.
        let mut schemas3 = vec![
            make_wide_schema("parent", None, 5),
            make_wide_schema("child", Some("parent"), PG_MAX_COLUMNS - 2),
        ];
        let warnings3 = apply_column_limit_guard(&mut schemas3);
        assert!(warnings3.is_empty(), "{} data cols (child) must not trigger the guard", PG_MAX_COLUMNS - 2);

        let mut schemas4 = vec![
            make_wide_schema("parent", None, 5),
            make_wide_schema("child", Some("parent"), PG_MAX_COLUMNS - 1),
        ];
        let warnings4 = apply_column_limit_guard(&mut schemas4);
        assert_eq!(warnings4.len(), 1, "{} data cols (child) must trigger the guard", PG_MAX_COLUMNS - 1);
    }

    #[test]
    fn test_column_limit_guard_preserves_children() {
        let parent = make_wide_schema("parent", None, 1601);
        let child  = make_wide_schema("child", Some("parent"), 10);
        let mut schemas = vec![parent, child];

        let warnings = apply_column_limit_guard(&mut schemas);

        assert_eq!(warnings.len(), 1);
        assert_eq!(schemas.len(), 2, "child table must not be removed");
        assert_eq!(schemas[1].name, "child");
        assert_eq!(schemas[1].data_columns().count(), 10, "child columns must be untouched");
    }

    #[test]
    fn test_column_limit_guard_converts_child_independently() {
        let parent = make_wide_schema("parent", None, 10);
        let child  = make_wide_schema("child", Some("parent"), 1601);
        let mut schemas = vec![parent, child];

        let warnings = apply_column_limit_guard(&mut schemas);

        assert_eq!(warnings.len(), 1);
        assert_eq!(warnings[0].table_name, "child");
        // Parent unchanged
        assert_eq!(schemas[0].data_columns().count(), 10);
        // Child converted
        assert_eq!(schemas[1].data_columns().count(), 1);
        assert!(matches!(schemas[1].wide_strategy, WideStrategy::Jsonb));
    }

    // -----------------------------------------------------------------------
    // child_compatibility_score
    // -----------------------------------------------------------------------

    fn make_schema_with_cols(name: &str, parent: Option<&str>, cols: &[&str], depth: usize) -> TableSchema {
        let mut s = TableSchema::new(
            name.to_string(),
            name.split('_').map(|p| p.to_string()).collect(),
            depth,
        );
        s.parent_table = parent.map(|p| p.to_string());
        for col in cols {
            s.columns.push(ColumnSchema {
                name: col.to_string(),
                original_name: col.to_string(),
                pg_type: crate::schema::type_tracker::PgType::Integer,
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
        }
        // Set path.last() to the last segment for key lookups
        s.path = name.split('_').map(|p| p.to_string()).collect();
        s
    }

    #[test]
    fn child_compat_no_children_is_one() {
        // Siblings A, B have no children → vacuously compatible
        let a = make_schema_with_cols("p_a", Some("p"), &["col1", "col2"], 1);
        let b = make_schema_with_cols("p_b", Some("p"), &["col1", "col2"], 1);
        let schemas = vec![
            make_schema_with_cols("p", None, &[], 0),
            a, b,
        ];
        let obj_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        let arr_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        let score = child_compatibility_score(&schemas, &[1, 2], &obj_map, &arr_map);
        assert_eq!(score, 1.0);
    }

    #[test]
    fn child_compat_shared_compatible_children_is_high() {
        // A has child A_k (col1, col2), B has child B_k (col1, col2) → same schema, compat ≈ 1.0
        let mut schemas = vec![
            make_schema_with_cols("p", None, &[], 0),       // 0
            make_schema_with_cols("p_a", Some("p"), &["col1"], 1),  // 1
            make_schema_with_cols("p_b", Some("p"), &["col1"], 1),  // 2
            make_schema_with_cols("p_a_k", Some("p_a"), &["col1", "col2"], 2), // 3
            make_schema_with_cols("p_b_k", Some("p_b"), &["col1", "col2"], 2), // 4
        ];
        // Force last path segment to "k" for both children
        schemas[3].path = vec!["p".to_string(), "a".to_string(), "k".to_string()];
        schemas[4].path = vec!["p".to_string(), "b".to_string(), "k".to_string()];

        let mut obj_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        obj_map.insert("p_a".to_string(), vec![3]);
        obj_map.insert("p_b".to_string(), vec![4]);
        let arr_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();

        let score = child_compatibility_score(&schemas, &[1, 2], &obj_map, &arr_map);
        assert_eq!(score, 1.0, "identical child schemas → compat = 1.0");
    }

    #[test]
    fn child_compat_shared_incompatible_children_is_zero() {
        // A has child A_k (col1, col2), B has child B_k (col3, col4) → disjoint schemas → Jaccard = 0
        let mut schemas = vec![
            make_schema_with_cols("p", None, &[], 0),
            make_schema_with_cols("p_a", Some("p"), &["col1"], 1),
            make_schema_with_cols("p_b", Some("p"), &["col1"], 1),
            make_schema_with_cols("p_a_k", Some("p_a"), &["col1", "col2"], 2),
            make_schema_with_cols("p_b_k", Some("p_b"), &["col3", "col4"], 2),
        ];
        schemas[3].path = vec!["p".to_string(), "a".to_string(), "k".to_string()];
        schemas[4].path = vec!["p".to_string(), "b".to_string(), "k".to_string()];

        let mut obj_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        obj_map.insert("p_a".to_string(), vec![3]);
        obj_map.insert("p_b".to_string(), vec![4]);
        let arr_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();

        let score = child_compatibility_score(&schemas, &[1, 2], &obj_map, &arr_map);
        assert_eq!(score, 0.0, "disjoint child schemas → compat = 0.0");
    }

    #[test]
    fn child_compat_unshared_keys_ignored() {
        // A has child A_k1, B has child B_k2 (different keys) → no shared keys → compat = 1.0
        let mut schemas = vec![
            make_schema_with_cols("p", None, &[], 0),
            make_schema_with_cols("p_a", Some("p"), &["col1"], 1),
            make_schema_with_cols("p_b", Some("p"), &["col1"], 1),
            make_schema_with_cols("p_a_k1", Some("p_a"), &["col1"], 2),
            make_schema_with_cols("p_b_k2", Some("p_b"), &["col3"], 2),
        ];
        schemas[3].path = vec!["p".to_string(), "a".to_string(), "k1".to_string()];
        schemas[4].path = vec!["p".to_string(), "b".to_string(), "k2".to_string()];

        let mut obj_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();
        obj_map.insert("p_a".to_string(), vec![3]);
        obj_map.insert("p_b".to_string(), vec![4]);
        let arr_map: std::collections::HashMap<String, Vec<usize>> = std::collections::HashMap::new();

        let score = child_compatibility_score(&schemas, &[1, 2], &obj_map, &arr_map);
        assert_eq!(score, 1.0, "no shared child keys → compat = 1.0 (unshared keys do not penalise)");
    }

    // ── Dot-in-key regression tests ──────────────────────────────────────────

    /// A JSON object field named "foo.bar" must NOT be merged with "foo_bar".
    /// Both are scalar columns → they should be two distinct columns in root.
    #[test]
    fn dot_in_scalar_field_distinct_from_underscore() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        reg.observe_root("root", make_root(&json!({"foo.bar": 1, "foo_bar": 2})));
        let schemas = reg.finalize();
        let root = schemas.iter().find(|s| s.name == "root").unwrap();
        assert!(
            root.find_by_original("foo.bar").is_some(),
            "column 'foo.bar' must be present in root"
        );
        assert!(
            root.find_by_original("foo_bar").is_some(),
            "column 'foo_bar' must be present in root"
        );
    }

    /// A field "foo.bar" that is an object must NOT be merged with a field
    /// "foo_bar" that is also an object. They must produce two distinct child
    /// tables, not one merged table.
    #[test]
    fn dot_in_object_field_creates_distinct_child_table() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        reg.observe_root("root", make_root(&json!({"foo.bar": {"baz": 1}})));
        reg.observe_root("root", make_root(&json!({"foo_bar": {"baz": 2}})));
        let schemas = reg.finalize();
        // root + two distinct children = 3 schemas total
        assert_eq!(
            schemas.len(), 3,
            "expected root + 2 distinct child tables, got: {:?}",
            schemas.iter().map(|s| &s.name).collect::<Vec<_>>()
        );
    }

    // ── Sibling determinism ───────────────────────────────────────────────────

    /// Observing siblings in different order must produce the same schema names.
    /// Catches non-determinism from par_iter() or HashMap iteration in the
    /// sibling detection pipeline.
    #[test]
    fn sibling_detection_schema_names_are_deterministic() {
        let fields_fwd = ["alpha", "beta", "gamma", "delta", "epsilon"];
        let fields_rev = ["epsilon", "delta", "gamma", "beta", "alpha"];

        let run = |order: &[&str]| -> Vec<String> {
            // threshold=3 so 5 siblings triggers detection (≥ 3)
            let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
            for field in order {
                reg.observe_root(
                    "root",
                    make_root(&json!({ *field: { "value": 1, "name": "x" } })),
                );
            }
            let mut names: Vec<String> = reg.finalize().into_iter().map(|s| s.name).collect();
            names.sort(); // sort for comparison (depth order may differ by run)
            names
        };

        let fwd = run(&fields_fwd);
        let rev = run(&fields_rev);
        assert_eq!(fwd, rev, "schema names must be identical regardless of observation order");
    }

    /// With threshold=2, a pair of 2 identical siblings must be auto-merged into a KeyedPivot.
    #[test]
    fn test_sibling_threshold_two_detects_pair() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 2, 0.5, 0.10, 0.001);
        let obj = json!({ "nutriscore": {
            "2021": { "grade": "b", "score": 45 },
            "2023": { "grade": "b", "score": 45 }
        }});
        reg.observe_root("products", make_root(&obj));
        let schemas = reg.finalize();
        let nutriscore = schemas.iter().find(|s| s.name == "products_nutriscore").unwrap();
        assert!(
            matches!(nutriscore.wide_strategy, WideStrategy::KeyedPivot(_)),
            "2 identical siblings with threshold=2 must become KeyedPivot, got: {:?}",
            nutriscore.wide_strategy
        );
    }

    /// With threshold=3, a pair of only 2 siblings must NOT be auto-merged.
    #[test]
    fn test_sibling_threshold_three_does_not_detect_pair() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        let obj = json!({ "nutriscore": {
            "2021": { "grade": "b", "score": 45 },
            "2023": { "grade": "b", "score": 45 }
        }});
        reg.observe_root("products", make_root(&obj));
        let schemas = reg.finalize();
        let nutriscore = schemas.iter().find(|s| s.name == "products_nutriscore").unwrap();
        assert!(
            !matches!(nutriscore.wide_strategy, WideStrategy::KeyedPivot(_)),
            "2 siblings with threshold=3 must NOT become KeyedPivot"
        );
    }

    /// Non-mixed group with 2 disjoint sub-schemas → greedy clustering produces MultiKeyedPivot.
    /// Without clustering this would fall through (global pairwise Jaccard = 0).
    #[test]
    fn test_schema_clustering_non_mixed_two_groups() {
        // threshold=2, min_jaccard=0.5
        // "front_*" siblings have {imgid, rev}, "ingr_*" have {angle, x} → disjoint
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 2, 0.5, 0.10, 0.001);
        let obj = json!({
            "dict": {
                "front_fr": { "imgid": "1", "rev": "3" },
                "front_en": { "imgid": "2", "rev": "4" },
                "ingr_fr":  { "angle": 10, "x": 100 },
                "ingr_en":  { "angle": 20, "x": 200 }
            }
        });
        reg.observe_root("root", make_root(&obj));
        let schemas = reg.finalize();
        let dict = schemas.iter().find(|s| s.name == "root_dict").unwrap();
        assert!(
            matches!(dict.wide_strategy, WideStrategy::MultiKeyedPivot(_)),
            "heterogeneous non-numeric group with 2 homogeneous clusters must produce MultiKeyedPivot, got: {:?}",
            dict.wide_strategy
        );
        if let WideStrategy::MultiKeyedPivot(groups) = &dict.wide_strategy {
            assert_eq!(groups.len(), 2, "must produce exactly 2 pivot groups (front + ingr)");
        }
    }

    /// Mixed group: numeric ok + non-numeric heterogeneous → numeric merged + clusters for non-numeric.
    #[test]
    fn test_schema_clustering_mixed_numeric_plus_clusters() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 2, 0.5, 0.10, 0.001);
        let obj = json!({
            "images": {
                "1": { "uploader": "u1", "uploaded_t": 123 },
                "2": { "uploader": "u2", "uploaded_t": 456 },
                "front_fr": { "imgid": "1", "rev": "3" },
                "front_en": { "imgid": "2", "rev": "4" },
                "ingr_fr":  { "angle": 10, "x": 100 },
                "ingr_en":  { "angle": 20, "x": 200 }
            }
        });
        reg.observe_root("root", make_root(&obj));
        let schemas = reg.finalize();
        let images = schemas.iter().find(|s| s.name == "root_images").unwrap();
        assert!(
            matches!(images.wide_strategy, WideStrategy::MultiKeyedPivot(_)),
            "mixed group with clusterable non-numeric siblings must produce MultiKeyedPivot, got: {:?}",
            images.wide_strategy
        );
        if let WideStrategy::MultiKeyedPivot(groups) = &images.wide_strategy {
            // numeric group + 2 non-numeric clusters = 3 total
            assert_eq!(groups.len(), 3, "must produce 3 groups (num + front + ingr), got {}", groups.len());
        }
    }

    /// Post-pass: after the BFS cascade, Columns children of a KeyedPivot parent that
    /// are numerous enough and sufficiently similar must be fused into a sub-pivot.
    ///
    /// Structure: `selected.{type}.{lang} = {imgid, rev}`
    /// Wave 0: selected absorbs {front, nutrition} → KeyedPivot (key = image type).
    /// Cascade wave 1: creates one T table per shared lang code
    ///   (root_selected_fr, root_selected_en, root_selected_de).
    /// Post-pass: those 3 Columns children of the KeyedPivot → merged into root_selected_key.
    #[test]
    fn test_keyed_pivot_orphan_children_merged_by_post_pass() {
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 2, 0.5, 0.10, 0.001);
        let obj = json!({
            "selected": {
                "front": {
                    "fr": { "imgid": "1", "rev": "2" },
                    "en": { "imgid": "3", "rev": "4" },
                    "de": { "imgid": "5", "rev": "6" }
                },
                "nutrition": {
                    "fr": { "imgid": "7",  "rev": "8"  },
                    "en": { "imgid": "9",  "rev": "10" },
                    "de": { "imgid": "11", "rev": "12" }
                }
            }
        });
        reg.observe_root("root", make_root(&obj));
        let schemas = reg.finalize();

        let selected = schemas.iter().find(|s| s.name == "root_selected").unwrap();
        assert!(
            matches!(selected.wide_strategy, WideStrategy::KeyedPivot(_)),
            "root_selected must remain KeyedPivot (type key), got: {:?}",
            selected.wide_strategy
        );

        // Post-pass must create a KeyedPivot sub-table directly under root_selected.
        let sub_pivot = schemas.iter().find(|s| {
            s.parent_table.as_deref() == Some("root_selected")
                && matches!(s.wide_strategy, WideStrategy::KeyedPivot(_))
        });
        assert!(
            sub_pivot.is_some(),
            "post-pass must create a KeyedPivot child of root_selected for the 3 lang T tables;\n\
             schemas: {:?}",
            schemas.iter().map(|s| (&s.name, &s.parent_table)).collect::<Vec<_>>()
        );

        // The 3 individual T tables must be absorbed — no Columns orphan remains.
        let columns_orphans = schemas
            .iter()
            .filter(|s| {
                s.parent_table.as_deref() == Some("root_selected")
                    && matches!(s.wide_strategy, WideStrategy::Columns)
            })
            .count();
        assert_eq!(
            columns_orphans, 0,
            "all Columns children of the KeyedPivot must be absorbed by the sub-pivot"
        );
    }

    /// Below threshold: if the cascade produces fewer orphan T tables than `threshold`,
    /// the post-pass must NOT fire.
    #[test]
    fn test_keyed_pivot_orphan_children_not_merged_below_threshold() {
        // threshold = 3, but only 2 lang codes → 2 T tables → no sub-pivot
        let mut reg = SchemaRegistry::new(256, false, usize::MAX, 3, 0.5, 0.10, 0.001);
        let obj = json!({
            "selected": {
                "front":     { "fr": { "imgid": "1", "rev": "2" }, "en": { "imgid": "3", "rev": "4" } },
                "nutrition": { "fr": { "imgid": "5", "rev": "6" }, "en": { "imgid": "7", "rev": "8" } }
            }
        });
        reg.observe_root("root", make_root(&obj));
        let schemas = reg.finalize();

        let sub_pivot = schemas.iter().find(|s| {
            s.parent_table.as_deref() == Some("root_selected")
                && matches!(s.wide_strategy, WideStrategy::KeyedPivot(_))
        });
        assert!(
            sub_pivot.is_none(),
            "with threshold=3, only 2 orphan T tables must NOT create a sub-pivot"
        );
    }
}

