use indexmap::IndexMap;
use rayon::prelude::*;
use serde_json::Value;

use super::naming::{ColumnCollision, ColumnNameRegistry, NamingRegistry, TruncatedName};
use super::stats::ColumnStats;
use super::suffix_detector::detect_suffix_schema;
use super::table_schema::{ChildKind, ColumnSchema, KeyShape, SiblingSchema, SuffixSchema, TableSchema, WideStrategy};
use super::type_tracker::{widen_pg_types, InferredType, PgType, TypeTracker};

/// One entry in the registry per table (keyed by dot-joined path).
#[derive(Debug)]
struct TableEntry {
    /// dot-joined path key, e.g. "users.orders.items"
    path_key: String,
    /// path segments, e.g. ["users", "orders", "items"]
    path: Vec<String>,
    /// parent path key (empty string for root)
    parent_key: String,
    /// column trackers, keyed by original JSON field name
    columns: IndexMap<String, TypeTracker>,
    /// child_kind for this table relative to its parent
    child_kind: Option<ChildKind>,
    /// for scalar junction tables, the element type tracker
    scalar_tracker: Option<TypeTracker>,
    /// element type trackers for scalar arrays stored as PG array columns (array_as_pg_array mode)
    array_columns: IndexMap<String, TypeTracker>,
    /// number of times observe_object() was called for this table (≈ row count)
    row_count: u64,
}

impl TableEntry {
    fn new(path: Vec<String>, parent_key: String, child_kind: Option<ChildKind>) -> Self {
        let path_key = path.join(".");
        Self {
            path_key,
            path,
            parent_key,
            columns: IndexMap::new(),
            child_kind,
            scalar_tracker: None,
            array_columns: IndexMap::new(),
            row_count: 0,
        }
    }

    fn observe_field(&mut self, field: &str, value: &Value, text_threshold: u32) {
        self.columns
            .entry(field.to_string())
            .or_insert_with(|| TypeTracker::new(text_threshold))
            .observe(value);
    }
}

/// Accumulates all type observations across the entire Pass 1 streaming scan.
pub struct SchemaRegistry {
    /// All tables discovered, keyed by dot-joined path
    tables: IndexMap<String, TableEntry>,
    naming: NamingRegistry,
    text_threshold: u32,
    /// When true, scalar arrays become array columns on the parent table instead of junction tables
    array_as_pg_array: bool,
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
            tables: IndexMap::new(),
            naming: NamingRegistry::new(),
            text_threshold,
            array_as_pg_array,
            wide_column_threshold,
            sibling_threshold,
            sibling_jaccard,
            stable_threshold,
            rare_threshold,
            column_collisions: Vec::new(),
        }
    }

    /// Observe a single root-level JSON object.
    ///
    /// Uses an explicit heap stack instead of recursion to handle arbitrarily
    /// deep nesting without risk of stack overflow.
    pub fn observe_root(&mut self, root_name: &str, obj: &serde_json::Map<String, Value>) {
        let root_path = vec![root_name.to_string()];
        self.ensure_table(root_path.clone(), String::new(), None);

        // Stack items: (path of current object, reference to its field map).
        // Lifetimes are tied to `obj` which lives for the duration of this call.
        let mut stack: Vec<(Vec<String>, &serde_json::Map<String, Value>)> =
            vec![(root_path, obj)];

        while let Some((path, map)) = stack.pop() {
            let path_key = path.join(".");

            if let Some(entry) = self.tables.get_mut(&path_key) {
                entry.row_count += 1;
            }

            for (field, value) in map {
                match value {
                    Value::Object(nested) => {
                        let child = child_path(&path, field);
                        self.ensure_table(child.clone(), path_key.clone(), Some(ChildKind::Object));
                        stack.push((child, nested));
                    }
                    Value::Array(arr) => {
                        if arr.is_empty() {
                            continue;
                        }
                        let child = child_path(&path, field);
                        let first_is_object = arr.iter().any(|v| matches!(v, Value::Object(_)));

                        if first_is_object {
                            self.ensure_table(
                                child.clone(),
                                path_key.clone(),
                                Some(ChildKind::ObjectArray),
                            );
                            for item in arr {
                                if let Value::Object(nested_obj) = item {
                                    stack.push((child.clone(), nested_obj));
                                }
                                // Non-object items in an array-of-objects → skip (anomaly)
                            }
                        } else if self.array_as_pg_array {
                            let threshold = self.text_threshold;
                            let entry = self.tables.get_mut(&path_key).unwrap();
                            let tracker = entry
                                .array_columns
                                .entry(field.to_string())
                                .or_insert_with(|| TypeTracker::new(threshold));
                            for item in arr {
                                tracker.observe(item);
                            }
                        } else {
                            let child_key = child.join(".");
                            self.ensure_table(
                                child,
                                path_key.clone(),
                                Some(ChildKind::ScalarArray),
                            );
                            let threshold = self.text_threshold;
                            let entry = self.tables.get_mut(&child_key).unwrap();
                            let tracker = entry
                                .scalar_tracker
                                .get_or_insert_with(|| TypeTracker::new(threshold));
                            for item in arr {
                                tracker.observe(item);
                            }
                        }
                    }
                    scalar => {
                        let threshold = self.text_threshold;
                        self.tables
                            .get_mut(&path_key)
                            .unwrap()
                            .observe_field(field, scalar, threshold);
                    }
                }
            }
        }
    }

    fn ensure_table(
        &mut self,
        path: Vec<String>,
        parent_key: String,
        child_kind: Option<ChildKind>,
    ) {
        let key = path.join(".");
        self.tables
            .entry(key)
            .or_insert_with(|| TableEntry::new(path, parent_key, child_kind));
    }

    // -----------------------------------------------------------------------
    // Finalization
    // -----------------------------------------------------------------------

    /// Convert all accumulated observations into finalized `TableSchema` objects,
    /// sorted topologically (parents before children).
    pub fn finalize(&mut self) -> Vec<TableSchema> {
        // Pre-register all table names so that table_name_lookup() (read-only) works in parallel.
        let paths: Vec<Vec<String>> = self.tables.values().map(|e| e.path.clone()).collect();
        for path in &paths {
            self.naming.table_name(path);
        }

        // Pre-compute set of path_keys that have at least one Object/ObjectArray child.
        let tables_with_object_children: std::collections::HashSet<String> = self
            .tables
            .values()
            .filter(|e| matches!(e.child_kind, Some(ChildKind::Object) | Some(ChildKind::ObjectArray)))
            .map(|e| e.parent_key.clone())
            .collect();

        // Build schemas in parallel — each entry is independent after pre-registration.
        let entries: Vec<&TableEntry> = self.tables.values().collect();
        let results: Vec<(TableSchema, Option<TableSchema>, Vec<ColumnCollision>)> = entries
            .par_iter()
            .map(|entry| {
                build_entry_schema(
                    entry,
                    &self.naming,
                    &tables_with_object_children,
                    self.wide_column_threshold,
                    self.stable_threshold,
                    self.rare_threshold,
                    self.text_threshold,
                )
            })
            .collect();

        let mut schemas: Vec<TableSchema> = Vec::with_capacity(results.len());
        let mut extra_schemas: Vec<TableSchema> = Vec::new();
        let mut all_collisions: Vec<ColumnCollision> = Vec::new();
        for (schema, extra, collisions) in results {
            schemas.push(schema);
            if let Some(e) = extra {
                extra_schemas.push(e);
            }
            all_collisions.extend(collisions);
        }

        self.column_collisions = all_collisions;

        schemas.extend(extra_schemas);
        schemas.sort_by_key(|s| s.depth);

        {
            let mut seen = std::collections::HashSet::new();
            schemas.retain(|s| seen.insert(s.name.clone()));
        }

        finalize_siblings(&mut schemas, self.sibling_threshold, self.sibling_jaccard);
        exclude_absorbed_children(&mut schemas);

        schemas
    }


    /// Collect type distribution statistics for every data column (excluding j2s_ generated columns).
    /// Call after `finalize()` — uses the same naming registry for consistent table/column names.
    pub fn collect_stats(&mut self) -> Vec<ColumnStats> {
        let mut result = Vec::new();

        for entry in self.tables.values() {
            let table_name = self.naming.table_name(&entry.path);

            // Regular scalar columns
            for (original_field, tracker) in &entry.columns {
                if tracker.is_object_field() || tracker.is_array_field() {
                    continue;
                }
                let col_name = NamingRegistry::column_name(original_field);
                result.push(ColumnStats {
                    table_name: table_name.clone(),
                    column_name: col_name,
                    pg_type: tracker.to_pg_type(),
                    total_count: tracker.total_count,
                    null_count: tracker.null_count,
                    type_histogram: type_histogram(tracker),
                });
            }

            // Junction table value column
            if let Some(ref tracker) = entry.scalar_tracker {
                result.push(ColumnStats {
                    table_name: table_name.clone(),
                    column_name: "value".to_string(),
                    pg_type: tracker.to_pg_type(),
                    total_count: tracker.total_count,
                    null_count: tracker.null_count,
                    type_histogram: type_histogram(tracker),
                });
            }

            // Array-as-column fields
            for (original_field, tracker) in &entry.array_columns {
                let col_name = NamingRegistry::column_name(original_field);
                let elem_type = tracker.to_pg_type();
                result.push(ColumnStats {
                    table_name: table_name.clone(),
                    column_name: col_name,
                    pg_type: PgType::Array(Box::new(elem_type)),
                    total_count: tracker.total_count,
                    null_count: tracker.null_count,
                    type_histogram: type_histogram(tracker),
                });
            }
        }

        // Sort by table then column for stable output
        result.sort_by(|a, b| {
            a.table_name
                .cmp(&b.table_name)
                .then(a.column_name.cmp(&b.column_name))
        });
        result
    }

    /// Return all table names that were truncated to fit the 63-byte PostgreSQL limit.
    pub fn truncated_names(&self) -> &[TruncatedName] {
        self.naming.truncated_names()
    }

    /// Return all column name collisions detected during finalize().
    pub fn column_collisions(&self) -> &[ColumnCollision] {
        &self.column_collisions
    }

    /// Return anomaly info for reporting: (table_pg_name, col_original, TypeTracker)
    pub fn anomaly_iter(&self) -> impl Iterator<Item = (&str, &str, &TypeTracker)> {
        self.tables.values().flat_map(|entry| {
            entry.columns.iter().filter_map(|(field, tracker)| {
                if tracker.has_anomalies() {
                    Some((entry.path_key.as_str(), field.as_str(), tracker))
                } else {
                    None
                }
            })
        })
    }
}

/// Build the `TableSchema` for a single `TableEntry`.
///
/// Pure function — no access to `SchemaRegistry` state. Called in parallel via rayon.
fn build_entry_schema(
    entry: &TableEntry,
    naming: &NamingRegistry,
    tables_with_object_children: &std::collections::HashSet<String>,
    wide_column_threshold: usize,
    stable_threshold: f64,
    rare_threshold: f64,
    text_threshold: u32,
) -> (TableSchema, Option<TableSchema>, Vec<ColumnCollision>) {
    let pg_name = naming.table_name_lookup(&entry.path);
    let depth = entry.path.len().saturating_sub(1);
    let parent_table: Option<String> = if entry.parent_key.is_empty() {
        None
    } else {
        let parent_path: Vec<String> =
            entry.parent_key.split('.').map(|s| s.to_string()).collect();
        Some(naming.table_name_lookup(&parent_path))
    };

    let mut schema = TableSchema::new(pg_name.clone(), entry.path.clone(), depth);
    schema.parent_table = parent_table;
    schema.child_kind = entry.child_kind.clone();

    // Generated columns first
    schema.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
    if let Some(ref p) = schema.parent_table {
        schema.columns.push(ColumnSchema::parent_fk(p));
    }
    if schema.has_order_column() {
        schema.columns.push(ColumnSchema::generated("j2s_order", PgType::BigInt));
    }

    let mut extra_schema: Option<TableSchema> = None;
    let mut local_collisions: Vec<ColumnCollision> = Vec::new();

    // Junction tables have a single `value` column
    if schema.is_junction() {
        if let Some(ref tracker) = entry.scalar_tracker {
            let pg_type = tracker.to_pg_type();
            schema.columns.push(ColumnSchema {
                name: "value".to_string(),
                original_name: "value".to_string(),
                pg_type,
                not_null: tracker.is_not_null(),
                is_generated: false,
                is_parent_fk: false,
            });
        }
    } else {
        let row_count = entry.row_count.max(1) as f64;

        // Build per-table column name registry to detect and resolve collisions
        let mut col_registry = ColumnNameRegistry::new();
        for (original_field, tracker) in &entry.columns {
            if !tracker.is_object_field() && !tracker.is_array_field() {
                col_registry.register(original_field);
            }
        }
        for original_field in entry.array_columns.keys() {
            col_registry.register(original_field);
        }
        col_registry.build(&pg_name);
        local_collisions.extend_from_slice(col_registry.collisions());

        // Regular data columns
        for (original_field, tracker) in &entry.columns {
            if tracker.is_object_field() || tracker.is_array_field() {
                continue;
            }
            let col_name = col_registry.resolve(original_field);
            schema.columns.push(ColumnSchema {
                name: col_name,
                original_name: original_field.clone(),
                pg_type: tracker.to_pg_type(),
                not_null: tracker.is_not_null(),
                is_generated: false,
                is_parent_fk: false,
            });
        }

        // Array-as-column fields (array_as_pg_array mode)
        for (original_field, elem_tracker) in &entry.array_columns {
            let elem_type = elem_tracker.to_pg_type();
            let col_name = col_registry.resolve(original_field);
            schema.columns.push(ColumnSchema {
                name: col_name,
                original_name: original_field.clone(),
                pg_type: PgType::Array(Box::new(elem_type)),
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
        }

        // Apply wide strategy if data column count exceeds threshold.
        // Only eligible for direct Object children (not ObjectArray/ScalarArray)
        // whose keys are dynamic and variable.
        let is_wide_eligible = matches!(entry.child_kind, Some(ChildKind::Object) | None);
        let data_col_count = schema.data_columns().count();
        if is_wide_eligible && data_col_count > wide_column_threshold {
            let is_root = entry.parent_key.is_empty();
            let has_object_children = tables_with_object_children.contains(&entry.path_key);

            // Compute ratio of "stable" keys (present in >= stable_threshold of rows).
            let stable_count = entry
                .columns
                .values()
                .filter(|t| !t.is_object_field() && !t.is_array_field())
                .filter(|t| t.total_count as f64 / row_count >= stable_threshold)
                .count();
            let ratio_stable = stable_count as f64 / data_col_count as f64;

            if ratio_stable > 0.5 && entry.row_count >= 10 {
                eprintln!(
                    "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: Columns \
                    (high stable ratio — legitimate schema, not key explosion)",
                    schema.name, data_col_count, ratio_stable * 100.0
                );
            } else if is_root && has_object_children {
                // P5: classify keys by frequency.
                let medium_keys: std::collections::HashSet<String> = entry
                    .columns
                    .iter()
                    .filter(|(_, t)| !t.is_object_field() && !t.is_array_field())
                    .filter(|(_, t)| {
                        let freq = t.total_count as f64 / row_count;
                        freq >= rare_threshold && freq < stable_threshold
                    })
                    .map(|(k, _)| k.clone())
                    .collect();

                // Drop medium and rare columns from main schema.
                schema.columns.retain(|c| {
                    if c.is_generated {
                        return true;
                    }
                    entry
                        .columns
                        .get(&c.original_name)
                        .map(|t| t.total_count as f64 / row_count >= stable_threshold)
                        .unwrap_or(false)
                });

                let stable_col_count = schema.data_columns().count();
                let rare_count = data_col_count
                    .saturating_sub(stable_col_count)
                    .saturating_sub(medium_keys.len());
                // Build the companion table name. Strip any existing `_wide` suffix first
                // to avoid `foo_wide_wide`. If the result still collides with the main
                // table name (e.g. main table is itself named `foo_wide`), fall back to `_eav`.
                let base_name = schema.name.strip_suffix("_wide").unwrap_or(&schema.name);
                let wide_candidate = format!("{}_wide", base_name);
                let wide_name = if wide_candidate == schema.name {
                    format!("{}_eav", base_name)
                } else {
                    wide_candidate
                };

                eprintln!(
                    "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: AutoSplit \
                    ({} stable cols, {} medium → {}, {} rare dropped)",
                    schema.name, data_col_count, ratio_stable * 100.0,
                    stable_col_count, medium_keys.len(), wide_name, rare_count,
                );

                // Compute widened value type from medium keys for the _wide table.
                let value_type = medium_keys
                    .iter()
                    .filter_map(|k| entry.columns.get(k))
                    .fold(None::<PgType>, |acc, t| {
                        Some(match acc {
                            None => t.to_pg_type(),
                            Some(a) => widen_pg_types(a, &t.to_pg_type()),
                        })
                    })
                    .unwrap_or(PgType::Text);

                // Build the synthetic _wide companion table (EAV Pivot, child of main).
                let mut wide_schema = TableSchema::new(
                    wide_name.clone(),
                    vec![wide_name.clone()],
                    depth + 1,
                );
                wide_schema.parent_table = Some(schema.name.clone());
                wide_schema.child_kind = Some(ChildKind::Object);
                wide_schema.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
                wide_schema.columns.push(ColumnSchema::parent_fk(&schema.name));
                wide_schema.columns.push(ColumnSchema {
                    name: "key".to_string(),
                    original_name: "key".to_string(),
                    pg_type: PgType::Text,
                    not_null: true,
                    is_generated: false,
                    is_parent_fk: false,
                });
                wide_schema.columns.push(ColumnSchema {
                    name: "value".to_string(),
                    original_name: "value".to_string(),
                    pg_type: value_type,
                    not_null: false,
                    is_generated: false,
                    is_parent_fk: false,
                });
                wide_schema.wide_strategy = WideStrategy::Pivot;
                extra_schema = Some(wide_schema);

                schema.wide_strategy = WideStrategy::AutoSplit {
                    stable_threshold,
                    rare_threshold,
                    medium_keys,
                    wide_table_name: wide_name,
                };
            } else {
                if let Some(suffix_schema) =
                    detect_suffix_schema(&entry.columns, 0.3, text_threshold)
                {
                    eprintln!(
                        "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: StructuredPivot ({} suffixes)",
                        schema.name, data_col_count, ratio_stable * 100.0, suffix_schema.suffix_cols.len()
                    );
                    apply_structured_pivot_columns(&mut schema, suffix_schema);
                } else {
                    let strategy = suggest_wide_strategy(entry);
                    eprintln!(
                        "  Wide table detected: {} ({} columns, {:.0}% stable) → strategy: {:?}",
                        schema.name, data_col_count, ratio_stable * 100.0, strategy
                    );
                    apply_wide_strategy_columns(&mut schema, strategy);
                }
            }
        }
    }

    (schema, extra_schema, local_collisions)
}

/// Remove tables whose parent (or any ancestor) absorbs children into a wide column
/// (Pivot, Jsonb, StructuredPivot, KeyedPivot). AutoSplit does NOT absorb children.
///
/// The schemas must be topologically sorted (parents before children) for the single-pass
/// transitive exclusion to work correctly. Safe to call multiple times (idempotent).
pub fn exclude_absorbed_children(schemas: &mut Vec<TableSchema>) {
    // O(n): pre-build set of table names whose wide_strategy absorbs children.
    let absorbers: std::collections::HashSet<&str> = schemas
        .iter()
        .filter(|s| s.wide_strategy.absorbs_children())
        .map(|s| s.name.as_str())
        .collect();

    if absorbers.is_empty() {
        return;
    }

    // Single forward pass exploiting topological order: if a parent is an absorber
    // or already excluded, so are its children (transitive).
    let mut excluded: std::collections::HashSet<String> = std::collections::HashSet::new();
    for schema in schemas.iter() {
        if let Some(ref parent) = schema.parent_table {
            if absorbers.contains(parent.as_str()) || excluded.contains(parent) {
                excluded.insert(schema.name.clone());
            }
        }
    }
    if !excluded.is_empty() {
        schemas.retain(|s| !excluded.contains(&s.name));
    }
}

/// Determine whether a wide table's values are type-homogeneous (→ Pivot) or not (→ Jsonb).
fn suggest_wide_strategy(entry: &TableEntry) -> WideStrategy {
    let mut has_string = false;
    let mut has_numeric = false;
    let mut has_boolean = false;
    let mut has_date_like = false;

    for tracker in entry.columns.values() {
        if tracker.is_object_field() || tracker.is_array_field() {
            continue;
        }
        match tracker.to_pg_type() {
            PgType::Text | PgType::VarChar(_) => has_string = true,
            PgType::Integer | PgType::BigInt | PgType::DoublePrecision => has_numeric = true,
            PgType::Boolean => has_boolean = true,
            PgType::Uuid | PgType::Date | PgType::Timestamp => has_date_like = true,
            PgType::Array(_) | PgType::Jsonb => has_string = true,
        }
    }

    let type_categories = [has_string, has_numeric, has_boolean, has_date_like]
        .iter()
        .filter(|&&x| x)
        .count();

    // Only one type category across all value columns → safe to pivot
    if type_categories <= 1 {
        WideStrategy::Pivot
    } else {
        WideStrategy::Jsonb
    }
}

/// Restructure a schema's data columns to match the given WideStrategy.
/// Replaces all non-generated columns with either (key, value) for Pivot
/// or (data JSONB) for Jsonb.
pub fn apply_wide_strategy_columns(schema: &mut TableSchema, strategy: WideStrategy) {
    match strategy {
        WideStrategy::Columns => {} // nothing to restructure
        WideStrategy::Pivot => {
            // Compute widest value type from existing data columns before clearing
            let value_type = schema
                .data_columns()
                .fold(None::<PgType>, |acc, col| {
                    Some(match acc {
                        None => col.pg_type.clone(),
                        Some(a) => widen_pg_types(a, &col.pg_type),
                    })
                })
                .unwrap_or(PgType::Text);
            schema.columns.retain(|c| c.is_generated);
            schema.columns.push(ColumnSchema {
                name: "key".to_string(),
                original_name: "key".to_string(),
                pg_type: PgType::Text,
                not_null: true,
                is_generated: false,
                is_parent_fk: false,
            });
            schema.columns.push(ColumnSchema {
                name: "value".to_string(),
                original_name: "value".to_string(),
                pg_type: value_type,
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
            schema.wide_strategy = WideStrategy::Pivot;
        }
        WideStrategy::Jsonb => {
            schema.columns.retain(|c| c.is_generated);
            schema.columns.push(ColumnSchema {
                name: "data".to_string(),
                original_name: "data".to_string(),
                pg_type: PgType::Jsonb,
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
            schema.wide_strategy = WideStrategy::Jsonb;
        }
        WideStrategy::StructuredPivot(suffix_schema) => {
            apply_structured_pivot_columns(schema, suffix_schema);
        }
        WideStrategy::KeyedPivot(_) => {
            // KeyedPivot is applied by finalize_siblings(), not through this path.
        }
        WideStrategy::AutoSplit { .. } | WideStrategy::Ignore => {
            // AutoSplit is handled inline in finalize(); Ignore is per-key, not per-table.
            // Neither reaches this function.
        }
    }
}

/// Restructure a wide table's columns for StructuredPivot:
/// (j2s_id, j2s_parent_id, name TEXT, value <type>, <suffix_col>...)
pub fn apply_structured_pivot_columns(schema: &mut TableSchema, suffix_schema: SuffixSchema) {
    schema.columns.retain(|c| c.is_generated);
    schema.columns.push(ColumnSchema {
        name: "name".to_string(),
        original_name: "name".to_string(),
        pg_type: PgType::Text,
        not_null: true,
        is_generated: false,
        is_parent_fk: false,
    });
    schema.columns.push(ColumnSchema {
        name: "value".to_string(),
        original_name: "value".to_string(),
        pg_type: suffix_schema.value_type.clone(),
        not_null: false,
        is_generated: false,
        is_parent_fk: false,
    });
    for sc in &suffix_schema.suffix_cols {
        schema.columns.push(ColumnSchema {
            name: sc.col_name.clone(),
            // original_name = the suffix string so pass2 can look it up
            original_name: sc.suffix.clone(),
            pg_type: sc.pg_type.clone(),
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
    }
    schema.wide_strategy = WideStrategy::StructuredPivot(suffix_schema);
}

// ---------------------------------------------------------------------------
// Sibling table detection
// ---------------------------------------------------------------------------

/// Detect groups of Object-child tables that share the same parent and the same
/// column schema (Jaccard ≥ 0.5). When a group has ≥ `threshold` members, the
/// sibling children are collapsed into the parent table as a `KeyedPivot`:
///   - parent gains a `key` column (the original JSON key of each sibling)
///   - parent gains the union of all sibling data columns (all nullable)
///   - parent.wide_strategy = KeyedPivot(SiblingSchema)
/// The sibling child schemas remain in the vector; the exclusion pass that follows
/// in `finalize()` will drop them (and their descendants) because the parent is now wide.
fn finalize_siblings(schemas: &mut Vec<TableSchema>, threshold: usize, min_jaccard: f64) {
    // Build parent_name → [child_index] map (only Object children, not arrays/junctions)
    let mut parent_to_object_children: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    for (i, schema) in schemas.iter().enumerate() {
        if let Some(ref parent) = schema.parent_table {
            if matches!(schema.child_kind, Some(ChildKind::Object)) {
                parent_to_object_children
                    .entry(parent.clone())
                    .or_default()
                    .push(i);
            }
        }
    }

    // Collect collapse operations (to avoid borrow conflicts when modifying schemas)
    struct Collapse {
        parent_idx: usize,
        key_col_name: String,
        key_shape: KeyShape,
        union_cols: Vec<ColumnSchema>,
        log_msg: String,
    }

    let mut collapses: Vec<Collapse> = Vec::new();

    // O(n): pre-build name → index map so parent lookup is O(1) instead of O(n).
    let name_to_idx: std::collections::HashMap<&str, usize> = schemas
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();

    for (parent_name, child_indices) in &parent_to_object_children {
        if child_indices.len() < threshold {
            continue;
        }

        // Find parent index in O(1)
        let parent_idx = match name_to_idx.get(parent_name.as_str()) {
            Some(&i) => i,
            None => continue,
        };

        // Only pure containers (parent has 0 data columns — all fields are child objects)
        if schemas[parent_idx].data_columns().count() > 0 {
            continue;
        }

        // All children must have good pairwise Jaccard similarity
        let actual_jaccard = pairwise_jaccard_min(schemas, child_indices);
        if actual_jaccard < min_jaccard {
            continue;
        }

        // Extract the sibling key (last path segment) for each child
        let keys: Vec<String> = child_indices
            .iter()
            .map(|&i| schemas[i].path.last().cloned().unwrap_or_default())
            .collect();

        let key_shape = classify_key_shape(&keys.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        let key_col_name = match &key_shape {
            KeyShape::Numeric => "key_id".to_string(),
            KeyShape::IsoLang => "lang_code".to_string(),
            _ => "key".to_string(),
        };

        let children: Vec<&TableSchema> = child_indices.iter().map(|&i| &schemas[i]).collect();
        let union_cols = build_union_columns(&children);

        let key_examples = keys.iter().take(5).map(|s| s.as_str()).collect::<Vec<_>>().join("\", \"");
        let more = if keys.len() > 5 {
            format!("\" (+{} more)", keys.len() - 5)
        } else {
            "\"".to_string()
        };
        let log_msg = format!(
            "  Sibling tables detected: {} ({} tables → 1)\n  Keys: \"{}{}\n  Jaccard min: {:.2} → strategy: KeyedPivot (col: {} {})",
            parent_name, child_indices.len(), key_examples, more,
            min_jaccard, key_col_name, key_shape,
        );

        collapses.push(Collapse { parent_idx, key_col_name, key_shape, union_cols, log_msg });
    }

    // Apply collapses
    for collapse in collapses {
        eprintln!("{}", collapse.log_msg);

        let sibling_schema = SiblingSchema {
            key_col_name: collapse.key_col_name.clone(),
            key_shape: collapse.key_shape,
        };
        let parent = &mut schemas[collapse.parent_idx];
        parent.columns.retain(|c| c.is_generated);
        parent.columns.push(ColumnSchema {
            name: collapse.key_col_name.clone(),
            original_name: collapse.key_col_name,
            pg_type: PgType::Text,
            not_null: true,
            is_generated: false,
            is_parent_fk: false,
        });
        for col in collapse.union_cols {
            parent.columns.push(col);
        }
        parent.wide_strategy = WideStrategy::KeyedPivot(sibling_schema);
    }
}

/// Compute the minimum pairwise Jaccard similarity of data-column names across all pairs.
///
/// Pre-builds one `HashSet` per sibling (O(n·m)) so the O(n²) pair loop only does
/// set intersection queries rather than rebuilding sets from scratch each time.
fn pairwise_jaccard_min(schemas: &[TableSchema], indices: &[usize]) -> f64 {
    if indices.len() < 2 {
        return 1.0;
    }
    // Build one HashSet per sibling — reused across all pairs.
    let col_sets: Vec<std::collections::HashSet<&str>> = indices
        .iter()
        .map(|&i| schemas[i].data_columns().map(|c| c.original_name.as_str()).collect())
        .collect();

    let mut min_j = 1.0_f64;
    for i in 0..col_sets.len() {
        for j in (i + 1)..col_sets.len() {
            let intersection = col_sets[i].iter().filter(|&&c| col_sets[j].contains(c)).count();
            let union = col_sets[i].len() + col_sets[j].len() - intersection;
            let j_val = if union == 0 { 1.0 } else { intersection as f64 / union as f64 };
            if j_val < min_j {
                min_j = j_val;
                if min_j == 0.0 {
                    return 0.0; // Early exit — can't get lower
                }
            }
        }
    }
    min_j
}

/// Build the union of data columns from all sibling children.
/// Types are widened across children; all columns are nullable (different siblings
/// may have different fields).
pub fn build_union_columns(children: &[&TableSchema]) -> Vec<ColumnSchema> {
    let mut col_map: IndexMap<String, (String, PgType)> = IndexMap::new();
    for child in children {
        for col in child.data_columns() {
            col_map
                .entry(col.original_name.clone())
                .and_modify(|(_, t)| *t = widen_pg_types(t.clone(), &col.pg_type))
                .or_insert((col.name.clone(), col.pg_type.clone()));
        }
    }
    col_map
        .into_iter()
        .map(|(original_name, (name, pg_type))| ColumnSchema {
            name,
            original_name,
            pg_type,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        })
        .collect()
}

/// Classify the shape of sibling keys to produce a semantic column name.
fn classify_key_shape(keys: &[&str]) -> KeyShape {
    let total = keys.len();
    if total == 0 {
        return KeyShape::Slug;
    }
    let numeric = keys
        .iter()
        .filter(|k| !k.is_empty() && k.chars().all(|c| c.is_ascii_digit()))
        .count();
    let isolang = keys
        .iter()
        .filter(|k| {
            (k.len() == 2 || k.len() == 3) && k.chars().all(|c| c.is_ascii_alphabetic())
        })
        .count();

    let numeric_ratio = numeric as f64 / total as f64;
    let isolang_ratio = isolang as f64 / total as f64;

    if numeric_ratio >= 0.8 {
        KeyShape::Numeric
    } else if isolang_ratio >= 0.8 {
        KeyShape::IsoLang
    } else if numeric > 0 && isolang > 0 {
        KeyShape::Mixed
    } else {
        KeyShape::Slug
    }
}

fn type_histogram(tracker: &TypeTracker) -> Vec<(String, u64)> {
    let mut hist: Vec<(String, u64)> = tracker
        .type_counts
        .iter()
        .filter(|(t, _)| !matches!(t, InferredType::Object | InferredType::Array))
        .map(|(t, &n)| (format!("{:?}", t), n))
        .collect();
    hist.sort_by(|a, b| b.1.cmp(&a.1)); // most frequent first
    hist
}

fn child_path(parent: &[String], field: &str) -> Vec<String> {
    let mut p = parent.to_vec();
    p.push(field.to_string());
    p
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
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
}
