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
        if let Some(tracker) = self.columns.get_mut(field) {
            tracker.observe(value);
        } else {
            self.columns
                .entry(field.to_string())
                .or_insert_with(|| TypeTracker::new(text_threshold))
                .observe(value);
        }
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
    ///
    /// The stack stores pre-joined path keys (String) rather than Vec<String> to
    /// avoid recomputing path.join(".") and cloning the path Vec on every node visit.
    pub fn observe_root(&mut self, root_name: &str, obj: &serde_json::Map<String, Value>) {
        self.ensure_table_key(root_name, "", None);

        // Stack items: (dot-joined path key, reference to field map).
        // Lifetimes of the map references are tied to `obj`.
        let mut stack: Vec<(String, &serde_json::Map<String, Value>)> =
            vec![(root_name.to_string(), obj)];

        while let Some((path_key, map)) = stack.pop() {
            if let Some(entry) = self.tables.get_mut(&path_key) {
                entry.row_count += 1;
            }

            for (field, value) in map {
                match value {
                    Value::Object(nested) => {
                        let safe_field = if field.contains('.') { field.replace('.', "_") } else { field.to_string() };
                        let child_key = format!("{}.{}", path_key, safe_field);
                        self.ensure_table_key(&child_key, &path_key, Some(ChildKind::Object));
                        stack.push((child_key, nested));
                    }
                    Value::Array(arr) => {
                        if arr.is_empty() {
                            continue;
                        }
                        let safe_field = if field.contains('.') { field.replace('.', "_") } else { field.to_string() };
                        let child_key = format!("{}.{}", path_key, safe_field);
                        let first_is_object = arr.iter().any(|v| matches!(v, Value::Object(_)));

                        if first_is_object {
                            self.ensure_table_key(&child_key, &path_key, Some(ChildKind::ObjectArray));
                            let mut objs: Vec<&serde_json::Map<String, Value>> = arr
                                .iter()
                                .filter_map(|v| if let Value::Object(o) = v { Some(o) } else { None })
                                .collect();
                            if let Some(last) = objs.pop() {
                                for obj in objs {
                                    stack.push((child_key.clone(), obj));
                                }
                                stack.push((child_key, last));
                            }
                        } else if self.array_as_pg_array {
                            let threshold = self.text_threshold;
                            let entry = self.tables.get_mut(&path_key).unwrap();
                            if let Some(tracker) = entry.array_columns.get_mut(field.as_str()) {
                                for item in arr {
                                    tracker.observe(item);
                                }
                            } else {
                                let tracker = entry
                                    .array_columns
                                    .entry(field.to_string())
                                    .or_insert_with(|| TypeTracker::new(threshold));
                                for item in arr {
                                    tracker.observe(item);
                                }
                            }
                        } else {
                            self.ensure_table_key(&child_key, &path_key, Some(ChildKind::ScalarArray));
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

    /// Register a table by its pre-joined dot-key. Only allocates when the table is new.
    fn ensure_table_key(&mut self, key: &str, parent_key: &str, child_kind: Option<ChildKind>) {
        self.tables.entry(key.to_string()).or_insert_with(|| {
            let path: Vec<String> = key.split('.').map(|s| s.to_string()).collect();
            TableEntry::new(path, parent_key.to_string(), child_kind)
        });
    }

    // -----------------------------------------------------------------------
    // Finalization
    // -----------------------------------------------------------------------

    /// Convert all accumulated observations into finalized `TableSchema` objects,
    /// sorted topologically (parents before children).
    pub fn finalize(&mut self) -> Vec<TableSchema> {
        // Pre-register all table names so that table_name_lookup() (read-only) works in parallel.
        // Collect dot-keys (one String clone per entry) rather than cloning the full path Vec
        // (which would be N_segments String clones per entry).
        let dot_keys: Vec<String> = self.tables.keys().cloned().collect();
        for key in &dot_keys {
            self.naming.table_name_from_dot_key(key);
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
            let table_name = self.naming.table_name_from_dot_key(&entry.path_key);

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
    let pg_name = naming.table_name_lookup_from_dot_key(&entry.path_key);
    let depth = entry.path.len().saturating_sub(1);
    let parent_table: Option<String> = if entry.parent_key.is_empty() {
        None
    } else {
        Some(naming.table_name_lookup_from_dot_key(&entry.parent_key))
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
        WideStrategy::NormalizeDynamicKeys { .. } | WideStrategy::Flatten { .. } | WideStrategy::JsonbFlatten => {
            // These strategies require the full schemas slice.
            // Use apply_normalize_dynamic_keys(), apply_flatten(), or apply_jsonb_flatten() instead.
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
    // Build parent_name → [child_index] maps: one for Object children, one for ObjectArray.
    let mut parent_to_object_children: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    let mut parent_to_array_children: std::collections::HashMap<String, Vec<usize>> =
        std::collections::HashMap::new();
    for (i, schema) in schemas.iter().enumerate() {
        if let Some(ref parent) = schema.parent_table {
            match schema.child_kind {
                Some(ChildKind::Object) => {
                    parent_to_object_children.entry(parent.clone()).or_default().push(i);
                }
                Some(ChildKind::ObjectArray) => {
                    parent_to_array_children.entry(parent.clone()).or_default().push(i);
                }
                _ => {}
            }
        }
    }

    // Collect collapse operations (to avoid borrow conflicts when modifying schemas)
    struct Collapse {
        parent_idx: usize,
        key_col_name: String,
        key_shape: KeyShape,
        union_cols: Vec<ColumnSchema>,
        array_children: bool,
        data_col_name: String,
        log_msg: String,
    }

    let mut collapses: Vec<Collapse> = Vec::new();

    // O(n): pre-build name → index map so parent lookup is O(1) instead of O(n).
    let name_to_idx: std::collections::HashMap<&str, usize> = schemas
        .iter()
        .enumerate()
        .map(|(i, s)| (s.name.as_str(), i))
        .collect();

    // Process both Object and ObjectArray sibling groups with the same logic.
    let groups: [(&std::collections::HashMap<String, Vec<usize>>, bool); 2] = [
        (&parent_to_object_children, false),
        (&parent_to_array_children, true),
    ];

    for (parent_map, array_children) in groups {
        for (parent_name, child_indices) in parent_map {
            if child_indices.len() < threshold {
                continue;
            }

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

            let data_col_name = "j2s_data".to_string();

            let key_examples = keys.iter().take(5).map(|s| s.as_str()).collect::<Vec<_>>().join("\", \"");
            let more = if keys.len() > 5 {
                format!("\" (+{} more)", keys.len() - 5)
            } else {
                "\"".to_string()
            };
            let kind_label = if array_children { "ObjectArray" } else { "Object" };
            let log_msg = format!(
                "  Sibling {} tables detected: {} ({} tables → 1)\n  Keys: \"{}{}\n  Jaccard min: {:.2} → strategy: KeyedPivot (col: {} {})",
                kind_label, parent_name, child_indices.len(), key_examples, more,
                min_jaccard, key_col_name, key_shape,
            );

            collapses.push(Collapse {
                parent_idx,
                key_col_name,
                key_shape,
                union_cols,
                array_children,
                data_col_name,
                log_msg,
            });
        }
    }

    // Apply collapses
    for collapse in collapses {
        eprintln!("{}", collapse.log_msg);

        let sibling_schema = SiblingSchema {
            key_col_name: collapse.key_col_name.clone(),
            key_shape: collapse.key_shape,
            array_children: collapse.array_children,
            data_col_name: collapse.data_col_name.clone(),
        };
        let parent = &mut schemas[collapse.parent_idx];
        parent.columns.retain(|c| c.is_generated);
        // ObjectArray siblings need j2s_order to track position within each array.
        if collapse.array_children {
            parent.columns.push(ColumnSchema::generated("j2s_order", PgType::BigInt));
        }
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
        // Always add a data JSONB column to capture the raw child object/array.
        parent.columns.push(ColumnSchema {
            name: collapse.data_col_name.clone(),
            original_name: collapse.data_col_name,
            pg_type: PgType::Jsonb,
            not_null: false,
            is_generated: true,
            is_parent_fk: false,
        });
        parent.wide_strategy = WideStrategy::KeyedPivot(sibling_schema);
    }
}

/// Compute the minimum pairwise Jaccard similarity of data-column names across all pairs.
///
/// Two fast paths avoid the O(n²) full pairwise loop for large sibling groups:
///
/// 1. **Pure-container fast path** — if every sibling has zero data columns (they are pure
///    containers whose data lives in their own children), the Jaccard is 1.0 by convention
///    (union = 0 for all pairs). This covers the common pangenomegraph/genome-key pattern.
///
/// 2. **Large-group fast path** — when N > PAIRWISE_LIMIT, compare each sibling against
///    sibling[0] instead of all N*(N-1)/2 pairs. Semantically equivalent for the homogeneous
///    schemas typical of KeyedPivot detection (language codes, numeric IDs, genome keys).
///    Outliers are still detected: any sibling with 0 column overlap with sibling[0] returns 0.
fn pairwise_jaccard_min(schemas: &[TableSchema], indices: &[usize]) -> f64 {
    if indices.len() < 2 {
        return 1.0;
    }

    // Fast path 1: pure containers — every sibling has no data columns.
    // Check before allocating col_sets to skip HashSet construction entirely.
    if indices.iter().all(|&i| schemas[i].data_columns().next().is_none()) {
        return 1.0;
    }

    // Build one HashSet per sibling — O(n·m).
    let col_sets: Vec<std::collections::HashSet<&str>> = indices
        .iter()
        .map(|&i| schemas[i].data_columns().map(|c| c.original_name.as_str()).collect())
        .collect();

    // Fast path 2: large groups — compare each sibling against sibling[0] in O(n·m).
    const PAIRWISE_LIMIT: usize = 200;
    if col_sets.len() > PAIRWISE_LIMIT {
        let reference = &col_sets[0];
        let mut min_j = 1.0_f64;
        for other in col_sets.iter().skip(1) {
            let intersection = reference.iter().filter(|&&c| other.contains(c)).count();
            let union = reference.len() + other.len() - intersection;
            let j_val = if union == 0 { 1.0 } else { intersection as f64 / union as f64 };
            if j_val < min_j {
                min_j = j_val;
                if min_j == 0.0 {
                    return 0.0;
                }
            }
        }
        return min_j;
    }

    // Full pairwise for small groups — exact result.
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
pub fn classify_key_shape(keys: &[&str]) -> KeyShape {
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

/// Apply NormalizeDynamicKeys strategy to a table: collapse all its direct Object children
/// into a single normalized table with `id_column` TEXT + union of value columns.
///
/// Equivalent to a user-triggered KeyedPivot with a custom ID column name.
/// Call `exclude_absorbed_children` after to remove the now-absorbed child tables.
pub fn apply_normalize_dynamic_keys(
    schemas: &mut Vec<TableSchema>,
    table_name: &str,
    id_column: String,
) {
    let target_idx = match schemas.iter().position(|s| s.name == table_name) {
        Some(i) => i,
        None => {
            eprintln!("WARNING: apply_normalize_dynamic_keys: table '{}' not found", table_name);
            return;
        }
    };

    let child_indices: Vec<usize> = schemas
        .iter()
        .enumerate()
        .filter(|(_, s)| {
            s.parent_table.as_deref() == Some(table_name)
                && matches!(s.child_kind, Some(ChildKind::Object))
        })
        .map(|(i, _)| i)
        .collect();

    if child_indices.is_empty() {
        eprintln!(
            "WARNING: apply_normalize_dynamic_keys: no Object children found for '{}'; strategy not applied",
            table_name
        );
        return;
    }

    let children: Vec<&TableSchema> = child_indices.iter().map(|&i| &schemas[i]).collect();
    let union_cols = build_union_columns(&children);

    let keys: Vec<String> = child_indices
        .iter()
        .map(|&i| schemas[i].path.last().cloned().unwrap_or_default())
        .collect();
    let key_shape = classify_key_shape(&keys.iter().map(|s| s.as_str()).collect::<Vec<_>>());

    let target = &mut schemas[target_idx];
    target.columns.retain(|c| c.is_generated);
    target.columns.push(ColumnSchema {
        name: id_column.clone(),
        original_name: id_column.clone(),
        pg_type: PgType::Text,
        not_null: true,
        is_generated: false,
        is_parent_fk: false,
    });
    for col in union_cols {
        target.columns.push(col);
    }
    eprintln!(
        "  NormalizeDynamicKeys: {} ({} child tables → 1, id_col: {} [{}])",
        table_name,
        child_indices.len(),
        id_column,
        key_shape,
    );
    target.wide_strategy = WideStrategy::NormalizeDynamicKeys { id_column };

    exclude_absorbed_children(schemas);
}

/// Apply Flatten strategy to a child table: inline its scalar columns into the parent table
/// with the given prefix. The child table is removed from the schema after inlining.
///
/// After this call, `schemas` no longer contains `child_table_name`. The parent table gains
/// new data columns and a populated `flatten_sources` map for Pass 2 lookups.
pub fn apply_flatten(
    schemas: &mut Vec<TableSchema>,
    child_table_name: &str,
    prefix: &str,
    max_depth: u8,
) {
    // Collect info before any mutations (avoids borrow conflicts)
    let (parent_name, field_name, new_cols) = {
        let child = match schemas.iter().find(|s| s.name == child_table_name) {
            Some(c) => c,
            None => {
                eprintln!("WARNING: apply_flatten: table '{}' not found", child_table_name);
                return;
            }
        };

        let parent_name = match child.parent_table.clone() {
            Some(p) => p,
            None => {
                eprintln!(
                    "WARNING: apply_flatten: '{}' is a root table, cannot flatten into parent",
                    child_table_name
                );
                return;
            }
        };

        // The JSON field name is the last path segment of the child table
        let field_name = child.path.last()
            .cloned()
            .unwrap_or_else(|| child_table_name.to_string());

        // Build prefixed copies of all data columns (max_depth=1: scalars only)
        let new_cols: Vec<ColumnSchema> = child
            .data_columns()
            .map(|col| ColumnSchema {
                name: format!("{}{}", prefix, col.name),
                original_name: col.original_name.clone(),
                pg_type: col.pg_type.clone(),
                not_null: false, // flattened columns are always nullable in parent
                is_generated: false,
                is_parent_fk: false,
            })
            .collect();

        (parent_name, field_name, new_cols)
    };

    // Mark child as Flatten so absorbs_children() returns true for its descendants
    if let Some(child) = schemas.iter_mut().find(|s| s.name == child_table_name) {
        child.wide_strategy = WideStrategy::Flatten { prefix: prefix.to_string(), max_depth };
    }

    // Remove descendants of the child (e.g. nutrients.sub_items)
    exclude_absorbed_children(schemas);

    // Add flattened columns + flatten_sources to parent
    if let Some(parent) = schemas.iter_mut().find(|s| s.name == parent_name) {
        for col in &new_cols {
            if !parent.columns.iter().any(|c| c.name == col.name) {
                parent.flatten_sources.insert(col.name.clone(), field_name.clone());
                parent.columns.push(col.clone());
            }
        }
        eprintln!(
            "  Flatten: {}.{} → {} columns inlined into {} (prefix: {:?})",
            parent_name,
            field_name,
            new_cols.len(),
            parent_name,
            prefix,
        );
    } else {
        eprintln!(
            "WARNING: apply_flatten: parent table '{}' not found for '{}'",
            parent_name, child_table_name
        );
    }

    // Remove the flattened child table from the schema
    schemas.retain(|s| !matches!(s.wide_strategy, WideStrategy::Flatten { .. }));
}

/// Inline a child table as a single JSONB column on the parent table.
/// The child table is removed from the schema; the parent gains `{child_table_name} JSONB`.
/// Used for WideStrategy::JsonbFlatten (IHM override "JSONB inline").
pub fn apply_jsonb_flatten(schemas: &mut Vec<TableSchema>, child_table_name: &str) {
    let (parent_name, field_name) = {
        let child = match schemas.iter().find(|s| s.name == child_table_name) {
            Some(c) => c,
            None => {
                eprintln!("WARNING: apply_jsonb_flatten: table '{}' not found", child_table_name);
                return;
            }
        };
        let parent = match child.parent_table.clone() {
            Some(p) => p,
            None => {
                eprintln!(
                    "WARNING: apply_jsonb_flatten: '{}' is a root table, cannot inline into parent",
                    child_table_name
                );
                return;
            }
        };
        // The JSON field name is the last path segment of the child table.
        let field = child.path.last()
            .cloned()
            .unwrap_or_else(|| child_table_name.to_string());
        (parent, field)
    };

    // Mark child as JsonbFlatten so absorbs_children() returns true for its descendants
    if let Some(child) = schemas.iter_mut().find(|s| s.name == child_table_name) {
        child.wide_strategy = WideStrategy::JsonbFlatten;
    }

    // Remove any nested children of the child table
    exclude_absorbed_children(schemas);

    // Add JSONB column to parent (SQL name = child table name, original = JSON field name).
    if let Some(parent) = schemas.iter_mut().find(|s| s.name == parent_name) {
        if !parent.columns.iter().any(|c| c.name == child_table_name) {
            parent.columns.push(ColumnSchema {
                name: child_table_name.to_string(),
                original_name: field_name,
                pg_type: PgType::Jsonb,
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
        }
    } else {
        eprintln!(
            "WARNING: apply_jsonb_flatten: parent table '{}' not found for '{}'",
            parent_name, child_table_name
        );
        return;
    }

    // Remove the child table and its absorbed descendants
    schemas.retain(|s| !matches!(s.wide_strategy, WideStrategy::JsonbFlatten));
}

fn type_histogram(tracker: &TypeTracker) -> Vec<(String, u64)> {
    let mut hist: Vec<(String, u64)> = tracker
        .iter_types()
        .filter(|(t, _)| !matches!(t, InferredType::Object | InferredType::Array))
        .map(|(t, n)| (format!("{:?}", t), n))
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

        // Must finish well under 500ms (O(N²) on 10k items = ~50M iterations ≈ 2s in debug)
        assert!(
            elapsed.as_millis() < 500,
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
}
