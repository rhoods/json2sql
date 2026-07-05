//! Phase 1 — construction des schémas de base, par table (parallèle via rayon).
//!
//! - fn `build_base_schemas` — construit les schémas de base.
//! - fn `build_entry_schema_base` — un `TableSchema` par `TableEntry`.
//! - fn `build_data_columns` — colonnes de données + détection de collisions.
//! - fn `push_array_columns` — colonnes array-as-column.
//! - fn `push_generated_columns` — ajoute `j2s_id`/FK/`j2s_order`.

use rayon::prelude::*;

use super::super::naming::{ColumnCollision, ColumnNameRegistry, NamingRegistry};
use super::super::observer::TableEntry;
use super::super::table_schema::{ColumnSchema, TableSchema};
use super::super::type_tracker::PgType;

/// Phase 1: build base `TableSchema` per `TableEntry` WITHOUT wide-table strategies.
/// Each table is processed independently (parallel). Returns schemas with initial dedup applied.
pub(super) fn build_base_schemas(
    tables: &indexmap::IndexMap<String, TableEntry>,
    naming: &NamingRegistry,
    _tables_with_object_children: &std::collections::HashSet<String>,
) -> (Vec<TableSchema>, Vec<ColumnCollision>) {
    let entries: Vec<&TableEntry> = tables.values().collect();
    let results: Vec<(TableSchema, Vec<ColumnCollision>)> = entries
        .par_iter()
        .map(|entry| build_entry_schema_base(entry, naming))
        .collect();
    let mut schemas: Vec<TableSchema> = Vec::with_capacity(results.len());
    let mut all_collisions: Vec<ColumnCollision> = Vec::new();
    for (schema, collisions) in results {
        schemas.push(schema);
        all_collisions.extend(collisions);
    }
    {
        let mut seen = std::collections::HashSet::new();
        schemas.retain(|s| seen.insert(s.name.clone()));
    }
    // Secondary sort by name within each depth level ensures deterministic ordering
    schemas.sort_by(|a, b| a.depth.cmp(&b.depth).then_with(|| a.name.cmp(&b.name)));
    (schemas, all_collisions)
}

fn push_generated_columns(schema: &mut TableSchema) {
    schema.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
    if let Some(ref p) = schema.parent_table {
        schema.columns.push(ColumnSchema::parent_fk(p));
    }
    if schema.has_order_column() {
        schema.columns.push(ColumnSchema::generated("j2s_order", PgType::BigInt));
    }
}

/// Build the base `TableSchema` for a single `TableEntry` WITHOUT applying wide-table strategies.
///
/// Pure function — no access to `SchemaRegistry` state. Called in parallel via rayon.
fn build_entry_schema_base(
    entry: &TableEntry,
    naming: &NamingRegistry,
) -> (TableSchema, Vec<ColumnCollision>) {
    let pg_name = naming.table_name_lookup_from_dot_key(&entry.path_key);
    let depth = entry.path.len().saturating_sub(1);
    let parent_table: Option<String> = if entry.parent_key.is_empty() {
        None
    } else {
        Some(naming.table_name_lookup_from_dot_key(&entry.parent_key))
    };

    let mut schema = TableSchema::new(pg_name.clone(), entry.path.clone(), depth);
    schema.parent_table = parent_table;
    schema.child_kind.clone_from(&entry.child_kind);
    schema.row_count = entry.row_count;

    push_generated_columns(&mut schema);

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
        return (schema, Vec::new());
    }

    let local_collisions = build_data_columns(&mut schema, entry, &pg_name);
    (schema, local_collisions)
}


/// Build regular data columns and array-as-column fields into `schema`.
///
/// Returns column collisions detected during name registration.
fn build_data_columns(
    schema: &mut TableSchema,
    entry: &TableEntry,
    pg_name: &str,
) -> Vec<ColumnCollision> {
    let mut col_registry = ColumnNameRegistry::new();
    for (original_field, tracker) in &entry.columns {
        if !tracker.is_object_field() && !tracker.is_array_field() {
            col_registry.register(original_field);
        }
    }
    for original_field in entry.array_columns.keys() {
        col_registry.register(original_field);
    }
    col_registry.build(pg_name);
    let local_collisions = col_registry.collisions().to_vec();

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

    push_array_columns(schema, entry, &col_registry);
    local_collisions
}

fn push_array_columns(schema: &mut TableSchema, entry: &TableEntry, col_registry: &ColumnNameRegistry) {
    for (original_field, elem_tracker) in &entry.array_columns {
        let elem_type = elem_tracker.to_pg_type();
        schema.columns.push(ColumnSchema {
            name: col_registry.resolve(original_field),
            original_name: original_field.clone(),
            pg_type: PgType::Array(Box::new(elem_type)),
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
    }
}
