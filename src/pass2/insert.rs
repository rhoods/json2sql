use std::collections::HashMap;

use serde_json::Value;
use uuid::Uuid;

use crate::anomaly::collect::AnomalyCollect;
use crate::db::copy_sink::RowBuilder;
use crate::db::copy_text::{escape_copy_text, CopyEscaped};
use crate::error::Result;
use crate::pass2::coercer::{coerce, CoerceResult};
use crate::pass2::sink::RowSink;
use crate::schema::PATH_SEP;
use crate::schema::table_schema::{TableSchema, WideStrategy};

use super::traversal::{
    insert_array, insert_jsonb_object, insert_keyed_pivot_object, insert_multi_keyed_pivot,
    insert_normalize_dynamic_keys, insert_pivot_object, insert_structured_pivot_object,
};

/// Mutable context for the recursive insert pass.
/// Separates `path_map` (immutable, borrowed independently during child lookups)
/// from the mutable handles so field-level borrows remain valid across recursive calls.
pub(crate) struct InsertCtx<'a, S, A> {
    pub(crate) sinks: &'a mut HashMap<String, S>,
    pub(crate) anomalies: &'a mut A,
}

pub(crate) fn insert_object<S: RowSink, A: AnomalyCollect>(
    path_map: &HashMap<String, TableSchema>,
    ctx: &mut InsertCtx<'_, S, A>,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
    parent_id: Option<Uuid>,
    order: Option<i64>,
) -> Result<()> {
    // Special case: root table with Jsonb strategy — write the full object as a JSONB blob,
    // then still recurse into child tables so they receive their data.
    if matches!(schema.wide_strategy, WideStrategy::Jsonb) && parent_id.is_none() {
        return write_root_jsonb(path_map, ctx, schema, obj, row_id);
    }

    let mut builder = RowBuilder::new();

    for col in &schema.columns {
        if col.is_generated {
            push_generated_col_insert(&mut builder, col, row_id, parent_id, order);
            continue;
        }

        // For columns inlined via Flatten strategy, look up the value in the nested object.
        let json_val = if let Some(source_field) = schema.flatten_sources.get(col.name.as_str()) {
            obj.get(source_field.as_str())
                .and_then(|v| v.as_object())
                .and_then(|nested| nested.get(col.original_name.as_str()))
                .unwrap_or(&Value::Null)
        } else {
            obj.get(&col.original_name).unwrap_or(&Value::Null)
        };

        // JSONB columns accept any JSON value — serialize the raw value directly.
        if matches!(col.pg_type, crate::schema::type_tracker::PgType::Jsonb) {
            push_jsonb_col(&mut builder, json_val);
            continue;
        }

        // Objects and non-array-typed arrays become child tables, not columns.
        if matches!(json_val, Value::Object(_))
            || (matches!(json_val, Value::Array(_))
                && !matches!(col.pg_type, crate::schema::type_tracker::PgType::Array(_)))
        {
            builder.push_null();
            continue;
        }

        match coerce(json_val, &col.pg_type) {
            CoerceResult::Ok(s) => builder.push_value(&s),
            CoerceResult::Null => builder.push_null(),
            CoerceResult::Anomaly { actual_value, actual_type } => {
                ctx.anomalies.record(
                    &schema.name,
                    &col.name,
                    &row_id.to_string(),
                    &col.pg_type.as_sql(),
                    &actual_value,
                    actual_type,
                )?;
                builder.push_null();
            }
        }
    }

    ctx.anomalies.inc_total(&schema.name);
    if let Some(sink) = ctx.sinks.get_mut(&schema.name) {
        sink.write_row(builder.finish())?;
    }

    write_autosplit_rows(path_map, ctx, schema, obj, row_id)?;
    recurse_children(path_map, ctx, schema, obj, row_id)?;
    Ok(())
}

fn push_generated_col_insert(
    builder: &mut RowBuilder,
    col: &crate::schema::table_schema::ColumnSchema,
    row_id: Uuid,
    parent_id: Option<Uuid>,
    order: Option<i64>,
) {
    if col.is_parent_fk {
        match parent_id { Some(pid) => builder.push_uuid(pid), None => builder.push_null() }
    } else {
        match col.name.as_str() {
            "j2s_id" => builder.push_uuid(row_id),
            "j2s_order" => match order {
                Some(ord) => builder.push_value(&CopyEscaped::from_safe_ascii(ord.to_string())),
                None => builder.push_null(),
            },
            _ => builder.push_null(),
        }
    }
}

fn push_jsonb_col(builder: &mut RowBuilder, json_val: &Value) {
    if matches!(json_val, Value::Null) { builder.push_null(); return; }
    let json_str = serde_json::to_string(json_val).unwrap_or_default();
    match escape_copy_text(&json_str) {
        Some(escaped) => builder.push_value(&escaped),
        None => builder.push_null(),
    }
}

/// Write the full object as a JSONB blob for a root table with Jsonb strategy,
/// then recurse into child fields so their tables still get populated.
fn write_root_jsonb<S: RowSink, A: AnomalyCollect>(
    path_map: &HashMap<String, TableSchema>,
    ctx: &mut InsertCtx<'_, S, A>,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
) -> Result<()> {
    let parent_path_key = schema.path.join(&PATH_SEP.to_string());
    let mut builder = RowBuilder::new();
    builder.push_uuid(row_id); // j2s_id (no j2s_parent_id for root)
    let json_str = serde_json::to_string(&Value::Object(obj.clone())).unwrap_or_default();
    match escape_copy_text(&json_str) {
        Some(escaped) => builder.push_value(&escaped),
        None => builder.push_null(), // null byte in JSON — treat as NULL
    }
    ctx.anomalies.inc_total(&schema.name);
    if let Some(sink) = ctx.sinks.get_mut(&schema.name) {
        sink.write_row(builder.finish())?;
    }
    for (field, value) in obj {
        let child_key = format!("{}{}{}", parent_path_key, PATH_SEP, field);
        match value {
            Value::Object(nested) => {
                if let Some(child_schema) = path_map.get(&child_key) {
                    dispatch_child_object(path_map, ctx, child_schema, nested, value, row_id)?;
                }
            }
            Value::Array(arr) => {
                if let Some(child_schema) = path_map.get(&child_key) {
                    insert_array(path_map, ctx.sinks, ctx.anomalies, child_schema, arr, row_id)?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Dispatch a child Object value to the appropriate insertion function based on its WideStrategy.
fn dispatch_child_object<S: RowSink, A: AnomalyCollect>(
    path_map: &HashMap<String, TableSchema>,
    ctx: &mut InsertCtx<'_, S, A>,
    child_schema: &TableSchema,
    nested: &serde_json::Map<String, Value>,
    value: &Value,
    parent_id: Uuid,
) -> Result<()> {
    match &child_schema.wide_strategy {
        WideStrategy::Pivot => {
            insert_pivot_object(ctx.sinks, ctx.anomalies, child_schema, nested, parent_id)?;
        }
        WideStrategy::Jsonb => {
            insert_jsonb_object(path_map, ctx.sinks, ctx.anomalies, child_schema, value, parent_id)?;
        }
        WideStrategy::StructuredPivot(suffix_schema) => {
            insert_structured_pivot_object(
                ctx.sinks, ctx.anomalies, child_schema, nested, parent_id, suffix_schema,
            )?;
        }
        WideStrategy::KeyedPivot(sibling_schema) => {
            insert_keyed_pivot_object(
                path_map, ctx.sinks, ctx.anomalies, child_schema, nested, parent_id, sibling_schema,
            )?;
        }
        WideStrategy::MultiKeyedPivot(groups) => {
            insert_multi_keyed_pivot(
                path_map, ctx.sinks, ctx.anomalies, child_schema, nested, parent_id, groups,
            )?;
        }
        WideStrategy::NormalizeDynamicKeys { id_column } => {
            insert_normalize_dynamic_keys(
                ctx.sinks, ctx.anomalies, child_schema, nested, parent_id, id_column,
            )?;
        }
        WideStrategy::Columns
        | WideStrategy::AutoSplit { .. }
        | WideStrategy::Ignore
        | WideStrategy::Flatten { .. }
        | WideStrategy::JsonbFlatten => {
            let child_id = Uuid::now_v7();
            insert_object(path_map, ctx, child_schema, nested, child_id, Some(parent_id), None)?;
        }
    }
    Ok(())
}

/// Write medium-frequency key-value pairs as EAV rows in the AutoSplit companion wide table.
/// Stable keys were already written as schema columns; rare keys are skipped entirely.
fn write_autosplit_rows<S: RowSink, A: AnomalyCollect>(
    path_map: &HashMap<String, TableSchema>,
    ctx: &mut InsertCtx<'_, S, A>,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
) -> Result<()> {
    let WideStrategy::AutoSplit { medium_keys, wide_table_name, .. } = &schema.wide_strategy else {
        return Ok(());
    };
    let wide_value_type = path_map
        .get(wide_table_name.as_str())
        .and_then(|ws| ws.find_by_original("value"))
        .map(|c| c.pg_type.clone());
    for (field, value) in obj {
        if !medium_keys.contains(field.as_str()) {
            continue;
        }
        if matches!(value, Value::Object(_) | Value::Array(_)) {
            continue;
        }
        let wide_id = Uuid::now_v7();
        let mut wb = RowBuilder::new();
        wb.push_uuid(wide_id);  // j2s_id
        wb.push_uuid(row_id);   // j2s_parent_id (anchor)
        // JSON field names can contain COPY-unsafe chars (\t, \n, \\, \0).
        match escape_copy_text(field) {
            Some(escaped) => wb.push_value(&escaped),
            None => wb.push_null(),
        }
        match &wide_value_type {
            Some(pg_type) => match coerce(value, pg_type) {
                CoerceResult::Ok(s) => wb.push_value(&s),
                CoerceResult::Null => wb.push_null(),
                CoerceResult::Anomaly { actual_value, actual_type } => {
                    ctx.anomalies.record(
                        wide_table_name, "value", &wide_id.to_string(),
                        &pg_type.as_sql(), &actual_value, actual_type,
                    )?;
                    wb.push_null();
                }
            },
            None => wb.push_null(),
        }
        ctx.anomalies.inc_total(wide_table_name);
        if let Some(sink) = ctx.sinks.get_mut(wide_table_name.as_str()) {
            sink.write_row(wb.finish())?;
        }
    }
    Ok(())
}

/// Recurse into child fields of `obj`, routing each to the appropriate insertion function.
fn recurse_children<S: RowSink, A: AnomalyCollect>(
    path_map: &HashMap<String, TableSchema>,
    ctx: &mut InsertCtx<'_, S, A>,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
) -> Result<()> {
    let parent_path_key = schema.path.join(&PATH_SEP.to_string());
    for (field, value) in obj {
        let child_key = format!("{}{}{}", parent_path_key, PATH_SEP, field);
        match value {
            Value::Object(nested) => {
                if let Some(child_schema) = path_map.get(&child_key) {
                    dispatch_child_object(path_map, ctx, child_schema, nested, value, row_id)?;
                }
            }
            Value::Array(arr) => {
                if let Some(child_schema) = path_map.get(&child_key) {
                    insert_array(path_map, ctx.sinks, ctx.anomalies, child_schema, arr, row_id)?;
                }
            }
            _ => {} // scalar — already handled as a column above
        }
    }
    Ok(())
}
