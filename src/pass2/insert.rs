use std::collections::HashMap;

use serde_json::Value;
use uuid::Uuid;

use crate::anomaly::collector::AnomalyCollector;
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

#[allow(clippy::too_many_arguments, clippy::too_many_lines, clippy::cognitive_complexity)]
// debt: monolithic JSON traversal — candidate for InsertContext struct + sub-functions
pub(crate) fn insert_object<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
    parent_id: Option<Uuid>,
    order: Option<i64>,
) -> Result<()> {
    // Pre-compute the parent path key once — reused for every child field lookup below.
    let parent_path_key = schema.path.join(&PATH_SEP.to_string());

    // Special case: root table (no parent) with Jsonb strategy set via config override.
    // Write the full object as a JSONB blob, then still recurse into child tables so
    // that children (e.g. products_nutriments) receive their data.
    if matches!(schema.wide_strategy, WideStrategy::Jsonb) && parent_id.is_none() {
        let mut builder = RowBuilder::new();
        builder.push_uuid(row_id); // j2s_id (no j2s_parent_id for root)
        let json_str =
            serde_json::to_string(&Value::Object(obj.clone())).unwrap_or_default();
        match escape_copy_text(&json_str) {
            Some(escaped) => builder.push_value(&escaped),
            None => builder.push_null(), // null byte in JSON — treat as NULL, not empty string
        }
        anomalies.inc_total(&schema.name);
        if let Some(sink) = sinks.get_mut(&schema.name) {
            sink.write_row(builder.finish())?;
        }
        // Recurse into child fields so their tables still get populated.
        for (field, value) in obj {
            let child_key = format!("{}{}{}", parent_path_key, PATH_SEP, field);
            match value {
                Value::Object(nested) => {
                    if let Some(child_schema) = path_map.get(&child_key) {
                        match &child_schema.wide_strategy {
                            WideStrategy::Pivot => {
                                insert_pivot_object(sinks, anomalies, child_schema, nested, row_id)?;
                            }
                            WideStrategy::Jsonb => {
                                insert_jsonb_object(path_map, sinks, anomalies, child_schema, value, row_id)?;
                            }
                            WideStrategy::StructuredPivot(suffix_schema) => {
                                insert_structured_pivot_object(
                                    sinks, anomalies, child_schema, nested, row_id, suffix_schema,
                                )?;
                            }
                            WideStrategy::KeyedPivot(sibling_schema) => {
                                insert_keyed_pivot_object(
                                    path_map, sinks, anomalies, child_schema, nested, row_id, sibling_schema,
                                )?;
                            }
                            WideStrategy::MultiKeyedPivot(groups) => {
                                insert_multi_keyed_pivot(
                                    path_map, sinks, anomalies, child_schema, nested, row_id, groups,
                                )?;
                            }
                            WideStrategy::NormalizeDynamicKeys { id_column } => {
                                insert_normalize_dynamic_keys(
                                    sinks, anomalies, child_schema, nested, row_id, id_column,
                                )?;
                            }
                            WideStrategy::Columns
                            | WideStrategy::AutoSplit { .. }
                            | WideStrategy::Ignore
                            | WideStrategy::Flatten { .. }
                            | WideStrategy::JsonbFlatten => {
                                let child_id = Uuid::now_v7();
                                insert_object(
                                    path_map, sinks, anomalies, child_schema,
                                    nested, child_id, Some(row_id), None,
                                )?;
                            }
                        }
                    }
                }
                Value::Array(arr) => {
                    if let Some(child_schema) = path_map.get(&child_key) {
                        insert_array(path_map, sinks, anomalies, child_schema, arr, row_id)?;
                    }
                }
                _ => {}
            }
        }
        return Ok(());
    }

    let mut builder = RowBuilder::new();

    for col in &schema.columns {
        if col.is_generated {
            if col.is_parent_fk {
                match parent_id {
                    Some(pid) => builder.push_uuid(pid),
                    None => builder.push_null(),
                }
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
            continue;
        }

        // For columns inlined via Flatten strategy, look up the value in the nested object.
        // flatten_sources maps column name → source JSON field (e.g. "nutrients_calories" → "nutrients").
        let json_val = if let Some(source_field) = schema.flatten_sources.get(col.name.as_str()) {
            obj.get(source_field.as_str())
                .and_then(|v| v.as_object())
                .and_then(|nested| nested.get(col.original_name.as_str()))
                .unwrap_or(&Value::Null)
        } else {
            obj.get(&col.original_name).unwrap_or(&Value::Null)
        };

        // JSONB columns (added by JsonbFlatten) accept any JSON value, including objects
        // and arrays — serialize the raw value directly.
        if matches!(col.pg_type, crate::schema::type_tracker::PgType::Jsonb) {
            if matches!(json_val, Value::Null) {
                builder.push_null();
            } else {
                let json_str = serde_json::to_string(json_val).unwrap_or_default();
                match escape_copy_text(&json_str) {
                    Some(escaped) => builder.push_value(&escaped),
                    None => builder.push_null(),
                }
            }
            continue;
        }

        // Objects and non-array-typed arrays become child tables, not columns.
        // Arrays typed as PgType::Array fall through to coerce() below.
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
                anomalies.record(
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

    anomalies.inc_total(&schema.name);

    if let Some(sink) = sinks.get_mut(&schema.name) {
        sink.write_row(builder.finish())?;
    }

    // AutoSplit: write medium-frequency key-value pairs as EAV rows in the companion _wide table.
    // Stable keys were already written above (they're schema columns). Children are recursed below.
    // Medium keys are scalars only — objects/arrays were excluded when medium_keys was built.
    if let WideStrategy::AutoSplit { medium_keys, wide_table_name, .. } = &schema.wide_strategy {
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
            wb.push_uuid(wide_id);   // j2s_id
            wb.push_uuid(row_id);    // j2s_parent_id (anchor)
            // JSON field names can contain COPY-unsafe chars (\t, \n, \\, \0).
            match escape_copy_text(field) {
                Some(escaped) => wb.push_value(&escaped),
                None => wb.push_null(), // null byte in key — treat as NULL
            }
            match &wide_value_type {
                Some(pg_type) => match coerce(value, pg_type) {
                    CoerceResult::Ok(s) => wb.push_value(&s),
                    CoerceResult::Null => wb.push_null(),
                    CoerceResult::Anomaly { actual_value, actual_type } => {
                        anomalies.record(
                            wide_table_name, "value", &wide_id.to_string(),
                            &pg_type.as_sql(), &actual_value, actual_type,
                        )?;
                        wb.push_null();
                    }
                },
                None => wb.push_null(),
            }
            anomalies.inc_total(wide_table_name);
            if let Some(sink) = sinks.get_mut(wide_table_name.as_str()) {
                sink.write_row(wb.finish())?;
            }
        }
    }

    // Recurse into child fields
    for (field, value) in obj {
        let child_key = format!("{}{}{}", parent_path_key, PATH_SEP, field);

        match value {
            Value::Object(nested) => {
                if let Some(child_schema) = path_map.get(&child_key) {
                    match &child_schema.wide_strategy {
                        WideStrategy::Pivot => {
                            insert_pivot_object(sinks, anomalies, child_schema, nested, row_id)?;
                        }
                        WideStrategy::Jsonb => {
                            insert_jsonb_object(path_map, sinks, anomalies, child_schema, value, row_id)?;
                        }
                        WideStrategy::StructuredPivot(suffix_schema) => {
                            insert_structured_pivot_object(
                                sinks, anomalies, child_schema, nested, row_id, suffix_schema,
                            )?;
                        }
                        WideStrategy::KeyedPivot(sibling_schema) => {
                            insert_keyed_pivot_object(
                                path_map, sinks, anomalies, child_schema, nested, row_id, sibling_schema,
                            )?;
                        }
                        WideStrategy::MultiKeyedPivot(groups) => {
                            insert_multi_keyed_pivot(
                                path_map, sinks, anomalies, child_schema, nested, row_id, groups,
                            )?;
                        }
                        WideStrategy::NormalizeDynamicKeys { id_column } => {
                            insert_normalize_dynamic_keys(
                                sinks, anomalies, child_schema, nested, row_id, id_column,
                            )?;
                        }
                        WideStrategy::Columns
                        | WideStrategy::AutoSplit { .. }
                        | WideStrategy::Ignore
                        | WideStrategy::Flatten { .. }
                        | WideStrategy::JsonbFlatten => {
                            let child_id = Uuid::now_v7();
                            insert_object(
                                path_map, sinks, anomalies, child_schema,
                                nested, child_id, Some(row_id), None,
                            )?;
                        }
                    }
                }
            }
            Value::Array(arr) => {
                if let Some(child_schema) = path_map.get(&child_key) {
                    insert_array(
                        path_map, sinks, anomalies, child_schema,
                        arr, row_id,
                    )?;
                }
            }
            _ => {} // scalar — already handled above
        }
    }

    Ok(())
}
