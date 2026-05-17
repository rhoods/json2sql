use std::collections::{BTreeMap, HashMap};

use serde_json::Value;
use uuid::Uuid;

use crate::anomaly::collector::AnomalyCollector;
use crate::db::copy_sink::{RowBuilder, TempFileSink};
use crate::db::copy_text::{escape_copy_text, CopyEscaped};
use crate::error::Result;
use crate::pass2::coercer::{coerce, CoerceResult};
use crate::schema::PATH_SEP;
use crate::schema::table_schema::{ChildKind, SiblingGroup, SiblingSchema, SuffixSchema, TableSchema, WideStrategy};

pub(crate) fn insert_object(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, TempFileSink>,
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

/// Insert one row per key-value pair for a Pivot wide table.
/// Columns: j2s_id, j2s_parent_id, key TEXT, value <type>
fn insert_pivot_object(
    sinks: &mut HashMap<String, TempFileSink>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
) -> Result<()> {
    let value_col = schema.find_by_original("value");
    for (key, val) in obj {
        let child_id = Uuid::now_v7();
        let mut builder = RowBuilder::new();
        builder.push_uuid(child_id);   // j2s_id
        builder.push_uuid(parent_id);  // j2s_parent_id
        match escape_copy_text(key) {
            Some(escaped) => builder.push_value(&escaped),
            None => builder.push_null(),
        }
        if let Some(col) = value_col {
            match coerce(val, &col.pg_type) {
                CoerceResult::Ok(s) => builder.push_value(&s),
                CoerceResult::Null => builder.push_null(),
                CoerceResult::Anomaly { actual_value, actual_type } => {
                    anomalies.record(
                        &schema.name, "value", &child_id.to_string(),
                        &col.pg_type.as_sql(), &actual_value, actual_type,
                    )?;
                    builder.push_null();
                }
            }
        } else {
            builder.push_null();
        }
        anomalies.inc_total(&schema.name);
        if let Some(sink) = sinks.get_mut(&schema.name) {
            sink.write_row(builder.finish())?;
        }
    }
    Ok(())
}

/// Insert one row containing the entire object serialized as JSONB, then recurse into
/// any children of this table so they still receive data.
/// Columns: j2s_id, j2s_parent_id, data JSONB
fn insert_jsonb_object(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, TempFileSink>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    value: &Value,
    parent_id: Uuid,
) -> Result<()> {
    let child_id = Uuid::now_v7();
    let mut builder = RowBuilder::new();
    builder.push_uuid(child_id);   // j2s_id
    builder.push_uuid(parent_id);  // j2s_parent_id
    let json_str = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string());
    match escape_copy_text(&json_str) {
        Some(escaped) => builder.push_value(&escaped),
        None => builder.push_null(), // null byte in JSON — treat as NULL, not empty string
    }
    anomalies.inc_total(&schema.name);
    if let Some(sink) = sinks.get_mut(&schema.name) {
        sink.write_row(builder.finish())?;
    }

    // Recurse into children of this JSONB table so they still receive data.
    if let Value::Object(obj) = value {
        let parent_path_key = schema.path.join(&PATH_SEP.to_string());
        for (field, child_value) in obj {
            let child_key = format!("{}{}{}", parent_path_key, PATH_SEP, field);
            match child_value {
                Value::Object(nested) => {
                    if let Some(child_schema) = path_map.get(&child_key) {
                        match &child_schema.wide_strategy {
                            WideStrategy::Pivot => {
                                insert_pivot_object(sinks, anomalies, child_schema, nested, child_id)?;
                            }
                            WideStrategy::Jsonb => {
                                insert_jsonb_object(path_map, sinks, anomalies, child_schema, child_value, child_id)?;
                            }
                            WideStrategy::StructuredPivot(suffix_schema) => {
                                insert_structured_pivot_object(
                                    sinks, anomalies, child_schema, nested, child_id, suffix_schema,
                                )?;
                            }
                            WideStrategy::KeyedPivot(sibling_schema) => {
                                insert_keyed_pivot_object(
                                    path_map, sinks, anomalies, child_schema, nested, child_id, sibling_schema,
                                )?;
                            }
                            WideStrategy::MultiKeyedPivot(groups) => {
                                insert_multi_keyed_pivot(
                                    path_map, sinks, anomalies, child_schema, nested, child_id, groups,
                                )?;
                            }
                            WideStrategy::NormalizeDynamicKeys { id_column } => {
                                insert_normalize_dynamic_keys(
                                    sinks, anomalies, child_schema, nested, child_id, id_column,
                                )?;
                            }
                            WideStrategy::Columns
                            | WideStrategy::AutoSplit { .. }
                            | WideStrategy::Ignore
                            | WideStrategy::Flatten { .. }
                            | WideStrategy::JsonbFlatten => {
                                let grandchild_id = Uuid::now_v7();
                                insert_object(
                                    path_map, sinks, anomalies, child_schema,
                                    nested, grandchild_id, Some(child_id), None,
                                )?;
                            }
                        }
                    }
                }
                Value::Array(arr) => {
                    if let Some(child_schema) = path_map.get(&child_key) {
                        insert_array(path_map, sinks, anomalies, child_schema, arr, child_id)?;
                    }
                }
                _ => {}
            }
        }
    }

    Ok(())
}

/// Insert one row per base name for a StructuredPivot wide table.
/// Columns: j2s_id, j2s_parent_id, name TEXT, value <type>, <suffix cols...>
///
/// For each JSON key, we check whether it ends with a known suffix.
/// Keys that match no suffix are treated as bare base keys (→ `value` column).
/// All keys sharing the same base are collapsed into a single row.
fn insert_structured_pivot_object(
    sinks: &mut HashMap<String, TempFileSink>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    suffix_schema: &SuffixSchema,
) -> Result<()> {
    // Group keys by base name: base → { "" for bare, "_suffix" for suffix keys }
    let mut groups: BTreeMap<String, HashMap<String, &Value>> = BTreeMap::new();

    for (key, val) in obj {
        let mut matched_suffix: Option<&str> = None;
        for sc in &suffix_schema.suffix_cols {
            if key.len() > sc.suffix.len() && key.ends_with(sc.suffix.as_str()) {
                matched_suffix = Some(sc.suffix.as_str());
                break;
            }
        }
        match matched_suffix {
            Some(suffix) => {
                let base = &key[..key.len() - suffix.len()];
                groups
                    .entry(base.to_string())
                    .or_default()
                    .insert(suffix.to_string(), val);
            }
            None => {
                // bare base key — goes into the "value" column
                groups
                    .entry(key.clone())
                    .or_default()
                    .insert(String::new(), val);
            }
        }
    }

    for (base, suffix_vals) in groups {
        let child_id = Uuid::now_v7();
        let mut builder = RowBuilder::new();

        for col in &schema.columns {
            if col.is_generated {
                if col.is_parent_fk {
                    builder.push_uuid(parent_id);
                } else {
                    match col.name.as_str() {
                        "j2s_id" => builder.push_uuid(child_id),
                        _ => builder.push_null(),
                    }
                }
                continue;
            }

            // `name` column: the base string (may contain COPY-unsafe chars)
            if col.original_name == "name" {
                match escape_copy_text(&base) {
                    Some(escaped) => builder.push_value(&escaped),
                    None => builder.push_null(),
                }
                continue;
            }

            // `value` column: bare base key (empty suffix)
            if col.original_name == "value" {
                if let Some(val) = suffix_vals.get("") {
                    match coerce(val, &col.pg_type) {
                        CoerceResult::Ok(s) => builder.push_value(&s),
                        CoerceResult::Null => builder.push_null(),
                        CoerceResult::Anomaly { actual_value, actual_type } => {
                            anomalies.record(
                                &schema.name, &col.name, &child_id.to_string(),
                                &col.pg_type.as_sql(), &actual_value, actual_type,
                            )?;
                            builder.push_null();
                        }
                    }
                } else {
                    builder.push_null();
                }
                continue;
            }

            // Suffix column: original_name is the suffix string e.g. "_100g"
            if let Some(val) = suffix_vals.get(&col.original_name) {
                match coerce(val, &col.pg_type) {
                    CoerceResult::Ok(s) => builder.push_value(&s),
                    CoerceResult::Null => builder.push_null(),
                    CoerceResult::Anomaly { actual_value, actual_type } => {
                        anomalies.record(
                            &schema.name, &col.name, &child_id.to_string(),
                            &col.pg_type.as_sql(), &actual_value, actual_type,
                        )?;
                        builder.push_null();
                    }
                }
            } else {
                builder.push_null();
            }
        }

        anomalies.inc_total(&schema.name);
        if let Some(sink) = sinks.get_mut(&schema.name) {
            sink.write_row(builder.finish())?;
        }
    }

    Ok(())
}

/// Route sub-objects and sub-arrays declared in `schema.child_routes` to their target tables.
/// Called after writing each pivot row so that nested tables created by cascade merging
/// receive their data with the correct parent FK.
fn dispatch_child_routes(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, TempFileSink>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    child_obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
) -> Result<()> {
    if schema.child_routes.is_empty() {
        return Ok(());
    }
    for (sub_key, child_table_name) in &schema.child_routes {
        let sub_value = match child_obj.get(sub_key) {
            Some(v) => v,
            None => continue,
        };
        let child_schema = match path_map.values().find(|s| s.name == *child_table_name) {
            Some(s) => s,
            None => continue,
        };
        match sub_value {
            Value::Object(nested) => {
                match &child_schema.wide_strategy {
                    WideStrategy::KeyedPivot(ss) => {
                        let ss = ss.clone();
                        insert_keyed_pivot_object(path_map, sinks, anomalies, child_schema, nested, row_id, &ss)?;
                    }
                    WideStrategy::MultiKeyedPivot(groups) => {
                        let groups = groups.clone();
                        insert_multi_keyed_pivot(path_map, sinks, anomalies, child_schema, nested, row_id, &groups)?;
                    }
                    _ => {
                        let child_id = Uuid::now_v7();
                        insert_object(path_map, sinks, anomalies, child_schema, nested, child_id, Some(row_id), None)?;
                    }
                }
            }
            Value::Array(arr) => {
                insert_array(path_map, sinks, anomalies, child_schema, arr, row_id)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Insert one row per sibling key for a KeyedPivot table.
/// Columns: j2s_id, j2s_parent_id, key TEXT, <union data cols...>
///
/// Each key in `obj` maps to a child object; the key becomes the `key_col`,
/// and the child object's scalar fields are spread across the union columns.
/// Non-Object values (scalars, arrays) are skipped.
///
/// When `sibling_schema.array_children` is true, each key maps to an array of objects
/// instead of a single object. One row is emitted per array element, with j2s_order set.
fn insert_keyed_pivot_object(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, TempFileSink>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    sibling_schema: &SiblingSchema,
) -> Result<()> {
    if sibling_schema.array_children {
        return insert_keyed_pivot_array_of_objects(path_map, sinks, anomalies, schema, obj, parent_id, sibling_schema);
    }

    for (key, value) in obj {
        let child_obj = match value {
            Value::Object(o) => o,
            _ => continue, // skip scalars and arrays
        };

        let row_id = Uuid::now_v7();
        let mut builder = RowBuilder::new();

        for col in &schema.columns {
            // data JSONB: is_generated but requires actual serialization — check before the
            // generated dispatch block which would otherwise emit NULL for unknown names.
            if col.name == sibling_schema.data_col_name {
                let json_str = serde_json::to_string(child_obj).unwrap_or_default();
                match escape_copy_text(&json_str) {
                    Some(escaped) => builder.push_value(&escaped),
                    None => builder.push_null(),
                }
                continue;
            }

            if col.is_generated {
                if col.is_parent_fk {
                    builder.push_uuid(parent_id);
                } else {
                    match col.name.as_str() {
                        "j2s_id" => builder.push_uuid(row_id),
                        _ => builder.push_null(),
                    }
                }
                continue;
            }

            // Key column: the original JSON key of this sibling (may contain COPY-unsafe chars)
            if col.original_name == sibling_schema.key_col_name {
                match escape_copy_text(key) {
                    Some(escaped) => builder.push_value(&escaped),
                    None => builder.push_null(),
                }
                continue;
            }

            // Data column: look up in child object by original field name
            let json_val = child_obj.get(&col.original_name).unwrap_or(&Value::Null);

            // Sub-objects and arrays within the child → NULL (they have no column)
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
                        &schema.name, &col.name, &row_id.to_string(),
                        &col.pg_type.as_sql(), &actual_value, actual_type,
                    )?;
                    builder.push_null();
                }
            }
        }

        anomalies.inc_total(&schema.name);
        if let Some(sink) = sinks.get_mut(&schema.name) {
            sink.write_row(builder.finish())?;
        }

        // Route sub-objects/arrays declared in child_routes to their target tables.
        dispatch_child_routes(path_map, sinks, anomalies, schema, child_obj, row_id)?;
    }
    Ok(())
}

/// Insert one row per array element per sibling key for an ObjectArray KeyedPivot table.
/// Columns: j2s_id, j2s_parent_id, j2s_order BIGINT, key TEXT, <union data cols...>
///
/// Each key in `obj` maps to an array of objects. One row is emitted per element,
/// with j2s_order tracking the element's position within the array.
fn insert_keyed_pivot_array_of_objects(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, TempFileSink>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    sibling_schema: &SiblingSchema,
) -> Result<()> {
    for (key, value) in obj {
        let arr = match value {
            Value::Array(a) => a,
            _ => continue, // skip non-array values
        };
        for (order, item) in arr.iter().enumerate() {
            let item_obj = match item {
                Value::Object(o) => o,
                _ => continue, // skip non-object array elements
            };

            let row_id = Uuid::now_v7();
            let mut builder = RowBuilder::new();

            for col in &schema.columns {
                // data JSONB: is_generated but requires actual serialization — check before the
                // generated dispatch block which would otherwise emit NULL for unknown names.
                if col.name == sibling_schema.data_col_name {
                    let json_str = serde_json::to_string(item_obj).unwrap_or_default();
                    match escape_copy_text(&json_str) {
                        Some(escaped) => builder.push_value(&escaped),
                        None => builder.push_null(),
                    }
                    continue;
                }

                if col.is_generated {
                    if col.is_parent_fk {
                        builder.push_uuid(parent_id);
                    } else {
                        match col.name.as_str() {
                            "j2s_id" => builder.push_uuid(row_id),
                            "j2s_order" => builder.push_value(
                                &CopyEscaped::from_safe_ascii(order.to_string()),
                            ),
                            _ => builder.push_null(),
                        }
                    }
                    continue;
                }

                // Key column: the original JSON key (genome name, etc.)
                if col.original_name == sibling_schema.key_col_name {
                    match escape_copy_text(key) {
                        Some(escaped) => builder.push_value(&escaped),
                        None => builder.push_null(),
                    }
                    continue;
                }

                // Data column: look up in the array element object
                let json_val = item_obj.get(&col.original_name).unwrap_or(&Value::Null);

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
                            &schema.name, &col.name, &row_id.to_string(),
                            &col.pg_type.as_sql(), &actual_value, actual_type,
                        )?;
                        builder.push_null();
                    }
                }
            }

            anomalies.inc_total(&schema.name);
            if let Some(sink) = sinks.get_mut(&schema.name) {
                sink.write_row(builder.finish())?;
            }

            // Route sub-objects/arrays declared in child_routes to their target tables.
            dispatch_child_routes(path_map, sinks, anomalies, schema, item_obj, row_id)?;
        }
    }
    Ok(())
}

/// Insert one row per key for a NormalizeDynamicKeys table.
/// Columns: j2s_id, j2s_parent_id, {id_column} TEXT, <union data cols...>
///
/// Mirrors insert_keyed_pivot_object but uses the user-configured id_column name
/// instead of a SiblingSchema. Non-Object values (scalars, arrays) are skipped.
fn insert_normalize_dynamic_keys(
    sinks: &mut HashMap<String, TempFileSink>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    id_column: &str,
) -> Result<()> {
    for (key, value) in obj {
        let child_obj = match value {
            Value::Object(o) => o,
            _ => continue, // skip scalars and arrays
        };

        let row_id = Uuid::now_v7();
        let mut builder = RowBuilder::new();

        for col in &schema.columns {
            if col.is_generated {
                if col.is_parent_fk {
                    builder.push_uuid(parent_id);
                } else {
                    match col.name.as_str() {
                        "j2s_id" => builder.push_uuid(row_id),
                        _ => builder.push_null(),
                    }
                }
                continue;
            }

            // ID column: the original JSON key of this entry
            if col.original_name == id_column {
                match escape_copy_text(key) {
                    Some(escaped) => builder.push_value(&escaped),
                    None => builder.push_null(),
                }
                continue;
            }

            // Data column: look up in child object by original field name
            let json_val = child_obj.get(&col.original_name).unwrap_or(&Value::Null);

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
                        &schema.name, &col.name, &row_id.to_string(),
                        &col.pg_type.as_sql(), &actual_value, actual_type,
                    )?;
                    builder.push_null();
                }
            }
        }

        anomalies.inc_total(&schema.name);
        if let Some(sink) = sinks.get_mut(&schema.name) {
            sink.write_row(builder.finish())?;
        }
    }
    Ok(())
}

/// Dispatch each key in a MultiKeyedPivot object to the matching synthetic pivot table.
///
/// The routing table (`schema`) is a MultiKeyedPivot parent with only generated columns.
/// One routing row is emitted per call; children FK to it by routing_id.
/// Keys that are all-digits go to the `_num` group; all others go to `_key`.
fn insert_multi_keyed_pivot(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, TempFileSink>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    groups: &[SiblingGroup],
) -> Result<()> {
    // Emit one routing row (only generated columns: j2s_id + FK to grandparent).
    let routing_id = Uuid::now_v7();
    let mut rb = RowBuilder::new();
    for col in &schema.columns {
        if col.name == "j2s_id" {
            rb.push_uuid(routing_id);
        } else if col.is_parent_fk {
            rb.push_uuid(parent_id);
        } else {
            rb.push_null();
        }
    }
    anomalies.inc_total(&schema.name);
    if let Some(sink) = sinks.get_mut(&schema.name) {
        sink.write_row(rb.finish())?;
    }

    let routing_path = schema.path.join(&PATH_SEP.to_string());

    // Build per-group submaps. Keys that have a dedicated child schema (significant
    // container left independent by finalize_siblings) are delegated directly rather
    // than being absorbed as JSONB into the group's synthetic table.
    let mut group_submaps: Vec<serde_json::Map<String, Value>> =
        vec![serde_json::Map::new(); groups.len()];

    for (key, value) in obj {
        // Check for a dedicated child schema surviving independently in path_map.
        let child_path_key = format!("{}{}{}", routing_path, PATH_SEP, key);
        if let Some(child_schema) = path_map.get(&child_path_key) {
            if child_schema.parent_table.as_deref() == Some(schema.name.as_str()) {
                if let Value::Object(nested) = value {
                    match &child_schema.wide_strategy {
                        WideStrategy::KeyedPivot(ss) => {
                            insert_keyed_pivot_object(
                                path_map, sinks, anomalies, child_schema, nested, routing_id, ss,
                            )?;
                        }
                        WideStrategy::MultiKeyedPivot(child_groups) => {
                            insert_multi_keyed_pivot(
                                path_map, sinks, anomalies, child_schema,
                                nested, routing_id, child_groups,
                            )?;
                        }
                        _ => {
                            let child_id = Uuid::now_v7();
                            insert_object(
                                path_map, sinks, anomalies, child_schema,
                                nested, child_id, Some(routing_id), None,
                            )?;
                        }
                    }
                }
                continue; // handled — do not route to group's synthetic table
            }
        }

        // Route to the matching group's submap.
        let key_is_numeric = key.chars().all(|c| c.is_ascii_digit());
        if let Some(idx) = groups.iter().position(|g| g.key_is_numeric == key_is_numeric) {
            group_submaps[idx].insert(key.clone(), value.clone());
        }
    }

    // Flush each group's submap into its synthetic pivot table.
    for (group, submap) in groups.iter().zip(group_submaps.iter()) {
        if submap.is_empty() {
            continue;
        }
        let suffix = if group.key_is_numeric { "num" } else { "key" };
        let pivot_path_key = format!("{}{}{}", routing_path, PATH_SEP, suffix);
        if let Some(pivot_schema) = path_map.get(&pivot_path_key) {
            insert_keyed_pivot_object(
                path_map, sinks, anomalies, pivot_schema, submap, routing_id, &group.sibling_schema,
            )?;
        }
    }

    Ok(())
}

fn insert_array(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, TempFileSink>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    arr: &[Value],
    parent_id: Uuid,
) -> Result<()> {
    for (i, item) in arr.iter().enumerate() {
        let child_id = Uuid::now_v7();
        let order = i as i64;

        match (&schema.child_kind, item) {
            (Some(ChildKind::ObjectArray), Value::Object(obj)) => {
                insert_object(
                    path_map, sinks, anomalies, schema,
                    obj, child_id, Some(parent_id), Some(order),
                )?;
            }
            (Some(ChildKind::ScalarArray), scalar) => {
                let mut builder = RowBuilder::new();
                builder.push_uuid(child_id);   // j2s_id
                builder.push_uuid(parent_id);  // j2s_parent_id
                builder.push_value(&CopyEscaped::from_safe_ascii(order.to_string())); // j2s_order

                // value column
                let value_col = schema.find_by_original("value");
                if let Some(col) = value_col {
                    match coerce(scalar, &col.pg_type) {
                        CoerceResult::Ok(s) => builder.push_value(&s),
                        CoerceResult::Null => builder.push_null(),
                        CoerceResult::Anomaly { actual_value, actual_type } => {
                            anomalies.record(
                                &schema.name, "value",
                                &child_id.to_string(),
                                &col.pg_type.as_sql(),
                                &actual_value, actual_type,
                            )?;
                            builder.push_null();
                        }
                    }
                } else {
                    builder.push_null();
                }

                anomalies.inc_total(&schema.name);
                if let Some(sink) = sinks.get_mut(&schema.name) {
                    sink.write_row(builder.finish())?;
                }
            }
            _ => {}
        }
    }

    Ok(())
}
