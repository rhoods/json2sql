use std::collections::{BTreeMap, HashMap};

use serde_json::Value;
use uuid::Uuid;

use crate::anomaly::collect::AnomalyCollect;
use crate::db::copy_sink::RowBuilder;
use crate::db::copy_text::{escape_copy_text, CopyEscaped};
use crate::error::Result;
use crate::pass2::coercer::{coerce, CoerceResult};
use crate::pass2::sink::RowSink;
use crate::schema::PATH_SEP;
use crate::schema::table_schema::{ChildKind, ColumnSchema, SiblingGroup, SiblingSchema, SuffixSchema, TableSchema, WideStrategy};

use super::insert::{insert_object, InsertCtx};

fn push_coerced<A: AnomalyCollect>(
    builder: &mut RowBuilder,
    anomalies: &mut A,
    table_name: &str,
    col: &ColumnSchema,
    row_id: &str,
    val: &Value,
) -> Result<()> {
    match coerce(val, &col.pg_type) {
        CoerceResult::Ok(s) => builder.push_value(&s),
        CoerceResult::Null => builder.push_null(),
        CoerceResult::Anomaly { actual_value, actual_type } => {
            anomalies.record(table_name, &col.name, row_id, &col.pg_type.as_sql(), &actual_value, actual_type)?;
            builder.push_null();
        }
    }
    Ok(())
}

/// Insert one row per key-value pair for a Pivot wide table.
/// Columns: j2s_id, j2s_parent_id, key TEXT, value <type>
pub(super) fn insert_pivot_object<S: RowSink>(
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
) -> Result<()> {
    let value_col = schema.find_by_original("value");
    for (key, val) in obj {
        let child_id = Uuid::now_v7();
        let mut builder = RowBuilder::new();
        builder.push_uuid(child_id);
        builder.push_uuid(parent_id);
        match escape_copy_text(key) {
            Some(escaped) => builder.push_value(&escaped),
            None => builder.push_null(),
        }
        if let Some(col) = value_col {
            push_coerced(&mut builder, anomalies, &schema.name, col, &child_id.to_string(), val)?;
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
pub(super) fn insert_jsonb_object<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    value: &Value,
    parent_id: Uuid,
) -> Result<()> {
    let child_id = Uuid::now_v7();
    let mut builder = RowBuilder::new();
    builder.push_uuid(child_id);
    builder.push_uuid(parent_id);
    let json_str = serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string());
    match escape_copy_text(&json_str) {
        Some(escaped) => builder.push_value(&escaped),
        None => builder.push_null(), // null byte in JSON — treat as NULL, not empty string
    }
    anomalies.inc_total(&schema.name);
    if let Some(sink) = sinks.get_mut(&schema.name) {
        sink.write_row(builder.finish())?;
    }
    if let Value::Object(obj) = value {
        dispatch_jsonb_children(path_map, sinks, anomalies, schema, obj, child_id)?;
    }
    Ok(())
}

fn dispatch_object_child<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    child_schema: &TableSchema,
    child_value: &Value,
    child_id: Uuid,
) -> Result<()> {
    let Value::Object(nested) = child_value else { return Ok(()); };
    match &child_schema.wide_strategy {
        WideStrategy::Pivot => insert_pivot_object(sinks, anomalies, child_schema, nested, child_id)?,
        WideStrategy::Jsonb => insert_jsonb_object(path_map, sinks, anomalies, child_schema, child_value, child_id)?,
        WideStrategy::StructuredPivot(suffix_schema) => {
            insert_structured_pivot_object(sinks, anomalies, child_schema, nested, child_id, suffix_schema)?;
        }
        WideStrategy::KeyedPivot(sibling_schema) => {
            insert_keyed_pivot_object(path_map, sinks, anomalies, child_schema, nested, child_id, sibling_schema)?;
        }
        WideStrategy::MultiKeyedPivot(groups) => {
            insert_multi_keyed_pivot(path_map, sinks, anomalies, child_schema, nested, child_id, groups)?;
        }
        WideStrategy::NormalizeDynamicKeys { id_column } => {
            insert_normalize_dynamic_keys(sinks, anomalies, child_schema, nested, child_id, id_column)?;
        }
        WideStrategy::Columns | WideStrategy::AutoSplit { .. } | WideStrategy::Ignore
        | WideStrategy::Flatten { .. } | WideStrategy::JsonbFlatten => {
            insert_object(
                path_map, &mut InsertCtx { sinks, anomalies },
                child_schema, nested, Uuid::now_v7(), Some(child_id), None,
            )?;
        }
    }
    Ok(())
}

fn dispatch_jsonb_children<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    child_id: Uuid,
) -> Result<()> {
    let parent_path_key = schema.path.join(&PATH_SEP.to_string());
    for (field, child_value) in obj {
        let child_key = format!("{}{}{}", parent_path_key, PATH_SEP, field);
        match child_value {
            Value::Object(_) => {
                if let Some(child_schema) = path_map.get(&child_key) {
                    dispatch_object_child(path_map, sinks, anomalies, child_schema, child_value, child_id)?;
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
    Ok(())
}

/// Group JSON object keys by their base name: `base → { suffix → &Value }`.
/// Keys with no matching suffix use an empty string as the suffix key (→ `value` column).
fn group_keys_by_base<'a>(
    obj: &'a serde_json::Map<String, Value>,
    suffix_schema: &SuffixSchema,
) -> BTreeMap<String, HashMap<String, &'a Value>> {
    let mut groups: BTreeMap<String, HashMap<String, &'a Value>> = BTreeMap::new();
    for (key, val) in obj {
        let matched_suffix = suffix_schema
            .suffix_cols
            .iter()
            .find(|sc| key.len() > sc.suffix.len() && key.ends_with(sc.suffix.as_str()))
            .map(|sc| sc.suffix.as_str());
        match matched_suffix {
            Some(suffix) => {
                let base = &key[..key.len() - suffix.len()];
                groups.entry(base.to_string()).or_default().insert(suffix.to_string(), val);
            }
            None => {
                groups.entry(key.clone()).or_default().insert(String::new(), val);
            }
        }
    }
    groups
}

fn write_structured_pivot_row<S: RowSink>(
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    base: &str,
    suffix_vals: &HashMap<String, &Value>,
    parent_id: Uuid,
) -> Result<()> {
    let child_id = Uuid::now_v7();
    let mut builder = RowBuilder::new();
    let row_id_str = child_id.to_string();
    for col in &schema.columns {
        if col.is_generated {
            if col.is_parent_fk { builder.push_uuid(parent_id); }
            else { match col.name.as_str() { "j2s_id" => builder.push_uuid(child_id), _ => builder.push_null() } }
            continue;
        }
        if col.original_name == "name" {
            match escape_copy_text(base) { Some(e) => builder.push_value(&e), None => builder.push_null() }
            continue;
        }
        if col.original_name == "value" {
            if let Some(&val) = suffix_vals.get("") {
                push_coerced(&mut builder, anomalies, &schema.name, col, &row_id_str, val)?;
            } else { builder.push_null(); }
            continue;
        }
        if let Some(&val) = suffix_vals.get(&col.original_name) {
            push_coerced(&mut builder, anomalies, &schema.name, col, &row_id_str, val)?;
        } else { builder.push_null(); }
    }
    anomalies.inc_total(&schema.name);
    if let Some(sink) = sinks.get_mut(&schema.name) { sink.write_row(builder.finish())?; }
    Ok(())
}

/// For each JSON key, we check whether it ends with a known suffix.
/// Keys that match no suffix are treated as bare base keys (→ `value` column).
/// All keys sharing the same base are collapsed into a single row.
pub(super) fn insert_structured_pivot_object<S: RowSink>(
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    suffix_schema: &SuffixSchema,
) -> Result<()> {
    let groups = group_keys_by_base(obj, suffix_schema);
    for (base, suffix_vals) in groups {
        write_structured_pivot_row(sinks, anomalies, schema, &base, &suffix_vals, parent_id)?;
    }
    Ok(())
}

/// Route sub-objects and sub-arrays declared in `schema.child_routes` to their target tables.
/// Called after writing each pivot row so that nested tables created by cascade merging
/// receive their data with the correct parent FK.
pub(super) fn dispatch_child_routes<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    child_obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
) -> Result<()> {
    if schema.child_routes.is_empty() {
        return Ok(());
    }
    for (sub_key, child_table_name) in &schema.child_routes {
        let sub_value = match child_obj.get(sub_key) { Some(v) => v, None => continue };
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
                        insert_object(path_map, &mut InsertCtx { sinks, anomalies }, child_schema, nested, child_id, Some(row_id), None)?;
                    }
                }
            }
            Value::Array(arr) => insert_array(path_map, sinks, anomalies, child_schema, arr, row_id)?,
            _ => {}
        }
    }
    Ok(())
}

struct KeyedPivotRowInput<'a> {
    key: &'a str,
    child_obj: &'a serde_json::Map<String, Value>,
    parent_id: Uuid,
    row_id: Uuid,
    /// `Some(i)` for array-of-objects rows (fills `j2s_order`), `None` otherwise.
    order: Option<usize>,
    sibling_schema: &'a SiblingSchema,
}

fn write_keyed_pivot_columns<A: AnomalyCollect>(
    builder: &mut RowBuilder,
    anomalies: &mut A,
    schema: &TableSchema,
    ctx: &KeyedPivotRowInput<'_>,
) -> Result<()> {
    let row_id_str = ctx.row_id.to_string();
    for col in &schema.columns {
        // data JSONB: is_generated but requires actual serialization
        if col.name == ctx.sibling_schema.data_col_name {
            let json_str = serde_json::to_string(ctx.child_obj).unwrap_or_default();
            match escape_copy_text(&json_str) { Some(e) => builder.push_value(&e), None => builder.push_null() }
            continue;
        }
        if col.is_generated {
            if col.is_parent_fk { builder.push_uuid(ctx.parent_id); }
            else {
                match col.name.as_str() {
                    "j2s_id" => builder.push_uuid(ctx.row_id),
                    "j2s_order" => match ctx.order {
                        Some(o) => builder.push_value(&CopyEscaped::from_safe_ascii(o.to_string())),
                        None => builder.push_null(),
                    },
                    _ => builder.push_null(),
                }
            }
            continue;
        }
        if col.original_name == ctx.sibling_schema.key_col_name {
            match escape_copy_text(ctx.key) { Some(e) => builder.push_value(&e), None => builder.push_null() }
            continue;
        }
        let json_val = ctx.child_obj.get(&col.original_name).unwrap_or(&Value::Null);
        if matches!(json_val, Value::Object(_))
            || (matches!(json_val, Value::Array(_))
                && !matches!(col.pg_type, crate::schema::type_tracker::PgType::Array(_)))
        {
            builder.push_null();
            continue;
        }
        push_coerced(builder, anomalies, &schema.name, col, &row_id_str, json_val)?;
    }
    Ok(())
}

/// Insert one row per sibling key for a KeyedPivot table.
/// Columns: j2s_id, j2s_parent_id, key TEXT, <union data cols...>
///
/// When `sibling_schema.array_children` is true, each key maps to an array of objects
/// instead of a single object. One row is emitted per array element, with j2s_order set.
pub(super) fn insert_keyed_pivot_object<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    sibling_schema: &SiblingSchema,
) -> Result<()> {
    if sibling_schema.array_children {
        return insert_keyed_pivot_array_of_objects(path_map, sinks, anomalies, schema, obj, parent_id, sibling_schema);
    }
    for (key, value) in obj {
        let child_obj = match value { Value::Object(o) => o, _ => continue };
        let row_id = Uuid::now_v7();
        let mut builder = RowBuilder::new();
        let ctx = KeyedPivotRowInput { key, child_obj, parent_id, row_id, order: None, sibling_schema };
        write_keyed_pivot_columns(&mut builder, anomalies, schema, &ctx)?;
        anomalies.inc_total(&schema.name);
        if let Some(sink) = sinks.get_mut(&schema.name) { sink.write_row(builder.finish())?; }
        dispatch_child_routes(path_map, sinks, anomalies, schema, child_obj, row_id)?;
    }
    Ok(())
}

/// Insert one row per array element per sibling key for an ObjectArray KeyedPivot table.
/// Columns: j2s_id, j2s_parent_id, j2s_order BIGINT, key TEXT, <union data cols...>
pub(super) fn insert_keyed_pivot_array_of_objects<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    sibling_schema: &SiblingSchema,
) -> Result<()> {
    for (key, value) in obj {
        let arr = match value { Value::Array(a) => a, _ => continue };
        for (order, item) in arr.iter().enumerate() {
            let item_obj = match item { Value::Object(o) => o, _ => continue };
            let row_id = Uuid::now_v7();
            let mut builder = RowBuilder::new();
            let ctx = KeyedPivotRowInput { key, child_obj: item_obj, parent_id, row_id, order: Some(order), sibling_schema };
            write_keyed_pivot_columns(&mut builder, anomalies, schema, &ctx)?;
            anomalies.inc_total(&schema.name);
            if let Some(sink) = sinks.get_mut(&schema.name) { sink.write_row(builder.finish())?; }
            dispatch_child_routes(path_map, sinks, anomalies, schema, item_obj, row_id)?;
        }
    }
    Ok(())
}

/// Insert one row per key for a NormalizeDynamicKeys table.
/// Columns: j2s_id, j2s_parent_id, {id_column} TEXT, <union data cols...>
pub(super) fn insert_normalize_dynamic_keys<S: RowSink>(
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    id_column: &str,
) -> Result<()> {
    for (key, value) in obj {
        let child_obj = match value { Value::Object(o) => o, _ => continue };
        let row_id = Uuid::now_v7();
        let mut builder = RowBuilder::new();
        let row_id_str = row_id.to_string();
        for col in &schema.columns {
            if col.is_generated {
                if col.is_parent_fk { builder.push_uuid(parent_id); }
                else { match col.name.as_str() { "j2s_id" => builder.push_uuid(row_id), _ => builder.push_null() } }
                continue;
            }
            if col.original_name == id_column {
                match escape_copy_text(key) { Some(e) => builder.push_value(&e), None => builder.push_null() }
                continue;
            }
            let json_val = child_obj.get(&col.original_name).unwrap_or(&Value::Null);
            if matches!(json_val, Value::Object(_))
                || (matches!(json_val, Value::Array(_))
                    && !matches!(col.pg_type, crate::schema::type_tracker::PgType::Array(_)))
            {
                builder.push_null();
                continue;
            }
            push_coerced(&mut builder, anomalies, &schema.name, col, &row_id_str, json_val)?;
        }
        anomalies.inc_total(&schema.name);
        if let Some(sink) = sinks.get_mut(&schema.name) { sink.write_row(builder.finish())?; }
    }
    Ok(())
}

fn emit_routing_row<S: RowSink>(
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    parent_id: Uuid,
) -> Result<Uuid> {
    let routing_id = Uuid::now_v7();
    let mut rb = RowBuilder::new();
    for col in &schema.columns {
        if col.name == "j2s_id" { rb.push_uuid(routing_id); }
        else if col.is_parent_fk { rb.push_uuid(parent_id); }
        else { rb.push_null(); }
    }
    anomalies.inc_total(&schema.name);
    if let Some(sink) = sinks.get_mut(&schema.name) { sink.write_row(rb.finish())?; }
    Ok(routing_id)
}

/// Route a key-value pair to an independent child schema if one exists.
/// Returns `true` if routed (caller should skip group routing), `false` otherwise.
fn route_independent_child<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema_name: &str,
    value: &Value,
    child_path_key: &str,
    routing_id: Uuid,
) -> Result<bool> {
    let child_schema = match path_map.get(child_path_key) { Some(s) => s, None => return Ok(false) };
    if child_schema.parent_table.as_deref() != Some(schema_name) { return Ok(false); }
    if let Value::Object(nested) = value {
        match &child_schema.wide_strategy {
            WideStrategy::KeyedPivot(ss) => insert_keyed_pivot_object(path_map, sinks, anomalies, child_schema, nested, routing_id, ss)?,
            WideStrategy::MultiKeyedPivot(cg) => insert_multi_keyed_pivot(path_map, sinks, anomalies, child_schema, nested, routing_id, cg)?,
            _ => {
                let child_id = Uuid::now_v7();
                insert_object(path_map, &mut InsertCtx { sinks, anomalies }, child_schema, nested, child_id, Some(routing_id), None)?;
            }
        }
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Dispatch each key in a MultiKeyedPivot object to the matching synthetic pivot table.
///
/// The routing table (`schema`) is a MultiKeyedPivot parent with only generated columns.
/// One routing row is emitted per call; children FK to it by routing_id.
/// Keys that are all-digits go to the `_num` group; all others go to `_key`.
pub(super) fn insert_multi_keyed_pivot<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    groups: &[SiblingGroup],
) -> Result<()> {
    let routing_id = emit_routing_row(sinks, anomalies, schema, parent_id)?;
    let routing_path = schema.path.join(&PATH_SEP.to_string());
    let mut group_submaps: Vec<serde_json::Map<String, Value>> = vec![serde_json::Map::new(); groups.len()];
    for (key, value) in obj {
        let child_path_key = format!("{}{}{}", routing_path, PATH_SEP, key);
        if route_independent_child(path_map, sinks, anomalies, &schema.name, value, &child_path_key, routing_id)? {
            continue;
        }
        let key_is_numeric = key.chars().all(|c| c.is_ascii_digit());
        if let Some(idx) = groups.iter().position(|g| g.key_is_numeric == key_is_numeric) {
            group_submaps[idx].insert(key.clone(), value.clone());
        }
    }
    for (group, submap) in groups.iter().zip(group_submaps.iter()) {
        if submap.is_empty() { continue; }
        let suffix = if group.key_is_numeric { "num" } else { "key" };
        let pivot_path_key = format!("{}{}{}", routing_path, PATH_SEP, suffix);
        if let Some(pivot_schema) = path_map.get(&pivot_path_key) {
            insert_keyed_pivot_object(path_map, sinks, anomalies, pivot_schema, submap, routing_id, &group.sibling_schema)?;
        }
    }
    Ok(())
}

pub(super) fn insert_array<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
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
                    path_map, &mut InsertCtx { sinks, anomalies },
                    schema, obj, child_id, Some(parent_id), Some(order),
                )?;
            }
            (Some(ChildKind::ScalarArray), scalar) => {
                let mut builder = RowBuilder::new();
                builder.push_uuid(child_id);
                builder.push_uuid(parent_id);
                builder.push_value(&CopyEscaped::from_safe_ascii(order.to_string()));
                let value_col = schema.find_by_original("value");
                if let Some(col) = value_col {
                    push_coerced(&mut builder, anomalies, &schema.name, col, &child_id.to_string(), scalar)?;
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
