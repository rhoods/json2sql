//! Navigation dans les sous-arbres JSON — pivot, keyed pivot, routing entre tables.
//!
//! Ce module sait *où aller* : il parcourt la hiérarchie JSON et *route* les valeurs
//! vers les bons sinks selon la structure du schéma (pivot key, structured pivot, jsonb…).
//! *Dispatcher* un nœud = choisir le handler selon son type JSON ou sa `InferredStrategy`.
//! *Router* une ligne = la rediriger vers la table cible correcte.
//!
//! Frontière avec `insert.rs` : `traversal.rs` navigue et dispatch depuis un contexte
//! de traversal (sinks/anomalies explicites). `insert.rs` écrit une ligne complète
//! depuis un `InsertCtx` bundlé.
//!
//! Fonctions :
//! - fn `push_coerced` — coerce et écrit une valeur de colonne (avec anomalie si besoin).
//!
//! Pivot :
//! - fn `insert_pivot_object` — une ligne par paire clé/valeur.
//!
//! Jsonb :
//! - fn `insert_jsonb_object` — une ligne JSONB puis récursion enfants.
//! - fn `dispatch_child_object` — route un enfant d'objet JSONB.
//! - fn `dispatch_jsonb_children` — route les enfants d'un objet JSONB.
//!
//! `StructuredPivot` :
//! - fn `group_keys_by_base` — regroupe les clés par préfixe/suffixe.
//! - fn `write_structured_pivot_row` — écrit une ligne fusionnée.
//! - fn `insert_structured_pivot_object` — orchestre les deux.
//!
//! Routing générique :
//! - fn `dispatch_child_routes` — route les sous-objets/arrays de `schema.child_routes`
//!   vers leurs tables cibles (post-cascade merge).
//!
//! `SiblingCollapse` (simple + multi) :
//! - struct `SiblingCollapseRowInput` — paramètres empaquetés d'une ligne keyed-pivot à écrire.
//! - fn `write_keyed_pivot_columns` — écrit les colonnes communes d'une ligne keyed-pivot.
//! - fn `insert_sibling_collapse_object` — une ligne par clé sibling.
//! - fn `insert_sibling_collapse_array_of_objects` — variante tableau d'objets (avec `j2s_order`).
//! - fn `emit_routing_row` — ligne de routing pure FK pour `SiblingCollapseMulti`.
//! - fn `route_independent_child` — route une clé vers un enfant indépendant si présent.
//! - fn `route_multi_collapse_children` — dispatch chaque clé vers son groupe pivot synthétique.
//! - fn `insert_sibling_collapse_multi` — émet la ligne de routing puis route les enfants.
//! - fn `find_group_for_key` — résout le groupe cible d'une clé (match exact puis heuristique numérique).
//!
//! `NormalizeDynamicKeys` :
//! - fn `insert_normalize_dynamic_keys` — une ligne par clé dynamique.
//!
//! Tableaux :
//! - fn `insert_array` — dispatch `ObjectArray`/`ScalarArray` élément par élément.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;
use uuid::Uuid;

use crate::anomaly::collector::AnomalyCollect;
use crate::db::copy_sink::RowBuilder;
use crate::db::copy_text::{escape_copy_text, CopyEscaped};
use crate::error::Result;
use crate::pass2::coercer::{coerce, CoerceResult};
use crate::pass2::sink::RowSink;
use crate::schema::PATH_SEP;
use crate::schema::table_schema::{ChildKind, ColumnSchema, SiblingGroup, SiblingSchema, SuffixSchema, TableSchema, InferredStrategy};

use super::insert::{insert_object, push_generated_col_insert, push_jsonb_col, InsertCtx};

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
/// Columns: `j2s_id`, `j2s_parent_id`, key TEXT, value <type>
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
            sink.write_row(&builder.finish())?;
        }
    }
    Ok(())
}

/// Insert one row containing the entire object serialized as JSONB, then recurse into
/// any children of this table so they still receive data.
///
/// Builds the row generically from `schema.columns` (rather than a fixed 3-field layout) so
/// this also works for an `ObjectArray`-parent Jsonb table, which carries a `j2s_order`
/// column that a fixed-arity builder would silently omit (issue #45, tâche 9). `order` is
/// `None` for `Object`-kind children (no positional meaning) and `Some(i)` when called from
/// `insert_array` for an `ObjectArray` element.
pub(super) fn insert_jsonb_object<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    value: &Value,
    parent_id: Uuid,
    order: Option<i64>,
) -> Result<()> {
    let child_id = Uuid::now_v7();
    let mut builder = RowBuilder::new();
    for col in &schema.columns {
        if col.is_generated {
            push_generated_col_insert(&mut builder, col, child_id, Some(parent_id), order);
        } else {
            push_jsonb_col(&mut builder, value);
        }
    }
    anomalies.inc_total(&schema.name);
    if let Some(sink) = sinks.get_mut(&schema.name) {
        sink.write_row(&builder.finish())?;
    }
    if let Value::Object(obj) = value {
        dispatch_jsonb_children(path_map, sinks, anomalies, schema, obj, child_id)?;
    }
    Ok(())
}

fn dispatch_child_object<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    child_schema: &TableSchema,
    child_value: &Value,
    child_id: Uuid,
) -> Result<()> {
    let Value::Object(nested) = child_value else { return Ok(()); };
    let eff = child_schema.effective_strategy();
    match &*eff {
        InferredStrategy::Pivot => insert_pivot_object(sinks, anomalies, child_schema, nested, child_id)?,
        InferredStrategy::Jsonb => insert_jsonb_object(path_map, sinks, anomalies, child_schema, child_value, child_id, None)?,
        InferredStrategy::StructuredPivot(suffix_schema) => {
            insert_structured_pivot_object(sinks, anomalies, child_schema, nested, child_id, suffix_schema)?;
        }
        InferredStrategy::SiblingCollapse(sibling_schema) => {
            insert_sibling_collapse_object(path_map, sinks, anomalies, child_schema, nested, child_id, sibling_schema)?;
        }
        InferredStrategy::SiblingCollapseMulti(groups) => {
            insert_sibling_collapse_multi(path_map, sinks, anomalies, child_schema, nested, child_id, groups)?;
        }
        InferredStrategy::NormalizeDynamicKeys { id_column } => {
            insert_normalize_dynamic_keys(sinks, anomalies, child_schema, nested, child_id, id_column)?;
        }
        InferredStrategy::Columns | InferredStrategy::AutoSplit { .. } | InferredStrategy::Ignore
        | InferredStrategy::Flatten { .. } | InferredStrategy::JsonbFlatten => {
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
        let child_key = format!("{parent_path_key}{PATH_SEP}{field}");
        match child_value {
            Value::Object(_) => {
                if let Some(child_schema) = path_map.get(&child_key) {
                    dispatch_child_object(path_map, sinks, anomalies, child_schema, child_value, child_id)?;
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
    if let Some(sink) = sinks.get_mut(&schema.name) { sink.write_row(&builder.finish())?; }
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
#[allow(clippy::too_many_lines)] // heterogeneous dispatch loop over child_routes, each arm calls a different strategy
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
        let Some(sub_value) = child_obj.get(sub_key) else { continue };
        let Some(child_schema) = path_map.values().find(|s| s.name == *child_table_name) else { continue };
        match sub_value {
            Value::Object(nested) => {
                let eff = child_schema.effective_strategy();
                match &*eff {
                    InferredStrategy::SiblingCollapse(ss) => {
                        let ss = ss.clone();
                        insert_sibling_collapse_object(path_map, sinks, anomalies, child_schema, nested, row_id, &ss)?;
                    }
                    InferredStrategy::SiblingCollapseMulti(groups) => {
                        let groups = groups.clone();
                        insert_sibling_collapse_multi(path_map, sinks, anomalies, child_schema, nested, row_id, &groups)?;
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

struct SiblingCollapseRowInput<'a> {
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
    ctx: &SiblingCollapseRowInput<'_>,
) -> Result<()> {
    let row_id_str = ctx.row_id.to_string();
    for col in &schema.columns {
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

/// Insert one row per sibling key for a `SiblingCollapse` table.
/// Columns: `j2s_id`, `j2s_parent_id`, key TEXT, <union data cols...>
///
/// When `sibling_schema.array_children` is true, each key maps to an array of objects
/// instead of a single object. One row is emitted per array element, with `j2s_order` set.
pub(super) fn insert_sibling_collapse_object<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    sibling_schema: &SiblingSchema,
) -> Result<()> {
    if sibling_schema.array_children {
        return insert_sibling_collapse_array_of_objects(path_map, sinks, anomalies, schema, obj, parent_id, sibling_schema);
    }
    for (key, value) in obj {
        let Value::Object(child_obj) = value else { continue };
        let row_id = Uuid::now_v7();
        let mut builder = RowBuilder::new();
        let ctx = SiblingCollapseRowInput { key, child_obj, parent_id, row_id, order: None, sibling_schema };
        write_keyed_pivot_columns(&mut builder, anomalies, schema, &ctx)?;
        anomalies.inc_total(&schema.name);
        if let Some(sink) = sinks.get_mut(&schema.name) { sink.write_row(&builder.finish())?; }
        dispatch_child_routes(path_map, sinks, anomalies, schema, child_obj, row_id)?;
    }
    Ok(())
}

/// Insert one row per array element per sibling key for an `ObjectArray` `SiblingCollapse` table.
/// Columns: `j2s_id`, `j2s_parent_id`, `j2s_order` BIGINT, key TEXT, <union data cols...>
pub(super) fn insert_sibling_collapse_array_of_objects<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    sibling_schema: &SiblingSchema,
) -> Result<()> {
    for (key, value) in obj {
        let Value::Array(arr) = value else { continue };
        for (order, item) in arr.iter().enumerate() {
            let Value::Object(item_obj) = item else { continue };
            let row_id = Uuid::now_v7();
            let mut builder = RowBuilder::new();
            let ctx = SiblingCollapseRowInput { key, child_obj: item_obj, parent_id, row_id, order: Some(order), sibling_schema };
            write_keyed_pivot_columns(&mut builder, anomalies, schema, &ctx)?;
            anomalies.inc_total(&schema.name);
            if let Some(sink) = sinks.get_mut(&schema.name) { sink.write_row(&builder.finish())?; }
            dispatch_child_routes(path_map, sinks, anomalies, schema, item_obj, row_id)?;
        }
    }
    Ok(())
}

/// Insert one row per key for a `NormalizeDynamicKeys` table.
/// Columns: `j2s_id`, `j2s_parent_id`, {`id_column`} TEXT, <union data cols...>
pub(super) fn insert_normalize_dynamic_keys<S: RowSink>(
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    id_column: &str,
) -> Result<()> {
    for (key, value) in obj {
        let Value::Object(child_obj) = value else { continue };
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
        if let Some(sink) = sinks.get_mut(&schema.name) { sink.write_row(&builder.finish())?; }
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
    if let Some(sink) = sinks.get_mut(&schema.name) { sink.write_row(&rb.finish())?; }
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
    let Some(child_schema) = path_map.get(child_path_key) else { return Ok(false) };
    if child_schema.parent_table.as_deref() != Some(schema_name) { return Ok(false); }
    if let Value::Object(nested) = value {
        let eff = child_schema.effective_strategy();
        match &*eff {
            InferredStrategy::SiblingCollapse(ss) => insert_sibling_collapse_object(path_map, sinks, anomalies, child_schema, nested, routing_id, ss)?,
            InferredStrategy::SiblingCollapseMulti(cg) => insert_sibling_collapse_multi(path_map, sinks, anomalies, child_schema, nested, routing_id, cg)?,
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

/// Dispatch each key in a `SiblingCollapseMulti` object to the matching synthetic pivot table.
///
/// The routing table (`schema`) is a `SiblingCollapseMulti` parent with only generated columns.
/// One routing row is emitted per call; children FK to it by `routing_id`.
/// Keys that are all-digits go to the `_num` group; all others are routed by
/// `absorbed_path_segments` lookup (exact match against keys seen in Pass 1), falling
/// back to the `key_is_numeric` heuristic for backward compat with old snapshots.
/// Route each key in `obj` to its matching pivot group using `routing_id` as the parent FK.
/// Does NOT emit a routing row — the caller is responsible for having emitted it already.
/// Used by both `insert_sibling_collapse_multi` (after `emit_routing_row`) and `insert_object`
/// (when the schema with `SiblingCollapseMulti` is the root entry point).
pub(super) fn route_multi_collapse_children<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    routing_id: Uuid,
    groups: &[SiblingGroup],
) -> Result<()> {
    let routing_path = schema.path.join(&PATH_SEP.to_string());
    let mut group_submaps: Vec<serde_json::Map<String, Value>> = vec![serde_json::Map::new(); groups.len()];
    for (key, value) in obj {
        let child_path_key = format!("{routing_path}{PATH_SEP}{key}");
        if route_independent_child(path_map, sinks, anomalies, &schema.name, value, &child_path_key, routing_id)? {
            continue;
        }
        if let Some(idx) = find_group_for_key(groups, key) {
            group_submaps[idx].insert(key.clone(), value.clone());
        }
    }
    for (group, submap) in groups.iter().zip(group_submaps.iter()) {
        if submap.is_empty() { continue; }
        let path_seg = if group.path_segment.is_empty() {
            if group.key_is_numeric { "num" } else { "key" }
        } else {
            &group.path_segment
        };
        let pivot_path_key = format!("{routing_path}{PATH_SEP}{path_seg}");
        if let Some(pivot_schema) = path_map.get(&pivot_path_key) {
            insert_sibling_collapse_object(path_map, sinks, anomalies, pivot_schema, submap, routing_id, &group.sibling_schema)?;
        }
    }
    Ok(())
}

pub(super) fn insert_sibling_collapse_multi<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    groups: &[SiblingGroup],
) -> Result<()> {
    let routing_id = emit_routing_row(sinks, anomalies, schema, parent_id)?;
    route_multi_collapse_children(path_map, sinks, anomalies, schema, obj, routing_id, groups)
}

/// Route a runtime JSON key to the matching `SiblingGroup` index.
///
/// Tries exact match in `absorbed_path_segments` first (populated by Pass 1 for all
/// schemas produced since the fix). Falls back to the `key_is_numeric` heuristic so
/// that old snapshots (empty `absorbed_path_segments`) continue to work correctly.
fn find_group_for_key(groups: &[SiblingGroup], key: &str) -> Option<usize> {
    if let Some(idx) = groups.iter().position(|g| g.absorbed_path_segments.iter().any(|s| s == key)) {
        return Some(idx);
    }
    let key_is_numeric = key.chars().all(|c| c.is_ascii_digit());
    groups.iter().position(|g| g.key_is_numeric == key_is_numeric)
}

#[allow(clippy::cast_possible_wrap)] // array index never reaches i64::MAX
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
            (Some(ChildKind::ObjectArray), Value::Object(_)) if matches!(*schema.effective_strategy(), InferredStrategy::Jsonb) => {
                // Jsonb needs the whole element as a blob, not just its object fields — insert_object's
                // generic per-column loop would look up a literal "data" JSON key that never exists and
                // silently write NULL (issue #45, tâche 10).
                insert_jsonb_object(path_map, sinks, anomalies, schema, item, parent_id, Some(order))?;
            }
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
                    sink.write_row(&builder.finish())?;
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use std::collections::HashMap;

    use crate::anomaly::collector::AnomalyCollect;
    use crate::error::Result;
    use crate::pass2::sink::RowSink;
    use crate::pass2::test_utils::CaptureSink;

    // ---------------------------------------------------------------------------
    // Test fakes
    // ---------------------------------------------------------------------------

    #[derive(Default)]
    pub(super) struct CountingAnomaly {
        pub records: Vec<(String, String)>, // (table, column)
        pub totals: HashMap<String, u64>,
    }

    impl AnomalyCollect for CountingAnomaly {
        fn record(
            &mut self,
            table: &str,
            column: &str,
            _row_id: &str,
            _expected_type: &str,
            _actual_value: &str,
            _actual_type: &str,
        ) -> Result<()> {
            self.records.push((table.to_string(), column.to_string()));
            Ok(())
        }

        fn inc_total(&mut self, table: &str) {
            *self.totals.entry(table.to_string()).or_default() += 1;
        }
    }

    use uuid::Uuid;

    use crate::schema::table_schema::{ChildKind, ColumnSchema, InferredStrategy, TableSchema};
    use crate::schema::type_tracker::PgType;

    // ---------------------------------------------------------------------------
    // Schema construction helpers
    // ---------------------------------------------------------------------------

    fn col(name: &str, pg_type: PgType) -> ColumnSchema {
        ColumnSchema { name: name.to_string(), original_name: name.to_string(), pg_type, not_null: false, is_generated: false, is_parent_fk: false }
    }

    fn generated_id() -> ColumnSchema {
        ColumnSchema { name: "j2s_id".to_string(), original_name: "j2s_id".to_string(), pg_type: PgType::Uuid, not_null: true, is_generated: true, is_parent_fk: false }
    }

    fn parent_fk(parent: &str) -> ColumnSchema {
        ColumnSchema { name: format!("j2s_{parent}_id"), original_name: format!("j2s_{parent}_id"), pg_type: PgType::Uuid, not_null: true, is_generated: true, is_parent_fk: true }
    }

    fn generated_order() -> ColumnSchema {
        ColumnSchema { name: "j2s_order".to_string(), original_name: "j2s_order".to_string(), pg_type: PgType::BigInt, not_null: true, is_generated: true, is_parent_fk: false }
    }

    /// Pivot schema: j2s_id, j2s_{parent}_id, key TEXT, value TEXT
    fn make_pivot_schema(name: &str, parent: &str) -> TableSchema {
        let mut s = TableSchema::new(name.to_string(), vec![name.to_string()], 1);
        s.inferred_strategy = InferredStrategy::Pivot;
        s.columns = vec![generated_id(), parent_fk(parent), col("key", PgType::Text), col("value", PgType::Text)];
        s
    }

    /// Jsonb schema: j2s_id, j2s_{parent}_id, data TEXT (JSONB stored as text in COPY)
    fn make_jsonb_schema(name: &str, parent: &str) -> TableSchema {
        let mut s = TableSchema::new(name.to_string(), vec![name.to_string()], 1);
        s.inferred_strategy = InferredStrategy::Jsonb;
        s.columns = vec![generated_id(), parent_fk(parent), col("data", PgType::Text)];
        s
    }

    /// Jsonb schema for an `ObjectArray`-parent table: j2s_id, j2s_{parent}_id, j2s_order, data.
    fn make_jsonb_object_array_schema(name: &str, parent: &str) -> TableSchema {
        let mut s = make_jsonb_schema(name, parent);
        s.child_kind = Some(ChildKind::ObjectArray);
        s.columns.insert(2, generated_order());
        s
    }

    fn make_sinks(names: &[&str]) -> HashMap<String, CaptureSink> {
        names.iter().map(|n| ((*n).to_string(), CaptureSink(vec![]))).collect()
    }

    fn dummy_parent_id() -> Uuid { Uuid::nil() }

    // ---------------------------------------------------------------------------
    // Row parsing helper
    // ---------------------------------------------------------------------------

    fn parse_fields(row: &[u8]) -> Vec<&str> {
        let row = row.strip_suffix(b"\n").unwrap_or(row);
        row.split(|&b| b == b'\t').map(|f| std::str::from_utf8(f).unwrap()).collect()
    }

    // ---------------------------------------------------------------------------
    // Helpers smoke test
    // ---------------------------------------------------------------------------

    #[test]
    fn make_pivot_schema_has_expected_columns() {
        let s = make_pivot_schema("attrs", "root");
        assert_eq!(s.columns.len(), 4);
        assert!(s.columns[0].is_generated && !s.columns[0].is_parent_fk);
        assert!(s.columns[1].is_generated && s.columns[1].is_parent_fk);
        assert_eq!(s.columns[3].original_name, "value");
    }

    // ---------------------------------------------------------------------------
    // ---------------------------------------------------------------------------
    // insert_pivot_object tests
    // ---------------------------------------------------------------------------

    #[test]
    fn pivot_nominal_two_keys_emit_two_rows() {
        let schema = make_pivot_schema("attrs", "root");
        let mut sinks = make_sinks(&["attrs"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"a": "hello", "b": "world"}"#).unwrap();

        super::insert_pivot_object(&mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id()).unwrap();

        let rows = &sinks["attrs"].0;
        assert_eq!(rows.len(), 2, "two key-value pairs → two rows");
        assert_eq!(anomalies.totals.get("attrs").copied().unwrap_or(0), 2);
        assert!(anomalies.records.is_empty(), "no type anomalies expected");
        for row in rows {
            let fields = parse_fields(row);
            assert_eq!(fields.len(), 4);
            assert_ne!(fields[2], r"\N", "key should not be null");
            assert_ne!(fields[3], r"\N", "value should not be null");
        }
    }

    #[test]
    fn pivot_null_value_pushes_null_no_anomaly() {
        let schema = make_pivot_schema("attrs", "root");
        let mut sinks = make_sinks(&["attrs"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"k": null}"#).unwrap();

        super::insert_pivot_object(&mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id()).unwrap();

        let rows = &sinks["attrs"].0;
        assert_eq!(rows.len(), 1);
        let fields = parse_fields(&rows[0]);
        assert_eq!(fields[3], r"\N", "null JSON value → \\N in COPY");
        assert!(anomalies.records.is_empty(), "null is not an anomaly");
    }

    #[test]
    fn pivot_type_mismatch_records_anomaly_and_pushes_null() {
        let mut schema = make_pivot_schema("attrs", "root");
        schema.columns[3].pg_type = crate::schema::type_tracker::PgType::Integer;
        let mut sinks = make_sinks(&["attrs"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"k": true}"#).unwrap();

        super::insert_pivot_object(&mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id()).unwrap();

        let rows = &sinks["attrs"].0;
        assert_eq!(rows.len(), 1);
        let fields = parse_fields(&rows[0]);
        assert_eq!(fields[3], r"\N", "anomalous value → \\N");
        assert_eq!(anomalies.records.len(), 1, "one anomaly recorded");
        assert_eq!(anomalies.records[0].0, "attrs");
    }

    #[test]
    fn pivot_null_byte_in_key_pushes_null_for_key() {
        let schema = make_pivot_schema("attrs", "root");
        let mut sinks = make_sinks(&["attrs"]);
        let mut anomalies = CountingAnomaly::default();
        let mut obj = serde_json::Map::new();
        obj.insert("ke\0y".to_string(), serde_json::Value::String("val".to_string()));

        super::insert_pivot_object(&mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id()).unwrap();

        let rows = &sinks["attrs"].0;
        assert_eq!(rows.len(), 1);
        let fields = parse_fields(&rows[0]);
        assert_eq!(fields[2], r"\N", "null byte in key → \\N for key field");
        assert!(anomalies.records.is_empty(), "null byte in key is not a type anomaly");
    }

    #[test]
    fn pivot_sink_absent_inc_total_still_called_no_row_written() {
        let schema = make_pivot_schema("attrs", "root");
        let mut sinks: HashMap<String, CaptureSink> = HashMap::new();
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"k": "v"}"#).unwrap();

        super::insert_pivot_object(&mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id()).unwrap();

        assert_eq!(sinks.len(), 0);
        assert_eq!(anomalies.totals.get("attrs").copied().unwrap_or(0), 1, "inc_total called even without sink");
    }

    // ---------------------------------------------------------------------------
    // insert_jsonb_object tests
    // ---------------------------------------------------------------------------

    fn make_jsonb_path_key(path: &[&str]) -> String {
        path.join("\x00")
    }

    #[test]
    fn jsonb_nominal_emits_one_row_with_serialized_json() {
        let schema = make_jsonb_schema("evts", "root");
        let mut sinks = make_sinks(&["evts"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let value = serde_json::json!({"x": 1, "y": "hello"});

        super::insert_jsonb_object(&path_map, &mut sinks, &mut anomalies, &schema, &value, dummy_parent_id(), None).unwrap();

        let rows = &sinks["evts"].0;
        assert_eq!(rows.len(), 1, "one JSONB row emitted");
        let fields = parse_fields(&rows[0]);
        assert_eq!(fields.len(), 3, "uuid, parent_uuid, json_data");
        // third field must be valid JSON containing x and y
        let parsed: serde_json::Value = serde_json::from_str(fields[2]).unwrap();
        assert_eq!(parsed["x"], 1);
        assert_eq!(parsed["y"], "hello");
    }

    #[test]
    fn jsonb_with_populated_path_map_recurses_into_child() {
        // Parent: "evts" with path ["evts"], strategy Jsonb
        let mut schema = make_jsonb_schema("evts", "root");
        schema.path = vec!["evts".to_string()];

        // Child Pivot for field "tags" → path_map key "evts\x00tags"
        let mut child_schema = make_pivot_schema("evts_tags", "evts");
        child_schema.path = vec!["evts".to_string(), "tags".to_string()];
        let child_key = make_jsonb_path_key(&["evts", "tags"]);

        let mut path_map = HashMap::new();
        path_map.insert(child_key, child_schema);

        let mut sinks = make_sinks(&["evts", "evts_tags"]);
        let mut anomalies = CountingAnomaly::default();
        let value = serde_json::json!({"tags": {"a": "v1", "b": "v2"}});

        super::insert_jsonb_object(&path_map, &mut sinks, &mut anomalies, &schema, &value, dummy_parent_id(), None).unwrap();

        assert_eq!(sinks["evts"].0.len(), 1, "one JSONB row for parent");
        assert_eq!(sinks["evts_tags"].0.len(), 2, "two pivot rows for child tags");
    }

    #[test]
    fn jsonb_empty_path_map_no_recursion() {
        let mut schema = make_jsonb_schema("evts", "root");
        schema.path = vec!["evts".to_string()];
        let mut sinks = make_sinks(&["evts"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map: HashMap<String, crate::schema::table_schema::TableSchema> = HashMap::new();
        let value = serde_json::json!({"nested": {"x": 1}});

        super::insert_jsonb_object(&path_map, &mut sinks, &mut anomalies, &schema, &value, dummy_parent_id(), None).unwrap();

        assert_eq!(sinks["evts"].0.len(), 1, "one row — no child dispatch without path_map");
    }

    // ---------------------------------------------------------------------------
    // [issue #45, tâche 9] ObjectArray+Jsonb : builder générique, j2s_order peuplé.
    //
    // Avant le fix, insert_jsonb_object écrivait un nombre de champs figé (uuid,
    // parent_uuid, json_data) sans jamais consulter schema.columns — sur un schéma
    // ObjectArray+Jsonb (4 colonnes : j2s_id, parent_fk, j2s_order, data), la ligne
    // COPY n'aurait que 3 champs pour 4 colonnes déclarées (arité invalide),
    // j2s_order n'étant jamais poussé.
    // ---------------------------------------------------------------------------
    #[test]
    fn jsonb_object_array_populates_order_column() {
        let schema = make_jsonb_object_array_schema("reviews", "produits");
        let mut sinks = make_sinks(&["reviews"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let value = serde_json::json!({"score": 5});

        super::insert_jsonb_object(&path_map, &mut sinks, &mut anomalies, &schema, &value, dummy_parent_id(), Some(2)).unwrap();

        let rows = &sinks["reviews"].0;
        assert_eq!(rows.len(), 1);
        let fields = parse_fields(&rows[0]);
        assert_eq!(fields.len(), 4, "uuid, parent_uuid, order, json_data — une ligne par colonne déclarée");
        assert_eq!(fields[2], "2", "j2s_order doit porter la position passée en paramètre");
        let parsed: serde_json::Value = serde_json::from_str(fields[3]).unwrap();
        assert_eq!(parsed["score"], 5);
    }

    #[test]
    fn jsonb_object_array_order_none_pushes_null() {
        let schema = make_jsonb_object_array_schema("reviews", "produits");
        let mut sinks = make_sinks(&["reviews"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let value = serde_json::json!({"score": 5});

        super::insert_jsonb_object(&path_map, &mut sinks, &mut anomalies, &schema, &value, dummy_parent_id(), None).unwrap();

        let fields = parse_fields(&sinks["reviews"].0[0]);
        assert_eq!(fields[2], r"\N", "pas d'ordre fourni → j2s_order NULL");
    }

    // ---------------------------------------------------------------------------
    // insert_structured_pivot_object tests
    // ---------------------------------------------------------------------------

    use crate::schema::table_schema::{SuffixColumn, SuffixSchema};

    fn make_structured_pivot_schema(name: &str, parent: &str, suffix_schema: &SuffixSchema) -> crate::schema::table_schema::TableSchema {
        use crate::schema::table_schema::InferredStrategy;
        let mut s = crate::schema::table_schema::TableSchema::new(name.to_string(), vec![name.to_string()], 1);
        s.inferred_strategy = InferredStrategy::StructuredPivot(suffix_schema.clone());
        let mut cols = vec![
            generated_id(),
            parent_fk(parent),
            col("name", PgType::Text),
            col("value", PgType::Text),
        ];
        for sc in &suffix_schema.suffix_cols {
            cols.push(ColumnSchema {
                name: sc.col_name.clone(),
                original_name: sc.suffix.clone(), // suffix string is the lookup key in write_structured_pivot_row
                pg_type: sc.pg_type.clone(),
                not_null: false,
                is_generated: false,
                is_parent_fk: false,
            });
        }
        s.columns = cols;
        s
    }

    fn make_suffix_schema(suffixes: &[&str]) -> SuffixSchema {
        SuffixSchema {
            suffix_cols: suffixes.iter().map(|s| SuffixColumn {
                suffix: s.to_string(),
                col_name: format!("c{}", s.replace('_', "")),
                pg_type: PgType::Text,
            }).collect(),
            value_type: PgType::Text,
        }
    }

    #[test]
    fn structured_pivot_two_keys_same_base_emit_one_row() {
        let ss = make_suffix_schema(&["_eur", "_usd"]);
        let schema = make_structured_pivot_schema("prices", "root", &ss);
        let mut sinks = make_sinks(&["prices"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"price_eur": "1.5", "price_usd": "2.0"}"#).unwrap();

        super::insert_structured_pivot_object(&mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id(), &ss).unwrap();

        let rows = &sinks["prices"].0;
        assert_eq!(rows.len(), 1, "two keys with same base → one row");
        let fields = parse_fields(&rows[0]);
        assert_eq!(fields[2], "price", "base key in 'name' column");
        assert_ne!(fields[4], r"\N", "_eur suffix value present");
        assert_ne!(fields[5], r"\N", "_usd suffix value present");
    }

    #[test]
    fn structured_pivot_key_without_suffix_goes_to_value_column() {
        let ss = make_suffix_schema(&["_eur"]);
        let schema = make_structured_pivot_schema("prices", "root", &ss);
        let mut sinks = make_sinks(&["prices"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"standalone": "val"}"#).unwrap();

        super::insert_structured_pivot_object(&mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id(), &ss).unwrap();

        let rows = &sinks["prices"].0;
        assert_eq!(rows.len(), 1);
        let fields = parse_fields(&rows[0]);
        assert_eq!(fields[2], "standalone", "key with no suffix → 'name' column");
        assert_eq!(fields[3], "val", "value goes to 'value' column (suffix \"\")");
        assert_eq!(fields[4], r"\N", "suffix column absent → \\N");
    }

    #[test]
    fn structured_pivot_multiple_bases_emit_multiple_rows() {
        let ss = make_suffix_schema(&["_eur"]);
        let schema = make_structured_pivot_schema("prices", "root", &ss);
        let mut sinks = make_sinks(&["prices"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"cost_eur": "1", "price_eur": "2"}"#).unwrap();

        super::insert_structured_pivot_object(&mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id(), &ss).unwrap();

        assert_eq!(sinks["prices"].0.len(), 2, "two distinct bases → two rows");
    }

    // ---------------------------------------------------------------------------
    // insert_sibling_collapse_object tests
    // ---------------------------------------------------------------------------

    use crate::schema::table_schema::{KeyShape, SiblingSchema};

    fn make_sibling_collapse_schema(name: &str, parent: &str, key_col: &str, data_cols: &[&str]) -> (crate::schema::table_schema::TableSchema, SiblingSchema) {
        use crate::schema::table_schema::InferredStrategy;
        let ss = SiblingSchema { key_col_name: key_col.to_string(), key_shape: KeyShape::Mixed, array_children: false };
        let mut s = crate::schema::table_schema::TableSchema::new(name.to_string(), vec![name.to_string()], 1);
        s.inferred_strategy = InferredStrategy::SiblingCollapse(ss.clone());
        let mut cols = vec![generated_id(), parent_fk(parent), col(key_col, PgType::Text)];
        for dc in data_cols { cols.push(col(dc, PgType::Text)); }
        s.columns = cols;
        (s, ss)
    }

    #[test]
    fn sibling_collapse_non_object_value_silently_skipped_zero_rows() {
        let (schema, ss) = make_sibling_collapse_schema("items", "root", "key", &["a"]);
        let mut sinks = make_sinks(&["items"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"k": "scalar_not_object"}"#).unwrap();

        super::insert_sibling_collapse_object(&path_map, &mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id(), &ss).unwrap();

        assert_eq!(sinks["items"].0.len(), 0, "scalar value → silently skipped, zero rows");
        assert_eq!(anomalies.totals.get("items").copied().unwrap_or(0), 0);
    }

    #[test]
    fn sibling_collapse_nominal_one_object_emits_one_row() {
        let (schema, ss) = make_sibling_collapse_schema("items", "root", "key", &["a", "b"]);
        let mut sinks = make_sinks(&["items"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"mykey": {"a": "v1", "b": "v2"}}"#).unwrap();

        super::insert_sibling_collapse_object(&path_map, &mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id(), &ss).unwrap();

        let rows = &sinks["items"].0;
        assert_eq!(rows.len(), 1);
        let fields = parse_fields(&rows[0]);
        // fields: j2s_id, j2s_parent_id, key, a, b
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[2], "mykey", "sibling key in key column");
        assert_eq!(fields[3], "v1");
        assert_eq!(fields[4], "v2");
    }

    #[test]
    fn sibling_collapse_multiple_keys_emit_multiple_rows() {
        let (schema, ss) = make_sibling_collapse_schema("items", "root", "key", &["val"]);
        let mut sinks = make_sinks(&["items"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"k1": {"val": "a"}, "k2": {"val": "b"}}"#).unwrap();

        super::insert_sibling_collapse_object(&path_map, &mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id(), &ss).unwrap();

        assert_eq!(sinks["items"].0.len(), 2, "two sibling keys → two rows");
    }

    // ---------------------------------------------------------------------------
    // insert_sibling_collapse_multi tests
    // ---------------------------------------------------------------------------

    use crate::schema::table_schema::SiblingGroup;

    fn make_routing_schema(name: &str, parent: &str, path: &[&str]) -> crate::schema::table_schema::TableSchema {
        let mut s = crate::schema::table_schema::TableSchema::new(
            name.to_string(), path.iter().map(|p| p.to_string()).collect(), 1
        );
        s.columns = vec![generated_id(), parent_fk(parent)];
        s
    }

    fn make_group(path_segment: &str, key_is_numeric: bool, absorbed: &[&str], key_col: &str) -> SiblingGroup {
        SiblingGroup {
            pivot_table: format!("pivot_{path_segment}"),
            key_is_numeric,
            sibling_schema: SiblingSchema { key_col_name: key_col.to_string(), key_shape: KeyShape::Mixed, array_children: false },
            absorbed_names: vec![],
            path_segment: path_segment.to_string(),
            absorbed_path_segments: absorbed.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn multi_empty_groups_emits_only_routing_row() {
        let schema = make_routing_schema("multi", "root", &["multi"]);
        let mut sinks = make_sinks(&["multi"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"k": {"a": 1}}"#).unwrap();

        super::insert_sibling_collapse_multi(&path_map, &mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id(), &[]).unwrap();

        assert_eq!(sinks["multi"].0.len(), 1, "one routing row emitted even with no groups");
    }

    #[test]
    fn multi_routes_by_key_is_numeric() {
        let schema = make_routing_schema("multi", "root", &["multi"]);
        let groups = vec![
            make_group("key", false, &[], "k"),
            make_group("num", true, &[], "k"),
        ];

        let (pivot_key_schema, pivot_key_ss) = make_sibling_collapse_schema("pivot_key", "multi", "k", &["val"]);
        let (pivot_num_schema, pivot_num_ss) = make_sibling_collapse_schema("pivot_num", "multi", "k", &["val"]);
        // Override sibling_schema from groups (not used directly — pivot schemas get their own)
        let _ = (pivot_key_ss, pivot_num_ss);

        let mut path_map = HashMap::new();
        path_map.insert("multi\x00key".to_string(), pivot_key_schema);
        path_map.insert("multi\x00num".to_string(), pivot_num_schema);

        let mut sinks = make_sinks(&["multi", "pivot_key", "pivot_num"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"foo": {"val": "x"}, "42": {"val": "y"}}"#).unwrap();

        super::insert_sibling_collapse_multi(&path_map, &mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id(), &groups).unwrap();

        assert_eq!(sinks["multi"].0.len(), 1, "routing row");
        assert_eq!(sinks["pivot_key"].0.len(), 1, "non-numeric key 'foo' → pivot_key");
        assert_eq!(sinks["pivot_num"].0.len(), 1, "numeric key '42' → pivot_num");
    }

    #[test]
    fn multi_absorbed_path_segments_match_takes_priority_over_key_is_numeric() {
        let schema = make_routing_schema("multi", "root", &["multi"]);
        // Both groups non-numeric — disambiguation via absorbed_path_segments
        let groups = vec![
            make_group("grp0", false, &["foo"], "k"),
            make_group("grp1", false, &["bar"], "k"),
        ];

        let (pivot_grp0_schema, _) = make_sibling_collapse_schema("pivot_grp0", "multi", "k", &["val"]);
        let (pivot_grp1_schema, _) = make_sibling_collapse_schema("pivot_grp1", "multi", "k", &["val"]);

        let mut path_map = HashMap::new();
        path_map.insert("multi\x00grp0".to_string(), pivot_grp0_schema);
        path_map.insert("multi\x00grp1".to_string(), pivot_grp1_schema);

        let mut sinks = make_sinks(&["multi", "pivot_grp0", "pivot_grp1"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"foo": {"val": "f"}, "bar": {"val": "b"}}"#).unwrap();

        super::insert_sibling_collapse_multi(&path_map, &mut sinks, &mut anomalies, &schema, &obj, dummy_parent_id(), &groups).unwrap();

        assert_eq!(sinks["pivot_grp0"].0.len(), 1, "'foo' → grp0 via absorbed_path_segments");
        assert_eq!(sinks["pivot_grp1"].0.len(), 1, "'bar' → grp1 via absorbed_path_segments");
    }

    // ---------------------------------------------------------------------------
    // find_group_for_key tests
    // ---------------------------------------------------------------------------

    fn group(key_is_numeric: bool, absorbed: &[&str]) -> SiblingGroup {
        make_group(if key_is_numeric { "num" } else { "key" }, key_is_numeric, absorbed, "k")
    }

    #[test]
    fn find_group_exact_match_absorbed_path_segments() {
        let groups = vec![group(false, &["foo", "bar"]), group(true, &["42"])];
        assert_eq!(super::find_group_for_key(&groups, "foo"), Some(0));
        assert_eq!(super::find_group_for_key(&groups, "bar"), Some(0));
        assert_eq!(super::find_group_for_key(&groups, "42"), Some(1));
    }

    #[test]
    fn find_group_fallback_numeric_key() {
        let groups = vec![group(false, &[]), group(true, &[])];
        assert_eq!(super::find_group_for_key(&groups, "123"), Some(1), "all-digit key → numeric group");
    }

    #[test]
    fn find_group_fallback_non_numeric_key() {
        let groups = vec![group(false, &[]), group(true, &[])];
        assert_eq!(super::find_group_for_key(&groups, "abc"), Some(0), "non-digit key → non-numeric group");
    }

    #[test]
    fn find_group_key_absent_returns_none() {
        let groups = vec![group(true, &["42"])]; // only numeric group
        assert_eq!(super::find_group_for_key(&groups, "foo"), None, "no matching group → None");
    }

    #[test]
    fn find_group_ambiguous_two_non_numeric_no_segments_returns_first() {
        let groups = vec![
            make_group("grp0", false, &[], "k"),
            make_group("grp1", false, &[], "k"),
        ];
        assert_eq!(super::find_group_for_key(&groups, "foo"), Some(0), "tie-break → first group wins");
    }

    #[test]
    fn find_group_absorbed_segments_take_priority_over_key_is_numeric() {
        // Key "123" is all-digits, but group 0 has it in absorbed_path_segments → should win over group 1 (numeric)
        let groups = vec![group(false, &["123"]), group(true, &[])];
        assert_eq!(super::find_group_for_key(&groups, "123"), Some(0), "exact match wins over key_is_numeric heuristic");
    }

    // ---------------------------------------------------------------------------
    // route_multi_collapse_children tests
    // ---------------------------------------------------------------------------

    #[test]
    fn route_multi_collapse_children_routes_both_groups_without_routing_row() {
        // Verifies that route_multi_collapse_children uses the supplied routing_id
        // (no emit_routing_row) and correctly distributes keys to each pivot group.
        let schema = make_routing_schema("root", "root", &["root"]);
        let groups = vec![
            make_group("a_key", false, &["a1", "a2"], "key"),
            make_group("b_key", false, &["b1", "b2"], "key"),
        ];

        let (pivot_a_schema, _) = make_sibling_collapse_schema("pivot_a_key", "root", "key", &["color"]);
        let (pivot_b_schema, _) = make_sibling_collapse_schema("pivot_b_key", "root", "key", &["size"]);

        let sep = crate::schema::PATH_SEP.to_string();
        let mut path_map = HashMap::new();
        path_map.insert(format!("root{sep}a_key"), pivot_a_schema);
        path_map.insert(format!("root{sep}b_key"), pivot_b_schema);

        // sinks: root sink intentionally absent to prove no routing row is emitted
        let mut sinks = make_sinks(&["pivot_a_key", "pivot_b_key"]);
        let mut anomalies = CountingAnomaly::default();
        let routing_id = uuid::Uuid::now_v7();

        let obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(
            r#"{"a1": {"color": "red"}, "a2": {"color": "blue"}, "b1": {"size": "10"}, "b2": {"size": "20"}}"#
        ).unwrap();

        super::route_multi_collapse_children(&path_map, &mut sinks, &mut anomalies, &schema, &obj, routing_id, &groups).unwrap();

        assert_eq!(sinks["pivot_a_key"].0.len(), 2, "a1 + a2 → pivot_a_key");
        assert_eq!(sinks["pivot_b_key"].0.len(), 2, "b1 + b2 → pivot_b_key");
        // root sink is absent → no routing row emitted by route_multi_collapse_children
    }

    // ---------------------------------------------------------------------------
    // [issue #45, tâche 10] insert_array : dispatch ObjectArray vers insert_jsonb_object
    // quand effective_strategy()==Jsonb, au lieu d'insert_object qui écrirait NULL
    // (le schéma Jsonb n'a qu'une colonne "data" — obj.get("data") ne trouve jamais
    // la clé JSON réelle, insert_object pousserait NULL silencieusement).
    // ---------------------------------------------------------------------------

    #[test]
    fn insert_array_object_array_jsonb_writes_blob_not_null() {
        let schema = make_jsonb_object_array_schema("reviews", "produits");
        let mut sinks = make_sinks(&["reviews"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let arr = vec![
            serde_json::json!({"score": 5, "note": "top"}),
            serde_json::json!({"score": 2}),
        ];

        super::insert_array(&path_map, &mut sinks, &mut anomalies, &schema, &arr, dummy_parent_id()).unwrap();

        let rows = &sinks["reviews"].0;
        assert_eq!(rows.len(), 2, "un élément d'array → une ligne JSONB, pas un no-op silencieux");
        for (i, row) in rows.iter().enumerate() {
            let fields = parse_fields(row);
            assert_eq!(fields.len(), 4, "uuid, parent_uuid, order, data");
            assert_ne!(fields[3], r"\N", "le blob JSON ne doit pas être NULL (bug pré-#45)");
            assert_eq!(fields[2], i.to_string(), "j2s_order doit suivre la position dans l'array");
        }
        let first: serde_json::Value = serde_json::from_str(parse_fields(&rows[0]).remove(3)).unwrap();
        assert_eq!(first["score"], 5);
        assert_eq!(first["note"], "top");
    }

    #[test]
    fn insert_array_object_array_columns_unaffected() {
        // Non-régression : le dispatch générique existant (insert_object, cf. tâches 6/7 —
        // AutoSplit après le split identité/compagnon, ou Columns simple) ne doit pas être
        // détourné par le nouveau branchement Jsonb quand effective_strategy() != Jsonb.
        let mut schema = TableSchema::new("items".to_string(), vec!["items".to_string()], 1);
        schema.child_kind = Some(ChildKind::ObjectArray);
        schema.columns = vec![generated_id(), parent_fk("root"), generated_order(), col("a", PgType::Text)];
        let mut sinks = make_sinks(&["items"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let arr = vec![serde_json::json!({"a": "x"})];

        super::insert_array(&path_map, &mut sinks, &mut anomalies, &schema, &arr, dummy_parent_id()).unwrap();

        let fields = parse_fields(&sinks["items"].0[0]);
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[3], "x", "colonne 'a' peuplée depuis la clé JSON réelle — chemin Columns inchangé");
    }

    // Smoke tests for fakes
    // ---------------------------------------------------------------------------

    #[test]
    fn capture_sink_accumulates_rows() {
        let mut s = CaptureSink(vec![]);
        s.write_row(b"row1\n").unwrap();
        s.write_row(b"row2\n").unwrap();
        assert_eq!(s.0.len(), 2);
        assert_eq!(s.0[0], b"row1\n");
    }

    #[test]
    fn counting_anomaly_record_returns_ok_and_counts() {
        let mut a = CountingAnomaly::default();
        a.record("t", "col", "id", "int4", "bad", "string").unwrap();
        a.inc_total("t");
        assert_eq!(a.records, vec![("t".to_string(), "col".to_string())]);
        assert_eq!(a.totals["t"], 1);
    }
}
