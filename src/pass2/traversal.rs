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
//! - fn `push_coerced_as` — idem pour une colonne donnée par son nom et son type.
//! - fn `push_text` — écrit un texte échappé pour COPY (`NULL` s'il contient un octet nul).
//! - fn `write_row` — compte une ligne et l'écrit dans le sink de sa table ; sink absent → erreur (#167).
//!
//! Pivot :
//! - fn `insert_pivot_object` — une ligne par paire clé/valeur, construite selon `schema.columns` (#172).
//!
//! Jsonb :
//! - fn `insert_jsonb_object` — une ligne JSONB puis récursion enfants.
//!
//! `StructuredPivot` :
//! - fn `group_keys_by_base` — regroupe les clés par préfixe/suffixe.
//! - fn `write_structured_pivot_row` — écrit une ligne fusionnée.
//! - fn `insert_structured_pivot_object` — orchestre les deux.
//!
//! Routing générique :
//! - fn `dispatch_child_routes` — route les sous-objets/arrays de `schema.child_routes`
//!   vers leurs tables cibles (post-cascade merge).
//! - fn `write_keyed_route_row` — route réécrite par la post-passe keyed-pivot : la clé routée est la clé
//!   de la ligne cible (#173).
//!
//! `SiblingCollapse` (simple + multi) :
//! - struct `SiblingCollapseRowInput` — paramètres empaquetés d'une ligne keyed-pivot à écrire.
//! - fn `write_keyed_pivot_columns` — écrit les colonnes communes d'une ligne keyed-pivot.
//! - fn `write_sibling_row` — écrit une ligne keyed-pivot puis route ses sous-objets.
//! - fn `insert_sibling_collapse_object` — une ligne par clé sibling.
//! - fn `insert_sibling_collapse_array_of_objects` — variante tableau d'objets (avec `j2s_order`) ;
//!   scalaires → anomalie.
//! - fn `route_independent_child` — route une clé vers un enfant indépendant si présent.
//! - fn `split_keys_by_group` — route les enfants indépendants, répartit les autres clés par groupe.
//! - fn `group_path_segment` — segment de chemin de la table pivot d'un groupe.
//! - fn `route_multi_collapse_children` — dispatch chaque clé vers son groupe pivot synthétique.
//! - fn `find_group_for_key` — résout le groupe cible d'une clé (match exact puis heuristique numérique).
//!
//! `NormalizeDynamicKeys` :
//! - fn `insert_normalize_dynamic_keys` — une ligne par clé dynamique.
//!
//! Tableaux :
//! - fn `keeps_scalars_in_array_column` — le parent garde-t-il les scalaires du champ dans une colonne tableau.
//! - fn `record_dropped_scalar` — anomalie pour une valeur non objet là où un objet est attendu, sans table où aller (#86, #162).
//! - fn `record_missing_table` — anomalie pour une clé objet ou tableau sans table, sauf abandon voulu (#167).
//! - fn `insert_array` — tableau d'objets : une ligne par élément (`ObjectArray`) ; scalaires → anomalie,
//!   ou blob en `Jsonb`.

use std::collections::{BTreeMap, HashMap};

use serde_json::Value;
use uuid::Uuid;

use crate::anomaly::collector::AnomalyCollect;
use crate::db::copy_sink::RowBuilder;
use crate::db::copy_text::escape_copy_text;
use crate::error::{J2sError, Result};
use crate::pass2::coercer::{coerce, json_type_name, CoerceResult};
use crate::pass2::sink::RowSink;
use crate::schema::table_schema::{
    ChildKind, ColumnSchema, InferredStrategy, SiblingGroup, SiblingSchema, SuffixSchema,
    TableSchema,
};
use crate::schema::type_tracker::PgType;
use crate::schema::PATH_SEP;

use super::insert::{
    insert_child_object, push_generated_col_insert, push_jsonb_col, recurse_children, InsertCtx,
};

pub(super) fn push_coerced<A: AnomalyCollect>(
    builder: &mut RowBuilder,
    anomalies: &mut A,
    table_name: &str,
    col: &ColumnSchema,
    row_id: Uuid,
    val: &Value,
) -> Result<()> {
    push_coerced_as(
        builder,
        anomalies,
        table_name,
        &col.name,
        &col.pg_type,
        row_id,
        val,
    )
}

/// Coerce `val` to `pg_type` and push it. A value that does not fit is recorded as an anomaly
/// of `table_name.column` (the row id is only formatted then).
pub(super) fn push_coerced_as<A: AnomalyCollect>(
    builder: &mut RowBuilder,
    anomalies: &mut A,
    table_name: &str,
    column: &str,
    pg_type: &PgType,
    row_id: Uuid,
    val: &Value,
) -> Result<()> {
    let (value, anomaly) = match coerce(val, pg_type) {
        CoerceResult::Ok(s) => (Some(s), None),
        CoerceResult::Null => (None, None),
        CoerceResult::OkWithAnomaly {
            value,
            actual_value,
            actual_type,
        } => (Some(value), Some((actual_value, actual_type))),
        CoerceResult::Anomaly {
            actual_value,
            actual_type,
        } => (None, Some((actual_value, actual_type))),
    };
    match value {
        Some(s) => builder.push_value(&s),
        None => builder.push_null(),
    }
    let Some((actual_value, actual_type)) = anomaly else {
        return Ok(());
    };
    let (row_id, expected) = (row_id.to_string(), pg_type.as_sql());
    anomalies.record(
        table_name,
        column,
        &row_id,
        &expected,
        &actual_value,
        actual_type,
    )
}

/// Push `text` escaped for COPY, or `NULL` when it holds a null byte.
pub(super) fn push_text(builder: &mut RowBuilder, text: &str) {
    match escape_copy_text(text) {
        Some(escaped) => builder.push_value(&escaped),
        None => builder.push_null(),
    }
}

/// Count one row for `table`, then write it to the table's sink. Every table of the schema has a sink, so a missing
/// one is a routing bug: an error rather than a row counted and lost (#167).
pub(super) fn write_row<S: RowSink>(
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    table: &str,
    builder: RowBuilder,
) -> Result<()> {
    let Some(sink) = sinks.get_mut(table) else {
        return Err(J2sError::Schema(format!(
            "Pass 2 : la table '{table}' n'a pas de sink, la ligne serait perdue"
        )));
    };
    anomalies.inc_total(table);
    sink.write_row(&builder.finish())
}

/// Insert one row per key-value pair for a Pivot wide table.
/// Columns: generated ones (`j2s_id`, parent FK, `j2s_order` for an array element), key TEXT, value <type>. The row
/// follows `schema.columns`, as in `insert_jsonb_object` (#172).
pub(super) fn insert_pivot_object<S: RowSink>(
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    order: Option<i64>,
) -> Result<()> {
    for (key, val) in obj {
        let child_id = Uuid::now_v7();
        let mut builder = RowBuilder::new();
        for col in &schema.columns {
            if col.is_generated {
                push_generated_col_insert(&mut builder, col, child_id, Some(parent_id), order);
            } else if col.original_name == "key" {
                push_text(&mut builder, key);
            } else {
                push_coerced(&mut builder, anomalies, &schema.name, col, child_id, val)?;
            }
        }
        write_row(sinks, anomalies, &schema.name, builder)?;
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
    write_row(sinks, anomalies, &schema.name, builder)?;
    if let Value::Object(obj) = value {
        let ctx = &mut InsertCtx { sinks, anomalies };
        recurse_children(path_map, ctx, schema, obj, child_id)?;
    }
    Ok(())
}

/// Group JSON object keys by their base name: `base → { suffix → &Value }`.
/// Keys with no matching suffix use an empty string as the suffix key (→ `value` column).
/// A key goes to the longest suffix that ends it, as in Pass 1 (`fat_prepared_100g` →
/// `_prepared_100g`, not `_100g`).
fn group_keys_by_base<'a>(
    entries: impl IntoIterator<Item = (&'a String, &'a Value)>,
    suffix_schema: &SuffixSchema,
) -> BTreeMap<String, HashMap<String, &'a Value>> {
    let mut groups: BTreeMap<String, HashMap<String, &'a Value>> = BTreeMap::new();
    for (key, val) in entries {
        let matched_suffix = suffix_schema
            .suffix_cols
            .iter()
            .filter(|sc| key.len() > sc.suffix.len() && key.ends_with(sc.suffix.as_str()))
            .max_by_key(|sc| sc.suffix.len())
            .map(|sc| sc.suffix.as_str());
        match matched_suffix {
            Some(suffix) => {
                let base = &key[..key.len() - suffix.len()];
                groups
                    .entry(base.to_string())
                    .or_default()
                    .insert(suffix.to_string(), val);
            }
            None => {
                groups
                    .entry(key.clone())
                    .or_default()
                    .insert(String::new(), val);
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
    order: Option<i64>,
) -> Result<()> {
    let child_id = Uuid::now_v7();
    let mut builder = RowBuilder::new();
    for col in &schema.columns {
        if col.is_generated {
            push_generated_col_insert(&mut builder, col, child_id, Some(parent_id), order);
            continue;
        }
        if col.original_name == "name" {
            push_text(&mut builder, base);
            continue;
        }
        // The bare base key (no suffix) fills the `value` column.
        let suffix = if col.original_name == "value" {
            ""
        } else {
            col.original_name.as_str()
        };
        match suffix_vals.get(suffix) {
            Some(&val) => push_coerced(&mut builder, anomalies, &schema.name, col, child_id, val)?,
            None => builder.push_null(),
        }
    }
    write_row(sinks, anomalies, &schema.name, builder)
}

/// For each JSON key, we check whether it ends with a known suffix.
/// Keys that match no suffix are treated as bare base keys (→ `value` column).
/// All keys sharing the same base are collapsed into a single row.
pub(super) fn insert_structured_pivot_object<'a, S: RowSink>(
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    entries: impl IntoIterator<Item = (&'a String, &'a Value)>,
    parent_id: Uuid,
    order: Option<i64>,
    suffix_schema: &SuffixSchema,
) -> Result<()> {
    let groups = group_keys_by_base(entries, suffix_schema);
    for (base, suffix_vals) in groups {
        write_structured_pivot_row(
            sinks,
            anomalies,
            schema,
            &base,
            &suffix_vals,
            parent_id,
            order,
        )?;
    }
    Ok(())
}

/// Route sub-objects and sub-arrays declared in `schema.child_routes` to their target tables.
/// Called after writing each pivot row so that nested tables created by cascade merging
/// receive their data with the correct parent FK.
#[allow(clippy::too_many_lines)] // one loop: keyed route first, then direct route by value type (#173)
pub(super) fn dispatch_child_routes<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    child_obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
) -> Result<()> {
    for (sub_key, child_table_name) in &schema.child_routes {
        let Some(sub_value) = child_obj.get(sub_key) else {
            continue;
        };
        let Some(child_schema) = path_map.values().find(|s| s.name == *child_table_name) else {
            continue;
        };
        if write_keyed_route_row(
            path_map,
            sinks,
            anomalies,
            child_schema,
            sub_key,
            sub_value,
            row_id,
        )? {
            continue;
        }
        match sub_value {
            Value::Object(nested) => {
                let ctx = &mut InsertCtx { sinks, anomalies };
                insert_child_object(path_map, ctx, child_schema, nested, sub_value, row_id, None)?;
            }
            Value::Array(arr) => {
                let keeps = keeps_scalars_in_array_column(schema, sub_key);
                insert_array(path_map, sinks, anomalies, child_schema, arr, row_id, keeps)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// A route the keyed post-pass rewrote (#173): `sub_key` is not the JSON key of `target` but the
/// key of one of its rows (`fr` → `selected_key`, whose path ends in `key`). Writes that row and
/// returns `true`; `false` for a direct route, or a target that does not write one row per key.
#[allow(clippy::too_many_lines)] // one row: route check, strategy's key column, then the write (#173)
fn write_keyed_route_row<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    target: &TableSchema,
    sub_key: &str,
    sub_value: &Value,
    parent_id: Uuid,
) -> Result<bool> {
    // shortcut: a row key spelled like the target's last path segment ("key") reads as a direct
    // route; mark keyed routes in the schema if such a key shows up.
    if target.path.last().is_some_and(|last| last == sub_key) {
        return Ok(false);
    }
    let strategy = target.effective_strategy();
    let (key_col_name, routes_children) = match &*strategy {
        InferredStrategy::SiblingCollapse(s) if !s.array_children => (&s.key_col_name[..], true),
        InferredStrategy::NormalizeDynamicKeys { id_column } => (&id_column[..], false),
        _ => return Ok(false),
    };
    let Value::Object(child_obj) = sub_value else {
        if !sub_value.is_null() {
            record_dropped_scalar(anomalies, &target.name, sub_key, parent_id, sub_value)?;
        }
        return Ok(true);
    };
    let row = SiblingCollapseRowInput {
        key: sub_key,
        child_obj,
        parent_id,
        row_id: Uuid::now_v7(),
        order: None,
        key_col_name,
    };
    if routes_children {
        write_sibling_row(path_map, sinks, anomalies, target, &row)?;
    } else {
        let mut builder = RowBuilder::new();
        write_keyed_pivot_columns(&mut builder, anomalies, target, &row)?;
        write_row(sinks, anomalies, &target.name, builder)?;
    }
    Ok(true)
}

/// One keyed-pivot row to write: a `SiblingCollapse` key, or a `NormalizeDynamicKeys` key.
struct SiblingCollapseRowInput<'a> {
    key: &'a str,
    child_obj: &'a serde_json::Map<String, Value>,
    parent_id: Uuid,
    row_id: Uuid,
    /// Fills `j2s_order`: the index in the array of objects of the key, or of the element the table is for (#172).
    order: Option<i64>,
    /// The column that receives `key`.
    key_col_name: &'a str,
}

#[allow(clippy::too_many_lines)] // single per-column dispatch loop, one responsibility (build one row)
fn write_keyed_pivot_columns<A: AnomalyCollect>(
    builder: &mut RowBuilder,
    anomalies: &mut A,
    schema: &TableSchema,
    ctx: &SiblingCollapseRowInput<'_>,
) -> Result<()> {
    for col in &schema.columns {
        if col.is_generated {
            push_generated_col_insert(builder, col, ctx.row_id, Some(ctx.parent_id), ctx.order);
            continue;
        }
        if col.original_name == ctx.key_col_name {
            push_text(builder, ctx.key);
            continue;
        }
        let json_val = ctx
            .child_obj
            .get(&col.original_name)
            .unwrap_or(&Value::Null);
        if matches!(json_val, Value::Object(_))
            || (matches!(json_val, Value::Array(_)) && !matches!(col.pg_type, PgType::Array(_)))
        {
            builder.push_null();
            continue;
        }
        push_coerced(builder, anomalies, &schema.name, col, ctx.row_id, json_val)?;
    }
    Ok(())
}

/// Write one keyed-pivot row, then route its sub-objects declared in `schema.child_routes`.
fn write_sibling_row<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    row: &SiblingCollapseRowInput<'_>,
) -> Result<()> {
    let mut builder = RowBuilder::new();
    write_keyed_pivot_columns(&mut builder, anomalies, schema, row)?;
    write_row(sinks, anomalies, &schema.name, builder)?;
    dispatch_child_routes(
        path_map,
        sinks,
        anomalies,
        schema,
        row.child_obj,
        row.row_id,
    )
}

/// Insert one row per sibling key for a `SiblingCollapse` table.
/// Columns: `j2s_id`, `j2s_parent_id`, key TEXT, <union data cols...>
///
/// When `sibling_schema.array_children` is true, each key maps to an array of objects
/// instead of a single object. One row is emitted per array element, with `j2s_order` set.
#[allow(clippy::too_many_arguments)] // order joined the six inputs every keyed-pivot writer takes (#172)
pub(super) fn insert_sibling_collapse_object<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    order: Option<i64>,
    sibling_schema: &SiblingSchema,
) -> Result<()> {
    if sibling_schema.array_children {
        return insert_sibling_collapse_array_of_objects(
            path_map,
            sinks,
            anomalies,
            schema,
            obj,
            parent_id,
            sibling_schema,
        );
    }
    for (key, value) in obj {
        let Value::Object(child_obj) = value else {
            if !value.is_null() {
                record_dropped_scalar(anomalies, &schema.name, key, parent_id, value)?;
            }
            continue;
        };
        let row = SiblingCollapseRowInput {
            key,
            child_obj,
            parent_id,
            row_id: Uuid::now_v7(),
            order,
            key_col_name: &sibling_schema.key_col_name,
        };
        write_sibling_row(path_map, sinks, anomalies, schema, &row)?;
    }
    Ok(())
}

/// Insert one row per array element per sibling key for an `ObjectArray` `SiblingCollapse` table.
/// Columns: `j2s_id`, `j2s_parent_id`, `j2s_order` BIGINT, key TEXT, <union data cols...>
#[allow(clippy::cast_possible_wrap)] // array index never reaches i64::MAX
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
        if let Some(item) = arr.iter().find(|v| !v.is_object() && !v.is_null()) {
            record_dropped_scalar(anomalies, &schema.name, key, parent_id, item)?;
        }
        for (order, item) in arr.iter().enumerate() {
            let Value::Object(item_obj) = item else {
                continue;
            };
            let row = SiblingCollapseRowInput {
                key,
                child_obj: item_obj,
                parent_id,
                row_id: Uuid::now_v7(),
                order: Some(order as i64),
                key_col_name: &sibling_schema.key_col_name,
            };
            write_sibling_row(path_map, sinks, anomalies, schema, &row)?;
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
    order: Option<i64>,
    id_column: &str,
) -> Result<()> {
    for (key, value) in obj {
        let Value::Object(child_obj) = value else {
            if !value.is_null() {
                record_dropped_scalar(anomalies, &schema.name, key, parent_id, value)?;
            }
            continue;
        };
        let row = SiblingCollapseRowInput {
            key,
            child_obj,
            parent_id,
            row_id: Uuid::now_v7(),
            order,
            key_col_name: id_column,
        };
        let mut builder = RowBuilder::new();
        write_keyed_pivot_columns(&mut builder, anomalies, schema, &row)?;
        write_row(sinks, anomalies, &schema.name, builder)?;
    }
    Ok(())
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
    let Some(child_schema) = path_map.get(child_path_key) else {
        return Ok(false);
    };
    if child_schema.parent_table.as_deref() != Some(schema_name) {
        return Ok(false);
    }
    let Value::Object(nested) = value else {
        return Ok(false);
    };
    let ctx = &mut InsertCtx { sinks, anomalies };
    insert_child_object(path_map, ctx, child_schema, nested, value, routing_id, None)?;
    Ok(true)
}

/// Route the keys of `obj` that have an independent child table, and split the others by
/// the group they belong to (one sub-object per group, in `groups` order).
fn split_keys_by_group<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    routing_id: Uuid,
    groups: &[SiblingGroup],
) -> Result<Vec<serde_json::Map<String, Value>>> {
    let routing_path = schema.path.join(&PATH_SEP.to_string());
    let mut group_submaps = vec![serde_json::Map::new(); groups.len()];
    for (key, value) in obj {
        let child_path_key = format!("{routing_path}{PATH_SEP}{key}");
        if route_independent_child(
            path_map,
            sinks,
            anomalies,
            &schema.name,
            value,
            &child_path_key,
            routing_id,
        )? {
            continue;
        }
        if let Some(idx) = find_group_for_key(groups, key) {
            group_submaps[idx].insert(key.clone(), value.clone());
        }
    }
    Ok(group_submaps)
}

/// Path segment of a group's pivot table under the routing table.
fn group_path_segment(group: &SiblingGroup) -> &str {
    if !group.path_segment.is_empty() {
        &group.path_segment
    } else if group.key_is_numeric {
        "num"
    } else {
        "key"
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
/// Called by `insert_object`, which writes the routing row of every `SiblingCollapseMulti` table.
pub(super) fn route_multi_collapse_children<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    routing_id: Uuid,
    groups: &[SiblingGroup],
) -> Result<()> {
    let group_submaps =
        split_keys_by_group(path_map, sinks, anomalies, schema, obj, routing_id, groups)?;
    let routing_path = schema.path.join(&PATH_SEP.to_string());
    for (group, submap) in groups.iter().zip(group_submaps.iter()) {
        if submap.is_empty() {
            continue;
        }
        let pivot_path_key = format!("{routing_path}{PATH_SEP}{}", group_path_segment(group));
        if let Some(pivot_schema) = path_map.get(&pivot_path_key) {
            insert_sibling_collapse_object(
                path_map,
                sinks,
                anomalies,
                pivot_schema,
                submap,
                routing_id,
                None,
                &group.sibling_schema,
            )?;
        } else {
            let segment = group_path_segment(group);
            let value = Value::Object(submap.clone());
            record_missing_table(path_map, anomalies, schema, segment, routing_id, &value)?;
        }
    }
    Ok(())
}

/// Route a runtime JSON key to the matching `SiblingGroup` index.
///
/// Tries exact match in `absorbed_path_segments` first (populated by Pass 1 for all
/// schemas produced since the fix). Falls back to the `key_is_numeric` heuristic so
/// that old snapshots (empty `absorbed_path_segments`) continue to work correctly.
fn find_group_for_key(groups: &[SiblingGroup], key: &str) -> Option<usize> {
    if let Some(idx) = groups
        .iter()
        .position(|g| g.absorbed_path_segments.iter().any(|s| s == key))
    {
        return Some(idx);
    }
    let key_is_numeric = key.chars().all(|c| c.is_ascii_digit());
    groups
        .iter()
        .position(|g| g.key_is_numeric == key_is_numeric)
}

/// True when `parent` stores `field` in an array column: the scalars of that field are kept
/// there, so its child `ObjectArray` table must not report them as lost (#86).
pub(super) fn keeps_scalars_in_array_column(parent: &TableSchema, field: &str) -> bool {
    parent
        .find_by_original(field)
        .is_some_and(|c| matches!(c.pg_type, PgType::Array(_)))
}

/// One anomaly for a non-object value where an object is expected: the scalars of an
/// `ObjectArray` (first one as the value, #86), or a scalar under a `SiblingCollapse` /
/// `NormalizeDynamicKeys` key (#162). These tables only have columns for object fields.
fn record_dropped_scalar(
    anomalies: &mut impl AnomalyCollect,
    table: &str,
    column: &str,
    parent_id: Uuid,
    item: &Value,
) -> Result<()> {
    anomalies.record(
        table,
        column,
        &parent_id.to_string(),
        "object",
        &item.to_string(),
        json_type_name(item),
    )
}

/// One anomaly for an object or array under `parent` whose key has no table to go to (#167), unless it is dropped on
/// purpose: empty, kept by the parent (its `Jsonb` blob, a column of the same name on it or on one of its column
/// groups, a flattened child) or Skip-ed by the user.
pub(super) fn record_missing_table(
    path_map: &HashMap<String, TableSchema>,
    anomalies: &mut impl AnomalyCollect,
    parent: &TableSchema,
    field: &str,
    parent_id: Uuid,
    value: &Value,
) -> Result<()> {
    let empty = match value {
        Value::Object(o) => o.is_empty(),
        Value::Array(a) => a.is_empty(),
        _ => true,
    };
    // Column groups take their columns and flatten sources from the parent (#78): they keep the field too.
    let keeps = |t: &TableSchema| {
        t.find_by_original(field).is_some() || t.flatten_sources.values().any(|f| f == field)
    };
    if empty
        || matches!(*parent.effective_strategy(), InferredStrategy::Jsonb)
        || keeps(parent)
        || parent
            .column_groups
            .iter()
            .filter_map(|g| path_map.get(g.as_str()))
            .any(keeps)
        || parent.skipped_children.iter().any(|f| f == field)
    {
        return Ok(());
    }
    anomalies.record(
        &parent.name,
        field,
        &parent_id.to_string(),
        "table",
        &value.to_string(),
        json_type_name(value),
    )
}

#[allow(clippy::cast_possible_wrap)] // array index never reaches i64::MAX
pub(super) fn insert_array<S: RowSink>(
    path_map: &HashMap<String, TableSchema>,
    sinks: &mut HashMap<String, S>,
    anomalies: &mut impl AnomalyCollect,
    schema: &TableSchema,
    arr: &[Value],
    parent_id: Uuid,
    parent_keeps_scalars: bool,
) -> Result<()> {
    if !matches!(schema.child_kind, Some(ChildKind::ObjectArray)) {
        return Ok(());
    }
    // An object element goes through the same dispatch as an object child (#172); in Jsonb a scalar element is
    // kept as a blob too (#45).
    let is_jsonb = matches!(*schema.effective_strategy(), InferredStrategy::Jsonb);
    let mut dropped: Option<&Value> = None;
    for (i, item) in arr.iter().enumerate() {
        let order = Some(i as i64);
        if !item.is_object() && (item.is_null() || parent_keeps_scalars) {
            continue;
        }
        if let Value::Object(obj) = item {
            let ctx = &mut InsertCtx { sinks, anomalies };
            insert_child_object(path_map, ctx, schema, obj, item, parent_id, order)?;
        } else if is_jsonb {
            insert_jsonb_object(path_map, sinks, anomalies, schema, item, parent_id, order)?;
        } else {
            // A scalar has no column in an ObjectArray table (#86).
            dropped.get_or_insert(item);
        }
    }
    if let Some(item) = dropped {
        let field = schema.path.last().map_or("", String::as_str);
        record_dropped_scalar(anomalies, &schema.name, field, parent_id, item)?;
    }
    Ok(())
}

#[cfg(test)]
#[allow(dead_code)]
#[cfg_attr(test, allow(clippy::disallowed_methods, clippy::too_many_lines))]
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
        ColumnSchema {
            name: name.to_string(),
            original_name: name.to_string(),
            pg_type,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        }
    }

    fn generated_id() -> ColumnSchema {
        ColumnSchema {
            name: "j2s_id".to_string(),
            original_name: "j2s_id".to_string(),
            pg_type: PgType::Uuid,
            not_null: true,
            is_generated: true,
            is_parent_fk: false,
        }
    }

    fn parent_fk(parent: &str) -> ColumnSchema {
        ColumnSchema {
            name: format!("j2s_{parent}_id"),
            original_name: format!("j2s_{parent}_id"),
            pg_type: PgType::Uuid,
            not_null: true,
            is_generated: true,
            is_parent_fk: true,
        }
    }

    fn generated_order() -> ColumnSchema {
        ColumnSchema {
            name: "j2s_order".to_string(),
            original_name: "j2s_order".to_string(),
            pg_type: PgType::BigInt,
            not_null: true,
            is_generated: true,
            is_parent_fk: false,
        }
    }

    /// Pivot schema: `j2s_id`, j2s_{parent}_id, key TEXT, value TEXT
    fn make_pivot_schema(name: &str, parent: &str) -> TableSchema {
        let mut s = TableSchema::new(name.to_string(), vec![name.to_string()], 1);
        s.inferred_strategy = InferredStrategy::Pivot;
        s.columns = vec![
            generated_id(),
            parent_fk(parent),
            col("key", PgType::Text),
            col("value", PgType::Text),
        ];
        s
    }

    /// Jsonb schema: `j2s_id`, j2s_{parent}_id, data TEXT (JSONB stored as text in COPY)
    fn make_jsonb_schema(name: &str, parent: &str) -> TableSchema {
        let mut s = TableSchema::new(name.to_string(), vec![name.to_string()], 1);
        s.inferred_strategy = InferredStrategy::Jsonb;
        s.columns = vec![generated_id(), parent_fk(parent), col("data", PgType::Text)];
        s
    }

    /// Jsonb schema for an `ObjectArray`-parent table: `j2s_id`, j2s_{parent}_id, `j2s_order`, data.
    fn make_jsonb_object_array_schema(name: &str, parent: &str) -> TableSchema {
        let mut s = make_jsonb_schema(name, parent);
        s.child_kind = Some(ChildKind::ObjectArray);
        s.columns.insert(2, generated_order());
        s
    }

    fn make_sinks(names: &[&str]) -> HashMap<String, CaptureSink> {
        names
            .iter()
            .map(|n| ((*n).to_string(), CaptureSink(vec![])))
            .collect()
    }

    fn dummy_parent_id() -> Uuid {
        Uuid::nil()
    }

    // ---------------------------------------------------------------------------
    // Row parsing helper
    // ---------------------------------------------------------------------------

    fn parse_fields(row: &[u8]) -> Vec<&str> {
        let row = row.strip_suffix(b"\n").unwrap_or(row);
        row.split(|&b| b == b'\t')
            .map(|f| std::str::from_utf8(f).unwrap())
            .collect()
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

        super::insert_pivot_object(
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
        )
        .unwrap();

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

        super::insert_pivot_object(
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
        )
        .unwrap();

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

        super::insert_pivot_object(
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
        )
        .unwrap();

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
        obj.insert(
            "ke\0y".to_string(),
            serde_json::Value::String("val".to_string()),
        );

        super::insert_pivot_object(
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
        )
        .unwrap();

        let rows = &sinks["attrs"].0;
        assert_eq!(rows.len(), 1);
        let fields = parse_fields(&rows[0]);
        assert_eq!(fields[2], r"\N", "null byte in key → \\N for key field");
        assert!(
            anomalies.records.is_empty(),
            "null byte in key is not a type anomaly"
        );
    }

    #[test]
    fn a_table_without_sink_is_an_error_not_a_lost_row() {
        let schema = make_pivot_schema("attrs", "root");
        let mut sinks: HashMap<String, CaptureSink> = HashMap::new();
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"k": "v"}"#).unwrap();

        let err = super::insert_pivot_object(
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
        )
        .unwrap_err();

        assert!(err.to_string().contains("attrs"), "{err}");
        assert!(
            anomalies.totals.is_empty(),
            "la ligne n'est pas comptée (#167)"
        );
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

        super::insert_jsonb_object(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &value,
            dummy_parent_id(),
            None,
        )
        .unwrap();

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

        super::insert_jsonb_object(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &value,
            dummy_parent_id(),
            None,
        )
        .unwrap();

        assert_eq!(sinks["evts"].0.len(), 1, "one JSONB row for parent");
        assert_eq!(
            sinks["evts_tags"].0.len(),
            2,
            "two pivot rows for child tags"
        );
    }

    #[test]
    fn jsonb_empty_path_map_no_recursion() {
        let mut schema = make_jsonb_schema("evts", "root");
        schema.path = vec!["evts".to_string()];
        let mut sinks = make_sinks(&["evts"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map: HashMap<String, crate::schema::table_schema::TableSchema> = HashMap::new();
        let value = serde_json::json!({"nested": {"x": 1}});

        super::insert_jsonb_object(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &value,
            dummy_parent_id(),
            None,
        )
        .unwrap();

        assert_eq!(
            sinks["evts"].0.len(),
            1,
            "one row — no child dispatch without path_map"
        );
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

        super::insert_jsonb_object(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &value,
            dummy_parent_id(),
            Some(2),
        )
        .unwrap();

        let rows = &sinks["reviews"].0;
        assert_eq!(rows.len(), 1);
        let fields = parse_fields(&rows[0]);
        assert_eq!(
            fields.len(),
            4,
            "uuid, parent_uuid, order, json_data — une ligne par colonne déclarée"
        );
        assert_eq!(
            fields[2], "2",
            "j2s_order doit porter la position passée en paramètre"
        );
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

        super::insert_jsonb_object(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &value,
            dummy_parent_id(),
            None,
        )
        .unwrap();

        let fields = parse_fields(&sinks["reviews"].0[0]);
        assert_eq!(fields[2], r"\N", "pas d'ordre fourni → j2s_order NULL");
    }

    // ---------------------------------------------------------------------------
    // insert_structured_pivot_object tests
    // ---------------------------------------------------------------------------

    use crate::schema::table_schema::{SuffixColumn, SuffixSchema};

    fn make_structured_pivot_schema(
        name: &str,
        parent: &str,
        suffix_schema: &SuffixSchema,
    ) -> crate::schema::table_schema::TableSchema {
        use crate::schema::table_schema::InferredStrategy;
        let mut s = crate::schema::table_schema::TableSchema::new(
            name.to_string(),
            vec![name.to_string()],
            1,
        );
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
            suffix_cols: suffixes
                .iter()
                .map(|s| SuffixColumn {
                    suffix: s.to_string(),
                    col_name: format!("c{}", s.replace('_', "")),
                    pg_type: PgType::Text,
                })
                .collect(),
            value_type: PgType::Text,
        }
    }

    #[test]
    fn structured_pivot_key_goes_to_its_longest_suffix() {
        // `_100g` sorts before `_prepared_100g`: the first match would make `fat_prepared` a base.
        let ss = make_suffix_schema(&["_100g", "_prepared_100g"]);
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"fat_100g": 1, "fat_prepared_100g": 2}"#).unwrap();
        let groups = super::group_keys_by_base(&obj, &ss);
        let bases: Vec<&str> = groups.keys().map(String::as_str).collect();
        assert_eq!(bases, ["fat"]);
    }

    #[test]
    fn structured_pivot_two_keys_same_base_emit_one_row() {
        let ss = make_suffix_schema(&["_eur", "_usd"]);
        let schema = make_structured_pivot_schema("prices", "root", &ss);
        let mut sinks = make_sinks(&["prices"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"price_eur": "1.5", "price_usd": "2.0"}"#).unwrap();

        super::insert_structured_pivot_object(
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
            &ss,
        )
        .unwrap();

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

        super::insert_structured_pivot_object(
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
            &ss,
        )
        .unwrap();

        let rows = &sinks["prices"].0;
        assert_eq!(rows.len(), 1);
        let fields = parse_fields(&rows[0]);
        assert_eq!(
            fields[2], "standalone",
            "key with no suffix → 'name' column"
        );
        assert_eq!(
            fields[3], "val",
            "value goes to 'value' column (suffix \"\")"
        );
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

        super::insert_structured_pivot_object(
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
            &ss,
        )
        .unwrap();

        assert_eq!(sinks["prices"].0.len(), 2, "two distinct bases → two rows");
    }

    // ---------------------------------------------------------------------------
    // insert_sibling_collapse_object tests
    // ---------------------------------------------------------------------------

    use crate::schema::table_schema::{KeyShape, SiblingSchema};

    fn make_sibling_collapse_schema(
        name: &str,
        parent: &str,
        key_col: &str,
        data_cols: &[&str],
    ) -> (crate::schema::table_schema::TableSchema, SiblingSchema) {
        use crate::schema::table_schema::InferredStrategy;
        let ss = SiblingSchema {
            key_col_name: key_col.to_string(),
            key_shape: KeyShape::Mixed,
            array_children: false,
        };
        let mut s = crate::schema::table_schema::TableSchema::new(
            name.to_string(),
            vec![name.to_string()],
            1,
        );
        s.inferred_strategy = InferredStrategy::SiblingCollapse(ss.clone());
        let mut cols = vec![
            generated_id(),
            parent_fk(parent),
            col(key_col, PgType::Text),
        ];
        for dc in data_cols {
            cols.push(col(dc, PgType::Text));
        }
        s.columns = cols;
        (s, ss)
    }

    #[test]
    fn sibling_collapse_scalar_value_records_anomaly() {
        let (schema, ss) = make_sibling_collapse_schema("items", "root", "key", &["a"]);
        let mut sinks = make_sinks(&["items"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"fr": {"a": "1"}, "de": "inconnu", "it": null}"#).unwrap();

        super::insert_sibling_collapse_object(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
            &ss,
        )
        .unwrap();

        assert_eq!(sinks["items"].0.len(), 1, "seul l'objet a une ligne");
        assert_eq!(
            anomalies.records,
            vec![("items".to_string(), "de".to_string())],
            "le scalaire laisse une anomalie, null n'en laisse pas (#162)"
        );
    }

    #[test]
    fn normalize_dynamic_keys_scalar_value_records_anomaly() {
        let (schema, _) = make_sibling_collapse_schema("items", "root", "key", &["a"]);
        let mut sinks = make_sinks(&["items"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"fr": {"a": "1"}, "de": "inconnu", "it": null}"#).unwrap();

        super::insert_normalize_dynamic_keys(
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
            "key",
        )
        .unwrap();

        assert_eq!(sinks["items"].0.len(), 1, "seul l'objet a une ligne");
        assert_eq!(
            anomalies.records,
            vec![("items".to_string(), "de".to_string())],
            "le scalaire laisse une anomalie, null n'en laisse pas (#162)"
        );
    }

    #[test]
    fn sibling_collapse_nominal_one_object_emits_one_row() {
        let (schema, ss) = make_sibling_collapse_schema("items", "root", "key", &["a", "b"]);
        let mut sinks = make_sinks(&["items"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"mykey": {"a": "v1", "b": "v2"}}"#).unwrap();

        super::insert_sibling_collapse_object(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
            &ss,
        )
        .unwrap();

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

        super::insert_sibling_collapse_object(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
            &ss,
        )
        .unwrap();

        assert_eq!(sinks["items"].0.len(), 2, "two sibling keys → two rows");
    }

    // ---------------------------------------------------------------------------
    // insert_sibling_collapse_multi tests
    // ---------------------------------------------------------------------------

    use crate::schema::table_schema::SiblingGroup;

    fn make_routing_schema(
        name: &str,
        parent: &str,
        path: &[&str],
    ) -> crate::schema::table_schema::TableSchema {
        let mut s = crate::schema::table_schema::TableSchema::new(
            name.to_string(),
            path.iter().map(std::string::ToString::to_string).collect(),
            1,
        );
        s.columns = vec![generated_id(), parent_fk(parent)];
        s
    }

    fn make_group(
        path_segment: &str,
        key_is_numeric: bool,
        absorbed: &[&str],
        key_col: &str,
    ) -> SiblingGroup {
        SiblingGroup {
            pivot_table: format!("pivot_{path_segment}"),
            key_is_numeric,
            sibling_schema: SiblingSchema {
                key_col_name: key_col.to_string(),
                key_shape: KeyShape::Mixed,
                array_children: false,
            },
            absorbed_names: vec![],
            path_segment: path_segment.to_string(),
            absorbed_path_segments: absorbed
                .iter()
                .map(std::string::ToString::to_string)
                .collect(),
        }
    }

    /// A `SiblingCollapseMulti` object child under `dummy_parent_id()`, written as `insert_child_object` writes it.
    fn insert_multi(
        path_map: &HashMap<String, TableSchema>,
        sinks: &mut HashMap<String, CaptureSink>,
        anomalies: &mut CountingAnomaly,
        schema: &TableSchema,
        obj: &serde_json::Map<String, serde_json::Value>,
        groups: &[SiblingGroup],
    ) -> Result<()> {
        let mut schema = schema.clone();
        schema.inferred_strategy = InferredStrategy::SiblingCollapseMulti(groups.to_vec());
        let ctx = &mut super::InsertCtx { sinks, anomalies };
        let value = serde_json::Value::Object(obj.clone());
        super::insert_child_object(path_map, ctx, &schema, obj, &value, dummy_parent_id(), None)
    }

    #[test]
    fn multi_empty_groups_emits_only_routing_row() {
        let schema = make_routing_schema("multi", "root", &["multi"]);
        let mut sinks = make_sinks(&["multi"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"k": {"a": 1}}"#).unwrap();

        insert_multi(&path_map, &mut sinks, &mut anomalies, &schema, &obj, &[]).unwrap();

        assert_eq!(
            sinks["multi"].0.len(),
            1,
            "one routing row emitted even with no groups"
        );
    }

    #[test]
    fn multi_routes_by_key_is_numeric() {
        let schema = make_routing_schema("multi", "root", &["multi"]);
        let groups = vec![
            make_group("key", false, &[], "k"),
            make_group("num", true, &[], "k"),
        ];

        let (pivot_key_schema, pivot_key_ss) =
            make_sibling_collapse_schema("pivot_key", "multi", "k", &["val"]);
        let (pivot_num_schema, pivot_num_ss) =
            make_sibling_collapse_schema("pivot_num", "multi", "k", &["val"]);
        // Override sibling_schema from groups (not used directly — pivot schemas get their own)
        let _ = (pivot_key_ss, pivot_num_ss);

        let mut path_map = HashMap::new();
        path_map.insert("multi\x00key".to_string(), pivot_key_schema);
        path_map.insert("multi\x00num".to_string(), pivot_num_schema);

        let mut sinks = make_sinks(&["multi", "pivot_key", "pivot_num"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"foo": {"val": "x"}, "42": {"val": "y"}}"#).unwrap();

        insert_multi(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            &groups,
        )
        .unwrap();

        assert_eq!(sinks["multi"].0.len(), 1, "routing row");
        assert_eq!(
            sinks["pivot_key"].0.len(),
            1,
            "non-numeric key 'foo' → pivot_key"
        );
        assert_eq!(
            sinks["pivot_num"].0.len(),
            1,
            "numeric key '42' → pivot_num"
        );
    }

    #[test]
    fn multi_absorbed_path_segments_match_takes_priority_over_key_is_numeric() {
        let schema = make_routing_schema("multi", "root", &["multi"]);
        // Both groups non-numeric — disambiguation via absorbed_path_segments
        let groups = vec![
            make_group("grp0", false, &["foo"], "k"),
            make_group("grp1", false, &["bar"], "k"),
        ];

        let (pivot_grp0_schema, _) =
            make_sibling_collapse_schema("pivot_grp0", "multi", "k", &["val"]);
        let (pivot_grp1_schema, _) =
            make_sibling_collapse_schema("pivot_grp1", "multi", "k", &["val"]);

        let mut path_map = HashMap::new();
        path_map.insert("multi\x00grp0".to_string(), pivot_grp0_schema);
        path_map.insert("multi\x00grp1".to_string(), pivot_grp1_schema);

        let mut sinks = make_sinks(&["multi", "pivot_grp0", "pivot_grp1"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"foo": {"val": "f"}, "bar": {"val": "b"}}"#).unwrap();

        insert_multi(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            &groups,
        )
        .unwrap();

        assert_eq!(
            sinks["pivot_grp0"].0.len(),
            1,
            "'foo' → grp0 via absorbed_path_segments"
        );
        assert_eq!(
            sinks["pivot_grp1"].0.len(),
            1,
            "'bar' → grp1 via absorbed_path_segments"
        );
    }

    // ---------------------------------------------------------------------------
    // find_group_for_key tests
    // ---------------------------------------------------------------------------

    fn group(key_is_numeric: bool, absorbed: &[&str]) -> SiblingGroup {
        make_group(
            if key_is_numeric { "num" } else { "key" },
            key_is_numeric,
            absorbed,
            "k",
        )
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
        assert_eq!(
            super::find_group_for_key(&groups, "123"),
            Some(1),
            "all-digit key → numeric group"
        );
    }

    #[test]
    fn find_group_fallback_non_numeric_key() {
        let groups = vec![group(false, &[]), group(true, &[])];
        assert_eq!(
            super::find_group_for_key(&groups, "abc"),
            Some(0),
            "non-digit key → non-numeric group"
        );
    }

    #[test]
    fn find_group_key_absent_returns_none() {
        let groups = vec![group(true, &["42"])]; // only numeric group
        assert_eq!(
            super::find_group_for_key(&groups, "foo"),
            None,
            "no matching group → None"
        );
    }

    #[test]
    fn find_group_ambiguous_two_non_numeric_no_segments_returns_first() {
        let groups = vec![
            make_group("grp0", false, &[], "k"),
            make_group("grp1", false, &[], "k"),
        ];
        assert_eq!(
            super::find_group_for_key(&groups, "foo"),
            Some(0),
            "tie-break → first group wins"
        );
    }

    #[test]
    fn find_group_absorbed_segments_take_priority_over_key_is_numeric() {
        // Key "123" is all-digits, but group 0 has it in absorbed_path_segments → should win over group 1 (numeric)
        let groups = vec![group(false, &["123"]), group(true, &[])];
        assert_eq!(
            super::find_group_for_key(&groups, "123"),
            Some(0),
            "exact match wins over key_is_numeric heuristic"
        );
    }

    // ---------------------------------------------------------------------------
    // route_multi_collapse_children tests
    // ---------------------------------------------------------------------------

    #[test]
    #[allow(clippy::similar_names)] // pivot_a_schema/pivot_b_schema deliberately parallel, testing symmetric groups
    fn route_multi_collapse_children_routes_both_groups_without_routing_row() {
        // Verifies that route_multi_collapse_children uses the supplied routing_id
        // (no emit_routing_row) and correctly distributes keys to each pivot group.
        let schema = make_routing_schema("root", "root", &["root"]);
        let groups = vec![
            make_group("a_key", false, &["a1", "a2"], "key"),
            make_group("b_key", false, &["b1", "b2"], "key"),
        ];

        let (pivot_a_schema, _) =
            make_sibling_collapse_schema("pivot_a_key", "root", "key", &["color"]);
        let (pivot_b_schema, _) =
            make_sibling_collapse_schema("pivot_b_key", "root", "key", &["size"]);

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

        super::route_multi_collapse_children(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            routing_id,
            &groups,
        )
        .unwrap();

        assert_eq!(sinks["pivot_a_key"].0.len(), 2, "a1 + a2 → pivot_a_key");
        assert_eq!(sinks["pivot_b_key"].0.len(), 2, "b1 + b2 → pivot_b_key");
        // root sink is absent → no routing row emitted by route_multi_collapse_children
    }

    #[test]
    fn a_missing_pivot_table_records_an_anomaly_unless_skipped() {
        let mut schema = make_routing_schema("root", "root", &["root"]);
        let groups = vec![
            make_group("a_key", false, &["a1"], "key"),
            make_group("b_key", false, &["b1"], "key"),
        ];
        let (pivot_a_schema, _) =
            make_sibling_collapse_schema("pivot_a_key", "root", "key", &["color"]);
        let sep = crate::schema::PATH_SEP.to_string();
        let path_map = HashMap::from([(format!("root{sep}a_key"), pivot_a_schema)]);
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"a1": {"color": "red"}, "b1": {"size": "10"}}"#).unwrap();

        let route = |schema: &TableSchema| {
            let mut sinks = make_sinks(&["pivot_a_key"]);
            let mut anomalies = CountingAnomaly::default();
            super::route_multi_collapse_children(
                &path_map,
                &mut sinks,
                &mut anomalies,
                schema,
                &obj,
                dummy_parent_id(),
                &groups,
            )
            .unwrap();
            assert_eq!(sinks["pivot_a_key"].0.len(), 1);
            anomalies.records
        };

        assert_eq!(
            route(&schema),
            [("root".to_string(), "b_key".to_string())],
            "pivot b_key absent : perte (#167)"
        );
        schema.skipped_children.push("b_key".to_string());
        assert!(route(&schema).is_empty(), "pivot Skip-é : abandon voulu");
    }

    #[test]
    fn record_missing_table_stays_quiet_on_intentional_drops() {
        let mut parent = make_routing_schema("t", "root", &["t"]);
        parent.columns.push(ColumnSchema {
            name: "tags".to_string(),
            original_name: "tags".to_string(),
            pg_type: PgType::Array(Box::new(PgType::Text)),
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        parent
            .flatten_sources
            .insert("n_k".to_string(), "n".to_string());
        parent.skipped_children.push("gone".to_string());
        let record = |parent: &TableSchema, field: &str, json: &str| {
            let mut anomalies = CountingAnomaly::default();
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            super::record_missing_table(
                &HashMap::new(),
                &mut anomalies,
                parent,
                field,
                dummy_parent_id(),
                &value,
            )
            .unwrap();
            anomalies.records.len()
        };

        assert_eq!(
            record(&parent, "lost", r#"{"k": 1}"#),
            1,
            "objet sans table"
        );
        assert_eq!(
            record(&parent, "lost", r#"[{"k": 1}]"#),
            1,
            "tableau sans table"
        );
        assert_eq!(record(&parent, "lost", "{}"), 0, "objet vide");
        assert_eq!(record(&parent, "lost", "[]"), 0, "tableau vide");
        assert_eq!(record(&parent, "tags", r#"["x"]"#), 0, "colonne tableau");
        assert_eq!(record(&parent, "n", r#"{"k": 1}"#), 0, "enfant aplati");
        assert_eq!(record(&parent, "gone", r#"{"k": 1}"#), 0, "enfant Skip-é");
        parent.inferred_strategy = InferredStrategy::Jsonb;
        assert_eq!(record(&parent, "lost", r#"{"k": 1}"#), 0, "blob Jsonb");
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

        super::insert_array(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &arr,
            dummy_parent_id(),
            false,
        )
        .unwrap();

        let rows = &sinks["reviews"].0;
        assert_eq!(
            rows.len(),
            2,
            "un élément d'array → une ligne JSONB, pas un no-op silencieux"
        );
        for (i, row) in rows.iter().enumerate() {
            let fields = parse_fields(row);
            assert_eq!(fields.len(), 4, "uuid, parent_uuid, order, data");
            assert_ne!(
                fields[3], r"\N",
                "le blob JSON ne doit pas être NULL (bug pré-#45)"
            );
            assert_eq!(
                fields[2],
                i.to_string(),
                "j2s_order doit suivre la position dans l'array"
            );
        }
        let first: serde_json::Value =
            serde_json::from_str(parse_fields(&rows[0]).remove(3)).unwrap();
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
        schema.columns = vec![
            generated_id(),
            parent_fk("root"),
            generated_order(),
            col("a", PgType::Text),
        ];
        let mut sinks = make_sinks(&["items"]);
        let mut anomalies = CountingAnomaly::default();
        let path_map = HashMap::new();
        let arr = vec![serde_json::json!({"a": "x"})];

        super::insert_array(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &schema,
            &arr,
            dummy_parent_id(),
            false,
        )
        .unwrap();

        let fields = parse_fields(&sinks["items"].0[0]);
        assert_eq!(fields.len(), 4);
        assert_eq!(
            fields[3], "x",
            "colonne 'a' peuplée depuis la clé JSON réelle — chemin Columns inchangé"
        );
    }

    // ---------------------------------------------------------------------------
    // Tableau mêlant objets et scalaires (#86)
    // ---------------------------------------------------------------------------

    fn make_columns_object_array_schema() -> TableSchema {
        let mut schema = TableSchema::new(
            "root_items".to_string(),
            vec!["root".to_string(), "items".to_string()],
            1,
        );
        schema.child_kind = Some(ChildKind::ObjectArray);
        schema.columns = vec![
            generated_id(),
            parent_fk("root"),
            generated_order(),
            col("a", PgType::Integer),
        ];
        schema
    }

    fn mixed_array() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({"a": 1}),
            serde_json::json!("x"),
            serde_json::Value::Null,
            serde_json::json!(2),
        ]
    }

    #[test]
    fn insert_array_mixed_scalars_record_one_anomaly() {
        let schema = make_columns_object_array_schema();
        let mut sinks = make_sinks(&["root_items"]);
        let mut anomalies = CountingAnomaly::default();
        let arr = mixed_array();

        super::insert_array(
            &HashMap::new(),
            &mut sinks,
            &mut anomalies,
            &schema,
            &arr,
            dummy_parent_id(),
            false,
        )
        .unwrap();

        assert_eq!(sinks["root_items"].0.len(), 1, "seul l'objet a une ligne");
        assert_eq!(
            anomalies.records,
            vec![("root_items".to_string(), "items".to_string())],
            "une anomalie par tableau, pas une perte silencieuse (#86)"
        );
    }

    #[test]
    fn insert_array_mixed_scalars_kept_by_parent_array_column_no_anomaly() {
        // Le champ vaut [1,2] dans une autre ligne : le parent a une colonne tableau qui garde
        // déjà les scalaires, l'enfant ne doit pas les signaler.
        let schema = make_columns_object_array_schema();
        let mut sinks = make_sinks(&["root_items"]);
        let mut anomalies = CountingAnomaly::default();
        let arr = mixed_array();

        super::insert_array(
            &HashMap::new(),
            &mut sinks,
            &mut anomalies,
            &schema,
            &arr,
            dummy_parent_id(),
            true,
        )
        .unwrap();

        assert_eq!(sinks["root_items"].0.len(), 1);
        assert_eq!(anomalies.records, [] as [(String, String); 0]);
    }

    #[test]
    fn keeps_scalars_in_array_column_reads_parent_column_type() {
        let mut parent = TableSchema::new("root".to_string(), vec!["root".to_string()], 0);
        parent.columns = vec![
            col("items", PgType::Array(Box::new(PgType::Integer))),
            col("name", PgType::Text),
        ];
        assert!(super::keeps_scalars_in_array_column(&parent, "items"));
        assert!(!super::keeps_scalars_in_array_column(&parent, "name"));
        assert!(!super::keeps_scalars_in_array_column(&parent, "absent"));
    }

    #[test]
    fn insert_array_jsonb_keeps_scalars_as_blobs() {
        let schema = make_jsonb_object_array_schema("reviews", "produits");
        let mut sinks = make_sinks(&["reviews"]);
        let mut anomalies = CountingAnomaly::default();
        let arr = mixed_array();

        super::insert_array(
            &HashMap::new(),
            &mut sinks,
            &mut anomalies,
            &schema,
            &arr,
            dummy_parent_id(),
            false,
        )
        .unwrap();

        let rows = &sinks["reviews"].0;
        assert_eq!(rows.len(), 3, "objet, \"x\" et 2 ; le null est sauté");
        let blobs: Vec<&str> = rows.iter().map(|r| parse_fields(r)[3]).collect();
        assert_eq!(blobs[1], r#""x""#);
        assert_eq!(blobs[2], "2");
        let orders: Vec<&str> = rows.iter().map(|r| parse_fields(r)[2]).collect();
        assert_eq!(
            orders,
            ["0", "1", "3"],
            "j2s_order garde la position d'origine"
        );
        assert_eq!(anomalies.records, [] as [(String, String); 0]);
    }

    #[test]
    fn sibling_collapse_array_mixed_scalars_record_one_anomaly() {
        let (schema, mut ss) = make_sibling_collapse_schema("items", "root", "key", &["a"]);
        ss.array_children = true;
        let mut sinks = make_sinks(&["items"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"fr": [{"a": "x"}, "y", 3], "en": [{"a": "z"}]}"#).unwrap();

        super::insert_sibling_collapse_object(
            &HashMap::new(),
            &mut sinks,
            &mut anomalies,
            &schema,
            &obj,
            dummy_parent_id(),
            None,
            &ss,
        )
        .unwrap();

        assert_eq!(sinks["items"].0.len(), 2);
        assert_eq!(
            anomalies.records,
            vec![("items".to_string(), "fr".to_string())]
        );
    }

    // ---------------------------------------------------------------------------
    // Une table absorbante atteinte par un tableau ou un routage (#172)
    // ---------------------------------------------------------------------------

    #[test]
    fn an_array_element_in_sibling_collapse_gets_one_row_per_key() {
        let (mut schema, _) = make_sibling_collapse_schema("items", "root", "key", &["a"]);
        schema.child_kind = Some(ChildKind::ObjectArray);
        schema.columns.insert(2, generated_order());
        let mut sinks = make_sinks(&["items"]);
        let mut anomalies = CountingAnomaly::default();
        let arr = vec![
            serde_json::json!({"fr": {"a": "1"}, "de": {"a": "2"}}),
            serde_json::json!({"en": {"a": "3"}}),
        ];

        super::insert_array(
            &HashMap::new(),
            &mut sinks,
            &mut anomalies,
            &schema,
            &arr,
            dummy_parent_id(),
            false,
        )
        .unwrap();

        let rows: Vec<Vec<&str>> = sinks["items"].0.iter().map(|r| parse_fields(r)).collect();
        let got: Vec<(&str, &str, &str)> = rows.iter().map(|f| (f[2], f[3], f[4])).collect();
        assert_eq!(
            got,
            vec![("0", "de", "2"), ("0", "fr", "1"), ("1", "en", "3")]
        );
        assert!(rows.iter().all(|f| f[1] == dummy_parent_id().to_string()));
        assert_eq!(anomalies.records, Vec::<(String, String)>::new());
    }

    #[test]
    fn an_array_element_in_pivot_fills_every_column() {
        let mut schema = make_pivot_schema("attrs", "root");
        schema.child_kind = Some(ChildKind::ObjectArray);
        schema.columns.insert(2, generated_order());
        let mut sinks = make_sinks(&["attrs"]);
        let mut anomalies = CountingAnomaly::default();
        let arr = vec![serde_json::json!({"k": "v"})];

        super::insert_array(
            &HashMap::new(),
            &mut sinks,
            &mut anomalies,
            &schema,
            &arr,
            dummy_parent_id(),
            false,
        )
        .unwrap();

        let fields = parse_fields(&sinks["attrs"].0[0]);
        assert_eq!(fields.len(), 5, "uuid, parent, order, key, value");
        assert_eq!(&fields[2..], ["0", "k", "v"]);
    }

    #[test]
    fn a_routed_table_in_normalize_dynamic_keys_gets_one_row_per_key() {
        let (mut target, _) = make_sibling_collapse_schema("images", "root", "code", &["a"]);
        target.inferred_strategy = InferredStrategy::NormalizeDynamicKeys {
            id_column: "code".into(),
        };
        target.path = vec!["root".to_string(), "sub".to_string()];
        let mut parent = TableSchema::new("root".to_string(), vec!["root".to_string()], 0);
        parent
            .child_routes
            .insert("sub".to_string(), "images".to_string());
        let path_map = HashMap::from([("images".to_string(), target)]);
        let mut sinks = make_sinks(&["images"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"sub": {"x": {"a": "1"}, "y": {"a": "2"}}}"#).unwrap();

        super::dispatch_child_routes(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &parent,
            &obj,
            dummy_parent_id(),
        )
        .unwrap();

        let got: Vec<(String, String)> = sinks["images"]
            .0
            .iter()
            .map(|r| {
                let f = parse_fields(r);
                (f[2].to_string(), f[3].to_string())
            })
            .collect();
        assert_eq!(
            got,
            vec![("x".into(), "1".into()), ("y".into(), "2".into())]
        );
    }

    /// A route the keyed post-pass rewrote (#173): the routed key is the key of the row, a
    /// scalar under it is reported, not lost.
    #[test]
    fn a_keyed_route_writes_one_row_keyed_by_the_routed_key() {
        let (mut target, _) = make_sibling_collapse_schema("images", "root", "code", &["a"]);
        target.inferred_strategy = InferredStrategy::NormalizeDynamicKeys {
            id_column: "code".into(),
        };
        target.path = vec!["root".to_string(), "key".to_string()];
        let mut parent = TableSchema::new("root".to_string(), vec!["root".to_string()], 0);
        for key in ["x", "y", "z"] {
            parent
                .child_routes
                .insert(key.to_string(), "images".to_string());
        }
        let path_map = HashMap::from([("images".to_string(), target)]);
        let mut sinks = make_sinks(&["images"]);
        let mut anomalies = CountingAnomaly::default();
        let obj: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(r#"{"x": {"a": "1"}, "y": {"a": "2"}, "z": "3"}"#).unwrap();

        super::dispatch_child_routes(
            &path_map,
            &mut sinks,
            &mut anomalies,
            &parent,
            &obj,
            dummy_parent_id(),
        )
        .unwrap();

        let mut got: Vec<(String, String)> = sinks["images"]
            .0
            .iter()
            .map(|r| {
                let f = parse_fields(r);
                (f[2].to_string(), f[3].to_string())
            })
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![("x".into(), "1".into()), ("y".into(), "2".into())]
        );
        assert_eq!(
            anomalies.records,
            vec![("images".to_string(), "z".to_string())]
        );
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
