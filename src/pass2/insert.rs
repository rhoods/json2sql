//! Écriture de lignes complètes en base — logique d'insertion par type de table.
//!
//! Ce module sait *quoi* écrire : une ligne root, une ligne wide (pivot, autosplit…),
//! un objet enfant. Il sélectionne la stratégie via `insert_child_object` qui aiguille
//! (*dispatch*) selon le `InferredStrategy` de la table cible.
//!
//! Frontière avec `traversal.rs` : `insert.rs` gère l'écriture d'une ligne complète
//! dans un contexte `InsertCtx`. `traversal.rs` gère la navigation dans les sous-arbres
//! JSON (pivot, keyed pivot, routing entre tables).
//!
//! Fonctions :
//! - struct `InsertCtx` — contexte mutable du passage d'insertion récursif (sinks + anomalies).
//! - fn `insert_object` — point d'entrée : écrit la ligne (colonnes + générées), gère le cas racine
//!   JSONB, délègue `AutoSplit` et récurse dans les enfants.
//! - fn `write_root_jsonb` — cas spécial racine en stratégie Jsonb : écrit le blob JSON puis recurse.
//! - fn `insert_child_object` — dispatch un objet enfant vers la fonction d'insertion selon son
//!   `InferredStrategy` (Pivot, Jsonb, `StructuredPivot`, `SiblingCollapse`(Multi), `NormalizeDynamicKeys`,
//!   ou insertion standard récursive).
//! - fn `recurse_children` — parcourt les champs de l'objet, route chaque enfant (objet/array) vers
//!   `insert_child_object`/`insert_array`.
//! - fn `write_row_of` — écrit la ligne d'une table pour un objet : colonnes générées puis colonnes de données.
//! - fn `write_group_rows` — écrit les lignes des tables de groupes de colonnes d'une table (une par groupe qui a au moins une valeur).
//! - fn `column_value` — valeur JSON d'une colonne de données dans l'objet de sa table (via le champ d'origine si elle vient d'un `Flatten`).
//! - fn `write_wide_row` — écrit une ligne EAV (clé, valeur) de la table wide compagnon.
//! - fn `write_autosplit_rows` — écrit les paires clé/valeur de fréquence moyenne en lignes EAV
//!   dans la table wide compagnon (stratégie `AutoSplit`).
//! - fn `push_data_col` — résout et coerce la valeur d'une colonne de données (flatten, JSONB, coercition).
//! - fn `push_generated_col_insert` — écrit une colonne générée (`j2s_id`, `j2s_parent_id`, `j2s_order`).
//! - fn `push_jsonb_col` — sérialise une valeur JSON brute en colonne JSONB.

use std::collections::HashMap;

use serde_json::Value;
use uuid::Uuid;

use crate::anomaly::collector::AnomalyCollect;
use crate::db::copy_sink::RowBuilder;
use crate::db::copy_text::{escape_copy_text, CopyEscaped};
use crate::error::Result;
use crate::pass2::sink::RowSink;
use crate::schema::grouping::sample::is_present;
use crate::schema::table_schema::{InferredStrategy, TableSchema};
use crate::schema::PATH_SEP;

use super::traversal::{
    insert_array, insert_jsonb_object, insert_normalize_dynamic_keys, insert_pivot_object,
    insert_sibling_collapse_multi, insert_sibling_collapse_object, insert_structured_pivot_object,
    push_coerced, push_coerced_as, push_text, route_multi_collapse_children, write_row,
};

/// Mutable context for the recursive insert pass.
/// Separates `path_map` (immutable, borrowed independently during child lookups)
/// from the mutable handles so field-level borrows remain valid across recursive calls.
pub struct InsertCtx<'a, S, A> {
    pub(crate) sinks: &'a mut HashMap<String, S>,
    pub(crate) anomalies: &'a mut A,
}

/// The JSON value of a data column in the object of its table: for a column inlined via the Flatten strategy, the
/// value is looked up in the nested object it came from.
fn column_value<'a>(
    schema: &TableSchema,
    col: &crate::schema::table_schema::ColumnSchema,
    obj: &'a serde_json::Map<String, Value>,
) -> &'a Value {
    schema.flatten_sources.get(col.name.as_str()).map_or_else(
        || obj.get(&col.original_name).unwrap_or(&Value::Null),
        |source_field| {
            obj.get(source_field.as_str())
                .and_then(|v| v.as_object())
                .and_then(|nested| nested.get(col.original_name.as_str()))
                .unwrap_or(&Value::Null)
        },
    )
}

fn push_data_col<A: AnomalyCollect>(
    builder: &mut RowBuilder,
    anomalies: &mut A,
    schema: &TableSchema,
    col: &crate::schema::table_schema::ColumnSchema,
    obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
) -> Result<()> {
    let json_val = column_value(schema, col, obj);

    // JSONB columns accept any JSON value — serialize the raw value directly.
    if matches!(col.pg_type, crate::schema::type_tracker::PgType::Jsonb) {
        push_jsonb_col(builder, json_val);
        return Ok(());
    }

    // Objects and non-array-typed arrays become child tables, not columns.
    if matches!(json_val, Value::Object(_))
        || (matches!(json_val, Value::Array(_))
            && !matches!(col.pg_type, crate::schema::type_tracker::PgType::Array(_)))
    {
        builder.push_null();
        return Ok(());
    }

    push_coerced(builder, anomalies, &schema.name, col, row_id, json_val)
}

pub fn insert_object<S: RowSink, A: AnomalyCollect>(
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
    if matches!(*schema.effective_strategy(), InferredStrategy::Jsonb) && parent_id.is_none() {
        return write_root_jsonb(path_map, ctx, schema, obj, row_id);
    }

    write_row_of(ctx, schema, obj, row_id, parent_id, order)?;

    // For SiblingCollapseMulti, the row above IS the routing row.
    // Delegate child routing using row_id as routing_id (no second emit_routing_row).
    if let InferredStrategy::SiblingCollapseMulti(groups) = &*schema.effective_strategy() {
        let groups = groups.clone();
        return route_multi_collapse_children(
            path_map,
            ctx.sinks,
            ctx.anomalies,
            schema,
            obj,
            row_id,
            &groups,
        );
    }

    write_group_rows(path_map, ctx, schema, obj, row_id)?;
    write_autosplit_rows(path_map, ctx, schema, obj, row_id)?;
    recurse_children(path_map, ctx, schema, obj, row_id)?;
    Ok(())
}

/// Write the row of `schema` for `obj`: its generated columns, then its data columns. It counts as a row written.
fn write_row_of<S: RowSink, A: AnomalyCollect>(
    ctx: &mut InsertCtx<'_, S, A>,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
    parent_id: Option<Uuid>,
    order: Option<i64>,
) -> Result<()> {
    let mut builder = RowBuilder::new();
    for col in &schema.columns {
        if col.is_generated {
            push_generated_col_insert(&mut builder, col, row_id, parent_id, order);
            continue;
        }
        push_data_col(&mut builder, ctx.anomalies, schema, col, obj, row_id)?;
    }
    write_row(ctx.sinks, ctx.anomalies, &schema.name, builder)
}

/// Write the rows of the column groups split off `schema` (#78): one row per group table that has at least one
/// value in `obj`, none when all its values are empty (null, `""`, `[]`, `{}`: the rule the groups were proposed
/// with). The row takes the id of the row just written for both its `j2s_id` and its foreign key, which is what makes
/// the table 1:1. A group table the path map does not have is skipped.
fn write_group_rows<S: RowSink, A: AnomalyCollect>(
    path_map: &HashMap<String, TableSchema>,
    ctx: &mut InsertCtx<'_, S, A>,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
) -> Result<()> {
    for name in &schema.column_groups {
        let Some(group) = path_map.get(name.as_str()) else {
            continue;
        };
        if group
            .data_columns()
            .any(|col| is_present(column_value(group, col, obj)))
        {
            write_row_of(ctx, group, obj, row_id, Some(row_id), None)?;
        }
    }
    Ok(())
}

pub(super) fn push_generated_col_insert(
    builder: &mut RowBuilder,
    col: &crate::schema::table_schema::ColumnSchema,
    row_id: Uuid,
    parent_id: Option<Uuid>,
    order: Option<i64>,
) {
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
}

pub(super) fn push_jsonb_col(builder: &mut RowBuilder, json_val: &Value) {
    if matches!(json_val, Value::Null) {
        builder.push_null();
        return;
    }
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
    let mut builder = RowBuilder::new();
    builder.push_uuid(row_id); // j2s_id (no j2s_parent_id for root)
                               // A null byte in the JSON makes the blob NULL.
    push_text(
        &mut builder,
        &serde_json::to_string(obj).unwrap_or_default(),
    );
    write_row(ctx.sinks, ctx.anomalies, &schema.name, builder)?;
    recurse_children(path_map, ctx, schema, obj, row_id)
}

/// Dispatch a child Object value to the appropriate insertion function based on its `InferredStrategy`.
#[allow(clippy::too_many_lines)] // exhaustive dispatch over all InferredStrategy variants
fn insert_child_object<S: RowSink, A: AnomalyCollect>(
    path_map: &HashMap<String, TableSchema>,
    ctx: &mut InsertCtx<'_, S, A>,
    child_schema: &TableSchema,
    nested: &serde_json::Map<String, Value>,
    value: &Value,
    parent_id: Uuid,
) -> Result<()> {
    let eff = child_schema.effective_strategy();
    match &*eff {
        InferredStrategy::Pivot => {
            insert_pivot_object(ctx.sinks, ctx.anomalies, child_schema, nested, parent_id)?;
        }
        InferredStrategy::Jsonb => {
            // Object-kind child: no positional order (only ObjectArray elements carry one,
            // cf. insert_array in traversal.rs).
            insert_jsonb_object(
                path_map,
                ctx.sinks,
                ctx.anomalies,
                child_schema,
                value,
                parent_id,
                None,
            )?;
        }
        InferredStrategy::StructuredPivot(suffix_schema) => {
            insert_structured_pivot_object(
                ctx.sinks,
                ctx.anomalies,
                child_schema,
                nested,
                parent_id,
                suffix_schema,
            )?;
        }
        InferredStrategy::SiblingCollapse(sibling_schema) => {
            insert_sibling_collapse_object(
                path_map,
                ctx.sinks,
                ctx.anomalies,
                child_schema,
                nested,
                parent_id,
                sibling_schema,
            )?;
        }
        InferredStrategy::SiblingCollapseMulti(groups) => {
            insert_sibling_collapse_multi(
                path_map,
                ctx.sinks,
                ctx.anomalies,
                child_schema,
                nested,
                parent_id,
                groups,
            )?;
        }
        InferredStrategy::NormalizeDynamicKeys { id_column } => {
            insert_normalize_dynamic_keys(
                ctx.sinks,
                ctx.anomalies,
                child_schema,
                nested,
                parent_id,
                id_column,
            )?;
        }
        InferredStrategy::Columns
        | InferredStrategy::AutoSplit { .. }
        | InferredStrategy::Ignore
        | InferredStrategy::Flatten { .. }
        | InferredStrategy::JsonbFlatten => {
            let child_id = Uuid::now_v7();
            insert_object(
                path_map,
                ctx,
                child_schema,
                nested,
                child_id,
                Some(parent_id),
                None,
            )?;
        }
    }
    Ok(())
}

/// Write one EAV row (`j2s_id`, parent, key, value) of the `AutoSplit` companion wide table
/// under `row_id`. Without a known value type the value is `NULL`.
fn write_wide_row<S: RowSink, A: AnomalyCollect>(
    ctx: &mut InsertCtx<'_, S, A>,
    wide_table_name: &str,
    value_type: Option<&crate::schema::type_tracker::PgType>,
    row_id: Uuid,
    key: &str,
    value: &Value,
) -> Result<()> {
    let wide_id = Uuid::now_v7();
    let mut wb = RowBuilder::new();
    wb.push_uuid(wide_id);
    wb.push_uuid(row_id);
    push_text(&mut wb, key);
    match value_type {
        Some(pg_type) => {
            push_coerced_as(
                &mut wb,
                ctx.anomalies,
                wide_table_name,
                "value",
                pg_type,
                wide_id,
                value,
            )?;
        }
        None => wb.push_null(),
    }
    write_row(ctx.sinks, ctx.anomalies, wide_table_name, wb)
}

/// Write medium-frequency key-value pairs as EAV rows in the `AutoSplit` companion wide table.
/// Stable keys were already written as schema columns; rare keys are skipped entirely.
fn write_autosplit_rows<S: RowSink, A: AnomalyCollect>(
    path_map: &HashMap<String, TableSchema>,
    ctx: &mut InsertCtx<'_, S, A>,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
) -> Result<()> {
    let eff = schema.effective_strategy();
    let InferredStrategy::AutoSplit {
        medium_keys,
        wide_table_name,
        ..
    } = &*eff
    else {
        return Ok(());
    };
    let wide_value_type = path_map
        .get(wide_table_name.as_str())
        .and_then(|ws| ws.find_by_original("value"))
        .map(|c| c.pg_type.clone());
    for (field, value) in obj {
        if !medium_keys.contains(field.as_str())
            || matches!(value, Value::Object(_) | Value::Array(_))
        {
            continue;
        }
        write_wide_row(
            ctx,
            wide_table_name,
            wide_value_type.as_ref(),
            row_id,
            field,
            value,
        )?;
    }
    Ok(())
}

/// Recurse into child fields of `obj`, routing each to the appropriate insertion function.
pub(super) fn recurse_children<S: RowSink, A: AnomalyCollect>(
    path_map: &HashMap<String, TableSchema>,
    ctx: &mut InsertCtx<'_, S, A>,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
) -> Result<()> {
    let parent_path_key = schema.path.join(&PATH_SEP.to_string());
    for (field, value) in obj {
        let child_key = format!("{parent_path_key}{PATH_SEP}{field}");
        match value {
            Value::Object(nested) => {
                if let Some(child_schema) = path_map.get(&child_key) {
                    insert_child_object(path_map, ctx, child_schema, nested, value, row_id)?;
                }
            }
            Value::Array(arr) => {
                if let Some(child_schema) = path_map.get(&child_key) {
                    insert_array(
                        path_map,
                        ctx.sinks,
                        ctx.anomalies,
                        child_schema,
                        arr,
                        row_id,
                    )?;
                }
            }
            _ => {} // scalar — already handled as a column above
        }
    }
    Ok(())
}

#[cfg(test)]
#[cfg_attr(test, allow(clippy::disallowed_methods, clippy::too_many_lines))]
mod tests {
    use super::{insert_object, InsertCtx};
    use crate::anomaly::collector::AnomalyCollect;
    use crate::anomaly::collector::AnomalyCollector;
    use crate::error::Result;
    use crate::pass2::test_utils::CaptureSink;
    use crate::schema::table_schema::ChildKind;
    use crate::schema::table_schema::{ColumnSchema, InferredStrategy, TableSchema};
    use crate::schema::type_tracker::PgType;
    use serde_json::Value;
    use std::collections::HashMap;
    use uuid::Uuid;

    // --- column groups (#78, T3b) ---

    /// Records the anomalies and the rows counted per table.
    #[derive(Default)]
    struct Recorder {
        anomalies: Vec<(String, String, String)>,
        totals: HashMap<String, u64>,
    }

    impl AnomalyCollect for Recorder {
        fn record(
            &mut self,
            table: &str,
            column: &str,
            row_id: &str,
            _: &str,
            _: &str,
            _: &str,
        ) -> Result<()> {
            self.anomalies
                .push((table.to_string(), column.to_string(), row_id.to_string()));
            Ok(())
        }

        fn inc_total(&mut self, table: &str) {
            *self.totals.entry(table.to_string()).or_default() += 1;
        }
    }

    fn column(name: &str, original: &str, pg_type: PgType) -> ColumnSchema {
        ColumnSchema {
            name: name.to_string(),
            original_name: original.to_string(),
            pg_type,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        }
    }

    /// `t` (`a` TEXT, `y` INTEGER) split in the group `t_g` (`x1` TEXT, `x2` TEXT), in a path map.
    fn split_table() -> HashMap<String, TableSchema> {
        let mut parent = TableSchema::new("t".to_string(), vec!["t".to_string()], 0);
        parent
            .columns
            .push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        parent.columns.push(column("a", "a", PgType::Text));
        parent.columns.push(column("y", "y", PgType::Integer));
        parent.column_groups.push("t_g".to_string());
        let mut group = TableSchema::new("t_g".to_string(), vec!["t_g".to_string()], 1);
        group.parent_table = Some("t".to_string());
        group.child_kind = Some(ChildKind::Object);
        group
            .columns
            .push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        group.columns.push(ColumnSchema::parent_fk("t"));
        group.columns.push(column("x1", "x1", PgType::Text));
        group.columns.push(column("x2", "x2", PgType::Text));
        HashMap::from([("t".to_string(), parent), ("t_g".to_string(), group)])
    }

    /// Writes `json` as an object of `t` and returns the rows of each table, as cells.
    fn write(
        path_map: &HashMap<String, TableSchema>,
        json: &str,
        recorder: &mut Recorder,
    ) -> HashMap<String, Vec<Vec<String>>> {
        let mut sinks: HashMap<String, CaptureSink> = path_map
            .keys()
            .map(|k| (k.clone(), CaptureSink(Vec::new())))
            .collect();
        let obj: serde_json::Map<String, Value> = serde_json::from_str(json).unwrap();
        let schema = &path_map["t"];
        insert_object(
            path_map,
            &mut InsertCtx {
                sinks: &mut sinks,
                anomalies: recorder,
            },
            schema,
            &obj,
            ID,
            None,
            None,
        )
        .unwrap();
        sinks
            .into_iter()
            .map(|(table, sink)| {
                let rows = sink
                    .0
                    .iter()
                    .map(|r| {
                        String::from_utf8_lossy(r)
                            .trim_end_matches('\n')
                            .split('\t')
                            .map(str::to_string)
                            .collect()
                    })
                    .collect();
                (table, rows)
            })
            .collect()
    }

    const ID: Uuid = Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);

    #[test]
    fn a_column_group_gets_one_row_that_shares_the_id_of_the_row_it_was_split_from() {
        let mut recorder = Recorder::default();
        let rows = write(
            &split_table(),
            r#"{"a": "A", "y": 2, "x1": "u", "x2": "v"}"#,
            &mut recorder,
        );
        let id = ID.to_string();
        assert_eq!(rows["t"], [[id.clone(), "A".to_string(), "2".to_string()]]);
        assert_eq!(
            rows["t_g"],
            [[id.clone(), id, "u".to_string(), "v".to_string()]],
            "j2s_id and the foreign key are the parent's id"
        );
        assert_eq!(
            (recorder.totals["t"], recorder.totals["t_g"]),
            (1, 1),
            "a group row counts as a row written"
        );
    }

    #[test]
    fn a_group_whose_values_are_all_empty_has_no_row() {
        for empty in [
            r#"{"a": "A", "y": 2}"#,
            r#"{"a": "A", "x1": null, "x2": ""}"#,
            r#"{"a": "A", "x1": [], "x2": {}}"#,
        ] {
            let mut recorder = Recorder::default();
            let rows = write(&split_table(), empty, &mut recorder);
            assert_eq!(rows["t"].len(), 1, "{empty}");
            assert!(rows["t_g"].is_empty(), "{empty}");
            assert!(!recorder.totals.contains_key("t_g"), "{empty}");
        }
    }

    #[test]
    fn one_value_is_enough_and_the_others_are_null() {
        let mut recorder = Recorder::default();
        let rows = write(&split_table(), r#"{"x2": "v"}"#, &mut recorder);
        assert_eq!(rows["t_g"][0][2..], ["\\N".to_string(), "v".to_string()]);
    }

    #[test]
    fn a_flattened_column_is_read_from_its_nested_field_in_the_group() {
        let mut path_map = split_table();
        let group = path_map.get_mut("t_g").unwrap();
        group.columns[2] = column("n_q", "q", PgType::Text);
        group
            .flatten_sources
            .insert("n_q".to_string(), "n".to_string());
        let mut recorder = Recorder::default();
        let rows = write(&path_map, r#"{"n": {"q": "deep"}}"#, &mut recorder);
        assert_eq!(rows["t_g"][0][2], "deep");
    }

    #[test]
    fn a_group_column_is_read_by_the_json_key_it_came_from() {
        let mut path_map = split_table();
        path_map.get_mut("t_g").unwrap().columns[2] = column("x_1", "X 1", PgType::Text);
        let mut recorder = Recorder::default();
        let rows = write(&path_map, r#"{"X 1": "v"}"#, &mut recorder);
        assert_eq!(
            rows["t_g"][0][2], "v",
            "the column is x_1 in the table and X 1 in the file"
        );
    }

    #[test]
    fn an_anomaly_in_a_group_column_is_recorded_for_the_group_table() {
        let mut path_map = split_table();
        path_map.get_mut("t_g").unwrap().columns[2] = column("x1", "x1", PgType::Integer);
        let mut recorder = Recorder::default();
        let rows = write(&path_map, r#"{"x1": "text", "x2": "v"}"#, &mut recorder);
        assert_eq!(
            recorder.anomalies,
            [("t_g".to_string(), "x1".to_string(), ID.to_string())]
        );
        assert_eq!(
            rows["t_g"][0][2], "\\N",
            "the value that cannot be coerced is null"
        );
    }

    #[test]
    fn a_group_table_the_path_map_does_not_have_is_skipped() {
        let mut path_map = split_table();
        path_map
            .get_mut("t")
            .unwrap()
            .column_groups
            .insert(0, "t_gone".to_string());
        let mut recorder = Recorder::default();
        let rows = write(&path_map, r#"{"a": "A", "x1": "u"}"#, &mut recorder);
        assert_eq!(rows["t"].len(), 1);
        assert_eq!(
            rows["t_g"].len(),
            1,
            "the groups that come after the missing one are still written"
        );
    }

    fn make_jsonb_root_schema() -> TableSchema {
        let mut schema = TableSchema::new("t".to_string(), vec!["t".to_string()], 0);
        schema.inferred_strategy = InferredStrategy::Jsonb;
        schema.columns.push(ColumnSchema {
            name: "j2s_id".to_string(),
            original_name: "j2s_id".to_string(),
            pg_type: PgType::Uuid,
            not_null: true,
            is_generated: true,
            is_parent_fk: false,
        });
        schema.columns.push(ColumnSchema {
            name: "data".to_string(),
            original_name: "data".to_string(),
            pg_type: PgType::Jsonb,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        schema
    }

    /// `write_root_jsonb` must serialize the full object as valid JSONB.
    /// Guards against regressions on the no-clone serialization path.
    #[test]
    fn write_root_jsonb_produces_valid_json_blob() {
        let schema = make_jsonb_root_schema();
        let path_map: HashMap<String, TableSchema> = HashMap::new();
        let mut anomalies = AnomalyCollector::new(None);
        let mut sinks: HashMap<String, CaptureSink> = HashMap::new();
        sinks.insert("t".to_string(), CaptureSink(Vec::new()));

        let obj: serde_json::Map<String, Value> =
            serde_json::from_str(r#"{"name":"Widget","count":3}"#).unwrap();

        insert_object(
            &path_map,
            &mut InsertCtx {
                sinks: &mut sinks,
                anomalies: &mut anomalies,
            },
            &schema,
            &obj,
            Uuid::nil(),
            None,
            None,
        )
        .unwrap();

        let rows = &sinks["t"].0;
        assert_eq!(rows.len(), 1);

        // Row format: uuid\tJSON\n — extract the JSONB column
        let row = std::str::from_utf8(&rows[0]).unwrap();
        let json_part = row.split_once('\t').unwrap().1.trim_end_matches('\n');
        let v: Value = serde_json::from_str(json_part).expect("JSONB blob must be valid JSON");
        assert_eq!(v["name"], "Widget");
        assert_eq!(v["count"], 3);
    }

    /// #84: a PG array cell with several unconvertible elements is still written (bad elements as
    /// NULL) and records ONE anomaly, on the array column, carrying the first faulty element.
    #[test]
    fn array_cell_with_bad_elements_records_one_anomaly() {
        let mut schema = TableSchema::new("t".to_string(), vec!["t".to_string()], 0);
        schema.columns.push(ColumnSchema {
            name: "j2s_id".to_string(),
            original_name: "j2s_id".to_string(),
            pg_type: PgType::Uuid,
            not_null: true,
            is_generated: true,
            is_parent_fk: false,
        });
        schema.columns.push(ColumnSchema {
            name: "tags".to_string(),
            original_name: "tags".to_string(),
            pg_type: PgType::Array(Box::new(PgType::VarChar(2))),
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        let path_map: HashMap<String, TableSchema> = HashMap::new();
        let mut anomalies = AnomalyCollector::new(None);
        let mut sinks: HashMap<String, CaptureSink> = HashMap::new();
        sinks.insert("t".to_string(), CaptureSink(Vec::new()));
        let obj: serde_json::Map<String, Value> =
            serde_json::from_str(r#"{"tags":["ab","abc","abcd"]}"#).unwrap();

        insert_object(
            &path_map,
            &mut InsertCtx {
                sinks: &mut sinks,
                anomalies: &mut anomalies,
            },
            &schema,
            &obj,
            Uuid::nil(),
            None,
            None,
        )
        .unwrap();

        let row = std::str::from_utf8(&sinks["t"].0[0]).unwrap();
        assert!(
            row.ends_with("\t{\"ab\",NULL,NULL}\n"),
            "the cell is written with NULL elements, got {row:?}"
        );
        let summaries = anomalies.summaries();
        assert_eq!(
            summaries.len(),
            1,
            "one (table, column) pair: {summaries:?}"
        );
        let s = &summaries[0];
        assert_eq!(
            (s.column.as_str(), s.expected_type.as_str(), s.anomaly_count),
            ("tags", "VARCHAR(2)[]", 1)
        );
        assert_eq!(
            s.examples[0].actual_value, "\"abc\"",
            "the first faulty element is the example"
        );
    }

    #[test]
    fn write_root_jsonb_via_ui_override_routes_correctly() {
        use crate::schema::table_schema::UserOverride;
        // Schema has inferred_strategy=Columns but ui_override=Jsonb — effective_strategy() returns Jsonb.
        let mut schema = TableSchema::new("t".to_string(), vec!["t".to_string()], 0);
        schema.inferred_strategy = InferredStrategy::Columns;
        schema.set_ui_override(Some(UserOverride::Jsonb));
        schema.columns.push(ColumnSchema {
            name: "j2s_id".to_string(),
            original_name: "j2s_id".to_string(),
            pg_type: PgType::Uuid,
            not_null: true,
            is_generated: true,
            is_parent_fk: false,
        });
        schema.columns.push(ColumnSchema {
            name: "data".to_string(),
            original_name: "data".to_string(),
            pg_type: PgType::Jsonb,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        let path_map: HashMap<String, TableSchema> = HashMap::new();
        let mut anomalies = AnomalyCollector::new(None);
        let mut sinks: HashMap<String, CaptureSink> = HashMap::new();
        sinks.insert("t".to_string(), CaptureSink(Vec::new()));
        let obj: serde_json::Map<String, Value> = serde_json::from_str(r#"{"x":1}"#).unwrap();
        insert_object(
            &path_map,
            &mut InsertCtx {
                sinks: &mut sinks,
                anomalies: &mut anomalies,
            },
            &schema,
            &obj,
            Uuid::nil(),
            None,
            None,
        )
        .unwrap();
        // Root Jsonb path writes exactly 1 row with a JSONB blob
        assert_eq!(
            sinks["t"].0.len(),
            1,
            "ui_override=Jsonb on root must produce one JSONB row"
        );
    }
}
