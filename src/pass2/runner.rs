use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio_postgres::Client;
use uuid::Uuid;

use crate::anomaly::collector::AnomalyCollector;
use crate::db::copy_sink::{RowBuilder, TempFileSink};
use crate::error::{J2sError, Result};
use crate::io::progress::ProgressTracker;
use crate::io::progress_event::{ProgressEvent, ProgressTx};
use crate::io::reader::{file_size, JsonReader};
use crate::db::copy_text::{escape_copy_text, CopyEscaped};
use crate::pass2::coercer::{coerce, CoerceResult};
use crate::schema::table_schema::{ChildKind, SiblingSchema, SuffixSchema, TableSchema, WideStrategy};
// NormalizeDynamicKeys and Flatten are handled via dedicated insert functions below.

/// Pass 2 result summary.
pub struct Pass2Result {
    pub rows_per_table: HashMap<String, u64>,
    pub anomaly_collector: AnomalyCollector,
}

/// Run Pass 2: stream through the file again, buffer rows to temp files,
/// then COPY each table into PostgreSQL.
///
/// `db_url` is only required when `parallel > 1` (each worker opens its own connection).
/// `anomaly_dir` is the directory where per-table NDJSON anomaly files are streamed;
/// `None` disables file streaming (counters and examples are still collected in RAM).
/// `progress_tx` — optional channel for streaming progress to the IHM.
pub async fn run(
    path: &Path,
    root_table: &str,
    schemas: &[TableSchema],
    client: &Client,
    pg_schema: &str,
    flush_threshold: usize,
    use_transaction: bool,
    db_url: Option<&str>,
    parallel: usize,
    anomaly_dir: Option<PathBuf>,
    progress_tx: Option<ProgressTx>,
) -> Result<Pass2Result> {
    let total_bytes = file_size(path)?;
    let progress = if progress_tx.is_none() {
        Some(ProgressTracker::new(total_bytes, "Pass 2"))
    } else {
        None
    };
    let mut rows_processed = 0u64;
    const PROGRESS_INTERVAL: u64 = 1_000;

    // Build path → schema lookup
    let path_map: HashMap<String, &TableSchema> =
        schemas.iter().map(|s| (s.path.join("."), s)).collect();

    // Open a TempFileSink for each table
    let mut sinks: HashMap<String, TempFileSink> = HashMap::new();
    for schema in schemas {
        sinks.insert(
            schema.name.clone(),
            TempFileSink::new(schema, pg_schema)?,
        );
    }

    // Create anomaly_dir if specified and not yet existing
    if let Some(ref dir) = anomaly_dir {
        std::fs::create_dir_all(dir).map_err(crate::error::J2sError::Io)?;
    }

    let mut anomalies = AnomalyCollector::new(anomaly_dir);
    let (mut reader, _format) = JsonReader::open(path)?;

    let root_schema = schemas
        .iter()
        .find(|s| s.path.join(".") == root_table)
        .ok_or_else(|| J2sError::Schema(format!("Root table '{}' not found", root_table)))?;

    let flush_threshold = flush_threshold as u64;

    // Pre-computed topological order: schemas is already sorted parents-before-children
    // by Pass 1. We use this order for periodic flushes to avoid FK constraint violations
    // (a child flush must never precede an unflushed parent).
    let topo_order: Vec<String> = schemas.iter().map(|s| s.name.clone()).collect();

    // Amortize the O(n_sinks) flush-threshold scan: only check every 1% of the threshold
    // (minimum 1). For a threshold of 100k rows this means checking every 1k root objects
    // instead of every single one, while still triggering the flush within 1% of the target.
    let flush_check_interval = if flush_threshold > 0 { (flush_threshold / 100).max(1) } else { 0 };
    let mut flush_check_counter = 0u64;

    while let Some(item) = reader.next() {
        let value = item?;
        if let Value::Object(ref obj) = value {
            let row_id = Uuid::now_v7();
            insert_object(
                &path_map,
                &mut sinks,
                &mut anomalies,
                root_schema,
                obj,
                row_id,
                None,
                None,
            )?;
            rows_processed += 1;
            if let Some(ref bar) = progress {
                bar.inc_rows(1);
            }
            if let Some(ref tx) = progress_tx {
                if rows_processed % PROGRESS_INTERVAL == 0 {
                    let _ = tx.send(ProgressEvent::Pass2Progress {
                        rows_processed,
                        bytes_read: reader.bytes_read(),
                        total_bytes,
                    });
                }
            }

            // Periodic flush: when any sink reaches the threshold, flush ALL sinks
            // in topological order (parents before children). This keeps temp-file
            // disk usage bounded while respecting FK constraints.
            if flush_check_interval > 0 {
                flush_check_counter += 1;
                if flush_check_counter >= flush_check_interval {
                    flush_check_counter = 0;
                    if sinks.values().any(|s| s.row_count >= flush_threshold) {
                        for name in &topo_order {
                            if let Some(sink) = sinks.get_mut(name.as_str()) {
                                if sink.row_count > 0 {
                                    let flushed = sink.row_count;
                                    sink.flush_to_db(client).await?;
                                    if let Some(ref tx) = progress_tx {
                                        let _ = tx.send(ProgressEvent::Pass2Flush {
                                            table_name: name.clone(),
                                            rows_flushed: flushed,
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    if let Some(ref tx) = progress_tx {
        if rows_processed > 0 && rows_processed % PROGRESS_INTERVAL != 0 {
            let _ = tx.send(ProgressEvent::Pass2Progress {
                rows_processed,
                bytes_read: reader.bytes_read(),
                total_bytes,
            });
        }
    }

    if let Some(ref bar) = progress {
        bar.finish();
    }
    eprintln!("Pass 2 streaming done. Flushing remaining rows to PostgreSQL...");

    // Group sinks by depth level (topological order: parents before children).
    // Within each level, tables can be COPYed independently.
    // O(n) pre-build instead of O(n²) linear scan per sink.
    let name_to_depth: HashMap<&str, usize> =
        schemas.iter().map(|s| (s.name.as_str(), s.depth)).collect();
    let mut by_depth: BTreeMap<usize, Vec<String>> = BTreeMap::new();
    for (name, _) in &sinks {
        let depth = name_to_depth.get(name.as_str()).copied().unwrap_or(0);
        by_depth.entry(depth).or_default().push(name.clone());
    }

    let use_parallel = parallel > 1;

    if use_parallel && use_transaction {
        eprintln!("WARNING: --transaction is not supported with --parallel > 1; running without transaction.");
    }

    if use_parallel {
        // Parallel COPY: process depth levels one by one, tables within each level in parallel.
        let db_url = db_url.ok_or_else(|| {
            J2sError::InvalidInput("--parallel requires a database URL".to_string())
        })?;
        let db_url = db_url.to_string();
        let semaphore = Arc::new(Semaphore::new(parallel));
        let mut rows_per_table = HashMap::new();

        for (_depth, names) in by_depth {
            let mut join_set: JoinSet<Result<(String, u64)>> = JoinSet::new();

            for name in names {
                if let Some(sink) = sinks.remove(&name) {
                    let row_count = sink.row_count;
                    eprintln!("  COPY {} ({} rows)...", name, row_count);
                    let url = db_url.clone();
                    let sem = semaphore.clone();
                    join_set.spawn(async move {
                        let _permit = sem.acquire().await.map_err(|e| {
                            J2sError::InvalidInput(format!("Semaphore error: {}", e))
                        })?;
                        let conn = crate::db::connection::connect(&url).await?;
                        let inserted = sink.copy_to_db(&conn).await?;
                        Ok((name, inserted))
                    });
                }
            }

            while let Some(res) = join_set.join_next().await {
                match res {
                    Ok(Ok((name, count))) => { rows_per_table.insert(name, count); }
                    Ok(Err(e)) => return Err(e),
                    Err(e) => return Err(J2sError::InvalidInput(format!("Task join error: {}", e))),
                }
            }
        }

        if let Some(ref tx) = progress_tx {
            let total_rows: u64 = rows_per_table.values().sum();
            let _ = tx.send(ProgressEvent::Pass2Done {
                total_rows,
                anomaly_count: anomalies.total_anomalies(),
            });
        }
        anomalies.finish()?;
        return Ok(Pass2Result { rows_per_table, anomaly_collector: anomalies });
    }

    // Sequential COPY (single connection)
    let table_names: Vec<String> = by_depth.into_values().flatten().collect();

    if use_transaction {
        client.execute("BEGIN", &[]).await.map_err(J2sError::Db)?;
    }

    let copy_result = async {
        let mut rows_per_table = HashMap::new();
        for name in &table_names {
            if let Some(sink) = sinks.remove(name) {
                let count = sink.row_count;
                eprintln!("  COPY {} ({} rows)...", name, count);
                if let Some(ref tx) = progress_tx {
                    let _ = tx.send(ProgressEvent::Pass2Log(
                        format!("COPY {} ({} rows)", name, count)
                    ));
                }
                let inserted = sink.copy_to_db(client).await?;
                rows_per_table.insert(name.clone(), inserted);
                if let Some(ref tx) = progress_tx {
                    // Use `count` (the final batch size), not `inserted` (cumulative total
                    // including previous streaming flushes). The UI accumulates all flush
                    // events; sending the cumulative total here would double-count.
                    let _ = tx.send(ProgressEvent::Pass2Flush {
                        table_name: name.clone(),
                        rows_flushed: count,
                    });
                }
            }
        }
        Ok::<_, J2sError>(rows_per_table)
    }
    .await;

    if use_transaction {
        match copy_result {
            Ok(rows_per_table) => {
                client.execute("COMMIT", &[]).await.map_err(J2sError::Db)?;
                if let Some(ref tx) = progress_tx {
                    let total_rows: u64 = rows_per_table.values().sum();
                    let _ = tx.send(ProgressEvent::Pass2Done {
                        total_rows,
                        anomaly_count: anomalies.total_anomalies(),
                    });
                }
                anomalies.finish()?;
                return Ok(Pass2Result { rows_per_table, anomaly_collector: anomalies });
            }
            Err(e) => {
                if let Err(rb_err) = client.execute("ROLLBACK", &[]).await {
                    eprintln!("WARNING: ROLLBACK failed after import error: {rb_err}");
                    eprintln!("         The database may be in an inconsistent state.");
                }
                return Err(e);
            }
        }
    }

    let rows_per_table = copy_result?;
    if let Some(ref tx) = progress_tx {
        let total_rows: u64 = rows_per_table.values().sum();
        let _ = tx.send(ProgressEvent::Pass2Done {
            total_rows,
            anomaly_count: anomalies.total_anomalies(),
        });
    }
    anomalies.finish()?;
    Ok(Pass2Result { rows_per_table, anomaly_collector: anomalies })
}

// ---------------------------------------------------------------------------
// Recursive row insertion (synchronous — writes to temp files)
// ---------------------------------------------------------------------------

fn insert_object(
    path_map: &HashMap<String, &TableSchema>,
    sinks: &mut HashMap<String, TempFileSink>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    row_id: Uuid,
    parent_id: Option<Uuid>,
    order: Option<i64>,
) -> Result<()> {
    // Pre-compute the parent path key once — reused for every child field lookup below.
    let parent_path_key = schema.path.join(".");

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
            let child_key = format!("{}.{}", parent_path_key, field);
            match value {
                Value::Object(nested) => {
                    if let Some(child_schema) = path_map.get(&child_key) {
                        match &child_schema.wide_strategy {
                            WideStrategy::Pivot => {
                                insert_pivot_object(sinks, anomalies, child_schema, nested, row_id)?;
                            }
                            WideStrategy::Jsonb => {
                                insert_jsonb_object(sinks, anomalies, child_schema, value, row_id)?;
                            }
                            WideStrategy::StructuredPivot(suffix_schema) => {
                                insert_structured_pivot_object(
                                    sinks, anomalies, child_schema, nested, row_id, suffix_schema,
                                )?;
                            }
                            WideStrategy::KeyedPivot(sibling_schema) => {
                                insert_keyed_pivot_object(
                                    sinks, anomalies, child_schema, nested, row_id, sibling_schema,
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
        let child_key = format!("{}.{}", parent_path_key, field);

        match value {
            Value::Object(nested) => {
                if let Some(child_schema) = path_map.get(&child_key) {
                    match &child_schema.wide_strategy {
                        WideStrategy::Pivot => {
                            insert_pivot_object(sinks, anomalies, child_schema, nested, row_id)?;
                        }
                        WideStrategy::Jsonb => {
                            insert_jsonb_object(sinks, anomalies, child_schema, value, row_id)?;
                        }
                        WideStrategy::StructuredPivot(suffix_schema) => {
                            insert_structured_pivot_object(
                                sinks, anomalies, child_schema, nested, row_id, suffix_schema,
                            )?;
                        }
                        WideStrategy::KeyedPivot(sibling_schema) => {
                            insert_keyed_pivot_object(
                                sinks, anomalies, child_schema, nested, row_id, sibling_schema,
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

/// Insert one row containing the entire object serialized as JSONB.
/// Columns: j2s_id, j2s_parent_id, data JSONB
fn insert_jsonb_object(
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

/// Insert one row per sibling key for a KeyedPivot table.
/// Columns: j2s_id, j2s_parent_id, key TEXT, <union data cols...>
///
/// Each key in `obj` maps to a child object; the key becomes the `key_col`,
/// and the child object's scalar fields are spread across the union columns.
/// Non-Object values (scalars, arrays) are skipped.
fn insert_keyed_pivot_object(
    sinks: &mut HashMap<String, TempFileSink>,
    anomalies: &mut AnomalyCollector,
    schema: &TableSchema,
    obj: &serde_json::Map<String, Value>,
    parent_id: Uuid,
    sibling_schema: &SiblingSchema,
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

fn insert_array(
    path_map: &HashMap<String, &TableSchema>,
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
