use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use futures_util::future::try_join_all;
use tokio_postgres::Client;

use crate::error::{J2sError, Result};
use crate::io::progress_event::{ProgressEvent, ProgressTx};
use crate::schema::table_schema::TableSchema;

fn emit(tx: Option<&ProgressTx>, event: ProgressEvent) {
    if let Some(t) = tx {
        let _ = t.send(event);
    }
}

fn pg_err(context: &str, e: tokio_postgres::Error) -> J2sError {
    let detail = if let Some(db) = e.as_db_error() {
        format!("{} (code: {})", db.message(), db.code().code())
    } else {
        e.to_string()
    };
    J2sError::DbContext(format!("{}: {}", context, detail))
}



/// Generate the CREATE TABLE SQL for a single schema.
/// Uses `IF NOT EXISTS` when `drop_existing` is false (append / rerun mode).
#[must_use]
pub fn generate_create_table(schema: &TableSchema, pg_schema: &str, drop_existing: bool) -> String {
    let if_not_exists = if drop_existing { "" } else { "IF NOT EXISTS " };
    let mut col_defs = Vec::new();

    for col in &schema.columns {
        // Only enforce NOT NULL for generated infrastructure columns (j2s_id, j2s_parent_id,
        // j2s_order). User-data columns can produce NULL via coercion anomalies even when
        // Pass 1 observed zero nulls, so we never add NOT NULL for them.
        let null_constraint = if col.not_null && col.is_generated { " NOT NULL" } else { "" };
        col_defs.push(format!(
            "    {} {}{}",
            quote_ident(&col.name),
            col.pg_type.as_sql(),
            null_constraint
        ));
    }

    // Primary key constraint — name is guaranteed ≤ 63 chars because NamingRegistry
    // caps table names at PG_TABLE_MAX_IDENT (53): "pk_" (3) + 53 = 56 ≤ 63.
    col_defs.push(format!(
        "    CONSTRAINT {} PRIMARY KEY (j2s_id)",
        quote_ident(&format!("pk_{}", schema.name))
    ));

    format!(
        "CREATE TABLE {}{}.{} (\n{}\n)",
        if_not_exists,
        quote_ident(pg_schema),
        quote_ident(&schema.name),
        col_defs.join(",\n")
    )
}

/// Generate CREATE TABLE SQL with columns only — no PRIMARY KEY or FOREIGN KEY constraints.
/// Always uses IF NOT EXISTS so it is safe to rerun.
/// Constraints are added separately via `add_constraints()` after data is loaded.
#[must_use]
pub fn generate_create_table_no_constraints(schema: &TableSchema, pg_schema: &str) -> String {
    let mut col_defs = Vec::new();
    for col in &schema.columns {
        let null_constraint = if col.not_null && col.is_generated { " NOT NULL" } else { "" };
        col_defs.push(format!(
            "    {} {}{}",
            quote_ident(&col.name),
            col.pg_type.as_sql(),
            null_constraint
        ));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (\n{}\n)",
        quote_ident(pg_schema),
        quote_ident(&schema.name),
        col_defs.join(",\n")
    )
}

/// Generate ALTER TABLE … ADD CONSTRAINT … PRIMARY KEY for a single schema.
#[must_use]
pub fn generate_add_pk_sql(schema: &TableSchema, pg_schema: &str) -> String {
    format!(
        "ALTER TABLE {}.{} ADD CONSTRAINT {} PRIMARY KEY (j2s_id)",
        quote_ident(pg_schema),
        quote_ident(&schema.name),
        quote_ident(&format!("pk_{}", schema.name)),
    )
}

/// Generate ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY for a child schema.
/// Returns None for root tables (no parent).
#[must_use]
pub fn generate_add_fk_sql(schema: &TableSchema, pg_schema: &str) -> Option<String> {
    let parent_name = schema.parent_table.as_ref()?;
    let fk_col = schema
        .columns
        .iter()
        .find(|c| c.is_parent_fk)
        .map(|c| c.name.as_str())
        .unwrap_or("j2s_parent_id");
    Some(format!(
        "ALTER TABLE {schema_q}.{table_q} \
         ADD CONSTRAINT {constraint_q} \
         FOREIGN KEY ({fk_col_q}) \
         REFERENCES {schema_q}.{parent_q} (j2s_id)",
        schema_q = quote_ident(pg_schema),
        table_q = quote_ident(&schema.name),
        constraint_q = quote_ident(&format!("fk_{}_parent", schema.name)),
        fk_col_q = quote_ident(fk_col),
        parent_q = quote_ident(parent_name),
    ))
}

const DDL_BATCH_SIZE: usize = 50;

/// Build a single SQL string that DROPs all schemas in one batch_execute call.
fn build_batch_drop_sql(schemas: &[TableSchema], pg_schema: &str) -> String {
    schemas.iter()
        .map(|s| format!(
            "DROP TABLE IF EXISTS {}.{} CASCADE",
            quote_ident(pg_schema),
            quote_ident(&s.name)
        ))
        .collect::<Vec<_>>()
        .join(";\n")
}

/// Build a single SQL string that CREATEs all schemas in one batch_execute call.
fn build_batch_create_sql(schemas: &[TableSchema], pg_schema: &str) -> String {
    schemas.iter()
        .map(|s| generate_create_table_no_constraints(s, pg_schema))
        .collect::<Vec<_>>()
        .join(";\n")
}

/// Create all tables without constraints (columns only).
/// Drops existing tables first when `drop_existing = true` (single batch).
/// Creates tables in batches of `DDL_BATCH_SIZE` to reduce round-trips.
pub async fn create_tables_no_constraints(
    client: &Client,
    schemas: &[TableSchema],
    pg_schema: &str,
    drop_existing: bool,
    progress_tx: Option<&ProgressTx>,
) -> Result<()> {
    let total = schemas.len();
    emit(progress_tx, ProgressEvent::DdlStart { table_count: total });

    if drop_existing && !schemas.is_empty() {
        let drop_sql = build_batch_drop_sql(schemas, pg_schema);
        client.batch_execute(&drop_sql).await
            .map_err(|e| pg_err("DROP TABLE batch", e))?;
    }

    let mut done = 0usize;
    for chunk in schemas.chunks(DDL_BATCH_SIZE) {
        let create_sql = build_batch_create_sql(chunk, pg_schema);
        client.batch_execute(&create_sql).await
            .map_err(|e| pg_err("CREATE TABLE batch", e))?;
        done += chunk.len();
        emit(progress_tx, ProgressEvent::DdlProgress { done, total });
    }

    emit(progress_tx, ProgressEvent::DdlDone);
    Ok(())
}

/// A constraint that could not be applied after loading data.
#[derive(Debug, Clone)]
pub struct ConstraintWarning {
    pub table: String,
    #[allow(dead_code)]
    pub constraint_type: ConstraintKind,
    pub message: String,
}

#[derive(Debug, Clone)]
pub enum ConstraintKind {
    #[allow(dead_code)]
    PrimaryKey,
    ForeignKey,
}

/// Group schemas by depth level (BTreeMap preserves ascending order → parents before children).
fn group_schemas_by_depth(schemas: &[TableSchema]) -> BTreeMap<usize, Vec<&TableSchema>> {
    let mut map: BTreeMap<usize, Vec<&TableSchema>> = BTreeMap::new();
    for schema in schemas {
        map.entry(schema.depth).or_default().push(schema);
    }
    map
}

/// Pre-built work item for one table's constraints: (table_name, pk_sql, fk_sql_opt).
type ConstraintWork = (String, String, Option<String>);

/// Add PRIMARY KEY + FOREIGN KEY constraints after data has been loaded.
///
/// Processes schemas level-by-level (grouped by `depth`): all tables at depth N
/// are constrained in parallel before moving to depth N+1.  Within each level,
/// each worker applies PK then FK for its assigned tables on a dedicated connection.
///
/// PK failures are fatal.  FK failures become warnings.
pub async fn add_constraints(
    db_url: &str,
    schemas: &[TableSchema],
    pg_schema: &str,
    parallel: usize,
    progress_tx: Option<&ProgressTx>,
) -> Result<Vec<ConstraintWarning>> {
    if schemas.is_empty() {
        emit(progress_tx, ProgressEvent::ConstraintsStart { table_count: 0 });
        emit(progress_tx, ProgressEvent::ConstraintsDone);
        return Ok(Vec::new());
    }

    let fk_count = schemas.iter().filter(|s| s.parent_table.is_some()).count();
    let total = schemas.len() + fk_count;
    let done = Arc::new(AtomicUsize::new(0));
    let ptx: Option<ProgressTx> = progress_tx.cloned();

    emit(progress_tx, ProgressEvent::ConstraintsStart { table_count: schemas.len() });

    let levels = group_schemas_by_depth(schemas);
    let mut all_warnings: Vec<ConstraintWarning> = Vec::new();

    for schemas_at_level in levels.values() {
        // Pre-build owned SQL so async move blocks have no lifetime issues.
        let work: Vec<ConstraintWork> = schemas_at_level.iter().map(|s| (
            s.name.clone(),
            generate_add_pk_sql(s, pg_schema),
            generate_add_fk_sql(s, pg_schema),
        )).collect();

        let conn_count = parallel.min(work.len()).max(1);
        let chunk_size = work.len().div_ceil(conn_count);

        let futures: Vec<_> = work.chunks(chunk_size).map(|chunk| {
            let chunk: Vec<ConstraintWork> = chunk.to_vec();
            let db_url = db_url.to_string();
            let done = Arc::clone(&done);
            let ptx = ptx.clone();
            async move {
                let (client, connection) = tokio_postgres::connect(&db_url, tokio_postgres::NoTls)
                    .await
                    .map_err(|e| J2sError::DbContext(format!("constraints connection: {e}")))?;
                tokio::spawn(async move { let _ = connection.await; });

                let mut warnings: Vec<ConstraintWarning> = Vec::new();
                for (table_name, pk_sql, fk_sql_opt) in &chunk {
                    client.execute(pk_sql.as_str(), &[]).await
                        .map_err(|e| pg_err(&format!("ADD PRIMARY KEY {table_name}"), e))?;
                    constraint_progress(&done, total, &ptx);

                    if let Some(fk_sql) = fk_sql_opt {
                        if let Err(e) = client.execute(fk_sql.as_str(), &[]).await {
                            warnings.push(to_fk_warning(e, table_name));
                        }
                        constraint_progress(&done, total, &ptx);
                    }
                }
                Ok::<Vec<ConstraintWarning>, J2sError>(warnings)
            }
        }).collect();

        let results = try_join_all(futures).await?;
        for w in results { all_warnings.extend(w); }
    }

    emit(progress_tx, ProgressEvent::ConstraintsDone);
    Ok(all_warnings)
}

fn constraint_progress(done: &std::sync::atomic::AtomicUsize, total: usize, ptx: &Option<ProgressTx>) {
    let d = done.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    if let Some(ref t) = ptx {
        let _ = t.send(ProgressEvent::ConstraintsProgress { done: d, total });
    }
}

fn to_fk_warning(e: tokio_postgres::Error, table: &str) -> ConstraintWarning {
    let detail = if let Some(db) = e.as_db_error() {
        format!("{} (code: {})", db.message(), db.code().code())
    } else {
        e.to_string()
    };
    ConstraintWarning { table: table.to_string(), constraint_type: ConstraintKind::ForeignKey, message: detail }
}

/// Generate a human-readable DDL preview for a single schema, including the FK constraint inline.
/// This is for display only — execution uses `generate_create_table` + a separate ALTER TABLE.
#[allow(dead_code)]
#[must_use]
pub fn generate_ddl_preview(schema: &TableSchema, pg_schema: &str) -> String {
    let mut col_defs = Vec::new();

    for col in &schema.columns {
        let null_constraint = if col.not_null && col.is_generated { " NOT NULL" } else { "" };
        col_defs.push(format!(
            "    {} {}{}",
            quote_ident(&col.name),
            col.pg_type.as_sql(),
            null_constraint
        ));
    }

    col_defs.push(format!(
        "    CONSTRAINT {} PRIMARY KEY (j2s_id)",
        quote_ident(&format!("pk_{}", schema.name))
    ));

    if let Some(ref parent_name) = schema.parent_table {
        let fk_col = schema
            .columns
            .iter()
            .find(|c| c.is_parent_fk)
            .map(|c| c.name.as_str())
            .unwrap_or("j2s_parent_id");
        col_defs.push(format!(
            "    CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}.{} (j2s_id)",
            quote_ident(&format!("fk_{}_parent", schema.name)),
            quote_ident(fk_col),
            quote_ident(pg_schema),
            quote_ident(parent_name),
        ));
    }

    format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (\n{}\n)",
        quote_ident(pg_schema),
        quote_ident(&schema.name),
        col_defs.join(",\n")
    )
}

/// Quote a PostgreSQL identifier with double quotes, escaping internal quotes.
#[must_use]
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::table_schema::{ColumnSchema, TableSchema};
    use crate::schema::type_tracker::PgType;

    #[test]
    fn test_generate_create_table() {
        let mut schema = TableSchema::new("users".to_string(), vec!["users".to_string()], 0);
        schema.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        schema.columns.push(ColumnSchema {
            name: "name".to_string(),
            original_name: "name".to_string(),
            pg_type: PgType::VarChar(60),
            not_null: true,
            is_generated: false,
            is_parent_fk: false,
        });
        schema.columns.push(ColumnSchema {
            name: "age".to_string(),
            original_name: "age".to_string(),
            pg_type: PgType::Integer,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });

        let sql = generate_create_table(&schema, "public", true);
        assert!(sql.contains("CREATE TABLE \"public\".\"users\""));
        assert!(sql.contains("\"j2s_id\" UUID NOT NULL"));
        assert!(sql.contains("\"name\" VARCHAR(60)"));
        assert!(sql.contains("\"age\" INTEGER"));
        assert!(sql.contains("PRIMARY KEY (j2s_id)"));
    }

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident("my\"table"), "\"my\"\"table\"");
    }

    fn make_root_schema() -> TableSchema {
        let mut s = TableSchema::new("products".to_string(), vec!["products".to_string()], 0);
        s.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        s.columns.push(ColumnSchema {
            name: "title".to_string(),
            original_name: "title".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        s
    }

    fn make_child_schema() -> TableSchema {
        let mut s = TableSchema::new("tags".to_string(), vec!["products".to_string(), "tags".to_string()], 1);
        s.parent_table = Some("products".to_string());
        s.columns.push(ColumnSchema::generated("j2s_id", PgType::Uuid));
        s.columns.push(ColumnSchema {
            name: "j2s_parent_id".to_string(),
            original_name: "j2s_parent_id".to_string(),
            pg_type: PgType::Uuid,
            not_null: true,
            is_generated: true,
            is_parent_fk: true,
        });
        s.columns.push(ColumnSchema {
            name: "value".to_string(),
            original_name: "value".to_string(),
            pg_type: PgType::Text,
            not_null: false,
            is_generated: false,
            is_parent_fk: false,
        });
        s
    }

    #[test]
    fn no_constraints_create_excludes_pk_and_fk() {
        let root = make_root_schema();
        let sql = generate_create_table_no_constraints(&root, "public");
        assert!(sql.contains("CREATE TABLE"), "should be a CREATE TABLE");
        assert!(!sql.contains("PRIMARY KEY"), "no PK inline");
        assert!(!sql.contains("FOREIGN KEY"), "no FK inline");
        assert!(sql.contains("\"j2s_id\""), "j2s_id column present");
        assert!(sql.contains("\"title\""), "title column present");
    }

    #[test]
    fn no_constraints_create_uses_if_not_exists() {
        let root = make_root_schema();
        let sql = generate_create_table_no_constraints(&root, "public");
        assert!(sql.contains("IF NOT EXISTS"), "always idempotent");
    }

    #[test]
    fn add_pk_sql_correct() {
        let root = make_root_schema();
        let sql = generate_add_pk_sql(&root, "public");
        assert!(sql.contains("ALTER TABLE"), "ALTER TABLE");
        assert!(sql.contains("ADD CONSTRAINT"), "ADD CONSTRAINT");
        assert!(sql.contains("PRIMARY KEY"), "PRIMARY KEY");
        assert!(sql.contains("\"pk_products\""), "constraint name");
        assert!(sql.contains("\"products\""), "table name");
    }

    #[test]
    fn add_fk_sql_root_returns_none() {
        let root = make_root_schema();
        assert!(generate_add_fk_sql(&root, "public").is_none(), "root has no parent");
    }

    #[test]
    fn add_fk_sql_child_correct() {
        let child = make_child_schema();
        let sql = generate_add_fk_sql(&child, "public").expect("child should have FK sql");
        assert!(sql.contains("ALTER TABLE"), "ALTER TABLE");
        assert!(sql.contains("FOREIGN KEY"), "FOREIGN KEY");
        assert!(sql.contains("REFERENCES"), "REFERENCES");
        assert!(sql.contains("\"products\""), "references parent");
        assert!(sql.contains("\"tags\""), "on child table");
    }

    #[test]
    fn emit_helper_sends_to_channel_and_ignores_none() {
        use crate::io::progress_event::ProgressEvent;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        emit(Some(&tx), ProgressEvent::DdlStart { table_count: 5 });
        emit(Some(&tx), ProgressEvent::DdlProgress { done: 1, total: 5 });
        emit(Some(&tx), ProgressEvent::DdlDone);
        emit(None, ProgressEvent::DdlDone); // None must not panic
        drop(tx);
        assert!(matches!(rx.try_recv().unwrap(), ProgressEvent::DdlStart { table_count: 5 }));
        assert!(matches!(rx.try_recv().unwrap(), ProgressEvent::DdlProgress { done: 1, total: 5 }));
        assert!(matches!(rx.try_recv().unwrap(), ProgressEvent::DdlDone));
        assert!(rx.try_recv().is_err(), "None emit produces no event");
    }

    #[test]
    fn constraints_total_counts_pk_plus_fk_only() {
        // 1 root (no FK) + 2 children (each has FK) → total = 3 PKs + 2 FKs = 5
        let root = make_root_schema();
        let child1 = make_child_schema();
        let mut child2 = make_child_schema();
        child2.name = "tags2".to_string();
        let schemas = [root, child1, child2];
        let fk_count = schemas.iter().filter(|s| s.parent_table.is_some()).count();
        let total = schemas.len() + fk_count;
        assert_eq!(fk_count, 2);
        assert_eq!(total, 5);
    }

    #[test]
    fn build_batch_create_sql_contains_all_tables() {
        let root = make_root_schema();
        let child = make_child_schema();
        let sql = build_batch_create_sql(&[root, child], "public");
        assert_eq!(sql.matches("CREATE TABLE").count(), 2, "one CREATE TABLE per schema");
        assert!(sql.contains("\"products\""), "root table present");
        assert!(sql.contains("\"tags\""), "child table present");
        assert!(sql.contains(";\n"), "statements separated by semicolon");
        // No PK or FK constraints (columns only)
        assert!(!sql.contains("PRIMARY KEY"), "no PK in no-constraints create");
        assert!(!sql.contains("FOREIGN KEY"), "no FK in no-constraints create");
    }

    #[test]
    fn build_batch_drop_sql_contains_all_tables_with_cascade() {
        let root = make_root_schema();
        let child = make_child_schema();
        let sql = build_batch_drop_sql(&[root, child], "public");
        assert_eq!(sql.matches("DROP TABLE IF EXISTS").count(), 2, "one DROP per schema");
        assert!(sql.contains("CASCADE"), "DROP includes CASCADE");
        assert!(sql.contains("\"products\""), "root table present");
        assert!(sql.contains("\"tags\""), "child table present");
    }

    #[test]
    fn build_batch_create_sql_empty_slice_returns_empty() {
        assert_eq!(build_batch_create_sql(&[], "public"), "");
        assert_eq!(build_batch_drop_sql(&[], "public"), "");
    }

    #[test]
    fn group_schemas_by_depth_splits_levels_correctly() {
        let root = make_root_schema();   // depth 0
        let child = make_child_schema(); // depth 1
        let schemas = vec![root, child];
        let groups = group_schemas_by_depth(&schemas);
        assert_eq!(groups.len(), 2, "two distinct levels");
        assert_eq!(groups[&0].len(), 1, "one root at depth 0");
        assert_eq!(groups[&1].len(), 1, "one child at depth 1");
        assert_eq!(groups[&0][0].name, "products");
        assert_eq!(groups[&1][0].name, "tags");
    }

    #[test]
    fn group_schemas_by_depth_sibling_tables_same_level() {
        let root = make_root_schema();
        let mut child1 = make_child_schema();
        let mut child2 = make_child_schema();
        child2.name = "tags2".to_string();
        let schemas = vec![root, child1, child2];
        let groups = group_schemas_by_depth(&schemas);
        assert_eq!(groups[&1].len(), 2, "sibling tables grouped at same depth");
    }

    #[test]
    fn group_schemas_by_depth_keys_sorted_ascending() {
        // BTreeMap ensures level 0 processed before level 1 (parents before children)
        let root = make_root_schema();
        let child = make_child_schema();
        let schemas = vec![child, root]; // intentionally reversed input order
        let groups = group_schemas_by_depth(&schemas);
        let levels: Vec<usize> = groups.keys().copied().collect();
        assert_eq!(levels, vec![0, 1], "BTreeMap preserves ascending order");
    }
}
