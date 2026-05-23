use tokio_postgres::Client;

use crate::error::{J2sError, Result};
use crate::schema::table_schema::TableSchema;

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

/// Create all tables without constraints (columns only).
/// Drops existing tables first when `drop_existing = true`.
pub async fn create_tables_no_constraints(
    client: &Client,
    schemas: &[TableSchema],
    pg_schema: &str,
    drop_existing: bool,
) -> Result<()> {
    for schema in schemas {
        if drop_existing {
            let drop_sql = format!(
                "DROP TABLE IF EXISTS {}.{} CASCADE",
                quote_ident(pg_schema),
                quote_ident(&schema.name)
            );
            client
                .execute(&drop_sql, &[])
                .await
                .map_err(|e| pg_err(&format!("DROP TABLE {}", schema.name), e))?;
        }
        let create_sql = generate_create_table_no_constraints(schema, pg_schema);
        client
            .execute(&create_sql, &[])
            .await
            .map_err(|e| pg_err(&format!("CREATE TABLE {}", schema.name), e))?;
        eprintln!("Created table: {}.{}", pg_schema, schema.name);
    }
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

/// Add PRIMARY KEY then FOREIGN KEY constraints after data has been loaded.
/// PK failures are fatal (UUID collision = bug). FK failures produce warnings.
/// Schemas must be provided in topological order (parents before children).
pub async fn add_constraints(
    client: &Client,
    schemas: &[TableSchema],
    pg_schema: &str,
) -> Result<Vec<ConstraintWarning>> {
    let mut warnings = Vec::new();

    // Primary keys first (FK references depend on them).
    for schema in schemas {
        let pk_sql = generate_add_pk_sql(schema, pg_schema);
        client
            .execute(&pk_sql, &[])
            .await
            .map_err(|e| pg_err(&format!("ADD PRIMARY KEY {}", schema.name), e))?;
    }

    // Foreign keys: failures are logged as warnings, not errors.
    for schema in schemas {
        if let Some(fk_sql) = generate_add_fk_sql(schema, pg_schema) {
            if let Err(e) = client.execute(&fk_sql, &[]).await {
                let detail = if let Some(db) = e.as_db_error() {
                    format!("{} (code: {})", db.message(), db.code().code())
                } else {
                    e.to_string()
                };
                warnings.push(ConstraintWarning {
                    table: schema.name.clone(),
                    constraint_type: ConstraintKind::ForeignKey,
                    message: detail,
                });
            }
        }
    }

    Ok(warnings)
}

/// Generate a human-readable DDL preview for a single schema, including the FK constraint inline.
/// This is for display only — execution uses `generate_create_table` + a separate ALTER TABLE.
#[allow(dead_code)]
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
}
