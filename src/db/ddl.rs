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

/// Generate and execute CREATE TABLE statements for all schemas.
/// Tables are created in topological order (parents first).
///
/// - `drop_existing = true`  → DROP CASCADE then CREATE (destructive, clean slate)
/// - `drop_existing = false` → CREATE IF NOT EXISTS (append mode, safe for reruns)
pub async fn create_tables(
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

        let create_sql = generate_create_table(schema, pg_schema, drop_existing);
        client
            .execute(&create_sql, &[])
            .await
            .map_err(|e| pg_err(&format!("CREATE TABLE {}", schema.name), e))?;

        eprintln!("Created table: {}.{}", pg_schema, schema.name);
    }

    // Add foreign key constraints after all tables exist.
    // Wrapped in a DO block so reruns skip constraints that already exist
    // (PostgreSQL has no ADD CONSTRAINT IF NOT EXISTS syntax).
    for schema in schemas {
        if let Some(ref parent_name) = schema.parent_table {
            let fk_col = schema
                .columns
                .iter()
                .find(|c| c.is_parent_fk)
                .map(|c| c.name.as_str())
                .unwrap_or("j2s_parent_id");
            let constraint_name = format!("fk_{}_parent", schema.name);
            let fk_sql = format!(
                "DO $$ BEGIN \
                    ALTER TABLE {schema_q}.{table_q} \
                    ADD CONSTRAINT {constraint_name} \
                    FOREIGN KEY ({fk_col_q}) \
                    REFERENCES {schema_q}.{parent_q} (j2s_id); \
                 EXCEPTION WHEN duplicate_object THEN NULL; \
                 END $$",
                schema_q = quote_ident(pg_schema),
                table_q = quote_ident(&schema.name),
                constraint_name = quote_ident(&constraint_name),
                fk_col_q = quote_ident(fk_col),
                parent_q = quote_ident(parent_name),
            );
            client
                .execute(&fk_sql, &[])
                .await
                .map_err(|e| pg_err(&format!("ADD CONSTRAINT fk_{}_parent", schema.name), e))?;
        }
    }

    Ok(())
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

    // Primary key constraint
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
}
