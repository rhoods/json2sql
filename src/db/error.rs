//! Shared error helpers for the database layer.
//!
//! Fonctions :
//! - `pg_err` — convertit une `tokio_postgres::Error` en `J2sError::DbContext`, avec le code PG si disponible.

use crate::error::J2sError;

pub fn pg_err(context: &str, e: &tokio_postgres::Error) -> J2sError {
    let detail = e.as_db_error().map_or_else(
        || e.to_string(),
        |db| format!("{} (code: {})", db.message(), db.code().code()),
    );
    J2sError::DbContext(format!("{context}: {detail}"))
}
