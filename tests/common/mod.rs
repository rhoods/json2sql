//! Shared helpers for integration tests.
//!
//! Ces tests nécessitent une instance PostgreSQL. Définir `TEST_DATABASE_URL` pour les activer :
//!
//!   TEST_DATABASE_URL=postgres://user:pass@localhost/testdb cargo test
//!
//! Sans cette variable, tous les tests sont silencieusement ignorés.
//!
//! ## Schémas de test
//!
//! Chaque test crée un schéma `j2s_test_<random>` via `with_schema()` et le supprime à la fin,
//! même en cas de panic. `with_schema` capture le panic via `catch_unwind`, drop le schéma
//! avec une connexion fraîche, puis resume le panic pour que le test soit marqué FAILED.

use std::future::Future;
use std::path::PathBuf;

use futures_util::FutureExt;
use uuid::Uuid;

use json2sql::db;

pub fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

pub fn db_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL").ok()
}

pub async fn connect_test_db() -> Option<tokio_postgres::Client> {
    let url = db_url()?;
    db::connection::connect(&url).await.ok()
}

pub fn unique_schema() -> String {
    // UUID v7 without dashes: chars [20..32] are in the rand_b field (62 mostly-random bits),
    // avoiding collisions between runs starting within the same millisecond.
    let id = Uuid::now_v7().to_string().replace('-', "");
    format!("j2s_test_{}", &id[20..32])
}

pub async fn row_count(client: &tokio_postgres::Client, schema: &str, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM \"{}\".\"{}\"", schema, table);
    let row = client.query_one(&sql, &[]).await.unwrap();
    row.get::<_, i64>(0)
}

pub async fn drop_schema(client: &tokio_postgres::Client, schema: &str) {
    let _ = client
        .execute(
            &format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema),
            &[],
        )
        .await;
}

/// Run a test body with a fresh PostgreSQL schema, guaranteed to be dropped even on panic.
///
/// If `TEST_DATABASE_URL` is not set or the connection fails, the test is silently skipped.
/// On panic: the schema is dropped via a fresh connection, then the panic is resumed so the
/// test is reported as FAILED rather than silently swallowed.
pub async fn with_schema<F, Fut>(f: F)
where
    F: FnOnce(tokio_postgres::Client, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let client = match db::connection::connect(&url).await {
        Ok(c) => c,
        Err(_) => return,
    };

    let schema = unique_schema();
    client
        .execute(&format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .unwrap();

    let result = std::panic::AssertUnwindSafe(f(client, schema.clone()))
        .catch_unwind()
        .await;

    // Always drop — open a fresh connection since the original was consumed by f().
    if let Ok(cleanup) = db::connection::connect(&url).await {
        drop_schema(&cleanup, &schema).await;
    }

    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

/// Variant of `with_schema` that also passes the database URL to the test body.
/// Needed for tests that open additional connections (e.g. parallel COPY).
pub async fn with_schema_url<F, Fut>(f: F)
where
    F: FnOnce(tokio_postgres::Client, String, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let url = match db_url() {
        Some(u) => u,
        None => return,
    };

    let client = match db::connection::connect(&url).await {
        Ok(c) => c,
        Err(_) => return,
    };

    let schema = unique_schema();
    client
        .execute(&format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .unwrap();

    let result = std::panic::AssertUnwindSafe(f(client, schema.clone(), url.clone()))
        .catch_unwind()
        .await;

    if let Ok(cleanup) = db::connection::connect(&url).await {
        drop_schema(&cleanup, &schema).await;
    }

    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
