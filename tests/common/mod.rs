//! Shared helpers for integration tests.
//!
//! Ces tests nécessitent une instance PostgreSQL. Définir `TEST_DATABASE_URL` pour les activer :
//!
//!   TEST_DATABASE_URL=postgres://user:pass@localhost/testdb cargo test
//!
//! Sans cette variable, tous les tests sont ignorés avec un message [SKIP] sur stderr.
//!
//! ## Schémas de test
//!
//! Chaque test crée un schéma `j2s_test_<random>` via `with_schema_url()` et le supprime à la fin,
//! même en cas de panic. `with_schema_url` capture le panic via `catch_unwind`, drop le schéma
//! avec une connexion fraîche, puis resume le panic pour que le test soit marqué FAILED.

use std::future::Future;
use std::path::PathBuf;

use futures_util::FutureExt;
use uuid::Uuid;

use json2sql::db::connection;
use json2sql::pass1::runner::Pass1Config;
use json2sql::pass2::Pass2Config;
use json2sql::schema::registry::RegistryConfig;

/// Default Pass1Config for integration tests — all strategies enabled, standard thresholds.
#[allow(dead_code)]
pub fn pass1_config(root_table: &str) -> Pass1Config {
    Pass1Config {
        root_table: root_table.to_string(),
        registry: RegistryConfig::default(),
        num_workers: None,
    }
}

/// Default Pass2Config for integration tests — parallel=1, no limit, no dirs.
#[allow(dead_code)]
pub fn pass2_config(root_table: &str, pg_schema: &str) -> Pass2Config {
    Pass2Config {
        root_table: root_table.to_string(),
        pg_schema: pg_schema.to_string(),
        parallel: 1,
        anomaly_dir: None,
        limit: None,
        mem_flush_threshold_bytes: None,
        ram_high_watermark: None,
        ram_low_watermark: None,
        verbose: false,
        hint_format: None,
        skip_constraints: false,
    }
}

#[allow(dead_code)]
pub fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

pub fn db_url() -> Option<String> {
    std::env::var("TEST_DATABASE_URL").ok()
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
/// Passes the database URL to the test body — needed for tests that open additional
/// connections (e.g. parallel COPY).
pub async fn with_schema_url<F, Fut>(f: F)
where
    F: FnOnce(tokio_postgres::Client, String, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let url = match db_url() {
        Some(u) => u,
        None => {
            eprintln!("[SKIP] TEST_DATABASE_URL not set — test requires a PostgreSQL connection.");
            return;
        }
    };

    let client = match connection::connect(&url).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[SKIP] Could not connect to TEST_DATABASE_URL: {e}");
            return;
        }
    };

    let schema = unique_schema();
    client
        .execute(&format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .unwrap();

    let result = std::panic::AssertUnwindSafe(f(client, schema.clone(), url.clone()))
        .catch_unwind()
        .await;

    if let Ok(cleanup) = connection::connect(&url).await {
        drop_schema(&cleanup, &schema).await;
    }

    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
