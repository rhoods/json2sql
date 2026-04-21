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
//! Chaque test crée un schéma `j2s_test_<random>` et le supprime à la fin.
//! En cas de panic avant le `drop_schema()` final, le schéma reste dans la base.
//! Pour supprimer manuellement les schémas orphelins :
//!
//! ```text
//! DO $$ DECLARE r RECORD; BEGIN
//!   FOR r IN (SELECT schema_name FROM information_schema.schemata
//!             WHERE schema_name LIKE 'j2s_test_%')
//!   LOOP EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(r.schema_name) || ' CASCADE';
//!   END LOOP;
//! END $$;
//! ```

use std::path::PathBuf;

use uuid::Uuid;

use json2sql::db;

pub fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

pub async fn connect_test_db() -> Option<tokio_postgres::Client> {
    let url = std::env::var("TEST_DATABASE_URL").ok()?;
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
