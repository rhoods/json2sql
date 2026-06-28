use std::future::Future;

use futures_util::FutureExt;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

use json2sql::db::connection;

pub struct PgContainer {
    _container: ContainerAsync<Postgres>,
    pub url: String,
}

pub async fn spawn_postgres() -> PgContainer {
    let container = Postgres::default()
        .with_tag("17-alpine")
        .start()
        .await
        .expect("failed to start postgres container — is Docker running?");

    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("failed to get postgres port");

    let url = format!("postgres://postgres:postgres@127.0.0.1:{}/postgres", port);

    PgContainer {
        _container: container,
        url,
    }
}

pub fn unique_schema() -> String {
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

/// Run a test body against a fresh PostgreSQL schema started via testcontainers.
///
/// Provides the test body with: a connected client, a unique schema name, and the DB URL.
/// The schema is dropped and the container stopped when the body completes, even on panic.
pub async fn with_pg_container<F, Fut>(f: F)
where
    F: FnOnce(tokio_postgres::Client, String, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let pg = spawn_postgres().await;
    let url = pg.url.clone();

    let client = connection::connect(&url)
        .await
        .expect("failed to connect to testcontainers postgres");

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
    // pg drops here → _container drops → Docker container is stopped automatically
}

pub fn fixture(name: &str) -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}
