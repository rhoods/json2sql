mod common;

use json2sql::{db, pass1, pass2};

// ---------------------------------------------------------------------------
// Feature B — --limit N (partial import / sample mode)
// ---------------------------------------------------------------------------
// Fixture: users.json — 3 root objects, nested tables (address, tags, orders…)
// All tests use with_schema_url so they skip cleanly when TEST_DATABASE_URL
// is not set.
// ---------------------------------------------------------------------------

// --- helper: run pass1 + DDL + pass2 with a given limit -------------------

async fn run_with_limit(
    client: &tokio_postgres::Client,
    url: &str,
    schema: &str,
    limit: Option<u64>,
) -> pass2::runner::Pass2Result {
    let path = common::fixture("users.json");
    let p1 = pass1::runner::run(&path, &common::pass1_config("users"), None).unwrap();
    db::ddl::create_tables_no_constraints(client, &p1.schemas, schema, false).await.unwrap();
    let cfg = pass2::Pass2Config {
        limit,
        ..common::pass2_config("users", schema)
    };
    pass2::runner::run(&path, &p1.schemas, client, url, &cfg, None)
        .await
        .unwrap()
}

// FR1 + NFR1: limit=N stops after exactly N root objects dispatched.
// FR2 (implicit): schema is identical between limited and full import.
#[tokio::test]
async fn test_limit_stops_after_n_objects() {
    common::with_schema_url(|client, schema, url| async move {
        let p2 = run_with_limit(&client, &url, &schema, Some(2)).await;
        assert_eq!(*p2.rows_per_table.get("users").unwrap_or(&0), 2,
            "users table must have exactly 2 rows with limit=2");
        assert_eq!(common::row_count(&client, &schema, "users").await, 2);
    }).await;
}

// FR3: limit=0 creates tables with no rows.
#[tokio::test]
async fn test_limit_zero_creates_empty_tables() {
    common::with_schema_url(|client, schema, url| async move {
        let p2 = run_with_limit(&client, &url, &schema, Some(0)).await;
        // Pass2Result rows_per_table may be empty or contain 0 — both valid.
        let users_rows = p2.rows_per_table.get("users").copied().unwrap_or(0);
        assert_eq!(users_rows, 0, "limit=0 must insert no rows");
        assert_eq!(common::row_count(&client, &schema, "users").await, 0,
            "DB must have 0 rows with limit=0");
        // Tables must still exist (DDL was applied).
        let table_exists: i64 = client.query_one(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'users'",
            &[&schema],
        ).await.unwrap().get(0);
        assert_eq!(table_exists, 1, "users table must exist even with limit=0");
    }).await;
}

// FR4: limit larger than file object count = full import.
#[tokio::test]
async fn test_limit_exceeds_file_size() {
    common::with_schema_url(|client, schema, url| async move {
        let p2 = run_with_limit(&client, &url, &schema, Some(999)).await;
        assert_eq!(*p2.rows_per_table.get("users").unwrap_or(&0), 3,
            "limit > file size must import all 3 root objects");
        assert_eq!(common::row_count(&client, &schema, "users").await, 3);
    }).await;
}

// FR2 + NFR3: no limit = full import, identical to pre-feature behavior.
#[tokio::test]
async fn test_no_limit_is_full_import() {
    common::with_schema_url(|client, schema, url| async move {
        let p2 = run_with_limit(&client, &url, &schema, None).await;
        assert_eq!(*p2.rows_per_table.get("users").unwrap_or(&0), 3,
            "no limit must import all 3 root objects");
        assert_eq!(common::row_count(&client, &schema, "users").await, 3);
        assert_eq!(common::row_count(&client, &schema, "users_address").await, 3);
        assert_eq!(common::row_count(&client, &schema, "users_tags").await, 6);
    }).await;
}

// NFR4: limit=N with parallel=4 dispatches exactly N root objects.
#[tokio::test]
async fn test_limit_with_parallel() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.json");
        let p1 = pass1::runner::run(&path, &common::pass1_config("users"), None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        let cfg = pass2::Pass2Config {
            limit: Some(2),
            parallel: 4,
            ..common::pass2_config("users", &schema)
        };
        let p2 = pass2::runner::run(&path, &p1.schemas, &client, &url, &cfg, None)
            .await.unwrap();
        // With parallel=4 and only 3 objects, limit=2 must still stop at 2 dispatched.
        assert_eq!(*p2.rows_per_table.get("users").unwrap_or(&0), 2,
            "limit=2 with parallel=4 must dispatch exactly 2 root objects");
        assert_eq!(common::row_count(&client, &schema, "users").await, 2);
    }).await;
}

// FR7: --limit composable with --disable-strategy.
#[tokio::test]
async fn test_limit_composable_with_disable_strategy() {
    use std::collections::HashSet;
    use json2sql::schema::strategies::StrategyName;

    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.json");
        let mut p1_cfg = common::pass1_config("users");
        p1_cfg.disabled_strategies = [StrategyName::Sibling].into_iter().collect::<HashSet<_>>();
        let p1 = pass1::runner::run(&path, &p1_cfg, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        let cfg = pass2::Pass2Config {
            limit: Some(2),
            ..common::pass2_config("users", &schema)
        };
        let p2 = pass2::runner::run(&path, &p1.schemas, &client, &url, &cfg, None)
            .await.unwrap();
        assert_eq!(*p2.rows_per_table.get("users").unwrap_or(&0), 2,
            "--disable-strategy sibling + --limit 2 must import exactly 2 root objects");
        assert_eq!(common::row_count(&client, &schema, "users").await, 2);
    }).await;
}

// limit=1 edge case: exactly one object dispatched.
#[tokio::test]
async fn test_limit_one() {
    common::with_schema_url(|client, schema, url| async move {
        let p2 = run_with_limit(&client, &url, &schema, Some(1)).await;
        assert_eq!(*p2.rows_per_table.get("users").unwrap_or(&0), 1,
            "limit=1 must insert exactly 1 root object");
        assert_eq!(common::row_count(&client, &schema, "users").await, 1);
    }).await;
}
