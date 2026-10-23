#![cfg(feature = "integration-tests")]

mod integration;

use std::collections::HashSet;

use json2sql::anomaly::reporter::AnomalyFormat;
use json2sql::pipeline::{run_pipeline, PipelineConfig};
use json2sql::db::ddl::add_constraints;
use json2sql::schema::table_schema::TableSchema;

fn base_config(url: String, schema: String) -> PipelineConfig {
    PipelineConfig {
        input: Some(integration::fixture("tc_nested_fk.jsonl")),
        root_table: "orders".to_string(),
        db_url: Some(url),
        pg_schema: schema,
        drop_existing: false,
        dry_run: false,
        text_threshold: 256,
        array_as_pg_array: false,
        depth_limit: None,
        wide_column_threshold: 100,
        sibling_threshold: 3,
        sibling_jaccard: 0.5,
        stable_threshold: 0.10,
        rare_threshold: 0.001,
        num_workers: 1,
        disabled_strategies: HashSet::new(),
        parallel: 1,
        anomaly_dir: None,
        limit: None,
        anomaly_format: AnomalyFormat::Json,
        anomaly_output: None,
        max_anomaly_rate: None,
        schema_config: None,
        schema_output: None,
        schema_input: None,
        schema_report: false,
        schema_report_output: None,
        skip_constraints: false,
        tune_wal: false,
    }
}

/// Nested FK — 3 orders + 6 items, FK ordering must not violate constraints.
#[tokio::test]
async fn test_nested_fk_e2e() {
    integration::with_pg_container(|client, schema, url| async move {
        let result = run_pipeline(base_config(url, schema.clone())).await;
        assert!(result.is_ok(), "nested_fk pipeline failed: {result:?}");

        let orders = integration::row_count(&client, &schema, "orders").await;
        assert_eq!(orders, 3, "expected 3 orders");

        let items = integration::row_count(&client, &schema, "orders_items").await;
        assert_eq!(items, 5, "expected 5 items (2+1+2)");
    })
    .await;
}

/// #65 — parallel flusher (4 connections) + `--tune-wal`: same rows and FK as the default path,
/// `max_wal_size` is back to its previous value and autovacuum is re-enabled on every table.
#[tokio::test]
async fn test_nested_fk_parallel_tune_wal() {
    // Keep the restore marker out of the developer's real ~/.local/state. The other test in this
    // binary never raises max_wal_size, so it is unaffected by where the marker lives.
    let state = tempfile::TempDir::new().expect("tempdir");
    std::env::set_var("XDG_STATE_HOME", state.path());
    integration::with_pg_container(|client, schema, url| async move {
        let wal_before: String = client.query_one("SHOW max_wal_size", &[]).await.expect("SHOW").get(0);
        let cfg = PipelineConfig { parallel: 4, tune_wal: true, drop_existing: true, ..base_config(url, schema.clone()) };
        let result = run_pipeline(cfg).await;
        assert!(result.is_ok(), "parallel tune_wal pipeline failed: {result:?}");

        assert_eq!(integration::row_count(&client, &schema, "orders").await, 3);
        assert_eq!(integration::row_count(&client, &schema, "orders_items").await, 5);

        // pg_reload_conf() is asynchronous: give the session a moment to see the restored value.
        let mut wal_after = String::new();
        for _ in 0..50 {
            wal_after = client.query_one("SHOW max_wal_size", &[]).await.expect("SHOW").get(0);
            if wal_after == wal_before { break; }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(wal_after, wal_before, "max_wal_size must be restored after the import");

        let autovacuum_off: i64 = client.query_one(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relkind = 'r' AND 'autovacuum_enabled=false' = ANY(c.reloptions)",
            &[&schema],
        ).await.expect("query reloptions").get(0);
        assert_eq!(autovacuum_off, 0, "autovacuum must be re-enabled on every table");

        let fks: i64 = client.query_one(
            "SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace \
             WHERE n.nspname = $1 AND c.contype = 'f'",
            &[&schema],
        ).await.expect("query FKs").get(0);
        assert!(fks > 0, "FK constraints must be applied");

        let not_validated: i64 = client.query_one(
            "SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace \
             WHERE n.nspname = $1 AND c.contype = 'f' AND NOT c.convalidated",
            &[&schema],
        ).await.expect("query convalidated").get(0);
        assert_eq!(not_validated, 0, "every FK added NOT VALID must end up validated (#68)");

        // #68: the import VACUUMs every table itself. Table stats are flushed asynchronously by the
        // closing VACUUM connections: retry briefly.
        let mut never_vacuumed = -1_i64;
        for _ in 0..50 {
            never_vacuumed = client.query_one(
                "SELECT count(*) FROM pg_stat_user_tables WHERE schemaname = $1 AND last_vacuum IS NULL",
                &[&schema],
            ).await.expect("query last_vacuum").get(0);
            if never_vacuumed == 0 { break; }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert_eq!(never_vacuumed, 0, "every imported table must be vacuumed by the import");
    })
    .await;
}

/// #68 — an orphan child row: VALIDATE fails → one FK warning, PKs kept, no NOT VALID FK left behind.
#[tokio::test]
async fn test_orphan_row_fk_warning_drops_constraint() {
    integration::with_pg_container(|client, schema, url| async move {
        client.batch_execute(&format!(
            "CREATE TABLE \"{schema}\".parent (j2s_id uuid);
             CREATE TABLE \"{schema}\".child (j2s_id uuid, j2s_parent_id uuid);
             INSERT INTO \"{schema}\".parent VALUES (gen_random_uuid());
             INSERT INTO \"{schema}\".child VALUES (gen_random_uuid(), gen_random_uuid());"
        )).await.expect("setup tables");

        let parent = TableSchema::new("parent".to_string(), vec!["parent".to_string()], 0);
        let mut child = TableSchema::new("child".to_string(), vec!["parent".to_string(), "child".to_string()], 1);
        child.parent_table = Some("parent".to_string());

        let warnings = add_constraints(&url, &[parent, child], &schema, 4, None).await.expect("PKs must succeed");
        assert_eq!(warnings.len(), 1, "one FK warning: {warnings:?}");
        assert_eq!(warnings[0].table, "child");

        let count = |contype: &'static str| {
            let (client, schema) = (&client, &schema);
            async move {
                client.query_one(
                    "SELECT count(*) FROM pg_constraint c JOIN pg_namespace n ON n.oid = c.connamespace \
                     WHERE n.nspname = $1 AND c.contype = $2::text::\"char\"",
                    &[schema, &contype],
                ).await.expect("query constraints").get::<_, i64>(0)
            }
        };
        assert_eq!(count("p").await, 2, "both PKs applied");
        assert_eq!(count("f").await, 0, "failed FK must be dropped, not left NOT VALID");
    })
    .await;
}
