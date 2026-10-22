#![cfg(feature = "integration-tests")]

mod integration;

use std::collections::HashSet;

use json2sql::anomaly::reporter::AnomalyFormat;
use json2sql::pipeline::{run_pipeline, PipelineConfig};

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
    })
    .await;
}
