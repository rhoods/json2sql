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
