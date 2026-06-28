#![cfg(feature = "integration-tests")]

mod integration;

use std::collections::HashSet;

use json2sql::anomaly::reporter::AnomalyFormat;
use json2sql::pipeline::{run_pipeline, PipelineConfig};

fn base_config(url: String, schema: String) -> PipelineConfig {
    PipelineConfig {
        input: Some(integration::fixture("tc_flat_table.jsonl")),
        root_table: "products".to_string(),
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

/// Flat table — 5 scalar records, single table, no FK, no anomalies.
#[tokio::test]
async fn test_flat_table_e2e() {
    integration::with_pg_container(|client, schema, url| async move {
        let result = run_pipeline(base_config(url, schema.clone())).await;
        assert!(result.is_ok(), "flat_table pipeline failed: {result:?}");

        let count = integration::row_count(&client, &schema, "products").await;
        assert_eq!(count, 5, "tc_flat_table.jsonl must produce 5 rows in products");
    })
    .await;
}
