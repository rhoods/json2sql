#![cfg(feature = "integration-tests")]

mod integration;

use std::collections::HashSet;

use json2sql::anomaly::reporter::AnomalyFormat;
use json2sql::pipeline::{run_pipeline, PipelineConfig};

fn base_config(url: String, schema: String) -> PipelineConfig {
    PipelineConfig {
        input: Some(integration::fixture("tc_wide_table.jsonl")),
        root_table: "metrics".to_string(),
        db_url: Some(url),
        pg_schema: schema,
        drop_existing: false,
        dry_run: false,
        text_threshold: 256,
        array_as_pg_array: false,
        depth_limit: None,
        wide_column_threshold: 50, // fixture has 57 columns → triggers wide strategy
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

/// Wide table — 57 columns triggers Jsonb strategy; 3 rows inserted, columns collapsed.
#[tokio::test]
async fn test_wide_table_e2e() {
    integration::with_pg_container(|client, schema, url| async move {
        let result = run_pipeline(base_config(url, schema.clone())).await;
        assert!(result.is_ok(), "wide_table pipeline failed: {result:?}");

        let count = integration::row_count(&client, &schema, "metrics").await;
        assert_eq!(count, 3, "expected 3 rows in metrics");

        // Wide strategy collapses the 55 attr_XX columns — the table must have
        // far fewer columns than the original 57 (j2s_id + name + 1 JSONB at most).
        let col_count: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = 'metrics'",
                &[&schema],
            )
            .await
            .unwrap()
            .get(0);
        assert!(
            col_count < 57,
            "wide strategy must reduce column count below 57, got {col_count}"
        );
    })
    .await;
}
