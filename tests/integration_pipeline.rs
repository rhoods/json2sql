//! Integration tests for `pipeline::run_pipeline()`.
//!
//! These tests build `PipelineConfig` directly — no CLI parsing — to verify that
//! the public API is usable in integration tests and external callers.
//!
//! Requires `TEST_DATABASE_URL` for DB tests; dry-run tests run without a database.

mod common;

use std::collections::HashSet;
use std::path::PathBuf;

use json2sql::anomaly::reporter::AnomalyFormat;
use json2sql::pipeline::{run_pipeline, PipelineConfig};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn base_config(root: &str, input: &str) -> PipelineConfig {
    PipelineConfig {
        input: Some(fixture(input)),
        root_table: root.to_string(),
        db_url: None,
        pg_schema: "public".to_string(),
        drop_existing: false,
        dry_run: false,
        text_threshold: 256,
        array_as_pg_array: false,
        depth_limit: None,
        wide_column_threshold: 100,
        sibling_threshold: 2,
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

/// run_pipeline with dry_run=true must succeed without any database connection.
#[tokio::test]
async fn test_pipeline_dry_run_no_db() {
    let config = PipelineConfig {
        dry_run: true,
        ..base_config("users", "users.json")
    };
    let result = run_pipeline(config).await;
    assert!(result.is_ok(), "dry_run should succeed without a DB: {result:?}");
}

/// Full end-to-end import via PipelineConfig (no CLI) — proves the API is usable in tests.
#[tokio::test]
async fn test_pipeline_end_to_end_basic() {
    common::with_schema_url(|client, schema, url| async move {
        let config = PipelineConfig {
            db_url: Some(url),
            pg_schema: schema.clone(),
            ..base_config("users", "users.json")
        };
        let result = run_pipeline(config).await;
        assert!(result.is_ok(), "pipeline should succeed: {result:?}");

        let count = common::row_count(&client, &schema, "users").await;
        assert_eq!(count, 3, "users.json has 3 root objects");
    })
    .await;
}
