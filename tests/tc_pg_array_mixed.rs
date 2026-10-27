#![cfg(feature = "integration-tests")]

mod integration;

use std::collections::HashSet;

use json2sql::anomaly::reporter::AnomalyFormat;
use json2sql::pipeline::{run_pipeline, PipelineConfig};

fn base_config(
    url: String,
    schema: String,
    fixture: &str,
    wide_column_threshold: usize,
) -> PipelineConfig {
    PipelineConfig {
        input: Some(integration::fixture(fixture)),
        root_table: "products".to_string(),
        db_url: Some(url),
        pg_schema: schema,
        drop_existing: false,
        dry_run: false,
        text_threshold: 256,
        depth_limit: None,
        wide_column_threshold,
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
        group_columns: false,
    }
}

/// Issue #48 — a field that is scalar on some rows and an array on others: one TEXT[] column,
/// scalar occurrences stored as one-element arrays (no 42701 crash).
#[tokio::test]
async fn test_pg_array_mixed_occurrences_e2e() {
    integration::with_pg_container(|client, schema, url| async move {
        let result = run_pipeline(base_config(
            url,
            schema.clone(),
            "tc_pg_array_mixed.jsonl",
            100,
        ))
        .await;
        assert!(result.is_ok(), "pg_array_mixed pipeline failed: {result:?}");
        assert_eq!(
            integration::row_count(&client, &schema, "products").await,
            3
        );

        let sql = format!(
            "SELECT array_to_string(\"tags\", ',') FROM \"{schema}\".\"products\" ORDER BY \"id\""
        );
        let tags: Vec<String> = client
            .query(&sql, &[])
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(tags, vec!["bio", "bio,vegan", "local"]);
    })
    .await;
}

/// Issue #48 — `AutoSplit` root (wide + object child): pure and mixed pg-array fields stay as
/// columns on the main table instead of being silently dropped.
#[tokio::test]
async fn test_pg_array_autosplit_keeps_arrays_e2e() {
    integration::with_pg_container(|client, schema, url| async move {
        let result = run_pipeline(base_config(url, schema.clone(), "tc_pg_array_autosplit.jsonl", 2)).await;
        assert!(result.is_ok(), "pg_array_autosplit pipeline failed: {result:?}");

        // the root must really be AutoSplit (3 scalars > threshold 2), else this test checks nothing (#84)
        let sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'products_wide')";
        let wide_exists: bool = client.query_one(sql, &[&schema]).await.unwrap().get(0);
        assert!(wide_exists, "AutoSplit companion `products_wide` missing: the root was not split");

        let sql = format!(
            "SELECT array_to_string(\"pure\", ','), array_to_string(\"mixed\", ',') FROM \"{schema}\".\"products\" ORDER BY \"id\""
        );
        let rows: Vec<(String, String)> = client.query(&sql, &[]).await.unwrap().iter().map(|r| (r.get(0), r.get(1))).collect();
        let expected = [("a", "s"), ("b,c", "t,u"), ("d", "v")].map(|(p, m)| (p.to_string(), m.to_string()));
        assert_eq!(rows, expected);
    })
    .await;
}

/// Issue #48 — wide non-root object → identity/companion split: the pg-array field stays on the
/// identity table (the scalar EAV companion cannot hold it).
#[tokio::test]
async fn test_pg_array_pivot_split_keeps_arrays_e2e() {
    integration::with_pg_container(|client, schema, url| async move {
        let result = run_pipeline(base_config(url, schema.clone(), "tc_pg_array_pivot_split.jsonl", 2)).await;
        assert!(result.is_ok(), "pg_array_pivot_split pipeline failed: {result:?}");

        let sql = format!(
            "SELECT array_to_string(n.\"tags\", ',') FROM \"{schema}\".\"products_nutrients\" n \
             JOIN \"{schema}\".\"products\" p ON n.\"j2s_products_id\" = p.\"j2s_id\" ORDER BY p.\"id\""
        );
        let tags: Vec<String> = client.query(&sql, &[]).await.unwrap().iter().map(|r| r.get(0)).collect();
        assert_eq!(tags, vec!["x,y", "z"]);
    })
    .await;
}

/// Issue #61 — `SiblingCollapse` where a field is scalar in some siblings and a pg array in
/// another: the union column stays an array (scalars wrapped as `{x}`) instead of TEXT + NULLs.
#[tokio::test]
async fn test_pg_array_sibling_union_keeps_arrays_e2e() {
    integration::with_pg_container(|client, schema, url| async move {
        let result = run_pipeline(base_config(url, schema.clone(), "tc_pg_array_siblings.jsonl", 100)).await;
        assert!(result.is_ok(), "pg_array_siblings pipeline failed: {result:?}");

        let sql = format!(
            "SELECT array_to_string(v.\"x\", ',') FROM \"{schema}\".\"products_variants\" v \
             JOIN \"{schema}\".\"products\" p ON v.\"j2s_products_id\" = p.\"j2s_id\" ORDER BY p.\"id\", v.\"key\""
        );
        let xs: Vec<String> = client.query(&sql, &[]).await.unwrap().iter().map(|r| r.get(0)).collect();
        assert_eq!(xs, vec!["s", "t,u", "v", "w", "y", "z"]);
    })
    .await;
}

/// Issue #61 — an object occurrence of a pg-array field is not lost: Pass 1 also creates the
/// `products_tags` child table, which receives it.
#[tokio::test]
async fn test_pg_array_object_occurrence_goes_to_child_e2e() {
    integration::with_pg_container(|client, schema, url| async move {
        let result = run_pipeline(base_config(
            url,
            schema.clone(),
            "tc_pg_array_object.jsonl",
            100,
        ))
        .await;
        assert!(
            result.is_ok(),
            "pg_array_object pipeline failed: {result:?}"
        );

        let sql = format!(
            "SELECT p.\"id\", t.\"x\" FROM \"{schema}\".\"products_tags\" t \
             JOIN \"{schema}\".\"products\" p ON t.\"j2s_products_id\" = p.\"j2s_id\""
        );
        let rows: Vec<(i32, i32)> = client
            .query(&sql, &[])
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect();
        assert_eq!(rows, vec![(2, 1)]);
    })
    .await;
}
