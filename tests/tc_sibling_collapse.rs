#![cfg(feature = "integration-tests")]

mod integration;

use std::collections::HashSet;

use json2sql::anomaly::reporter::AnomalyFormat;
use json2sql::pipeline::{run_pipeline, PipelineConfig};

fn base_config(url: String, schema: String) -> PipelineConfig {
    PipelineConfig {
        input: Some(integration::fixture("tc_sibling_collapse.jsonl")),
        root_table: "books".to_string(),
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

/// SiblingCollapse — 4 sibling translation objects collapsed into one table.
/// 3 books × 4 translations = 12 rows in books_translations, not 4 separate tables.
#[tokio::test]
async fn test_sibling_collapse_e2e() {
    integration::with_pg_container(|client, schema, url| async move {
        let result = run_pipeline(base_config(url, schema.clone())).await;
        assert!(result.is_ok(), "sibling_collapse pipeline failed: {result:?}");

        let books = integration::row_count(&client, &schema, "books").await;
        assert_eq!(books, 3, "expected 3 books");

        let translations = integration::row_count(&client, &schema, "books_translations").await;
        assert_eq!(translations, 12, "expected 12 translations (3 books × 4 languages)");

        // The 4 sibling tables must NOT exist — they were collapsed
        let sibling_tables: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name IN \
                 ('books_translations_en','books_translations_fr',\
                  'books_translations_de','books_translations_es')",
                &[&schema],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(sibling_tables, 0, "sibling tables must not exist after collapse");
    })
    .await;
}
