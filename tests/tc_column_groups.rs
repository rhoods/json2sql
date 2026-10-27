#![cfg(feature = "integration-tests")]
#![allow(clippy::disallowed_methods)]
//! #78, T3b: an import with `column_groups` writes the group tables from the object of their table.
//!
//! The same file is imported twice, with and without the groups, and the values are compared through the join.

mod integration;

use std::collections::HashSet;
use std::path::Path;

use json2sql::anomaly::reporter::AnomalyFormat;
use json2sql::pipeline::{run_pipeline, PipelineConfig};
use serde_json::Value;

const GROUPS: &str = r#"
[items]
zulu_a = "INTEGER"

[items_meta]
strategy = "flatten"
prefix = "m_"

[items.column_groups]
kiwi = ["kiwi_a", "kiwi_b", "kiwi_c", "kiwi_d", "kiwi_e", "kiwi_f"]
zulu = ["zulu_a", "zulu_b", "zulu_c", "zulu_d", "zulu_e", "zulu_f"]
origin = ["m_origin", "m_batch", "m_region"]

[items_detail.column_groups]
dg = ["d1", "d2", "d3"]

[items_reviews.column_groups]
rg = ["r1", "r2", "r3"]
"#;
const PLAIN: &str = "[items]\nzulu_a = \"INTEGER\"\n\n[items_meta]\nstrategy = \"flatten\"\nprefix = \"m_\"\n";

fn config(url: &str, schema: &str, toml: &str, anomaly_dir: Option<&Path>) -> (PipelineConfig, tempfile::NamedTempFile) {
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), toml).unwrap();
    let cfg = PipelineConfig {
        input: Some(integration::fixture("tc_column_groups.jsonl")),
        root_table: "items".to_string(),
        db_url: Some(url.to_string()),
        pg_schema: schema.to_string(),
        drop_existing: false,
        dry_run: false,
        text_threshold: 256,
        depth_limit: None,
        wide_column_threshold: 100,
        sibling_threshold: 3,
        sibling_jaccard: 0.5,
        stable_threshold: 0.10,
        rare_threshold: 0.001,
        num_workers: 1,
        disabled_strategies: HashSet::new(),
        parallel: 1,
        anomaly_dir: anomaly_dir.map(Path::to_path_buf),
        limit: None,
        anomaly_format: AnomalyFormat::Json,
        anomaly_output: None,
        max_anomaly_rate: None,
        schema_config: Some(file.path().to_path_buf()),
        schema_output: None,
        schema_input: None,
        schema_report: false,
        schema_report_output: None,
        skip_constraints: false,
        tune_wal: false,
        group_columns: false,
    };
    (cfg, file)
}

/// The rows of the fixture.
fn fixture_rows() -> Vec<Value> {
    let text = std::fs::read_to_string(integration::fixture("tc_column_groups.jsonl")).unwrap();
    text.lines().map(|l| serde_json::from_str(l).unwrap()).collect()
}

fn present(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
        _ => true,
    }
}

/// Rows of `rows` where at least one of `keys` (in `object` when given) has a value.
fn with_a_value(rows: &[Value], object: Option<&str>, keys: &[&str]) -> i64 {
    let n = rows
        .iter()
        .filter(|row| {
            let holder = object.map_or(Some(*row), |o| row.get(o));
            holder.is_some_and(|h| keys.iter().any(|k| h.get(k).is_some_and(present)))
        })
        .count();
    i64::try_from(n).unwrap()
}

/// Elements of the arrays `array` that have a value in one of `keys`.
fn elements_with_a_value(rows: &[Value], array: &str, keys: &[&str]) -> i64 {
    let n = rows
        .iter()
        .filter_map(|row| row.get(array)?.as_array())
        .flatten()
        .filter(|element| keys.iter().any(|k| element.get(k).is_some_and(present)))
        .count();
    i64::try_from(n).unwrap()
}

async fn count(client: &tokio_postgres::Client, sql: &str) -> i64 {
    client.query_one(sql, &[]).await.unwrap().get::<_, i64>(0)
}

/// How many of `columns` the table still has.
async fn columns_left(client: &tokio_postgres::Client, schema: &str, table: &str, columns: &[&str]) -> i64 {
    let list = columns.iter().map(|c| format!("'{c}'")).collect::<Vec<_>>().join(", ");
    count(client, &format!("SELECT count(*) FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}' AND column_name IN ({list})")).await
}

/// Number of rows in which `columns` of `table` differ between the grouped import and the plain one.
async fn rest_differences(client: &tokio_postgres::Client, (grouped, plain): (&str, &str), table: &str, columns: &[&str]) -> i64 {
    let cells = columns.iter().map(|c| format!("\"{c}\"::text")).collect::<Vec<_>>().join(", ");
    let select = |s: &str| format!("SELECT {cells} FROM \"{s}\".\"{table}\"");
    let (a, b) = (select(grouped), select(plain));
    count(client, &format!("SELECT count(*) FROM (({a} EXCEPT {b}) UNION ALL ({b} EXCEPT {a})) d")).await
}

/// Number of rows in which the grouped import, joined back, differs from the plain one (empty and NULL are equal:
/// a group row only exists when it has a value).
async fn differences(
    client: &tokio_postgres::Client,
    (grouped, plain): (&str, &str),
    (parent, group, key): (&str, &str, &str),
    columns: &[&str],
) -> i64 {
    let cells = |alias: &str| columns.iter().map(|c| format!("NULLIF({alias}.\"{c}\"::text, '')")).collect::<Vec<_>>().join(", ");
    let (joined, flat) = (
        format!("SELECT p.\"{key}\", {} FROM \"{grouped}\".\"{parent}\" p LEFT JOIN \"{grouped}\".\"{group}\" g ON g.j2s_id = p.j2s_id", cells("g")),
        format!("SELECT p.\"{key}\", {} FROM \"{plain}\".\"{parent}\" p", cells("p")),
    );
    count(
        client,
        &format!("SELECT count(*) FROM ((({joined}) EXCEPT ({flat})) UNION ALL (({flat}) EXCEPT ({joined}))) d"),
    )
    .await
}

#[tokio::test]
async fn test_column_groups_are_written_from_the_object_of_their_table() {
    integration::with_pg_container(|client, schema, url| async move {
        let plain = format!("{schema}_plain");
        client.execute(&format!("CREATE SCHEMA \"{plain}\""), &[]).await.unwrap();
        let anomalies = tempfile::tempdir().unwrap();
        let (grouped_cfg, _keep) = config(&url, &schema, GROUPS, Some(anomalies.path()));
        run_pipeline(grouped_cfg).await.expect("import with column groups");
        let (plain_cfg, _keep2) = config(&url, &plain, PLAIN, None);
        run_pipeline(plain_cfg).await.expect("import without column groups");

        let rows = fixture_rows();
        let n = i64::try_from(rows.len()).unwrap();
        let kiwi = ["kiwi_a", "kiwi_b", "kiwi_c", "kiwi_d", "kiwi_e", "kiwi_f"];
        let zulu = ["zulu_a", "zulu_b", "zulu_c", "zulu_d", "zulu_e", "zulu_f"];
        assert_eq!(integration::row_count(&client, &schema, "items").await, n);
        assert_eq!(integration::row_count(&client, &schema, "items_kiwi").await, with_a_value(&rows, None, &kiwi));
        assert_eq!(integration::row_count(&client, &schema, "items_zulu").await, with_a_value(&rows, None, &zulu));
        let meta = ["origin", "batch", "region"];
        assert_eq!(integration::row_count(&client, &schema, "items_origin").await, with_a_value(&rows, Some("meta"), &meta));
        let detail = with_a_value(&rows, Some("detail"), &["d1", "d2", "d3"]);
        assert_eq!(integration::row_count(&client, &schema, "items_detail_dg").await, detail);
        let (reviews, reviews_with_group) = (
            elements_with_a_value(&rows, "reviews", &["rid"]),
            elements_with_a_value(&rows, "reviews", &["r1", "r2", "r3"]),
        );
        assert_eq!(integration::row_count(&client, &schema, "items_reviews").await, reviews);
        assert_eq!(integration::row_count(&client, &schema, "items_reviews_rg").await, reviews_with_group);
        assert!(detail > 0 && detail < n && with_a_value(&rows, None, &kiwi) < n, "the fixture has rows without a group");
        let flattened = with_a_value(&rows, Some("meta"), &meta);
        assert!(flattened > 0 && flattened < n, "the fixture has rows without the flattened object");
        assert!(reviews_with_group > 0 && reviews_with_group < reviews, "the fixture has elements without a group");

        // the moved columns left their table
        let moved = kiwi.iter().chain(&zulu).chain(&["m_origin", "m_batch", "m_region"]).copied().collect::<Vec<_>>();
        assert_eq!(columns_left(&client, &schema, "items", &moved).await, 0);
        assert_eq!(columns_left(&client, &plain, "items", &["m_origin", "m_batch", "m_region"]).await, 3, "the plain import flattens too");
        assert_eq!(columns_left(&client, &schema, "items_detail", &["d1", "d2", "d3"]).await, 0);
        assert_eq!(columns_left(&client, &schema, "items_reviews", &["r1", "r2", "r3"]).await, 0);

        // 1:1: the group row has the id of its row, twice; a primary key and a foreign key hold the tables together
        let groups = [
            ("items_kiwi", "j2s_items_id"),
            ("items_zulu", "j2s_items_id"),
            ("items_origin", "j2s_items_id"),
            ("items_detail_dg", "j2s_items_detail_id"),
            ("items_reviews_rg", "j2s_items_reviews_id"),
        ];
        for (group, fk) in groups {
            let apart = count(&client, &format!("SELECT count(*) FROM \"{schema}\".\"{group}\" WHERE j2s_id IS DISTINCT FROM \"{fk}\"")).await;
            assert_eq!(apart, 0, "{group}");
            let constraints = count(&client, &format!("SELECT count(*) FROM pg_constraint c JOIN pg_class t ON t.oid = c.conrelid JOIN pg_namespace n ON n.oid = t.relnamespace WHERE n.nspname = '{schema}' AND t.relname = '{group}' AND c.contype IN ('p', 'f')")).await;
            assert_eq!(constraints, 2, "{group}: primary key and foreign key");
        }

        // same values as the import without groups
        let pair = (schema.as_str(), plain.as_str());
        assert_eq!(differences(&client, pair, ("items", "items_kiwi", "id"), &kiwi).await, 0);
        assert_eq!(differences(&client, pair, ("items", "items_zulu", "id"), &zulu).await, 0);
        assert_eq!(differences(&client, pair, ("items", "items_origin", "id"), &["m_origin", "m_batch", "m_region"]).await, 0);
        assert_eq!(differences(&client, pair, ("items_detail", "items_detail_dg", "d4"), &["d1", "d2", "d3"]).await, 0);
        assert_eq!(differences(&client, pair, ("items_reviews", "items_reviews_rg", "rid"), &["r1", "r2", "r3"]).await, 0);
        // what stayed in the tables is untouched
        let kept = ["id", "name", "quartz", "bamboo", "violin", "ember", "glacier", "harbor", "lantern", "meadow", "oyster", "pebble"];
        assert_eq!(rest_differences(&client, pair, "items", &kept).await, 0);
        assert_eq!(rest_differences(&client, pair, "items_detail", &["d4", "d5", "d6", "d7", "d8"]).await, 0);
        assert_eq!(rest_differences(&client, pair, "items_reviews", &["rid", "r4", "r5"]).await, 0);

        // the other child tables still hang from the original table
        let joined = count(&client, &format!("SELECT count(*) FROM \"{schema}\".items_reviews m JOIN \"{schema}\".items i ON i.j2s_id = m.j2s_items_id")).await;
        assert_eq!(joined, integration::row_count(&client, &schema, "items_reviews").await);

        // an anomaly in a moved column is reported for the group table
        let file = anomalies.path().join("items_zulu_anomalies.ndjson");
        let lines = std::fs::read_to_string(&file).unwrap_or_default().lines().count();
        assert_eq!(lines, 3, "rows 11, 22 and 33 have text in zulu_a: {}", file.display());
        assert_eq!(std::fs::read_dir(anomalies.path()).unwrap().count(), 1, "no other table has anomalies");
    })
    .await;
}

/// #91: `--group-columns` makes the groups the names confirm and Pass 2 fills them like groups of a config.
#[tokio::test]
async fn test_group_columns_writes_the_groups_its_names_find() {
    integration::with_pg_container(|client, schema, url| async move {
        let plain = format!("{schema}_plain");
        client.execute(&format!("CREATE SCHEMA \"{plain}\""), &[]).await.unwrap();
        let (grouped_cfg, _keep) = config(&url, &schema, PLAIN, None);
        run_pipeline(PipelineConfig { group_columns: true, ..grouped_cfg }).await.expect("import with --group-columns");
        let (plain_cfg, _keep2) = config(&url, &plain, PLAIN, None);
        run_pipeline(plain_cfg).await.expect("import without groups");

        let rows = fixture_rows();
        let kiwi = ["kiwi_a", "kiwi_b", "kiwi_c", "kiwi_d", "kiwi_e", "kiwi_f"];
        let zulu = ["zulu_a", "zulu_b", "zulu_c", "zulu_d", "zulu_e", "zulu_f"];
        assert_eq!(integration::row_count(&client, &schema, "items_kiwi").await, with_a_value(&rows, None, &kiwi));
        assert_eq!(integration::row_count(&client, &schema, "items_zulu").await, with_a_value(&rows, None, &zulu));
        assert_eq!(columns_left(&client, &schema, "items", &kiwi).await, 0);
        let pair = (schema.as_str(), plain.as_str());
        assert_eq!(differences(&client, pair, ("items", "items_kiwi", "id"), &kiwi).await, 0);
        assert_eq!(differences(&client, pair, ("items", "items_zulu", "id"), &zulu).await, 0);
        assert_eq!(all_rows(&client, &plain).await + with_a_value(&rows, None, &kiwi) + with_a_value(&rows, None, &zulu),
            all_rows(&client, &schema).await, "only the group rows are added");
    })
    .await;
}

/// Rows of all the tables of `schema`.
async fn all_rows(client: &tokio_postgres::Client, schema: &str) -> i64 {
    let tables = client
        .query("SELECT table_name FROM information_schema.tables WHERE table_schema = $1", &[&schema])
        .await
        .unwrap();
    let mut total = 0;
    for table in &tables {
        total += integration::row_count(client, schema, table.get::<_, &str>(0)).await;
    }
    total
}

/// The rate behind `--max-anomaly-rate` is the anomalies over the rows written in all the tables, so the rows of the
/// groups are part of the rows: the 3 anomalies of the fixture are a smaller share of the grouped import.
#[tokio::test]
async fn test_group_rows_count_in_the_anomaly_rate() {
    integration::with_pg_container(|client, schema, url| async move {
        let [plain, grouped, plain_capped, grouped_capped] =
            ["plain", "grouped", "plain_capped", "grouped_capped"].map(|s| format!("{schema}_{s}"));
        for name in [&plain, &grouped, &plain_capped, &grouped_capped] {
            client.execute(&format!("CREATE SCHEMA \"{name}\""), &[]).await.unwrap();
        }
        let (cfg, _keep) = config(&url, &plain, PLAIN, None);
        run_pipeline(cfg).await.expect("import without column groups");
        let (cfg, _keep2) = config(&url, &grouped, GROUPS, None);
        run_pipeline(cfg).await.expect("import with column groups");
        let (without, with) = (all_rows(&client, &plain).await, all_rows(&client, &grouped).await);
        assert!(with > without);

        // 3 / ((without + with) / 2): above the rate with the group rows, below the rate without them
        let cap = 6.0 / f64::from(i32::try_from(without + with).unwrap());
        let (mut cfg, _keep3) = config(&url, &plain_capped, PLAIN, None);
        cfg.max_anomaly_rate = Some(cap);
        let err = run_pipeline(cfg).await.expect_err("3 anomalies are over the cap without the group rows");
        assert!(err.to_string().contains("exceeds threshold"), "{err}");
        let (mut cfg, _keep4) = config(&url, &grouped_capped, GROUPS, None);
        cfg.max_anomaly_rate = Some(cap);
        run_pipeline(cfg).await.expect("the same 3 anomalies are under the cap once the group rows are counted");
    })
    .await;
}
