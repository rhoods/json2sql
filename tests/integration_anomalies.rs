mod common;

use json2sql::{db, pass1, pass2};

// ---------------------------------------------------------------------------
// Test 3 — Détection d'anomalie de type
//
// Fixture : 3 enregistrements, colonne `score` majoritairement DOUBLE PRECISION
// mais une valeur `true` (BOOLEAN) → 1 anomalie, NULL inséré pour cette ligne.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_anomaly_detection() {
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("anomalies.jsonl");
    let p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    assert_eq!(common::row_count(&client, &schema, "people").await, 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

    let sql = format!(
        "SELECT COUNT(*) FROM \"{}\".\"people\" WHERE \"score\" IS NULL",
        schema
    );
    let null_count: i64 = client.query_one(&sql, &[]).await.unwrap().get(0);
    assert_eq!(null_count, 1);

    common::drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 3b — anomaly_dir: NDJSON files are created for tables with anomalies
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_anomaly_dir_streaming() {
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("anomalies.jsonl");
    let p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();

    let anomaly_dir = tempfile::TempDir::new().unwrap();
    let mut p2 = pass2::runner::run(
        &path, "people", &p1.schemas, &client, &schema, 1000, false,
        None, 1, Some(anomaly_dir.path().to_path_buf()), None,
    ).await.unwrap();

    assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

    // Explicit finish() — verifies the file is flushed by finish(), not just by Drop.
    p2.anomaly_collector.finish().unwrap();

    let written = p2.anomaly_collector.written_paths();
    assert!(written.contains_key("people"), "expected anomaly file for 'people'");

    let file_path = &written["people"];
    assert!(file_path.exists(), "NDJSON file must exist on disk");

    let content = std::fs::read_to_string(file_path).unwrap();
    let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 1, "exactly one anomaly line expected");

    let entry: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(entry["table"], "people");
    assert_eq!(entry["column"], "score");
    assert_eq!(entry["expected_type"], "DOUBLE PRECISION");
    assert_eq!(entry["actual_type"], "boolean");

    common::drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 10 — Float anomaly : boolean dans une colonne DOUBLE PRECISION
//
// Fixture : 5 enregistrements, colonne `score` majoritairement float.
// Pass 1 infère DOUBLE PRECISION (Float gagne sur Boolean dans to_pg_type).
// La valeur `true` (boolean) ne peut pas être coercée → 1 anomalie, NULL inséré.
//
// Note : les strings "NaN"/"Infinity" ne produisent PAS d'anomalie dans le
// pipeline complet car Pass 1 voit un mélange Float+Varchar et élargit la
// colonne à VARCHAR (les strings gagnent toujours sur les numériques dans
// to_pg_type). Les anomalies de coerce_float sont testées en unitaire dans
// coercer.rs.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_float_anomaly_boolean_value() {
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("anomalies_float.jsonl");
    let p1 = pass1::runner::run(&path, "items", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    let items_schema = p1.schemas.iter().find(|s| s.name == "items").unwrap();
    let score_col = items_schema.find_by_original("score").unwrap();
    assert!(
        matches!(score_col.pg_type, json2sql::schema::type_tracker::PgType::DoublePrecision),
        "score should be inferred as DOUBLE PRECISION, got {:?}", score_col.pg_type
    );

    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "items", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    assert_eq!(common::row_count(&client, &schema, "items").await, 5);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

    let sql = format!("SELECT COUNT(*) FROM \"{}\".\"items\" WHERE \"score\" IS NULL", schema);
    let null_count: i64 = client.query_one(&sql, &[]).await.unwrap().get(0);
    assert_eq!(null_count, 1);

    common::drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 11 — Null bytes dans les strings → anomalie + NULL
//
// Fixture : 3 enregistrements, colonne `bio` TEXT.
// 1 valeur contient un null byte → 1 anomalie, NULL inséré pour cette ligne.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_null_byte_anomaly() {
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("anomalies_nullbytes.jsonl");
    let p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    assert_eq!(common::row_count(&client, &schema, "people").await, 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

    let sql = format!("SELECT COUNT(*) FROM \"{}\".\"people\" WHERE \"bio\" IS NULL", schema);
    let null_count: i64 = client.query_one(&sql, &[]).await.unwrap().get(0);
    assert_eq!(null_count, 1);

    common::drop_schema(&client, &schema).await;
}
