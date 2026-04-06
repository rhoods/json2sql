//! Integration tests for json2sql.
//!
//! Ces tests nécessitent une instance PostgreSQL. Définir `TEST_DATABASE_URL` pour les activer :
//!
//!   TEST_DATABASE_URL=postgres://user:pass@localhost/testdb cargo test --test integration
//!
//! Sans cette variable, tous les tests sont silencieusement ignorés.

use std::path::PathBuf;

use uuid::Uuid;

use json2sql::{db, pass1, pass2};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

async fn connect_test_db() -> Option<tokio_postgres::Client> {
    let url = std::env::var("TEST_DATABASE_URL").ok()?;
    db::connection::connect(&url).await.ok()
}

fn unique_schema() -> String {
    // e.g. "j2s_test_019123abcdef" — safe PostgreSQL identifier
    let id = Uuid::now_v7().to_string().replace('-', "");
    format!("j2s_test_{}", &id[..12])
}

async fn row_count(client: &tokio_postgres::Client, schema: &str, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM \"{}\".\"{}\"", schema, table);
    let row = client.query_one(&sql, &[]).await.unwrap();
    row.get::<_, i64>(0)
}

async fn drop_schema(client: &tokio_postgres::Client, schema: &str) {
    let _ = client
        .execute(
            &format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", schema),
            &[],
        )
        .await;
}

// ---------------------------------------------------------------------------
// Test 1 — Structure imbriquée complète (format JSON array)
//
// Vérifie que les 5 tables sont créées avec le bon nombre de lignes :
//   users          : 3 lignes
//   users_address  : 3 lignes (une adresse par utilisateur)
//   users_tags     : 6 lignes (Alice:2, Bob:1, Charlie:3)
//   users_orders   : 3 lignes (Alice:2, Bob:0, Charlie:1)
//   users_orders_items : 3 lignes (ORD-001:2, ORD-002:1, ORD-003:0)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_nested_row_counts_json_array() {
    let Some(client) = connect_test_db().await else {
        return;
    };
    let schema = unique_schema();
    client
        .execute(&format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .unwrap();

    let path = fixture("users.json");
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None)
        .await
        .unwrap();

    // Vérification via le résultat Pass2 (compteur en mémoire)
    assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_address").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_tags").unwrap(), 6);
    assert_eq!(*p2.rows_per_table.get("users_orders").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_orders_items").unwrap(), 3);

    // Vérification via SELECT COUNT(*) en base
    assert_eq!(row_count(&client, &schema, "users").await, 3);
    assert_eq!(row_count(&client, &schema, "users_address").await, 3);
    assert_eq!(row_count(&client, &schema, "users_tags").await, 6);
    assert_eq!(row_count(&client, &schema, "users_orders").await, 3);
    assert_eq!(row_count(&client, &schema, "users_orders_items").await, 3);

    // Pas d'anomalie de type sur ce jeu de données propre
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 2 — Format NDJSON (même données, même résultats attendus)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_nested_row_counts_ndjson() {
    let Some(client) = connect_test_db().await else {
        return;
    };
    let schema = unique_schema();
    client
        .execute(&format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .unwrap();

    let path = fixture("users.jsonl");
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None)
        .await
        .unwrap();

    assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
    assert_eq!(row_count(&client, &schema, "users").await, 3);
    assert_eq!(row_count(&client, &schema, "users_tags").await, 6);
    assert_eq!(row_count(&client, &schema, "users_orders").await, 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 3 — Détection d'anomalie de type
//
// Fixture : 3 enregistrements, colonne `score` majoritairement DOUBLE PRECISION
// mais une valeur `true` (BOOLEAN) → 1 anomalie, NULL inséré pour cette ligne.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_anomaly_detection() {
    let Some(client) = connect_test_db().await else {
        return;
    };
    let schema = unique_schema();
    client
        .execute(&format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .unwrap();

    let path = fixture("anomalies.jsonl");
    let p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();
    let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None)
        .await
        .unwrap();

    // Les 3 lignes sont insérées (la valeur anomale devient NULL, pas une erreur)
    assert_eq!(row_count(&client, &schema, "people").await, 3);

    // Une anomalie : score = true (BOOLEAN) attendu DOUBLE PRECISION
    assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

    // La ligne avec l'anomalie a score = NULL
    let sql = format!(
        "SELECT COUNT(*) FROM \"{}\".\"people\" WHERE \"score\" IS NULL",
        schema
    );
    let null_count: i64 = client.query_one(&sql, &[]).await.unwrap().get(0);
    assert_eq!(null_count, 1);

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 3b — anomaly_dir: NDJSON files are created for tables with anomalies
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_anomaly_dir_streaming() {
    let Some(client) = connect_test_db().await else {
        return;
    };
    let schema = unique_schema();
    client
        .execute(&format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .unwrap();

    let path = fixture("anomalies.jsonl");
    let p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();

    let anomaly_dir = tempfile::TempDir::new().unwrap();
    let p2 = pass2::runner::run(
        &path, "people", &p1.schemas, &client, &schema, 1000, false,
        None, 1, Some(anomaly_dir.path().to_path_buf()),
    )
    .await
    .unwrap();

    // One anomaly recorded in memory
    assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

    // One NDJSON file created for "people" table
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
    assert_eq!(entry["expected_type"], "double precision");
    assert_eq!(entry["actual_type"], "string");

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 4 — Option drop_existing
//
// Deux imports successifs avec drop_existing=true sur le second :
// le résultat final doit contenir exactement 3 lignes (pas 6).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_drop_existing() {
    let Some(client) = connect_test_db().await else {
        return;
    };
    let schema = unique_schema();
    client
        .execute(&format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .unwrap();

    let path = fixture("users.json");

    // Premier import
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();
    pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None)
        .await
        .unwrap();
    assert_eq!(row_count(&client, &schema, "users").await, 3);

    // Second import avec drop_existing=true → repart de zéro
    let p1b = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();
    db::ddl::create_tables(&client, &p1b.schemas, &schema, true)
        .await
        .unwrap();
    pass2::runner::run(&path, "users", &p1b.schemas, &client, &schema, 1000, false, None, 1, None)
        .await
        .unwrap();

    // Toujours 3 lignes, pas 6
    assert_eq!(row_count(&client, &schema, "users").await, 3);

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 5 — Option --transaction
//
// Vérifie qu'un import réussi avec use_transaction=true committe bien
// les données (résultat identique à use_transaction=false).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_transaction_commit() {
    let Some(client) = connect_test_db().await else {
        return;
    };
    let schema = unique_schema();
    client
        .execute(&format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .unwrap();

    let path = fixture("users.json");
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, true, None, 1, None)
        .await
        .unwrap();

    // Les données doivent être committées normalement
    assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
    assert_eq!(row_count(&client, &schema, "users").await, 3);
    assert_eq!(row_count(&client, &schema, "users_tags").await, 6);
    assert_eq!(row_count(&client, &schema, "users_orders").await, 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 7 — Inférence de schéma (sans base de données)
//
// Vérifie que Pass 1 produit bien 5 tables dans le bon ordre topologique.
// ---------------------------------------------------------------------------
#[test]
fn test_schema_inference_no_db() {
    let path = fixture("users.json");
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();

    // 5 tables attendues
    assert_eq!(p1.schemas.len(), 5);

    let names: Vec<&str> = p1.schemas.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"users"));
    assert!(names.contains(&"users_address"));
    assert!(names.contains(&"users_tags"));
    assert!(names.contains(&"users_orders"));
    assert!(names.contains(&"users_orders_items"));

    // Ordre topologique : parent avant enfant
    let pos = |n: &str| names.iter().position(|&x| x == n).unwrap();
    assert!(pos("users") < pos("users_address"));
    assert!(pos("users") < pos("users_orders"));
    assert!(pos("users_orders") < pos("users_orders_items"));

    // 3 lignes au total en passe 1
    assert_eq!(p1.total_rows, 3);
}

// ---------------------------------------------------------------------------
// Test 8 — Option --array-as-pg-array
//
// Avec array_as_pg_array=true :
//   - users.json produit 4 tables (users_tags devient une colonne TEXT[])
//   - la colonne tags[] est bien présente sur users
//   - les valeurs sont correctement insérées
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_array_as_pg_array() {
    let Some(client) = connect_test_db().await else {
        return;
    };
    let schema = unique_schema();
    client
        .execute(&format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .unwrap();

    let path = fixture("users.json");
    // array_as_pg_array = true → scalar arrays become columns
    let p1 = pass1::runner::run(&path, "users", 256, true, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();

    // users_tags table must NOT exist; we expect 4 tables instead of 5
    assert_eq!(p1.schemas.len(), 4);
    let names: Vec<&str> = p1.schemas.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"users"));
    assert!(!names.contains(&"users_tags"), "users_tags should not exist with array_as_pg_array");

    // The users table should have a `tags` column of type TEXT[]
    let users_schema = p1.schemas.iter().find(|s| s.name == "users").unwrap();
    let tags_col = users_schema.find_by_original("tags").unwrap();
    assert!(
        matches!(
            &tags_col.pg_type,
            json2sql::schema::type_tracker::PgType::Array(_)
        ),
        "tags column should be PgType::Array"
    );

    // Insert data and verify
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None)
        .await
        .unwrap();

    assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
    assert_eq!(row_count(&client, &schema, "users").await, 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    // Alice has tags = ["rust","sql"] → expect array length 2
    let sql = format!(
        "SELECT array_length(\"tags\", 1) FROM \"{}\".\"users\" WHERE \"name\" = 'Alice'",
        schema
    );
    let row = client.query_one(&sql, &[]).await.unwrap();
    let len: i32 = row.get(0);
    assert_eq!(len, 2);

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 10 — NaN / Infinity → anomalie + NULL
//
// Fixture : 5 enregistrements, colonne `score` DOUBLE PRECISION.
// 3 valeurs non-finies ("NaN", "Infinity", "-Infinity") → 3 anomalies, NULL inséré.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_float_nan_infinity_anomaly() {
    let Some(client) = connect_test_db().await else { return };
    let schema = unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = fixture("anomalies_float.jsonl");
    let p1 = pass1::runner::run(&path, "items", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "items", &p1.schemas, &client, &schema, 1000, false, None, 1, None)
        .await.unwrap();

    // Toutes les lignes sont insérées
    assert_eq!(row_count(&client, &schema, "items").await, 5);

    // 3 anomalies : NaN, Infinity, -Infinity
    assert_eq!(p2.anomaly_collector.total_anomalies(), 3);

    // Les 3 lignes non-finies ont score = NULL
    let sql = format!("SELECT COUNT(*) FROM \"{}\".\"items\" WHERE \"score\" IS NULL", schema);
    let null_count: i64 = client.query_one(&sql, &[]).await.unwrap().get(0);
    assert_eq!(null_count, 3);

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 11 — Null bytes dans les strings → anomalie + NULL
//
// Fixture : 3 enregistrements, colonne `bio` TEXT.
// 1 valeur contient un null byte → 1 anomalie, NULL inséré pour cette ligne.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_null_byte_anomaly() {
    let Some(client) = connect_test_db().await else { return };
    let schema = unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = fixture("anomalies_nullbytes.jsonl");
    let p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None)
        .await.unwrap();

    // Toutes les lignes sont insérées
    assert_eq!(row_count(&client, &schema, "people").await, 3);

    // 1 anomalie : null byte dans bio
    assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

    // La ligne avec le null byte a bio = NULL
    let sql = format!("SELECT COUNT(*) FROM \"{}\".\"people\" WHERE \"bio\" IS NULL", schema);
    let null_count: i64 = client.query_one(&sql, &[]).await.unwrap().get(0);
    assert_eq!(null_count, 1);

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 9 — Parallélisme COPY
//
// Même données que test 1, mais avec parallel=3.
// Vérifie que les comptages sont identiques à l'import séquentiel.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_parallel_copy() {
    let db_url = match std::env::var("TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => return,
    };
    let client = match json2sql::db::connection::connect(&db_url).await {
        Ok(c) => c,
        Err(_) => return,
    };
    let schema = unique_schema();
    client
        .execute(&format!("CREATE SCHEMA \"{}\"", schema), &[])
        .await
        .unwrap();

    let path = fixture("users.json");
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();

    // parallel = 3: tables at the same depth level are COPYed concurrently
    let p2 = pass2::runner::run(
        &path, "users", &p1.schemas, &client, &schema, 1000, false,
        Some(&db_url), 3, None,
    )
    .await
    .unwrap();

    assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_address").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_tags").unwrap(), 6);
    assert_eq!(*p2.rows_per_table.get("users_orders").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_orders_items").unwrap(), 3);

    assert_eq!(row_count(&client, &schema, "users").await, 3);
    assert_eq!(row_count(&client, &schema, "users_tags").await, 6);
    assert_eq!(row_count(&client, &schema, "users_orders_items").await, 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    drop_schema(&client, &schema).await;
}
