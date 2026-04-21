mod common;

use json2sql::{db, pass1, pass2};

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
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("users.json");
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_address").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_tags").unwrap(), 6);
    assert_eq!(*p2.rows_per_table.get("users_orders").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_orders_items").unwrap(), 3);

    assert_eq!(common::row_count(&client, &schema, "users").await, 3);
    assert_eq!(common::row_count(&client, &schema, "users_address").await, 3);
    assert_eq!(common::row_count(&client, &schema, "users_tags").await, 6);
    assert_eq!(common::row_count(&client, &schema, "users_orders").await, 3);
    assert_eq!(common::row_count(&client, &schema, "users_orders_items").await, 3);

    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    common::drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 2 — Format NDJSON (même données, même résultats attendus)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_nested_row_counts_ndjson() {
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("users.jsonl");
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
    assert_eq!(common::row_count(&client, &schema, "users").await, 3);
    assert_eq!(common::row_count(&client, &schema, "users_tags").await, 6);
    assert_eq!(common::row_count(&client, &schema, "users_orders").await, 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    common::drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 4 — Option drop_existing
//
// Deux imports successifs avec drop_existing=true sur le second :
// le résultat final doit contenir exactement 3 lignes (pas 6).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_drop_existing() {
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("users.json");

    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();
    assert_eq!(common::row_count(&client, &schema, "users").await, 3);

    // Second import avec drop_existing=true → repart de zéro
    let p1b = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1b.schemas, &schema, true).await.unwrap();
    pass2::runner::run(&path, "users", &p1b.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    assert_eq!(common::row_count(&client, &schema, "users").await, 3);

    common::drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 5 — Option --transaction
//
// Vérifie qu'un import réussi avec use_transaction=true committe bien
// les données (résultat identique à use_transaction=false).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_transaction_commit() {
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("users.json");
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, true, None, 1, None, None)
        .await.unwrap();

    assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
    assert_eq!(common::row_count(&client, &schema, "users").await, 3);
    assert_eq!(common::row_count(&client, &schema, "users_tags").await, 6);
    assert_eq!(common::row_count(&client, &schema, "users_orders").await, 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    common::drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 7 — Inférence de schéma (sans base de données)
//
// Vérifie que Pass 1 produit bien 5 tables dans le bon ordre topologique.
// ---------------------------------------------------------------------------
#[test]
fn test_schema_inference_no_db() {
    let path = common::fixture("users.json");
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    assert_eq!(p1.schemas.len(), 5);

    let names: Vec<&str> = p1.schemas.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"users"));
    assert!(names.contains(&"users_address"));
    assert!(names.contains(&"users_tags"));
    assert!(names.contains(&"users_orders"));
    assert!(names.contains(&"users_orders_items"));

    let pos = |n: &str| names.iter().position(|&x| x == n).unwrap();
    assert!(pos("users") < pos("users_address"));
    assert!(pos("users") < pos("users_orders"));
    assert!(pos("users_orders") < pos("users_orders_items"));

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
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("users.json");
    let p1 = pass1::runner::run(&path, "users", 256, true, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    assert_eq!(p1.schemas.len(), 4);
    let names: Vec<&str> = p1.schemas.iter().map(|s| s.name.as_str()).collect();
    assert!(names.contains(&"users"));
    assert!(!names.contains(&"users_tags"), "users_tags should not exist with array_as_pg_array");

    let users_schema = p1.schemas.iter().find(|s| s.name == "users").unwrap();
    let tags_col = users_schema.find_by_original("tags").unwrap();
    assert!(
        matches!(&tags_col.pg_type, json2sql::schema::type_tracker::PgType::Array(_)),
        "tags column should be PgType::Array"
    );

    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
    assert_eq!(common::row_count(&client, &schema, "users").await, 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    let sql = format!(
        "SELECT array_length(\"tags\", 1) FROM \"{}\".\"users\" WHERE \"name\" = 'Alice'",
        schema
    );
    let row = client.query_one(&sql, &[]).await.unwrap();
    let len: i32 = row.get(0);
    assert_eq!(len, 2);

    common::drop_schema(&client, &schema).await;
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
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("users.json");
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();

    let p2 = pass2::runner::run(
        &path, "users", &p1.schemas, &client, &schema, 1000, false,
        Some(&db_url), 3, None, None,
    ).await.unwrap();

    assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_address").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_tags").unwrap(), 6);
    assert_eq!(*p2.rows_per_table.get("users_orders").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("users_orders_items").unwrap(), 3);

    assert_eq!(common::row_count(&client, &schema, "users").await, 3);
    assert_eq!(common::row_count(&client, &schema, "users_tags").await, 6);
    assert_eq!(common::row_count(&client, &schema, "users_orders_items").await, 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    common::drop_schema(&client, &schema).await;
}
