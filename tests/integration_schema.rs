mod common;

// pass2 is used by all async tests in this file; pass1-only test_schema_inference_no_db
// does not use it but sharing the import avoids per-test redundancy.
use json2sql::{db, pass1, pass2};

#[tokio::test]
async fn test_nested_row_counts_json_array() {
    common::with_schema(|client, schema| async move {
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
    }).await;
}

#[tokio::test]
async fn test_nested_row_counts_ndjson() {
    common::with_schema(|client, schema| async move {
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
    }).await;
}

// ---------------------------------------------------------------------------
// Deux imports successifs avec drop_existing=true sur le second :
// le résultat final doit contenir exactement 3 lignes (pas 6).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_drop_existing() {
    common::with_schema(|client, schema| async move {
        let path = common::fixture("users.json");

        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
        pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
            .await.unwrap();
        assert_eq!(common::row_count(&client, &schema, "users").await, 3);

        let p1b = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables(&client, &p1b.schemas, &schema, true).await.unwrap();
        pass2::runner::run(&path, "users", &p1b.schemas, &client, &schema, 1000, false, None, 1, None, None)
            .await.unwrap();

        assert_eq!(common::row_count(&client, &schema, "users").await, 3);
    }).await;
}

// ---------------------------------------------------------------------------
// Vérifie qu'un import réussi avec use_transaction=true committe bien les données.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_transaction_commit() {
    common::with_schema(|client, schema| async move {
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
    }).await;
}

// ---------------------------------------------------------------------------
// Vérifie que Pass 1 produit bien 5 tables dans le bon ordre topologique.
// Test sans base de données — pas besoin de with_schema.
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
// Pass 1 parallèle doit produire le même schéma que séquentiel.
// ---------------------------------------------------------------------------
#[test]
fn test_schema_inference_parallel_parity() {
    let path = common::fixture("users.json");

    let seq = pass1::runner::run(
        &path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None,
    ).unwrap();

    let par = pass1::runner::run_parallel(
        &path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None, 2,
    ).unwrap();

    assert_eq!(seq.total_rows, par.total_rows, "row count must match");
    assert_eq!(seq.schemas.len(), par.schemas.len(), "table count must match");

    for s in &seq.schemas {
        let p = par.schemas.iter().find(|ps| ps.name == s.name)
            .unwrap_or_else(|| panic!("table {} missing from parallel result", s.name));
        assert_eq!(s.columns.len(), p.columns.len(),
            "column count mismatch for table {}", s.name);
        for col in &s.columns {
            let pc = p.columns.iter().find(|c| c.name == col.name)
                .unwrap_or_else(|| panic!("column {}.{} missing from parallel result", s.name, col.name));
            assert_eq!(col.pg_type, pc.pg_type,
                "pg_type mismatch for {}.{}", s.name, col.name);
            assert_eq!(col.not_null, pc.not_null,
                "not_null mismatch for {}.{}", s.name, col.name);
        }
    }
}

// ---------------------------------------------------------------------------
// Avec array_as_pg_array=true : users_tags devient une colonne TEXT[]
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_array_as_pg_array() {
    common::with_schema(|client, schema| async move {
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
    }).await;
}

// ---------------------------------------------------------------------------
// parallel=3 : tables au même niveau de profondeur COPYées en concurrence.
// Nécessite l'URL brute pour ouvrir des connexions supplémentaires.
//
// Note CI : ce test ouvre 3 connexions PG simultanées (pool interne pass2).
// Sur une instance avec max_connections <= 5, il peut interférer avec les
// autres crates de test lancées en parallèle par cargo test.
// Exécution isolée si nécessaire :
//   cargo test --test integration_schema test_parallel_copy
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_parallel_copy() {
    common::with_schema_url(|client, schema, db_url| async move {
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
    }).await;
}
