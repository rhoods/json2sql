//! Integration tests for json2sql.
//!
//! Ces tests nécessitent une instance PostgreSQL. Définir `TEST_DATABASE_URL` pour les activer :
//!
//!   TEST_DATABASE_URL=postgres://user:pass@localhost/testdb cargo test --test integration
//!
//! Sans cette variable, tous les tests sont silencieusement ignorés.
//!
//! ## Schémas de test
//!
//! Chaque test crée un schéma `j2s_test_<random>` et le supprime à la fin.
//! En cas de panic avant le `drop_schema()` final, le schéma reste dans la base.
//! Rust async ne fournit pas d'équivalent RAII pour garantir le nettoyage en cas
//! de panic (pas d'`async Drop`). Pour supprimer manuellement les schémas orphelins :
//!
//! ```text
//! DO $$ DECLARE r RECORD; BEGIN
//!   FOR r IN (SELECT schema_name FROM information_schema.schemata
//!             WHERE schema_name LIKE 'j2s_test_%')
//!   LOOP EXECUTE 'DROP SCHEMA IF EXISTS ' || quote_ident(r.schema_name) || ' CASCADE';
//!   END LOOP;
//! END $$;
//! ```
//! (à exécuter dans `psql` directement ou via `psql -f cleanup.sql`)

use std::path::PathBuf;

use uuid::Uuid;

use json2sql::{db, pass1, pass2};
use json2sql::schema::config::{apply_overrides, SchemaConfig};
use json2sql::schema::registry::{apply_flatten, apply_wide_strategy_columns};
use json2sql::schema::table_schema::WideStrategy;

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
    // UUID v7 without dashes: first 12 hex chars are the 48-bit millisecond timestamp.
    // Chars [20..32] are in the rand_b field (62 mostly-random bits after the 2-bit
    // variant marker), which avoids collisions between runs starting within the same ms.
    let id = Uuid::now_v7().to_string().replace('-', "");
    format!("j2s_test_{}", &id[20..32])
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
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
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
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
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
    let p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();
    let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
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
    let p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();

    let anomaly_dir = tempfile::TempDir::new().unwrap();
    let mut p2 = pass2::runner::run(
        &path, "people", &p1.schemas, &client, &schema, 1000, false,
        None, 1, Some(anomaly_dir.path().to_path_buf()), None,
    )
    .await
    .unwrap();

    // One anomaly recorded in memory
    assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

    // Explicit finish() — verifies the file is flushed by finish(), not just by Drop.
    p2.anomaly_collector.finish().unwrap();

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
    assert_eq!(entry["expected_type"], "DOUBLE PRECISION");
    // "score": true is a JSON boolean coerced to double precision → actual_type = "boolean"
    assert_eq!(entry["actual_type"], "boolean");

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
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();
    pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await
        .unwrap();
    assert_eq!(row_count(&client, &schema, "users").await, 3);

    // Second import avec drop_existing=true → repart de zéro
    let p1b = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1b.schemas, &schema, true)
        .await
        .unwrap();
    pass2::runner::run(&path, "users", &p1b.schemas, &client, &schema, 1000, false, None, 1, None, None)
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
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, true, None, 1, None, None)
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
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

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
    let p1 = pass1::runner::run(&path, "users", 256, true, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

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
    let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
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
    let Some(client) = connect_test_db().await else { return };
    let schema = unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = fixture("anomalies_float.jsonl");
    let p1 = pass1::runner::run(&path, "items", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    // Pass 1 doit inférer DOUBLE PRECISION (Float domine, Boolean minoritaire)
    let items_schema = p1.schemas.iter().find(|s| s.name == "items").unwrap();
    let score_col = items_schema.find_by_original("score").unwrap();
    assert!(
        matches!(score_col.pg_type, json2sql::schema::type_tracker::PgType::DoublePrecision),
        "score should be inferred as DOUBLE PRECISION, got {:?}", score_col.pg_type
    );

    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "items", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    // Toutes les lignes sont insérées
    assert_eq!(row_count(&client, &schema, "items").await, 5);

    // 1 anomalie : `true` (boolean) non-coerçable en DOUBLE PRECISION
    assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

    // La ligne avec le boolean a score = NULL
    let sql = format!("SELECT COUNT(*) FROM \"{}\".\"items\" WHERE \"score\" IS NULL", schema);
    let null_count: i64 = client.query_one(&sql, &[]).await.unwrap().get(0);
    assert_eq!(null_count, 1);

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
    let p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
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
    let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
    db::ddl::create_tables(&client, &p1.schemas, &schema, false)
        .await
        .unwrap();

    // parallel = 3: tables at the same depth level are COPYed concurrently
    let p2 = pass2::runner::run(
        &path, "users", &p1.schemas, &client, &schema, 1000, false,
        Some(&db_url), 3, None, None,
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

// ---------------------------------------------------------------------------
// Test 12 — WideStrategy::Jsonb sur une table enfant
//
// Fixture : 3 produits avec un objet enfant `attrs` à clés dynamiques.
// Après apply_wide_strategy_columns(Jsonb), la table products_attrs doit
// avoir une seule colonne `data JSONB` qui contient l'objet entier.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_jsonb_strategy() {
    let Some(client) = connect_test_db().await else { return };
    let schema = unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = fixture("wide_jsonb.jsonl");
    let p1 = pass1::runner::run(&path, "products", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    // Deux tables attendues : products et products_attrs
    assert_eq!(p1.schemas.len(), 2);
    assert!(p1.schemas.iter().any(|s| s.name == "products"));
    assert!(p1.schemas.iter().any(|s| s.name == "products_attrs"));

    // Appliquer la stratégie Jsonb sur la table enfant
    let mut schemas = p1.schemas;
    apply_wide_strategy_columns(
        schemas
            .iter_mut()
            .find(|s| s.name == "products_attrs")
            .expect("products_attrs not found — naming regression in pass1?"),
        WideStrategy::Jsonb,
    );

    // La table products_attrs doit maintenant avoir exactement une colonne data JSONB
    let attrs_schema = schemas.iter().find(|s| s.name == "products_attrs").unwrap();
    let data_cols: Vec<_> = attrs_schema.data_columns().collect();
    assert_eq!(data_cols.len(), 1, "Jsonb strategy: exactly one data column expected");
    assert_eq!(data_cols[0].name, "data");
    assert!(
        matches!(data_cols[0].pg_type, json2sql::schema::type_tracker::PgType::Jsonb),
        "data column must be PgType::Jsonb"
    );
    // Dériver les noms de colonnes depuis les schémas pour ne pas coupler le test
    // aux conventions de nommage internes (parent_fk, generated id).
    let fk_col_name = attrs_schema
        .columns
        .iter()
        .find(|c| c.is_parent_fk)
        .map(|c| c.name.clone())
        .expect("products_attrs must have a parent FK column");
    // "j2s_id" est le nom de convention fixe de la colonne d'identité générée.
    // La recherche vérifie l'existence sans dériver le nom (toujours "j2s_id").
    schemas
        .iter()
        .find(|s| s.name == "products")
        .and_then(|s| s.columns.iter().find(|c| c.name == "j2s_id"))
        .expect("products must have a j2s_id column");
    let products_id_col = "j2s_id";

    db::ddl::create_tables(&client, &schemas, &schema, false).await.unwrap();

    // Vérification intermédiaire : les deux tables existent avant de démarrer Pass 2
    let tables_created: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name IN ('products', 'products_attrs')",
            &[&schema],
        )
        .await.unwrap().get("count");
    assert_eq!(tables_created, 2, "products et products_attrs doivent exister avant Pass 2");

    let p2 = pass2::runner::run(&path, "products", &schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    // 3 produits, 3 objets attrs — vérification compteur mémoire ET base
    assert_eq!(*p2.rows_per_table.get("products").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("products_attrs").unwrap(), 3);
    assert_eq!(row_count(&client, &schema, "products").await, 3);
    assert_eq!(row_count(&client, &schema, "products_attrs").await, 3);
    // La stratégie Jsonb bypasse la coercion : aucune anomalie attendue,
    // même si les clés sont hétérogènes entre les lignes (color/weight/speed/fragile…).
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    // Les trois produits en une seule jointure — JSONB ->> renvoie du TEXT.
    // Les nombres JSON (42, 100) deviennent des chaînes ("42", "100").
    // Les clés absentes retournent NULL (pas une erreur).
    let sql_all = format!(
        "SELECT p.name, \
                pa.data->>'color'  AS color, \
                pa.data->>'weight' AS weight, \
                pa.data->>'speed'  AS speed, \
                pa.data->>'size'   AS size \
         FROM \"{s}\".\"products\" p \
         JOIN \"{s}\".\"products_attrs\" pa ON pa.{fk} = p.{id} \
         ORDER BY p.id",
        s = schema,
        fk = fk_col_name,
        id = products_id_col,
    );
    let rows = client.query(&sql_all, &[]).await.unwrap();
    assert_eq!(rows.len(), 3, "3 lignes attendues depuis la jointure JSONB");

    // Widget : color='red', weight='100' (nombre JSON → string via ->>)
    let widget = rows.iter().find(|r| r.get::<_, &str>("name") == "Widget")
        .expect("Widget not found");
    assert_eq!(widget.get::<_, Option<String>>("color").as_deref(), Some("red"));
    assert_eq!(widget.get::<_, Option<String>>("weight").as_deref(), Some("100"));

    // Gadget : speed='42' (nombre JSON → string via ->>), color='blue'
    let gadget = rows.iter().find(|r| r.get::<_, &str>("name") == "Gadget")
        .expect("Gadget not found");
    assert_eq!(gadget.get::<_, Option<String>>("speed").as_deref(), Some("42"));
    assert_eq!(gadget.get::<_, Option<String>>("color").as_deref(), Some("blue"));

    // Doohickey : 'color' absent → NULL, 'size' présent → 'large'
    let doohickey = rows.iter().find(|r| r.get::<_, &str>("name") == "Doohickey")
        .expect("Doohickey not found");
    assert!(doohickey.get::<_, Option<String>>("color").is_none(),
        "clé absente dans JSONB doit retourner NULL, pas une erreur");
    assert_eq!(doohickey.get::<_, Option<String>>("size").as_deref(), Some("large"),
        "Doohickey attrs.size doit être 'large'");

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 13 — WideStrategy Flatten : colonnes enfant inlinées dans le parent
//
// Fixture : 3 produits avec un objet enfant `dims` (width, height, depth).
// Après apply_flatten("products_dims", "dims_", 1) :
//   - products_dims est supprimé des schémas
//   - products gagne les colonnes dims_width, dims_height, dims_depth
//   - dims_depth = NULL pour Doohickey (clé absente dans la fixture)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_flatten_strategy() {
    let Some(client) = connect_test_db().await else { return };
    let schema = unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = fixture("flatten_nested.jsonl");
    let p1 = pass1::runner::run(&path, "products", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    // Avant flatten : 2 tables
    assert_eq!(p1.schemas.len(), 2);
    assert!(p1.schemas.iter().any(|s| s.name == "products_dims"));

    let mut schemas = p1.schemas;
    apply_flatten(&mut schemas, "products_dims", "dims_", 1);

    // Après flatten : products_dims supprimé, il reste 1 table
    assert_eq!(schemas.len(), 1);
    assert!(schemas.iter().all(|s| s.name != "products_dims"));

    // products doit avoir les colonnes dims_width, dims_height, dims_depth
    // On vérifie le nom PG ET le original_name séparément pour détecter tout bug partiel.
    let products_schema = schemas.iter().find(|s| s.name == "products").unwrap();
    assert!(
        products_schema.columns.iter().any(|c| c.name == "dims_width"),
        "dims_width column expected in products after flatten (pg name)"
    );
    assert!(
        products_schema.find_by_original("width").is_some(),
        "column with original_name 'width' expected after flatten"
    );
    assert!(products_schema.columns.iter().any(|c| c.name == "dims_height"), "dims_height missing");
    assert!(products_schema.columns.iter().any(|c| c.name == "dims_depth"), "dims_depth missing");
    // 5 colonnes de données au total : id, name + 3 dims inlinées (width, height, depth)
    let data_col_count = products_schema.data_columns().count();
    assert_eq!(data_col_count, 5, "products doit avoir 5 colonnes de données après flatten (id, name, dims_*)");

    db::ddl::create_tables(&client, &schemas, &schema, false).await.unwrap();

    // Vérification intermédiaire : products existe, products_dims n'a pas été créé
    let products_exists: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = 'products'",
            &[&schema],
        )
        .await.unwrap().get("count");
    assert_eq!(products_exists, 1, "products doit exister après create_tables");
    let dims_absent_before: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = 'products_dims'",
            &[&schema],
        )
        .await.unwrap().get("count");
    assert_eq!(dims_absent_before, 0, "products_dims ne doit pas être créé (flatten supprime la table enfant)");

    let p2 = pass2::runner::run(&path, "products", &schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    // 3 produits insérés dans la table unique
    assert_eq!(row_count(&client, &schema, "products").await, 3);
    assert_eq!(*p2.rows_per_table.get("products").unwrap(), 3);
    // products_dims ne doit PAS apparaître — si apply_flatten a no-opé silencieusement,
    // Pass 2 aurait tenté d'écrire dans la table enfant et ce compteur serait non-nul.
    assert!(
        p2.rows_per_table.get("products_dims").is_none(),
        "products_dims must not receive any rows after flatten"
    );
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    // Valeurs inlinées : Widget width=10, height=20, depth=5
    let sql = format!(
        "SELECT dims_width, dims_height, dims_depth \
         FROM \"{}\".\"products\" WHERE name = 'Widget'",
        schema
    );
    let row = client.query_opt(&sql, &[]).await.unwrap().expect("Widget row not found");
    let w: i32 = row.get("dims_width");
    let h: i32 = row.get("dims_height");
    let d: i32 = row.get("dims_depth");
    assert_eq!(w, 10);
    assert_eq!(h, 20);
    assert_eq!(d, 5);

    // Gadget : ligne complète sans NULL (width=15, height=30, depth=8)
    let sql_gadget = format!(
        "SELECT dims_width, dims_height, dims_depth \
         FROM \"{}\".\"products\" WHERE name = 'Gadget'",
        schema
    );
    let row_g = client.query_opt(&sql_gadget, &[]).await.unwrap().expect("Gadget row not found");
    assert_eq!(row_g.get::<_, i32>("dims_width"), 15);
    assert_eq!(row_g.get::<_, i32>("dims_height"), 30);
    assert_eq!(row_g.get::<_, i32>("dims_depth"), 8);

    // Doohickey n'a pas de depth → dims_depth IS NULL
    let sql_null = format!(
        "SELECT dims_depth FROM \"{}\".\"products\" WHERE name = 'Doohickey'",
        schema
    );
    let row_null = client
        .query_opt(&sql_null, &[])
        .await
        .unwrap()
        .expect("Doohickey row not found");
    let depth_null: Option<i32> = row_null.get("dims_depth");
    assert!(depth_null.is_none(), "dims_depth should be NULL for Doohickey");

    // Vérification DB : products_dims ne doit pas exister en tant que table
    let absent: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = 'products_dims'",
            &[&schema],
        )
        .await
        .unwrap()
        .get("count");
    assert_eq!(absent, 0, "products_dims table must not be created in DB after flatten");

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 14 — Motifs null : clé absente vs null JSON vs string "null"
//
// Fixture : 4 lignes, colonne `tag` TEXT.
//   - Alice  : tag = "present"  → stocké tel quel
//   - Bob    : tag = null       → SQL NULL (null JSON explicite)
//   - Charlie: (pas de clé tag) → SQL NULL (clé absente)
//   - Diana  : tag = "null"     → stocké comme la chaîne 'null', PAS SQL NULL
//
// Les deux formes de null (explicite et absent) doivent produire IS NULL en base.
// La string JSON "null" est distincte : stockée littéralement comme 'null'.
// Note : si la colonne était INTEGER, "null" string produirait une anomalie de
// coercion (PgType::Integer ne peut pas coercer une string non-numérique).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_null_patterns() {
    let Some(client) = connect_test_db().await else { return };
    let schema = unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = fixture("null_patterns.jsonl");
    let p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    // Pass 1 doit inférer TEXT ou VarChar pour `tag` (string "present" et "null" présentes).
    // Si la colonne était inférée comme autre chose (e.g. Boolean à cause des null majoritaires),
    // les insertions suivantes produiraient des anomalies non attendues.
    // L'assertion est faite ici — avant tout autre usage de p1.schemas — pour garantir
    // qu'elle porte sur les schémas non-mutés tels qu'ils seront passés à create_tables.
    let people_schema = p1.schemas.iter().find(|s| s.name == "people").unwrap();
    let tag_col = people_schema.find_by_original("tag").unwrap();
    assert!(
        matches!(
            &tag_col.pg_type,
            json2sql::schema::type_tracker::PgType::Text
                | json2sql::schema::type_tracker::PgType::VarChar(_)
        ),
        "tag doit être inféré TEXT/VarChar (valeurs string présentes), obtenu {:?}",
        tag_col.pg_type
    );

    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    // 4 lignes insérées, aucune anomalie (tag est TEXT, toutes les valeurs sont coerçables)
    assert_eq!(*p2.rows_per_table.get("people").unwrap(), 4);
    assert_eq!(row_count(&client, &schema, "people").await, 4);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    // Alice → tag = 'present'
    let sql_present = format!(
        "SELECT tag FROM \"{}\".\"people\" WHERE name = 'Alice'",
        schema
    );
    let row = client.query_opt(&sql_present, &[]).await.unwrap().expect("Alice row not found");
    let tag: Option<String> = row.get("tag");
    assert_eq!(tag.as_deref(), Some("present"));

    // Bob (null JSON) → IS NULL
    let sql_bob = format!(
        "SELECT COUNT(*) FROM \"{}\".\"people\" WHERE name = 'Bob' AND tag IS NULL",
        schema
    );
    let bob_null: i64 = client.query_one(&sql_bob, &[]).await.unwrap().get("count");
    assert_eq!(bob_null, 1, "JSON null should produce SQL NULL");

    // Charlie (clé absente) → IS NULL
    let sql_charlie = format!(
        "SELECT COUNT(*) FROM \"{}\".\"people\" WHERE name = 'Charlie' AND tag IS NULL",
        schema
    );
    let charlie_null: i64 = client.query_one(&sql_charlie, &[]).await.unwrap().get("count");
    assert_eq!(charlie_null, 1, "absent key should produce SQL NULL");

    // Diana (string "null") → stocké comme 'null', PAS IS NULL
    let sql_diana = format!(
        "SELECT tag FROM \"{}\".\"people\" WHERE name = 'Diana'",
        schema
    );
    let row_diana = client.query_opt(&sql_diana, &[]).await.unwrap().expect("Diana row not found");
    let diana_tag: Option<String> = row_diana.get("tag");
    assert_eq!(diana_tag.as_deref(), Some("null"), "string 'null' must be stored as text, not SQL NULL");

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 15 — Override de type valide : score FLOAT → TEXT
//
// users.jsonl contient `score` comme float (9.5, 7.2, 8.8).
// Pass 1 l'infère DoublePrecision/Float.
// override_score.toml force `score = "TEXT"` pour la table "people".
// Après apply_overrides + Pass 2, la colonne score doit être de type `text`
// dans pg_catalog (format_type = 'text').
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_override_type_valid() {
    let Some(client) = connect_test_db().await else { return };
    let schema = unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = fixture("users.jsonl");
    let mut p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    // issue #6 — vérifier que Pass1 a bien inféré DoublePrecision AVANT override,
    // pour prouver que l'override a un effet réel et n'est pas un no-op.
    let pre_schema = p1.schemas.iter().find(|s| s.name == "people").unwrap();
    let pre_col = pre_schema.find_by_original("score").unwrap();
    assert!(
        matches!(&pre_col.pg_type, json2sql::schema::type_tracker::PgType::DoublePrecision),
        "Pass1 doit inférer DoublePrecision pour score avant override, obtenu {:?}", pre_col.pg_type
    );

    let config = SchemaConfig::from_file(&fixture("override_score.toml")).unwrap();
    apply_overrides(&mut p1.schemas, &config);

    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    assert_eq!(*p2.rows_per_table.get("people").unwrap(), 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0, "no anomalies expected — score values are valid text");

    let type_sql = "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) \
         FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = 'people' AND a.attname = 'score' AND a.attnum > 0";
    // issue #5 — query_opt + expect clair si la colonne est absente (renommage, sanitizer, etc.)
    let row = client.query_opt(type_sql, &[&schema]).await.unwrap()
        .expect("colonne 'score' introuvable dans pg_attribute pour la table 'people' — vérifier le naming sanitizer");
    let pg_type: String = row.get(0);
    assert_eq!(pg_type, "text", "score doit être TEXT après override, obtenu: {}", pg_type);

    drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 16 — Override invalide : score → INTEGER (floats → anomalies),
//           colonne inexistante, table fantôme (warnings silencieux, no crash)
//
// override_bad.toml contient :
//   [people]  score = "INTEGER"    ← floats 9.5, 7.2, 8.8 ne peuvent pas être INTEGER
//             nonexistent = "TEXT" ← colonne absente → warning silencieux
//   [ghost_table]  col = "TEXT"   ← table absente → warning silencieux
//
// Le pipeline ne doit pas paniquer. Les lignes avec score float doivent
// produire des anomalies de coercion (score non coerçable en INTEGER).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_override_bad() {
    let Some(client) = connect_test_db().await else { return };
    let schema = unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = fixture("users.jsonl");
    let mut p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    let config = SchemaConfig::from_file(&fixture("override_bad.toml")).unwrap();
    apply_overrides(&mut p1.schemas, &config);

    let people_schema = p1.schemas.iter().find(|s| s.name == "people").unwrap();
    let score_col = people_schema.find_by_original("score").unwrap();
    assert!(
        matches!(&score_col.pg_type, json2sql::schema::type_tracker::PgType::Integer),
        "score doit être INTEGER après override, obtenu {:?}", score_col.pg_type
    );

    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();

    // issue #7 — vérifier le type DDL réel en base AVANT Pass 2
    let type_sql = "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) \
         FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = 'people' AND a.attname = 'score' AND a.attnum > 0";
    let row = client.query_opt(type_sql, &[&schema]).await.unwrap()
        .expect("colonne 'score' introuvable dans pg_attribute — l'override DDL n'a pas été appliqué");
    let pg_type: String = row.get(0);
    assert_eq!(pg_type, "integer", "score doit être INTEGER en base après override, obtenu: {}", pg_type);

    let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    // P2/#1 — les 3 lignes arrivent en base même si score est en anomalie (pas de rejet de ligne)
    assert_eq!(*p2.rows_per_table.get("people").unwrap(), 3);
    // P2/#3 — nonexistent col + ghost_table n'ont aucun side-effect sur le row count DB réel
    assert_eq!(row_count(&client, &schema, "people").await, 3);

    // P2/#8 — exactement 3 anomalies : une par ligne sur score, les tables enfants n'en produisent pas
    assert_eq!(
        p2.anomaly_collector.total_anomalies(), 3,
        "exactement 3 anomalies attendues (score sur 3 lignes), obtenu: {}", p2.anomaly_collector.total_anomalies()
    );

    // P2/#10 — coerceur strict : score IS NULL pour les 3 lignes, pas de trunc silencieux (9.5 → 9)
    let null_count_sql = format!(
        "SELECT COUNT(*) FROM \"{}\".\"people\" WHERE score IS NULL",
        schema
    );
    let null_count: i64 = client.query_one(&null_count_sql, &[]).await.unwrap().get(0);
    assert_eq!(null_count, 3, "score doit être NULL pour les 3 lignes (coercion stricte float→INTEGER), obtenu {} NULL", null_count);

    drop_schema(&client, &schema).await;
}
