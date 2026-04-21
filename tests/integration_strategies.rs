mod common;

use json2sql::{db, pass1, pass2};
use json2sql::schema::registry::{apply_flatten, apply_wide_strategy_columns};
use json2sql::schema::table_schema::WideStrategy;

// ---------------------------------------------------------------------------
// Test 12 — WideStrategy::Jsonb sur une table enfant
//
// Fixture : 3 produits avec un objet enfant `attrs` à clés dynamiques.
// Après apply_wide_strategy_columns(Jsonb), la table products_attrs doit
// avoir une seule colonne `data JSONB` qui contient l'objet entier.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_jsonb_strategy() {
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("wide_jsonb.jsonl");
    let p1 = pass1::runner::run(&path, "products", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    assert_eq!(p1.schemas.len(), 2);
    assert!(p1.schemas.iter().any(|s| s.name == "products"));
    assert!(p1.schemas.iter().any(|s| s.name == "products_attrs"));

    let mut schemas = p1.schemas;
    apply_wide_strategy_columns(
        schemas.iter_mut().find(|s| s.name == "products_attrs")
            .expect("products_attrs not found — naming regression in pass1?"),
        WideStrategy::Jsonb,
    );

    let attrs_schema = schemas.iter().find(|s| s.name == "products_attrs").unwrap();
    let data_cols: Vec<_> = attrs_schema.data_columns().collect();
    assert_eq!(data_cols.len(), 1, "Jsonb strategy: exactly one data column expected");
    assert_eq!(data_cols[0].name, "data");
    assert!(
        matches!(data_cols[0].pg_type, json2sql::schema::type_tracker::PgType::Jsonb),
        "data column must be PgType::Jsonb"
    );

    let fk_col_name = attrs_schema.columns.iter()
        .find(|c| c.is_parent_fk)
        .map(|c| c.name.clone())
        .expect("products_attrs must have a parent FK column");
    schemas.iter().find(|s| s.name == "products")
        .and_then(|s| s.columns.iter().find(|c| c.name == "j2s_id"))
        .expect("products must have a j2s_id column");
    let products_id_col = "j2s_id";

    db::ddl::create_tables(&client, &schemas, &schema, false).await.unwrap();

    let tables_created: i64 = client
        .query_one(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name IN ('products', 'products_attrs')",
            &[&schema],
        ).await.unwrap().get("count");
    assert_eq!(tables_created, 2, "products et products_attrs doivent exister avant Pass 2");

    let p2 = pass2::runner::run(&path, "products", &schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    assert_eq!(*p2.rows_per_table.get("products").unwrap(), 3);
    assert_eq!(*p2.rows_per_table.get("products_attrs").unwrap(), 3);
    assert_eq!(common::row_count(&client, &schema, "products").await, 3);
    assert_eq!(common::row_count(&client, &schema, "products_attrs").await, 3);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    let sql_all = format!(
        "SELECT p.name, \
                pa.data->>'color'  AS color, \
                pa.data->>'weight' AS weight, \
                pa.data->>'speed'  AS speed, \
                pa.data->>'size'   AS size \
         FROM \"{s}\".\"products\" p \
         JOIN \"{s}\".\"products_attrs\" pa ON pa.{fk} = p.{id} \
         ORDER BY p.id",
        s = schema, fk = fk_col_name, id = products_id_col,
    );
    let rows = client.query(&sql_all, &[]).await.unwrap();
    assert_eq!(rows.len(), 3, "3 lignes attendues depuis la jointure JSONB");

    let widget = rows.iter().find(|r| r.get::<_, &str>("name") == "Widget").expect("Widget not found");
    assert_eq!(widget.get::<_, Option<String>>("color").as_deref(), Some("red"));
    assert_eq!(widget.get::<_, Option<String>>("weight").as_deref(), Some("100"));

    let gadget = rows.iter().find(|r| r.get::<_, &str>("name") == "Gadget").expect("Gadget not found");
    assert_eq!(gadget.get::<_, Option<String>>("speed").as_deref(), Some("42"));
    assert_eq!(gadget.get::<_, Option<String>>("color").as_deref(), Some("blue"));

    let doohickey = rows.iter().find(|r| r.get::<_, &str>("name") == "Doohickey").expect("Doohickey not found");
    assert!(doohickey.get::<_, Option<String>>("color").is_none(),
        "clé absente dans JSONB doit retourner NULL, pas une erreur");
    assert_eq!(doohickey.get::<_, Option<String>>("size").as_deref(), Some("large"),
        "Doohickey attrs.size doit être 'large'");

    common::drop_schema(&client, &schema).await;
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
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("flatten_nested.jsonl");
    let p1 = pass1::runner::run(&path, "products", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    assert_eq!(p1.schemas.len(), 2);
    assert!(p1.schemas.iter().any(|s| s.name == "products_dims"));

    let mut schemas = p1.schemas;
    apply_flatten(&mut schemas, "products_dims", "dims_", 1);

    assert_eq!(schemas.len(), 1);
    assert!(schemas.iter().all(|s| s.name != "products_dims"));

    let products_schema = schemas.iter().find(|s| s.name == "products").unwrap();
    assert!(products_schema.columns.iter().any(|c| c.name == "dims_width"),
        "dims_width column expected in products after flatten (pg name)");
    assert!(products_schema.find_by_original("width").is_some(),
        "column with original_name 'width' expected after flatten");
    assert!(products_schema.columns.iter().any(|c| c.name == "dims_height"), "dims_height missing");
    assert!(products_schema.columns.iter().any(|c| c.name == "dims_depth"), "dims_depth missing");
    let data_col_count = products_schema.data_columns().count();
    assert_eq!(data_col_count, 5, "products doit avoir 5 colonnes de données après flatten (id, name, dims_*)");

    db::ddl::create_tables(&client, &schemas, &schema, false).await.unwrap();

    let products_exists: i64 = client
        .query_one("SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = 'products'", &[&schema])
        .await.unwrap().get("count");
    assert_eq!(products_exists, 1, "products doit exister après create_tables");

    let dims_absent: i64 = client
        .query_one("SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = 'products_dims'", &[&schema])
        .await.unwrap().get("count");
    assert_eq!(dims_absent, 0, "products_dims ne doit pas être créé (flatten supprime la table enfant)");

    let p2 = pass2::runner::run(&path, "products", &schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    assert_eq!(common::row_count(&client, &schema, "products").await, 3);
    assert_eq!(*p2.rows_per_table.get("products").unwrap(), 3);
    assert!(p2.rows_per_table.get("products_dims").is_none(),
        "products_dims must not receive any rows after flatten");
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    let row = client.query_opt(
        &format!("SELECT dims_width, dims_height, dims_depth FROM \"{}\".\"products\" WHERE name = 'Widget'", schema),
        &[]).await.unwrap().expect("Widget row not found");
    assert_eq!(row.get::<_, i32>("dims_width"), 10);
    assert_eq!(row.get::<_, i32>("dims_height"), 20);
    assert_eq!(row.get::<_, i32>("dims_depth"), 5);

    let row_g = client.query_opt(
        &format!("SELECT dims_width, dims_height, dims_depth FROM \"{}\".\"products\" WHERE name = 'Gadget'", schema),
        &[]).await.unwrap().expect("Gadget row not found");
    assert_eq!(row_g.get::<_, i32>("dims_width"), 15);
    assert_eq!(row_g.get::<_, i32>("dims_height"), 30);
    assert_eq!(row_g.get::<_, i32>("dims_depth"), 8);

    let row_null = client.query_opt(
        &format!("SELECT dims_depth FROM \"{}\".\"products\" WHERE name = 'Doohickey'", schema),
        &[]).await.unwrap().expect("Doohickey row not found");
    let depth_null: Option<i32> = row_null.get("dims_depth");
    assert!(depth_null.is_none(), "dims_depth should be NULL for Doohickey");

    let absent: i64 = client
        .query_one("SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name = 'products_dims'", &[&schema])
        .await.unwrap().get("count");
    assert_eq!(absent, 0, "products_dims table must not be created in DB after flatten");

    common::drop_schema(&client, &schema).await;
}

// ---------------------------------------------------------------------------
// Test 14 — Motifs null : clé absente vs null JSON vs string "null"
//
// Fixture : 4 lignes, colonne `tag` TEXT.
//   - Alice  : tag = "present"  → stocké tel quel
//   - Bob    : tag = null       → SQL NULL (null JSON explicite)
//   - Charlie: (pas de clé tag) → SQL NULL (clé absente)
//   - Diana  : tag = "null"     → stocké comme la chaîne 'null', PAS SQL NULL
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_null_patterns() {
    let Some(client) = common::connect_test_db().await else { return };
    let schema = common::unique_schema();
    client.execute(&format!("CREATE SCHEMA \"{}\"", schema), &[]).await.unwrap();

    let path = common::fixture("null_patterns.jsonl");
    let p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    let people_schema = p1.schemas.iter().find(|s| s.name == "people").unwrap();
    let tag_col = people_schema.find_by_original("tag").unwrap();
    assert!(
        matches!(
            &tag_col.pg_type,
            json2sql::schema::type_tracker::PgType::Text
                | json2sql::schema::type_tracker::PgType::VarChar(_)
        ),
        "tag doit être inféré TEXT/VarChar (valeurs string présentes), obtenu {:?}", tag_col.pg_type
    );

    db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
    let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
        .await.unwrap();

    assert_eq!(*p2.rows_per_table.get("people").unwrap(), 4);
    assert_eq!(common::row_count(&client, &schema, "people").await, 4);
    assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

    let row = client.query_opt(
        &format!("SELECT tag FROM \"{}\".\"people\" WHERE name = 'Alice'", schema), &[])
        .await.unwrap().expect("Alice row not found");
    assert_eq!(row.get::<_, Option<String>>("tag").as_deref(), Some("present"));

    let bob_null: i64 = client.query_one(
        &format!("SELECT COUNT(*) FROM \"{}\".\"people\" WHERE name = 'Bob' AND tag IS NULL", schema), &[])
        .await.unwrap().get("count");
    assert_eq!(bob_null, 1, "JSON null should produce SQL NULL");

    let charlie_null: i64 = client.query_one(
        &format!("SELECT COUNT(*) FROM \"{}\".\"people\" WHERE name = 'Charlie' AND tag IS NULL", schema), &[])
        .await.unwrap().get("count");
    assert_eq!(charlie_null, 1, "absent key should produce SQL NULL");

    let row_diana = client.query_opt(
        &format!("SELECT tag FROM \"{}\".\"people\" WHERE name = 'Diana'", schema), &[])
        .await.unwrap().expect("Diana row not found");
    assert_eq!(row_diana.get::<_, Option<String>>("tag").as_deref(), Some("null"),
        "string 'null' must be stored as text, not SQL NULL");

    common::drop_schema(&client, &schema).await;
}
