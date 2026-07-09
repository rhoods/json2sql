#![allow(clippy::disallowed_methods)]
mod common;

use json2sql::{db, pass1, pass2};
use json2sql::schema::registry::RegistryConfig;
use json2sql::schema::wide_strategies::{apply_flatten, apply_jsonb_flatten, apply_normalize_dynamic_keys, apply_structured_pivot_columns, apply_wide_strategy_columns};
use json2sql::schema::table_schema::{SiblingSchema, SuffixColumn, SuffixSchema, InferredStrategy};
use json2sql::schema::type_tracker::PgType;

// ---------------------------------------------------------------------------
// InferredStrategy::Pivot (EAV) sur une table enfant.
// Fixture : 3 produits avec un objet enfant `nutrients` à clés dynamiques
// homogènes (entiers). Après apply_wide_strategy_columns(Pivot) :
//   - products_nutrients a exactement 2 colonnes de données : key TEXT, value <int>
//   - Widget  → 4 paires EAV (calories, fat, protein, carbs)
//   - Gadget  → 4 paires EAV
//   - Doohickey → 3 paires (pas de carbs)
//   Total : 11 lignes EAV
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_pivot_strategy() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("pivot_eav.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("products"), None).unwrap();

        assert_eq!(p1.schemas.len(), 2);
        assert!(p1.schemas.iter().any(|s| s.name == "products"));
        assert!(p1.schemas.iter().any(|s| s.name == "products_nutrients"));

        let mut schemas = p1.schemas;
        apply_wide_strategy_columns(
            schemas.iter_mut().find(|s| s.name == "products_nutrients")
                .expect("products_nutrients not found"),
            InferredStrategy::Pivot,
        );

        let nutrients_schema = schemas.iter().find(|s| s.name == "products_nutrients").unwrap();
        let data_cols: Vec<_> = nutrients_schema.data_columns().collect();
        assert_eq!(data_cols.len(), 2, "Pivot: exactement 2 colonnes de données (key, value)");
        assert!(data_cols.iter().any(|c| c.name == "key"), "colonne 'key' absente");
        assert!(data_cols.iter().any(|c| c.name == "value"), "colonne 'value' absente");

        let fk_col_name = nutrients_schema.columns.iter()
            .find(|c| c.is_parent_fk)
            .map(|c| c.name.clone())
            .expect("products_nutrients must have a parent FK column");

        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("products", &schema), None)
            .await.unwrap();

        // Widget:4 + Gadget:4 + Doohickey:3 = 11 paires EAV
        assert_eq!(*p2.rows_per_table.get("products").unwrap(), 3);
        assert_eq!(*p2.rows_per_table.get("products_nutrients").unwrap(), 11);
        assert_eq!(common::row_count(&client, &schema, "products").await, 3);
        assert_eq!(common::row_count(&client, &schema, "products_nutrients").await, 11);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

        // Widget a bien 4 lignes EAV
        let widget_count: i64 = client.query_one(
            &format!(
                "SELECT COUNT(*) FROM \"{s}\".\"products_nutrients\" n \
                 JOIN \"{s}\".\"products\" p ON n.{fk} = p.j2s_id \
                 WHERE p.name = 'Widget'",
                s = schema, fk = fk_col_name,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(widget_count, 4);

        // Gadget → protein = 15
        let gadget_protein: i64 = client.query_one(
            &format!(
                "SELECT CAST(n.value AS bigint) FROM \"{s}\".\"products_nutrients\" n \
                 JOIN \"{s}\".\"products\" p ON n.{fk} = p.j2s_id \
                 WHERE p.name = 'Gadget' AND n.key = 'protein'",
                s = schema, fk = fk_col_name,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(gadget_protein, 15);

        // Doohickey : pas de ligne 'carbs'
        let doohickey_carbs: i64 = client.query_one(
            &format!(
                "SELECT COUNT(*) FROM \"{s}\".\"products_nutrients\" n \
                 JOIN \"{s}\".\"products\" p ON n.{fk} = p.j2s_id \
                 WHERE p.name = 'Doohickey' AND n.key = 'carbs'",
                s = schema, fk = fk_col_name,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(doohickey_carbs, 0, "Doohickey ne doit pas avoir de ligne 'carbs'");
    }).await;
}

// ---------------------------------------------------------------------------
// InferredStrategy::Jsonb sur une table enfant.
// Fixture : 3 produits avec un objet enfant `attrs` à clés dynamiques.
// Après apply_wide_strategy_columns(Jsonb), products_attrs a une seule
// colonne `data JSONB` contenant l'objet entier.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_jsonb_strategy() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("wide_jsonb.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("products"), None).unwrap();

        assert_eq!(p1.schemas.len(), 2);
        assert!(p1.schemas.iter().any(|s| s.name == "products"));
        assert!(p1.schemas.iter().any(|s| s.name == "products_attrs"));

        let mut schemas = p1.schemas;
        apply_wide_strategy_columns(
            schemas.iter_mut().find(|s| s.name == "products_attrs")
                .expect("products_attrs not found — naming regression in pass1?"),
            InferredStrategy::Jsonb,
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

        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let tables_created: i64 = client
            .query_one(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name IN ('products', 'products_attrs')",
                &[&schema],
            ).await.unwrap().get("count");
        assert_eq!(tables_created, 2);

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("products", &schema), None)
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
        assert_eq!(rows.len(), 3);

        let widget = rows.iter().find(|r| r.get::<_, &str>("name") == "Widget").expect("Widget not found");
        assert_eq!(widget.get::<_, Option<String>>("color").as_deref(), Some("red"));
        assert_eq!(widget.get::<_, Option<String>>("weight").as_deref(), Some("100"));

        let gadget = rows.iter().find(|r| r.get::<_, &str>("name") == "Gadget").expect("Gadget not found");
        assert_eq!(gadget.get::<_, Option<String>>("speed").as_deref(), Some("42"));
        assert_eq!(gadget.get::<_, Option<String>>("color").as_deref(), Some("blue"));

        let doohickey = rows.iter().find(|r| r.get::<_, &str>("name") == "Doohickey").expect("Doohickey not found");
        assert!(doohickey.get::<_, Option<String>>("color").is_none(),
            "clé absente dans JSONB doit retourner NULL");
        assert_eq!(doohickey.get::<_, Option<String>>("size").as_deref(), Some("large"));
    }).await;
}

// ---------------------------------------------------------------------------
// InferredStrategy Flatten : colonnes enfant inlinées dans le parent.
// Fixture : 3 produits avec un objet enfant `dims` (width, height, depth).
// Après apply_flatten("products_dims", "dims_", 1) :
//   - products_dims est supprimé des schémas
//   - products gagne les colonnes dims_width, dims_height, dims_depth
//   - dims_depth = NULL pour Doohickey (clé absente dans la fixture)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_flatten_strategy() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("flatten_nested.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("products"), None).unwrap();

        assert_eq!(p1.schemas.len(), 2);
        assert!(p1.schemas.iter().any(|s| s.name == "products_dims"));

        let mut schemas = p1.schemas;
        apply_flatten(&mut schemas, "products_dims", "dims_", 1).unwrap();

        assert_eq!(schemas.len(), 1);
        assert!(schemas.iter().all(|s| s.name != "products_dims"));

        let products_schema = schemas.iter().find(|s| s.name == "products").unwrap();
        assert!(products_schema.columns.iter().any(|c| c.name == "dims_width"),
            "dims_width column expected after flatten");
        assert!(products_schema.find_by_original("width").is_some(),
            "column with original_name 'width' expected after flatten");
        assert!(products_schema.columns.iter().any(|c| c.name == "dims_height"));
        assert!(products_schema.columns.iter().any(|c| c.name == "dims_depth"));
        assert_eq!(products_schema.data_columns().count(), 5,
            "products doit avoir 5 colonnes de données après flatten (id, name, dims_*)");

        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let dims_absent: i64 = client
            .query_one("SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name = 'products_dims'", &[&schema])
            .await.unwrap().get("count");
        assert_eq!(dims_absent, 0, "products_dims ne doit pas être créé");

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("products", &schema), None)
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
        assert!(row_null.get::<_, Option<i32>>("dims_depth").is_none(),
            "dims_depth should be NULL for Doohickey");
    }).await;
}

// ---------------------------------------------------------------------------
// Motifs null : clé absente vs null JSON vs string "null".
// Fixture : 4 lignes, colonne `tag` TEXT.
//   - Alice  : tag = "present"  → stocké tel quel
//   - Bob    : tag = null       → SQL NULL (null JSON explicite)
//   - Charlie: (pas de clé tag) → SQL NULL (clé absente)
//   - Diana  : tag = "null"     → stocké comme la chaîne 'null', PAS SQL NULL
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_null_patterns() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("null_patterns.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("people"), None).unwrap();

        let people_schema = p1.schemas.iter().find(|s| s.name == "people").unwrap();
        let tag_col = people_schema.find_by_original("tag").unwrap();
        assert!(
            matches!(
                &tag_col.pg_type,
                json2sql::schema::type_tracker::PgType::Text
                    | json2sql::schema::type_tracker::PgType::VarChar(_)
            ),
            "tag doit être inféré TEXT/VarChar, obtenu {:?}", tag_col.pg_type
        );

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None).await.unwrap();
        let p2 = pass2::runner::run(&path, &p1.schemas, &client, &url, &common::pass2_config("people", &schema), None)
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
    }).await;
}

// ---------------------------------------------------------------------------
// InferredStrategy::StructuredPivot sur une table enfant.
// Fixture : 3 produits avec `nutrients` contenant des clés à suffixes communs
// (_100g, _serving). Après apply_structured_pivot_columns :
//   - products_nutrients a : name TEXT, value INT, per_100g INT, per_serving INT
//   - Widget  → 2 lignes (calories, fat) — per_serving rempli pour les deux
//   - Gadget  → 2 lignes (calories, fat) — per_serving NULL
//   - Doohickey → 1 ligne (calories seulement)
//   Total : 5 lignes
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_structured_pivot_strategy() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("structured_pivot.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("products"), None).unwrap();

        assert!(p1.schemas.iter().any(|s| s.name == "products_nutrients"),
            "products_nutrients doit exister après pass1");

        let suffix_schema = SuffixSchema {
            suffix_cols: vec![
                SuffixColumn { suffix: "_100g".to_string(),    col_name: "per_100g".to_string(),    pg_type: PgType::Integer },
                SuffixColumn { suffix: "_serving".to_string(), col_name: "per_serving".to_string(), pg_type: PgType::Integer },
            ],
            value_type: PgType::Integer,
        };

        let mut schemas = p1.schemas;
        apply_structured_pivot_columns(
            schemas.iter_mut().find(|s| s.name == "products_nutrients")
                .expect("products_nutrients not found"),
            suffix_schema,
        );

        let nutrients_schema = schemas.iter().find(|s| s.name == "products_nutrients").unwrap();
        let data_cols: Vec<_> = nutrients_schema.data_columns().collect();
        assert_eq!(data_cols.len(), 4, "StructuredPivot: 4 colonnes de données (name, value, per_100g, per_serving)");
        assert!(data_cols.iter().any(|c| c.name == "name"),       "colonne 'name' absente");
        assert!(data_cols.iter().any(|c| c.name == "value"),      "colonne 'value' absente");
        assert!(data_cols.iter().any(|c| c.name == "per_100g"),   "colonne 'per_100g' absente");
        assert!(data_cols.iter().any(|c| c.name == "per_serving"),"colonne 'per_serving' absente");
        assert!(matches!(nutrients_schema.inferred_strategy, InferredStrategy::StructuredPivot(_)));

        let fk_col_name = nutrients_schema.columns.iter()
            .find(|c| c.is_parent_fk)
            .map(|c| c.name.clone())
            .expect("products_nutrients must have a parent FK column");

        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("products", &schema), None)
            .await.unwrap();

        // Widget:2 + Gadget:2 + Doohickey:1 = 5 lignes
        assert_eq!(*p2.rows_per_table.get("products").unwrap(), 3);
        assert_eq!(*p2.rows_per_table.get("products_nutrients").unwrap(), 5);
        assert_eq!(common::row_count(&client, &schema, "products").await, 3);
        assert_eq!(common::row_count(&client, &schema, "products_nutrients").await, 5);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

        // Widget calories : value=100, per_100g=200, per_serving=150
        let row = client.query_one(
            &format!(
                "SELECT n.value, n.per_100g, n.per_serving \
                 FROM \"{s}\".\"products_nutrients\" n \
                 JOIN \"{s}\".\"products\" p ON n.{fk} = p.j2s_id \
                 WHERE p.name = 'Widget' AND n.name = 'calories'",
                s = schema, fk = fk_col_name,
            ),
            &[],
        ).await.unwrap();
        assert_eq!(row.get::<_, Option<i32>>("value"),      Some(100));
        assert_eq!(row.get::<_, Option<i32>>("per_100g"),   Some(200));
        assert_eq!(row.get::<_, Option<i32>>("per_serving"),Some(150));

        // Gadget fat : per_serving NULL (clé absente dans la fixture)
        let gadget_fat = client.query_one(
            &format!(
                "SELECT n.per_serving FROM \"{s}\".\"products_nutrients\" n \
                 JOIN \"{s}\".\"products\" p ON n.{fk} = p.j2s_id \
                 WHERE p.name = 'Gadget' AND n.name = 'fat'",
                s = schema, fk = fk_col_name,
            ),
            &[],
        ).await.unwrap();
        assert!(gadget_fat.get::<_, Option<i32>>("per_serving").is_none(),
            "Gadget fat.per_serving doit être NULL");

        // Doohickey : exactement 1 ligne (pas de fat)
        let doohickey_count: i64 = client.query_one(
            &format!(
                "SELECT COUNT(*) FROM \"{s}\".\"products_nutrients\" n \
                 JOIN \"{s}\".\"products\" p ON n.{fk} = p.j2s_id \
                 WHERE p.name = 'Doohickey'",
                s = schema, fk = fk_col_name,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(doohickey_count, 1, "Doohickey doit avoir exactement 1 ligne nutriment");
    }).await;
}

// ---------------------------------------------------------------------------
// InferredStrategy::AutoSplit sur la table racine.
// Fixture : 5 produits, chacun avec un objet enfant `details` (déclencheur
// de is_root && has_object_children).
// Colonnes scalaires :
//   - id, name         → stables (5/5 = 100% ≥ 80%)
//   - tag_a, tag_b     → medium  (3/5 = 60%, entre 30% et 80%)
//   - rare_key         → rare    (1/5 = 20% < 30%) → ignoré
// Après AutoSplit (wide_column_threshold=3, stable=0.80, rare=0.30) :
//   - products     : j2s_id, id, name seulement (colonnes stables)
//   - products_wide : EAV (key, value) pour tag_a/tag_b — 6 lignes
//   - products_details : color TEXT — 5 lignes
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_auto_split_strategy() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("auto_split.jsonl");
        // wide_column_threshold=3 : 5 colonnes scalaires > 3 → wide
        // stable_threshold=0.80, rare_threshold=0.30
        let p1 = pass1::runner::run(&path, &pass1::runner::Pass1Config {
            root_table: "products".to_string(),
            registry: RegistryConfig { wide_column_threshold: 3, stable_threshold: 0.80, rare_threshold: 0.30, ..Default::default() },
            num_workers: None,
        }, None).unwrap();

        assert!(p1.schemas.iter().any(|s| s.name == "products"),         "products manquant");
        assert!(p1.schemas.iter().any(|s| s.name == "products_wide"),    "products_wide manquant");
        assert!(p1.schemas.iter().any(|s| s.name == "products_details"), "products_details manquant");

        let products_schema = p1.schemas.iter().find(|s| s.name == "products").unwrap();
        assert!(
            matches!(products_schema.inferred_strategy, InferredStrategy::AutoSplit { .. }),
            "products doit avoir la stratégie AutoSplit"
        );

        // Après AutoSplit, seules id et name restent dans products (stables)
        let stable_cols: Vec<_> = products_schema.data_columns().collect();
        assert_eq!(stable_cols.len(), 2, "products doit avoir 2 colonnes stables (id, name)");
        assert!(stable_cols.iter().any(|c| c.original_name == "id"),   "colonne id absente");
        assert!(stable_cols.iter().any(|c| c.original_name == "name"), "colonne name absente");

        let schemas = p1.schemas;
        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("products", &schema), None)
            .await.unwrap();

        assert_eq!(*p2.rows_per_table.get("products").unwrap(),         5);
        assert_eq!(*p2.rows_per_table.get("products_details").unwrap(), 5);
        // Widget+Gadget+Doohickey × 2 medium keys (tag_a, tag_b) = 6
        assert_eq!(*p2.rows_per_table.get("products_wide").unwrap(),    6);
        assert_eq!(common::row_count(&client, &schema, "products").await,         5);
        assert_eq!(common::row_count(&client, &schema, "products_details").await, 5);
        assert_eq!(common::row_count(&client, &schema, "products_wide").await,    6);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

        // Widget a bien tag_a="x1" et tag_b="y1" dans products_wide
        let wide_rows = client.query(
            &format!(
                "SELECT w.key, w.value::text AS val \
                 FROM \"{s}\".\"products_wide\" w \
                 JOIN \"{s}\".\"products\" p ON w.j2s_products_id = p.j2s_id \
                 WHERE p.name = 'Widget' \
                 ORDER BY w.key",
                s = schema,
            ),
            &[],
        ).await.unwrap();
        assert_eq!(wide_rows.len(), 2, "Widget doit avoir 2 lignes dans products_wide");
        assert_eq!(wide_rows[0].get::<_, &str>("key"), "tag_a");
        assert_eq!(wide_rows[0].get::<_, &str>("val"), "x1");
        assert_eq!(wide_rows[1].get::<_, &str>("key"), "tag_b");
        assert_eq!(wide_rows[1].get::<_, &str>("val"), "y1");

        // Thingamajig : aucune ligne dans products_wide (pas de tag_a/tag_b)
        let thingamajig_wide: i64 = client.query_one(
            &format!(
                "SELECT COUNT(*) FROM \"{s}\".\"products_wide\" w \
                 JOIN \"{s}\".\"products\" p ON w.j2s_products_id = p.j2s_id \
                 WHERE p.name = 'Thingamajig'",
                s = schema,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(thingamajig_wide, 0, "Thingamajig ne doit pas avoir de lignes dans products_wide");

        // rare_key (Whatsit) ne doit pas apparaître dans products_wide
        let rare_key_count: i64 = client.query_one(
            &format!("SELECT COUNT(*) FROM \"{s}\".\"products_wide\" WHERE key = 'rare_key'", s = schema),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(rare_key_count, 0, "rare_key ne doit pas être écrit dans products_wide");
    }).await;
}

// ---------------------------------------------------------------------------
// InferredStrategy::SiblingCollapse sur une table intermédiaire pure container.
// Fixture : 2 produits, chacun avec un objet `translations` contenant
// exactement 3 clés ISO (fr, en, de) — chaque clé est un objet {label, desc}.
// `translations` est un pure container (0 colonnes scalaires).
//
// Conditions auto-détection (sibling_threshold=3, jaccard=0.5) :
//   - products_translations_fr/en/de → 3 siblings de même schéma (Jaccard=1.0)
//   - products_translations fusionné → (j2s_id, j2s_products_id, lang_code, label, desc)
//   - tables fr/en/de supprimées du schéma final
//
// Assertions :
//   - 2 schemas (products + products_translations)
//   - products_translations a SiblingCollapse, lang_code TEXT, label TEXT, desc TEXT
//   - 2 lignes dans products, 6 dans products_translations
//   - Widget/fr → label="Bonjour", desc="Rouge"
//   - Gadget/de → desc="Blau"
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_keyed_pivot_strategy() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("keyed_pivot.jsonl");
        // sibling_threshold=3 : au moins 3 siblings pour déclencher SiblingCollapse
        // sibling_jaccard=0.5 : Jaccard min acceptable
        let p1 = pass1::runner::run(&path, &common::pass1_config("products"), None).unwrap();

        // fr/en/de sont absorbés → exactement 2 schemas
        assert_eq!(p1.schemas.len(), 2, "fr/en/de doivent être absorbés, 2 schemas attendus");
        assert!(p1.schemas.iter().any(|s| s.name == "products"),              "products manquant");
        assert!(p1.schemas.iter().any(|s| s.name == "products_translations"), "products_translations manquant");

        let translations_schema = p1.schemas.iter()
            .find(|s| s.name == "products_translations").unwrap();
        assert!(
            matches!(translations_schema.inferred_strategy, InferredStrategy::SiblingCollapse(_)),
            "products_translations doit avoir la stratégie SiblingCollapse"
        );

        let data_cols: Vec<_> = translations_schema.data_columns().collect();
        assert!(data_cols.iter().any(|c| c.name == "lang_code"), "colonne lang_code absente");
        assert!(data_cols.iter().any(|c| c.name == "label"),     "colonne label absente");
        assert!(data_cols.iter().any(|c| c.name == "desc"),      "colonne desc absente");

        let schemas = p1.schemas;
        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        // Vérifier que les tables fr/en/de ne sont PAS créées en base
        let sibling_tables: i64 = client.query_one(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name IN \
             ('products_translations_fr','products_translations_en','products_translations_de')",
            &[&schema],
        ).await.unwrap().get(0);
        assert_eq!(sibling_tables, 0, "tables siblings ne doivent pas être créées");

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("products", &schema), None)
            .await.unwrap();

        assert_eq!(*p2.rows_per_table.get("products").unwrap(),              2);
        assert_eq!(*p2.rows_per_table.get("products_translations").unwrap(), 6);
        assert_eq!(common::row_count(&client, &schema, "products").await,              2);
        assert_eq!(common::row_count(&client, &schema, "products_translations").await, 6);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

        // Widget/fr → label="Bonjour", desc="Rouge"
        let widget_fr = client.query_one(
            &format!(
                "SELECT t.label, t.desc \
                 FROM \"{s}\".\"products_translations\" t \
                 JOIN \"{s}\".\"products\" p ON t.j2s_products_id = p.j2s_id \
                 WHERE p.name = 'Widget' AND t.lang_code = 'fr'",
                s = schema,
            ),
            &[],
        ).await.unwrap();
        assert_eq!(widget_fr.get::<_, &str>("label"), "Bonjour");
        assert_eq!(widget_fr.get::<_, &str>("desc"),  "Rouge");

        // Gadget/de → desc="Blau"
        let gadget_de = client.query_one(
            &format!(
                "SELECT t.desc FROM \"{s}\".\"products_translations\" t \
                 JOIN \"{s}\".\"products\" p ON t.j2s_products_id = p.j2s_id \
                 WHERE p.name = 'Gadget' AND t.lang_code = 'de'",
                s = schema,
            ),
            &[],
        ).await.unwrap();
        assert_eq!(gadget_de.get::<_, &str>("desc"), "Blau");

        // Widget/fr → verify all data is in typed columns

    }).await;
}

// ---------------------------------------------------------------------------
// SiblingCollapse pure container — siblings dont les enfants sont eux-mêmes des
// objets (pas de scalaires directs). L'union est vide mais data JSONB capture
// la sous-structure complète.
//
// Fixture : 2 enregistrements "graph", chacun avec un objet `genomes`
// contenant 3 clés génome (gcf_001/002/003) qui valent chacune un objet
// contig {NC_xxx: {is_circular: bool}} — pure containers.
//
// Assertions Pass 1 :
//   - 2 schemas (graph + graph_genomes), gcf_* et NC_* absorbés
//   - graph_genomes a SiblingCollapse, union_cols vide, colonne data JSONB présente
//
// Assertions Pass 2 :
//   - 6 lignes dans graph_genomes (2 records × 3 génomes)
//   - id=1/gcf_001 → data contient {"NC_001": {"is_circular": false}}
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_keyed_pivot_pure_container() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("keyed_pivot_pure_container.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("graph"), None).unwrap();

        // gcf_* et NC_* absorbés → exactement 2 schemas
        assert_eq!(p1.schemas.len(), 2, "2 schemas attendus (graph + graph_genomes)");
        assert!(p1.schemas.iter().any(|s| s.name == "graph"),         "table graph manquante");
        assert!(p1.schemas.iter().any(|s| s.name == "graph_genomes"), "table graph_genomes manquante");

        let genomes_schema = p1.schemas.iter().find(|s| s.name == "graph_genomes").unwrap();
        assert!(
            matches!(genomes_schema.inferred_strategy, InferredStrategy::SiblingCollapse(_)),
            "graph_genomes doit avoir SiblingCollapse"
        );

        // Union vide (pure containers)
        let data_cols: Vec<_> = genomes_schema.data_columns().collect();
        // Aucune autre colonne de données (l'union est vide)
        assert_eq!(
            data_cols.iter().filter(|c| c.name != "key").count(),
            0,
            "aucune colonne union attendue pour des pure containers"
        );

        let schemas = p1.schemas;
        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("graph", &schema), None)
        .await.unwrap();

        assert_eq!(*p2.rows_per_table.get("graph").unwrap(),         2);
        assert_eq!(*p2.rows_per_table.get("graph_genomes").unwrap(), 6);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
    }).await;
}

// ---------------------------------------------------------------------------
// InferredStrategy::NormalizeDynamicKeys sur une table intermédiaire.
// Fixture : 3 produits avec un objet `images` à clés dynamiques (image IDs).
// Chaque clé mappe vers un objet {url, width}.
//
// Différence avec SiblingCollapse : appliqué manuellement (pas auto-détecté),
// le nom de la colonne ID est libre ("image_id").
//
// sibling_threshold=10 → empêche l'auto-détection SiblingCollapse (5 < 10)
// apply_normalize_dynamic_keys → products_images : image_id TEXT, url TEXT, width INT
// Les 5 tables images enfants sont absorbées et supprimées.
//
// Assertions :
//   - 2 schemas après application (products + products_images)
//   - products_images : NormalizeDynamicKeys, colonnes image_id/url/width
//   - 3 lignes products, 5 lignes products_images
//   - Widget a 2 images
//   - img_789 → url="http://c.com", width=1024
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_normalize_dynamic_keys_strategy() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("normalize_dynamic_keys.jsonl");
        // sibling_threshold=10 : 5 tables images < 10 → pas d'auto-détection SiblingCollapse
        let p1 = pass1::runner::run(&path, &pass1::runner::Pass1Config {
            root_table: "products".to_string(),
            registry: RegistryConfig { sibling_threshold: 10, ..Default::default() },
            num_workers: None,
        }, None).unwrap();

        assert!(p1.schemas.iter().any(|s| s.name == "products_images"),
            "products_images doit exister après pass1");

        let mut schemas = p1.schemas;
        apply_normalize_dynamic_keys(&mut schemas, "products_images", "image_id".to_string()).unwrap();

        // Les 5 tables images enfants doivent être absorbées
        assert_eq!(schemas.len(), 2, "2 schemas attendus après absorption des enfants");
        assert!(schemas.iter().any(|s| s.name == "products"),        "products manquant");
        assert!(schemas.iter().any(|s| s.name == "products_images"), "products_images manquant");

        let images_schema = schemas.iter().find(|s| s.name == "products_images").unwrap();
        assert!(
            matches!(images_schema.inferred_strategy, InferredStrategy::NormalizeDynamicKeys { .. }),
            "products_images doit avoir la stratégie NormalizeDynamicKeys"
        );

        let data_cols: Vec<_> = images_schema.data_columns().collect();
        assert!(data_cols.iter().any(|c| c.name == "image_id"), "colonne image_id absente");
        assert!(data_cols.iter().any(|c| c.name == "url"),      "colonne url absente");
        assert!(data_cols.iter().any(|c| c.name == "width"),    "colonne width absente");

        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("products", &schema), None)
            .await.unwrap();

        assert_eq!(*p2.rows_per_table.get("products").unwrap(),        3);
        // Widget:2 + Gadget:1 + Doohickey:2 = 5
        assert_eq!(*p2.rows_per_table.get("products_images").unwrap(), 5);
        assert_eq!(common::row_count(&client, &schema, "products").await,        3);
        assert_eq!(common::row_count(&client, &schema, "products_images").await, 5);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

        // Widget a bien 2 images
        let widget_count: i64 = client.query_one(
            &format!(
                "SELECT COUNT(*) FROM \"{s}\".\"products_images\" i \
                 JOIN \"{s}\".\"products\" p ON i.j2s_products_id = p.j2s_id \
                 WHERE p.name = 'Widget'",
                s = schema,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(widget_count, 2, "Widget doit avoir 2 lignes dans products_images");

        // img_789 → url="http://c.com", width=1024
        let img_789 = client.query_one(
            &format!(
                "SELECT i.url, i.width FROM \"{s}\".\"products_images\" i \
                 WHERE i.image_id = 'img_789'",
                s = schema,
            ),
            &[],
        ).await.unwrap();
        assert_eq!(img_789.get::<_, &str>("url"),   "http://c.com");
        assert_eq!(img_789.get::<_, i32>("width"),  1024);
    }).await;
}

// ---------------------------------------------------------------------------
// InferredStrategy::JsonbFlatten : données enfant inlinées en JSONB dans le parent.
// Fixture : 3 produits avec un objet enfant `dims` (width, height, depth).
// Après apply_jsonb_flatten("products_dims") :
//   - products_dims est supprimé des schémas
//   - products gagne la colonne `products_dims JSONB`
//   - Les données brutes sont stockées dans la colonne JSONB
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_jsonb_flatten_strategy() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("flatten_nested.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("products"), None).unwrap();

        assert_eq!(p1.schemas.len(), 2);
        assert!(p1.schemas.iter().any(|s| s.name == "products_dims"));

        let mut schemas = p1.schemas;
        apply_jsonb_flatten(&mut schemas, "products_dims").unwrap();

        // Child table removed
        assert_eq!(schemas.len(), 1);
        assert!(schemas.iter().all(|s| s.name != "products_dims"));

        // Parent gains a JSONB column named after the child table
        let products_schema = schemas.iter().find(|s| s.name == "products").unwrap();
        let jsonb_col = products_schema.columns.iter().find(|c| c.name == "products_dims");
        assert!(jsonb_col.is_some(), "products doit avoir une colonne products_dims JSONB");
        assert_eq!(jsonb_col.unwrap().pg_type, PgType::Jsonb);

        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let dims_absent: i64 = client
            .query_one("SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name = 'products_dims'", &[&schema])
            .await.unwrap().get("count");
        assert_eq!(dims_absent, 0, "products_dims ne doit pas être créé");

        let jsonb_col_present: i64 = client
            .query_one("SELECT COUNT(*) FROM information_schema.columns \
                 WHERE table_schema = $1 AND table_name = 'products' AND column_name = 'products_dims'",
                 &[&schema])
            .await.unwrap().get("count");
        assert_eq!(jsonb_col_present, 1, "products doit avoir une colonne products_dims");

        pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("products", &schema), None).await.unwrap();

        // Widget : dims = {"width": 10, "height": 20, "depth": 5}
        let widget = client.query_one(
            &format!("SELECT products_dims::text FROM \"{s}\".\"products\" WHERE id = 1", s = schema),
            &[],
        ).await.unwrap();
        let dims: serde_json::Value = serde_json::from_str(widget.get::<_, &str>("products_dims")).unwrap();
        assert_eq!(dims["width"], serde_json::json!(10));
        assert_eq!(dims["height"], serde_json::json!(20));

        // Doohickey : dims partial (no depth)
        let doohickey = client.query_one(
            &format!("SELECT products_dims::text FROM \"{s}\".\"products\" WHERE id = 3", s = schema),
            &[],
        ).await.unwrap();
        let dims3: serde_json::Value = serde_json::from_str(doohickey.get::<_, &str>("products_dims")).unwrap();
        assert_eq!(dims3["width"], serde_json::json!(5));
        assert!(dims3.get("depth").is_none() || dims3["depth"].is_null());
    }).await;
}

// ---------------------------------------------------------------------------
// InferredStrategy::SiblingCollapse sur des siblings ObjectArray.
// Fixture : 2 enregistrements "graph", chacun avec un objet `genomes`
// contenant 3 clés génome (gcf_001/002/003) qui valent chacune un tableau
// d'objets {length, source, target}. gcf_001 a 2 éléments pour id=1.
//
// Conditions auto-détection (sibling_threshold=3, jaccard=0.5) :
//   - graph_genomes_{gcf_001,gcf_002,gcf_003} → 3 siblings ObjectArray, même schéma
//   - graph_genomes fusionné → (j2s_id, j2s_graph_id, j2s_order, key, length, source, target)
//   - SiblingSchema::array_children = true
//
// Assertions Pass 1 :
//   - 2 schemas (graph + graph_genomes), tables gcf_* absorbées
//   - graph_genomes a SiblingCollapse avec array_children=true
//   - graph_genomes a j2s_order parmi les colonnes générées
//
// Assertions Pass 2 :
//   - 2 lignes dans graph, 7 dans graph_genomes
//   - id=1/gcf_001 → 2 lignes (j2s_order 0 et 1)
//   - order=0 → length=100, source="A", target="B"
//   - order=1 → length=150, source="C", target="D"
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_keyed_pivot_array_strategy() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("keyed_pivot_array.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("graph"), None).unwrap();

        // gcf_001/002/003 absorbés → exactement 2 schemas
        assert_eq!(p1.schemas.len(), 2, "les tables gcf_* doivent être absorbées, 2 schemas attendus");
        assert!(p1.schemas.iter().any(|s| s.name == "graph"),         "table graph manquante");
        assert!(p1.schemas.iter().any(|s| s.name == "graph_genomes"), "table graph_genomes manquante");

        let genomes_schema = p1.schemas.iter().find(|s| s.name == "graph_genomes").unwrap();

        // La stratégie doit être SiblingCollapse avec array_children=true
        match &genomes_schema.inferred_strategy {
            InferredStrategy::SiblingCollapse(SiblingSchema { array_children: true, .. }) => {}
            other => panic!("expected SiblingCollapse(array_children=true), got {:?}", other),
        }

        // j2s_order doit être présent parmi les colonnes générées
        assert!(
            genomes_schema.columns.iter().any(|c| c.is_generated && c.name == "j2s_order"),
            "j2s_order manquant dans graph_genomes"
        );

        // Colonnes de données : key + length + source + target
        let data_cols: Vec<_> = genomes_schema.data_columns().collect();
        assert!(data_cols.iter().any(|c| c.name == "key"),    "colonne key absente");
        assert!(data_cols.iter().any(|c| c.name == "length"), "colonne length absente");
        assert!(data_cols.iter().any(|c| c.name == "source"), "colonne source absente");
        assert!(data_cols.iter().any(|c| c.name == "target"), "colonne target absente");

        let schemas = p1.schemas;
        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        // Vérifier que les tables gcf_* ne sont PAS créées en base
        let gcf_tables: i64 = client.query_one(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name LIKE 'graph_genomes_gcf_%'",
            &[&schema],
        ).await.unwrap().get(0);
        assert_eq!(gcf_tables, 0, "les tables gcf_* ne doivent pas être créées");

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("graph", &schema), None)
        .await.unwrap();

        // graph : 2 lignes
        // graph_genomes : id=1 → gcf_001×2 + gcf_002×1 + gcf_003×1 = 4; id=2 → 3 = 7 total
        assert_eq!(*p2.rows_per_table.get("graph").unwrap(),         2);
        assert_eq!(*p2.rows_per_table.get("graph_genomes").unwrap(), 7);
        assert_eq!(common::row_count(&client, &schema, "graph").await,         2);
        assert_eq!(common::row_count(&client, &schema, "graph_genomes").await, 7);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);

        // id=1 / gcf_001 → 2 lignes (j2s_order 0 et 1)
        let gcf001_count: i64 = client.query_one(
            &format!(
                "SELECT COUNT(*) FROM \"{s}\".\"graph_genomes\" g \
                 JOIN \"{s}\".\"graph\" r ON g.j2s_graph_id = r.j2s_id \
                 WHERE r.id = 1 AND g.key = 'gcf_001'",
                s = schema,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(gcf001_count, 2, "id=1/gcf_001 doit avoir 2 lignes");

        // order=0 → length=100, source="A", target="B"
        let first = client.query_one(
            &format!(
                "SELECT g.length, g.source, g.target \
                 FROM \"{s}\".\"graph_genomes\" g \
                 JOIN \"{s}\".\"graph\" r ON g.j2s_graph_id = r.j2s_id \
                 WHERE r.id = 1 AND g.key = 'gcf_001' AND g.j2s_order = 0",
                s = schema,
            ),
            &[],
        ).await.unwrap();
        assert_eq!(first.get::<_, i32>("length"),  100);
        assert_eq!(first.get::<_, &str>("source"), "A");
        assert_eq!(first.get::<_, &str>("target"), "B");

        // order=1 → length=150, source="C", target="D"
        let second = client.query_one(
            &format!(
                "SELECT g.length, g.source, g.target \
                 FROM \"{s}\".\"graph_genomes\" g \
                 JOIN \"{s}\".\"graph\" r ON g.j2s_graph_id = r.j2s_id \
                 WHERE r.id = 1 AND g.key = 'gcf_001' AND g.j2s_order = 1",
                s = schema,
            ),
            &[],
        ).await.unwrap();
        assert_eq!(second.get::<_, i32>("length"),  150);
        assert_eq!(second.get::<_, &str>("source"), "C");
        assert_eq!(second.get::<_, &str>("target"), "D");
    }).await;
}

// ---------------------------------------------------------------------------
// Tests end-to-end disabled_strategies dans Pass1Config
// ---------------------------------------------------------------------------

// disable sibling → aucune table SiblingCollapse dans le résultat
#[test]
fn test_disable_sibling_no_keyed_pivot_integration() {
    use std::collections::HashSet;
    use json2sql::schema::strategies::StrategyName;
    use json2sql::schema::table_schema::InferredStrategy;

    let path = common::fixture("keyed_pivot.jsonl");
    // Avec sibling activé : SiblingCollapse attendu
    let p1_normal = pass1::runner::run(&path, &common::pass1_config("products"), None).unwrap();
    assert!(p1_normal.schemas.iter().any(|s| matches!(s.inferred_strategy, InferredStrategy::SiblingCollapse(_))),
        "sibling enabled → SiblingCollapse attendu dans le schema normal");

    // Avec sibling désactivé : aucun SiblingCollapse
    let config_disabled = pass1::runner::Pass1Config {
        registry: RegistryConfig { disabled_strategies: HashSet::from([StrategyName::Sibling]), ..Default::default() },
        ..common::pass1_config("products")
    };
    let p1_disabled = pass1::runner::run(&path, &config_disabled, None).unwrap();
    assert!(!p1_disabled.schemas.iter().any(|s| matches!(s.inferred_strategy, InferredStrategy::SiblingCollapse(_))),
        "sibling disabled → aucun SiblingCollapse dans le schema");
}

// ---------------------------------------------------------------------------
// InferredStrategy::Jsonb sur la table RACINE (parent_id.is_none()).
// Chemin spécial dans insert_object : l'objet entier est écrit en JSONB blob,
// puis la récursion peuple quand même les tables enfant.
// Fixture : 3 produits avec un objet enfant `attrs`.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_jsonb_strategy_root_table() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("wide_jsonb.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("products"), None).unwrap();

        assert!(p1.schemas.iter().any(|s| s.name == "products"));
        assert!(p1.schemas.iter().any(|s| s.name == "products_attrs"));

        let mut schemas = p1.schemas;
        // Apply Jsonb to the root table — each root object becomes a JSONB blob.
        apply_wide_strategy_columns(
            schemas.iter_mut().find(|s| s.name == "products").expect("products not found"),
            InferredStrategy::Jsonb,
        );

        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let p2 = pass2::runner::run(
            &path, &schemas, &client, &url,
            &common::pass2_config("products", &schema), None,
        ).await.unwrap();

        // Root table: 3 rows, each as a JSONB blob.
        assert_eq!(*p2.rows_per_table.get("products").unwrap_or(&0), 3);
        assert_eq!(common::row_count(&client, &schema, "products").await, 3);

        // Child table still populated via recursion from the root Jsonb path.
        assert_eq!(*p2.rows_per_table.get("products_attrs").unwrap_or(&0), 3);
        assert_eq!(common::row_count(&client, &schema, "products_attrs").await, 3);

        // Root JSONB blob contains the full object — verify via JSONB field access.
        let rows = client.query(
            &format!(
                "SELECT data->>'name' AS name FROM \"{}\".\"products\" ORDER BY data->>'name'",
                schema
            ),
            &[],
        ).await.unwrap();
        assert_eq!(rows.len(), 3);
        let names: Vec<&str> = rows.iter().map(|r| r.get("name")).collect();
        assert!(names.contains(&"Widget"), "Widget manquant dans JSONB root");
        assert!(names.contains(&"Gadget"), "Gadget manquant dans JSONB root");
        assert!(names.contains(&"Doohickey"), "Doohickey manquant dans JSONB root");

        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
    }).await;
}

// disable pivot → InferredStrategy::Jsonb au lieu de Pivot pour les tables homogènes.
// wide_column_threshold=3 : nutrients a 4 colonnes homogènes (int) → Pivot automatique.
#[test]
fn test_disable_pivot_gives_jsonb_integration() {
    use std::collections::HashSet;
    use json2sql::schema::strategies::StrategyName;
    use json2sql::schema::table_schema::InferredStrategy;

    let path = common::fixture("pivot_eav.jsonl");
    // Config avec wide_column_threshold=3 pour déclencher la détection wide sur nutrients
    let base_config = pass1::runner::Pass1Config {
        registry: RegistryConfig { wide_column_threshold: 3, ..Default::default() },
        ..common::pass1_config("products")
    };

    // Avec pivot activé : Pivot attendu sur products_nutrients
    let p1_normal = pass1::runner::run(&path, &base_config, None).unwrap();
    assert!(p1_normal.schemas.iter().any(|s| s.inferred_strategy == InferredStrategy::Pivot),
        "pivot enabled → InferredStrategy::Pivot attendu sur products_nutrients");

    // Avec pivot désactivé : Jsonb à la place
    let config_disabled = pass1::runner::Pass1Config {
        registry: RegistryConfig { disabled_strategies: HashSet::from([StrategyName::Pivot]), wide_column_threshold: 3, ..Default::default() },
        ..common::pass1_config("products")
    };
    let p1_disabled = pass1::runner::run(&path, &config_disabled, None).unwrap();
    assert!(!p1_disabled.schemas.iter().any(|s| s.inferred_strategy == InferredStrategy::Pivot),
        "pivot disabled → aucun InferredStrategy::Pivot, Jsonb attendu");
    assert!(p1_disabled.schemas.iter().any(|s| s.inferred_strategy == InferredStrategy::Jsonb),
        "pivot disabled → InferredStrategy::Jsonb attendu");
}

// disable structured_pivot → suffix detection skippée → fallback suggest_wide_strategy.
// Fixture : nutrients avec 6 colonnes numériques à pattern _100g/_serving.
// wide_column_threshold=4 : 6 colonnes > 4 → wide detection active.
// Activé : StructuredPivot. Désactivé : Pivot (all-numeric fallback).
#[test]
fn test_disable_structured_pivot_gives_pivot_integration() {
    use std::collections::HashSet;
    use json2sql::schema::strategies::StrategyName;
    use json2sql::schema::table_schema::InferredStrategy;

    let path = common::fixture("structured_pivot.jsonl");
    let base_config = pass1::runner::Pass1Config {
        registry: RegistryConfig { wide_column_threshold: 4, ..Default::default() },
        ..common::pass1_config("products")
    };

    // structured_pivot activé : StructuredPivot attendu sur products_nutrients
    let p1_normal = pass1::runner::run(&path, &base_config, None).unwrap();
    assert!(
        p1_normal.schemas.iter().any(|s| matches!(s.inferred_strategy, InferredStrategy::StructuredPivot(_))),
        "structured_pivot enabled → InferredStrategy::StructuredPivot attendu sur products_nutrients"
    );

    // structured_pivot désactivé : aucun StructuredPivot, Pivot à la place (all-numeric)
    let config_disabled = pass1::runner::Pass1Config {
        registry: RegistryConfig { disabled_strategies: HashSet::from([StrategyName::StructuredPivot]), wide_column_threshold: 4, ..Default::default() },
        ..common::pass1_config("products")
    };
    let p1_disabled = pass1::runner::run(&path, &config_disabled, None).unwrap();
    assert!(
        !p1_disabled.schemas.iter().any(|s| matches!(s.inferred_strategy, InferredStrategy::StructuredPivot(_))),
        "structured_pivot disabled → aucun InferredStrategy::StructuredPivot"
    );
    assert!(
        p1_disabled.schemas.iter().any(|s| s.inferred_strategy == InferredStrategy::Pivot),
        "structured_pivot disabled → InferredStrategy::Pivot attendu (fallback all-numeric)"
    );
}

// ---------------------------------------------------------------------------
// [issue #45, tâche 2 — RED] Split identité/compagnon systématique quand
// apply_non_autosplit_strategy choisit Pivot sur une table Object-parent
// SANS enfant réel.
//
// Fixture : racine `produits` (id, name — stables, reste en Columns), enfant
// `nutriments` (ChildKind::Object) avec 2 clés stables (calcium, iron,
// présentes dans les 10 lignes) et 3 clés rares (selenium/iodine/zinc,
// chacune présente dans 1 seule ligne sur 10 → sous rare_threshold=0.15).
//
// Design attendu (voir issue #45, section "Design final validé") :
//   - `produits` reste Columns (2 colonnes stables, aucun split — ratio
//     stable 100%, bien au-dessus du seuil "Columns").
//   - `produits_nutriments` (identité) : SEULEMENT colonnes générées
//     (j2s_id, FK vers produits) — rétention inconditionnelle à zéro colonne
//     de donnée, même les clés stables (calcium/iron) migrent vers le
//     compagnon.
//   - `produits_nutriments_pivot` (compagnon) : (key, value) EAV, FK vers
//     l'identité — TOUTES les clés y arrivent, y compris les rares (aucune
//     ne doit être droppée silencieusement comme le ferait l'ancien
//     mécanisme AutoSplit `collect_medium_keys`).
//
// État avant les tâches 3/4 : ce test est ROUGE — `apply_non_autosplit_strategy`
// pivote aujourd'hui `produits_nutriments` sur place (une seule table, pas de
// split identité/compagnon) : `schemas.len() == 2`, pas 3.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_pivot_identity_companion_split_no_real_child() {
    use std::collections::HashSet;

    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("pivot_split_no_child.jsonl");
        let p1 = pass1::runner::run(&path, &pass1::runner::Pass1Config {
            root_table: "produits".to_string(),
            registry: RegistryConfig { wide_column_threshold: 1, stable_threshold: 0.5, rare_threshold: 0.15, ..Default::default() },
            num_workers: None,
        }, None).unwrap();

        // `produits` (racine) reste en Columns — ratio stable 100% (id, name toujours présents).
        let produits_schema = p1.schemas.iter().find(|s| s.name == "produits")
            .expect("produits manquant");
        assert_eq!(produits_schema.inferred_strategy, InferredStrategy::Columns,
            "produits doit rester Columns (ratio stable 100%, pas de split)");
        assert_eq!(produits_schema.data_columns().count(), 2, "produits doit garder id + name");

        // 3 tables attendues : produits, produits_nutriments (identité), produits_nutriments_pivot (compagnon).
        assert_eq!(p1.schemas.len(), 3,
            "3 tables attendues (produits + identité + compagnon) — trouvé : {:?}",
            p1.schemas.iter().map(|s| &s.name).collect::<Vec<_>>());

        let identity = p1.schemas.iter().find(|s| s.name == "produits_nutriments")
            .expect("produits_nutriments (identité) manquante");
        let companion = p1.schemas.iter().find(|s| s.name == "produits_nutriments_pivot")
            .expect("produits_nutriments_pivot (compagnon) manquant");

        // Identité : rétention inconditionnelle à zéro colonne de donnée — même calcium/iron migrent.
        assert_eq!(identity.data_columns().count(), 0,
            "identité doit avoir 0 colonne de donnée — toutes les clés migrent vers le compagnon");

        // Compagnon : (key, value) EAV.
        let companion_data_cols: Vec<_> = companion.data_columns().collect();
        assert_eq!(companion_data_cols.len(), 2, "compagnon : exactement (key, value)");
        assert!(companion_data_cols.iter().any(|c| c.name == "key"));
        assert!(companion_data_cols.iter().any(|c| c.name == "value"));

        // medium_keys doit contenir TOUTES les clés (stables + rares), aucune droppée.
        match &identity.inferred_strategy {
            InferredStrategy::AutoSplit { medium_keys, wide_table_name, .. } => {
                assert_eq!(
                    medium_keys.clone(),
                    HashSet::from([
                        "calcium".to_string(), "iron".to_string(),
                        "selenium".to_string(), "iodine".to_string(), "zinc".to_string(),
                    ]),
                    "medium_keys doit contenir les 5 clés — aucune rétention, aucun drop"
                );
                assert_eq!(wide_table_name, "produits_nutriments_pivot");
            }
            other => panic!("identité doit porter InferredStrategy::AutoSplit, trouvé : {other:?}"),
        }

        // Compagnon FK → identité (pas directement vers produits).
        let companion_fk = companion.columns.iter().find(|c| c.is_parent_fk)
            .expect("compagnon doit avoir une FK vers son parent");
        let identity_fk = identity.columns.iter().find(|c| c.is_parent_fk)
            .expect("identité doit avoir une FK vers produits");

        let schemas = p1.schemas.clone();
        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("produits", &schema), None)
            .await.unwrap();

        assert_eq!(p2.anomaly_collector.total_anomalies(), 0, "aucune anomalie attendue");
        assert_eq!(*p2.rows_per_table.get("produits").unwrap(), 10);
        assert_eq!(*p2.rows_per_table.get("produits_nutriments").unwrap(), 10,
            "identité : une ligne d'ancrage par occurrence");
        // calcium×10 + iron×10 + selenium×1 + iodine×1 + zinc×1 = 23
        assert_eq!(*p2.rows_per_table.get("produits_nutriments_pivot").unwrap(), 23);

        assert_eq!(common::row_count(&client, &schema, "produits").await, 10);
        assert_eq!(common::row_count(&client, &schema, "produits_nutriments").await, 10);
        assert_eq!(common::row_count(&client, &schema, "produits_nutriments_pivot").await, 23);

        // La clé rare "selenium" (P1) n'est pas droppée et sa valeur traverse intacte.
        let selenium_value: i64 = client.query_one(
            &format!(
                "SELECT CAST(c.value AS bigint) FROM \"{s}\".\"produits_nutriments_pivot\" c \
                 JOIN \"{s}\".\"produits_nutriments\" i ON c.{cfk} = i.j2s_id \
                 JOIN \"{s}\".\"produits\" p ON i.{ifk} = p.j2s_id \
                 WHERE p.name = 'P1' AND c.key = 'selenium'",
                s = schema, cfk = companion_fk.name, ifk = identity_fk.name,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(selenium_value, 1, "selenium (clé rare) doit atteindre le compagnon avec sa valeur intacte");

        // P4 n'a aucune ligne 'selenium' (n'a jamais eu cette clé).
        let p4_selenium: i64 = client.query_one(
            &format!(
                "SELECT COUNT(*) FROM \"{s}\".\"produits_nutriments_pivot\" c \
                 JOIN \"{s}\".\"produits_nutriments\" i ON c.{cfk} = i.j2s_id \
                 JOIN \"{s}\".\"produits\" p ON i.{ifk} = p.j2s_id \
                 WHERE p.name = 'P4' AND c.key = 'selenium'",
                s = schema, cfk = companion_fk.name, ifk = identity_fk.name,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(p4_selenium, 0, "P4 ne doit pas avoir de ligne 'selenium'");
    }).await;
}

// ---------------------------------------------------------------------------
// [issue #45, tâche 3] `is_wide_eligible` doit accepter `ChildKind::ObjectArray`.
//
// Fixture : racine `produits`, chacun avec un array `avis` d'un seul objet
// review portant UNE clé numérique distincte par produit (score_a..score_f,
// 6 produits) — 6 clés au total, type homogène (entier) → `suggest_wide_strategy`
// choisirait Pivot une fois la table éligible (freq=1/6 par clé — au-dessus du
// `stable_threshold` par défaut 0.10, mais `row_count`(6) < 10 évite le garde-fou
// "high stable ratio → Columns" de `wide_strategy.rs:98`, qui exige row_count>=10).
//
// Avant le fix : `avis` (ChildKind::ObjectArray) reste Columns quel que soit
// le nombre de colonnes — `is_wide_eligible` l'exclut structurellement
// (`wide_strategy.rs:79`). Ce test est donc rouge tant que la tâche 3 n'est
// pas faite.
//
// Portée volontairement limitée au niveau schéma (pas de pass2/DB), et à la
// seule éligibilité (pas Columns) sans figer la stratégie exacte : la tâche 4
// fera basculer ce cas précis (ObjectArray-parent → Pivot suggéré) sur le
// split identité/compagnon (AutoSplit), et le chemin d'écriture correspondant
// n'existe pas avant les tâches 9-10 (insert_array route encore vers
// insert_object) — écrire un test pass2/DB ici serait donc prématuré.
// ---------------------------------------------------------------------------
#[test]
fn test_object_array_eligible_for_wide_strategy() {
    let path = common::fixture("wide_object_array.jsonl");
    let config = pass1::runner::Pass1Config {
        registry: RegistryConfig { wide_column_threshold: 2, ..Default::default() },
        ..common::pass1_config("produits")
    };

    let p1 = pass1::runner::run(&path, &config, None).unwrap();

    let avis_schema = p1.schemas.iter().find(|s| s.name == "produits_avis")
        .expect("produits_avis manquant");
    assert_eq!(
        avis_schema.child_kind,
        Some(json2sql::schema::table_schema::ChildKind::ObjectArray),
        "produits_avis doit être ChildKind::ObjectArray"
    );
    assert_ne!(
        avis_schema.inferred_strategy, InferredStrategy::Columns,
        "produits_avis (ObjectArray, 6 colonnes > seuil 2) doit maintenant être éligible \
         au wide-table strategy — plus jamais bloqué en Columns"
    );
    // Pas d'assertion sur la stratégie exacte (Pivot aujourd'hui) : dès la tâche 4, ce cas
    // (ObjectArray-parent → apply_non_autosplit_strategy choisirait Pivot) bascule sur le split
    // identité/compagnon, donc sur InferredStrategy::AutoSplit. Seule la portée de la tâche 3
    // (éligibilité) est garantie ici — figer Pivot ferait de ce test un faux rouge à la tâche 4.
}

// ---------------------------------------------------------------------------
// [issue #45, tâche 6] Repro empirique nutriments/historique — le cas réel qui a
// déclenché le chantier : une table Object-parent (`nutriments`) choisie Pivot ET
// possédant un vrai enfant ObjectArray (`historique`, ex: historique de mesures).
//
// Avant #45 : `historique` était exclue du schéma final et ses données coercées en
// texte dans la colonne `value` du pivot → anomalies + perte de données (Problème 1
// de l'issue). Après le split identité/compagnon, `historique` reste une table à
// part entière, routée via `recurse_children` (réutilisation du chemin AutoSplit)
// vers l'identité — jamais absorbée par le compagnon EAV.
//
// Fixture : 10 produits, `nutriments` avec calcium/iron stables + 3 clés rares
// (une par produit sur 3), et `historique` (1 à 2 entrées date_mesure/labo) sur
// chaque produit.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_pivot_split_with_real_object_array_child() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("pivot_split_real_child.jsonl");
        let p1 = pass1::runner::run(&path, &pass1::runner::Pass1Config {
            root_table: "produits".to_string(),
            registry: RegistryConfig { wide_column_threshold: 1, stable_threshold: 0.5, rare_threshold: 0.15, ..Default::default() },
            num_workers: None,
        }, None).unwrap();

        // 4 tables : produits, identité, compagnon, ET historique (vrai enfant préservé).
        assert_eq!(p1.schemas.len(), 4,
            "4 tables attendues (produits + identité + compagnon + historique) — trouvé : {:?}",
            p1.schemas.iter().map(|s| &s.name).collect::<Vec<_>>());

        let identity = p1.schemas.iter().find(|s| s.name == "produits_nutriments")
            .expect("produits_nutriments (identité) manquante");
        let historique = p1.schemas.iter().find(|s| s.name == "produits_nutriments_historique")
            .expect("produits_nutriments_historique (vrai enfant) manquante — absorbée/droppée ?");

        // historique.parent_fk doit référencer l'identité (pas produits directement).
        assert_eq!(historique.parent_table.as_deref(), Some("produits_nutriments"),
            "historique doit être rattachée à l'identité produits_nutriments");
        let historique_fk = historique.columns.iter().find(|c| c.is_parent_fk)
            .expect("historique doit avoir une FK");
        let identity_fk = identity.columns.iter().find(|c| c.is_parent_fk)
            .expect("identité doit avoir une FK vers produits");

        let schemas = p1.schemas.clone();
        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("produits", &schema), None)
            .await.unwrap();

        assert_eq!(p2.anomaly_collector.total_anomalies(), 0,
            "0 anomalie attendue — historique ne doit plus être coercée en texte dans value");
        assert_eq!(*p2.rows_per_table.get("produits").unwrap(), 10);
        assert_eq!(*p2.rows_per_table.get("produits_nutriments").unwrap(), 10);
        assert_eq!(*p2.rows_per_table.get("produits_nutriments_pivot").unwrap(), 23);
        // P2 a 2 entrées, les 9 autres en ont 1 chacune = 11.
        assert_eq!(*p2.rows_per_table.get("produits_nutriments_historique").unwrap(), 11);

        assert_eq!(common::row_count(&client, &schema, "produits_nutriments_historique").await, 11);

        // Les 2 entrées d'historique de P2 sont bien rattachées à l'identité de P2, pas à un
        // ancêtre commun ou à une ligne du compagnon.
        let p2_historique_count: i64 = client.query_one(
            &format!(
                "SELECT COUNT(*) FROM \"{s}\".\"produits_nutriments_historique\" h \
                 JOIN \"{s}\".\"produits_nutriments\" i ON h.{hfk} = i.j2s_id \
                 JOIN \"{s}\".\"produits\" p ON i.{ifk} = p.j2s_id \
                 WHERE p.name = 'P2'",
                s = schema, hfk = historique_fk.name, ifk = identity_fk.name,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(p2_historique_count, 2, "P2 doit avoir 2 entrées d'historique");

        // Round-trip d'une valeur : le labo de la 1ère entrée de P1.
        let p1_labo: String = client.query_one(
            &format!(
                "SELECT h.labo FROM \"{s}\".\"produits_nutriments_historique\" h \
                 JOIN \"{s}\".\"produits_nutriments\" i ON h.{hfk} = i.j2s_id \
                 JOIN \"{s}\".\"produits\" p ON i.{ifk} = p.j2s_id \
                 WHERE p.name = 'P1'",
                s = schema, hfk = historique_fk.name, ifk = identity_fk.name,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(p1_labo, "LabA");
    }).await;
}

// ---------------------------------------------------------------------------
// [issue #45, tâche 7] `ObjectArray`+`Pivot` avec un vrai enfant propre (petit-enfant).
//
// Contrairement à la tâche 6 (table `Object`-parent avec un vrai enfant), ici c'est
// la table pivotée elle-même qui est `ObjectArray`-parent (relation 1:n avec SA
// PROPRE mère) — le cas rendu éligible par la tâche 3 — ET qui possède en plus un
// vrai enfant à elle (`commentaires`, petit-enfant de la racine). Vérifie que
// `recurse_children` route ce petit-enfant vers l'identité de la table `avis`
// (et non vers la racine `produits` ni vers le compagnon), quelle que soit la
// relation de la table pivotée avec SA PROPRE mère.
//
// Fixture : 6 produits, chacun avec un seul élément `avis` (clé de score dynamique
// distincte par produit → Pivot une fois éligible, cf. tâche 3) et un vrai enfant
// `commentaires` (1 à 2 entrées texte/auteur).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_pivot_split_object_array_parent_with_real_grandchild() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("wide_object_array_with_grandchild.jsonl");
        let p1 = pass1::runner::run(&path, &pass1::runner::Pass1Config {
            root_table: "produits".to_string(),
            registry: RegistryConfig { wide_column_threshold: 2, ..Default::default() },
            num_workers: None,
        }, None).unwrap();

        // 4 tables : produits, identité (avis), compagnon (avis_pivot), commentaires (petit-enfant).
        assert_eq!(p1.schemas.len(), 4,
            "4 tables attendues (produits + identité + compagnon + commentaires) — trouvé : {:?}",
            p1.schemas.iter().map(|s| &s.name).collect::<Vec<_>>());

        let identity = p1.schemas.iter().find(|s| s.name == "produits_avis")
            .expect("produits_avis (identité) manquante");
        assert!(matches!(&identity.inferred_strategy, InferredStrategy::AutoSplit { .. }),
            "identité doit porter AutoSplit");
        assert_eq!(identity.data_columns().count(), 0, "identité : 0 colonne de donnée");

        let companion = p1.schemas.iter().find(|s| s.name == "produits_avis_pivot")
            .expect("produits_avis_pivot (compagnon) manquant");
        assert_eq!(companion.data_columns().count(), 2, "compagnon : (key, value)");

        let commentaires = p1.schemas.iter().find(|s| s.name == "produits_avis_commentaires")
            .expect("produits_avis_commentaires (petit-enfant) manquante — absorbée/droppée ?");
        assert_eq!(commentaires.parent_table.as_deref(), Some("produits_avis"),
            "commentaires doit être rattachée à l'identité produits_avis, pas à produits ni au compagnon");

        let commentaires_fk = commentaires.columns.iter().find(|c| c.is_parent_fk)
            .expect("commentaires doit avoir une FK");
        let identity_fk = identity.columns.iter().find(|c| c.is_parent_fk)
            .expect("identité doit avoir une FK vers produits");

        let schemas = p1.schemas.clone();
        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("produits", &schema), None)
            .await.unwrap();

        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
        assert_eq!(*p2.rows_per_table.get("produits").unwrap(), 6);
        assert_eq!(*p2.rows_per_table.get("produits_avis").unwrap(), 6, "1 avis par produit");
        assert_eq!(*p2.rows_per_table.get("produits_avis_pivot").unwrap(), 6, "1 clé de score par avis");
        // P2 a 2 commentaires, les 5 autres en ont 1 chacun = 7.
        assert_eq!(*p2.rows_per_table.get("produits_avis_commentaires").unwrap(), 7);

        // Les 2 commentaires de P2 sont bien rattachés à l'identité de l'avis de P2.
        let p2_comment_count: i64 = client.query_one(
            &format!(
                "SELECT COUNT(*) FROM \"{s}\".\"produits_avis_commentaires\" c \
                 JOIN \"{s}\".\"produits_avis\" i ON c.{cfk} = i.j2s_id \
                 JOIN \"{s}\".\"produits\" p ON i.{ifk} = p.j2s_id \
                 WHERE p.name = 'P2'",
                s = schema, cfk = commentaires_fk.name, ifk = identity_fk.name,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(p2_comment_count, 2);

        // Round-trip : l'auteur du commentaire unique de P1.
        let p1_auteur: String = client.query_one(
            &format!(
                "SELECT c.auteur FROM \"{s}\".\"produits_avis_commentaires\" c \
                 JOIN \"{s}\".\"produits_avis\" i ON c.{cfk} = i.j2s_id \
                 JOIN \"{s}\".\"produits\" p ON i.{ifk} = p.j2s_id \
                 WHERE p.name = 'P1'",
                s = schema, cfk = commentaires_fk.name, ifk = identity_fk.name,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(p1_auteur, "Alice");
    }).await;
}

// ---------------------------------------------------------------------------
// [issue #45, tâche 11] `ObjectArray` hétérogène → Jsonb (Problème 2b).
//
// Ferme la boucle des tâches 3/9/10 : une table `ObjectArray`-parent dont les clés
// sont hétérogènes (int + string + bool → plusieurs catégories de type,
// `suggest_wide_strategy` choisit Jsonb au lieu de Pivot) doit produire un DDL à
// 4 colonnes (j2s_id, parent_fk, j2s_order, data JSONB), un blob round-trip
// correct par élément, et `j2s_order` peuplé selon la position dans l'array.
//
// Avant #45 : `avis` restait bloquée en `Columns` (tâche 3) ; même une fois
// éligible, `insert_array` aurait écrit `NULL` dans `data` pour chaque élément
// (tâche 10, confirmé par test unitaire rouge).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_object_array_heterogeneous_jsonb_end_to_end() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("wide_object_array_jsonb.jsonl");
        let p1 = pass1::runner::run(&path, &pass1::runner::Pass1Config {
            root_table: "produits".to_string(),
            registry: RegistryConfig { wide_column_threshold: 2, ..Default::default() },
            num_workers: None,
        }, None).unwrap();

        let avis = p1.schemas.iter().find(|s| s.name == "produits_avis")
            .expect("produits_avis manquant");
        assert_eq!(avis.inferred_strategy, InferredStrategy::Jsonb,
            "score/comment/verified hétérogènes → Jsonb attendu");
        assert_eq!(avis.data_columns().count(), 1, "une seule colonne data");
        assert!(avis.find_by_original("data").is_some());
        // DDL : j2s_id, j2s_produits_id, j2s_order, data — 4 colonnes au total.
        assert_eq!(avis.columns.len(), 4, "j2s_id + parent_fk + j2s_order + data");
        assert!(avis.columns.iter().any(|c| c.original_name == "j2s_order"),
            "j2s_order doit être présent sur une table ObjectArray, même en Jsonb");

        let schemas = p1.schemas.clone();
        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();

        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("produits", &schema), None)
            .await.unwrap();

        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
        assert_eq!(*p2.rows_per_table.get("produits").unwrap(), 6);
        // 6 produits, P2 a 2 avis, les 5 autres en ont 1 = 7.
        assert_eq!(*p2.rows_per_table.get("produits_avis").unwrap(), 7);
        assert_eq!(common::row_count(&client, &schema, "produits_avis").await, 7);

        // Round-trip + j2s_order : les 2 avis de P2, dans l'ordre.
        let rows = client.query(
            &format!(
                "SELECT a.j2s_order, \
                        a.data->>'score'    AS score, \
                        a.data->>'comment'  AS comment, \
                        a.data->>'verified' AS verified \
                 FROM \"{s}\".\"produits_avis\" a \
                 JOIN \"{s}\".\"produits\" p ON a.j2s_produits_id = p.j2s_id \
                 WHERE p.name = 'P2' ORDER BY a.j2s_order",
                s = schema,
            ),
            &[],
        ).await.unwrap();
        assert_eq!(rows.len(), 2, "P2 doit avoir 2 lignes dans produits_avis");
        assert_eq!(rows[0].get::<_, i64>("j2s_order"), 0);
        assert_eq!(rows[0].get::<_, &str>("score"), "20");
        assert_eq!(rows[0].get::<_, &str>("comment"), "moyen");
        assert_eq!(rows[0].get::<_, &str>("verified"), "false");
        assert_eq!(rows[1].get::<_, i64>("j2s_order"), 1);
        assert_eq!(rows[1].get::<_, &str>("score"), "25");
        assert_eq!(rows[1].get::<_, &str>("comment"), "pas mal");
        assert_eq!(rows[1].get::<_, &str>("verified"), "true");
    }).await;
}
