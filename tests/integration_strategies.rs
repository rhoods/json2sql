mod common;

use json2sql::{db, pass1, pass2};
use json2sql::schema::registry::{apply_flatten, apply_normalize_dynamic_keys, apply_structured_pivot_columns, apply_wide_strategy_columns};
use json2sql::schema::table_schema::{SuffixColumn, SuffixSchema, WideStrategy};
use json2sql::schema::type_tracker::PgType;

// ---------------------------------------------------------------------------
// WideStrategy::Pivot (EAV) sur une table enfant.
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
    common::with_schema(|client, schema| async move {
        let path = common::fixture("pivot_eav.jsonl");
        let p1 = pass1::runner::run(&path, "products", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

        assert_eq!(p1.schemas.len(), 2);
        assert!(p1.schemas.iter().any(|s| s.name == "products"));
        assert!(p1.schemas.iter().any(|s| s.name == "products_nutrients"));

        let mut schemas = p1.schemas;
        apply_wide_strategy_columns(
            schemas.iter_mut().find(|s| s.name == "products_nutrients")
                .expect("products_nutrients not found"),
            WideStrategy::Pivot,
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

        db::ddl::create_tables(&client, &schemas, &schema, false).await.unwrap();

        let p2 = pass2::runner::run(&path, "products", &schemas, &client, &schema, 1000, false, None, 1, None, None)
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
// WideStrategy::Jsonb sur une table enfant.
// Fixture : 3 produits avec un objet enfant `attrs` à clés dynamiques.
// Après apply_wide_strategy_columns(Jsonb), products_attrs a une seule
// colonne `data JSONB` contenant l'objet entier.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_jsonb_strategy() {
    common::with_schema(|client, schema| async move {
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
        assert_eq!(tables_created, 2);

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
// WideStrategy Flatten : colonnes enfant inlinées dans le parent.
// Fixture : 3 produits avec un objet enfant `dims` (width, height, depth).
// Après apply_flatten("products_dims", "dims_", 1) :
//   - products_dims est supprimé des schémas
//   - products gagne les colonnes dims_width, dims_height, dims_depth
//   - dims_depth = NULL pour Doohickey (clé absente dans la fixture)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_flatten_strategy() {
    common::with_schema(|client, schema| async move {
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
            "dims_width column expected after flatten");
        assert!(products_schema.find_by_original("width").is_some(),
            "column with original_name 'width' expected after flatten");
        assert!(products_schema.columns.iter().any(|c| c.name == "dims_height"));
        assert!(products_schema.columns.iter().any(|c| c.name == "dims_depth"));
        assert_eq!(products_schema.data_columns().count(), 5,
            "products doit avoir 5 colonnes de données après flatten (id, name, dims_*)");

        db::ddl::create_tables(&client, &schemas, &schema, false).await.unwrap();

        let dims_absent: i64 = client
            .query_one("SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_name = 'products_dims'", &[&schema])
            .await.unwrap().get("count");
        assert_eq!(dims_absent, 0, "products_dims ne doit pas être créé");

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
    common::with_schema(|client, schema| async move {
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
            "tag doit être inféré TEXT/VarChar, obtenu {:?}", tag_col.pg_type
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
    }).await;
}

// ---------------------------------------------------------------------------
// WideStrategy::StructuredPivot sur une table enfant.
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
    common::with_schema(|client, schema| async move {
        let path = common::fixture("structured_pivot.jsonl");
        let p1 = pass1::runner::run(&path, "products", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

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
        assert!(matches!(nutrients_schema.wide_strategy, WideStrategy::StructuredPivot(_)));

        let fk_col_name = nutrients_schema.columns.iter()
            .find(|c| c.is_parent_fk)
            .map(|c| c.name.clone())
            .expect("products_nutrients must have a parent FK column");

        db::ddl::create_tables(&client, &schemas, &schema, false).await.unwrap();

        let p2 = pass2::runner::run(&path, "products", &schemas, &client, &schema, 1000, false, None, 1, None, None)
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
// WideStrategy::AutoSplit sur la table racine.
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
    common::with_schema(|client, schema| async move {
        let path = common::fixture("auto_split.jsonl");
        // wide_column_threshold=3 : 5 colonnes scalaires > 3 → wide
        // stable_threshold=0.80, rare_threshold=0.30
        let p1 = pass1::runner::run(&path, "products", 256, false, 3, 3, 0.5, 0.80, 0.30, None).unwrap();

        assert!(p1.schemas.iter().any(|s| s.name == "products"),         "products manquant");
        assert!(p1.schemas.iter().any(|s| s.name == "products_wide"),    "products_wide manquant");
        assert!(p1.schemas.iter().any(|s| s.name == "products_details"), "products_details manquant");

        let products_schema = p1.schemas.iter().find(|s| s.name == "products").unwrap();
        assert!(
            matches!(products_schema.wide_strategy, WideStrategy::AutoSplit { .. }),
            "products doit avoir la stratégie AutoSplit"
        );

        // Après AutoSplit, seules id et name restent dans products (stables)
        let stable_cols: Vec<_> = products_schema.data_columns().collect();
        assert_eq!(stable_cols.len(), 2, "products doit avoir 2 colonnes stables (id, name)");
        assert!(stable_cols.iter().any(|c| c.original_name == "id"),   "colonne id absente");
        assert!(stable_cols.iter().any(|c| c.original_name == "name"), "colonne name absente");

        let schemas = p1.schemas;
        db::ddl::create_tables(&client, &schemas, &schema, false).await.unwrap();

        let p2 = pass2::runner::run(&path, "products", &schemas, &client, &schema, 1000, false, None, 1, None, None)
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
// WideStrategy::KeyedPivot sur une table intermédiaire pure container.
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
//   - products_translations a KeyedPivot, lang_code TEXT, label TEXT, desc TEXT
//   - 2 lignes dans products, 6 dans products_translations
//   - Widget/fr → label="Bonjour", desc="Rouge"
//   - Gadget/de → desc="Blau"
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_keyed_pivot_strategy() {
    common::with_schema(|client, schema| async move {
        let path = common::fixture("keyed_pivot.jsonl");
        // sibling_threshold=3 : au moins 3 siblings pour déclencher KeyedPivot
        // sibling_jaccard=0.5 : Jaccard min acceptable
        let p1 = pass1::runner::run(&path, "products", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

        // fr/en/de sont absorbés → exactement 2 schemas
        assert_eq!(p1.schemas.len(), 2, "fr/en/de doivent être absorbés, 2 schemas attendus");
        assert!(p1.schemas.iter().any(|s| s.name == "products"),              "products manquant");
        assert!(p1.schemas.iter().any(|s| s.name == "products_translations"), "products_translations manquant");

        let translations_schema = p1.schemas.iter()
            .find(|s| s.name == "products_translations").unwrap();
        assert!(
            matches!(translations_schema.wide_strategy, WideStrategy::KeyedPivot(_)),
            "products_translations doit avoir la stratégie KeyedPivot"
        );

        let data_cols: Vec<_> = translations_schema.data_columns().collect();
        assert!(data_cols.iter().any(|c| c.name == "lang_code"), "colonne lang_code absente");
        assert!(data_cols.iter().any(|c| c.name == "label"),     "colonne label absente");
        assert!(data_cols.iter().any(|c| c.name == "desc"),      "colonne desc absente");

        let schemas = p1.schemas;
        db::ddl::create_tables(&client, &schemas, &schema, false).await.unwrap();

        // Vérifier que les tables fr/en/de ne sont PAS créées en base
        let sibling_tables: i64 = client.query_one(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = $1 AND table_name IN \
             ('products_translations_fr','products_translations_en','products_translations_de')",
            &[&schema],
        ).await.unwrap().get(0);
        assert_eq!(sibling_tables, 0, "tables siblings ne doivent pas être créées");

        let p2 = pass2::runner::run(&path, "products", &schemas, &client, &schema, 1000, false, None, 1, None, None)
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
    }).await;
}

// ---------------------------------------------------------------------------
// WideStrategy::NormalizeDynamicKeys sur une table intermédiaire.
// Fixture : 3 produits avec un objet `images` à clés dynamiques (image IDs).
// Chaque clé mappe vers un objet {url, width}.
//
// Différence avec KeyedPivot : appliqué manuellement (pas auto-détecté),
// le nom de la colonne ID est libre ("image_id").
//
// sibling_threshold=10 → empêche l'auto-détection KeyedPivot (5 < 10)
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
    common::with_schema(|client, schema| async move {
        let path = common::fixture("normalize_dynamic_keys.jsonl");
        // sibling_threshold=10 : 5 tables images < 10 → pas d'auto-détection KeyedPivot
        let p1 = pass1::runner::run(&path, "products", 256, false, usize::MAX, 10, 0.5, 0.10, 0.001, None).unwrap();

        assert!(p1.schemas.iter().any(|s| s.name == "products_images"),
            "products_images doit exister après pass1");

        let mut schemas = p1.schemas;
        apply_normalize_dynamic_keys(&mut schemas, "products_images", "image_id".to_string());

        // Les 5 tables images enfants doivent être absorbées
        assert_eq!(schemas.len(), 2, "2 schemas attendus après absorption des enfants");
        assert!(schemas.iter().any(|s| s.name == "products"),        "products manquant");
        assert!(schemas.iter().any(|s| s.name == "products_images"), "products_images manquant");

        let images_schema = schemas.iter().find(|s| s.name == "products_images").unwrap();
        assert!(
            matches!(images_schema.wide_strategy, WideStrategy::NormalizeDynamicKeys { .. }),
            "products_images doit avoir la stratégie NormalizeDynamicKeys"
        );

        let data_cols: Vec<_> = images_schema.data_columns().collect();
        assert!(data_cols.iter().any(|c| c.name == "image_id"), "colonne image_id absente");
        assert!(data_cols.iter().any(|c| c.name == "url"),      "colonne url absente");
        assert!(data_cols.iter().any(|c| c.name == "width"),    "colonne width absente");

        db::ddl::create_tables(&client, &schemas, &schema, false).await.unwrap();

        let p2 = pass2::runner::run(&path, "products", &schemas, &client, &schema, 1000, false, None, 1, None, None)
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
