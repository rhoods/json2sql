mod common;

// pass2 is used by all async tests in this file; pass1-only test_schema_inference_no_db
// does not use it but sharing the import avoids per-test redundancy.
use json2sql::{db, pass1, pass2};
use json2sql::db::copy_sink::{merge_copy_to_db, TempFileSink};
use json2sql::schema::table_schema::{ColumnSchema, TableSchema};
use json2sql::schema::type_tracker::PgType;
use std::io::Write;

// Build a minimal single-column TableSchema for merge_copy_to_db tests.
fn single_col_schema(table: &str) -> TableSchema {
    let mut schema = TableSchema::new(table.to_string(), vec![table.to_string()], 0);
    schema.columns.push(ColumnSchema {
        name: "v".to_string(),
        original_name: "v".to_string(),
        pg_type: PgType::Text,
        not_null: false,
        is_generated: false,
        is_parent_fk: false,
    });
    schema
}

#[tokio::test]
async fn test_nested_row_counts_json_array() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.json");
        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &url, &schema, 1, None, None, None)
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
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.jsonl");
        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &url, &schema, 1, None, None, None)
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
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.json");

        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        pass2::runner::run(&path, "users", &p1.schemas, &client, &url, &schema, 1, None, None, None)
            .await.unwrap();
        assert_eq!(common::row_count(&client, &schema, "users").await, 3);

        let p1b = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1b.schemas, &schema, true).await.unwrap();
        pass2::runner::run(&path, "users", &p1b.schemas, &client, &url, &schema, 1, None, None, None)
            .await.unwrap();

        assert_eq!(common::row_count(&client, &schema, "users").await, 3);
    }).await;
}

// ---------------------------------------------------------------------------
// Vérifie qu'un import réussi avec use_transaction=true committe bien les données.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_transaction_commit() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.json");
        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &url, &schema, 1, None, None, None)
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
// Keyed_pivot avec clés de formes mixtes (numériques + textuelles).
// Fixture : 2 produits avec un objet `images` qui a 3 clés numériques (schema
// {imgid, uploader}) et 3 clés textuelles (schema {imgid, rev}).
//
// Sans le fix : Jaccard global = 1/3 ≈ 0.33 < 0.5 → pas de pivot → 12 tables.
// Avec le fix  : meilleur Jaccard sous-groupe = 1.0 ≥ 0.5 → pivot → 2 tables.
// ---------------------------------------------------------------------------
// Keyed_pivot avec clés de formes mixtes (numériques + textuelles).
// Fixture : 2 produits avec un objet `images` qui a 3 clés numériques (schema
// {imgid, uploader}) et 3 clés textuelles (schema {imgid, rev}).
//
// Résultat attendu : MultiKeyedPivot — deux tables pivots distinctes.
//   products_images_num  ← absorbe les clés numériques (imgid, uploader)
//   products_images_key  ← absorbe les clés textuelles (imgid, rev)
// Les 12 enfants originaux sont exclus du schéma.
#[test]
fn test_keyed_pivot_mixed_key_shapes() {
    use json2sql::schema::table_schema::WideStrategy;

    let path = common::fixture("keyed_pivot_mixed_shape.jsonl");
    let p1 = pass1::runner::run(&path, "products", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    let names: Vec<&str> = p1.schemas.iter().map(|s| s.name.as_str()).collect();

    // products + products_images (parent MultiKeyedPivot) + 2 tables pivots synthétiques.
    assert_eq!(p1.schemas.len(), 4, "attendu 4 schemas, obtenu: {:?}", names);
    assert!(names.contains(&"products"),              "products manquant");
    assert!(names.contains(&"products_images"),       "products_images (parent) manquant");
    assert!(names.contains(&"products_images_num"),   "products_images_num (pivot numérique) manquant");
    assert!(names.contains(&"products_images_key"),   "products_images_key (pivot textuel) manquant");

    let images = p1.schemas.iter().find(|s| s.name == "products_images").unwrap();
    assert!(
        matches!(images.wide_strategy, WideStrategy::MultiKeyedPivot(_)),
        "products_images doit avoir MultiKeyedPivot"
    );

    // Table pivot numérique : key_id + imgid + uploader
    let num_pivot = p1.schemas.iter().find(|s| s.name == "products_images_num").unwrap();
    assert!(matches!(num_pivot.wide_strategy, WideStrategy::KeyedPivot(_)));
    let num_cols: Vec<&str> = num_pivot.data_columns().map(|c| c.name.as_str()).collect();
    assert!(num_cols.contains(&"imgid"),    "pivot numérique : imgid manquant");
    assert!(num_cols.contains(&"uploader"), "pivot numérique : uploader manquant");
    assert!(!num_cols.contains(&"rev"),     "pivot numérique ne doit pas avoir rev");

    // Table pivot textuelle : key + imgid + rev
    let key_pivot = p1.schemas.iter().find(|s| s.name == "products_images_key").unwrap();
    assert!(matches!(key_pivot.wide_strategy, WideStrategy::KeyedPivot(_)));
    let key_cols: Vec<&str> = key_pivot.data_columns().map(|c| c.name.as_str()).collect();
    assert!(key_cols.contains(&"imgid"),     "pivot textuel : imgid manquant");
    assert!(key_cols.contains(&"rev"),       "pivot textuel : rev manquant");
    assert!(!key_cols.contains(&"uploader"), "pivot textuel ne doit pas avoir uploader");
}

// ---------------------------------------------------------------------------
// T1: significant container in non-numeric group must not dilute Jaccard.
// T2: parent with data columns gets a synthetic pivot child.
//
// Fixture: images has 3 numeric-keyed tables (front_*, ingredients_*, nutrition_*)
// AND an "uploaded" sub-object that is itself a pure container with 3 numeric children.
// Expected:
//   - images → MultiKeyedPivot (text group = front/ingredients/nutrition, NOT uploaded)
//   - images_key → KeyedPivot for text children
//   - images_uploaded → remains as independent table (significant container)
//   - images_uploaded_num → KeyedPivot for the numeric uploaded children
// ---------------------------------------------------------------------------
#[test]
fn test_sibling_significant_container_not_diluting_jaccard() {
    use json2sql::schema::table_schema::WideStrategy;

    let path = common::fixture("sibling_significant_container.jsonl");
    let p1 = pass1::runner::run(&path, "root", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    let names: Vec<&str> = p1.schemas.iter().map(|s| s.name.as_str()).collect();

    // T1: images_uploaded must NOT have been absorbed into the text pivot.
    // It is a significant container (pure container with many children) — left independent.
    assert!(names.contains(&"root_images_uploaded"),
        "images_uploaded doit rester indépendant (significant container); schemas: {:?}", names);

    // T1: text pivot must exist (front/ingredients/nutrition collapsed, uploaded excluded).
    assert!(names.contains(&"root_images_key"),
        "root_images_key (pivot textuel) doit exister; schemas: {:?}", names);

    // T1: images parent — all text children → synthetic pivot → MultiKeyedPivot with one group.
    let images = p1.schemas.iter().find(|s| s.name == "root_images").unwrap();
    assert!(matches!(images.wide_strategy, WideStrategy::MultiKeyedPivot(_)),
        "root_images doit avoir MultiKeyedPivot; actual: {:?}", images.wide_strategy);

    // Pure container detection: images_uploaded is itself a pure container → classic KeyedPivot.
    // Its numeric children (100..108) are collapsed into it directly.
    let uploaded = p1.schemas.iter().find(|s| s.name == "root_images_uploaded").unwrap();
    assert!(matches!(uploaded.wide_strategy, WideStrategy::KeyedPivot(_)),
        "root_images_uploaded doit avoir KeyedPivot (pure container); actual: {:?}", uploaded.wide_strategy);
    let cols: Vec<&str> = uploaded.data_columns().map(|c| c.name.as_str()).collect();
    assert!(cols.contains(&"uploaded_t"), "KeyedPivot uploaded : uploaded_t manquant; cols: {:?}", cols);
    assert!(cols.contains(&"uploader"),   "KeyedPivot uploaded : uploader manquant; cols: {:?}", cols);

    // The individual numeric children must have been absorbed (excluded from schema).
    assert!(!names.iter().any(|n| n.starts_with("root_images_uploaded_1")),
        "root_images_uploaded_1xx doivent être absorbés; schemas: {:?}", names);
}

// ---------------------------------------------------------------------------
// Noise-filtered Jaccard : des colonnes rares dans quelques schemas siblings
// ne doivent pas empêcher le collapse du groupe.
//
// Fixture : 5 produits avec images.uploaded.{1,2,3} → {uploaded_t, uploader}
//           + 1 produit où uploaded.4 a en plus 10 colonnes parasites rares
//             (imgid, rev, angle, geometry, x1, x2, y1, y2, white_magic, normalize).
//
// Sans filtre : Jaccard(uploaded.1, uploaded.4) = 2/12 ≈ 0.17 < 0.5 → pas de collapse.
// Avec filtre : colonnes rares exclues → Jaccard = 1.0 → images_uploaded = KeyedPivot.
// ---------------------------------------------------------------------------
#[test]
fn test_sibling_noisy_schema_jaccard_filter() {
    use json2sql::schema::table_schema::WideStrategy;

    let path = common::fixture("sibling_noisy_schema.jsonl");
    let p1 = pass1::runner::run(&path, "root", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    let names: Vec<&str> = p1.schemas.iter().map(|s| s.name.as_str()).collect();

    // Le groupe {uploaded.1, uploaded.2, uploaded.3, uploaded.4} doit avoir collapser
    // malgré les colonnes parasites dans uploaded.4.
    let uploaded = p1.schemas.iter().find(|s| s.name == "root_images_uploaded");
    assert!(uploaded.is_some(),
        "root_images_uploaded doit exister; schemas: {:?}", names);
    let uploaded = uploaded.unwrap();
    assert!(matches!(uploaded.wide_strategy, WideStrategy::KeyedPivot(_)),
        "root_images_uploaded doit avoir KeyedPivot; actual: {:?}", uploaded.wide_strategy);

    // Les colonnes stables (uploaded_t, uploader) doivent être présentes.
    let cols: Vec<&str> = uploaded.data_columns().map(|c| c.name.as_str()).collect();
    assert!(cols.contains(&"uploaded_t"), "uploaded_t manquant; cols: {:?}", cols);
    assert!(cols.contains(&"uploader"),   "uploader manquant; cols: {:?}", cols);

    // Les tables individuelles uploaded.N doivent être absorbées.
    assert!(!names.iter().any(|n| n.starts_with("root_images_uploaded_")),
        "root_images_uploaded_N doivent être absorbés; schemas: {:?}", names);
}

// ---------------------------------------------------------------------------
// T2a: Siblings qui sont TOUS des pure containers avec >= threshold enfants
// (significant containers) doivent quand même collapser.
//
// Fixture : uploaded.{1,2,3} sont des pure containers, chacun avec 3 enfants
// (sizes, imgid, selected) → child_count = 3 >= threshold = 3 → sans fix :
// le filtre significant-container les élimine tous → regular vide → pas de collapse.
//
// Avec fix all-pure + fix child_routes : wave 0 fusionne uploaded.{1,2,3} en
// KeyedPivot, waves 1-2 créent les tables fusionnées pour les co-siblings et
// celles-ci survivent à exclude_absorbed_children via child_routes.
// ---------------------------------------------------------------------------
#[test]
fn test_sibling_all_pure_container_collapse() {
    use json2sql::schema::table_schema::WideStrategy;

    let path = common::fixture("sibling_all_pure_containers.jsonl");
    let p1 = pass1::runner::run(&path, "root", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    let names: Vec<&str> = p1.schemas.iter().map(|s| s.name.as_str()).collect();

    // root_uploaded doit exister et avoir KeyedPivot.
    let uploaded = p1.schemas.iter().find(|s| s.name == "root_uploaded");
    assert!(uploaded.is_some(),
        "root_uploaded doit exister; schemas: {:?}", names);
    assert!(matches!(uploaded.unwrap().wide_strategy, WideStrategy::KeyedPivot(_)),
        "root_uploaded doit avoir KeyedPivot; actual: {:?}", uploaded.unwrap().wide_strategy);

    // Les tables sibling individuelles (1, 2, 3) doivent être absorbées.
    for n in &["root_uploaded_1", "root_uploaded_2", "root_uploaded_3"] {
        assert!(!names.contains(n),
            "{} doit être absorbé; schemas: {:?}", n, names);
    }

    // Les tables de cascade (co-siblings fusionnés) doivent survivre.
    assert!(names.contains(&"root_uploaded_sizes"),
        "root_uploaded_sizes doit survivre (wave-1 merge); schemas: {:?}", names);
    assert!(names.contains(&"root_uploaded_imgid"),
        "root_uploaded_imgid doit survivre (wave-1 merge); schemas: {:?}", names);
    assert!(names.contains(&"root_uploaded_selected"),
        "root_uploaded_selected doit survivre (wave-1 merge); schemas: {:?}", names);
    assert!(names.contains(&"root_uploaded_sizes_100"),
        "root_uploaded_sizes_100 doit survivre (wave-2 merge); schemas: {:?}", names);

    // root_uploaded.child_routes doit pointer vers les tables fusionnées.
    let uploaded = uploaded.unwrap();
    assert_eq!(
        uploaded.child_routes.get("sizes").map(|s| s.as_str()),
        Some("root_uploaded_sizes"),
        "root_uploaded.child_routes[\"sizes\"] doit pointer vers root_uploaded_sizes"
    );

    // root + root_uploaded + sizes + imgid + selected + sizes_100 = 6.
    assert_eq!(p1.schemas.len(), 6,
        "attendu 6 schemas; obtenu: {:?}", names);
}

// ---------------------------------------------------------------------------
// Régression : les tables créées par cascade wave 1+ (co-sibling merge) ne
// doivent pas être exclues par exclude_absorbed_children même quand leur
// parent synthétique a KeyedPivot (absorbs_children = true).
// Les tables enregistrées dans child_routes doivent être protégées.
//
// Structure : root.front.{en,fr,de}.sizes = {w, h}
//   Wave 0 : front.{en,fr,de} → root_front (KeyedPivot)
//   Wave 1 : front.*.sizes → root_front_sizes (T, via child_routes)
//   Bug    : T était exclu car root_front.absorbs_children() = true
// ---------------------------------------------------------------------------
#[test]
fn test_cascade_wave1_child_route_target_survives_keyed_pivot_parent() {
    use json2sql::schema::table_schema::WideStrategy;

    let path = common::fixture("cascade_wave1_child_route_survives.jsonl");
    let p1 = pass1::runner::run(&path, "root", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    let names: Vec<&str> = p1.schemas.iter().map(|s| s.name.as_str()).collect();

    // root_front doit avoir KeyedPivot (wave 0 a fusionné en, fr, de).
    let front = p1.schemas.iter().find(|s| s.name == "root_front");
    assert!(front.is_some(), "root_front doit exister; schemas: {:?}", names);
    assert!(
        matches!(front.unwrap().wide_strategy, WideStrategy::KeyedPivot(_)),
        "root_front doit avoir KeyedPivot; actual: {:?}", front.unwrap().wide_strategy
    );

    // root_front_sizes doit survivre (target de child_routes, wave 1).
    let sizes = p1.schemas.iter().find(|s| s.name == "root_front_sizes");
    assert!(
        sizes.is_some(),
        "root_front_sizes doit survivre (child_route cible); schemas: {:?}", names
    );

    // root_front_sizes doit avoir les colonnes w et h (union des co-siblings).
    let sizes = sizes.unwrap();
    let cols: Vec<&str> = sizes.data_columns().map(|c| c.name.as_str()).collect();
    assert!(cols.contains(&"w"), "w manquant dans root_front_sizes; cols: {:?}", cols);
    assert!(cols.contains(&"h"), "h manquant dans root_front_sizes; cols: {:?}", cols);

    // root_front.child_routes["sizes"] doit pointer vers root_front_sizes.
    let front = front.unwrap();
    assert_eq!(
        front.child_routes.get("sizes").map(|s| s.as_str()),
        Some("root_front_sizes"),
        "root_front.child_routes[\"sizes\"] doit pointer vers root_front_sizes"
    );

    // Tables individuelles absorbées.
    for n in &["root_front_en", "root_front_fr", "root_front_de",
               "root_front_en_sizes", "root_front_fr_sizes", "root_front_de_sizes"] {
        assert!(!names.contains(n), "{} doit être absorbé; schemas: {:?}", n, names);
    }

    // root + root_front + root_front_sizes = 3.
    assert_eq!(p1.schemas.len(), 3,
        "attendu 3 schemas; obtenu: {:?}", names);
}

// ---------------------------------------------------------------------------
// T2a: Un pure container non-significatif dans un groupe data-bearing ne doit
// pas faire tomber le Jaccard à 0 et bloquer le collapse.
//
// Fixture : uploaded.{1,2,3} data-bearing (x, y) + uploaded.4 pure container
// avec 1 seul enfant (inner → non-significant). Sans fix : Jaccard = 0 car 4
// est pur → pas de collapse. Avec fix : Jaccard calculé sur data_bearing
// uniquement → 1.0 → collapse de tous (1, 2, 3, 4).
// ---------------------------------------------------------------------------
#[test]
fn test_sibling_pure_diluter_absorbed() {
    use json2sql::schema::table_schema::WideStrategy;

    let path = common::fixture("sibling_pure_diluter.jsonl");
    let p1 = pass1::runner::run(&path, "root", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    let names: Vec<&str> = p1.schemas.iter().map(|s| s.name.as_str()).collect();

    // root_uploaded doit avoir KeyedPivot.
    let uploaded = p1.schemas.iter().find(|s| s.name == "root_uploaded");
    assert!(uploaded.is_some(),
        "root_uploaded doit exister; schemas: {:?}", names);
    let uploaded = uploaded.unwrap();
    assert!(matches!(uploaded.wide_strategy, WideStrategy::KeyedPivot(_)),
        "root_uploaded doit avoir KeyedPivot; actual: {:?}", uploaded.wide_strategy);

    // Les colonnes data-bearing (x, y) doivent être présentes.
    let cols: Vec<&str> = uploaded.data_columns().map(|c| c.name.as_str()).collect();
    assert!(cols.contains(&"x"), "x manquant dans root_uploaded; cols: {:?}", cols);
    assert!(cols.contains(&"y"), "y manquant dans root_uploaded; cols: {:?}", cols);

    // Tous les enfants (1, 2, 3, 4) doivent être absorbés.
    assert!(!names.iter().any(|n| n.starts_with("root_uploaded_")),
        "root_uploaded_N doivent être absorbés; schemas: {:?}", names);
}

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// T3: is_mixed fallback unifié — quand ni le sous-groupe numérique (< threshold)
// ni le sous-groupe texte (< threshold) n'est suffisant, mais que le groupe
// combiné l'est, collapser en un seul KeyedPivot avec key TEXT.
//
// Fixture : sizes.{100, 400} (numériques, 2 < threshold=3) + sizes.{full}
// (texte, 1 < threshold). Séparément : ni num_ok ni non_ok. Combiné : 3
// tables, toutes avec {w, h} → Jaccard = 1.0 → KeyedPivot unifié.
// ---------------------------------------------------------------------------
#[test]
fn test_sibling_mixed_unified_fallback() {
    use json2sql::schema::table_schema::WideStrategy;

    let path = common::fixture("sibling_mixed_unified_fallback.jsonl");
    let p1 = pass1::runner::run(&path, "root", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

    let names: Vec<&str> = p1.schemas.iter().map(|s| s.name.as_str()).collect();

    // root_sizes doit exister et avoir KeyedPivot (pas MultiKeyedPivot).
    let sizes = p1.schemas.iter().find(|s| s.name == "root_sizes");
    assert!(sizes.is_some(),
        "root_sizes doit exister; schemas: {:?}", names);
    let sizes = sizes.unwrap();
    assert!(matches!(sizes.wide_strategy, WideStrategy::KeyedPivot(_)),
        "root_sizes doit avoir KeyedPivot (fallback unifié); actual: {:?}", sizes.wide_strategy);

    // Les colonnes w et h doivent être présentes.
    let cols: Vec<&str> = sizes.data_columns().map(|c| c.name.as_str()).collect();
    assert!(cols.contains(&"w"), "w manquant dans root_sizes; cols: {:?}", cols);
    assert!(cols.contains(&"h"), "h manquant dans root_sizes; cols: {:?}", cols);

    // Toutes les tables enfants (100, 400, full) doivent être absorbées.
    assert!(!names.iter().any(|n| n.starts_with("root_sizes_")),
        "root_sizes_N doivent être absorbés; schemas: {:?}", names);

    // Seuls root et root_sizes doivent rester.
    assert_eq!(p1.schemas.len(), 2,
        "attendu 2 schemas (root + root_sizes), obtenu: {:?}", names);
}

// ---------------------------------------------------------------------------
// Pass 1 parallèle doit produire le même schéma que séquentiel — NDJSON.
// ---------------------------------------------------------------------------
#[test]
fn test_schema_inference_parallel_parity_ndjson() {
    let path = common::fixture("users.jsonl");

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
        }
    }
}

// ---------------------------------------------------------------------------
// run_parallel doit retourner une erreur si un élément racine n'est pas un objet.
// ---------------------------------------------------------------------------
#[test]
fn test_parallel_non_object_root_returns_error() {
    use std::io::Write;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    // JSON array whose second element is a number, not an object.
    f.write_all(b"[{\"a\": 1}, 42, {\"b\": 2}]").unwrap();
    f.flush().unwrap();

    let result = pass1::runner::run_parallel(
        f.path(), "root", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None, 2,
    );
    match result {
        Err(e) => assert!(e.to_string().contains("root level"),
            "error message should mention root level: {}", e),
        Ok(_) => panic!("expected Err for non-object root element, got Ok"),
    };
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
    common::with_schema_url(|client, schema, url| async move {
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

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &url, &schema, 1, None, None, None)
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
// Table NON-RACINE auto-convertie en JSONB par le column limit guard :
// - la colonne data doit contenir l'objet sérialisé (pas NULL)
// - les tables enfants de la table JSONB doivent toujours recevoir leurs lignes
//
// Structure : root → middle (converti JSONB) → leaf
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_column_limit_guard_jsonb_non_root_with_children() {
    common::with_schema_url(|client, schema, url| async move {
        // root → middle (3 champs scalaires + 1 objet enfant leaf)
        // On force Jsonb sur "root_middle" (non-racine, a un parent) pour tester
        // le chemin Pass 2 qui était manquant.
        let json = br#"[
            {"id": 1, "middle": {"d": 4, "e": 5, "f": 6, "leaf": {"g": 7}}},
            {"id": 2, "middle": {"d": 8, "e": 9, "f": 10, "leaf": {"g": 11}}}
        ]"#;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(json).unwrap();
        f.flush().unwrap();

        let mut p1 = pass1::runner::run(
            f.path(), "root", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None,
        ).unwrap();

        // Force Jsonb sur "root_middle" (non-racine — a un parent_table = "root").
        {
            use json2sql::schema::wide_strategies::apply_wide_strategy_columns;
            use json2sql::schema::table_schema::WideStrategy;
            if let Some(mid) = p1.schemas.iter_mut().find(|s| s.name == "root_middle") {
                apply_wide_strategy_columns(mid, WideStrategy::Jsonb);
            }
        }

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        let p2 = pass2::runner::run(
            f.path(), "root", &p1.schemas, &client, &url, &schema, 1, None, None, None,
        ).await.unwrap();

        assert_eq!(common::row_count(&client, &schema, "root").await, 2);
        assert_eq!(common::row_count(&client, &schema, "root_middle").await, 2,
            "JSONB non-root table must have 2 rows");

        // La colonne data ne doit pas être NULL
        let sql = format!(
            "SELECT COUNT(*) FROM \"{}\".\"root_middle\" WHERE \"data\" IS NOT NULL",
            schema
        );
        let row = client.query_one(&sql, &[]).await.unwrap();
        let non_null: i64 = row.get(0);
        assert_eq!(non_null, 2, "data column must not be NULL for non-root JSONB table");

        // L'enfant de la table JSONB doit toujours recevoir ses lignes
        assert_eq!(common::row_count(&client, &schema, "root_middle_leaf").await, 2,
            "children of JSONB non-root table must still receive their rows");

        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
        let _ = p2;
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
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.json");
        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();

        let p2 = pass2::runner::run(
            &path, "users", &p1.schemas, &client, &url, &schema, 3, None, None, None,
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

#[tokio::test]
async fn test_pass2_timing_fields_populated() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.json");
        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        let p2 = pass2::runner::run(&path, "users", &p1.schemas, &client, &url, &schema, 1, None, None, None)
            .await.unwrap();

        // Identity sanity — total_ms() is defined as the sum of both phases.
        assert_eq!(p2.timing.total_ms(), p2.timing.streaming_ms + p2.timing.copy_ms);
        // Fields must be populated: a real COPY to PostgreSQL always takes ≥ 1 ms.
        assert!(p2.timing.copy_ms > 0, "copy_ms must be > 0 after a real COPY to PostgreSQL");
    }).await;
}

// ---------------------------------------------------------------------------
// Parallel streaming produces identical row counts to sequential (parallel=1).
// Runs two imports on separate schemas and compares per-table row totals.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_parallel_streaming_matches_sequential() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.json");
        let p1 = pass1::runner::run(
            &path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None,
        ).unwrap();

        // Sequential run on the schema provided by with_schema_url
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        let seq = pass2::runner::run(
            &path, "users", &p1.schemas, &client, &url, &schema, 1, None, None, None,
        ).await.unwrap();

        // Parallel streaming run on a second schema
        let schema2 = common::unique_schema();
        let client2 = db::connection::connect(&url).await.unwrap();
        client2.execute(&format!("CREATE SCHEMA \"{}\"", schema2), &[]).await.unwrap();
        db::ddl::create_tables_no_constraints(&client2, &p1.schemas, &schema2, false).await.unwrap();
        let par = pass2::runner::run(
            &path, "users", &p1.schemas, &client2, &url, &schema2, 2, None, None, None,
        ).await.unwrap();
        common::drop_schema(&client2, &schema2).await;

        for (table, &seq_count) in &seq.rows_per_table {
            let par_count = par.rows_per_table.get(table).copied().unwrap_or(0);
            assert_eq!(
                seq_count, par_count,
                "table {table}: seq={seq_count} par={par_count}"
            );
        }
        assert_eq!(par.timing.total_ms(), par.timing.streaming_ms + par.timing.copy_ms);
        assert_eq!(par.anomaly_collector.total_anomalies(), 0);
    }).await;
}

// ---------------------------------------------------------------------------
// merge_copy_to_db — integration tests
// Each test creates a one-column table in a temporary schema and verifies that
// merge_copy_to_db delivers exactly the expected rows to PostgreSQL.
// ---------------------------------------------------------------------------

/// Single sink, all rows in pending (no disk spill): all rows must arrive in PG.
#[tokio::test]
async fn test_merge_copy_single_sink_pending_only() {
    common::with_schema(|client, schema| async move {
        client.execute(&format!("CREATE TABLE \"{schema}\".\"t\" (v TEXT)"), &[]).await.unwrap();
        let ts = single_col_schema("t");
        let mut sink = TempFileSink::new(&ts, &schema).unwrap();
        sink.write_row(b"hello\n".to_vec()).unwrap();
        sink.write_row(b"world\n".to_vec()).unwrap();
        sink.write_row(b"rust\n".to_vec()).unwrap();

        let rows = merge_copy_to_db(vec![sink], &client).await.unwrap();
        assert_eq!(rows, 3);
        assert_eq!(common::row_count(&client, &schema, "t").await, 3);
    }).await;
}

/// Single sink whose pending buffer exceeds SPILL_THRESHOLD (256 KB) before merging:
/// the spilled data must arrive in PG via the temp file path.
#[tokio::test]
async fn test_merge_copy_single_sink_with_spill() {
    common::with_schema(|client, schema| async move {
        client.execute(&format!("CREATE TABLE \"{schema}\".\"t\" (v TEXT)"), &[]).await.unwrap();
        let ts = single_col_schema("t");
        let mut sink = TempFileSink::new(&ts, &schema).unwrap();

        // Write one row larger than SPILL_THRESHOLD (256 KiB) to force a disk spill,
        // then a small second row that stays in pending.
        let mut big: Vec<u8> = vec![b'a'; 260 * 1024];
        big.push(b'\n');
        sink.write_row(big).unwrap();
        sink.write_row(b"small\n".to_vec()).unwrap();

        // After the big write, the sink must have spilled at least once (FD is open).
        assert!(sink.is_open(), "expected a disk spill before merging");

        let rows = merge_copy_to_db(vec![sink], &client).await.unwrap();
        assert_eq!(rows, 2);
        assert_eq!(common::row_count(&client, &schema, "t").await, 2);
    }).await;
}

/// Multiple sinks for the same table: all rows from every sink must land in PG
/// via a single COPY session (the core merge behaviour).
#[tokio::test]
async fn test_merge_copy_multiple_sinks_same_table() {
    common::with_schema(|client, schema| async move {
        client.execute(&format!("CREATE TABLE \"{schema}\".\"t\" (v TEXT)"), &[]).await.unwrap();
        let ts = single_col_schema("t");

        let mut s1 = TempFileSink::new(&ts, &schema).unwrap();
        s1.write_row(b"a\n".to_vec()).unwrap();
        s1.write_row(b"b\n".to_vec()).unwrap();

        let mut s2 = TempFileSink::new(&ts, &schema).unwrap();
        s2.write_row(b"c\n".to_vec()).unwrap();

        let mut s3 = TempFileSink::new(&ts, &schema).unwrap();
        s3.write_row(b"d\n".to_vec()).unwrap();
        s3.write_row(b"e\n".to_vec()).unwrap();
        s3.write_row(b"f\n".to_vec()).unwrap();

        let rows = merge_copy_to_db(vec![s1, s2, s3], &client).await.unwrap();
        assert_eq!(rows, 6);
        assert_eq!(common::row_count(&client, &schema, "t").await, 6);
    }).await;
}

/// Pass2Flush events must be emitted after each COPY completes and their row
/// counts must match the data actually in PostgreSQL across all generated tables.
#[tokio::test]
async fn test_pass2_flush_events_emitted_after_copy() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.jsonl");
        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();

        let (ptx, mut prx) = tokio::sync::mpsc::unbounded_channel::<json2sql::io::progress_event::ProgressEvent>();
        pass2::runner::run(&path, "users", &p1.schemas, &client, &url, &schema, 2, None, Some(ptx), None)
            .await.unwrap();

        // Collect all Pass2Flush events — typed, no string parsing.
        let mut flush_rows: u64 = 0;
        while let Ok(event) = prx.try_recv() {
            if let json2sql::io::progress_event::ProgressEvent::Pass2Flush { rows_flushed, .. } = event {
                flush_rows += rows_flushed;
            }
        }

        // Sum all rows across every table in the schema — no hardcoded table list.
        let table_names: Vec<String> = client
            .query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = $1 AND table_type = 'BASE TABLE'",
                &[&schema],
            )
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect();

        let mut total_pg: u64 = 0;
        for table in &table_names {
            total_pg += common::row_count(&client, &schema, table).await as u64;
        }

        assert!(flush_rows > 0, "expected at least one Pass2Flush event");
        assert_eq!(flush_rows, total_pg,
            "Pass2Flush row counts must match actual PG rows after COPY");
    }).await;
}

/// Empty sink list returns Ok(0) without touching the database.
#[tokio::test]
async fn test_merge_copy_empty_sinks_returns_zero() {
    common::with_schema(|client, schema| async move {
        client.execute(&format!("CREATE TABLE \"{schema}\".\"t\" (v TEXT)"), &[]).await.unwrap();
        let rows = merge_copy_to_db(vec![], &client).await.unwrap();
        assert_eq!(rows, 0);
        assert_eq!(common::row_count(&client, &schema, "t").await, 0);
    }).await;
}

/// Sinks with row_count == 0 (and total_flushed == 0) are skipped transparently.
#[tokio::test]
async fn test_merge_copy_skips_empty_sinks_among_non_empty() {
    common::with_schema(|client, schema| async move {
        client.execute(&format!("CREATE TABLE \"{schema}\".\"t\" (v TEXT)"), &[]).await.unwrap();
        let ts = single_col_schema("t");

        let empty = TempFileSink::new(&ts, &schema).unwrap();
        let mut full = TempFileSink::new(&ts, &schema).unwrap();
        full.write_row(b"x\n".to_vec()).unwrap();

        let rows = merge_copy_to_db(vec![empty, full], &client).await.unwrap();
        assert_eq!(rows, 1);
        assert_eq!(common::row_count(&client, &schema, "t").await, 1);
    }).await;
}

/// When tables don't exist, all COPYs fail: run() must return Err AND emit at
/// least one Pass2Error event before returning, so the IHM is notified in real time.
#[tokio::test]
async fn test_pass2_error_event_emitted_on_copy_failure() {
    common::with_schema_url(|_client, schema, url| async move {
        let path = common::fixture("users.jsonl");
        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

        // Intentionally skip DDL — tables don't exist, every COPY will fail.
        let fresh = json2sql::db::connection::connect(&url).await.unwrap();
        let (ptx, mut prx) = tokio::sync::mpsc::unbounded_channel::<json2sql::io::progress_event::ProgressEvent>();
        let result = pass2::runner::run(&path, "users", &p1.schemas, &fresh, &url, &schema, 2, None, Some(ptx), None)
            .await;

        assert!(result.is_err(), "expected run() to fail when tables don't exist");

        let mut had_error_event = false;
        while let Ok(event) = prx.try_recv() {
            if let json2sql::io::progress_event::ProgressEvent::Pass2Error { .. } = event {
                had_error_event = true;
            }
        }
        assert!(had_error_event, "expected at least one Pass2Error event when COPY fails");
    }).await;
}

// ---------------------------------------------------------------------------
// Byte budget drain-max: ram_pressure_pct = Some(100) disables RAM pressure so
// only the byte-budget branch can fire. With the small test fixture the threshold
// (512 MiB / parallel) is never reached, but this validates the else-if branch
// compiles and runs correctly and catches any panic/data-loss regression.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_byte_budget_drain_max_correctness() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.json");
        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        let p2 = pass2::runner::run(
            &path, "users", &p1.schemas, &client, &url, &schema, 2, None, None, Some(100),
        ).await.unwrap();
        assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
        assert_eq!(*p2.rows_per_table.get("users_address").unwrap(), 3);
        assert_eq!(*p2.rows_per_table.get("users_tags").unwrap(), 6);
        assert_eq!(common::row_count(&client, &schema, "users").await, 3);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
    }).await;
}

// ---------------------------------------------------------------------------
// RAM pressure: force_spill in-place — no handoff, sink stays in worker.
// ram_pressure_pct = Some(0) forces pressure unconditionally (0% threshold →
// any RSS > 0 triggers). The initial check fires before the first dispatch so
// workers see the flag from their very first object even on small fixtures.
// Verifies data integrity: all tables get the correct row counts despite
// constant in-place force_spill on every worker iteration.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_ram_pressure_force_spill_in_place() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.json");
        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        let p2 = pass2::runner::run(
            &path, "users", &p1.schemas, &client, &url, &schema, 2, None, None, Some(0),
        ).await.unwrap();

        // All nested tables must have correct counts despite constant RAM pressure.
        assert_eq!(*p2.rows_per_table.get("users").unwrap(), 3);
        assert_eq!(*p2.rows_per_table.get("users_address").unwrap(), 3);
        assert_eq!(*p2.rows_per_table.get("users_tags").unwrap(), 6);
        assert_eq!(*p2.rows_per_table.get("users_orders").unwrap(), 3);
        assert_eq!(*p2.rows_per_table.get("users_orders_items").unwrap(), 3);
        assert_eq!(common::row_count(&client, &schema, "users").await, 3);
        assert_eq!(common::row_count(&client, &schema, "users_tags").await, 6);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
    }).await;
}

// ---------------------------------------------------------------------------
// Guard: si des tables racines sont non-vides avant l'import, Pass 2 doit
// émettre un Pass2Log WARNING et continuer normalement (les données sont appendées).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_warn_on_nonempty_root_table_before_import() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.jsonl");
        let p1 = pass1::runner::run(&path, "users", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

        // Premier import — remplit la table racine "users" avec 3 lignes.
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false).await.unwrap();
        pass2::runner::run(&path, "users", &p1.schemas, &client, &url, &schema, 1, None, None, None)
            .await.unwrap();
        assert_eq!(common::row_count(&client, &schema, "users").await, 3);

        // Second import — la table racine est non-vide, un warning doit être émis.
        let (ptx, mut prx) = tokio::sync::mpsc::unbounded_channel::<json2sql::io::progress_event::ProgressEvent>();
        let result = pass2::runner::run(
            &path, "users", &p1.schemas, &client, &url, &schema, 1, None, Some(ptx), None,
        ).await;

        // Doit toujours réussir (warning, pas erreur).
        assert!(result.is_ok(), "Pass 2 doit réussir même si la table racine est non-vide");

        // Doit avoir émis un Pass2Log WARNING mentionnant "users".
        let mut had_warning = false;
        while let Ok(event) = prx.try_recv() {
            if let json2sql::io::progress_event::ProgressEvent::Pass2Log(msg) = event {
                if msg.contains("WARNING") && msg.contains("users") {
                    had_warning = true;
                }
            }
        }
        assert!(had_warning, "expected a Pass2Log WARNING about non-empty root table 'users'");

        // Les données ont été appendées — 6 lignes au total.
        assert_eq!(
            common::row_count(&client, &schema, "users").await,
            6,
            "rows should be doubled after second import (append, not replace)"
        );
    }).await;
}
