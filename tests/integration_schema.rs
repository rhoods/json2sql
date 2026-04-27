mod common;

// pass2 is used by all async tests in this file; pass1-only test_schema_inference_no_db
// does not use it but sharing the import avoids per-test redundancy.
use json2sql::{db, pass1, pass2};
use std::io::Write;

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
// Avec fix : all_pure → bypass du filtre → Jaccard = 1.0 → KeyedPivot.
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

    // Les tables individuelles uploaded.N doivent être absorbées.
    assert!(!names.iter().any(|n| n.starts_with("root_uploaded_")),
        "root_uploaded_N doivent être absorbés; schemas: {:?}", names);

    // Seuls root et root_uploaded doivent rester (petits-enfants exclus).
    assert_eq!(p1.schemas.len(), 2,
        "attendu 2 schemas (root + root_uploaded), obtenu: {:?}", names);
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
// Table NON-RACINE auto-convertie en JSONB par le column limit guard :
// - la colonne data doit contenir l'objet sérialisé (pas NULL)
// - les tables enfants de la table JSONB doivent toujours recevoir leurs lignes
//
// Structure : root → middle (converti JSONB) → leaf
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_column_limit_guard_jsonb_non_root_with_children() {
    common::with_schema(|client, schema| async move {
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
            use json2sql::schema::registry::apply_wide_strategy_columns;
            use json2sql::schema::table_schema::WideStrategy;
            if let Some(mid) = p1.schemas.iter_mut().find(|s| s.name == "root_middle") {
                apply_wide_strategy_columns(mid, WideStrategy::Jsonb);
            }
        }

        db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
        let p2 = pass2::runner::run(
            f.path(), "root", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None,
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
