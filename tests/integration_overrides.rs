mod common;

use json2sql::{db, pass1, pass2};
use json2sql::schema::config::{apply_overrides, SchemaConfig};
use json2sql::schema::table_schema::InferredStrategy;

// ---------------------------------------------------------------------------
// Override de type valide : score FLOAT → TEXT.
// users.jsonl contient `score` comme float (9.5, 7.2, 8.8).
// override_score.toml force `score = "TEXT"` pour la table "people".
// Vérifie : type DDL via pg_catalog, valeurs exactes, IS NOT NULL.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_override_type_valid() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.jsonl");
        let mut p1 = pass1::runner::run(&path, &common::pass1_config("people"), None).unwrap();

        // Pass1 doit inférer DoublePrecision AVANT override — prouve que l'override n'est pas un no-op.
        let pre_schema = p1.schemas.iter().find(|s| s.name == "people").unwrap();
        let pre_col = pre_schema.find_by_original("score").unwrap();
        assert!(
            matches!(&pre_col.pg_type, json2sql::schema::type_tracker::PgType::DoublePrecision),
            "Pass1 doit inférer DoublePrecision pour score avant override, obtenu {:?}", pre_col.pg_type
        );

        let config = SchemaConfig::from_file(&common::fixture("override_score.toml")).unwrap();
        apply_overrides(&mut p1.schemas, &config);

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None).await.unwrap();
        let p2 = pass2::runner::run(&path, &p1.schemas, &client, &url, &common::pass2_config("people", &schema), None)
            .await.unwrap();

        assert_eq!(*p2.rows_per_table.get("people").unwrap(), 3);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 0,
            "no anomalies expected — score values are valid text");

        // score IS NOT NULL pour les 3 lignes : total_anomalies()==0 ne détecte pas un NULL silencieux.
        let not_null_count: i64 = client.query_one(
            &format!("SELECT COUNT(*) FROM \"{}\".\"people\" WHERE score IS NOT NULL", schema), &[])
            .await.unwrap().get(0);
        assert_eq!(not_null_count, 3, "score doit être NOT NULL pour les 3 lignes");

        // Valeurs exactes : float JSON → string, pas de représentation inattendue.
        for (name, expected_score) in [("Alice", "9.5"), ("Bob", "7.2"), ("Charlie", "8.8")] {
            let row = client.query_opt(
                &format!("SELECT score FROM \"{}\".\"people\" WHERE name = '{}'", schema, name), &[])
                .await.unwrap()
                .unwrap_or_else(|| panic!("ligne introuvable pour {}", name));
            let score: Option<String> = row.get("score");
            assert_eq!(score.as_deref(), Some(expected_score),
                "score de {} doit être '{}', obtenu {:?}", name, expected_score, score);
        }

        // Type DDL réel via pg_catalog.
        let type_sql = "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = 'people' AND a.attname = 'score' AND a.attnum > 0";
        let row = client.query_opt(type_sql, &[&schema]).await.unwrap()
            .expect("colonne 'score' introuvable dans pg_attribute — vérifier le naming sanitizer");
        let pg_type: String = row.get(0);
        assert_eq!(pg_type, "text", "score doit être TEXT après override, obtenu: {}", pg_type);
    }).await;
}

// ---------------------------------------------------------------------------
// Override invalide : score → INTEGER (floats → anomalies),
// colonne inexistante et table fantôme (warnings silencieux, no crash).
//
// override_bad.toml :
//   [people]  score = "INTEGER"    ← floats 9.5, 7.2, 8.8 → 3 anomalies coercion
//             nonexistent = "TEXT" ← colonne absente → warning silencieux
//   [ghost_table]  col = "TEXT"   ← table absente → warning silencieux
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_override_bad() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.jsonl");
        let mut p1 = pass1::runner::run(&path, &common::pass1_config("people"), None).unwrap();

        // Pass1 doit inférer DoublePrecision AVANT override — prouve que l'override n'est pas un no-op.
        let pre_schema = p1.schemas.iter().find(|s| s.name == "people").unwrap();
        let pre_col = pre_schema.find_by_original("score").unwrap();
        assert!(
            matches!(&pre_col.pg_type, json2sql::schema::type_tracker::PgType::DoublePrecision),
            "Pass1 doit inférer DoublePrecision pour score avant override, obtenu {:?}", pre_col.pg_type
        );

        let config = SchemaConfig::from_file(&common::fixture("override_bad.toml")).unwrap();
        apply_overrides(&mut p1.schemas, &config);

        let people_schema = p1.schemas.iter().find(|s| s.name == "people").unwrap();
        let score_col = people_schema.find_by_original("score").unwrap();
        assert!(
            matches!(&score_col.pg_type, json2sql::schema::type_tracker::PgType::Integer),
            "score doit être INTEGER après override, obtenu {:?}", score_col.pg_type
        );

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None).await.unwrap();

        // Type DDL réel en base AVANT Pass 2.
        let type_sql = "SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = 'people' AND a.attname = 'score' AND a.attnum > 0";
        let row = client.query_opt(type_sql, &[&schema]).await.unwrap()
            .expect("colonne 'score' introuvable dans pg_attribute — l'override DDL n'a pas été appliqué");
        let pg_type: String = row.get(0);
        assert_eq!(pg_type, "integer", "score doit être INTEGER en base, obtenu: {}", pg_type);

        let p2 = pass2::runner::run(&path, &p1.schemas, &client, &url, &common::pass2_config("people", &schema), None)
            .await.unwrap();

        // rows_per_table = compteur pipeline interne ; row_count = vérité DB via COUNT(*).
        // Les deux sont nécessaires : rows_per_table détecte un bug flush/batch,
        // row_count détecte une divergence silencieuse pipeline/réalité.
        assert_eq!(*p2.rows_per_table.get("people").unwrap(), 3);
        assert_eq!(common::row_count(&client, &schema, "people").await, 3);

        // Les 3 anomalies viennent exclusivement de people.score (float→INTEGER).
        // Les tables enfants (people_address, people_tags, etc.) n'ont pas de colonne score.
        let summaries = p2.anomaly_collector.summaries();
        assert_eq!(summaries.len(), 1,
            "une seule paire (table, col) en anomalie attendue, obtenu: {:?}",
            summaries.iter().map(|s| format!("{}.{}", s.table, s.column)).collect::<Vec<_>>());
        assert_eq!(summaries[0].table, "people");
        assert_eq!(summaries[0].column, "score");
        assert_eq!(summaries[0].anomaly_count, 3,
            "exactement 3 anomalies sur people.score, obtenu: {}", summaries[0].anomaly_count);

        // Coerceur strict : score IS NULL pour les 3 lignes (pas de trunc silencieux 9.5 → 9).
        let null_count: i64 = client.query_one(
            &format!("SELECT COUNT(*) FROM \"{}\".\"people\" WHERE score IS NULL", schema), &[])
            .await.unwrap().get(0);
        assert_eq!(null_count, 3, "score doit être NULL pour les 3 lignes, obtenu {} NULL", null_count);

        // L'override sur score ne corrompt pas les colonnes adjacentes.
        let alice_row = client
            .query_opt(&format!("SELECT name FROM \"{}\".\"people\" WHERE name = 'Alice'", schema), &[])
            .await.unwrap()
            .expect("ligne Alice introuvable — l'override a peut-être corrompu la table");
        assert_eq!(alice_row.get::<_, String>("name"), "Alice");

        // Les tables enfants reçoivent leurs lignes malgré l'override sur la table root.
        assert_eq!(common::row_count(&client, &schema, "people_address").await, 3,
            "people_address doit avoir 3 lignes (une par utilisateur)");
    }).await;
}

// ---------------------------------------------------------------------------
// [issue #45, tâche 8] Override manuel --schema-config forçant Pivot : le chemin
// plat existant (insert_pivot_object) doit rester inchangé — pas de split
// identité/compagnon (celui-ci ne s'applique qu'au pipeline automatique, cf.
// tâche 4 : apply_wide_strategy n'est jamais invoquée pour un override manuel,
// apply_user_overrides s'applique après finalize() sur le schéma déjà figé).
//
// Fixture : people_address (Object child de people, 2 colonnes street/city,
// Columns par défaut car sous le seuil wide) — override_pivot.toml force
// strategy = "pivot" dessus.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_override_strategy_pivot_flat_path_unchanged() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.jsonl");
        let mut p1 = pass1::runner::run(&path, &common::pass1_config("people"), None).unwrap();

        let pre_address = p1.schemas.iter().find(|s| s.name == "people_address").unwrap();
        assert_eq!(pre_address.inferred_strategy, InferredStrategy::Columns,
            "people_address doit être Columns avant override (2 colonnes, sous le seuil wide)");

        let config = SchemaConfig::from_file(&common::fixture("override_pivot.toml")).unwrap();
        let warnings = apply_overrides(&mut p1.schemas, &config).unwrap();
        assert!(warnings.is_empty(), "aucun warning attendu, obtenu : {warnings:?}");

        let address = p1.schemas.iter().find(|s| s.name == "people_address").unwrap();
        // ui_override prioritaire — inferred_strategy n'est jamais muté par un override manuel.
        assert_eq!(address.inferred_strategy, InferredStrategy::Columns, "inferred_strategy inchangé");
        assert_eq!(*address.effective_strategy(), InferredStrategy::Pivot);
        // Chemin plat : (key, value) sur LA MÊME table, pas de split identité/compagnon.
        assert_eq!(address.data_columns().count(), 2, "(key, value) — table plate, pas de split");
        assert!(
            !p1.schemas.iter().any(|s| s.name == "people_address_pivot"),
            "aucun compagnon _pivot ne doit être créé pour un override manuel"
        );

        let schemas = p1.schemas.clone();
        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None).await.unwrap();
        let p2 = pass2::runner::run(&path, &schemas, &client, &url, &common::pass2_config("people", &schema), None)
            .await.unwrap();

        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
        // street + city = 2 paires EAV par personne × 3 personnes = 6.
        assert_eq!(*p2.rows_per_table.get("people_address").unwrap(), 6);
        assert_eq!(common::row_count(&client, &schema, "people_address").await, 6);

        let alice_city: String = client.query_one(
            &format!(
                "SELECT a.value FROM \"{s}\".\"people_address\" a \
                 JOIN \"{s}\".\"people\" p ON a.j2s_people_id = p.j2s_id \
                 WHERE p.name = 'Alice' AND a.key = 'city'",
                s = schema,
            ),
            &[],
        ).await.unwrap().get(0);
        assert_eq!(alice_city, "Springfield");
    }).await;
}
