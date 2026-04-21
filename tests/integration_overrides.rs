mod common;

use json2sql::{db, pass1, pass2};
use json2sql::schema::config::{apply_overrides, SchemaConfig};

// ---------------------------------------------------------------------------
// Override de type valide : score FLOAT → TEXT.
// users.jsonl contient `score` comme float (9.5, 7.2, 8.8).
// override_score.toml force `score = "TEXT"` pour la table "people".
// Vérifie : type DDL via pg_catalog, valeurs exactes, IS NOT NULL.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_override_type_valid() {
    common::with_schema(|client, schema| async move {
        let path = common::fixture("users.jsonl");
        let mut p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

        // Pass1 doit inférer DoublePrecision AVANT override — prouve que l'override n'est pas un no-op.
        let pre_schema = p1.schemas.iter().find(|s| s.name == "people").unwrap();
        let pre_col = pre_schema.find_by_original("score").unwrap();
        assert!(
            matches!(&pre_col.pg_type, json2sql::schema::type_tracker::PgType::DoublePrecision),
            "Pass1 doit inférer DoublePrecision pour score avant override, obtenu {:?}", pre_col.pg_type
        );

        let config = SchemaConfig::from_file(&common::fixture("override_score.toml")).unwrap();
        apply_overrides(&mut p1.schemas, &config);

        db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();
        let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
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
    common::with_schema(|client, schema| async move {
        let path = common::fixture("users.jsonl");
        let mut p1 = pass1::runner::run(&path, "people", 256, false, usize::MAX, 3, 0.5, 0.10, 0.001, None).unwrap();

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

        db::ddl::create_tables(&client, &p1.schemas, &schema, false).await.unwrap();

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

        let p2 = pass2::runner::run(&path, "people", &p1.schemas, &client, &schema, 1000, false, None, 1, None, None)
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
