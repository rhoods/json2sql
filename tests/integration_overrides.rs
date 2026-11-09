#![allow(clippy::disallowed_methods)]
mod common;

use json2sql::schema::config::{apply_overrides, SchemaConfig};
use json2sql::schema::table_schema::{InferredStrategy, TableSchema};
use json2sql::{db, pass1, pass2};

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
        apply_overrides(&mut p1.schemas, &config).unwrap();

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None).await.unwrap();
        let p2 = pass2::runner::run(&path, &p1.schemas, &client, &url, &common::pass2_config("people", &schema), None)
            .await.unwrap();

        assert_eq!(*p2.rows_per_table.get("people").unwrap(), 3);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 0,
            "no anomalies expected — score values are valid text");

        // score IS NOT NULL pour les 3 lignes : total_anomalies()==0 ne détecte pas un NULL silencieux.
        let not_null_count: i64 = client.query_one(
            &format!("SELECT COUNT(*) FROM \"{schema}\".\"people\" WHERE score IS NOT NULL"), &[])
            .await.unwrap().get(0);
        assert_eq!(not_null_count, 3, "score doit être NOT NULL pour les 3 lignes");

        // Valeurs exactes : float JSON → string, pas de représentation inattendue.
        for (name, expected_score) in [("Alice", "9.5"), ("Bob", "7.2"), ("Charlie", "8.8")] {
            let row = client.query_opt(
                &format!("SELECT score FROM \"{schema}\".\"people\" WHERE name = '{name}'"), &[])
                .await.unwrap()
                .unwrap_or_else(|| panic!("ligne introuvable pour {name}"));
            let score: Option<String> = row.get("score");
            assert_eq!(score.as_deref(), Some(expected_score),
                "score de {name} doit être '{expected_score}', obtenu {score:?}");
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
        assert_eq!(pg_type, "text", "score doit être TEXT après override, obtenu: {pg_type}");
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
        apply_overrides(&mut p1.schemas, &config).unwrap();

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
        assert_eq!(pg_type, "integer", "score doit être INTEGER en base, obtenu: {pg_type}");

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
            &format!("SELECT COUNT(*) FROM \"{schema}\".\"people\" WHERE score IS NULL"), &[])
            .await.unwrap().get(0);
        assert_eq!(null_count, 3, "score doit être NULL pour les 3 lignes, obtenu {null_count} NULL");

        // L'override sur score ne corrompt pas les colonnes adjacentes.
        let alice_row = client
            .query_opt(&format!("SELECT name FROM \"{schema}\".\"people\" WHERE name = 'Alice'"), &[])
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

        let pre_address = p1
            .schemas
            .iter()
            .find(|s| s.name == "people_address")
            .unwrap();
        assert_eq!(
            pre_address.inferred_strategy,
            InferredStrategy::Columns,
            "people_address doit être Columns avant override (2 colonnes, sous le seuil wide)"
        );

        let config = SchemaConfig::from_file(&common::fixture("override_pivot.toml")).unwrap();
        let warnings = apply_overrides(&mut p1.schemas, &config).unwrap();
        assert!(
            warnings.is_empty(),
            "aucun warning attendu, obtenu : {warnings:?}"
        );

        let address = p1
            .schemas
            .iter()
            .find(|s| s.name == "people_address")
            .unwrap();
        // ui_override prioritaire — inferred_strategy n'est jamais muté par un override manuel.
        assert_eq!(
            address.inferred_strategy,
            InferredStrategy::Columns,
            "inferred_strategy inchangé"
        );
        assert_eq!(*address.effective_strategy(), InferredStrategy::Pivot);
        // Chemin plat : (key, value) sur LA MÊME table, pas de split identité/compagnon.
        assert_eq!(
            address.data_columns().count(),
            2,
            "(key, value) — table plate, pas de split"
        );
        assert!(
            !p1.schemas.iter().any(|s| s.name == "people_address_pivot"),
            "aucun compagnon _pivot ne doit être créé pour un override manuel"
        );

        let schemas = p1.schemas.clone();
        db::ddl::create_tables_no_constraints(&client, &schemas, &schema, false, None)
            .await
            .unwrap();
        let p2 = pass2::runner::run(
            &path,
            &schemas,
            &client,
            &url,
            &common::pass2_config("people", &schema),
            None,
        )
        .await
        .unwrap();

        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
        // street + city = 2 paires EAV par personne × 3 personnes = 6.
        assert_eq!(*p2.rows_per_table.get("people_address").unwrap(), 6);
        assert_eq!(
            common::row_count(&client, &schema, "people_address").await,
            6
        );

        let alice_city: String = client
            .query_one(
                &format!(
                    "SELECT a.value FROM \"{schema}\".\"people_address\" a \
                 JOIN \"{schema}\".\"people\" p ON a.j2s_people_id = p.j2s_id \
                 WHERE p.name = 'Alice' AND a.key = 'city'",
                ),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(alice_city, "Springfield");
    })
    .await;
}

// ---------------------------------------------------------------------------
// [issue #130] Override Jsonb de l'IHM (snapshot rechargé) sur la racine : la table
// enfant people_address est absorbée par le blob JSONB du parent. Sans
// exclude_absorbed_children, la Pass 2 l'écrivait AUSSI dans people_address (doublon).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_ui_jsonb_on_root_does_not_duplicate_children() {
    use json2sql::schema::config::apply_user_overrides;
    use json2sql::schema::table_schema::UserOverride;
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("users.jsonl");
        let mut p1 = pass1::runner::run(&path, &common::pass1_config("people"), None).unwrap();
        let overrides =
            std::collections::HashMap::from([("people".to_string(), UserOverride::Jsonb)]);
        apply_user_overrides(&mut p1.schemas, &overrides);

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();
        pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &common::pass2_config("people", &schema),
            None,
        )
        .await
        .unwrap();

        let address_rows: Option<i64> = if p1.schemas.iter().any(|s| s.name == "people_address") {
            Some(common::row_count(&client, &schema, "people_address").await)
        } else {
            None
        };
        assert_eq!(
            address_rows, None,
            "people_address must not exist: its data is already in people.data"
        );
        let with_address: i64 = client
            .query_one(
                &format!("SELECT count(*) FROM \"{schema}\".\"people\" WHERE data ? 'address'"),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(with_address, 3, "every people.data blob keeps its address");
    })
    .await;
}

// ---------------------------------------------------------------------------
// [issue #171] Groupe TOML keyed_pivot : people_home et people_work fusionnées dans
// people_addresses. Chaque objet home / work donne une ligne du groupe, avec sa clé et
// sa personne. Avant #171, la Pass 2 cherchait people.home et people.work par leur
// chemin : table du groupe vide, une anomalie « table » par objet sur people.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_toml_group_keyed_pivot_fills_the_group_table() {
    use json2sql::schema::config::apply_overrides_complete;
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("group_keyed_pivot.jsonl");
        let mut p1 = pass1::runner::run(&path, &common::pass1_config("people"), None).unwrap();
        let config = SchemaConfig::from_file(&common::fixture("group_keyed_pivot.toml")).unwrap();
        assert_eq!(
            apply_overrides_complete(&mut p1.schemas, &config).unwrap(),
            []
        );

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();
        let p2 = pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &common::pass2_config("people", &schema),
            None,
        )
        .await
        .unwrap();

        let anomalies: Vec<(String, String, String)> = p2
            .anomaly_collector
            .summaries()
            .into_iter()
            .map(|s| (s.table, s.column, s.expected_type))
            .collect();
        assert_eq!(anomalies, [], "home and work have the group table");
        let rows: Vec<(String, String, String, i64)> = client
            .query(
                &format!(
                    "SELECT p.name, g.key_id, g.city, g.floor::bigint \
                     FROM \"{schema}\".\"people_addresses\" g \
                     JOIN \"{schema}\".\"people\" p ON g.j2s_people_id = p.j2s_id \
                     ORDER BY p.name, g.key_id"
                ),
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
            .collect();
        let expected = [
            ("Alice", "home", "Paris", 3),
            ("Alice", "work", "Lyon", 7),
            ("Bob", "home", "Nantes", 1),
            ("Bob", "work", "Lille", 2),
            ("Carol", "home", "Rennes", 5),
        ]
        .map(|(n, k, c, f)| (n.to_string(), k.to_string(), c.to_string(), f));
        assert_eq!(
            rows, expected,
            "one row per home / work object, keyed by its member"
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// [issue #117] `suffix_columns` du TOML sur des clés texte (`fat_unit`, `sodium_unit`…) :
// la colonne `unit` du pivot est typée d'après les types déjà résolus de ces clés. Avant
// #117, leur longueur était perdue : `unit` sortait en VARCHAR(1), et la Pass 2 mettait
// à NULL (anomalie `string_too_long`) toute unité de plus d'une lettre (`mg`, `kcal`).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_toml_suffix_columns_keep_every_unit() {
    use json2sql::schema::config::apply_overrides_complete;
    use json2sql::schema::type_tracker::PgType;
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("suffix_columns_units.jsonl");
        let mut p1 = pass1::runner::run(&path, &common::pass1_config("products"), None).unwrap();
        let config =
            SchemaConfig::from_file(&common::fixture("suffix_columns_units.toml")).unwrap();
        assert_eq!(
            apply_overrides_complete(&mut p1.schemas, &config).unwrap(),
            []
        );

        let pivot = p1
            .schemas
            .iter()
            .find(|s| s.name == "products_nutriments_pivot")
            .expect("suffix_columns gives products_nutriments its _pivot companion");
        let unit = &pivot.find_by_original("_unit").unwrap().pg_type;
        assert!(
            matches!(unit, PgType::Text) || matches!(unit, PgType::VarChar(n) if *n >= 4),
            "unit must hold \"kcal\", got {unit:?}"
        );

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();
        let p2 = pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &common::pass2_config("products", &schema),
            None,
        )
        .await
        .unwrap();

        let anomalies: Vec<(String, String, String)> = p2
            .anomaly_collector
            .summaries()
            .into_iter()
            .map(|s| (s.table, s.column, s.expected_type))
            .collect();
        assert_eq!(anomalies, [], "every unit fits in the unit column");
        let mut rows: Vec<(i64, String, Option<String>)> = client
            .query(
                &format!(
                    "SELECT p.id::bigint, v.name, v.unit \
                     FROM \"{schema}\".\"products_nutriments_pivot\" v \
                     JOIN \"{schema}\".\"products_nutriments\" n \
                       ON v.j2s_products_nutriments_id = n.j2s_id \
                     JOIN \"{schema}\".\"products\" p ON n.j2s_products_id = p.j2s_id"
                ),
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect();
        rows.sort();
        let expected = [
            (1, "energy", "kcal"),
            (1, "fat", "g"),
            (1, "sodium", "mg"),
            (1, "vitamin-d", "µg"),
            (2, "energy", "kJ"),
            (2, "fat", "g"),
            (2, "sodium", "g"),
            (3, "energy", "kcal"),
            (3, "fat", "g"),
            (3, "sodium", "mg"),
        ]
        .map(|(id, name, unit)| (id, name.to_string(), Some(unit.to_string())));
        assert_eq!(rows, expected, "one row per nutrient, with its unit");
    })
    .await;
}

// ---------------------------------------------------------------------------
// [issue #199] `prefix_columns` : `min_temp`, `max_temp`… donnent une ligne par mesure (`name`) et une colonne par
// préfixe (`min`, `max`). Une clé absente laisse sa colonne à NULL ; une clé hors de la grille reste dans la table.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_toml_prefix_columns_give_one_row_per_measure() {
    use json2sql::schema::config::apply_overrides_complete;
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("prefix_columns_stats.jsonl");
        let mut p1 = pass1::runner::run(&path, &common::pass1_config("obs"), None).unwrap();
        let config =
            SchemaConfig::from_file(&common::fixture("prefix_columns_stats.toml")).unwrap();
        assert_eq!(
            apply_overrides_complete(&mut p1.schemas, &config).unwrap(),
            []
        );
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();
        let p2 = pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &common::pass2_config("obs", &schema),
            None,
        )
        .await
        .unwrap();
        assert_eq!(p2.anomaly_collector.summaries().len(), 0);

        let mut rows: Vec<(i64, String, Option<f64>, Option<f64>)> = client
            .query(
                &format!(
                    "SELECT o.id::bigint, v.name, v.min, v.max \
                     FROM \"{schema}\".\"obs_meteo_pivot\" v \
                     JOIN \"{schema}\".\"obs_meteo\" m ON v.j2s_obs_meteo_id = m.j2s_id \
                     JOIN \"{schema}\".\"obs\" o ON m.j2s_obs_id = o.j2s_id"
                ),
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
            .collect();
        rows.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        let expected = [
            (1, "temp", Some(2.5), Some(9.5)),
            (1, "wind", Some(0.5), Some(40.5)),
            (2, "temp", Some(-1.5), Some(4.5)),
            (2, "wind", None, Some(22.5)),
        ]
        .map(|(id, name, min, max)| (id, name.to_string(), min, max));
        assert_eq!(rows, expected);
        let stations: Vec<String> = client
            .query(
                &format!("SELECT station FROM \"{schema}\".\"obs_meteo\" ORDER BY station"),
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(stations, ["a", "b"]);
    })
    .await;
}

// ---------------------------------------------------------------------------
// [issue #170] `flatten` ne remonte dans le parent que les champs simples d'un objet.
// Avant #170, il s'appliquait aussi à un tableau d'objets (la colonne restait NULL),
// à une table Jsonb (colonne `data` NULL) et à un objet qui a ses propres tables
// (supprimées avec leurs données), sans aucune anomalie. Ces flatten sont refusés
// avec un avertissement qui nomme la cause : la table enfant reste telle quelle.
// ---------------------------------------------------------------------------

/// Pass 1 of `fixture` from the root `root`, `prepare`, the TOML `config`, then the import into `schema`: the messages
/// of the TOML warnings, the schemas imported, and (table, column, count) of each anomaly summary.
async fn import_with_toml(
    client: &tokio_postgres::Client,
    (schema, url): (&str, &str),
    (fixture, root): (&str, &str),
    prepare: impl FnOnce(&mut Vec<TableSchema>),
    config: &str,
) -> (Vec<String>, Vec<TableSchema>, Vec<(String, String, u64)>) {
    use json2sql::schema::config::apply_overrides_complete;
    let path = common::fixture(fixture);
    let mut p1 = pass1::runner::run(&path, &common::pass1_config(root), None).unwrap();
    prepare(&mut p1.schemas);
    let config: SchemaConfig = toml::from_str(config).unwrap();
    let warnings = apply_overrides_complete(&mut p1.schemas, &config)
        .unwrap()
        .iter()
        .map(json2sql::schema::config::ConfigWarning::to_message)
        .collect();
    db::ddl::create_tables_no_constraints(client, &p1.schemas, schema, false, None)
        .await
        .unwrap();
    let p2 = pass2::runner::run(
        &path,
        &p1.schemas,
        client,
        url,
        &common::pass2_config(root, schema),
        None,
    )
    .await
    .unwrap();
    let anomalies = p2
        .anomaly_collector
        .summaries()
        .into_iter()
        .map(|s| (s.table, s.column, s.anomaly_count))
        .collect();
    (warnings, p1.schemas, anomalies)
}

/// The single text column of each row of `sql`, sorted.
async fn sorted_strings(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let mut rows: Vec<String> = client
        .query(sql, &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    rows.sort();
    rows
}

#[tokio::test]
async fn test_toml_flatten_is_refused_when_the_child_would_lose_data() {
    common::with_schema_url(|client, schema, url| async move {
        let (warnings, schemas, anomalies) = import_with_toml(
            &client,
            (&schema, &url),
            ("flatten_refused.jsonl", "t"),
            |_| {},
            "[t_brand]\nstrategy = \"flatten\"\n\
             [t_nutrients]\nstrategy = \"flatten\"\n\
             [t_items]\nstrategy = \"flatten\"\n",
        )
        .await;

        assert_eq!(warnings.len(), 2, "t_nutrients and t_items: {warnings:?}");
        assert!(
            warnings
                .iter()
                .any(|w| w.contains("'t_nutrients'") && w.contains("t_nutrients_sub")),
            "the warning names the table the flatten would remove: {warnings:?}"
        );
        assert!(
            warnings.iter().any(|w| w.contains("'t_items'")),
            "an array of objects is refused: {warnings:?}"
        );
        let mut names: Vec<&str> = schemas.iter().map(|s| s.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            ["t", "t_items", "t_nutrients", "t_nutrients_sub"],
            "only the plain object t_brand is flattened"
        );
        assert_eq!(anomalies, [], "nothing is lost");
        let s = &schema;
        assert_eq!(
            sorted_strings(
                &client,
                &format!("SELECT id || ':' || t_brand_name FROM \"{s}\".t")
            )
            .await,
            ["1:Alpen", "2:Lacto"]
        );
        assert_eq!(
            sorted_strings(
                &client,
                &format!(
                    "SELECT t.id || ':' || n.kcal || ':' || x.x FROM \"{s}\".t \
                     JOIN \"{s}\".t_nutrients n ON n.j2s_t_id = t.j2s_id \
                     JOIN \"{s}\".t_nutrients_sub x ON x.j2s_t_nutrients_id = n.j2s_id"
                )
            )
            .await,
            ["1:10:1", "2:20:2"],
            "nutrients.sub.x is kept with nutrients"
        );
        assert_eq!(
            sorted_strings(
                &client,
                &format!(
                    "SELECT t.id || ':' || i.a FROM \"{s}\".t \
                     JOIN \"{s}\".t_items i ON i.j2s_t_id = t.j2s_id"
                )
            )
            .await,
            ["1:1", "1:2", "2:3"],
            "every element of items is kept"
        );
    })
    .await;
}

#[tokio::test]
async fn test_toml_flatten_of_a_key_seen_as_object_and_array_keeps_every_element() {
    common::with_schema_url(|client, schema, url| async move {
        let (warnings, _, anomalies) = import_with_toml(
            &client,
            (&schema, &url),
            ("object_or_array.jsonl", "t"),
            |_| {},
            "[t_x]\nstrategy = \"flatten\"\n",
        )
        .await;

        assert_eq!(
            warnings.len(),
            1,
            "t_x is an array of objects (#169): {warnings:?}"
        );
        assert!(warnings[0].contains("'t_x'"), "{warnings:?}");
        assert_eq!(anomalies, []);
        let s = &schema;
        assert_eq!(
            sorted_strings(
                &client,
                &format!(
                    "SELECT t.id || ':' || x.j2s_order || ':' || x.a FROM \"{s}\".t \
                     JOIN \"{s}\".t_x x ON x.j2s_t_id = t.j2s_id"
                )
            )
            .await,
            ["1:0:1", "2:0:2", "2:1:3"],
            "the object of line 1 and both elements of line 2"
        );
    })
    .await;
}

#[tokio::test]
async fn test_toml_flatten_of_an_object_reports_the_arrays_it_cannot_hold() {
    use json2sql::schema::table_schema::ChildKind;
    common::with_schema_url(|client, schema, url| async move {
        // A snapshot saved before #169 keeps `Object` for a key first seen as an object: the flatten is accepted,
        // and the array of line 2 has no column to go to.
        let old_snapshot = |schemas: &mut Vec<TableSchema>| {
            let t_x = schemas.iter_mut().find(|s| s.name == "t_x").unwrap();
            t_x.child_kind = Some(ChildKind::Object);
        };
        let (warnings, _, anomalies) = import_with_toml(
            &client,
            (&schema, &url),
            ("object_or_array.jsonl", "t"),
            old_snapshot,
            "[t_x]\nstrategy = \"flatten\"\n",
        )
        .await;

        assert_eq!(warnings, Vec::<String>::new());
        let s = &schema;
        assert_eq!(
            sorted_strings(
                &client,
                &format!("SELECT id || ':' || coalesce(t_x_a::text, '-') FROM \"{s}\".t")
            )
            .await,
            ["1:1", "2:-"]
        );
        assert_eq!(
            anomalies,
            [("t".to_string(), "x".to_string(), 1)],
            "one anomaly for the array, not a silent NULL"
        );
    })
    .await;
}

#[tokio::test]
async fn test_toml_flatten_of_a_jsonb_child_keeps_its_blob() {
    use json2sql::schema::wide_strategies::apply_wide_strategy_columns;
    common::with_schema_url(|client, schema, url| async move {
        // `attrs` as Pass 1 infers a wide object of mixed types: one `data` column, not one per key.
        let jsonb = |schemas: &mut Vec<TableSchema>| {
            let attrs = schemas
                .iter_mut()
                .find(|s| s.name == "products_attrs")
                .unwrap();
            apply_wide_strategy_columns(attrs, InferredStrategy::Jsonb);
        };
        let (warnings, schemas, anomalies) = import_with_toml(
            &client,
            (&schema, &url),
            ("wide_jsonb.jsonl", "products"),
            jsonb,
            "[products_attrs]\nstrategy = \"flatten\"\n",
        )
        .await;

        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("'products_attrs'") && warnings[0].contains("Jsonb"),
            "{warnings:?}"
        );
        assert!(schemas.iter().any(|s| s.name == "products_attrs"));
        assert_eq!(anomalies, []);
        let keys: i64 = client
            .query_one(
                &format!(
                    "SELECT count(*) FROM \"{schema}\".products_attrs a, jsonb_object_keys(a.data)"
                ),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(keys, 8, "the 8 values of attrs are in their blobs");
    })
    .await;
}
