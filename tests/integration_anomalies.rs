#![allow(clippy::disallowed_methods)]
mod common;

use json2sql::{db, pass1, pass2};

// ---------------------------------------------------------------------------
// Fixture anomalies.jsonl : colonne `score` majoritairement DOUBLE PRECISION
// mais une valeur `true` (BOOLEAN) → 1 anomalie, NULL inséré pour cette ligne.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_anomaly_detection() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("anomalies.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("people"), None).unwrap();
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

        assert_eq!(common::row_count(&client, &schema, "people").await, 3);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

        let null_count: i64 = client
            .query_one(
                &format!("SELECT COUNT(*) FROM \"{schema}\".\"people\" WHERE \"score\" IS NULL"),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(null_count, 1);
    })
    .await;
}

// ---------------------------------------------------------------------------
// anomaly_dir : les fichiers NDJSON sont créés pour les tables avec anomalies.
// Vérifie le flush explicite via finish() et le contenu du fichier.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_anomaly_dir_streaming() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("anomalies.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("people"), None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();

        let anomaly_dir = tempfile::TempDir::new().unwrap();
        let mut p2 = pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &json2sql::pass2::Pass2Config {
                root_table: "people".to_string(),
                pg_schema: schema.clone(),
                parallel: 1,
                anomaly_dir: Some(anomaly_dir.path().to_path_buf()),
                limit: None,
                mem_flush_threshold_bytes: None,
                ram_high_watermark: None,
                ram_low_watermark: None,
                verbose: false,
                hint_format: None,
                skip_constraints: false,
                tune_wal: false,
            },
            None,
        )
        .await
        .unwrap();

        assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

        // Explicit finish() — verifies the file is flushed by finish(), not just by Drop.
        p2.anomaly_collector.finish().unwrap();

        let written = p2.anomaly_collector.written_paths();
        assert!(
            written.contains_key("people"),
            "expected anomaly file for 'people'"
        );

        let file_path = &written["people"];
        assert!(file_path.exists(), "NDJSON file must exist on disk");

        let content = std::fs::read_to_string(file_path).unwrap();
        let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 1, "exactly one anomaly line expected");

        let entry: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(entry["table"], "people");
        assert_eq!(entry["column"], "score");
        assert_eq!(entry["expected_type"], "DOUBLE PRECISION");
        assert_eq!(entry["actual_type"], "boolean");
    })
    .await;
}

// ---------------------------------------------------------------------------
// Float anomaly : boolean dans une colonne DOUBLE PRECISION.
// Pass 1 infère DOUBLE PRECISION (Float gagne sur Boolean dans to_pg_type).
// La valeur `true` ne peut pas être coercée → 1 anomalie, NULL inséré.
//
// Note : les strings "NaN"/"Infinity" ne produisent PAS d'anomalie dans le
// pipeline complet — Pass 1 élargit la colonne à VARCHAR. Voir coercer.rs.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_float_anomaly_boolean_value() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("anomalies_float.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("items"), None).unwrap();

        let items_schema = p1.schemas.iter().find(|s| s.name == "items").unwrap();
        let score_col = items_schema.find_by_original("score").unwrap();
        assert!(
            matches!(
                score_col.pg_type,
                json2sql::schema::type_tracker::PgType::DoublePrecision
            ),
            "score should be inferred as DOUBLE PRECISION, got {:?}",
            score_col.pg_type
        );

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();
        let p2 = pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &common::pass2_config("items", &schema),
            None,
        )
        .await
        .unwrap();

        assert_eq!(common::row_count(&client, &schema, "items").await, 5);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

        let null_count: i64 = client
            .query_one(
                &format!("SELECT COUNT(*) FROM \"{schema}\".\"items\" WHERE \"score\" IS NULL"),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(null_count, 1);
    })
    .await;
}

// ---------------------------------------------------------------------------
// Null bytes dans les strings → anomalie + NULL.
// Fixture : 3 enregistrements, colonne `bio` TEXT, 1 valeur avec null byte.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_null_byte_anomaly() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("anomalies_nullbytes.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("people"), None).unwrap();
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

        assert_eq!(common::row_count(&client, &schema, "people").await, 3);
        assert_eq!(p2.anomaly_collector.total_anomalies(), 1);

        let null_count: i64 = client
            .query_one(
                &format!("SELECT COUNT(*) FROM \"{schema}\".\"people\" WHERE \"bio\" IS NULL"),
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(null_count, 1);
    })
    .await;
}

// ---------------------------------------------------------------------------
// #84 — élément de tableau PG plus long que le VARCHAR(n) de la colonne (snapshot périmé,
// --schema-input) : l'élément devient NULL, UNE anomalie par cellule, et le COPY ne s'arrête pas
// (avant : `value too long for type character varying(2)` faisait échouer tout l'import).
// Pass 1 infère un VARCHAR(n)[] assez large ; on resserre la colonne à VARCHAR(2)[] avant le DDL.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_array_element_overflow_is_an_anomaly_not_a_copy_error() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("anomalies_array.jsonl");
        let mut p1 = pass1::runner::run(&path, &common::pass1_config("items"), None).unwrap();
        let tags = p1
            .schemas
            .iter_mut()
            .find(|s| s.name == "items")
            .unwrap()
            .columns
            .iter_mut()
            .find(|c| c.name == "tags")
            .unwrap();
        tags.pg_type = json2sql::schema::type_tracker::PgType::Array(Box::new(
            json2sql::schema::type_tracker::PgType::VarChar(2),
        ));

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();
        let p2 = pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &common::pass2_config("items", &schema),
            None,
        )
        .await
        .unwrap();

        assert_eq!(common::row_count(&client, &schema, "items").await, 3);
        assert_eq!(
            p2.anomaly_collector.total_anomalies(),
            1,
            "one anomaly for the cell, not one per overflowing element"
        );
        let cells: Vec<String> = client
            .query(
                &format!("SELECT \"tags\"::text FROM \"{schema}\".\"items\" ORDER BY \"id\""),
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(cells, vec!["{ab,cd}", "{ab,NULL,NULL}", "{}"]);
    })
    .await;
}

// ---------------------------------------------------------------------------
// #83, point 2 — GeoJSON : `coordinates` vaut [x, y] (Point) ou des tableaux de tableaux (Polygon,
// MultiPolygon), et `ALIASES` mêle chaînes et tableaux. Chaque cellule se relit à l'identique, dans
// les deux ordres de lignes. Avant : un Point suffisait à typer la colonne en DOUBLE PRECISION[] et
// chaque polygone devenait {NULL} ; un tableau parmi des chaînes devenait NULL (VARCHAR(n) trop court).
// La géométrie `null` du fichier de l'issue est retirée : elle relève du point 3 (colonne fantôme).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_nested_arrays_are_kept_in_both_row_orders() {
    use std::collections::BTreeMap;

    use serde_json::Value;

    let input = std::fs::read_to_string(common::fixture("geojson_mixed.jsonl")).unwrap();
    let rows: Vec<Value> = input
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    for reversed in [false, true] {
        let mut ordered: Vec<String> = rows.iter().map(ToString::to_string).collect();
        if reversed {
            ordered.reverse();
        }
        let file = tempfile::Builder::new()
            .suffix(".jsonl")
            .tempfile()
            .unwrap();
        std::fs::write(file.path(), ordered.join("\n")).unwrap();
        let (path, rows) = (file.path().to_path_buf(), &rows);
        common::with_schema_url(|client, schema, url| async move {
            let p1 = pass1::runner::run(&path, &common::pass1_config("features"), None).unwrap();
            db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
                .await
                .unwrap();
            let p2 = pass2::runner::run(
                &path,
                &p1.schemas,
                &client,
                &url,
                &common::pass2_config("features", &schema),
                None,
            )
            .await
            .unwrap();

            // (object, field naming the row, array field)
            for (object, key, field) in [
                ("geometry", "type", "coordinates"),
                ("properties", "NAME", "ALIASES"),
            ] {
                let table = p1
                    .schemas
                    .iter()
                    .find(|s| s.find_by_original(field).is_some())
                    .unwrap();
                let sql = format!(
                    "SELECT \"{}\", to_jsonb(\"{}\")::text FROM \"{schema}\".\"{}\"",
                    table.find_by_original(key).unwrap().name,
                    table.find_by_original(field).unwrap().name,
                    table.name
                );
                let stored: BTreeMap<String, Option<Value>> = client
                    .query(&sql, &[])
                    .await
                    .unwrap()
                    .iter()
                    .map(|r| {
                        let json: Option<String> = r.get(1);
                        (r.get(0), json.map(|j| serde_json::from_str(&j).unwrap()))
                    })
                    .collect();
                let expected: BTreeMap<String, Option<Value>> = rows
                    .iter()
                    .map(|row| {
                        let obj = &row[object];
                        (
                            obj[key].as_str().unwrap().to_owned(),
                            obj.get(field).cloned(),
                        )
                    })
                    .collect();
                assert_eq!(stored, expected, "{field}, reversed: {reversed}");
            }
            assert_eq!(
                p2.anomaly_collector.total_anomalies(),
                0,
                "reversed: {reversed}"
            );
        })
        .await;
    }
}

// ---------------------------------------------------------------------------
// #116 — formats incompatibles dans une colonne (UUID + dates, UUID + timestamps, dates + un
// nombre) et dates qui n'existent pas (`2024-13-45`, `…T10:30 junk`) : la Pass 1 type ces colonnes
// en texte, rien n'est perdu. Avant : DATE ou TIMESTAMP, et PostgreSQL arrêtait le COPY sur la
// première valeur refusée (ou INTEGER, et chaque date devenait NULL + anomalie).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_incompatible_date_formats_are_text_not_a_copy_error() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("anomalies_dates.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("events"), None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();
        let p2 = pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &common::pass2_config("events", &schema),
            None,
        )
        .await
        .unwrap();

        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
        let cells: Vec<Vec<String>> = client
            .query(
                &format!(
                    "SELECT \"ref\", \"at_ref\", \"day\", \"at\", \"code\" \
                     FROM \"{schema}\".\"events\" ORDER BY \"id\""
                ),
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (0..5).map(|i| r.get(i)).collect())
            .collect();
        assert_eq!(
            cells,
            [
                [
                    "550e8400-e29b-41d4-a716-446655440000",
                    "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                    "2024-01-15",
                    "2024-01-15T10:30:00Z",
                    "42",
                ],
                [
                    "2024-02-20",
                    "2024-02-20T08:00:00+02:00",
                    "2024-13-45",
                    "2024-01-15T10:30 junk",
                    "2024-02-20",
                ],
                [
                    "2024-03-01",
                    "2024-03-01 12:00",
                    "2024-03-01",
                    "2024-03-01T08:00:00.5Z",
                    "2024-03-01",
                ],
            ]
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// #116 — colonne UUID, DATE ou TIMESTAMP imposée (snapshot --schema-input, surcharge de type) qui
// reçoit une valeur d'un autre format : NULL + une anomalie par cellule, le COPY continue. Une
// colonne DATE garde la date d'un timestamp, comme PostgreSQL. Avant : le COPY s'arrêtait.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_value_of_another_format_in_a_typed_column_is_an_anomaly() {
    use json2sql::schema::type_tracker::PgType;
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("anomalies_dates.jsonl");
        let mut p1 = pass1::runner::run(&path, &common::pass1_config("events"), None).unwrap();
        let events = p1.schemas.iter_mut().find(|s| s.name == "events").unwrap();
        for (column, pg_type) in [
            ("ref", PgType::Uuid),
            ("at_ref", PgType::Date),
            ("day", PgType::Date),
            ("at", PgType::Timestamp),
        ] {
            events
                .columns
                .iter_mut()
                .find(|c| c.name == column)
                .unwrap()
                .pg_type = pg_type;
        }

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();
        let p2 = pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &common::pass2_config("events", &schema),
            None,
        )
        .await
        .unwrap();

        // ref: the two dates; at_ref: the UUID; day: 2024-13-45; at: the trailing junk
        assert_eq!(p2.anomaly_collector.total_anomalies(), 5);
        let cells: Vec<Vec<Option<String>>> = client
            .query(
                &format!(
                    "SELECT \"ref\"::text, \"at_ref\"::text, \"day\"::text, \"at\"::text \
                     FROM \"{schema}\".\"events\" ORDER BY \"id\""
                ),
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (0..4).map(|i| r.get(i)).collect())
            .collect();
        let some = |s: &str| Some(s.to_string());
        assert_eq!(
            cells,
            [
                [
                    some("550e8400-e29b-41d4-a716-446655440000"),
                    None,
                    some("2024-01-15"),
                    some("2024-01-15 10:30:00"),
                ],
                [None, some("2024-02-20"), None, None],
                [
                    None,
                    some("2024-03-01"),
                    some("2024-03-01"),
                    some("2024-03-01 08:00:00.5"),
                ],
            ]
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// #169 — clé tantôt objet, tantôt tableau d'objets, dans les deux ordres, Pass 1 séquentielle et
// parallèle : la table de `x` reçoit chaque élément, l'objet seul comme un tableau d'un élément
// (`j2s_order` 0). Avant : objet d'abord, la table était `Object` et les éléments du tableau
// disparaissaient sans anomalie ; tableau d'abord, l'objet seul avait `j2s_order` NULL et
// PostgreSQL arrêtait le COPY (NOT NULL).
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_key_seen_as_object_and_as_array_of_objects_keeps_every_element() {
    for fixture in ["object_or_array.jsonl", "array_or_object.jsonl"] {
        for workers in [1, 4] {
            common::with_schema_url(|client, schema, url| async move {
                let path = common::fixture(fixture);
                let config = pass1::runner::Pass1Config {
                    num_workers: Some(workers),
                    ..common::pass1_config("p")
                };
                let p1 = if workers > 1 {
                    pass1::runner::run_parallel(&path, &config, None)
                } else {
                    pass1::runner::run(&path, &config, None)
                }
                .unwrap();
                db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
                    .await
                    .unwrap();
                let p2 = pass2::runner::run(
                    &path,
                    &p1.schemas,
                    &client,
                    &url,
                    &common::pass2_config("p", &schema),
                    None,
                )
                .await
                .unwrap();

                let case = format!("{fixture}, {workers} worker(s)");
                assert_eq!(p2.anomaly_collector.total_anomalies(), 0, "{case}");
                let rows: Vec<(i32, i64)> = client
                    .query(
                        &format!(
                            "SELECT \"a\", \"j2s_order\" FROM \"{schema}\".\"p_x\" ORDER BY \"a\""
                        ),
                        &[],
                    )
                    .await
                    .unwrap()
                    .iter()
                    .map(|r| (r.get(0), r.get(1)))
                    .collect();
                assert_eq!(rows, [(1, 0), (2, 0), (3, 1)], "{case}: (a, j2s_order)");
            })
            .await;
        }
    }
}

// ---------------------------------------------------------------------------
// Éléments d'un tableau UUID[], DATE[] ou TIMESTAMP[] imposé (snapshot --schema-input périmé) qui ne
// sont pas de ce format : NULL dans le tableau + une anomalie par cellule, le COPY continue, comme
// pour un scalaire depuis #116. Avant : élément écrit sans vérification, et le COPY s'arrêtait.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_array_element_of_another_format_is_an_anomaly() {
    use json2sql::schema::type_tracker::PgType;
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("anomalies_dates_array.jsonl");
        let mut p1 = pass1::runner::run(&path, &common::pass1_config("events"), None).unwrap();
        let events = p1.schemas.iter_mut().find(|s| s.name == "events").unwrap();
        for (column, elem) in [
            ("refs", PgType::Uuid),
            ("days", PgType::Date),
            ("ats", PgType::Timestamp),
        ] {
            events
                .columns
                .iter_mut()
                .find(|c| c.name == column)
                .unwrap()
                .pg_type = PgType::Array(Box::new(elem));
        }

        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();
        let p2 = pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &common::pass2_config("events", &schema),
            None,
        )
        .await
        .expect("an element of another format must not abort the COPY");

        // first row: one faulty element per column
        assert_eq!(p2.anomaly_collector.total_anomalies(), 3);
        let cells: Vec<Vec<String>> = client
            .query(
                &format!(
                    "SELECT \"refs\"::text, \"days\"::text, \"ats\"::text \
                     FROM \"{schema}\".\"events\" ORDER BY \"id\""
                ),
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (0..3).map(|i| r.get(i)).collect())
            .collect();
        assert_eq!(
            cells,
            [
                [
                    "{550e8400-e29b-41d4-a716-446655440000,NULL}",
                    "{2024-01-15,NULL}",
                    "{\"2024-01-15 10:30:00\",NULL}",
                ],
                [
                    "{6ba7b810-9dad-11d1-80b4-00c04fd430c8}",
                    "{2024-02-20}",
                    "{\"2024-03-01 00:00:00\"}",
                ],
            ]
        );
    })
    .await;
}

// ---------------------------------------------------------------------------
// #194 — timestamps avec décalage horaire : chaque valeur garde son instant. Avant : colonne TIMESTAMP,
// PostgreSQL ignorait le décalage, et 10:30Z, 10:30+02:00 et 10:30-05:00 valaient tous 10:30:00,
// sans anomalie.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_timestamp_offsets_keep_the_instant() {
    common::with_schema_url(|client, schema, url| async move {
        let path = common::fixture("timestamps_offsets.jsonl");
        let p1 = pass1::runner::run(&path, &common::pass1_config("events"), None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();
        let p2 = pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &common::pass2_config("events", &schema),
            None,
        )
        .await
        .unwrap();

        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
        let epochs: Vec<i64> = client
            .query(
                &format!(
                    "SELECT extract(epoch FROM \"at\")::bigint \
                     FROM \"{schema}\".\"events\" ORDER BY \"id\""
                ),
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        // seconds since 1970-01-01 UTC: 10:30, 08:30 and 15:30 UTC on 2024-01-15
        assert_eq!(epochs, [1_705_314_600, 1_705_307_400, 1_705_332_600]);
    })
    .await;
}

// ---------------------------------------------------------------------------
// #194 — bords du passage en UTC, dans une colonne TIMESTAMP et dans un TIMESTAMP[] : chaque valeur vaut
// l'instant que PostgreSQL lit lui-même dans la chaîne d'origine en TIMESTAMPTZ, ramené en UTC.
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_timestamp_offsets_in_utc_match_postgresql() {
    const INPUTS: [&str; 15] = [
        "2024-01-15T23:30:00-05:00", // next day
        "2024-01-31T20:00:00-05:00", // next month
        "2024-12-31T22:00:00-03:00", // next year
        "2024-02-28T23:00:00-02:00", // to a leap day
        "2024-03-01T00:30:00+01:00", // back to a leap day
        "2023-03-01T00:30:00+01:00", // back to February 28
        "2024-01-01T01:00:00+02:00", // previous year
        "2024-01-15T10:30+15:59",    // no seconds, largest offset
        "2024-01-15T10:30:00+02",
        "2024-01-15T10:30:00-0530",
        "2024-01-15 10:30:00.5+05:45",
        "2024-01-15T10:30:00.123456789+02:00",
        "0001-01-01T00:30:00+01:00", // to 1 BC: PostgreSQL has no year 0
        "9999-12-31T23:00:00-05:00", // to year 10000
        "2024-01-15T10:30:00Z",
    ];
    let lines: Vec<String> = (0_i32..)
        .zip(INPUTS)
        .map(|(id, at)| serde_json::json!({"id": id, "at": at, "ats": [at]}).to_string())
        .collect();
    let file = tempfile::Builder::new()
        .suffix(".jsonl")
        .tempfile()
        .unwrap();
    std::fs::write(file.path(), lines.join("\n")).unwrap();
    let path = file.path().to_path_buf();
    common::with_schema_url(|client, schema, url| async move {
        let p1 = pass1::runner::run(&path, &common::pass1_config("events"), None).unwrap();
        db::ddl::create_tables_no_constraints(&client, &p1.schemas, &schema, false, None)
            .await
            .unwrap();
        let p2 = pass2::runner::run(
            &path,
            &p1.schemas,
            &client,
            &url,
            &common::pass2_config("events", &schema),
            None,
        )
        .await
        .unwrap();

        assert_eq!(p2.anomaly_collector.total_anomalies(), 0);
        for (id, input) in (0_i32..).zip(INPUTS) {
            let row = client
                .query_one(
                    &format!(
                        "SELECT \"at\"::text, \"ats\"[1]::text, \
                         ($1::text::timestamptz AT TIME ZONE 'UTC')::text \
                         FROM \"{schema}\".\"events\" WHERE \"id\" = $2"
                    ),
                    &[&input, &id],
                )
                .await
                .unwrap();
            let (at, elem, expected): (String, String, String) =
                (row.get(0), row.get(1), row.get(2));
            assert_eq!((&at, &elem), (&expected, &expected), "{input}");
        }
    })
    .await;
}
