#![allow(clippy::disallowed_methods)]
//! Le schéma que la CLI produit, réglages par défaut, sur des JSON de formes variées (#200, T1).
//!
//! Filet posé avant de simplifier les stratégies : chaque test lance le binaire en `--dry-run`, comme un
//! utilisateur, puis compare les tables du DDL et leurs colonnes de données (sans les `j2s_*`). Les attendus sont
//! le résultat d'aujourd'hui ; une tâche de #200 qui en change un change l'attendu en même temps, et le commentaire
//! « Cible » de chaque test dit laquelle et comment. Les JSON sont générés sans hasard : mêmes lignes à chaque run.

use std::collections::BTreeMap;
use std::process::Command;

use serde_json::{json, Map, Value};

/// Ce que donne un `--dry-run` : les colonnes de données de chaque table, avec leur type, et la sortie d'erreur.
struct DryRun {
    tables: BTreeMap<String, Vec<(String, String)>>,
    stderr: String,
}

impl DryRun {
    /// Les tables et leurs colonnes de données, triées : la forme comparée par les tests.
    fn shape(&self) -> BTreeMap<&str, Vec<&str>> {
        self.tables
            .iter()
            .map(|(table, cols)| {
                let mut names: Vec<&str> = cols.iter().map(|(name, _)| name.as_str()).collect();
                names.sort_unstable();
                (table.as_str(), names)
            })
            .collect()
    }

    fn type_of(&self, table: &str, column: &str) -> &str {
        let cols = &self.tables[table];
        &cols
            .iter()
            .find(|(name, _)| name == column)
            .expect(column)
            .1
    }
}

/// Lance `json2sql --input <rows> --table <table> --dry-run <args>`, avec `toml` en `--schema-config` s'il y en a un.
fn dry_run(table: &str, rows: &[Value], args: &[&str], toml: Option<&str>) -> DryRun {
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("input.jsonl");
    let lines: String = rows.iter().map(|row| format!("{row}\n")).collect();
    std::fs::write(&input, lines).unwrap();
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_json2sql"));
    cmd.arg("--input").arg(&input);
    cmd.args(["--table", table, "--dry-run"]).args(args);
    if let Some(toml) = toml {
        let config = dir.path().join("config.toml");
        std::fs::write(&config, toml).unwrap();
        cmd.arg("--schema-config").arg(&config);
    }
    let out = cmd.output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(out.status.success(), "json2sql a échoué :\n{stderr}");
    DryRun {
        tables: parse_ddl(&String::from_utf8_lossy(&out.stdout)),
        stderr,
    }
}

/// Les `CREATE TABLE` du DDL : nom de la table → colonnes de données (`"nom" TYPE`), dans l'ordre du DDL.
fn parse_ddl(ddl: &str) -> BTreeMap<String, Vec<(String, String)>> {
    let mut tables = BTreeMap::new();
    let mut current: Option<String> = None;
    for line in ddl.lines() {
        if let Some(rest) = line.strip_prefix("CREATE TABLE IF NOT EXISTS ") {
            let name = rest.trim_end_matches(" (").rsplit('.').next().unwrap();
            let name = name.trim_matches('"').to_string();
            tables.insert(name.clone(), Vec::new());
            current = Some(name);
        } else if line.starts_with(')') {
            current = None;
        } else if let Some(table) = &current {
            // Les lignes `CONSTRAINT …` ne commencent pas par un nom de colonne entre guillemets.
            let Some(column) = line.trim().trim_end_matches(',').strip_prefix('"') else {
                continue;
            };
            let (name, ty) = column.split_once("\" ").unwrap();
            if !name.starts_with("j2s_") {
                tables
                    .get_mut(table)
                    .unwrap()
                    .push((name.to_string(), ty.to_string()));
            }
        }
    }
    tables
}

/// La forme attendue : `(table, colonnes)` ; l'ordre des colonnes ne compte pas.
fn expected<'a>(tables: &[(&'a str, &[&'a str])]) -> BTreeMap<&'a str, Vec<&'a str>> {
    tables
        .iter()
        .map(|(table, cols)| {
            let mut cols = cols.to_vec();
            cols.sort_unstable();
            (*table, cols)
        })
        .collect()
}

/// Une valeur d'un des quatre types JSON scalaires, choisi par `n` : texte, réel, booléen, entier.
fn mixed_value(n: usize) -> Value {
    match n % 4 {
        0 => json!("steel"),
        1 => json!(2.5),
        2 => json!(true),
        _ => json!(7),
    }
}

/// Un objet dont les clés sont `keys` et les valeurs données par `value`.
fn object<'a>(keys: impl IntoIterator<Item = String>, value: impl Fn(&str) -> Value + 'a) -> Value {
    let map: Map<String, Value> = keys
        .into_iter()
        .map(|k| {
            let v = value(&k);
            (k, v)
        })
        .collect();
    Value::Object(map)
}

// ---------------------------------------------------------------------------
// A. Des clés qui sont des données, toutes du même type : 150 devises, 7 par ligne, `usd` dans toutes.
// Aujourd'hui : un seul type → split identité / compagnon, toutes les clés en clé/valeur, même `usd`.
// Cible (T3) : inchangé.
// ---------------------------------------------------------------------------
#[test]
fn a_dynamic_keys_of_one_type_all_go_to_key_value() {
    let rows: Vec<Value> = (0..200)
        .map(|i| {
            let others = (0..6).map(|k| format!("c{:03}", (i * 6 + k) % 149));
            let keys = std::iter::once("usd".to_string()).chain(others);
            let rates = object(keys, |k| {
                json!(if k == "usd" { 1.0 } else { 0.5 + i as f64 })
            });
            json!({"date": format!("2026-01-{:02}", i % 28 + 1), "rates": rates})
        })
        .collect();
    let run = dry_run("rates", &rows, &[], None);
    assert_eq!(
        run.shape(),
        expected(&[
            ("rates", &["date"]),
            ("rates_rates", &[]),
            ("rates_rates_pivot", &["key", "value"]),
        ])
    );
    assert_eq!(
        run.type_of("rates_rates_pivot", "value"),
        "DOUBLE PRECISION"
    );
}

// ---------------------------------------------------------------------------
// B. Un objet enfant large aux types mélangés : `color` partout, et 4 attributs sur 80 par ligne.
// Aujourd'hui : types mélangés → tout l'objet dans une colonne JSONB, `color` compris.
// Cible (T3) : `products_attrs (color)` + `products_attrs_wide (key, value)`.
// ---------------------------------------------------------------------------
#[test]
fn b_wide_child_of_mixed_types_becomes_one_jsonb_column() {
    let rows: Vec<Value> = (0..300)
        .map(|i| {
            let mut attrs = object((0..4).map(|k| format!("attr_{}", (i * 4 + k) % 80)), |k| {
                mixed_value(k.trim_start_matches("attr_").parse().unwrap())
            });
            attrs["color"] = json!(if i % 2 == 0 { "red" } else { "blue" });
            json!({"sku": format!("P{i}"), "attrs": attrs})
        })
        .collect();
    let run = dry_run("products", &rows, &[], None);
    assert_eq!(
        run.shape(),
        expected(&[("products", &["sku"]), ("products_attrs", &["data"])])
    );
    assert_eq!(run.type_of("products_attrs", "data"), "JSONB");
}

/// C. 1001 événements : `id`, `ts`, `type` partout, 2 clés moyennes sur 60 par ligne (≈ 3 %), et 30 clés vues
/// une seule fois (sous le seuil de 0,1 %) ; avec `meta`, chaque ligne a aussi un objet enfant.
fn events(meta: bool) -> Vec<Value> {
    (0..1001)
        .map(|i| {
            let mut row = object((0..2).map(|k| format!("m_{}", (i * 2 + k) % 60)), |k| {
                mixed_value(k.trim_start_matches("m_").parse().unwrap())
            });
            row["id"] = json!(i);
            row["ts"] = json!(format!("2026-01-01T00:{:02}:00", i % 60));
            row["type"] = json!(if i % 2 == 0 { "click" } else { "view" });
            if i < 30 {
                row[format!("rare_{i}")] = json!("x");
            }
            if meta {
                row["meta"] = json!({"ip": "10.0.0.1", "ua": "curl"});
            }
            row
        })
        .collect()
}

// ---------------------------------------------------------------------------
// C, sans objet enfant. Aujourd'hui : la racine large aux types mélangés part entière en JSONB, même `id`.
// Cible (T2, T3) : comme avec un objet enfant, `events (id, ts, type)` + `events_wide`, avec les 90 clés.
// ---------------------------------------------------------------------------
#[test]
fn c_wide_root_without_object_child_becomes_one_jsonb_column() {
    let run = dry_run("events", &events(false), &[], None);
    assert_eq!(run.shape(), expected(&[("events", &["data"])]));
    assert_eq!(run.type_of("events", "data"), "JSONB");
}

// ---------------------------------------------------------------------------
// C, avec l'objet `meta` : AutoSplit. Les 60 clés moyennes et les 30 clés vues une seule fois vont dans
// `events_wide` : rien n'est jeté (T2).
// ---------------------------------------------------------------------------
#[test]
fn c_wide_root_with_object_child_is_split_and_keeps_rare_keys() {
    let run = dry_run("events", &events(true), &[], None);
    assert_eq!(
        run.shape(),
        expected(&[
            ("events", &["id", "ts", "type"]),
            ("events_meta", &["ip", "ua"]),
            ("events_wide", &["key", "value"]),
        ])
    );
    assert_eq!(run.type_of("events_wide", "value"), "TEXT");
    assert!(
        run.stderr.contains("90 medium → events_wide") && !run.stderr.contains("NOT imported"),
        "{}",
        run.stderr
    );
}

/// D. 50 clients aux 20 colonnes toujours remplies, dont 17 en familles de noms.
fn customers() -> Vec<Value> {
    let families: [(&str, &[&str]); 5] = [
        ("address", &["street", "city", "zip", "country"]),
        ("contact", &["email", "phone", "fax"]),
        ("billing", &["iban", "bic", "vat", "currency"]),
        ("shipping", &["carrier", "speed", "cost"]),
        ("prefs", &["lang", "newsletter", "theme"]),
    ];
    (0..50)
        .map(|i| {
            let keys = families
                .iter()
                .flat_map(|(family, cols)| cols.iter().map(move |c| format!("{family}_{c}")));
            let mut row = object(keys, |k| json!(format!("{k}{i}")));
            row["id"] = json!(i);
            row["name"] = json!(format!("n{i}"));
            row["created_at"] = json!("2026-01-01");
            row
        })
        .collect()
}

// ---------------------------------------------------------------------------
// D. Table dense : une seule table par défaut ; `--group-columns` la découpe par familles de noms.
// Cible (PR 2, T9 et T10) : groupes proposés, appliqués une fois validés ou avec `--group-columns` : même découpage.
// ---------------------------------------------------------------------------
#[test]
fn d_dense_table_stays_whole_and_group_columns_splits_it_by_name() {
    let rows = customers();
    let whole = dry_run("customers", &rows, &[], None);
    assert_eq!(whole.shape().len(), 1);
    assert_eq!(whole.shape()["customers"].len(), 20);

    let grouped = dry_run("customers", &rows, &["--group-columns"], None);
    assert_eq!(
        grouped.shape(),
        expected(&[
            ("customers", &["created_at", "id", "name"]),
            (
                "customers_address",
                &[
                    "address_city",
                    "address_country",
                    "address_street",
                    "address_zip"
                ],
            ),
            (
                "customers_billing",
                &[
                    "billing_bic",
                    "billing_currency",
                    "billing_iban",
                    "billing_vat"
                ],
            ),
            (
                "customers_contact",
                &["contact_email", "contact_fax", "contact_phone"]
            ),
            (
                "customers_prefs",
                &["prefs_lang", "prefs_newsletter", "prefs_theme"]
            ),
            (
                "customers_shipping",
                &["shipping_carrier", "shipping_cost", "shipping_speed"]
            ),
        ])
    );
}

// ---------------------------------------------------------------------------
// E. Objets frères sous des clés de 3 lettres (des devises).
// Aujourd'hui : fusion `sibling`, colonne clé `lang_code` (toute clé de 2 ou 3 lettres passe pour une langue).
// Cible (T5) : colonne clé `key`.
// ---------------------------------------------------------------------------
#[test]
fn e_sibling_objects_under_three_letter_keys_get_a_lang_code_column() {
    let rows: Vec<Value> = (0..20)
        .map(|i| {
            let prices = object(
                ["usd", "eur", "gbp", "chf"].map(String::from),
                |_| json!({"amount": 10.5 + f64::from(i), "vat": 0.2}),
            );
            json!({"sku": i, "prices": prices})
        })
        .collect();
    let run = dry_run("products", &rows, &[], None);
    assert_eq!(
        run.shape(),
        expected(&[
            ("products", &["sku"]),
            ("products_prices", &["amount", "lang_code", "vat"]),
        ])
    );
}

// ---------------------------------------------------------------------------
// E'. Objets frères identiques sous des clés numériques et texte.
// Aujourd'hui : la forme des clés coupe la fusion en deux tables (`_num`, clé `key_id` ; `_key`, clé `key`).
// Cible (T5) : une seule table `items_slots (key, url, w)`.
// ---------------------------------------------------------------------------
#[test]
fn e_identical_siblings_are_split_by_the_shape_of_their_keys() {
    let rows: Vec<Value> = (0..20)
        .map(|i| {
            let slots = object(
                ["1", "2", "3", "front", "back"].map(String::from),
                |k| json!({"url": format!("u{k}"), "w": 800}),
            );
            json!({"id": i, "slots": slots})
        })
        .collect();
    let run = dry_run("items", &rows, &[], None);
    assert_eq!(
        run.shape(),
        expected(&[
            ("items", &["id"]),
            ("items_slots", &[]),
            ("items_slots_key", &["key", "url", "w"]),
            ("items_slots_num", &["key_id", "url", "w"]),
        ])
    );
}

/// F. 50 lignes d'une grille dense : 7 nutriments × (`n`, `n_points`, `n_value`), toujours remplis.
fn nutrient_scores() -> Vec<Value> {
    let nutrients = [
        "sugars", "fat", "fiber", "salt", "proteins", "energy", "sodium",
    ];
    (0..50)
        .map(|i| {
            let keys = nutrients
                .iter()
                .flat_map(|n| [n.to_string(), format!("{n}_points"), format!("{n}_value")]);
            let score = object(keys, |k| {
                if k.ends_with("_value") {
                    json!(1.5 + f64::from(i))
                } else {
                    json!(i)
                }
            });
            json!({"id": i, "score": score})
        })
        .collect()
}

// ---------------------------------------------------------------------------
// F. Grille nom × suffixe dans une table dense : aujourd'hui 21 colonnes, la grille n'est pas cherchée ; forcée dans
// le TOML, `name, value, points, norm_value` (`value` vient de `sugars`, `norm_value` de `sugars_value`, #93).
// Cible (PR 2, T8) : la grille est proposée, jamais appliquée seule, et jamais une table par nom.
// ---------------------------------------------------------------------------
#[test]
fn f_dense_name_suffix_grid_stays_columns_unless_the_toml_asks() {
    let rows = nutrient_scores();
    let run = dry_run("nutri", &rows, &[], None);
    assert_eq!(run.shape()["nutri_score"].len(), 21);

    let toml = "[nutri_score]\nstrategy = \"structured_pivot\"\nsuffix_columns = [\"_points\", \"_value\"]\n";
    let forced = dry_run("nutri", &rows, &[], Some(toml));
    assert_eq!(
        forced.shape(),
        expected(&[
            ("nutri", &["id"]),
            ("nutri_score", &[]),
            (
                "nutri_score_pivot",
                &["name", "norm_value", "points", "value"]
            ),
        ])
    );
}

/// 300 lignes `{"id": i, "obj": {…}}` : l'objet de la ligne `i` est donné par `obj(i)`.
fn rows_with_obj(obj: impl Fn(usize) -> Value) -> Vec<Value> {
    (0..300).map(|i| json!({"id": i, "obj": obj(i)})).collect()
}

/// Le résultat des JSON de suffixes aujourd'hui : `t (id)`, `t_obj` vide (ou avec `rest`), et la grille `t_obj_pivot`.
/// Cible (PR 2, T8) : plus de grille sans TOML ; la table large et creuse passe par le split.
fn assert_grid(run: &DryRun, rest: &[&str], grid: &[&str]) {
    assert_eq!(
        run.shape(),
        expected(&[("t", &["id"]), ("t_obj", rest), ("t_obj_pivot", grid)])
    );
}

const METEO: [&str; 12] = [
    "temperature",
    "pressure",
    "humidity",
    "wind_speed",
    "rainfall",
    "uv_index",
    "visibility",
    "dew_point",
    "cloud_cover",
    "snow_depth",
    "soil_moisture",
    "solar_radiation",
];

// Suffixes « aspects » remplis ensemble : la grille est bonne.
#[test]
fn suffix_grid_of_statistics() {
    let rows = rows_with_obj(|i| {
        let base = METEO[i % 12];
        object(["min", "max", "avg"].map(|s| format!("{base}_{s}")), |_| {
            json!(1.5)
        })
    });
    assert_grid(
        &dry_run("t", &rows, &[], None),
        &[],
        &["name", "avg", "max", "min"],
    );
}

// Unités alternatives, une par objet : la grille est trouvée (coupée sur `_temp_c`…), mais 2 colonnes sur 3 vides.
#[test]
fn suffix_grid_of_alternative_units() {
    let temps = [
        "air_temp",
        "water_temp",
        "soil_temp",
        "engine_temp",
        "oil_temp",
        "cabin_temp",
    ];
    let rows = rows_with_obj(|i| {
        let key = format!("{}_{}", temps[i % 6], ["c", "f", "k"][i / 6 % 3]);
        object([key], |_| json!(21.5))
    });
    assert_grid(
        &dry_run("t", &rows, &[], None),
        &[],
        &["name", "temp_c", "temp_f", "temp_k"],
    );
}

const LANGS: [&str; 15] = [
    "fr", "en", "de", "es", "it", "pt", "nl", "sv", "pl", "cs", "ro", "hu", "el", "fi", "da",
];

// Traductions, une langue par objet : une colonne par langue, 14 vides sur 15 par ligne.
#[test]
fn suffix_grid_of_languages() {
    let rows = rows_with_obj(|i| {
        let lang = LANGS[i % 15];
        let fields = ["title", "description", "summary", "keywords", "slug"];
        object(fields.map(|f| format!("{f}_{lang}")), |k| json!(k))
    });
    let mut grid = vec!["name"];
    grid.extend(LANGS);
    assert_grid(&dry_run("t", &rows, &[], None), &[], &grid);
}

// Séries annuelles, une année par objet : une colonne par année (`_2015`… : un nom ne commence pas par un chiffre).
#[test]
fn suffix_grid_of_years() {
    let bases = ["revenue", "cost", "margin", "headcount", "capex", "opex"];
    let rows = rows_with_obj(|i| {
        object([format!("{}_{}", bases[i % 6], 2015 + i / 6 % 11)], |_| {
            json!(42)
        })
    });
    let years: Vec<String> = (2015..=2025).map(|y| format!("_{y}")).collect();
    let mut grid = vec!["name"];
    grid.extend(years.iter().map(String::as_str));
    assert_grid(&dry_run("t", &rows, &[], None), &[], &grid);
}

// Statistiques en préfixe (`min_temperature`) : la grille est trouvée à l'envers, une ligne par statistique.
#[test]
fn suffix_grid_of_prefixed_statistics_is_upside_down() {
    let measures = [
        "temperature",
        "pressure",
        "humidity",
        "wind",
        "rain",
        "snow",
        "uv",
        "dew",
        "cloud",
        "fog",
        "hail",
        "sun",
    ];
    let rows = rows_with_obj(|i| {
        let measure = measures[i % 12];
        object(
            ["min", "max", "avg"].map(|s| format!("{s}_{measure}")),
            |_| json!(3.5),
        )
    });
    let mut grid = vec!["name"];
    grid.extend(measures);
    assert_grid(&dry_run("t", &rows, &[], None), &[], &grid);
}

// Le piège : des identifiants, des dates et des noms d'objets sans rapport donnent une grille absurde, appliquée sans
// rien demander. Cible (PR 2, T8) : aucune grille appliquée.
#[test]
fn suffix_trap_of_ids_and_dates_gives_an_absurd_grid() {
    let keys = [
        "user_id",
        "order_id",
        "product_id",
        "store_id",
        "created_at",
        "updated_at",
        "shipped_at",
        "paid_at",
        "user_name",
        "product_name",
        "store_name",
        "status",
        "amount",
        "currency",
        "note",
        "channel",
        "coupon",
        "carrier",
        "invoice",
        "refund",
        "tax",
        "discount",
        "weight",
        "zone",
    ];
    let rows = rows_with_obj(|i| {
        let key = keys[i % 24];
        let value = if key.ends_with("_id") {
            json!(i)
        } else if key.ends_with("_at") {
            json!("2026-01-01T00:00:00")
        } else {
            json!("x")
        };
        object([key.to_string()], |_| value.clone())
    });
    let rest = [
        "status", "amount", "currency", "note", "channel", "coupon", "carrier", "invoice",
        "refund", "tax", "discount", "weight", "zone",
    ];
    assert_grid(
        &dry_run("t", &rows, &[], None),
        &rest,
        &["name", "at", "id", "norm_name"],
    );
}

// ---------------------------------------------------------------------------
// G. Des mesures en plusieurs unités (`length_mm`, `length_cm`, `weight_g`…), une unité par mesure et par objet :
// chaque clé est présente dans au moins 10 % des lignes, la table est stable et reste en 18 colonnes.
// Cible : inchangé (la grille ne serait qu'une proposition, PR 2).
// ---------------------------------------------------------------------------
#[test]
fn g_measures_in_several_units_stay_columns() {
    let rows: Vec<Value> = (0..50)
        .map(|i| {
            let lengths = ["length", "width", "height", "depth"]
                .into_iter()
                .enumerate()
                .map(|(b, base)| format!("{base}_{}", ["mm", "cm", "m"][(i + b) % 3]));
            let weights = ["weight", "net_weight", "gross_weight"]
                .into_iter()
                .enumerate()
                .map(|(b, base)| format!("{base}_{}", ["g", "kg"][(i + b) % 2]));
            json!({"sku": i, "measures": object(lengths.chain(weights), |_| json!(12.5))})
        })
        .collect();
    let run = dry_run("products", &rows, &[], None);
    assert_eq!(run.shape().len(), 2);
    assert_eq!(run.shape()["products_measures"].len(), 18);
}
