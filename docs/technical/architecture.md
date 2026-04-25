# Architecture technique

Vue d'ensemble du pipeline `json2sql` et documentation détaillée de chaque module.

---

## Vue d'ensemble

`json2sql` est un outil CLI en Rust qui importe des fichiers JSON/NDJSON dans PostgreSQL. Il fonctionne en **deux passes** :

- **Pass 1** : lecture complète du fichier pour inférer le schéma (tables, colonnes, types)
- **Pass 2** : relecture du fichier pour insérer les données via `COPY FROM STDIN`

```
src/
├── main.rs              # point d'entrée, orchestration
├── lib.rs               # déclaration des modules publics
├── cli.rs               # arguments CLI
├── error.rs             # types d'erreur
├── anomaly/             # détection et rapport d'anomalies de type
├── db/                  # couche PostgreSQL
├── io/                  # lecture JSON, progress bar, événements IHM
├── pass1/               # Pass 1 : inférence de schéma
├── pass2/               # Pass 2 : insertion des données
└── schema/              # modèle de schéma, inférence, config, persistance
```

---

## `src/main.rs`

Point d'entrée et orchestrateur principal. Séquence complète :

1. Parse les arguments CLI
2. Résout l'entrée (fichier ou stdin → fichier temporaire)
3. Lance la Pass 1 (inférence du schéma)
4. Affiche les avertissements (noms tronqués, collisions de colonnes, dépassement de profondeur)
5. Applique les surcharges TOML (`--schema-config`)
6. Ré-exclut les tables enfants absorbées après les surcharges
7. Génère le rapport de schéma (optionnel)
8. En mode `--dry-run` : affiche le DDL SQL et sort
9. Sinon : se connecte à PostgreSQL, crée les tables, lance la Pass 2
10. Affiche le résumé d'import et les anomalies

---

## `src/lib.rs`

Déclare tous les modules comme publics pour que les tests d'intégration (`tests/`) puissent y accéder.

---

## `src/cli.rs`

Définit la structure `Cli` via `clap`. Tous les paramètres CLI de l'outil y sont déclarés. Voir [../functional/usage.md](../functional/usage.md) pour la référence complète des flags.

---

## `src/error.rs`

Définit `J2sError`, l'enum d'erreur centrale du projet, et l'alias `Result<T>`.

| Variante | Source | Usage |
|---|---|---|
| `Io` | `std::io::Error` | Lecture de fichiers |
| `Json` | `serde_json::Error` | Parse JSON avec position byte |
| `Db` | `tokio_postgres::Error` | Erreurs PostgreSQL génériques |
| `DbContext` | String construite | Erreurs PostgreSQL avec contexte (nom table/opération) |
| `InvalidInput` | String | Validation de paramètres |
| `Schema` | String | Erreurs de config TOML |
| `AnomalyReport` | String | Écriture du rapport d'anomalies |

---

## `src/anomaly/`

Gestion des anomalies de type : valeurs dont le type JSON diffère du type dominant de la colonne.

### `collector.rs`

- **`AnomalyExample`** : un exemple d'anomalie conservé en mémoire (row_id, valeur tronquée à 200 chars, type)
- **`AnomalyCollector`** : accumulateur d'anomalies pendant la Pass 2. Consommation mémoire bornée : compteurs + max 5 exemples par `(table, colonne)`. Si `anomaly_dir` est fourni, chaque anomalie est streamée dans `<dir>/<table>_anomalies.ndjson`.
  - `record()` → `Result<()>` : fast-path quand les exemples sont cappés et qu'il n'y a pas de streaming fichier
  - Méthodes : `record()`, `inc_total()`, `summaries()`, `total_anomalies()` O(1), `finish()` (flush + idempotent), `written_paths()`, `overall_anomaly_rate()`
- **`AnomalySummary`** : statistiques agrégées par `(table, colonne)` : count, total_rows, taux, jusqu'à 5 exemples

### `reporter.rs`

- **`write_report()`** : génère le rapport de synthèse en JSON ou CSV vers un fichier ou stdout
- Format JSON : `{ summaries, total_anomalies, overall_anomaly_rate }`, summaries triés par count desc
- Format CSV : `table, column, expected_type, anomaly_count, total_rows, anomaly_rate_pct, example_value, example_type`

---

## `src/db/`

Couche d'accès PostgreSQL.

### `connection.rs`

- **`connect()`** : établit une connexion `tokio_postgres` sans TLS, spawn la tâche de gestion de la connexion en background

### `ddl.rs`

- **`create_tables()`** : crée toutes les tables dans l'ordre topologique (parents avant enfants). Si `--drop-existing`, supprime d'abord en CASCADE. Ajoute les contraintes FK après la création de toutes les tables.
- **`generate_create_table()`** : génère le SQL `CREATE TABLE` complet pour une `TableSchema`
- **`quote_ident()`** : échappe les identifiants PostgreSQL avec guillemets doubles

### `copy_text.rs`

Type et fonction garantissant la sécurité du format COPY PostgreSQL texte au niveau du type Rust.

- **`CopyEscaped`** : newtype wrappant une `String` dont tous les caractères COPY-dangereux (`\t`, `\n`, `\r`, `\\`) ont été échappés et qui ne contient pas d'octet nul. Champ interne privé — s'obtient uniquement via `escape_copy_text()` ou `CopyEscaped::from_safe_ascii()`.
- **`escape_copy_text()`** : fast-path sans allocation si la chaîne ne contient aucun caractère spécial ; retourne `None` si la chaîne contient un octet nul.
- **`CopyEscaped::from_safe_ascii()`** : wrapping sans échappement pour les valeurs dont la sécurité est une invariante de compilation (entiers, booléens, UUIDs générés, etc.).

### `copy_sink.rs`

Implémente le chargement via le protocole `COPY FROM STDIN`.

- **`RowBuilder`** : construit une ligne au format texte COPY (colonnes séparées par `\t`, NULL représenté par `\N`). `push_value()` prend un `&CopyEscaped`.
- **`TempFileSink`** : accumule les lignes dans un fichier temporaire. `flush_to_db()` envoie le contenu en COPY puis réinitialise le sink — appelé périodiquement quand `row_count` atteint `batch_size`. `total_flushed` comptabilise les lignes envoyées sur tous les flush.
- **`copy_to_db()`** : ouvre une session COPY, transmet le fichier temporaire par blocs de 1 Mo, ferme la session.

---

## `src/io/`

Lecture de fichiers JSON, suivi de progression et communication avec l'IHM.

### `reader.rs`

Lecteur de fichiers JSON en streaming, sans charger tout le fichier en mémoire.

- **`JsonFormat`** : détecte automatiquement le format (`Array` = `[...]` ou `Lines` = NDJSON)
- **`JsonLinesReader`** : itérateur ligne par ligne pour le format NDJSON
- **`JsonArrayReader`** : parseur de tableau JSON par profondeur de bracket. Extrait chaque objet sans charger tout le tableau.
- **`JsonReader`** : enum unificateur exposant `Iterator<Item = serde_json::Value>` pour les deux formats

### `progress.rs`

- **`ProgressTracker`** : affiche deux barres de progression via `indicatif` (bytes et lignes). Affiche le débit en bytes/s et lignes/s. Utilisé uniquement en mode CLI pur (sans IHM).

### `progress_event.rs`

Protocole de communication entre les runners et l'IHM Dioxus.

- **`ProgressEvent`** : enum des événements streamés via un canal `tokio::sync::mpsc::UnboundedSender<ProgressEvent>`. Le sender est optionnel — `None` en mode CLI, fourni par l'IHM sinon.

| Variante | Données | Émis par |
|---|---|---|
| `Pass1Progress` | rows_scanned, bytes_read, total_bytes | Pass 1, périodique |
| `Pass1Done` | total_rows, tables_count, columns_count | Pass 1, fin |
| `Pass2Progress` | rows_processed, bytes_read, total_bytes | Pass 2, périodique |
| `Pass2Flush` | table_name, rows_flushed | Pass 2, à chaque COPY batch |
| `Pass2Log` | String | Pass 2, messages de log |
| `Pass2Done` | total_rows, anomaly_count | Pass 2, fin |

- **`ProgressTx`** : alias `tokio::sync::mpsc::UnboundedSender<ProgressEvent>`

---

## `src/pass1/`

### `runner.rs`

Orchestre la Pass 1. Lit le fichier en streaming, appelle `registry.observe_root()` pour chaque objet JSON racine, suit la progression, puis finalise le registre.

Retourne `Pass1Result` contenant :
- `schemas` : liste topologique des `TableSchema`
- `total_rows` : nombre total de lignes lues
- `stats` : statistiques de colonnes pour le rapport
- `truncated_names` : noms de tables tronqués à 63 bytes
- `column_collisions` : collisions de noms de colonnes résolues par hash

---

## `src/pass2/`

### `coercer.rs`

Convertit les valeurs JSON en format texte COPY PostgreSQL selon le type PG cible.

- **`CoerceResult`** : `Ok(CopyEscaped)`, `Null` ou `Anomaly`
- **`coerce()`** : dispatch principal vers des coerceurs spécialisés par type
- Types gérés : Integer (contrôle de plage i32), BigInt, DoublePrecision (NaN/Infini → NULL), Boolean (flexible : "yes"/"no"/"1"/"0"...), UUID, Date, Timestamp, Text, VarChar, Jsonb, Array PG
- **`coerce_pg_array()`** : sérialise un tableau JSON en litéral PostgreSQL `{elem1,elem2,NULL}`

### `runner.rs`

Orchestre la Pass 2. Relit le fichier et insère les données.

- **`run()`** : pour chaque objet racine, appelle `insert_object()` récursivement, remplit un `TempFileSink` par table, puis exécute les COPY dans l'ordre topologique avec flush périodique
- **`insert_object()`** : construit une ligne pour une table selon sa `WideStrategy` :
  - `Columns` : une colonne par champ JSON
  - `Pivot` : une ligne `(parent_id, key, value)` par champ
  - `Jsonb` : l'objet entier sérialisé en JSONB
  - `StructuredPivot` : une ligne par `(parent_id, base, val_suffixe1, val_suffixe2...)`
  - `KeyedPivot` : dispatche les sous-objets clé/valeur en lignes (fusion de tables sœurs) ; sérialise l'objet enfant dans `j2s_data JSONB` ; pour ObjectArray, une ligne par élément avec `j2s_order`
  - `AutoSplit` : colonnes stables → table principale, colonnes médiums → table `_wide` (EAV)
  - `Ignore` : clé supprimée
- **`insert_array()`** : gère les tableaux JSON. Si tableau d'objets → `insert_object()` récursif. Si tableau de scalaires → ligne de junction `(parent_id, order, value)`
- Mode **séquentiel** : une seule connexion PG, optionnellement dans une transaction
- Mode **parallèle** : N connexions simultanées avec un sémaphore, une connexion par table

---

## `src/schema/`

Modèle de données du schéma et toute la logique d'inférence.

### `table_schema.rs`

Définit les types de données fondamentaux du schéma :

- **`WideStrategy`** : stratégie de stockage pour les tables larges
  - `Columns` : colonnes normales (défaut)
  - `Pivot` : EAV générique
  - `Jsonb` : objet JSONB
  - `StructuredPivot(SuffixSchema)` : pivot par pattern de suffixes
  - `KeyedPivot(SiblingSchema)` : fusion de tables sœurs
  - `AutoSplit { stable_threshold, rare_threshold, medium_keys, wide_table_name }` : tri par fréquence
  - `Ignore` : clé supprimée
- **`ColumnSchema`** : définition d'une colonne (nom PG, nom original, type, nullable, is_generated)
- **`TableSchema`** : définition complète d'une table (nom, chemin JSON, colonnes, parent, profondeur, stratégie)
- **`ChildKind`** : `Object`, `ObjectArray`, `ScalarArray`
- **`KeyShape`** : forme des clés détectées (Numeric, IsoLang, Slug, Mixed)
- **`SuffixSchema`** / **`SiblingSchema`** : métadonnées pour StructuredPivot et KeyedPivot. `SiblingSchema` contient le nom de la colonne clé, la forme des clés (`KeyShape`), le flag `array_children` (ObjectArray vs Object) et `data_col_name` (nom de la colonne JSONB brute, toujours `j2s_data`)

### `type_tracker.rs`

Accumule les observations de type pour un champ JSON.

- **`TypeTracker`** : histogramme de types + max longueur chaîne + compteurs null/total
- **`InferredType`** : types JSON observés
- **`PgType`** : types PostgreSQL cibles avec méthode `as_sql()` pour la génération DDL

### `registry.rs`

Le cœur de la Pass 1. Accumule toutes les observations et construit les `TableSchema`.

- **`SchemaRegistry`** : registre central. `HashMap<String, TableEntry>` indexée par `path_key` (chemin JSON joint par `.`)
- **`TableEntry`** : état d'observation d'une table (colonnes, compteurs, type d'enfants)
- **`observe_root()`** → **`observe_object()`** → **`observe_array()`** : traversée récursive
- **`finalize()`** : construit les `TableSchema`, détecte les tables larges, applique les stratégies, trie topologiquement, déduplique, fusionne les sœurs, exclut les enfants absorbés
- **`exclude_absorbed_children()`** : fonction publique standalone, appelée après les surcharges TOML
- **`finalize_siblings()`** : détection et fusion des tables sœurs (KeyedPivot)
- **`collect_stats()`** : collecte les statistiques de colonnes pour le rapport

### `naming.rs`

Sanitisation et déduplication des noms d'identifiants PostgreSQL.

- **`NamingRegistry`** : mappe les chemins JSON vers des noms de tables PG uniques. Gère les truncations et les collisions via hash 7 hex (FNV).
- **`ColumnNameRegistry`** : détecte les collisions de noms de colonnes (plusieurs clés JSON → même identifiant SQL)
- **`sanitize_identifier()`** : minuscules, non-alphanumériques → `_`, déduplication des `_`
- **`truncate_to_pg_limit()`** : tronque à 63 bytes en préservant un hash d'unicité

### `config.rs`

Parsing et application des surcharges manuelles depuis un fichier TOML.

- **`SchemaConfig`** : structure parsée depuis le TOML (`HashMap<table_name, TableOverride>`)
- **`TableOverride`** : strategy + suffix_columns + overrides de colonnes
- **`apply_overrides()`** : applique les surcharges sur la liste de `TableSchema` finalisés
- **`parse_pg_type()`** : parse les chaînes de type (`INTEGER`, `BIGINT`, `VARCHAR(255)`, etc.)

### `stats.rs`

Rapport de statistiques du schéma post-inférence.

- **`ColumnStats`** : métadonnées d'une colonne (table, nom, type PG, taux nullité, histogramme de types)
- **`write_text_report()`** : rapport lisible en texte vers un writer (stderr ou fichier), avec marqueur `MIXED` pour les colonnes hétérogènes

### `suffix_detector.rs`

Détection automatique de patterns `{base}_{suffixe}` dans les tables larges.

- **`detect_suffix_schema()`** : analyse les colonnes d'une table large et identifie les suffixes fréquents
- **`build_suffix_schema_from_list()`** : construit un `SuffixSchema` depuis une liste explicite (TOML)

### `persistence.rs`

Sérialisation et désérialisation du résultat Pass 1 — permet à l'IHM de découpler les deux passes.

- **`SchemaSnapshot`** : struct sérialisable contenant `version`, `total_rows`, `schemas`, `truncated_names`, `column_collisions`, `stats`
- **`save()`** : sérialise un résultat Pass 1 en JSON vers un fichier
- **`load()`** : désérialise et vérifie la version (`SCHEMA_FORMAT_VERSION = 1`)

---

## `tests/`

Tests d'intégration Rust qui testent des cas end-to-end sur une vraie base PostgreSQL.

| Fichier | Couverture |
|---|---|
| `integration_schema.rs` | Inférence de schéma : tables, colonnes, types, normalisation |
| `integration_strategies.rs` | Stratégies wide : AutoSplit, Pivot, Jsonb, StructuredPivot, KeyedPivot |
| `integration_overrides.rs` | Surcharges TOML : types forcés, stratégies manuelles |
| `integration_anomalies.rs` | Pipeline d'anomalies : détection, rapport, max-anomaly-rate |
| `common/` | Helpers partagés (connexion DB, setup/teardown de schéma) |
| `fixtures/` | Fichiers JSON/NDJSON d'entrée pour les tests |

---

## Fichiers racine

| Fichier | Description |
|---|---|
| `Cargo.toml` | Dépendances Rust et métadonnées du projet |
| `Cargo.lock` | Versions exactes des dépendances (reproductibilité) |
| `openfoodfacts.toml` | Config TOML de surcharges pour le dataset OpenFoodFacts |
| `preprocess_off.py` | Script Python de prétraitement du fichier OpenFoodFacts |
| `schema_off.json` | Snapshot schéma Pass 1 sérialisé pour OpenFoodFacts |
| `schema_yelp_review.json` | Snapshot schéma Pass 1 sérialisé pour Yelp Review |
