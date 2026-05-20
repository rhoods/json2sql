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

La DDL est séparée en deux temps : **création de tables sans contraintes** (avant le chargement) + **ajout des contraintes** (après le chargement). Cela permet de charger en parallèle sans conflit de FK.

- **`create_tables_no_constraints()`** : crée toutes les tables sans PK ni FK (`IF NOT EXISTS`). Si `drop_existing = true`, supprime d'abord en CASCADE. À appeler avant `pass2::runner::run()`.
- **`create_tables()`** : version legacy (DDL + contraintes en une passe). Utilisée par `main.rs` dans les chemins non-parallèles.
- **`generate_create_table_no_constraints()`** : génère le SQL `CREATE TABLE IF NOT EXISTS` sans contraintes pour un schéma.
- **`generate_create_table()`** : génère le SQL `CREATE TABLE` complet avec PK inline (used by `create_tables()`).
- **`generate_add_pk_sql()`** : génère l'`ALTER TABLE … ADD CONSTRAINT … PRIMARY KEY` pour un schéma.
- **`generate_add_fk_sql()`** : génère l'`ALTER TABLE … ADD CONSTRAINT … FOREIGN KEY` pour un schéma enfant. Retourne `None` pour les tables racines.
- **`add_constraints()`** : ajoute d'abord les PK (fatal en cas d'erreur — collision UUID = bug), puis les FK (échecs → `ConstraintWarning`). Appelé en Phase D de la Pass 2, après le chargement complet.
- **`ConstraintWarning`** / **`ConstraintKind`** : contrainte non appliquée après le chargement. `ConstraintKind::PrimaryKey` est toujours fatal ; `ForeignKey` produit un warning.
- **`generate_ddl_preview()`** : DDL lisible avec FK inline, pour affichage uniquement (dry-run, IHM).
- **`quote_ident()`** : échappe les identifiants PostgreSQL avec guillemets doubles.

### `copy_text.rs`

Type et fonction garantissant la sécurité du format COPY PostgreSQL texte au niveau du type Rust.

- **`CopyEscaped`** : newtype wrappant une `String` dont tous les caractères COPY-dangereux (`\t`, `\n`, `\r`, `\\`) ont été échappés et qui ne contient pas d'octet nul. Champ interne privé — s'obtient uniquement via `escape_copy_text()` ou `CopyEscaped::from_safe_ascii()`.
- **`escape_copy_text()`** : fast-path sans allocation si la chaîne ne contient aucun caractère spécial ; retourne `None` si la chaîne contient un octet nul.
- **`CopyEscaped::from_safe_ascii()`** : wrapping sans échappement pour les valeurs dont la sécurité est une invariante de compilation (entiers, booléens, UUIDs générés, etc.).

### `copy_sink.rs`

Implémente le chargement via le protocole `COPY FROM STDIN`.

Constantes publiques :
- **`MAX_OPEN_TEMP_FILES`** (950) : nombre maximal de FD temp-file ouverts simultanément dans le processus. En-dessous du ulimit typique de 1024 pour laisser de la marge aux connexions PG et autres FD.
- **`INTERIM_FLUSH_THRESHOLD`** (512 MiB) : seuil par worker au-delà duquel le sink le plus gros est expédié à la flush task pour libérer l'espace disque.

Types et fonctions :
- **`RowBuilder`** : construit une ligne au format texte COPY (colonnes séparées par `\t`, NULL représenté par `\N`). `push_value()` prend un `&CopyEscaped`. `push_null()` et `push_uuid()` disponibles. `finish()` ajoute le `\n` et retourne le buffer.
- **`TempFileSink`** : accumule les lignes d'une table pendant la Pass 2. Utilise un **buffer in-memory `pending`** qui se renverse dans un fichier temporaire quand il dépasse `SPILL_THRESHOLD` (256 KiB). Champs publics : `table_name`, `row_count`, `total_flushed`, `bytes_buffered`.
  - `write_row()` : ajoute une ligne ; déclenche un spill si `pending` dépasse le seuil.
  - `hibernate()` : ferme le FD du fichier temporaire **sans toucher `pending`**. Coût = un syscall `close()`. Le FD est rouvert sur le prochain spill.
  - `is_open()` : vrai si un FD est actuellement ouvert.
  - `flush_to_db()` : envoie toutes les données (fichier + `pending`) en COPY puis réinitialise le sink pour réutilisation (flush périodique).
  - `copy_to_db()` : flush final, consomme le sink, supprime le fichier temporaire.
- **`merge_copy_to_db(sinks, client)`** : ouvre **une seule session COPY** et y stream les données de N sinks appartenant à la même table. Réduit le overhead COPY de `N_workers × N_tables` à `~N_tables` dans le cas des petites tables.

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
| `Pass2Done` | total_rows, anomaly_count, constraint_warning_count | Pass 2, fin |

- **`ProgressTx`** : alias `tokio::sync::mpsc::UnboundedSender<ProgressEvent>`

---

## `src/pass1/`

### `runner.rs`

Orchestre la Pass 1. Lit le fichier en streaming, appelle `SchemaObserver::observe_root()` pour chaque objet JSON racine, suit la progression, puis délègue la finalisation à `SchemaFinalizer`.

Retourne `Pass1Result` contenant :
- `schemas` : liste topologique des `TableSchema`
- `total_rows` : nombre total de lignes lues
- `stats` : statistiques de colonnes pour le rapport
- `truncated_names` : noms de tables tronqués à 63 bytes
- `column_collisions` : collisions de noms de colonnes résolues par hash

---

## `src/pass2/`

### `sink.rs`

Abstraction sur l'écriture de lignes, découplant la logique d'insertion du stockage concret.

- **`trait RowSink`** : `write_row(&mut self, row: Vec<u8>) -> Result<()>`. Implémenté par `TempFileSink` (dans `db/copy_sink.rs`) ; une implémentation in-memory peut être fournie pour les tests unitaires.

### `traversal.rs`

Toutes les fonctions `insert_*` privées — traversal du graphe JSON selon les `WideStrategy`.

- `insert_pivot_object()` : une ligne `(parent_id, key, value)` par champ
- `insert_jsonb_object()` : l'objet entier sérialisé en JSONB
- `insert_structured_pivot_object()` : une ligne par `(parent_id, base, val_suffixe1…)`
- `dispatch_child_routes()` : dispatch récursif vers les tables enfants
- `insert_keyed_pivot_object()` / `insert_keyed_pivot_array_of_objects()` : fusion de tables sœurs
- `insert_normalize_dynamic_keys()` : clé/valeur normalisée depuis les sous-objets dynamiques
- `insert_multi_keyed_pivot()` : pivot multiple via tables synthétiques
- `insert_array()` : tableau de scalaires → table de jonction ; tableau d'objets → `insert_object()` récursif

### `coercer.rs`

Convertit les valeurs JSON en format texte COPY PostgreSQL selon le type PG cible.

- **`CoerceResult`** : `Ok(CopyEscaped)`, `Null` ou `Anomaly`
- **`coerce()`** : dispatch principal vers des coerceurs spécialisés par type
- Types gérés : Integer (contrôle de plage i32), BigInt, DoublePrecision (NaN/Infini → NULL), Boolean (flexible : "yes"/"no"/"1"/"0"...), UUID, Date, Timestamp, Text, VarChar, Jsonb, Array PG
- **`coerce_pg_array()`** : sérialise un tableau JSON en litéral PostgreSQL `{elem1,elem2,NULL}`

### `runner.rs`

Orchestre la Pass 2. Relit le fichier et insère les données via une architecture **3 couches** :

```
Dispatcher (main task)
  └─► N Worker tasks  ──flush_tx──►  Flush task (accumulation)
                                         └─► N Conn tasks  ──► PostgreSQL
```

**Signature `run()`** :

```rust
pub async fn run(
    path: &Path,
    root_table: &str,
    schemas: &[TableSchema],
    client: &Client,     // pour les contraintes (Phase D)
    pg_url: &str,        // pour les N connexions COPY
    pg_schema: &str,
    parallel: usize,
    anomaly_dir: Option<PathBuf>,
    progress_tx: Option<ProgressTx>,
) -> Result<Pass2Result>
```

Le `client` est utilisé uniquement pour la Phase D (contraintes). Les COPYs passent par des connexions dédiées ouvertes à partir de `pg_url`.

**Phase B — Streaming parallèle (`parallel` workers)** :
- Le dispatcher lit le fichier raw byte-par-byte et envoie chaque objet JSON (sérialisé `Vec<u8>`) à un worker en round-robin.
- Chaque worker parse avec `simd_json`, appelle `insert_object()` récursivement, et accumule les lignes dans son propre `HashMap<table_name, TempFileSink>`.
- **Gestion des FD** : un compteur global `AtomicUsize` trace les FD ouverts entre tous les workers. Quand le budget global (`FD_GLOBAL_THRESHOLD = MAX_OPEN_TEMP_FILES × 90%`) ou le budget par worker est dépassé, tous les sinks du worker sont hibernés (`hibernate()`).
- **Interim flush** : quand le total en bytes d'un worker dépasse `INTERIM_FLUSH_THRESHOLD`, le sink le plus gros est hiberné et envoyé à la flush task via `flush_tx`. Un sink vide le remplace.
- **Annulation** : un `CancellationToken` + `DropGuard` est créé au début de `run()`. Si le caller avorte la tâche Tokio (ex : bouton Cancel de l'IHM), le guard est droppé, le token est annulé, et tous les workers/flush/conn tasks sortent de leur `tokio::select!`.

**Flush task — accumulateur par table** :
- Reçoit les `(table_name, TempFileSink)` des workers via `flush_rx`.
- Accumule par table dans `table_pending: HashMap<String, Vec<TempFileSink>>`.
- **Dispatch anticipé** : si une table accumule ≥ 1 MiB de bytes, ses sinks sont envoyés immédiatement à un conn worker (tables larges).
- **Dispatch final** : une fois `flush_rx` fermé (tous les workers terminés), les sinks restants (tables petites) sont envoyés table par table — `merge_copy_to_db()` fusionne tous les sinks en un seul COPY.

**Conn workers (`parallel` tâches)** :
- Chaque conn worker ouvre une connexion PG indépendante via `pg_url`.
- Reçoit des `Vec<TempFileSink>` (sinks d'une même table) et appelle `merge_copy_to_db()`.
- Renvoie `Result<(table_name, row_count)>` via `result_tx`.

**Phase D — Contraintes** :
- `add_constraints(client, schemas, pg_schema)` ajoute les PK (fatal en cas d'erreur) puis les FK (échecs → `constraint_warnings`).

**Types de retour** :
- **`Pass2Timing { streaming_ms, copy_ms }`** : durée de chaque phase. `total_ms()` retourne la somme.
- **`Pass2Result { rows_per_table, anomaly_collector, constraint_warnings, timing }`** : résumé complet de la Pass 2.

**`insert_object()`** (dans `insert.rs`) : orchestrateur principal (~50 lignes). Construit les colonnes générées (UUID, FK parent, j2s_order) puis délègue à `traversal.rs` selon la `WideStrategy` :
  - `Columns` : une colonne par champ JSON
  - `Pivot` : `insert_pivot_object()` — une ligne `(parent_id, key, value)` par champ
  - `Jsonb` : `insert_jsonb_object()` — l'objet entier sérialisé en JSONB
  - `StructuredPivot` : `insert_structured_pivot_object()` — une ligne par `(parent_id, base, val_suffixe1...)`
  - `KeyedPivot` : `insert_keyed_pivot_object()` / `insert_keyed_pivot_array_of_objects()` — fusion de tables sœurs ; sérialise l'objet enfant dans `j2s_data JSONB`
  - `AutoSplit` : colonnes stables → table principale, colonnes médiums → table `_wide` (EAV)
  - `Ignore` : clé supprimée

**Traversal des tableaux** (dans `traversal.rs`) : `insert_array()` — tableau d'objets → `insert_object()` récursif ; tableau de scalaires → ligne de jonction `(parent_id, order, value)`.

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

### `observer.rs`

Observation mutable row-by-row pendant le streaming JSON.

- **`SchemaObserver`** : accumule les observations dans un `IndexMap<String, TableEntry>` indexé par `path_key` (chemin JSON joint par `\x00`)
- **`TableEntry`** (privé) : état d'observation d'une table (colonnes `IndexMap<String, TypeTracker>`, compteurs, type d'enfants)
- **`observe_root()`** → **`observe_object()`** → **`observe_array()`** : traversée récursive — enregistre chaque champ JSON dans le `TypeTracker` correspondant
- **`merge()`** : fusionne deux `SchemaObserver` (parallélisme de la Pass 1)

### `finalizer.rs`

Transformations post-stream sur `Vec<TableSchema>` — après que l'observation est terminée.

- **`SchemaFinalizer`** : configuration (`wide_column_threshold`, `sibling_threshold`, `jaccard_threshold`…)
- **`run()`** : pipeline complet — construit les `TableSchema`, détecte les tables larges, applique les stratégies via `wide_strategies`, trie topologiquement, délègue la détection de sœurs à `cascading::finalize_cascading()`
- **`build_entry_schema()`** : convertit un `TableEntry` en `TableSchema` avec résolution des types
- **`apply_column_limit_guard()`** : détecte les tables dépassant `PG_MAX_COLUMNS` (1 600) et applique Jsonb
- **`exclude_absorbed_children()`** : supprime les tables enfants absorbées par une stratégie wide (Flatten, NormalizeDynamicKeys…)
- **`OverflowWarning`** : type retourné quand une table est forcée en Jsonb par le garde-fou

### `cascading.rs`

Algorithme de détection et fusion des tables sœurs (sibling detection).

- **`finalize_cascading()`** : détecte les tables sœurs candidate et les fusionne en `KeyedPivot` ou `MultiKeyedPivot`
- **`child_compatibility_score()`** : score de compatibilité structurelle entre deux tables sœurs (jaccard sur les colonnes)
- **`pairwise_jaccard_min()`** : jaccard minimum parmi toutes les paires d'un groupe
- **`build_parent_child_maps()`**, **`run_sibling_wave()`**, **`process_co_sibling_group()`** : helpers de l'algorithme de fusion par vagues

### `wide_strategies.rs`

Application des stratégies de stockage wide sur les `TableSchema`.

- **`apply_wide_strategy_columns()`** : restructure les colonnes d'une table selon une `WideStrategy` (Pivot, Jsonb, StructuredPivot…)
- **`apply_structured_pivot_columns()`** : colonnes pour StructuredPivot — `(name TEXT, value <type>, <suffix_col>...)`
- **`apply_normalize_dynamic_keys()`** : collapse les sous-objets dynamiques d'une table en une seule table normalisée avec colonne d'ID
- **`apply_flatten()`** : inline les colonnes scalaires d'un enfant dans le parent avec un préfixe
- **`apply_jsonb_flatten()`** : inline un enfant comme une seule colonne JSONB sur le parent
- **`build_union_columns()`** : union des colonnes de plusieurs tables sœurs (plus large type par colonne)
- **`classify_key_shape()`** / **`suggest_wide_strategy()`** : classification des clés et sélection automatique de la stratégie

### `reporter.rs`

Lecture des résultats d'observation après finalisation.

- **`collect_stats()`** : collecte les statistiques de colonnes (`ColumnStats`) depuis l'observer
- **`anomaly_iter()`** : itérateur sur les anomalies de type détectées
- **`truncated_names()`** / **`column_collisions()`** : noms tronqués et collisions de colonnes

### `registry.rs`

Façade publique (~100 lignes de code prod) qui délègue à `SchemaObserver`, `SchemaFinalizer` et `SchemaReporter`.

- **`SchemaRegistry`** : agrège observer + finalizer + reporter. Expose `observe_root()`, `merge()`, `finalize()`, `collect_stats()`, `anomaly_iter()`, `truncated_names()`, `column_collisions()`
- Conservé pour la compatibilité de l'API publique — ne contient plus de logique métier

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
