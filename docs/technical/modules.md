# json2sql — Référence des modules

Rôle rapide de chaque fichier et dossier dans `src/`. Pour le détail des types et fonctions, voir [architecture.md](architecture.md).

## Convention des headers `//!`

Chaque fichier `.rs` de `src/`, `json2sql-worker/src/` et `json2sql-ui/src/` porte un header `//!` avec :
1. La/les responsabilité(s) réelles du fichier (prose courte).
2. Une liste `Fonctions :` couvrant **toutes** les fonctions du fichier (publiques et privées, hors `#[cfg(test)]`), une entrée par fonction avec sa responsabilité en quelques mots — plus court que le `///` existant de la fonction, pas un résumé complet.

Règles :
- Méthodes d'`impl` : préfixer par le type (`Type::method`), pas juste le nom, pour lever l'ambiguïté entre méthodes homonymes de types différents dans le même fichier.
- Noms de fonctions en simple `` `code span` ``, jamais en lien intra-doc `` [`nom`] `` — un lien vers un item privé casse sous `cargo doc` et déclenche les lints deny du crate `json2sql-ui`. Les liens `` [`Type`] `` restent réservés aux types publics.
- Fichier à un seul point d'entrée (ex. composant Dioxus avec logique en closures internes) : lister honnêtement cette unique fonction, ne pas forcer une structure absente.
- Un fichier est candidat à un refactor futur si sa responsabilité ne peut pas s'exprimer en un seul domaine cohérent (2+ domaines non reliés par un rapport direct de cause à effet) — voir la liste de candidats dans l'issue de suivi.

---

## Racine `src/`

| Fichier | Rôle |
|---|---|
| `main.rs` | Point d'entrée. Orchestre la séquence complète : parse CLI → Pass 1 → overrides TOML → Pass 2 → rapport |
| `lib.rs` | Déclare tous les modules publics (nécessaire pour les tests d'intégration dans `tests/`) |
| `cli.rs` | Définit la struct `Cli` via `clap` — tous les flags et options de la commande |
| `error.rs` | Enum `J2sError` et alias `Result<T>` utilisés dans tout le projet |

---

## `src/pass1/`

Pass 1 : lecture du fichier en streaming pour inférer le schéma.

| Fichier | Rôle |
|---|---|
| `runner.rs` | Orchestre la Pass 1 : itère sur les objets JSON racine, appelle le registre, suit la progression. Retourne `Pass1Result` |
| `mod.rs` | Déclare le module et ré-exporte `Pass1Result` |

---

## `src/pass2/`

Pass 2 : relecture du fichier pour insérer les données.

| Fichier | Rôle |
|---|---|
| `runner.rs` | Orchestre la Pass 2 : dispatcher → N workers → flush task → N conn tasks → PostgreSQL COPY |
| `insert.rs` | `insert_object()` — orchestrateur (~50 lignes). Construit les colonnes générées et délègue à `traversal.rs` selon la `WideStrategy` |
| `traversal.rs` | Toutes les fonctions `insert_*` privées : traversal du graphe JSON selon les stratégies (`insert_pivot_object`, `insert_jsonb_object`, `insert_flatten`, `insert_array`…) |
| `sink.rs` | `trait RowSink { write_row() }` — abstraction sur l'écriture de lignes. `TempFileSink` (dans `db/copy_sink.rs`) en est l'implémentation concrète |
| `coercer.rs` | Convertit les valeurs JSON en format texte COPY PostgreSQL selon le type PG cible. Produit `Ok(CopyEscaped)`, `Null` ou `Anomaly` |
| `mod.rs` | Déclare le module |

---

## `src/schema/`

Modèle de données du schéma, inférence, nommage, config et persistance.

| Fichier | Rôle |
|---|---|
| `table_schema.rs` | Types fondamentaux : `TableSchema`, `ColumnSchema`, `InferredStrategy`, `UserOverride`, `ChildKind`, `KeyShape`, `SuffixSchema`, `SiblingSchema` |
| `type_tracker.rs` | `TypeTracker` : histogramme de types par colonne. `InferredType` et `PgType` avec règles de résolution et d'élargissement |
| `observer.rs` | `SchemaObserver` : accumule les observations row-by-row via `observe_root()` et `merge()`. Contient `TableEntry` (privé) |
| `finalizer.rs` | `SchemaFinalizer` : transformations post-stream — construit les `TableSchema`, applique les stratégies, trie topologiquement. Contient `apply_column_limit_guard()`, `exclude_absorbed_children()` |
| `cascading/` | Détection et fusion des tables sœurs (BFS cascade) — voir sous-section dédiée ci-dessous |
| `wide_strategies.rs` | Fonctions d'application des stratégies wide : `apply_wide_strategy_columns`, `apply_flatten`, `apply_jsonb_flatten`, `apply_normalize_dynamic_keys`, `build_union_columns`, `classify_key_shape` |
| `strategies.rs` | `StrategyName` (enum des stratégies optionnelles désactivables : `Sibling`, `Pivot`, `StructuredPivot`), `StrategyError`, `parse_disabled_strategies()`. Utilisé par `Pass1Config` et `SchemaRegistry` pour gater les stratégies via `--disable-strategy`. |
| `inspector.rs` | Statistiques d'inspection : `collect_stats()` construit les `ColumnStats` (histogramme de types) pour le mode `--inspect` |
| `registry.rs` | `SchemaRegistry` : façade publique (~100 lignes prod). Délègue à `observer`, `finalizer` et `inspector` |
| `naming.rs` | Sanitisation des identifiants PG, déduplication, troncature à 63 bytes avec hash, détection de collisions de noms de colonnes |
| `suffix_detector.rs` | Détecte les patterns `{base}_{suffixe}` dans les tables larges pour la stratégie `StructuredPivot` |
| `config.rs` | Parse et applique le fichier TOML de surcharges manuelles (`--schema-config`) |
| `stats.rs` | Struct `ColumnStats` + génère le rapport texte de statistiques de colonnes (types inférés, taux de nullité, colonnes MIXED) |
| `persistence.rs` | Sérialise/désérialise un résultat Pass 1 en JSON (`SchemaSnapshot`) — utilisé par l'IHM pour séparer Pass 1 et Pass 2 |
| `mod.rs` | Déclare le module, exporte `PATH_SEP` |

### `src/schema/cascading/`

BFS cascade — détection des groupes de tables sœurs (*siblings*) et fusion (*collapse*) en tables canoniques.

| Fichier | Rôle |
|---|---|
| `detection.rs` | `finalize_cascading()` — orchestre les vagues BFS. Fichier dense (~1200 lignes, 39 fonctions privées) : candidat à une décomposition en sous-modules |
| `scoring.rs` | Jaccard (`pairwise_jaccard_min`), clustering glouton — fonctions pures sur `&[TableSchema]` et indices |
| `merge.rs` | `build_sibling_collapse_from_siblings()` — API publique pour la fusion manuelle depuis l'IHM |
| `mod.rs` | Déclare les sous-modules, ré-exporte `finalize_cascading` |

---

## `src/db/`

Couche d'accès PostgreSQL.

| Fichier | Rôle |
|---|---|
| `connection.rs` | Établit une connexion `tokio_postgres` (sans TLS) |
| `ddl.rs` | Génère et exécute les `CREATE TABLE` en ordre topologique. Gère `DROP ... CASCADE` et les contraintes FK |
| `copy_text.rs` | Newtype `CopyEscaped` : garantit au niveau du type que les valeurs sont COPY-safe (échappement `\t`, `\n`, `\r`, `\\`, nul) |
| `copy_sink.rs` | `TempFileSink` : accumule les lignes dans un fichier temporaire, puis exécute `COPY FROM STDIN` par batch (`flush_to_db`) |
| `mod.rs` | Déclare le module |

---

## `src/io/`

Lecture de fichiers JSON et communication avec l'IHM.

| Fichier | Rôle |
|---|---|
| `reader.rs` | Lecteur JSON streaming : détecte automatiquement le format Array `[...]` ou NDJSON. Itérateur d'objets JSON sans chargement complet en mémoire |
| `progress.rs` | `ProgressTracker` : affiche deux barres de progression via `indicatif` (bytes et lignes, débit en temps réel) — utilisé en mode CLI |
| `progress_event.rs` | `ProgressEvent` : enum des événements streamés vers l'IHM Dioxus via un canal `UnboundedSender`. Couvre Pass1Progress, Pass1Done, Pass2Progress, Pass2Flush, Pass2Log, Pass2Done |
| `mod.rs` | Déclare le module |

---

## `src/anomaly/`

Détection et rapport des anomalies de type.

| Fichier | Rôle |
|---|---|
| `collector.rs` | `AnomalyCollector` : accumule les anomalies pendant la Pass 2. Consommation mémoire bornée (5 exemples max par colonne). Streaming optionnel vers `<dir>/<table>_anomalies.ndjson` |
| `reporter.rs` | `write_report()` : génère le rapport de synthèse en JSON ou CSV |
| `mod.rs` | Déclare le module |

---

## `json2sql-worker/src/`

Process worker séparé : exécute le pipeline d'import (Pass 2) hors du process IHM, piloté via stdin/socket Unix.

| Fichier | Rôle |
|---|---|
| `main.rs` | Point d'entrée : lit la config sur stdin, verrouille le lockfile, sert le socket en parallèle du pipeline |
| `pipeline.rs` | `run_pipeline()` — connexion PG → DDL → Pass 2 → `WorkerResult`, relaie les événements vers `ImportSummary` |
| `serve.rs` | Sert le socket Unix : diffuse historique + événements en direct, traduit `{"cmd":"cancel"}` |
| `summary.rs` | `ImportSummary` — état en mémoire de l'import (historique d'événements + notification) |
| `cancel.rs` | `CancelToken` — signal d'annulation partagé entre le handler de commande et la boucle d'import |

---

## `json2sql-ui/src/`

IHM desktop Dioxus (5 écrans du workflow d'import).

| Fichier | Rôle |
|---|---|
| `main.rs` | Point d'entrée Dioxus : construit la fenêtre, route vers l'écran courant |
| `state.rs` | `AppState` — état racine (candidat refactor : mélange définitions d'état, logique de sélection UI, calcul Jaccard) |
| `config.rs` | Persistance de la config projet (`~/.config/json2sql/last_project.toml`, sans password) |
| `worker_client.rs` | Client du worker subprocess : spawn, connexion socket, lecture des événements, détection de résumption |
| `theme.rs` | Charge le CSS embarqué (`assets/styles.css`) |

### `json2sql-ui/src/screens/`

| Fichier | Rôle |
|---|---|
| `mod.rs` | Utilitaires partagés : view-model de la liste de tables, sélecteurs de fichiers, composant `ProgressBar`, application des overrides |
| `setup.rs` | Écran 1 — Setup (stepper 4 accordéons) |
| `analysis.rs` | Écran 2 — Analysis (progression Pass 1) |
| `strategy.rs` | Écran 3 — Strategy Editor |
| `preview.rs` | Écran 4 — Aperçu SQL (DDL + coloration syntaxique) |
| `import.rs` | Écran 5 — Import (Pass 2, lance/pilote le worker) |
| `resume.rs` | Écran affiché au démarrage si un worker actif est détecté |
| `table_list.rs` | Composant partagé : liste de tables avec badges (purement présentationnel) |

---

## `tests/`

Tests d'intégration end-to-end (Pass 1 + Pass 2 sur une vraie base PostgreSQL).

| Fichier | Rôle |
|---|---|
| `integration_schema.rs` | Tests de l'inférence de schéma : tables, colonnes, types, normalisation |
| `integration_strategies.rs` | Tests des stratégies wide : AutoSplit, Pivot, Jsonb, StructuredPivot, KeyedPivot |
| `integration_overrides.rs` | Tests des surcharges TOML : types forcés, stratégies manuelles |
| `integration_anomalies.rs` | Tests du pipeline d'anomalies : détection, rapport, max-anomaly-rate |
| `common/` | Helpers partagés entre les fichiers de tests |
| `fixtures/` | Fichiers JSON/NDJSON utilisés comme entrées de test |

---

## Fichiers racine notables

| Fichier | Rôle |
|---|---|
| `Cargo.toml` | Dépendances Rust et métadonnées du projet |
| `openfoodfacts.toml` | Config TOML de surcharges pour le dataset OpenFoodFacts (exemple réel) |
| `preprocess_off.py` | Script Python de prétraitement du fichier OpenFoodFacts |
| `schema_off.json` / `schema_yelp_review.json` | Snapshots de schéma Pass 1 sérialisés (format `SchemaSnapshot`) |
