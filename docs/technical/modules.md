# json2sql — Référence des modules

Rôle rapide de chaque fichier et dossier dans `src/`. Pour le détail des types et fonctions, voir [architecture.md](architecture.md).

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
| `runner.rs` | Orchestre la Pass 2 : pour chaque objet, construit les lignes selon la `WideStrategy`, remplit les `TempFileSink`, flush par batch vers PostgreSQL |
| `coercer.rs` | Convertit les valeurs JSON en format texte COPY PostgreSQL selon le type PG cible. Produit `Ok(CopyEscaped)`, `Null` ou `Anomaly` |
| `mod.rs` | Déclare le module |

---

## `src/schema/`

Modèle de données du schéma, inférence, nommage, config et persistance.

| Fichier | Rôle |
|---|---|
| `table_schema.rs` | Types fondamentaux : `TableSchema`, `ColumnSchema`, `WideStrategy`, `ChildKind`, `KeyShape`, `SuffixSchema`, `SiblingSchema` |
| `registry.rs` | Cœur de la Pass 1 : `SchemaRegistry` accumule les observations et construit les `TableSchema` via `finalize()` |
| `type_tracker.rs` | `TypeTracker` : histogramme de types par colonne. `InferredType` et `PgType` avec règles de résolution et d'élargissement |
| `naming.rs` | Sanitisation des identifiants PG, déduplication, troncature à 63 bytes avec hash, détection de collisions de noms de colonnes |
| `suffix_detector.rs` | Détecte les patterns `{base}_{suffixe}` dans les tables larges pour la stratégie `StructuredPivot` |
| `config.rs` | Parse et applique le fichier TOML de surcharges manuelles (`--schema-config`) |
| `stats.rs` | Génère le rapport de statistiques de colonnes (types inférés, taux de nullité, colonnes MIXED) |
| `persistence.rs` | Sérialise/désérialise un résultat Pass 1 en JSON (`SchemaSnapshot`) — utilisé par l'IHM pour séparer Pass 1 et Pass 2 |
| `mod.rs` | Déclare le module |

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
