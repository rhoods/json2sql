# json2sql — Référence des modules

Rôle rapide de chaque fichier et dossier dans `src/`. Pour le détail des types et fonctions, voir [architecture.md](architecture.md).

## Convention des headers `//!`

Chaque fichier `.rs` de `src/`, `json2sql-worker/src/` et `json2sql-ui/src/` porte un header `//!` avec :
1. La/les responsabilité(s) réelles du fichier (prose courte).
2. Un bloc `Fonctions :` couvrant **toutes** les fonctions, méthodes, structs et enums du fichier (publics et privés, hors `#[cfg(test)]`), au format strict tagué ci-dessous — un vérificateur CI (issue #33) s'appuie dessus, le format doit rester trivialement parsable ligne à ligne.

**Format (une entrée par ligne) :**
```
//! Fonctions :
//! - fn `{nom}` — description libre
//! - fn `{Type}::{méthode}` — description libre
//! - struct `{Nom}` — description libre
//! - enum `{Nom}` — description libre
```

Règles :
- **Une ligne = une entrée.** Jamais de regroupement virgule/point-virgule, jamais deux entrées sur la même ligne.
- Le nom qualifié est **entre backticks**, juste après le tag (`fn`/`struct`/`enum`) — sans backticks, `cargo clippy` sous `#![deny(clippy::pedantic)]` (crate `json2sql-ui`) échoue sur `doc_markdown` (identifiant en case mixte hors code span). Pas de décoration d'argument dans le nom (ex. `(&UserOverride)`) : ce détail va dans la description libre après le `—`.
- Méthodes d'`impl` : toujours `fn `{Type}::{méthode}``, jamais juste le nom, pour lever l'ambiguïté entre méthodes homonymes de types différents. `impl Trait for Type` → `Type` est toujours le type receveur, jamais le trait (ex. `impl std::fmt::Display for KeyShape` → `` fn `KeyShape::fmt` ``). Self-type générique recopié littéralement (ex. `impl RowSink for Arc<Mutex<MemSink>>` → `` fn `Arc<Mutex<MemSink>>::write_row` ``).
- Les libellés de section (ex. `Orchestration :`, `Flusher :` sur les gros fichiers comme `pass2/runner/flusher.rs`) restent autorisés comme lignes libres **non taguées** entre le marqueur `Fonctions :` et les entrées — le parseur n'agit que sur les lignes commençant par `- fn `/`- struct `/`- enum `, tout le reste est ignoré.
- Fichier à un seul point d'entrée (ex. composant Dioxus avec logique en closures internes) : lister honnêtement cette unique fonction, ne pas forcer une structure absente.
- Fonctions/structs/enums imbriqués dans le corps d'une autre fonction (helper local, type ad-hoc pour une sérialisation ponctuelle) ne sont **pas** des définitions à documenter — même règle que les closures. Une signature de méthode de `trait` sans corps (`trait X { fn y(...); }`) non plus : ce sont les `impl X for Type` qui portent l'implémentation réelle à documenter.
- Fichier sans fonction/struct/enum à documenter (ex. `mod.rs` ne déclarant que des sous-modules) : pas de bloc `Fonctions :` du tout.
- Un fichier est candidat à un refactor futur si sa responsabilité ne peut pas s'exprimer en un seul domaine cohérent (2+ domaines non reliés par un rapport direct de cause à effet) — voir la liste de candidats dans l'issue de suivi.

**Vérification :** `cargo test --test header_consistency` (issue #33) compare chaque header au code réel dans les deux sens et échoue avec la liste complète des incohérences si le header dérive. Ce test tourne automatiquement via `make test`/`make check` — aucune configuration CI séparée n'est nécessaire.

---

## Racine `src/`

| Fichier | Rôle |
|---|---|
| `main.rs` | Point d'entrée CLI : parse les arguments, gère `inspect`, construit `PipelineConfig` |
| `pipeline.rs` | `run_pipeline()` indépendant de clap : stdin → Pass 1 → overrides TOML → dry-run ou Pass 2 → rapport |
| `ipc.rs` | Protocole IHM ↔ worker (config JSON sur stdin, socket Unix, résultat sur disque) — Linux/macOS uniquement |
| `lib.rs` | Déclare tous les modules publics (nécessaire pour les tests d'intégration dans `tests/`) |
| `cli.rs` | Définit la struct `Cli` via `clap` — tous les flags et options de la commande |
| `error.rs` | Enum `J2sError` et alias `Result<T>` utilisés dans tout le projet |

---

## `src/pass1/`

Pass 1 : lecture du fichier en streaming pour inférer le schéma.

| Fichier | Rôle |
|---|---|
| `runner.rs` | Orchestre la Pass 1 : itère sur les objets JSON racine, appelle le registre, suit la progression. Le thread de lecture nourrit `RowSampler` ; à la fin, `samples_of` tire des lignes gardées les échantillons des tables larges (#91). Retourne `Pass1Result` |
| `row_sample.rs` | Échantillon borné des lignes racine (#91, phase 2) : `RowSampler` garde la première ligne qui finit après chaque multiple de `taille / 20 000` octets, sous 20 000 lignes et 256 Mo ; au-delà, une ligne sur deux part et le pas double. Seul le thread de lecture choisit, donc l'échantillon ne dépend pas du nombre de workers |
| `mod.rs` | Déclare le module et ré-exporte `Pass1Result` |

---

## `src/pass2/`

Pass 2 : relecture du fichier pour insérer les données.

| Fichier | Rôle |
|---|---|
| `runner/` | Pipeline diskless : `mod.rs` (`run()`), `dispatch.rs` (dispatcher round-robin), `worker.rs` (parse + `MemSink` par table), `flusher.rs` (COPY vers PG, pause sous pression mémoire), `config.rs` (`Pass2Config`) |
| `insert.rs` | `insert_object()` — orchestrateur. Écrit la ligne de la table (`write_row_of`), puis la ligne 1:1 de chaque table de groupe de colonnes (`write_group_rows`, #78 : même `j2s_id` et même clé étrangère que la ligne d'origine, seulement si une colonne du groupe a une valeur au sens de `is_present`), puis délègue à `traversal.rs` selon la `WideStrategy` |
| `traversal.rs` | Toutes les fonctions `insert_*` privées : traversal du graphe JSON selon les stratégies (`insert_pivot_object`, `insert_jsonb_object`, `insert_flatten`, `insert_array`…) |
| `sink.rs` | `trait RowSink { write_row() }` — abstraction sur l'écriture de lignes. `MemSink` (dans `db/copy_sink.rs`) en est l'implémentation concrète |
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
| `finalizer/` | `SchemaFinalizer` : transformations post-stream — construit les `TableSchema`, applique les stratégies, trie topologiquement — voir sous-section dédiée ci-dessous |
| `cascading/` | Détection et fusion des tables sœurs (BFS cascade) — voir sous-section dédiée ci-dessous |
| `grouping/` | Découpage thématique des tables larges (#78) : propose des groupes de colonnes qui deviendront des tables 1:1 — voir sous-section dédiée ci-dessous |
| `wide_strategies.rs` | Fonctions d'application des stratégies wide : `apply_wide_strategy_columns`, `apply_flatten`, `apply_jsonb_flatten`, `apply_normalize_dynamic_keys`, `build_union_columns`, `classify_key_shape` |
| `strategies.rs` | `StrategyName` (enum des stratégies optionnelles désactivables : `Sibling`, `Pivot`, `StructuredPivot`), `StrategyError`, `parse_disabled_strategies()`. Utilisé par `Pass1Config` et `SchemaRegistry` pour gater les stratégies via `--disable-strategy`. |
| `inspector.rs` | Statistiques d'inspection : `collect_stats()` construit les `ColumnStats` (histogramme de types) pour le mode `--inspect` |
| `registry.rs` | `SchemaRegistry` : façade publique (~100 lignes prod). Délègue à `observer`, `finalizer` et `inspector` |
| `naming.rs` | Sanitisation des identifiants PG, déduplication, troncature à 63 bytes avec hash, détection de collisions de noms de colonnes |
| `suffix_detector.rs` | Détecte les patterns `{base}_{suffixe}` dans les tables larges pour la stratégie `StructuredPivot` |
| `config.rs` | Parse et applique le fichier TOML de surcharges manuelles (`--schema-config`) |
| `column_groups.rs` | Applique la clé `column_groups` d'une table de la config (#78) : chaque groupe de colonnes devient une table fille 1:1 (`{table}_{groupe}`, 53 octets au plus, hash au-delà) qui reçoit les colonnes et leurs `flatten_sources`, la table garde les autres colonnes et la liste de ses tables de groupe (`column_groups`). Groupes appliqués après les types, stratégies et `flatten`, dans l'ordre alphabétique de leurs noms. Un groupe invalide (colonne inconnue, générée ou déjà prise, moins de 3 colonnes, nom de table pris) est ignoré avec un avertissement ; une table qui n'est ni `Columns` ni `AutoSplit`, ou dont toutes les colonnes partiraient, n'est pas découpée. Réappliquer la config ne change rien ; une table de groupe existante avec d'autres colonnes est une erreur. `dissolve_column_groups()` remet les colonnes dans la table (avant un `Pivot`/`Jsonb`), `prune_missing_groups()` retire les noms de tables disparues, `reindex_stats()` rattache les statistiques d'une colonne déplacée à sa table de groupe |
| `stats.rs` | Struct `ColumnStats` + génère le rapport texte de statistiques de colonnes (types inférés, taux de nullité, colonnes MIXED) |
| `persistence.rs` | Sérialise/désérialise un résultat Pass 1 en JSON (`SchemaSnapshot`) — utilisé par l'IHM pour séparer Pass 1 et Pass 2. Version 2, ou 3 quand une table est découpée en groupes de colonnes (`format_version()`) : un programme plus ancien refuse alors le fichier au lieu de lire une table sans ses colonnes déplacées |
| `mod.rs` | Déclare le module, exporte `PATH_SEP` |

### `src/schema/finalizer/`

Transforme les observations brutes (`TableEntry`) en `TableSchema` finalisés : construction des colonnes, sélection de la stratégie wide-table, détection et collapse des siblings, tri topologique.

| Fichier | Rôle |
|---|---|
| `orchestrator.rs` | `SchemaFinalizer` (struct + `impl`, dont `run()`) — orchestre les 4 phases (base → cascade → wide strategies → guard) |
| `base.rs` | Phase 1 — construction des schémas de base, par table (parallèle via rayon) |
| `wide_strategy.rs` | Phase 3 — décision de la stratégie wide-table (Columns/`StructuredPivot`/Pivot/Jsonb/`AutoSplit`), par table |
| `guard.rs` | Phase 4 — garde-fou 1600 colonnes (`apply_column_limit_guard`), exclusion des enfants absorbés (`exclude_absorbed_children`) |
| `mod.rs` | Déclare les sous-modules, ré-exporte `SchemaFinalizer`, `OverflowWarning`, `exclude_absorbed_children` |

### `src/schema/cascading/`

BFS cascade — détection des groupes de tables sœurs (*siblings*) et fusion (*collapse*) en tables canoniques.

| Fichier | Rôle |
|---|---|
| `detection/` | Détection BFS par vagues, découpée par phase : `mod.rs` (types partagés, orchestration), `wave0.rs` (groupes de siblings), `apply.rs` (application des collapses), `cascade.rs` (co-siblings, waves 1+), `keyed_pivot.rs` (post-pass keyed-pivot) |
| `scoring.rs` | Jaccard (`pairwise_jaccard_min`), clustering glouton — fonctions pures sur `&[TableSchema]` et indices |
| `merge.rs` | `build_sibling_collapse_from_siblings()` — API publique pour la fusion manuelle depuis l'IHM |
| `mod.rs` | Déclare les sous-modules, ré-exporte `finalize_cascading` |

### `src/schema/grouping/`

Découpage thématique des tables larges (#78) : propose des groupes de colonnes qui deviendront des tables 1:1. Appelé par `--suggest-groups` (`pipeline::suggest_groups`), et par la stratégie `--group-columns` (`auto.rs`, #91) qui applique directement les groupes confirmés par les noms. Le TOML `column_groups` qu'il produit s'applique avec `--schema-config` (`schema/column_groups.rs`) et Pass 2 écrit les tables de groupe (`pass2/insert.rs`).

| Fichier | Rôle |
|---|---|
| `names.rs` | Normalisation des `original_name` d'une table en radicaux : coupe `snake_case` / `camelCase` / chiffres, puis retire les jetons de format (`tags`, `lc`…), la clé des familles (`NAME_FR`) et les mots de bruit (> 40 % des colonnes). `normalize()` renvoie un `NameStems` ; `numbered_families()` repère les familles numérotées (`strIngredient1..20`, au moins 8 colonnes) |
| `lexical.rs` | Regroupement lexical des radicaux : vecteurs de trigrammes TF-IDF, liaison moyenne coupée à 0,20, groupes d'au moins 4 colonnes, puis les colonnes restées seules entre elles (minimum 3). `lexical_groups()` renvoie des listes d'indices ; refuse plus de 2 000 colonnes (matrice n×n). `group_cohesion()` : similarité moyenne des paires d'un groupe, pour le rapport |
| `presence.rs` | Co-présence des colonnes dans un échantillon de lignes (« taux de correspondance ») : bitsets de présence, cinq strates de richesse de fiche, φ de présence ajusté par strate et signé, colonnes informatives (présentes dans 2 % à 98 % des lignes). `lift()` : présences communes sur présences attendues strate par strate, qui ne s'écrase pas pour une colonne rare comme φ (#91). `Presence`, `PhiMatrix`, `PresenceStrata` |
| `packed.rs` | Forme compacte des tables de nombres d'un échantillon dans le snapshot : une chaîne hexadécimale par table (bits de présence, empreintes, richesse, rangs, NaN compris), au lieu d'un nombre par ligne de JSON indenté (#91) |
| `sample.rs` | `StoredSample` (l'échantillon d'une table pris au Pass 1, sérialisé dans le snapshot) et `observe_row()` (une ligne ajoutée à l'échantillon d'une table). Échantillon borné relu dans le fichier d'entrée après finalisation, pour `--suggest-groups` : une ligne racine sur `ceil(lignes racine / budget)` (20 000 par défaut), `--limit` respecté, une table enfant atteinte par un chemin d'objets (un tableau sur le chemin est refusé). `sample_tables()` lit plusieurs tables (`SampleTarget` : chemin vers l'objet et chemins des colonnes) dans les mêmes lignes en un seul passage et renvoie, par table, la présence et les valeurs |
| `values.rs` | Valeurs échantillonnées des colonnes, remplies ligne à ligne pendant le même passage que la présence : vocabulaire des colonnes texte (empreintes 64 bits des chaînes de 128 octets au plus, borné à 20 000 valeurs distinctes), étendue et entiers des colonnes numériques, sous-échantillon d'environ 2 000 lignes pour les rangs. `ValueProfile`, `Nature` |
| `shared.rs` | Valeurs partagées entre colonnes texte : contenance pondérée par la rareté (`ln(colonnes / colonnes qui contiennent la valeur)`), au moins 5 valeurs distinctes et 3 en commun, colonnes texte seulement. `shared_value_pairs()` |
| `numeric.rs` | Signaux numériques : corrélation de rang de Spearman entre colonnes numériques (au moins 30 lignes communes, `|ρ|` ≥ 0,9) et formes précises (année, epoch en secondes, degrés). Une forme ou une corrélation n'est jamais décisive seule. `rank_correlations()`, `shape()` |
| `embedding.rs` | Embeddings statiques Model2Vec (optionnels) : lecteur maison, sans dépendance, de `model.safetensors` (un seul tenseur `embeddings` en `F32`) et de `tokenizer.json` de type WordPiece. Un texte est mis en minuscules, sans accents (table Latin-1 et Latin étendu A, marques U+0300..U+036F), coupé aux espaces et à la ponctuation, découpé en sous-mots (plus long préfixe d'abord, `##`, mot de plus de 100 caractères ignoré), les lignes de la table sont additionnées puis normalisées. Autre type de tokenizer, autre tenseur ou autre type de données : refusé avec un message qui nomme le fichier. `StaticEmbedder` |
| `placement.rs` | Pour chaque colonne laissée dans une table, les deux groupes confirmés dont le centre (somme normalisée des vecteurs des radicaux de ses membres) est le plus proche de son radical, avec le cosinus ; seuls les cosinus positifs comptent. Une indication, jamais une décision. `placements()` |
| `proposal.rs` | Assemblage d'une proposition pour une table : règle des deux signaux. Un groupe lexical d'une seule famille de préfixe est confirmé par les noms ; une fusion de plusieurs familles l'est seulement si un signal de données soutient les liens entre familles (au moins 3 paires et 20 % des paires où le signal est défini ; la présence doit aussi dépasser deux fois le bruit de fond de la table), sinon elle est « à confirmer » et remplace les familles d'au moins 3 colonnes, qui restent des groupes actifs. Les familles à clé ou numérotées sont signalées, jamais transformées. Un groupe dont la table existerait déjà prend le suffixe `_cols`, puis `_cols_2`… (`avoid_taken_names()`). Avec des signaux de données, `attachments` liste les colonnes laissées dans la table que le lift rapproche d'un seul groupe confirmé (lift moyen ≥ 2, 30 % d'avance, ≥ 1 % des lignes) : candidats affichés, jamais appliqués. `propose()`, `DataSignals`, `TableProposal` |
| `render.rs` | Sorties d'une proposition : TOML `[table.column_groups]` en noms PG (groupes confirmés actifs, fusions à confirmer en commentaire, une ligne de preuves par groupe) et rapport Markdown. `to_toml()`, `to_markdown()` |
| `suggest.rs` | L'exécution de `--suggest-groups` sur le schéma finalisé : toutes les tables `Columns` / `AutoSplit` d'au moins 20 colonnes de données, en un seul échantillon relu comme Pass 2 lit ses colonnes (`flatten_sources` puis `original_name`). Les signaux de données d'une table sont éteints avec leur raison (nombre de lignes racine inconnu, `--limit 0`, tableau sur le chemin, aucune ligne qui contienne l'objet, erreur de lecture). L'échantillon et la proposition prennent les colonnes d'une table triées par nom (`data_columns_by_name()`) : le résultat ne dépend pas de l'ordre que la Pass 1 leur donne (#90) et les signaux restent sur leurs colonnes. `suggest()` |
| `auto.rs` | La stratégie `--group-columns` et la case de l'IHM (#91). Chaque table prend les signaux de son échantillon du Pass 1 s'il correspond à ses colonnes, ses noms seuls sinon : `name_proposals()` propose par les noms seuls les tables éligibles qui ne sont ni déjà découpées ni des tables de groupe, dans l'ordre de leurs noms ; `apply_name_groups()` passe leurs groupes confirmés (jamais une fusion à confirmer) à `column_groups::apply_column_groups()` par une config construite en mémoire, avec ses validations, ses noms et son idempotence |
| `mod.rs` | Déclare les sous-modules |

---

## `src/db/`

Couche d'accès PostgreSQL.

| Fichier | Rôle |
|---|---|
| `connection.rs` | Établit une connexion `tokio_postgres` (sans TLS) |
| `ddl.rs` | Génère et exécute les `CREATE TABLE` en ordre topologique. Gère `DROP ... CASCADE` et les contraintes FK |
| `copy_text.rs` | Newtype `CopyEscaped` : garantit au niveau du type que les valeurs sont COPY-safe (échappement `\t`, `\n`, `\r`, `\\`, nul) |
| `copy_sink.rs` | `RowBuilder` (ligne COPY texte), `MemSink` (buffer mémoire par table) et `flush_mem_sink_to_pg` (`COPY FROM STDIN` par chunks de 4 MiB) — aucun fichier temporaire |
| `error.rs` | Helpers d'erreur partagés de la couche DB |
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
| `state/` | `mod.rs` : `AppState`, état racine et traitement des `ProgressEvent` ; `selection.rs` : logique de sélection UI |
| `config.rs` | Persistance de la config projet (`~/.config/json2sql/last_project.toml`, sans password) |
| `worker_client.rs` | Client du worker subprocess : spawn, connexion socket, lecture des événements, détection de résumption |
| `theme.rs` | Charge le CSS embarqué (`assets/styles.css`) |

### `json2sql-ui/src/screens/`

| Fichier | Rôle |
|---|---|
| `mod.rs` | Réexporte les utilitaires de `common/` (voir ci-dessous) pour que `crate::screens::X` reste résoluble |
| `setup.rs` | Écran 1 — Setup (stepper 4 accordéons) |
| `analysis.rs` | Écran 2 — Analysis (progression Pass 1) |
| `strategy.rs` | Écran 3 — Strategy Editor |
| `preview.rs` | Écran 4 — Aperçu SQL (DDL + coloration syntaxique) |
| `import.rs` | Écran 5 — Import (Pass 2, lance/pilote le worker) |
| `resume.rs` | Écran affiché au démarrage si un worker actif est détecté |
| `table_list.rs` | Composant partagé : liste de tables avec badges (purement présentationnel) |

#### `json2sql-ui/src/screens/common/`

Utilitaires transverses consommés par plusieurs écrans (extraits de `screens/mod.rs` en #38, regroupés en #43).

| Fichier | Rôle |
|---|---|
| `mod.rs` | Câblage : `mod`/`pub use` des 6 fichiers ci-dessous |
| `timer.rs` | Hook `use_elapsed_timer` |
| `file_picker.rs` | Sélecteurs de fichiers/dossiers natifs (`pick_file`, `pick_folder`, `pick_save_file`) |
| `strategy_badges.rs` | Labels et badges de stratégie (`strategy_label`, `strategy_badge`, `user_override_badge`) |
| `progress.rs` | Composant `ProgressBar` et calcul `progress_pct` |
| `overrides.rs` | Application des overrides utilisateur et des cases « Column groups » (`build_effective_schemas`), tables de l'écran Strategy (`display_schemas`), tables découpables (`splittable_tables`) et aperçu des groupes d'une table (`group_preview`) |
| `table_rows.rs` | View-model de la liste de tables (`TableRowViewModel`, `TableRowsCtx`, `build_table_rows`) |

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
| `Cargo.toml` | Workspace (`json2sql`, `json2sql-ui`, `json2sql-worker`) et dépendances |
| `Makefile` | Raccourcis build / test / lint |
| `LICENSE` | Licence MIT |
