---
type: carte
sujet: Pass 1, lecture du fichier, observation des types et échantillon
verified_at: d84fee2
files:
  - src/pass1/
  - src/io/reader.rs
  - src/io/mem.rs
  - src/schema/registry.rs
  - src/schema/observer.rs
  - src/schema/type_tracker.rs
  - src/schema/stats.rs
  - src/schema/inspector.rs
  - src/pipeline.rs
  - json2sql-ui/src/screens/analysis.rs
  - docs/technical/architecture.md
  - docs/technical/schema-inference.md
  - tests/integration_schema.rs
symbols:
  - run_pipeline
  - load_or_infer_schema
  - restore_from_snapshot
  - run_pass1_workers
  - save_schema_snapshot
  - AnalysisScreen
  - run
  - run_parallel
  - run_inspect
  - effective_workers
  - scan_json_rows
  - report_progress
  - read_and_dispatch
  - spawn_worker_threads
  - join_and_merge_workers
  - build_pass1_result
  - build_inspect_registry
  - samples_of
  - samples_of_rows
  - Pass1Config
  - Pass1Result
  - RowSampler
  - SampledRow
  - StoredSample
  - JsonFormat
  - JsonReader
  - detect_format
  - RegistryConfig
  - RegistryConfig::default
  - TableSchema
  - SchemaRegistry
  - SchemaRegistry::merge
  - SchemaRegistry::finalize_with_pg_guard
  - SchemaRegistry::collect_stats
  - SchemaFinalizer
  - SchemaObserver
  - SchemaObserver::observe_root
  - TableEntry
  - TableEntry::merge
  - TypeTracker
  - TypeTracker::observe
  - TypeTracker::merge
  - TypeTracker::to_pg_type
  - TypeTracker::has_anomalies
  - InferredType
  - infer_string_type
  - infer_number_type
  - widen_pg_types
  - PgType
  - PgType::as_sql
  - ColumnStats
  - make_stat
  - collect_stats
  - write_text_report
  - coerce
  - coerce_pg_array_element
  - prime_tracker_from_pg_type
  - parse_pg_type
  - NamingRegistry
  - ProgressEvent
---

# Pass 1 : lecture et observation

Carte de la Pass 1, pour savoir où intervenir et ce qui casse sans erreur de compilation. Les règles d'inférence
(élargissement des types, tables enfants, tables larges) sont décrites dans
[schema-inference.md](../technical/schema-inference.md) ; les modules dans
[architecture.md § src/pass1/](../technical/architecture.md#srcpass1) et § `src/schema/`.

## Rôle

La Pass 1 lit le fichier une fois, en entier, et **observe** chaque objet racine : pour chaque champ de chaque table,
un `TypeTracker` compte les types vus. Elle garde aussi un échantillon borné de lignes (`RowSampler`, pour les groupes
de colonnes, #91). Elle ne décide rien elle-même : à la fin, `SchemaRegistry::finalize_with_pg_guard` passe la main à
la finalisation (`SchemaFinalizer` : noms, stratégies, tables sœurs), qui produit les `TableSchema` de la Pass 2.

`registry.rs` est la **frontière** : `SchemaRegistry` réunit l'observation (`SchemaObserver`) et la finalisation. Cette
fiche le décrit comme point d'entrée ; ce qui se passe après `finalize_with_pg_guard` relève de la finalisation.

## 1. Où intervenir

| Changement | Où | Le compilateur aide ? |
|---|---|---|
| Nouveau format de fichier | `JsonFormat`, `detect_format`, `JsonReader` (`reader.rs`) | oui pour les `match` sur `JsonFormat`, non pour la détection |
| Nouvelle statistique par colonne | `TypeTracker` : `TypeTracker::observe`, `TypeTracker::merge`, puis `ColumnStats` (§ 4) | **non** pour la fusion |
| Nouvelle donnée par table | `TableEntry`, `SchemaObserver::observe_root`, `TableEntry::merge` | **non** pour la fusion |
| Nouveau type inféré | `InferredType`, `infer_string_type` / `infer_number_type`, `TypeTracker::to_pg_type` (§ 4) | **non** |
| Nouveau seuil ou réglage | `RegistryConfig` ; rempli par la CLI (`run_pass1_workers`) et l'IHM (`AnalysisScreen`) | oui : construit en littéral partout |
| Progression | `report_progress` (séquentiel) et `read_and_dispatch` (parallèle), événements `ProgressEvent` | non : deux copies |
| Échantillon de lignes | `RowSampler`, `samples_of` | non |

Les types inférés, et le type PostgreSQL (`PgType`) qu'ils donnent quand ils sont seuls dans une colonne :

| `InferredType` | `PgType` | Produit par |
|---|---|---|
| `Null` | — | un `null` ; compté à part (`null_count`), jamais dans l'histogramme ; doit rester l'indice 0 |
| `Boolean` | `Boolean` | un booléen |
| `Integer` | `Integer` | un entier qui tient sur 32 bits (`infer_number_type`) |
| `BigInt` | `BigInt` | un entier plus grand |
| `Float` | `DoublePrecision` | un nombre à virgule, ou un entier au-delà de 64 bits |
| `Uuid` | `Uuid` | une chaîne de 36 octets au format UUID (`infer_string_type`) |
| `Date` | `Date` | une chaîne de 10 octets `AAAA-MM-JJ` |
| `Timestamp` | `Timestamp` | une chaîne d'au moins 16 octets qui commence par une date et une heure |
| `Varchar` | `VarChar(n)` ou `Text` | toute autre chaîne ; `Text` au-delà de `text_threshold` |
| `Text` | `Text` | **jamais produit** : le choix `VarChar` / `Text` se fait sur la longueur, dans `TypeTracker::to_pg_type` |
| `Object` | — | un objet : devient une table enfant, jamais une colonne |
| `Array` | — | un tableau : table enfant (tableau d'objets) ou colonne tableau (tableau de scalaires) |

## 2. Flux

```text
CLI : run_pipeline → load_or_infer_schema ─┬─ --schema-input : restore_from_snapshot (pas de Pass 1)
                                            └─ run_pass1_workers ─┐
IHM : AnalysisScreen ─────────────────────────────────────────────┴─► workers > 1 ? run_parallel : run
inspect (CLI) : run_inspect → build_inspect_registry (seuils désactivés), N premiers objets, sans stats

run (séquentiel)
  └─ JsonReader::open (detect_format) → scan_json_rows : pour chaque objet racine
       ├─ SchemaRegistry → SchemaObserver::observe_root (pile, pas de récursion)
       │    ├─ objet           → table enfant, empilé
       │    ├─ tableau d'objets → table enfant, chaque objet empilé
       │    ├─ tableau de scalaires → array_columns (un TypeTracker des éléments)
       │    └─ scalaire        → TypeTracker::observe
       ├─ RowSampler : garde la ligne si son offset passe le pas suivant
       └─ report_progress

run_parallel
  ├─ read_and_dispatch (thread appelant) : lit les octets bruts, échantillonne, envoie dans un canal
  ├─ spawn_worker_threads : N threads, un SchemaRegistry chacun, parse puis observe_root
  └─ join_and_merge_workers : SchemaRegistry::merge → TableEntry::merge → TypeTracker::merge

build_pass1_result (les deux) : finalize_with_pg_guard, puis samples_of, collect_stats, noms tronqués, collisions
  → Pass1Result
```

Le worker de l'IHM (`json2sql-worker`) ne fait jamais de Pass 1 : il reçoit les schémas déjà finalisés.

## 3. Invariants et pièges

- **La fusion parallèle recopie les champs à la main.** `TypeTracker::merge` et `TableEntry::merge` additionnent les
  compteurs un par un. Un champ ajouté à l'une de ces structures compile, mais il est perdu (ou gardé d'un seul thread)
  dès que `--workers` dépasse 1. Les tests séquentiels restent verts.
- **Un nouveau type inféré devient `TEXT` en silence.** `TypeTracker::to_pg_type` est une suite de `if` qui finit par
  `PgType::Text`, pas un `match` sans cas par défaut. `widen_pg_types` (élargissement de deux `PgType`) a un `_ =>`
  vers `Text`. L'assertion `const` de `type_tracker.rs` vérifie `ALL` contre `COUNT`, mais ne voit pas une variante
  ajoutée sans toucher `COUNT` : le compteur de types (`type_counts`, de taille `COUNT`) déborde alors à l'exécution.
- **Deux règles d'élargissement qui diffèrent.** Dans une colonne, `TypeTracker::to_pg_type` donne `Timestamp` pour
  dates + timestamps, `Integer` pour entiers + booléens (les booléens deviennent des anomalies). Entre deux `PgType`,
  `widen_pg_types` donne `Text` dans ces deux cas. Un nouveau type doit trouver sa place dans les deux.
- **L'ordre des colonnes est l'ordre de première rencontre.** `TableEntry` range ses colonnes dans un `IndexMap`, et
  la finalisation garde cet ordre pour le DDL. En parallèle, les threads prennent les lignes au fil de l'eau (pas en
  tourniquet) et la fusion ajoute à la fin les colonnes inconnues : l'ordre des colonnes peut changer d'un run à
  l'autre. De même, une table vue tantôt comme objet, tantôt comme tableau d'objets prend le genre de la première
  occurrence. Les tests de parité (`tests/integration_schema.rs`) cherchent les colonnes par nom et ne voient pas ces
  écarts.
- **L'échantillon n'est pas pris pareil dans les deux modes.** `scan_json_rows` garde la ligne réécrite en JSON
  compact, `read_and_dispatch` les octets bruts du fichier. Le budget en octets de `RowSampler` est donc atteint plus
  tôt en parallèle sur un fichier indenté, et les lignes gardées peuvent différer.
- **L'ordre des appels de `build_pass1_result` compte.** `SchemaRegistry::collect_stats` et les noms tronqués lisent le
  `NamingRegistry` que remplit la finalisation : ils doivent venir après `finalize_with_pg_guard`.
- **`ColumnStats` est écrit dans le snapshot sans `#[serde(default)]`.** Un champ ajouté sans ce défaut rend illisibles
  les snapshots déjà enregistrés. Les noms des variantes d'`InferredType` y sont aussi écrits (histogramme de
  `make_stat`, en `{:?}`) : renommer une variante change le contenu des snapshots.
- **`Pass1Result` est recopié champ par champ** : `save_schema_snapshot`, `restore_from_snapshot` et la reprise dans
  `AnalysisScreen` (IHM). Un champ ajouté compile et n'est ni sauvé, ni restauré, ni montré.
- **Les réglages par défaut diffèrent selon le point d'entrée.** CLI (`--wide-column-threshold` 15, sibling 2,
  jaccard 0,8), IHM (constantes `PASS1_*` : 10, 3, 0,5) et `RegistryConfig::default` (tables larges désactivées), que
  les tests utilisent. Le même fichier ne donne pas le même schéma en CLI et dans l'IHM, et les tests ne couvrent pas
  les défauts de la CLI.
- **`run_inspect` ignore la config reçue**, sauf `text_threshold` : `build_inspect_registry` fixe les autres seuils.
- **Une erreur de lecture arrête la Pass 1**, pas d'anomalie. Une racine qui n'est pas un objet aussi. En parallèle,
  le thread de lecture va jusqu'au bout du fichier avant de rendre l'erreur d'un worker. Exception : un tableau JSON
  coupé entre deux éléments est lu comme une fin normale (#37).
- **Ce que l'observateur ne voit pas** : un tableau vide (pareil qu'un champ absent) ; les scalaires d'un tableau qui
  contient aussi des objets (aucun type observé, pas de colonne : la Pass 2 en fait une anomalie, #86).
- **La Pass 1 ne s'annule pas.** Dans l'IHM, l'annulation fait `abort()` sur une tâche `spawn_blocking`, sans effet
  une fois lancée : la lecture va jusqu'au bout du fichier.

## 4. Points d'extension

Les endroits prévus sont listés au § 1. Deux changements courants demandent des étapes que le compilateur ne rappelle
pas ; ce sont les recettes de #96 « Ajouter un signal ou une statistique en Pass 1 » et « Ajouter un type inféré ».

**Nouvelle statistique par colonne.**

- [ ] le champ dans `TypeTracker`, initialisé dans son constructeur (le compilateur le signale) ;
- [ ] sa mise à jour dans `TypeTracker::observe` ; décider si les colonnes tableau (`array_columns`, un compte par
  élément et non par ligne) l'alimentent ;
- [ ] sa fusion dans `TypeTracker::merge`, dans un calcul qui ne dépend pas de l'ordre (somme, maximum) ;
- [ ] un test qui compare `run` et `run_parallel` sur cette statistique ;
- [ ] `observe` est appelé pour chaque champ de chaque ligne : pas d'allocation, et une limite à tout ensemble de
  valeurs distinctes ;
- [ ] pour l'exposer : un champ dans `ColumnStats` avec `#[serde(default)]`, rempli dans `make_stat`, affiché si
  besoin par `write_text_report`.

**Nouveau type inféré.**

- [ ] la variante dans `InferredType`, avec l'indice suivant ; augmenter `COUNT` et compléter `ALL` (rien ne force à
  toucher `COUNT`) ; une ligne dans le tableau du § 1 (le test `guides_consistency` le vérifie) ;
- [ ] sa détection dans `infer_string_type` ou `infer_number_type` (attention au tri par longueur de chaîne : 36, 10,
  16 octets et plus) ;
- [ ] sa place dans `TypeTracker::to_pg_type` et dans `widen_pg_types` ;
- [ ] s'il faut un nouveau `PgType` : le compilateur signale `PgType::as_sql`, `coerce` et
  `prime_tracker_from_pg_type`, mais pas `widen_pg_types`, `coerce_pg_array_element` ni `parse_pg_type` (types des
  overrides TOML) ;
- [ ] `TypeTracker::has_anomalies` compte toute colonne à deux types comme mixte : un type plus fin augmente ce compte.

## 5. Vérifier

```bash
cargo test --lib pass1
cargo test --lib io::reader
cargo test --lib schema::type_tracker
cargo test --lib schema::registry
cargo test --test integration_schema
cargo test --test guides_consistency
cargo test --lib --bins
```

Aucun de ces tests ne demande PostgreSQL. `observer.rs`, `stats.rs` et `inspector.rs` n'ont pas de tests propres :
ils passent par ceux de `registry.rs` et de `runner.rs`.

## 6. Odeurs constatées

Vues en écrivant la fiche, pas tranchées.

1. **Trois boucles de lecture.** `scan_json_rows`, `read_and_dispatch` et la boucle de `run_inspect` répètent la
   vérification de l'objet racine, l'échantillon et la progression (dont une constante de pas de progression définie
   deux fois). La progression parallèle compte les lignes envoyées, pas observées.
2. **Fusion sans garde.** `TypeTracker::merge` et `TableEntry::merge` pourraient commencer par déstructurer `other`
   sans `..` : un champ ajouté deviendrait une erreur de compilation.
3. **Réglages recopiés.** `RegistryConfig` est recopié dans les champs de `SchemaRegistry`, puis passé à
   `SchemaFinalizer` en sept arguments par position, dont trois `f64` voisins qu'on peut intervertir sans erreur.
   Les défauts existent en quatre endroits (§ 3).
4. **`registry.rs` est surtout des tests du finaliseur** : environ 160 lignes de code pour 1 400 de tests, qui visent
   la finalisation, les tables sœurs et les stratégies plutôt que la façade.
5. **`io/mem.rs` n'est appelé nulle part** : ses fonctions de mesure de la RAM n'ont pas d'appelant.
6. **Docs décalées.** architecture.md cite un `SchemaReporter` qui n'existe pas ; l'en-tête de `runner.rs` parle de
   tourniquet (le canal est partagé) ; le commentaire de l'assertion `const` de `type_tracker.rs` promet plus qu'elle
   ne vérifie ; `stats.rs` se dit réservé à `inspect`, alors que seule la Pass 1 complète calcule les stats.
