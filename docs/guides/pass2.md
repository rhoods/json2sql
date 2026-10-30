---
type: carte
sujet: Pass 2, parcours du JSON et insertion dans PostgreSQL
verified_at: d84fee2
files:
  - src/pass2/
  - src/db/copy_sink.rs
  - src/db/copy_text.rs
  - src/schema/table_schema.rs
  - src/anomaly/collector.rs
  - src/pipeline.rs
  - json2sql-worker/src/pipeline.rs
  - src/ipc.rs
  - docs/technical/architecture.md
  - tests/integration_strategies.rs
  - tests/integration_anomalies.rs
  - tests/integration_limit.rs
symbols:
  - run_interruptible
  - load_and_constrain
  - run_pass2
  - run_pipeline
  - dispatch_loop
  - run_worker_diskless
  - process_worker_item_diskless
  - run_flusher
  - next_flush_candidate
  - schema_parent_map
  - build_copy_sql_map
  - build_freeze_targets
  - freeze_load_allowed
  - copy_sql
  - MemSink
  - RowBuilder
  - RowSink
  - CaptureSink
  - InsertCtx
  - insert_object
  - write_row_of
  - write_root_jsonb
  - write_group_rows
  - write_autosplit_rows
  - recurse_children
  - insert_child_object
  - insert_array
  - insert_pivot_object
  - push_generated_col_insert
  - push_data_col
  - push_coerced
  - push_coerced_as
  - write_row
  - insert_jsonb_object
  - insert_routed_object
  - coerce
  - CoerceResult
  - AnomalyCollect
  - AnomalyProxy
  - AnomalyCollector
  - Pass2Config
  - WorkerConfig::into_pass2_config
  - validate_run_params
  - ColumnSchema::generated
  - ColumnSchema::parent_fk
  - TableSchema::effective_strategy
  - TableSchema::find_by_original
  - TableSchema
  - InferredStrategy
  - PgType
  - insert_sibling_collapse_object
  - insert_sibling_collapse_multi
  - insert_sibling_collapse_array_of_objects
  - insert_normalize_dynamic_keys
---

# Pass 2 : parcours et insertion

Carte de la zone `src/pass2/`, pour savoir où intervenir et ce qui casse sans erreur de compilation. Le fonctionnement
(pipeline *diskless*, flusher, `COPY FREEZE`, réglages PG, annulation) est décrit dans
[architecture.md § src/pass2/](../technical/architecture.md#srcpass2) ; ajouter une stratégie a sa propre fiche :
[ajouter-une-strategie.md](ajouter-une-strategie.md).

## Rôle

La Pass 2 relit le fichier source et écrit chaque objet JSON dans les tables que la Pass 1 a décidées. Elle ne décide
rien sur le schéma : elle suit `schemas` (une liste de `TableSchema` finalisés, overrides appliqués) et ne fait que
**router** les valeurs vers les tables et les **convertir** vers leur type PostgreSQL.

## 1. Où intervenir

| Changement | Où | Le compilateur aide ? |
|---|---|---|
| Nouvelle stratégie, ou routage d'une stratégie | `insert_child_object`, puis la fiche [ajouter-une-strategie.md](ajouter-une-strategie.md) | oui pour ce `match`, non pour le reste |
| Nouveau type PostgreSQL | `coerce` (un `match` sans cas par défaut sur `PgType`) | oui |
| Nouvelle colonne générée (`j2s_…`) | `push_generated_col_insert`, et chaque écriture de ligne codée en dur (§ 3) | **non** |
| Nouvelle forme de ligne (une table écrite autrement) | une fonction `insert_*` dans `traversal.rs`, appelée depuis le répartiteur | non |
| Nouveau paramètre du run | `Pass2Config`, `validate_run_params` si besoin ; il se remplit dans `run_pass2` (CLI) et `WorkerConfig::into_pass2_config` (IHM) | oui : `Pass2Config` est construit en littéral partout, tests compris |
| Ordonnancement des COPY, mémoire | `run_flusher`, `next_flush_candidate` ; lire d'abord architecture.md | non |
| Nouvelle anomalie de valeur | `CoerceResult` et son traitement dans `push_coerced_as` (le seul, appelé aussi via `push_coerced`) | en partie |

## 2. Flux

```text
run_pass2 (CLI)  ─┐
run_pipeline (worker, IHM) ─┴─► run_interruptible
   └─ load_and_constrain
      ├─ path_map : chemin JSON joint → TableSchema ; un MemSink par table et par worker
      ├─ dispatch_loop ──round-robin──► run_worker_diskless (× --parallel)
      │     └─ process_worker_item_diskless : parse, puis
      │        insert_object(racine, j2s_id = UUIDv7, parent = None)
      │          ├─ write_row_of : une ligne, dans l'ordre de schema.columns
      │          ├─ write_group_rows, write_autosplit_rows
      │          └─ recurse_children : pour chaque champ objet / tableau
      │               ├─ objet  → insert_child_object (répartit selon effective_strategy)
      │               └─ tableau → insert_array (un insert_object par élément objet, scalaires → anomalie)
      ├─ run_flusher : COPY FROM STDIN par lots, parents avant enfants au drain final
      └─ COMMIT, VACUUM (ANALYZE), contraintes (PK puis FK)
```

Les identifiants sont des UUIDv7 tirés dans le worker. Aucune ligne ne dépend d'une autre : les workers n'ont rien à
se partager, et les PK/FK ne sont posées qu'à la fin.

## 3. Invariants et pièges

- **Une ligne écrite = les colonnes de `schema.columns`, dans cet ordre.** `copy_sql` liste ces colonnes pour le
  `COPY`, et `write_row_of` les parcourt. Mais plusieurs écritures de `traversal.rs` posent leurs valeurs en dur :
  `insert_pivot_object` écrit `j2s_id`, le parent, la clé puis la valeur, sans lire `schema.columns`. Si la Pass 1
  produit ces colonnes dans un autre ordre, ou en ajoute une, le `COPY` échoue sur un type ; pire, deux colonnes de
  même type (deux UUID, deux textes) sont échangées **sans erreur**.
- **Une table sans schéma est sautée en silence.** `recurse_children` cherche le chemin du champ dans `path_map` ;
  absent, l'objet ou le tableau est ignoré. C'est voulu (table absorbée, `Skip`), mais c'est aussi ce que donne un
  chemin mal construit : des données perdues, sans anomalie.
- **Un sink absent est sauté en silence.** Chaque écriture passe par `write_row`, qui compte la ligne puis fait
  `if let Some(sink) = sinks.get_mut(…)`. Les sinks et
  `path_map` viennent de la même liste `schemas` : une table ajoutée ailleurs que dans cette liste n'est jamais écrite.
- **Une colonne générée inconnue vaut `NULL`.** `push_generated_col_insert` reconnaît `j2s_id`, `j2s_order` et la
  clé parente (`is_parent_fk`) ; tout autre nom tombe dans `_ => push_null()`. Une colonne `NOT NULL` le signale au
  `COPY` ; sinon, rien. Les colonnes générées se construisent avec `ColumnSchema::generated` et
  `ColumnSchema::parent_fk`.
- **Un tableau d'objets n'a pas de colonne pour ses scalaires.** `insert_array` (et
  `insert_sibling_collapse_array_of_objects`) écrit une ligne par élément objet. Les éléments scalaires donnent **une
  anomalie par tableau** (#86), avec le premier comme valeur, sauf en stratégie `Jsonb`, où chacun devient un blob.
  Exception : si le parent a une colonne tableau pour ce champ (le champ vaut ailleurs un tableau de scalaires),
  c'est elle qui garde les scalaires, et il n'y a pas d'anomalie. Un tableau de scalaires seul n'y passe pas : c'est
  une colonne tableau, convertie par `coerce`.
- **Une valeur qui ne se convertit pas devient `NULL` et une anomalie** (`CoerceResult`), jamais une erreur : le
  `COPY` ne doit pas échouer sur une donnée. Une erreur de `COPY` est donc un bug de schéma ou d'écriture, pas de
  donnée. Chaque ligne compte aussi dans le total de sa table (`inc_total`), dénominateur du taux d'anomalies.
- **La stratégie lue est `TableSchema::effective_strategy`**, pas `inferred_strategy` (voir la fiche des stratégies).
- **Deux points d'entrée.** La CLI (`run_pass2`) et le worker de l'IHM (`run_pipeline` de `json2sql-worker`) appellent
  tous deux `run_interruptible`, chacun après son propre DDL. Un changement d'appel se fait dans les deux.
- **Les tests PostgreSQL passent au vert sans base.** Sans `TEST_DATABASE_URL`, ils affichent `[SKIP]` : un run vert
  n'a alors pas testé le `COPY`, le flusher ni les contraintes.

## 4. Points d'extension

Les endroits prévus pour étendre la zone sont listés au § 1. Deux d'entre eux demandent des étapes que le
compilateur ne rappelle pas.

Les écritures de ligne se testent sans base : `CaptureSink` (dans `test_utils.rs`) implémente `RowSink` et garde les
lignes en mémoire. C'est le premier test à écrire pour une nouvelle forme de ligne, en comparant la ligne à
`schema.columns`.

**Nouvelle colonne générée** (une colonne `j2s_…` que json2sql remplit lui-même). `push_generated_col_insert` choisit
la valeur à écrire d'après le **nom** de la colonne, avec un cas par défaut qui écrit `NULL`. Une nouvelle colonne
compile donc sans erreur et reste vide en base. À faire à la main :

- [ ] la créer côté schéma (`ColumnSchema::generated`) ;
- [ ] ajouter son nom au `match` de `push_generated_col_insert`, avec la valeur à écrire ;
- [ ] mettre à jour chaque écriture de `traversal.rs` qui pose ses valeurs dans un ordre fixe au lieu de suivre
  `schema.columns` (`insert_pivot_object` et les autres `insert_*` qui remplissent un `RowBuilder` elles-mêmes) ;
- [ ] un test `CaptureSink` par forme de ligne touchée, puis un test PostgreSQL.

**Nouveau paramètre du run.** `Pass2Config` force tous les sites de construction, mais chacun y met sa valeur à la main.
Vérifier que la CLI (`run_pass2`) et l'IHM (`WorkerConfig::into_pass2_config`) passent bien la valeur voulue, pas
une valeur par défaut recopiée.

## 5. Vérifier

```bash
cargo test --lib pass2
cargo test --lib --bins
TEST_DATABASE_URL=postgres://user:pass@localhost/testdb cargo test --test integration_strategies --test integration_anomalies --test integration_limit
cargo test --features integration-tests --test tc_nested_fk --test tc_sibling_collapse --test tc_wide_table
```

Les tests `tc_*` démarrent un PostgreSQL dans Docker (testcontainers) : il faut Docker, pas `TEST_DATABASE_URL`.

## 6. Odeurs constatées

Vues en écrivant la fiche, pas tranchées.

1. **Les écritures en dur de `traversal.rs`.** L'ordre des colonnes est défini deux fois : par la Pass 1 (le schéma)
   et par chaque `insert_*` (les valeurs). Un `RowBuilder` qui remplirait les valeurs par nom de colonne supprimerait
   le piège de l'ordre.
2. **Deux styles de contexte.** `insert.rs` passe un `InsertCtx`, `traversal.rs` passe `sinks` et `anomalies`
   séparément. `insert_array`, `insert_jsonb_object` et `insert_routed_object` reçoivent les deux morceaux et
   reconstruisent un `InsertCtx` pour appeler `insert_object` ou `recurse_children`.
3. **Les pertes silencieuses** (chemin absent de `path_map`, sink absent) ne laissent aucune trace. Un compteur, ou une anomalie, distinguerait « absorbé exprès » de « perdu ».
4. **architecture.md décalé sur `traversal.rs`** : il cite `insert_keyed_pivot_object`, `insert_multi_keyed_pivot`
   et un enum `WideStrategy`, qui n'existent plus (aujourd'hui `insert_sibling_collapse_object`,
   `insert_sibling_collapse_multi` et `InferredStrategy`).
