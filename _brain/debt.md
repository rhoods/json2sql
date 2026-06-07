# Dette Technique — json2sql

## À adresser avant release
<!-- Ex: "Authentification non implémentée — accès libre aux routes" -->

## Dette Complexité — fonctions trop longues (T5, 2026-06-02)

Fonctions annotées `#[allow(clippy::too_many_lines/cognitive_complexity)]` — à refactoriser par priorité :

| Priorité | Fonction | Lignes | Complexité | Refactoring suggéré |
|---|---|---|---|---|
| ~~🔴 1~~ ✅ | ~~`cascading.rs::run_sibling_wave`~~ | ~~528~~ → ~97 | ~~57~~ → 0 | Livré 2026-06-04 : 7 fonctions extraites + SiblingDetectCtx, `#[allow]` retiré |
| ~~🔴 2~~ ✅ | ~~`pass2/runner.rs::run`~~ | ~~341~~ → ~90 | ~~42~~ → 0 | Livré 2026-06-04 : 11 fonctions extraites, `#[allow]` retiré |
| ~~🔴 3~~ ✅ | ~~`pass2/insert.rs::insert_object`~~ | ~~235~~ → ~110 | ~~41~~ → 0 | Livré 2026-06-04 : InsertCtx<S,A> struct + 4 fonctions extraites, `#[allow]` retiré |
| ~~🟡 4~~ ✅ | ~~`schema/finalizer.rs::build_entry_schema`~~ | ~~193~~ | — | Livré 2026-06-04 : FinalizerConfig + build_data_columns + apply_wide_strategy + apply_autosplit_strategy |
| ~~🟡 5~~ ✅ | ~~`schema/config.rs::apply_overrides`~~ | ~~145~~ | — | Livré 2026-06-04 : apply_strategy_override + apply_suffix_columns_override + apply_column_type_overrides |
| ~~🟡 6~~ ✅ | ~~`cascading.rs::run_keyed_pivot_children_wave`~~ | ~~134~~ | — | Livré 2026-06-04 : collect_keyed_pivot_work_items + build_sub_pivot_columns + reparent_and_update_routes |
| ~~🟢 7~~ ✅ | ~~`cascading.rs::process_co_sibling_group`~~ | ~~107~~ | — | Livré 2026-06-04 : handle_single_co_sibling + merge_co_sibling_group + reparent_siblings_individually |
| ~~🟢 8~~ ✅ | ~~`pass1/runner.rs::run_parallel`~~ | ~~123~~ | — | Livré 2026-06-04 : spawn_worker_threads + read_and_dispatch + join_and_merge_workers |

Gate CI : `cargo clippy -- -D warnings -D clippy::too_many_lines -D clippy::cognitive_complexity`
Seuil actuel : **30 lignes** (abaissé depuis 60, défini dans `clippy.toml`).
Prochain palier : à définir.

### Fonctions extraites de `cascading.rs::run_sibling_wave` (2026-06-04)
`SiblingDetectCtx` struct · `build_work_items` · `make_subgroup` · `try_unified_fallback` · `try_cluster_fallback` · `detect_mixed_collapse` · `detect_homogeneous_collapse` · `apply_single_collapse` · `apply_multi_collapse` · `apply_collapses`
10 nouveaux tests unitaires ajoutés (build_work_items×3, make_subgroup×3, detect_*×4)

### Fonctions extraites de `pass2/insert.rs::insert_object` (2026-06-04)
`InsertCtx<'a, S, A>` struct · `write_root_jsonb` · `dispatch_child_object` (élimine duplication) · `write_autosplit_rows` · `recurse_children`
1 nouveau test d'intégration ajouté : `test_jsonb_strategy_root_table` (chemin root-Jsonb non couvert précédemment)

### Fonctions extraites de `pass2/runner.rs::run` (2026-06-04)
`WorkerConfig` struct · `InterimCopyHandle`/`WorkerHandle` type aliases · `trigger_budget_flush` · `run_worker` · `phase_copy` · `spawn_anomaly_writer` · `preflight_warn_nonempty` · `dispatch_loop` · `join_phase_a` · `finalize_dispatch` · `emit_completion_events` · `log_constraint_warnings`

## Dette Complexité — fonctions restantes (inventaire 2026-06-05)

Résultat de l'audit clippy `too_many_lines` — classées en trois catégories.

### ~~Catégorie B — Extractibles (phases séquentielles nommées)~~ ✅ SOLDÉE (vérifié 2026-06-06)

Toutes les fonctions sont ≤ 30L — clippy core tourne à zéro violation `too_many_lines`.

| Fonction | Taille actuelle | État |
|---|---|---|
| `run_inspect` | 14L | ✅ |
| `detect_suffix_schema` | 25L | ✅ |
| `detect_mixed_collapse` | 27L | ✅ |
| `detect_homogeneous_collapse` | 25L | ✅ |
| `process_keyed_pivot_work_item` | 27L | ✅ |
| `run_worker` | 25L | ✅ |
| `dispatch_loop` | 31L | ✅ clippy silencieux |
| `apply_normalize_dynamic_keys` | 26L | ✅ |

### Catégorie A — Dispatch exhaustif sur enum — légitimes, ne pas refactoriser

Ces fonctions sont longues parce qu'elles ont de nombreux variants — pas parce qu'elles font plusieurs choses. Extraire les bras en fonctions n'améliorerait pas la lisibilité.

| Fichier | Fonction | Raison |
|---|---|---|
| `src/pass2/coercer.rs:26,131,167,201` | 4 fonctions coerce_* | dispatch exhaustif sur `PgType` variants |
| `src/pass2/insert.rs:195` | dispatch WideStrategy | dispatch exhaustif sur `WideStrategy` variants |
| `src/pass2/traversal.rs:307` | dispatch colonnes | boucle per-colonne avec branches distinctes |
| `src/schema/config.rs:104` | `apply_strategy_override` | dispatch exhaustif sur strategy override variants |

### Catégorie C — Algo compact ou couplage fort — ne pas toucher

| Fichier | Fonction | Raison |
|---|---|---|
| `src/schema/cascading/scoring.rs:60` | algorithme glouton | O(n²) auto-contenu, découper = perte de lisibilité algo |
| `src/io/reader.rs:200` | state machine inline | state machine lexicale, découper = régression de performance |
| `src/schema/cascading/merge.rs:96` | construction symétrique | 2 groupes miroirs, pas de phases |
| `src/pass2/runner.rs:444` | `join_phase_a` | accumulation d'erreurs non-factorisable |
| `src/db/ddl.rs:233` | pipeline async DDL | pipeline séquentiel déjà optimal |
| `src/pass2/runner.rs:130` | `trigger_budget_flush` | décision + spawn tightly coupled autour de `sink_arc` |

## Dette Complexité — fonctions trop longues json2sql-ui (inventaire 2026-06-06)

Résultat de l'audit clippy `too_many_lines` sur le crate UI — classées par catégorie.

### Catégorie B — Extractibles — à traiter

| Priorité | Fichier | Fonction | Taille | Phases identifiées |
|---|---|---|---|---|
| ~~🔴 1~~ ✅ | ~~`json2sql-ui/src/screens/mod.rs:80`~~ | ~~`build_table_rows`~~ | ~~88L~~ → 10L | Livré 2026-06-06 : `TableRowsCtx<'a>` + `RowFlags::compute` + `build_row`. `#[allow]` retiré, clippy UI zéro violation. |
| ~~🟡 2~~ ✅ | ~~`json2sql-ui/src/screens/mod.rs:371`~~ | ~~`build_effective_schemas`~~ | ~~48L~~ → 65L | Clippy UI silencieux — pas de violation détectée. |

### Catégorie A — Dispatch exhaustif — légitimes, ne pas refactoriser

| Fichier | Fonction | Raison |
|---|---|---|
| `json2sql-ui/src/state.rs:429` | `apply_progress_event` | dispatch exhaustif sur tous les variants `ProgressEvent` |

### Catégorie C — Algo compact ou couplage fort — ne pas toucher

| Fichier | Fonction | Raison |
|---|---|---|
| `json2sql-ui/src/screens/preview.rs:296` | `tokenize_ddl` | lexer état-par-état, découper = perte de lisibilité |
| `json2sql-ui/src/screens/mod.rs:166` | `tree_display_order` | DFS tree walk auto-contenu |
| `json2sql-ui/src/main.rs:24` | `main` | config desktop + injection JS — couplage fort Dioxus |

## ~~Améliorations futures~~ ✅ résolues (confirmé 2026-06-06)

~~Log des flush périodiques~~ · ~~Double barre progression ImportScreen~~

## Backlog sibling detection — analyse sur schema_261_tables.json (2026-05-26)

### ~~Option A~~ ✅ livré (commit 6436ad0)
Bypass child-compat gate quand Jaccard frères ≥ 0.9. `run_sibling_wave`, `HIGH_JACCARD = 0.9`.

### ~~Option B~~ ✅ livré (commit cc8d1ae)
Second passage `run_sibling_wave` après le cascade BFS. `finalize_cascading`.

### ~~Option C~~ ✅ livré 2026-05-29
ScalarArray inclus dans `build_parent_child_maps` → `arr_map`.
`src/schema/cascading.rs` ligne ~153 : `Some(ChildKind::ObjectArray) | Some(ChildKind::ScalarArray)`.
2 tests ajoutés dans `registry.rs`.

## Dette Design — suppression de `j2s_data jsonb` quand inutile (2026-06-06)

**Constat :** `j2s_data jsonb` est aujourd'hui ajoutée à toutes les tables comme colonne de débordement,
même quand tous les champs JSON sont déjà mappés en colonnes typées explicites.

**Impact :** chaque insert doit sérialiser l'objet JSON complet en jsonb pour chaque ligne — coût inutile
quand aucun champ ne déborde.

**Action :** conditionner l'ajout de `j2s_data` à la présence effective de champs non-mappés.
Identifier le point d'injection (rechercher `j2s_data` dans `src/schema/` et `src/pass2/`) et ajouter
un flag de schéma `has_overflow: bool` calculé en fin de Pass 1.

**Précaution :** vérifier l'impact sur Pass 2 (`insert.rs`) qui lit peut-être toujours `j2s_data`
comme colonne attendue — adapter la logique d'écriture en conséquence.

## Dette Design — architecture de la finalisation du schéma (2026-06-07)

Discussion ouverte — à affiner avant implémentation. Trois problèmes distincts identifiés.

### Problème 1 — `WideStrategy` mélange deux catégories différentes

L'enum `WideStrategy` (`src/schema/table_schema.rs`) contient :
- des **stratégies auto-inférées** par Pass 1 : `Pivot`, `Jsonb`, `StructuredPivot`, `KeyedPivot`, `AutoSplit`, `MultiKeyedPivot`
- des **overrides manuels IHM** : `Flatten`, `JsonbFlatten`, `NormalizeDynamicKeys`, `Ignore`

Ces deux catégories ont des cycles de vie différents (inférence vs. décision utilisateur) et des sites d'application différents (finalizer vs. config.rs). Les mélanger dans un seul enum rend difficile de raisonner sur "qu'est-ce que le système a décidé seul" vs. "qu'est-ce que l'utilisateur a demandé".

**Piste :** séparer en `InferredStrategy` et `ManualOverride`, avec `TableSchema.strategy` et `TableSchema.override` distincts — ou conserver un seul champ mais typer proprement la distinction.

**Question ouverte :** l'IHM sérialise/désérialise `WideStrategy` dans le JSON de schéma — un split d'enum implique une migration du format de persistance.

---

### Problème 2 — `apply_column_limit_guard` vit hors du pipeline de stratégies

La seule transformation *obligatoire* (respect de la limite 1600 colonnes PostgreSQL) est appelée dans `pass1/runner.rs::build_pass1_result` (ligne ~156), *après* `registry.finalize()`. Elle n'est pas dans `SchemaFinalizer` et n'est pas visible dans le pipeline de décision.

**Piste :** remonter `apply_column_limit_guard` dans `SchemaFinalizer::run()`, après le cascade BFS. La transformation obligatoire serait alors dans le même endroit que les transformations optionnelles, et l'ordre des opérations serait explicite.

**Conséquence :** `Pass1Result.overflow_warnings` reste inchangé côté API publique.

---

### Problème 3 — Deux phases de transformation implicites et non nommées

La finalisation comporte deux phases distinctes qui ne sont nulle part documentées comme telles :
1. **Phase per-table (parallèle)** : `build_entry_schema` → `apply_wide_strategy` → décide Pivot/Jsonb/StructuredPivot/AutoSplit pour chaque table indépendamment
2. **Phase multi-table (BFS séquentiel)** : `finalize_cascading` → décide KeyedPivot/MultiKeyedPivot en comparant les tables entre elles

Ces deux phases ont des contraintes différentes (indépendance vs. vision globale) et des ordres de traitement différents (top-down vs. bottom-up). Le code ne le dit nulle part explicitement.

**Piste :** introduire une struct `FinalizationPlan` ou deux méthodes nommées `apply_per_table_strategies` et `apply_cross_table_strategies` dans `SchemaFinalizer::run()`, avec un commentaire expliquant pourquoi l'ordre importe.

**Question ouverte liée :** la vision bottom-up pure (partir des feuilles, remonter niveau par niveau, boucler tant qu'il y a des fusions) serait plus cohérente que le découpage actuel. À évaluer si le gain de clarté justifie la réécriture de `finalize_cascading`.

---

### Principe directeur émergent

La seule stratégie *obligatoire* est le respect de la limite PostgreSQL (`apply_column_limit_guard`). Toutes les autres transformations sont des heuristiques optionnelles activées par les seuils de config. Le mode "aucune stratégie sélectionnée" devrait produire un schéma brut (une colonne par clé JSON) + uniquement le guard PG. Ce principe n'est pas actuellement exprimable directement — il faut désactiver chaque stratégie manuellement via `disabled_strategies`.
