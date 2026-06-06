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

### Catégorie B — Extractibles (phases séquentielles nommées) — à traiter

| Priorité | Fichier | Fonction | Taille | Phases identifiées |
|---|---|---|---|---|
| 🔴 1 | `src/pass1/runner.rs:187` | `run_inspect` | ~51L | registry config → scan with limit → result assembly |
| 🔴 2 | `src/schema/suffix_detector.rs:51` | `detect_suffix_schema` | ~? | build → filter → coverage → sanity → build-result |
| 🟡 3 | `src/schema/cascading/detection.rs:282` | `detect_mixed_collapse` | ~48L | decision gate multi-branch déjà partiellement délégué |
| 🟡 4 | `src/schema/cascading/detection.rs:409` | `detect_homogeneous_collapse` | ~54L | filter → jaccard → child-compat → multi vs classic dispatch |
| 🟡 5 | `src/schema/cascading/detection.rs:940` | `process_keyed_pivot_work_item` | ~43L | jaccard gate → resolve → build TableSchema → co-siblings → reparent |
| 🟢 6 | `src/pass2/runner.rs:176` | `run_worker` (async) | ~44L | receive → parse → insert → budget check |
| 🟢 7 | `src/pass2/runner.rs:400` | `dispatch_loop` (async) | ~44L | dispatch + limit + progress bar + progress channel |
| 🟢 8 | `src/schema/wide_strategies.rs:242` | `apply_normalize_dynamic_keys` | ~46L | find → collect → classify → mutate → log → exclude |

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

## Améliorations futures
- Log des flush périodiques (`flush tablename (N rows)`)
- Double barre progression ImportScreen (Phase A streaming / Phase B COPY) — voir ux-todo.md

## Backlog sibling detection — analyse sur schema_261_tables.json (2026-05-26)

### ~~Option A~~ ✅ livré (commit 6436ad0)
Bypass child-compat gate quand Jaccard frères ≥ 0.9. `run_sibling_wave`, `HIGH_JACCARD = 0.9`.

### ~~Option B~~ ✅ livré (commit cc8d1ae)
Second passage `run_sibling_wave` après le cascade BFS. `finalize_cascading`.

### ~~Option C~~ ✅ livré 2026-05-29
ScalarArray inclus dans `build_parent_child_maps` → `arr_map`.
`src/schema/cascading.rs` ligne ~153 : `Some(ChildKind::ObjectArray) | Some(ChildKind::ScalarArray)`.
2 tests ajoutés dans `registry.rs`.
