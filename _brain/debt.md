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
Seuil actuel : **60 lignes** (abaissé depuis 80 le 2026-06-04).
Prochain palier : à définir.

### Fonctions extraites de `cascading.rs::run_sibling_wave` (2026-06-04)
`SiblingDetectCtx` struct · `build_work_items` · `make_subgroup` · `try_unified_fallback` · `try_cluster_fallback` · `detect_mixed_collapse` · `detect_homogeneous_collapse` · `apply_single_collapse` · `apply_multi_collapse` · `apply_collapses`
10 nouveaux tests unitaires ajoutés (build_work_items×3, make_subgroup×3, detect_*×4)

### Fonctions extraites de `pass2/insert.rs::insert_object` (2026-06-04)
`InsertCtx<'a, S, A>` struct · `write_root_jsonb` · `dispatch_child_object` (élimine duplication) · `write_autosplit_rows` · `recurse_children`
1 nouveau test d'intégration ajouté : `test_jsonb_strategy_root_table` (chemin root-Jsonb non couvert précédemment)

### Fonctions extraites de `pass2/runner.rs::run` (2026-06-04)
`WorkerConfig` struct · `InterimCopyHandle`/`WorkerHandle` type aliases · `trigger_budget_flush` · `run_worker` · `phase_copy` · `spawn_anomaly_writer` · `preflight_warn_nonempty` · `dispatch_loop` · `join_phase_a` · `finalize_dispatch` · `emit_completion_events` · `log_constraint_warnings`

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
