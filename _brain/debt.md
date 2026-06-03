# Dette Technique — json2sql

## À adresser avant release
<!-- Ex: "Authentification non implémentée — accès libre aux routes" -->

## Dette Complexité — fonctions trop longues (T5, 2026-06-02)

Fonctions annotées `#[allow(clippy::too_many_lines/cognitive_complexity)]` — à refactoriser par priorité :

| Priorité | Fonction | Lignes | Complexité | Refactoring suggéré |
|---|---|---|---|---|
| 🔴 1 | `cascading.rs::run_sibling_wave` | 528 | 57 | Extraire: Jaccard pass, clustering pass, re-parenting pass |
| 🔴 2 | `pass2/runner.rs::run` | 341 | 42 | Extraire: phase_stream(), phase_copy(), phase_constraints() |
| 🔴 3 | `pass2/insert.rs::insert_object` | 235 | 41 | InsertContext struct + sous-fonctions par type JSON |
| 🟡 4 | `schema/finalizer.rs::build_entry_schema` | 193 | — | FinalizerConfig struct |
| 🟡 5 | `schema/config.rs::apply_overrides` | 145 | — | Sub-handlers par type d'override |
| 🟡 6 | `cascading.rs::run_keyed_pivot_children_wave` | 134 | — | Extraction pivot logic |
| 🟢 7 | `cascading.rs::process_co_sibling_group` | 107 | — | Extraction absorption logic |
| 🟢 8 | `pass1/runner.rs::run_parallel` | 123 | — | Extraction worker setup |

Gate CI : `cargo clippy -- -D warnings -D clippy::too_many_lines -D clippy::cognitive_complexity`
Chaque `#[allow]` retiré = dette remboursée.

## Améliorations futures
- IHM Leptos bancale — à consolider (visualisation du schéma)
- Log des flush périodiques (`flush tablename (N rows)`)

## Backlog sibling detection — analyse sur schema_261_tables.json (2026-05-26)

### ~~Option A~~ ✅ livré (commit 6436ad0)
Bypass child-compat gate quand Jaccard frères ≥ 0.9. `run_sibling_wave`, `HIGH_JACCARD = 0.9`.

### ~~Option B~~ ✅ livré (commit cc8d1ae)
Second passage `run_sibling_wave` après le cascade BFS. `finalize_cascading`.

### ~~Option C~~ ✅ livré 2026-05-29
ScalarArray inclus dans `build_parent_child_maps` → `arr_map`.
`src/schema/cascading.rs` ligne ~153 : `Some(ChildKind::ObjectArray) | Some(ChildKind::ScalarArray)`.
2 tests ajoutés dans `registry.rs`.
