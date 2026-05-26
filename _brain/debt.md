# Dette Technique — json2sql

## À adresser avant release
<!-- Ex: "Authentification non implémentée — accès libre aux routes" -->

## Améliorations futures
- IHM Leptos bancale — à consolider (visualisation du schéma)
- Log des flush périodiques (`flush tablename (N rows)`)

## Backlog sibling detection — analyse sur schema_261_tables.json (2026-05-26)

### Option A — Assouplir le child-compat gate
**Cas pertinent** : `nutriscore_2021` vs `nutriscore_2023` — Jaccard propre = 1.0 mais
child-compat bloque car `nutriscore_2021_data` et `nutriscore_2023_data` ont des schémas
différents (formule nutriscore différente entre 2021 et 2023).
Fix : bypass le child-compat quand Jaccard propre ≥ seuil_haut (ex. 0.9).
Code : `src/schema/cascading.rs`, fonction `run_sibling_wave`, branche non-mixed, après
le check `child_compatibility_score` (ligne ~530).

### Option B — Second passage de run_sibling_wave après le cascade BFS
**Cas pertinent** : `cluster_0_sizes` et `cluster_1_sizes` — les tables `100`, `200`, `400`,
`full`, `num` sont créées par `process_co_sibling_group` APRÈS que `run_sibling_wave` a
tourné. Le post-pass (`run_keyed_pivot_children_wave`) ne les voit pas car il ne cible que
les parents `KeyedPivot`, or `cluster_X_sizes` a `WideStrategy::Columns`.
Fix : appeler `run_sibling_wave` une seconde fois après le cascade BFS (avant le post-pass),
ou étendre `run_keyed_pivot_children_wave` aux parents `Columns` ayant des enfants similaires.
Code : `src/schema/cascading.rs`, fonction `finalize_cascading`, après les deux loops BFS.

### Option C — ScalarArray inclus dans la détection de frères
**Cas pertinent** : `nova_groups_markers` (tables 2, 3, 4 — toutes `['value']`) et
`ingredients_analysis` (9 tables hash — toutes `['value']`) ont `child_kind=ScalarArray`.
Dans `build_parent_child_maps`, le match `_ => {}` exclut ScalarArray → jamais évalués.
Fix : ajouter `Some(ChildKind::ScalarArray) => arr_map.entry(...).push(i)` (ou obj_map)
dans `build_parent_child_maps`.
Code : `src/schema/cascading.rs`, fonction `build_parent_child_maps`, ligne ~134.
