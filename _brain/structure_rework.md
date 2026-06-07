
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
