# Feature : Corrections code review — COPY direct grandes tables

## Description
Corriger 6 issues identifiées par code review dans l'implémentation COPY-direct grandes tables. 2 bugs HIGH bloquants prod (semaphore bypass + synchronous_commit absent), 2 MEDIUM (row_count=0 tables synthétiques + confusion Closed/Full), 2 LOW (spill file orphan + driver error masqué).

## Motivation
Issue #1 ouvre N connexions PG sans garde (`too many clients` en prod avec N grandes tables). Issue #2 annule le bénéfice perf principal du COPY-direct (WAL flush non désactivé, 10x plus lent). Les 4 autres dégradent la robustesse, le périmètre de l'optimisation, et la diagnosabilité des erreurs.

## Modules / fichiers impactés
- `src/pass2/runner.rs` — issues #1, #2, #4, #6
- `src/schema/cascading/detection.rs` — issue #3
- `src/db/copy_sink.rs` — issue #5

## Tâches de développement
1. **#1 HIGH** — Acquérir `copy_sem` dans `run_copy_direct_task` avant `tokio_postgres::connect` (renommer `_copy_sem` → `copy_sem`)
2. **#2 HIGH** — Appeler `try_set_synchronous_commit_off` dans `run_copy_direct_task` juste après connexion
3. **#4 MEDIUM** — `trigger_budget_flush` : distinguer `Err(Closed)` de `Err(Full)` et propager l'erreur immédiatement au lieu d'appeler `force_spill()`
4. **#3 MEDIUM** — Propager la somme des `row_count` sources dans les 3 builders synthétiques (`build_multi_group_entry`, `build_co_sibling_schema`, `build_sub_pivot_schema`) dans `detection.rs`
5. **#5 LOW** — `stream_snapshot_to_open_copy` : appeler `cleanup_spill_file` avant le `?` sur erreurs non-NotFound de `verify_spill_file_exists`
6. **#6 LOW** — `run_copy_direct_task` : spawner le driver PG et logger l'erreur driver en `tracing::warn!` au lieu de `drop()`

## Impacts et edge cases

### Risques de régression identifiés
1. **CRITIQUE — Deadlock `worker_teardown_flush`** (`runner.rs:288-310`) — Quand N grandes tables > `parallel`, les COPY-direct tasks bloquent sur `acquire_owned()`. Teardown appelle `send().await` sur un channel plein → workers bloqués → import figé indéfiniment. Fix requis : remplacer `send().await` par `try_send` + `force_spill` fallback (même pattern que `trigger_budget_flush`).
2. **MOYEN — Même bug #5 dans `copy_snapshot_to_pg`** (`copy_sink.rs:160`) — La fix #5 corrige `stream_snapshot_to_open_copy` mais pas `copy_snapshot_to_pg` qui a le même pattern `verify_spill_file_exists(...).await?` sans cleanup. Les deux doivent être corrigés.
3. **MOYEN — Saturation sémaphore** (`runner.rs:765-784`) — Les COPY-direct tasks tiennent leur permit pour toute la durée de Phase A. Si N grandes tables >= `parallel`, zéro permit disponible pour les interim-COPY des petites tables → toutes tombent sur le chemin Phase B. Performance dégradée, fonctionnement correct.
4. **FAIBLE — Over-estimate row_count SiblingCollapse** (`detection.rs:689-706`) — Pour les tables pivot, `sum(absorbed.row_count)` peut sur-estimer le vrai row_count. Des petites tables synthétiques pourraient être classifiées "large". Résultat correct mais overhead COPY-direct inutile.
5. **FAIBLE — Attribution erreur toujours incomplète** (`runner.rs:577-615`) — La fix #4 améliore la détection mais l'erreur root (failure COPY-direct) reste cachée derrière l'erreur worker dans `first_error`.

## Documentation
- Spec technique : `_bmad-output/feature-code-review-copy-direct-technical.md`
