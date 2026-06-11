# Feature : Fix code review + observabilité Pass2

## Description
Deux phases. D'abord corriger les 6 findings du code review dans `runner.rs` (1 deadlock critique, 1 bug de validation, 1 erreur silencieuse, 1 dead branch de spill, 2 corrections de commentaires). Ensuite ajouter du logging structuré pour savoir, par table, combien de données passent par copy_direct (mémoire→BDD direct) vs spill disque→Phase B.

## Motivation
Le deadlock (#1) est un bug de production reproductible dès que le nombre de grandes tables atteint le parallélisme. Les bugs #2 et #3 masquent des erreurs silencieuses. L'observabilité est nécessaire pour diagnostiquer les 1h30+ de streaming sur 70 GB / 255 tables (dont 15 000 sans sibling fusion).

## Modules / fichiers impactés
- `src/pass2/runner.rs` (principal)
- Tests unitaires / intégration à identifier

## Tâches de développement

### Phase 1 — Corrections

1. **[Critical]** Séparer les sémaphores copy_direct et interim-COPY pour supprimer le deadlock (runner.rs:718)
2. **[High]** Valider `per_worker_budget > 0` dans `run()` avec erreur explicite (runner.rs:121)
3. **[Medium]** Dans `worker_teardown_flush`, distinguer `TrySendError::Closed` (propager l'erreur) vs `Full` (spill normal) (runner.rs:371)
4. **[Medium]** Baisser `MIN_SPILL_BYTES` en dessous de `MIN_SINK_COPY_BYTES` pour que le spill branch soit atteignable en config par défaut (runner.rs:137)
5. **[Low]** Corriger le commentaire ligne 44 (calcul mémoire in-flight cap × per_worker_budget → cap × SPILL_THRESHOLD)
6. **[Low]** Corriger le docstring `worker_teardown_flush` ligne 354 ("guaranteed blocking send" → non-blocking try_reserve)

### Phase 2 — Observabilité

7. Logger par table : bytes envoyés via copy_direct vs bytes spillés sur disque, + nombre de fichiers créés en Phase B

## Impacts et edge cases

### Risques de régression identifiés

1. **Fix #1 — compilation** : `spawn_copy_direct_task` perd son paramètre `copy_sem` → 3 tests cassent à la compilation (`copy_direct_task_propagates_pg_connection_error`, `copy_direct_task_sender_cap_none_uses_default`, `copy_direct_task_sender_cap_some_is_preserved`). Le test `copy_direct_task_blocks_when_semaphore_exhausted` teste le bug lui-même → à supprimer.
2. **Fix #1 — connexions PG** : sans semaphore sur les copy_direct tasks, N grandes tables = N connexions COPY simultanées. Si `|large_table_set| > max_connections - parallel`, PG refuse des connexions. Pas de guard actuel → documenter la contrainte.
3. **Fix #3 — handles orphelins** : si `worker_teardown_flush` propage une erreur Closed, les `copy_handles` JoinHandles du worker sont droppés sans être joinés. Les tâches background continuent de tenir des connexions PG jusqu'à leur fin naturelle.
4. **Fix #3 — shadowing d'erreur** : l'erreur de teardown (Closed) peut s'enregistrer comme `first_error` dans `join_phase_a` avant que l'erreur PG réelle de la copy_direct task soit lue. Le message "channel closed" masque la cause racine.
5. **Fix #4 — commentaire contradictoire** : le commentaire lignes 132-136 défend explicitement `MIN_SPILL_BYTES == MIN_SINK_COPY_BYTES` → à réécrire.
6. **Fix #4 — bytes_buffered après force_spill** : vérifier que `force_spill` remet `bytes_buffered` à 0 ; sinon le load balancer Phase B (`distribute_sinks`) surpondère les sinks déjà spillés.
7. **Observabilité — compteurs** : incrémenter `bytes_spilled` dans le chemin Full (fallback), `bytes_sent_direct` uniquement dans le chemin `Ok(permit)` — les deux chemins doivent être distincts pour que les métriques soient correctes.

## Documentation
- Spec technique : `_bmad-output/feature-code-review-pass2-technical.md`
