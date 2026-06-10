# Feature : Pass 2 — Budget RAM dynamique par worker

## Description
Remplacer la constante `PER_WORKER_FLUSH_THRESHOLD` hardcodée (256 MiB) par un budget calculé dynamiquement au lancement de Pass 2, basé sur la RAM disponible détectée via `sysinfo`. La fonction `compute_worker_budget` prend `factor` et `floor` en paramètres pour permettre les tests déterministes.

## Motivation
12 GB de fichiers temporaires observés pour 6.8 GB scannés avec 16 workers. Le budget fixe 256 MiB × 16 = 4 GB ne s'adapte pas à la RAM disponible, causant des spills disque inutiles sur machines bien dotées.

## Modules / fichiers impactés
- `src/pass2/runner.rs` — constantes, `Pass2Config`, `run()`, nouvelle fn `compute_worker_budget`
- `src/pipeline.rs` — construction `Pass2Config`
- `json2sql-ui/src/screens/import.rs` — construction `Pass2Config`
- `tests/common/mod.rs` — construction `Pass2Config`
- `tests/integration_*.rs` — constructions `Pass2Config`

## Tâches de développement
1. `MIN_SPILL_BYTES` : 1 MiB → 4 MiB dans `runner.rs` (alignement `MIN_SINK_COPY_BYTES`)
2. Fonction `compute_worker_budget(num_workers, override_budget, factor, floor)` + remplacement dans `run()`
3. Exposer `ram_usage_factor: Option<f64>` et `min_budget_floor: Option<u64>` dans `Pass2Config` (obligatoire pour testabilité)
4. Tests : override bypass, floor via paramètres injectés, guard `num_workers > 0`

## Décisions de design
- `usage_factor = 0.4` par défaut (laisser 60% libre pour PG, OS, buffers)
- `MIN_BUDGET_FLOOR = 64 MiB` par défaut
- `MIN_SPILL_BYTES = 4 MiB` aligné sur `MIN_SINK_COPY_BYTES`
- Guard `max(1, num_workers)` dans `compute_worker_budget`
- `sys.refresh_memory()` seul (pas `new_all()`) pour minimiser l'overhead
- Le calcul est bypass si `Pass2Config.per_worker_budget` est `Some(_)`

## Impacts et edge cases

### Risques de régression identifiés
1. **Ordre d'appel `validate_run_params` / `compute_worker_budget`** — `runner.rs:run()` — si `compute_worker_budget` est appelée avant `validate_run_params`, `parallel=0` cause une division par zéro. Appeler après, ou utiliser `parallel` post-`max(1)` (ligne 785).
2. **`config.parallel` vs `parallel` post-max** — `runner.rs:785` — `let parallel = config.parallel.max(1)` existe déjà. `compute_worker_budget` doit recevoir ce `parallel` post-max, pas `config.parallel` brut.
3. **`ram_usage_factor` non validé** — `Pass2Config` — `Some(2.0)` ou `Some(-0.1)` produisent un budget absurde sans erreur. Valider dans `run()` ou `compute_worker_budget`.
4. **`min_budget_floor = Some(0)` non validé** — `Pass2Config` — budget 0 → flush sur chaque objet inséré, régression de performance catastrophique.
5. **Commentaire `COPY_DIRECT_CHANNEL_CAP`** — `runner.rs:46` — référence `PER_WORKER_FLUSH_THRESHOLD` comme multiplicateur ; deviendra faux avec budget dynamique. Mettre à jour.
6. **10+ call sites `Pass2Config`** — `pipeline.rs`, `import.rs`, `tests/common/mod.rs`, `tests/integration_*.rs` — compiler force les mises à jour, mais le crate `json2sql-ui` risque d'être oublié si compilé indépendamment.
7. **Test `trigger_budget_flush_tiny_sink_stays_in_memory`** — `runner.rs:1654` — hardcode `min_spill_bytes: 1024*1024` (1 MiB) alors que `MIN_SPILL_BYTES` passe à 4 MiB en Tâche 1 — drift sémantique à corriger.
8. **`sysinfo` retourne 0 en environnement restreint** — `compute_worker_budget` — sur containers sans `/proc`, le warning "RAM insuffisante" est trompeur. Distinguer "RAM basse" de "sysinfo indisponible".

### Ignorés
_(aucun)_

## Documentation
- Spec technique : `_bmad-output/feature-pass2-dynamic-ram-budget-technical.md`
