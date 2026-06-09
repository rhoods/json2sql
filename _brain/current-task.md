# Tâches en cours — Pass 2 : COPY direct pour grandes tables

Source : `_brain/current-feature.md`
Spec technique : `_bmad-output/feature-pass2-copy-direct-large-tables-technical.md`

## Tâches
- [x] 1. `Pass2Config` : ajouter `large_table_threshold: Option<u64>` — propager dans `pipeline.rs::run_pass2` et `PipelineConfig`
- [x] 2. `classify_tables(schemas, threshold) → HashSet<String>` — filtre par `TableSchema::row_count`
- [x] 3. `spawn_copy_direct_task(table_name, copy_sql, pg_url, copy_sem, progress_tx)` — task persistante COPY STDIN, loop recv FlushSnapshot, close quand channel fermé, retourne `(table_name, rows)`
- [x] 4. `WorkerConfig` : ajouter `copy_direct_senders: Arc<HashMap<String, Sender<FlushSnapshot>>>`; initialiser dans `run()`
- [x] 5. `trigger_budget_flush` : pour les grandes tables → `try_reserve()` + `take_flush_snapshot()` + `permit.send()`; si Full → fallback `force_spill`
- [x] 6. `run_worker` teardown : `worker_teardown_flush()` — snapshot final + `send().await` pour grandes tables; `force_spill` pour petites tables
- [x] 7. `join_phase_a` : recevoir `Vec<CopyDirectHandle>` en paramètre, les inclure dans l'await après les workers
- [x] 8. `run()` : orchestrer classify → spawn_copy_direct_tasks → WorkerConfig mis à jour → drop(worker_cfg) → join_phase_a avec les deux sets
- [ ] 9. CLI `--large-table-threshold` (cli.rs) + UI Dioxus Setup Advanced (json2sql-ui)
- [ ] 10. Benchmarks : comparer temps total + pic disque avant/après sur le dataset 70 GB

## Notes d'implémentation
- `try_reserve()` utilisé (pas `try_send`) pour éliminer la race TOCTOU entre check capacity et prise de snapshot
- `worker_teardown_flush()` nouvelle fonction async — `run_worker` l'appelle à la fin
- `drop(worker_cfg)` explicite dans `run()` avant `join_phase_a` pour que les COPY-direct tasks voient le channel fermé dès que les workers finissent
- `TempFileSink::copy_sql()` getter ajouté (`pub(crate)`)
- `CopyDirectHandle` et `COPY_DIRECT_CHANNEL_CAP = 4` définis dans runner.rs
- 324 tests unitaires passent, 0 failures
