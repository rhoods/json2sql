# Tâches en cours — Pass 2 — Pipeline sans disque (diskless)

Source : `_brain/current-feature.md`
Spec technique : `_bmad-output/feature-pass2-diskless-pipeline-technical.md`

## Tâches
- [x] 1. `copy_sink.rs` — créer `MemSink` + `flush_mem_sink_to_pg` (guard buf vide)
- [x] 2. `sink.rs` — adapter `RowSink` : supprimer impls `TempFileSink`, ajouter `impl RowSink for MemSink`
- [x] 3. `runner.rs` — créer `run_flusher` (channel rx, HashMap BytesMut, select! + timer 1s, sysinfo poll, pause_flag, error_flag)
- [x] 4. `runner.rs` — créer `run_worker_diskless` (sinks locaux, send().await après insert_object, yield_now sur pause_flag, check error_flag)
- [x] 5. `runner.rs` — modifier `run()` : pipeline diskless, flusher concurrent, await flusher avant add_constraints
- [x] 6. `runner.rs` — ajouter `mem_flush_threshold_bytes`, `ram_high_watermark`, `ram_low_watermark` à `Pass2Config` ; `build_copy_sql_map` helper
- [x] 7. `pass2/mod.rs` — re-exports à nettoyer
- [x] 8. `pipeline.rs` + `cli.rs` — supprimer `temp_dir`, `large_table_threshold` (breaking CLI)
- [x] 9. Supprimer/adapter tests obsolètes (tous fichiers + tests d'intégration)

## Résultat final
- `cargo build` : 0 erreur, 0 warning
- `cargo test --lib --bins` : 290 tests passent
- TempFileSink entièrement supprimé ; pipeline 100% MemSink
