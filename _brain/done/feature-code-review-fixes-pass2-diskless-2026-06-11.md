# Feature : Code Review Fixes — Pass 2 Diskless Pipeline

## Description
Corriger les 9 bugs identifiés en code review sur le pipeline diskless livré. 2 critiques (deadlock sur erreur PG + flusher leak), 2 high (mauvaise erreur remontée + UI regression Pass2Error), 5 medium (validations de config manquantes + performance buffer).

## Motivation
Le pipeline diskless est livré mais présente des bugs de correction garantissant un hang sur erreur PG en RAM-pressure, un flusher qui continue en background après early return, et des régressions visibles (plus d'événement Pass2Error, section UI dead code). Les validations manquantes peuvent rendre le backpressure silencieusement inopérant (ram_high > 1.0, threshold = 0).

## Modules / fichiers impactés
- `src/pass2/runner.rs` — bugs #1, #2, #3, #4, #6, #7, #8
- `src/db/copy_sink.rs` — bug #9
- `json2sql-ui/src/screens/setup.rs` — bug #5

## Tâches de développement
1. **[Critical]** `runner.rs` — Fix deadlock : vérifier `error_flag` dans la pause spin loop + `pause_flag.store(false)` sur tous les chemins de sortie de `run_flusher`
2. **[Critical]** `runner.rs` — Fix flusher leak : await `flusher_handle` dans tous les chemins de sortie de `run()`, y compris early return `anomaly_writer_handle`
3. **[High]** `runner.rs` — Fix error surfacing : examiner `flusher_result` avant `first_error`, retourner l'erreur PG réelle
4. **[High]** `runner.rs` — Fix Pass2Error event : émettre `progress_event::Pass2Error` depuis `flush_table_to_pg` sur échec PG
5. **[High]** `setup.rs` — Supprimer la section "Temp directory" et les warnings disk-space (dead code post-diskless)
6. **[Medium]** `runner.rs` — Ajouter validation `Pass2Config` : `ram_high_watermark` ∈ `(0.0, 1.0]`, `mem_flush_threshold_bytes` > 0
7. **[Medium]** `runner.rs` — Fix RAM flush : flush toutes les tables au-dessus du watermark par tick (pas seulement la plus grande)
8. **[Medium]** `copy_sink.rs` — Implémenter chunked streaming dans `flush_mem_sink_to_pg` (chunks de 4 MiB max)

## Impacts et edge cases

### Risques de régression identifiés
1. **runner.rs:287 — Pause spin + error_flag timing** — Même avec fix #1 (pause_flag cleared), la boucle `while pause_flag { yield_now() }` ne vérifie pas `error_flag` en interne. Worker peut sortir du spin et traiter un item supplémentaire avant de voir `error_flag` au prochain tour. Fix complémentaire : `while pause_flag.load(Relaxed) && !error_flag.load(Acquire) { yield_now().await; }`.
2. **ProgressEvent::Pass2Error variant (fix #4)** — Si la variante n'existe pas dans `progress_event.rs`, fix #4 ne compile pas. Changement en deux parties : ajouter la variante à l'enum ET un handler dans le composant UI.
3. **NaN/+Inf dans ram_high_watermark (fix #6)** — Validation `> 1.0` contourne NaN (comparaison toujours false). Besoin de `!ram_high.is_finite()` en plus de la borne.
4. **Abort flusher sur anomaly_writer error (fix #2)** — Si le flusher draine ses buffers finaux quand on l'abort, des lignes sont perdues sans avertissement. Acceptable, mais devrait émettre un log stderr.
5. **Flush multi-tables par tick (#8) + WORKER_CHANNEL_CAP** — Le drain de toutes les tables dans le bras tick bloque `rx.recv`. Avec `WORKER_CHANNEL_CAP=256` et workers paused, pas de deadlock, mais à documenter.
6. **CopyInSink drop mid-chunk (fix #9)** — Drop sans `close()` envoie une erreur PG et rollback proprement. Correct, mais risque si retry ajouté plus tard : ne pas réutiliser le même `Bytes` après échec partiel.
7. **Signaux orphelins setup.rs (fix #5)** — `picking_temp`, `temp_free_bytes`, `DiskWarnLevel`, `pick_folder`, `pg_host_is_local` potentiellement dead code après suppression de la section. Grep avant de supprimer.
8. **temp_dir pass-through** — Vérifier qu'aucun code glue ne mappe encore `ProjectConfig.temp_dir` vers `Pass2Config` ou les args CLI.

## Documentation
- Spec technique : `_bmad-output/feature-code-review-fixes-pass2-diskless-technical.md`
