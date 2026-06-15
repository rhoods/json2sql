# Feature : code_review fixes — Phase B+D performance & correctness

## Description
Implémenter les 7 findings restants de la code review sur `ddl.rs`, `copy_sink.rs` et `runner.rs`. Deux correctifs performance (Phase D `maintenance_work_mem` + Phase B bin-packing) et cinq correctifs robustesse/mémoire (fuite fichier, session COPY orpheline, memcpy redondant, sinks vides, heap non libérée).

## Motivation
Phase D prend 3h+ à cause du `maintenance_work_mem` par défaut (64 MB) sur les builds d'index PK — un `SET maintenance_work_mem = '4GB'` devrait ramener ça à ~20 min. Phase B ne parallélise pas vraiment sur données skewed car le round-robin ignore la taille des tables. Les autres findings couvrent des fuites fichiers, corruptions silencieuses et gaspillage mémoire sur longs runs.

## Modules / fichiers impactés
- `src/db/ddl.rs`
- `src/db/copy_sink.rs`
- `src/pass2/runner.rs`

## Tâches de développement
1. **ddl.rs:204** — `add_constraints` : exécuter `SET maintenance_work_mem = '4GB'; SET synchronous_commit = off;` juste après `connect`
2. **runner.rs:329** — Phase B : remplacer round-robin par bin-packing greedy (trier sinks par taille DESC, assigner à la connexion la moins chargée)
3. **runner.rs:265** — filtrer les sinks interim-only (`row_count == 0`) avant distribution Phase B
4. **copy_sink.rs:137** — vérifier existence du spill file avant d'ouvrir la session COPY
5. **copy_sink.rs:141** — corriger la fuite de fichier sur chemin d'erreur (cleanup explicite si `stream_file_chunks` échoue)
6. **copy_sink.rs:344** — éliminer le memcpy redondant dans `stream_file_chunks` (BytesMut::read_buf + freeze)
7. **copy_sink.rs:279** — `spill()` : `mem::take(&mut self.pending)` au lieu de `pending.clear()` pour libérer la capacité heap

## Impacts et edge cases

### Risques de régression identifiés
1. **ddl.rs:204 — maintenance_work_mem hardcodé** — avec `parallel=8`, PG reçoit 8 connexions demandant 4GB chacune → risque OOM sur petits serveurs. Utiliser 1GB par défaut ou rendre configurable via `J2S_MAINTENANCE_WORK_MEM`.
2. **runner.rs:329 — poids bin-packing incorrect** — le poids doit être `row_count` seul (données Phase B restantes), pas `total_flushed + row_count` (`total_flushed` est déjà dans `interim_rows`).
3. **runner.rs:329 — parallel=0** — `table_batches` vide, accès index 0 → panic. Garder avec early return si `parallel == 0`.
4. **copy_sink.rs:137 — race condition file check** — la vérification d'existence avant `copy_in` ne ferme pas la race ; si le fichier disparaît entre le check et `stream_file_chunks`, le `CopyInSink` reste ouvert. Le Drop de `tokio_postgres` envoie un abort sync — comportement correct mais implicite.
5. **copy_sink.rs:344 — BytesMut re-alloc** — après `split()`, `buf` perd sa capacité. Créer un nouveau `BytesMut::with_capacity(4MiB)` à chaque itération plutôt que de gérer la capacité résiduelle.

## Documentation
- Spec technique : `_bmad-output/feature-code-review-fixes-phase-bd-technical.md`
