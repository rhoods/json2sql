# Features terminées — json2sql

_Archivage des features complétées avec date. Ordre anti-chronologique : la plus récente en premier._

<!-- Format :
## YYYY-MM-DD — Nom de la feature
Description courte de ce qui a été fait.

Ajouter toujours EN HAUT du fichier. -->

## 2026-06-11 — Code review fixes — Pass 2 diskless pipeline (9 findings)

9 bugs corrigés sur le pipeline diskless. 312 tests passent (lib + UI).

**Critical #1** — Deadlock sur erreur PG en RAM-pressure : `run_flusher` vide `pause_flag` avant tout chemin d'erreur ; worker pause spin vérifie `error_flag` + re-check après spin. Sans ces deux fixes combinés, les workers se gelaient indefiniment sur erreur PG.

**Critical #2** — Flusher leak : `flusher_handle.abort()` + await dans les deux bras d'erreur de `anomaly_writer_handle` dans `run()`.

**High #3** — Error surfacing : `flusher_result` examiné avant `first_error` — retourne l'erreur PG réelle (table + SQL) au lieu du message générique "flusher reported a fatal error".

**High #4** — `Pass2Error` event : `flush_table_to_pg` émet `ProgressEvent::Pass2Error { table_name, message }` en temps réel avant de retourner `Err`.

**High #5** — UI dead code post-diskless : section "Temp directory", `picking_temp`, `temp_free_bytes`, `DiskWarnLevel`, `disk_warning_level` + 6 tests supprimés de `setup.rs`. `ProjectConfig.temp_dir` conservé pour compat TOML.

**Medium #6+7** — Validation `Pass2Config` : `validate_watermarks` extraite et testée (9 tests) — `is_finite` + bornes `(0.0, 1.0]` pour `ram_high`, `(0.0, 1.0)` pour `ram_low`, `low < high`, `threshold > 0`.

**Medium #8** — Flush toutes tables non-vides par tick RAM : `find_largest_buffer` remplacé par `find_all_nonempty_buffers` (4 tests).

**Medium #9** — Chunked streaming dans `flush_mem_sink_to_pg` : boucle `feed` 4 MiB + `flush()` + `close()` (5 tests), remplace le `send(buf)` monolithique.

**Fichiers** : `src/pass2/runner.rs`, `src/db/copy_sink.rs`, `json2sql-ui/src/screens/setup.rs`, `src/io/progress_event.rs`.

## 2026-06-10 — Fix code review + observabilité Pass2 (6 findings runner.rs)

6 corrections chirurgicales dans `src/pass2/runner.rs` + 2 champs observabilité dans `copy_sink.rs`. 346 tests passent.

**Critical — Fix #1** : `copy_sem` retiré de `run_copy_direct_task` / `spawn_copy_direct_task` — supprime le deadlock quand `|large_table_set| >= parallel` (copy_direct tasks tenaient tous les permits, bloquant les interim-COPY tasks → deadlock). Test `copy_direct_task_blocks_when_semaphore_exhausted` supprimé (testait le bug), remplacé par `copy_direct_task_completes_without_semaphore`.

**High — Fix #2** : `per_worker_budget = Some(0)` validé dans `validate_run_params` → erreur explicite. Évitait un flush sur chaque ligne JSON.

**Medium — Fix #3** : `worker_teardown_flush` distingue `TrySendError::Closed` (propager erreur, copy_direct task crashée) vs `Full` (spill disque normal). Avant : crash silencieux masqué.

**Medium — Fix #4** : `MIN_SPILL_BYTES` abaissé de 4 MiB à 512 KiB — la branche spill dans `trigger_budget_flush` était morte (MIN_SPILL == MIN_SINK_COPY == 4 MiB). Sur un schéma 255 tables, des centaines de sinks sub-4 MiB s'accumulaient en RAM jusqu'au teardown → 110 GB sur disque après streaming. Désormais, les sinks entre 512 KiB et 4 MiB sont spillés progressivement.

**Low — Fix #5+6** : Commentaire ligne 44 (cap × per_worker_budget → cap × SPILL_THRESHOLD ≈ 48 MiB) ; docstring `worker_teardown_flush` ("guaranteed blocking send" → "non-blocking try_reserve").

**Phase 2 — Observabilité** : `bytes_sent_direct` et `bytes_spilled` ajoutés à `TempFileSink`. Log par table après Phase A : `[Pass2 routing] table=X: sent_direct=NB spilled=MB`. Permet de diagnostiquer le ratio mémoire→PG direct vs disque→Phase B sur le fichier 70 GB / 255 tables.

**Fichiers** : `src/pass2/runner.rs`, `src/db/copy_sink.rs`.

## 2026-06-09 — Correctifs code review — 10 findings

7 corrections sur `ddl.rs`, `pass2/runner.rs`, `copy_sink.rs`. 309 tests passent.

**Sécurité/durabilité** : `SET maintenance_work_mem='1GB'` + `SET synchronous_commit=off` supprimés des connexions contraintes (Phase D) — élimine OOM avec high parallelism, régression durabilité WAL, et crash fatal sur RDS/Supabase.

**Load-balancing** : `distribute_sinks` utilise `bytes_buffered` au lieu de `row_count` — corrige le sous-pondèrage des tables à lignes larges en Phase B.

**Observabilité** : `cleanup_spill_file` log les erreurs de suppression sur stderr (était silencieux).

**Performance** : `stream_file_chunks` buffer `BytesMut` alloué hors loop + `buf.split()` — élimine ~17 500 allocs de 4 MB sur un spill de 70 GB.

**Robustesse** : contexte `NotFound` dans `stream_file_chunks` pour TOCTOU post-verify ; `unwrap_or(0)` → `.expect(...)` dans `distribute_sinks`.

**Nettoyage** : test orphelin `stream_file_reads_all_bytes_in_chunks` supprimé ; idiome `let _ = mem::take` corrigé.

## 2026-06-09 — code_review fixes — Phase B+D performance & correctness

7 findings de code review implémentés sur `ddl.rs`, `copy_sink.rs`, `runner.rs`. 311 tests passent.

**Performance** : `apply_constraints_chunk` exécute `SET maintenance_work_mem='1GB'` + `SET synchronous_commit=off` sur chaque connexion DDL — élimine les tris multi-pass sur disque pour les gros index PK (Phase D). Phase B remplace le round-robin alphabétique par un bin-packing greedy (`distribute_sinks`, poids=`row_count` DESC) — connexion la moins chargée reçoit la prochaine table.

**Robustesse** : sinks interim-only (`row_count==0`) filtrés avant Phase B (`unwrap_and_sort_sinks`). `verify_spill_file_exists` détecte les fichiers manquants avant d'ouvrir la session COPY. `cleanup_spill_file` garantit la suppression du spill file même si `stream_file_chunks` échoue.

**Mémoire/perf I/O** : `stream_file_chunks` utilise `BytesMut::read_buf` + `freeze` (élimine 1 memcpy par chunk de 4 MiB). `spill()` utilise `mem::take` au lieu de `clear()` — libère la capacité heap entre les cycles de flush.

**Fichiers** : `src/db/ddl.rs`, `src/db/copy_sink.rs`, `src/pass2/runner.rs`.

## 2026-06-08 — Table name trimming par suppression de segments gauches

Remplace le hash immédiat par suppression progressive des segments de gauche quand un nom de table dépasse `PG_TABLE_MAX_IDENT` (53 chars).

**Algorithme** : Phase 1 — supprime les segments de gauche un à un jusqu'à ce que le nom tienne. Phase 2 — fallback hash FNV-1a gardant parent direct + leaf (`parent_leaf_XXXXXXX`). Cas mono-segment : `leaf_XXXXXXX`.

**Fichiers** : `src/schema/naming.rs` (`floor_char_boundary`, `truncate_table_name`, `ensure_unique` recâblé) + `src/schema/cascading/scoring.rs` (commentaire mis à jour).

**Tests** : 294 passent (+15 nouveaux). `truncate_to_pg_limit` (colonnes) inchangée.

**Breaking change** : `TruncatedName.pg_name` dans vieux snapshots est hash-based — diverge silencieusement des nouveaux noms.

## 2026-06-07 — Refactoring finalisation de schéma : InferredStrategy + UserOverride (T1–T10)

Sprint complet — 10 tâches, 271 tests passent, zéro warning.

**T1** — `WideStrategy` renommé `InferredStrategy` ; `UserOverride { Pivot, Jsonb, Skip }` ajouté avec `#[serde(alias = "Ignore")]` sur `Skip`.

**T2** — `finalizer.rs` restructuré : `apply_per_table_strategies` + `apply_cross_table_strategies` ; `Ignore` conservé comme marqueur interne de sœurs absorbées.

**T3** — `apply_column_limit_guard` intégré dans `SchemaFinalizer::run()` via champ `apply_pg_guard: bool`.

**T4** — `exclude_absorbed_children` déplacé **après** `apply_column_limit_guard` (invariant : guard voit les stratégies finales avant exclusion).

**T5** — Deux phases nommées dans `finalizer.rs` (per-table / cross-table).

**T6 + T8** — `strategy_overrides: HashMap<String, UserOverride>` partout (persistence, state, UI) ; IHM réduite à `Pivot | Jsonb | Skip` ; bouton "merge as siblings" retiré.

**T7** — `apply_user_overrides()` dans `config.rs` : `Skip` retire la table + le companion `_wide` si `AutoSplit`.

**T9** — `restore_from_snapshot` applique les overrides ; `SchemaRegistry::finalize_with_pg_guard()` encapsule le guard ; appel direct retiré de `runner.rs`.

**T10** — Tous les tests adaptés (271 passent) ; `build_effective_schemas` (UI) délègue à `apply_user_overrides` pour éviter la divergence sur AutoSplit + Skip (fix post-review).

**Règle runtime** : UI pas testée manuellement (Dioxus desktop) — vérification runtime toujours à faire avant release.

## 2026-05-30 — IHM : temp dir picker + double barre progression (T1–T2)

Deux features UI suite au pass2-refactor (séparation streaming/COPY) :

**T1 — Temp dir picker (Setup → Advanced)**
- `ProjectState.temp_dir: Option<PathBuf>` + persistance TOML (`ProjectConfig`)
- Bloc field-row "Temp directory" dans Step 4 Advanced, pattern identique à anomaly_dir
- Probe disk free space via `fs2::available_space` (nouveau dep cross-platform)
- Warning vert/jaune/rouge : `disk_warning_level(free, source_size)` fn pure testée × 6
- Note conditionnelle "PG local → réserver 2×" si pg.host == localhost
- `temp_dir` propagé dans le `use_coroutine` de ImportScreen (remplace le `None` hardcodé)

**T2 — Double barre de progression (ImportScreen)**
- `progress_pct(done, total) → u32` fn pure extraite dans `screens/mod.rs` (testée × 5)
- Composant `ProgressBar { pct, done, label, phase }` partagé Analysis + Import
- ImportScreen : 2 barres en grid 1fr/1fr — "A · Streaming" (bytes_read/total) + "B · Inserting" (tables_done/total)
- AnalysisScreen refactorisé pour utiliser `ProgressBar` + `progress_pct`
- Zéro changement backend (les events `Pass2Progress` et `Pass2Flush` existaient déjà)

## 2026-05-30 — Pass 2 refactoring : séparation streaming / COPY (T1–T5)

Refactoring complet du pipeline Pass 2 pour résoudre l'accumulation de ~71 000 fichiers
temporaires observée sur OpenFoodFacts (70 GB). Root cause : flush_task unbounded sans
backpressure. Architecture retenue : Phase A streaming pur (aucune connexion PG), Phase B
COPY post-streaming parallèle.

- **T1** `--temp-dir` CLI : `Option<&Path>` dans `TempFileSink::new()` → `NamedTempFile::new_in()`, propagé jusqu'aux workers. 2 nouveaux tests.
- **T2** Streaming COPY par chunks : `merge_copy_to_db` remplace `tokio::fs::read()` par boucle `AsyncReadExt::read()` 4 MiB. Évite OOM sur tables denses (~500 MB). 2 nouveaux tests.
- **T3** Auto-hibernate FD : `spill()` ferme le FD immédiatement après `write_all`. `is_open()` toujours `false` après spill. Suppression `global_open_fds`, `FD_GLOBAL_THRESHOLD`, `global_sub()`, `my_open`.
- **T4** Suppression infrastructure flush-during-streaming : `flush_task`, `conn_pool`, drain cycle, RAM pressure check, `MIN_SINK_HANDOFF_BYTES`, `bytes_on_disk`, `INTERIM_FLUSH_THRESHOLD` entièrement supprimés. Workers retournent leurs sinks via `JoinHandle`.
- **T5** Phase B parallèle : pool de `parallel` connexions PG, tables distribuées round-robin, résultats via `JoinHandle<Result<Vec<(String, u64)>>>` (plus de `result_tx`/`result_rx`).

Résultat : ~3 920 fichiers (245 × 16) au lieu de ~71 000. Même disk usage peak, filesystem sain.

## 2026-05-25 — T2+T3 Jaccard similarity + Merge as siblings (Strategy panel)

- **Backend** : `pairwise_jaccard_min` → `pub` dans `cascading.rs` ; `build_keyed_pivot_from_siblings(&[TableSchema], &[usize], key_col_name) -> Result<MergeResult, MergeError>` — valide la sélection (≥2 tables, même parent, pas de routing table), extrait les clés depuis les suffixes de noms, produit `KeyedPivot` (clés homogènes) ou `MultiKeyedPivot` (clés mixtes num+txt) ; `extract_key_suffixes` privée ; 8 tests unitaires
- **State** : `JaccardDisplay`, `compute_jaccard_display` (score + ratio common/union + same_parent check) ; `SchemaState::apply_sibling_merge` (appelle builder, écrit overrides, reset sélection sur parent) ; 6 tests
- **UI** : panneau droit Strategy en mode multi-select — barre Jaccard colorée (rouge/orange/vert) + ratio colonnes + bloc "Merge as siblings" conditionnel (visible si même parent) avec champ key_col_name, warning low similarity, feedback d'erreur

## 2026-05-23 — IHM — Refacto SOLID (T-A, T-B) dead code + double-launch fix

- **T-A** : suppression de `strategy_configurator.rs` (282 lignes) jamais utilisé depuis la refonte E1-E5 ; retire `pub mod strategy_configurator` de `screens/mod.rs`
- **T-B** : once-flag `use_signal<bool>` ajouté en tête de `use_coroutine` dans `analysis.rs` et `import.rs` — élimine la race condition issue #9 (double-launch si Dioxus remonte le composant avant que `abort_handle` soit écrit)

## 2026-05-23 — IHM — Refacto SOLID (T5–T6)

- **T5** `TableRowViewModel` + `build_table_rows` : pure function extraite dans `screens/mod.rs`, 14 tests unitaires
- **T6** `TableListPanel` unifié : composant partagé par Strategy et Preview via `show_checkboxes: bool`

## 2026-05-23 — IHM — Refacto SOLID (T1–T4)

Quatre tâches de maintenabilité préparatoires aux prochaines modifications UX.
- **T1** `strategy_configurator.rs` : 10 boutons copier-collé → boucle sur `strategy_options()` + helper `apply_strategy_override` (pur, testé). Ajouter une stratégie = 1 ligne.
- **T2** Timer coroutine : `use_elapsed_timer<F>` hook extrait dans `screens/mod.rs`, partagé par `analysis.rs` et `import.rs`.
- **T3** Constantes Pass1 : `TEXT_THRESHOLD` etc. déplacées de `analysis.rs` vers `state.rs` (6 `pub const PASS1_*` avec doc comments).
- **T4** `PgConnectionForm` : ~145 lignes extraites de `SetupScreen` — inputs PG + test connexion encapsulés dans un sous-composant dédié.

## 2026-05-22 — IHM Dioxus — refonte design system (E1–E5 complet)

Refonte complète des 5 écrans avec le design system CSS (styles.css 1046 lignes).
- **F3** : persistance TOML via `directories-next` (config.rs, password exclu, load on startup)
- **E1 SetupScreen** : wizard 4 étapes accordion (`.step-card`, file picker rfd, test connexion `.cdot`, parallelism cards, try_save sur chaque write)
- **E2 AnalysisScreen** : dashboard 4-up `.stat-tile` + `pre.code` log colorisé + elapsed timer coroutine
- **E3 StrategyScreen** : `.split-3` + `.pane` collapsibles + `table.t` filtrable + `.strat-list`/`.strat-btn` + multi-select + save schema
- **E4 PreviewScreen** : `.split-3` + `pre.code` DDL avec tokenizer SQL (`.kw`/`.ty`/`.pn`) + diff summary overrides
- **E5 ImportScreen** : `.split-60-40` + `.log` colorisé + `table.t` per-table rows + `.prog.thick` + success banner
Suppression de `TableListPanel`, `SummaryRow`, `strategy_configurator.rs` usage. Zéro erreur Rust.

## 2026-04-12 — IHM Dioxus — implémentation complète
5 écrans entièrement câblés. Setup (file picker rfd + formulaire PG + test connexion),
Analysis (Pass 1 runner via use_coroutine + spawn_blocking), Strategy (éditeur de stratégie
interactif, badges, indentation depth), Preview (DDL généré via generate_create_table),
Import (Pass 2 pipeline complet : connect → DDL → COPY, per-table progress).
Cancel/abort_handle, log cappé 500 lignes, reset état complet.

## 2026-04-12 — IHM Dioxus — squelette
Migration de Leptos vers Dioxus. Squelette complet 5 écrans (Setup, Analysis, Strategy, Preview, Import/Done).
État global via `Signal<AppState>`, design system `theme.rs` aligné sur `docs/DESIGN.md`.
`AppState::apply_progress_event` consomme les `ProgressEvent` du CLI (Pass1/Pass2).

## 2026-03-23 — Schema persistence
`--schema-input` / `--schema-output` : sauvegarde et rechargement du snapshot JSON après Pass 1, skip du Pass 1 entièrement si fourni.

## 2026-03-23 — Fix VARCHAR overflow
`coerce()` vérifie la longueur pour `PgType::VarChar(n)` → retourne `Anomaly` au lieu de crasher le COPY.

## 2026-03-23 — Fix NOT NULL violation
NOT NULL uniquement pour colonnes générées (`j2s_id`, `j2s_parent_id`, `j2s_order`), jamais pour les colonnes user-data.

## 2026-03-23 — Fix FK violation flush
Flush de toutes les tables en ordre topologique quand n'importe quelle table atteint le seuil de batch.
## 2026-06-11 — Task 9 complete : TempFileSink entièrement supprimé
copy_sink.rs, sink.rs, runner.rs, et tous les tests d'intégration nettoyés. Pipeline 100%% MemSink.
cargo build : 0 erreur, 0 warning. cargo test --lib : 290 tests passent.

