# Features terminées — json2sql

_Archivage des features complétées avec date. Ordre anti-chronologique : la plus récente en premier._

<!-- Format :
## YYYY-MM-DD — Nom de la feature
Description courte de ce qui a été fait.

Ajouter toujours EN HAUT du fichier. -->

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
